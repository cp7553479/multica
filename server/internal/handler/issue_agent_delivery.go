package handler

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const issueAgentDeliveryCommentLimit = 2000

type IssueAgentDeliveryReceipt struct {
	EventID        string  `json:"event_id"`
	SessionID      string  `json:"session_id"`
	AgentID        string  `json:"agent_id"`
	AgentName      string  `json:"agent_name"`
	AgentAvatarURL *string `json:"agent_avatar_url"`
	SourceID       string  `json:"source_id"`
	SourceRevision int64   `json:"source_revision"`
	Status         string  `json:"status"`
	DeliveredAt    *string `json:"delivered_at"`
}

type IssueAgentCommentDelivery struct {
	CommentID      string                      `json:"comment_id"`
	SourceRevision int64                       `json:"source_revision"`
	Receipts       []IssueAgentDeliveryReceipt `json:"receipts"`
}

type IssueAgentDeliverySnapshot struct {
	SourceRevision *int64                      `json:"source_revision"`
	Title          string                      `json:"title"`
	Description    *string                     `json:"description"`
	Status         string                      `json:"status"`
	Stage          *int32                      `json:"stage"`
	AssigneeID     *string                     `json:"assignee_id"`
	Receipts       []IssueAgentDeliveryReceipt `json:"receipts"`
}

type IssueAgentDeliveriesResponse struct {
	Issue    IssueAgentDeliverySnapshot  `json:"issue"`
	Comments []IssueAgentCommentDelivery `json:"comments"`
}

// ListIssueAgentDeliveries projects durable receipts for the content currently
// displayed on an issue. POST only bounds the read's comment-id input; it does
// not create an event, execution, or acknowledgement.
func (h *Handler) ListIssueAgentDeliveries(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	var req struct {
		CommentIDs *[]string `json:"comment_ids"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10))
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.CommentIDs == nil || len(*req.CommentIDs) > issueAgentDeliveryCommentLimit {
		writeError(w, http.StatusBadRequest, "comment_ids must contain at most 2000 UUIDs")
		return
	}
	commentIDs, ok := parseUUIDSliceOrBadRequest(w, *req.CommentIDs, "comment_ids")
	if !ok {
		return
	}
	ctx := r.Context()
	workspaceID := uuidToString(issue.WorkspaceID)
	actorType, actorID := h.resolveActor(r, requestUserID(r), workspaceID)
	sessions, err := h.Queries.ListIssueAgentSessions(ctx, db.ListIssueAgentSessionsParams{WorkspaceID: issue.WorkspaceID, IssueID: issue.ID})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load delivery access")
		return
	}
	visibleSessionIDs := make([]pgtype.UUID, 0, len(sessions))
	visibleAgents := make(map[pgtype.UUID]db.Agent, len(sessions))
	for _, session := range sessions {
		agent, err := h.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: session.AgentID, WorkspaceID: issue.WorkspaceID})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to load delivery access")
			return
		}
		if h.canAccessPrivateAgent(ctx, agent, actorType, actorID, workspaceID) {
			visibleSessionIDs = append(visibleSessionIDs, session.ID)
			visibleAgents[agent.ID] = agent
		}
	}
	snapshot, err := h.Queries.GetIssueAgentDeliverySnapshot(ctx, db.GetIssueAgentDeliverySnapshotParams{WorkspaceID: issue.WorkspaceID, IssueID: issue.ID})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load issue delivery snapshot")
		return
	}
	response := IssueAgentDeliveriesResponse{
		Issue: IssueAgentDeliverySnapshot{
			Title: snapshot.Title, Description: textToPtr(snapshot.Description), Status: snapshot.Status,
			AssigneeID: uuidToPtr(snapshot.AssigneeID), Receipts: []IssueAgentDeliveryReceipt{},
		},
		Comments: []IssueAgentCommentDelivery{},
	}
	if snapshot.SourceRevision > 0 {
		response.Issue.SourceRevision = &snapshot.SourceRevision
	}
	if snapshot.Stage.Valid {
		response.Issue.Stage = &snapshot.Stage.Int32
	}
	comments, err := h.Queries.ListIssueAgentDeliveryComments(ctx, db.ListIssueAgentDeliveryCommentsParams{
		WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, CommentIds: commentIDs,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load comment delivery versions")
		return
	}
	commentIndex := make(map[string]int, len(comments))
	for _, comment := range comments {
		id := uuidToString(comment.ID)
		commentIndex[id] = len(response.Comments)
		response.Comments = append(response.Comments, IssueAgentCommentDelivery{
			CommentID: id, SourceRevision: comment.Revision, Receipts: []IssueAgentDeliveryReceipt{},
		})
	}
	if len(visibleSessionIDs) > 0 {
		commentReceipts, err := h.Queries.ListIssueAgentCommentDeliveryReceipts(ctx, db.ListIssueAgentCommentDeliveryReceiptsParams{
			WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, CommentIds: commentIDs, SessionIds: visibleSessionIDs,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to load comment delivery receipts")
			return
		}
		for _, row := range commentReceipts {
			i, found := commentIndex[uuidToString(row.SourceID)]
			if found && response.Comments[i].SourceRevision == row.SourceRevision {
				agent := visibleAgents[row.AgentID]
				response.Comments[i].Receipts = append(response.Comments[i].Receipts, IssueAgentDeliveryReceipt{
					EventID: uuidToString(row.ID), SessionID: uuidToString(row.SessionID), AgentID: uuidToString(agent.ID),
					AgentName: agent.Name, AgentAvatarURL: h.resolveAvatarURLPtr(textToPtr(agent.AvatarUrl)),
					SourceID: uuidToString(row.SourceID), SourceRevision: row.SourceRevision,
					Status: row.Status, DeliveredAt: timestampToPtr(row.DeliveredAt),
				})
			}
		}
		issueReceipts, err := h.Queries.ListIssueAgentIssueDeliveryReceipts(ctx, db.ListIssueAgentIssueDeliveryReceiptsParams{
			WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, SessionIds: visibleSessionIDs,
		})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to load issue delivery receipts")
			return
		}
		for _, row := range issueReceipts {
			if response.Issue.SourceRevision != nil && *response.Issue.SourceRevision == row.SourceRevision {
				agent := visibleAgents[row.AgentID]
				response.Issue.Receipts = append(response.Issue.Receipts, IssueAgentDeliveryReceipt{
					EventID: uuidToString(row.ID), SessionID: uuidToString(row.SessionID), AgentID: uuidToString(agent.ID),
					AgentName: agent.Name, AgentAvatarURL: h.resolveAvatarURLPtr(textToPtr(agent.AvatarUrl)),
					SourceID: uuidToString(row.SourceID), SourceRevision: row.SourceRevision,
					Status: row.Status, DeliveredAt: timestampToPtr(row.DeliveredAt),
				})
			}
		}
	}
	writeJSON(w, http.StatusOK, response)
}
