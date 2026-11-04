package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/attribution"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type wakeupSessionPayload struct {
	WakeupID        string          `json:"wakeup_id"`
	Revision        int64           `json:"wakeup_revision"`
	AgentID         string          `json:"target_agent_id"`
	System          string          `json:"system_rule,omitempty"`
	Instruction     string          `json:"instruction"`
	Note            string          `json:"note"`
	Evidence        json.RawMessage `json:"wakeup_evidence"`
	Chain           []string        `json:"wakeup_chain"`
	ReceiptIDs      []pgtype.UUID   `json:"receipt_ids"`
	ParentCommentID *string         `json:"parent_comment_id,omitempty"`
}

// transferToSession atomically hands rule receipts to the durable session inbox.
// processed_at means transferred, not delivered to the provider; event status
// records delivery and survives task failure or a server restart.
func (s *IssueWakeupService) transferToSession(ctx context.Context, q *db.Queries, issue db.Issue, agent db.Agent, w db.IssueWakeup, receipts []db.IssueWakeupReceipt, principal pgtype.UUID, chain []string) (IssueAgentEventDelivery, error) {
	note, evidence := buildWakeupEvidence(w, receipts)
	payload, err := json.Marshal(wakeupSessionPayload{
		WakeupID: util.UUIDToString(w.ID), Revision: w.Revision, AgentID: util.UUIDToString(agent.ID),
		System: w.SystemRule.String, Instruction: w.Instruction, Note: note, Evidence: evidence,
		Chain: chain, ReceiptIDs: receiptIDs(receipts), ParentCommentID: util.UUIDToPtr(w.ParentCommentID),
	})
	if err != nil {
		return IssueAgentEventDelivery{}, err
	}
	delivery, err := s.Tasks.appendIssueAgentEvent(ctx, q, issue, agent.ID, IssueAgentEventInput{
		Kind: "wakeup.fired", SourceID: receipts[0].ID, SourceRevision: w.Revision,
		ActorType: "system", OriginatorUserID: principal, Payload: payload,
	})
	if err != nil {
		return delivery, err
	}
	err = q.ConsumeWakeupReceipts(ctx, db.ConsumeWakeupReceiptsParams{Ids: receiptIDs(receipts)})
	return delivery, err
}

func (s *TaskService) issueAgentEventCanInvoke(ctx context.Context, q *db.Queries, agent db.Agent, event db.IssueAgentEvent) bool {
	if event.Kind == "wakeup.fired" {
		return (&IssueWakeupService{Tasks: s}).allowSessionEvent(ctx, q, agent, event)
	}
	issue, err := q.GetIssue(ctx, eventIssueID(ctx, q, event))
	if err != nil || issue.AssigneeType.String != "agent" || issue.AssigneeID != agent.ID {
		return false
	}
	if input, ok := decodeAutopilotIssueSessionInput(event); ok {
		return autopilotIssueSessionInputAllowed(ctx, q, issue, agent, event, input)
	}
	if event.Kind == "assigned" && event.Sequence == 1 && issue.OriginType.String == "autopilot" {
		return false
	}
	return s.IssueSessionCanInvoke != nil && s.IssueSessionCanInvoke(ctx, agent, event)
}

func eventIssueID(ctx context.Context, q *db.Queries, event db.IssueAgentEvent) pgtype.UUID {
	session, err := q.GetIssueAgentSession(ctx, event.SessionID)
	if err != nil {
		return pgtype.UUID{}
	}
	return session.IssueID
}

// A rule event grants only its explicitly named target. Assignment never
// redirects it, and neither a departed creator nor a changed rule can lend rights.
func (s *IssueWakeupService) allowSessionEvent(ctx context.Context, q *db.Queries, agent db.Agent, event db.IssueAgentEvent) bool {
	var payload wakeupSessionPayload
	if json.Unmarshal(event.Payload, &payload) != nil || payload.AgentID != util.UUIDToString(agent.ID) || event.ActorType != "system" {
		return false
	}
	id, err := util.ParseUUID(payload.WakeupID)
	if err != nil {
		return false
	}
	w, err := q.GetIssueWakeup(ctx, db.GetIssueWakeupParams{ID: id, WorkspaceID: event.WorkspaceID})
	if err != nil || w.DisabledAt.Valid || w.Revision != payload.Revision || w.Revision != event.SourceRevision {
		return false
	}
	session, err := q.GetIssueAgentSession(ctx, event.SessionID)
	if err != nil || session.WorkspaceID != w.WorkspaceID || session.IssueID != w.IssueID || session.AgentID != agent.ID {
		return false
	}
	issue, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: w.IssueID, WorkspaceID: w.WorkspaceID})
	if err != nil {
		return false
	}
	active, err := wakeupIssueActive(ctx, q, issue)
	if err != nil || !active {
		return false
	}
	if !w.SystemRule.Valid {
		return w.AgentID == agent.ID && event.OriginatorUserID == w.CreatedBy && s.authorize(ctx, q, w.WorkspaceID, w.CreatedBy, agent) == nil
	}
	if !w.Enabled || payload.System != w.SystemRule.String {
		return false
	}
	target, err := resolveWakeTarget(ctx, q, issue)
	if err != nil || target.Agent.ID != agent.ID {
		return false
	}
	attr, err := s.childDoneRunAs(ctx, issue, agent)
	if err != nil || attr.UserID != event.OriginatorUserID {
		return false
	}
	if attr.UserID.Valid {
		return s.authorize(ctx, q, w.WorkspaceID, attr.UserID, agent) == nil
	}
	return s.Tasks.IssueSessionCanInvoke != nil && s.Tasks.IssueSessionCanInvoke(ctx, agent, event)
}

func wakeupEventAttribution(event db.IssueAgentEvent) attribution.Result {
	var payload wakeupSessionPayload
	_ = json.Unmarshal(event.Payload, &payload)
	id, _ := util.ParseUUID(payload.WakeupID)
	return attribution.TriggerOwner(event.OriginatorUserID, attribution.EvidenceKind("issue_wakeup"), id)
}

func hasSessionWakeup(ctx context.Context, q *db.Queries, sessionID pgtype.UUID) (bool, error) {
	event, err := q.GetFirstUnsettledIssueAgentEvent(ctx, sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil && event.Kind == "wakeup.fired", err
}

// Cancellation of an unstarted consumer may reveal another principal's input.
// Preserve the logical session and let its head authorize the replacement turn.
func (s *IssueWakeupService) wakeWithdrawnIssueSessions(ctx context.Context, issueID pgtype.UUID) {
	issue, err := s.Tasks.Queries.GetIssue(ctx, issueID)
	if err != nil {
		return
	}
	sessions, err := s.Tasks.Queries.ListIssueAgentSessions(ctx, db.ListIssueAgentSessionsParams{IssueID: issue.ID, WorkspaceID: issue.WorkspaceID})
	if err != nil {
		slog.WarnContext(ctx, "wake inbox after rule withdrawal deferred", "error", err)
		return
	}
	for _, session := range sessions {
		if session.ActiveTaskID.Valid || session.State == "blocked" {
			continue
		}
		if _, err := s.Tasks.WakeIssueAgentSession(ctx, session.ID); err != nil {
			slog.WarnContext(ctx, "wake inbox after rule withdrawal deferred", "session_id", util.UUIDToString(session.ID), "error", err)
		}
	}
}
