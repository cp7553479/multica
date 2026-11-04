package handler

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// noteTriggerFailure extends the existing runtime refusal comments. Notices
// are best-effort and bypass the comment-trigger pipeline to avoid new runs.
func (h *Handler) noteTriggerFailure(ctx context.Context, issue db.Issue, content string, logAttrs ...slog.Attr) {
	params := db.CreateCommentParams{
		ID: dbid.NewV7(), IssueID: issue.ID, WorkspaceID: issue.WorkspaceID,
		AuthorType: "system", AuthorID: pgtype.UUID{Valid: true},
		Content: content, Type: "system",
	}
	created, err := h.Queries.CreateComment(ctx, params)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			logAttrs = append(logAttrs, slog.Any("error", err), slog.String("issue_id", uuidToString(issue.ID)))
			slog.LogAttrs(ctx, slog.LevelWarn, "trigger failure notice: create system comment failed", logAttrs...)
		}
		return
	}
	comment := created.Comment()
	h.publish(protocol.EventCommentCreated, uuidToString(issue.WorkspaceID), "system", "", map[string]any{
		"comment":             commentToResponse(comment, nil, nil),
		"issue_title":         issue.Title,
		"issue_assignee_type": textToPtr(issue.AssigneeType),
		"issue_assignee_id":   uuidToPtr(issue.AssigneeID),
		"issue_status":        issue.Status,
		"issue_revision":      created.IssueRevision,
	})
}
