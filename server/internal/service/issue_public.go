package service

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// ErrIssueRevisionConflict means the caller based an update on a stale issue
// revision. Transports map it to their stable conflict response.
var ErrIssueRevisionConflict = errors.New("issue revision conflict")

// IssueContentPatch is the first shared Public API write primitive. It is
// deliberately transport- and credential-agnostic: App, PAT, and Plugin
// entrypoints authorize independently, then call the same business operation.
type IssueContentPatch struct {
	Title            *string
	Description      *string
	ExpectedRevision *int64
	ActorType        string
	ActorID          pgtype.UUID
}

// UpdateContent updates only the low-risk issue content fields exposed in the
// first Public API slice. Assignment, status, project, and hierarchy changes
// remain separate operations because each has additional policy and side
// effects.
func (s *IssueService) UpdateContent(ctx context.Context, issue db.Issue, patch IssueContentPatch) (db.Issue, error) {
	params := db.UpdateIssueParams{
		ID:            issue.ID,
		AssigneeType:  issue.AssigneeType,
		AssigneeID:    issue.AssigneeID,
		StartDate:     issue.StartDate,
		DueDate:       issue.DueDate,
		ParentIssueID: issue.ParentIssueID,
		ProjectID:     issue.ProjectID,
		Stage:         issue.Stage,
	}
	if patch.ExpectedRevision != nil {
		params.ExpectedRevision = pgtype.Int8{Int64: *patch.ExpectedRevision, Valid: true}
	}
	if patch.Title != nil {
		params.Title = pgtype.Text{String: *patch.Title, Valid: true}
	}
	if patch.Description != nil {
		params.Description = pgtype.Text{String: *patch.Description, Valid: true}
	}

	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return db.Issue{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT set_config('multica.actor_type',$1,true),set_config('multica.actor_id',$2,true)`, patch.ActorType, util.UUIDToString(patch.ActorID)); err != nil {
		return db.Issue{}, err
	}
	updated, err := s.Queries.WithTx(tx).UpdateIssue(ctx, params)
	if patch.ExpectedRevision != nil && errors.Is(err, pgx.ErrNoRows) {
		return db.Issue{}, ErrIssueRevisionConflict
	}
	if err != nil {
		return db.Issue{}, err
	}
	return updated, tx.Commit(ctx)
}
