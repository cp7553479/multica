package service

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

var ErrIssueAgentSessionPinConflict = errors.New("issue session pin conflicts with its recorded identity or active execution")

// PinIssueAgentSession commits the provider identity to the execution and its
// logical session together. A delayed first receipt may repair a cancelled run,
// but cannot replace an established conversation or another execution's pin.
func (s *TaskService) PinIssueAgentSession(ctx context.Context, taskID pgtype.UUID, providerSessionID, workDir string) error {
	if s.TxStarter == nil {
		return errors.New("issue session pin requires a transaction")
	}
	if (providerSessionID != "" && strings.TrimSpace(providerSessionID) == "") || (workDir != "" && strings.TrimSpace(workDir) == "") || (providerSessionID == "" && workDir == "") {
		return ErrIssueAgentSessionPinConflict
	}
	return s.runInTx(ctx, func(q *db.Queries) error {
		task, err := q.LockIssueAgentSessionTask(ctx, taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrIssueAgentSessionPinConflict
		}
		if err != nil {
			return err
		}
		sessionID := IssueAgentSessionID(task)
		if !sessionID.Valid {
			return ErrIssueAgentSessionPinConflict
		}
		session, err := q.GetIssueAgentSessionForUpdate(ctx, sessionID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrIssueAgentSessionPinConflict
		}
		if err != nil {
			return err
		}
		if !task.RuntimeID.Valid || task.RuntimeID != session.RuntimeID || task.AgentID != session.AgentID || task.IssueID != session.IssueID {
			return ErrIssueAgentSessionPinConflict
		}
		terminal := task.Status == "cancelled" || task.Status == "failed" || task.Status == "completed"
		if session.ActiveTaskID != task.ID && !(terminal && !session.ActiveTaskID.Valid) {
			return ErrIssueAgentSessionPinConflict
		}
		if task.Status != "running" && task.Status != "dispatched" && !terminal {
			return ErrIssueAgentSessionPinConflict
		}
		matches := func(recorded pgtype.Text, received string) bool {
			return received == "" || !recorded.Valid || recorded.String == received
		}
		if !matches(task.SessionID, providerSessionID) || !matches(session.ProviderSessionID, providerSessionID) || !matches(task.WorkDir, workDir) || !matches(session.WorkDir, workDir) {
			return ErrIssueAgentSessionPinConflict
		}
		if task.SessionID.Valid && session.ProviderSessionID.Valid && task.SessionID.String != session.ProviderSessionID.String {
			return ErrIssueAgentSessionPinConflict
		}
		if task.WorkDir.Valid && session.WorkDir.Valid && task.WorkDir.String != session.WorkDir.String {
			return ErrIssueAgentSessionPinConflict
		}
		resolve := func(received string, execution, logical pgtype.Text) string {
			if received != "" {
				return received
			}
			if execution.Valid && execution.String != "" {
				return execution.String
			}
			return logical.String
		}
		providerSessionID = resolve(providerSessionID, task.SessionID, session.ProviderSessionID)
		workDir = resolve(workDir, task.WorkDir, session.WorkDir)
		if strings.TrimSpace(providerSessionID) == "" || strings.TrimSpace(workDir) == "" {
			return ErrIssueAgentSessionPinConflict
		}
		pinned, err := q.PinIssueAgentSessionTask(ctx, db.PinIssueAgentSessionTaskParams{
			TaskID: task.ID, ProviderSessionID: pgtype.Text{String: providerSessionID, Valid: true}, WorkDir: pgtype.Text{String: workDir, Valid: true},
		})
		if err != nil {
			return err
		}
		_, err = q.PinIssueAgentSessionPointer(ctx, db.PinIssueAgentSessionPointerParams{
			SessionID: session.ID, ProviderSessionID: pinned.SessionID, WorkDir: pinned.WorkDir,
		})
		return err
	})
}
