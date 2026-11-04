package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const maxWakeupCheckinRunes = 500

// lockManagedWakeup takes Save's lock order (workspace, issue, rule) and
// checks that member may manage the rule: its creator or a workspace admin.
func lockManagedWakeup(ctx context.Context, tx pgx.Tx, q *db.Queries, issueID, id, member pgtype.UUID) (db.Issue, db.IssueWakeup, error) {
	var workspace pgtype.UUID
	if err := tx.QueryRow(ctx, "SELECT w.id FROM workspace w JOIN issue i ON i.workspace_id=w.id WHERE i.id=$1 FOR KEY SHARE OF w", issueID).Scan(&workspace); err != nil {
		return db.Issue{}, db.IssueWakeup{}, err
	}
	issue, err := q.LockWakeupIssue(ctx, issueID)
	if err != nil {
		return issue, db.IssueWakeup{}, err
	}
	membership, err := q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{UserID: member, WorkspaceID: issue.WorkspaceID})
	if err != nil {
		return issue, db.IssueWakeup{}, ErrWakeupForbidden
	}
	w, err := q.LockIssueWakeup(ctx, id)
	if err != nil {
		return issue, w, err
	}
	if w.IssueID != issue.ID || w.WorkspaceID != issue.WorkspaceID {
		return issue, db.IssueWakeup{}, pgx.ErrNoRows
	}
	// System rules are changed on the issue's system rule, not as a person's rule.
	if w.SystemRule.Valid {
		return issue, db.IssueWakeup{}, ErrWakeupForbidden
	}
	if w.CreatedBy != member && membership.Role != "owner" && membership.Role != "admin" {
		return issue, db.IssueWakeup{}, ErrWakeupForbidden
	}
	return issue, w, nil
}

// Trigger ("wake now") queues one run of the rule immediately, as if its
// trigger had happened. It goes through ordinary dispatch, so it merges with
// a queued run and re-checks the creator's permission to invoke the agent. A
// rule someone turned off, or one the platform stopped for a loop or burst,
// must be turned on first.
func (s *IssueWakeupService) Trigger(ctx context.Context, issueID, id, member pgtype.UUID) error {
	tx, err := s.Tasks.TxStarter.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	q := s.Tasks.Queries.WithTx(tx)
	issue, w, err := lockManagedWakeup(ctx, tx, q, issueID, id, member)
	if err != nil {
		return err
	}
	active, err := wakeupIssueActive(ctx, q, issue)
	if err != nil {
		return err
	}
	if !active {
		return fmt.Errorf("%w: issue is closed", ErrWakeupInput)
	}
	if w.DisabledAt.Valid {
		return fmt.Errorf("%w: turn the wakeup on first", ErrWakeupInput)
	}
	agent, err := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: w.AgentID, WorkspaceID: w.WorkspaceID})
	if err != nil {
		return ErrWakeupForbidden
	}
	if err = s.authorize(ctx, q, w.WorkspaceID, member, agent); err != nil {
		return err
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return err
	}
	key := "manual:" + util.UUIDToString(dbid.NewV7())
	payload, _ := json.Marshal(map[string]any{"event_id": key, "requested_at": now.UTC().Format(time.RFC3339),
		"actor_type": "member", "actor_id": util.UUIDToString(member)})
	if _, err = q.RecordWakeupReceipt(ctx, db.RecordWakeupReceiptParams{ID: dbid.NewV7(), WakeupID: w.ID, Revision: w.Revision, EventKey: key, EventType: wakeupManualEventType, Payload: payload}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	// Dispatch now rather than on the next tick; the scheduler retries if
	// this attempt loses a lock race.
	_ = s.dispatch(ctx, w)
	return nil
}

// Delete removes a rule and its pending inputs, and withdraws its runs that
// have not started. Started runs keep the ordinary Stop control, and their
// history stays in the run list.
func (s *IssueWakeupService) Delete(ctx context.Context, issueID, id, member pgtype.UUID) error {
	tx, err := s.Tasks.TxStarter.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	q := s.Tasks.Queries.WithTx(tx)
	if _, _, err = lockManagedWakeup(ctx, tx, q, issueID, id, member); err != nil {
		return err
	}
	tasks, err := q.CancelUnstartedWakeupTasks(ctx, util.UUIDToString(id))
	if err != nil {
		return err
	}
	if err = q.DeleteIssueWakeupReceipts(ctx, id); err != nil {
		return err
	}
	if err = q.DeleteIssueWakeup(ctx, db.DeleteIssueWakeupParams{ID: id, IssueID: issueID}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	for _, task := range tasks {
		s.Tasks.broadcastTaskEvent(ctx, protocol.EventTaskCancelled, db.AgentTaskQueue(task))
	}
	s.wakeWithdrawnIssueSessions(ctx, issueID)
	return nil
}

// CheckIn records an explicit audit note for a scheduled rule whose input this
// execution received. Notes belong to the rule; final output stays in the run
// transcript and comments are published only through explicit issue tools.
func (s *IssueWakeupService) CheckIn(ctx context.Context, issueID, id, taskID pgtype.UUID, agentID string, note string) error {
	note = strings.TrimSpace(note)
	if note == "" || utf8.RuneCountInString(note) > maxWakeupCheckinRunes {
		return fmt.Errorf("%w: note must contain 1–%d characters", ErrWakeupInput, maxWakeupCheckinRunes)
	}
	tx, err := s.Tasks.TxStarter.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	q := s.Tasks.Queries.WithTx(tx)
	var status string
	var taskIssue, taskAgent pgtype.UUID
	err = tx.QueryRow(ctx, "SELECT status,issue_id,agent_id FROM agent_task_queue WHERE id=$1 FOR UPDATE", taskID).Scan(&status, &taskIssue, &taskAgent)
	if err != nil {
		return ErrWakeupForbidden
	}
	if taskIssue != issueID || util.UUIDToString(taskAgent) != agentID {
		return ErrWakeupForbidden
	}
	if status != "running" {
		return fmt.Errorf("%w: only a running check can check in", ErrWakeupConflict)
	}
	w, err := q.LocklessWakeup(ctx, id)
	if err != nil {
		return err
	}
	var received bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM issue_agent_event e JOIN issue_agent_session s ON s.id=e.session_id
	 WHERE e.delivery_task_id=$1 AND e.kind='wakeup.fired' AND e.payload->>'wakeup_id'=$2
	 AND e.status IN ('delivering','delivered') AND s.issue_id=$3 AND s.agent_id=$4)`, taskID, util.UUIDToString(id), issueID, taskAgent).Scan(&received); err != nil {
		return err
	}
	if !received {
		return ErrWakeupForbidden
	}
	if w.Kind != "every" && w.Kind != "cron" {
		return fmt.Errorf("%w: check-ins are for scheduled checks; post a comment instead", ErrWakeupInput)
	}
	var now time.Time
	if err = tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
		return err
	}
	checkin, _ := json.Marshal(map[string]any{"note": note, "at": now.UTC().Format(time.RFC3339)})
	if _, err = tx.Exec(ctx, "UPDATE agent_task_queue SET context=COALESCE(context,'{}'::jsonb)||jsonb_build_object('wakeup_checkins',COALESCE(context->'wakeup_checkins','{}'::jsonb)||jsonb_build_object($3::text,$2::jsonb)) WHERE id=$1", taskID, checkin, util.UUIDToString(id)); err != nil {
		return err
	}
	activity, err := recordWakeupActivity(ctx, q, w, wakeupActivityCheckin, "agent", taskAgent, map[string]any{"note": note, "task_id": util.UUIDToString(taskID)})
	if err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	s.publishWakeupActivities(activity)
	return nil
}
