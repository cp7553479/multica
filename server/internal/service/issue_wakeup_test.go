package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func wakeFixture(t *testing.T) (principalFixture, *IssueWakeupService, pgtype.UUID, string) {
	t.Helper()
	f, owner := newPrincipalFixture(t)
	if err := f.q.SeedIssueStatusEntries(context.Background(), parseTestUUID(t, f.WorkspaceID)); err != nil {
		t.Fatal(err)
	}
	issue := f.Issue(t, "Wakeup test")
	agent := f.privateAgentOwnedBy(t, owner, "wake")
	// Service-created data is cleaned before fixture owners, including receipts.
	f.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", issue)
	f.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", issue)
	f.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issue)
	return f, &IssueWakeupService{Tasks: f.svc.TaskSvc}, parseTestUUID(t, issue), agent
}
func parseTestUUID(t *testing.T, s string) pgtype.UUID {
	t.Helper()
	id, e := util.ParseUUID(s)
	if e != nil {
		t.Fatal(e)
	}
	return id
}
func wakeCreate(t *testing.T, f principalFixture, s *IssueWakeupService, issue pgtype.UUID, in WakeupInput) db.IssueWakeup {
	t.Helper()
	w, e := s.Create(context.Background(), issue, parseTestUUID(t, f.UserID), pgtype.UUID{}, in)
	if e != nil {
		t.Fatal(e)
	}
	return w
}
func wakeDispatch(t *testing.T, s *IssueWakeupService, w db.IssueWakeup) {
	t.Helper()
	if e := s.dispatch(context.Background(), w); e != nil {
		t.Fatal(e)
	}
}

// wakeSetStatus writes a status the way production writers do: the status
// update and StopClosedIssueWakeups commit in one transaction.
func wakeSetStatus(t *testing.T, f principalFixture, issue pgtype.UUID, status string) []db.AgentTaskQueue {
	t.Helper()
	ctx := context.Background()
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	q := f.q.WithTx(tx)
	updated, err := q.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{ID: issue, Status: status, WorkspaceID: parseTestUUID(t, f.WorkspaceID)})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := StopClosedIssueWakeups(ctx, q, updated)
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return cancelled
}

// wakeupInbox reads the durable inputs transferred by this rule. Receipt
// consumption is independent of provider delivery.
func wakeupInbox(t *testing.T, f principalFixture, id pgtype.UUID) []db.IssueAgentEvent {
	t.Helper()
	rows, err := f.Pool.Query(context.Background(), "SELECT id FROM issue_agent_event WHERE kind='wakeup.fired' AND payload->>'wakeup_id'=$1 ORDER BY sequence", util.UUIDToString(id))
	if err != nil {
		t.Fatal(err)
	}
	var ids []pgtype.UUID
	for rows.Next() {
		var id pgtype.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	var out []db.IssueAgentEvent
	for _, id := range ids {
		e, err := f.q.GetIssueAgentEvent(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}
func wakeupTargetSession(t *testing.T, f principalFixture, issue pgtype.UUID, agent string) db.IssueAgentSession {
	t.Helper()
	s, err := f.q.GetIssueAgentSessionForPair(context.Background(), db.GetIssueAgentSessionForPairParams{WorkspaceID: parseTestUUID(t, f.WorkspaceID), IssueID: issue, AgentID: parseTestUUID(t, agent)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func wakeupSessionTask(t *testing.T, f principalFixture, issue pgtype.UUID, agent string) db.AgentTaskQueue {
	t.Helper()
	session := wakeupTargetSession(t, f, issue, agent)
	if !session.ActiveTaskID.Valid {
		t.Fatalf("session has no active execution: %+v", session)
	}
	task, err := f.q.GetAgentTask(context.Background(), session.ActiveTaskID)
	if err != nil {
		t.Fatal(err)
	}
	if !IsIssueAgentSessionTask(task) || task.HandoffNote.Valid || task.TriggerCommentID.Valid || task.CommentThreadID.Valid {
		t.Fatalf("rule created a legacy execution: %+v", task)
	}
	var carrier map[string]json.RawMessage
	if err := json.Unmarshal(task.Context, &carrier); err != nil {
		t.Fatal(err)
	}
	if _, ok := carrier["wakeup_id"]; ok {
		t.Fatalf("rule id leaked into scheduling context: %s", task.Context)
	}
	return task
}
func wakeupClaimEvents(t *testing.T, f principalFixture, s *IssueWakeupService, task db.AgentTaskQueue, limit int32) []db.IssueAgentEvent {
	t.Helper()
	if task.Status == "queued" {
		f.Exec(t, "UPDATE agent_task_queue SET status='dispatched',dispatched_at=clock_timestamp() WHERE id=$1", task.ID)
	}
	events, err := s.Tasks.ClaimIssueAgentEvents(context.Background(), task.ID, limit)
	if err != nil {
		t.Fatal(err)
	}
	return events
}
func wakeupAckEvents(t *testing.T, s *IssueWakeupService, task db.AgentTaskQueue, events []db.IssueAgentEvent) {
	t.Helper()
	for _, e := range events {
		if err := s.Tasks.AckIssueAgentEvent(context.Background(), task.ID, e.ID, true, ""); err != nil {
			t.Fatal(err)
		}
	}
}
func wakeupFinishExecution(t *testing.T, f principalFixture, s *IssueWakeupService, task db.AgentTaskQueue, status string) {
	t.Helper()
	f.Exec(t, "UPDATE agent_task_queue SET status=$2,completed_at=now(),session_id='provider-wakeup-session',work_dir='/tmp/wakeup-session' WHERE id=$1", task.ID, status)
	ended, err := f.q.GetAgentTask(context.Background(), task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Tasks.runInTx(context.Background(), func(q *db.Queries) error { return SettleTerminalTaskState(context.Background(), q, ended) }); err != nil {
		t.Fatal(err)
	}
}

func TestIssueWakeupEventAtomicOnceAndIndependentInputs(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	ctx := context.Background()
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", EventTypes: []string{"comment.created"}, Instruction: "Review latest changes"})
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, "INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content,type) VALUES($1,$2,'member',$3,'rollback','comment')", issue, f.WorkspaceID, f.UserID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1", w.ID); n != 0 {
		t.Fatalf("rollback left %d receipts", n)
	}
	comment := f.Comment(t, util.UUIDToString(issue), "source")
	wakeDispatch(t, s, w)
	wakeDispatch(t, s, w)
	events := wakeupInbox(t, f, w.ID)
	if len(events) != 1 || events[0].Status != "pending" || events[0].DeliveryTaskID.Valid {
		t.Fatalf("once transfer = %+v, want one undelivered inbox input", events)
	}
	if !strings.Contains(string(events[0].Payload), comment) || !strings.Contains(string(events[0].Payload), "Review latest changes") {
		t.Fatalf("lost source or instruction: %s", events[0].Payload)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NOT NULL AND task_id IS NULL", w.ID); n != 1 {
		t.Fatalf("durably transferred receipts = %d, want 1", n)
	}
	got, err := f.q.GetIssueWakeup(ctx, db.GetIssueWakeupParams{ID: w.ID, WorkspaceID: w.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || got.FireCount != 1 {
		t.Fatalf("once rule did not settle after durable transfer: %+v", got)
	}
	task := wakeupSessionTask(t, f, issue, agent)
	if task.OriginatorUserID != parseTestUUID(t, f.UserID) {
		t.Fatal("execution lost the rule creator")
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue); n != 1 {
		t.Fatalf("duplicate dispatch created %d executions", n)
	}
}

func TestIssueWakeupStartsItsOwnRunWhenNothingWaits(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", EventTypes: []string{"comment.created"}, Instruction: "Review latest changes"})
	f.Comment(t, util.UUIDToString(issue), "source")
	wakeDispatch(t, s, w)
	task := wakeupSessionTask(t, f, issue, agent)
	if task.OriginatorUserID != parseTestUUID(t, f.UserID) || task.WorkflowStep.Valid {
		t.Fatalf("explicit auxiliary execution has wrong principal or workflow ownership: %+v", task)
	}
	events := wakeupClaimEvents(t, f, s, task, 32)
	if len(events) != 1 || events[0].Kind != "wakeup.fired" {
		t.Fatalf("session claim = %+v", events)
	}
	wakeupAckEvents(t, s, task, events)
	if got := wakeupInbox(t, f, w.ID); len(got) != 1 || got[0].Status != "delivered" {
		t.Fatalf("provider delivery was not recorded separately: %+v", got)
	}
}

func TestIssueWakeupTerminalStateAndScope(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	source := f.Task(t, agent, testutil.Cols{"issue_id": issue, "status": "completed", "runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agent + "')")})
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", EventTypes: []string{"task.completed"}, FilterTaskID: source, Instruction: "inspect output"})
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1", w.ID); n != 1 {
		t.Fatal("missed already-terminal run")
	}
	wakeDispatch(t, s, w)
	otherIssue := f.Issue(t, "Other issue")
	_, err := s.Create(context.Background(), parseTestUUID(t, otherIssue), parseTestUUID(t, f.UserID), pgtype.UUID{}, WakeupInput{AgentID: agent, Kind: "event", EventTypes: []string{"task.completed"}, FilterTaskID: source, Instruction: "invalid scope"})
	if !errors.Is(err, ErrWakeupInput) {
		t.Fatalf("scope accepted: %v", err)
	}
}

func TestIssueWakeupContinuousSelfLoopAndClose(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	ctx := context.Background()
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created", "task.completed"}, Instruction: "check"})
	f.Comment(t, util.UUIDToString(issue), "one")
	wakeDispatch(t, s, w)
	first := wakeupSessionTask(t, f, issue, agent)
	delivered := wakeupClaimEvents(t, f, s, first, 32)
	wakeupAckEvents(t, s, first, delivered)
	wakeStart(t, f, util.UUIDToString(first.ID))
	f.Comment(t, util.UUIDToString(issue), "self", testutil.Cols{"author_type": "agent", "author_id": agent, "source_task_id": first.ID})
	wakeDispatch(t, s, w)
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 {
		t.Fatalf("self comment produced another session input: %+v", events)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL", w.ID); n != 0 {
		t.Fatal("self comment remained eligible for another dispatch")
	}
	f.Comment(t, util.UUIDToString(issue), "two")
	wakeDispatch(t, s, w)
	f.Comment(t, util.UUIDToString(issue), "three")
	wakeDispatch(t, s, w)
	if task := wakeupSessionTask(t, f, issue, agent); task.ID != first.ID {
		t.Fatal("busy inbox created a second execution")
	}
	if events := wakeupInbox(t, f, w.ID); len(events) != 3 || events[1].Status != "pending" || events[2].Status != "pending" {
		t.Fatalf("busy inbox inputs = %+v", events)
	}
	f.Insert(t, "issue_status", testutil.Cols{"workspace_id": f.WorkspaceID, "key": "finished", "name": "Finished", "category": "done", "color": "#000000"})
	if cancelled := wakeSetStatus(t, f, issue, "finished"); len(cancelled) != 0 {
		t.Fatalf("close cancelled the session execution: %+v", cancelled)
	}
	got, err := f.q.GetIssueWakeup(ctx, db.GetIssueWakeupParams{ID: w.ID, WorkspaceID: w.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || !got.DisabledAt.Valid {
		t.Fatal("custom end state did not disable")
	}
	events := wakeupInbox(t, f, w.ID)
	if events[0].Status != "delivered" || events[1].Status != "ignored" || events[2].Status != "ignored" {
		t.Fatalf("close did not withdraw only undelivered rule inputs: %+v", events)
	}
	task, err := f.q.GetAgentTask(ctx, first.ID)
	if err != nil || task.Status != "running" {
		t.Fatalf("close stopped active execution: %+v %v", task, err)
	}
	if cancelled := wakeSetStatus(t, f, issue, "todo"); len(cancelled) != 0 {
		t.Fatal("reopen cancelled executions")
	}
	got, err = f.q.GetIssueWakeup(ctx, db.GetIssueWakeupParams{ID: w.ID, WorkspaceID: w.WorkspaceID})
	if err != nil || got.Enabled {
		t.Fatal("reopen enabled old subscription")
	}
}

func TestIssueWakeupTimeRecoveryUpdateAndRevocation(t *testing.T) {
	for _, kind := range []string{"at", "every", "cron"} {
		t.Run(kind, func(t *testing.T) {
			f, s, issue, agent := wakeFixture(t)
			ctx := context.Background()
			in := WakeupInput{AgentID: agent, Kind: kind, Instruction: "check CI"}
			switch kind {
			case "at":
				in.AfterSeconds = 600
			case "every":
				in.IntervalSeconds = 3600
			case "cron":
				in.CronExpression = "0 * * * *"
				in.Timezone = "Asia/Shanghai"
			}
			w := wakeCreate(t, f, s, issue, in)
			f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '3 hours' WHERE id=$1", w.ID)
			f.Exec(t, "UPDATE agent_runtime SET status='offline' WHERE id=(SELECT runtime_id FROM agent WHERE id=$1)", agent)
			wakeDispatch(t, s, w)
			wakeDispatch(t, s, w)
			events := wakeupInbox(t, f, w.ID)
			if len(events) != 1 || events[0].Status != "pending" {
				t.Fatalf("offline catchup inputs = %+v", events)
			}
			task := wakeupSessionTask(t, f, issue, agent)
			updated, err := s.Save(ctx, issue, parseTestUUID(t, f.UserID), pgtype.UUID{}, w.ID, in)
			if err != nil {
				t.Fatal(err)
			}
			if updated.Revision != 2 {
				t.Fatal("revision missing")
			}
			events = wakeupInbox(t, f, w.ID)
			if events[0].Status != "ignored" {
				t.Fatalf("stale revision still deliverable: %+v", events)
			}
			current, err := f.q.GetAgentTask(ctx, task.ID)
			if err != nil || current.Status != "cancelled" {
				t.Fatalf("configuration edit retained an execution with no remaining input: %+v %v", current, err)
			}
			if session := wakeupTargetSession(t, f, issue, agent); session.ActiveTaskID.Valid {
				t.Fatalf("configuration edit retained the cancelled consumer: %+v", session)
			}
			f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", w.ID)
			f.Exec(t, "DELETE FROM member WHERE user_id=$1 AND workspace_id=$2", f.UserID, f.WorkspaceID)
			wakeDispatch(t, s, updated)
			got, err := f.q.GetIssueWakeup(ctx, db.GetIssueWakeupParams{ID: w.ID, WorkspaceID: w.WorkspaceID})
			if err != nil || got.Enabled || !got.LastError.Valid {
				t.Fatalf("permission revoked but still enabled: %+v %v", got, err)
			}
			if len(wakeupInbox(t, f, w.ID)) != 1 {
				t.Fatal("revoked creator transferred another input")
			}
		})
	}
}

func TestIssueWakeupValidate(t *testing.T) {
	s := IssueWakeupService{}
	for _, in := range []WakeupInput{{Kind: "event", Instruction: "x", EventTypes: []string{"review_ready"}}, {Kind: "every", Instruction: "x", IntervalSeconds: 1}, {Kind: "cron", Instruction: "x", CronExpression: "bad"}, {Kind: "at", Instruction: "x", AfterSeconds: -1}} {
		if _, err := s.Validate(&in, time.Now()); !errors.Is(err, ErrWakeupInput) {
			t.Errorf("accepted %+v", in)
		}
	}
}

func TestIssueWakeupStatusSourceAndClaimAuthority(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	ctx := context.Background()
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"issue.status_changed"}, Instruction: "check state"})
	f.Exec(t, "UPDATE issue SET status='in_progress',revision=revision+1 WHERE id=$1", issue)
	wakeDispatch(t, s, w)
	task := wakeupSessionTask(t, f, issue, agent)
	claimed := wakeupClaimEvents(t, f, s, task, 32)
	if len(claimed) != 1 {
		t.Fatalf("authorized creator could not claim: %+v", claimed)
	}
	wakeupAckEvents(t, s, task, claimed)
	wakeStart(t, f, util.UUIDToString(task.ID))
	if _, err := f.q.UpdateIssue(ctx, db.UpdateIssueParams{ID: issue, Status: pgtype.Text{String: "in_review", Valid: true}, SourceTaskID: task.ID}); err != nil {
		t.Fatal(err)
	}
	wakeDispatch(t, s, w)
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 {
		t.Fatalf("self status update produced another session input: %+v", events)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL", w.ID); n != 0 {
		t.Fatal("self status input remained eligible for another dispatch")
	}
	f.Exec(t, "UPDATE issue SET status='todo',revision=revision+1 WHERE id=$1", issue)
	wakeDispatch(t, s, w)
	if len(wakeupInbox(t, f, w.ID)) != 2 {
		t.Fatal("missing independent status input")
	}
	f.Exec(t, "DELETE FROM member WHERE user_id=$1 AND workspace_id=$2", f.UserID, f.WorkspaceID)
	if events := wakeupClaimEvents(t, f, s, task, 32); len(events) != 0 {
		t.Fatalf("revoked creator's event reached provider: %+v", events)
	}
	if events := wakeupInbox(t, f, w.ID); events[1].Status != "ignored" || events[1].FailureReason.String != "invocation_not_allowed" {
		t.Fatalf("revoked event not visibly refused: %+v", events)
	}
}

func TestIssueWakeupRetryKeepsPromptAndSchedulingScope(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	ctx := context.Background()
	parent := f.Comment(t, util.UUIDToString(issue), "original request")
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "at", AfterSeconds: 1, ParentCommentID: parent, Instruction: "specific wakeup instruction"})
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", w.ID)
	wakeDispatch(t, s, w)
	session := wakeupTargetSession(t, f, issue, agent)
	task := wakeupSessionTask(t, f, issue, agent)
	batch := wakeupClaimEvents(t, f, s, task, 1)
	if len(batch) != 1 {
		t.Fatalf("claim = %+v", batch)
	}
	if err := s.Tasks.AckIssueAgentEvent(ctx, task.ID, batch[0].ID, false, "provider_rejected"); err != nil {
		t.Fatal(err)
	}
	wakeupFinishExecution(t, f, s, task, "failed")
	if err := s.Tasks.RetryIssueAgentEvent(ctx, session.ID, batch[0].ID); err != nil {
		t.Fatal(err)
	}
	resumed := wakeupTargetSession(t, f, issue, agent)
	next := wakeupSessionTask(t, f, issue, agent)
	if resumed.ID != session.ID || resumed.ProviderSessionID.String != "provider-wakeup-session" || resumed.WorkDir.String != "/tmp/wakeup-session" || next.ID == task.ID {
		t.Fatalf("retry lost persistent session: %+v next=%+v", resumed, next)
	}
	again := wakeupClaimEvents(t, f, s, next, 1)
	if len(again) != 1 || again[0].ID != batch[0].ID || string(again[0].Payload) != string(batch[0].Payload) || again[0].AttemptCount != 2 {
		t.Fatalf("retry changed or duplicated the inbox input: %+v", again)
	}
	var payload wakeupSessionPayload
	if err := json.Unmarshal(again[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.ParentCommentID == nil || *payload.ParentCommentID != parent || payload.Instruction != "specific wakeup instruction" {
		t.Fatalf("retry lost explicit reply metadata: %+v", payload)
	}
	if len(wakeupInbox(t, f, w.ID)) != 1 {
		t.Fatal("retry created another rule input")
	}
}

func TestIssueWakeupConcurrentDispatchIsIdempotent(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "at", AfterSeconds: 1, Instruction: "once"})
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", w.ID)
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 6 {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errs <- s.dispatch(ctx, w)
		})
	}
	wg.Wait()
	close(errs)
	// dispatch fails fast (lock_timeout 50ms) instead of queueing behind the
	// rule's lock holder; Tick retries it next round. So a loser may see
	// lock_not_available when the winner's transaction runs long, as on a
	// slow CI runner. The contract is one winner and one run.
	winners := 0
	for err := range errs {
		switch {
		case err == nil:
			winners++
		case !isLockTimeout(err):
			t.Error(err)
		}
	}
	if winners == 0 {
		t.Fatal("no dispatch succeeded")
	}
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 {
		t.Fatalf("concurrent dispatch transferred %d inputs", len(events))
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue); n != 1 {
		t.Fatalf("concurrent dispatch created %d session executions", n)
	}
}
