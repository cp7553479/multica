package service

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type wakeupSessionBeginProbe struct {
	TxStarter
	started chan int32
}

func (s wakeupSessionBeginProbe) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return nil, err
	}
	var pid int32
	if err := tx.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&pid); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	s.started <- pid
	return tx, nil
}

func TestWakeupDisableTaskLockWaitDoesNotHoldCompletionReceipt(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	w := wakeCreate(t, f, s, issue, WakeupInput{
		AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created", "task.completed"}, Instruction: "Process new input",
	})
	f.Comment(t, util.UUIDToString(issue), "first input")
	wakeDispatch(t, s, w)
	task := wakeupSessionTask(t, f, issue, agent)
	initial := wakeupClaimEvents(t, f, s, task, 32)
	if len(initial) != 1 {
		t.Fatalf("initial rule delivery=%+v", initial)
	}
	f.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", task.ID)
	wakeupAckEvents(t, s, task, initial)
	if err := s.Tasks.PinIssueAgentSession(ctx, task.ID, "running-rule-thread", "/tmp/running-rule-directory"); err != nil {
		t.Fatal(err)
	}
	// A later event waits in the same busy session, so withdrawing this rule
	// needs the active task lock even though the task itself must keep running.
	f.Comment(t, util.UUIDToString(issue), "second durable input")
	wakeDispatch(t, s, w)
	if inputs := wakeupInbox(t, f, w.ID); len(inputs) != 2 || inputs[1].Status != "pending" {
		t.Fatalf("busy session lacks pending rule input: %+v", inputs)
	}
	// Completing this unrelated source establishes the coalesced receipt that
	// the active consumer's completion will update in its task-status trigger.
	source := f.Task(t, agent, testutil.Cols{"issue_id": issue, "runtime_id": task.RuntimeID, "status": "running"})
	f.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", source)
	var receiptID pgtype.UUID
	if err := f.Pool.QueryRow(ctx, "SELECT id FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND event_type='task.completed' AND processed_at IS NULL", w.ID).Scan(&receiptID); err != nil {
		t.Fatal(err)
	}

	completion, err := f.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer completion.Rollback(context.Background())
	var locked pgtype.UUID
	if err := completion.QueryRow(ctx, "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE", task.ID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	originalStarter := s.Tasks.TxStarter
	defer func() { s.Tasks.TxStarter = originalStarter }()
	started := make(chan int32, 1)
	s.Tasks.TxStarter = wakeupSessionBeginProbe{TxStarter: originalStarter, started: started}
	disabled := make(chan error, 1)
	member := parseTestUUID(t, f.UserID)
	go func() {
		_, err := s.Disable(ctx, issue, w.ID, member)
		disabled <- err
	}()
	pid := <-started
	for {
		var waiting bool
		if err := f.Pool.QueryRow(ctx, "SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-disabled:
			t.Fatalf("disable did not wait for active task lock: %v", err)
		case <-ctx.Done():
			t.Fatal("disable never reached the task lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := completion.QueryRow(ctx, "SELECT id FROM issue_wakeup_receipt WHERE id=$1 FOR UPDATE NOWAIT", receiptID).Scan(&locked); err != nil {
		t.Fatalf("disable held receipt while waiting for task, reversing completion lock order: %v", err)
	}
	q := f.q.WithTx(completion)
	completed, err := q.CompleteAgentTask(ctx, db.CompleteAgentTaskParams{
		ID: task.ID, Result: []byte(`{"output":""}`),
		SessionID: pgtype.Text{String: "running-rule-thread", Valid: true}, WorkDir: pgtype.Text{String: "/tmp/running-rule-directory", Valid: true},
	})
	if err != nil {
		t.Fatalf("completion could not capture its receipt while disable waited: %v", err)
	}
	if err := SettleTerminalTaskState(ctx, q, completed); err != nil {
		t.Fatal(err)
	}
	if err := completion.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-disabled; err != nil {
		t.Fatalf("disable after completion: %v", err)
	}
	if got := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL", w.ID); got != 0 {
		t.Fatalf("disabled rule retained %d pending receipts", got)
	}
	if got := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE id=$1 AND status='completed'", task.ID); got != 1 {
		t.Fatal("disabling the rule changed the completed consumer's outcome")
	}
}
