package service

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type issueSessionAckTxProbe struct {
	TxStarter
	started chan int32
}

func (s issueSessionAckTxProbe) Begin(ctx context.Context) (pgx.Tx, error) {
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

func TestIssueAgentSessionWorkflowAckWaitsForTaskWithoutHoldingSession(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	issue, session := newAssignedSessionIssue(t, f, "workflow ack completion boundary")
	task := claimSessionExecution(t, f, session, true)
	write, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback(context.Background())
	if _, err := write.Exec(ctx, `SELECT set_config('multica.actor_type','member',true),
		set_config('multica.actor_id',$1,true),set_config('multica.workflow_handoff','true',true)`, f.fx.UserID); err != nil {
		t.Fatal(err)
	}
	if _, err := write.Exec(ctx, `UPDATE issue SET status='in_review',revision=revision+1 WHERE id=$1`, issue.ID); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	event, err := f.svc.ClaimIssueAgentEvent(ctx, task.ID)
	if err != nil || event == nil {
		t.Fatalf("workflow event=%+v error=%v", event, err)
	}

	terminal, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer terminal.Rollback(context.Background())
	var locked pgtype.UUID
	if err := terminal.QueryRow(ctx, "SELECT id FROM agent_task_queue WHERE id=$1 FOR UPDATE", task.ID).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	started := make(chan int32, 1)
	ackService := NewTaskService(f.svc.Queries, issueSessionAckTxProbe{TxStarter: f.pool, started: started}, f.svc.Hub, f.svc.Bus)
	acked := make(chan error, 1)
	go func() { acked <- ackService.AckIssueAgentEvent(ctx, task.ID, event.ID, true, "") }()
	pid := <-started
	for {
		var waiting bool
		if err := f.pool.QueryRow(ctx, "SELECT COALESCE(wait_event_type='Lock',false) FROM pg_stat_activity WHERE pid=$1", pid).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-acked:
			t.Fatalf("ack did not wait for terminal task lock: %v", err)
		case <-ctx.Done():
			t.Fatal("ack did not reach its task lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
	probe, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Rollback(context.Background())
	if err := probe.QueryRow(ctx, "SELECT id FROM issue_agent_session WHERE id=$1 FOR UPDATE NOWAIT", session.ID).Scan(&locked); err != nil {
		t.Fatalf("workflow ack held the session while waiting for task: %v", err)
	}
	if err := probe.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	q := f.svc.Queries.WithTx(terminal)
	completed, err := q.CompleteAgentTask(ctx, db.CompleteAgentTaskParams{
		ID: task.ID, Result: []byte(`{"output":""}`),
		SessionID: pgtype.Text{String: "recorded-thread", Valid: true},
		WorkDir:   pgtype.Text{String: "/tmp/recorded-issue", Valid: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := SettleTerminalTaskState(ctx, q, completed); err != nil {
		t.Fatal(err)
	}
	if err := terminal.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-acked; err != nil {
		t.Fatalf("ack after terminal settlement: %v", err)
	}
	if got := f.fx.Count(t, "SELECT count(*) FROM issue_agent_event WHERE id=$1 AND status='delivered' AND delivery_task_id=$2", util.UUIDToString(event.ID), task.ID); got != 1 {
		t.Fatal("completion/ack boundary lost the delivery receipt")
	}
}
