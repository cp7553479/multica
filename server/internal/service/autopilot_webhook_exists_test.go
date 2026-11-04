package service

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type webhookTaskReadTrace struct {
	reads   int
	maxRows int64
}
type webhookTaskReadTraceKey struct{}

func (tr *webhookTaskReadTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	return context.WithValue(ctx, webhookTaskReadTraceKey{}, strings.Contains(data.SQL, "FROM agent_task_queue"))
}
func (tr *webhookTaskReadTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	if tracked, _ := ctx.Value(webhookTaskReadTraceKey{}).(bool); tracked {
		tr.reads++
		tr.maxRows = max(tr.maxRows, data.CommandTag.RowsAffected())
	}
}

func TestWebhookRecoverySettledRunsDoNotReadTaskBodies(t *testing.T) {
	ctx := context.Background()
	tr := &webhookTaskReadTrace{}
	config := sharedTestPool(t).Config()
	config.ConnConfig.Tracer = tr
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	fx := testutil.New(pool, "", "")
	suffix := time.Now().UnixNano()
	fx.UserID = fx.User(t, "webhook reader", fmt.Sprintf("webhook-reader-%d@example.test", suffix))
	fx.WorkspaceID = fx.Workspace(t, "webhook reader", fmt.Sprintf("webhook-reader-%d", suffix))
	fx.Member(t, fx.WorkspaceID, fx.UserID, "owner")
	runtimeID := fx.Runtime(t, "webhook reader")
	agentID := fx.Agent(t, "webhook reader", runtimeID)
	q := db.New(pool)
	svc := &AutopilotService{Queries: q}
	for _, status := range []string{"completed", "failed", "cancelled", "queued", "deferred"} {
		t.Run(status, func(t *testing.T) {
			issueID := fx.Issue(t, "already handed off")
			// A nil TaskSvc deliberately makes an accidental duplicate enqueue fail.
			count := 64
			if status == "queued" || status == "deferred" {
				count = 1
			}
			var taskID string
			for i := 0; i < count; i++ {
				taskStatus := status
				taskID = fx.Task(t, agentID, testutil.Cols{"issue_id": issueID, "runtime_id": runtimeID, "status": taskStatus, "context": testutil.Raw(`jsonb_build_object('large',repeat('x',65536))`), "result": testutil.Raw(`jsonb_build_object('large',repeat('y',65536))`)})
			}
			tr.reads = 0
			tr.maxRows = 0
			if err := svc.ensureWebhookCreateIssueTask(ctx, db.Autopilot{}, db.AutopilotRun{IssueID: util.MustParseUUID(issueID), Status: "issue_created", TaskID: util.MustParseUUID(taskID)}); err != nil {
				t.Fatal(err)
			}
			if tr.reads != 0 || tr.maxRows != 0 {
				t.Fatalf("task inspection transferred %d rows across %d reads", tr.maxRows, tr.reads)
			}
		})
	}
	// An unlinked active run still checks issue lifecycle; another issue
	// cannot supply its execution, and a deleted issue surfaces its load error.
	missing := util.MustParseUUID("ffffffff-ffff-ffff-ffff-fffffffffff1")
	if err := svc.ensureWebhookCreateIssueTask(ctx, db.Autopilot{}, db.AutopilotRun{IssueID: missing, Status: "issue_created"}); err == nil || !strings.Contains(err.Error(), "load linked issue") {
		t.Fatalf("missing issue: %v", err)
	}
	parked := fx.Issue(t, "parked issue", testutil.Cols{"status": "backlog"})
	if err := svc.ensureWebhookCreateIssueTask(ctx, db.Autopilot{}, db.AutopilotRun{IssueID: util.MustParseUUID(parked), Status: "issue_created"}); err != nil {
		t.Fatal(err)
	}
	runnable := fx.Issue(t, "repair missing task", testutil.Cols{"status": "todo", "assignee_type": "agent", "assignee_id": agentID})
	svc.TaskSvc = NewTaskService(q, pool, nil, events.New())
	fx.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", runnable)
	for range 2 {
		if err := svc.ensureWebhookCreateIssueTask(ctx, db.Autopilot{}, db.AutopilotRun{IssueID: util.MustParseUUID(runnable), Status: "issue_created"}); err != nil {
			t.Fatal(err)
		}
	}
	var taskCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", runnable).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 0 {
		t.Fatalf("non-autopilot assignment repair created %d legacy tasks, want zero", taskCount)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := svc.ensureWebhookCreateIssueTask(cancelled, db.Autopilot{}, db.AutopilotRun{IssueID: util.MustParseUUID(parked), Status: "issue_created"}); err == nil || !strings.Contains(err.Error(), "load linked issue") {
		t.Fatalf("query failure: %v", err)
	}
}
