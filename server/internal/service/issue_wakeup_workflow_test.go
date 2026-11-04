package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Project workflows × wakeups (MUL-7420).

// Workflow ownership follows the current issue assignment. Registering a rule
// while a prior step ran cannot revive that step after a member moved the issue.
func TestWakeupSessionUsesCurrentOwnerRoleWithoutRevivingSourceStep(t *testing.T) {
	for _, owner := range []bool{true, false} {
		t.Run(fmt.Sprintf("current owner %t", owner), func(t *testing.T) {
			f, s, issue, agent := wakeFixture(t)
			ctx := context.Background()
			source := f.Task(t, agent, testutil.Cols{
				"issue_id": issue, "status": "completed", "workflow_step": "in_progress",
				"runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agent + "')"),
			})
			fromRun, err := s.Create(ctx, issue, parseTestUUID(t, f.UserID), parseTestUUID(t, source),
				WakeupInput{AgentID: agent, Kind: "at", AfterSeconds: 1, Instruction: "check CI, then hand to review"})
			if err != nil {
				t.Fatal(err)
			}
			fromMember := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "at", AfterSeconds: 1, Instruction: "daily check"})
			if owner {
				f.Exec(t, "UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1", issue, agent)
				f.Exec(t, "UPDATE issue_agent_event SET status='ignored',failure_reason='fixture_setup' WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=$1) AND kind='assigned'", issue)
			}
			f.Exec(t, "UPDATE issue SET status='blocked' WHERE id=$1", issue)
			for _, w := range []db.IssueWakeup{fromRun, fromMember} {
				f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1", w.ID)
				wakeDispatch(t, s, w)
			}
			task := wakeupSessionTask(t, f, issue, agent)
			if owner && task.WorkflowStep.String != "blocked" || !owner && task.WorkflowStep.Valid {
				t.Fatalf("rule revived a source step or acquired auxiliary workflow rights: owner=%t step=%+v", owner, task.WorkflowStep)
			}
			if task.IsLeaderTask || task.SquadID.Valid {
				t.Fatalf("ordinary rule acquired a squad leader role: %+v", task)
			}
			inputs := wakeupClaimEvents(t, f, s, task, 32)
			if len(inputs) != 2 || inputs[0].SessionID != inputs[1].SessionID || inputs[0].DeliveryTaskID != task.ID || inputs[1].DeliveryTaskID != task.ID {
				t.Fatalf("two rules did not share the current pair's consumer: %+v", inputs)
			}
			if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running')", issue); n != 1 {
				t.Fatalf("rules created %d active consumers", n)
			}
		})
	}
}

// A retried workflow handoff keeps its brief, as a retried wakeup keeps its
// prompt; a row from before workflow steps drops its retired note.
func TestRetryKeepsWorkflowHandoffBrief(t *testing.T) {
	f, _, issue, agent := wakeFixture(t)
	ctx := context.Background()
	runtime := testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agent + "')")
	retry := func(cols testutil.Cols) db.AgentTaskQueue {
		t.Helper()
		cols["issue_id"], cols["status"], cols["runtime_id"] = issue, "failed", runtime
		parent := f.Task(t, agent, cols)
		child, err := f.q.CreateRetryTask(ctx, db.CreateRetryTaskParams{ID: parseTestUUID(t, parent)})
		if err != nil {
			t.Fatal(err)
		}
		f.Cleanup(t, "DELETE FROM agent_task_queue WHERE id=$1", child.ID)
		f.Exec(t, "UPDATE agent_task_queue SET status='cancelled' WHERE id=$1", child.ID)
		return child
	}
	if got := retry(testutil.Cols{"handoff_note": "This project's workflow — Delivery", "workflow_step": "in_progress"}); got.HandoffNote != (pgtype.Text{String: "This project's workflow — Delivery", Valid: true}) || got.WorkflowStep.String != "in_progress" {
		t.Fatalf("retry lost the handoff brief or step: %+v / %+v", got.HandoffNote, got.WorkflowStep)
	}
	if got := retry(testutil.Cols{"handoff_note": "retired assignment note"}); got.HandoffNote.Valid {
		t.Fatalf("retry revived a retired note: %q", got.HandoffNote.String)
	}
}
