package service

import (
	"testing"

	"github.com/multica-ai/multica/server/internal/util"
)

func TestAutopilotIssueFirstAssignmentToAnotherAgentUsesCurrentMemberRights(t *testing.T) {
	for _, allowed := range []bool{true, false} {
		name := "member without invocation rights"
		if allowed {
			name = "owner explicitly assigns"
		}
		t.Run(name, func(t *testing.T) {
			f := newAutopilotIssueSessionFixture(t)
			run := f.dispatchIssue(t)
			target := f.Agent(t, "human reassignment", f.runtimeID)
			actor := f.triggerOwner
			if allowed {
				actor = f.UserID
			}
			tx, err := f.Pool.Begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(t.Context())
			if _, err = tx.Exec(t.Context(), "SELECT set_config('multica.actor_type','member',true),set_config('multica.actor_id',$1,true)", actor); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(t.Context(), "UPDATE issue SET assignee_id=$2 WHERE id=$1", run.IssueID, target); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(t.Context()); err != nil {
				t.Fatal(err)
			}
			session := wakeupTargetSession(t, f.principalFixture, run.IssueID, target)
			task, err := f.svc.TaskSvc.WakeIssueAgentSession(t.Context(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !allowed {
				if task != nil {
					t.Fatal("human assignment bypassed private-agent invocation policy")
				}
				if n := f.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND status='ignored' AND failure_reason='invocation_not_allowed'", session.ID); n != 1 {
					t.Fatal("missing invocation refusal receipt")
				}
				return
			}
			if task == nil || IssueAgentSessionID(*task) != session.ID || task.OriginatorUserID != util.MustParseUUID(actor) {
				t.Fatalf("authorized human reassignment was treated as an unbound Autopilot dispatch: task=%+v", task)
			}
			claimed, err := f.svc.TaskSvc.ClaimTask(t.Context(), util.MustParseUUID(target))
			if err != nil || claimed == nil {
				t.Fatalf("claim reassigned session: task=%+v err=%v", claimed, err)
			}
			events, err := f.svc.TaskSvc.ClaimIssueAgentEvents(t.Context(), claimed.ID, 32)
			if err != nil || len(events) != 1 || events[0].ActorType != "member" || events[0].ActorID != util.MustParseUUID(actor) {
				t.Fatalf("human reassignment input: events=%+v err=%v", events, err)
			}
			f.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2", run.IssueID, target)
		})
	}
}
