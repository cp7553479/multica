package handler

import (
	"context"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestCommentSessionInputCannotRewriteAnExistingRunAttribution(t *testing.T) {
	issueID, agentID, runtimeID := issueSessionCaptureFixture(t)
	dbfx.Exec(t, "UPDATE agent SET permission_mode='private', owner_id=$2 WHERE id=$1", agentID, testUserID)
	captureSessionComment(t, issueID, map[string]any{"content": "A member instruction"}, "", "")
	var taskID, originator, source string
	dbfx.QueryRow(t, `SELECT id, originator_user_id, originator_source FROM agent_task_queue WHERE issue_id=$1`, issueID).Scan(&taskID, &originator, &source)
	if originator != testUserID {
		t.Fatalf("initial run originator = %q, want %q", originator, testUserID)
	}
	dbfx.Exec(t, "UPDATE workspace SET attribution_fail_closed=true WHERE id=$1", testWorkspaceID)
	t.Cleanup(func() {
		dbfx.Exec(t, "UPDATE workspace SET attribution_fail_closed=false WHERE id=$1", testWorkspaceID)
	})
	otherAgentID := dbfx.Agent(t, "Unattributed comment author", runtimeID)
	dbfx.Comment(t, issueID, "An unattributed agent instruction", testutil.Cols{
		"author_type": "agent", "author_id": otherAgentID,
	})
	issue, err := testHandler.Queries.GetIssue(context.Background(), parseUUID(issueID))
	if err != nil {
		t.Fatal(err)
	}
	outcomes := testHandler.commentSessionOutcomes(context.Background(), issue, "agent", otherAgentID, "")
	if len(outcomes) != 1 || outcomes[0].Status != DispatchBlocked || outcomes[0].ReasonCode != ReasonInvocationNotAllowed {
		t.Fatalf("unattributed input outcome = %+v, want invocation denial", outcomes)
	}
	task, err := testHandler.Queries.GetAgentTask(context.Background(), parseUUID(taskID))
	if err != nil {
		t.Fatal(err)
	}
	if uuidToString(task.OriginatorUserID) != originator || task.OriginatorSource.String != source || task.TriggerCommentID.Valid || len(task.CoalescedCommentIds) != 0 {
		t.Fatalf("new event changed the existing run's attribution or inputs: %+v", task)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueID); n != 1 {
		t.Fatalf("comment created a separate execution: %d", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM issue_agent_event WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=$1) AND actor_id=$2 AND originator_user_id IS NULL`, issueID, otherAgentID); n != 1 {
		t.Fatalf("unattributed event lost its own actor provenance: %d", n)
	}
}
