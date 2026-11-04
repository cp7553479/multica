package handler

import (
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestQuickActionSessionPreservesCrossIssueDelegation(t *testing.T) {
	for _, permission := range []string{"private", "member_allowlist"} {
		t.Run(permission, func(t *testing.T) {
			issueID, targetID, runtimeID := issueSessionCaptureFixture(t)
			if permission == "member_allowlist" {
				otherOwner := dbfx.User(t, "quick action owner", "quick-action-owner-"+targetID+"@example.com")
				dbfx.Exec(t, `UPDATE agent SET owner_id=$2,permission_mode='public_to' WHERE id=$1`, targetID, otherOwner)
				dbfx.InsertNoID(t, "agent_invocation_target", testutil.Cols{"agent_id": targetID, "target_type": "member", "target_id": testUserID}, "agent_id=$1", targetID)
			}
			dbfx.Exec(t, `UPDATE issue_agent_event SET status='delivered' WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=$1)`, issueID)
			sourceAgent := dbfx.Agent(t, "quick action source", runtimeID)
			sourceIssue := dbfx.Issue(t, "different source issue")
			sourceTask := dbfx.Task(t, sourceAgent, testutil.Cols{"issue_id": sourceIssue, "runtime_id": runtimeID, "status": "running", "originator_user_id": testUserID, "accountable_user_id": testUserID})
			var action QuickActionResponse
			testutil.Call(t, testHandler.CreateQuickAction, newRequest(http.MethodPost, "/quick-actions", map[string]any{
				"name": "delegated action", "prompt": "Apply the delegated request.", "visibility": "private", "assignee_type": "agent", "assignee_id": targetID,
			})).Want(http.StatusCreated).JSON(&action)
			dbfx.Cleanup(t, `DELETE FROM quick_action WHERE id=$1`, action.ID)
			req := testutil.WithURLParams(newRequest(http.MethodPost, "/quick-actions/run", nil), "id", issueID, "quickActionId", action.ID)
			req.Header.Set("X-Agent-ID", sourceAgent)
			req.Header.Set("X-Task-ID", sourceTask)
			req.Header.Set("X-Actor-Source", "task_token")
			var comment CommentResponse
			testutil.Call(t, testHandler.RunQuickAction, req).Want(http.StatusCreated).JSON(&comment)
			var commentSource, eventSource, originator, executionOriginator, state string
			dbfx.QueryRow(t, `SELECT c.source_task_id,e.source_task_id,e.originator_user_id,t.originator_user_id,e.status
                FROM comment c JOIN issue_agent_event e ON e.source_id=c.id AND e.kind='comment.created'
                JOIN issue_agent_session s ON s.id=e.session_id JOIN agent_task_queue t ON t.id=s.active_task_id
                WHERE c.id=$1`, comment.ID).Scan(&commentSource, &eventSource, &originator, &executionOriginator, &state)
			if commentSource != sourceTask || eventSource != sourceTask || originator != testUserID || executionOriginator != testUserID || state != "pending" {
				t.Fatalf("quick action lost delegated authority: comment=%s event=%s human=%s execution=%s state=%s", commentSource, eventSource, originator, executionOriginator, state)
			}
		})
	}
}
