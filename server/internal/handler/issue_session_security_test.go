package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestIssueSessionTerminalTaskCannotBorrowHumanInvocationRights(t *testing.T) {
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	dbfx.Exec(t, `UPDATE issue_agent_event SET status='delivered' WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=$1)`, issueID)
	authorID := dbfx.Agent(t, "finished author", runtimeID)
	sourceIssue := dbfx.Issue(t, "finished source issue")
	sourceTask := dbfx.Task(t, authorID, testutil.Cols{
		"issue_id": sourceIssue, "runtime_id": runtimeID, "status": "completed",
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	comment := captureSessionComment(t, issueID, map[string]any{"content": "please start new private work"}, authorID, sourceTask)
	var originator *string
	var status, reason, source string
	dbfx.QueryRow(t, `SELECT originator_user_id::text,status,failure_reason,source_task_id FROM issue_agent_event WHERE source_id=$1 AND kind='comment.created'`, comment.ID).
		Scan(&originator, &status, &reason, &source)
	if originator != nil || status != "ignored" || reason != "invocation_not_allowed" || source != sourceTask {
		t.Fatalf("terminal task borrowed authority: originator=%v status=%s reason=%s source=%s", originator, status, reason, source)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2`, issueID, ownerID); count != 0 {
		t.Fatalf("terminal token created %d private executions", count)
	}
}

func TestIssueSessionPluginWritesDoNotAcquireAgentInvocationScope(t *testing.T) {
	installationID := installPluginForAction(t, []string{"issues:read", "issues:write", "comments:write"})
	token, err := testHandler.PluginService.IssueInstallToken(context.Background(), parseUUID(installationID))
	if err != nil {
		t.Fatal(err)
	}
	for _, tokenCaller := range []bool{false, true} {
		name := "member surface"
		if tokenCaller {
			name = "event hook"
		}
		t.Run(name, func(t *testing.T) {
			issueID, _, runtimeID := issueSessionCaptureFixture(t)
			dbfx.Exec(t, `UPDATE issue_agent_event SET status='delivered' WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=$1)`, issueID)
			params := map[string]string{"issue_ref": issueID}
			commentReq := pluginActionRequest(http.MethodPost, "/comments", installationID, map[string]any{"content": "plugin output"}, params)
			patchReq := pluginActionRequest(http.MethodPatch, "/issue", installationID, map[string]any{"title": "plugin title"}, params)
			if tokenCaller {
				commentReq = pluginInstallTokenRequest(http.MethodPost, "/comments", token, map[string]any{"content": "plugin output"}, params)
				patchReq = pluginInstallTokenRequest(http.MethodPatch, "/issue", token, map[string]any{"title": "plugin title"}, params)
			}
			var created struct {
				ID string `json:"id"`
			}
			testutil.Call(t, testHandler.CreatePluginComment, commentReq).Want(http.StatusCreated).JSON(&created)
			testutil.Call(t, testHandler.PatchPluginIssue, patchReq).Want(http.StatusOK)
			if _, err := testHandler.TaskService.WakeIssueAgentSessionsForRuntime(context.Background(), parseUUID(runtimeID), 32); err != nil {
				t.Fatal(err)
			}
			if count := dbfx.Count(t, `SELECT count(*) FROM issue_agent_event WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=$1) AND status='ignored' AND failure_reason='plugin_write_requires_invoke'`, issueID); count != 2 {
				t.Fatalf("plugin writes did not retain non-invoking audit events: %d", count)
			}
			if count := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); count != 0 {
				t.Fatalf("plugin write scopes created %d paid executions", count)
			}
			// The original plugin marker is audit provenance, not a permanent
			// ban on a later authorized human editing that comment normally.
			edit := testutil.WithURLParams(newRequest(http.MethodPut, "/comments/"+created.ID, map[string]any{"content": "human follow-up edit"}), "commentId", created.ID)
			testutil.Call(t, testHandler.UpdateComment, edit).Want(http.StatusOK)
			var status, actor, originator string
			dbfx.QueryRow(t, `SELECT status,actor_type,originator_user_id FROM issue_agent_event WHERE source_id=$1 AND kind='comment.updated'`, created.ID).Scan(&status, &actor, &originator)
			if status != "pending" || actor != "member" || originator != testUserID {
				t.Fatalf("human edit inherited obsolete plugin execution scope: status=%s actor=%s originator=%s", status, actor, originator)
			}
		})
	}
}
