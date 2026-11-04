package handler

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/testutil"
)

func createRuntimeAccessDeniedAgent(t *testing.T, ctx context.Context, runtimeID, name string) string {
	t.Helper()
	foreignOwnerID := dbfx.User(t, name+" owner", "runtime-access-owner-"+uuid.NewString()+"@example.com")
	dbfx.Member(t, testWorkspaceID, foreignOwnerID, "member")
	agentID := createCascadeFixtureAgent(t, ctx, runtimeID, name)
	dbfx.Exec(t, `UPDATE agent SET owner_id = $1, visibility = 'workspace', permission_mode = 'public_to' WHERE id = $2`, foreignOwnerID, agentID)
	dbfx.Exec(t, `
		INSERT INTO agent_invocation_target (agent_id, target_type, target_id)
		VALUES ($1, 'workspace', $2)
		ON CONFLICT (agent_id, target_type, target_id) DO NOTHING
	`, agentID, testWorkspaceID)
	return agentID
}

func TestCreateIssue_RuntimeAccessDeniedLeavesNoticeWithoutTask(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createCascadeFixtureRuntime(t, ctx, "Assignment access denied runtime")
	agentID := createRuntimeAccessDeniedAgent(t, ctx, runtimeID, "Assignment access denied agent")

	w := testutil.Call(t, testHandler.CreateIssue, newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title":         "assignment access denied issue",
		"status":        "todo",
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})).Want(http.StatusCreated)
	var issue IssueResponse
	w.JSON(&issue)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM issue WHERE id = $1`, issue.ID) })

	var taskCount int
	dbfx.QueryRow(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1`, issue.ID).Scan(&taskCount)
	if taskCount != 0 {
		t.Fatalf("assignment created %d task rows, want 0", taskCount)
	}
	var notice string
	dbfx.QueryRow(t, `
		SELECT content FROM comment
		WHERE issue_id = $1 AND author_type = 'system' AND type = 'system'
		ORDER BY created_at DESC LIMIT 1
	`, issue.ID).Scan(&notice)
	if !strings.Contains(notice, "public") || !strings.Contains(notice, "rebind/copy") {
		t.Fatalf("assignment notice = %q, want both recovery paths", notice)
	}
}

func TestSendChatMessage_RuntimeAccessDeniedReturnsStructuredConflict(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createCascadeFixtureRuntime(t, ctx, "Chat access denied runtime")
	agentID := createRuntimeAccessDeniedAgent(t, ctx, runtimeID, "Chat access denied agent")
	sessionID := createHandlerTestChatSession(t, agentID)

	req := newRequest(http.MethodPost, "/api/chat/sessions/"+sessionID+"/messages", map[string]any{"content": "please help"})
	req = withURLParam(req, "sessionId", sessionID)
	req = withChatTestWorkspaceCtx(t, req)
	w := testutil.Call(t, testHandler.SendChatMessage, req).Want(http.StatusConflict)
	var body struct {
		ReasonCode string `json:"reason_code"`
	}
	w.JSON(&body)
	if body.ReasonCode != string(ReasonRuntimeAccessDenied) {
		t.Fatalf("reason_code = %q, want %s", body.ReasonCode, ReasonRuntimeAccessDenied)
	}
	var taskCount int
	dbfx.QueryRow(t, `SELECT count(*) FROM agent_task_queue WHERE chat_session_id = $1`, sessionID).Scan(&taskCount)
	if taskCount != 0 {
		t.Fatalf("blocked chat created %d task rows, want 0", taskCount)
	}
}

func TestCommentMention_RuntimeAccessDeniedReportsTargetAndNotice(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createCascadeFixtureRuntime(t, ctx, "Mention access denied runtime")
	agentID := createRuntimeAccessDeniedAgent(t, ctx, runtimeID, "Mention access denied agent")
	issueID := dbfx.Issue(t, "Owner session runtime access denied", testutil.Cols{
		"assignee_type": "agent", "assignee_id": agentID,
	})
	dbfx.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issueID)

	// Comments belong to the assigned owner; runtime-owner consent is checked
	// before any execution payload or task token reaches the daemon.
	comment := captureSessionComment(t, issueID, map[string]any{"content": "Please help with this issue"}, "", "")
	if len(comment.TriggerOutcomes) != 1 || comment.TriggerOutcomes[0].TargetID != agentID {
		t.Fatalf("comment did not route to the assigned owner: %+v", comment.TriggerOutcomes)
	}
	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 1 || sessions[0].AgentID != agentID || sessions[0].ActiveTaskID == nil {
		t.Fatalf("owner session did not retain the input: %+v", sessions)
	}
	taskID := *sessions[0].ActiveTaskID
	req := withURLParam(newOwnerDaemonRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil), "runtimeId", runtimeID)
	w := testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK)
	if strings.TrimSpace(w.Body.String()) != `{"task":null}` {
		t.Fatalf("private runtime received an unauthorized execution: %s", w.Body.String())
	}
	var status, failureReason string
	dbfx.QueryRow(t, "SELECT status,failure_reason FROM agent_task_queue WHERE id=$1", taskID).Scan(&status, &failureReason)
	if status != "failed" || failureReason != string(ReasonRuntimeAccessDenied) {
		t.Fatalf("execution state = %s/%s, want failed/runtime_access_denied", status, failureReason)
	}
	after := captureIssueSessions(t, issueID)
	if len(after) != 1 || after[0].ID != sessions[0].ID || after[0].State != "blocked" || after[0].FailureReason == nil || *after[0].FailureReason != string(ReasonRuntimeAccessDenied) || after[0].ActiveTaskID != nil {
		t.Fatalf("runtime refusal was not visible on the session: %+v", after)
	}
	if len(after[0].Events) != 2 || after[0].Events[1].Kind != "comment.created" || after[0].Events[1].SourceID == nil || *after[0].Events[1].SourceID != comment.ID {
		t.Fatalf("runtime refusal lost the owner's comment input: %+v", after[0].Events)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM task_token WHERE task_id=$1", taskID); n != 0 {
		t.Fatalf("unauthorized execution received %d task tokens", n)
	}
}
