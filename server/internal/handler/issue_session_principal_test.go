package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestIssueSessionInitialClaimReauthorizesWithoutFailingNextMember(t *testing.T) {
	agentID, runtimeID, _ := createRuntimeGuardAgent(t, context.Background())
	dbfx.Exec(t, `UPDATE agent SET permission_mode='public_to',visibility='workspace' WHERE id=$1`, agentID)
	dbfx.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agentID, "target_type": "workspace", "target_id": testWorkspaceID})
	departed := dbfx.User(t, "Departing session author", "departing-session-author@multica.test")
	dbfx.Member(t, testWorkspaceID, departed, "member")
	issueID := dbfx.Issue(t, "Reauthorize initial session input", testutil.Cols{
		"creator_id": departed, "assignee_type": "agent", "assignee_id": agentID,
	})
	commentID := dbfx.Comment(t, issueID, "The remaining member's instruction")
	var sessionID string
	dbfx.QueryRow(t, `SELECT id FROM issue_agent_session WHERE issue_id=$1 AND agent_id=$2`, issueID, agentID).Scan(&sessionID)
	dbfx.Exec(t, `UPDATE issue_agent_session SET provider_session_id='original-thread',work_dir='/tmp/original-session-dir' WHERE id=$1`, sessionID)
	old, err := testHandler.TaskService.WakeIssueAgentSession(t.Context(), parseUUID(sessionID))
	if err != nil || old == nil || uuidToString(old.OriginatorUserID) != departed {
		t.Fatalf("first author's execution: task=%+v error=%v", old, err)
	}
	dbfx.Cleanup(t, `DELETE FROM agent_task_queue WHERE issue_id=$1`, issueID)
	dbfx.Insert(t, "task_token", testutil.Cols{
		"token_hash": "superseded-session-token", "task_id": old.ID, "agent_id": agentID,
		"workspace_id": testWorkspaceID, "user_id": testUserID, "expires_at": testutil.Raw("now()+interval '1 hour'"),
	})
	dbfx.Exec(t, `DELETE FROM member WHERE workspace_id=$1 AND user_id=$2`, testWorkspaceID, departed)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='dispatched',dispatched_at=now() WHERE id=$1`, old.ID)
	old.Status = "dispatched"
	req := newRequest(http.MethodPost, "/claim", nil)
	response := AgentTaskResponse{IssueAgentSessionID: sessionID}
	failure := testHandler.attachInitialIssueAgentEvents(req, old, &response)
	if failure == nil || failure.outcome != "issue_session_execution_superseded" || failure.status != http.StatusConflict || !failure.settled {
		t.Fatalf("superseded claim must bypass the failure settlement path: %+v", failure)
	}
	var oldStatus, state, activeID, provider, workDir string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1`, old.ID).Scan(&oldStatus)
	dbfx.QueryRow(t, `SELECT state,active_task_id,provider_session_id,work_dir FROM issue_agent_session WHERE id=$1`, sessionID).Scan(&state, &activeID, &provider, &workDir)
	if oldStatus != "cancelled" || state != "ready" || activeID == uuidToString(old.ID) || provider != "original-thread" || workDir != "/tmp/original-session-dir" {
		t.Fatalf("replacement changed session continuity: old=%s state=%s active=%s provider=%s dir=%s", oldStatus, state, activeID, provider, workDir)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM task_token WHERE task_id=$1`, old.ID); n != 0 {
		t.Fatalf("superseded execution retained %d task tokens", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_id=$2 AND status='pending' AND failure_reason IS NULL`, sessionID, commentID); n != 1 {
		t.Fatalf("another member's input was failed or lost: %d pending", n)
	}
	next, err := testHandler.Queries.GetAgentTask(t.Context(), parseUUID(activeID))
	if err != nil || next.RuntimeID != parseUUID(runtimeID) || uuidToString(next.OriginatorUserID) != testUserID {
		t.Fatalf("replacement did not reauthorize the remaining member: %+v error=%v", next, err)
	}
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='dispatched',dispatched_at=now() WHERE id=$1`, next.ID)
	next.Status = "dispatched"
	response = AgentTaskResponse{IssueAgentSessionID: sessionID}
	if failure := testHandler.attachInitialIssueAgentEvents(req, &next, &response); failure != nil {
		t.Fatalf("replacement claim failed: %+v", failure)
	}
	if len(response.IssueSessionEvents) != 1 || response.IssueSessionEvents[0].SourceID == nil || *response.IssueSessionEvents[0].SourceID != commentID {
		t.Fatalf("replacement claimed another author's input: %+v", response.IssueSessionEvents)
	}
}
