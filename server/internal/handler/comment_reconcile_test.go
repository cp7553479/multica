package handler

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
)

// completeTaskViaHandler drives the daemon CompleteTask endpoint for taskID.
func completeTaskViaHandler(t *testing.T, taskID, output string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := newOwnerDaemonRequest("POST", "/api/daemon/tasks/"+taskID+"/complete",
		map[string]any{"output": output},
	)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("taskId", taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	testHandler.CompleteTask(w, req)
	return w
}

// pendingTaskCountForAgentIssue counts claimable (queued/dispatched) tasks for
// an (issue, agent) pair.
func pendingTaskCountForAgentIssue(t *testing.T, issueID, agentID string) int {
	t.Helper()
	var n int
	dbfx.QueryRow(t,
		`SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued', 'dispatched')`,
		issueID, agentID).Scan(&n)
	return n
}

// queuedTaskCountForAgentIssue counts only QUEUED (not dispatched) tasks for an
// (issue, agent) pair. Used to distinguish a freshly-enqueued follow-up from a
// pre-seeded dispatched task in the same assertion.
func queuedTaskCountForAgentIssue(t *testing.T, issueID, agentID string) int {
	t.Helper()
	var n int
	dbfx.QueryRow(t,
		`SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`,
		issueID, agentID).Scan(&n)
	return n
}

// handlerWorkspaceMember inserts a fresh user + workspace member and returns
// the user id (for a second distinct originator).
func handlerWorkspaceMember(t *testing.T, slug string) string {
	t.Helper()
	var userID string
	email := slug + "-" + time.Now().Format("150405.000000") + "@example.test"
	dbfx.QueryRow(t, `INSERT INTO "user" (name, email) VALUES ($1, $2) RETURNING id`,
		"Reconcile Test "+slug, email).Scan(&userID)
	dbfx.Exec(t, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'admin')`,
		testWorkspaceID, userID)
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM member WHERE user_id = $1`, userID)
		testPool.Exec(context.Background(), `DELETE FROM "user" WHERE id = $1`, userID)
	})
	return userID
}

// taskTriggerOriginatorCoalesced returns (trigger_comment_id, originator_user_id,
// coalesced_comment_ids) as text for the most recent task of (issue, agent).
func taskTriggerOriginatorCoalesced(t *testing.T, issueID, agentID string) (string, string, []string) {
	t.Helper()
	var trigger, originator string
	var coalesced []string
	dbfx.QueryRow(t, `
		SELECT COALESCE(trigger_comment_id::text, ''),
		       COALESCE(originator_user_id::text, ''),
		       coalesced_comment_ids::text[]
		  FROM agent_task_queue
		 WHERE issue_id = $1 AND agent_id = $2
		 ORDER BY created_at DESC
		 LIMIT 1
	`, issueID, agentID).Scan(&trigger, &originator, &coalesced)
	return trigger, originator, coalesced
}

func containsUUID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}
