package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/internal/handler"
	"github.com/multica-ai/multica/server/internal/testutil"
)

// authRequestWithAgent makes a request the server resolves as coming from an
// agent, by authenticating with a real `mat_` task token bound to (agent, task,
// workspace, user) — the same credential the daemon injects into an agent
// process.
//
// It used to send a member token plus X-Agent-ID / X-Task-ID headers. That
// stopped working, on purpose: both middlewares now strip client-supplied agent
// identity, because both ids are observable by any workspace member and the
// pair therefore proved nothing (MUL-3428). A test that poses as an agent must
// hold the credential a real agent holds.
func authRequestWithAgent(t *testing.T, method, path string, body any, agentID string) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, testServer.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+mintAgentTaskToken(t, agentID, ensureAgentTask(t, agentID), testUserID))

	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return r
}

// mintAgentTaskToken issues a task-scoped mat_ token for (agentID, taskID) in
// the test workspace and returns its raw value. The auth middleware re-stamps
// X-User-ID / X-Agent-ID / X-Task-ID / X-Workspace-ID from the stored row, so
// callers pass no identity headers of their own.
// boundUserID is the token's owning human — the runtime owner in production.
// It is NOT the human an agent's request acts for; that is the task's
// originator (MUL-6951), which is why the two are separable in tests.
func mintAgentTaskToken(t *testing.T, agentID, taskID, boundUserID string) string {
	t.Helper()
	raw := fmt.Sprintf("mat_test_%s_%d", strings.ReplaceAll(agentID, "-", ""), time.Now().UnixNano())
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO task_token (token_hash, task_id, agent_id, workspace_id, user_id, expires_at)
		VALUES ($1, $2, $3, $4, $5, now() + interval '1 hour')
	`, auth.HashToken(raw), taskID, agentID, testWorkspaceID, boundUserID); err != nil {
		t.Fatalf("mint task token: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM task_token WHERE token_hash = $1`, auth.HashToken(raw))
	})
	return raw
}

// ensureAgentTask returns a task UUID belonging to the given agent, inserting a
// queued one if none exists. authRequestWithAgent binds its mat_ token to that
// task, so callers keep treating "name the agent" as the single knob for
// posing as one.
func ensureAgentTask(t *testing.T, agentID string) string {
	t.Helper()
	ctx := context.Background()
	var taskID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent_task_queue WHERE agent_id = $1 LIMIT 1`,
		agentID,
	).Scan(&taskID); err == nil && taskID != "" {
		return taskID
	}
	var runtimeID string
	if err := testPool.QueryRow(ctx,
		`SELECT runtime_id::text FROM agent WHERE id = $1`,
		agentID,
	).Scan(&runtimeID); err != nil {
		t.Fatalf("ensureAgentTask: load runtime_id for agent %s: %v", agentID, err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, status, priority)
		VALUES ($1, $2, 'queued', 0)
		RETURNING id::text
	`, agentID, runtimeID).Scan(&taskID); err != nil {
		t.Fatalf("ensureAgentTask: insert task for agent %s: %v", agentID, err)
	}
	return taskID
}

// countPendingTasks returns the number of queued/dispatched tasks for an issue.
func countPendingTasks(t *testing.T, issueID string) int {
	t.Helper()
	var count int
	err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND status IN ('queued', 'dispatched')`,
		issueID).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count pending tasks: %v", err)
	}
	return count
}

func countPendingTasksForAgent(t *testing.T, issueID, agentID string) int {
	t.Helper()
	var count int
	err := testPool.QueryRow(context.Background(),
		`SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued', 'dispatched')`,
		issueID, agentID).Scan(&count)
	if err != nil {
		t.Fatalf("failed to count pending tasks for agent: %v", err)
	}
	return count
}

// clearTasks deletes all tasks for an issue (cleanup between subtests).
func clearTasks(t *testing.T, issueID string) {
	t.Helper()
	_, err := testPool.Exec(context.Background(),
		`DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	if err != nil {
		t.Fatalf("failed to clear tasks: %v", err)
	}
}

// latestTriggerCommentID returns the trigger_comment_id of the most recently
// created queued/dispatched task for the given issue, or empty string if none.
func latestTriggerCommentID(t *testing.T, issueID string) string {
	t.Helper()
	var triggerID *string
	err := testPool.QueryRow(context.Background(),
		`SELECT trigger_comment_id::text
		   FROM agent_task_queue
		  WHERE issue_id = $1 AND status IN ('queued', 'dispatched')
		  ORDER BY created_at DESC
		  LIMIT 1`,
		issueID).Scan(&triggerID)
	if err != nil {
		t.Fatalf("failed to fetch trigger_comment_id: %v", err)
	}
	if triggerID == nil {
		return ""
	}
	return *triggerID
}

// getAgentID returns the ID of the first agent in the test workspace.
func getAgentID(t *testing.T) string {
	t.Helper()
	resp := authRequest(t, "GET", "/api/agents?workspace_id="+testWorkspaceID, nil)
	var agents []map[string]any
	readJSON(t, resp, &agents)
	if len(agents) == 0 {
		t.Fatal("no agents in test workspace")
	}
	return agents[0]["id"].(string)
}

// createSecondAgent creates a second agent in the test workspace and returns its ID.
// It reuses the same runtime as the first agent.
func createSecondAgent(t *testing.T) string {
	t.Helper()
	// Fetch the first agent to get its runtime_id.
	resp := authRequest(t, "GET", "/api/agents?workspace_id="+testWorkspaceID, nil)
	var agents []map[string]any
	readJSON(t, resp, &agents)
	if len(agents) == 0 {
		t.Fatal("no agents in test workspace")
	}
	runtimeID := agents[0]["runtime_id"].(string)

	resp = authRequest(t, "POST", "/api/agents?workspace_id="+testWorkspaceID, map[string]any{
		"name":       fmt.Sprintf("Second Test Agent %d", time.Now().UnixNano()),
		"runtime_id": runtimeID,
		"visibility": "workspace",
	})
	if resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("CreateAgent: expected 201, got %d: %s", resp.StatusCode, body)
	}
	var agent map[string]any
	readJSON(t, resp, &agent)
	id := agent["id"].(string)
	t.Cleanup(func() {
		authRequest(t, "POST", "/api/agents/"+id+"/archive?workspace_id="+testWorkspaceID, nil)
	})
	return id
}

// createIssueAssignedToAgent creates a todo issue assigned to the given agent.
func createIssueAssignedToAgent(t *testing.T, title, agentID string) string {
	t.Helper()
	resp := authRequest(t, "PUT", fmt.Sprintf("/api/issues/%s", createIssue(t, title)), map[string]any{
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	var issue map[string]any
	readJSON(t, resp, &issue)
	return issue["id"].(string)
}

// createIssue creates a basic todo issue and returns its ID.
func createIssue(t *testing.T, title string) string {
	t.Helper()
	resp := authRequest(t, "POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title":  title,
		"status": "todo",
	})
	if resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("CreateIssue: expected 201, got %d: %s", resp.StatusCode, body)
	}
	var issue map[string]any
	readJSON(t, resp, &issue)
	return issue["id"].(string)
}

// postComment posts a comment as the test member.
func postComment(t *testing.T, issueID, content string, parentID *string) string {
	t.Helper()
	body := map[string]any{
		"content": content,
		"type":    "comment",
	}
	if parentID != nil {
		body["parent_id"] = *parentID
	}
	resp := authRequest(t, "POST", "/api/issues/"+issueID+"/comments", body)
	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("postComment: expected 201, got %d: %s", resp.StatusCode, b)
	}
	var comment map[string]any
	readJSON(t, resp, &comment)
	return comment["id"].(string)
}

// postCommentAsAgent posts a comment authenticated as the agent, using the
// real mat_ task token authRequestWithAgent mints.
func postCommentAsAgent(t *testing.T, issueID, content, agentID string, parentID *string) string {
	t.Helper()
	body := map[string]any{
		"content": content,
		"type":    "comment",
	}
	if parentID != nil {
		body["parent_id"] = *parentID
	}
	resp := authRequestWithAgent(t, "POST", "/api/issues/"+issueID+"/comments", body, agentID)
	if resp.StatusCode != 201 {
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("postCommentAsAgent: expected 201, got %d: %s", resp.StatusCode, b)
	}
	var comment map[string]any
	readJSON(t, resp, &comment)
	return comment["id"].(string)
}

// strPtr returns a pointer to a string.
func strPtr(s string) *string { return &s }

// newHTTPCommentOwner isolates the runtime and owner from other integration
// cases, while creating and assigning the issue through the real HTTP surface.
func newHTTPCommentOwner(t *testing.T, title string) (issueID, agentID string) {
	t.Helper()
	agentID = newHTTPCommentAgent(t)
	issueID = createIssueAssignedToAgent(t, title, agentID)
	fixture := testutil.New(testPool, testWorkspaceID, testUserID)
	fixture.Cleanup(t, "DELETE FROM issue WHERE id=$1", issueID)
	fixture.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issueID)
	return
}

func newHTTPCommentAgent(t *testing.T) string {
	t.Helper()
	fixture := testutil.New(testPool, testWorkspaceID, testUserID)
	runtimeID := fixture.Runtime(t, t.Name()+" runtime", testutil.Cols{"provider": "codex"})
	return fixture.Agent(t, fmt.Sprintf("%s owner %d", t.Name(), time.Now().UnixNano()), runtimeID)
}

func readHTTPCommentSession(t *testing.T, issueID string) handler.IssueAgentSessionResponse {
	t.Helper()
	resp := authRequest(t, http.MethodGet, "/api/issues/"+issueID+"/agent-sessions", nil)
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("read issue session: expected 200, got %d: %s", resp.StatusCode, body)
	}
	var sessions []handler.IssueAgentSessionResponse
	readJSON(t, resp, &sessions)
	if len(sessions) != 1 {
		t.Fatalf("issue sessions = %+v, want one owner", sessions)
	}
	return sessions[0]
}

func TestHTTPOwnerInboxReceivesTopLevelAndThreadedComments(t *testing.T) {
	issueID, ownerID := newHTTPCommentOwner(t, "Owner inbox HTTP integration")
	initial := readHTTPCommentSession(t, issueID)
	if initial.ActiveTaskID == nil || len(initial.Events) != 1 || initial.Events[0].Kind != "assigned" {
		t.Fatalf("assignment did not open the owner's session: %+v", initial)
	}
	if _, err := testPool.Exec(context.Background(), "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", *initial.ActiveTaskID); err != nil {
		t.Fatal(err)
	}
	agentRoot := postCommentAsAgent(t, issueID, "Explicit owner publication", ownerID, nil)
	memberRoot := postComment(t, issueID, "Initial member instruction", nil)
	comments := []struct {
		content string
		parent  *string
	}{
		{"Please fix this bug", nil},
		{"[@Other](mention://agent/00000000-0000-0000-0000-000000000001) context", nil},
		{fmt.Sprintf("[@Owner](mention://agent/%s) context", ownerID), nil},
		{"Member reply under owner publication", &agentRoot},
		{"Member reply under another member", &memberRoot},
	}
	var ids []string
	for _, comment := range comments {
		ids = append(ids, postComment(t, issueID, comment.content, comment.parent))
	}
	session := readHTTPCommentSession(t, issueID)
	if session.ID != initial.ID || session.AgentID != ownerID || session.ActiveTaskID == nil || *session.ActiveTaskID != *initial.ActiveTaskID || len(session.Events) != 8 {
		t.Fatalf("comments replaced their issue session: %+v", session)
	}
	if session.Events[1].SourceID == nil || *session.Events[1].SourceID != agentRoot || session.Events[1].Status != "ignored" {
		t.Fatalf("owner publication could wake itself: %+v", session.Events[1])
	}
	for i, id := range ids {
		event := session.Events[i+3]
		if event.SourceID == nil || *event.SourceID != id || event.Sequence != int64(i+4) || event.Kind != "comment.created" {
			t.Fatalf("message %s lost its ordered inbox event: %+v", id, event)
		}
	}
	var executions int
	if err := testPool.QueryRow(context.Background(), "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueID).Scan(&executions); err != nil || executions != 1 {
		t.Fatalf("comments created per-comment executions: count=%d, error=%v", executions, err)
	}
}

func TestHTTPBroadcastMentionsDoNotSuppressTheOwnerInbox(t *testing.T) {
	issueID, ownerID := newHTTPCommentOwner(t, "Broadcasts remain owner messages")
	initial := readHTTPCommentSession(t, issueID)
	root := postComment(t, issueID, "Thread context", nil)
	contents := []string{
		"[@All](mention://all/all) heads up everyone",
		fmt.Sprintf("[@All](mention://all/all) [@Owner](mention://agent/%s) update", ownerID),
		"[@All](mention://all/all) [@Other](mention://agent/00000000-0000-0000-0000-000000000001) update",
	}
	for _, content := range contents {
		postComment(t, issueID, content, &root)
	}
	session := readHTTPCommentSession(t, issueID)
	if session.ID != initial.ID || session.AgentID != ownerID || len(session.Events) != 5 || countPendingTasks(t, issueID) != 1 {
		t.Fatalf("broadcast mentions changed owner delivery: %+v", session)
	}
	for _, event := range session.Events[1:] {
		if event.Kind != "comment.created" || event.Status != "pending" {
			t.Fatalf("broadcast content suppressed an inbox message: %+v", event)
		}
	}
}

// TestCommentTriggerOnAssignNoStatusGate verifies that assigning an agent to
// a non-todo issue still triggers the agent (status gate was removed).
func TestCommentTriggerOnAssignNoStatusGate(t *testing.T) {
	agentID := newHTTPCommentAgent(t)

	// Create an in_progress issue.
	issueID := createIssue(t, "On-assign status gate test")
	resp := authRequest(t, "PUT", "/api/issues/"+issueID, map[string]any{
		"status": "in_progress",
	})
	resp.Body.Close()

	t.Cleanup(func() {
		clearTasks(t, issueID)
		resp := authRequest(t, "DELETE", "/api/issues/"+issueID, nil)
		resp.Body.Close()
	})

	// Assign the agent — should trigger despite non-todo status.
	resp = authRequest(t, "PUT", "/api/issues/"+issueID, map[string]any{
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		t.Fatalf("assign agent: expected 200, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	if n := countPendingTasks(t, issueID); n != 1 {
		t.Errorf("expected 1 pending task after assigning to in_progress issue, got %d", n)
	}
}

func TestHTTPMentionOnDoneIssueRequiresExplicitOwnerAssignment(t *testing.T) {
	agentID := newHTTPCommentAgent(t)
	issueID := createIssue(t, "Done issue with explicit owner")
	fixture := testutil.New(testPool, testWorkspaceID, testUserID)
	fixture.Cleanup(t, "DELETE FROM issue WHERE id=$1", issueID)
	fixture.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issueID)
	resp := authRequest(t, http.MethodPut, "/api/issues/"+issueID, map[string]any{"status": "done"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mark done: %d", resp.StatusCode)
	}
	resp.Body.Close()
	postComment(t, issueID, fmt.Sprintf("[@Agent](mention://agent/%s) follow-up context", agentID), nil)
	if n := countPendingTasks(t, issueID); n != 0 {
		t.Fatalf("a mention on an unassigned done issue dispatched %d tasks", n)
	}
	resp = authRequest(t, http.MethodPut, "/api/issues/"+issueID, map[string]any{"assignee_type": "agent", "assignee_id": agentID})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("assign done issue: %d", resp.StatusCode)
	}
	resp.Body.Close()
	session := readHTTPCommentSession(t, issueID)
	if session.AgentID != agentID || session.ActiveTaskID == nil || len(session.Events) != 1 || session.Events[0].Kind != "assigned" || countPendingTasks(t, issueID) != 1 {
		t.Fatalf("done status suppressed an explicit owner assignment: %+v", session)
	}
}

func TestHTTPThreadMentionsCannotSelectAnotherSessionOwner(t *testing.T) {
	issueID, ownerID := newHTTPCommentOwner(t, "One owner across comment threads")
	otherID := newHTTPCommentAgent(t)
	root := postComment(t, issueID, fmt.Sprintf("[@Other](mention://agent/%s) context", otherID), nil)
	contents := []string{
		"Plain follow-up", fmt.Sprintf("[@Other](mention://agent/%s) update", otherID),
		fmt.Sprintf("[@Owner](mention://agent/%s) update", ownerID),
		fmt.Sprintf("[@Member](mention://member/%s) context", testUserID),
	}
	for _, content := range contents {
		postComment(t, issueID, content, &root)
	}
	session := readHTTPCommentSession(t, issueID)
	if session.AgentID != ownerID || len(session.Events) != 6 || countPendingTasks(t, issueID) != 1 || countPendingTasksForAgent(t, issueID, otherID) != 0 {
		t.Fatalf("thread mentions selected another execution owner: %+v", session)
	}
	for _, event := range session.Events[2:] {
		var payload struct {
			ParentCommentID string `json:"parent_comment_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.ParentCommentID != root {
			t.Fatalf("thread event lost its actual parent: %+v, %v", payload, err)
		}
	}
	otherIssueID := createIssue(t, "Foreign parent issue")
	fixture := testutil.New(testPool, testWorkspaceID, testUserID)
	fixture.Cleanup(t, "DELETE FROM issue WHERE id=$1", otherIssueID)
	foreignParent := postComment(t, otherIssueID, "A separate issue's thread", nil)
	resp := authRequest(t, http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{"content": "Must remain on this issue", "parent_id": foreignParent})
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("cross-issue parent: expected 400, got %d", resp.StatusCode)
	}
	resp.Body.Close()
	if after := readHTTPCommentSession(t, issueID); len(after.Events) != len(session.Events) {
		t.Fatal("a rejected cross-issue reply appended an inbox event")
	}
}

func TestHTTPCommentEditAndDeletionAppendOrderedEventsWithoutCancellingTheOwner(t *testing.T) {
	issueID, _ := newHTTPCommentOwner(t, "Comment lifecycle in one inbox")
	initial := readHTTPCommentSession(t, issueID)
	commentID := postComment(t, issueID, "Original instruction", nil)
	replyID := postComment(t, issueID, "Keep the reply thread", &commentID)
	resp := authRequest(t, http.MethodPut, "/api/comments/"+commentID, map[string]any{"content": "Corrected instruction", "expected_revision": 1})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("edit comment: expected 200, got %d", resp.StatusCode)
	}
	var updated handler.CommentResponse
	readJSON(t, resp, &updated)
	resp = authRequest(t, http.MethodDelete, "/api/comments/"+commentID, nil)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete comment: expected 204, got %d", resp.StatusCode)
	}
	resp.Body.Close()
	session := readHTTPCommentSession(t, issueID)
	if initial.ActiveTaskID == nil || session.ActiveTaskID == nil || *initial.ActiveTaskID != *session.ActiveTaskID || len(session.Events) != 5 || countPendingTasks(t, issueID) != 1 {
		t.Fatalf("comment lifecycle cancelled or replaced the owner: %+v", session)
	}
	kinds := []string{"assigned", "comment.created", "comment.created", "comment.updated", "comment.deleted"}
	for i, kind := range kinds {
		if session.Events[i].Kind != kind || session.Events[i].Sequence != int64(i+1) {
			t.Fatalf("event %d = %+v, want %s in order", i, session.Events[i], kind)
		}
	}
	if updated.Revision <= session.Events[1].SourceRevision || session.Events[3].SourceRevision != updated.Revision || session.Events[4].SourceRevision <= updated.Revision {
		t.Fatalf("source revisions did not advance across edit and tombstone: %+v", session.Events)
	}
	resp = authRequest(t, http.MethodGet, "/api/issues/"+issueID+"/comments", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list comments: %d", resp.StatusCode)
	}
	var comments []handler.CommentResponse
	readJSON(t, resp, &comments)
	var foundRoot, foundReply bool
	for _, comment := range comments {
		if comment.ID == commentID {
			foundRoot = comment.DeletedAt != nil && comment.Content == ""
		}
		if comment.ID == replyID {
			foundReply = comment.ParentID != nil && *comment.ParentID == commentID
		}
	}
	if !foundRoot || !foundReply {
		t.Fatalf("deletion lost the tombstone or reply parent: %+v", comments)
	}
}

func TestHTTPCommentThreadsShareTheirIssueSession(t *testing.T) {
	issueID, ownerID := newHTTPCommentOwner(t, "Independent comment threads in one session")
	initial := readHTTPCommentSession(t, issueID)
	first := postComment(t, issueID, "First comment", nil)
	second := postComment(t, issueID, "Second comment", nil)
	reply := postComment(t, issueID, "Follow-up to the first", &first)
	session := readHTTPCommentSession(t, issueID)
	if session.ID != initial.ID || len(session.Events) != 4 || countPendingTasks(t, issueID) != 1 {
		t.Fatalf("comment threads started separate executions: %+v", session)
	}
	for i, id := range []string{first, second, reply} {
		event := session.Events[i+1]
		if event.SourceID == nil || *event.SourceID != id || event.Sequence != int64(i+2) {
			t.Fatalf("thread event order = %+v", event)
		}
	}
	var commentTasks int
	if err := testPool.QueryRow(context.Background(), "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND (trigger_comment_id IS NOT NULL OR cardinality(coalesced_comment_ids)>0)", issueID).Scan(&commentTasks); err != nil || commentTasks != 0 {
		t.Fatalf("owner session created old per-comment tasks: %d, %v", commentTasks, err)
	}
	otherIssueID := createIssueAssignedToAgent(t, "Same agent, independent issue", ownerID)
	fixture := testutil.New(testPool, testWorkspaceID, testUserID)
	fixture.Cleanup(t, "DELETE FROM issue WHERE id=$1", otherIssueID)
	fixture.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", otherIssueID)
	postComment(t, otherIssueID, "A different issue can receive input concurrently", nil)
	other := readHTTPCommentSession(t, otherIssueID)
	if other.ID == session.ID || other.ActiveTaskID == nil || session.ActiveTaskID == nil || *other.ActiveTaskID == *session.ActiveTaskID || other.AgentID != ownerID || countPendingTasks(t, otherIssueID) != 1 {
		t.Fatalf("the same agent configuration serialized two issues into one session: %+v / %+v", session, other)
	}
}

func TestHTTPOwnerInboxAcceptsPlainAndMentionedFollowupsAfterDone(t *testing.T) {
	issueID, ownerID := newHTTPCommentOwner(t, "Done owner session follow-ups")
	initial := readHTTPCommentSession(t, issueID)
	resp := authRequest(t, http.MethodPut, "/api/issues/"+issueID, map[string]any{"status": "done"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("mark done: %d", resp.StatusCode)
	}
	resp.Body.Close()
	plain := postComment(t, issueID, "A follow-up after completion", nil)
	mentioned := postComment(t, issueID, fmt.Sprintf("[@Owner](mention://agent/%s) another detail", ownerID), nil)
	session := readHTTPCommentSession(t, issueID)
	if session.ID != initial.ID || session.ActiveTaskID == nil || initial.ActiveTaskID == nil || *session.ActiveTaskID != *initial.ActiveTaskID || countPendingTasks(t, issueID) != 1 || len(session.Events) != 4 {
		t.Fatalf("done status changed the owner routing: %+v", session)
	}
	for i, id := range []string{plain, mentioned} {
		event := session.Events[i+2]
		if event.Kind != "comment.created" || event.SourceID == nil || *event.SourceID != id || event.Status != "pending" {
			t.Fatalf("done follow-up was not delivered to the owner inbox: %+v", event)
		}
	}
}
