package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

// createPrivateAgentOwnedBy inserts a private agent owned by ownerID and
// returns its id. Mirrors privateAgentTestFixture's agent shape (visibility
// 'private', no explicit permission_mode so it keeps the private default the
// canInvokeAgent gate treats as owner-only), but lets a test stand up a SECOND
// private agent under the same human so a leader and a worker can both be
// private and owned by the same originator.
func createPrivateAgentOwnedBy(t *testing.T, name, ownerID string) string {
	t.Helper()
	var agentID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO agent (
			workspace_id, name, description, runtime_mode, runtime_config,
			runtime_id, visibility, max_concurrent_tasks, owner_id,
			instructions, custom_env, custom_args
		)
		VALUES ($1, $2, '', 'cloud', '{}'::jsonb,
		        $3, 'private', 1, $4, '', '{}'::jsonb, '[]'::jsonb)
		RETURNING id
	`, testWorkspaceID, name, handlerTestRuntimeID(t), ownerID).Scan(&agentID); err != nil {
		t.Fatalf("create private agent %q: %v", name, err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, agentID) })
	return agentID
}

// Preserve the full authorization chain: human H → agent A creates a
// squad-assigned issue → private leader L explicitly creates a worker subissue.
// Both the ordinary create provenance and the worker's persistent session must
// carry H; a mention alone cannot dispatch the private worker.
func TestAgentCreateOriginator_E2E_ExplicitPrivateWorkerAssignment(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	// Private worker J and the human originator H (J's owner).
	workerJID, ownerH, _ := privateAgentTestFixture(t)

	// Private squad leader L, owned by the same human H.
	leaderID := createPrivateAgentOwnedBy(t, "mul4305-e2e-private-leader", ownerH)

	var squadID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO squad (workspace_id, name, description, leader_id, creator_id)
		VALUES ($1, 'MUL-4305 E2E Squad', '', $2, $3)
		RETURNING id
	`, testWorkspaceID, leaderID, testUserID).Scan(&squadID); err != nil {
		t.Fatalf("create squad: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM squad WHERE id = $1`, squadID) })

	// Creator agent A, running a task on behalf of the human H. resolveActor
	// validates the (A, task) pair; the handler then trusts X-Task-ID as A's
	// acting task and inherits H from it.
	creatorAID := createHandlerTestAgent(t, "mul4305-e2e-creator-agent", nil)
	var creatorTaskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, status, priority, originator_user_id, accountable_user_id)
		VALUES ($1, (SELECT runtime_id FROM agent WHERE id = $1), 'running', 0, $2, $2)
		RETURNING id
	`, creatorAID, ownerH).Scan(&creatorTaskID); err != nil {
		t.Fatalf("create A's acting task: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, creatorTaskID)
	})

	// Step 1: agent A creates an issue through the ordinary create path and
	// assigns it to the private-leader squad in the same call.
	w := httptest.NewRecorder()
	r := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title":         "MUL-4305 E2E agent-created + squad-assigned",
		"assignee_type": "squad",
		"assignee_id":   squadID,
	})
	r.Header.Set("X-Agent-ID", creatorAID)
	r.Header.Set("X-Task-ID", creatorTaskID)
	testHandler.CreateIssue(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("CreateIssue: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var created IssueResponse
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("decode issue: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, created.ID)
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, created.ID)
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, created.ID)
	})

	// The create must have stamped the issue back to A's acting task.
	var originType, originID string
	if err := testPool.QueryRow(ctx,
		`SELECT COALESCE(origin_type, ''), COALESCE(origin_id::text, '') FROM issue WHERE id = $1`, created.ID,
	).Scan(&originType, &originID); err != nil {
		t.Fatalf("load issue origin: %v", err)
	}
	if originType != "agent_create" || originID != creatorTaskID {
		t.Fatalf("issue origin = (%q,%q), want (agent_create,%s)", originType, originID, creatorTaskID)
	}

	// The squad-leader assignment task must have been enqueued carrying H.
	var leaderTaskID, leaderOriginator string
	if err := testPool.QueryRow(ctx, `
		SELECT id, COALESCE(originator_user_id::text, '')
		FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 AND is_leader_task
		ORDER BY created_at DESC LIMIT 1
	`, created.ID, leaderID).Scan(&leaderTaskID, &leaderOriginator); err != nil {
		t.Fatalf("load squad-leader task (expected one enqueued on create): %v", err)
	}
	if leaderOriginator != ownerH {
		t.Fatalf("squad-leader task originator = %q, want the original human H %q", leaderOriginator, ownerH)
	}

	// The leader now runs its explicitly assigned work for H.
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", leaderTaskID)
	comment := captureSessionComment(t, created.ID, map[string]any{
		"content": "private work context [@Worker](mention://agent/" + workerJID + ")",
	}, leaderID, leaderTaskID)
	if comment.SourceTaskID == nil || *comment.SourceTaskID != leaderTaskID || len(comment.TriggerOutcomes) != 0 {
		t.Fatalf("leader comment lost its trusted source or created a route: %+v", comment)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2", created.ID, workerJID); n != 0 {
		t.Fatalf("private worker received %d tasks from a mention", n)
	}
	// The explicit subissue assignment is the collaboration action.
	r = newRequest(http.MethodPost, "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title": "Private worker explicitly assigned by leader", "parent_issue_id": created.ID,
		"assignee_type": "agent", "assignee_id": workerJID,
	})
	r.Header.Set("X-Agent-ID", leaderID)
	r.Header.Set("X-Task-ID", leaderTaskID)
	r.Header.Set("X-Actor-Source", "task_token")
	var child IssueResponse
	testutil.Call(t, testHandler.CreateIssue, r).Want(http.StatusCreated).JSON(&child)
	dbfx.Cleanup(t, "DELETE FROM issue WHERE id=$1", child.ID)
	dbfx.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", child.ID)
	var parentIssueID, childOriginType, childOriginID string
	dbfx.QueryRow(t, "SELECT parent_issue_id,origin_type,origin_id FROM issue WHERE id=$1", child.ID).Scan(&parentIssueID, &childOriginType, &childOriginID)
	if parentIssueID != created.ID || childOriginType != "agent_create" || childOriginID != leaderTaskID {
		t.Fatalf("worker subissue lost its parent or authoring run: parent=%s origin=%s/%s", parentIssueID, childOriginType, childOriginID)
	}
	sessions := captureIssueSessions(t, child.ID)
	if len(sessions) != 1 || sessions[0].AgentID != workerJID || sessions[0].ActiveTaskID == nil || len(sessions[0].Events) != 1 || sessions[0].Events[0].Kind != "assigned" {
		t.Fatalf("explicit private-worker assignment did not create its session: %+v", sessions)
	}
	workerTask, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(*sessions[0].ActiveTaskID))
	if err != nil || workerTask.Status != "queued" || uuidToString(workerTask.AgentID) != workerJID || uuidToString(workerTask.OriginatorUserID) != ownerH || uuidToString(workerTask.AccountableUserID) != ownerH {
		t.Fatalf("worker session lost the original authorizing human: %+v, %v", workerTask, err)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_task_id=$2 AND originator_user_id=$3", sessions[0].ID, leaderTaskID, ownerH); n != 1 {
		t.Fatalf("worker event failed to preserve leader-to-worker provenance: %d", n)
	}

}

// TestAgentCreateOriginator_E2E_UpdateAssignSquad_HandlerGateAdmitsPrivateLeader
// locks the specific squad-leader GATE change in this PR, which the create path
// above does not exercise (create routes through the ungated service enqueue).
//
// Agent A (running for human H) creates an UNASSIGNED issue via the ordinary
// path, then assigns it to a private-leader squad via UpdateIssue. That assign
// routes through the handler enqueueSquadLeaderTask gate. Pre-fix the gate saw
// an empty originator for an agent actor and denied the private leader, so no
// leader task was enqueued even though the HTTP assign returned 200. With the
// agent_create stamp feeding the shared OriginatorForIssueTask, the gate now
// resolves H and enqueues the leader task carrying H.
func TestAgentCreateOriginator_E2E_UpdateAssignSquad_HandlerGateAdmitsPrivateLeader(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	// Reuse the private-agent fixture purely for a private agent + its human
	// owner H; the fixture agent here plays the squad leader.
	leaderID, ownerH, _ := privateAgentTestFixture(t)

	var squadID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO squad (workspace_id, name, description, leader_id, creator_id)
		VALUES ($1, 'MUL-4305 E2E Update-Assign Squad', '', $2, $3)
		RETURNING id
	`, testWorkspaceID, leaderID, testUserID).Scan(&squadID); err != nil {
		t.Fatalf("create squad: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM squad WHERE id = $1`, squadID) })

	creatorAID := createHandlerTestAgent(t, "mul4305-e2e-update-creator", nil)
	var creatorTaskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, status, priority, originator_user_id, accountable_user_id)
		VALUES ($1, (SELECT runtime_id FROM agent WHERE id = $1), 'running', 0, $2, $2)
		RETURNING id
	`, creatorAID, ownerH).Scan(&creatorTaskID); err != nil {
		t.Fatalf("create A's acting task: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1`, creatorTaskID)
	})

	// Agent A creates an unassigned issue via the ordinary path.
	w := httptest.NewRecorder()
	r := newRequest("POST", "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
		"title": "MUL-4305 E2E unassigned then squad-assigned",
	})
	r.Header.Set("X-Agent-ID", creatorAID)
	r.Header.Set("X-Task-ID", creatorTaskID)
	testHandler.CreateIssue(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("CreateIssue: expected 201, got %d: %s", w.Code, w.Body.String())
	}
	var created IssueResponse
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil {
		t.Fatalf("decode issue: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, created.ID)
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, created.ID)
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, created.ID)
	})

	// Agent A assigns the issue to the private-leader squad via UpdateIssue.
	w = httptest.NewRecorder()
	r = newRequest("PATCH", "/api/issues/"+created.ID, map[string]any{
		"assignee_type": "squad",
		"assignee_id":   squadID,
	})
	r.Header.Set("X-Agent-ID", creatorAID)
	r.Header.Set("X-Task-ID", creatorTaskID)
	r = withURLParam(r, "id", created.ID)
	testHandler.UpdateIssue(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("UpdateIssue (assign squad): expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// The gated handler path must have enqueued the private leader carrying H.
	var leaderCount int
	if err := testPool.QueryRow(ctx, `
		SELECT count(*) FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 AND is_leader_task AND originator_user_id = $3
	`, created.ID, leaderID, ownerH).Scan(&leaderCount); err != nil {
		t.Fatalf("count leader tasks: %v", err)
	}
	if leaderCount == 0 {
		t.Fatalf("private squad leader got 0 tasks attributed to H after agent-triggered assign; the enqueue gate denied it (MUL-4305 gate regression)")
	}
}
