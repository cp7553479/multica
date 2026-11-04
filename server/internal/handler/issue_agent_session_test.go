package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func issueAgentSessionFixture(t *testing.T) (issueID, sessionID, taskID, runtimeID, daemonID string) {
	t.Helper()
	if testHandler == nil || testPool == nil {
		t.Fatal("PostgreSQL is required for issue session tests")
	}
	agentID, runtimeID, daemonID := createRuntimeGuardAgent(t, context.Background())
	issueID = dbfx.Issue(t, "durable issue session", testutil.Cols{
		"assignee_type": "agent", "assignee_id": agentID,
	})
	dbfx.QueryRow(t, `SELECT id FROM issue_agent_session WHERE issue_id=$1 AND agent_id=$2`, issueID, agentID).Scan(&sessionID)
	dbfx.Exec(t, `UPDATE issue_agent_session SET provider_session_id='persistent-provider-thread', work_dir='/tmp/persistent-issue-session' WHERE id=$1`, sessionID)
	// A later historical task must not override the logical session's pointer.
	dbfx.Task(t, agentID, testutil.Cols{
		"issue_id": issueID, "runtime_id": runtimeID, "status": "completed",
		"session_id": "unrelated-history-thread", "work_dir": "/tmp/unrelated-history",
	})
	task, err := testHandler.TaskService.WakeIssueAgentSession(t.Context(), parseUUID(sessionID))
	if err != nil || task == nil {
		t.Fatalf("wake assigned issue session with the creator's authority: task=%v error=%v", task, err)
	}
	taskID = uuidToString(task.ID)
	dbfx.Cleanup(t, `DELETE FROM agent_task_queue WHERE id=$1`, taskID)
	if uuidToString(task.OriginatorUserID) != testUserID {
		t.Fatalf("session execution principal=%s, want assigning member %s", uuidToString(task.OriginatorUserID), testUserID)
	}
	return issueID, sessionID, taskID, runtimeID, daemonID
}

func TestIssueSessionClaimUsesPersistentPointerAndInitialInbox(t *testing.T) {
	_, sessionID, taskID, runtimeID, daemonID := issueAgentSessionFixture(t)
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil, testWorkspaceID, daemonID)
	req = testutil.WithURLParams(req, "runtimeId", runtimeID)
	req.Header.Set("X-Client-Capabilities", protocol.DaemonCapabilityIssueSessionEventsV1)
	var response struct {
		Task *AgentTaskResponse `json:"task"`
	}
	testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK).JSON(&response)
	if response.Task == nil {
		t.Fatal("expected issue session execution")
	}
	got := response.Task
	if got.ID != taskID || got.IssueAgentSessionID != sessionID {
		t.Fatalf("claim session identity = (%s, %s), want (%s, %s)", got.ID, got.IssueAgentSessionID, taskID, sessionID)
	}
	if got.PriorSessionID != "persistent-provider-thread" || got.PriorWorkDir != "/tmp/persistent-issue-session" {
		t.Fatalf("claim replaced persistent pointer with task history: (%s, %s)", got.PriorSessionID, got.PriorWorkDir)
	}
	if got.TriggerCommentID != nil || len(got.IssueSessionEvents) != 1 || got.IssueSessionEvents[0].Kind != "assigned" || got.IssueSessionEvents[0].Status != "delivering" {
		t.Fatalf("initial inbox = %+v, trigger=%v", got.IssueSessionEvents, got.TriggerCommentID)
	}
	var status, deliveryTask string
	dbfx.QueryRow(t, `SELECT status,delivery_task_id FROM issue_agent_event WHERE id=$1`, got.IssueSessionEvents[0].ID).Scan(&status, &deliveryTask)
	if status != "delivering" || deliveryTask != taskID {
		t.Fatalf("initial delivery receipt = (%s,%s)", status, deliveryTask)
	}
}

func TestIssueSessionClaimRefusesOldDaemon(t *testing.T) {
	_, _, taskID, runtimeID, daemonID := issueAgentSessionFixture(t)
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil, testWorkspaceID, daemonID)
	req = testutil.WithURLParams(req, "runtimeId", runtimeID)
	var response struct {
		Task *AgentTaskResponse `json:"task"`
	}
	testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK).JSON(&response)
	if response.Task != nil {
		t.Fatal("old daemon received an issue session it cannot render")
	}
	var status, message string
	dbfx.QueryRow(t, `SELECT status,COALESCE(error,'') FROM agent_task_queue WHERE id=$1`, taskID).Scan(&status, &message)
	if status != "failed" || !strings.Contains(message, "persistent issue sessions") {
		t.Fatalf("unsupported daemon outcome = (%s,%s)", status, message)
	}
}

func TestIssueSessionStartRequiresLiveInputCapability(t *testing.T) {
	_, _, taskID, _, _ := issueAgentSessionFixture(t)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='dispatched',dispatched_at=now() WHERE id=$1`, taskID)
	req := testutil.WithURLParams(newOwnerDaemonRequest(http.MethodPost, "/start", map[string]any{
		"capabilities": []string{protocol.DaemonCapabilityIssueSessionEventsV1},
	}), "taskId", taskID)
	testutil.Call(t, testHandler.StartTask, req).Want(http.StatusPreconditionFailed)
	var state string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1`, taskID).Scan(&state)
	if state != "dispatched" {
		t.Fatalf("unsupported adapter started execution: %s", state)
	}
}

func TestIssueSessionReadAndRetryAreScopedToIssue(t *testing.T) {
	issueID, sessionID, _, _, _ := issueAgentSessionFixture(t)
	var sessions []IssueAgentSessionResponse
	req := testutil.WithURLParams(newRequest(http.MethodGet, "/agent-sessions", nil), "id", issueID)
	testutil.Call(t, testHandler.ListIssueAgentSessions, req).Want(http.StatusOK).JSON(&sessions)
	if len(sessions) != 1 || sessions[0].ID != sessionID || len(sessions[0].Events) != 1 {
		t.Fatalf("issue sessions = %+v", sessions)
	}
	otherIssueID := dbfx.Issue(t, "another issue")
	retry := testutil.WithURLParams(newRequest(http.MethodPost, "/retry", nil),
		"id", otherIssueID, "sessionId", sessionID, "eventId", sessions[0].Events[0].ID)
	testutil.Call(t, testHandler.RetryIssueAgentEvent, retry).Want(http.StatusNotFound)
	var eventState string
	dbfx.QueryRow(t, `SELECT status FROM issue_agent_event WHERE id=$1`, sessions[0].Events[0].ID).Scan(&eventState)
	if eventState != "pending" {
		t.Fatalf("wrong-issue retry changed event to %s", eventState)
	}
}

func TestIssueSessionReadKeepsFailedHeadAndRecentEvents(t *testing.T) {
	issueID, sessionID, _, _, _ := issueAgentSessionFixture(t)
	dbfx.Exec(t, `UPDATE issue_agent_event SET status='failed',failure_reason='provider_unsupported' WHERE session_id=$1`, sessionID)
	dbfx.Exec(t, `INSERT INTO issue_agent_event (
		workspace_id,session_id,sequence,event_key,kind,source_id,source_revision,actor_type,actor_id,payload,status,failure_reason
	) SELECT $1,$2,n,'page-test:'||n,'issue.updated',$3,n,'member',$4,'{}'::jsonb,'pending',NULL
	FROM generate_series(2,205) n`, testWorkspaceID, sessionID, issueID, testUserID)
	var sessions []IssueAgentSessionResponse
	req := testutil.WithURLParams(newRequest(http.MethodGet, "/agent-sessions", nil), "id", issueID)
	testutil.Call(t, testHandler.ListIssueAgentSessions, req).Want(http.StatusOK).JSON(&sessions)
	if len(sessions) != 1 || !sessions[0].EventsTruncated || len(sessions[0].Events) != 200 {
		t.Fatalf("recent inbox page = %+v", sessions)
	}
	events := sessions[0].Events
	if events[0].Sequence != 1 || events[0].Status != "failed" || events[1].Sequence != 7 || events[199].Sequence != 205 {
		t.Fatalf("recent page omitted the blocked head or recent input: first=%+v next=%+v last=%+v", events[0], events[1], events[199])
	}
}

func TestIssueSessionRejectsLegacySupplementWithoutCreatingComment(t *testing.T) {
	issueID, _, taskID, _, _ := issueAgentSessionFixture(t)
	before := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, issueID)
	req := testutil.WithURLParams(newRequest(http.MethodPost, "/supplements", map[string]any{
		"content": "must be a session event", "client_request_id": "00000000-0000-0000-0000-000000000001",
	}), "id", issueID, "taskId", taskID)
	testutil.Call(t, testHandler.CreateTaskSupplement, req).Want(http.StatusConflict)
	if after := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, issueID); after != before {
		t.Fatalf("legacy supplement created a second input: before=%d after=%d", before, after)
	}
}

func TestIssueSessionFailureAcknowledgementStoresOnlyStableReason(t *testing.T) {
	_, _, taskID, runtimeID, daemonID := issueAgentSessionFixture(t)
	req := testutil.WithURLParams(newDaemonTokenRequest(http.MethodPost, "/claim", nil, testWorkspaceID, daemonID), "runtimeId", runtimeID)
	req.Header.Set("X-Client-Capabilities", protocol.DaemonCapabilityIssueSessionEventsV1)
	var response struct {
		Task *AgentTaskResponse `json:"task"`
	}
	testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK).JSON(&response)
	if response.Task == nil || len(response.Task.IssueSessionEvents) != 1 {
		t.Fatal("expected initial inbox event")
	}
	eventID := response.Task.IssueSessionEvents[0].ID
	ack := testutil.WithURLParams(newDaemonTokenRequest(http.MethodPost, "/ack", map[string]any{
		"delivered": false, "failure_reason": "provider diagnostic containing a credential must stay local",
	}, testWorkspaceID, daemonID), "taskId", taskID, "eventId", eventID)
	testutil.Call(t, testHandler.AckIssueAgentEvent, ack).Want(http.StatusOK)
	var reason string
	dbfx.QueryRow(t, `SELECT failure_reason FROM issue_agent_event WHERE id=$1`, eventID).Scan(&reason)
	if reason != "provider_rejected" {
		t.Fatalf("untrusted provider diagnostic was persisted: %q", reason)
	}
}

func TestIssueSessionLegacyRerunCannotCreateExecution(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(fmt.Sprintf("fresh=%t", fresh), func(t *testing.T) {
			issueID, sessionID, taskID, _, _ := issueAgentSessionFixture(t)
			before := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID)
			req := testutil.WithURLParams(newRequest(http.MethodPost, "/rerun", map[string]any{"fresh": fresh}), "taskId", taskID)
			var response struct {
				Code string `json:"code"`
			}
			testutil.Call(t, testHandler.RerunTaskByUser, withChatTestWorkspaceCtx(t, req)).Want(http.StatusConflict).JSON(&response)
			if response.Code != "issue_session_event_required" {
				t.Fatalf("legacy rerun refusal code=%q", response.Code)
			}
			if after := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); after != before {
				t.Fatalf("legacy rerun created a separate task: before=%d after=%d", before, after)
			}
			var activeTask, taskState, providerThread string
			dbfx.QueryRow(t, `SELECT s.active_task_id,t.status,s.provider_session_id FROM issue_agent_session s JOIN agent_task_queue t ON t.id=s.active_task_id WHERE s.id=$1`, sessionID).Scan(&activeTask, &taskState, &providerThread)
			if activeTask != taskID || taskState != "queued" || providerThread != "persistent-provider-thread" {
				t.Fatalf("refused rerun changed session: (%s,%s,%s)", activeTask, taskState, providerThread)
			}
		})
	}
}

func TestIssueSessionLegacyRerunRejectsReassignedSource(t *testing.T) {
	issueID, sessionID, taskID, runtimeID, _ := issueAgentSessionFixture(t)
	newAgent := dbfx.Agent(t, "replacement issue owner", runtimeID)
	dbfx.Exec(t, `UPDATE issue SET assignee_id=$2,revision=revision+1 WHERE id=$1`, issueID, newAgent)
	before := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID)
	req := testutil.WithURLParams(newRequest(http.MethodPost, "/rerun", map[string]any{"fresh": true}), "taskId", taskID)
	testutil.Call(t, testHandler.RerunTaskByUser, withChatTestWorkspaceCtx(t, req)).Want(http.StatusConflict)
	if after := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); after != before {
		t.Fatalf("reassigned source rerun created another consumer: before=%d after=%d", before, after)
	}
	var state string
	dbfx.QueryRow(t, `SELECT state FROM issue_agent_session WHERE id=$1`, sessionID).Scan(&state)
	if state != "detached" {
		t.Fatalf("legacy rerun reattached displaced session: %s", state)
	}
}

func TestIssueSessionLegacyRerunChecksSourceIssueOwnership(t *testing.T) {
	issueID, _, taskID, _, _ := issueAgentSessionFixture(t)
	otherIssueID := dbfx.Issue(t, "another rerun target")
	req := testutil.WithURLParams(newRequest(http.MethodPost, "/rerun", map[string]any{"task_id": taskID}), "id", otherIssueID)
	testutil.Call(t, testHandler.RerunIssue, req).Want(http.StatusBadRequest)
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, otherIssueID); n != 0 {
		t.Fatalf("wrong issue rerun created %d tasks", n)
	}
	var state string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1 AND issue_id=$2`, taskID, issueID).Scan(&state)
	if state != "queued" {
		t.Fatalf("wrong issue rerun changed source: %s", state)
	}
}

func TestIssueSessionServiceRejectsLegacyRerunBeforeFirstInput(t *testing.T) {
	issueID, sessionID, taskID, _, _ := issueAgentSessionFixture(t)
	// A migrated, already assigned issue may not have a logical session yet.
	// That absence must not authorize the old task-per-comment architecture.
	dbfx.Exec(t, `DELETE FROM issue_agent_event WHERE session_id=$1`, sessionID)
	dbfx.Exec(t, `DELETE FROM issue_agent_session WHERE id=$1`, sessionID)
	for _, fresh := range []bool{false, true} {
		result, err := testHandler.TaskService.RerunIssue(t.Context(), parseUUID(issueID), pgtype.UUID{}, pgtype.UUID{}, parseUUID(testUserID), fresh, nil)
		if result != nil || !errors.Is(err, service.ErrIssueSessionRerunRequiresEvent) {
			t.Fatalf("direct service rerun bypassed event model: task=%v error=%v", result, err)
		}
	}
	var state string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1`, taskID).Scan(&state)
	if state != "queued" || dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID) != 2 {
		t.Fatal("refused service rerun changed the existing tasks")
	}
}
