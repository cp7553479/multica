package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// actAsRun makes a request come from an agent's run, as the CLI does inside a
// task.
func actAsRun(req *http.Request, agentID, taskID string) *http.Request {
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Task-ID", taskID)
	req.Header.Set("X-Actor-Source", "task_token")
	return req
}

func setStatusAsRun(t *testing.T, issueID, status, agentID, taskID string) {
	t.Helper()
	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issueID, map[string]any{"status": status}), "id", issueID)
	if agentID != "" {
		req = actAsRun(req, agentID, taskID)
	}
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("set status %q: %d %s", status, w.Code, w.Body.String())
	}
}

func commentOn(t *testing.T, issueID, content, agentID, taskID string) {
	t.Helper()
	w := httptest.NewRecorder()
	req := withURLParam(newRequest("POST", "/api/issues/"+issueID+"/comments", map[string]any{"content": content}), "id", issueID)
	if agentID != "" {
		req = actAsRun(req, agentID, taskID)
	}
	testHandler.CreateComment(w, req)
	if w.Code != http.StatusCreated && w.Code != http.StatusOK {
		t.Fatalf("comment: %d %s", w.Code, w.Body.String())
	}
}

func wakeupInputsOf(t *testing.T, issueID, wakeupID string) []IssueAgentEventResponse {
	t.Helper()
	rows, err := testPool.Query(t.Context(), `SELECT e.id FROM issue_agent_event e
		JOIN issue_agent_session s ON s.id=e.session_id
		WHERE s.issue_id=$1 AND e.kind='wakeup.fired' AND e.payload->>'wakeup_id'=$2 ORDER BY e.sequence`, issueID, wakeupID)
	if err != nil {
		t.Fatal(err)
	}
	var ids []pgtype.UUID
	for rows.Next() {
		var id pgtype.UUID
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	out := make([]IssueAgentEventResponse, 0, len(ids))
	for _, id := range ids {
		event, err := testHandler.Queries.GetIssueAgentEvent(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, issueAgentEventResponse(event))
	}
	return out
}

func lastWakeupInputOutcome(t *testing.T, issueID string) (outcome, sessionID, eventID string) {
	t.Helper()
	var raw []byte
	dbfx.QueryRow(t, `SELECT details FROM activity_log WHERE issue_id=$1 AND action='wakeup_triggered'
		ORDER BY created_at DESC,id DESC LIMIT 1`, issueID).Scan(&raw)
	var details struct {
		Outcome   string `json:"outcome"`
		SessionID string `json:"session_id"`
		EventID   string `json:"event_id"`
	}
	if err := json.Unmarshal(raw, &details); err != nil {
		t.Fatal(err)
	}
	return details.Outcome, details.SessionID, details.EventID
}

func claimWakeupSession(t *testing.T, taskID string, supportsEvents bool) (AgentTaskResponse, *claimBuildFailure) {
	t.Helper()
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='dispatched',dispatched_at=clock_timestamp() WHERE id=$1`, taskID)
	task, err := testHandler.Queries.GetAgentTask(t.Context(), parseUUID(taskID))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := testHandler.Queries.GetAgentRuntime(t.Context(), task.RuntimeID)
	if err != nil {
		t.Fatal(err)
	}
	req := newOwnerDaemonRequest(http.MethodPost, "/claim", nil)
	if supportsEvents {
		req.Header.Set("X-Client-Capabilities", protocol.DaemonCapabilityIssueSessionEventsV1)
	} else {
		req.Header.Set("X-Client-Capabilities", protocol.DaemonCapabilityJoinedWakeupsV1)
	}
	resp, _, _, _, _, failure := testHandler.buildClaimedTaskResponse(req, &task, runtime, uuidToString(task.RuntimeID), testWorkspaceID)
	return resp, failure
}

func createRule(t *testing.T, issueID, agentID string, source pgtype.UUID, in service.WakeupInput) string {
	t.Helper()
	in.AgentID = agentID
	w, err := (&service.IssueWakeupService{Tasks: testHandler.TaskService}).Create(context.Background(), parseUUID(issueID), parseUUID(testUserID), source, in)
	if err != nil {
		t.Fatal(err)
	}
	return uuidToString(w.ID)
}

// #8849: the parent's agent closes the last sub-issue of a stage from its own
// run on the parent. That run knows, so the rule records the stage without
// queuing a second run; once that run has ended, or when someone else closes
// the stage, the agent is woken as before.
func TestChildDoneDoesNotWakeTheRunThatClosedTheStage(t *testing.T) {
	cases := []struct {
		name      string
		runStatus string
		byAgent   bool
		wantRuns  int
		wantEntry string
	}{
		{"own run still going", "running", true, 0, "acknowledged"},
		{"own run already ended", "completed", true, 1, "queued"},
		{"someone else closed it", "running", false, 1, "queued"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fx := newChildDoneFixture(t, "in_progress")
			agentID := handlerTestAgentID(t)
			setIssueAssigneeDirect(t, fx.parent.ID, "agent", agentID)
			run := dbfx.Task(t, agentID, testutil.Cols{"issue_id": fx.parent.ID, "runtime_id": testRuntimeID, "status": tc.runStatus, "originator_user_id": testUserID, "accountable_user_id": testUserID})
			if tc.byAgent {
				setStatusAsRun(t, fx.child.ID, "done", agentID, run)
			} else {
				updateChildStatus(t, fx.child.ID, "done")
			}
			if runs := childDoneRuns(t, fx.parent.ID); len(runs) != tc.wantRuns {
				t.Fatalf("runs = %+v, want %d", runs, tc.wantRuns)
			}
			entries := childDoneEntries(t, fx.parent.ID)
			if len(entries) != 1 || entries[0].Outcome != tc.wantEntry {
				t.Fatalf("entries = %+v, want %s", entries, tc.wantEntry)
			}
			if tc.wantRuns > 0 {
				inputs := childDoneRuns(t, fx.parent.ID)
				if inputs[0].SessionID == "" || inputs[0].EventID == "" || entries[0].SessionID != inputs[0].SessionID || entries[0].EventID != inputs[0].EventID || entries[0].TaskID != "" {
					t.Fatalf("timeline did not name its durable system input: entries=%+v inputs=%+v", entries, inputs)
				}
			} else if entries[0].SessionID != "" || entries[0].EventID != "" {
				t.Fatalf("self-acknowledgment created another input: %+v", entries)
			}
		})
	}
}

// A comment from the agent itself is acknowledged; external input is
// transferred to the explicitly subscribed agent's session.
func TestWakeupSkipsTheAgentsOwnComment(t *testing.T) {
	issue := dbfx.Issue(t, "own comment", testutil.Cols{"status": "in_progress"})
	t.Cleanup(func() { cleanupChildDoneIssue(issue) })
	agentID := handlerTestAgentID(t)
	rule := createRule(t, issue, agentID, pgtype.UUID{}, service.WakeupInput{Instruction: "Reply to new comments", Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}})
	run := dbfx.Task(t, agentID, testutil.Cols{"issue_id": issue, "runtime_id": testRuntimeID, "status": "running", "originator_user_id": testUserID, "accountable_user_id": testUserID})
	commentOn(t, issue, "Deployed the fix.", agentID, run)
	runWakeupTick(t)
	if inputs := wakeupInputsOf(t, issue, rule); len(inputs) != 0 {
		t.Fatalf("the agent's own comment created inputs: %+v", inputs)
	}
	if outcome, sessionID, eventID := lastWakeupInputOutcome(t, issue); outcome != "acknowledged" || sessionID != "" || eventID != "" {
		t.Fatalf("self-comment outcome = (%q,%q,%q)", outcome, sessionID, eventID)
	}
	commentOn(t, issue, "Can you double-check staging?", "", "")
	runWakeupTick(t)
	inputs := wakeupInputsOf(t, issue, rule)
	if len(inputs) != 1 || inputs[0].Status != "pending" {
		t.Fatalf("member comment did not enter the inbox: %+v", inputs)
	}
	if outcome, sessionID, eventID := lastWakeupInputOutcome(t, issue); outcome != "queued" || sessionID != inputs[0].SessionID || eventID != inputs[0].ID {
		t.Fatalf("member-comment timeline does not reference its input: (%q,%q,%q)", outcome, sessionID, eventID)
	}
	task := childDoneSessionTask(t, issue, agentID)
	if task.WorkflowStep.Valid || task.IsLeaderTask {
		t.Fatalf("auxiliary subscription acquired workflow ownership: %+v", task)
	}
}

func TestWakeupInputsShareTheAgentsWaitingIssueSession(t *testing.T) {
	issue, sessionID, waiting, runtimeID, _ := issueAgentSessionFixture(t)
	rule := createRule(t, issue, captureSessionAgent(t, sessionID), pgtype.UUID{}, service.WakeupInput{Instruction: "Summarize the discussion", Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}})
	commentOn(t, issue, "Here is more context.", "", "")
	runWakeupTick(t)
	commentOn(t, issue, "One more thing.", "", "")
	runWakeupTick(t)
	inputs := wakeupInputsOf(t, issue, rule)
	if len(inputs) != 2 || inputs[0].SessionID != sessionID || inputs[1].SessionID != sessionID || inputs[0].Sequence >= inputs[1].Sequence {
		t.Fatalf("rule inputs did not share their ordered issue session: %+v", inputs)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running')`, issue); n != 1 {
		t.Fatalf("rule created %d waiting consumers", n)
	}
	resp, failure := claimWakeupSession(t, waiting, true)
	if failure != nil {
		t.Fatalf("claim: %+v", failure)
	}
	if resp.WakeupJoined != "" || resp.WakeupID != "" || resp.HandoffNote != "" || resp.IssueAgentSessionID != sessionID {
		t.Fatalf("claim projected a legacy joined prompt: %+v", resp)
	}
	var ruleInputs []IssueAgentEventResponse
	for _, input := range resp.IssueSessionEvents {
		if input.Kind == "wakeup.fired" {
			ruleInputs = append(ruleInputs, input)
		}
	}
	if len(ruleInputs) != 2 || ruleInputs[0].ID != inputs[0].ID || ruleInputs[1].ID != inputs[1].ID {
		t.Fatalf("claim lost the independent rule inputs: %+v", ruleInputs)
	}
	for _, input := range ruleInputs {
		var payload struct {
			Instruction string `json:"instruction"`
			Evidence    struct {
				Facts []struct {
					EventType string `json:"event_type"`
				} `json:"facts"`
			} `json:"wakeup_evidence"`
		}
		if err := json.Unmarshal(input.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if input.Status != "delivering" || payload.Instruction != "Summarize the discussion" || len(payload.Evidence.Facts) != 1 || payload.Evidence.Facts[0].EventType != "comment.created" {
			t.Fatalf("claimed input lost its instruction or receipt fact: %+v", input)
		}
	}
	// Rendering events is not enough to start: a provider must also support
	// safe input while a turn is running, and present the exact claim generation.
	if resp.DispatchedAt == nil {
		t.Fatal("claim omitted its start generation")
	}
	start := func(capabilities []string) *http.Request {
		return testutil.WithURLParams(newOwnerDaemonRequest(http.MethodPost, "/start", map[string]any{
			"capabilities": capabilities, "runtime_id": runtimeID, "dispatched_at": *resp.DispatchedAt,
		}), "taskId", waiting)
	}
	testutil.Call(t, testHandler.StartTask, start([]string{protocol.DaemonCapabilityIssueSessionEventsV1})).Want(http.StatusPreconditionFailed)
	testutil.Call(t, testHandler.StartTask, start([]string{protocol.DaemonCapabilityIssueSessionEventsV1, protocol.DaemonCapabilityTaskSupplementV1})).Want(http.StatusOK)
	for _, input := range resp.IssueSessionEvents {
		ack := testutil.WithURLParams(newOwnerDaemonRequest(http.MethodPost, "/ack", map[string]any{"delivered": true}), "taskId", waiting, "eventId", input.ID)
		testutil.Call(t, testHandler.AckIssueAgentEvent, ack).Want(http.StatusOK)
	}
	for _, input := range wakeupInputsOf(t, issue, rule) {
		if input.Status != "delivered" {
			t.Fatalf("claimed rule input was not acknowledged: %+v", input)
		}
	}
}

func captureSessionAgent(t *testing.T, sessionID string) string {
	t.Helper()
	var agent string
	dbfx.QueryRow(t, `SELECT agent_id FROM issue_agent_session WHERE id=$1`, sessionID).Scan(&agent)
	return agent
}

func TestWakeupInputExplicitlyRejectsAnOlderDaemon(t *testing.T) {
	issue, sessionID, waiting, _, _ := issueAgentSessionFixture(t)
	agentID := captureSessionAgent(t, sessionID)
	rule := createRule(t, issue, agentID, pgtype.UUID{}, service.WakeupInput{Instruction: "Summarize the discussion", Kind: "event", EventTypes: []string{"comment.created"}})
	commentOn(t, issue, "Here is more context.", "", "")
	runWakeupTick(t)
	inputs := wakeupInputsOf(t, issue, rule)
	if len(inputs) != 1 {
		t.Fatalf("expected one durable rule input: %+v", inputs)
	}
	_, failure := claimWakeupSession(t, waiting, false)
	if failure == nil || failure.status != http.StatusPreconditionFailed || failure.outcome != "issue_session_daemon_unsupported" {
		t.Fatalf("older daemon was not explicitly rejected: %+v", failure)
	}
	if current := wakeupInputsOf(t, issue, rule); len(current) != 1 || current[0].ID != inputs[0].ID || current[0].SessionID != sessionID || current[0].Status == "delivering" || current[0].Status == "delivered" {
		t.Fatalf("unsupported claim consumed or replaced its input: %+v", current)
	}
	var status string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1`, waiting).Scan(&status)
	if status != "failed" {
		t.Fatalf("unsupported execution status = %q", status)
	}
	runWakeupTick(t)
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND context->>'type'='issue_session'`, issue); n != 1 {
		t.Fatalf("older daemon rejection created %d session executions", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND context->>'wakeup_id' IS NOT NULL`, issue); n != 0 {
		t.Fatalf("older daemon rejection created %d fallback wakeup tasks", n)
	}
}

func TestConditionRuleAndTheAgentsOwnChange(t *testing.T) {
	cases := []struct {
		name       string
		ownRule    bool
		wantInputs int
	}{{"rule the agent set up", true, 0}, {"rule a person set up", false, 1}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issue := dbfx.Issue(t, "own condition", testutil.Cols{"status": "in_progress"})
			t.Cleanup(func() { cleanupChildDoneIssue(issue) })
			agentID := handlerTestAgentID(t)
			var source pgtype.UUID
			if tc.ownRule {
				earlier := dbfx.Task(t, agentID, testutil.Cols{"issue_id": issue, "runtime_id": testRuntimeID, "status": "completed", "originator_user_id": testUserID, "accountable_user_id": testUserID})
				source = parseUUID(earlier)
			}
			rule := createRule(t, issue, agentID, source, service.WakeupInput{Instruction: "Run the release checklist", Kind: "event", Condition: json.RawMessage(`{"type":"issue_field","field":"status","value":"in_review"}`)})
			runWakeupTick(t)
			run := dbfx.Task(t, agentID, testutil.Cols{"issue_id": issue, "runtime_id": testRuntimeID, "status": "running", "originator_user_id": testUserID, "accountable_user_id": testUserID})
			setStatusAsRun(t, issue, "in_review", agentID, run)
			runWakeupTick(t)
			inputs := wakeupInputsOf(t, issue, rule)
			if len(inputs) != tc.wantInputs {
				t.Fatalf("condition inputs = %+v, want %d", inputs, tc.wantInputs)
			}
			if tc.wantInputs > 0 {
				if !strings.Contains(string(inputs[0].Payload), "condition.met") || !strings.Contains(string(inputs[0].Payload), "Run the release checklist") {
					t.Fatalf("condition input lost its facts or instruction: %s", inputs[0].Payload)
				}
				if task := childDoneSessionTask(t, issue, agentID); task.WorkflowStep.Valid || task.IsLeaderTask {
					t.Fatalf("auxiliary condition acquired workflow permissions: %+v", task)
				}
			}
		})
	}
}
