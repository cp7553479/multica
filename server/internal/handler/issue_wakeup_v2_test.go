package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// A status write outside the HTTP handlers still records the child's change
// in its own transaction; the sweep turns it into the parent's wake exactly
// once.
func TestChildDoneRecordedByAnyWriterAndSweptOnce(t *testing.T) {
	fx := newChildDoneFixture(t, "in_progress")
	sweep := func() {
		t.Helper()
		if err := (&service.IssueWakeupService{Tasks: testHandler.TaskService}).SweepChildEvents(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	dbfx.Exec(t, "UPDATE issue SET status='done' WHERE id=$1", fx.child.ID)
	if n := dbfx.Count(t, "SELECT count(*) FROM issue_child_event WHERE parent_id=$1 AND child_id=$2 AND kind='closed' AND processed_at IS NULL", fx.parent.ID, fx.child.ID); n != 1 {
		t.Fatalf("recorded closings = %d, want 1", n)
	}
	// Moving between closed statuses is not a change the rule cares about.
	dbfx.Exec(t, "UPDATE issue SET status='cancelled' WHERE id=$1", fx.child.ID)
	if n := dbfx.Count(t, "SELECT count(*) FROM issue_child_event WHERE parent_id=$1 AND kind IN ('closed','reopened')", fx.parent.ID); n != 1 {
		t.Fatalf("closed-to-closed recorded %d changes, want 1", n)
	}
	// Fresh rows belong to the request that wrote them; the sweep waits.
	sweep()
	if got := len(childDoneEntries(t, fx.parent.ID)); got != 0 {
		t.Fatalf("sweep took a fresh change: %d entries", got)
	}
	dbfx.Exec(t, "UPDATE issue_child_event SET created_at=created_at-interval '1 minute' WHERE parent_id=$1", fx.parent.ID)
	sweep()
	sweep()
	if got := len(childDoneEntries(t, fx.parent.ID)); got != 1 {
		t.Fatalf("swept change fired %d entries, want 1", got)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM issue_child_event WHERE parent_id=$1 AND processed_at IS NULL", fx.parent.ID); n != 0 {
		t.Fatalf("%d changes left pending", n)
	}
}

// The request path processes its own change; a later sweep finds nothing.
func TestChildDoneRequestPathLeavesNothingToSweep(t *testing.T) {
	fx := newChildDoneFixture(t, "in_progress")
	updateChildStatus(t, fx.child.ID, "done")
	dbfx.Exec(t, "UPDATE issue_child_event SET created_at=created_at-interval '1 hour',claimed_at=claimed_at-interval '1 hour' WHERE parent_id=$1", fx.parent.ID)
	if err := (&service.IssueWakeupService{Tasks: testHandler.TaskService}).SweepChildEvents(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(childDoneEntries(t, fx.parent.ID)); got != 1 {
		t.Fatalf("request plus sweep fired %d entries, want 1", got)
	}
}

// A rule created while the workspace default is off stays off until the
// issue sets its own.
func TestChildDoneWorkspaceDefault(t *testing.T) {
	var settings []byte
	dbfx.QueryRow(t, "SELECT settings FROM workspace WHERE id=$1", testWorkspaceID).Scan(&settings)
	t.Cleanup(func() {
		testPool.Exec(context.Background(), "UPDATE workspace SET settings=$2 WHERE id=$1", testWorkspaceID, settings)
	})
	dbfx.Exec(t, `UPDATE workspace SET settings=settings||'{"system_wakeup_child_done":false}' WHERE id=$1`, testWorkspaceID)

	off := newChildDoneFixture(t, "in_progress")
	if rules := listSystemWakeupsFor(t, off.parent.ID); len(rules) != 1 || rules[0].Enabled || rules[0].WorkspaceDefault {
		t.Fatalf("workspace default not applied: %+v", rules)
	}
	updateChildStatus(t, off.child.ID, "done")
	if got := len(childDoneEntries(t, off.parent.ID)); got != 0 {
		t.Fatalf("rule off by default still fired: %d", got)
	}
	// The issue's own setting wins over the default; a partial body keeps
	// the instruction.
	on := newChildDoneFixture(t, "in_progress")
	if w := putChildDoneRule(t, on.parent.ID, map[string]any{"instruction": "Check the window first."}); w.Code != http.StatusOK {
		t.Fatalf("save instruction: %d %s", w.Code, w.Body.String())
	}
	if w := putChildDoneRule(t, on.parent.ID, map[string]any{"enabled": true}); w.Code != http.StatusOK {
		t.Fatalf("enable: %d %s", w.Code, w.Body.String())
	}
	if rules := listSystemWakeupsFor(t, on.parent.ID); len(rules) != 1 || !rules[0].Enabled || rules[0].Instruction != "Check the window first." {
		t.Fatalf("override: %+v", rules)
	}
	updateChildStatus(t, on.child.ID, "done")
	if got := len(childDoneEntries(t, on.parent.ID)); got != 1 {
		t.Fatalf("issue override on: %d entries, want 1", got)
	}
}

type workspaceWakeupRow struct {
	ID              string  `json:"id"`
	IssueID         string  `json:"issue_id"`
	Source          string  `json:"source"`
	Rule            *string `json:"rule"`
	SystemStage     *int    `json:"system_stage"`
	SystemRemaining *int    `json:"system_remaining"`
	Revision        *int64  `json:"revision"`
	Enabled         bool    `json:"enabled"`
	PausedReason    *string `json:"paused_reason"`
	Runs7d          int     `json:"runs_7d"`
	CanManage       bool    `json:"can_manage"`
	Condition       any     `json:"condition"`
	LastTaskID      *string `json:"last_task_id"`
	ActiveRuns      int     `json:"active_runs"`
	Task            *struct {
		ID string `json:"id"`
	} `json:"task"`
}

func listWorkspaceWakeupRows(t *testing.T, query string) ([]workspaceWakeupRow, map[string]int) {
	t.Helper()
	rec := httptest.NewRecorder()
	testHandler.ListWorkspaceWakeups(rec, newRequest("GET", "/api/issue-wakeups?"+query, nil))
	if rec.Code != 200 {
		t.Fatalf("list %s: %d %s", query, rec.Code, rec.Body.String())
	}
	var out struct {
		Items  []workspaceWakeupRow `json:"items"`
		Counts map[string]int       `json:"counts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out.Items, out.Counts
}

func TestWorkspaceWakeupsListSourcesSystemRulesAndPaused(t *testing.T) {
	fx := newChildDoneFixture(t, "in_progress")
	createStagedChild(t, fx.parent.ID, 1, "done")
	createStagedChild(t, fx.parent.ID, 1, "todo")
	createStagedChild(t, fx.parent.ID, 2, "backlog")
	agent := dbfx.Agent(t, "list target", testRuntimeID)
	issue := dbfx.Issue(t, "list rules")
	dbfx.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", issue)
	dbfx.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", issue)
	dbfx.Cleanup(t, "DELETE FROM activity_log WHERE issue_id=$1", issue)
	dbfx.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issue)
	svc := service.IssueWakeupService{Tasks: testHandler.TaskService}
	create := func(in service.WakeupInput) string {
		t.Helper()
		in.AgentID, in.Instruction = agent, "check"
		w, err := svc.Create(context.Background(), parseUUID(issue), parseUUID(testUserID), pgtype.UUID{}, in)
		if err != nil {
			t.Fatal(err)
		}
		return uuidToString(w.ID)
	}
	member := create(service.WakeupInput{Kind: "event", Condition: json.RawMessage(`{"type":"issue_field","field":"status","value":"in_review"}`)})
	paused := create(service.WakeupInput{Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}})
	var lastRun string
	for turn := range 2 {
		if err := svc.Trigger(t.Context(), parseUUID(issue), parseUUID(paused), parseUUID(testUserID)); err != nil {
			t.Fatal(err)
		}
		if turn == 0 {
			// Two rule firings delivered to one consumer count as one execution.
			if err := svc.Trigger(t.Context(), parseUUID(issue), parseUUID(paused), parseUUID(testUserID)); err != nil {
				t.Fatal(err)
			}
		}
		session, err := testHandler.Queries.GetIssueAgentSessionForPair(t.Context(), db.GetIssueAgentSessionForPairParams{WorkspaceID: parseUUID(testWorkspaceID), IssueID: parseUUID(issue), AgentID: parseUUID(agent)})
		if err != nil || !session.ActiveTaskID.Valid {
			t.Fatalf("rule consumer: %+v %v", session, err)
		}
		lastRun = uuidToString(session.ActiveTaskID)
		dbfx.Exec(t, "UPDATE agent_task_queue SET status='dispatched',dispatched_at=now() WHERE id=$1", lastRun)
		if err := testHandler.TaskService.PinIssueAgentSession(t.Context(), session.ActiveTaskID, "inventory-provider-session", "/tmp/wakeup-inventory-session"); err != nil {
			t.Fatal(err)
		}
		inputs, err := testHandler.TaskService.ClaimIssueAgentEvents(t.Context(), session.ActiveTaskID, 32)
		want := 1
		if turn == 0 {
			want = 2
		}
		if err != nil || len(inputs) != want {
			t.Fatalf("actual rule claims=%d want=%d error=%v", len(inputs), want, err)
		}
		rows, _ := listWorkspaceWakeupRows(t, "scope=all&limit=100")
		found := false
		for _, row := range rows {
			if row.ID == paused {
				found = true
				if row.ActiveRuns != 1 || row.Runs7d != turn+1 || row.Task == nil || row.Task.ID != lastRun {
					t.Fatalf("claimed rule inputs did not identify one active execution: %+v", row)
				}
			}
		}
		if !found {
			t.Fatal("active rule missing from workspace inventory")
		}
		for _, input := range inputs {
			if err := testHandler.TaskService.AckIssueAgentEvent(t.Context(), session.ActiveTaskID, input.ID, true, ""); err != nil {
				t.Fatal(err)
			}
		}
		dbfx.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", lastRun)
		if _, err := testHandler.TaskService.CompleteTask(t.Context(), session.ActiveTaskID, []byte(`{"output":""}`), "inventory-provider-session", "/tmp/wakeup-inventory-session", "", false, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	dbfx.Exec(t, "UPDATE issue_wakeup SET enabled=false,paused_reason='rate',disabled_at=now() WHERE id=$1", paused)

	if w := putChildDoneRule(t, fx.parent.ID, map[string]any{"instruction": "Advance the next stage."}); w.Code != http.StatusOK {
		t.Fatalf("create system rule: %d", w.Code)
	}
	rows, counts := listWorkspaceWakeupRows(t, "scope=all&limit=100")
	byID := map[string]workspaceWakeupRow{}
	var system workspaceWakeupRow
	ok := false
	for _, r := range rows {
		byID[r.ID] = r
		if r.IssueID == fx.parent.ID && r.Source == "system" {
			system, ok = r, true
		}
	}
	if !ok || system.Rule == nil || *system.Rule != "child_done" || system.SystemStage == nil || *system.SystemStage != 1 ||
		system.SystemRemaining == nil || *system.SystemRemaining != 1 || !system.Enabled || !system.CanManage || system.Revision == nil {
		t.Fatalf("system row: %+v (found %t)", system, ok)
	}
	if byID[member].Source != "member" || byID[member].Condition == nil {
		t.Fatalf("member row: %+v", byID[member])
	}
	if byID[paused].PausedReason == nil || *byID[paused].PausedReason != "rate" || byID[paused].Runs7d != 2 || byID[paused].ActiveRuns != 0 || byID[paused].LastTaskID == nil || *byID[paused].LastTaskID != lastRun || byID[paused].Task == nil || byID[paused].Task.ID != lastRun {
		t.Fatalf("paused row: %+v", byID[paused])
	}
	if counts["paused"] < 1 {
		t.Fatalf("paused count missing: %+v", counts)
	}
	rows, _ = listWorkspaceWakeupRows(t, "scope=paused&limit=100")
	for _, r := range rows {
		if r.PausedReason == nil {
			t.Fatalf("paused scope returned %+v", r)
		}
	}
	rows, _ = listWorkspaceWakeupRows(t, "scope=all&source=system&limit=100")
	for _, r := range rows {
		if r.Source != "system" {
			t.Fatalf("system filter returned %+v", r)
		}
	}
	rec := httptest.NewRecorder()
	testHandler.ListWorkspaceWakeups(rec, newRequest("GET", "/api/issue-wakeups?source=robots", nil))
	if rec.Code != 400 {
		t.Fatalf("bad source accepted: %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	testHandler.ListPausedWakeups(rec, newRequest("GET", "/api/issue-wakeup-paused", nil))
	var pausedRows []struct {
		IssueID string `json:"issue_id"`
		ID      string `json:"id"`
		Reason  string `json:"paused_reason"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &pausedRows); err != nil || rec.Code != 200 {
		t.Fatalf("paused list %d: %s", rec.Code, rec.Body.String())
	}
	found := false
	for _, r := range pausedRows {
		found = found || (r.ID == paused && r.IssueID == issue && r.Reason == "rate")
	}
	if !found {
		t.Fatalf("paused rule missing: %+v", pausedRows)
	}
}

func TestIssueWakeupManagementEndpoints(t *testing.T) {
	issue := dbfx.Issue(t, "wakeup management")
	agent := dbfx.Agent(t, "management target", testRuntimeID)
	dbfx.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", issue)
	dbfx.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", issue)
	dbfx.Cleanup(t, "DELETE FROM activity_log WHERE issue_id=$1", issue)
	dbfx.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issue)
	svc := service.IssueWakeupService{Tasks: testHandler.TaskService}
	w, err := svc.Create(context.Background(), parseUUID(issue), parseUUID(testUserID), pgtype.UUID{}, service.WakeupInput{AgentID: agent, Kind: "every", IntervalSeconds: 3600, Instruction: "check staging"})
	if err != nil {
		t.Fatal(err)
	}
	id := uuidToString(w.ID)
	call := func(handler http.HandlerFunc, method string, body any, headers map[string]string) *httptest.ResponseRecorder {
		t.Helper()
		req := withURLParams(newRequest(method, "/", body), "id", issue, "wakeupID", id)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		handler(rec, req)
		return rec
	}
	if rec := call(testHandler.TriggerIssueWakeup, "POST", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("wake now: %d %s", rec.Code, rec.Body.String())
	}
	session, err := testHandler.Queries.GetIssueAgentSessionForPair(t.Context(), db.GetIssueAgentSessionForPairParams{
		WorkspaceID: parseUUID(testWorkspaceID), IssueID: parseUUID(issue), AgentID: parseUUID(agent),
	})
	if err != nil || !session.ActiveTaskID.Valid {
		t.Fatalf("wake did not start its persistent session consumer: %+v %v", session, err)
	}
	run := uuidToString(session.ActiveTaskID)
	task, err := testHandler.Queries.GetAgentTask(t.Context(), session.ActiveTaskID)
	if err != nil || !service.IsIssueAgentSessionTask(task) || task.HandoffNote.Valid || task.TriggerCommentID.Valid {
		t.Fatalf("wake created a legacy execution: %+v %v", task, err)
	}
	// Transferring receipts into the inbox does not yet associate a rule with
	// a provider delivery or grant this consumer the right to check in.
	rec := call(testHandler.ListIssueWakeupRuns, "GET", nil, nil)
	var runs []wakeupRunResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &runs); err != nil || rec.Code != http.StatusOK || len(runs) != 0 {
		t.Fatalf("unclaimed inbox incorrectly counted as delivered run history: %d %s", rec.Code, rec.Body.String())
	}
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", run)
	// A member cannot check in for the run.
	if rec := call(testHandler.CheckInIssueWakeup, "POST", map[string]any{"note": "fine"}, map[string]string{"X-Task-ID": run}); rec.Code != http.StatusForbidden {
		t.Fatalf("member check-in: %d %s", rec.Code, rec.Body.String())
	}
	agentHeaders := map[string]string{"X-Task-ID": run, "X-Agent-ID": agent, "X-Actor-Source": "task_token"}
	if rec := call(testHandler.CheckInIssueWakeup, "POST", map[string]any{"note": "not received"}, agentHeaders); rec.Code != http.StatusForbidden {
		t.Fatalf("unclaimed input granted check-in rights: %d %s", rec.Code, rec.Body.String())
	}
	if err := testHandler.TaskService.PinIssueAgentSession(t.Context(), task.ID, "management-provider-session", "/tmp/wakeup-management-session"); err != nil {
		t.Fatal(err)
	}
	events, err := testHandler.TaskService.ClaimIssueAgentEvents(t.Context(), task.ID, 32)
	if err != nil || len(events) != 1 || events[0].Kind != "wakeup.fired" || events[0].DeliveryTaskID != task.ID || events[0].Status != "delivering" {
		t.Fatalf("rule input was not actually claimed by its consumer: count=%d error=%v", len(events), err)
	}
	if err := testHandler.TaskService.AckIssueAgentEvent(t.Context(), task.ID, events[0].ID, true, ""); err != nil {
		t.Fatal(err)
	}
	if rec := call(testHandler.CheckInIssueWakeup, "POST", map[string]any{"note": "Nothing changed"}, agentHeaders); rec.Code != http.StatusNoContent {
		t.Fatalf("agent check-in: %d %s", rec.Code, rec.Body.String())
	}
	rec = call(testHandler.ListIssueWakeupRuns, "GET", nil, nil)
	if err := json.Unmarshal(rec.Body.Bytes(), &runs); err != nil || rec.Code != 200 {
		t.Fatalf("runs %d: %s", rec.Code, rec.Body.String())
	}
	if len(runs) != 1 || runs[0].ID != run || runs[0].CheckinNote != "Nothing changed" || len(runs[0].Triggers) != 1 || runs[0].Triggers[0] != "wakeup.manual" {
		t.Fatalf("runs: %+v", runs)
	}
	var note string
	var legacyCarrier bool
	dbfx.QueryRow(t, "SELECT context->'wakeup_checkins'->$2::text->>'note', context ? 'wakeup_id' FROM agent_task_queue WHERE id=$1", run, id).Scan(&note, &legacyCarrier)
	if note != "Nothing changed" || legacyCarrier {
		t.Fatal("check-in was not stored under the rule in the session execution")
	}
	if rec := call(testHandler.DeleteIssueWakeup, "DELETE", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body.String())
	}
	stillRunning, err := testHandler.Queries.GetAgentTask(t.Context(), task.ID)
	if err != nil || stillRunning.Status != "running" {
		t.Fatalf("deleting a rule cancelled its already running consumer: %+v %v", stillRunning, err)
	}
	if rec := call(testHandler.ListIssueWakeupRuns, "GET", nil, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("runs of a deleted rule: %d", rec.Code)
	}
}
