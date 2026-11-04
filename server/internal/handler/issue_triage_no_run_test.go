package handler

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/dispatch"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Automatic issue inputs stay in the owner's inbox while Triage has no
// executor. Accepting the issue releases that same session; text mentions never
// select another executor. Explicit quick actions still obey the Triage gate.

type triageRunFixture struct {
	agentID   string
	squadID   string
	leaderID  string
	actionID  string
	runtimeID string
}

// newTriageRunFixture builds targets a plain workspace member is allowed to
// invoke, so a blocked run is blocked by Triage and not by the permission gate.
func newTriageRunFixture(t *testing.T) triageRunFixture {
	t.Helper()
	runtimeID := dbfx.Runtime(t, "triage no-run runtime")
	invocable := func(name string) string {
		id := dbfx.Agent(t, name, runtimeID, testutil.Cols{
			"visibility":      "workspace",
			"permission_mode": "public_to",
		})
		dbfx.InsertNoID(t, "agent_invocation_target", testutil.Cols{
			"agent_id": id, "target_type": "workspace", "target_id": testWorkspaceID,
		}, "agent_id = $1 AND target_type = 'workspace' AND target_id = $2", id, testWorkspaceID)
		return id
	}
	agentID := invocable("triage no-run agent")
	leaderID := invocable("triage no-run leader")
	squadID := dbfx.Squad(t, "triage no-run squad", leaderID)

	var actionID string
	dbfx.QueryRow(t, `
		INSERT INTO quick_action (
			workspace_id, name, description, assignee_type, assignee_id, prompt,
			visibility, created_by_type, created_by_id
		) VALUES ($1, 'Triage No Run', '', 'agent', $2, 'take a look', 'public', 'member', $3)
		RETURNING id
	`, testWorkspaceID, agentID, testUserID).Scan(&actionID)
	dbfx.Cleanup(t, `DELETE FROM quick_action WHERE id = $1`, actionID)

	return triageRunFixture{agentID: agentID, squadID: squadID, leaderID: leaderID, actionID: actionID, runtimeID: runtimeID}
}

// issueFor creates an issue assigned to the fixture agent. triageState is the
// value of the column that puts it in Triage; "" is an ordinary issue.
//
// Both halves carry status `todo`, which is the point: Triage is no longer a
// status, so the two issues differ in exactly one field and nothing else can
// explain a difference in behavior between them.
func (f triageRunFixture) issueFor(t *testing.T, triageState, title string) string {
	t.Helper()
	cols := testutil.Cols{
		"status":        "todo",
		"assignee_type": "agent",
		"assignee_id":   f.agentID,
		"number":        nextWorkspaceIssueNumber(t),
	}
	if triageState != "" {
		cols["triage_state"] = triageState
	}
	issueID := dbfx.Issue(t, title, cols)
	dbfx.Cleanup(t, `DELETE FROM comment WHERE issue_id = $1`, issueID)
	dbfx.Cleanup(t, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	return issueID
}

// triagePending is the only Triage state today.
const triagePending = "pending"

func tasksOn(t *testing.T, issueID string) int {
	t.Helper()
	return dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1`, issueID)
}

func commentsOn(t *testing.T, issueID string) int {
	t.Helper()
	return dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id = $1`, issueID)
}

// Pair every Triage refusal with an ordinary-issue control. The retired rerun
// endpoint is refused on both, so its positive control is the session inbox.
type triageEntryPoint struct {
	name                string
	runsOnOrdinaryIssue bool
	call                func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response
}

func triageEntryPoints() []triageEntryPoint {
	comment := func(t *testing.T, issueID, content string) *testutil.Response {
		t.Helper()
		return testutil.Call(t, testHandler.CreateComment, withURLParam(
			newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{"content": content}), "id", issueID))
	}
	return []triageEntryPoint{
		{"comment mentioning an agent", true, func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response {
			return comment(t, issueID, "[@Agent](mention://agent/"+f.agentID+") please take a look")
		}},
		{"comment mentioning a squad", true, func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response {
			return comment(t, issueID, "[@Squad](mention://squad/"+f.squadID+") please take a look")
		}},
		// Reply placement does not change the owner-session address.
		{"reply to an agent's comment", true, func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response {
			parentID := dbfx.Comment(t, issueID, "here is what I found", testutil.Cols{
				"author_type": "agent", "author_id": f.agentID,
			})
			return testutil.Call(t, testHandler.CreateComment, withURLParam(
				newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{
					"content": "thanks, what about the other case?", "parent_id": parentID,
				}), "id", issueID))
		}},
		{"plain comment routed to the assignee", true, func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response {
			return comment(t, issueID, "any progress?")
		}},
		{"manual rerun", false, func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response {
			return testutil.Call(t, testHandler.RerunIssue, withURLParam(
				newRequest(http.MethodPost, "/api/issues/"+issueID+"/rerun", nil), "id", issueID))
		}},
		{"quick action", true, func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response {
			return testutil.Call(t, testHandler.RunQuickAction, testutil.WithURLParams(
				newRequest(http.MethodPost, "/api/issues/"+issueID+"/quick-actions/"+f.actionID+"/run", nil),
				"id", issueID, "quickActionId", f.actionID))
		}},
		// Explicit squad assignment remains separate from automatic comments.
		{"reassignment", true, func(t *testing.T, f triageRunFixture, issueID string) *testutil.Response {
			return testutil.Call(t, testHandler.UpdateIssue, withURLParam(
				newRequest(http.MethodPut, "/api/issues/"+issueID, map[string]any{
					"assignee_type": "squad", "assignee_id": f.squadID,
				}), "id", issueID))
		}},
	}
}

func TestTriageNoRunEntryPointsUseOrdinaryIssueSessions(t *testing.T) {
	f := newTriageRunFixture(t)
	for _, entry := range triageEntryPoints() {
		t.Run(entry.name, func(t *testing.T) {
			issueID := f.issueFor(t, "", "ordinary issue: "+entry.name)
			response := entry.call(t, f, issueID)
			if !entry.runsOnOrdinaryIssue {
				if code := response.Want(http.StatusConflict).Map()["code"]; code != "issue_session_event_required" {
					t.Fatalf("retired rerun code = %v, want issue_session_event_required", code)
				}
				if n := tasksOn(t, issueID); n != 0 {
					t.Fatalf("retired rerun started %d executions", n)
				}
				return
			}
			response.WantOneOf(http.StatusOK, http.StatusCreated, http.StatusAccepted)
			if n := tasksOn(t, issueID); n == 0 {
				t.Fatalf("%s did not start work on an ordinary issue", entry.name)
			}
		})
	}
}

func TestTriageInputsCannotStartExecutionsBeforeAcceptance(t *testing.T) {
	f := newTriageRunFixture(t)
	for _, entry := range triageEntryPoints() {
		t.Run(entry.name, func(t *testing.T) {
			issueID := f.issueFor(t, triagePending, "triage issue: "+entry.name)
			entry.call(t, f, issueID).WantOneOf(http.StatusOK, http.StatusCreated, http.StatusForbidden)
			if n := tasksOn(t, issueID); n != 0 {
				t.Fatalf("%s started %d executions before acceptance", entry.name, n)
			}
		})
	}
}

// The two entry points the caller is waiting on a response from are refused
// outright, rather than accepted and silently dropped at the queue door.
func TestTriageRefusesRerunAndQuickActionWithAReason(t *testing.T) {
	f := newTriageRunFixture(t)

	t.Run("rerun", func(t *testing.T) {
		issueID := f.issueFor(t, triagePending, "triage rerun")
		resp := testutil.Call(t, testHandler.RerunIssue, withURLParam(
			newRequest(http.MethodPost, "/api/issues/"+issueID+"/rerun", nil), "id", issueID))
		if got := resp.Want(http.StatusForbidden).Map()["reason_code"]; got != string(dispatch.ReasonIssueInTriage) {
			t.Fatalf("reason_code = %v, want issue_in_triage: %s", got, resp.Text())
		}
	})

	// A quick action is a comment AND a run. Refusing it after the comment was
	// written would leave a prompt addressed to nobody, so nothing is written.
	t.Run("quick action writes no comment", func(t *testing.T) {
		issueID := f.issueFor(t, triagePending, "triage quick action")
		resp := testutil.Call(t, testHandler.RunQuickAction, testutil.WithURLParams(
			newRequest(http.MethodPost, "/api/issues/"+issueID+"/quick-actions/"+f.actionID+"/run", nil),
			"id", issueID, "quickActionId", f.actionID))
		if got := resp.Want(http.StatusForbidden).Map()["reason_code"]; got != string(dispatch.ReasonIssueInTriage) {
			t.Fatalf("reason_code = %v, want issue_in_triage: %s", got, resp.Text())
		}
		if n := commentsOn(t, issueID); n != 0 {
			t.Fatalf("refused quick action left %d comment(s) on the issue", n)
		}
	})
}

func TestTriageCommentMentionsWaitForOwnerAcceptance(t *testing.T) {
	f := newTriageRunFixture(t)
	issueID := f.issueFor(t, triagePending, "triage owner inbox")
	content := "[@Agent](mention://agent/" + f.agentID + ") and [@Squad](mention://squad/" + f.squadID + ") please look"
	comment := captureSessionComment(t, issueID, map[string]any{"content": content}, "", "")
	if len(comment.TriggerOutcomes) != 1 || comment.TriggerOutcomes[0].TargetID != f.agentID || comment.TriggerOutcomes[0].Status != DispatchDeferred {
		t.Fatalf("comment changed the pending owner receipt: %+v", comment.TriggerOutcomes)
	}
	if n := tasksOn(t, issueID); n != 0 {
		t.Fatalf("triage comment started %d executions", n)
	}
	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 1 || sessions[0].AgentID != f.agentID || sessions[0].State != "blocked" || sessions[0].FailureReason == nil || *sessions[0].FailureReason != "issue_not_ready" {
		t.Fatalf("triage did not hold its owner inbox: %+v", sessions)
	}
	if len(sessions[0].Events) != 2 || sessions[0].Events[1].SourceID == nil || *sessions[0].Events[1].SourceID != comment.ID || sessions[0].Events[1].Status != "pending" {
		t.Fatalf("triage lost the durable input: %+v", sessions[0].Events)
	}
	// Acceptance changes the gate, without replaying the comment or assigning
	// the mentioned squad. Runtime polling releases the same inbox.
	dbfx.Exec(t, `UPDATE issue SET triage_state=NULL WHERE id=$1`, issueID)
	if _, err := testHandler.TaskService.WakeIssueAgentSessionsForRuntime(t.Context(), parseUUID(f.runtimeID), 32); err != nil {
		t.Fatal(err)
	}
	accepted := captureIssueSessions(t, issueID)
	if len(accepted) != 1 || accepted[0].ID != sessions[0].ID || accepted[0].ActiveTaskID == nil || len(accepted[0].Events) != 2 {
		t.Fatalf("acceptance changed session identity or replayed input: %+v", accepted)
	}
	if n := tasksOn(t, issueID); n != 1 {
		t.Fatalf("accepted input started %d executions, want only the owner", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id<>$2`, issueID, f.agentID); n != 0 {
		t.Fatalf("mentioned squad started %d unrelated executions", n)
	}
}

// The preview has to agree with the door. A preview promising a run that the
// enqueue then discards is worse than no preview: dispatchIssueRun drops the
// error, so nothing would ever report the difference.
func TestPreviewIssueTriggerReportsNoRunForTriage(t *testing.T) {
	f := newTriageRunFixture(t)
	issueID := f.issueFor(t, triagePending, "triage preview")

	// Reassigning inside Triage is allowed and would otherwise preview a run:
	// assignment is the one trigger source that never looked at status.
	reassign := map[string]any{
		"issue_ids":     []string{issueID},
		"assignee_type": "agent",
		"assignee_id":   f.leaderID,
	}
	if preview := previewIssueTrigger(t, reassign); preview.TotalCount != 0 {
		t.Errorf("triage reassign previews %+v, want no run", preview)
	}

	// Accept clears the column and the issue becomes ordinary work. The same
	// request now previews a run, which is what makes the zero above the
	// column's doing rather than the fixture's.
	dbfx.Exec(t, `UPDATE issue SET triage_state = NULL WHERE id = $1`, issueID)
	accepted := previewIssueTrigger(t, reassign)
	if accepted.TotalCount != 1 || len(accepted.Triggers) != 1 {
		t.Fatalf("accepted issue previews %+v, want exactly one run", accepted)
	}
	if accepted.Triggers[0].Source != "assign" || accepted.Triggers[0].AgentID != f.leaderID {
		t.Errorf("accepted preview = %+v, want the new assignee started by an assign", accepted.Triggers[0])
	}
}

// The queue door itself, on the enqueue entry points no HTTP request reaches.
// This is the check the whole rule rests on: dispatchIssueRun discards enqueue
// errors, so a path that slipped past every short-circuit above would fail
// silently rather than loudly.
//
// The same funnel is exercised from both sides, because the door does not judge
// the funnel — it judges what the caller says the run is. Passing nothing is
// OriginDerived, so a future enqueue path cannot leak into Triage by omission.
func TestEnqueueRefusesADerivedRunInTriage(t *testing.T) {
	ctx := context.Background()
	f := newTriageRunFixture(t)
	issueID := f.issueFor(t, triagePending, "triage queue door")
	issue, err := testHandler.Queries.GetIssue(ctx, parseUUID(issueID))
	if err != nil {
		t.Fatal(err)
	}
	agent, squad := parseUUID(f.agentID), parseUUID(f.squadID)
	leader := parseUUID(f.leaderID)

	derived := map[string]func() error{
		"assignee": func() error {
			_, err := testHandler.TaskService.EnqueueTaskForIssue(ctx, issue)
			return err
		},
		"mention funnel, derived": func() error {
			_, err := testHandler.TaskService.EnqueueTaskForMention(ctx, issue, agent, pgtype.UUID{}, service.OriginDerived)
			return err
		},
		"squad leader, derived": func() error {
			_, err := testHandler.TaskService.EnqueueTaskForSquadLeader(ctx, issue, leader, squad, pgtype.UUID{}, service.OriginDerived)
			return err
		},
		"squad leader on assign": func() error {
			_, err := testHandler.TaskService.EnqueueTaskForSquadLeaderByActor(ctx, issue, leader, squad, pgtype.UUID{})
			return err
		},
		"rerun with no source": func() error {
			_, err := testHandler.TaskService.RerunIssue(ctx, issue.ID, pgtype.UUID{}, pgtype.UUID{}, pgtype.UUID{}, false, nil)
			return err
		},
	}
	for name, call := range derived {
		t.Run(name, func(t *testing.T) {
			if err := call(); !errors.Is(err, service.ErrIssueInTriage) {
				t.Fatalf("enqueue = %v, want ErrIssueInTriage", err)
			}
		})
	}
	if n := tasksOn(t, issueID); n != 0 {
		t.Fatalf("refused enqueues still wrote %d task row(s)", n)
	}

	// The other side of the same door, so the refusals above are the origin's
	// doing and not the funnel's.
	named := map[string]func() error{
		"mention funnel, named": func() error {
			_, err := testHandler.TaskService.EnqueueTaskForMention(ctx, issue, agent, pgtype.UUID{}, service.OriginNamed)
			return err
		},
		"thread parent": func() error {
			_, err := testHandler.TaskService.EnqueueTaskForThreadParent(ctx, issue, leader, pgtype.UUID{})
			return err
		},
	}
	for name, call := range named {
		t.Run(name, func(t *testing.T) {
			if err := call(); err != nil {
				t.Fatalf("named enqueue = %v, want it admitted", err)
			}
		})
	}
	if n := tasksOn(t, issueID); n != len(named) {
		t.Fatalf("named enqueues wrote %d task row(s), want %d", n, len(named))
	}
}

// Auto-retry is the one enqueue with no caller to refuse: the sweeper decides
// on its own. A failed run on an issue in Triage stays failed.
func TestAutoRetrySkipsAnIssueInTriage(t *testing.T) {
	ctx := context.Background()
	f := newTriageRunFixture(t)
	issueID := f.issueFor(t, triagePending, "triage auto retry")
	taskID := dbfx.Task(t, f.agentID, testutil.Cols{
		"runtime_id":     f.runtimeID,
		"issue_id":       issueID,
		"status":         "failed",
		"failure_reason": "timeout",
		"attempt":        1,
		"max_attempts":   2,
		"started_at":     testutil.Raw("now() - interval '2 minutes'"),
		"completed_at":   testutil.Raw("now() - interval '1 minute'"),
	})
	parent, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(taskID))
	if err != nil {
		t.Fatal(err)
	}
	// Nothing about this task is itself unretryable — on a todo issue the same
	// row retries — so the skip below can only come from the issue's status.
	if !retryableOnItsOwn(parent) {
		t.Fatal("fixture task is not retryable on its own terms")
	}
	child, err := testHandler.TaskService.MaybeRetryFailedTask(ctx, parent)
	if err != nil {
		t.Fatalf("MaybeRetryFailedTask = %v, want no error", err)
	}
	if child != nil {
		t.Fatalf("auto-retry created task %s for an issue in Triage", uuidToString(child.ID))
	}
	if n := tasksOn(t, issueID); n != 1 {
		t.Fatalf("issue has %d task(s), want only the original failed one", n)
	}
}

// retryableOnItsOwn mirrors the budget/reason half of the retry decision so the
// test above can prove the skip came from Triage and not from an exhausted
// fixture. It deliberately does not call retryEligible, which is in another
// package and is itself part of what this change altered.
func retryableOnItsOwn(t db.AgentTaskQueue) bool {
	return t.Status == "failed" && t.FailureReason.String == "timeout" &&
		t.Attempt < t.MaxAttempts && t.IssueID.Valid && !t.AutopilotRunID.Valid
}

// A triage run must not outlive Triage as something to repeat (MUL-7189 §5.6).
//
// This is the one path that survives accept: once the issue is runnable again
// nothing above refuses it, and rerun resolves its session from the task the
// user named rather than from GetLastTaskSession — so the query-level exclusion
// cannot see it. It would also target the triager instead of the assignee.
func TestRerunRefusesAHistoricalTriageTaskAfterAccept(t *testing.T) {
	f := newTriageRunFixture(t)
	// Already accepted: the issue itself runs fine, which is what makes the
	// source task the only thing left to refuse.
	issueID := f.issueFor(t, "", "accepted out of triage")

	sourceTask := func(cols testutil.Cols) string {
		base := testutil.Cols{
			"runtime_id":   f.runtimeID,
			"issue_id":     issueID,
			"status":       "completed",
			"session_id":   "TRIAGE-SESSION",
			"work_dir":     "/tmp/triage",
			"started_at":   testutil.Raw("now() - interval '2 minutes'"),
			"completed_at": testutil.Raw("now() - interval '1 minute'"),
		}
		for k, v := range cols {
			base[k] = v
		}
		return dbfx.Task(t, f.agentID, base)
	}
	rerun := func(taskID string) *testutil.Response {
		return testutil.Call(t, testHandler.RerunIssue, withURLParam(
			newRequest(http.MethodPost, "/api/issues/"+issueID+"/rerun", map[string]any{"task_id": taskID}), "id", issueID))
	}

	ordinaryID := sourceTask(nil)
	before := tasksOn(t, issueID)
	if code := rerun(ordinaryID).Want(http.StatusConflict).Map()["code"]; code != "issue_session_event_required" {
		t.Fatalf("ordinary source bypassed the session inbox: %v", code)
	}
	if got := tasksOn(t, issueID); got != before {
		t.Fatalf("retired rerun added %d tasks", got-before)
	}
	// The accepted issue is runnable through its authorized event inbox.
	captureSessionComment(t, issueID, map[string]any{"content": "continue accepted work"}, "", "")
	if got := tasksOn(t, issueID); got != before+1 {
		t.Fatalf("accepted input added %d tasks, want one owner execution", got-before)
	}

	triageID := sourceTask(testutil.Cols{"context": testutil.Raw(`'{"type":"triage"}'::jsonb`)})
	before = tasksOn(t, issueID)
	if body := rerun(triageID).Want(http.StatusBadRequest).Map()["error"]; body == nil {
		t.Fatal("refused triage rerun carried no error message")
	}
	if got := tasksOn(t, issueID); got != before {
		t.Fatalf("refused triage rerun created %d task(s)", got-before)
	}
}

// A historical discussion task cannot reopen the retired issue-run route;
// naming an actual triage task retains its more specific refusal.
func TestLegacyRerunCannotBypassTriageSessionEvents(t *testing.T) {
	f := newTriageRunFixture(t)
	issueID := f.issueFor(t, triagePending, "triage rerun by source")

	// A discussion run persisted by an older server.
	discussionID := dbfx.Task(t, f.agentID, testutil.Cols{
		"runtime_id":   f.runtimeID,
		"issue_id":     issueID,
		"status":       "completed",
		"started_at":   testutil.Raw("now() - interval '2 minutes'"),
		"completed_at": testutil.Raw("now() - interval '1 minute'"),
	})
	before := tasksOn(t, issueID)
	response := testutil.Call(t, testHandler.RerunIssue, withURLParam(
		newRequest(http.MethodPost, "/api/issues/"+issueID+"/rerun", map[string]any{"task_id": discussionID}), "id", issueID))
	if code := response.Want(http.StatusConflict).Map()["code"]; code != "issue_session_event_required" {
		t.Fatalf("discussion rerun bypassed the session inbox: %v", code)
	}
	if got := tasksOn(t, issueID); got != before {
		t.Fatalf("discussion rerun added %d tasks", got-before)
	}

	// The triage run itself is still refused, with its own reason.
	triageID := dbfx.Task(t, f.agentID, testutil.Cols{
		"runtime_id":   f.runtimeID,
		"issue_id":     issueID,
		"status":       "completed",
		"context":      testutil.Raw(`'{"type":"triage"}'::jsonb`),
		"started_at":   testutil.Raw("now() - interval '2 minutes'"),
		"completed_at": testutil.Raw("now() - interval '1 minute'"),
	})
	before = tasksOn(t, issueID)
	testutil.Call(t, testHandler.RerunIssue, withURLParam(
		newRequest(http.MethodPost, "/api/issues/"+issueID+"/rerun", map[string]any{"task_id": triageID}),
		"id", issueID)).Want(http.StatusBadRequest)
	if got := tasksOn(t, issueID); got != before {
		t.Fatalf("refused triage rerun created %d task(s)", got-before)
	}
}

// The claim-side half of the same rule. The service refuses first, so this
// guards a rerun row written by an older server during a rolling deploy — the
// one case where the refusal above was not in effect when the row was created.
func TestRerunSourceScopeRejectsATriageSource(t *testing.T) {
	agent := parseUUID(testUserID)
	issue := pgtype.UUID{Bytes: [16]byte{7}, Valid: true}
	task := db.AgentTaskQueue{AgentID: agent, IssueID: issue}
	ordinary := db.AgentTaskQueue{AgentID: agent, IssueID: issue}
	triage := db.AgentTaskQueue{AgentID: agent, IssueID: issue, Context: []byte(`{"type":"triage"}`)}

	if !rerunSourceMatchesTaskScope(task, ordinary) {
		t.Fatal("an ordinary same-issue source is in scope, so the triage case below proves nothing")
	}
	if rerunSourceMatchesTaskScope(task, triage) {
		t.Error("a triage source is in scope, so its session and workdir would be reused")
	}
}

// FailTask's in-transaction retry is the one enqueue that must refuse WITHOUT
// failing: the transaction also carries the parent's failed status, so aborting
// would leave the task stuck in 'running'.
func TestFailTaskRetryStartsNoRunForATriageIssue(t *testing.T) {
	ctx := context.Background()
	f := newTriageRunFixture(t)

	for _, tc := range []struct {
		name        string
		triageState string
		wantChild   bool
		wantOnFail  string
	}{
		{"ordinary", "", true, "an ordinary issue retries, so the triage case below proves nothing"},
		{"triage", triagePending, false, "an issue in Triage was given a retry"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issueID := f.issueFor(t, tc.triageState, "fail task retry: "+tc.name)
			taskID := dbfx.Task(t, f.agentID, testutil.Cols{
				"runtime_id":   f.runtimeID,
				"issue_id":     issueID,
				"status":       "running",
				"attempt":      1,
				"max_attempts": 2,
				"started_at":   testutil.Raw("now() - interval '1 minute'"),
			})
			if _, err := testHandler.TaskService.FailTask(ctx, parseUUID(taskID), "runtime went away", "", "", "", "timeout", false, "", ""); err != nil {
				t.Fatalf("FailTask: %v", err)
			}
			// The parent must land failed either way. A guard that aborted the
			// transaction would leave it 'running' and the task stuck forever.
			var status string
			dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id = $1`, taskID).Scan(&status)
			if status != "failed" {
				t.Fatalf("parent task status = %q, want failed", status)
			}
			children := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND id <> $2`, issueID, taskID)
			if (children > 0) != tc.wantChild {
				t.Fatalf("%s (children = %d)", tc.wantOnFail, children)
			}
		})
	}
}
