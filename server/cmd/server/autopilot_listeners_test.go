package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/service"
	dbfx "github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// seedAutopilotScheduleTrigger gives an autopilot a schedule trigger recording its
// creator. Since MUL-6951 an automatic dispatch resolves the human it acts as from
// this row — and admits the assignee agent as that human — so a dispatch with no
// trigger has no principal and fails closed.
func seedAutopilotScheduleTrigger(t *testing.T, queries *db.Queries, ap db.Autopilot) pgtype.UUID {
	t.Helper()
	trig, err := queries.CreateAutopilotTrigger(context.Background(), db.CreateAutopilotTriggerParams{
		AutopilotID:    ap.ID,
		Kind:           "schedule",
		Enabled:        true,
		CronExpression: pgtype.Text{String: "*/5 * * * *", Valid: true},
		Timezone:       pgtype.Text{String: "UTC", Valid: true},
		CreatedByType:  pgtype.Text{String: "member", Valid: ap.CreatedByType == "member"},
		CreatedByID:    ap.CreatedByID,
	})
	if err != nil {
		t.Fatalf("seed schedule trigger: %v", err)
	}
	return trig.ID
}

func TestAutopilotRunOnlyTaskTerminalEventsUpdateRun(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerAutopilotListeners(bus, autopilotSvc)

	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load fixture agent: %v", err)
	}

	tests := []struct {
		name       string
		finalize   func(task db.AgentTaskQueue)
		wantStatus string
		wantResult string
		wantReason string
	}{
		{
			name: "completed",
			finalize: func(task db.AgentTaskQueue) {
				if _, err := taskSvc.CompleteTask(ctx, task.ID, []byte(`{"output":"done"}`), "", "", "", false, "", ""); err != nil {
					t.Fatalf("CompleteTask: %v", err)
				}
			},
			wantStatus: "completed",
			wantResult: "done",
		},
		{
			name: "failed",
			finalize: func(task db.AgentTaskQueue) {
				if _, err := taskSvc.FailTask(ctx, task.ID, "boom", "", "", "", "agent_error", false, "", ""); err != nil {
					t.Fatalf("FailTask: %v", err)
				}
			},
			wantStatus: "failed",
			wantReason: "boom",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
				WorkspaceID:        parseUUID(testWorkspaceID),
				Title:              "Run-only listener " + tc.name,
				Description:        pgtype.Text{String: "Run listener regression test", Valid: true},
				AssigneeType:       "agent",
				AssigneeID:         parseUUID(agentID),
				Status:             "active",
				ExecutionMode:      "run_only",
				IssueTitleTemplate: pgtype.Text{},
				CreatedByType:      "member",
				CreatedByID:        parseUUID(testUserID),
			})
			if err != nil {
				t.Fatalf("CreateAutopilot: %v", err)
			}
			t.Cleanup(func() {
				if _, err := testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID); err != nil {
					t.Logf("cleanup autopilot: %v", err)
				}
			})

			run, err := autopilotSvc.DispatchAutopilot(ctx, ap, seedAutopilotScheduleTrigger(t, queries, ap), "manual", nil)
			if err != nil {
				t.Fatalf("DispatchAutopilot: %v", err)
			}
			if !run.TaskID.Valid {
				t.Fatal("run_only dispatch did not link a task")
			}

			if _, err := testPool.Exec(ctx,
				`UPDATE agent_task_queue SET status = 'dispatched', dispatched_at = now() WHERE id = $1`,
				run.TaskID,
			); err != nil {
				t.Fatalf("mark task dispatched: %v", err)
			}
			task, err := queries.StartAgentTaskWithSupplement(ctx, db.StartAgentTaskWithSupplementParams{TaskID: run.TaskID})
			if err != nil {
				t.Fatalf("StartAgentTaskWithSupplement: %v", err)
			}

			tc.finalize(task)

			updatedRun, err := queries.GetAutopilotRun(ctx, run.ID)
			if err != nil {
				t.Fatalf("GetAutopilotRun: %v", err)
			}
			if updatedRun.Status != tc.wantStatus {
				t.Fatalf("expected run status %q, got %q", tc.wantStatus, updatedRun.Status)
			}
			if tc.wantResult != "" && !strings.Contains(string(updatedRun.Result), tc.wantResult) {
				t.Fatalf("expected run result to contain %q, got %s", tc.wantResult, string(updatedRun.Result))
			}
			if tc.wantReason != "" {
				if !updatedRun.FailureReason.Valid {
					t.Fatalf("expected failure reason %q, got invalid", tc.wantReason)
				}
				if updatedRun.FailureReason.String != tc.wantReason {
					t.Fatalf("expected failure reason %q, got %q", tc.wantReason, updatedRun.FailureReason.String)
				}
			}
		})
	}
}

// linkedIssueAutopilotFixture starts from a real create_issue dispatch. Its
// first session consumer is linked from the run, never classified as run_only.
type linkedIssueAutopilotFixture struct {
	taskSvc      *service.TaskService
	autopilotSvc *service.AutopilotService
	queries      *db.Queries
	run          *db.AutopilotRun
	taskID       pgtype.UUID
}

// dispatchCreateIssueAutopilot creates an active create_issue autopilot,
// dispatches it, and returns the linked run plus its single issue task.
// Cleanup (autopilot, issue, tasks, comments) is registered on t.
func dispatchCreateIssueAutopilot(t *testing.T, title string) linkedIssueAutopilotFixture {
	t.Helper()
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerAutopilotListeners(bus, autopilotSvc)

	fixture := dbfx.New(testPool, testWorkspaceID, testUserID)
	runtimeID := fixture.Runtime(t, title+" runtime", dbfx.Cols{"provider": "codex"})
	agentID := fixture.Agent(t, title+" agent", runtimeID)

	ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              title,
		Description:        pgtype.Text{String: "VEN-661 / VEN-662 regression test", Valid: true},
		AssigneeType:       "agent",
		AssigneeID:         parseUUID(agentID),
		Status:             "active",
		ExecutionMode:      "create_issue",
		IssueTitleTemplate: pgtype.Text{String: "Linked issue", Valid: true},
		CreatedByType:      "member",
		CreatedByID:        parseUUID(testUserID),
	})
	if err != nil {
		t.Fatalf("CreateAutopilot: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	run, err := autopilotSvc.DispatchAutopilot(ctx, ap, seedAutopilotScheduleTrigger(t, queries, ap), "schedule", nil)
	if err != nil {
		t.Fatalf("DispatchAutopilot: %v", err)
	}
	if !run.IssueID.Valid {
		t.Fatal("create_issue dispatch did not link an issue")
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, run.IssueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, run.IssueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, run.IssueID)
	})

	tasks, err := queries.ListTasksByIssue(ctx, run.IssueID)
	if err != nil {
		t.Fatalf("ListTasksByIssue: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one issue task, got %d", len(tasks))
	}
	if tasks[0].AutopilotRunID.Valid || !service.IsIssueAgentSessionTask(tasks[0]) || run.TaskID != tasks[0].ID {
		t.Fatalf("create_issue did not atomically link its initial session consumer: run=%+v task=%+v", run, tasks[0])
	}
	initial, err := queries.GetAutopilotIssueAgentAssignment(ctx, service.IssueAgentSessionID(tasks[0]))
	if err != nil {
		t.Fatal(err)
	}
	var binding struct {
		Dispatch struct {
			RunID string `json:"run_id"`
		} `json:"autopilot_dispatch"`
	}
	if json.Unmarshal(initial.Payload, &binding) != nil || binding.Dispatch.RunID != util.UUIDToString(run.ID) {
		t.Fatalf("first input lost its exact dispatch identity: %s", initial.Payload)
	}
	if run.Status != "issue_created" {
		t.Fatalf("expected pre-failure run status issue_created, got %q", run.Status)
	}

	return linkedIssueAutopilotFixture{taskSvc: taskSvc, autopilotSvc: autopilotSvc, queries: queries, run: run, taskID: tasks[0].ID}
}

// runTaskWithBudget exercises the real agent claim and initial input
// delivery. Session executions pin provider identity before a terminal outcome.
func runTaskWithBudget(t *testing.T, taskSvc *service.TaskService, taskID pgtype.UUID, maxAttempts int) []db.IssueAgentEvent {
	t.Helper()
	queries := taskSvc.Queries
	task, err := queries.GetAgentTask(t.Context(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testPool.Exec(t.Context(), `UPDATE agent_task_queue SET max_attempts=$2 WHERE id=$1`, taskID, maxAttempts); err != nil {
		t.Fatal(err)
	}
	claimed, err := taskSvc.ClaimTask(t.Context(), task.AgentID)
	if err != nil || claimed == nil || claimed.ID != taskID {
		t.Fatalf("claim session execution: expected=%s task=%+v error=%v", util.UUIDToString(taskID), claimed, err)
	}
	var inputs []db.IssueAgentEvent
	if service.IsIssueAgentSessionTask(*claimed) {
		inputs, err = taskSvc.ClaimIssueAgentEvents(t.Context(), taskID, 32)
		if err != nil || len(inputs) == 0 {
			t.Fatalf("claim actual session input: events=%+v error=%v", inputs, err)
		}
	}
	if _, err := taskSvc.StartTask(t.Context(), taskID, service.TaskStartCapabilities{Supplement: true}); err != nil {
		t.Fatal(err)
	}
	if service.IsIssueAgentSessionTask(*claimed) {
		if err := taskSvc.PinIssueAgentSession(t.Context(), taskID, "autopilot-session-"+util.UUIDToString(service.IssueAgentSessionID(*claimed)), "/tmp/autopilot-session-"+util.UUIDToString(service.IssueAgentSessionID(*claimed))); err != nil {
			t.Fatal(err)
		}
	}
	return inputs
}

func acknowledgeAutopilotInputs(t *testing.T, f linkedIssueAutopilotFixture, taskID pgtype.UUID, inputs []db.IssueAgentEvent) {
	t.Helper()
	for _, input := range inputs {
		if err := f.taskSvc.AckIssueAgentEvent(t.Context(), taskID, input.ID, true, ""); err != nil {
			t.Fatal(err)
		}
	}
}

// TestAutopilotCreateIssueTaskNoProgressFailureUpdatesRun is the original
// VEN-661 regression: a Codex no-progress failure closes the linked run and
// retains the provider's failure reason.
func TestAutopilotCreateIssueTaskNoProgressFailureUpdatesRun(t *testing.T) {
	ctx := context.Background()
	f := dispatchCreateIssueAutopilot(t, "Create-issue no-progress listener")

	runTaskWithBudget(t, f.taskSvc, f.taskID, 1)

	const errMsg = "codex app-server no progress timeout after 30s"
	if _, err := f.taskSvc.FailTask(ctx, f.taskID, errMsg, "", "", "", "codex_semantic_inactivity", false, "", ""); err != nil {
		t.Fatalf("FailTask: %v", err)
	}

	updatedRun, err := f.queries.GetAutopilotRun(ctx, f.run.ID)
	if err != nil {
		t.Fatalf("GetAutopilotRun: %v", err)
	}
	if updatedRun.Status != "failed" {
		t.Fatalf("expected run status failed, got %q", updatedRun.Status)
	}
	if !updatedRun.FailureReason.Valid || !strings.Contains(updatedRun.FailureReason.String, "no progress timeout") {
		t.Fatalf("expected no-progress failure reason, got %+v", updatedRun.FailureReason)
	}
}

// TestAutopilotCreateIssueTaskAgentErrorFailureUpdatesRun covers the VEN-662
// generalization: an ordinary, non-retryable agent failure must also close the
// linked run instead of leaving it stuck in issue_created.
func TestAutopilotCreateIssueTaskAgentErrorFailureUpdatesRun(t *testing.T) {
	ctx := context.Background()
	f := dispatchCreateIssueAutopilot(t, "Create-issue agent-error listener")

	runTaskWithBudget(t, f.taskSvc, f.taskID, 1)

	// agent_error is not in retryableReasons, so the first terminal failure is
	// final — the run must fail carrying the agent's error text.
	const errMsg = "build failed: ./pkg/foo: undefined: Bar"
	if _, err := f.taskSvc.FailTask(ctx, f.taskID, errMsg, "", "", "", "agent_error", false, "", ""); err != nil {
		t.Fatalf("FailTask: %v", err)
	}

	updatedRun, err := f.queries.GetAutopilotRun(ctx, f.run.ID)
	if err != nil {
		t.Fatalf("GetAutopilotRun: %v", err)
	}
	if updatedRun.Status != "failed" {
		t.Fatalf("expected run status failed, got %q", updatedRun.Status)
	}
	if !updatedRun.FailureReason.Valid || !strings.Contains(updatedRun.FailureReason.String, "build failed") {
		t.Fatalf("expected agent-error failure reason, got %+v", updatedRun.FailureReason)
	}
}

// A timeout fails the current consumer and its unacknowledged input. Session
// recovery needs an explicit retry of that durable input, regardless of budget.
func TestAutopilotCreateIssueFailureBlocksTheEventWithoutAutomaticTaskRetry(t *testing.T) {
	f := dispatchCreateIssueAutopilot(t, "Create-issue explicit retry listener")
	inputs := runTaskWithBudget(t, f.taskSvc, f.taskID, 2)
	if len(inputs) != 1 {
		t.Fatalf("initial assignment input = %+v", inputs)
	}
	if _, err := f.taskSvc.FailTask(t.Context(), f.taskID, "runtime went offline", "", "", "", "timeout", false, "", ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, f, "failed")
	tasks, err := f.queries.ListTasksByIssue(t.Context(), f.run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].Status != "failed" {
		t.Fatalf("timeout created an implicit task retry: tasks=%+v error=%v", tasks, err)
	}
	event, err := f.queries.GetIssueAgentEvent(t.Context(), inputs[0].ID)
	if err != nil || event.Status != "failed" || event.DeliveryTaskID != f.taskID {
		t.Fatalf("failed delivery lost its durable identity: event=%+v error=%v", event, err)
	}
	session, err := f.queries.GetIssueAgentSession(t.Context(), event.SessionID)
	if err != nil || session.State != "blocked" || session.ActiveTaskID.Valid || !session.ProviderSessionID.Valid || !session.WorkDir.Valid {
		t.Fatalf("failed session is not explicitly resumable: session=%+v error=%v", session, err)
	}
}

// setLinkedIssueStatus moves the run's linked issue to status and feeds the
// reloaded row to SyncRunFromIssue, the same entry the issue:updated listener
// reaches after its status gate.
func setLinkedIssueStatus(t *testing.T, f linkedIssueAutopilotFixture, status string) {
	t.Helper()
	ctx := context.Background()
	if _, err := testPool.Exec(ctx, `UPDATE issue SET status = $2 WHERE id = $1`, f.run.IssueID, status); err != nil {
		t.Fatalf("set issue status %q: %v", status, err)
	}
	issue, err := f.queries.GetIssue(ctx, f.run.IssueID)
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	f.autopilotSvc.SyncRunFromIssue(ctx, issue)
}

func requireRunStatus(t *testing.T, f linkedIssueAutopilotFixture, want string) db.AutopilotRun {
	t.Helper()
	run, err := f.queries.GetAutopilotRun(context.Background(), f.run.ID)
	if err != nil {
		t.Fatalf("GetAutopilotRun: %v", err)
	}
	if run.Status != want {
		t.Fatalf("expected run status %q, got %q (reason %+v)", want, run.Status, run.FailureReason)
	}
	return run
}

// TestAutopilotCreateIssueTaskCompletionCompletesRun: the first agent task on
// the linked issue finishing cleanly is run success, and a later blocked
// status on the issue must not flip it to failed. (MUL-8200)
func TestAutopilotCreateIssueTaskCompletionCompletesRun(t *testing.T) {
	ctx := context.Background()
	f := dispatchCreateIssueAutopilot(t, "Create-issue completion listener")

	inputs := runTaskWithBudget(t, f.taskSvc, f.taskID, 1)
	acknowledgeAutopilotInputs(t, f, f.taskID, inputs)
	if _, err := f.taskSvc.CompleteTask(ctx, f.taskID, []byte(`{"output":"done"}`), "", "", "", false, "", ""); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	requireRunStatus(t, f, "completed")

	setLinkedIssueStatus(t, f, "blocked")
	requireRunStatus(t, f, "completed")
	setLinkedIssueStatus(t, f, "cancelled")
	requireRunStatus(t, f, "completed")
}

// TestAutopilotCreateIssueBlockedIssueKeepsRunOpen is the MUL-8198 shape: a
// reviewer blocks the issue while the autopilot's task is still running. The
// run must stay open and complete once the task finishes. (MUL-8200)
func TestAutopilotCreateIssueBlockedIssueKeepsRunOpen(t *testing.T) {
	ctx := context.Background()
	f := dispatchCreateIssueAutopilot(t, "Create-issue blocked listener")

	inputs := runTaskWithBudget(t, f.taskSvc, f.taskID, 1)
	acknowledgeAutopilotInputs(t, f, f.taskID, inputs)
	setLinkedIssueStatus(t, f, "blocked")
	requireRunStatus(t, f, "issue_created")

	if _, err := f.taskSvc.CompleteTask(ctx, f.taskID, []byte(`{"output":"done"}`), "", "", "", false, "", ""); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	requireRunStatus(t, f, "completed")
}

// TestAutopilotCreateIssueCancelledBeforeTaskFailsRun: cancelling the issue
// before any task finishes still fails the run, so it does not hang. (MUL-8200)
func TestAutopilotCreateIssueCancelledBeforeTaskFailsRun(t *testing.T) {
	f := dispatchCreateIssueAutopilot(t, "Create-issue cancelled listener")

	setLinkedIssueStatus(t, f, "cancelled")
	run := requireRunStatus(t, f, "failed")
	if !run.FailureReason.Valid || run.FailureReason.String != "issue cancelled" {
		t.Fatalf("expected failure reason %q, got %+v", "issue cancelled", run.FailureReason)
	}
}

// TestAutopilotCreateIssueEarlyDeliveryWaitsForTaskOutcome: an issue moved to
// in_review/done while the dispatch task still runs must not complete the run;
// the task's failure decides it. (MUL-8200 review)
func TestAutopilotCreateIssueEarlyDeliveryWaitsForTaskOutcome(t *testing.T) {
	for _, status := range []string{"in_review", "done"} {
		t.Run(status, func(t *testing.T) {
			ctx := context.Background()
			f := dispatchCreateIssueAutopilot(t, "Create-issue early delivery "+status)
			runTaskWithBudget(t, f.taskSvc, f.taskID, 1)
			setLinkedIssueStatus(t, f, status)
			before, err := f.queries.GetAutopilotRun(ctx, f.run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if before.Status != "issue_created" {
				t.Errorf("issue became %s while first task is still running: run=%s, want issue_created", status, before.Status)
			}
			if _, err := f.taskSvc.FailTask(ctx, f.taskID, "runtime exited after setting issue status", "", "", "", "agent_error", false, "", ""); err != nil {
				t.Fatal(err)
			}
			requireRunStatus(t, f, "failed")
		})
	}
}

// Explicit assistance on the same issue has its own session, but cannot
// settle an Autopilot whose initial input belongs to another consumer.
func TestAutopilotCreateIssueUnrelatedSessionDoesNotSettleRun(t *testing.T) {
	ctx := t.Context()
	f := dispatchCreateIssueAutopilot(t, "Create-issue unrelated session")
	runTaskWithBudget(t, f.taskSvc, f.taskID, 1)
	first, err := f.queries.GetAgentTask(ctx, f.taskID)
	if err != nil {
		t.Fatal(err)
	}
	fixture := dbfx.New(testPool, testWorkspaceID, testUserID)
	otherAgent := fixture.Agent(t, "Explicit auxiliary agent", util.UUIDToString(first.RuntimeID))
	other := explicitAutopilotAuxiliary(t, f.taskSvc, f.run.IssueID, parseUUID(otherAgent))
	inputs := runTaskWithBudget(t, f.taskSvc, other.ID, 1)
	acknowledgeAutopilotInputs(t, f, other.ID, inputs)
	if _, err := f.taskSvc.CompleteTask(ctx, other.ID, []byte(`{"output":"Auxiliary answer"}`), "", "", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, f, "issue_created")
	if _, err := f.taskSvc.FailTask(ctx, f.taskID, "original autopilot task crashed", "", "", "", "agent_error", false, "", ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, f, "failed")
}

func explicitAutopilotAuxiliary(t *testing.T, taskSvc *service.TaskService, issueID, agentID pgtype.UUID) db.AgentTaskQueue {
	t.Helper()
	svc := &service.IssueWakeupService{Tasks: taskSvc}
	rule, err := svc.Create(t.Context(), issueID, parseUUID(testUserID), pgtype.UUID{}, service.WakeupInput{
		AgentID: util.UUIDToString(agentID), Kind: "event", EventTypes: []string{"task.completed"}, Instruction: "Provide the explicitly requested auxiliary review",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), "DELETE FROM issue_wakeup_receipt WHERE wakeup_id=$1", rule.ID)
		_, _ = testPool.Exec(context.Background(), "DELETE FROM issue_wakeup WHERE id=$1", rule.ID)
	})
	if err := svc.Trigger(t.Context(), issueID, rule.ID, parseUUID(testUserID)); err != nil {
		t.Fatal(err)
	}
	session, err := taskSvc.Queries.GetIssueAgentSessionForPair(t.Context(), db.GetIssueAgentSessionForPairParams{WorkspaceID: parseUUID(testWorkspaceID), IssueID: issueID, AgentID: agentID})
	if err != nil || !session.ActiveTaskID.Valid {
		t.Fatalf("explicit auxiliary input did not open its session: session=%+v error=%v", session, err)
	}
	task, err := taskSvc.Queries.GetAgentTask(t.Context(), session.ActiveTaskID)
	if err != nil || !service.IsIssueAgentSessionTask(task) {
		t.Fatalf("auxiliary input used the legacy task route: task=%+v error=%v", task, err)
	}
	return task
}

func TestAutopilotCreateIssueExplicitEventRetryResumesWithoutRewritingFailedRun(t *testing.T) {
	f := dispatchCreateIssueAutopilot(t, "Create-issue same event recovery")
	inputs := runTaskWithBudget(t, f.taskSvc, f.taskID, 2)
	if _, err := f.taskSvc.FailTask(t.Context(), f.taskID, "runtime went offline", "", "", "", "timeout", false, "", ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, f, "failed")
	before, err := f.queries.GetIssueAgentSession(t.Context(), inputs[0].SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.taskSvc.RetryIssueAgentEvent(t.Context(), inputs[0].SessionID, inputs[0].ID); err != nil {
		t.Fatalf("retry the same durable input: %v", err)
	}
	session, err := f.queries.GetIssueAgentSession(t.Context(), inputs[0].SessionID)
	if err != nil || !session.ActiveTaskID.Valid || session.ActiveTaskID == f.taskID {
		t.Fatalf("explicit retry did not resume the session: %+v error=%v", session, err)
	}
	retry, err := f.queries.GetAgentTask(t.Context(), session.ActiveTaskID)
	if err != nil || service.IssueAgentSessionID(retry) != inputs[0].SessionID || retry.RetryOfTaskID.Valid || retry.AutopilotRunID.Valid || session.ProviderSessionID != before.ProviderSessionID || session.WorkDir != before.WorkDir {
		t.Fatalf("explicit event retry changed conversation or used legacy task retry: %+v error=%v", retry, err)
	}
	replayed := runTaskWithBudget(t, f.taskSvc, retry.ID, 1)
	if len(replayed) != 1 || replayed[0].ID != inputs[0].ID || replayed[0].Sequence != inputs[0].Sequence {
		t.Fatalf("retry replaced or reordered the original input: %+v", replayed)
	}
	acknowledgeAutopilotInputs(t, f, retry.ID, replayed)
	if _, err := f.taskSvc.CompleteTask(t.Context(), retry.ID, []byte(`{"output":"recovered"}`), "", "", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	run := requireRunStatus(t, f, "failed")
	if run.TaskID != f.taskID {
		t.Fatalf("later session execution rewrote the Autopilot's dispatch anchor: %+v", run)
	}
	event, err := f.queries.GetIssueAgentEvent(t.Context(), inputs[0].ID)
	if err != nil || event.Status != "delivered" || event.DeliveryTaskID != retry.ID {
		t.Fatalf("explicit retry did not deliver the original input: %+v error=%v", event, err)
	}
}

// TestAutopilotCreateIssueDeliveryAfterCommittedFailureFailsRun: the dispatch
// failure is committed but task:failed not yet processed when the issue moves
// to in_review; the committed failure must win. (MUL-8200 re-review)
func TestAutopilotCreateIssueDeliveryAfterCommittedFailureFailsRun(t *testing.T) {
	ctx := context.Background()
	f := dispatchCreateIssueAutopilot(t, "Delivery racing failure notification")
	runTaskWithBudget(t, f.taskSvc, f.taskID, 1)
	observed := false
	// Put the observer before the Autopilot listener on the task event bus.
	// Session failures publish no implicit comment; this is the committed
	// failure / not-yet-handled task:failed boundary.
	f.taskSvc.Bus = events.New()
	f.taskSvc.Bus.Subscribe(protocol.EventTaskFailed, func(e events.Event) {
		task, err := f.queries.GetAgentTask(ctx, f.taskID)
		if err != nil {
			t.Fatal(err)
		}
		if task.Status != "failed" {
			t.Fatalf("expected a committed failure, got %s", task.Status)
		}
		observed = true
		setLinkedIssueStatus(t, f, "in_review")
	})
	registerAutopilotListeners(f.taskSvc.Bus, f.autopilotSvc)
	if _, err := f.taskSvc.FailTask(ctx, f.taskID, "dispatch task crashed", "", "", "", "agent_error", false, "", ""); err != nil {
		t.Fatal(err)
	}
	if !observed {
		t.Fatal("did not exercise the post-commit/pre-task-event window")
	}
	requireRunStatus(t, f, "failed")
}

// An explicitly requested auxiliary consumer can precede the Autopilot's
// first consumer; its earlier timestamp never makes it the dispatch anchor.
func TestAutopilotCreateIssueAuxiliaryConsumerBeforeDispatch(t *testing.T) {
	ctx := context.Background()
	fixture := dbfx.New(testPool, testWorkspaceID, testUserID)
	runtimeID := fixture.Runtime(t, "Dispatch order runtime")
	ownerID := fixture.Agent(t, "Autopilot assignee", runtimeID)
	otherID := fixture.Agent(t, "Early mention agent", runtimeID)
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	apSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerAutopilotListeners(bus, apSvc)
	ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID: parseUUID(testWorkspaceID), Title: "Dispatch order autopilot",
		AssigneeType: "agent", AssigneeID: parseUUID(ownerID), Status: "active",
		ExecutionMode: "create_issue", IssueTitleTemplate: pgtype.Text{String: "Dispatch order issue", Valid: true},
		CreatedByType: "member", CreatedByID: parseUUID(testUserID),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.Cleanup(t, "DELETE FROM autopilot WHERE id = $1", ap.ID)
	var auxiliary db.AgentTaskQueue
	// The issue and bound input are committed at issue:created, before wake.
	bus.Subscribe(protocol.EventIssueCreated, func(e events.Event) {
		var issueID pgtype.UUID
		fixture.QueryRow(t, "SELECT id FROM issue WHERE origin_id = $1", ap.ID).Scan(&issueID)
		fixture.Cleanup(t, "DELETE FROM issue WHERE id = $1", issueID)
		fixture.Cleanup(t, "DELETE FROM comment WHERE issue_id = $1", issueID)
		fixture.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id = $1", issueID)
		auxiliary = explicitAutopilotAuxiliary(t, taskSvc, issueID, parseUUID(otherID))
	})
	run, err := apSvc.DispatchAutopilot(ctx, ap, seedAutopilotScheduleTrigger(t, queries, ap), "schedule", nil)
	if err != nil {
		t.Fatal(err)
	}
	var dispatchedID pgtype.UUID
	fixture.QueryRow(t, "SELECT id FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2", run.IssueID, ownerID).Scan(&dispatchedID)
	f := linkedIssueAutopilotFixture{taskSvc: taskSvc, autopilotSvc: apSvc, queries: queries, run: run, taskID: dispatchedID}
	runTaskWithBudget(t, taskSvc, dispatchedID, 1)
	inputs := runTaskWithBudget(t, taskSvc, auxiliary.ID, 1)
	acknowledgeAutopilotInputs(t, f, auxiliary.ID, inputs)
	if _, err := taskSvc.CompleteTask(ctx, auxiliary.ID, []byte(`{"output":"Quick answer"}`), "", "", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	state, err := queries.GetAutopilotRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "issue_created" {
		t.Errorf("early auxiliary session completed the run: got %s, want issue_created", state.Status)
	}
	if _, err := taskSvc.FailTask(ctx, dispatchedID, "real dispatch failed", "", "", "", "agent_error", false, "", ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, f, "failed")
}

// The dispatch link commits with the first consumer, so even an observer of
// task:queued sees the exact anchor before a status update can race it.
func TestAutopilotCreateIssueConsumerIsLinkedBeforeItIsAnnounced(t *testing.T) {
	ctx := context.Background()
	fixture := dbfx.New(testPool, testWorkspaceID, testUserID)
	runtimeID := fixture.Runtime(t, "Link window runtime")
	agentID := fixture.Agent(t, "Link window assignee", runtimeID)
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	apSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerAutopilotListeners(bus, apSvc)
	ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID: parseUUID(testWorkspaceID), Title: "Link window autopilot",
		AssigneeType: "agent", AssigneeID: parseUUID(agentID), Status: "active",
		ExecutionMode: "create_issue", IssueTitleTemplate: pgtype.Text{String: "Link window issue", Valid: true},
		CreatedByType: "member", CreatedByID: parseUUID(testUserID),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.Cleanup(t, "DELETE FROM autopilot WHERE id = $1", ap.ID)
	var f linkedIssueAutopilotFixture
	// Drive a real status update from the first post-commit notification.
	bus.Subscribe(protocol.EventTaskQueued, func(e events.Event) {
		taskID := parseUUID(e.Payload.(map[string]any)["task_id"].(string))
		task, err := queries.GetAgentTask(ctx, taskID)
		if err != nil {
			t.Fatal(err)
		}
		run, err := queries.GetAutopilotRunByIssue(ctx, task.IssueID)
		if err != nil {
			t.Fatal(err)
		}
		if run.TaskID != task.ID || task.AutopilotRunID.Valid || !service.IsIssueAgentSessionTask(task) {
			t.Fatalf("consumer was announced before its exact Autopilot link committed: run=%+v task=%+v", run, task)
		}
		fixture.Cleanup(t, "DELETE FROM issue WHERE id = $1", task.IssueID)
		fixture.Cleanup(t, "DELETE FROM comment WHERE issue_id = $1", task.IssueID)
		fixture.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id = $1", task.IssueID)
		f = linkedIssueAutopilotFixture{taskSvc: taskSvc, autopilotSvc: apSvc, queries: queries, run: &run, taskID: taskID}
		runTaskWithBudget(t, taskSvc, taskID, 1)
		setLinkedIssueStatus(t, f, "in_review")
	})
	if _, err := apSvc.DispatchAutopilot(ctx, ap, seedAutopilotScheduleTrigger(t, queries, ap), "schedule", nil); err != nil {
		t.Fatal(err)
	}
	if f.run == nil {
		t.Fatal("queued event was not observed")
	}
	if _, err := taskSvc.FailTask(ctx, f.taskID, "failed after early delivery", "", "", "", "agent_error", false, "", ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, f, "failed")
}

// TestAutopilotCreateIssueWebhookRecoveryLinksDispatch: a webhook-recovered
// dispatch recovers both before its consumer exists and after its atomic
// link commits, then settles from the exact dispatch consumer's outcome.
func TestAutopilotCreateIssueWebhookRecoveryLinksDispatch(t *testing.T) {
	for _, taskWasEnqueued := range []bool{false, true} {
		name := "before_session_consumer"
		if taskWasEnqueued {
			name = "after_atomically_linked_consumer"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			f := dispatchCreateIssueAutopilot(t, "Webhook recovery "+name)
			fixture := dbfx.New(testPool, testWorkspaceID, testUserID)
			// The issue plus dispatch input can commit before the consumer is
			// created. Once a consumer exists, its run link is atomic.
			fixture.Exec(t, "UPDATE autopilot_run SET source='webhook',webhook_delivery_id=id WHERE id=$1", f.run.ID)
			if !taskWasEnqueued {
				fixture.Exec(t, "UPDATE autopilot_run SET task_id=NULL WHERE id=$1", f.run.ID)
				fixture.Exec(t, "UPDATE issue_agent_session SET active_task_id=NULL WHERE active_task_id=$1", f.taskID)
				fixture.Exec(t, "DELETE FROM agent_task_queue WHERE id=$1", f.taskID)
			}
			ap, err := f.queries.GetAutopilot(ctx, f.run.AutopilotID)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.autopilotSvc.DispatchAutopilotForWebhookDelivery(ctx, ap, f.run.TriggerID, nil, f.run.ID); err != nil {
				t.Fatal(err)
			}
			tasks, err := f.queries.ListTasksByIssue(ctx, f.run.IssueID)
			if err != nil {
				t.Fatal(err)
			}
			if len(tasks) != 1 || !service.IsIssueAgentSessionTask(tasks[0]) {
				t.Fatalf("recovery produced tasks=%+v, want one session consumer", tasks)
			}
			repaired, err := f.queries.GetAutopilotRun(ctx, f.run.ID)
			if err != nil || repaired.TaskID != tasks[0].ID || taskWasEnqueued && repaired.TaskID != f.taskID {
				t.Fatalf("recovery did not retain its exact dispatch anchor: %+v error=%v", repaired, err)
			}
			runTaskWithBudget(t, f.taskSvc, tasks[0].ID, 1)
			if _, err := f.taskSvc.FailTask(ctx, tasks[0].ID, "recovered dispatch failed", "", "", "", "agent_error", false, "", ""); err != nil {
				t.Fatal(err)
			}
			requireRunStatus(t, f, "failed")
		})
	}
}

// TestAutopilotCreateIssuePreUpgradeRunObservesTaskFailure: an active run
// dispatched before task_id was recorded still fails from its dispatch task.
// (MUL-8200 third review)
func TestAutopilotCreateIssuePreUpgradeRunObservesTaskFailure(t *testing.T) {
	ctx := context.Background()
	f := dispatchCreateIssueAutopilot(t, "Pre-upgrade running autopilot")
	fixture := dbfx.New(testPool, testWorkspaceID, testUserID)
	fixture.Exec(t, "UPDATE autopilot_run SET task_id=NULL WHERE id=$1", f.run.ID)
	// A real pre-upgrade dispatch has no bound session dispatch metadata and
	// runs as an ordinary historical task. Only read-side recognition remains.
	fixture.Exec(t, "UPDATE issue_agent_event SET payload=payload-'autopilot_dispatch',status='ignored',failure_reason='historical_fixture' WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=$1)", f.run.IssueID)
	fixture.Exec(t, "UPDATE issue_agent_session SET active_task_id=NULL WHERE active_task_id=$1", f.taskID)
	fixture.Exec(t, "UPDATE agent_task_queue SET context='{}' WHERE id=$1", f.taskID)
	runTaskWithBudget(t, f.taskSvc, f.taskID, 1)
	if _, err := f.taskSvc.FailTask(ctx, f.taskID, "pre-upgrade dispatch failed", "", "", "", "agent_error", false, "", ""); err != nil {
		t.Fatal(err)
	}
	requireRunStatus(t, f, "failed")
}

// TestAutopilotCreateIssueDeliveryWithoutDispatchTask: with no dispatch task
// yet, in_review waits while the issue is agent-assigned (enqueue pending) and
// completes the run once the issue belongs to a member. (MUL-8200)
func TestAutopilotCreateIssueDeliveryWithoutDispatchTask(t *testing.T) {
	f := dispatchCreateIssueAutopilot(t, "Create-issue delivery without dispatch task")
	fixture := dbfx.New(testPool, testWorkspaceID, testUserID)
	fixture.Exec(t, "UPDATE autopilot_run SET task_id = NULL WHERE id = $1", f.run.ID)
	fixture.Exec(t, "UPDATE issue_agent_session SET active_task_id=NULL WHERE active_task_id=$1", f.taskID)
	fixture.Exec(t, "DELETE FROM agent_task_queue WHERE id = $1", f.taskID)

	setLinkedIssueStatus(t, f, "in_review")
	requireRunStatus(t, f, "issue_created")

	fixture.Exec(t, "UPDATE issue SET assignee_type = 'member', assignee_id = $2 WHERE id = $1", f.run.IssueID, parseUUID(testUserID))
	setLinkedIssueStatus(t, f, "done")
	requireRunStatus(t, f, "completed")
}

// TestAutopilotDispatchSkipsWhenRuntimeOffline locks in the MUL-1899
// admission gate: when the assignee agent's runtime is not online we must
// record a `skipped` autopilot_run with a failure_reason and NOT enqueue an
// agent_task_queue row. This is the fix for "活跃 schedule 持续给离线 local
// agent 入队".
func TestAutopilotDispatchSkipsWhenRuntimeOffline(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)

	// Spin up a dedicated runtime + agent so we can flip the runtime to
	// offline without affecting the shared fixture used by other tests.
	var runtimeID, agentID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_runtime (
			workspace_id, daemon_id, name, runtime_mode, provider, status, device_info, metadata, last_seen_at
		)
		VALUES ($1, NULL, 'Offline runtime', 'local', 'mul1899_offline_runtime', 'offline', '{}'::jsonb, '{}'::jsonb, now())
		RETURNING id::text
	`, parseUUID(testWorkspaceID)).Scan(&runtimeID); err != nil {
		t.Fatalf("create offline runtime: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_runtime WHERE id = $1`, runtimeID)
	})

	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent (
			workspace_id, name, description, runtime_mode, runtime_config,
			runtime_id, visibility, max_concurrent_tasks, owner_id
		)
		VALUES ($1, 'mul1899-offline-agent', '', 'local', '{}'::jsonb, $2, 'workspace', 1, $3)
		RETURNING id::text
	`, parseUUID(testWorkspaceID), runtimeID, parseUUID(testUserID)).Scan(&agentID); err != nil {
		t.Fatalf("create offline agent: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, agentID)
	})

	ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Offline-runtime autopilot",
		Description:        pgtype.Text{String: "MUL-1899 admission test", Valid: true},
		AssigneeType:       "agent",
		AssigneeID:         parseUUID(agentID),
		Status:             "active",
		ExecutionMode:      "run_only",
		IssueTitleTemplate: pgtype.Text{},
		CreatedByType:      "member",
		CreatedByID:        parseUUID(testUserID),
	})
	if err != nil {
		t.Fatalf("CreateAutopilot: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	run, err := autopilotSvc.DispatchAutopilot(ctx, ap, seedAutopilotScheduleTrigger(t, queries, ap), "schedule", nil)
	if err != nil {
		t.Fatalf("DispatchAutopilot: %v", err)
	}
	if run == nil {
		t.Fatal("expected a run, got nil")
	}
	if run.Status != "skipped" {
		t.Fatalf("expected run status 'skipped', got %q", run.Status)
	}
	if !run.FailureReason.Valid || !strings.Contains(run.FailureReason.String, "offline") {
		t.Fatalf("expected failure reason mentioning 'offline', got %+v", run.FailureReason)
	}
	if run.TaskID.Valid {
		t.Fatalf("expected no task to be enqueued, got task_id %v", run.TaskID)
	}

	// Defensive: confirm at the DB layer that nothing landed on the queue.
	var taskCount int
	if err := testPool.QueryRow(ctx,
		`SELECT count(*) FROM agent_task_queue WHERE agent_id = $1`,
		agentID,
	).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCount != 0 {
		t.Fatalf("expected 0 queued tasks for offline-runtime agent, got %d", taskCount)
	}
}

// TestAutopilotCreateIssueDispatchCreatesIssueWhenRuntimeOffline locks in the
// audit-trail contract for tracked autopilots: create_issue mode must still
// create a visible issue when the assignee runtime is offline, leaving the
// issue/task to be claimed when the runtime comes back instead of silently
// recording an unrecoverable skipped run.
func TestAutopilotCreateIssueDispatchCreatesIssueWhenRuntimeOffline(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)

	var runtimeID, agentID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_runtime (
			workspace_id, daemon_id, name, runtime_mode, provider, status, device_info, metadata, last_seen_at
		)
		VALUES ($1, NULL, 'Offline create-issue runtime', 'local', 'ws1325_offline_runtime', 'offline', '{}'::jsonb, '{}'::jsonb, now())
		RETURNING id::text
	`, parseUUID(testWorkspaceID)).Scan(&runtimeID); err != nil {
		t.Fatalf("create offline runtime: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_runtime WHERE id = $1`, runtimeID)
	})

	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent (
			workspace_id, name, description, runtime_mode, runtime_config,
			runtime_id, visibility, max_concurrent_tasks, owner_id
		)
		VALUES ($1, 'ws1325-offline-create-issue-agent', '', 'local', '{}'::jsonb, $2, 'workspace', 1, $3)
		RETURNING id::text
	`, parseUUID(testWorkspaceID), runtimeID, parseUUID(testUserID)).Scan(&agentID); err != nil {
		t.Fatalf("create offline agent: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, agentID)
	})

	ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Offline create-issue autopilot",
		Description:        pgtype.Text{String: "WS-1325 regression test", Valid: true},
		AssigneeType:       "agent",
		AssigneeID:         parseUUID(agentID),
		Status:             "active",
		ExecutionMode:      "create_issue",
		IssueTitleTemplate: pgtype.Text{String: "Tracked issue", Valid: true},
		CreatedByType:      "member",
		CreatedByID:        parseUUID(testUserID),
	})
	if err != nil {
		t.Fatalf("CreateAutopilot: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	run, err := autopilotSvc.DispatchAutopilot(ctx, ap, seedAutopilotScheduleTrigger(t, queries, ap), "schedule", nil)
	if err != nil {
		t.Fatalf("DispatchAutopilot: %v", err)
	}
	if run == nil {
		t.Fatal("expected a run, got nil")
	}
	if run.Status != "issue_created" {
		t.Fatalf("expected run status 'issue_created', got %q", run.Status)
	}
	if !run.IssueID.Valid {
		t.Fatal("create_issue dispatch did not link an issue")
	}
	if run.FailureReason.Valid {
		t.Fatalf("expected no failure reason, got %q", run.FailureReason.String)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, run.IssueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, run.IssueID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, run.IssueID)
	})

	tasks, err := queries.ListTasksByIssue(ctx, run.IssueID)
	if err != nil {
		t.Fatalf("ListTasksByIssue: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected one queued issue task, got %d", len(tasks))
	}
	if tasks[0].AgentID != parseUUID(agentID) {
		t.Fatalf("task agent mismatch: got %v want %v", tasks[0].AgentID, parseUUID(agentID))
	}
	if tasks[0].RuntimeID != parseUUID(runtimeID) {
		t.Fatalf("task runtime mismatch: got %v want %v", tasks[0].RuntimeID, parseUUID(runtimeID))
	}
}

// TestManualTriggerDoesNotErrorOnPostAdmissionSkip locks in PR #2888 review
// fix #2: if the dispatcher decides to skip after the admission gate has
// already passed (e.g. the leader's runtime went offline between admission
// and task creation), DispatchAutopilot must return (run, nil) with
// status='skipped' rather than (nil, err). Without this, manual trigger
// surfaces a 500 to the user even though the work was correctly suppressed
// — the same regression Emacs flagged on the original PR.
//
// We synthesise the race by:
//  1. Creating an online runtime + agent so the admission gate passes.
//  2. Flipping the runtime to offline.
//  3. Triggering the autopilot. Admission has already loaded the agent +
//     runtime once with status='online' at row-fetch time, so the second
//     check inside dispatchRunOnly is what catches the offline state.
//
// In this implementation the admission gate also re-reads the runtime, so
// the same offline state actually fires the admission skip first. That is
// fine for the assertion we care about: the manual trigger must not 500 and
// the run must be `skipped`. The post-admission branch is exercised
// separately by the errDispatchSkipped unwrap unit test in the service
// package.
func TestManualTriggerDoesNotErrorOnPostAdmissionSkip(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)

	var runtimeID, agentID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_runtime (
			workspace_id, daemon_id, name, runtime_mode, provider, status, device_info, metadata, last_seen_at
		)
		VALUES ($1, NULL, 'Manual-trigger skip runtime', 'local', 'mul2429_manual_skip_runtime', 'offline', '{}'::jsonb, '{}'::jsonb, now())
		RETURNING id::text
	`, parseUUID(testWorkspaceID)).Scan(&runtimeID); err != nil {
		t.Fatalf("create runtime: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_runtime WHERE id = $1`, runtimeID)
	})

	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent (
			workspace_id, name, description, runtime_mode, runtime_config,
			runtime_id, visibility, max_concurrent_tasks, owner_id
		)
		VALUES ($1, 'mul2429-manual-skip-agent', '', 'local', '{}'::jsonb, $2, 'workspace', 1, $3)
		RETURNING id::text
	`, parseUUID(testWorkspaceID), runtimeID, parseUUID(testUserID)).Scan(&agentID); err != nil {
		t.Fatalf("create agent: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, agentID)
	})

	ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Manual-trigger skip autopilot",
		Description:        pgtype.Text{String: "PR #2888 review fix #2", Valid: true},
		AssigneeType:       "agent",
		AssigneeID:         parseUUID(agentID),
		Status:             "active",
		ExecutionMode:      "run_only",
		IssueTitleTemplate: pgtype.Text{},
		CreatedByType:      "member",
		CreatedByID:        parseUUID(testUserID),
	})
	if err != nil {
		t.Fatalf("CreateAutopilot: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	run, err := autopilotSvc.DispatchAutopilot(ctx, ap, seedAutopilotScheduleTrigger(t, queries, ap), "manual", nil)
	if err != nil {
		t.Fatalf("manual DispatchAutopilot returned error (would 500 the handler): %v", err)
	}
	if run == nil {
		t.Fatal("expected a run, got nil")
	}
	if run.Status != "skipped" {
		t.Fatalf("expected run status 'skipped', got %q", run.Status)
	}
}
