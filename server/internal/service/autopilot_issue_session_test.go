package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/attribution"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

type autopilotIssueSessionFixture struct {
	principalFixture
	agentID, runtimeID, triggerOwner string
	autopilot                        db.Autopilot
	triggerID                        pgtype.UUID
}

func newAutopilotIssueSessionFixture(t *testing.T) autopilotIssueSessionFixture {
	t.Helper()
	pool := newResolveOriginatorPool(t)
	fx := testutil.New(pool, "", "")
	suffix := util.UUIDToString(dbid.NewV7())
	fx.UserID = fx.User(t, "autopilot session owner", "autopilot-session-owner-"+suffix+"@multica.test")
	fx.WorkspaceID = fx.Workspace(t, "autopilot sessions", "autopilot-session-"+suffix)
	fx.Member(t, fx.WorkspaceID, fx.UserID, "owner")
	q := db.New(pool)
	bus := events.New()
	f := principalFixture{Fixture: fx, q: q, svc: &AutopilotService{Queries: q, TxStarter: pool, Bus: bus, TaskSvc: NewTaskService(q, pool, nil, bus)}}
	creator := fx.UserID
	f.Exec(t, "UPDATE workspace SET attribution_fail_closed=false WHERE id=$1", f.WorkspaceID)
	if err := f.q.SeedIssueStatusEntries(t.Context(), parseTestUUID(t, f.WorkspaceID)); err != nil {
		t.Fatal(err)
	}
	runtimeID := f.Runtime(t, "Autopilot session runtime", testutil.Cols{"visibility": "public", "status": "online"})
	agentID := f.Agent(t, "Autopilot shared assignee", runtimeID, testutil.Cols{"permission_mode": "public_to", "visibility": "workspace", "max_concurrent_tasks": 2})
	f.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agentID, "target_type": "workspace", "target_id": f.WorkspaceID})
	triggerOwner := f.member(t, "autopilot-session-trigger-owner-"+f.WorkspaceID)
	apID, triggerID := f.autopilotWithTrigger(t, agentID, creator, triggerOwner)
	f.Exec(t, "UPDATE autopilot SET execution_mode='create_issue',issue_title_template=$2 WHERE id=$1", apID, "Persistent autopilot issue "+apID)
	ap, err := f.q.GetAutopilot(t.Context(), parseTestUUID(t, apID))
	if err != nil {
		t.Fatal(err)
	}
	if err := RecordAutopilotRuleVersion(t.Context(), f.q, ap, "member", parseTestUUID(t, creator)); err != nil {
		t.Fatal(err)
	}
	f.svc.TaskSvc.IssueSessionCanInvoke = func(ctx context.Context, agent db.Agent, event db.IssueAgentEvent) bool {
		principal := issueAgentEventPrincipal(event)
		return principal.Valid && CanMemberInvokeAgent(ctx, f.q, agent, principal, agent.WorkspaceID)
	}
	f.Cleanup(t, "DELETE FROM autopilot_rule_version WHERE autopilot_id=$1", ap.ID)
	f.Cleanup(t, "DELETE FROM issue WHERE origin_type='autopilot' AND origin_id=$1", ap.ID)
	f.Cleanup(t, "DELETE FROM autopilot_run WHERE autopilot_id=$1", ap.ID)
	f.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id IN (SELECT id FROM issue WHERE origin_type='autopilot' AND origin_id=$1)", ap.ID)
	return autopilotIssueSessionFixture{principalFixture: f, agentID: agentID, runtimeID: runtimeID, triggerOwner: triggerOwner, autopilot: ap, triggerID: parseTestUUID(t, triggerID)}
}

func (f autopilotIssueSessionFixture) dispatchIssue(t *testing.T) *db.AutopilotRun {
	t.Helper()
	run, err := f.svc.DispatchAutopilot(t.Context(), f.autopilot, f.triggerID, "schedule", nil)
	if err != nil || run == nil || !run.IssueID.Valid {
		t.Fatalf("real DispatchAutopilot did not create its issue: run=%+v err=%v", run, err)
	}
	return run
}

func TestAutopilotCreateIssueUsesPersistentSessionExecution(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	run := f.dispatchIssue(t)
	tasks, err := f.q.ListTasksByIssue(t.Context(), run.IssueID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("autopilot issue tasks=%+v err=%v", tasks, err)
	}
	if !IsIssueAgentSessionTask(tasks[0]) {
		t.Fatalf("Autopilot create_issue bypassed persistent session with ordinary queued task %s context=%s", util.UUIDToString(tasks[0].ID), tasks[0].Context)
	}
	if tasks[0].AutopilotRunID.Valid {
		t.Fatal("issue session execution was classified as a run_only autopilot task")
	}
}

func TestAutopilotIssueSessionRejectsDepartedMemberWhoStillOwnsPrivateAgent(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	ctx := t.Context()
	f.Exec(t, "UPDATE agent SET owner_id=$2,permission_mode='private',visibility='private' WHERE id=$1", f.agentID, f.triggerOwner)
	run := f.dispatchIssue(t)
	task, err := f.svc.TaskSvc.ClaimTaskForRuntime(ctx, parseTestUUID(t, f.runtimeID))
	if err != nil || task == nil || task.IssueID != run.IssueID || !IsIssueAgentSessionTask(*task) {
		t.Fatalf("initial admitted claim: task=%+v err=%v", task, err)
	}
	f.Exec(t, "DELETE FROM member WHERE workspace_id=$1 AND user_id=$2", f.WorkspaceID, f.triggerOwner)
	inputs, err := f.svc.TaskSvc.ClaimIssueAgentEvents(ctx, task.ID, 32)
	if err != nil || len(inputs) != 0 {
		t.Fatalf("departed agent owner borrowed private-owner shortcut: inputs=%+v err=%v", inputs, err)
	}
	if count := f.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND status='ignored' AND failure_reason='invocation_not_allowed'", IssueAgentSessionID(*task)); count != 1 {
		t.Fatalf("revoked autopilot input did not retain its denial receipt: count=%d", count)
	}
}

func TestAutopilotIssueSessionsUseBothClaimPathsAndContinueSameConversation(t *testing.T) {
	for _, source := range []string{"schedule", "manual"} {
		for _, singleFirst := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/single_first=%v", source, singleFirst), func(t *testing.T) {
				f := newAutopilotIssueSessionFixture(t)
				ctx := t.Context()
				principal, expectedSource := f.triggerOwner, attribution.SourceTriggerOwner
				var run *db.AutopilotRun
				if source == "schedule" {
					run = f.dispatchIssue(t)
				} else {
					principal = f.member(t, "autopilot-session-manual-actor-"+f.WorkspaceID)
					expectedSource = attribution.SourceDirectHuman
					var err error
					run, _, err = f.svc.DispatchAutopilotManual(ctx, f.autopilot, f.triggerID, nil, parseTestUUID(t, principal))
					if err != nil || run == nil || !run.IssueID.Valid {
						t.Fatalf("manual create_issue dispatch: run=%+v err=%v", run, err)
					}
				}
				initialTasks, err := f.q.ListTasksByIssue(ctx, run.IssueID)
				if err != nil || len(initialTasks) != 1 || !IsIssueAgentSessionTask(initialTasks[0]) {
					t.Fatalf("autopilot bypassed the shared consumer: tasks=%+v err=%v", initialTasks, err)
				}
				sessionID := IssueAgentSessionID(initialTasks[0])
				if initialTasks[0].AutopilotRunID.Valid || initialTasks[0].OriginatorSource.String != string(expectedSource) || initialTasks[0].OriginatorUserID != parseTestUUID(t, principal) || initialTasks[0].AccountableUserID != parseTestUUID(t, principal) {
					t.Fatalf("autopilot session lost dispatch principal: task=%+v want=%s/%s", initialTasks[0], expectedSource, principal)
				}
				if run.TaskID != initialTasks[0].ID {
					t.Fatalf("run.task_id=%s does not link its session execution %s", util.UUIDToString(run.TaskID), util.UUIDToString(initialTasks[0].ID))
				}
				ordinaryIssue := f.Issue(t, "ordinary issue on same runtime", testutil.Cols{"assignee_type": "agent", "assignee_id": f.agentID})
				f.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", ordinaryIssue)
				var claimed []db.AgentTaskQueue
				runtimeID := parseTestUUID(t, f.runtimeID)
				if singleFirst {
					first, err := f.svc.TaskSvc.ClaimTaskForRuntime(ctx, runtimeID)
					if err != nil || first == nil {
						t.Fatalf("single claim after autopilot dispatch: task=%+v err=%v", first, err)
					}
					claimed = append(claimed, *first)
					batch, err := f.svc.TaskSvc.ClaimTasksForRuntimes(ctx, []pgtype.UUID{runtimeID}, 4)
					if err != nil {
						t.Fatal(err)
					}
					claimed = append(claimed, batch...)
				} else {
					batch, err := f.svc.TaskSvc.ClaimTasksForRuntimes(ctx, []pgtype.UUID{runtimeID}, 1)
					if err != nil || len(batch) != 1 {
						t.Fatalf("batch claim after autopilot dispatch: tasks=%+v err=%v", batch, err)
					}
					claimed = append(claimed, batch...)
					second, err := f.svc.TaskSvc.ClaimTaskForRuntime(ctx, runtimeID)
					if err != nil || second == nil {
						t.Fatalf("ordinary issue blocked behind autopilot: task=%+v err=%v", second, err)
					}
					claimed = append(claimed, *second)
				}
				if len(claimed) != 2 || claimed[0].ID == claimed[1].ID || claimed[0].IssueID == claimed[1].IssueID {
					t.Fatalf("claim paths did not independently dispatch both issue sessions: %+v", claimed)
				}
				var execution db.AgentTaskQueue
				seenOrdinary := false
				for _, task := range claimed {
					if !IsIssueAgentSessionTask(task) {
						t.Fatalf("claim returned ordinary per-issue execution: %+v", task)
					}
					if task.IssueID == run.IssueID {
						execution = task
					}
					seenOrdinary = seenOrdinary || task.IssueID == parseTestUUID(t, ordinaryIssue)
				}
				if !execution.ID.Valid || !seenOrdinary || IssueAgentSessionID(execution) != sessionID {
					t.Fatalf("claims changed autopilot session or omitted ordinary issue: %+v", claimed)
				}
				initial, err := f.svc.TaskSvc.ClaimIssueAgentEvents(ctx, execution.ID, 32)
				if err != nil || len(initial) != 1 || initial[0].Kind != "assigned" || initial[0].SessionID != sessionID {
					t.Fatalf("initial assigned event not delivered to session claimant: events=%+v err=%v", initial, err)
				}
				var payload struct {
					Dispatch struct {
						RunID       pgtype.UUID        `json:"run_id"`
						Attribution attribution.Result `json:"attribution"`
					} `json:"autopilot_dispatch"`
				}
				if err := json.Unmarshal(initial[0].Payload, &payload); err != nil || payload.Dispatch.RunID != run.ID || payload.Dispatch.Attribution.UserID != parseTestUUID(t, principal) || payload.Dispatch.Attribution.Source != expectedSource {
					t.Fatalf("assignment lost its transactional autopilot snapshot: payload=%s err=%v", initial[0].Payload, err)
				}
				if _, err := f.svc.TaskSvc.StartTask(ctx, execution.ID, TaskStartCapabilities{Supplement: true}); err != nil {
					t.Fatal(err)
				}
				if err := f.svc.TaskSvc.AckIssueAgentEvent(ctx, execution.ID, initial[0].ID, true, ""); err != nil {
					t.Fatal(err)
				}
				if err := f.svc.TaskSvc.PinIssueAgentSession(ctx, execution.ID, "autopilot-provider-thread", "/tmp/autopilot-provider-directory"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.svc.TaskSvc.CompleteTask(ctx, execution.ID, []byte(`{"output":""}`), "autopilot-provider-thread", "/tmp/autopilot-provider-directory", "", false, "", ""); err != nil {
					t.Fatal(err)
				}
				commentID := f.Comment(t, util.UUIDToString(run.IssueID), "follow up in the same conversation", testutil.Cols{"author_id": principal})
				followup, err := f.svc.TaskSvc.ClaimTaskForRuntime(ctx, runtimeID)
				if err != nil || followup == nil || followup.IssueID != run.IssueID || IssueAgentSessionID(*followup) != sessionID || followup.AutopilotRunID.Valid {
					t.Fatalf("follow-up did not resume its issue session: task=%+v err=%v", followup, err)
				}
				continued, err := f.q.GetIssueAgentSession(ctx, sessionID)
				if err != nil || continued.ProviderSessionID.String != "autopilot-provider-thread" || continued.WorkDir.String != "/tmp/autopilot-provider-directory" {
					t.Fatalf("follow-up lost provider context: session=%+v err=%v", continued, err)
				}
				inputs, err := f.svc.TaskSvc.ClaimIssueAgentEvents(ctx, followup.ID, 32)
				if err != nil || len(inputs) != 1 || inputs[0].SourceID != parseTestUUID(t, commentID) || inputs[0].SessionID != sessionID {
					t.Fatalf("follow-up inbox=%+v err=%v", inputs, err)
				}
				linked, err := f.q.GetAutopilotRun(ctx, run.ID)
				if err != nil || linked.TaskID != execution.ID {
					t.Fatalf("ordinary follow-up replaced autopilot dispatch link: run=%+v err=%v", linked, err)
				}
			})
		}
	}
}
