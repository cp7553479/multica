package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// wakeStart marks the run started, as the daemon's start call does.
func wakeStart(t *testing.T, f principalFixture, taskID string) {
	t.Helper()
	f.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=clock_timestamp() WHERE id=$1", taskID)
}

func invocableByWorkspace(t *testing.T, f principalFixture, agent string) {
	t.Helper()
	f.Exec(t, "UPDATE agent SET permission_mode='public_to',visibility='workspace' WHERE id=$1", agent)
	f.Insert(t, "agent_invocation_target", testutil.Cols{"agent_id": agent, "target_type": "workspace", "target_id": f.WorkspaceID})
}

// The cap counts a durable inbox transfer, independently of provider delivery.
func TestJoinedWakeupCountsTowardTheCap(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", MaxFires: 1, EventTypes: []string{"comment.created"}, Instruction: "Review"})
	f.Comment(t, util.UUIDToString(issue), "trigger")
	got := wakeTick(t, f, s, w.ID)
	if got.Enabled || got.PausedReason.String != wakeupPausedMaxFires || got.FireCount != 1 || got.DisabledAt.Valid {
		t.Fatalf("durable transfer did not reach the cap: %+v", got)
	}
	events := wakeupInbox(t, f, w.ID)
	if len(events) != 1 || events[0].Status != "pending" {
		t.Fatalf("cap withdrew its legitimate last input: %+v", events)
	}
	task := wakeupSessionTask(t, f, issue, agent)
	if claimed := wakeupClaimEvents(t, f, s, task, 1); len(claimed) != 1 || claimed[0].ID != events[0].ID {
		t.Fatalf("capped rule's final input cannot be delivered: %+v", claimed)
	}
}

func TestTurnedOffWakeupHandsNothingOver(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name, state string
		remove      bool
	}{
		{"delete pending", "pending", true}, {"disable pending", "pending", false}, {"disable failed", "failed", false}, {"disable delivering", "delivering", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, s, issue, agent := conditionFixture(t)
			member := parseTestUUID(t, f.UserID)
			host := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "every", IntervalSeconds: 3600, Instruction: "Independent scheduled work"})
			if err := s.Trigger(ctx, issue, host.ID, member); err != nil {
				t.Fatal(err)
			}
			w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "Work that may be withdrawn"})
			f.Comment(t, util.UUIDToString(issue), "trigger")
			wakeTick(t, f, s, w.ID)
			task := wakeupSessionTask(t, f, issue, agent)
			events := wakeupInbox(t, f, w.ID)
			if len(events) != 1 {
				t.Fatalf("rule inputs = %+v", events)
			}
			if tc.state == "failed" {
				f.Exec(t, "UPDATE issue_agent_event SET status='failed',failure_reason='provider_rejected' WHERE id=$1", events[0].ID)
			}
			if tc.state == "delivering" {
				prefix := wakeupClaimEvents(t, f, s, task, 1)
				if len(prefix) != 1 {
					t.Fatal("independent prefix missing")
				}
				wakeupAckEvents(t, s, task, prefix)
				wakeStart(t, f, util.UUIDToString(task.ID))
				task.Status = "running"
				claimed := wakeupClaimEvents(t, f, s, task, 1)
				if len(claimed) != 1 || claimed[0].ID != events[0].ID {
					t.Fatalf("rule input not claimed: %+v", claimed)
				}
			}
			if tc.remove {
				if err := s.Delete(ctx, issue, w.ID, member); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := s.Disable(ctx, issue, w.ID, member); err != nil {
					t.Fatal(err)
				}
			}
			after := wakeupInbox(t, f, w.ID)
			want := "ignored"
			if tc.state == "delivering" {
				want = "delivering"
			}
			if len(after) != 1 || after[0].Status != want {
				t.Fatalf("turning off %s input: %+v, want %s", tc.state, after, want)
			}
			independent := wakeupInbox(t, f, host.ID)
			if len(independent) != 1 || independent[0].Status == "ignored" || independent[0].Status == "failed" {
				t.Fatalf("turning off one rule withdrew another input: %+v", independent)
			}
			active := wakeupSessionTask(t, f, issue, agent)
			if active.ID != task.ID || active.Status == "cancelled" {
				t.Fatalf("turning off one rule cancelled the shared execution: %+v", active)
			}
			if tc.state == "delivering" {
				wakeupAckEvents(t, s, task, after)
			}
		})
	}
}

func TestTurnedOffChildDoneRuleLeavesARequeuedRun(t *testing.T) {
	for _, scope := range []string{"issue", "workspace"} {
		t.Run(scope, func(t *testing.T) {
			f, s, issue, agent := conditionFixture(t)
			ctx := context.Background()
			f.Exec(t, "UPDATE issue SET status='in_progress',assignee_type='agent',assignee_id=$2 WHERE id=$1", issue, agent)
			child := f.Issue(t, "child", testutil.Cols{"parent_issue_id": issue, "status": "in_progress"})
			f.Cleanup(t, "DELETE FROM issue_child_event WHERE parent_id=$1", issue)
			if err := s.ProcessChildEvents(ctx, issue); err != nil {
				t.Fatal(err)
			}
			f.Exec(t, "UPDATE issue SET status='done' WHERE id=$1", child)
			if err := s.ProcessChildEvents(ctx, issue); err != nil {
				t.Fatal(err)
			}
			w, err := f.q.GetSystemWakeup(ctx, db.GetSystemWakeupParams{IssueID: issue, SystemRule: systemRuleText(SystemRuleChildDone)})
			if err != nil {
				t.Fatal(err)
			}
			before := wakeupInbox(t, f, w.ID)
			if len(before) != 1 || before[0].Status != "pending" {
				t.Fatalf("child close did not enter inbox: %+v", before)
			}
			task := wakeupSessionTask(t, f, issue, agent)
			other := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "every", IntervalSeconds: 3600, Instruction: "Independent check"})
			if err := s.Trigger(ctx, issue, other.ID, parseTestUUID(t, f.UserID)); err != nil {
				t.Fatal(err)
			}
			off := false
			if scope == "issue" {
				_, err = s.UpdateChildDoneRule(ctx, issue, SystemWakeupInput{Enabled: &off})
			} else {
				_, err = s.SetChildDoneDefault(ctx, parseTestUUID(t, f.WorkspaceID), &off, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if events := wakeupInbox(t, f, w.ID); len(events) != 1 || events[0].Status != "ignored" {
				t.Fatalf("disabled child rule retained pending input: %+v", events)
			}
			if events := wakeupInbox(t, f, other.ID); len(events) != 1 || events[0].Status != "pending" {
				t.Fatalf("disabled child rule withdrew unrelated input: %+v", events)
			}
			if current := wakeupSessionTask(t, f, issue, agent); current.ID != task.ID {
				t.Fatal("disabled child rule replaced shared execution")
			}
		})
	}
}

func TestWakeupJoinsOnlyRunsOfTheSamePerson(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	invocableByWorkspace(t, f, agent)
	host := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "every", IntervalSeconds: 3600, Instruction: "First member's work"})
	if err := s.Trigger(ctx, issue, host.ID, parseTestUUID(t, f.UserID)); err != nil {
		t.Fatal(err)
	}
	session := wakeupTargetSession(t, f, issue, agent)
	first := wakeupSessionTask(t, f, issue, agent)
	other := f.member(t, "wake-other")
	w, err := s.Create(ctx, issue, parseTestUUID(t, other), pgtype.UUID{}, WakeupInput{AgentID: agent, Kind: "event", EventTypes: []string{"comment.created"}, Instruction: "Other member's instruction"})
	if err != nil {
		t.Fatal(err)
	}
	f.Comment(t, util.UUIDToString(issue), "trigger")
	wakeTick(t, f, s, w.ID)
	event := wakeupInbox(t, f, w.ID)
	if len(event) != 1 || event[0].OriginatorUserID != parseTestUUID(t, other) {
		t.Fatalf("rule lost its own principal: %+v", event)
	}
	prefix := wakeupClaimEvents(t, f, s, first, 32)
	if len(prefix) != 1 || prefix[0].OriginatorUserID != parseTestUUID(t, f.UserID) {
		t.Fatalf("mixed principals in first execution: %+v", prefix)
	}
	wakeupAckEvents(t, s, first, prefix)
	wakeStart(t, f, util.UUIDToString(first.ID))
	first.Status = "running"
	if batch := wakeupClaimEvents(t, f, s, first, 32); len(batch) != 0 {
		t.Fatalf("another person's input entered the current execution: %+v", batch)
	}
	wakeupFinishExecution(t, f, s, first, "completed")
	if _, err = s.Tasks.WakeIssueAgentSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	next := wakeupSessionTask(t, f, issue, agent)
	resumed := wakeupTargetSession(t, f, issue, agent)
	if next.ID == first.ID || next.OriginatorUserID != parseTestUUID(t, other) || resumed.ID != session.ID || resumed.ProviderSessionID.String != "provider-wakeup-session" || resumed.WorkDir.String != "/tmp/wakeup-session" {
		t.Fatalf("next principal lost the persistent conversation: %+v next=%+v", resumed, next)
	}
	batch := wakeupClaimEvents(t, f, s, next, 32)
	if len(batch) != 1 || batch[0].ID != event[0].ID {
		t.Fatalf("next principal did not receive its own input: %+v", batch)
	}
}

func TestJoinRechecksTheCreatorsAccess(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	invocableByWorkspace(t, f, agent)
	other := f.member(t, "wake-revoked")
	w, err := s.Create(ctx, issue, parseTestUUID(t, other), pgtype.UUID{}, WakeupInput{AgentID: agent, Kind: "event", EventTypes: []string{"comment.created"}, Instruction: "Check access"})
	if err != nil {
		t.Fatal(err)
	}
	f.Comment(t, util.UUIDToString(issue), "trigger")
	wakeTick(t, f, s, w.ID)
	task := wakeupSessionTask(t, f, issue, agent)
	f.Exec(t, "DELETE FROM member WHERE user_id=$1 AND workspace_id=$2", other, f.WorkspaceID)
	if batch := wakeupClaimEvents(t, f, s, task, 32); len(batch) != 0 {
		t.Fatalf("creator lost access but input still reached provider: %+v", batch)
	}
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 || events[0].Status != "ignored" || events[0].FailureReason.String != "invocation_not_allowed" {
		t.Fatalf("revoked rule input was not visibly refused: %+v", events)
	}
}

func TestWakeupKeepsItsInputWhenTheWaitingRunDoesNotTakeIt(t *testing.T) {
	for _, reason := range []string{"provider_unsupported", "delivery_unconfirmed"} {
		t.Run(reason, func(t *testing.T) {
			f, s, issue, agent := conditionFixture(t)
			ctx := context.Background()
			w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", EventTypes: []string{"comment.created"}, Instruction: "Keep this instruction"})
			f.Comment(t, util.UUIDToString(issue), "trigger")
			wakeTick(t, f, s, w.ID)
			task := wakeupSessionTask(t, f, issue, agent)
			claimed := wakeupClaimEvents(t, f, s, task, 1)
			if len(claimed) != 1 {
				t.Fatalf("claim = %+v", claimed)
			}
			if reason == "provider_unsupported" {
				if err := s.Tasks.AckIssueAgentEvent(ctx, task.ID, claimed[0].ID, false, reason); err != nil {
					t.Fatal(err)
				}
			}
			wakeupFinishExecution(t, f, s, task, "failed")
			wakeDispatch(t, s, w)
			events := wakeupInbox(t, f, w.ID)
			if len(events) != 1 || events[0].ID != claimed[0].ID || events[0].Status != "failed" || events[0].FailureReason.String != reason {
				t.Fatalf("failed delivery lost or duplicated input: %+v", events)
			}
			if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NOT NULL", w.ID); n != 1 {
				t.Fatal("execution failure reverted durable transfer")
			}
			if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue); n != 1 {
				t.Fatalf("failed delivery silently started %d executions", n)
			}
			var payload wakeupSessionPayload
			if err := json.Unmarshal(events[0].Payload, &payload); err != nil || payload.Instruction != "Keep this instruction" {
				t.Fatalf("instruction lost: %+v %v", payload, err)
			}
		})
	}
}

func TestChildDoneBackfillKeepsAPendingClose(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	f.Exec(t, "UPDATE issue SET status='in_progress',assignee_type='agent',assignee_id=$2 WHERE id=$1", issue, agent)
	child := f.Issue(t, "child", testutil.Cols{"parent_issue_id": issue, "status": "in_progress"})
	f.Cleanup(t, "DELETE FROM issue_child_event WHERE parent_id=$1", issue)
	// A sub-issue from before the rule existed: only its closing is recorded.
	f.Exec(t, "DELETE FROM issue_child_event WHERE parent_id=$1", issue)
	f.Exec(t, "UPDATE issue SET status='done' WHERE id=$1", child)
	parent, err := f.q.GetIssue(ctx, issue)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.ensureRule(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if err = s.ProcessChildEvents(ctx, issue); err != nil {
		t.Fatal(err)
	}
	w, err := f.q.GetSystemWakeup(ctx, db.GetSystemWakeupParams{IssueID: issue, SystemRule: systemRuleText(SystemRuleChildDone)})
	if err != nil {
		t.Fatal(err)
	}
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 || events[0].Status != "pending" {
		t.Fatalf("pending closing lost after the backfill: events=%+v condition_state=%q", events, w.ConditionState)
	}
	_ = wakeupSessionTask(t, f, issue, agent)
}

// A named rule keeps its target when ownership changes; ordinary issue inputs
// belong only to the new owner, including when the old execution ends later.
func TestWakeupReassignmentPreservesExplicitAuxiliaryInput(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	member := parseTestUUID(t, f.UserID)
	s.Tasks.IssueSessionCanInvoke = func(_ context.Context, _ db.Agent, event db.IssueAgentEvent) bool {
		return event.OriginatorUserID == member
	}
	assign := func(target string) {
		t.Helper()
		tx, err := f.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err = tx.Exec(ctx, "SELECT set_config('multica.actor_type','member',true),set_config('multica.actor_id',$1,true)", f.UserID); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Exec(ctx, "UPDATE issue SET assignee_type='agent',assignee_id=$2,status='in_progress' WHERE id=$1", issue, target); err != nil {
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	assign(agent)
	session := wakeupTargetSession(t, f, issue, agent)
	if _, err := s.Tasks.WakeIssueAgentSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	first := wakeupSessionTask(t, f, issue, agent)
	ordinary := wakeupClaimEvents(t, f, s, first, 1)
	if len(ordinary) != 1 || ordinary[0].Kind != "assigned" {
		t.Fatalf("owner assignment = %+v", ordinary)
	}
	wakeStart(t, f, util.UUIDToString(first.ID))
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "every", IntervalSeconds: 3600, Instruction: "Explicit auxiliary work"})
	if err := s.Trigger(ctx, issue, w.ID, member); err != nil {
		t.Fatal(err)
	}
	other := f.privateAgentOwnedBy(t, f.UserID, "new-owner")
	assign(other)
	comment := f.Comment(t, util.UUIDToString(issue), "Only the new owner receives this")
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 || events[0].Status != "pending" {
		t.Fatalf("reassignment withdrew the explicit rule input: %+v", events)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_id=$2", session.ID, comment); n != 0 {
		t.Fatal("old owner received the new owner's comment")
	}
	newOwner := wakeupTargetSession(t, f, issue, other)
	if n := f.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_id=$2 AND kind='comment.created' AND status='pending'", newOwner.ID, comment); n != 1 {
		t.Fatal("new owner did not receive its comment")
	}
	wakeupFinishExecution(t, f, s, first, "failed")
	old, err := f.q.GetIssueAgentEvent(ctx, ordinary[0].ID)
	if err != nil || old.Status != "ignored" || old.FailureReason.String != "reassigned" {
		t.Fatalf("unconfirmed former-owner input blocked auxiliary work: %+v %v", old, err)
	}
	if _, err = s.Tasks.WakeIssueAgentSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	next := wakeupSessionTask(t, f, issue, agent)
	if next.ID == first.ID || next.WorkflowStep.Valid {
		t.Fatalf("auxiliary input reused owner execution semantics: %+v", next)
	}
	if current := wakeupTargetSession(t, f, issue, agent); current.ID != session.ID || current.ProviderSessionID.String != "provider-wakeup-session" {
		t.Fatalf("auxiliary input lost the persistent conversation: %+v", current)
	}
	batch := wakeupClaimEvents(t, f, s, next, 32)
	if len(batch) != 1 || batch[0].Kind != "wakeup.fired" {
		t.Fatalf("auxiliary claim = %+v", batch)
	}
}
