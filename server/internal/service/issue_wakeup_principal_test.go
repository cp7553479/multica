package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type wakeupPrincipalPair struct {
	f         principalFixture
	s         *IssueWakeupService
	issue     pgtype.UUID
	agent     string
	creatorB  pgtype.UUID
	ruleA     db.IssueWakeup
	ruleB     db.IssueWakeup
	taskA     db.AgentTaskQueue
	sessionID pgtype.UUID
}

func queuedWakeupPrincipalPair(t *testing.T) wakeupPrincipalPair {
	t.Helper()
	f, s, issue, agent := conditionFixture(t)
	invocableByWorkspace(t, f, agent)
	creatorA := parseTestUUID(t, f.UserID)
	creatorB := parseTestUUID(t, f.member(t, "wakeup-principal-other"))
	ruleA := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "every", IntervalSeconds: 3600, Instruction: "Creator A's scheduled work"})
	ruleB, err := s.Create(context.Background(), issue, creatorB, pgtype.UUID{}, WakeupInput{AgentID: agent, Kind: "every", IntervalSeconds: 3600, Instruction: "Creator B's scheduled work"})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Trigger(context.Background(), issue, ruleA.ID, creatorA); err != nil {
		t.Fatal(err)
	}
	taskA := wakeupSessionTask(t, f, issue, agent)
	session := wakeupTargetSession(t, f, issue, agent)
	if err = s.Trigger(context.Background(), issue, ruleB.ID, creatorB); err != nil {
		t.Fatal(err)
	}
	return wakeupPrincipalPair{f: f, s: s, issue: issue, agent: agent, creatorB: creatorB, ruleA: ruleA, ruleB: ruleB, taskA: taskA, sessionID: session.ID}
}

func TestIssueWakeupDisableReplacesQueuedConsumerForRemainingPrincipal(t *testing.T) {
	pair := queuedWakeupPrincipalPair(t)
	if active := wakeupSessionTask(t, pair.f, pair.issue, pair.agent); active.ID != pair.taskA.ID || active.Status != "queued" || active.OriginatorUserID != parseTestUUID(t, pair.f.UserID) {
		t.Fatalf("both inputs did not share the first creator's queued consumer: %+v", active)
	}
	if n := pair.f.Count(t, "SELECT count(*) FROM issue_agent_session WHERE issue_id=$1 AND agent_id=$2", pair.issue, pair.agent); n != 1 {
		t.Fatalf("one target got %d logical sessions", n)
	}
	for _, rule := range []db.IssueWakeup{pair.ruleA, pair.ruleB} {
		if input := wakeupInbox(t, pair.f, rule.ID); len(input) != 1 || input[0].Status != "pending" || input[0].SessionID != pair.sessionID || input[0].OriginatorUserID != rule.CreatedBy {
			t.Fatalf("creator's pending input lost its principal or session: %+v", input)
		}
	}
	if _, err := pair.s.Disable(context.Background(), pair.issue, pair.ruleA.ID, parseTestUUID(t, pair.f.UserID)); err != nil {
		t.Fatal(err)
	}
	old, err := pair.f.q.GetAgentTask(context.Background(), pair.taskA.ID)
	if err != nil || old.Status != "cancelled" || old.StartedAt.Valid {
		t.Fatalf("obsolete creator's unstarted consumer survived: %+v %v", old, err)
	}
	if input := wakeupInbox(t, pair.f, pair.ruleA.ID); len(input) != 1 || input[0].Status != "ignored" || input[0].FailureReason.String != "wakeup_withdrawn" {
		t.Fatalf("withdrawn creator's input was not refused: %+v", input)
	}
	remaining := wakeupInbox(t, pair.f, pair.ruleB.ID)
	if len(remaining) != 1 || remaining[0].Status != "pending" || remaining[0].OriginatorUserID != pair.creatorB {
		t.Fatalf("remaining creator's input was lost or claimed by its predecessor: %+v", remaining)
	}
	next := wakeupSessionTask(t, pair.f, pair.issue, pair.agent)
	if next.ID == pair.taskA.ID || next.Status != "queued" || next.OriginatorUserID != pair.creatorB || IssueAgentSessionID(next) != pair.sessionID {
		t.Fatalf("remaining creator did not receive its own consumer in the same session: %+v", next)
	}
	if n := pair.f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", pair.issue); n != 2 {
		t.Fatalf("principal handoff created %d executions, want the cancelled predecessor and one replacement", n)
	}
	claimed := wakeupClaimEvents(t, pair.f, pair.s, next, 32)
	if len(claimed) != 1 || claimed[0].ID != remaining[0].ID || claimed[0].OriginatorUserID != pair.creatorB {
		t.Fatalf("replacement consumer claimed another creator's input: %+v", claimed)
	}
	wakeupAckEvents(t, pair.s, next, claimed)
}

func TestIssueWakeupClaimReplacesSupersededConsumerAfterRulePause(t *testing.T) {
	pair := queuedWakeupPrincipalPair(t)
	if active := wakeupSessionTask(t, pair.f, pair.issue, pair.agent); active.ID != pair.taskA.ID || active.Status != "queued" || active.OriginatorUserID != parseTestUUID(t, pair.f.UserID) {
		t.Fatalf("both inputs did not share the first creator's queued consumer: %+v", active)
	}
	if n := pair.f.Count(t, "SELECT count(*) FROM issue_agent_session WHERE issue_id=$1 AND agent_id=$2", pair.issue, pair.agent); n != 1 {
		t.Fatalf("one target got %d logical sessions", n)
	}
	for _, rule := range []db.IssueWakeup{pair.ruleA, pair.ruleB} {
		if input := wakeupInbox(t, pair.f, rule.ID); len(input) != 1 || input[0].Status != "pending" || input[0].SessionID != pair.sessionID || input[0].OriginatorUserID != rule.CreatedBy {
			t.Fatalf("creator's pending input lost its principal or session: %+v", input)
		}
	}
	// Pause commits before its subsequent withdrawal pass. A daemon can claim
	// during that window and must release the obsolete unstarted principal.
	pair.f.Exec(t, "UPDATE issue_wakeup SET enabled=false,disabled_at=now(),updated_at=now() WHERE id=$1", pair.ruleA.ID)
	pair.f.Exec(t, "UPDATE agent_task_queue SET status='dispatched',dispatched_at=clock_timestamp() WHERE id=$1", pair.taskA.ID)
	pair.f.Cleanup(t, "DELETE FROM task_token WHERE task_id=$1", pair.taskA.ID)
	if _, err := pair.f.q.CreateTaskToken(context.Background(), db.CreateTaskTokenParams{
		TokenHash: "superseded-consumer-" + util.UUIDToString(pair.taskA.ID),
		TaskID:    pair.taskA.ID, AgentID: pair.taskA.AgentID, WorkspaceID: parseTestUUID(t, pair.f.WorkspaceID),
		UserID: pair.taskA.OriginatorUserID, ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Hour), Valid: true},
	}); err != nil {
		t.Fatal(err)
	}
	batch, claimErr := pair.s.Tasks.ClaimIssueAgentEvents(context.Background(), pair.taskA.ID, 32)
	if !errors.Is(claimErr, ErrIssueAgentSessionExecutionSuperseded) || len(batch) != 0 {
		t.Fatalf("obsolete consumer was not explicitly superseded: events=%+v err=%v", batch, claimErr)
	}
	old, err := pair.f.q.GetAgentTask(context.Background(), pair.taskA.ID)
	if err != nil || old.Status != "cancelled" || old.StartedAt.Valid {
		t.Fatalf("obsolete creator's unstarted consumer survived: %+v %v", old, err)
	}
	if input := wakeupInbox(t, pair.f, pair.ruleA.ID); len(input) != 1 || input[0].Status != "ignored" || input[0].FailureReason.String != "invocation_not_allowed" {
		t.Fatalf("withdrawn creator's input was not refused: %+v", input)
	}
	remaining := wakeupInbox(t, pair.f, pair.ruleB.ID)
	if len(remaining) != 1 || remaining[0].Status != "pending" || remaining[0].OriginatorUserID != pair.creatorB {
		t.Fatalf("remaining creator's input was lost or claimed by its predecessor: %+v", remaining)
	}
	next := wakeupSessionTask(t, pair.f, pair.issue, pair.agent)
	if next.ID == pair.taskA.ID || next.Status != "queued" || next.OriginatorUserID != pair.creatorB || IssueAgentSessionID(next) != pair.sessionID {
		t.Fatalf("remaining creator did not receive its own consumer in the same session: %+v", next)
	}
	if n := pair.f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", pair.issue); n != 2 {
		t.Fatalf("principal handoff created %d executions, want the cancelled predecessor and one replacement", n)
	}
	if n := pair.f.Count(t, "SELECT count(*) FROM task_token WHERE task_id=$1", pair.taskA.ID); n != 0 {
		t.Fatal("superseded consumer retained its stale tool token")
	}
	if _, err = pair.s.Disable(context.Background(), pair.issue, pair.ruleA.ID, parseTestUUID(t, pair.f.UserID)); err != nil {
		t.Fatal(err)
	}
	if active := wakeupSessionTask(t, pair.f, pair.issue, pair.agent); active.ID != next.ID || active.Status != "queued" || active.OriginatorUserID != pair.creatorB {
		t.Fatalf("late withdrawal replaced the remaining creator's consumer: before=%+v after=%+v", next, active)
	}
	claimed := wakeupClaimEvents(t, pair.f, pair.s, next, 32)
	if len(claimed) != 1 || claimed[0].ID != remaining[0].ID || claimed[0].OriginatorUserID != pair.creatorB {
		t.Fatalf("replacement consumer claimed another creator's input: %+v", claimed)
	}
	wakeupAckEvents(t, pair.s, next, claimed)
}
