package service

import (
	"bytes"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
)

func TestAutopilotIssueSessionRepairDoesNotReplayTerminalHistory(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	run := f.dispatchIssue(t)
	session := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
	// A pre-session dispatch completed without consuming the capture-only
	// assignment or recording the newer run-to-task linkage.
	f.Exec(t, `UPDATE agent_task_queue SET status='completed',completed_at=now(),context=NULL WHERE id=$1`, run.TaskID)
	f.Exec(t, `UPDATE issue_agent_session SET active_task_id=NULL WHERE id=$1`, session.ID)
	f.Exec(t, `UPDATE issue_agent_event SET payload=payload-'autopilot_dispatch',actor_type='agent',actor_id=$2,originator_user_id=NULL,status='pending' WHERE session_id=$1 AND sequence=1`, session.ID, f.agentID)
	f.Exec(t, `UPDATE autopilot_run SET status='completed',completed_at=now(),task_id=NULL WHERE id=$1`, run.ID)
	historical, err := f.q.GetAutopilotRun(t.Context(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.svc.ensureWebhookCreateIssueTask(t.Context(), f.autopilot, historical); err != nil {
		t.Fatal(err)
	}
	after, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n := f.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, run.IssueID); n != 1 {
		t.Fatalf("terminal webhook recovery replayed work: %d tasks", n)
	}
	if !bytes.Equal(after.Payload, before.Payload) || after.Status != before.Status || after.OriginatorUserID != before.OriginatorUserID {
		t.Fatalf("terminal webhook recovery rewrote historical input: before=%+v after=%+v", before, after)
	}
	current, err := f.q.GetIssueAgentSession(t.Context(), session.ID)
	if err != nil || current.ActiveTaskID.Valid {
		t.Fatalf("terminal webhook recovery started another consumer: session=%+v err=%v", current, err)
	}
}

func TestAutopilotIssueSessionRepairPreservesHumanReassignment(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	run := f.dispatchIssue(t)
	first := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
	f.Exec(t, `UPDATE agent_task_queue SET status='cancelled',completed_at=now() WHERE id=$1`, run.TaskID)
	f.Exec(t, `UPDATE issue_agent_session SET active_task_id=NULL WHERE id=$1`, first.ID)
	f.Exec(t, `UPDATE autopilot_run SET task_id=NULL WHERE id=$1`, run.ID)
	newAgent := f.Agent(t, "Human reassignment target", f.runtimeID, testutil.Cols{"owner_id": f.UserID})
	human := f.member(t, "autopilot-repair-assigner")
	tx, err := f.Pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(t.Context())
	if _, err = tx.Exec(t.Context(), `SELECT set_config('multica.actor_type','member',true),set_config('multica.actor_id',$1,true)`, human); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), `UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1`, run.IssueID, newAgent); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(t.Context()); err != nil {
		t.Fatal(err)
	}
	newSession := wakeupTargetSession(t, f.principalFixture, run.IssueID, newAgent)
	before, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), newSession.ID)
	if err != nil || before.ActorType != "member" || before.ActorID != parseTestUUID(t, human) {
		t.Fatalf("reassignment lost the human actor before repair: input=%+v err=%v", before, err)
	}
	partial, err := f.q.GetAutopilotRun(t.Context(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.svc.ensureWebhookCreateIssueTask(t.Context(), f.autopilot, partial); err != nil {
		t.Fatal(err)
	}
	after, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), newSession.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before.Payload, after.Payload) || before.ActorType != after.ActorType || before.ActorID != after.ActorID || before.OriginatorUserID != after.OriginatorUserID || before.Status != after.Status {
		t.Fatalf("Autopilot repair replaced the human's assignment: before=%+v after=%+v", before, after)
	}
	if _, bound := decodeAutopilotIssueSessionInput(after); bound {
		t.Fatal("Autopilot repair borrowed the new owner's session")
	}
	if n := f.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2`, run.IssueID, newAgent); n != 0 {
		t.Fatalf("old Autopilot recovery dispatched %d tasks to the human's new target", n)
	}
}

func TestAutopilotIssueSessionRepairLinksExistingInitialConsumer(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	run := f.dispatchIssue(t)
	session := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
	initial, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	firstTaskID := run.TaskID
	f.Exec(t, `UPDATE autopilot_run SET task_id=NULL WHERE id=$1`, run.ID)
	partial, err := f.q.GetAutopilotRun(t.Context(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.svc.ensureWebhookCreateIssueTask(t.Context(), f.autopilot, partial); err != nil {
		t.Fatal(err)
	}
	repaired, err := f.q.GetAutopilotRun(t.Context(), run.ID)
	if err != nil || repaired.TaskID != firstTaskID || repaired.Status != "issue_created" {
		t.Fatalf("lost wake repair did not link the existing consumer: run=%+v err=%v", repaired, err)
	}
	current, err := f.q.GetIssueAgentSession(t.Context(), session.ID)
	if err != nil || current.ActiveTaskID != firstTaskID {
		t.Fatalf("repair replaced the logical session's consumer: session=%+v err=%v", current, err)
	}
	after, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), session.ID)
	if err != nil || after.ID != initial.ID || after.Sequence != initial.Sequence || after.Status != "pending" || !bytes.Equal(after.Payload, initial.Payload) {
		t.Fatalf("repair replayed or rewrote the initial input: input=%+v err=%v", after, err)
	}
	tasks, err := f.q.ListTasksByIssue(t.Context(), run.IssueID)
	if err != nil || len(tasks) != 1 || !IsIssueAgentSessionTask(tasks[0]) || tasks[0].AutopilotRunID.Valid || util.UUIDToString(tasks[0].OriginatorUserID) != f.triggerOwner {
		t.Fatalf("repair changed execution identity: tasks=%+v err=%v", tasks, err)
	}
}

func TestAutopilotIssueSessionCommittedInputSurvivesWakeFailure(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	originalStarter := f.svc.TaskSvc.TxStarter
	// Autopilot's issue transaction uses its own starter. Only the subsequent
	// session wake fails, after the issue, run linkage and input have committed.
	f.svc.TaskSvc.TxStarter = &failNthBeginTxStarter{inner: originalStarter, failOn: 1}
	run := f.dispatchIssue(t)
	if run.Status != "issue_created" || run.TaskID.Valid {
		t.Fatalf("post-commit wake failure settled a recoverable run: %+v", run)
	}
	session := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
	input, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), session.ID)
	if err != nil || input.Status != "pending" || session.ActiveTaskID.Valid {
		t.Fatalf("wake failure lost committed input: session=%+v input=%+v err=%v", session, input, err)
	}
	if n := f.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, run.IssueID); n != 0 {
		t.Fatalf("failed wake left %d partial consumers", n)
	}
	f.svc.TaskSvc.TxStarter = originalStarter
	if _, err = f.svc.TaskSvc.WakeIssueAgentSession(t.Context(), session.ID); err != nil {
		t.Fatal(err)
	}
	claimed, err := f.svc.TaskSvc.ClaimTask(t.Context(), parseTestUUID(t, f.agentID))
	if err != nil || claimed == nil || !IsIssueAgentSessionTask(*claimed) {
		t.Fatalf("runtime could not claim the recovered input: task=%+v err=%v", claimed, err)
	}
	events, err := f.svc.TaskSvc.ClaimIssueAgentEvents(t.Context(), claimed.ID, 32)
	if err != nil || len(events) != 1 || events[0].ID != input.ID || events[0].Sequence != input.Sequence {
		t.Fatalf("recovery replaced or replayed committed input: events=%+v err=%v", events, err)
	}
	recovered, err := f.q.GetAutopilotRun(t.Context(), run.ID)
	if err != nil || recovered.Status != "issue_created" || recovered.TaskID != claimed.ID {
		t.Fatalf("recovery lost the run's execution link: run=%+v err=%v", recovered, err)
	}
	if n := f.Count(t, `SELECT count(*) FROM issue WHERE origin_type='autopilot' AND origin_id=$1`, f.autopilot.ID); n != 1 {
		t.Fatalf("wake recovery created %d issues", n)
	}
}

func TestAutopilotIssueSessionRepairLinksUnrecordedHistoricalDispatch(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	run := f.dispatchIssue(t)
	session := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
	historicalID := run.TaskID
	// Before the routing cutover, the ordinary dispatch could commit before
	// its run linkage while the captured assignment remained unbound.
	f.Exec(t, `UPDATE agent_task_queue SET context=NULL WHERE id=$1`, historicalID)
	f.Exec(t, `UPDATE issue_agent_session SET active_task_id=NULL WHERE id=$1`, session.ID)
	f.Exec(t, `UPDATE issue_agent_event SET payload=payload-'autopilot_dispatch',actor_type='agent',actor_id=$2,originator_user_id=NULL,status='pending' WHERE session_id=$1 AND sequence=1`, session.ID, f.agentID)
	f.Exec(t, `UPDATE autopilot_run SET task_id=NULL WHERE id=$1`, run.ID)
	partial, err := f.q.GetAutopilotRun(t.Context(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	before, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.svc.ensureWebhookCreateIssueTask(t.Context(), f.autopilot, partial); err != nil {
		t.Fatal(err)
	}
	repaired, err := f.q.GetAutopilotRun(t.Context(), run.ID)
	if err != nil || repaired.TaskID != historicalID || repaired.Status != "issue_created" {
		t.Fatalf("repair did not retain the historical dispatch: run=%+v err=%v", repaired, err)
	}
	tasks, err := f.q.ListTasksByIssue(t.Context(), run.IssueID)
	if err != nil || len(tasks) != 1 || tasks[0].ID != historicalID || tasks[0].Status != "queued" || IsIssueAgentSessionTask(tasks[0]) {
		t.Fatalf("repair replayed or replaced historical work: tasks=%+v err=%v", tasks, err)
	}
	after, err := f.q.GetAutopilotIssueAgentAssignment(t.Context(), session.ID)
	if err != nil || !bytes.Equal(after.Payload, before.Payload) || after.Status != before.Status || after.ActorType != before.ActorType || after.ActorID != before.ActorID || after.OriginatorUserID != before.OriginatorUserID {
		t.Fatalf("repair rewrote the historical assignment: before=%+v after=%+v err=%v", before, after, err)
	}
	current, err := f.q.GetIssueAgentSession(t.Context(), session.ID)
	if err != nil || current.ActiveTaskID.Valid {
		t.Fatalf("historical dispatch repair created a session consumer: session=%+v err=%v", current, err)
	}
}
