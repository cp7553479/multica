package service

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/util"
)

func TestAutopilotIssueSessionMigrationRequiresExplicitPreflightForMissingInput(t *testing.T) {
	for _, missing := range []string{"session", "assignment"} {
		t.Run(missing, func(t *testing.T) {
			f := newAutopilotIssueSessionFixture(t)
			ctx := t.Context()
			run := f.dispatchIssue(t)
			session := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
			// Model a pre-cutover issue_created run whose original task and
			// captured session input are unavailable. Repair must not invent them.
			f.Exec(t, "UPDATE autopilot_run SET task_id=NULL WHERE id=$1", run.ID)
			f.Exec(t, "UPDATE issue_agent_session SET active_task_id=NULL WHERE id=$1", session.ID)
			f.Exec(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", run.IssueID)
			f.Exec(t, "DELETE FROM issue_agent_event WHERE session_id=$1", session.ID)
			if missing == "session" {
				f.Exec(t, "DELETE FROM issue_agent_session WHERE id=$1", session.ID)
			}
			beforeRun, err := f.q.GetAutopilotRun(ctx, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			beforeIssue, err := f.q.GetIssue(ctx, run.IssueID)
			if err != nil {
				t.Fatal(err)
			}
			beforeAgent, err := f.q.GetAgent(ctx, parseTestUUID(t, f.agentID))
			if err != nil {
				t.Fatal(err)
			}
			for attempt := 0; attempt < 2; attempt++ {
				err := f.svc.ensureWebhookCreateIssueTask(ctx, f.autopilot, beforeRun)
				if !errors.Is(err, errAutopilotIssueSessionMigrationRequired) {
					t.Fatalf("missing %s error=%v, want explicit migration requirement", missing, err)
				}
				for _, required := range []string{util.UUIDToString(run.ID), util.UUIDToString(run.IssueID), "migration preflight", "docs/engineering/issue-agent-sessions.md"} {
					if !strings.Contains(err.Error(), required) {
						t.Fatalf("migration error lacks actionable %q: %v", required, err)
					}
				}
			}
			afterRun, err := f.q.GetAutopilotRun(ctx, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			afterIssue, err := f.q.GetIssue(ctx, run.IssueID)
			if err != nil {
				t.Fatal(err)
			}
			afterAgent, err := f.q.GetAgent(ctx, parseTestUUID(t, f.agentID))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(beforeRun, afterRun) || !reflect.DeepEqual(beforeIssue, afterIssue) || !reflect.DeepEqual(beforeAgent, afterAgent) {
				t.Fatalf("migration preflight changed run, issue, or agent ownership: run=%+v issue=%+v agent=%+v", afterRun, afterIssue, afterAgent)
			}
			if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", run.IssueID); n != 0 {
				t.Fatalf("migration check manufactured %d tasks", n)
			}
			if n := f.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1", session.ID); n != 0 {
				t.Fatalf("migration check manufactured %d inputs", n)
			}
			wantSessions := 0
			if missing == "assignment" {
				wantSessions = 1
			}
			if n := f.Count(t, "SELECT count(*) FROM issue_agent_session WHERE issue_id=$1", run.IssueID); n != wantSessions {
				t.Fatalf("migration check changed session count to %d, want %d", n, wantSessions)
			}
		})
	}
}

func TestAutopilotIssueSessionMigrationLeavesTerminalRunsUntouched(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	ctx := t.Context()
	run := f.dispatchIssue(t)
	session := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
	f.Exec(t, "UPDATE autopilot_run SET task_id=NULL,status='completed',completed_at=now() WHERE id=$1", run.ID)
	f.Exec(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", run.IssueID)
	f.Exec(t, "DELETE FROM issue_agent_event WHERE session_id=$1", session.ID)
	f.Exec(t, "DELETE FROM issue_agent_session WHERE id=$1", session.ID)
	before, err := f.q.GetAutopilotRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.ensureWebhookCreateIssueTask(ctx, f.autopilot, before); err != nil {
		t.Fatalf("terminal history unexpectedly requires migration: %v", err)
	}
	after, err := f.q.GetAutopilotRun(ctx, run.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("terminal preflight changed run: run=%+v err=%v", after, err)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", run.IssueID); n != 0 {
		t.Fatalf("terminal preflight created %d tasks", n)
	}
}

func TestAutopilotIssueSessionMigrationRetainsHistoricalDispatchWithoutSession(t *testing.T) {
	f := newAutopilotIssueSessionFixture(t)
	ctx := t.Context()
	run := f.dispatchIssue(t)
	session := wakeupTargetSession(t, f.principalFixture, run.IssueID, f.agentID)
	historicalTaskID := run.TaskID
	f.Exec(t, "UPDATE agent_task_queue SET context=NULL WHERE id=$1", historicalTaskID)
	f.Exec(t, "UPDATE autopilot_run SET task_id=NULL WHERE id=$1", run.ID)
	f.Exec(t, "DELETE FROM issue_agent_event WHERE session_id=$1", session.ID)
	f.Exec(t, "DELETE FROM issue_agent_session WHERE id=$1", session.ID)
	partial, err := f.q.GetAutopilotRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.ensureWebhookCreateIssueTask(ctx, f.autopilot, partial); err != nil {
		t.Fatalf("existing historical dispatch unexpectedly requires migration: %v", err)
	}
	repaired, err := f.q.GetAutopilotRun(ctx, run.ID)
	if err != nil || repaired.TaskID != historicalTaskID || repaired.Status != partial.Status || repaired.IssueID != partial.IssueID {
		t.Fatalf("historical dispatch link changed: run=%+v err=%v", repaired, err)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", run.IssueID); n != 1 {
		t.Fatalf("historical audit repair created %d tasks, want original only", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_agent_session WHERE issue_id=$1", run.IssueID); n != 0 {
		t.Fatalf("historical audit repair manufactured %d sessions", n)
	}
}
