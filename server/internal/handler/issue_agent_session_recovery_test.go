package handler

import (
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
)

type issueSessionRecoveryFixture struct {
	issueID, sessionID, agentID, runtimeID, eventID string
	knownPointer                                    bool
}

func blockedIssueSessionRecoveryFixture(t *testing.T, reason string, knownPointer, started bool) issueSessionRecoveryFixture {
	t.Helper()
	issueID, sessionID, taskID, runtimeID, _ := issueAgentSessionFixture(t)
	f := issueSessionRecoveryFixture{issueID: issueID, sessionID: sessionID, runtimeID: runtimeID, knownPointer: knownPointer}
	dbfx.QueryRow(t, `SELECT agent_id FROM agent_task_queue WHERE id=$1`, taskID).Scan(&f.agentID)
	dbfx.QueryRow(t, `SELECT id FROM issue_agent_event WHERE session_id=$1 ORDER BY sequence LIMIT 1`, sessionID).Scan(&f.eventID)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='failed',completed_at=now(),started_at=CASE WHEN $2 THEN now() ELSE NULL END WHERE id=$1`, taskID, started)
	dbfx.Exec(t, `UPDATE issue_agent_session SET state='blocked',failure_reason=$2,active_task_id=NULL,
		provider_session_id=CASE WHEN $3 THEN provider_session_id ELSE NULL END,
		work_dir=CASE WHEN $3 THEN work_dir ELSE NULL END WHERE id=$1`, sessionID, reason, knownPointer)
	dbfx.Exec(t, `UPDATE issue_agent_event SET status='failed',failure_reason=$2 WHERE id=$1`, f.eventID, reason)
	return f
}

func readIssueSessionRecovery(t *testing.T, f issueSessionRecoveryFixture, userID string) IssueAgentSessionResponse {
	t.Helper()
	req := testutil.WithURLParams(newRequestAs(userID, http.MethodGet, "/agent-sessions", nil), "id", f.issueID)
	var sessions []IssueAgentSessionResponse
	testutil.Call(t, testHandler.ListIssueAgentSessions, req).Want(http.StatusOK).JSON(&sessions)
	if len(sessions) != 1 || sessions[0].ID != f.sessionID {
		t.Fatalf("unexpected issue sessions: %+v", sessions)
	}
	return sessions[0]
}

func retryIssueSessionRecovery(t *testing.T, f issueSessionRecoveryFixture, status int) {
	t.Helper()
	req := testutil.WithURLParams(newRequest(http.MethodPost, "/retry", nil),
		"id", f.issueID, "sessionId", f.sessionID, "eventId", f.eventID)
	testutil.Call(t, testHandler.RetryIssueAgentEvent, req).Want(status)
}

func assertIssueSessionRecoveryUnchanged(t *testing.T, f issueSessionRecoveryFixture, reason string) {
	t.Helper()
	var runtimeID, state, storedReason, eventState string
	var activeTask pgtype.UUID
	var pointer, workDir pgtype.Text
	dbfx.QueryRow(t, `SELECT runtime_id,state,failure_reason,active_task_id,provider_session_id,work_dir FROM issue_agent_session WHERE id=$1`, f.sessionID).
		Scan(&runtimeID, &state, &storedReason, &activeTask, &pointer, &workDir)
	dbfx.QueryRow(t, `SELECT status FROM issue_agent_event WHERE id=$1`, f.eventID).Scan(&eventState)
	if runtimeID != f.runtimeID || state != "blocked" || storedReason != reason || activeTask.Valid || eventState != "failed" {
		t.Fatalf("refused recovery mutated session/event: runtime=%s state=%s reason=%s active=%v event=%s", runtimeID, state, storedReason, activeTask, eventState)
	}
	if f.knownPointer {
		if pointer.String != "persistent-provider-thread" || workDir.String != "/tmp/persistent-issue-session" {
			t.Fatalf("refused recovery replaced recorded pointer: %q / %q", pointer.String, workDir.String)
		}
	} else if pointer.Valid || workDir.Valid {
		t.Fatal("refused recovery invented a provider session or directory")
	}
}

func assertIssueSessionRecoveryQueued(t *testing.T, f issueSessionRecoveryFixture) {
	t.Helper()
	var runtimeID, state, eventState, taskSessionID string
	var activeTask pgtype.UUID
	var pointer, workDir pgtype.Text
	dbfx.QueryRow(t, `SELECT runtime_id,state,active_task_id,provider_session_id,work_dir FROM issue_agent_session WHERE id=$1`, f.sessionID).
		Scan(&runtimeID, &state, &activeTask, &pointer, &workDir)
	dbfx.QueryRow(t, `SELECT status FROM issue_agent_event WHERE id=$1`, f.eventID).Scan(&eventState)
	if !activeTask.Valid {
		t.Fatal("accepted event recovery did not wake its logical session")
	}
	dbfx.Cleanup(t, `DELETE FROM agent_task_queue WHERE id=$1`, activeTask)
	dbfx.QueryRow(t, `SELECT context->>'issue_agent_session_id' FROM agent_task_queue WHERE id=$1`, activeTask).Scan(&taskSessionID)
	if runtimeID != f.runtimeID || state != "ready" || eventState != "pending" || taskSessionID != f.sessionID {
		t.Fatalf("recovery changed logical ownership: runtime=%s state=%s event=%s task_session=%s", runtimeID, state, eventState, taskSessionID)
	}
	if f.knownPointer {
		if pointer.String != "persistent-provider-thread" || workDir.String != "/tmp/persistent-issue-session" {
			t.Fatalf("accepted recovery replaced recorded pointer: %q / %q", pointer.String, workDir.String)
		}
	} else if pointer.Valid || workDir.Valid {
		t.Fatal("an initial execution retry invented a provider session")
	}
}

func TestIssueSessionRuntimeRecoveryCanRetryAfterOriginalRuntimeReturns(t *testing.T) {
	for _, knownPointer := range []bool{false, true} {
		t.Run(map[bool]string{false: "before_provider_start", true: "recorded_session"}[knownPointer], func(t *testing.T) {
			f := blockedIssueSessionRecoveryFixture(t, "runtime_changed", knownPointer, knownPointer)
			otherRuntime := dbfx.Runtime(t, "temporary replacement runtime")
			dbfx.Exec(t, `UPDATE agent SET runtime_id=$2 WHERE id=$1`, f.agentID, otherRuntime)
			if readIssueSessionRecovery(t, f, testUserID).CanRetry {
				t.Fatal("another runtime was offered the recorded session")
			}
			retryIssueSessionRecovery(t, f, http.StatusConflict)
			assertIssueSessionRecoveryUnchanged(t, f, "runtime_changed")

			dbfx.Exec(t, `UPDATE agent SET runtime_id=$2 WHERE id=$1`, f.agentID, f.runtimeID)
			if !readIssueSessionRecovery(t, f, testUserID).CanRetry {
				t.Fatal("the restored original runtime remained permanently blocked")
			}
			retryIssueSessionRecovery(t, f, http.StatusOK)
			assertIssueSessionRecoveryQueued(t, f)
		})
	}
}

func TestIssueSessionResumeRecoveryRevalidatesRecordedPointer(t *testing.T) {
	f := blockedIssueSessionRecoveryFixture(t, "resume_unavailable", true, true)
	if !readIssueSessionRecovery(t, f, testUserID).CanRetry {
		t.Fatal("a known provider thread and directory cannot be revalidated")
	}
	retryIssueSessionRecovery(t, f, http.StatusOK)
	assertIssueSessionRecoveryQueued(t, f)
}

func TestIssueSessionResumeRecoveryWithoutPointerCannotStartFresh(t *testing.T) {
	for _, reason := range []string{"resume_unavailable", "runtime_changed"} {
		t.Run(reason, func(t *testing.T) {
			f := blockedIssueSessionRecoveryFixture(t, reason, false, true)
			if readIssueSessionRecovery(t, f, testUserID).CanRetry {
				t.Fatal("an unknown first provider thread was offered a fresh execution")
			}
			retryIssueSessionRecovery(t, f, http.StatusConflict)
			assertIssueSessionRecoveryUnchanged(t, f, reason)
		})
	}
}

func TestIssueSessionRecoveryAvailabilityIncludesInvokerPermission(t *testing.T) {
	f := blockedIssueSessionRecoveryFixture(t, "resume_unavailable", true, true)
	viewer := dbfx.User(t, "session recovery viewer", "session-recovery-viewer@multica.test")
	dbfx.Member(t, testWorkspaceID, viewer, "admin")
	if readIssueSessionRecovery(t, f, viewer).CanRetry {
		t.Fatal("viewing a private agent granted invocation permission")
	}
}
