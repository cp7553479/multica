package handler

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
)

func issueSessionPinAPIFixture(t *testing.T) (sessionID, taskID string) {
	t.Helper()
	_, sessionID, taskID, _, _ = issueAgentSessionFixture(t)
	dbfx.Exec(t, `UPDATE issue_agent_session SET provider_session_id=NULL,work_dir=NULL WHERE id=$1`, sessionID)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1`, taskID)
	return sessionID, taskID
}

func issueSessionPinAPIRequest(taskID, providerID, workDir string) *http.Request {
	return testutil.WithURLParams(newOwnerDaemonRequest(http.MethodPost, "/session", map[string]any{
		"session_id": providerID, "work_dir": workDir,
	}), "taskId", taskID)
}

func assertIssueSessionPinPointers(t *testing.T, sessionID, taskID, providerID, workDir string) {
	t.Helper()
	var sessionPointer, sessionDirectory, taskPointer, taskDirectory pgtype.Text
	dbfx.QueryRow(t, `SELECT s.provider_session_id,s.work_dir,t.session_id,t.work_dir
		FROM issue_agent_session s JOIN agent_task_queue t ON t.id=$2 WHERE s.id=$1`, sessionID, taskID).
		Scan(&sessionPointer, &sessionDirectory, &taskPointer, &taskDirectory)
	if sessionPointer.String != providerID || taskPointer.String != providerID || sessionDirectory.String != workDir || taskDirectory.String != workDir {
		t.Fatalf("logical/task pin diverged: logical=(%q,%q) task=(%q,%q)", sessionPointer.String, sessionDirectory.String, taskPointer.String, taskDirectory.String)
	}
}

func TestIssueSessionPinAPIStoresEarlyIdentityAndDirectoryAtomically(t *testing.T) {
	sessionID, taskID := issueSessionPinAPIFixture(t)
	for range 2 {
		testutil.Call(t, testHandler.PinTaskSession, issueSessionPinAPIRequest(taskID, "first-provider-thread", "/tmp/persistent-session-workdir")).Want(http.StatusNoContent)
		assertIssueSessionPinPointers(t, sessionID, taskID, "first-provider-thread", "/tmp/persistent-session-workdir")
	}
	testutil.Call(t, testHandler.PinTaskSession, issueSessionPinAPIRequest(taskID, "different-thread", "/tmp/another-workdir")).Want(http.StatusConflict)
	assertIssueSessionPinPointers(t, sessionID, taskID, "first-provider-thread", "/tmp/persistent-session-workdir")
}

func TestIssueSessionPinAPIRequiresBothIdentityAndDirectory(t *testing.T) {
	sessionID, taskID := issueSessionPinAPIFixture(t)
	for _, input := range [][2]string{{"first-provider-thread", ""}, {"", "/tmp/persistent-session-workdir"}} {
		testutil.Call(t, testHandler.PinTaskSession, issueSessionPinAPIRequest(taskID, input[0], input[1])).Want(http.StatusBadRequest)
		assertIssueSessionPinPointers(t, sessionID, taskID, "", "")
	}
}

type unavailableIssueSessionPinStore struct{}

func (unavailableIssueSessionPinStore) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("injected session pin store outage")
}

func TestIssueSessionPinAPIStorageFailureIsRetryableWithoutPartialPin(t *testing.T) {
	sessionID, taskID := issueSessionPinAPIFixture(t)
	h := *testHandler
	h.TaskService = &service.TaskService{Queries: testHandler.Queries, TxStarter: unavailableIssueSessionPinStore{}}
	testutil.Call(t, h.PinTaskSession, issueSessionPinAPIRequest(taskID, "first-provider-thread", "/tmp/persistent-session-workdir")).Want(http.StatusServiceUnavailable)
	assertIssueSessionPinPointers(t, sessionID, taskID, "", "")
	testutil.Call(t, testHandler.PinTaskSession, issueSessionPinAPIRequest(taskID, "first-provider-thread", "/tmp/persistent-session-workdir")).Want(http.StatusNoContent)
	assertIssueSessionPinPointers(t, sessionID, taskID, "first-provider-thread", "/tmp/persistent-session-workdir")
}

func TestIssueSessionPinAPILateCancellationPreservesRecoverableIdentity(t *testing.T) {
	sessionID, taskID := issueSessionPinAPIFixture(t)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='cancelled',completed_at=now() WHERE id=$1`, taskID)
	dbfx.Exec(t, `UPDATE issue_agent_session SET active_task_id=NULL,state='blocked',failure_reason='resume_unavailable' WHERE id=$1`, sessionID)
	testutil.Call(t, testHandler.PinTaskSession, issueSessionPinAPIRequest(taskID, "first-provider-thread", "/tmp/persistent-session-workdir")).Want(http.StatusNoContent)
	assertIssueSessionPinPointers(t, sessionID, taskID, "first-provider-thread", "/tmp/persistent-session-workdir")
	var active pgtype.UUID
	dbfx.QueryRow(t, `SELECT active_task_id FROM issue_agent_session WHERE id=$1`, sessionID).Scan(&active)
	if active.Valid {
		t.Fatal("a late durable pin silently started another execution")
	}
}
