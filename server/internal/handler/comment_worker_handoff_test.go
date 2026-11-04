package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func runningWorkerReportSession(t *testing.T) (issueID, ownerID, sessionID, ownerTaskID, workerID, workerTaskID, rootID string) {
	t.Helper()
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	root := captureSessionComment(t, issueID, map[string]any{"content": "Coordinate this issue"}, "", "")
	rootID = root.ID
	dbfx.QueryRow(t, "SELECT id,active_task_id FROM issue_agent_session WHERE issue_id=$1", issueID).Scan(&sessionID, &ownerTaskID)
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", ownerTaskID)
	events, err := testHandler.TaskService.ClaimIssueAgentEvents(context.Background(), parseUUID(ownerTaskID), 32)
	if err != nil || len(events) != 2 {
		t.Fatalf("initial inbox = %+v, error=%v", events, err)
	}
	for _, event := range events {
		if err := testHandler.TaskService.AckIssueAgentEvent(context.Background(), parseUUID(ownerTaskID), event.ID, true, ""); err != nil {
			t.Fatal(err)
		}
	}
	workerID = dbfx.Agent(t, "Explicitly assigned worker", runtimeID)
	childID := dbfx.Issue(t, "Assigned worker subissue", testutil.Cols{
		"parent_issue_id": issueID, "assignee_type": "agent", "assignee_id": workerID,
	})
	var workerSessionID string
	dbfx.QueryRow(t, "SELECT id FROM issue_agent_session WHERE issue_id=$1", childID).Scan(&workerSessionID)
	task, err := testHandler.TaskService.WakeIssueAgentSession(context.Background(), parseUUID(workerSessionID))
	if err != nil || task == nil {
		t.Fatalf("explicit assignment did not create worker execution: %+v, %v", task, err)
	}
	workerTaskID = uuidToString(task.ID)
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", workerTaskID)
	return
}

func completeWorkerReportOwner(t *testing.T, taskID string) {
	t.Helper()
	req := withURLParam(newOwnerDaemonRequest(http.MethodPost, "/complete", map[string]any{
		"output": "Private final transcript", "session_id": "owner-provider-thread", "work_dir": "/tmp/worker-report-owner", "durable_work_dir": "/tmp/worker-report-owner",
	}), "taskId", taskID)
	testutil.Call(t, testHandler.CompleteTask, req).Want(http.StatusOK)
}

func TestWorkerReportsEnterTheExplicitOwnersSessionWhileBusyAndIdle(t *testing.T) {
	issueID, ownerID, sessionID, ownerTaskID, workerID, workerTaskID, rootID := runningWorkerReportSession(t)
	progress := captureSessionComment(t, issueID, map[string]any{
		"content": "Verification started", "parent_id": rootID,
	}, workerID, workerTaskID)
	result := captureSessionComment(t, issueID, map[string]any{"content": "Verification complete"}, workerID, workerTaskID)
	events, err := testHandler.TaskService.ClaimIssueAgentEvents(context.Background(), parseUUID(ownerTaskID), 32)
	if err != nil || len(events) != 2 {
		t.Fatalf("busy owner inbox = %+v, error=%v", events, err)
	}
	for i, commentID := range []string{progress.ID, result.ID} {
		event := events[i]
		if uuidToString(event.SourceID) != commentID || event.Sequence != int64(i+3) || uuidToString(event.SourceTaskID) != workerTaskID || uuidToString(event.OriginatorUserID) != testUserID {
			t.Fatalf("worker report lost order or trusted author provenance: %+v", event)
		}
		if err := testHandler.TaskService.AckIssueAgentEvent(context.Background(), parseUUID(ownerTaskID), event.ID, true, ""); err != nil {
			t.Fatal(err)
		}
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueID); n != 1 {
		t.Fatalf("busy reports spawned separate comment runs: %d", n)
	}
	completeWorkerReportOwner(t, ownerTaskID)
	if n := countAgentCommentsForIssue(t, issueID, ownerID); n != 0 {
		t.Fatalf("owner completion published an implicit response: %d", n)
	}
	later := captureSessionComment(t, issueID, map[string]any{"content": "Additional verification evidence"}, workerID, workerTaskID)
	captureSessionComment(t, issueID, map[string]any{"content": "One more detail before the owner resumes", "parent_id": rootID}, workerID, workerTaskID)
	var resumedSessionID, resumedTaskID, providerID string
	dbfx.QueryRow(t, "SELECT id,active_task_id,provider_session_id FROM issue_agent_session WHERE issue_id=$1", issueID).Scan(&resumedSessionID, &resumedTaskID, &providerID)
	if resumedSessionID != sessionID || resumedTaskID == ownerTaskID || providerID != "owner-provider-thread" {
		t.Fatalf("idle report lost session continuity: session=%s task=%s provider=%s", resumedSessionID, resumedTaskID, providerID)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status='queued'", issueID); n != 1 {
		t.Fatalf("idle reports created %d queued executions, want one session resume", n)
	}
	dbfx.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", resumedTaskID)
	events, err = testHandler.TaskService.ClaimIssueAgentEvents(context.Background(), parseUUID(resumedTaskID), 32)
	if err != nil || len(events) != 2 || uuidToString(events[0].SourceID) != later.ID || events[0].Sequence != 5 || events[1].Sequence != 6 {
		t.Fatalf("resumed inbox replayed old reports or lost new reports: %+v, %v", events, err)
	}
}

func TestWorkerReportRacingOwnerCompletionKeepsOneDurableInboxEvent(t *testing.T) {
	for attempt := 0; attempt < 4; attempt++ {
		t.Run(fmt.Sprint(attempt), func(t *testing.T) {
			issueID, _, sessionID, ownerTaskID, workerID, workerTaskID, _ := runningWorkerReportSession(t)
			start := make(chan struct{})
			completed := make(chan error, 1)
			posted := make(chan *httptest.ResponseRecorder, 1)
			go func() {
				<-start
				_, _, err := testHandler.TaskService.CompleteTaskWithTransition(context.Background(), parseUUID(ownerTaskID), json.RawMessage(`{"output":"Private final transcript"}`), "owner-provider-thread", "/tmp/worker-report-owner", "", false, "", "/tmp/worker-report-owner")
				completed <- err
			}()
			go func() {
				<-start
				req := withURLParam(newRequest(http.MethodPost, "/comments", map[string]any{"content": "Result committed at the turn boundary"}), "id", issueID)
				req.Header.Set("X-Agent-ID", workerID)
				req.Header.Set("X-Task-ID", workerTaskID)
				req.Header.Set("X-Actor-Source", "task_token")
				recorder := httptest.NewRecorder()
				testHandler.CreateComment(recorder, req)
				posted <- recorder
			}()
			close(start)
			completionErr := <-completed
			recorder := <-posted
			if completionErr != nil {
				t.Fatal(completionErr)
			}
			if recorder.Code != http.StatusCreated {
				t.Fatalf("worker report = %d: %s", recorder.Code, recorder.Body.String())
			}
			var comment CommentResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &comment); err != nil {
				t.Fatal(err)
			}
			if n := dbfx.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_id=$2 AND status='pending'", sessionID, comment.ID); n != 1 {
				t.Fatalf("completion race lost or duplicated the durable input: %d", n)
			}
			if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running')", issueID); n != 1 {
				t.Fatalf("completion race left %d active executions, want one owner-session resume", n)
			}
		})
	}
}
