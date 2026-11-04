package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type supplementFixture struct {
	runtimeID string
	agentID   string
	issueID   string
	taskID    string
	triggerID string
}

func TestStableTaskSupplementFailureReason(t *testing.T) {
	for _, reason := range []string{
		protocol.TaskSupplementFailureTurnNotStarted,
		protocol.TaskSupplementFailureProviderRejected,
		protocol.TaskSupplementFailureTimeout,
		protocol.TaskSupplementFailureTurnEnded,
	} {
		if got := stableTaskSupplementFailureReason(reason); got != reason {
			t.Fatalf("stableTaskSupplementFailureReason(%q) = %q", reason, got)
		}
	}
	for _, raw := range []string{"", "供应商错误：无法发送", strings.Repeat("界", 600)} {
		if got := stableTaskSupplementFailureReason(raw); got != protocol.TaskSupplementFailureProviderRejected {
			t.Fatalf("raw reason %q escaped as %q", raw, got)
		}
	}
}

func newSupplementFixture(t *testing.T, provider, status string, negotiated bool) supplementFixture {
	t.Helper()
	runtimeID := dbfx.Runtime(t, "supplement-"+provider, testutil.Cols{"provider": provider})
	agentID := dbfx.Agent(t, "Supplement "+provider, runtimeID)
	issueID := dbfx.Issue(t, "supplement "+provider)
	triggerID := dbfx.Comment(t, issueID, "original objective")
	taskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id": issueID, "runtime_id": runtimeID, "trigger_comment_id": triggerID,
		"status": status, "started_at": testutil.Raw("CASE WHEN '" + status + "' = 'running' THEN now() ELSE NULL END"),
	})
	if negotiated {
		dbfx.Exec(t, `INSERT INTO task_supplement_capability (task_id, workspace_id, issue_id, capability) VALUES ($1, $2, $3, $4)`,
			taskID, testWorkspaceID, issueID, protocol.DaemonCapabilityTaskSupplementV1)
		dbfx.Cleanup(t, `DELETE FROM task_supplement_capability WHERE task_id=$1`, taskID)
	}
	dbfx.Cleanup(t, `DELETE FROM task_supplement WHERE task_id=$1`, taskID)
	return supplementFixture{runtimeID: runtimeID, agentID: agentID, issueID: issueID, taskID: taskID, triggerID: triggerID}
}

// Historical receipts are inserted as persisted upgrade data. No live endpoint
// can create or retry them after issue sessions replace per-run routing.
func insertHistoricalTaskSupplement(t *testing.T, f supplementFixture, content, status, authorType, authorID string) string {
	t.Helper()
	commentID := dbfx.Comment(t, f.issueID, content, testutil.Cols{"author_type": authorType, "author_id": authorID})
	dbfx.Exec(t, `INSERT INTO task_supplement (task_id,workspace_id,issue_id,comment_id,author_id,client_request_id,status)
		VALUES ($1,$2,$3,$4,$5,$4,$6)`, f.taskID, testWorkspaceID, f.issueID, commentID, authorID, status)
	return commentID
}

func supplementRequest(t *testing.T, fixture supplementFixture, requestID, content string) *testutil.Response {
	t.Helper()
	req := newRequest(http.MethodPost, "/api/issues/"+fixture.issueID+"/tasks/"+fixture.taskID+"/supplements", map[string]any{
		"client_request_id": requestID, "content": content,
	})
	return testutil.Call(t, testHandler.CreateTaskSupplement,
		withURLParams(req, "id", fixture.issueID, "taskId", fixture.taskID))
}

func TestTaskSupplementLegacyWritesRequireIssueSessionComments(t *testing.T) {
	for _, session := range []bool{false, true} {
		for _, status := range []string{"queued", "running", "completed", "failed", "cancelled"} {
			t.Run(fmt.Sprintf("session=%v/%s", session, status), func(t *testing.T) {
				f := newSupplementFixture(t, "codex", status, true)
				if session {
					dbfx.Exec(t, `UPDATE agent_task_queue SET context='{"type":"issue_session","issue_agent_session_id":"0199a4e8-22ce-7b01-bba5-111111111111"}'::jsonb WHERE id=$1`, f.taskID)
				}
				commentID := insertHistoricalTaskSupplement(t, f, "Earlier delivery failed", "failed", "member", testUserID)
				var created, retried map[string]any
				supplementRequest(t, f, "not-a-request-id", "must not be stored").Want(http.StatusConflict).JSON(&created)
				req := withURLParams(newRequest(http.MethodPost, "/retry", nil), "id", f.issueID, "taskId", f.taskID, "commentId", commentID)
				testutil.Call(t, testHandler.RetryTaskSupplement, req).Want(http.StatusConflict).JSON(&retried)
				if created["code"] != "issue_session_comment_required" || retried["code"] != "issue_session_comment_required" {
					t.Fatalf("legacy write errors = create %+v, retry %+v", created, retried)
				}
				if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, f.issueID); n != 2 {
					t.Fatalf("rejected writes changed comments: %d, want 2", n)
				}
				if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1 AND status='failed'`, f.taskID); n != 1 {
					t.Fatalf("rejected writes changed historical receipts: %d", n)
				}
				if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, f.issueID); n != 1 {
					t.Fatalf("rejected writes created executions: %d", n)
				}
			})
		}
	}
}

func TestTaskSupplementLegacyWritesPreserveAuthorization(t *testing.T) {
	f := newSupplementFixture(t, "codex", "running", true)
	otherUser := dbfx.Insert(t, "user", testutil.Cols{"name": "No Invoke", "email": "no-invoke-supplement@example.test"})
	dbfx.Member(t, testWorkspaceID, otherUser, "member")
	foreignIssueID, foreignTaskID := setupForeignWorkspaceFixture(t)
	otherIssue := dbfx.Issue(t, "Different task issue")
	for _, endpoint := range []struct {
		name string
		call http.HandlerFunc
	}{{"create", testHandler.CreateTaskSupplement}, {"retry", testHandler.RetryTaskSupplement}} {
		t.Run(endpoint.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, user, issue, task string
				want                    int
			}{
				{"invoke denied", otherUser, f.issueID, f.taskID, http.StatusForbidden},
				{"foreign workspace", testUserID, foreignIssueID, foreignTaskID, http.StatusNotFound},
				{"wrong issue", testUserID, otherIssue, f.taskID, http.StatusNotFound},
				{"invalid task", testUserID, f.issueID, "not-a-uuid", http.StatusBadRequest},
			} {
				t.Run(tc.name, func(t *testing.T) {
					req := withURLParams(newRequestAs(tc.user, http.MethodPost, "/supplements", nil), "id", tc.issue, "taskId", tc.task, "commentId", f.triggerID)
					testutil.Call(t, endpoint.call, req).Want(tc.want)
				})
			}
		})
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1`, f.taskID); n != 0 {
		t.Fatalf("denied writes created %d receipts", n)
	}
}

func TestTaskSupplementSessionTasksRejectLegacyDaemonTransport(t *testing.T) {
	f := newSupplementFixture(t, "codex", "running", true)
	dbfx.Exec(t, `UPDATE agent_task_queue SET context='{"type":"issue_session","issue_agent_session_id":"0199a4e8-22ce-7b01-bba5-111111111111"}'::jsonb WHERE id=$1`, f.taskID)
	commentID := insertHistoricalTaskSupplement(t, f, "Old endpoint must not deliver this", "pending", "member", testUserID)
	for _, endpoint := range []struct {
		name string
		call http.HandlerFunc
	}{{"claim", testHandler.ClaimTaskSupplement}, {"ack", testHandler.AckTaskSupplement}} {
		t.Run(endpoint.name, func(t *testing.T) {
			var out map[string]any
			req := withURLParams(newOwnerDaemonRequest(http.MethodPost, "/supplements", map[string]any{"delivered": true}), "taskId", f.taskID, "commentId", commentID)
			testutil.Call(t, endpoint.call, req).Want(http.StatusConflict).JSON(&out)
			if out["code"] != "issue_session_events_required" {
				t.Fatalf("legacy transport error = %+v", out)
			}
		})
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1 AND status='pending' AND attempt_count=0`, f.taskID); n != 1 {
		t.Fatalf("new session used legacy transport: %d unchanged receipts", n)
	}
}

func TestTaskSupplementHistoricalDaemonAccessIsRuntimeScoped(t *testing.T) {
	f := newSupplementFixture(t, "codex", "running", true)
	commentID := insertHistoricalTaskSupplement(t, f, "Historical input", "delivering", "member", testUserID)
	otherUser := dbfx.Insert(t, "user", testutil.Cols{"name": "Other Runtime Member", "email": "other-supplement-runtime@example.test"})
	dbfx.Member(t, testWorkspaceID, otherUser, "member")
	for _, endpoint := range []struct {
		name string
		call http.HandlerFunc
	}{{"claim", testHandler.ClaimTaskSupplement}, {"ack", testHandler.AckTaskSupplement}} {
		t.Run(endpoint.name, func(t *testing.T) {
			for _, req := range []*http.Request{
				newRequestAs(otherUser, http.MethodPost, "/supplements", map[string]any{"delivered": true}),
				newDaemonTokenRequest(http.MethodPost, "/supplements", map[string]any{"delivered": true}, testWorkspaceID, "different-daemon"),
				newDaemonTokenRequest(http.MethodPost, "/supplements", map[string]any{"delivered": true}, "0199a4e8-22ce-7b01-bba5-777777777777", "different-workspace"),
			} {
				testutil.Call(t, endpoint.call, withURLParams(req, "taskId", f.taskID, "commentId", commentID)).Want(http.StatusNotFound)
			}
		})
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1 AND status='delivering'`, f.taskID); n != 1 {
		t.Fatalf("unauthorized daemon changed historical receipt: %d", n)
	}
}

func TestTaskSupplementHistoricalAckSanitizesPrivateProviderErrors(t *testing.T) {
	f := newSupplementFixture(t, "codex", "running", true)
	commentID := insertHistoricalTaskSupplement(t, f, "Historical input", "pending", "member", testUserID)
	var claim map[string]string
	req := withURLParam(newOwnerDaemonRequest(http.MethodPost, "/claim", map[string]any{}), "taskId", f.taskID)
	testutil.Call(t, testHandler.ClaimTaskSupplement, req).Want(http.StatusOK).JSON(&claim)
	if claim["comment_id"] != commentID || claim["author_type"] != "member" {
		t.Fatalf("historical claim = %+v", claim)
	}
	ack := withURLParams(newOwnerDaemonRequest(http.MethodPost, "/ack", map[string]any{"delivered": false, "error": "private provider credential diagnostic"}), "taskId", f.taskID, "commentId", commentID)
	var receipt taskSupplementReceiptResponse
	testutil.Call(t, testHandler.AckTaskSupplement, ack).Want(http.StatusOK).JSON(&receipt)
	if receipt.Status != "failed" || receipt.FailureReason == nil || *receipt.FailureReason != protocol.TaskSupplementFailureProviderRejected {
		t.Fatalf("historical receipt leaked provider diagnostics: %+v", receipt)
	}
	stored, err := testHandler.Queries.GetTaskSupplementForRun(t.Context(), db.GetTaskSupplementForRunParams{
		CommentID: parseUUID(commentID), TaskID: parseUUID(f.taskID), WorkspaceID: parseUUID(testWorkspaceID),
	})
	if err != nil || stored.FailureReason.String != protocol.TaskSupplementFailureProviderRejected {
		t.Fatalf("stored receipt = %+v: %v", stored, err)
	}
	// Another task cannot acknowledge the same comment's historical receipt.
	otherTask := dbfx.Task(t, f.agentID, testutil.Cols{"issue_id": f.issueID, "runtime_id": f.runtimeID, "status": "running"})
	wrong := withURLParams(newOwnerDaemonRequest(http.MethodPost, "/ack", map[string]any{"delivered": true}), "taskId", otherTask, "commentId", commentID)
	testutil.Call(t, testHandler.AckTaskSupplement, wrong).Want(http.StatusConflict)
}

func TestTaskSupplementHistoricalReceiptSettlesOnTerminalTransition(t *testing.T) {
	for _, transition := range []string{"complete", "fail", "cancel"} {
		t.Run(transition, func(t *testing.T) {
			f := newSupplementFixture(t, "codex", "running", true)
			commentID := insertHistoricalTaskSupplement(t, f, "Unread historical input", "pending", "member", testUserID)
			switch transition {
			case "complete":
				if w := completeTaskViaHandler(t, f.taskID, "done"); w.Code != http.StatusOK {
					t.Fatalf("complete: %s", w.Body.String())
				}
			case "fail":
				if w := failTaskViaHandler(t, f.taskID); w.Code != http.StatusOK {
					t.Fatalf("fail: %s", w.Body.String())
				}
			case "cancel":
				req := withURLParams(newRequest(http.MethodPost, "/cancel", nil), "id", f.issueID, "taskId", f.taskID)
				testutil.Call(t, testHandler.CancelTask, req).Want(http.StatusOK)
			}
			var status, reason string
			dbfx.QueryRow(t, `SELECT status,failure_reason FROM task_supplement WHERE task_id=$1 AND comment_id=$2`, f.taskID, commentID).Scan(&status, &reason)
			if status != "failed" || reason != protocol.TaskSupplementFailureTurnEnded {
				t.Fatalf("terminal historical receipt = %s/%s", status, reason)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, f.issueID); n != 1 {
				t.Fatalf("historical input created %d executions", n)
			}
		})
	}
}

func TestTaskSupplementTerminalWriteRollsBackWhenSettlementFails(t *testing.T) {
	f := newSupplementFixture(t, "codex", "running", true)
	commentID := insertHistoricalTaskSupplement(t, f, "Must settle atomically", "pending", "member", testUserID)
	dbfx.Exec(t, `CREATE OR REPLACE FUNCTION reject_task_supplement_settlement() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'settlement rejected'; END; $$ LANGUAGE plpgsql;
		CREATE TRIGGER reject_task_supplement_settlement BEFORE UPDATE OF status ON task_supplement
		FOR EACH ROW EXECUTE FUNCTION reject_task_supplement_settlement()`)
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DROP TRIGGER IF EXISTS reject_task_supplement_settlement ON task_supplement`)
		_, _ = testPool.Exec(context.Background(), `DROP FUNCTION IF EXISTS reject_task_supplement_settlement()`)
	})
	if w := completeTaskViaHandler(t, f.taskID, "done"); w.Code != http.StatusInternalServerError {
		t.Fatalf("complete with rejected settlement: %d: %s", w.Code, w.Body.String())
	}
	var taskStatus, receiptStatus string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1`, f.taskID).Scan(&taskStatus)
	dbfx.QueryRow(t, `SELECT status FROM task_supplement WHERE comment_id=$1`, commentID).Scan(&receiptStatus)
	if taskStatus != "running" || receiptStatus != "pending" {
		t.Fatalf("partial terminal commit: task=%s receipt=%s", taskStatus, receiptStatus)
	}
}

func TestTaskSupplementCapabilityDoesNotBreakNonIssueStarts(t *testing.T) {
	for _, provider := range []string{"codex", "claude", "grok", "cursor", "kiro"} {
		t.Run(provider, func(t *testing.T) {
			runtimeID := dbfx.Runtime(t, "supplement-no-issue", testutil.Cols{"provider": provider})
			agentID := dbfx.Agent(t, "Supplement no issue", runtimeID)
			taskID := dbfx.Task(t, agentID, testutil.Cols{"runtime_id": runtimeID, "issue_id": nil, "status": "dispatched"})
			started, err := testHandler.TaskService.StartTask(t.Context(), parseUUID(taskID), service.TaskStartCapabilities{Supplement: true})
			if err != nil || started.Status != "running" {
				t.Fatalf("non-issue start = %#v: %v", started, err)
			}
			if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement_capability WHERE task_id=$1`, taskID); n != 0 {
				t.Fatalf("non-issue capability rows = %d", n)
			}
		})
	}
}
