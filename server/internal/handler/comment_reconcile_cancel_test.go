package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Stored comment tasks remain readable and cancellable, but their completion
// cannot recreate the retired comment queue. New input uses the session inbox.
func TestCompleteTask_CancelledCommentDoesNotReplayQueuedInput(t *testing.T) {
	for _, inputType := range []string{"member", "agent"} {
		t.Run(inputType, func(t *testing.T) {
			if testHandler == nil {
				t.Skip("database not available")
			}
			ctx := context.Background()

			var agentID, runtimeID string
			dbfx.QueryRow(t,
				`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
				testWorkspaceID).Scan(&agentID, &runtimeID)
			issueID := dbfx.Issue(t, "reconcile explicit cancel fixture", testutil.Cols{
				"status":        "in_progress",
				"number":        999101,
				"assignee_type": "agent",
				"assignee_id":   agentID,
			})
			triggerCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
				"created_at": testutil.Raw("now() - interval '10 minutes'"),
			})
			queuedContent := "reviewer request during run"
			queuedCols := testutil.Cols{
				"created_at": testutil.Raw("now() - interval '1 minute'"),
				"parent_id":  triggerCommentID,
			}
			if inputType == "agent" {
				reviewerID := createHandlerTestAgent(t, "Cancellation reviewer", nil)
				queuedCols["author_type"] = "agent"
				queuedCols["author_id"] = reviewerID
				queuedContent = "[@owner](mention://agent/" + agentID + ") review result"
			}
			queuedCommentID := dbfx.Comment(t, issueID, queuedContent, queuedCols)

			var runningTaskID, queuedTaskID string
			dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id`, agentID, runtimeID, issueID, triggerCommentID).Scan(&runningTaskID)
			dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at)
		VALUES ($1, $2, $3, $4, '{}', 'queued', 0, now() - interval '1 minute')
		RETURNING id`, agentID, runtimeID, issueID, queuedCommentID).Scan(&queuedTaskID)
			t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

			w := httptest.NewRecorder()
			testHandler.CancelTaskByUser(w, cancelTaskByUserRequest(t, testUserID, queuedTaskID))
			if w.Code != http.StatusOK {
				t.Fatalf("CancelTaskByUser: expected 200, got %d: %s", w.Code, w.Body.String())
			}
			var cancelledByType, cancelledByID string
			var delivered []string
			dbfx.QueryRow(t, `
		SELECT cancelled_by_type, cancelled_by_id::text, delivered_comment_ids::text[]
		FROM agent_task_queue WHERE id = $1`, queuedTaskID).
				Scan(&cancelledByType, &cancelledByID, &delivered)
			if cancelledByType != "member" || cancelledByID != testUserID {
				t.Fatalf("cancel actor = %s/%s, want member/%s", cancelledByType, cancelledByID, testUserID)
			}
			if len(delivered) != 0 {
				t.Fatalf("explicit cancellation changed delivered_comment_ids = %v", delivered)
			}

			// The old running task completes after the queued successor was cancelled.
			if w := completeTaskViaHandler(t, runningTaskID, "done"); w.Code != http.StatusOK {
				t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
			}
			if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 0 {
				t.Fatalf("cancelled unchanged comment was replayed into %d pending task(s)", n)
			}

			// A genuinely newer input remains eligible after the cancellation.
			testutil.Call(t, testHandler.CreateComment, withURLParam(
				newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{
					"content":   "new input after cancel",
					"parent_id": triggerCommentID,
				}), "id", issueID)).Want(http.StatusCreated)
			if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
				t.Fatalf("new comment after cancellation created %d pending task(s), want 1", n)
			}

		})
	}
}

// Historical task completion must not recreate the retired comment queue, even
// when the cancelled successor has no explicit cancellation actor.
func TestCompleteTask_AutomaticallyCancelledHistoricalCommentDoesNotReplay(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)
	issueID := dbfx.Issue(t, "reconcile automatic cancel fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999102,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	triggerCommentID := dbfx.Comment(t, issueID, "automatic recovery input", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	queuedCommentID := dbfx.Comment(t, issueID, "automatic recovery input during run", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '1 minute'"),
		"parent_id":  triggerCommentID,
	})
	var runningTaskID, cancelledTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id`, agentID, runtimeID, issueID, triggerCommentID).Scan(&runningTaskID)
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at)
		VALUES ($1, $2, $3, $4, '{}', 'queued', 0, now() - interval '30 seconds')
		RETURNING id`, agentID, runtimeID, issueID, queuedCommentID).Scan(&cancelledTaskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })
	if _, err := testHandler.TaskService.CancelTask(ctx, parseUUID(cancelledTaskID)); err != nil {
		t.Fatalf("automatic CancelTask: %v", err)
	}

	if w := completeTaskViaHandler(t, runningTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 0 {
		t.Fatalf("historical completion replayed automatically cancelled input into %d task(s)", n)
	}
	var status string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id=$1`, cancelledTaskID).Scan(&status)
	if status != "cancelled" {
		t.Fatalf("historical cancellation was overwritten: %s", status)
	}
}

func TestCompleteTask_CancelledHistoricalCommentEditEntersSessionInbox(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)
	issueID := dbfx.Issue(t, "reconcile edited cancel fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999103,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	rootID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	commentID := dbfx.Comment(t, issueID, "reviewer request during run", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '1 minute'"),
		"parent_id":  rootID,
	})
	var runningTaskID, queuedTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id`, agentID, runtimeID, issueID, rootID).Scan(&runningTaskID)
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at)
		VALUES ($1, $2, $3, $4, '{}', 'queued', 0, now() - interval '30 seconds')
		RETURNING id`, agentID, runtimeID, issueID, commentID).Scan(&queuedTaskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })
	w := httptest.NewRecorder()
	testHandler.CancelTaskByUser(w, cancelTaskByUserRequest(t, testUserID, queuedTaskID))
	if w.Code != http.StatusOK {
		t.Fatalf("CancelTaskByUser: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	edit := httptest.NewRecorder()
	testHandler.UpdateComment(edit, withURLParam(
		newRequest(http.MethodPut, "/api/comments/"+commentID, map[string]any{
			"content": "edited reviewer request",
		}), "commentId", commentID))
	if edit.Code != http.StatusOK {
		t.Fatalf("UpdateComment: expected 200, got %d: %s", edit.Code, edit.Body.String())
	}
	if w := completeTaskViaHandler(t, runningTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
		t.Fatalf("edited historical comment has %d pending session execution(s), want 1", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM issue_agent_event e JOIN issue_agent_session s ON s.id=e.session_id WHERE s.issue_id=$1 AND s.agent_id=$2 AND e.source_id=$3 AND e.kind='comment.updated'`, issueID, agentID, commentID); n != 1 {
		t.Fatalf("edited historical comment produced %d session input(s), want one", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2 AND status='queued' AND trigger_comment_id IS NOT NULL`, issueID, agentID); n != 0 {
		t.Fatalf("edited historical comment recreated %d old comment task(s)", n)
	}
}

// A cancellation that predates the completing task belongs to an earlier
// attempt. It must not suppress a later explicit retry of the same comment.
func TestListReconcilableComments_HistoricalExplicitCancelDoesNotSuppressLaterRun(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)
	issueID := dbfx.Issue(t, "historical cancel does not suppress retry", testutil.Cols{
		"status":        "in_progress",
		"number":        999104,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	commentID := dbfx.Comment(t, issueID, "retry this exact input", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '3 hours'"),
	})
	dbfx.Exec(t, `UPDATE comment SET updated_at = now() - interval '2 hours' WHERE id = $1`, commentID)
	dbfx.Exec(t, `
		INSERT INTO agent_task_queue (
			agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids,
			status, priority, created_at, completed_at, cancelled_by_type, cancelled_by_id
		) VALUES ($1, $2, $3, $4, '{}', 'cancelled', 0,
			now() - interval '2 hours', now() - interval '90 minutes', 'member', $5)`,
		agentID, runtimeID, issueID, commentID, testUserID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	comments, err := testHandler.Queries.ListReconcilableCommentsForIssueSince(ctx, db.ListReconcilableCommentsForIssueSinceParams{
		IssueID:           parseUUID(issueID),
		AgentID:           parseUUID(agentID),
		PlannedCommentIds: []pgtype.UUID{parseUUID(commentID)},
		Since:             pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
	})
	if err != nil {
		t.Fatalf("ListReconcilableCommentsForIssueSince: %v", err)
	}
	if len(comments) != 1 || comments[0].ID != parseUUID(commentID) {
		t.Fatalf("historical explicit cancel suppressed later retry: got %d comments", len(comments))
	}
}
