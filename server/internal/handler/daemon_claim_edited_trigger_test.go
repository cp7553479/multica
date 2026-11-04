package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Stored comment plans still expose edit metadata when read. Active issue
// sessions deliver each new revision as an ordered event without cancelling
// the conversation that already read the previous revision.

func TestTriggerCommentEditedSince(t *testing.T) {
	start := time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) pgtype.Timestamptz {
		return pgtype.Timestamptz{Time: start.Add(d), Valid: true}
	}
	runStarted := ts(0)

	cases := []struct {
		name    string
		comment db.Comment
		started pgtype.Timestamptz
		want    bool
	}{
		{
			name:    "edited after the run started",
			comment: db.Comment{CreatedAt: ts(-time.Minute), UpdatedAt: ts(time.Minute), Revision: 2},
			started: runStarted,
			want:    true,
		},
		{
			name:    "never edited",
			comment: db.Comment{CreatedAt: ts(-time.Minute), UpdatedAt: ts(-time.Minute), Revision: 1},
			started: runStarted,
		},
		{
			name:    "edited before the run started: the run already read the new text",
			comment: db.Comment{CreatedAt: ts(-2 * time.Minute), UpdatedAt: ts(-time.Minute), Revision: 2},
			started: runStarted,
		},
		{
			name:    "created after the run started: the session never saw any version",
			comment: db.Comment{CreatedAt: ts(time.Minute), UpdatedAt: ts(2 * time.Minute), Revision: 2},
			started: runStarted,
		},
		{
			name: "only change is a single resolve",
			comment: db.Comment{
				CreatedAt: ts(-time.Minute), UpdatedAt: ts(time.Minute), Revision: 2,
				ResolvedAt: ts(time.Minute),
			},
			started: runStarted,
		},
		{
			name: "edit then resolve: the resolve must not hide the edit",
			comment: db.Comment{
				CreatedAt: ts(-time.Minute), UpdatedAt: ts(2 * time.Minute), Revision: 3,
				ResolvedAt: ts(2 * time.Minute),
			},
			started: runStarted,
			want:    true,
		},
		{
			name: "edited after an earlier resolve",
			comment: db.Comment{
				CreatedAt: ts(-time.Minute), UpdatedAt: ts(2 * time.Minute), Revision: 3,
				ResolvedAt: ts(time.Minute),
			},
			started: runStarted,
			want:    true,
		},
		{
			name:    "resumed run never started",
			comment: db.Comment{CreatedAt: ts(-time.Minute), UpdatedAt: ts(time.Minute), Revision: 2},
			started: pgtype.Timestamptz{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := triggerCommentEditedSince(tc.comment, tc.started); got != tc.want {
				t.Fatalf("triggerCommentEditedSince = %v, want %v", got, tc.want)
			}
		})
	}
}

type claimEditedTriggerResp struct {
	Task *struct {
		PriorSessionID         string `json:"prior_session_id"`
		TriggerCommentID       string `json:"trigger_comment_id"`
		TriggerCommentContent  string `json:"trigger_comment_content"`
		TriggerCommentEditedAt string `json:"trigger_comment_edited_at"`
		TriggerCommentRevision int64  `json:"trigger_comment_revision"`
	} `json:"task"`
}

func claimEditedTriggerTask(t *testing.T, runtimeID string) claimEditedTriggerResp {
	t.Helper()
	req := newOwnerDaemonRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil)
	req = withURLParam(req, "runtimeId", runtimeID)
	w := testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK)
	var resp claimEditedTriggerResp
	w.JSON(&resp)
	if resp.Task == nil {
		t.Fatalf("expected a claimed task, got nil: %s", w.Body.String())
	}
	return resp
}

// TestClaimTask_FlagsTriggerEditedAfterResumedCancelledRun is the reported
// scenario: the resumed session belongs to the CANCELLED run that read X.
func TestClaimTask_FlagsTriggerEditedAfterResumedCancelledRun(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createClaimReclaimRuntime(t, ctx, "Edited trigger runtime")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Edited trigger agent")

	commentID := dbfx.Comment(t, issueID, "Now it connect", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
		"updated_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	dbfx.Task(t, agentID, testutil.Cols{
		"runtime_id":         runtimeID,
		"issue_id":           issueID,
		"trigger_comment_id": commentID,
		"status":             "cancelled",
		"session_id":         "edited-trigger-cancelled-session",
		"started_at":         testutil.Raw("now() - interval '9 minutes'"),
		"completed_at":       testutil.Raw("now() - interval '5 minutes'"),
	})
	// The edit: new body, revision bump, updated_at after the run started.
	dbfx.Exec(t, `UPDATE comment SET content = 'Now it connects over TLS', revision = revision + 1, updated_at = now() - interval '5 minutes' WHERE id = $1`, commentID)
	dbfx.Task(t, agentID, testutil.Cols{
		"runtime_id":         runtimeID,
		"issue_id":           issueID,
		"trigger_comment_id": commentID,
	})

	resp := claimEditedTriggerTask(t, runtimeID)
	if resp.Task.PriorSessionID != "edited-trigger-cancelled-session" {
		t.Fatalf("PriorSessionID = %q, want the cancelled run's session", resp.Task.PriorSessionID)
	}
	if resp.Task.TriggerCommentContent != "Now it connects over TLS" {
		t.Fatalf("TriggerCommentContent = %q, want the edited body", resp.Task.TriggerCommentContent)
	}
	if resp.Task.TriggerCommentEditedAt == "" {
		t.Fatal("TriggerCommentEditedAt is empty, want the edit time: the resumed history quotes the old text")
	}
	if resp.Task.TriggerCommentRevision != 2 {
		t.Fatalf("TriggerCommentRevision = %d, want 2", resp.Task.TriggerCommentRevision)
	}
}

// TestClaimTask_NoEditFlagWhenTriggerUnchangedSinceResumedRun guards the
// common follow-up: nothing was edited, so nothing is flagged.
func TestClaimTask_NoEditFlagWhenTriggerUnchangedSinceResumedRun(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createClaimReclaimRuntime(t, ctx, "Unedited trigger runtime")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Unedited trigger agent")

	dbfx.Task(t, agentID, testutil.Cols{
		"runtime_id":   runtimeID,
		"issue_id":     issueID,
		"status":       "completed",
		"session_id":   "unedited-trigger-session",
		"started_at":   testutil.Raw("now() - interval '1 hour'"),
		"completed_at": testutil.Raw("now() - interval '50 minutes'"),
	})
	createCommentTriggeredClaimTask(t, ctx, agentID, runtimeID, issueID, nil)

	resp := claimEditedTriggerTask(t, runtimeID)
	if resp.Task.PriorSessionID != "unedited-trigger-session" {
		t.Fatalf("PriorSessionID = %q, want unedited-trigger-session", resp.Task.PriorSessionID)
	}
	if resp.Task.TriggerCommentEditedAt != "" || resp.Task.TriggerCommentRevision != 0 {
		t.Fatalf("edit fields = (%q, %d), want empty for a new, unedited comment",
			resp.Task.TriggerCommentEditedAt, resp.Task.TriggerCommentRevision)
	}
}

// The edit is delivered to the same live session, whose history already quotes
// the previous version. Its event carries the exact source revision.
func TestUpdateComment_EditedTriggerReachesResumedRun(t *testing.T) {
	issueID, sessionID, taskID, commentID := runningEditedCommentSessionFixture(t)
	for i, body := range []string{"edited instruction Y", "edited instruction Z"} {
		revision := int64(i + 2)
		req := withURLParam(newRequest(http.MethodPut, "/api/comments/"+commentID, map[string]any{
			"content": body, "expected_revision": revision - 1,
		}), "commentId", commentID)
		testutil.Call(t, testHandler.UpdateComment, req).Want(http.StatusOK)
		if got := taskStatus(t, taskID); got != "running" {
			t.Fatalf("edit %d interrupted the live session: status=%s", i, got)
		}

		event := claimEditedCommentSessionEvent(t, taskID)
		var payload struct {
			CommentID string `json:"comment_id"`
			Content   string `json:"content"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if event.SessionID != parseUUID(sessionID) || event.SourceID != parseUUID(commentID) || event.Kind != "comment.updated" || event.SourceRevision != revision || payload.CommentID != commentID || payload.Content != body {
			t.Fatalf("edit %d input=%+v payload=%+v", i, event, payload)
		}
		if event.ActorType != "member" || event.ActorID != parseUUID(testUserID) {
			t.Fatalf("edit lost its authenticated author: %s/%s", event.ActorType, uuidToString(event.ActorID))
		}
		if err := testHandler.TaskService.AckIssueAgentEvent(t.Context(), parseUUID(taskID), event.ID, true, ""); err != nil {
			t.Fatal(err)
		}
	}
	var provider, activeTask string
	dbfx.QueryRow(t, `SELECT provider_session_id,active_task_id FROM issue_agent_session WHERE id=$1`, sessionID).Scan(&provider, &activeTask)
	if provider != "persistent-provider-thread" || activeTask != taskID {
		t.Fatalf("edit replaced session ownership: provider=%s active_task=%s", provider, activeTask)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND status IN ('queued','dispatched','running')`, issueID); n != 1 {
		t.Fatalf("edits created %d active executions, want the same running execution", n)
	}
	if next, err := testHandler.TaskService.ClaimIssueAgentEvent(t.Context(), parseUUID(taskID)); err != nil || next != nil {
		t.Fatalf("acknowledged edits were replayed: event=%v error=%v", next, err)
	}
}

func TestClaimTask_EditThenResolveKeepsEditFlag(t *testing.T) {
	_, sessionID, taskID, commentID := runningEditedCommentSessionFixture(t)
	req := withURLParam(newRequest(http.MethodPut, "/api/comments/"+commentID, map[string]any{
		"content": "edited instruction Y", "expected_revision": 1,
	}), "commentId", commentID)
	testutil.Call(t, testHandler.UpdateComment, req).Want(http.StatusOK)
	resolved := resolveCommentHTTP(t, commentID)
	if resolved.Revision != 3 || resolved.ResolvedAt == nil {
		t.Fatalf("resolve did not preserve the edited row: %+v", resolved)
	}
	event := claimEditedCommentSessionEvent(t, taskID)
	var payload struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if event.Kind != "comment.updated" || event.SourceRevision != 2 || payload.Content != "edited instruction Y" {
		t.Fatalf("resolve masked or rewrote the prior edit: event=%+v content=%s", event, payload.Content)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_id=$2 AND kind='comment.updated'`, sessionID, commentID); n != 1 {
		t.Fatalf("resolve manufactured %d edit inputs, want one semantic content edit", n)
	}
	if got := taskStatus(t, taskID); got != "running" {
		t.Fatalf("resolve interrupted live session: %s", got)
	}
}

func TestClaimTask_FlagsEditedCoalescedComment(t *testing.T) {
	issueID, sessionID, taskID, commentID := runningEditedCommentSessionFixture(t)
	req := withURLParam(newRequest(http.MethodPut, "/api/comments/"+commentID, map[string]any{
		"content": "edited instruction Y", "expected_revision": 1,
	}), "commentId", commentID)
	testutil.Call(t, testHandler.UpdateComment, req).Want(http.StatusOK)
	var reply CommentResponse
	req = withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{
		"content": "Please continue with the current instructions.", "parent_id": commentID,
	}), "id", issueID)
	testutil.Call(t, testHandler.CreateComment, req).Want(http.StatusCreated).JSON(&reply)

	var previousSequence int64
	for i, want := range []struct {
		kind, source, body, parent string
		revision                   int64
	}{
		{kind: "comment.updated", source: commentID, body: "edited instruction Y", revision: 2},
		{kind: "comment.created", source: reply.ID, body: reply.Content, parent: commentID, revision: 1},
	} {
		event := claimEditedCommentSessionEvent(t, taskID)
		var payload struct {
			Content         string `json:"content"`
			ParentCommentID string `json:"parent_comment_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if event.SessionID != parseUUID(sessionID) || event.Kind != want.kind || uuidToString(event.SourceID) != want.source || event.SourceRevision != want.revision || payload.Content != want.body || payload.ParentCommentID != want.parent || event.Sequence <= previousSequence {
			t.Fatalf("ordered input %d=%+v payload=%+v want=%+v", i, event, payload, want)
		}
		previousSequence = event.Sequence
		if err := testHandler.TaskService.AckIssueAgentEvent(t.Context(), parseUUID(taskID), event.ID, true, ""); err != nil {
			t.Fatal(err)
		}
	}
	if next, err := testHandler.TaskService.ClaimIssueAgentEvent(t.Context(), parseUUID(taskID)); err != nil || next != nil {
		t.Fatalf("edit/reply duplicated input: event=%v error=%v", next, err)
	}
}

func runningEditedCommentSessionFixture(t *testing.T) (issueID, sessionID, taskID, commentID string) {
	t.Helper()
	issueID, sessionID, taskID, _, _ = issueAgentSessionFixture(t)
	commentID = dbfx.Comment(t, issueID, "latest instruction")
	// Represent the same provider turn after it received the assignment and
	// original comment; later mutations must enter this turn's event inbox.
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='running',started_at=now()-interval '30 seconds',session_id='persistent-provider-thread' WHERE id=$1`, taskID)
	dbfx.Exec(t, `UPDATE issue_agent_event SET status='delivered',delivery_task_id=$2,delivered_at=now() WHERE session_id=$1`, sessionID, taskID)
	return
}

func claimEditedCommentSessionEvent(t *testing.T, taskID string) db.IssueAgentEvent {
	t.Helper()
	event, err := testHandler.TaskService.ClaimIssueAgentEvent(t.Context(), parseUUID(taskID))
	if err != nil || event == nil {
		t.Fatalf("claim session event: event=%v error=%v", event, err)
	}
	return *event
}
