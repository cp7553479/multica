package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Deleting a comment removes only that comment (#8296). These tests pin the
// three outcomes: a comment with replies becomes a tombstone that keeps them
// attached, a comment without replies is removed, and a tombstone is removed
// once its last reply is gone.

type commentRowState struct {
	exists   bool
	content  string
	parentID string
	deleted  bool
	resolved bool
}

func loadCommentRowState(t *testing.T, id string) commentRowState {
	t.Helper()
	var state commentRowState
	var parentID *string
	var deletedAt, resolvedAt *time.Time
	err := testPool.QueryRow(context.Background(),
		`SELECT content, parent_id::text, deleted_at, resolved_at FROM comment WHERE id = $1`, id,
	).Scan(&state.content, &parentID, &deletedAt, &resolvedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return commentRowState{}
	}
	if err != nil {
		t.Fatalf("load comment %s: %v", id, err)
	}
	state.exists = true
	if parentID != nil {
		state.parentID = *parentID
	}
	state.deleted = deletedAt != nil
	state.resolved = resolvedAt != nil
	return state
}

// commentEventRecorder is a handler copy whose bus records the comment events
// a delete publishes, in order.
type commentEventRecorder struct {
	h      *Handler
	events []events.Event
}

func newCommentEventRecorder() *commentEventRecorder {
	h := *testHandler
	h.Bus = events.New()
	rec := &commentEventRecorder{h: &h}
	for _, eventType := range []string{protocol.EventCommentUpdated, protocol.EventCommentDeleted} {
		h.Bus.Subscribe(eventType, func(event events.Event) {
			rec.events = append(rec.events, event)
		})
	}
	return rec
}

func (rec *commentEventRecorder) delete(t *testing.T, commentID string) *testutil.Response {
	t.Helper()
	rec.events = nil
	req := withURLParam(newRequest(http.MethodDelete, "/api/comments/"+commentID, nil), "commentId", commentID)
	return testutil.Call(t, rec.h.DeleteComment, req)
}

// deletedIDs returns the comment ids of the recorded comment:deleted events.
func (rec *commentEventRecorder) deletedIDs() []string {
	var ids []string
	for _, event := range rec.events {
		if event.Type == protocol.EventCommentDeleted {
			ids = append(ids, event.Payload.(map[string]any)["comment_id"].(string))
		}
	}
	return ids
}

func TestDeleteCommentWithRepliesLeavesTombstone(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	issueID := dbfx.Issue(t, "delete keeps replies")
	// A → B → C → E, plus A → D.
	a := dbfx.Comment(t, issueID, "root A")
	b := dbfx.Comment(t, issueID, "reply B", testutil.Cols{"parent_id": a})
	c := dbfx.Comment(t, issueID, "reply C", testutil.Cols{"parent_id": b})
	e := dbfx.Comment(t, issueID, "reply E", testutil.Cols{"parent_id": c})
	d := dbfx.Comment(t, issueID, "reply D", testutil.Cols{"parent_id": a})
	dbfx.Insert(t, "attachment", testutil.Cols{
		"workspace_id": testWorkspaceID, "issue_id": issueID, "comment_id": b,
		"uploader_type": "member", "uploader_id": testUserID,
		"filename": "b.png", "url": "https://example.test/b.png", "content_type": "image/png", "size_bytes": 1,
	})
	dbfx.Insert(t, "comment_reaction", testutil.Cols{
		"comment_id": b, "workspace_id": testWorkspaceID, "actor_type": "member", "actor_id": testUserID, "emoji": "👍",
	})
	dbfx.Exec(t, `UPDATE comment SET resolved_at = now(), resolved_by_type = 'member', resolved_by_id = $2 WHERE id = $1`, b, testUserID)

	rec := newCommentEventRecorder()
	rec.delete(t, b).Want(http.StatusNoContent)

	if got := loadCommentRowState(t, b); !got.exists || !got.deleted || got.content != "" || got.resolved || got.parentID != a {
		t.Fatalf("B = %+v, want a tombstone under A with its body and resolution cleared", got)
	}
	for id, content := range map[string]string{a: "root A", c: "reply C", e: "reply E", d: "reply D"} {
		if got := loadCommentRowState(t, id); !got.exists || got.deleted || got.content != content {
			t.Fatalf("comment %q = %+v, want it untouched", content, got)
		}
	}
	if got := loadCommentRowState(t, c); got.parentID != b {
		t.Fatalf("C parent = %s, want it still attached to B", got.parentID)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM attachment WHERE comment_id = $1`, b); n != 0 {
		t.Fatalf("tombstone kept %d attachments", n)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment_reaction WHERE comment_id = $1`, b); n != 0 {
		t.Fatalf("tombstone kept %d reactions", n)
	}

	if len(rec.events) != 1 || rec.events[0].Type != protocol.EventCommentUpdated {
		t.Fatalf("events = %+v, want exactly one comment:updated", rec.events)
	}
	tombstone := rec.events[0].Payload.(map[string]any)["comment"].(CommentResponse)
	if tombstone.ID != b || tombstone.DeletedAt == nil || tombstone.Content != "" || len(tombstone.Attachments) != 0 {
		t.Fatalf("comment:updated payload = %+v, want B's tombstone", tombstone)
	}

	entries, status := fetchTimeline(t, issueID)
	if status != http.StatusOK {
		t.Fatalf("timeline status = %d", status)
	}
	var sawTombstone bool
	for _, entry := range entries {
		if entry.ID == b {
			sawTombstone = entry.DeletedAt != nil && entry.Content != nil && *entry.Content == ""
		}
	}
	if !sawTombstone {
		t.Fatalf("timeline did not return B as a tombstone: %+v", entries)
	}

	// The tombstone is no longer a comment anyone can act on.
	rec.delete(t, b).Want(http.StatusNotFound)
	testutil.Call(t, testHandler.UpdateComment, withURLParam(newRequest(http.MethodPut, "/api/comments/"+b, map[string]any{
		"content": "resurrected",
	}), "commentId", b)).Want(http.StatusNotFound)
	testutil.Call(t, testHandler.AddReaction, withURLParam(newRequest(http.MethodPost, "/api/comments/"+b+"/reactions", map[string]any{
		"emoji": "🎉",
	}), "commentId", b)).Want(http.StatusNotFound)
	testutil.Call(t, testHandler.ResolveComment, withURLParam(newRequest(http.MethodPost, "/api/comments/"+b+"/resolve", nil), "commentId", b)).
		Want(http.StatusNotFound)

	// Deleting a leaf under a live parent removes the leaf alone.
	rec.delete(t, e).Want(http.StatusNoContent)
	if got := loadCommentRowState(t, e); got.exists {
		t.Fatalf("E = %+v, want it removed", got)
	}
	if got := loadCommentRowState(t, c); !got.exists || got.deleted {
		t.Fatalf("C = %+v, want it untouched", got)
	}
	if ids := rec.deletedIDs(); len(ids) != 1 || ids[0] != e {
		t.Fatalf("comment:deleted ids = %v, want only E", ids)
	}
}

func TestDeleteCommentPrunesTombstonesLeftWithoutReplies(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	issueID := dbfx.Issue(t, "delete prunes tombstones")
	// A → B → C, plus A → D.
	a := dbfx.Comment(t, issueID, "root A")
	b := dbfx.Comment(t, issueID, "reply B", testutil.Cols{"parent_id": a})
	c := dbfx.Comment(t, issueID, "reply C", testutil.Cols{"parent_id": b})
	d := dbfx.Comment(t, issueID, "reply D", testutil.Cols{"parent_id": a})

	rec := newCommentEventRecorder()
	rec.delete(t, b).Want(http.StatusNoContent)
	rec.delete(t, a).Want(http.StatusNoContent)
	for _, id := range []string{a, b} {
		if got := loadCommentRowState(t, id); !got.exists || !got.deleted {
			t.Fatalf("comment %s = %+v, want a tombstone", id, got)
		}
	}

	// A still holds B's branch, so removing D leaves A in place.
	rec.delete(t, d).Want(http.StatusNoContent)
	if ids := rec.deletedIDs(); len(ids) != 1 || ids[0] != d {
		t.Fatalf("comment:deleted ids = %v, want only D", ids)
	}
	if got := loadCommentRowState(t, a); !got.exists || !got.deleted {
		t.Fatalf("A = %+v, want it kept as a tombstone", got)
	}

	// C was the last live reply: it goes, and so do the tombstones above it.
	rec.delete(t, c).Want(http.StatusNoContent)
	if ids := rec.deletedIDs(); len(ids) != 3 || ids[0] != c || ids[1] != b || ids[2] != a {
		t.Fatalf("comment:deleted ids = %v, want C, B, A in that order", ids)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id = $1`, issueID); n != 0 {
		t.Fatalf("issue still has %d comments, want none", n)
	}
}

// The inbox keeps the original input and a separate deletion event. A
// tombstone must not become an empty comment or create another execution.
func TestCommentTombstoneIsDeliveredAsDeletionEvent(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	issueID, ownerID, _ := issueSessionCaptureFixture(t)
	root := captureSessionComment(t, issueID, map[string]any{"content": "please look at this"}, "", "")
	captureSessionComment(t, issueID, map[string]any{"content": "a reply keeps the root", "parent_id": root.ID}, "", "")
	before := captureIssueSessions(t, issueID)
	if len(before) != 1 || len(before[0].Events) != 3 || before[0].ActiveTaskID == nil {
		t.Fatalf("session before delete = %+v, want three inputs and one execution", before)
	}

	rec := newCommentEventRecorder()
	rec.delete(t, root.ID).Want(http.StatusNoContent)
	after := captureIssueSessions(t, issueID)
	if len(after) != 1 || after[0].ID != before[0].ID || after[0].AgentID != ownerID || len(after[0].Events) != 4 {
		t.Fatalf("session after delete = %+v, want one deletion appended to the same owner", after)
	}
	if after[0].ActiveTaskID == nil || *after[0].ActiveTaskID != *before[0].ActiveTaskID {
		t.Fatalf("deletion replaced the execution: before=%+v after=%+v", before[0], after[0])
	}
	created, deleted := after[0].Events[1], after[0].Events[3]
	if created.Kind != "comment.created" || created.SourceID == nil || *created.SourceID != root.ID {
		t.Fatalf("original input changed after deletion: %+v", created)
	}
	if deleted.Kind != "comment.deleted" || deleted.SourceID == nil || *deleted.SourceID != root.ID || deleted.Sequence != created.Sequence+2 || deleted.ActorType != "member" || deleted.ActorID == nil || *deleted.ActorID != testUserID {
		t.Fatalf("deletion event = %+v, want an ordered member-authored deletion of the root", deleted)
	}
	var originalPayload, deletionPayload map[string]json.RawMessage
	if err := json.Unmarshal(created.Payload, &originalPayload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(deleted.Payload, &deletionPayload); err != nil {
		t.Fatal(err)
	}
	if string(originalPayload["content"]) != `"please look at this"` || string(deletionPayload["content"]) != "null" {
		t.Fatalf("captured content: original=%s deletion=%s", created.Payload, deleted.Payload)
	}
	rec.delete(t, root.ID).Want(http.StatusNotFound)
	if events := captureIssueSessions(t, issueID)[0].Events; len(events) != len(after[0].Events) {
		t.Fatalf("repeated deletion appended another event: %+v", events)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueID); n != 1 {
		t.Fatalf("deletion created %d executions, want the existing one", n)
	}
}

// Reply placement stays in the comment tree; delivery belongs to the issue's
// owner even when the parent is a deleted comment by a different agent.
func TestReplyUnderDeletedAgentCommentEntersOwnerSession(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	authorID := dbfx.Agent(t, "Deleted comment author", runtimeID)
	authorTaskID := dbfx.Task(t, authorID, testutil.Cols{
		"issue_id": issueID, "runtime_id": runtimeID, "status": "running", "started_at": testutil.Raw("now()"),
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	root := captureSessionComment(t, issueID, map[string]any{"content": "I am handling this"}, authorID, authorTaskID)
	captureSessionComment(t, issueID, map[string]any{"content": "earlier reply", "parent_id": root.ID}, "", "")
	newCommentEventRecorder().delete(t, root.ID).Want(http.StatusNoContent)
	before := captureIssueSessions(t, issueID)
	if len(before) != 1 || before[0].ActiveTaskID == nil {
		t.Fatalf("session before reply = %+v, want the owner's existing execution", before)
	}

	body := map[string]any{"content": "please follow up", "parent_id": root.ID}
	var preview CommentTriggerPreviewResponse
	req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments/preview", body), "id", issueID)
	testutil.Call(t, testHandler.PreviewCommentTriggers, req).Want(http.StatusOK).JSON(&preview)
	if len(preview.Agents) != 1 || preview.Agents[0].ID != ownerID || len(preview.Blocked) != 0 {
		t.Fatalf("reply preview = %+v, want only the issue owner", preview)
	}
	reply := captureSessionComment(t, issueID, body, "", "")
	if reply.ParentID == nil || *reply.ParentID != root.ID || len(reply.TriggerOutcomes) != 1 || reply.TriggerOutcomes[0].TargetID != ownerID {
		t.Fatalf("reply was redirected away from its tree or owner: %+v", reply)
	}
	after := captureIssueSessions(t, issueID)
	if len(after) != 1 || after[0].ID != before[0].ID || after[0].AgentID != ownerID || len(after[0].Events) != len(before[0].Events)+1 {
		t.Fatalf("reply did not enter the same owner inbox: %+v", after)
	}
	event := after[0].Events[len(after[0].Events)-1]
	if event.Kind != "comment.created" || event.SourceID == nil || *event.SourceID != reply.ID || event.Sequence != before[0].Events[len(before[0].Events)-1].Sequence+1 {
		t.Fatalf("reply event = %+v, want the next owner input", event)
	}
	if after[0].ActiveTaskID == nil || *after[0].ActiveTaskID != *before[0].ActiveTaskID {
		t.Fatalf("reply replaced the owner's execution: before=%+v after=%+v", before[0], after[0])
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2", issueID, authorID); n != 1 {
		t.Fatalf("reply created another execution for the deleted comment author: %d", n)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2", issueID, ownerID); n != 1 {
		t.Fatalf("reply created %d owner executions, want the existing one", n)
	}
}

// A comment deleted after capture reads as removed, not as emptied; one that
// was already a tombstone when captured is not a change at all.
func TestSourceContextThreadChangesTreatTombstonesAsAbsent(t *testing.T) {
	root := "00000000-0000-0000-0000-00000000000a"
	node := func(id string, deleted bool) service.SourceContextCommentSnapshot {
		parent := root
		comment := service.SourceContextCommentSnapshot{ID: id, ParentID: &parent, Type: "comment", Content: "body " + id, Deleted: deleted}
		if deleted {
			comment.Content = ""
		}
		if id == root {
			comment.ParentID = nil
		}
		return comment
	}
	snapshot := func(nodes ...service.SourceContextCommentSnapshot) service.SourceContextSnapshot {
		return service.SourceContextSnapshot{AnchorCommentID: root, CommentThread: nodes}
	}

	deletedAfterCapture := sourceContextThreadChangeDetails(
		snapshot(node(root, false), node("b", false), node("c", false)),
		snapshot(node(root, false), node("b", true), node("c", false)),
	)
	if len(deletedAfterCapture.RemovedCommentIDs) != 1 || deletedAfterCapture.RemovedCommentIDs[0] != "b" || len(deletedAfterCapture.ChangedCommentIDs) != 0 {
		t.Fatalf("changes = %+v, want b removed and nothing edited", deletedAfterCapture)
	}

	alreadyDeleted := sourceContextThreadChangeDetails(
		snapshot(node(root, false), node("b", true), node("c", false)),
		snapshot(node(root, false), node("b", true), node("c", false)),
	)
	if len(alreadyDeleted.Reasons) != 0 {
		t.Fatalf("changes = %+v, want an unchanged thread", alreadyDeleted)
	}
}

func TestPublicPluginCommentExposesTombstone(t *testing.T) {
	live := publicPluginComment(db.Comment{Content: "hello"})
	if live.DeletedAt != "" {
		t.Fatalf("live comment deleted_at = %q, want empty", live.DeletedAt)
	}
	deletedAt := time.Date(2026, time.September, 11, 8, 0, 0, 0, time.UTC)
	tombstone := publicPluginComment(db.Comment{DeletedAt: pgtype.Timestamptz{Time: deletedAt, Valid: true}})
	if tombstone.DeletedAt == "" || tombstone.Content != "" {
		t.Fatalf("tombstone = %+v, want deleted_at set and empty content", tombstone)
	}
}

// recordingStorage records which stored objects a handler cleaned up.
type recordingStorage struct {
	mockStorage
	mu      sync.Mutex
	deleted []string
}

func (s *recordingStorage) DeleteKeys(_ context.Context, keys []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, keys...)
}

// A write to a comment's children that waited on the comment while a delete
// tombstoned it must not land on the tombstone. The holder transaction stands
// in for a delete that already holds the comment lock: the write blocks on
// LockLiveComment, the tombstone commits, and the write must be refused.
func TestCommentChildWritesWaitingOnDeleteDoNotLandOnTombstone(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	storage := &recordingStorage{}
	h := *testHandler
	h.Storage = storage

	tests := []struct {
		name      string
		run       func(commentID, issueID string) *httptest.ResponseRecorder
		wantCode  int
		leftovers string
	}{
		{
			name: "reaction",
			run: func(commentID, _ string) *httptest.ResponseRecorder {
				w := httptest.NewRecorder()
				h.AddReaction(w, withURLParam(newRequest(http.MethodPost, "/api/comments/"+commentID+"/reactions", map[string]any{
					"emoji": "👍",
				}), "commentId", commentID))
				return w
			},
			wantCode:  http.StatusNotFound,
			leftovers: `SELECT count(*) FROM comment_reaction WHERE comment_id = $1`,
		},
		{
			name: "attachment upload",
			run: func(commentID, issueID string) *httptest.ResponseRecorder {
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				part, _ := writer.CreateFormFile("file", "late.txt")
				part.Write([]byte("late"))
				writer.WriteField("issue_id", issueID)
				writer.WriteField("comment_id", commentID)
				writer.Close()
				req := httptest.NewRequest(http.MethodPost, "/api/upload-file", &body)
				req.Header.Set("Content-Type", writer.FormDataContentType())
				req.Header.Set("X-User-ID", testUserID)
				req.Header.Set("X-Workspace-ID", testWorkspaceID)
				w := httptest.NewRecorder()
				h.UploadFile(w, req)
				return w
			},
			wantCode:  http.StatusForbidden,
			leftovers: `SELECT count(*) FROM attachment WHERE comment_id = $1`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			issueID := dbfx.Issue(t, "child write races delete "+tt.name)
			target := dbfx.Comment(t, issueID, "about to be deleted")
			dbfx.Comment(t, issueID, "reply keeps it as a tombstone", testutil.Cols{"parent_id": target})

			holder, err := testPool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin holder: %v", err)
			}
			defer holder.Rollback(ctx)
			if _, err := holder.Exec(ctx, `SELECT id FROM comment WHERE id = $1 FOR UPDATE`, target); err != nil {
				t.Fatalf("lock comment: %v", err)
			}

			var got *httptest.ResponseRecorder
			done := make(chan error, 1)
			go func() {
				got = tt.run(target, issueID)
				done <- nil
			}()
			waitForCommentMutationLock(t, "LockLiveComment", done)

			if _, err := holder.Exec(ctx, `UPDATE comment SET content = '', deleted_at = now() WHERE id = $1`, target); err != nil {
				t.Fatalf("tombstone comment: %v", err)
			}
			if err := holder.Commit(ctx); err != nil {
				t.Fatalf("commit holder: %v", err)
			}
			<-done

			if got.Code != tt.wantCode {
				t.Fatalf("%s after delete = %d, want %d: %s", tt.name, got.Code, tt.wantCode, got.Body.String())
			}
			if n := dbfx.Count(t, tt.leftovers, target); n != 0 {
				t.Fatalf("%s landed on the tombstone (%d rows)", tt.name, n)
			}
		})
	}
	if len(storage.deleted) != 1 {
		t.Fatalf("refused upload cleaned up %v, want its one stored object", storage.deleted)
	}
}

func TestRemoveReactionOnTombstoneIsNotFound(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	issueID := dbfx.Issue(t, "remove reaction on tombstone")
	target := dbfx.Comment(t, issueID, "deleted", testutil.Cols{"deleted_at": testutil.Raw("now()"), "content": ""})
	testutil.Call(t, testHandler.RemoveReaction, withURLParam(newRequest(http.MethodDelete, "/api/comments/"+target+"/reactions", map[string]any{
		"emoji": "👍",
	}), "commentId", target)).Want(http.StatusNotFound)
}
