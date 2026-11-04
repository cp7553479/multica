package handler

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
)

func TestCreateComment_StripsNullBytesInsteadOf500(t *testing.T) {
	issueID, ownerID, _ := issueSessionCaptureFixture(t)
	posted := captureSessionComment(t, issueID, map[string]any{"content": "diagnosis body\x00 with a stray NUL byte"}, "", "")
	want := "diagnosis body with a stray NUL byte"
	if posted.Content != want || strings.ContainsRune(posted.Content, '\x00') {
		t.Fatalf("response content = %q, want NUL stripped", posted.Content)
	}
	var stored, payload, owner string
	dbfx.QueryRow(t, `SELECT c.content,e.payload->>'content',s.agent_id FROM comment c JOIN issue_agent_event e ON e.source_id=c.id AND e.kind='comment.created' JOIN issue_agent_session s ON s.id=e.session_id WHERE c.id=$1`, posted.ID).Scan(&stored, &payload, &owner)
	if stored != want || payload != want || owner != ownerID {
		t.Fatalf("sanitized input diverged: stored=%q event=%q owner=%s", stored, payload, owner)
	}
}

func TestCommentSession_NullByteHiddenMentionsCannotRedirectSanitizedInputs(t *testing.T) {
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	mentioned := dbfx.Agent(t, "Hidden mention target", runtimeID)
	content := fmt.Sprintf("please take a look [@Target](mention://agent/%s\x00)", mentioned)
	if mentions := util.ParseMentions(content); len(mentions) != 0 {
		t.Fatalf("precondition: raw NUL content unexpectedly parsed: %+v", mentions)
	}
	preview := previewCommentTriggersForTest(t, issueID, map[string]any{"content": content})
	requirePreviewAgents(t, preview, ownerID)
	posted := captureSessionComment(t, issueID, map[string]any{"content": content}, "", "")
	if len(posted.TriggerOutcomes) != 1 || posted.TriggerOutcomes[0].TargetID != ownerID {
		t.Fatalf("sanitized mention redirected create receipt: %+v", posted.TriggerOutcomes)
	}
	editContent := "Edited " + content
	editPreview := previewCommentTriggersForTest(t, issueID, map[string]any{"content": editContent, "editing_comment_id": posted.ID})
	requirePreviewAgents(t, editPreview, ownerID)
	request := withURLParam(newRequest(http.MethodPut, "/api/comments/"+posted.ID, map[string]any{"content": editContent}), "commentId", posted.ID)
	var updated CommentResponse
	testutil.Call(t, testHandler.UpdateComment, request).Want(http.StatusOK).JSON(&updated)
	if len(updated.TriggerOutcomes) != 1 || updated.TriggerOutcomes[0].TargetID != ownerID {
		t.Fatalf("sanitized mention redirected edit receipt: %+v", updated.TriggerOutcomes)
	}
	rows, err := testPool.Query(t.Context(), `SELECT e.kind,e.payload->>'content' FROM issue_agent_event e JOIN issue_agent_session s ON s.id=e.session_id WHERE e.source_id=$1 AND s.agent_id=$2 ORDER BY e.sequence`, posted.ID, ownerID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := []struct{ kind, content string }{{"comment.created", strings.ReplaceAll(content, "\x00", "")}, {"comment.updated", strings.ReplaceAll(editContent, "\x00", "")}}
	index := 0
	for rows.Next() {
		var kind, payload string
		if err := rows.Scan(&kind, &payload); err != nil {
			t.Fatal(err)
		}
		if index >= len(want) || kind != want[index].kind || payload != want[index].content {
			t.Fatalf("event %d = %s %q, want ordered sanitized input", index, kind, payload)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if index != len(want) {
		t.Fatalf("captured %d events, want %d", index, len(want))
	}
	if tasks := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); tasks != 1 {
		t.Fatalf("create/edit made %d executions, want one owner execution", tasks)
	}
	if sessions := captureIssueSessions(t, issueID); len(sessions) != 1 || sessions[0].AgentID != ownerID {
		t.Fatalf("sanitized mentions selected another agent: %+v", sessions)
	}
}
