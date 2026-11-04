package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func TestCommentSessionReceiptsDoNotCreateMentionDisclosureNotices(t *testing.T) {
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	privateID, _, readerID := privateAgentTestFixture(t)
	foreignWorkspace := dbfx.Workspace(t, "Other notice workspace", "notice-"+uuid.NewString())
	foreignID := dbfx.Agent(t, "Other workspace secret", runtimeID, testutil.Cols{"workspace_id": foreignWorkspace})
	content := fmt.Sprintf("[@Private alias](mention://agent/%s) [@Missing alias](mention://agent/%s) [@Foreign alias](mention://agent/%s)", privateID, uuid.NewString(), foreignID)
	preview := previewCommentTriggersForTest(t, issueID, map[string]any{"content": content})
	requirePreviewAgents(t, preview, ownerID)
	if len(preview.Blocked) != 0 {
		t.Fatalf("preview enumerated text targets: %+v", preview.Blocked)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, issueID); count != 0 {
		t.Fatalf("preview wrote %d comments", count)
	}
	h := *testHandler
	h.Bus = events.New()
	var notices int
	h.Bus.Subscribe(protocol.EventCommentCreated, func(event events.Event) {
		if payload, ok := event.Payload.(map[string]any); ok {
			if comment, ok := payload["comment"].(CommentResponse); ok && comment.AuthorType == "system" {
				notices++
			}
		}
	})
	var posted CommentResponse
	testutil.Call(t, h.CreateComment, withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{"content": content}), "id", issueID)).Want(http.StatusCreated).JSON(&posted)
	if len(posted.TriggerOutcomes) != 1 || posted.TriggerOutcomes[0].TargetID != ownerID || posted.TriggerOutcomes[0].Status != DispatchDeferred {
		t.Fatalf("receipt disclosed or dispatched mention targets: %+v", posted.TriggerOutcomes)
	}
	if notices != 0 {
		t.Fatalf("comment broadcast %d obsolete mention notices", notices)
	}
	var comments []CommentResponse
	testutil.Call(t, h.ListComments, withURLParam(newRequestAs(readerID, http.MethodGet, "/api/issues/"+issueID+"/comments", nil), "id", issueID)).Want(http.StatusOK).JSON(&comments)
	if len(comments) != 1 || comments[0].ID != posted.ID || comments[0].Content != content {
		t.Fatalf("reader received generated disclosures instead of original input: %+v", comments)
	}
	var timeline []TimelineEntry
	testutil.Call(t, h.ListTimeline, withURLParam(newRequestAs(readerID, http.MethodGet, "/api/issues/"+issueID+"/timeline", nil), "id", issueID)).Want(http.StatusOK).JSON(&timeline)
	found := false
	for _, entry := range timeline {
		if entry.ID == posted.ID && entry.Content != nil && *entry.Content == content {
			found = true
		}
	}
	if !found {
		t.Fatal("original submitted input is absent from the timeline")
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1 AND author_type='system'`, issueID); count != 0 {
		t.Fatalf("saved %d obsolete mention refusal notices", count)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id<>$2`, issueID, ownerID); count != 0 {
		t.Fatalf("mentions or notices started %d unrelated executions", count)
	}
}

func TestCommentSessionAgentEditsKeepImmutableInputHistoryWithoutNotices(t *testing.T) {
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	authorID := dbfx.Agent(t, "Agent editing its output", runtimeID)
	sourceIssue := dbfx.Issue(t, "Author's source task")
	sourceTaskID := dbfx.Task(t, authorID, testutil.Cols{"issue_id": sourceIssue, "runtime_id": runtimeID, "status": "running", "originator_user_id": testUserID, "accountable_user_id": testUserID})
	privateID, _, _ := privateAgentTestFixture(t)
	content := fmt.Sprintf("[@Worker alias](mention://agent/%s) take over", privateID)
	posted := captureSessionComment(t, issueID, map[string]any{"content": content}, authorID, sourceTaskID)
	if posted.AuthorType != "agent" || len(posted.TriggerOutcomes) != 1 || posted.TriggerOutcomes[0].TargetID != ownerID {
		t.Fatalf("agent output lost owner receipt: %+v", posted)
	}
	for _, text := range []string{content, "Handoff withdrawn", content} {
		request := withURLParam(newRequest(http.MethodPut, "/api/comments/"+posted.ID, map[string]any{"content": text}), "commentId", posted.ID)
		request.Header.Set("X-Agent-ID", authorID)
		request.Header.Set("X-Task-ID", sourceTaskID)
		request.Header.Set("X-Actor-Source", "task_token")
		testutil.Call(t, testHandler.UpdateComment, request).Want(http.StatusOK)
	}
	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 1 {
		t.Fatalf("agent edits split the session: %+v", sessions)
	}
	var inputs []IssueAgentEventResponse
	for _, event := range sessions[0].Events {
		if event.SourceID != nil && *event.SourceID == posted.ID {
			inputs = append(inputs, event)
		}
	}
	want := []struct{ kind, text string }{{"comment.created", content}, {"comment.updated", "Handoff withdrawn"}, {"comment.updated", content}}
	if len(inputs) != len(want) {
		t.Fatalf("edit history = %+v, want three immutable inputs", inputs)
	}
	for i, event := range inputs {
		var payload struct {
			Content string `json:"content"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if event.Kind != want[i].kind || payload.Content != want[i].text || event.ActorType != "agent" || event.ActorID == nil || *event.ActorID != authorID {
			t.Fatalf("input %d changed history or authority: %+v payload=%q", i, event, payload.Content)
		}
		if i > 0 && (event.SourceRevision <= inputs[i-1].SourceRevision || event.Sequence <= inputs[i-1].Sequence) {
			t.Fatalf("edit input order did not advance: %+v", inputs)
		}
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, issueID); count != 1 {
		t.Fatalf("agent edits created %d comments, want only the edited original", count)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); count != 1 {
		t.Fatalf("agent edits created %d executions, want the single owner execution", count)
	}
}

func TestCommentSessionDeletionDuringPublicationLeavesNoOrphanNotice(t *testing.T) {
	issueID, ownerID, _ := issueSessionCaptureFixture(t)
	privateOwner := handlerWorkspaceMember(t, "denied-session-owner")
	dbfx.Exec(t, `UPDATE agent SET owner_id=$2,permission_mode='private' WHERE id=$1`, ownerID, privateOwner)
	h := *testHandler
	h.Bus = events.New()
	h.Bus.Subscribe(protocol.EventCommentCreated, func(event events.Event) {
		payload, ok := event.Payload.(map[string]any)
		if !ok {
			return
		}
		comment, ok := payload["comment"].(CommentResponse)
		if ok && comment.AuthorType == "member" {
			dbfx.Exec(t, `DELETE FROM comment WHERE id=$1 AND workspace_id=$2`, comment.ID, testWorkspaceID)
		}
	})
	var posted CommentResponse
	testutil.Call(t, h.CreateComment, withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{"content": "Input deleted during publication"}), "id", issueID)).Want(http.StatusCreated).JSON(&posted)
	if len(posted.TriggerOutcomes) != 1 || posted.TriggerOutcomes[0].TargetID != ownerID || posted.TriggerOutcomes[0].Status != DispatchBlocked || posted.TriggerOutcomes[0].ReasonCode != ReasonInvocationNotAllowed {
		t.Fatalf("deletion changed the owner permission receipt: %+v", posted.TriggerOutcomes)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM comment WHERE issue_id=$1`, issueID); count != 0 {
		t.Fatalf("deletion left %d comments or orphan notices", count)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM issue_agent_event WHERE source_id=$1 AND kind IN ('comment.created','comment.deleted')`, posted.ID); count != 2 {
		t.Fatalf("deletion lost immutable source events: %d", count)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); count != 0 {
		t.Fatalf("refused input started %d executions", count)
	}
}
