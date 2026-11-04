package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/handler"
)

func TestEditCommentTriggers(t *testing.T) {
	mentionedAgentID := newHTTPCommentAgent(t)
	mention := fmt.Sprintf("[@Agent](mention://agent/%s)", mentionedAgentID)
	for _, tc := range []struct {
		name    string
		initial string
		edited  string
	}{
		{"adds agent mention", "plain comment no mentions", mention + " please review"},
		{"removes agent mention", mention + " fix this", "removed the mention, nevermind"},
		{"keeps mention and changes content", mention + " fix bug A", mention + " actually fix bug B instead"},
		{"changes content without mentions", "fix the login page", "actually fix the signup page instead"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			issueID, ownerID := newHTTPCommentOwner(t, "Comment edit "+tc.name)
			commentID := postComment(t, issueID, tc.initial, nil)
			before := readHTTPCommentSession(t, issueID)
			if before.AgentID != ownerID || before.ActiveTaskID == nil || len(before.Events) == 0 {
				t.Fatalf("comment did not enter its owner's active session: %+v", before)
			}
			var original handler.IssueAgentEventResponse
			for _, event := range before.Events {
				if event.Kind == "comment.created" && event.SourceID != nil && *event.SourceID == commentID {
					original = event
				}
			}
			if original.ID == "" {
				t.Fatalf("session omitted the original comment: %+v", before.Events)
			}

			edit := func(content string) handler.CommentResponse {
				t.Helper()
				resp := authRequest(t, http.MethodPut, "/api/comments/"+commentID, map[string]any{"content": content})
				if resp.StatusCode != http.StatusOK {
					body, _ := io.ReadAll(resp.Body)
					resp.Body.Close()
					t.Fatalf("UpdateComment: expected 200, got %d: %s", resp.StatusCode, body)
				}
				var comment handler.CommentResponse
				readJSON(t, resp, &comment)
				return comment
			}
			updated := edit(tc.edited)
			if updated.ID != commentID || updated.Content != tc.edited || updated.Revision <= original.SourceRevision {
				t.Fatalf("edit did not update the original comment: %+v", updated)
			}
			if len(updated.TriggerOutcomes) != 1 || updated.TriggerOutcomes[0].TargetID != ownerID {
				t.Fatalf("edit changed the delivery owner: %+v", updated.TriggerOutcomes)
			}
			after := readHTTPCommentSession(t, issueID)
			if after.ID != before.ID || after.AgentID != ownerID || after.ActiveTaskID == nil || *after.ActiveTaskID != *before.ActiveTaskID || len(after.Events) != len(before.Events)+1 {
				t.Fatalf("edit replaced the session or execution: before=%+v after=%+v", before, after)
			}
			for i, event := range before.Events {
				got := after.Events[i]
				if got.ID != event.ID || got.Sequence != event.Sequence || got.Kind != event.Kind || got.SourceRevision != event.SourceRevision || string(got.Payload) != string(event.Payload) {
					t.Fatalf("edit rewrote historical event: before=%+v after=%+v", event, got)
				}
			}
			change := after.Events[len(after.Events)-1]
			if change.Kind != "comment.updated" || change.SourceID == nil || *change.SourceID != commentID || change.SourceRevision != updated.Revision || change.Sequence != before.Events[len(before.Events)-1].Sequence+1 || change.ActorType != "member" || change.ActorID == nil || *change.ActorID != testUserID {
				t.Fatalf("edit was not the next member-authored input: %+v", change)
			}
			var payload struct {
				Content string `json:"content"`
			}
			if err := json.Unmarshal(change.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Content != tc.edited {
				t.Fatalf("edit payload content = %q, want %q", payload.Content, tc.edited)
			}

			unchanged := edit(tc.edited)
			repeated := readHTTPCommentSession(t, issueID)
			if unchanged.Revision != updated.Revision || repeated.ID != after.ID || repeated.ActiveTaskID == nil || *repeated.ActiveTaskID != *after.ActiveTaskID || len(repeated.Events) != len(after.Events) || repeated.Events[len(repeated.Events)-1].ID != change.ID {
				t.Fatalf("an unchanged edit appended input or replaced the execution: %+v", repeated)
			}
			if n := countPendingTasksForAgent(t, issueID, ownerID); n != 1 {
				t.Fatalf("comment and edit created %d owner executions, want one", n)
			}
			if n := countPendingTasksForAgent(t, issueID, mentionedAgentID); n != 0 {
				t.Fatalf("editing mentions started %d executions for the mention target", n)
			}
		})
	}
}
