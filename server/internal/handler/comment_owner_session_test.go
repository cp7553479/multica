package handler

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestCommentSessionOwnerIgnoresContentAndRetiredRoutingFields(t *testing.T) {
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	otherID := dbfx.Agent(t, "Mentioned agent", runtimeID)
	squadID := dbfx.Squad(t, "Mentioned squad", otherID)
	contents := []string{
		"Plain instruction",
		"/note this still belongs to the issue session",
		"[@all](mention://all/all) update",
		fmt.Sprintf("[@Member](mention://member/%s) update", testUserID),
		fmt.Sprintf("[@Agent](mention://agent/%s) update", otherID),
		fmt.Sprintf("[@Squad](mention://squad/%s) update", squadID),
		"[@Malformed](mention://agent/not-a-uuid) update",
	}
	for _, content := range contents {
		var preview CommentTriggerPreviewResponse
		req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments/preview", map[string]any{"content": content}), "id", issueID)
		testutil.Call(t, testHandler.PreviewCommentTriggers, req).Want(http.StatusOK).JSON(&preview)
		if len(preview.Agents) != 1 || preview.Agents[0].ID != ownerID || preview.Agents[0].Source != "issue_assignee" || len(preview.Blocked) != 0 {
			t.Fatalf("preview for %q changed owner: %+v", content, preview)
		}
		comment := captureSessionComment(t, issueID, map[string]any{
			"content": content, "suppress_agent_ids": []string{ownerID}, "steer": true, "steer_task_ids": []string{"retired-invalid-turn"},
		}, "", "")
		if len(comment.TriggerOutcomes) != 1 || comment.TriggerOutcomes[0].TargetID != ownerID {
			t.Fatalf("posting %q changed owner: %+v", content, comment.TriggerOutcomes)
		}
	}
	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 1 || sessions[0].AgentID != ownerID || len(sessions[0].Events) != len(contents)+1 {
		t.Fatalf("comments did not enter one ordered owner inbox: %+v", sessions)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueID); n != 1 {
		t.Fatalf("comments spawned separate runs: %d", n)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM task_supplement WHERE issue_id=$1", issueID); n != 0 {
		t.Fatalf("retired steering path accepted comments: %d", n)
	}
}

func TestCommentSessionPrivateOwnerDeniesUnauthorizedInvocation(t *testing.T) {
	issueID, agentID, _ := issueSessionCaptureFixture(t)
	privateOwnerID := handlerWorkspaceMember(t, "private-session-owner")
	dbfx.Exec(t, "UPDATE agent SET owner_id=$2, permission_mode='private' WHERE id=$1", agentID, privateOwnerID)
	var preview CommentTriggerPreviewResponse
	req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments/preview", map[string]any{"content": "An instruction"}), "id", issueID)
	testutil.Call(t, testHandler.PreviewCommentTriggers, req).Want(http.StatusOK).JSON(&preview)
	if len(preview.Agents) != 0 || len(preview.Blocked) != 1 || preview.Blocked[0].TargetID != agentID || preview.Blocked[0].ReasonCode != ReasonInvocationNotAllowed {
		t.Fatalf("private owner preview = %+v, want an invocation denial", preview)
	}
	comment := captureSessionComment(t, issueID, map[string]any{"content": "Still visible in the issue"}, "", "")
	if len(comment.TriggerOutcomes) != 1 || comment.TriggerOutcomes[0].Status != DispatchBlocked || comment.TriggerOutcomes[0].ReasonCode != ReasonInvocationNotAllowed {
		t.Fatalf("private owner was invoked: %+v", comment.TriggerOutcomes)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM comment WHERE id=$1 AND content=$2", comment.ID, comment.Content); n != 1 {
		t.Fatal("blocked session delivery discarded the author's comment")
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueID); n != 0 {
		t.Fatalf("private owner received an unauthorized execution: %d", n)
	}
}

func TestCommentMentionsCannotStartAgentsWithoutAnAgentOwner(t *testing.T) {
	runtimeID := dbfx.Runtime(t, "No agent owner runtime")
	agentID := dbfx.Agent(t, "Mention target", runtimeID)
	squadID := dbfx.Squad(t, "Mention squad", agentID)
	for _, ownerType := range []string{"member", "squad"} {
		t.Run(ownerType, func(t *testing.T) {
			ownerID := testUserID
			if ownerType == "squad" {
				ownerID = squadID
			}
			issueID := dbfx.Issue(t, "Explicit agent assignment required", testutil.Cols{"assignee_type": ownerType, "assignee_id": ownerID})
			comment := captureSessionComment(t, issueID, map[string]any{"content": fmt.Sprintf("[@Agent](mention://agent/%s) [@Squad](mention://squad/%s) help", agentID, squadID)}, "", "")
			if len(comment.TriggerOutcomes) != 0 || len(captureIssueSessions(t, issueID)) != 0 {
				t.Fatalf("mentions selected a new execution owner: %+v", comment.TriggerOutcomes)
			}
			if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueID); n != 0 {
				t.Fatalf("mentions created agent tasks without assignment: %d", n)
			}
		})
	}
}
