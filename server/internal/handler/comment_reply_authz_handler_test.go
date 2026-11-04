package handler

import (
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestCreateCommentAgentChoosesTopLevelOrAnIssueReply(t *testing.T) {
	issueID, agentID, runtimeID := issueSessionCaptureFixture(t)
	triggerID := dbfx.Comment(t, issueID, "Original input")
	otherParentID := dbfx.Comment(t, issueID, "Another discussion on this issue")
	taskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id": issueID, "runtime_id": runtimeID, "status": "running", "trigger_comment_id": triggerID,
	})
	for _, parentID := range []string{"", triggerID, otherParentID} {
		t.Run(parentID, func(t *testing.T) {
			body := map[string]any{"content": "Explicitly published result"}
			if parentID != "" {
				body["parent_id"] = parentID
			}
			comment := captureSessionComment(t, issueID, body, agentID, taskID)
			if parentID == "" {
				if comment.ParentID != nil {
					t.Fatalf("top-level comment acquired a parent: %+v", comment)
				}
			} else if comment.ParentID == nil || *comment.ParentID != parentID {
				t.Fatalf("reply parent = %v, want %s", comment.ParentID, parentID)
			}
			if comment.SourceTaskID == nil || *comment.SourceTaskID != taskID {
				t.Fatalf("comment lost its authoring task: %+v", comment)
			}
		})
	}
}

func TestCreateCommentAgentCannotReplyAcrossIssues(t *testing.T) {
	issueID, agentID, runtimeID := issueSessionCaptureFixture(t)
	otherIssueID := dbfx.Issue(t, "Another issue")
	foreignParentID := dbfx.Comment(t, otherIssueID, "A different issue's discussion")
	taskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id": issueID, "runtime_id": runtimeID, "status": "running",
	})
	req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{
		"content": "Must stay on its chosen issue", "parent_id": foreignParentID,
	}), "id", issueID)
	req.Header.Set("X-Agent-ID", agentID)
	req.Header.Set("X-Task-ID", taskID)
	req.Header.Set("X-Actor-Source", "task_token")
	testutil.Call(t, testHandler.CreateComment, req).Want(http.StatusBadRequest)
	if n := dbfx.Count(t, "SELECT count(*) FROM comment WHERE issue_id=$1", issueID); n != 0 {
		t.Fatalf("invalid cross-issue reply was stored: %d comments", n)
	}
}
