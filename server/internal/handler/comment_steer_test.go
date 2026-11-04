package handler

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func agentMention(name, id string) string {
	return fmt.Sprintf("[@%s](mention://agent/%s)", name, id)
}

func TestTaskRunsRetainHistoricalSupplementMetadataWithoutOfferingWrites(t *testing.T) {
	f := newSupplementFixture(t, "codex", "running", true)
	commentID := insertHistoricalTaskSupplement(t, f, "Historical receipt", "delivered", "member", testUserID)
	var comments []CommentResponse
	commentReq := withURLParam(newRequest(http.MethodGet, "/api/issues/"+f.issueID+"/comments", nil), "id", f.issueID)
	testutil.Call(t, testHandler.ListComments, commentReq).Want(http.StatusOK).JSON(&comments)
	foundReceipt := false
	for _, comment := range comments {
		if comment.ID == commentID {
			foundReceipt = true
			if len(comment.Supplements) != 1 || comment.Supplements[0].TaskID != f.taskID || comment.SupplementTaskID != f.taskID || comment.SupplementStatus != "delivered" {
				t.Fatalf("historical comment receipt lost compatibility fields: %+v", comment)
			}
		}
	}
	if !foundReceipt {
		t.Fatal("historical comment was not returned")
	}
	caller := dbfx.Agent(t, "Historical metadata reader", f.runtimeID)
	callerTask := dbfx.Task(t, caller, testutil.Cols{"issue_id": f.issueID, "runtime_id": f.runtimeID, "status": "running"})
	for _, actor := range []string{"member", "agent"} {
		t.Run(actor, func(t *testing.T) {
			req := newRequest(http.MethodGet, "/api/issues/"+f.issueID+"/task-runs", nil)
			if actor == "agent" {
				req.Header.Set("X-Actor-Source", "task_token")
				req.Header.Set("X-Agent-ID", caller)
				req.Header.Set("X-Task-ID", callerTask)
			}
			var runs []AgentTaskResponse
			testutil.Call(t, testHandler.ListTasksByIssue, withURLParam(req, "id", f.issueID)).Want(http.StatusOK).JSON(&runs)
			found := false
			for _, run := range runs {
				if run.CanSupplement {
					t.Errorf("run %s still advertises the removed supplement write API", run.ID)
				}
				if run.ID == f.taskID {
					found = true
					if run.SupplementCapability != protocol.DaemonCapabilityTaskSupplementV1 || len(run.SupplementCommentIDs) != 1 || run.SupplementCommentIDs[0] != commentID {
						t.Errorf("historical metadata lost: %+v", run)
					}
				}
			}
			if !found {
				t.Fatal("historical run not returned")
			}
		})
	}
}
