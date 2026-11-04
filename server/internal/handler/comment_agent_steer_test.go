package handler

import (
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func TestClaimHistoricalTaskSupplementPreservesAgentAttribution(t *testing.T) {
	f := newSupplementFixture(t, "claude", "running", true)
	author := dbfx.Agent(t, "Historical author", f.runtimeID)
	commentID := insertHistoricalTaskSupplement(t, f, "Earlier agent input", "pending", "agent", author)
	claim := func(capabilities []string) map[string]string {
		req := withURLParam(newOwnerDaemonRequest(http.MethodPost, "/claim", map[string]any{"capabilities": capabilities}), "taskId", f.taskID)
		var out map[string]string
		testutil.Call(t, testHandler.ClaimTaskSupplement, req).Want(http.StatusOK).JSON(&out)
		return out
	}
	if old := claim(nil); len(old) != 0 {
		t.Fatalf("old daemon would misattribute an agent's historical input: %+v", old)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM task_supplement WHERE task_id=$1 AND status='pending' AND attempt_count=0`, f.taskID); n != 1 {
		t.Fatal("unsupported author rendering consumed the historical receipt")
	}
	current := claim([]string{protocol.DaemonCapabilityTaskSupplementAuthorV1})
	if current["comment_id"] != commentID || current["author_type"] != "agent" || current["author_name"] != "Historical author" || current["content"] != "Earlier agent input" {
		t.Fatalf("author-aware historical claim = %+v", current)
	}
}
