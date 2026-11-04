package handler

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/testutil"
)

func TestCreateComment_MixedMentionsReportOnlyOwnerSession(t *testing.T) {
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	mentionedID := dbfx.Agent(t, "Mentioned agent", runtimeID)
	privateID, _, _ := privateAgentTestFixture(t)
	squadID := dbfx.Squad(t, "Mentioned private squad", privateID)
	foreignWorkspace := dbfx.Workspace(t, "Foreign mention workspace", "outcome-"+uuid.NewString())
	foreignID := dbfx.Agent(t, "Secret foreign agent", runtimeID, testutil.Cols{"workspace_id": foreignWorkspace})
	content := fmt.Sprintf("[@Allowed](mention://agent/%s) [@Private](mention://agent/%s) [@Squad](mention://squad/%s) [@Foreign](mention://agent/%s) [@Missing](mention://agent/%s) please review", mentionedID, privateID, squadID, foreignID, uuid.NewString())

	preview := previewCommentTriggersForTest(t, issueID, map[string]any{"content": content})
	requirePreviewAgents(t, preview, ownerID)
	if len(preview.Blocked) != 0 {
		t.Fatalf("text mentions changed owner admission: %+v", preview.Blocked)
	}
	posted := captureSessionComment(t, issueID, map[string]any{"content": content}, "", "")
	if len(posted.TriggerOutcomes) != 1 {
		t.Fatalf("outcomes = %+v, want only the assigned session receipt", posted.TriggerOutcomes)
	}
	receipt := posted.TriggerOutcomes[0]
	if receipt.TargetType != "agent" || receipt.TargetID != ownerID || receipt.Status != DispatchDeferred || receipt.ReasonCode != ReasonDeferred {
		t.Fatalf("owner session receipt = %+v", receipt)
	}
	var eventOwner, payload string
	dbfx.QueryRow(t, `SELECT s.agent_id,e.payload->>'content' FROM issue_agent_event e JOIN issue_agent_session s ON s.id=e.session_id WHERE e.source_id=$1 AND e.kind='comment.created'`, posted.ID).Scan(&eventOwner, &payload)
	if eventOwner != ownerID || payload != content {
		t.Fatalf("saved event routed to %s with payload %q", eventOwner, payload)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); count != 1 {
		t.Fatalf("mentions created %d executions, want the single owner execution", count)
	}
	if count := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id<>$2`, issueID, ownerID); count != 0 {
		t.Fatalf("mentions started %d unrelated agents", count)
	}
}

func TestCreateComment_SquadMentionsDoNotChangeOwnerExecutionRole(t *testing.T) {
	issueID, ownerID, _ := issueSessionCaptureFixture(t)
	squad1 := dbfx.Squad(t, "First owner squad", ownerID)
	squad2 := dbfx.Squad(t, "Second owner squad", ownerID)
	contents := []string{
		fmt.Sprintf("[@Owner](mention://agent/%s) [@First](mention://squad/%s)", ownerID, squad1),
		fmt.Sprintf("[@First](mention://squad/%s) [@Owner](mention://agent/%s)", squad1, ownerID),
		fmt.Sprintf("[@First](mention://squad/%s) [@Second](mention://squad/%s)", squad1, squad2),
	}
	for _, content := range contents {
		posted := captureSessionComment(t, issueID, map[string]any{"content": content}, "", "")
		if len(posted.TriggerOutcomes) != 1 || posted.TriggerOutcomes[0].TargetID != ownerID {
			t.Fatalf("squad mention changed session ownership: %+v", posted.TriggerOutcomes)
		}
	}
	var count int
	var leader bool
	var squad string
	dbfx.QueryRow(t, `SELECT count(*),COALESCE(bool_or(is_leader_task),false),COALESCE(max(squad_id::text),'') FROM agent_task_queue WHERE issue_id=$1`, issueID).Scan(&count, &leader, &squad)
	if count != 1 || leader || squad != "" {
		t.Fatalf("mentions changed plain owner execution: count=%d leader=%t squad=%q", count, leader, squad)
	}
	var kind string
	dbfx.QueryRow(t, `SELECT context->>'type' FROM agent_task_queue WHERE issue_id=$1`, issueID).Scan(&kind)
	if kind != "issue_session" {
		t.Fatalf("execution kind = %q, want issue_session", kind)
	}
	request := withURLParam(newRequest(http.MethodGet, "/api/issues/"+issueID+"/agent-sessions", nil), "id", issueID)
	var sessions []IssueAgentSessionResponse
	testutil.Call(t, testHandler.ListIssueAgentSessions, request).Want(http.StatusOK).JSON(&sessions)
	if len(sessions) != 1 || len(sessions[0].Events) != len(contents)+1 {
		t.Fatalf("squad mentions split the owner inbox: %+v", sessions)
	}
}
