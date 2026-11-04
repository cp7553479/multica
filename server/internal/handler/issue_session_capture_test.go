package handler

import (
	"context"
	"net/http"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

func issueSessionCaptureFixture(t *testing.T) (issueID, agentID, runtimeID string) {
	t.Helper()
	runtimeID = dbfx.Runtime(t, "Issue session capture", testutil.Cols{"provider": "codex"})
	agentID = dbfx.Agent(t, "Issue session owner", runtimeID)
	issueID = dbfx.Issue(t, "Original session issue", testutil.Cols{"assignee_type": "agent", "assignee_id": agentID})
	dbfx.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issueID)
	return
}

func captureSessionComment(t *testing.T, issueID string, body map[string]any, actorAgentID, taskID string) CommentResponse {
	t.Helper()
	req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", body), "id", issueID)
	if actorAgentID != "" {
		req.Header.Set("X-Agent-ID", actorAgentID)
		req.Header.Set("X-Task-ID", taskID)
		req.Header.Set("X-Actor-Source", "task_token")
	}
	var out CommentResponse
	testutil.Call(t, testHandler.CreateComment, req).Want(http.StatusCreated).JSON(&out)
	return out
}

func captureSessionUpdateIssue(t *testing.T, issueID string, body map[string]any, actorAgentID, taskID string) {
	t.Helper()
	req := withURLParam(newRequest(http.MethodPut, "/api/issues/"+issueID, body), "id", issueID)
	if actorAgentID != "" {
		req.Header.Set("X-Agent-ID", actorAgentID)
		req.Header.Set("X-Task-ID", taskID)
		req.Header.Set("X-Actor-Source", "task_token")
	}
	testutil.Call(t, testHandler.UpdateIssue, req).Want(http.StatusOK)
}

func captureIssueSessions(t *testing.T, issueID string) []IssueAgentSessionResponse {
	t.Helper()
	var sessions []IssueAgentSessionResponse
	req := withURLParam(newRequest(http.MethodGet, "/api/issues/"+issueID+"/agent-sessions", nil), "id", issueID)
	testutil.Call(t, testHandler.ListIssueAgentSessions, req).Want(http.StatusOK).JSON(&sessions)
	return sessions
}

func TestIssueSessionCaptureOrdersCommentsRepliesEditsAndIssueChanges(t *testing.T) {
	issueID, agentID, _ := issueSessionCaptureFixture(t)
	comment := captureSessionComment(t, issueID, map[string]any{"content": "First instruction"}, "", "")
	reply := captureSessionComment(t, issueID, map[string]any{"content": "Follow-up instruction", "parent_id": comment.ID}, "", "")
	req := withURLParam(newRequest(http.MethodPatch, "/api/comments/"+comment.ID, map[string]any{
		"content": "Corrected instruction", "expected_revision": comment.Revision,
	}), "commentId", comment.ID)
	testutil.Call(t, testHandler.UpdateComment, req).Want(http.StatusOK)
	captureSessionUpdateIssue(t, issueID, map[string]any{"title": "Revised session issue"}, "", "")
	captureSessionUpdateIssue(t, issueID, map[string]any{"description": "New scope"}, "", "")

	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 1 || sessions[0].AgentID != agentID {
		t.Fatalf("sessions = %+v", sessions)
	}
	events := sessions[0].Events
	wantKinds := []string{"assigned", "comment.created", "comment.created", "comment.updated", "issue.updated", "issue.updated"}
	if len(events) != len(wantKinds) {
		t.Fatalf("events = %+v, want six ordered inputs", events)
	}
	for i, kind := range wantKinds {
		if events[i].Kind != kind || events[i].Sequence != int64(i+1) {
			t.Errorf("event %d = %+v, want sequence %d / %s", i, events[i], i+1, kind)
		}
		if events[i].ActorType != "member" {
			t.Errorf("event %d actor = %s, want member", i, events[i].ActorType)
		}
	}
	if events[1].SourceID == nil || *events[1].SourceID != comment.ID || events[2].SourceID == nil || *events[2].SourceID != reply.ID {
		t.Fatalf("comment sources were lost: %+v", events)
	}
	if events[3].SourceRevision <= events[1].SourceRevision {
		t.Fatal("edit did not advance the event's source revision")
	}
}

func TestIssueSessionCaptureSelfAuthoredToolsStayVisibleWithoutWakingSession(t *testing.T) {
	issueID, agentID, runtimeID := issueSessionCaptureFixture(t)
	triggerID := dbfx.Comment(t, issueID, "An earlier input")
	taskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id": issueID, "runtime_id": runtimeID, "status": "running", "started_at": testutil.Raw("now()"), "trigger_comment_id": triggerID,
	})
	comment := captureSessionComment(t, issueID, map[string]any{"content": "Independent top-level result"}, agentID, taskID)
	if comment.ParentID != nil {
		t.Fatalf("explicit top-level comment acquired a parent: %+v", comment)
	}
	captureSessionUpdateIssue(t, issueID, map[string]any{"description": "Agent-authored scope"}, agentID, taskID)

	sessions := captureIssueSessions(t, issueID)
	events := sessions[0].Events
	if len(events) != 4 {
		t.Fatalf("events = %+v, want assigned, member input, own comment, own edit", events)
	}
	for _, event := range events[2:] {
		if event.Status != "ignored" || event.ActorType != "agent" || event.ActorID == nil || *event.ActorID != agentID || event.FailureReason == nil || *event.FailureReason != "self_authored" {
			t.Errorf("self-authored action could wake its session: %+v", event)
		}
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM comment WHERE id=$1 AND author_type='agent' AND parent_id IS NULL AND source_task_id=$2", comment.ID, taskID); n != 1 {
		t.Fatalf("explicit agent comment not stored with its task attribution: %d", n)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_task_id=$2 AND status='ignored'", sessions[0].ID, taskID); n != 2 {
		t.Fatalf("self-authored event attribution lost its task: %d, want 2", n)
	}
}

func TestIssueSessionCaptureDeduplicatesAndRollsBackWithMutation(t *testing.T) {
	issueID, _, _ := issueSessionCaptureFixture(t)
	comment := captureSessionComment(t, issueID, map[string]any{"content": "Original input"}, "", "")
	session := captureIssueSessions(t, issueID)[0]
	before := len(session.Events)
	for i := 0; i < 2; i++ {
		dbfx.Exec(t, `SELECT append_issue_agent_event($1,'comment.created',$2,$3,'member',$4,NULL,'{}'::jsonb)`, issueID, comment.ID, comment.Revision, testUserID)
	}
	if events := captureIssueSessions(t, issueID)[0].Events; len(events) != before {
		t.Fatalf("duplicate append produced %d events, want %d", len(events), before)
	}
	var sequenceBefore int64
	dbfx.QueryRow(t, "SELECT next_sequence FROM issue_agent_session WHERE id=$1", session.ID).Scan(&sequenceBefore)

	tx, err := testPool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `UPDATE comment SET content='A rolled-back edit' WHERE id=$1`, comment.ID); err != nil {
		t.Fatal(err)
	}
	var during int
	if err := tx.QueryRow(context.Background(), "SELECT count(*) FROM issue_agent_event WHERE session_id=$1", session.ID).Scan(&during); err != nil {
		t.Fatal(err)
	}
	if during != before+1 {
		t.Fatalf("mutation transaction did not capture its event: %d", during)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	var content string
	var sequenceAfter int64
	dbfx.QueryRow(t, "SELECT content FROM comment WHERE id=$1", comment.ID).Scan(&content)
	dbfx.QueryRow(t, "SELECT next_sequence FROM issue_agent_session WHERE id=$1", session.ID).Scan(&sequenceAfter)
	if content != "Original input" || sequenceAfter != sequenceBefore {
		t.Fatalf("rollback left content=%q sequence=%d, want original and %d", content, sequenceAfter, sequenceBefore)
	}
	if events := captureIssueSessions(t, issueID)[0].Events; len(events) != before {
		t.Fatalf("rollback left %d events, want %d", len(events), before)
	}
}

func TestIssueSessionCaptureReassignmentSeparatesOwnershipAndHistory(t *testing.T) {
	issueID, firstAgent, runtimeID := issueSessionCaptureFixture(t)
	captureSessionComment(t, issueID, map[string]any{"content": "Input for first owner"}, "", "")
	firstSession := captureIssueSessions(t, issueID)[0]
	dbfx.Exec(t, "UPDATE issue_agent_session SET provider_session_id='provider-first',work_dir='/tmp/first-owner' WHERE id=$1", firstSession.ID)
	secondAgent := dbfx.Agent(t, "Second issue owner", runtimeID)
	captureSessionUpdateIssue(t, issueID, map[string]any{"assignee_type": "agent", "assignee_id": secondAgent}, "", "")
	secondComment := captureSessionComment(t, issueID, map[string]any{"content": "Input for second owner"}, "", "")
	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 2 {
		t.Fatalf("sessions = %+v, want one per owner", sessions)
	}
	var oldFound, newFound bool
	for _, session := range sessions {
		switch session.AgentID {
		case firstAgent:
			oldFound = true
			if session.ID != firstSession.ID || session.State != "detached" {
				t.Errorf("old session changed identity/state: %+v", session)
			}
			for _, event := range session.Events {
				if event.Status != "ignored" {
					t.Errorf("detached session retained runnable event: %+v", event)
				}
			}
		case secondAgent:
			newFound = true
			if session.ID == firstSession.ID || len(session.Events) != 2 || session.Events[0].Kind != "assigned" || session.Events[1].SourceID == nil || *session.Events[1].SourceID != secondComment.ID {
				t.Errorf("new owner inherited wrong events: %+v", session)
			}
		}
	}
	if !oldFound || !newFound {
		t.Fatalf("owners missing: %+v", sessions)
	}
	captureSessionUpdateIssue(t, issueID, map[string]any{"assignee_type": "agent", "assignee_id": firstAgent}, "", "")
	var provider, workdir string
	dbfx.QueryRow(t, "SELECT provider_session_id,work_dir FROM issue_agent_session WHERE id=$1", firstSession.ID).Scan(&provider, &workdir)
	if provider != "provider-first" || workdir != "/tmp/first-owner" {
		t.Fatalf("reassignment lost original session pointers: %q %q", provider, workdir)
	}
}

func TestIssueSessionCaptureLegacySteeringAndSuppressionCannotRedirectOwner(t *testing.T) {
	issueID, owner, runtimeID := issueSessionCaptureFixture(t)
	other := dbfx.Agent(t, "Mentioned agent", runtimeID)
	legacyTask := dbfx.Task(t, other, testutil.Cols{"issue_id": issueID, "runtime_id": runtimeID, "status": "running", "started_at": testutil.Raw("now()")})
	comment := captureSessionComment(t, issueID, map[string]any{
		"content": agentMention("Mentioned", other) + " [@all](mention://all/all) new owner input",
		"steer":   true, "steer_task_ids": []string{legacyTask}, "suppress_agent_ids": []string{owner},
	}, "", "")
	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 1 || sessions[0].AgentID != owner {
		t.Fatalf("legacy fields redirected owner session: %+v", sessions)
	}
	events := sessions[0].Events
	if len(events) != 2 || events[1].SourceID == nil || *events[1].SourceID != comment.ID || events[1].Status != "pending" {
		t.Fatalf("owner input was suppressed: %+v", events)
	}
	if len(comment.Supplements) != 0 {
		t.Fatalf("comment still carries legacy steering receipts: %+v", comment)
	}
	for _, outcome := range comment.TriggerOutcomes {
		if outcome.TargetType != "agent" || outcome.TargetID != owner {
			t.Fatalf("compatibility receipt redirected ownership: %+v", outcome)
		}
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM task_supplement WHERE comment_id=$1", comment.ID); n != 0 {
		t.Fatalf("legacy steering created %d supplements", n)
	}
}
