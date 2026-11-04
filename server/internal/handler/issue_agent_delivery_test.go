package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type issueDeliveryFixture struct {
	issueID, sessionID, taskID, agentID string
}

func newIssueDeliveryFixture(t *testing.T) issueDeliveryFixture {
	t.Helper()
	issueID, sessionID, taskID, _, _ := issueAgentSessionFixture(t)
	var agentID string
	dbfx.QueryRow(t, `SELECT agent_id FROM issue_agent_session WHERE id=$1`, sessionID).Scan(&agentID)
	return issueDeliveryFixture{issueID, sessionID, taskID, agentID}
}

func readIssueDeliveries(t *testing.T, h *Handler, userID, issueID string, commentIDs ...string) IssueAgentDeliveriesResponse {
	t.Helper()
	if commentIDs == nil {
		commentIDs = []string{}
	}
	req := withURLParam(newRequestAs(userID, http.MethodPost, "/api/issues/"+issueID+"/agent-deliveries", map[string]any{"comment_ids": commentIDs}), "id", issueID)
	var out IssueAgentDeliveriesResponse
	testutil.Call(t, h.ListIssueAgentDeliveries, req).Want(http.StatusOK).JSON(&out)
	return out
}

func (f issueDeliveryFixture) start(t *testing.T) []db.IssueAgentEvent {
	t.Helper()
	task, err := testHandler.TaskService.ClaimTask(t.Context(), parseUUID(f.agentID))
	if err != nil || task == nil || uuidToString(task.ID) != f.taskID {
		t.Fatalf("claim execution: task=%+v error=%v", task, err)
	}
	batch, err := testHandler.TaskService.ClaimIssueAgentEvents(t.Context(), task.ID, 32)
	if err != nil || len(batch) == 0 {
		t.Fatalf("claim initial inbox: len=%d error=%v", len(batch), err)
	}
	if _, err := testHandler.TaskService.StartTask(t.Context(), task.ID, service.TaskStartCapabilities{Supplement: true}); err != nil {
		t.Fatalf("start issue execution: %v", err)
	}
	return batch
}

func (f issueDeliveryFixture) ack(t *testing.T, events []db.IssueAgentEvent) {
	t.Helper()
	for _, event := range events {
		if err := testHandler.TaskService.AckIssueAgentEvent(t.Context(), parseUUID(f.taskID), event.ID, true, ""); err != nil {
			t.Fatalf("ack event %s: %v", uuidToString(event.ID), err)
		}
	}
}

func (f issueDeliveryFixture) claim(t *testing.T) []db.IssueAgentEvent {
	t.Helper()
	batch, err := testHandler.TaskService.ClaimIssueAgentEvents(t.Context(), parseUUID(f.taskID), 32)
	if err != nil || len(batch) == 0 {
		t.Fatalf("claim live inbox: len=%d error=%v", len(batch), err)
	}
	return batch
}

func TestIssueAgentDeliveriesTrackServiceClaimsAcknowledgementsAndRestart(t *testing.T) {
	f := newIssueDeliveryFixture(t)
	before := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
	if before.Issue.SourceRevision == nil || len(before.Issue.Receipts) != 1 || before.Issue.Receipts[0].Status != "pending" {
		t.Fatalf("assigned input before claim = %+v", before.Issue)
	}
	initial := f.start(t)
	claimed := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
	if len(claimed.Issue.Receipts) != 1 || claimed.Issue.Receipts[0].Status != "delivering" || claimed.Issue.Receipts[0].DeliveredAt != nil {
		t.Fatalf("claimed issue marker = %+v", claimed.Issue)
	}
	f.ack(t, initial)
	commentID := dbfx.Comment(t, f.issueID, "Please read this input")
	pending := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	if len(pending.Comments) != 1 || len(pending.Comments[0].Receipts) != 1 || pending.Comments[0].Receipts[0].Status != "pending" {
		t.Fatalf("new comment marker = %+v", pending.Comments)
	}
	events := f.claim(t)
	inFlight := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	if inFlight.Comments[0].Receipts[0].Status != "delivering" {
		t.Fatalf("claimed comment marker = %+v", inFlight.Comments)
	}
	f.ack(t, events)
	const currentAgentName = "Renamed receipt receiver"
	dbfx.Exec(t, `UPDATE agent SET name=$2 WHERE id=$1`, f.agentID, currentAgentName)
	first := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	f.ack(t, events)
	repeated := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	if len(first.Comments[0].Receipts) != 1 || first.Comments[0].Receipts[0].Status != "delivered" || first.Comments[0].Receipts[0].DeliveredAt == nil {
		t.Fatalf("acknowledged comment marker = %+v", first.Comments)
	}
	if receipt := first.Comments[0].Receipts[0]; receipt.AgentName != currentAgentName || receipt.AgentAvatarURL != nil {
		t.Fatalf("receipt did not resolve the current receiver record with optional avatar: %+v", receipt)
	}
	if first.Issue.Receipts[0].Status != "delivered" || first.Issue.Receipts[0].DeliveredAt == nil {
		t.Fatalf("acknowledged issue marker = %+v", first.Issue)
	}
	originalJSON, _ := json.Marshal(first)
	repeatedJSON, _ := json.Marshal(repeated)
	if string(originalJSON) != string(repeatedJSON) {
		t.Fatalf("repeated query/ack changed receipts:\n%s\n%s", originalJSON, repeatedJSON)
	}
	// A newly constructed handler has no delivery state in memory.
	restarted := New(testHandler.Queries, testPool, testHandler.Hub, testHandler.Bus, testHandler.EmailService,
		nil, nil, testHandler.Analytics, Config{})
	persisted := readIssueDeliveries(t, restarted, testUserID, f.issueID, commentID)
	persistedJSON, _ := json.Marshal(persisted)
	if string(originalJSON) != string(persistedJSON) {
		t.Fatalf("recreated handler lost persisted receipts:\n%s\n%s", originalJSON, persistedJSON)
	}
	const avatarURL = "https://example.test/receipt-avatar.png"
	dbfx.Exec(t, `UPDATE agent SET avatar_url=$2 WHERE id=$1`, f.agentID, avatarURL)
	withAvatar := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	if avatar := withAvatar.Comments[0].Receipts[0].AgentAvatarURL; avatar == nil || *avatar != avatarURL {
		t.Fatalf("receipt did not resolve the receiver avatar: %v", avatar)
	}
}

func TestIssueAgentDeliveriesRetryAcknowledgesTheSameFailedEvent(t *testing.T) {
	f := newIssueDeliveryFixture(t)
	f.ack(t, f.start(t))
	commentID := dbfx.Comment(t, f.issueID, "Retry this delivery")
	batch := f.claim(t)
	if len(batch) != 1 {
		t.Fatalf("expected one comment input, got %d", len(batch))
	}
	eventID := batch[0].ID
	if err := testHandler.TaskService.AckIssueAgentEvent(t.Context(), parseUUID(f.taskID), eventID, false, "provider_rejected"); err != nil {
		t.Fatal(err)
	}
	failed := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	if len(failed.Comments[0].Receipts) != 1 || failed.Comments[0].Receipts[0].Status != "failed" || failed.Comments[0].Receipts[0].DeliveredAt != nil {
		t.Fatalf("failed input marker = %+v", failed.Comments)
	}
	if err := testHandler.TaskService.RetryIssueAgentEvent(t.Context(), parseUUID(f.sessionID), eventID); err != nil {
		t.Fatalf("retry failed input: %v", err)
	}
	retried := f.claim(t)
	if len(retried) != 1 || retried[0].ID != eventID {
		t.Fatalf("retry replaced the event: %+v", retried)
	}
	f.ack(t, retried)
	delivered := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	receipts := delivered.Comments[0].Receipts
	if len(receipts) != 1 || receipts[0].EventID != uuidToString(eventID) || receipts[0].Status != "delivered" || receipts[0].DeliveredAt == nil {
		t.Fatalf("retried input marker = %+v", receipts)
	}
}

func TestIssueAgentDeliveriesCommentEditRequiresTheCurrentRevision(t *testing.T) {
	f := newIssueDeliveryFixture(t)
	f.ack(t, f.start(t))
	commentID := dbfx.Comment(t, f.issueID, "Original instruction")
	f.ack(t, f.claim(t))
	before := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	originalRevision := before.Comments[0].SourceRevision
	req := withURLParam(newRequest(http.MethodPatch, "/api/comments/"+commentID, map[string]any{
		"content": "Corrected instruction", "expected_revision": originalRevision,
	}), "commentId", commentID)
	testutil.Call(t, testHandler.UpdateComment, req).Want(http.StatusOK)
	// Unrelated timer receipts must never mark an edited comment as read.
	dbfx.Exec(t, `INSERT INTO issue_agent_event (workspace_id,session_id,sequence,event_key,kind,source_id,source_revision,actor_type,actor_id,payload,status,delivered_at)
		SELECT $1,$2,next_sequence+1,'delivery-wakeup','wakeup.fired',$3,$4,'member',$5,'{}'::jsonb,'delivered',now()
		FROM issue_agent_session WHERE id=$2`, testWorkspaceID, f.sessionID, commentID, originalRevision+1, testUserID)
	after := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	if len(after.Comments) != 1 || after.Comments[0].SourceRevision <= originalRevision || len(after.Comments[0].Receipts) != 1 || after.Comments[0].Receipts[0].Status != "pending" || after.Comments[0].Receipts[0].DeliveredAt != nil {
		t.Fatalf("old ACK or timer marked an edit as read: %+v", after.Comments)
	}
	if after.Comments[0].Receipts[0].EventID == before.Comments[0].Receipts[0].EventID {
		t.Fatal("edited comment reused the previous revision's receipt")
	}
}

func TestIssueAgentDeliveriesReadOldCommentsBeyondInboxPageAndFilterIDs(t *testing.T) {
	f := newIssueDeliveryFixture(t)
	f.ack(t, f.start(t))
	oldComment := dbfx.Comment(t, f.issueID, "Old acknowledged input")
	f.ack(t, f.claim(t))
	withoutReceipt := dbfx.Comment(t, f.issueID, "System timeline entry", testutil.Cols{"type": "system"})
	otherIssue := dbfx.Issue(t, "Other comment issue")
	otherComment := dbfx.Comment(t, otherIssue, "Not requested issue")
	foreignIssue, _ := setupForeignWorkspaceFixture(t)
	var foreignWorkspace string
	dbfx.QueryRow(t, `SELECT workspace_id FROM issue WHERE id=$1`, foreignIssue).Scan(&foreignWorkspace)
	foreignComment := dbfx.Comment(t, foreignIssue, "Other workspace input", testutil.Cols{"workspace_id": foreignWorkspace})
	// Push the acknowledged input out of the normal session's recent 199 events.
	dbfx.Exec(t, `INSERT INTO issue_agent_event (workspace_id,session_id,sequence,event_key,kind,source_id,source_revision,actor_type,actor_id,payload,status)
		SELECT $1,$2,n,'delivery-history:'||n,'wakeup.fired',$3,n,'member',$4,'{}'::jsonb,'pending'
		FROM generate_series(3,207) n`, testWorkspaceID, f.sessionID, f.issueID, testUserID)
	beforeEvents := dbfx.Count(t, `SELECT count(*) FROM issue_agent_event WHERE session_id=$1`, f.sessionID)
	beforeTasks := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, f.issueID)
	out := readIssueDeliveries(t, testHandler, testUserID, f.issueID, oldComment, withoutReceipt, oldComment, otherComment, foreignComment)
	byID := make(map[string]IssueAgentCommentDelivery)
	for _, comment := range out.Comments {
		if _, duplicate := byID[comment.CommentID]; duplicate {
			t.Fatalf("duplicate requested ID repeated response: %s", comment.CommentID)
		}
		byID[comment.CommentID] = comment
	}
	if len(byID) != 2 || len(byID[oldComment].Receipts) != 1 || byID[oldComment].Receipts[0].Status != "delivered" || len(byID[withoutReceipt].Receipts) != 0 {
		t.Fatalf("requested comments beyond recent inbox = %+v", out.Comments)
	}
	if _, leaked := byID[otherComment]; leaked {
		t.Fatal("cross-issue comment leaked")
	}
	if _, leaked := byID[foreignComment]; leaked {
		t.Fatal("cross-workspace comment leaked")
	}
	if dbfx.Count(t, `SELECT count(*) FROM issue_agent_event WHERE session_id=$1`, f.sessionID) != beforeEvents || dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, f.issueID) != beforeTasks {
		t.Fatal("read-only delivery lookup changed inbox events or execution count")
	}
	foreignReq := withURLParam(newRequest(http.MethodPost, "/agent-deliveries", map[string]any{"comment_ids": []string{foreignComment}}), "id", foreignIssue)
	testutil.Call(t, testHandler.ListIssueAgentDeliveries, foreignReq).Want(http.StatusNotFound)
}

func TestIssueAgentDeliveriesFilterPrivateReceiverByViewPermission(t *testing.T) {
	agentID, ownerID, memberID := privateAgentTestFixture(t)
	issueID := dbfx.Issue(t, "Private receiver issue", testutil.Cols{"assignee_type": "agent", "assignee_id": agentID})
	commentID := dbfx.Comment(t, issueID, "Visible issue, private receiver")
	owner := readIssueDeliveries(t, testHandler, ownerID, issueID, commentID)
	if len(owner.Issue.Receipts) != 1 || len(owner.Comments) != 1 || len(owner.Comments[0].Receipts) != 1 {
		t.Fatalf("agent owner lost viewable receipts: %+v", owner)
	}
	member := readIssueDeliveries(t, testHandler, memberID, issueID, commentID)
	if len(member.Issue.Receipts) != 0 || len(member.Comments) != 1 || len(member.Comments[0].Receipts) != 0 {
		t.Fatalf("private receiver receipts leaked to plain member: %+v", member)
	}
	if n := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id=$1`, issueID); n != 0 {
		t.Fatalf("read-only delivery lookup woke %d executions", n)
	}
}

func TestIssueAgentDeliveriesIssueMarkerRequiresMatchingSnapshotAndAssignment(t *testing.T) {
	f := newIssueDeliveryFixture(t)
	f.ack(t, f.start(t))
	var eventID string
	var payload []byte
	dbfx.QueryRow(t, `SELECT id,payload FROM issue_agent_event WHERE session_id=$1 AND kind='assigned'`, f.sessionID).Scan(&eventID, &payload)
	before := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
	if len(before.Issue.Receipts) != 1 || before.Issue.Receipts[0].Status != "delivered" {
		t.Fatalf("initial marker = %+v", before.Issue)
	}
	for _, field := range []struct{ name, replacement string }{
		{"title", `"A different title"`}, {"description", `"A different description"`}, {"status", `"backlog"`}, {"stage", `12345`}, {"assignee_id", `"0199a4e8-22ce-7b01-bba5-111111111111"`},
	} {
		t.Run(field.name, func(t *testing.T) {
			dbfx.Exec(t, `UPDATE issue_agent_event SET payload=jsonb_set($2::jsonb,ARRAY[$3::text],$4::jsonb) WHERE id=$1`, eventID, payload, field.name, field.replacement)
			out := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
			if len(out.Issue.Receipts) != 0 {
				t.Fatalf("mismatched %s retained issue marker: %+v", field.name, out.Issue)
			}
		})
	}
	dbfx.Exec(t, `UPDATE issue_agent_event SET payload=$2 WHERE id=$1`, eventID, payload)
	dbfx.Exec(t, `UPDATE issue SET assignee_type=NULL,assignee_id=NULL,revision=revision+1 WHERE id=$1`, f.issueID)
	unassigned := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
	if len(unassigned.Issue.Receipts) != 0 || unassigned.Issue.AssigneeID != nil {
		t.Fatalf("unassigned issue retained delivery marker: %+v", unassigned.Issue)
	}
}

func TestIssueAgentDeliveriesRejectInvalidAndOversizedReadRequests(t *testing.T) {
	f := newIssueDeliveryFixture(t)
	many := make([]string, 2001)
	for i := range many {
		many[i] = fmt.Sprintf("0199a4e8-22ce-7b01-bba5-%012x", i+1)
	}
	for _, body := range []any{
		map[string]any{"comment_ids": []string{"not-a-uuid"}},
		map[string]any{"comment_ids": many},
		`{"comment_ids":[],"extra":"` + strings.Repeat("x", 128*1024) + `"}`,
		`{"comment_ids":`,
	} {
		req := testutil.JSONRequest(http.MethodPost, "/agent-deliveries", body)
		req.Header.Set("X-User-ID", testUserID)
		req.Header.Set("X-Workspace-ID", testWorkspaceID)
		testutil.Call(t, testHandler.ListIssueAgentDeliveries, withURLParam(req, "id", f.issueID)).Want(http.StatusBadRequest)
	}
}

func TestIssueAgentDeliveriesUseOneLatestIssueSnapshotAcrossReceivers(t *testing.T) {
	f := newIssueDeliveryFixture(t)
	f.ack(t, f.start(t))
	var runtimeID string
	var originalPayload []byte
	dbfx.QueryRow(t, `SELECT runtime_id FROM issue_agent_session WHERE id=$1`, f.sessionID).Scan(&runtimeID)
	dbfx.QueryRow(t, `SELECT payload FROM issue_agent_event WHERE session_id=$1 AND kind='assigned'`, f.sessionID).Scan(&originalPayload)
	otherAgent := dbfx.Agent(t, "Another issue receiver", runtimeID)
	otherSession := dbfx.Insert(t, "issue_agent_session", testutil.Cols{
		"workspace_id": testWorkspaceID, "issue_id": f.issueID, "agent_id": otherAgent, "runtime_id": runtimeID,
	})
	dbfx.Exec(t, `INSERT INTO issue_agent_event (workspace_id,session_id,sequence,event_key,kind,source_id,source_revision,actor_type,actor_id,payload,status,delivered_at)
		VALUES ($1,$2,1,'delivery-secondary-old','assigned',$3,1,'member',$4,$5,'delivered',now())`, testWorkspaceID, otherSession, f.issueID, testUserID, originalPayload)
	sameSnapshot := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
	issueReceivers := make(map[string]string)
	for _, receipt := range sameSnapshot.Issue.Receipts {
		issueReceivers[receipt.SessionID] = receipt.AgentID
		if receipt.SourceRevision != 1 || receipt.SourceID != f.issueID {
			t.Fatalf("same-version receiver has wrong source: %+v", receipt)
		}
	}
	if len(sameSnapshot.Issue.Receipts) != 2 || issueReceivers[f.sessionID] != f.agentID || issueReceivers[otherSession] != otherAgent {
		t.Fatalf("same-version issue receivers were collapsed: %+v", sameSnapshot.Issue.Receipts)
	}
	commentID := dbfx.Comment(t, f.issueID, "Input delivered to two receivers")
	dbfx.Exec(t, `INSERT INTO issue_agent_event (workspace_id,session_id,sequence,event_key,kind,source_id,source_revision,actor_type,actor_id,payload,status,delivered_at)
		SELECT workspace_id,$2,2,event_key,kind,source_id,source_revision,actor_type,actor_id,payload,'delivered',now()
		FROM issue_agent_event WHERE session_id=$1 AND source_id=$3 AND kind='comment.created'`, f.sessionID, otherSession, commentID)
	commentSnapshot := readIssueDeliveries(t, testHandler, testUserID, f.issueID, commentID)
	if len(commentSnapshot.Comments) != 1 || len(commentSnapshot.Comments[0].Receipts) != 2 {
		t.Fatalf("same-version comment receivers were collapsed: %+v", commentSnapshot.Comments)
	}
	commentReceivers := make(map[string]string)
	for _, receipt := range commentSnapshot.Comments[0].Receipts {
		commentReceivers[receipt.SessionID] = receipt.AgentID
		if receipt.SourceRevision != commentSnapshot.Comments[0].SourceRevision || receipt.SourceID != commentID {
			t.Fatalf("comment receiver lost exact source version: %+v", receipt)
		}
	}
	if commentReceivers[f.sessionID] != f.agentID || commentReceivers[otherSession] != otherAgent {
		t.Fatalf("comment receiver identity changed: %+v", commentSnapshot.Comments[0].Receipts)
	}
	req := withURLParam(newRequest(http.MethodPatch, "/api/issues/"+f.issueID, map[string]any{"title": "Latest captured issue title"}), "id", f.issueID)
	testutil.Call(t, testHandler.UpdateIssue, req).Want(http.StatusOK)
	current := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
	if current.Issue.SourceRevision == nil || *current.Issue.SourceRevision <= 1 || len(current.Issue.Receipts) != 1 || current.Issue.Receipts[0].SessionID != f.sessionID || current.Issue.Receipts[0].SourceRevision != *current.Issue.SourceRevision {
		t.Fatalf("receivers mixed current and historical issue snapshots: %+v", current.Issue)
	}
	// A newer captured snapshot that disagrees with the live issue cannot cause
	// a per-receiver fallback to either older acknowledged snapshot.
	dbfx.Exec(t, `INSERT INTO issue_agent_event (workspace_id,session_id,sequence,event_key,kind,source_id,source_revision,actor_type,actor_id,payload,status,delivered_at)
		VALUES ($1,$2,3,'delivery-secondary-newer','issue.updated',$3,$4,'member',$5,$6,'delivered',now())`,
		testWorkspaceID, otherSession, f.issueID, *current.Issue.SourceRevision+1, testUserID, originalPayload)
	mismatched := readIssueDeliveries(t, testHandler, testUserID, f.issueID)
	if len(mismatched.Issue.Receipts) != 0 {
		t.Fatalf("newer mismatched snapshot reused an older issue marker: %+v", mismatched.Issue)
	}
}
