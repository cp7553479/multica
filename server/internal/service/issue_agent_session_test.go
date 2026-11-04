package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func newIssueSessionFixture(t *testing.T) *claimDecisionFixture {
	t.Helper()
	f := newClaimDecisionFixture(t, 2)
	f.svc.IssueSessionCanInvoke = func(_ context.Context, agent db.Agent, event db.IssueAgentEvent) bool {
		return event.ActorType == "member" && event.ActorID == agent.OwnerID
	}
	return f
}

func newAssignedSessionIssue(t *testing.T, f *claimDecisionFixture, title string) (db.Issue, db.IssueAgentSession) {
	t.Helper()
	id := f.fx.Issue(t, title, testutil.Cols{"assignee_type": "agent", "assignee_id": util.UUIDToString(f.agentID)})
	issue, err := f.svc.Queries.GetIssue(context.Background(), util.MustParseUUID(id))
	if err != nil {
		t.Fatal(err)
	}
	session, err := f.svc.Queries.GetIssueAgentSessionForPair(context.Background(), db.GetIssueAgentSessionForPairParams{WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, AgentID: f.agentID})
	if err != nil {
		t.Fatalf("assignment did not capture session: %v", err)
	}
	return issue, session
}

func claimSessionExecution(t *testing.T, f *claimDecisionFixture, session db.IssueAgentSession, ackInitial bool) db.AgentTaskQueue {
	t.Helper()
	ctx := context.Background()
	if _, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil {
		t.Fatalf("wake session: %v", err)
	}
	task, err := f.svc.ClaimTask(ctx, f.agentID)
	if err != nil || task == nil {
		t.Fatalf("claim session task: task=%v err=%v", task, err)
	}
	batch, err := f.svc.ClaimIssueAgentEvents(ctx, task.ID, 32)
	if err != nil || len(batch) == 0 {
		t.Fatalf("claim initial events: len=%d err=%v", len(batch), err)
	}
	if _, err := f.svc.StartTask(ctx, task.ID, TaskStartCapabilities{Supplement: true}); err != nil {
		t.Fatalf("start session task: %v", err)
	}
	if ackInitial {
		for _, event := range batch {
			if err := f.svc.AckIssueAgentEvent(ctx, task.ID, event.ID, true, ""); err != nil {
				t.Fatalf("ack initial event: %v", err)
			}
		}
	}
	return *task
}

func TestIssueAgentSessionsDifferentIssuesRunInParallelAndCommentsStayInSession(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issueA, sessionA := newAssignedSessionIssue(t, f, "A")
	_, sessionB := newAssignedSessionIssue(t, f, "B")
	a := claimSessionExecution(t, f, sessionA, true)
	b := claimSessionExecution(t, f, sessionB, true)
	if a.ID == b.ID || IssueAgentSessionID(a) == IssueAgentSessionID(b) {
		t.Fatal("different issues shared an execution or session")
	}
	comment := f.fx.Comment(t, util.UUIDToString(issueA.ID), "A2 arrives while A1 runs")
	if task, err := f.svc.WakeIssueAgentSession(ctx, sessionA.ID); err != nil || task != nil {
		t.Fatalf("busy session created another task: task=%v err=%v", task, err)
	}
	if got := f.fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issueA.ID); got != 1 {
		t.Fatalf("A has %d runs after A2, want the one existing execution", got)
	}
	event, err := f.svc.ClaimIssueAgentEvent(ctx, a.ID)
	if err != nil || event == nil || event.SourceID != util.MustParseUUID(comment) {
		t.Fatalf("A2 did not reach A session: event=%+v err=%v", event, err)
	}
	if err := f.svc.AckIssueAgentEvent(ctx, a.ID, event.ID, true, ""); err != nil {
		t.Fatal(err)
	}
	f.fx.Comment(t, util.UUIDToString(issueA.ID), "agent's explicit result", testutil.Cols{"author_type": "agent", "author_id": util.UUIDToString(f.agentID), "source_task_id": util.UUIDToString(a.ID)})
	if again, err := f.svc.ClaimIssueAgentEvent(ctx, a.ID); err != nil || again != nil {
		t.Fatalf("agent's own write created a loop: event=%+v err=%v", again, err)
	}
	if a.TriggerCommentID.Valid || b.TriggerCommentID.Valid {
		t.Fatal("session executions still belong to a triggering comment")
	}
}

func TestIssueAgentSessionCompletionWakesPendingEventsAndKeepsPointer(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "persistent conversation")
	first := claimSessionExecution(t, f, session, true)
	comment := f.fx.Comment(t, util.UUIDToString(issue.ID), "arrived at completion boundary")
	if _, err := f.svc.CompleteTask(ctx, first.ID, []byte(`{"output":""}`), "thread-A", "/tmp/issue-A", "branch-A", false, "", ""); err != nil {
		t.Fatal(err)
	}
	saved, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || saved.ProviderSessionID.String != "thread-A" || saved.WorkDir.String != "/tmp/issue-A" || !saved.ActiveTaskID.Valid {
		t.Fatalf("completion did not preserve pointer and wake pending input: session=%+v err=%v", saved, err)
	}
	if saved.ActiveTaskID == first.ID {
		t.Fatal("completed task retained active ownership")
	}
	next, err := f.svc.ClaimTask(ctx, f.agentID)
	if err != nil || next == nil || IssueAgentSessionID(*next) != session.ID {
		t.Fatalf("next execution changed conversation: task=%+v err=%v", next, err)
	}
	batch, err := f.svc.ClaimIssueAgentEvents(ctx, next.ID, 32)
	if err != nil || len(batch) != 1 || batch[0].SourceID != util.MustParseUUID(comment) {
		t.Fatalf("boundary event lost or replayed initial input: events=%+v err=%v", batch, err)
	}
	if got := f.fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue.ID); got != 2 {
		t.Fatalf("inbox produced %d executions, want initial+one resumed execution", got)
	}
}

func TestIssueAgentSessionEventDeduplication(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "dedup")
	commentID := f.fx.Comment(t, util.UUIDToString(issue.ID), "same durable source")
	comment, err := f.svc.Queries.GetComment(ctx, util.MustParseUUID(commentID))
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.svc.PublishIssueAgentEvent(ctx, issue, f.agentID, IssueAgentEventInput{
		Kind: "comment.created", SourceID: comment.ID, SourceRevision: comment.Revision,
		ActorType: "member", ActorID: util.MustParseUUID(f.fx.UserID), Payload: json.RawMessage(`{"content":"duplicate"}`),
	})
	if err != nil || !result.Duplicate || result.Event.SessionID != session.ID {
		t.Fatalf("duplicate capture was not reused: result=%+v err=%v", result, err)
	}
	if got := f.fx.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND source_id=$2", session.ID, comment.ID); got != 1 {
		t.Fatalf("source event count = %d, want 1", got)
	}
}

func TestIssueAgentSessionRuntimePollRecoversCommittedInputAfterServiceRestart(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "restart recovery")
	first := claimSessionExecution(t, f, session, true)
	if _, err := f.svc.CompleteTask(ctx, first.ID, []byte(`{"output":""}`), "persistent-thread", "/tmp/persistent-restart-workdir", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	// The content and inbox commit, then the process disappears before sending
	// a wake hint. A fresh service must reconstruct everything from PostgreSQL.
	commentID := f.fx.Comment(t, util.UUIDToString(issue.ID), "committed before the process stopped")
	restarted := NewTaskService(db.New(f.pool), f.pool, nil, f.svc.Bus)
	restarted.IssueSessionCanInvoke = f.svc.IssueSessionCanInvoke
	next, err := restarted.ClaimTaskForRuntime(ctx, f.runtimeID)
	if err != nil || next == nil || IssueAgentSessionID(*next) != session.ID {
		t.Fatalf("runtime poll lost committed input after restart: task=%+v err=%v", next, err)
	}
	saved, err := restarted.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || saved.ProviderSessionID.String != "persistent-thread" || saved.WorkDir.String != "/tmp/persistent-restart-workdir" || saved.ActiveTaskID != next.ID {
		t.Fatalf("restart changed conversation ownership: session=%+v err=%v", saved, err)
	}
	batch, err := restarted.ClaimIssueAgentEvents(ctx, next.ID, 32)
	if err != nil || len(batch) != 1 || batch[0].SourceID != util.MustParseUUID(commentID) {
		t.Fatalf("restart lost or replayed inbox input: batch=%+v err=%v", batch, err)
	}
}

func TestIssueAgentSessionFailureKeepsPendingInputForExplicitRetry(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "retry")
	first := claimSessionExecution(t, f, session, true)
	if _, err := f.svc.FailTask(ctx, first.ID, "provider operation failed", "thread-A", "/tmp/issue-A", "", "agent_error.unknown", false, "", ""); err != nil {
		t.Fatal(err)
	}
	comment := f.fx.Comment(t, util.UUIDToString(issue.ID), "please continue")
	if task, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil || task != nil {
		t.Fatalf("failure silently resumed: task=%+v err=%v", task, err)
	}
	events, err := f.svc.Queries.ListIssueAgentEvents(ctx, db.ListIssueAgentEventsParams{SessionID: session.ID, EventLimit: 10})
	if err != nil {
		t.Fatal(err)
	}
	var pending db.IssueAgentEvent
	for _, event := range events {
		if event.SourceID == util.MustParseUUID(comment) {
			pending = event
		}
	}
	if pending.Status != "pending" {
		t.Fatalf("new comment state = %s, want pending behind explicit failure", pending.Status)
	}
	if err := f.svc.RetryIssueAgentEvent(ctx, session.ID, pending.ID); err != nil {
		t.Fatal(err)
	}
	saved, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || !saved.ActiveTaskID.Valid || saved.ProviderSessionID.String != "thread-A" {
		t.Fatalf("explicit retry did not preserve conversation: session=%+v err=%v", saved, err)
	}
}

func TestIssueAgentSessionUnacknowledgedDeliveryNeverSilentlyReplays(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "uncertain delivery")
	first := claimSessionExecution(t, f, session, false)
	if _, err := f.svc.CompleteTask(ctx, first.ID, []byte(`{"output":""}`), "thread-A", "/tmp/issue-A", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	head, err := f.svc.Queries.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
	if err != nil || head.Status != "failed" || head.FailureReason.String != "delivery_unconfirmed" {
		t.Fatalf("uncertain delivery disappeared: event=%+v err=%v", head, err)
	}
	if count, err := f.svc.WakeIssueAgentSessionsForRuntime(ctx, f.runtimeID, 32); err != nil || count != 0 {
		t.Fatalf("sweeper silently replayed uncertain delivery: count=%d err=%v", count, err)
	}
	if got := f.fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue.ID); got != 1 {
		t.Fatalf("uncertain event created %d executions", got)
	}
}

func TestIssueAgentSessionReassignmentDoesNotReviveDetachedWorker(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "reassign")
	first := claimSessionExecution(t, f, session, true)
	if _, err := f.pool.Exec(ctx, `UPDATE issue SET assignee_type='member',assignee_id=$2,revision=revision+1 WHERE id=$1`, issue.ID, util.MustParseUUID(f.fx.UserID)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.CompleteTask(ctx, first.ID, []byte(`{"output":""}`), "thread-A", "/tmp/issue-A", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	saved, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || saved.State != "detached" || saved.ActiveTaskID.Valid || saved.ProviderSessionID.String != "thread-A" {
		t.Fatalf("late completion revived detached session: session=%+v err=%v", saved, err)
	}
}

func TestIssueAgentSessionInvocationDeniedIsVisibleAndDoesNotStartExecution(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "private owner")
	f.svc.IssueSessionCanInvoke = func(context.Context, db.Agent, db.IssueAgentEvent) bool { return false }
	if task, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil || task != nil {
		t.Fatalf("denied owner event started execution: task=%+v err=%v", task, err)
	}
	saved, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || saved.State != "ready" || saved.ActiveTaskID.Valid {
		t.Fatalf("permission failure was hidden: session=%+v err=%v", saved, err)
	}
	receipts, err := f.svc.Queries.ListIssueAgentEvents(ctx, db.ListIssueAgentEventsParams{SessionID: session.ID, EventLimit: 10})
	if err != nil || len(receipts) != 1 || receipts[0].Status != "ignored" || receipts[0].FailureReason.String != "invocation_not_allowed" {
		t.Fatalf("permission failure lacked event receipt: events=%+v err=%v", receipts, err)
	}
	f.svc.IssueSessionCanInvoke = func(_ context.Context, agent db.Agent, event db.IssueAgentEvent) bool {
		return event.ActorType == "member" && event.ActorID == agent.OwnerID
	}
	f.fx.Comment(t, util.UUIDToString(issue.ID), "authorized owner can continue")
	if task, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil || task == nil {
		t.Fatalf("denied message blocked the owner's later input: task=%+v err=%v", task, err)
	}
}

func TestIssueAgentSessionLiveClaimSkipsDeniedMessageAndDeliversOwnerInput(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	issue, session := newAssignedSessionIssue(t, f, "ordered authorized messages")
	task := claimSessionExecution(t, f, session, true)
	other := f.fx.User(t, "other commenter", fmt.Sprintf("session-other-%d@example.com", time.Now().UnixNano()))
	denied := f.fx.Comment(t, util.UUIDToString(issue.ID), "cannot invoke this agent", testutil.Cols{"author_id": other})
	owner := f.fx.Comment(t, util.UUIDToString(issue.ID), "owner follow-up")
	batch, err := f.svc.ClaimIssueAgentEvents(ctx, task.ID, 32)
	if err != nil || len(batch) != 1 || batch[0].SourceID != util.MustParseUUID(owner) {
		t.Fatalf("owner input was blocked or denied input delivered: events=%+v err=%v", batch, err)
	}
	if got := f.fx.Count(t, "SELECT count(*) FROM issue_agent_event WHERE source_id=$1 AND status='ignored' AND failure_reason='invocation_not_allowed'", util.MustParseUUID(denied)); got != 1 {
		t.Fatal("denied live message lacks its visible ignored receipt")
	}
}

func TestIssueAgentSessionPrincipalBoundaryResumesSameConversationWithNewAuthority(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	f.svc.IssueSessionCanInvoke = func(context.Context, db.Agent, db.IssueAgentEvent) bool { return true }
	issue, session := newAssignedSessionIssue(t, f, "public agent with two authorized humans")
	first := claimSessionExecution(t, f, session, true)
	secondUser := f.fx.User(t, "second authorized commenter", fmt.Sprintf("session-second-%d@example.com", time.Now().UnixNano()))
	f.fx.Member(t, f.fx.WorkspaceID, secondUser, "member")
	secondInput := f.fx.Comment(t, util.UUIDToString(issue.ID), "second human's request", testutil.Cols{"author_id": secondUser})
	f.fx.Comment(t, util.UUIDToString(issue.ID), "first human follows second")
	if event, err := f.svc.ClaimIssueAgentEvent(ctx, first.ID); err != nil || event != nil {
		t.Fatalf("second human borrowed first human's active authority: event=%+v err=%v", event, err)
	}
	if _, err := f.svc.CompleteTask(ctx, first.ID, []byte(`{"output":""}`), "shared-thread", "/tmp/shared-issue", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	second, err := f.svc.ClaimTask(ctx, f.agentID)
	if err != nil || second == nil || second.OriginatorUserID != util.MustParseUUID(secondUser) || IssueAgentSessionID(*second) != session.ID {
		t.Fatalf("authority boundary changed session or retained first principal: task=%+v err=%v", second, err)
	}
	batch, err := f.svc.ClaimIssueAgentEvents(ctx, second.ID, 32)
	if err != nil || len(batch) != 1 || batch[0].SourceID != util.MustParseUUID(secondInput) {
		t.Fatalf("initial batch crossed next principal boundary: events=%+v err=%v", batch, err)
	}
	saved, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || saved.ProviderSessionID.String != "shared-thread" || saved.WorkDir.String != "/tmp/shared-issue" {
		t.Fatalf("principal boundary lost persistent conversation: session=%+v err=%v", saved, err)
	}
}

func TestIssueAgentSessionMissingFirstProviderPointerCannotStartFreshThread(t *testing.T) {
	for _, terminal := range []string{"failed", "completed"} {
		t.Run(terminal, func(t *testing.T) {
			f := newIssueSessionFixture(t)
			ctx := context.Background()
			issue, session := newAssignedSessionIssue(t, f, "first provider pointer missing")
			task := claimSessionExecution(t, f, session, true)
			if terminal == "failed" {
				if _, err := f.svc.FailTask(ctx, task.ID, "lost provider process before pin", "", "/tmp/issue-first", "", "agent_error.unknown", false, "", ""); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.svc.CompleteTask(ctx, task.ID, []byte(`{"output":""}`), "", "/tmp/issue-first", "", false, "", ""); err != nil {
					t.Fatal(err)
				}
			}
			comment := f.fx.Comment(t, util.UUIDToString(issue.ID), "continue the same conversation")
			events, err := f.svc.Queries.ListIssueAgentEvents(ctx, db.ListIssueAgentEventsParams{SessionID: session.ID, EventLimit: 10})
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range events {
				if event.SourceID == util.MustParseUUID(comment) && f.svc.RetryIssueAgentEvent(ctx, session.ID, event.ID) == nil {
					t.Fatal("missing started session pointer silently permitted fresh execution")
				}
			}
			saved, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
			if err != nil || saved.State != "blocked" || saved.FailureReason.String != "resume_unavailable" || saved.ActiveTaskID.Valid {
				t.Fatalf("missing pointer did not fail visibly: session=%+v err=%v", saved, err)
			}
		})
	}
}

func TestIssueAgentSessionBacklogPreservesInboxUntilIssueIsReady(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	id := f.fx.Issue(t, "parked issue", testutil.Cols{"status": "backlog", "assignee_type": "agent", "assignee_id": util.UUIDToString(f.agentID)})
	session, err := f.svc.Queries.GetIssueAgentSessionForPair(ctx, db.GetIssueAgentSessionForPairParams{WorkspaceID: util.MustParseUUID(f.fx.WorkspaceID), IssueID: util.MustParseUUID(id), AgentID: f.agentID})
	if err != nil {
		t.Fatal(err)
	}
	f.fx.Comment(t, id, "note while parked")
	if task, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil || task != nil {
		t.Fatalf("backlog ran its inbox: task=%+v err=%v", task, err)
	}
	if _, err := f.pool.Exec(ctx, "UPDATE issue SET status='todo', revision=revision+1 WHERE id=$1", session.IssueID); err != nil {
		t.Fatal(err)
	}
	if count, err := f.svc.WakeIssueAgentSessionsForRuntime(ctx, f.runtimeID, 32); err != nil || count != 1 {
		t.Fatalf("ready issue did not recover its same inbox: count=%d err=%v", count, err)
	}
}

func TestIssueAgentSessionTerminalSourceCannotLendHumanIdentityThroughPublicAgent(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := context.Background()
	otherOwner := f.fx.User(t, "public agent owner", fmt.Sprintf("session-public-owner-%d@example.com", time.Now().UnixNano()))
	f.fx.Member(t, f.fx.WorkspaceID, otherOwner, "member")
	publicAgent := f.fx.Agent(t, "public B", util.UUIDToString(f.runtimeID), testutil.Cols{"owner_id": otherOwner})
	privateAgent := f.fx.Agent(t, "private C", util.UUIDToString(f.runtimeID))
	parent := f.fx.Task(t, util.UUIDToString(f.agentID), testutil.Cols{
		"runtime_id": util.UUIDToString(f.runtimeID), "issue_id": f.issueID, "status": "completed",
		"originator_user_id": f.fx.UserID, "accountable_user_id": f.fx.UserID, "originator_source": "direct_human",
	})
	f.svc.IssueSessionCanInvoke = func(_ context.Context, agent db.Agent, event db.IssueAgentEvent) bool {
		return agent.ID == util.MustParseUUID(publicAgent) || event.OriginatorUserID == agent.OwnerID
	}
	makeTarget := func(agentID, title string) db.IssueAgentSession {
		id := f.fx.Issue(t, title, testutil.Cols{"assignee_type": "agent", "assignee_id": agentID})
		session, err := f.svc.Queries.GetIssueAgentSessionForPair(ctx, db.GetIssueAgentSessionForPairParams{WorkspaceID: util.MustParseUUID(f.fx.WorkspaceID), IssueID: util.MustParseUUID(id), AgentID: util.MustParseUUID(agentID)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.pool.Exec(ctx, "UPDATE issue_agent_event SET status='ignored' WHERE session_id=$1 AND kind='assigned'", session.ID); err != nil {
			t.Fatal(err)
		}
		return session
	}
	publicSession := makeTarget(publicAgent, "public target")
	f.fx.Comment(t, util.UUIDToString(publicSession.IssueID), "late output from terminal A", testutil.Cols{"author_type": "agent", "author_id": util.UUIDToString(f.agentID), "source_task_id": parent})
	publicTask, err := f.svc.WakeIssueAgentSession(ctx, publicSession.ID)
	if err != nil || publicTask == nil {
		t.Fatalf("public agent could not process unattributed event: task=%+v err=%v", publicTask, err)
	}
	if publicTask.OriginatorUserID.Valid || publicTask.AccountableUserID != util.MustParseUUID(otherOwner) {
		t.Fatalf("public agent borrowed terminal source identity: %+v", publicTask)
	}
	privateSession := makeTarget(privateAgent, "private target")
	f.fx.Comment(t, util.UUIDToString(privateSession.IssueID), "B cannot forward old authority", testutil.Cols{"author_type": "agent", "author_id": publicAgent, "source_task_id": util.UUIDToString(publicTask.ID)})
	if task, err := f.svc.WakeIssueAgentSession(ctx, privateSession.ID); err != nil || task != nil {
		t.Fatalf("terminal A -> public B gained private C authority: task=%+v err=%v", task, err)
	}
	if got := f.fx.Count(t, "SELECT count(*) FROM issue_agent_event WHERE session_id=$1 AND status='ignored' AND failure_reason='invocation_not_allowed'", privateSession.ID); got != 1 {
		t.Fatal("private target did not record denied forwarded event")
	}
}
