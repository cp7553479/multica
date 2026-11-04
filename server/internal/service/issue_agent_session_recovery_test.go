package service

import (
	"errors"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
)

func TestIssueAgentSessionExplicitRetryRequiresOriginalRuntimeAndPreservesIdentity(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := t.Context()
	issue, session := newAssignedSessionIssue(t, f, "restore original runtime")
	task := claimSessionExecution(t, f, session, true)
	if _, err := f.svc.CompleteTask(ctx, task.ID, []byte(`{"output":""}`), "original-provider-thread", "/tmp/original-issue", "", false, "", ""); err != nil {
		t.Fatal(err)
	}
	f.fx.Comment(t, util.UUIDToString(issue.ID), "continue the existing conversation")
	head, err := f.svc.Queries.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	otherRuntime := f.fx.Runtime(t, "temporary other runtime")
	f.fx.Exec(t, "UPDATE agent SET runtime_id=$2 WHERE id=$1", f.agentID, otherRuntime)
	if next, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil || next != nil {
		t.Fatalf("changed runtime unexpectedly woke: task=%+v err=%v", next, err)
	}
	blocked, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || blocked.FailureReason.String != "runtime_changed" {
		t.Fatalf("changed runtime session=%+v err=%v", blocked, err)
	}
	if allowed, err := f.svc.CanRetryIssueAgentSession(ctx, blocked); err != nil || allowed {
		t.Fatalf("other-runtime retry eligibility=%v err=%v", allowed, err)
	}
	if err := f.svc.RetryIssueAgentEvent(ctx, session.ID, head.ID); !errors.Is(err, ErrIssueAgentSessionResumeBlocked) {
		t.Fatalf("other-runtime retry error=%v", err)
	}
	if current, err := f.svc.Queries.GetIssueAgentEvent(ctx, head.ID); err != nil || current.Status != "pending" || current.DeliveryTaskID.Valid {
		t.Fatalf("rejected retry mutated receipt=%+v err=%v", current, err)
	}
	f.fx.Exec(t, "UPDATE agent SET runtime_id=$2 WHERE id=$1", f.agentID, f.runtimeID)
	if allowed, err := f.svc.CanRetryIssueAgentSession(ctx, blocked); err != nil || !allowed {
		t.Fatalf("restored-runtime retry eligibility=%v err=%v", allowed, err)
	}
	if err := f.svc.RetryIssueAgentEvent(ctx, session.ID, head.ID); err != nil {
		t.Fatal(err)
	}
	restored, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || !restored.ActiveTaskID.Valid || restored.RuntimeID != f.runtimeID || restored.ProviderSessionID.String != "original-provider-thread" || restored.WorkDir.String != "/tmp/original-issue" {
		t.Fatalf("restored runtime changed logical identity=%+v err=%v", restored, err)
	}
}

func TestIssueAgentSessionKnownPointerCanExplicitlyRevalidateRepairedStorage(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := t.Context()
	issue, session := newAssignedSessionIssue(t, f, "repair original provider storage")
	task := claimSessionExecution(t, f, session, true)
	if err := f.svc.PinIssueAgentSession(ctx, task.ID, "known-original-thread", "/tmp/known-original-issue"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.svc.FailTask(ctx, task.ID, "provider history is temporarily unavailable", "", "", "", "agent_error.unknown", true, "", ""); err != nil {
		t.Fatal(err)
	}
	f.fx.Comment(t, util.UUIDToString(issue.ID), "retry after restoring the original storage")
	head, err := f.svc.Queries.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	blocked, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || blocked.FailureReason.String != "resume_unavailable" {
		t.Fatalf("storage failure=%+v err=%v", blocked, err)
	}
	if allowed, err := f.svc.CanRetryIssueAgentSession(ctx, blocked); err != nil || !allowed {
		t.Fatalf("known original storage cannot be revalidated: allowed=%v err=%v", allowed, err)
	}
	if err := f.svc.RetryIssueAgentEvent(ctx, session.ID, head.ID); err != nil {
		t.Fatal(err)
	}
	restored, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || restored.ProviderSessionID != blocked.ProviderSessionID || restored.WorkDir != blocked.WorkDir || !restored.ActiveTaskID.Valid {
		t.Fatalf("storage revalidation replaced identity=%+v err=%v", restored, err)
	}
}

func TestIssueAgentSessionRuntimeRoundTripCannotHideUnknownFirstSession(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := t.Context()
	issue, session := newAssignedSessionIssue(t, f, "unknown first provider identity")
	task := claimSessionExecution(t, f, session, true)
	if _, err := f.svc.FailTask(ctx, task.ID, "provider disappeared before returning its ID", "", "/tmp/unknown-first-issue", "", "agent_error.unknown", false, "", ""); err != nil {
		t.Fatal(err)
	}
	f.fx.Comment(t, util.UUIDToString(issue.ID), "must not create a fresh thread")
	otherRuntime := f.fx.Runtime(t, "temporary alternate runtime", testutil.Cols{"visibility": "public"})
	f.fx.Exec(t, "UPDATE agent SET runtime_id=$2 WHERE id=$1", f.agentID, otherRuntime)
	if _, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil {
		t.Fatal(err)
	}
	f.fx.Exec(t, "UPDATE agent SET runtime_id=$2 WHERE id=$1", f.agentID, f.runtimeID)
	blocked, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := f.svc.CanRetryIssueAgentSession(ctx, blocked); err != nil || allowed {
		t.Fatalf("runtime round trip hid unknown first thread: allowed=%v err=%v", allowed, err)
	}
	head, err := f.svc.Queries.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.RetryIssueAgentEvent(ctx, session.ID, head.ID); !errors.Is(err, ErrIssueAgentSessionResumeBlocked) {
		t.Fatalf("unknown first session retried fresh: %v", err)
	}
}

func TestIssueAgentSessionReassignmentCannotFreshStartUnknownConversation(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := t.Context()
	issue, session := newAssignedSessionIssue(t, f, "reassignment of unknown first conversation")
	task := claimSessionExecution(t, f, session, true)
	if _, err := f.svc.FailTask(ctx, task.ID, "provider disappeared before its first durable ID", "", "/tmp/original-unknown-thread", "", "agent_error.unknown", false, "", ""); err != nil {
		t.Fatal(err)
	}
	write, err := f.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer write.Rollback(ctx)
	if _, err := write.Exec(ctx, `SELECT set_config('multica.actor_type','member',true),set_config('multica.actor_id',$1,true)`, f.fx.UserID); err != nil {
		t.Fatal(err)
	}
	if _, err := write.Exec(ctx, `UPDATE issue SET assignee_type='member',assignee_id=$2,revision=revision+1 WHERE id=$1`, issue.ID, f.fx.UserID); err != nil {
		t.Fatal(err)
	}
	if _, err := write.Exec(ctx, `UPDATE issue SET assignee_type='agent',assignee_id=$2,revision=revision+1 WHERE id=$1`, issue.ID, f.agentID); err != nil {
		t.Fatal(err)
	}
	if err := write.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	ready, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || ready.State != "ready" {
		t.Fatalf("capture did not reactivate reassigned session: session=%+v err=%v", ready, err)
	}
	if next, err := f.svc.WakeIssueAgentSession(ctx, session.ID); err != nil || next != nil {
		t.Fatalf("reassignment silently fresh-started unknown conversation: task=%+v err=%v", next, err)
	}
	blocked, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || blocked.State != "blocked" || blocked.FailureReason.String != "resume_unavailable" || blocked.ActiveTaskID.Valid || blocked.ProviderSessionID.Valid {
		t.Fatalf("wake continuity fence did not preserve unknown conversation: session=%+v err=%v", blocked, err)
	}
	if count := f.fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue.ID); count != 1 {
		t.Fatalf("reassignment produced %d executions, want original only", count)
	}
}
