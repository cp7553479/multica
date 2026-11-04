package service

import (
	"errors"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
)

func TestIssueAgentSessionPinCommitsExecutionAndLogicalIdentityTogether(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := t.Context()
	_, session := newAssignedSessionIssue(t, f, "atomic first identity")
	task := claimSessionExecution(t, f, session, true)
	originalStarter := f.svc.TxStarter
	commitErr := errors.New("pin commit rejected")
	f.svc.TxStarter = &rejectIssueCommitTxStarter{pool: f.pool, err: commitErr}
	if err := f.svc.PinIssueAgentSession(ctx, task.ID, "durable-provider-thread", "/tmp/durable-session-directory"); !errors.Is(err, commitErr) {
		t.Fatalf("pin commit failure=%v", err)
	}
	assertEmpty := func() {
		t.Helper()
		execution, err := f.svc.Queries.GetAgentTask(ctx, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		logical, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
		if err != nil {
			t.Fatal(err)
		}
		if execution.SessionID.Valid || execution.WorkDir.Valid || logical.ProviderSessionID.Valid || logical.WorkDir.Valid {
			t.Fatalf("failed pin left partial identity: task=%+v logical=%+v", execution, logical)
		}
	}
	assertEmpty()
	f.svc.TxStarter = originalStarter
	for _, fields := range [][2]string{{"durable-provider-thread", ""}, {"", "/tmp/durable-session-directory"}} {
		if err := f.svc.PinIssueAgentSession(ctx, task.ID, fields[0], fields[1]); !errors.Is(err, ErrIssueAgentSessionPinConflict) {
			t.Fatalf("incomplete first pin error=%v", err)
		}
		assertEmpty()
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := f.svc.PinIssueAgentSession(ctx, task.ID, "durable-provider-thread", "/tmp/durable-session-directory"); err != nil {
			t.Fatal(err)
		}
	}
	for _, fields := range [][2]string{{"replacement-thread", "/tmp/durable-session-directory"}, {"durable-provider-thread", "/tmp/replacement-directory"}} {
		if err := f.svc.PinIssueAgentSession(ctx, task.ID, fields[0], fields[1]); !errors.Is(err, ErrIssueAgentSessionPinConflict) {
			t.Fatalf("identity replacement accepted: %v", err)
		}
	}
	execution, err := f.svc.Queries.GetAgentTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	logical, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || execution.SessionID.String != "durable-provider-thread" || execution.WorkDir.String != "/tmp/durable-session-directory" || logical.ProviderSessionID != execution.SessionID || logical.WorkDir != execution.WorkDir {
		t.Fatalf("pin did not persist one identity: execution=%+v logical=%+v err=%v", execution, logical, err)
	}
}

func TestIssueAgentSessionLateCancelledPinRepairsOriginalConversationForExplicitRetry(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := t.Context()
	issue, session := newAssignedSessionIssue(t, f, "delayed provider identity after cancel")
	task := claimSessionExecution(t, f, session, true)
	if _, err := f.svc.CancelTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	blocked, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if allowed, err := f.svc.CanRetryIssueAgentSession(ctx, blocked); err != nil || allowed {
		t.Fatalf("unknown cancelled thread was retryable: allowed=%v err=%v", allowed, err)
	}
	if err := f.svc.PinIssueAgentSession(ctx, task.ID, "late-original-thread", "/tmp/late-original-directory"); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.PinIssueAgentSession(ctx, task.ID, "late-original-thread", "/tmp/late-original-directory"); err != nil {
		t.Fatalf("late pin receipt not idempotent: %v", err)
	}
	repaired, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || repaired.ProviderSessionID.String != "late-original-thread" || repaired.WorkDir.String != "/tmp/late-original-directory" || repaired.ActiveTaskID.Valid {
		t.Fatalf("late cancellation pin missed logical session: session=%+v err=%v", repaired, err)
	}
	if allowed, err := f.svc.CanRetryIssueAgentSession(ctx, repaired); err != nil || !allowed {
		t.Fatalf("recorded cancelled conversation cannot be explicitly retried: allowed=%v err=%v", allowed, err)
	}
	f.fx.Comment(t, util.UUIDToString(issue.ID), "explicitly continue after cancellation")
	head, err := f.svc.Queries.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.svc.RetryIssueAgentEvent(ctx, session.ID, head.ID); err != nil {
		t.Fatal(err)
	}
	resumed, err := f.svc.ClaimTask(ctx, f.agentID)
	if err != nil || resumed == nil || IssueAgentSessionID(*resumed) != session.ID || resumed.RuntimeID != f.runtimeID {
		t.Fatalf("late pin resumed a different logical conversation: task=%+v err=%v", resumed, err)
	}
	final, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || final.ProviderSessionID != repaired.ProviderSessionID || final.WorkDir != repaired.WorkDir {
		t.Fatalf("retry replaced cancelled provider identity: session=%+v err=%v", final, err)
	}
}

func TestIssueAgentSessionLatePinCannotReplaceAnotherActiveExecution(t *testing.T) {
	f := newIssueSessionFixture(t)
	ctx := t.Context()
	issue, session := newAssignedSessionIssue(t, f, "stale delayed pin")
	task := claimSessionExecution(t, f, session, true)
	if _, err := f.svc.CancelTask(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	newer := f.fx.Task(t, util.UUIDToString(f.agentID), testutil.Cols{
		"runtime_id": util.UUIDToString(f.runtimeID), "issue_id": util.UUIDToString(issue.ID),
		"context": testutil.Raw("jsonb_build_object('type','issue_session','issue_agent_session_id','" + util.UUIDToString(session.ID) + "')"),
	})
	f.fx.Exec(t, "UPDATE issue_agent_session SET active_task_id=$2 WHERE id=$1", session.ID, newer)
	if err := f.svc.PinIssueAgentSession(ctx, task.ID, "stale-thread", "/tmp/stale-directory"); !errors.Is(err, ErrIssueAgentSessionPinConflict) {
		t.Fatalf("stale pin replaced newer execution: %v", err)
	}
	logical, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
	if err != nil || logical.ActiveTaskID != util.MustParseUUID(newer) || logical.ProviderSessionID.Valid || logical.WorkDir.Valid {
		t.Fatalf("rejected stale pin changed logical identity: session=%+v err=%v", logical, err)
	}
}

func TestIssueAgentSessionTerminalReportCannotReplacePinnedIdentity(t *testing.T) {
	for _, terminal := range []string{"completed", "failed"} {
		t.Run(terminal, func(t *testing.T) {
			f := newIssueSessionFixture(t)
			ctx := t.Context()
			issue, session := newAssignedSessionIssue(t, f, "terminal provider identity conflict")
			task := claimSessionExecution(t, f, session, true)
			if err := f.svc.PinIssueAgentSession(ctx, task.ID, "original-thread", "/tmp/original-directory"); err != nil {
				t.Fatal(err)
			}
			if terminal == "completed" {
				if _, err := f.svc.CompleteTask(ctx, task.ID, []byte(`{"output":""}`), "unexpected-thread", "/tmp/original-directory", "", false, "", ""); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := f.svc.FailTask(ctx, task.ID, "provider returned a different directory", "original-thread", "/tmp/unexpected-directory", "", "agent_error.unknown", false, "", ""); err != nil {
					t.Fatal(err)
				}
			}
			saved, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
			if err != nil || saved.ProviderSessionID.String != "original-thread" || saved.WorkDir.String != "/tmp/original-directory" || saved.State != "blocked" || saved.FailureReason.String != "resume_unavailable" {
				t.Fatalf("terminal report replaced pinned identity: session=%+v err=%v", saved, err)
			}
			f.fx.Comment(t, util.UUIDToString(issue.ID), "explicitly revalidate the original thread")
			head, err := f.svc.Queries.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.svc.RetryIssueAgentEvent(ctx, session.ID, head.ID); err != nil {
				t.Fatal(err)
			}
			retry, err := f.svc.Queries.GetIssueAgentSession(ctx, session.ID)
			if err != nil || !retry.ActiveTaskID.Valid || retry.ProviderSessionID != saved.ProviderSessionID || retry.WorkDir != saved.WorkDir {
				t.Fatalf("conflict retry substituted the reported identity: session=%+v err=%v", retry, err)
			}
		})
	}
}
