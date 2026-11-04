package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon/execenv"
	"github.com/multica-ai/multica/server/pkg/agent"
)

func TestIssueSessionDeliversInitialThenLiveEventsInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var mu sync.Mutex
	var order []string
	record := func(s string) { mu.Lock(); order = append(order, s); mu.Unlock() }
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/daemon/tasks/task/issue-events/initial/ack":
			record("initial ack")
		case "/api/daemon/tasks/task/issue-events/claim":
			record("claim")
			_ = json.NewEncoder(w).Encode(map[string]any{"event": IssueSessionEvent{ID: "live", SessionID: "session", Sequence: 2, Kind: "issue.updated", ActorType: "member", Payload: json.RawMessage(`{"title":"new title"}`)}})
		case "/api/daemon/tasks/task/issue-events/live/ack":
			record("live ack")
			cancel()
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
		}
	})
	session := &agent.Session{SupplementReady: func() bool { return true }, Supplement: func(_ context.Context, input string) error {
		record("inject")
		if !strings.Contains(input, "new title") || !strings.Contains(input, "EVENT 2") {
			t.Errorf("event framing lost: %s", input)
		}
		return nil
	}}
	err := d.runIssueSessionLoop(ctx, session, "task", issueSessionTransport{Initial: []IssueSessionEvent{{ID: "initial", Sequence: 1}}}, nil, d.logger)
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ",") != "initial ack,claim,inject,live ack" {
		t.Fatalf("delivery order = %v", order)
	}
}

func TestIssueSessionStopsAfterRejectedDelivery(t *testing.T) {
	var claims atomic.Int32
	var receipt struct {
		Delivered bool   `json:"delivered"`
		Reason    string `json:"failure_reason"`
	}
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/claim") {
			claims.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"event": IssueSessionEvent{ID: "event", Sequence: 1}})
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&receipt); err != nil {
			t.Error(err)
		}
	})
	session := &agent.Session{SupplementReady: func() bool { return true }, Supplement: func(context.Context, string) error { return errors.New("provider refused") }}
	if err := d.runIssueSessionLoop(t.Context(), session, "task", issueSessionTransport{}, nil, d.logger); err == nil {
		t.Fatal("delivery rejection was hidden")
	}
	if claims.Load() != 1 || receipt.Delivered || receipt.Reason != "provider_rejected" {
		t.Fatalf("claims=%d receipt=%+v", claims.Load(), receipt)
	}
}

func TestIssueSessionDoesNotResendAfterUncertainAcknowledgement(t *testing.T) {
	var claims, injections, receipts atomic.Int32
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/claim") {
			claims.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"event": IssueSessionEvent{ID: "event", Sequence: 1}})
			return
		}
		receipts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	session := &agent.Session{SupplementReady: func() bool { return true }, Supplement: func(context.Context, string) error {
		injections.Add(1)
		return nil
	}}
	err := d.runIssueSessionLoop(t.Context(), session, "task", issueSessionTransport{}, nil, d.logger)
	if err == nil || !strings.Contains(err.Error(), "acknowledgement uncertain") {
		t.Fatalf("receipt failure was hidden: %v", err)
	}
	if claims.Load() != 1 || injections.Load() != 1 || receipts.Load() != 3 {
		t.Fatalf("uncertain input was replayed: claims=%d injections=%d receipts=%d", claims.Load(), injections.Load(), receipts.Load())
	}
}

func TestIssueSessionWaitsForProviderBeforeInitialAck(t *testing.T) {
	var requests atomic.Int32
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	session := &agent.Session{SupplementReady: func() bool { return false }, Supplement: func(context.Context, string) error { t.Fatal("injected before provider ready"); return nil }}
	if err := d.runIssueSessionLoop(ctx, session, "task", issueSessionTransport{Initial: []IssueSessionEvent{{ID: "first"}}}, nil, d.logger); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatalf("HTTP requests before provider ready=%d", requests.Load())
	}
}

func TestIssueSessionAcknowledgesFastCompletedInitialTurn(t *testing.T) {
	for _, status := range []string{"completed", "failed"} {
		t.Run(status, func(t *testing.T) {
			var receipts atomic.Int32
			d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/session") {
					return
				}
				if r.URL.Path != "/api/daemon/tasks/task/issue-events/initial/ack" {
					t.Errorf("unexpected endpoint %s", r.URL.Path)
				}
				receipts.Add(1)
			})
			messages := make(chan agent.Message)
			close(messages)
			results := make(chan agent.Result, 1)
			results <- agent.Result{Status: status, SessionID: "persisted"}
			close(results)
			backend := persistingIssueSessionBackend{session: &agent.Session{
				Messages: messages, Result: results,
				SupplementReady: func() bool { return false },
				Supplement:      func(context.Context, string) error { t.Fatal("injected after turn finished"); return nil },
			}}
			transport := issueSessionTransport{Initial: []IssueSessionEvent{{ID: "initial"}}, AckedInitial: &atomic.Int32{}}
			ctx := context.WithValue(t.Context(), issueSessionTransportKey{}, transport)
			if _, _, err := d.executeAndDrain(ctx, backend, "original", agent.ExecOptions{EnableTaskSupplement: true}, d.logger, "task", "", &atomic.Int32{}); err != nil {
				t.Fatal(err)
			}
			want := int32(0)
			if status == "completed" {
				want = 1
			}
			if receipts.Load() != want || transport.AckedInitial.Load() != want {
				t.Fatalf("initial delivery receipts=%d acked=%d want=%d", receipts.Load(), transport.AckedInitial.Load(), want)
			}
		})
	}
}

func TestIssueSessionLeavesPendingEventsForIdleResume(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	var claims atomic.Int32
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/ack") {
			ready.Store(false)
		} else {
			claims.Add(1)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	session := &agent.Session{SupplementReady: ready.Load, Supplement: func(context.Context, string) error { t.Fatal("injected into completed turn"); return nil }}
	if err := d.runIssueSessionLoop(ctx, session, "task", issueSessionTransport{Initial: []IssueSessionEvent{{ID: "initial"}}}, nil, d.logger); err != nil {
		t.Fatal(err)
	}
	if claims.Load() != 0 {
		t.Fatal("claimed a pending event after provider turn ended")
	}
}

func TestIssueSessionPromptPreservesEventKindsAndAuthority(t *testing.T) {
	prompt := issueSessionPrompt(Task{IssueAgentSessionID: "session", IssueID: "issue", AgentID: "agent", IssueSessionEvents: []IssueSessionEvent{{ID: "deleted", Sequence: 9, Kind: "comment.deleted", ActorType: "agent", Payload: json.RawMessage(`{"comment_id":"old"}`)}}}, "continue")
	for _, want := range []string{"persistent issue session session", "comment.deleted", "EVENT 9", "does not carry a human's authority", "retained in the run transcript"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt missing %q: %s", want, prompt)
		}
	}
}

func TestIssueSessionWorktreeReuseRequiresExactOwnership(t *testing.T) {
	root := t.TempDir()
	root, _ = filepath.EvalSymlinks(root)
	envRoot := filepath.Join(root, "workspace", "task")
	workDir := filepath.Join(envRoot, "worktree", "subdir")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatal(err)
	}
	task := Task{WorkspaceID: "workspace", IssueID: "issue", AgentID: "agent", IssueAgentSessionID: "session", PriorWorkDir: workDir}
	if err := execenv.WriteManagedEnvProvenance(envRoot, execenv.ManagedEnvProvenance{WorkspaceID: task.WorkspaceID, IssueID: task.IssueID, AgentID: task.AgentID, IssueAgentSessionID: task.IssueAgentSessionID, WorkDir: workDir}); err != nil {
		t.Fatal(err)
	}
	if path, ok := shouldReusePriorWorkdir(task, &localDirectoryAssignment{}, root); !ok || path != workDir {
		t.Fatalf("owned nested worktree reuse=%q,%v", path, ok)
	}
	task.IssueAgentSessionID = "different"
	if _, ok := shouldReusePriorWorkdir(task, nil, root); ok {
		t.Fatal("adopted another session's worktree")
	}
	stats := &gcStats{}
	if got := new(Daemon).applyGCAction(t.Context(), envRoot, gcActionClean, stats); got != 0 || stats.skipped != 1 {
		t.Fatalf("persistent workdir was offered to GC: removed=%d skipped=%d", got, stats.skipped)
	}
}

func TestIssueSessionHistoryStoreIsNotPruned(t *testing.T) {
	store := t.TempDir()
	if err := pinIssueSessionStore(store, "session"); err != nil {
		t.Fatal(err)
	}
	if _, ok := new(Daemon).reserveStoreForDeletion(store); ok {
		t.Fatal("persistent issue history offered for deletion")
	}
}

func TestIssueSessionsDoNotSerializeOnProjectDirectory(t *testing.T) {
	const daemonID = "daemon"
	ref, err := json.Marshal(localDirectoryRef{LocalPath: t.TempDir(), DaemonID: daemonID, ExecutionMode: localDirectoryModeInPlace})
	if err != nil {
		t.Fatal(err)
	}
	d := &Daemon{cfg: Config{DaemonID: daemonID}, localPathLocks: NewLocalPathLocker(), logger: slog.Default()}
	for _, id := range []string{"issue-a", "issue-b"} {
		task := Task{ID: id, IssueID: id, AgentID: "same-agent", IssueAgentSessionID: id + "-session", ProjectResources: []ProjectResourceData{{ResourceType: localDirectoryResourceType, ResourceRef: ref}}}
		assignment, err := localDirectoryAssignmentForTask(task, daemonID)
		if err != nil || assignment == nil || !assignment.UsesWorktree() {
			t.Fatalf("session %s did not get an isolated worktree: assignment=%+v err=%v", id, assignment, err)
		}
		if release, aborted := d.acquireLocalDirectoryLockIfNeeded(t.Context(), task, d.logger); release != nil || aborted {
			t.Fatalf("session %s queued on the shared project directory", id)
		}
	}
}
