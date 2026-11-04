package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/multica-ai/multica/server/pkg/agent"
)

type persistingIssueSessionBackend struct {
	session *agent.Session
	before  func(context.Context, agent.ExecOptions) error
}

func (b persistingIssueSessionBackend) Execute(ctx context.Context, _ string, opts agent.ExecOptions) (*agent.Session, error) {
	if opts.PersistSession == nil {
		return nil, errors.New("durable provider callback missing")
	}
	if err := opts.PersistSession(ctx, "persisted"); err != nil {
		return nil, err
	}
	if b.before != nil {
		if err := b.before(ctx, opts); err != nil {
			return nil, err
		}
	}
	return b.session, nil
}

func completedPersistentFakeSession() *agent.Session {
	messages := make(chan agent.Message)
	close(messages)
	results := make(chan agent.Result, 1)
	results <- agent.Result{Status: "completed", SessionID: "persisted"}
	close(results)
	return &agent.Session{Messages: messages, Result: results, SupplementReady: func() bool { return false }, Supplement: func(context.Context, string) error { return nil }}
}

func TestIssueSessionPinsIdentityAndWorkdirBeforeAcknowledgingInput(t *testing.T) {
	var pins, acks atomic.Int32
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/session") {
			var body struct{ SessionID, WorkDir string }
			var raw map[string]string
			if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
				t.Error(err)
			}
			body.SessionID, body.WorkDir = raw["session_id"], raw["work_dir"]
			if body.SessionID != "persisted" || body.WorkDir != "/persistent/worktree" {
				t.Errorf("early pin lost session storage: %+v", raw)
			}
			if pins.Add(1) < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
			return
		}
		if pins.Load() != 3 || !strings.HasSuffix(r.URL.Path, "/initial/ack") {
			t.Errorf("input reached server before durable session receipt: pins=%d path=%s", pins.Load(), r.URL.Path)
		}
		acks.Add(1)
	})
	ctx := context.WithValue(t.Context(), issueSessionTransportKey{}, issueSessionTransport{Initial: []IssueSessionEvent{{ID: "initial"}}, AckedInitial: &atomic.Int32{}})
	result, _, err := d.executeAndDrain(ctx, persistingIssueSessionBackend{session: completedPersistentFakeSession()}, "first input", agent.ExecOptions{Cwd: "/persistent/worktree", EnableTaskSupplement: true}, d.logger, "task", "", &atomic.Int32{})
	if err != nil || result.SessionID != "persisted" || pins.Load() != 3 || acks.Load() != 1 {
		t.Fatalf("durable receipt result=%+v err=%v pins=%d acks=%d", result, err, pins.Load(), acks.Load())
	}
}

func TestIssueSessionPinFailureStopsBeforeInputAcknowledgement(t *testing.T) {
	var pins, otherRequests atomic.Int32
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/session") {
			pins.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		otherRequests.Add(1)
	})
	var inputStarted atomic.Bool
	backend := persistingIssueSessionBackend{session: completedPersistentFakeSession(), before: func(context.Context, agent.ExecOptions) error { inputStarted.Store(true); return nil }}
	ctx := context.WithValue(t.Context(), issueSessionTransportKey{}, issueSessionTransport{Initial: []IssueSessionEvent{{ID: "initial"}}, AckedInitial: &atomic.Int32{}})
	result, _, err := d.executeAndDrain(ctx, backend, "first input", agent.ExecOptions{Cwd: "/persistent/worktree", EnableTaskSupplement: true}, d.logger, "task", "", &atomic.Int32{})
	if err == nil || !strings.Contains(err.Error(), "could not be durably saved") || result.SessionID != "persisted" || pins.Load() != 3 || inputStarted.Load() || otherRequests.Load() != 0 {
		t.Fatalf("failed pin continued input: result=%+v err=%v pins=%d started=%v other=%d", result, err, pins.Load(), inputStarted.Load(), otherRequests.Load())
	}
}

func TestIssueSessionFirstProviderIdentitySurvivesAbruptExit(t *testing.T) {
	var persisted atomic.Value
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/session") {
			t.Errorf("unexpected request before crash: %s", r.URL.Path)
		}
		var raw map[string]string
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Error(err)
		}
		persisted.Store(raw["session_id"])
	})
	backend := persistingIssueSessionBackend{before: func(context.Context, agent.ExecOptions) error {
		if persisted.Load() != "persisted" {
			t.Fatal("provider reached crash boundary before its identity was saved")
		}
		return errors.New("fake provider killed before first result")
	}}
	ctx := context.WithValue(t.Context(), issueSessionTransportKey{}, issueSessionTransport{})
	result, _, err := d.executeAndDrain(ctx, backend, "first input", agent.ExecOptions{Cwd: "/persistent/worktree", EnableTaskSupplement: true}, d.logger, "task", "", &atomic.Int32{})
	if err == nil || result.SessionID != persisted.Load() || persisted.Load() == nil {
		t.Fatalf("first result absent lost durable identity: result=%+v err=%v persisted=%v", result, err, persisted.Load())
	}
}

func TestIssueSessionCompletedWithoutEarlyPinRemainsUnconfirmed(t *testing.T) {
	var requests atomic.Int32
	d := taskSupplementTestDaemon(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) })
	ctx := context.WithValue(t.Context(), issueSessionTransportKey{}, issueSessionTransport{Initial: []IssueSessionEvent{{ID: "initial"}}, AckedInitial: &atomic.Int32{}})
	_, _, err := d.executeAndDrain(ctx, sessionBackend{session: completedPersistentFakeSession()}, "first input", agent.ExecOptions{EnableTaskSupplement: true}, d.logger, "task", "", &atomic.Int32{})
	if err == nil || !strings.Contains(err.Error(), "without durably saving") || requests.Load() != 0 {
		t.Fatalf("completed turn without pin was acknowledged: err=%v requests=%d", err, requests.Load())
	}
}
