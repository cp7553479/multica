package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

type crashRecoveryFakeBackend struct {
	url, mode string
}

func (b crashRecoveryFakeBackend) Execute(ctx context.Context, _ string, opts agent.ExecOptions) (*agent.Session, error) {
	method := "thread/start"
	if opts.ResumeSessionID != "" {
		method = "thread/resume"
	}
	data, _ := json.Marshal(map[string]string{"session_id": opts.ResumeSessionID, "work_dir": opts.Cwd})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.url+"/fake-provider/"+method, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	var body struct {
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil || response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fake provider restore rejected: status=%d error=%v", response.StatusCode, err)
	}
	if b.mode == "first" {
		if err := os.WriteFile(filepath.Join(opts.Cwd, "unfinished.txt"), []byte("first turn work"), 0o600); err != nil {
			return nil, err
		}
	} else if data, err := os.ReadFile(filepath.Join(opts.Cwd, "unfinished.txt")); err != nil || string(data) != "first turn work" {
		return nil, fmt.Errorf("restore lost original workdir: content=%q error=%v", data, err)
	}
	if err := opts.PersistSession(ctx, body.SessionID); err != nil {
		return nil, err
	}
	messages := make(chan agent.Message)
	results := make(chan agent.Result, 1)
	ready := b.mode == "first"
	if !ready {
		close(messages)
		results <- agent.Result{Status: "completed", SessionID: body.SessionID}
		close(results)
	}
	return &agent.Session{Messages: messages, Result: results, SupplementReady: func() bool { return ready }, Supplement: func(context.Context, string) error { return nil }}, nil
}

// This helper is a fake daemon execution process. The parent test owns durable
// server/provider records and kills this process after its initial receipt.
func TestIssueSessionCrashProcessHelper(t *testing.T) {
	mode := os.Getenv("MULTICA_FAKE_DAEMON_SESSION_MODE")
	if mode == "" {
		t.Skip("subprocess fixture")
	}
	url := os.Getenv("MULTICA_FAKE_DAEMON_SESSION_URL")
	d := &Daemon{client: NewClient(url), logger: slog.New(slog.NewTextHandler(io.Discard, nil)), taskSupplementReadyInterval: time.Millisecond, taskSupplementPollInterval: 10 * time.Millisecond}
	eventID := "initial"
	if mode == "resume" {
		eventID = "followup"
	}
	ctx := context.WithValue(t.Context(), issueSessionTransportKey{}, issueSessionTransport{Initial: []IssueSessionEvent{{ID: eventID}}, AckedInitial: &atomic.Int32{}})
	_, _, err := d.executeAndDrain(ctx, crashRecoveryFakeBackend{url: url, mode: mode}, "input", agent.ExecOptions{
		EnableTaskSupplement: true, StrictSessionResume: true,
		ResumeSessionID: os.Getenv("MULTICA_FAKE_DAEMON_PROVIDER_ID"), Cwd: os.Getenv("MULTICA_FAKE_DAEMON_WORKDIR"),
	}, d.logger, mode, "", &atomic.Int32{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestIssueSessionDaemonSIGKILLRestoresDurablyPinnedIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL subprocess fixture is Unix-only")
	}
	const providerID = "first-provider-thread"
	workdir := t.TempDir()
	var mu sync.Mutex
	var durableID, durableDir string
	starts, resumes := 0, 0
	firstAck := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/fake-provider/thread/start":
			starts++
			_ = json.NewEncoder(w).Encode(map[string]string{"session_id": providerID})
		case "/fake-provider/thread/resume":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["session_id"] != durableID || body["work_dir"] != durableDir || durableID != providerID {
				w.WriteHeader(http.StatusConflict)
				return
			}
			resumes++
			_ = json.NewEncoder(w).Encode(map[string]string{"session_id": durableID})
		case "/api/daemon/tasks/first/session", "/api/daemon/tasks/resume/session":
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["session_id"] != providerID || body["work_dir"] != workdir {
				t.Errorf("incomplete durable pin: %v", body)
			}
			durableID, durableDir = body["session_id"], body["work_dir"]
		case "/api/daemon/tasks/first/issue-events/initial/ack":
			if durableID == "" || durableDir == "" {
				t.Error("initial acknowledgement preceded durable pin")
			}
			select {
			case firstAck <- struct{}{}:
			default:
			}
		case "/api/daemon/tasks/resume/issue-events/followup/ack":
		case "/api/daemon/tasks/first/issue-events/claim", "/api/daemon/tasks/resume/issue-events/claim":
			_, _ = io.WriteString(w, `{"event":null}`)
		default:
			t.Errorf("unexpected request %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := func(ctx context.Context, mode, id, dir string) *exec.Cmd {
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestIssueSessionCrashProcessHelper$", "-test.count=1")
		cmd.Env = append(os.Environ(), "MULTICA_FAKE_DAEMON_SESSION_MODE="+mode, "MULTICA_FAKE_DAEMON_SESSION_URL="+server.URL, "MULTICA_FAKE_DAEMON_PROVIDER_ID="+id, "MULTICA_FAKE_DAEMON_WORKDIR="+dir)
		return cmd
	}
	firstCtx, cancelFirst := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelFirst()
	first := child(firstCtx, "first", "", workdir)
	var output bytes.Buffer
	first.Stdout, first.Stderr = &output, &output
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Process.Kill() })
	select {
	case <-firstAck:
	case <-firstCtx.Done():
		_ = first.Process.Kill()
		_ = first.Wait()
		t.Fatalf("fake daemon never acknowledged initial input: %s", output.String())
	}
	if err := first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err == nil {
		t.Fatal("first daemon exited normally instead of being hard-killed")
	}
	mu.Lock()
	id, dir := durableID, durableDir
	mu.Unlock()
	resumeCtx, cancelResume := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelResume()
	resumedOutput, err := child(resumeCtx, "resume", id, dir).CombinedOutput()
	if err != nil {
		t.Fatalf("new daemon could not restore saved session: %v\n%s", err, resumedOutput)
	}
	mu.Lock()
	defer mu.Unlock()
	if starts != 1 || resumes != 1 || durableID != providerID || durableDir != workdir || strings.Contains(string(resumedOutput), "FAIL") {
		t.Fatalf("hard crash opened a replacement: starts=%d resumes=%d session=%s dir=%s", starts, resumes, durableID, durableDir)
	}
}
