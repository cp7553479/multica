package agent

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamSessionPersistsBeforeOutputAndSupplement(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	for _, provider := range []string{"claude", "cursor"} {
		for _, reject := range []bool{false, true} {
			name := provider + "/accepted"
			if reject {
				name = provider + "/rejected"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				fakePath := filepath.Join(dir, provider)
				script := `#!/bin/sh
cat >/dev/null
printf '%s\n' '{"type":"system","subtype":"init","session_id":"durable-thread"}'
printf '%s\n' '{"type":"tool_call","subtype":"started","call_id":"tool-1","session_id":"durable-thread","tool_call":{"readToolCall":{"args":{"path":"a.txt"}}}}'
`
				if provider == "claude" {
					script = `#!/bin/sh
IFS= read -r init
printf '%s\n' '{"type":"control_response","response":{"subtype":"success","request_id":"multica-supplement-initialize","response":{}}}'
IFS= read -r prompt
printf '%s\n' '{"type":"control_request","request_id":"first-hook","request":{"subtype":"hook_callback","callback_id":"multica-supplement-UserPromptSubmit","input":{"hook_event_name":"UserPromptSubmit","session_id":"durable-thread"}}}'
IFS= read -r reply
printf '%s\n' '{"type":"system","subtype":"init","session_id":"durable-thread"}'
printf '%s\n' '{"type":"assistant","session_id":"durable-thread","message":{"content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{"file_path":"a.txt"}}]}}'
`
				}
				script += `while [ ! -f release ]; do sleep 0.01; done
printf '%s\n' '{"type":"result","subtype":"success","session_id":"durable-thread","result":"done"}'
`
				writeTestExecutable(t, fakePath, []byte(script))
				backend, err := New(provider, Config{ExecutablePath: fakePath})
				if err != nil {
					t.Fatal(err)
				}
				entered := make(chan string, 1)
				allow := make(chan struct{})
				var calls atomic.Int32
				ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
				defer cancel()
				session, err := backend.Execute(ctx, "original task", ExecOptions{
					Cwd: dir, EnableTaskSupplement: true, StrictSessionResume: true,
					CursorHookCommand: testCursorHookCommand, HandshakeTimeout: time.Second,
					PersistSession: func(ctx context.Context, sid string) error {
						calls.Add(1)
						entered <- sid
						select {
						case <-allow:
						case <-ctx.Done():
							return ctx.Err()
						}
						if reject {
							return errors.New("pin unavailable")
						}
						return nil
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				select {
				case sid := <-entered:
					if sid != "durable-thread" {
						t.Fatalf("early identity = %q", sid)
					}
				case <-ctx.Done():
					t.Fatal("provider did not establish its session before completion")
				}
				if session.SupplementReady() {
					t.Fatal("supplement opened before durable session confirmation")
				}
				if err := session.Supplement(ctx, "too early"); err == nil {
					t.Fatal("accepted additional input before durable confirmation")
				}
				select {
				case msg := <-session.Messages:
					t.Fatalf("processed output before durable confirmation: %+v", msg)
				default:
				}
				select {
				case result := <-session.Result:
					t.Fatalf("completed before durable confirmation: %+v", result)
				default:
				}
				close(allow)
				if !reject {
					deadline := time.After(3 * time.Second)
					for !session.SupplementReady() {
						select {
						case <-deadline:
							t.Fatal("supplement stayed closed after durable confirmation")
						case <-time.After(time.Millisecond):
						}
					}
					if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				statusSeen, tools := false, 0
				for msg := range session.Messages {
					if msg.Type == MessageStatus && msg.SessionID == "durable-thread" {
						statusSeen = true
					}
					if msg.Type == MessageToolUse {
						tools++
					}
				}
				result := <-session.Result
				if result.SessionID != "durable-thread" {
					t.Fatalf("lost established identity: %+v", result)
				}
				if reject {
					if result.Status != "failed" || !strings.Contains(result.Error, "pin unavailable") || tools != 0 {
						t.Fatalf("persistence rejection continued execution: tools=%d result=%+v", tools, result)
					}
				} else if result.Status != "completed" || !statusSeen || tools != 1 {
					t.Fatalf("confirmed session: statusSeen=%v tools=%d result=%+v", statusSeen, tools, result)
				}
				if n := calls.Load(); n != 1 {
					t.Fatalf("durable callback calls = %d, want one", n)
				}
			})
		}
	}
}

func TestCursorHookPersistsBeforeStreamInit(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "accepted"
		if reject {
			name = "rejected"
		}
		t.Run(name, func(t *testing.T) { testCursorHookPersistsBeforeStreamInit(t, reject) })
	}
}

func testCursorHookPersistsBeforeStreamInit(t *testing.T, reject bool) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	dir := t.TempDir()
	fakePath := filepath.Join(dir, "cursor")
	writeTestExecutable(t, fakePath, []byte(`#!/bin/sh
cat >/dev/null
printf '%s\n%s\n' "$MULTICA_CURSOR_HOOK_URL" "$MULTICA_CURSOR_HOOK_TOKEN" > hook-env
while [ ! -f init-release ]; do sleep 0.01; done
printf '%s\n' '{"type":"system","subtype":"init","session_id":"hook-thread"}'
while [ ! -f release ]; do sleep 0.01; done
printf '%s\n' '{"type":"result","subtype":"success","session_id":"hook-thread","result":"done"}'
`))
	backend, err := New("cursor", Config{ExecutablePath: fakePath})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	entered, allow := make(chan string, 1), make(chan struct{})
	var calls atomic.Int32
	session, err := backend.Execute(ctx, "original task", ExecOptions{
		Cwd: dir, EnableTaskSupplement: true, CursorHookCommand: testCursorHookCommand,
		PersistSession: func(ctx context.Context, sid string) error {
			calls.Add(1)
			entered <- sid
			select {
			case <-allow:
				if reject {
					return errors.New("hook pin unavailable")
				}
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var env []string
	for len(env) != 2 {
		data, _ := os.ReadFile(filepath.Join(dir, "hook-env"))
		env = strings.Fields(string(data))
		select {
		case <-ctx.Done():
			t.Fatal("fake did not receive hook environment")
		case <-time.After(time.Millisecond):
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, env[0], strings.NewReader(`{"hook_event_name":"postToolUse","conversation_id":"hook-thread","tool_use_id":"tool-1"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+env[1])
	type hookResult struct {
		status int
		err    error
	}
	hookDone := make(chan hookResult, 1)
	go func() {
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			hookDone <- hookResult{err: err}
			return
		}
		defer res.Body.Close()
		hookDone <- hookResult{status: res.StatusCode}
	}()
	select {
	case sid := <-entered:
		if sid != "hook-thread" {
			t.Fatalf("hook identity=%q", sid)
		}
	case <-ctx.Done():
		t.Fatal("hook did not persist early identity")
	}
	if session.SupplementReady() {
		t.Fatal("hook activated before pin")
	}
	if err := os.WriteFile(filepath.Join(dir, "init-release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case reply := <-hookDone:
		t.Fatalf("hook released before pin: %+v", reply)
	default:
	}
	close(allow)
	select {
	case reply := <-hookDone:
		expectedStatus := http.StatusOK
		if reject {
			expectedStatus = http.StatusServiceUnavailable
		}
		// Cancelling the provider also closes the hook server's active request.
		if (!reject && reply.err != nil) || (reply.err == nil && reply.status != expectedStatus) {
			t.Fatalf("confirmed hook: %+v", reply)
		}
	case <-ctx.Done():
		t.Fatal("confirmed hook did not release")
	}
	if reject {
		for range session.Messages {
		}
		if result := <-session.Result; result.Status != "failed" || result.SessionID != "hook-thread" || !strings.Contains(result.Error, "hook pin unavailable") || calls.Load() != 1 {
			t.Fatalf("hook persistence failure: calls=%d result=%+v", calls.Load(), result)
		}
		return
	}
	if !session.SupplementReady() {
		t.Fatal("confirmed hook did not activate supplement")
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	statusSeen := false
	for msg := range session.Messages {
		if msg.Type == MessageStatus && msg.SessionID == "hook-thread" {
			statusSeen = true
		}
	}
	if result := <-session.Result; result.Status != "completed" || result.SessionID != "hook-thread" || !statusSeen || calls.Load() != 1 {
		t.Fatalf("hook and stream identity: statusSeen=%v calls=%d result=%+v", statusSeen, calls.Load(), result)
	}
}

func TestStreamSessionRejectsExecutionBeforeIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	for _, provider := range []string{"claude", "cursor"} {
		t.Run(provider, func(t *testing.T) {
			dir := t.TempDir()
			fakePath := filepath.Join(dir, provider)
			script := "#!/bin/sh\ncat >/dev/null\nprintf '%s\\n' '{\"type\":\"tool_call\",\"subtype\":\"started\",\"call_id\":\"tool-1\"}'\n"
			if provider == "claude" {
				script = `#!/bin/sh
IFS= read -r init
printf '%s\n' '{"type":"control_response","response":{"subtype":"success","request_id":"multica-supplement-initialize","response":{}}}'
IFS= read -r prompt
printf '%s\n' '{"type":"control_request","request_id":"first-hook","request":{"subtype":"hook_callback","callback_id":"multica-supplement-UserPromptSubmit","input":{"hook_event_name":"UserPromptSubmit"}}}'
IFS= read -r reply
`
			}
			writeTestExecutable(t, fakePath, []byte(script))
			backend, err := New(provider, Config{ExecutablePath: fakePath})
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			session, err := backend.Execute(ctx, "original task", ExecOptions{Cwd: dir, EnableTaskSupplement: true, CursorHookCommand: testCursorHookCommand, HandshakeTimeout: time.Second, PersistSession: func(context.Context, string) error { calls.Add(1); return nil }})
			if err != nil {
				t.Fatal(err)
			}
			for msg := range session.Messages {
				if msg.Type == MessageToolUse {
					t.Fatal("processed tool before provider identity")
				}
			}
			result := <-session.Result
			if result.Status != "failed" || !strings.Contains(result.Error, "session identity unavailable") || result.SessionID != "" || calls.Load() != 0 {
				t.Fatalf("unidentified execution: calls=%d result=%+v", calls.Load(), result)
			}
		})
	}
}
