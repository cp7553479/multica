package agent

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestACPPersistentSessionRejectsReplacementBeforePrompt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	for _, provider := range []string{"grok", "kiro"} {
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			requests := filepath.Join(dir, "requests.jsonl")
			fakePath := filepath.Join(dir, provider)
			writeTestExecutable(t, fakePath, []byte(`#!/bin/sh
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$REQUESTS_FILE"
  id=$(printf '%s' "$line" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":1,"authMethods":[{"id":"cached_token","name":"Cached login"}],"agentCapabilities":{"loadSession":true}}}\n' "$id"
      ;;
    *'"method":"authenticate"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id"
      ;;
    *'"method":"session/load"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"replacement"}}\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"stopReason":"end_turn"}}\n' "$id"
      exit 0
      ;;
  esac
done
`))
			backend, err := New(provider, Config{ExecutablePath: fakePath, Env: map[string]string{"REQUESTS_FILE": requests}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			session, err := backend.Execute(ctx, "continue", ExecOptions{ResumeSessionID: "persisted", StrictSessionResume: true})
			if err != nil {
				t.Fatal(err)
			}
			for range session.Messages {
			}
			result := <-session.Result
			if result.Status != "failed" || result.SessionID != "persisted" || !strings.Contains(result.Error, "identity changed") {
				t.Fatalf("replacement result: %+v", result)
			}
			wire, err := os.ReadFile(requests)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(wire), `"method":"session/load"`) || strings.Contains(string(wire), `"method":"session/prompt"`) || strings.Contains(string(wire), `"method":"session/new"`) {
				t.Fatalf("replacement received new input: %s", wire)
			}
		})
	}
}

func TestPiPersistentSessionRejectsMissingHistoryBeforeLaunch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	dir := t.TempDir()
	fakePath := filepath.Join(dir, "pi")
	launched := filepath.Join(dir, "launched")
	writeTestExecutable(t, fakePath, []byte("#!/bin/sh\nprintf launched > \"$LAUNCHED_FILE\"\n"))
	backend, err := New("pi", Config{ExecutablePath: fakePath, Env: map[string]string{"LAUNCHED_FILE": launched}})
	if err != nil {
		t.Fatal(err)
	}
	history := filepath.Join(dir, "missing.jsonl")
	session, err := backend.Execute(t.Context(), "continue", ExecOptions{ResumeSessionID: history, StrictSessionResume: true})
	if err == nil || session != nil || !strings.Contains(err.Error(), "history is unavailable") {
		t.Fatalf("missing history: session=%v err=%v", session, err)
	}
	for _, path := range []string{history, launched} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("strict resume created %s: %v", path, err)
		}
	}
}

func TestCursorPersistentSessionRequiresHooksBeforeLaunch(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	dir := t.TempDir()
	fakePath := filepath.Join(dir, "cursor-agent")
	launched := filepath.Join(dir, "launched")
	writeTestExecutable(t, fakePath, []byte("#!/bin/sh\nprintf launched > \"$LAUNCHED_FILE\"\n"))
	backend, err := New("cursor", Config{ExecutablePath: fakePath, Env: map[string]string{"LAUNCHED_FILE": launched}})
	if err != nil {
		t.Fatal(err)
	}
	session, err := backend.Execute(t.Context(), "continue", ExecOptions{Cwd: dir, ResumeSessionID: "persisted", StrictSessionResume: true, EnableTaskSupplement: true})
	if err == nil || session != nil || !strings.Contains(err.Error(), "live delivery unavailable") {
		t.Fatalf("missing hooks: session=%v err=%v", session, err)
	}
	if _, err := os.Stat(launched); !os.IsNotExist(err) {
		t.Fatalf("provider launched without hooks: %v", err)
	}
}

func TestClaudePersistentSessionRequiresHandshakeBeforePrompt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	dir := t.TempDir()
	requests := filepath.Join(dir, "requests.jsonl")
	fakePath := filepath.Join(dir, "claude")
	writeTestExecutable(t, fakePath, []byte(`#!/bin/sh
while IFS= read -r line; do
  printf '%s\n' "$line" >> "$REQUESTS_FILE"
  case "$line" in
    *'"subtype":"initialize"'*)
      printf '{"type":"control_response","response":{"subtype":"error","request_id":"multica-supplement-initialize","error":"unsupported"}}\n'
      ;;
  esac
done
`))
	backend, err := New("claude", Config{ExecutablePath: fakePath, Env: map[string]string{"REQUESTS_FILE": requests}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "continue", ExecOptions{StrictSessionResume: true, EnableTaskSupplement: true, HandshakeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for range session.Messages {
	}
	result := <-session.Result
	if result.Status == "completed" || !strings.Contains(result.Error, "live delivery unavailable") {
		t.Fatalf("handshake rejection result: %+v", result)
	}
	wire, err := os.ReadFile(requests)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wire), `"subtype":"initialize"`) || strings.Contains(string(wire), `"type":"user"`) {
		t.Fatalf("initial input sent without safe delivery handshake: %s", wire)
	}
}

func TestStreamPersistentSessionStopsOnReplacementIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	for _, provider := range []string{"claude", "cursor"} {
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			fakePath := filepath.Join(t.TempDir(), provider)
			readInput := "cat > /dev/null\n"
			if provider == "claude" {
				readInput = "IFS= read -r _\n"
			}
			writeTestExecutable(t, fakePath, []byte("#!/bin/sh\n"+readInput+`
printf '%s\n' '{"type":"system","subtype":"init","session_id":"replacement"}'
printf '%s\n' '{"type":"assistant","session_id":"replacement","message":{"content":[{"type":"tool_use","id":"wrong-session-tool","name":"Bash","input":{"command":"echo wrong"}}]}}'
printf '%s\n' '{"type":"result","subtype":"success","is_error":false,"result":"done","session_id":"replacement"}'
`))
			backend, err := New(provider, Config{ExecutablePath: fakePath, Env: map[string]string{"IS_SANDBOX": "1"}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			session, err := backend.Execute(ctx, "continue", ExecOptions{ResumeSessionID: "persisted", StrictSessionResume: true})
			if err != nil {
				t.Fatal(err)
			}
			for message := range session.Messages {
				if message.Type == MessageToolUse {
					t.Fatal("processed tool activity from a replacement session")
				}
			}
			result := <-session.Result
			if result.Status != "failed" || result.SessionID != "persisted" || !strings.Contains(result.Error, "identity changed") {
				t.Fatalf("replacement identity was accepted: %+v", result)
			}
		})
	}
}
