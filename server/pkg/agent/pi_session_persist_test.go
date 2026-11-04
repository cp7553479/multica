package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// This fake initializes --session just as Pi does before reading its first RPC
// prompt. It never resolves or executes an installed provider binary.
const piPersistentSessionFake = `#!/bin/sh
session=
while [ "$#" -gt 0 ]; do
  case "$1" in --session) shift; session="$1";; esac
  shift
done
printf 'launch\n' >> "$LAUNCHES"
if [ ! -s "$session" ]; then
  printf '%s\n' '{"type":"session","version":3,"id":"pi-real-session","timestamp":"2026-11-04T00:00:00Z","cwd":"/tmp"}' > "$session"
fi
IFS= read -r initial || exit 0
printf '%s\n' "$initial" >> "$INPUTS"
printf '%s\n' '{"type":"message","id":"user-input","parentId":null,"message":{"role":"user","content":[{"type":"text","text":"stored input"}]}}' >> "$session"
printf '%s\n' '{"type":"agent_start"}'
if [ "$MODE" = complete ]; then
  printf '%s\n' '{"type":"agent_end","messages":[]}' '{"type":"agent_settled","aborted":false}'
  cat >/dev/null
else
  while [ ! -f "$CRASH_NOW" ]; do sleep 0.01; done
  kill -KILL $$
fi
`

func newPiPersistentSessionFake(t *testing.T, dir, mode string) *piBackend {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	fakePath := filepath.Join(dir, "fake-pi")
	writeTestExecutable(t, fakePath, []byte(piPersistentSessionFake))
	backend, err := New("pi", Config{ExecutablePath: fakePath, Env: map[string]string{
		"LAUNCHES": filepath.Join(dir, "launches"), "INPUTS": filepath.Join(dir, "inputs"),
		"CRASH_NOW": filepath.Join(dir, "crash-now"), "MODE": mode,
	}})
	if err != nil {
		t.Fatal(err)
	}
	pi := backend.(*piBackend)
	pi.terminateGrace = 50 * time.Millisecond
	return pi
}

func TestPiPersistsSessionBeforeFirstInputAndProviderCrash(t *testing.T) {
	dir := t.TempDir()
	backend := newPiPersistentSessionFake(t, dir, "crash")
	t.Setenv("HOME", dir)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	entered := make(chan string, 1)
	release := make(chan struct{})
	persisted := filepath.Join(dir, "persisted-session")
	session, err := backend.Execute(ctx, "initial", ExecOptions{
		Cwd: dir, EnableTaskSupplement: true, HandshakeTimeout: time.Second,
		PersistSession: func(ctx context.Context, path string) error {
			if err := readPiSessionHeader(path); err != nil {
				return err
			}
			entered <- path
			select {
			case <-release:
				return os.WriteFile(persisted, []byte(path), 0o600)
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var path string
	select {
	case path = <-entered:
	case <-ctx.Done():
		t.Fatal("provider history was not pinned during startup")
	}
	if _, err := os.Stat(filepath.Join(dir, "inputs")); !os.IsNotExist(err) {
		t.Fatalf("provider received input before durable pin: %v", err)
	}
	if session.SupplementReady() {
		t.Fatal("supplements were ready before durable pin")
	}
	if err := session.Supplement(ctx, "too early"); err == nil || !strings.Contains(err.Error(), "persistence") {
		t.Fatalf("supplement bypassed pending pin: %v", err)
	}
	close(release)
	select {
	case message := <-session.Messages:
		if message.Type != MessageStatus || message.Status != "running" {
			t.Fatalf("first provider message = %+v", message)
		}
	case <-ctx.Done():
		t.Fatal("provider did not start after durable pin")
	}
	if err := os.WriteFile(filepath.Join(dir, "crash-now"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for range session.Messages {
	}
	result := waitPiResult(t, session, 5*time.Second)
	if result.Status != "failed" || result.SessionID != path {
		t.Fatalf("interrupted first turn = %+v, pinned %q", result, path)
	}
	stored, err := os.ReadFile(persisted)
	if err != nil || string(stored) != path {
		t.Fatalf("durable pointer after crash = %q, %v", stored, err)
	}
	before, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(before), "stored input") {
		t.Fatalf("first turn history unavailable after crash: %s, %v", before, err)
	}

	// Recovery uses exactly the retained transcript; no replacement file or
	// second initial session is permitted after the first provider dies.
	resumed := newPiPersistentSessionFake(t, dir, "complete")
	resume, err := resumed.Execute(ctx, "continue", ExecOptions{
		Cwd: dir, ResumeSessionID: string(stored), StrictSessionResume: true,
		EnableTaskSupplement: true,
		PersistSession: func(_ context.Context, got string) error {
			if got != path {
				return errors.New("replacement session")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range resume.Messages {
	}
	if result := waitPiResult(t, resume, 5*time.Second); result.Status != "completed" || result.SessionID != path {
		t.Fatalf("strict recovery = %+v", result)
	}
	after, err := os.ReadFile(path)
	if err != nil || !strings.HasPrefix(string(after), string(before)) {
		t.Fatalf("strict recovery replaced history: %s, %v", after, err)
	}
}

func TestPiSessionPersistenceFailureStopsBeforeModelInput(t *testing.T) {
	dir := t.TempDir()
	backend := newPiPersistentSessionFake(t, dir, "complete")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	session, err := backend.Execute(ctx, "must not reach model", ExecOptions{
		Cwd: dir, ResumeSessionID: filepath.Join(dir, "session.jsonl"), EnableTaskSupplement: true,
		PersistSession: func(context.Context, string) error {
			calls++
			return errors.New("durable pin rejected")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for message := range session.Messages {
		if message.Type == MessageToolUse {
			t.Fatalf("tool ran after rejected pin: %+v", message)
		}
	}
	result := waitPiResult(t, session, 5*time.Second)
	if result.Status != "failed" || !strings.Contains(result.Error, "session persistence failed: durable pin rejected") {
		t.Fatalf("rejected pin = %+v", result)
	}
	if calls != 1 || session.SupplementReady() {
		t.Fatalf("pin calls=%d, supplementary input ready=%v", calls, session.SupplementReady())
	}
	if _, err := os.Stat(filepath.Join(dir, "inputs")); !os.IsNotExist(err) {
		t.Fatalf("provider received model input after failed pin: %v", err)
	}
	launches, err := os.ReadFile(filepath.Join(dir, "launches"))
	if err != nil || string(launches) != "launch\n" {
		t.Fatalf("provider was relaunched after failed pin: %q, %v", launches, err)
	}
}

func TestPiEmptySessionReservationCannotBePersisted(t *testing.T) {
	dir := t.TempDir()
	backend := newPiPersistentSessionFake(t, dir, "complete")
	writeTestExecutable(t, filepath.Join(dir, "fake-pi"), []byte("#!/bin/sh\ncat > \"$INPUTS\"\n"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	calls := 0
	session, err := backend.Execute(ctx, "must not reach model", ExecOptions{
		Cwd: dir, ResumeSessionID: filepath.Join(dir, "session.jsonl"), EnableTaskSupplement: true,
		HandshakeTimeout: 50 * time.Millisecond,
		PersistSession: func(context.Context, string) error {
			calls++
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range session.Messages {
	}
	result := waitPiResult(t, session, 5*time.Second)
	if result.Status != "failed" || !strings.Contains(result.Error, "timed out waiting for session history") {
		t.Fatalf("empty reservation = %+v", result)
	}
	if calls != 0 || session.SupplementReady() || result.SessionID != "" {
		t.Fatalf("empty reservation pinned %d times; supplementary input ready=%v; reported session=%q", calls, session.SupplementReady(), result.SessionID)
	}
	input, err := os.ReadFile(filepath.Join(dir, "inputs"))
	if err != nil && !os.IsNotExist(err) || len(input) != 0 {
		t.Fatalf("model input reached provider before real session: %q, %v", input, err)
	}
}

func TestPiPersistentSessionRejectsEmptyAndInvalidHistory(t *testing.T) {
	for _, content := range []string{"", "{}\n", "{\"type\":\"session\"}\n", "not-json\n", "{\"type\":\"session\",\"id\":\"valid\"}junk\n"} {
		t.Run(content, func(t *testing.T) {
			dir := t.TempDir()
			backend := newPiPersistentSessionFake(t, dir, "complete")
			path := filepath.Join(dir, "history.jsonl")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			session, err := backend.Execute(t.Context(), "continue", ExecOptions{ResumeSessionID: path, StrictSessionResume: true})
			if err == nil || session != nil || !strings.Contains(err.Error(), "history is unavailable") {
				t.Fatalf("invalid strict history: session=%v err=%v", session, err)
			}
			if _, err := os.Stat(filepath.Join(dir, "launches")); !os.IsNotExist(err) {
				t.Fatalf("strict history validation launched provider: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil || string(got) != content {
				t.Fatalf("strict history was modified: %q, %v", got, err)
			}
		})
	}
}
