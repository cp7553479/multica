package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRPCProviderPersistsSessionBeforeFirstPrompt(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-script fixture is POSIX-only")
	}
	for _, provider := range []string{"codex", "kiro", "grok"} {
		for _, reject := range []bool{false, true} {
			t.Run(provider+map[bool]string{false: "/provider-crash", true: "/pin-rejected"}[reject], func(t *testing.T) {
				dir := t.TempDir()
				requests := filepath.Join(dir, "requests.jsonl")
				fakePath := filepath.Join(dir, provider)
				writeTestExecutable(t, fakePath, []byte(`#!/bin/sh
if [ "$1" = "--version" ]; then echo test-cli; exit 0; fi
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
    *'"method":"thread/start"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"thread":{"id":"created-session"}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"sessionId":"created-session"}}\n' "$id"
      ;;
    *'"method":"turn/start"'*|*'"method":"session/prompt"'*)
      exit 1
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
				pinCalled := make(chan string, 1)
				releasePin := make(chan struct{})
				var calls atomic.Int32
				session, err := backend.Execute(ctx, "initial input", ExecOptions{StrictSessionResume: true, PersistSession: func(_ context.Context, id string) error {
					calls.Add(1)
					pinCalled <- id
					select {
					case <-releasePin:
					case <-ctx.Done():
						return ctx.Err()
					}
					if reject {
						return errors.New("storage rejected pin")
					}
					return nil
				}})
				if err != nil {
					t.Fatal(err)
				}
				select {
				case id := <-pinCalled:
					if id != "created-session" {
						t.Fatalf("pin identity=%s", id)
					}
				case <-ctx.Done():
					t.Fatal("provider did not pin its newly established session")
				}
				wire, err := os.ReadFile(requests)
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(wire), `"method":"turn/start"`) || strings.Contains(string(wire), `"method":"session/prompt"`) {
					t.Fatalf("provider received input before durable pin: %s", wire)
				}
				close(releasePin)
				for range session.Messages {
				}
				result := <-session.Result
				if calls.Load() != 1 || result.Status == "completed" || result.SessionID != "created-session" {
					t.Fatalf("first-turn early termination lost its pinned identity: calls=%d result=%+v", calls.Load(), result)
				}
				wire, err = os.ReadFile(requests)
				if err != nil {
					t.Fatal(err)
				}
				promptSent := strings.Contains(string(wire), `"method":"turn/start"`) || strings.Contains(string(wire), `"method":"session/prompt"`)
				if reject && (promptSent || !strings.Contains(result.Error, "persistence failed")) {
					t.Fatalf("failed pin allowed a model turn: result=%+v wire=%s", result, wire)
				}
				if !reject && !promptSent {
					t.Fatalf("pinned provider never reached the simulated crash boundary: %s", wire)
				}
			})
		}
	}
}
