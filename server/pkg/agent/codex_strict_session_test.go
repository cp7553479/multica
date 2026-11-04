package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestCodexPersistentSessionNeverStartsReplacementThread(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response rpcResponse
	}{
		{name: "rejected", response: rpcResponse{method: "thread/resume", errMsg: "unknown thread", errCode: -32602}},
		{name: "different thread id", response: rpcResponse{method: "thread/resume", result: json.RawMessage(`{"thread":{"id":"replacement"}}`)}},
		{name: "missing thread id", response: rpcResponse{method: "thread/resume", result: json.RawMessage(`{}`)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, fs, _ := newTestCodexClient(t)
			wait := drainRPCScript(t, c, fs, []rpcResponse{tc.response})
			defer wait()
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			thread, resumed, err := c.startOrResumeThread(ctx, ExecOptions{Cwd: "/work", ResumeSessionID: "persisted", StrictSessionResume: true}, slog.Default())
			if err == nil || thread != "" || resumed {
				t.Fatalf("strict resume=%q,%v,%v", thread, resumed, err)
			}
			lines := fs.Lines()
			if len(lines) != 1 || !strings.Contains(lines[0], "thread/resume") {
				t.Fatalf("strict resume sent replacement RPC: %v", lines)
			}
		})
	}
}
