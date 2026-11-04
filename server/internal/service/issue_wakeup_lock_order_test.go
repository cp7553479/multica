package service

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/util"
)

type wakeupSessionLockProbe struct {
	TxStarter
	beforeSessionLock func()
}

func (s wakeupSessionLockProbe) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &wakeupProbeTx{Tx: tx, beforeSessionLock: s.beforeSessionLock}, nil
}

type wakeupProbeTx struct {
	pgx.Tx
	beforeSessionLock func()
	checked           bool
}

func (tx *wakeupProbeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if !tx.checked && strings.Contains(sql, "-- name: EnsureIssueAgentSession") {
		tx.checked = true
		tx.beforeSessionLock()
	}
	return tx.Tx.QueryRow(ctx, sql, args...)
}

func TestIssueWakeupSessionLockWaitDoesNotHoldReceipt(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "check"})
	f.Comment(t, util.UUIDToString(issue), "first")
	wakeDispatch(t, s, w)
	f.Comment(t, util.UUIDToString(issue), "pending")
	checked := false
	s.Tasks.TxStarter = wakeupSessionLockProbe{TxStarter: f.Pool, beforeSessionLock: func() {
		checked = true
		// Dispatch must reach the session lock before taking a receipt lock.
		// A concurrent capture of the same coalesced receipt must still commit.
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := f.Pool.Exec(ctx, `INSERT INTO comment(issue_id,workspace_id,author_type,author_id,content,type) VALUES($1,$2,'member',$3,'concurrent','comment')`, issue, f.WorkspaceID, f.UserID)
		if err != nil {
			t.Errorf("comment capture blocked behind session-lock wait: %v", err)
		}
	}}
	wakeDispatch(t, s, w)
	if !checked {
		t.Fatal("session-lock boundary not exercised")
	}
}
