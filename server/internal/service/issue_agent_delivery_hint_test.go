package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type issueAgentDeliveryHintObservation struct {
	event          events.Event
	status         string
	failure        pgtype.Text
	deliveredAt    pgtype.Timestamptz
	sessionState   string
	sessionFailure pgtype.Text
	err            error
}

type issueAgentDeliveryHintRecorder struct {
	observations []issueAgentDeliveryHintObservation
	session      db.IssueAgentSession
}

func watchIssueAgentDeliveryHints(t *testing.T, f *claimDecisionFixture, session db.IssueAgentSession, eventID pgtype.UUID) *issueAgentDeliveryHintRecorder {
	t.Helper()
	// Hold this connection before the operation starts, so it cannot be the
	// connection that owns the ACK/retry transaction. The callback must see
	// committed rows without depending on the transaction's own reads.
	observer, err := f.pool.Acquire(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(observer.Release)
	recorder := &issueAgentDeliveryHintRecorder{session: session}
	f.svc.Bus.Subscribe(protocol.EventIssueAgentDeliveryUpdated, func(event events.Event) {
		observation := issueAgentDeliveryHintObservation{event: event}
		observation.err = observer.QueryRow(t.Context(), `
			SELECT e.status, e.failure_reason, e.delivered_at, s.state, s.failure_reason
			FROM issue_agent_event e JOIN issue_agent_session s ON s.id = e.session_id
			WHERE e.id = $1`, eventID).Scan(
			&observation.status, &observation.failure, &observation.deliveredAt,
			&observation.sessionState, &observation.sessionFailure,
		)
		recorder.observations = append(recorder.observations, observation)
	})
	return recorder
}

func (r *issueAgentDeliveryHintRecorder) assertObservation(t *testing.T, index int, status, failure, sessionState string) {
	t.Helper()
	if len(r.observations) <= index {
		t.Fatalf("delivery hints = %d, want observation %d", len(r.observations), index)
	}
	got := r.observations[index]
	if got.err != nil {
		t.Fatalf("read committed receipt in delivery callback: %v", got.err)
	}
	if got.status != status || got.failure.String != failure || got.sessionState != sessionState {
		t.Errorf("callback saw receipt status=%q failure=%q session=%q, want %q/%q/%q", got.status, got.failure.String, got.sessionState, status, failure, sessionState)
	}
	if (status == "delivered") != got.deliveredAt.Valid {
		t.Errorf("callback delivered_at valid = %v for status %q", got.deliveredAt.Valid, status)
	}
	if got.event.Type != "issue:agent_delivery_updated" || got.event.WorkspaceID != util.UUIDToString(r.session.WorkspaceID) {
		t.Errorf("delivery hint scope/type = %q/%q, want session workspace and issue delivery type", got.event.WorkspaceID, got.event.Type)
	}
	if got.event.TaskID != "" || got.event.ChatSessionID != "" || got.event.ChatSessionOwnerID != "" || got.event.MemberPrivateUserID != "" || got.event.ActorID != "" {
		t.Errorf("delivery hint leaked execution or recipient scope: %+v", got.event)
	}
	raw, err := json.Marshal(got.event.Payload)
	if err != nil {
		t.Fatalf("marshal delivery hint: %v", err)
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode delivery hint payload: %v", err)
	}
	var issueID string
	if err := json.Unmarshal(payload["issue_id"], &issueID); err != nil {
		t.Fatalf("delivery hint missing issue ID: %s (%v)", raw, err)
	}
	if len(payload) != 1 || issueID != util.UUIDToString(r.session.IssueID) {
		t.Errorf("delivery hint payload = %s, want only the session's issue_id", raw)
	}
}

func initialIssueAgentDelivery(t *testing.T, f *claimDecisionFixture, session db.IssueAgentSession) (db.AgentTaskQueue, db.IssueAgentEvent) {
	t.Helper()
	task := claimSessionExecution(t, f, session, false)
	batch, err := f.svc.Queries.ListDeliveringIssueAgentEventsForTask(t.Context(), db.ListDeliveringIssueAgentEventsForTaskParams{SessionID: session.ID, TaskID: task.ID})
	if err != nil || len(batch) != 1 {
		t.Fatalf("initial delivery batch = %+v, error = %v; want one assignment event", batch, err)
	}
	return task, batch[0]
}

func TestIssueAgentDeliveryHintSuccessfulAckIsCommittedAndIdempotent(t *testing.T) {
	f := newIssueSessionFixture(t)
	_, session := newAssignedSessionIssue(t, f, "successful delivery notification")
	task, event := initialIssueAgentDelivery(t, f, session)
	hints := watchIssueAgentDeliveryHints(t, f, session, event.ID)

	for attempt := 0; attempt < 2; attempt++ {
		if err := f.svc.AckIssueAgentEvent(t.Context(), task.ID, event.ID, true, ""); err != nil {
			t.Fatalf("ACK attempt %d: %v", attempt, err)
		}
		if len(hints.observations) != 1 {
			t.Fatalf("successful ACK attempt %d published %d hints, want exactly one overall", attempt, len(hints.observations))
		}
	}
	hints.assertObservation(t, 0, "delivered", "", "ready")
}

func TestIssueAgentDeliveryHintFailedAckPublishesOnlyStateChanges(t *testing.T) {
	f := newIssueSessionFixture(t)
	_, session := newAssignedSessionIssue(t, f, "failed delivery notification")
	task, event := initialIssueAgentDelivery(t, f, session)
	hints := watchIssueAgentDeliveryHints(t, f, session, event.ID)

	for attempt, reason := range []string{"provider_rejected", "provider_rejected", "timeout"} {
		if err := f.svc.AckIssueAgentEvent(t.Context(), task.ID, event.ID, false, reason); err != nil {
			t.Fatalf("failed ACK attempt %d: %v", attempt, err)
		}
		want := 1
		if attempt == 2 {
			want = 2
		}
		if len(hints.observations) != want {
			t.Fatalf("failed ACK attempt %d published %d hints, want %d", attempt, len(hints.observations), want)
		}
	}
	hints.assertObservation(t, 0, "failed", "provider_rejected", "blocked")
	hints.assertObservation(t, 1, "failed", "timeout", "blocked")
	for _, observation := range hints.observations {
		if observation.sessionFailure != observation.failure {
			t.Errorf("callback saw session failure %+v, want committed event failure %+v", observation.sessionFailure, observation.failure)
		}
	}
}

func TestIssueAgentDeliveryHintAckRollbackPublishesNothing(t *testing.T) {
	for _, delivered := range []bool{true, false} {
		name := "failed_ack"
		if delivered {
			name = "successful_ack"
		}
		t.Run(name, func(t *testing.T) {
			f := newIssueSessionFixture(t)
			_, session := newAssignedSessionIssue(t, f, "rolled back delivery notification")
			task, event := initialIssueAgentDelivery(t, f, session)
			hints := watchIssueAgentDeliveryHints(t, f, session, event.ID)
			commitErr := errors.New("delivery ACK commit rejected")
			f.svc.TxStarter = &rejectIssueCommitTxStarter{pool: f.pool, err: commitErr}

			if err := f.svc.AckIssueAgentEvent(t.Context(), task.ID, event.ID, delivered, "provider_rejected"); !errors.Is(err, commitErr) {
				t.Fatalf("ACK commit error = %v, want injected error", err)
			}
			if len(hints.observations) != 0 {
				t.Fatalf("rolled-back ACK published %d hints", len(hints.observations))
			}
			saved, err := f.svc.Queries.GetIssueAgentEvent(t.Context(), event.ID)
			if err != nil {
				t.Fatal(err)
			}
			logical, err := f.svc.Queries.GetIssueAgentSession(t.Context(), session.ID)
			if err != nil {
				t.Fatal(err)
			}
			if saved.Status != event.Status || saved.FailureReason != event.FailureReason || saved.DeliveredAt != event.DeliveredAt || saved.UpdatedAt != event.UpdatedAt || logical.State != "ready" || logical.FailureReason.Valid {
				t.Fatalf("rolled-back ACK changed receipt/session: receipt=%+v session=%+v", saved, logical)
			}
		})
	}
}

func TestIssueAgentDeliveryHintRetryPublishesCommittedPendingState(t *testing.T) {
	f := newIssueSessionFixture(t)
	_, session := newAssignedSessionIssue(t, f, "retry delivery notification")
	task, event := initialIssueAgentDelivery(t, f, session)
	if err := f.svc.PinIssueAgentSession(t.Context(), task.ID, "delivery-hint-thread", "/tmp/delivery-hint-directory"); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.AckIssueAgentEvent(t.Context(), task.ID, event.ID, false, "provider_rejected"); err != nil {
		t.Fatal(err)
	}
	hints := watchIssueAgentDeliveryHints(t, f, session, event.ID)
	if err := f.svc.RetryIssueAgentEvent(context.Background(), session.ID, event.ID); err != nil {
		t.Fatalf("retry event: %v", err)
	}
	if len(hints.observations) != 1 {
		t.Fatalf("retry published %d hints, want one", len(hints.observations))
	}
	hints.assertObservation(t, 0, "pending", "", "ready")
}
