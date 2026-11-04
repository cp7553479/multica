package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestIssueWakeupLargeSingleFactPreservesReferences(t *testing.T) {
	payload, _ := json.Marshal(map[string]any{"comment_id": "source-comment", "changed_fields": strings.Repeat("字段", 20000)})
	w := db.IssueWakeup{Instruction: strings.Repeat("指", 4000)}
	note, _ := buildWakeupEvidence(w, []db.IssueWakeupReceipt{{EventType: "comment.updated", Payload: payload}})
	if len(note) > wakeupNoteLimit || !utf8.ValidString(note) || !strings.Contains(note, w.Instruction) || !strings.Contains(note, "source-comment") || !strings.Contains(note, wakeupOmittedEvidence) {
		t.Fatalf("oversized fact not safely condensed: %d bytes", len(note))
	}
}

func TestIssueWakeupRegistrationRunDoesNotTriggerItself(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	source := f.Task(t, agent, testutil.Cols{"issue_id": issue, "status": "running", "runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agent + "')")})
	w, err := s.Create(context.Background(), issue, parseTestUUID(t, f.UserID), parseTestUUID(t, source), WakeupInput{
		AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created", "task.completed"}, Instruction: "Wait for new input",
	})
	if err != nil {
		t.Fatal(err)
	}
	f.Comment(t, util.UUIDToString(issue), "Registered", testutil.Cols{"author_type": "agent", "author_id": agent, "source_task_id": source})
	f.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", source)
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1", w.ID); n != 0 {
		t.Fatalf("registration run produced %d receipts for itself", n)
	}
	f.Comment(t, util.UUIDToString(issue), "External input")
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1", w.ID); n != 1 {
		t.Fatalf("external input lost: %d", n)
	}
	wakeDispatch(t, s, w)
}

func TestIssueWakeupOversizedPendingEvidenceStillConsumesInputs(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "Keep this instruction"})
	first := f.Comment(t, util.UUIDToString(issue), "first")
	// Real receipt payloads may contain large changed-field sets. Bound the
	// rendered event while retaining its source reference and raw receipt.
	f.Exec(t, "UPDATE issue_wakeup_receipt SET payload=payload||jsonb_build_object('changed_fields',$2::text) WHERE wakeup_id=$1 AND processed_at IS NULL", w.ID, strings.Repeat("字段", 20000))
	wakeDispatch(t, s, w)
	task := wakeupSessionTask(t, f, issue, agent)
	latest := f.Comment(t, util.UUIDToString(issue), "latest")
	wakeDispatch(t, s, w)
	inputs := wakeupInbox(t, f, w.ID)
	if len(inputs) != 2 {
		t.Fatalf("durable rule inputs = %+v, want both", inputs)
	}
	for i, want := range []string{first, latest} {
		var payload wakeupSessionPayload
		if err := json.Unmarshal(inputs[i].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if !utf8.ValidString(payload.Note) || len(payload.Note) > wakeupNoteLimit || !strings.Contains(payload.Note, want) || !strings.Contains(payload.Note, w.Instruction) || len(payload.ReceiptIDs) != 1 {
			t.Fatalf("input %d lost bounded instruction/source/evidence: %s", i, inputs[i].Payload)
		}
		if i == 0 && !strings.Contains(payload.Note, wakeupOmittedEvidence) {
			t.Fatalf("oversized input lacked current-state instruction: %s", inputs[i].Payload)
		}
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL", w.ID); n != 0 {
		t.Fatalf("%d inputs stalled", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt r JOIN issue_agent_event e ON e.source_id=r.id WHERE r.wakeup_id=$1 AND e.kind='wakeup.fired'", w.ID); n != 2 {
		t.Fatalf("lost durable receipt references: %d", n)
	}
	if claimed := wakeupClaimEvents(t, f, s, task, 32); len(claimed) != 2 {
		t.Fatalf("bounded inputs not deliverable: %+v", claimed)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue); n != 1 {
		t.Fatalf("duplicate consumers: %d", n)
	}
}

func TestIssueWakeupDispatchedSessionKeepsNewInputsUntilRunningDelivery(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "Wait for input"})
	first := f.Comment(t, util.UUIDToString(issue), "first")
	wakeDispatch(t, s, w)
	task := wakeupSessionTask(t, f, issue, agent)
	initial := wakeupClaimEvents(t, f, s, task, 32)
	if len(initial) != 1 || !strings.Contains(string(initial[0].Payload), first) {
		t.Fatalf("initial delivery = %+v", initial)
	}
	f.Exec(t, "UPDATE agent_task_queue SET dispatched_at=now()-interval '10 minutes',prepare_lease_expires_at=now()-interval '1 minute' WHERE id=$1", task.ID)
	task.Status = "dispatched"
	latest := f.Comment(t, util.UUIDToString(issue), "next")
	wakeDispatch(t, s, w)
	inputs := wakeupInbox(t, f, w.ID)
	if len(inputs) != 2 || inputs[0].Status != "delivering" || inputs[1].Status != "pending" {
		t.Fatalf("preparation wait lost claimed or pending input: %+v", inputs)
	}
	if replay := wakeupClaimEvents(t, f, s, task, 32); len(replay) != 1 || replay[0].ID != initial[0].ID || string(replay[0].Payload) != string(initial[0].Payload) {
		t.Fatalf("preparation replay changed its claimed input: %+v", replay)
	}
	current := wakeupSessionTask(t, f, issue, agent)
	if current.ID != task.ID || !current.PrepareLeaseExpiresAt.Valid || !current.PrepareLeaseExpiresAt.Time.Before(time.Now()) {
		t.Fatalf("stale preparation was hidden or replaced: %+v", current)
	}
	wakeupAckEvents(t, s, task, initial)
	f.Exec(t, "UPDATE agent_task_queue SET status='running',started_at=now() WHERE id=$1", task.ID)
	task.Status = "running"
	next := wakeupClaimEvents(t, f, s, task, 32)
	if len(next) != 1 || next[0].ID != inputs[1].ID || !strings.Contains(string(next[0].Payload), latest) {
		t.Fatalf("running safe boundary did not deliver next input: %+v", next)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue); n != 1 {
		t.Fatalf("preparation wait created %d consumers", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL", w.ID); n != 0 {
		t.Fatalf("session transfer left %d stalled receipts", n)
	}
}
