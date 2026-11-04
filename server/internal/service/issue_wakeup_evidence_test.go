package service

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestWakeupEvidenceKeepsStructuredReceiptFacts(t *testing.T) {
	w := db.IssueWakeup{Kind: "event", Instruction: "Inspect current state"}
	receipts := []db.IssueWakeupReceipt{
		{EventType: "comment.created", Payload: json.RawMessage(`{"comment_id":"first","coalesced_count":1}`)},
		{EventType: "comment.created", Payload: json.RawMessage(`{"comment_id":"second","coalesced_count":1}`)},
	}
	note, raw := buildWakeupEvidence(w, receipts)
	var evidence wakeupEvidence
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.Version != 1 || len(evidence.Facts) != 2 || strings.Count(note, "Instruction:") != 1 {
		t.Fatalf("receipts were not preserved as structured facts: %s", raw)
	}
	for i, want := range []string{"first", "second"} {
		if evidence.Facts[i].EventType != "comment.created" || !strings.Contains(string(evidence.Facts[i].Payload), want) || !strings.Contains(note, want) {
			t.Fatalf("receipt %d missing from evidence or rendering: %s", i, raw)
		}
	}
}

func TestWakeupEvidenceJSONBFormattingKeepsNumberPrecision(t *testing.T) {
	w := db.IssueWakeup{Kind: "event", Instruction: "inspect"}
	receipt := db.IssueWakeupReceipt{EventType: "comment.created", Payload: json.RawMessage(`{"revision":9007199254740993, "comment_id":"first"}`)}
	note, raw := buildWakeupEvidence(w, []db.IssueWakeupReceipt{receipt})
	var evidence wakeupEvidence
	if err := json.Unmarshal(raw, &evidence); err != nil {
		t.Fatal(err)
	}
	if len(evidence.Facts) != 1 || !strings.Contains(string(raw), "9007199254740993") || !strings.Contains(note, "9007199254740993") {
		t.Fatalf("JSONB formatting lost structured data or number precision: %s", raw)
	}
}

func TestIssueWakeupStructuredEvidenceSurvivesDatabaseRoundTrip(t *testing.T) {
	f, s, issue, agent := wakeFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "read state"})
	first := f.Comment(t, util.UUIDToString(issue), "first")
	wakeDispatch(t, s, w)
	second := f.Comment(t, util.UUIDToString(issue), "second")
	wakeDispatch(t, s, w)
	task := wakeupSessionTask(t, f, issue, agent)
	inputs := wakeupInbox(t, f, w.ID)
	if len(inputs) != 2 || inputs[0].SessionID != inputs[1].SessionID || inputs[0].Sequence >= inputs[1].Sequence {
		t.Fatalf("independent rule inputs lost order or session identity: %+v", inputs)
	}
	for i, want := range []string{first, second} {
		var payload wakeupSessionPayload
		var evidence wakeupEvidence
		if err := json.Unmarshal(inputs[i].Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(payload.Evidence, &evidence); err != nil {
			t.Fatal(err)
		}
		if len(evidence.Facts) != 1 || evidence.Instruction != w.Instruction || !strings.Contains(payload.Note, want) || !strings.Contains(string(evidence.Facts[0].Payload), want) {
			t.Fatalf("input %d lost its structured evidence: %s", i, inputs[i].Payload)
		}
	}
	claimed := wakeupClaimEvents(t, f, s, task, 32)
	if len(claimed) != 2 || claimed[0].ID != inputs[0].ID || claimed[1].ID != inputs[1].ID {
		t.Fatalf("delivery did not preserve round-tripped evidence order: %+v", claimed)
	}
	if n := f.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1", issue); n != 1 {
		t.Fatalf("receipt transfers created %d consumers", n)
	}
}

func TestWakeupEvidenceInstructionEditDoesNotReintroduceOldPrompt(t *testing.T) {
	w := db.IssueWakeup{Kind: "event", Instruction: "old instructions"}
	first, raw := buildWakeupEvidence(w, []db.IssueWakeupReceipt{{EventType: "comment.created", Payload: json.RawMessage(`{"comment_id":"first"}`)}})
	w.Instruction = "new instructions"
	note, current := buildWakeupEvidence(w, []db.IssueWakeupReceipt{{EventType: "comment.created", Payload: json.RawMessage(`{"comment_id":"second"}`)}})
	var evidence wakeupEvidence
	if err := json.Unmarshal(current, &evidence); err != nil {
		t.Fatal(err)
	}
	if len(evidence.Facts) != 1 || evidence.Instruction != w.Instruction || strings.Contains(note, "old instructions") || strings.Contains(note, "first") || !strings.Contains(note, "new instructions") {
		t.Fatalf("new input revived a prior prompt: %s", note)
	}
	if !strings.Contains(first, "old instructions") || !strings.Contains(string(raw), "first") {
		t.Fatal("rendering the next input mutated the previous immutable evidence")
	}
}
