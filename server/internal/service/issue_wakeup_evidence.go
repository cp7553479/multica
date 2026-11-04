package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Retain the existing 40KB prompt budget, including the <=12KB instruction.
// Original receipts remain addressable during the retention window.
const wakeupNoteLimit = 40000
const wakeupOmittedEvidence = "Some trigger details were omitted to keep this prompt bounded. Read current issue comments, runs and state before deciding what to do.\n"

// Facts stay structured in the immutable session event; the note is only a rendering.
type wakeupEvidence struct {
	Version     int          `json:"version"`
	Instruction string       `json:"instruction,omitempty"`
	Facts       []wakeupFact `json:"facts,omitempty"`
	Omitted     bool         `json:"omitted,omitempty"`
}
type wakeupFact struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

func buildWakeupEvidence(w db.IssueWakeup, receipts []db.IssueWakeupReceipt) (string, json.RawMessage) {
	evidence := wakeupEvidence{Version: 1}

	header := wakeupNoteHeader(w)
	budget := wakeupNoteLimit - len(header) - len(wakeupOmittedEvidence)
	budget = max(budget, 0)
	total := 0
	for _, fact := range evidence.Facts {
		total += len(fact.EventType) + len(fact.Payload) + 2
	}
	trim := func() {
		for total > budget && len(evidence.Facts) > 0 {
			fact := evidence.Facts[0]
			total -= len(fact.EventType) + len(fact.Payload) + 2
			evidence.Facts = evidence.Facts[1:]
			evidence.Omitted = true
		}
	}
	trim()
	for _, r := range receipts {
		var summary struct {
			Count int64 `json:"coalesced_count"`
		}
		_ = json.Unmarshal(r.Payload, &summary)
		if summary.Count > 1 {
			evidence.Omitted = true
		}
		payload := canonicalWakeupPayload(r.Payload)
		if len(r.EventType)+len(payload)+2 > budget {
			// Keep references as data even when a changed-field list is oversized.
			var fields map[string]json.RawMessage
			_ = json.Unmarshal(payload, &fields)
			refs := map[string]json.RawMessage{}
			for _, key := range []string{"event_id", "occurred_at", "first_occurred_at", "coalesced_count", "task_id", "source_task_id", "comment_id", "thread_id", "attachment_id", "agent_id", "actor_type", "actor_id"} {
				if value := fields[key]; len(value) > 0 && len(value) <= 256 {
					refs[key] = value
				}
			}
			refs["receipt_id"], _ = json.Marshal(util.UUIDToString(r.ID))
			payload, _ = json.Marshal(refs)
			evidence.Omitted = true
		}
		evidence.Facts = append(evidence.Facts, wakeupFact{EventType: r.EventType, Payload: payload})
		total += len(r.EventType) + len(payload) + 2
		trim()
	}
	evidence.Instruction = w.Instruction
	raw, _ := json.Marshal(evidence)
	return renderWakeupEvidence(w, evidence), raw
}

func renderWakeupEvidence(w db.IssueWakeup, evidence wakeupEvidence) string {
	header := wakeupNoteHeader(w)
	if evidence.Omitted {
		header += wakeupOmittedEvidence
	}
	var b strings.Builder
	b.WriteString(header)
	for _, fact := range evidence.Facts {
		fmt.Fprintf(&b, "%s %s\n", fact.EventType, fact.Payload)
	}
	return b.String()
}

func canonicalWakeupPayload(raw json.RawMessage) json.RawMessage {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&value) != nil {
		return json.RawMessage(`{}`)
	}
	result, _ := json.Marshal(value)
	return result
}

// wakeupNoteHeader opens the [WAKEUP] block. A scheduled check may end with a
// check-in instead of a comment when nothing needs a reply; say so, with the
// exact command, only for those rules.
func wakeupNoteHeader(w db.IssueWakeup) string {
	header := "Wakeup " + util.UUIDToString(w.ID) + " triggered. Instruction:\n" + w.Instruction + "\n"
	if w.Kind == "every" || w.Kind == "cron" {
		header += "This is a scheduled check. If it finds nothing that needs a reply, end with `multica issue wakeup checkin " + util.UUIDToString(w.IssueID) + " " + util.UUIDToString(w.ID) + " --note \"<what you checked and found>\"` instead of posting a comment; the note is shown on the rule. Post a comment when something changed, needs attention, or the check is done.\n"
	}
	return header + "Trigger facts (read current state before deciding what to do):\n"
}
