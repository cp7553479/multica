package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/attribution"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"strings"
)

const wakeupOutcomeAcknowledged = "acknowledged"

// receiptActor reads who produced an event receipt. Coalesced receipts keep
// only the latest actor, so they never count as the agent's own.
func receiptActor(r db.IssueWakeupReceipt) (actorType, actorID string, single bool) {
	var p struct {
		ActorType string `json:"actor_type"`
		ActorID   string `json:"actor_id"`
		AgentID   string `json:"agent_id"`
		Count     int64  `json:"coalesced_count"`
	}
	_ = json.Unmarshal(r.Payload, &p)
	if p.ActorType == "" && p.AgentID != "" {
		p.ActorType, p.ActorID = "agent", p.AgentID
	}
	return p.ActorType, p.ActorID, p.Count <= 1
}

// acknowledgedBySelf reports whether every input of this firing came from the
// target agent itself (see the rules above).
func acknowledgedBySelf(ctx context.Context, q *db.Queries, w db.IssueWakeup, agentID, issueID pgtype.UUID, receipts []db.IssueWakeupReceipt) (bool, error) {
	if len(receipts) == 0 {
		return false, nil
	}
	agent := util.UUIDToString(agentID)
	ownRule := w.SystemRule.Valid
	if !ownRule && w.SourceTaskID.Valid {
		source, err := q.GetAgentTask(ctx, w.SourceTaskID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
		ownRule = err == nil && source.AgentID == agentID
	}
	seen := map[pgtype.UUID]bool{}
	var causes []pgtype.UUID
	for _, r := range receipts {
		// Even an explicit actor filter must not feed a rule the writes made by
		// the execution already processing that same rule. An external run of
		// the named agent remains a legitimate trigger. Coalesced sources cannot
		// prove that every observation came from this consumer.
		var provenance struct {
			SourceTaskID string `json:"source_task_id"`
			TaskID       string `json:"task_id"`
			Count        int64  `json:"coalesced_count"`
		}
		_ = json.Unmarshal(r.Payload, &provenance)
		ref := provenance.SourceTaskID
		if ref == "" && strings.HasPrefix(r.EventType, "task.") {
			ref = provenance.TaskID
		}
		if taskID, parseErr := util.ParseUUID(ref); parseErr == nil && provenance.Count <= 1 {
			known, err := q.HasReceivedIssueWakeupInput(ctx, db.HasReceivedIssueWakeupInputParams{
				TaskID: taskID, WakeupID: util.UUIDToString(w.ID), IssueID: issueID, AgentID: agentID,
			})
			if err != nil {
				return false, err
			}
			if known {
				continue
			}
		}
		switch {
		case r.EventType == wakeupConditionEventType:
			if !ownRule {
				return false, nil
			}
			var p struct {
				SourceTaskIDs []string `json:"source_task_ids"`
			}
			_ = json.Unmarshal(r.Payload, &p)
			if len(p.SourceTaskIDs) == 0 {
				return false, nil
			}
			for _, raw := range p.SourceTaskIDs {
				id, err := util.ParseUUID(raw)
				if err != nil {
					return false, nil
				}
				if !seen[id] {
					seen[id] = true
					causes = append(causes, id)
				}
			}
		case strings.HasPrefix(r.EventType, "task."), r.EventType == "time.due", r.EventType == wakeupManualEventType, r.EventType == wakeupTimeoutEventType:
			// A run ending, a time, a person's "wake now" and a deadline are
			// never the agent's own action.
			return false, nil
		default:
			// A rule that names the agent itself as the actor asked for this.
			if w.FilterActorType.String == "agent" && w.FilterActorID == agentID {
				return false, nil
			}
			actorType, actorID, single := receiptActor(r)
			if !single || actorType != "agent" || actorID != agent {
				return false, nil
			}
		}
	}
	if len(causes) == 0 {
		return true, nil
	}
	active, err := q.CountActiveIssueRunsOfAgent(ctx, db.CountActiveIssueRunsOfAgentParams{Ids: causes, AgentID: agentID, IssueID: issueID})
	if err != nil {
		return false, err
	}
	return int(active) == len(causes), nil
}

// childDoneRunAs is who a child_done run for this agent runs as: whoever
// caused the parent to exist, as a run the parent's own assignment would
// start.
func (s *IssueWakeupService) childDoneRunAs(ctx context.Context, issue db.Issue, agent db.Agent) (attribution.Result, error) {
	attr := s.Tasks.attributionForIssueTask(ctx, issue, pgtype.UUID{}, attribution.SourceDelegation, pgtype.UUID{})
	return s.Tasks.applyAttributionFallback(ctx, attr, agent)
}

func receiptIDs(receipts []db.IssueWakeupReceipt) []pgtype.UUID {
	ids := make([]pgtype.UUID, 0, len(receipts))
	for _, r := range receipts {
		ids = append(ids, r.ID)
	}
	return ids
}
