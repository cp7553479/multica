package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/attribution"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// create_issue retains its existing run linkage and attribution, while its
// assignment is input to the issue's logical session rather than a second task.
type autopilotIssueSessionInput struct {
	RunID       pgtype.UUID        `json:"run_id"`
	Attribution attribution.Result `json:"attribution"`
}

func decodeAutopilotIssueSessionInput(event db.IssueAgentEvent) (*autopilotIssueSessionInput, bool) {
	var body struct {
		Dispatch *autopilotIssueSessionInput `json:"autopilot_dispatch"`
	}
	if json.Unmarshal(event.Payload, &body) != nil || body.Dispatch == nil {
		return nil, false
	}
	return body.Dispatch, true
}

// bindAutopilotIssueSessionInput runs before the issue/run transaction commits,
// so runtime polling cannot claim an assignment without its dispatch identity.
func (s *AutopilotService) bindAutopilotIssueSessionInput(ctx context.Context, q *db.Queries, issue db.Issue, run db.AutopilotRun, actor pgtype.UUID) (db.IssueAgentSession, attribution.Result, error) {
	var attr attribution.Result
	if issue.CreatorType != "agent" || issue.AssigneeID != issue.CreatorID || issue.AssigneeType.String != "agent" || issue.OriginType.String != "autopilot" || issue.OriginID != run.AutopilotID || run.IssueID != issue.ID {
		return db.IssueAgentSession{}, attr, errors.New("autopilot issue session binding does not match its run")
	}
	session, err := q.GetIssueAgentSessionForPair(ctx, db.GetIssueAgentSessionForPairParams{WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, AgentID: issue.AssigneeID})
	if err != nil {
		return session, attr, err
	}
	initial, err := q.GetAutopilotIssueAgentAssignment(ctx, session.ID)
	if err != nil {
		return session, attr, fmt.Errorf("load autopilot assignment: %w", err)
	}
	if prior, ok := decodeAutopilotIssueSessionInput(initial); ok {
		if prior.RunID != run.ID {
			return session, attr, errors.New("autopilot assignment already belongs to another run")
		}
		return session, prior.Attribution, nil
	}
	if actor.Valid {
		attr = attribution.DirectHumanRun(actor, attribution.EvidenceIssueAssignment, issue.ID)
	} else {
		attr = triggerOwnerAttribution(ctx, q, run.TriggerID, issue.WorkspaceID, run.AutopilotID, attribution.EvidenceIssueAssignment, issue.ID)
	}
	payload, err := json.Marshal(autopilotIssueSessionInput{RunID: run.ID, Attribution: attr})
	if err != nil {
		return session, attr, err
	}
	if _, err = q.BindAutopilotIssueAgentAssignment(ctx, db.BindAutopilotIssueAgentAssignmentParams{SessionID: session.ID, OriginatorUserID: attr.UserID, Dispatch: payload}); err != nil {
		return session, attr, err
	}
	return session, attr, nil
}

// The captured principal is rechecked at Wake and Claim. A publisher or owner
// fallback is audit evidence, never substitute authority for this input.
func autopilotIssueSessionInputAllowed(ctx context.Context, q *db.Queries, issue db.Issue, agent db.Agent, event db.IssueAgentEvent, input *autopilotIssueSessionInput) bool {
	if event.Kind != "assigned" || event.Sequence != 1 || !input.RunID.Valid || issue.OriginType.String != "autopilot" || !input.Attribution.UserID.Valid || input.Attribution.UserID != event.OriginatorUserID {
		return false
	}
	run, err := q.GetAutopilotRun(ctx, input.RunID)
	if err != nil || run.IssueID != issue.ID || run.AutopilotID != issue.OriginID {
		return false
	}
	if _, err := q.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{UserID: input.Attribution.UserID, WorkspaceID: issue.WorkspaceID}); err != nil {
		return false
	}
	return CanMemberInvokeAgent(ctx, q, agent, input.Attribution.UserID, issue.WorkspaceID)
}

func (s *AutopilotService) wakeAutopilotIssueSession(ctx context.Context, issue db.Issue, run *db.AutopilotRun) error {
	session, err := s.Queries.GetIssueAgentSessionForPair(ctx, db.GetIssueAgentSessionForPairParams{WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, AgentID: issue.AssigneeID})
	if err != nil {
		return err
	}
	if _, err = s.TaskSvc.WakeIssueAgentSession(ctx, session.ID); err != nil {
		return err
	}
	// A consumer may have been created by runtime polling before this wake.
	// Repair only the consumer carrying this initial input, never later comments.
	initial, err := s.Queries.GetAutopilotIssueAgentAssignment(ctx, session.ID)
	if err != nil {
		return err
	}
	if input, ok := decodeAutopilotIssueSessionInput(initial); ok && input.RunID == run.ID {
		taskID := initial.DeliveryTaskID
		if !taskID.Valid && initial.Status == "pending" {
			current, err := s.Queries.GetIssueAgentSession(ctx, session.ID)
			if err != nil {
				return err
			}
			if current.ActiveTaskID.Valid {
				task, err := s.Queries.GetAgentTask(ctx, current.ActiveTaskID)
				if err != nil {
					return err
				}
				if IssueAgentSessionID(task) == session.ID && task.OriginatorUserID == input.Attribution.UserID {
					taskID = task.ID
				}
			}
		}
		if taskID.Valid {
			if _, err := s.Queries.SetAutopilotRunDispatchTask(ctx, db.SetAutopilotRunDispatchTaskParams{ID: run.ID, TaskID: taskID}); err != nil {
				return err
			}
		}
	}
	updated, err := s.Queries.GetAutopilotRun(ctx, run.ID)
	if err != nil {
		return err
	}
	*run = updated
	return nil
}
