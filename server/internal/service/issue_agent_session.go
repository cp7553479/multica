package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/attribution"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const issueAgentSessionContextType = "issue_session"

var ErrIssueAgentSessionResumeBlocked = errors.New("issue session cannot resume until its original runtime and session storage are restored")

var ErrIssueAgentSessionExecutionSuperseded = errors.New("issue session execution was superseded before launch")

type IssueAgentEventInput struct {
	Kind             string
	SourceID         pgtype.UUID
	SourceRevision   int64
	ActorType        string
	ActorID          pgtype.UUID
	OriginatorUserID pgtype.UUID
	SourceTaskID     pgtype.UUID
	Payload          json.RawMessage
}

type IssueAgentEventDelivery struct {
	Session   db.IssueAgentSession
	Event     db.IssueAgentEvent
	Duplicate bool
}

func IssueAgentSessionID(task db.AgentTaskQueue) pgtype.UUID {
	var carrier struct {
		Type      string `json:"type"`
		SessionID string `json:"issue_agent_session_id"`
	}
	if json.Unmarshal(task.Context, &carrier) != nil || carrier.Type != issueAgentSessionContextType {
		return pgtype.UUID{}
	}
	id, err := util.ParseUUID(carrier.SessionID)
	if err != nil {
		return pgtype.UUID{}
	}
	return id
}

func IsIssueAgentSessionTask(task db.AgentTaskQueue) bool {
	return IssueAgentSessionID(task).Valid
}

func issueAgentEventPrincipal(event db.IssueAgentEvent) pgtype.UUID {
	if event.ActorType == "member" {
		return event.ActorID
	}
	return event.OriginatorUserID
}

func issueAgentEventKey(input IssueAgentEventInput) (string, error) {
	if !input.SourceID.Valid || input.SourceRevision < 0 {
		return "", errors.New("event source and non-negative revision are required")
	}
	switch input.Kind {
	case "assigned", "comment.created", "comment.updated", "comment.deleted", "issue.updated", "wakeup.fired":
	default:
		return "", errors.New("unsupported issue session event kind")
	}
	if input.ActorType != "member" && input.ActorType != "agent" && input.ActorType != "system" {
		return "", errors.New("unsupported issue session actor type")
	}
	return fmt.Sprintf("%s:%s:%d", input.Kind, util.UUIDToString(input.SourceID), input.SourceRevision), nil
}

// appendIssueAgentEvent shares the caller's mutation transaction. Waking the
// consumer happens only after that transaction commits.
func (s *TaskService) appendIssueAgentEvent(ctx context.Context, q *db.Queries, issue db.Issue, agentID pgtype.UUID, input IssueAgentEventInput) (IssueAgentEventDelivery, error) {
	var out IssueAgentEventDelivery
	key, err := issueAgentEventKey(input)
	if err != nil || !json.Valid(input.Payload) {
		if err == nil {
			err = errors.New("event payload must be valid JSON")
		}
		return out, err
	}
	agent, err := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: agentID, WorkspaceID: issue.WorkspaceID})
	if err != nil {
		return out, err
	}
	session, err := q.EnsureIssueAgentSession(ctx, db.EnsureIssueAgentSessionParams{
		ID: dbid.NewV7(), WorkspaceID: issue.WorkspaceID, IssueID: issue.ID, AgentID: agentID, RuntimeID: agent.RuntimeID,
	})
	if err != nil {
		return out, err
	}
	out.Session = session
	prior, err := q.GetIssueAgentEventByKey(ctx, db.GetIssueAgentEventByKeyParams{SessionID: session.ID, EventKey: key})
	if err == nil {
		out.Event, out.Duplicate = prior, true
		return out, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	sequence, err := q.NextIssueAgentEventSequence(ctx, session.ID)
	if err != nil {
		return out, err
	}
	status, reason := "pending", pgtype.Text{}
	if input.ActorType == "agent" && input.ActorID == agentID {
		status, reason = "ignored", pgtype.Text{String: "own_write", Valid: true}
	}
	originator := input.OriginatorUserID
	if input.ActorType == "member" {
		originator = input.ActorID
	}
	out.Event, err = q.CreateIssueAgentEvent(ctx, db.CreateIssueAgentEventParams{
		ID: dbid.NewV7(), WorkspaceID: issue.WorkspaceID, SessionID: session.ID, Sequence: sequence,
		EventKey: key, Kind: input.Kind, SourceID: input.SourceID, SourceRevision: input.SourceRevision,
		ActorType: input.ActorType, ActorID: input.ActorID, OriginatorUserID: originator,
		SourceTaskID: input.SourceTaskID, Payload: input.Payload, Status: status, FailureReason: reason,
	})
	return out, err
}

// WakeIssueAgentSession creates one execution for an inbox, irrespective of the
// number of queued events. A session row lock covers the active-task decision.
func (s *TaskService) WakeIssueAgentSession(ctx context.Context, sessionID pgtype.UUID) (*db.AgentTaskQueue, error) {
	var created *db.AgentTaskQueue
	var signalTask, signalRuntime pgtype.UUID
	err := s.runInTx(ctx, func(q *db.Queries) error {
		ownersExist, err := q.LockIssueAgentSessionOwners(ctx, sessionID)
		if err != nil {
			return err
		}
		if !ownersExist {
			return errors.New("issue session owners no longer exist")
		}
		session, err := q.GetIssueAgentSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if session.WakeAfter.Valid && session.WakeAfter.Time.After(time.Now()) {
			return nil
		}
		agent, err := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: session.AgentID, WorkspaceID: session.WorkspaceID})
		if err != nil {
			return err
		}
		if !session.RuntimeID.Valid && agent.RuntimeID.Valid && !session.ProviderSessionID.Valid && !session.WorkDir.Valid && !session.ActiveTaskID.Valid {
			session, err = q.BindUnstartedIssueAgentSessionRuntime(ctx, db.BindUnstartedIssueAgentSessionRuntimeParams{ID: session.ID, RuntimeID: agent.RuntimeID})
			if err != nil {
				return err
			}
		}
		block := func(reason string) error {
			_, err := q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{ID: session.ID, State: "blocked", FailureReason: pgtype.Text{String: reason, Valid: true}})
			return err
		}
		if !agent.RuntimeID.Valid {
			return block("runtime_unbound")
		}
		if session.RuntimeID != agent.RuntimeID {
			return block("runtime_changed")
		}
		if agent.ArchivedAt.Valid {
			return block("agent_archived")
		}
		issue, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: session.IssueID, WorkspaceID: session.WorkspaceID})
		if err != nil {
			return err
		}
		owner := issue.AssigneeType.String == "agent" && issue.AssigneeID == session.AgentID
		if !owner {
			explicit, err := hasSessionWakeup(ctx, q, session.ID)
			if err != nil {
				return err
			}
			if !explicit {
				_, err = q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{ID: session.ID, State: "detached", FailureReason: pgtype.Text{String: "reassigned", Valid: true}})
				return err
			}
			if session.State == "detached" && session.FailureReason.String == "reassigned" {
				session, err = q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{ID: session.ID, State: "ready"})
				if err != nil {
					return err
				}
			}
		}
		if issue.Status == "backlog" || issue.TriageState.Valid {
			if session.State == "ready" || session.FailureReason.String == "issue_not_ready" {
				return block("issue_not_ready")
			}
			return nil
		}
		if session.State == "blocked" && session.FailureReason.String == "issue_not_ready" {
			session, err = q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{ID: session.ID, State: "ready"})
			if err != nil {
				return err
			}
		}
		if session.State != "ready" {
			return nil
		}
		if session.ActiveTaskID.Valid {
			signalTask, signalRuntime = session.ActiveTaskID, session.RuntimeID
			return nil
		}
		knownSession := session.ProviderSessionID.Valid && strings.TrimSpace(session.ProviderSessionID.String) != ""
		knownDirectory := session.WorkDir.Valid && strings.TrimSpace(session.WorkDir.String) != ""
		if !knownSession || !knownDirectory {
			started, err := q.HasStartedIssueAgentSessionExecution(ctx, session.ID)
			if err != nil {
				return err
			}
			if started {
				return block("resume_unavailable")
			}
		}
		var event db.IssueAgentEvent
		for {
			event, err = q.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil
			}
			if err != nil {
				return err
			}
			if event.Status != "pending" {
				return block(event.FailureReason.String)
			}
			if s.issueAgentEventCanInvoke(ctx, q, agent, event) {
				break
			}
			if _, err := q.IgnorePendingIssueAgentEvent(ctx, db.IgnorePendingIssueAgentEventParams{EventID: event.ID, FailureReason: pgtype.Text{String: "invocation_not_allowed", Valid: true}}); err != nil {
				return err
			}
		}
		attr := attribution.Unattributed(attribution.EvidenceIssueAssignment, issue.ID)
		if input, ok := decodeAutopilotIssueSessionInput(event); ok {
			attr = input.Attribution
		} else if event.Kind == "wakeup.fired" {
			attr = wakeupEventAttribution(event)
		} else if event.ActorType == "member" {
			attr = attribution.DirectHumanRun(event.ActorID, attribution.EvidenceIssueAssignment, issue.ID)
		} else if event.SourceTaskID.Valid {
			parent, err := q.GetAgentTask(ctx, event.SourceTaskID)
			if err != nil {
				return err
			}
			if parent.AgentID != event.ActorID {
				return block("source_task_mismatch")
			}
			// Capture validates the author's authorization at mutation time. A
			// terminal source task cannot lend its old human identity to a new run.
			attr = attribution.ClassifyDirect(attribution.DirectFacts{IssueID: issue.ID, OriginType: "agent_create", OriginTaskID: parent.ID, OriginOriginator: event.OriginatorUserID})
		}
		attr, err = s.applyAttributionFallback(ctx, attr, agent)
		if err != nil {
			return block("attribution_unavailable")
		}
		overlay := s.buildRuntimeMCPOverlay(ctx, attr.UserID, agent)
		source, delegated, evidenceKind, evidenceRef := attributionCreateParams(attr)
		var workflowStep pgtype.Text
		var squadID pgtype.UUID
		if owner {
			workflowStep = pgtype.Text{String: issue.Status, Valid: true}
		}
		if issue.AssigneeType.String == "squad" && issue.AssigneeID.Valid {
			squad, e := q.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: issue.AssigneeID, WorkspaceID: issue.WorkspaceID})
			if e == nil && !squad.ArchivedAt.Valid && squad.LeaderID == agent.ID {
				squadID, workflowStep = squad.ID, pgtype.Text{String: issue.Status, Valid: true}
			} else if e != nil && !errors.Is(e, pgx.ErrNoRows) {
				return e
			}
		}
		task, err := q.CreateIssueAgentSessionTask(ctx, db.CreateIssueAgentSessionTaskParams{
			ID: dbid.NewV7(), SessionID: session.ID, Priority: priorityToInt(issue.Priority),
			OriginatorUserID: attr.UserID, AccountableUserID: attr.AccountableUserID,
			OriginatorSource: source, DelegatedFromTaskID: delegated, RuleVersionID: attr.RuleVersionID,
			TriggerEvidenceKind: evidenceKind, TriggerEvidenceRefID: evidenceRef,
			RuntimeMcpOverlay: overlay.Overlay, RuntimeConnectedApps: overlay.ConnectedApps,
			WorkflowStep: workflowStep, SquadID: squadID, IsLeaderTask: squadID.Valid,
		})
		if err != nil {
			return err
		}
		if _, err := q.SetIssueAgentSessionActiveTask(ctx, db.SetIssueAgentSessionActiveTaskParams{ID: session.ID, TaskID: task.ID}); err != nil {
			return err
		}
		if input, ok := decodeAutopilotIssueSessionInput(event); ok {
			if _, err := q.SetAutopilotRunDispatchTask(ctx, db.SetAutopilotRunDispatchTaskParams{ID: input.RunID, TaskID: task.ID}); err != nil {
				return err
			}
		}
		created = &task
		return nil
	})
	if err != nil {
		return nil, err
	}
	if created != nil {
		s.broadcastTaskEvent(ctx, protocol.EventTaskQueued, *created)
		s.NotifyTaskEnqueued(ctx, *created)
	} else if signalTask.Valid && s.IssueSessionWakeup != nil {
		s.IssueSessionWakeup.NotifyTaskSupplementAvailable(util.UUIDToString(signalRuntime), util.UUIDToString(signalTask))
	}
	return created, nil
}

// ReleaseIssueAgentSession allows a media-gated inbox to start once its issue
// attachments have committed. Pending events remain in the same session.
func (s *TaskService) ReleaseIssueAgentSession(ctx context.Context, sessionID pgtype.UUID) error {
	if _, err := s.Queries.ReleaseIssueAgentSession(ctx, sessionID); err != nil {
		return err
	}
	_, err := s.WakeIssueAgentSession(ctx, sessionID)
	return err
}

func (s *TaskService) WakeIssueAgentSessionsForRuntime(ctx context.Context, runtimeID pgtype.UUID, limit int32) (int, error) {
	if limit < 1 || limit > 100 {
		limit = 32
	}
	sessions, err := s.Queries.ListWakeableIssueAgentSessions(ctx, db.ListWakeableIssueAgentSessionsParams{RuntimeID: runtimeID, SessionLimit: limit})
	if err != nil {
		return 0, err
	}
	count := 0
	for _, session := range sessions {
		task, err := s.WakeIssueAgentSession(ctx, session.ID)
		if err != nil {
			return count, err
		}
		if task != nil {
			count++
		}
	}
	return count, nil
}

func (s *TaskService) ClaimIssueAgentEvents(ctx context.Context, taskID pgtype.UUID, limit int32) ([]db.IssueAgentEvent, error) {
	if limit < 1 || limit > 32 {
		limit = 32
	}
	var out []db.IssueAgentEvent

	stalePrincipal := false
	err := s.runInTx(ctx, func(q *db.Queries) error {
		task, err := q.GetAgentTask(ctx, taskID)
		if err != nil {
			return err
		}
		sessionID := IssueAgentSessionID(task)
		if !sessionID.Valid {
			return errors.New("task is not an issue session execution")
		}
		session, err := q.GetIssueAgentSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if session.ActiveTaskID != task.ID || session.IssueID != task.IssueID || session.AgentID != task.AgentID || session.RuntimeID != task.RuntimeID {
			return errors.New("task does not own this issue session")
		}
		if task.Status != "dispatched" && task.Status != "running" {
			return errors.New("issue session execution is not active")
		}
		if task.Status == "dispatched" {
			out, err = q.ListDeliveringIssueAgentEventsForTask(ctx, db.ListDeliveringIssueAgentEventsForTaskParams{SessionID: session.ID, TaskID: task.ID})
			if err != nil || len(out) != 0 {
				return err
			}
		}
		if session.State != "ready" {
			return nil
		}
		agent, err := q.GetAgent(ctx, task.AgentID)
		if err != nil {
			return err
		}
		candidates, err := q.ListPendingIssueAgentEventCandidates(ctx, db.ListPendingIssueAgentEventCandidatesParams{SessionID: session.ID, EventLimit: limit})
		if err != nil {
			return err
		}
		allowed := int32(0)
		blocked := ""
		payloadBytes := 0
		for _, event := range candidates {
			if !s.issueAgentEventCanInvoke(ctx, q, agent, event) {
				if _, err := q.IgnorePendingIssueAgentEvent(ctx, db.IgnorePendingIssueAgentEventParams{EventID: event.ID, FailureReason: pgtype.Text{String: "invocation_not_allowed", Valid: true}}); err != nil {
					return err
				}
				continue
			}
			// Tool actions inherit the execution's authorizing principal. Keep a
			// different principal's event for the next execution of this session.
			if issueAgentEventPrincipal(event) != task.OriginatorUserID {
				stalePrincipal = task.Status == "dispatched" && allowed == 0
				break
			}
			if payloadBytes+len(event.Payload) > 256*1024 {
				if allowed > 0 {
					break
				}
				blocked = "event_payload_too_large"
			}
			if blocked != "" {
				if _, err := q.FailPendingIssueAgentEvent(ctx, db.FailPendingIssueAgentEventParams{EventID: event.ID, FailureReason: pgtype.Text{String: blocked, Valid: true}}); err != nil {
					return err
				}
				break
			}
			payloadBytes += len(event.Payload)
			allowed++
		}
		if allowed > 0 {
			out, err = q.ClaimIssueAgentEvents(ctx, db.ClaimIssueAgentEventsParams{SessionID: session.ID, TaskID: task.ID, EventLimit: allowed})
			if err != nil {
				return err
			}
		}
		if blocked != "" && allowed == 0 {
			_, err = q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{ID: session.ID, State: "blocked", FailureReason: pgtype.Text{String: blocked, Valid: true}})
		}
		return err
	})
	if err == nil && stalePrincipal {
		if err = s.supersedeUnstartedIssueAgentSessionTask(ctx, taskID); err != nil {
			return nil, err
		}
		return nil, ErrIssueAgentSessionExecutionSuperseded
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Sequence < out[j].Sequence })
	return out, err
}

// A permission change may remove the first input after this execution was
// queued. Reauthorize its successor without failing another person's input.
// Lock the task before its session, as terminal settlement and pinning do.
func (s *TaskService) supersedeUnstartedIssueAgentSessionTask(ctx context.Context, taskID pgtype.UUID) error {
	var cancelled *db.AgentTaskQueue
	var sessionID pgtype.UUID
	err := s.runInTx(ctx, func(q *db.Queries) error {
		task, err := q.LockIssueAgentSessionTask(ctx, taskID)
		if err != nil {
			return err
		}
		sessionID = IssueAgentSessionID(task)
		session, err := q.GetIssueAgentSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		if task.Status != "dispatched" || task.StartedAt.Valid || session.ActiveTaskID != task.ID || session.State != "ready" {
			return nil
		}
		delivering, err := q.ListDeliveringIssueAgentEventsForTask(ctx, db.ListDeliveringIssueAgentEventsForTaskParams{SessionID: session.ID, TaskID: task.ID})
		if err != nil || len(delivering) != 0 {
			return err
		}
		head, err := q.GetFirstUnsettledIssueAgentEvent(ctx, session.ID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err == nil && (head.Status != "pending" || issueAgentEventPrincipal(head) == task.OriginatorUserID) {
			return nil
		}
		ended, err := q.SupersedeUnstartedIssueAgentSessionTask(ctx, task.ID)
		if err != nil {
			return err
		}
		if err = q.DeleteTaskTokensByTask(ctx, task.ID); err != nil {
			return err
		}
		if _, err = q.FinishIssueAgentSessionTask(ctx, db.FinishIssueAgentSessionTaskParams{ID: session.ID, TaskID: task.ID, State: "ready"}); err != nil {
			return err
		}
		cancelled = &ended
		return nil
	})
	if err != nil {
		return err
	}
	if cancelled != nil {
		s.broadcastTaskEvent(ctx, protocol.EventTaskCancelled, *cancelled)
	}
	if sessionID.Valid {
		_, err = s.WakeIssueAgentSession(ctx, sessionID)
	}
	return err
}

func (s *TaskService) ClaimIssueAgentEvent(ctx context.Context, taskID pgtype.UUID) (*db.IssueAgentEvent, error) {
	batch, err := s.ClaimIssueAgentEvents(ctx, taskID, 1)
	if err != nil || len(batch) == 0 {
		return nil, err
	}
	return &batch[0], nil
}

func (s *TaskService) AckIssueAgentEvent(ctx context.Context, taskID, eventID pgtype.UUID, delivered bool, failureReason string) error {
	var session db.IssueAgentSession
	changed := false
	err := s.runInTx(ctx, func(q *db.Queries) error {
		task, err := q.LockIssueAgentEventDeliveryTask(ctx, db.LockIssueAgentEventDeliveryTaskParams{TaskID: taskID, EventID: eventID})
		if err != nil {
			return err
		}
		event, err := q.GetIssueAgentEvent(ctx, eventID)
		if err != nil {
			return err
		}
		session, err = q.GetIssueAgentSessionForUpdate(ctx, event.SessionID)
		if err != nil {
			return err
		}
		if IssueAgentSessionID(task) != session.ID || task.AgentID != session.AgentID || task.IssueID != session.IssueID || task.RuntimeID != session.RuntimeID {
			return errors.New("delivery task does not belong to this issue session")
		}
		if delivered {
			acked, err := q.AckIssueAgentEventDelivered(ctx, db.AckIssueAgentEventDeliveredParams{EventID: eventID, TaskID: taskID})
			if err != nil {
				return err
			}
			changed = event.Status != acked.Status || event.FailureReason != acked.FailureReason
			return advanceIssueSessionWorkflowStep(ctx, q, task, event)
		}
		failureReason = strings.TrimSpace(failureReason)
		if failureReason == "" || len(failureReason) > 100 {
			failureReason = "provider_rejected"
		}
		acked, err := q.AckIssueAgentEventFailed(ctx, db.AckIssueAgentEventFailedParams{EventID: eventID, TaskID: taskID, FailureReason: pgtype.Text{String: failureReason, Valid: true}})
		if err != nil {
			return err
		}
		changed = event.Status != acked.Status || event.FailureReason != acked.FailureReason
		if session.State == "detached" {
			return nil
		}
		_, err = q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{ID: event.SessionID, State: "blocked", FailureReason: pgtype.Text{String: failureReason, Valid: true}})
		return err
	})
	if err == nil && changed {
		s.publishIssueAgentDeliveryUpdated(session)
	}
	return err
}

func (s *TaskService) publishIssueAgentDeliveryUpdated(session db.IssueAgentSession) {
	if s.Bus == nil {
		return
	}
	s.Bus.Publish(events.Event{
		Type: protocol.EventIssueAgentDeliveryUpdated, WorkspaceID: util.UUIDToString(session.WorkspaceID),
		ActorType: "system", Payload: map[string]any{"issue_id": util.UUIDToString(session.IssueID)},
	})
}

func advanceIssueSessionWorkflowStep(ctx context.Context, q *db.Queries, task db.AgentTaskQueue, event db.IssueAgentEvent) error {
	if event.Kind != "assigned" && event.Kind != "issue.updated" {
		return nil
	}
	var handoff struct {
		Workflow bool   `json:"workflow_handoff"`
		Status   string `json:"status"`
	}
	if json.Unmarshal(event.Payload, &handoff) != nil || !handoff.Workflow || handoff.Status == "" {
		return nil
	}
	if task.Status != "running" || IssueAgentSessionID(task) != event.SessionID || issueAgentEventPrincipal(event) != task.OriginatorUserID {
		return nil
	}
	issue, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{ID: task.IssueID, WorkspaceID: event.WorkspaceID})
	if err != nil {
		return err
	}
	// An acknowledged old handoff cannot authorize work on a newer stage.
	if issue.AssigneeType.String != "agent" || issue.AssigneeID != task.AgentID || issue.Status != handoff.Status {
		return nil
	}
	return q.SetTaskWorkflowStep(ctx, db.SetTaskWorkflowStepParams{ID: task.ID, WorkflowStep: pgtype.Text{String: handoff.Status, Valid: true}})
}

// CanRetryIssueAgentSession reports whether an explicit retry may revalidate the
// recorded conversation. The daemon still verifies the exact provider history
// and owned directory before starting; this never permits a fresh substitute.
func (s *TaskService) CanRetryIssueAgentSession(ctx context.Context, session db.IssueAgentSession) (bool, error) {
	return canRetryIssueAgentSession(ctx, s.Queries, session)
}

func canRetryIssueAgentSession(ctx context.Context, q *db.Queries, session db.IssueAgentSession) (bool, error) {
	if session.State == "detached" {
		explicit, err := hasSessionWakeup(ctx, q, session.ID)
		if err != nil || !explicit {
			return false, err
		}
	}
	agent, err := q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: session.AgentID, WorkspaceID: session.WorkspaceID})
	if err != nil {
		return false, err
	}
	if agent.ArchivedAt.Valid || !agent.RuntimeID.Valid || (session.RuntimeID.Valid && agent.RuntimeID != session.RuntimeID) {
		return false, nil
	}
	knownSession := session.ProviderSessionID.Valid && strings.TrimSpace(session.ProviderSessionID.String) != ""
	knownDirectory := session.WorkDir.Valid && strings.TrimSpace(session.WorkDir.String) != ""
	if knownSession && knownDirectory {
		return session.RuntimeID.Valid, nil
	}
	if session.FailureReason.String == "resume_unavailable" {
		return false, nil
	}
	started, err := q.HasStartedIssueAgentSessionExecution(ctx, session.ID)
	if err != nil {
		return false, err
	}
	return !started && (session.RuntimeID.Valid || (!knownSession && !knownDirectory && !session.ActiveTaskID.Valid)), nil
}

func (s *TaskService) RetryIssueAgentEvent(ctx context.Context, sessionID, eventID pgtype.UUID) error {
	var session db.IssueAgentSession
	if err := s.runInTx(ctx, func(q *db.Queries) error {
		ownersExist, err := q.LockIssueAgentSessionOwners(ctx, sessionID)
		if err != nil {
			return err
		}
		if !ownersExist {
			return ErrIssueAgentSessionResumeBlocked
		}
		session, err = q.GetIssueAgentSessionForUpdate(ctx, sessionID)
		if err != nil {
			return err
		}
		allowed, err := canRetryIssueAgentSession(ctx, q, session)
		if err != nil {
			return err
		}
		if !allowed {
			return ErrIssueAgentSessionResumeBlocked
		}
		if _, err := q.RetryIssueAgentEvent(ctx, db.RetryIssueAgentEventParams{SessionID: sessionID, EventID: eventID}); err != nil {
			return err
		}
		_, err = q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{ID: sessionID, State: "ready"})
		return err
	}); err != nil {
		return err
	}
	s.publishIssueAgentDeliveryUpdated(session)
	_, err := s.WakeIssueAgentSession(ctx, sessionID)
	return err
}

func settleIssueAgentSessionTask(ctx context.Context, q *db.Queries, task db.AgentTaskQueue) error {
	sessionID := IssueAgentSessionID(task)
	if !sessionID.Valid {
		return nil
	}
	session, err := q.GetIssueAgentSessionForUpdate(ctx, sessionID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := q.FailUnacknowledgedIssueAgentEvents(ctx, task.ID); err != nil {
		return err
	}
	if session.State == "detached" && session.FailureReason.String == "reassigned" {
		if err := q.IgnoreDetachedIssueAgentOwnerEvents(ctx, sessionID); err != nil {
			return err
		}
	}
	state, reason := "ready", ""
	if task.Status != "completed" {
		state, reason = "blocked", task.FailureReason.String
		if reason == "" {
			reason = "run_" + task.Status
		}
	}
	if task.SessionRolloutMissing || task.RetiredSessionID.Valid || ResumeUnsafeFailure(task.FailureReason.String, task.Error.String) {
		state, reason = "blocked", "resume_unavailable"
	}
	if (task.StartedAt.Valid || task.Status == "completed") && !task.SessionID.Valid && !session.ProviderSessionID.Valid {
		state, reason = "blocked", "resume_unavailable"
	}
	if (task.StartedAt.Valid || task.Status == "completed") && !task.WorkDir.Valid && !session.WorkDir.Valid {
		state, reason = "blocked", "resume_unavailable"
	}
	identityMismatch := (session.ProviderSessionID.Valid && session.ProviderSessionID.String != "" && task.SessionID.Valid && task.SessionID.String != session.ProviderSessionID.String) ||
		(session.WorkDir.Valid && session.WorkDir.String != "" && task.WorkDir.Valid && task.WorkDir.String != session.WorkDir.String)
	if identityMismatch {
		state, reason = "blocked", "resume_unavailable"
		if session.State != "detached" {
			if _, err := q.SetIssueAgentSessionState(ctx, db.SetIssueAgentSessionStateParams{
				ID: session.ID, State: state, FailureReason: pgtype.Text{String: reason, Valid: true},
			}); err != nil {
				return err
			}
		}
	}
	first, err := q.GetFirstUnsettledIssueAgentEvent(ctx, sessionID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err == nil && first.Status == "failed" && state == "ready" {
		state, reason = "blocked", first.FailureReason.String
	}
	if err == nil && first.Status == "pending" && state == "blocked" && session.State != "detached" {
		if _, err := q.FailPendingIssueAgentEvent(ctx, db.FailPendingIssueAgentEventParams{EventID: first.ID, FailureReason: pgtype.Text{String: reason, Valid: true}}); err != nil {
			return err
		}
	}
	_, err = q.FinishIssueAgentSessionTask(ctx, db.FinishIssueAgentSessionTaskParams{
		ID: sessionID, TaskID: task.ID, ProviderSessionID: task.SessionID, WorkDir: task.WorkDir,
		State: state, FailureReason: pgtype.Text{String: reason, Valid: reason != ""},
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	return err
}

func (s *TaskService) wakeCompletedIssueAgentSession(ctx context.Context, task db.AgentTaskQueue) {
	if sessionID := IssueAgentSessionID(task); sessionID.Valid {
		if _, err := s.WakeIssueAgentSession(ctx, sessionID); err != nil {
			slog.Error("wake issue session after completion failed", "task_id", util.UUIDToString(task.ID), "error", err)
		}
	}
}
