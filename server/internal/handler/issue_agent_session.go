package handler

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

type IssueAgentEventResponse struct {
	ID             string          `json:"id"`
	SessionID      string          `json:"session_id"`
	Sequence       int64           `json:"sequence"`
	Kind           string          `json:"kind"`
	SourceID       *string         `json:"source_id"`
	SourceRevision int64           `json:"source_revision"`
	ActorType      string          `json:"actor_type"`
	ActorID        *string         `json:"actor_id"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	AttemptCount   int32           `json:"attempt_count"`
	FailureReason  *string         `json:"failure_reason"`
	DeliveredAt    *string         `json:"delivered_at"`
	CreatedAt      string          `json:"created_at"`
	UpdatedAt      string          `json:"updated_at"`
}

type IssueAgentSessionResponse struct {
	ID              string                    `json:"id"`
	WorkspaceID     string                    `json:"workspace_id"`
	IssueID         string                    `json:"issue_id"`
	AgentID         string                    `json:"agent_id"`
	RuntimeID       *string                   `json:"runtime_id"`
	ActiveTaskID    *string                   `json:"active_task_id"`
	State           string                    `json:"state"`
	CanRetry        bool                      `json:"can_retry"`
	FailureReason   *string                   `json:"failure_reason"`
	CreatedAt       string                    `json:"created_at"`
	UpdatedAt       string                    `json:"updated_at"`
	Events          []IssueAgentEventResponse `json:"events"`
	EventsTruncated bool                      `json:"events_truncated"`
}

func issueAgentEventResponse(event db.IssueAgentEvent) IssueAgentEventResponse {
	return IssueAgentEventResponse{
		ID: uuidToString(event.ID), SessionID: uuidToString(event.SessionID),
		Sequence: event.Sequence, Kind: event.Kind, SourceID: uuidToPtr(event.SourceID), SourceRevision: event.SourceRevision,
		ActorType: event.ActorType, ActorID: uuidToPtr(event.ActorID), Payload: json.RawMessage(event.Payload),
		Status: event.Status, AttemptCount: event.AttemptCount, FailureReason: textToPtr(event.FailureReason),
		DeliveredAt: timestampToPtr(event.DeliveredAt), CreatedAt: timestampToString(event.CreatedAt), UpdatedAt: timestampToString(event.UpdatedAt),
	}
}

func (h *Handler) ListIssueAgentSessions(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	workspaceID := uuidToString(issue.WorkspaceID)
	actorType, actorID := h.resolveActor(r, requestUserID(r), workspaceID)
	sessions, err := h.Queries.ListIssueAgentSessions(r.Context(), db.ListIssueAgentSessionsParams{
		WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load issue sessions")
		return
	}
	response := make([]IssueAgentSessionResponse, 0, len(sessions))
	for _, session := range sessions {
		agent, err := h.Queries.GetAgentInWorkspace(r.Context(), db.GetAgentInWorkspaceParams{ID: session.AgentID, WorkspaceID: issue.WorkspaceID})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to resolve session access")
			return
		}
		if !h.canAccessPrivateAgent(r.Context(), agent, actorType, actorID, workspaceID) {
			continue
		}
		canRetry, err := h.TaskService.CanRetryIssueAgentSession(r.Context(), session)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to check issue session recovery")
			return
		}
		canRetry = canRetry && h.canInvokeAgent(r.Context(), agent, actorType, actorID, h.invokeOriginatorFromRequest(r, actorType, actorID), workspaceID)
		rows, err := h.Queries.ListIssueAgentEvents(r.Context(), db.ListIssueAgentEventsParams{SessionID: session.ID, EventLimit: 199})
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to load issue session events")
			return
		}
		total, err := h.Queries.CountIssueAgentEvents(r.Context(), session.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to count issue session events")
			return
		}
		truncated := total > int64(len(rows))
		events := make([]IssueAgentEventResponse, 0, len(rows))
		for _, event := range rows {
			events = append(events, issueAgentEventResponse(event))
		}
		response = append(response, IssueAgentSessionResponse{
			ID: uuidToString(session.ID), WorkspaceID: workspaceID, IssueID: uuidToString(issue.ID), AgentID: uuidToString(session.AgentID),
			RuntimeID: uuidToPtr(session.RuntimeID), ActiveTaskID: uuidToPtr(session.ActiveTaskID), State: session.State, CanRetry: canRetry,
			FailureReason: textToPtr(session.FailureReason), CreatedAt: timestampToString(session.CreatedAt), UpdatedAt: timestampToString(session.UpdatedAt),
			Events: events, EventsTruncated: truncated,
		})
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) RetryIssueAgentEvent(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	sessionID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "sessionId"), "session_id")
	if !ok {
		return
	}
	eventID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "eventId"), "event_id")
	if !ok {
		return
	}
	session, err := h.Queries.GetIssueAgentSession(r.Context(), sessionID)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && (session.WorkspaceID != issue.WorkspaceID || session.IssueID != issue.ID)) {
		writeError(w, http.StatusNotFound, "issue session not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load issue session")
		return
	}
	agent, err := h.Queries.GetAgentInWorkspace(r.Context(), db.GetAgentInWorkspaceParams{ID: session.AgentID, WorkspaceID: issue.WorkspaceID})
	if err != nil {
		writeError(w, http.StatusNotFound, "session agent not found")
		return
	}
	workspaceID := uuidToString(issue.WorkspaceID)
	actorType, actorID := h.resolveActor(r, requestUserID(r), workspaceID)
	if !h.canInvokeAgent(r.Context(), agent, actorType, actorID, h.invokeOriginatorFromRequest(r, actorType, actorID), workspaceID) {
		writeError(w, http.StatusForbidden, "not allowed to invoke this agent")
		return
	}
	if err := h.TaskService.RetryIssueAgentEvent(r.Context(), sessionID, eventID); err != nil {
		if errors.Is(err, service.ErrIssueAgentSessionResumeBlocked) {
			writeErrorCode(w, http.StatusConflict, "issue_session_resume_blocked", "restore this issue session's assignment, runtime, and recorded session storage before retrying")
			return
		}
		if errors.Is(err, pgx.ErrNoRows) {
			writeErrorCode(w, http.StatusConflict, "issue_event_not_retryable", "this event cannot be retried")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to retry issue event")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (h *Handler) ClaimIssueAgentEvent(w http.ResponseWriter, r *http.Request) {
	task, ok := h.requireDaemonTaskAccess(w, r, chi.URLParam(r, "taskId"))
	if !ok {
		return
	}
	if !service.IsIssueAgentSessionTask(task) || task.Status != "running" {
		writeError(w, http.StatusConflict, "issue session is not running")
		return
	}
	event, err := h.TaskService.ClaimIssueAgentEvent(r.Context(), task.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to claim issue event")
		return
	}
	var response *IssueAgentEventResponse
	if event != nil {
		value := issueAgentEventResponse(*event)
		response = &value
	}
	writeJSON(w, http.StatusOK, map[string]any{"event": response})
}

func (h *Handler) AckIssueAgentEvent(w http.ResponseWriter, r *http.Request) {
	task, ok := h.requireDaemonTaskAccess(w, r, chi.URLParam(r, "taskId"))
	if !ok {
		return
	}
	if !service.IsIssueAgentSessionTask(task) {
		writeError(w, http.StatusConflict, "not an issue session execution")
		return
	}
	eventID, ok := parseUUIDOrBadRequest(w, chi.URLParam(r, "eventId"), "event_id")
	if !ok {
		return
	}
	var req struct {
		Delivered     bool   `json:"delivered"`
		FailureReason string `json:"failure_reason"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.TaskService.AckIssueAgentEvent(r.Context(), task.ID, eventID, req.Delivered, stableIssueAgentFailureReason(req.FailureReason)); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusConflict, "issue event is no longer acknowledgeable")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to acknowledge issue event")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func stableIssueAgentFailureReason(reason string) string {
	switch reason {
	case "provider_unsupported", "provider_rejected", "timeout", "turn_ended", "turn_not_started", "delivery_unconfirmed":
		return reason
	default:
		return "provider_rejected"
	}
}

func (h *Handler) applyIssueAgentSessionResume(r *http.Request, task *db.AgentTaskQueue, response *AgentTaskResponse) *claimBuildFailure {
	if !requestHasClientCapability(r, protocol.DaemonCapabilityIssueSessionEventsV1) {
		return h.failClaimedTaskBeforeLaunch(r.Context(), task, "Update this daemon to support persistent issue sessions.", taskfailure.ReasonAgentUnknown, "issue_session_daemon_unsupported", http.StatusPreconditionFailed, "issue session daemon capability required")
	}
	sessionID := service.IssueAgentSessionID(*task)
	session, err := h.Queries.GetIssueAgentSession(r.Context(), sessionID)
	if err != nil {
		return h.rejectClaimSourceLoad(r.Context(), task, err, "issue_session", uuidToString(sessionID))
	}
	if session.WorkspaceID != parseUUID(response.WorkspaceID) || session.IssueID != task.IssueID || session.AgentID != task.AgentID || session.ActiveTaskID != task.ID {
		return h.failClaimedTaskBeforeLaunch(r.Context(), task, "Issue session identity no longer matches this execution.", taskfailure.ReasonInvalidTaskIdentity, "issue_session_identity_invalid", http.StatusConflict, "issue session identity is invalid")
	}
	if session.RuntimeID != task.RuntimeID {
		return h.failClaimedTaskBeforeLaunch(r.Context(), task, "The issue session belongs to another runtime. Restore its runtime before retrying.", taskfailure.ReasonInvalidTaskIdentity, "issue_session_runtime_changed", http.StatusConflict, "issue session runtime changed")
	}
	if session.State != "ready" {
		return h.failClaimedTaskBeforeLaunch(r.Context(), task, "Issue session is blocked or paused; resolve its event state before retrying.", taskfailure.ReasonAgentUnknown, "issue_session_not_ready", http.StatusConflict, "issue session is not ready")
	}
	response.IssueAgentSessionID = uuidToString(session.ID)
	response.PriorSessionID = session.ProviderSessionID.String
	response.PriorWorkDir = session.WorkDir.String
	return nil
}

func (h *Handler) attachInitialIssueAgentEvents(r *http.Request, task *db.AgentTaskQueue, response *AgentTaskResponse) *claimBuildFailure {
	rows, err := h.TaskService.ClaimIssueAgentEvents(r.Context(), task.ID, 32)
	if errors.Is(err, service.ErrIssueAgentSessionExecutionSuperseded) {
		return &claimBuildFailure{outcome: "issue_session_execution_superseded", status: http.StatusConflict, message: "issue session input changed; claim its replacement execution", settled: true}
	}
	if err != nil {
		return h.rejectClaimSourceLoad(r.Context(), task, err, "issue_session_events", response.IssueAgentSessionID)
	}
	response.IssueSessionEvents = make([]IssueAgentEventResponse, 0, len(rows))
	for _, row := range rows {
		response.IssueSessionEvents = append(response.IssueSessionEvents, issueAgentEventResponse(row))
	}
	if len(rows) == 0 {
		return h.failClaimedTaskBeforeLaunch(r.Context(), task, "The issue session has no deliverable events. Check its delivery status before retrying.", taskfailure.ReasonAgentUnknown, "issue_session_inbox_blocked", http.StatusConflict, "issue session inbox is blocked")
	}
	return nil
}
