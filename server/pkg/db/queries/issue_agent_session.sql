-- name: EnsureIssueAgentSession :one
INSERT INTO issue_agent_session (id, workspace_id, issue_id, agent_id, runtime_id)
SELECT @id, @workspace_id, @issue_id, @agent_id, @runtime_id
WHERE lock_task_owner_rows(@agent_id, @issue_id, @runtime_id)
ON CONFLICT (workspace_id, issue_id, agent_id) DO UPDATE
SET issue_id = EXCLUDED.issue_id
RETURNING *;

-- name: GetIssueAgentSession :one
SELECT * FROM issue_agent_session WHERE id = @id;

-- name: GetIssueAgentSessionForUpdate :one
SELECT * FROM issue_agent_session WHERE id = @id FOR UPDATE;

-- name: LockIssueAgentSessionTask :one
SELECT t.* FROM agent_task_queue t
WHERE t.id = @task_id AND t.context->>'type' = 'issue_session'
FOR UPDATE;

-- name: PinIssueAgentSessionTask :one
UPDATE agent_task_queue
SET session_id = COALESCE(session_id, sqlc.narg(provider_session_id)),
    work_dir = COALESCE(work_dir, sqlc.narg(work_dir))
WHERE id = @task_id
RETURNING *;

-- name: SupersedeUnstartedIssueAgentSessionTask :one
UPDATE agent_task_queue
SET status = 'cancelled', completed_at = now(), prepare_lease_expires_at = NULL,
    error = 'Issue session input now requires a different authorizing principal',
    cancelled_by_type = 'system', cancelled_by_id = NULL, cancelled_by_name = NULL
WHERE id = @task_id AND status = 'dispatched' AND started_at IS NULL
  AND context->>'type' = 'issue_session'
RETURNING *;

-- name: PinIssueAgentSessionPointer :one
UPDATE issue_agent_session
SET provider_session_id = COALESCE(provider_session_id, sqlc.narg(provider_session_id)),
    work_dir = COALESCE(work_dir, sqlc.narg(work_dir)), updated_at = now()
WHERE id = @session_id
RETURNING *;

-- name: HasStartedIssueAgentSessionExecution :one
SELECT EXISTS (
    SELECT 1 FROM agent_task_queue t
    WHERE t.context->>'type' = 'issue_session'
      AND t.context->>'issue_agent_session_id' = CAST(sqlc.arg(session_id) AS uuid)::text
      AND (t.started_at IS NOT NULL OR t.status = 'completed')
);

-- name: LockIssueAgentSessionOwners :one
SELECT lock_task_owner_rows(s.agent_id, s.issue_id, s.runtime_id)::boolean AS owners_exist
FROM issue_agent_session s WHERE s.id = @id;

-- name: GetIssueAgentSessionForPair :one
SELECT * FROM issue_agent_session
WHERE workspace_id = @workspace_id AND issue_id = @issue_id AND agent_id = @agent_id;

-- name: ListIssueAgentSessions :many
SELECT * FROM issue_agent_session
WHERE workspace_id = @workspace_id AND issue_id = @issue_id
ORDER BY created_at, id;

-- name: DeferIssueAgentSessionUntil :one
UPDATE issue_agent_session SET wake_after = @wake_after, updated_at = now()
WHERE id = @id AND active_task_id IS NULL
RETURNING *;

-- name: ReleaseIssueAgentSession :one
UPDATE issue_agent_session SET wake_after = NULL, updated_at = now()
WHERE id = @id
RETURNING *;

-- name: NextIssueAgentEventSequence :one
UPDATE issue_agent_session
SET next_sequence = next_sequence + 1, updated_at = now()
WHERE id = @id
RETURNING next_sequence;

-- name: GetIssueAgentEventByKey :one
SELECT * FROM issue_agent_event WHERE session_id = @session_id AND event_key = @event_key;

-- name: GetIssueAgentEvent :one
SELECT * FROM issue_agent_event WHERE id = @id;

-- name: CreateIssueAgentEvent :one
INSERT INTO issue_agent_event (
    id, workspace_id, session_id, sequence, event_key, kind,
    source_id, source_revision, actor_type, actor_id, originator_user_id, source_task_id, payload,
    status, failure_reason
)
VALUES (@id, @workspace_id, @session_id, @sequence, @event_key, @kind,
    @source_id, @source_revision, @actor_type, sqlc.narg(actor_id), sqlc.narg(originator_user_id), sqlc.narg(source_task_id), @payload,
    @status, sqlc.narg(failure_reason))
RETURNING *;

-- name: ListIssueAgentEvents :many
SELECT e.* FROM issue_agent_event e
WHERE e.id IN (
    (SELECT recent.id FROM issue_agent_event recent WHERE recent.session_id = @session_id
     ORDER BY recent.sequence DESC LIMIT @event_limit)
    UNION
    (SELECT head.id FROM issue_agent_event head WHERE head.session_id = @session_id
     AND head.status IN ('pending', 'delivering', 'failed') ORDER BY head.sequence LIMIT 1)
)
ORDER BY e.sequence;

-- name: CountIssueAgentEvents :one
SELECT count(*) FROM issue_agent_event WHERE session_id = @session_id;

-- name: ListDeliveringIssueAgentEventsForTask :many
SELECT * FROM issue_agent_event
WHERE session_id = @session_id AND delivery_task_id = @task_id AND status = 'delivering'
ORDER BY sequence;

-- name: ListWakeableIssueAgentSessions :many
SELECT s.* FROM issue_agent_session s
JOIN agent a ON a.id = s.agent_id AND a.workspace_id = s.workspace_id
WHERE s.active_task_id IS NULL
  AND (s.wake_after IS NULL OR s.wake_after <= now())
  AND a.runtime_id = @runtime_id
  AND (s.state = 'ready' OR (s.state = 'blocked' AND s.failure_reason IN ('runtime_unbound', 'issue_not_ready'))
       OR (s.state = 'detached' AND s.failure_reason = 'reassigned' AND EXISTS (
           SELECT 1 FROM issue_agent_event wake WHERE wake.session_id=s.id AND wake.kind='wakeup.fired' AND wake.status='pending')))
  AND EXISTS (SELECT 1 FROM issue_agent_event e WHERE e.session_id = s.id AND e.status = 'pending')
ORDER BY s.updated_at, s.id
LIMIT @session_limit;

-- name: BindUnstartedIssueAgentSessionRuntime :one
UPDATE issue_agent_session
SET runtime_id = @runtime_id, state = 'ready', failure_reason = NULL, updated_at = now()
WHERE id = @id AND runtime_id IS NULL AND provider_session_id IS NULL
  AND work_dir IS NULL AND active_task_id IS NULL
RETURNING *;

-- name: GetFirstUnsettledIssueAgentEvent :one
SELECT * FROM issue_agent_event
WHERE session_id = @session_id AND status IN ('pending', 'delivering', 'failed')
ORDER BY sequence
LIMIT 1;

-- name: ListPendingIssueAgentEventCandidates :many
SELECT e.* FROM issue_agent_event e
WHERE e.session_id = @session_id AND e.status = 'pending'
  AND NOT EXISTS (
      SELECT 1 FROM issue_agent_event earlier
      WHERE earlier.session_id = e.session_id AND earlier.sequence < e.sequence
        AND earlier.status IN ('delivering', 'failed')
  )
ORDER BY e.sequence LIMIT @event_limit;

-- name: FailPendingIssueAgentEvent :one
UPDATE issue_agent_event SET status = 'failed', failure_reason = @failure_reason, updated_at = now()
WHERE id = @event_id AND status = 'pending'
RETURNING *;

-- name: IgnorePendingIssueAgentEvent :one
UPDATE issue_agent_event SET status = 'ignored', failure_reason = @failure_reason, updated_at = now()
WHERE id = @event_id AND status = 'pending'
RETURNING *;

-- name: ClaimIssueAgentEvents :many
WITH candidates AS MATERIALIZED (
    SELECT e.id
    FROM issue_agent_event e
    JOIN issue_agent_session s ON s.id = e.session_id
    JOIN agent_task_queue t ON t.id = s.active_task_id
    WHERE s.id = @session_id
      AND s.state = 'ready'
      AND t.id = @task_id
      AND t.status IN ('dispatched', 'running')
      AND e.status = 'pending'
      AND NOT EXISTS (
          SELECT 1 FROM issue_agent_event earlier
          WHERE earlier.session_id = s.id
            AND earlier.sequence < e.sequence
            AND earlier.status IN ('delivering', 'failed')
      )
    ORDER BY e.sequence
    LIMIT @event_limit
    FOR UPDATE OF e
)
UPDATE issue_agent_event e
SET status = 'delivering', delivery_task_id = @task_id,
    attempt_count = attempt_count + 1, failure_reason = NULL, updated_at = now()
FROM candidates c WHERE e.id = c.id
RETURNING e.*;

-- name: AckIssueAgentEventDelivered :one
UPDATE issue_agent_event e
SET status = 'delivered', delivered_at = COALESCE(delivered_at, now()),
    failure_reason = NULL, updated_at = now()
WHERE e.id = @event_id AND e.delivery_task_id = @task_id
  AND e.status IN ('delivering', 'delivered', 'failed')
  AND NOT EXISTS (
      SELECT 1 FROM issue_agent_event earlier
      WHERE earlier.session_id = e.session_id AND earlier.sequence < e.sequence
        AND earlier.status IN ('pending', 'delivering', 'failed')
  )
RETURNING *;

-- name: LockIssueAgentEventDeliveryTask :one
-- Match the receipt before taking the task lock. Terminal settlement also
-- locks task before session, so acknowledgements must use the same order.
SELECT t.* FROM agent_task_queue t
JOIN issue_agent_event e ON e.delivery_task_id = t.id
WHERE t.id = @task_id AND e.id = @event_id
  AND t.context->>'type' = 'issue_session'
FOR UPDATE OF t;

-- name: AckIssueAgentEventFailed :one
UPDATE issue_agent_event
SET status = 'failed', failure_reason = @failure_reason, updated_at = now()
WHERE id = @event_id AND delivery_task_id = @task_id AND status IN ('delivering', 'failed')
RETURNING *;

-- name: RetryIssueAgentEvent :one
UPDATE issue_agent_event
SET status = 'pending', delivery_task_id = NULL, failure_reason = NULL, updated_at = now()
WHERE id = @event_id AND session_id = @session_id AND status IN ('pending', 'failed')
RETURNING *;

-- name: SetIssueAgentSessionState :one
UPDATE issue_agent_session
SET state = @state, failure_reason = sqlc.narg(failure_reason), updated_at = now()
WHERE id = @id
RETURNING *;

-- name: SetIssueAgentSessionActiveTask :one
UPDATE issue_agent_session SET active_task_id = @task_id, updated_at = now()
WHERE id = @id
RETURNING *;

-- name: FinishIssueAgentSessionTask :one
UPDATE issue_agent_session
SET active_task_id = NULL,
    provider_session_id = COALESCE(NULLIF(provider_session_id, ''), sqlc.narg(provider_session_id)),
    work_dir = COALESCE(NULLIF(work_dir, ''), sqlc.narg(work_dir)),
    state = CASE WHEN state IN ('detached', 'blocked') THEN state ELSE @state END,
    failure_reason = CASE WHEN state IN ('detached', 'blocked') THEN failure_reason ELSE sqlc.narg(failure_reason) END,
    updated_at = now()
WHERE id = @id AND active_task_id = @task_id
RETURNING *;

-- name: FailUnacknowledgedIssueAgentEvents :exec
UPDATE issue_agent_event
SET status = 'failed', failure_reason = 'delivery_unconfirmed', updated_at = now()
WHERE delivery_task_id = @task_id AND status = 'delivering';

-- name: IgnoreDetachedIssueAgentOwnerEvents :exec
UPDATE issue_agent_event SET status='ignored',failure_reason='reassigned',updated_at=now()
WHERE session_id= @session_id AND kind<>'wakeup.fired' AND status IN ('pending','failed');

-- name: CreateIssueAgentSessionTask :one
INSERT INTO agent_task_queue (
    id, agent_id, runtime_id, issue_id, status, priority, context, max_attempts,
    originator_user_id, accountable_user_id, originator_source,
    delegated_from_task_id, rule_version_id, trigger_evidence_kind, trigger_evidence_ref_id,
    runtime_mcp_overlay, runtime_connected_apps, workflow_step, squad_id, is_leader_task
)
SELECT @id, s.agent_id, s.runtime_id, s.issue_id, 'queued', @priority,
    jsonb_build_object('type', 'issue_session', 'issue_agent_session_id', s.id::text), 1,
    sqlc.narg(originator_user_id), sqlc.narg(accountable_user_id), sqlc.narg(originator_source),
    sqlc.narg(delegated_from_task_id), sqlc.narg(rule_version_id), sqlc.narg(trigger_evidence_kind), sqlc.narg(trigger_evidence_ref_id),
    sqlc.narg(runtime_mcp_overlay), sqlc.narg(runtime_connected_apps),
    sqlc.narg(workflow_step), sqlc.narg(squad_id), @is_leader_task
FROM issue_agent_session s
WHERE s.id = @session_id
  AND s.active_task_id IS NULL AND s.state = 'ready'
  AND (s.wake_after IS NULL OR s.wake_after <= now())
  AND lock_task_owner_rows(s.agent_id, s.issue_id, s.runtime_id)
RETURNING agent_task_queue.*;


-- name: GetAutopilotIssueAgentAssignment :one
SELECT * FROM issue_agent_event WHERE session_id= @session_id AND sequence=1 AND kind='assigned';

-- name: BindAutopilotIssueAgentAssignment :one
UPDATE issue_agent_event SET actor_type='system',actor_id=NULL,
 originator_user_id=sqlc.narg(originator_user_id),
 payload=payload||jsonb_build_object('autopilot_dispatch', @dispatch::jsonb),
 status='pending',failure_reason=NULL,updated_at=now()
WHERE session_id= @session_id AND sequence=1 AND kind='assigned'
 AND status IN ('pending','ignored') AND NOT payload ? 'autopilot_dispatch'
RETURNING *;

-- name: FindLegacyAutopilotIssueDispatch :one
-- Read-only cutover evidence; never manufacture another legacy execution.
SELECT id FROM agent_task_queue
WHERE issue_id= @issue_id AND agent_id= @agent_id
 AND context->>'type' IS DISTINCT FROM 'issue_session'
 AND trigger_comment_id IS NULL AND comment_thread_id IS NULL
 AND retry_of_task_id IS NULL AND rerun_of_task_id IS NULL
 AND autopilot_run_id IS NULL
ORDER BY created_at,id LIMIT 1;
