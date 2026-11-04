-- name: CreateIssueWakeup :one
INSERT INTO issue_wakeup(id,workspace_id,issue_id,agent_id,created_by,source_task_id,parent_comment_id,instruction,kind,mode,event_types,filter_agent_id,filter_task_id,filter_actor_type,filter_actor_id,interval_seconds,cron_expression,timezone,next_fire_at,expires_at,expiry_seconds,on_timeout,condition,max_fires)
VALUES(@id,@workspace_id,@issue_id,@agent_id,@created_by,sqlc.narg(source_task_id),sqlc.narg(parent_comment_id),@instruction,@kind,@mode,@event_types,sqlc.narg(filter_agent_id),sqlc.narg(filter_task_id),sqlc.narg(filter_actor_type),sqlc.narg(filter_actor_id),sqlc.narg(interval_seconds),sqlc.narg(cron_expression),@timezone,sqlc.narg(next_fire_at),sqlc.narg(expires_at),sqlc.narg(expiry_seconds),sqlc.narg(on_timeout),sqlc.narg(condition),sqlc.narg(max_fires)) RETURNING *;
-- name: ListIssueWakeups :many
SELECT w.id,w.workspace_id,w.issue_id,w.agent_id,w.created_by,w.source_task_id,w.parent_comment_id,w.instruction,
 w.kind,w.mode,w.event_types,w.filter_actor_type,
 (CASE WHEN actor_agent.id IS NOT NULL OR actor_member.user_id IS NOT NULL THEN w.filter_actor_id END)::uuid AS filter_actor_id,
 COALESCE(actor_agent.name,actor_user.name,'')::text AS filter_actor_name,
 (CASE WHEN source.id IS NOT NULL THEN w.filter_agent_id END)::uuid AS filter_agent_id,
 (CASE WHEN EXISTS(SELECT 1 FROM agent_task_queue ft JOIN agent fa ON fa.id=ft.agent_id AND fa.workspace_id=w.workspace_id
  WHERE ft.id=w.filter_task_id AND ft.issue_id=w.issue_id AND fa.id=ANY(@agent_ids::uuid[])) THEN w.filter_task_id END)::uuid AS filter_task_id,
 w.interval_seconds,w.cron_expression,w.timezone,w.next_fire_at,w.enabled,w.disabled_at,w.revision,
 latest_task.task_id AS last_task_id,w.last_error,w.created_at,w.updated_at,a.name AS agent_name,source.name AS filter_agent_name,t.status AS last_task_status,
 w.expires_at,w.expiry_seconds,w.on_timeout,w.timed_out_at,
 w.condition,w.max_fires,w.fire_count,w.paused_reason,
 (w.source_task_id IS NOT NULL)::bool AS created_by_agent,creator.name AS created_by_name,
 (CASE WHEN creator_agent.id IS NOT NULL THEN creator_agent.id END)::uuid AS source_agent_id,creator_agent.name AS source_agent_name
FROM issue_wakeup w JOIN agent a ON a.id=w.agent_id AND a.workspace_id=w.workspace_id
LEFT JOIN "user" creator ON creator.id=w.created_by
LEFT JOIN agent_task_queue creator_task ON creator_task.id=w.source_task_id
LEFT JOIN agent creator_agent ON creator_agent.id=creator_task.agent_id AND creator_agent.workspace_id=w.workspace_id AND creator_agent.id=ANY(@agent_ids::uuid[])
LEFT JOIN agent actor_agent ON w.filter_actor_type='agent' AND actor_agent.id=w.filter_actor_id AND actor_agent.workspace_id=w.workspace_id AND actor_agent.id=ANY(@agent_ids::uuid[])
LEFT JOIN member actor_member ON w.filter_actor_type='member' AND actor_member.user_id=w.filter_actor_id AND actor_member.workspace_id=w.workspace_id
LEFT JOIN "user" actor_user ON actor_user.id=actor_member.user_id
LEFT JOIN agent source ON source.id=w.filter_agent_id AND source.workspace_id=w.workspace_id AND source.id=ANY(@agent_ids::uuid[])
LEFT JOIN LATERAL (
 SELECT e.delivery_task_id AS task_id FROM issue_agent_event e JOIN issue_agent_session sess ON sess.id=e.session_id
 WHERE e.kind='wakeup.fired' AND e.payload->>'wakeup_id'=w.id::text AND e.delivery_task_id IS NOT NULL
 AND sess.issue_id=w.issue_id AND sess.workspace_id=w.workspace_id AND sess.agent_id=w.agent_id
 ORDER BY e.created_at DESC,e.id DESC LIMIT 1
) latest_task ON true
LEFT JOIN agent_task_queue t ON t.id=latest_task.task_id AND t.issue_id=w.issue_id AND t.agent_id=w.agent_id
WHERE w.workspace_id= @workspace_id AND w.issue_id= @issue_id ORDER BY w.created_at,w.id;

-- name: ListWorkspaceWakeupSummaryRows :many
-- No prompts/history; at most three previews per issue plus exact counts.
WITH ranked AS (
 SELECT w.issue_id,w.id,w.agent_id,a.name AS agent_name,w.kind,w.mode,w.event_types,w.filter_actor_type,
 (CASE WHEN actor_agent.id IS NOT NULL OR actor_member.user_id IS NOT NULL THEN w.filter_actor_id END)::uuid AS filter_actor_id,
 COALESCE(actor_agent.name,actor_user.name,'')::text AS filter_actor_name,
  (CASE WHEN EXISTS(SELECT 1 FROM agent_task_queue ft JOIN agent fa ON fa.id=ft.agent_id AND fa.workspace_id=w.workspace_id
   WHERE ft.id=w.filter_task_id AND ft.issue_id=w.issue_id AND fa.id=ANY(@agent_ids::uuid[])) THEN w.filter_task_id END)::uuid AS filter_task_id,
  source.name AS filter_agent_name,w.interval_seconds,w.cron_expression,w.timezone,w.next_fire_at,w.condition,
  count(*) OVER(PARTITION BY w.issue_id) AS active_count,
  count(*) FILTER(WHERE w.kind='event') OVER(PARTITION BY w.issue_id) AS event_count,
  row_number() OVER(PARTITION BY w.issue_id ORDER BY w.next_fire_at NULLS LAST,w.created_at,w.id) AS rank
 FROM issue_wakeup w
 JOIN issue i ON i.id=w.issue_id AND i.workspace_id=w.workspace_id
 JOIN agent a ON a.id=w.agent_id AND a.workspace_id=w.workspace_id
 LEFT JOIN agent actor_agent ON w.filter_actor_type='agent' AND actor_agent.id=w.filter_actor_id AND actor_agent.workspace_id=w.workspace_id AND actor_agent.id=ANY(@agent_ids::uuid[])
LEFT JOIN member actor_member ON w.filter_actor_type='member' AND actor_member.user_id=w.filter_actor_id AND actor_member.workspace_id=w.workspace_id
LEFT JOIN "user" actor_user ON actor_user.id=actor_member.user_id
LEFT JOIN agent source ON source.id=w.filter_agent_id AND source.workspace_id=w.workspace_id AND source.id=ANY(@agent_ids::uuid[])
 WHERE w.workspace_id= @workspace_id AND w.enabled
  AND i.status NOT IN ('done','cancelled')
  AND NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=i.workspace_id AND s.key=i.status AND s.category IN ('done','closed'))
)
SELECT issue_id,id,agent_id,agent_name,kind,mode,event_types,filter_actor_type,filter_actor_id,filter_actor_name,filter_task_id,filter_agent_name,interval_seconds,cron_expression,timezone,next_fire_at,condition,active_count,event_count
FROM ranked WHERE rank<=3 ORDER BY issue_id,rank;
-- name: GetIssueWakeup :one
SELECT * FROM issue_wakeup WHERE id= @id AND workspace_id= @workspace_id;
-- name: LockIssueWakeup :one
SELECT * FROM issue_wakeup WHERE id= @id FOR UPDATE;
-- name: LockWakeupIssue :one
SELECT * FROM issue WHERE id= @id FOR NO KEY UPDATE;
-- name: LockWakeupSourceTask :one
SELECT * FROM agent_task_queue WHERE id= @id AND issue_id= @issue_id FOR UPDATE NOWAIT;
-- name: CancelUnstartedWakeupTasks :many
-- Withdraw inputs that have not reached a provider. A session consumer with
-- other inputs keeps them. An unclaimed consumer is replaced if its inbox is
-- empty or its next input belongs to a different authorizing principal.
WITH locked_tasks AS MATERIALIZED (
 SELECT t.id FROM agent_task_queue t JOIN issue_agent_session s ON s.active_task_id=t.id
 WHERE EXISTS (SELECT 1 FROM issue_agent_event e WHERE e.session_id=s.id
   AND e.kind='wakeup.fired' AND e.payload->>'wakeup_id'= @wakeup_id::text AND e.status IN ('pending','failed'))
 ORDER BY t.id FOR UPDATE OF t
), locked_sessions AS MATERIALIZED (
 SELECT s.id FROM issue_agent_session s
 WHERE (s.active_task_id IS NULL OR s.active_task_id IN (SELECT id FROM locked_tasks))
 AND EXISTS (SELECT 1 FROM issue_agent_event e WHERE e.session_id=s.id
   AND e.kind='wakeup.fired' AND e.payload->>'wakeup_id'= @wakeup_id::text AND e.status IN ('pending','failed'))
 ORDER BY s.id FOR UPDATE OF s
), withdrawn AS (
 UPDATE issue_agent_event SET status='ignored',failure_reason='wakeup_withdrawn',updated_at=now()
 WHERE kind='wakeup.fired' AND payload->>'wakeup_id'= @wakeup_id::text
   AND session_id IN (SELECT id FROM locked_sessions)
   AND status IN ('pending','failed') RETURNING id,session_id
), cancelled AS (
 UPDATE agent_task_queue t SET status='cancelled',completed_at=now(),error='Wakeup disabled or updated'
 WHERE t.status IN ('queued','deferred') AND t.started_at IS NULL
   AND t.context->>'type'='issue_session'
   AND EXISTS (SELECT 1 FROM issue_agent_session s JOIN withdrawn w ON w.session_id=s.id WHERE s.active_task_id=t.id)
   AND (NOT EXISTS (SELECT 1 FROM issue_agent_event e
     WHERE e.session_id::text=t.context->>'issue_agent_session_id' AND e.status IN ('pending','delivering','failed')
     AND e.id NOT IN (SELECT id FROM withdrawn))
     OR (SELECT CASE WHEN e.actor_type='member' THEN e.actor_id ELSE e.originator_user_id END
       FROM issue_agent_event e WHERE e.session_id::text=t.context->>'issue_agent_session_id'
       AND e.status IN ('pending','delivering','failed') AND e.id NOT IN (SELECT id FROM withdrawn)
       ORDER BY e.sequence LIMIT 1) IS DISTINCT FROM t.originator_user_id)
 RETURNING t.*
), released AS (
 UPDATE issue_agent_session SET active_task_id=NULL,updated_at=now() WHERE active_task_id IN (SELECT id FROM cancelled)
)
SELECT * FROM cancelled;
-- name: DisableIssueWakeups :exec
-- Closing wins over every subscription on the issue; reopening does not rearm.
-- A system rule only rests while its issue is closed and resumes on reopen.
UPDATE issue_wakeup SET enabled=false,disabled_at=clock_timestamp(),updated_at=clock_timestamp()
WHERE issue_id= @issue_id AND disabled_at IS NULL AND system_rule IS NULL;
-- name: CancelUnstartedIssueWakeupTasks :many
WITH locked_tasks AS MATERIALIZED (
 SELECT t.id FROM agent_task_queue t JOIN issue_agent_session s ON s.active_task_id=t.id
 WHERE s.issue_id= @issue_id ORDER BY t.id FOR UPDATE OF t
), locked_sessions AS MATERIALIZED (
 SELECT s.id FROM issue_agent_session s WHERE s.issue_id= @issue_id
 AND (s.active_task_id IS NULL OR s.active_task_id IN (SELECT id FROM locked_tasks))
 ORDER BY s.id FOR UPDATE OF s
), withdrawn AS (
 UPDATE issue_agent_event e SET status='ignored',failure_reason='issue_closed',updated_at=now()
 WHERE e.kind='wakeup.fired' AND e.status IN ('pending','failed')
   AND e.session_id IN (SELECT id FROM locked_sessions)
 RETURNING e.id,e.session_id
), cancelled AS (
 UPDATE agent_task_queue t SET status='cancelled',completed_at=now(),error='Issue closed; wakeup disabled'
 WHERE t.issue_id= @issue_id AND t.context->>'type'='issue_session'
   AND t.status IN ('queued','deferred') AND t.started_at IS NULL
   AND EXISTS (SELECT 1 FROM issue_agent_session s JOIN withdrawn w ON w.session_id=s.id WHERE s.active_task_id=t.id)
   AND (NOT EXISTS (SELECT 1 FROM issue_agent_event e
     WHERE e.session_id::text=t.context->>'issue_agent_session_id' AND e.status IN ('pending','delivering','failed')
     AND e.id NOT IN (SELECT id FROM withdrawn))
     OR (SELECT CASE WHEN e.actor_type='member' THEN e.actor_id ELSE e.originator_user_id END
       FROM issue_agent_event e WHERE e.session_id::text=t.context->>'issue_agent_session_id'
       AND e.status IN ('pending','delivering','failed') AND e.id NOT IN (SELECT id FROM withdrawn)
       ORDER BY e.sequence LIMIT 1) IS DISTINCT FROM t.originator_user_id)
 RETURNING t.*
), released AS (
 UPDATE issue_agent_session SET active_task_id=NULL,updated_at=now() WHERE active_task_id IN (SELECT id FROM cancelled)
)
SELECT * FROM cancelled;
-- name: ListInputWakeups :many
-- Scheduler lane 1: rules with unprocessed inputs (subscribed events,
-- condition hints, "wake now"). Deliberately not filtered by enabled: a once
-- rule that stopped still delivers the inputs it captured before stopping.
-- The subquery reads only pending rows (issue_wakeup_receipt_pending_idx).
-- Rules already visited in this pass are skipped.
SELECT w.* FROM issue_wakeup w
WHERE w.id IN (SELECT r.wakeup_id FROM issue_wakeup_receipt r WHERE r.processed_at IS NULL)
  AND (sqlc.narg('workspace_ids')::uuid[] IS NULL OR w.workspace_id = ANY(sqlc.narg('workspace_ids')::uuid[]))
  AND NOT (w.id = ANY(@skip_ids::uuid[]))
ORDER BY w.updated_at,w.id LIMIT @row_limit;
-- name: ListDueWakeups :many
-- Scheduler lane 2: schedules, deadlines and condition checks that are due.
-- Rules already visited in this pass are skipped.
WITH candidates AS (
 SELECT id FROM issue_wakeup WHERE enabled AND kind<>'event' AND next_fire_at<=now()
 UNION
 SELECT id FROM issue_wakeup WHERE enabled AND expires_at<=now()
 UNION
 SELECT id FROM issue_wakeup WHERE enabled AND condition IS NOT NULL AND next_fire_at<=now()
)
SELECT w.* FROM candidates c JOIN issue_wakeup w ON w.id=c.id
WHERE (sqlc.narg('workspace_ids')::uuid[] IS NULL OR w.workspace_id = ANY(sqlc.narg('workspace_ids')::uuid[]))
  AND NOT (w.id = ANY(@skip_ids::uuid[]))
ORDER BY w.updated_at,w.id LIMIT @row_limit;
-- name: ListPendingWakeupInputGroups :many
-- Rule receipts wait only for transactional transfer to the session inbox.
-- Provider delivery is observed on issue_agent_event, never by holding receipts
-- behind a queued or claimed execution.
WITH groups AS (
 SELECT w.id AS wakeup_id,w.issue_id,(w.system_rule IS NOT NULL)::bool AS system_rule,
 false::bool AS reserved,false::bool AS own_claimed,false::bool AS user_waiting,
 min(r.created_at)::timestamptz AS oldest,count(*)::int AS inputs
 FROM issue_wakeup_receipt r JOIN issue_wakeup w ON w.id=r.wakeup_id
 WHERE r.processed_at IS NULL
 AND (sqlc.narg('workspace_ids')::uuid[] IS NULL OR w.workspace_id=ANY(sqlc.narg('workspace_ids')::uuid[]))
 GROUP BY 1,2,3
)
SELECT g.wakeup_id,g.issue_id,g.system_rule,g.reserved,g.own_claimed,g.user_waiting,g.oldest,g.inputs FROM groups g
WHERE sqlc.narg('after_oldest')::timestamptz IS NULL
 OR (g.oldest,g.wakeup_id,g.reserved)>(sqlc.narg('after_oldest')::timestamptz,@after_wakeup_id::uuid,@after_reserved::bool)
ORDER BY g.oldest,g.wakeup_id,g.reserved LIMIT @page_size;
-- name: ListPendingWakeupReceipts :many
SELECT * FROM issue_wakeup_receipt WHERE wakeup_id= @wakeup_id AND revision= @revision AND processed_at IS NULL ORDER BY created_at,id LIMIT 100 FOR UPDATE;

-- name: DeleteExpiredWakeupReceipts :execrows
-- Pending inputs are never expired. Bound work and avoid waiting on dispatch.
-- MATERIALIZED evaluates the batch once. As an IN subquery, a nested-loop plan
-- rescans it per outer row, skips the rows this DELETE already removed, and
-- slides the LIMIT window until every expired row is gone.
WITH batch AS MATERIALIZED (
 SELECT expired.id FROM issue_wakeup_receipt expired WHERE expired.processed_at < @cutoff
 ORDER BY expired.processed_at,expired.id LIMIT 1000 FOR UPDATE SKIP LOCKED
)
DELETE FROM issue_wakeup_receipt r USING batch WHERE r.id=batch.id;
-- name: RecordWakeupReceipt :one
INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload)
VALUES(@id,@wakeup_id,@revision,@event_key,@event_type,@payload)
ON CONFLICT(wakeup_id,revision,event_key) DO UPDATE SET event_key=EXCLUDED.event_key RETURNING *;
-- name: ConsumeWakeupReceipts :exec
UPDATE issue_wakeup_receipt SET task_id=sqlc.narg(task_id),processed_at=now() WHERE id=ANY(@ids::uuid[]);
-- name: DiscardWakeupReceipts :exec
UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id= @id AND processed_at IS NULL;
-- name: AdvanceIssueWakeup :exec
UPDATE issue_wakeup SET enabled= @enabled,next_fire_at=sqlc.narg(next_fire_at),last_task_id=COALESCE(sqlc.narg(last_task_id),last_task_id),last_error=sqlc.narg(last_error),updated_at=clock_timestamp() WHERE id= @id;
-- name: LocklessWakeup :one
SELECT * FROM issue_wakeup WHERE id= @id;

-- name: NoteWakeupFailure :exec
UPDATE issue_wakeup SET last_error=sqlc.narg(last_error),updated_at=clock_timestamp() WHERE id= @id;
-- name: TouchWakeupDispatch :exec
-- Move blocked configurations to the back of the scan so one noisy issue
-- cannot monopolize the bounded batch.
UPDATE issue_wakeup SET updated_at=clock_timestamp() WHERE id= @id;

-- name: MarkWakeupTimedOut :exec
-- The deadline ended the rule. disabled_at stays NULL: a timeout run created in
-- the same transaction must remain claimable, and the rule reads as timed out,
-- not as turned off by a person.
UPDATE issue_wakeup SET enabled=false,next_fire_at=NULL,timed_out_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id= @id;

-- name: SetWakeupConditionState :exec
UPDATE issue_wakeup SET condition_state= @condition_state,next_fire_at=sqlc.narg(next_fire_at),updated_at=clock_timestamp() WHERE id= @id;

-- name: PauseIssueWakeup :exec
-- The platform stopped a rule. A detected loop or burst also sets disabled_at
-- so queued runs of this rule can no longer be claimed; hitting the cap does
-- not, because the run that reached it is legitimate.
UPDATE issue_wakeup SET enabled=false,next_fire_at=NULL,paused_reason= @paused_reason,
 disabled_at=CASE WHEN @block_runs::bool THEN COALESCE(disabled_at,clock_timestamp()) ELSE disabled_at END,
 updated_at=clock_timestamp() WHERE id= @id;

-- name: CountWakeupFires :exec
UPDATE issue_wakeup SET fire_count=fire_count+1 WHERE id= @id;

-- name: CountRecentWakeupTasks :one
SELECT count(*) FROM issue_agent_event e JOIN issue_agent_session s ON s.id=e.session_id
WHERE e.kind='wakeup.fired' AND e.payload->>'wakeup_id'= @wakeup_id::text
 AND s.issue_id= @issue_id AND e.created_at> @since;

-- name: ListWakeupChains :many
-- Rules whose runs produced the source events, including rules further up
-- each run's own trigger chain.
SELECT e.delivery_task_id AS id, COALESCE(e.payload->>'wakeup_id','')::text AS wakeup_id,
 COALESCE(e.payload->'wakeup_chain','[]'::jsonb)::jsonb AS chain
FROM issue_agent_event e WHERE e.kind='wakeup.fired' AND e.delivery_task_id=ANY(@ids::uuid[]);

-- name: ListWakeupRuns :many
-- The rule's latest runs with what triggered each one, a check-in note when
-- the run ended silently, and whether it left a comment.
SELECT t.id,t.status,t.created_at,t.started_at,t.completed_at,
 COALESCE(t.context->'wakeup_checkins'->(sqlc.arg(wakeup_id)::text)->>'note','')::text AS checkin_note,
 COALESCE((SELECT array_agg(DISTINCT f->>'event_type') FROM issue_agent_event e CROSS JOIN LATERAL jsonb_array_elements(COALESCE(e.payload->'wakeup_evidence'->'facts','[]'::jsonb)) f WHERE e.delivery_task_id=t.id AND e.kind='wakeup.fired' AND e.payload->>'wakeup_id'= @wakeup_id::text),'{}')::text[] AS triggers,
 EXISTS(SELECT 1 FROM comment c WHERE c.issue_id=t.issue_id AND c.source_task_id=t.id)::bool AS commented
FROM agent_task_queue t WHERE t.issue_id= @issue_id AND EXISTS (
 SELECT 1 FROM issue_agent_event e WHERE e.delivery_task_id=t.id AND e.kind='wakeup.fired' AND e.payload->>'wakeup_id'= @wakeup_id::text)
ORDER BY t.created_at DESC,t.id DESC LIMIT 10;


-- name: DeleteIssueWakeupReceipts :exec
DELETE FROM issue_wakeup_receipt WHERE wakeup_id= @id;

-- name: DeleteIssueWakeup :exec
DELETE FROM issue_wakeup WHERE id= @id AND issue_id= @issue_id;

-- name: ListPausedWakeupIssues :many
-- Rules the platform paused on open issues, for board cues and the
-- workspace banner. Access follows shared issue visibility.
SELECT w.issue_id,w.id,w.agent_id,w.paused_reason
FROM issue_wakeup w JOIN issue i ON i.id=w.issue_id AND i.workspace_id=w.workspace_id
WHERE w.workspace_id= @workspace_id AND NOT w.enabled AND w.paused_reason IN ('loop','rate','max_fires')
 AND i.status NOT IN ('done','cancelled')
 AND NOT EXISTS(SELECT 1 FROM issue_status s WHERE s.workspace_id=i.workspace_id AND s.key=i.status AND s.category IN ('done','closed'))
ORDER BY w.updated_at DESC LIMIT 200;

-- name: CountActiveIssueRunsOfAgent :one
-- How many of these runs are the agent's own runs on this issue that have
-- not finished: the run that made a change still knows about it.
SELECT count(*) FROM agent_task_queue WHERE id=ANY(@ids::uuid[]) AND agent_id= @agent_id AND issue_id= @issue_id
 AND status IN ('dispatched','running','waiting_local_directory');

-- name: HasReceivedIssueWakeupInput :one
SELECT EXISTS (
 SELECT 1 FROM issue_agent_event e JOIN issue_agent_session s ON s.id=e.session_id
 WHERE e.delivery_task_id= @task_id AND e.kind='wakeup.fired'
 AND e.status IN ('delivering','delivered') AND e.payload->>'wakeup_id'= @wakeup_id::text
 AND s.issue_id= @issue_id AND s.agent_id= @agent_id
);
