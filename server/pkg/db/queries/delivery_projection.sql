-- name: ListIssueAgentDeliveryComments :many
SELECT c.id, c.revision
FROM comment c
WHERE c.workspace_id = @workspace_id AND c.issue_id = @issue_id
  AND c.id = ANY(@comment_ids::uuid[])
  AND c.deleted_at IS NULL
ORDER BY c.created_at, c.id;

-- name: ListIssueAgentCommentDeliveryReceipts :many
-- Look up exact capture keys through the session/key index. A receipt for an
-- earlier body must never be projected onto an edited comment.
SELECT e.id, e.session_id, e.source_id, e.source_revision, e.status, e.delivered_at, s.agent_id
FROM comment c
JOIN issue_agent_session s ON s.workspace_id = c.workspace_id AND s.issue_id = c.issue_id
JOIN issue_agent_event e ON e.session_id = s.id
  AND e.event_key IN (
    'comment.created:' || c.id::text || ':' || c.revision::text,
    'comment.updated:' || c.id::text || ':' || c.revision::text
  )
WHERE c.workspace_id = @workspace_id AND c.issue_id = @issue_id
  AND c.id = ANY(@comment_ids::uuid[]) AND c.deleted_at IS NULL
  AND s.id = ANY(@session_ids::uuid[])
  AND e.workspace_id = c.workspace_id AND e.source_id = c.id
  AND e.source_revision = c.revision AND e.kind IN ('comment.created', 'comment.updated')
ORDER BY c.id, s.created_at, s.id, e.sequence;

-- name: GetIssueAgentDeliverySnapshot :one
SELECT i.title, i.description, i.status, i.stage, i.assignee_id,
  COALESCE((SELECT e.source_revision
   FROM issue_agent_event e
   JOIN issue_agent_session s ON s.id = e.session_id AND s.workspace_id = e.workspace_id
   WHERE e.workspace_id = i.workspace_id AND e.source_id = i.id AND s.issue_id = i.id
     AND e.kind IN ('assigned', 'issue.updated')
     AND i.assignee_type = 'agent' AND i.assignee_id IS NOT NULL
   ORDER BY e.source_revision DESC LIMIT 1
  ), 0)::bigint AS source_revision
FROM issue i
WHERE i.workspace_id = @workspace_id AND i.id = @issue_id;

-- name: ListIssueAgentIssueDeliveryReceipts :many
-- Choose the version across all receivers before applying view permissions.
-- An old receiver's acknowledgement cannot prove delivery of a newer snapshot.
WITH latest AS (
  SELECT e.source_revision
  FROM issue_agent_event e
  JOIN issue_agent_session s ON s.id = e.session_id AND s.workspace_id = e.workspace_id
  WHERE e.workspace_id = @workspace_id AND e.source_id = @issue_id AND s.issue_id = @issue_id
    AND e.kind IN ('assigned', 'issue.updated')
  ORDER BY e.source_revision DESC LIMIT 1
)
SELECT e.id, e.session_id, e.source_id, e.source_revision, e.status, e.delivered_at, s.agent_id
FROM issue i
JOIN issue_agent_session s ON s.workspace_id = i.workspace_id AND s.issue_id = i.id
JOIN issue_agent_event e ON e.session_id = s.id AND e.workspace_id = i.workspace_id
JOIN latest ON latest.source_revision = e.source_revision
WHERE i.workspace_id = @workspace_id AND i.id = @issue_id
  AND i.assignee_type = 'agent' AND i.assignee_id IS NOT NULL
  AND s.id = ANY(@session_ids::uuid[])
  AND e.source_id = i.id AND e.kind IN ('assigned', 'issue.updated')
  AND e.payload->'title' = to_jsonb(i.title)
  AND e.payload->'description' = COALESCE(to_jsonb(i.description), 'null'::jsonb)
  AND e.payload->'status' = to_jsonb(i.status)
  AND e.payload->'stage' = COALESCE(to_jsonb(i.stage), 'null'::jsonb)
  AND e.payload->'assignee_id' = to_jsonb(i.assignee_id)
ORDER BY s.created_at, s.id, e.sequence;
