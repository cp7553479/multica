-- Read-only. Run after migrations 614-618, before starting the new daemon.
-- Review each result; this script never creates or replays execution input.
SELECT r.id AS run_id, i.id AS issue_id, r.source, i.status AS issue_status,
       i.assignee_id AS agent_id,
       CASE WHEN s.id IS NULL THEN 'session_missing' ELSE 'assignment_missing' END AS migration_gap
FROM autopilot_run r
JOIN autopilot a ON a.id=r.autopilot_id AND a.execution_mode='create_issue'
JOIN issue i ON i.id=r.issue_id AND i.origin_type='autopilot' AND i.origin_id=a.id
LEFT JOIN issue_agent_session s ON s.workspace_id=i.workspace_id
    AND s.issue_id=i.id AND s.agent_id=i.assignee_id
WHERE r.status='issue_created' AND r.task_id IS NULL
  AND i.creator_type='agent' AND i.assignee_type='agent' AND i.assignee_id=i.creator_id
  AND NOT EXISTS (SELECT 1 FROM agent_task_queue t WHERE t.issue_id=i.id AND t.agent_id=i.creator_id)
  AND (s.id IS NULL OR NOT EXISTS (
      SELECT 1 FROM issue_agent_event e WHERE e.session_id=s.id AND e.sequence=1 AND e.kind='assigned'))
ORDER BY r.created_at,r.id;
