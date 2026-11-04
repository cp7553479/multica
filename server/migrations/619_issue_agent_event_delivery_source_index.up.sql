CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_issue_agent_event_delivery_source
    ON issue_agent_event (workspace_id, source_id, source_revision DESC)
    WHERE kind IN ('assigned', 'issue.updated');
