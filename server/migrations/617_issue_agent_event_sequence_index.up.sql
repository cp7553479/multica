CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_issue_agent_event_sequence
    ON issue_agent_event (session_id, sequence);
