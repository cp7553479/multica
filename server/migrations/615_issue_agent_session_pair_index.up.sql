CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_issue_agent_session_pair
    ON issue_agent_session (workspace_id, issue_id, agent_id);
