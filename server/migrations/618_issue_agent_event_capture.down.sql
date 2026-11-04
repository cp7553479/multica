DROP TRIGGER IF EXISTS cleanup_issue_agent_workspace ON workspace;
DROP TRIGGER IF EXISTS cleanup_issue_agent_principal ON agent;
DROP FUNCTION IF EXISTS cleanup_issue_agent_principal();
DROP TRIGGER IF EXISTS capture_issue_agent_change ON issue;
DROP FUNCTION IF EXISTS capture_issue_agent_change();
DROP TRIGGER IF EXISTS capture_issue_agent_comment ON comment;
DROP FUNCTION IF EXISTS capture_issue_agent_comment();
DROP FUNCTION IF EXISTS append_issue_agent_event(uuid,text,uuid,bigint,text,uuid,uuid,jsonb);
