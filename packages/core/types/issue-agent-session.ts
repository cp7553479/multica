/** A persistent conversation owned by one issue and agent. */
export interface IssueAgentSession {
  id: string;
  workspace_id: string;
  issue_id: string;
  agent_id: string;
  runtime_id: string | null;
  state: string;
  failure_reason: string | null;
  active_task_id: string | null;
  can_retry?: boolean;
  created_at: string;
  updated_at: string;
  events_truncated: boolean;
  events: IssueAgentSessionEvent[];
}

export interface IssueAgentSessionEvent {
  id: string;
  session_id: string;
  sequence: number;
  kind: string;
  source_id: string | null;
  source_revision: number;
  actor_type: string;
  actor_id: string | null;
  payload: Record<string, unknown>;
  status: string;
  attempt_count: number;
  failure_reason: string | null;
  delivered_at: string | null;
  created_at: string;
  updated_at: string;
}
