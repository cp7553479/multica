import { z } from "zod";

export const IssueAgentSessionEventSchema = z.object({
  id: z.string(),
  session_id: z.string(),
  sequence: z.number().int().positive(),
  kind: z.string(),
  source_id: z.string().nullable().optional().default(null),
  source_revision: z.number().int().nonnegative().optional().default(0),
  actor_type: z.string().optional().default("system"),
  actor_id: z.string().nullable().optional().default(null),
  payload: z.record(z.string(), z.unknown()).optional().default({}),
  status: z.string(),
  attempt_count: z.number().int().nonnegative().optional().default(0),
  failure_reason: z.string().nullable().optional().default(null),
  delivered_at: z.string().nullable().optional().default(null),
  created_at: z.string(),
  updated_at: z.string(),
});

export const IssueAgentSessionsSchema = z.array(z.object({
  id: z.string(),
  workspace_id: z.string(),
  issue_id: z.string(),
  agent_id: z.string(),
  runtime_id: z.string().nullable().optional().default(null),
  state: z.string(),
  failure_reason: z.string().nullable().optional().default(null),
  active_task_id: z.string().nullable().optional().default(null),
  can_retry: z.boolean().optional().catch(undefined),
  created_at: z.string(),
  updated_at: z.string(),
  events_truncated: z.boolean().optional().default(false),
  events: z.array(IssueAgentSessionEventSchema),
}));

export const RetryIssueAgentSessionEventSchema = z.object({ ok: z.literal(true) });
