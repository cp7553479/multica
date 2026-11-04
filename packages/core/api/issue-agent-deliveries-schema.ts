import { z } from "zod";

const IssueDeliveryReceiptSchema = z.object({
  event_id: z.string().uuid(),
  session_id: z.string().uuid(),
  agent_id: z.string().uuid(),
  agent_name: z.string().optional().default(""),
  agent_avatar_url: z.string().nullable().optional().default(null),
  source_id: z.string().uuid(),
  source_revision: z.number().int().positive(),
  status: z.string(),
  delivered_at: z.string().nullable().optional().default(null),
});

export const IssueAgentDeliveriesSchema = z.object({
  issue: z.object({
    source_revision: z.number().int().positive().nullable(),
    title: z.string(),
    description: z.string().nullable(),
    status: z.string(),
    stage: z.number().int().nullable(),
    assignee_id: z.string().uuid().nullable(),
    receipts: z.array(IssueDeliveryReceiptSchema).optional().default([]),
  }),
  comments: z.array(z.object({
    comment_id: z.string().uuid(),
    source_revision: z.number().int().positive(),
    receipts: z.array(IssueDeliveryReceiptSchema).optional().default([]),
  })),
});
