import type { CommentSupplementReceipt, TimelineEntry } from "../types";

type ReceiptFields = Pick<
  TimelineEntry,
  "supplements" | "supplement_task_id" | "supplement_status" | "supplement_failure_reason" | "supplement_delivered_at"
>;

/**
 * Every running turn a comment steered. Servers that predate multi-run
 * steering send only the single supplement_* fields, without the agent.
 */
export function commentSupplementReceipts(entry: ReceiptFields): CommentSupplementReceipt[] {
  if (entry.supplements?.length) return entry.supplements;
  if (!entry.supplement_task_id || !entry.supplement_status) return [];
  return [{
    task_id: entry.supplement_task_id,
    status: entry.supplement_status,
    failure_reason: entry.supplement_failure_reason,
    delivered_at: entry.supplement_delivered_at,
  }];
}

/** A receipt whose turn has not received it yet. */
export function isSupplementInFlight(receipt: CommentSupplementReceipt): boolean {
  return receipt.status === "pending" || receipt.status === "delivering";
}
