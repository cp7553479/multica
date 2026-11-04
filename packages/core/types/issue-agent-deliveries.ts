/** A provider acknowledgement for one exact issue or comment revision. */
export interface IssueDeliveryReceipt {
  event_id: string;
  session_id: string;
  agent_id: string;
  agent_name?: string;
  agent_avatar_url?: string | null;
  source_id: string;
  source_revision: number;
  status: string;
  delivered_at: string | null;
}

/** A bounded, read-only projection for the currently loaded issue timeline. */
export interface IssueAgentDeliveries {
  issue: {
    source_revision: number | null;
    title: string;
    description: string | null;
    status: string;
    stage: number | null;
    assignee_id: string | null;
    receipts: IssueDeliveryReceipt[];
  };
  comments: {
    comment_id: string;
    source_revision: number;
    receipts: IssueDeliveryReceipt[];
  }[];
}
