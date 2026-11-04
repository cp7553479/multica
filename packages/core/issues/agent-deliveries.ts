"use client";

import { useCallback } from "react";
import { queryOptions, useMutationState, useQuery, useQueryClient, type QueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { useWSEvent, useWSReconnect } from "../realtime";
import type { Comment, Issue, IssueAgentDeliveries } from "../types";
import { issueAgentSessionKeys } from "./agent-sessions";
import { issueKeys } from "./queries";
import type { UpdateCommentVars } from "./mutations";

export type IssueAgentDeliverySnapshot = Pick<Issue,
  "revision" | "title" | "description" | "status" | "stage" | "assignee_id"
>;
type CommentVersion = Pick<Comment, "id" | "revision">;

function positiveRevision(revision: number | undefined): number | null {
  return revision !== undefined && Number.isSafeInteger(revision) && revision > 0 ? revision : null;
}

function commentVersions(comments: readonly CommentVersion[]) {
  const revisions = new Map<string, number | null>();
  for (const comment of comments) {
    const revision = positiveRevision(comment.revision);
    revisions.set(comment.id, revisions.has(comment.id) && revisions.get(comment.id) !== revision ? null : revision);
  }
  return [...revisions.entries()].sort(([a], [b]) => a.localeCompare(b));
}

export const issueAgentDeliveryKeys = {
  all: (wsId: string, issueId: string) => [...issueKeys.all(wsId), issueId, "agent-deliveries"] as const,
  current: (wsId: string, issueId: string, issue: IssueAgentDeliverySnapshot | undefined, comments: readonly CommentVersion[]) => [
    ...issueAgentDeliveryKeys.all(wsId, issueId),
    issue ? {
      revision: positiveRevision(issue.revision), title: issue.title, description: issue.description,
      status: issue.status, stage: issue.stage, assignee_id: issue.assignee_id,
    } : null,
    commentVersions(comments),
  ] as const,
};

/** General issue revisions also advance for unrelated mutations. Match the
 * captured fields, while the query key fences edits, undo and reassignment. */
export function issueDeliverySnapshotMatches(
  projection: IssueAgentDeliveries["issue"],
  issue: IssueAgentDeliverySnapshot | undefined,
): boolean {
  const revision = positiveRevision(issue?.revision);
  return !!issue && revision !== null && projection.source_revision !== null &&
    projection.source_revision <= revision &&
    projection.title === issue.title && projection.description === issue.description &&
    projection.status === issue.status && projection.stage === issue.stage &&
    projection.assignee_id === issue.assignee_id;
}

export function fenceIssueAgentDeliveries(
  data: IssueAgentDeliveries,
  issueId: string,
  issue: IssueAgentDeliverySnapshot | undefined,
  comments: readonly CommentVersion[],
  pendingCommentIds: readonly string[] = [],
): IssueAgentDeliveries {
  const revisions = new Map(commentVersions(comments));
  const pending = new Set(pendingCommentIds);
  return {
    issue: {
      ...data.issue,
      receipts: issueDeliverySnapshotMatches(data.issue, issue)
        ? data.issue.receipts.filter((receipt) => receipt.source_id === issueId && receipt.source_revision === data.issue.source_revision)
        : [],
    },
    comments: data.comments.filter((comment) => revisions.has(comment.comment_id)).map((comment) => ({
      ...comment,
      receipts: !pending.has(comment.comment_id) && comment.source_revision === revisions.get(comment.comment_id)
        ? comment.receipts.filter((receipt) => receipt.source_id === comment.comment_id && receipt.source_revision === comment.source_revision)
        : [],
    })),
  };
}

/** One query owns all loaded comments; each HTTP read stays within the
 * server's bound, including timelines loaded beyond the current hard cap. */
export async function fetchIssueAgentDeliveries(issueId: string, commentIds: readonly string[], signal: AbortSignal) {
  const batches: string[][] = [];
  for (let offset = 0; offset < commentIds.length; offset += 2_000) {
    batches.push(commentIds.slice(offset, offset + 2_000));
  }
  if (batches.length === 0) batches.push([]);
  const pages = await Promise.all(batches.map((ids) => api.getIssueAgentDeliveries(issueId, ids, { signal })));
  const first = pages[0]!;
  const consistent = pages.every(({ issue }) =>
    issue.source_revision === first.issue.source_revision && issue.title === first.issue.title &&
    issue.description === first.issue.description && issue.status === first.issue.status &&
    issue.stage === first.issue.stage && issue.assignee_id === first.issue.assignee_id,
  );
  return {
    issue: consistent ? first.issue : { ...first.issue, source_revision: null, receipts: [] },
    comments: pages.flatMap((page) => page.comments),
  };
}

export function issueAgentDeliveriesOptions(
  wsId: string,
  issueId: string,
  issue: IssueAgentDeliverySnapshot | undefined,
  comments: readonly CommentVersion[],
  pendingCommentIds: readonly string[] = [],
) {
  const ids = commentVersions(comments).map(([id]) => id);
  return queryOptions({
    queryKey: issueAgentDeliveryKeys.current(wsId, issueId, issue, comments),
    queryFn: ({ signal }) => fetchIssueAgentDeliveries(issueId, ids, signal),
    select: (data) => fenceIssueAgentDeliveries(data, issueId, issue, comments, pendingCommentIds),
    enabled: !!wsId && !!issueId && !!issue,
    staleTime: 0,
    refetchInterval: 5_000,
    refetchOnWindowFocus: true,
    refetchOnReconnect: true,
  });
}

async function refreshIssueAgentDeliveries(client: QueryClient, wsId: string, issueId: string) {
  const queryKey = issueAgentDeliveryKeys.all(wsId, issueId);
  await client.cancelQueries({ queryKey });
  await Promise.all([
    client.invalidateQueries({ queryKey }),
    client.invalidateQueries({ queryKey: issueAgentSessionKeys.list(wsId, issueId) }),
  ]);
}

export function useIssueAgentDeliveries(
  wsId: string,
  issueId: string,
  issue: IssueAgentDeliverySnapshot | undefined,
  comments: readonly CommentVersion[],
) {
  const client = useQueryClient();
  const pendingCommentIds = useMutationState({
    filters: { mutationKey: issueKeys.commentUpdate(wsId, issueId), exact: true, status: "pending" },
    select: (mutation) => (mutation.state.variables as UpdateCommentVars | undefined)?.commentId,
  }).filter((id): id is string => id !== undefined);
  const refresh = useCallback(() => {
    if (wsId && issueId) void refreshIssueAgentDeliveries(client, wsId, issueId);
  }, [client, wsId, issueId]);
  const onDelivery = useCallback((payload: unknown) => {
    if (typeof payload === "object" && payload !== null && "issue_id" in payload && payload.issue_id === issueId) refresh();
  }, [issueId, refresh]);
  useWSEvent("issue:agent_delivery_updated", onDelivery);
  useWSReconnect(refresh);
  return useQuery(issueAgentDeliveriesOptions(wsId, issueId, issue, comments, pendingCommentIds));
}
