// @vitest-environment jsdom
import { act, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider, useMutation, useQuery } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "../api";
import type { Comment, IssueAgentDeliveries, TimelineEntry } from "../types";
import { useUpdateComment } from "./mutations";
import { issueKeys } from "./queries";
import {
  fenceIssueAgentDeliveries, fetchIssueAgentDeliveries, issueAgentDeliveriesOptions,
  issueAgentDeliveryKeys, issueDeliverySnapshotMatches, useIssueAgentDeliveries,
  type IssueAgentDeliverySnapshot,
} from "./agent-deliveries";

const realtime = vi.hoisted(() => ({
  delivery: undefined as undefined | ((payload: unknown) => void),
  reconnect: undefined as undefined | (() => void),
}));
vi.mock("../api", () => ({ api: { getIssueAgentDeliveries: vi.fn(), updateComment: vi.fn() } }));
vi.mock("../hooks", () => ({ useWorkspaceId: () => "ws" }));
vi.mock("../realtime", () => ({
  useWSEvent: (event: string, callback: (payload: unknown) => void) => { if (event === "issue:agent_delivery_updated") realtime.delivery = callback; },
  useWSReconnect: (callback: () => void) => { realtime.reconnect = callback; },
}));

const snapshot: IssueAgentDeliverySnapshot = { revision: 8, title: "Current", description: "Body", status: "todo", stage: 1, assignee_id: "agent" };
const comments = [{ id: "comment", revision: 3 }];
const receipt = { event_id: "event", session_id: "session", agent_id: "agent", source_id: "comment", source_revision: 3, status: "delivered", delivered_at: "2026-11-04T12:00:00Z" };
const projection: IssueAgentDeliveries = {
  issue: { ...snapshot, source_revision: 7, receipts: [{ ...receipt, source_id: "issue", source_revision: 7 }] },
  comments: [{ comment_id: "comment", source_revision: 3, receipts: [receipt] }],
};

afterEach(() => { vi.resetAllMocks(); realtime.delivery = undefined; realtime.reconnect = undefined; });

describe("delivery source version fences", () => {
  it("keys all loaded sources stably and isolates workspace, edit and undo revisions", () => {
    const key = issueAgentDeliveryKeys.current("ws", "issue", snapshot, [...comments, { id: "earlier", revision: 2 }]);
    expect(key).toEqual(issueAgentDeliveryKeys.current("ws", "issue", snapshot, [{ id: "earlier", revision: 2 }, ...comments, ...comments]));
    expect(key).not.toEqual(issueAgentDeliveryKeys.current("other-ws", "issue", snapshot, comments));
    expect(key).not.toEqual(issueAgentDeliveryKeys.current("ws", "issue", snapshot, [{ id: "comment", revision: 4 }]));
    expect(issueAgentDeliveryKeys.current("ws", "issue", snapshot, comments)).not.toEqual(issueAgentDeliveryKeys.current("ws", "issue", { ...snapshot, revision: 10 }, comments));
  });

  it("accepts the captured snapshot despite unrelated general revision increments", () => {
    expect(issueDeliverySnapshotMatches(projection.issue, snapshot)).toBe(true);
    expect(fenceIssueAgentDeliveries(projection, "issue", snapshot, comments)).toEqual(projection);
  });

  it.each([
    { title: "Edited" }, { description: null }, { status: "blocked" }, { stage: 2 }, { assignee_id: "replacement" }, { revision: undefined }, { revision: 6 },
  ])("hides stale issue receipts after current snapshot changes: %j", (patch) => {
    const result = fenceIssueAgentDeliveries(projection, "issue", { ...snapshot, ...patch }, comments);
    expect(result.issue.receipts).toEqual([]);
    expect(result.comments[0]?.receipts).toEqual([receipt]);
  });

  it("requires each current comment revision and exact receipt source, including resolve/reaction edits", () => {
    const mixed: IssueAgentDeliveries = { ...projection, comments: [
      { ...projection.comments[0]!, receipts: [receipt, { ...receipt, source_id: "different" }, { ...receipt, source_revision: 2 }] },
      { comment_id: "unrequested", source_revision: 3, receipts: [receipt] },
    ] };
    expect(fenceIssueAgentDeliveries(mixed, "issue", snapshot, comments).comments).toEqual(projection.comments);
    for (const revision of [undefined, 4]) {
      expect(fenceIssueAgentDeliveries(mixed, "issue", snapshot, [{ id: "comment", revision }]).comments[0]?.receipts).toEqual([]);
    }
    expect(fenceIssueAgentDeliveries(mixed, "issue", snapshot, [{ id: "comment", revision: 3 }, { id: "comment", revision: 4 }]).comments[0]?.receipts).toEqual([]);
  });

  it("hides only the pending edit's receipts without changing the captured source version", () => {
    const result = fenceIssueAgentDeliveries(projection, "issue", snapshot, comments, ["comment"]);
    expect(result.comments[0]?.receipts).toEqual([]);
    expect(result.issue.receipts).toEqual(projection.issue.receipts);
    expect(fenceIssueAgentDeliveries(projection, "issue", snapshot, comments, ["different"]).comments).toEqual(projection.comments);
  });
});

describe("bounded timeline delivery query", () => {
  it("uses one read for an ordinary timeline, including an issue with no comments", async () => {
    vi.mocked(api.getIssueAgentDeliveries).mockResolvedValue(projection);
    const signal = new AbortController().signal;
    await fetchIssueAgentDeliveries("issue", [], signal);
    expect(api.getIssueAgentDeliveries).toHaveBeenCalledExactlyOnceWith("issue", [], { signal });
    const options = issueAgentDeliveriesOptions("ws", "issue", snapshot, comments);
    expect(options).toMatchObject({ refetchInterval: 5_000, refetchOnWindowFocus: true, refetchOnReconnect: true });
  });

  it("batches all historical IDs without truncation and forwards one cancellation signal", async () => {
    vi.mocked(api.getIssueAgentDeliveries).mockImplementation(async (_issueId, ids) => ({ ...projection, comments: ids.map((id) => ({ comment_id: id, source_revision: 1, receipts: [] })) }));
    const signal = new AbortController().signal;
    const ids = Array.from({ length: 4_001 }, (_, i) => `comment-${i}`);
    const result = await fetchIssueAgentDeliveries("issue", ids, signal);
    expect(vi.mocked(api.getIssueAgentDeliveries).mock.calls.map((call) => call[1].length)).toEqual([2_000, 2_000, 1]);
    expect(result.comments.map((comment) => comment.comment_id)).toEqual(ids);
    for (const call of vi.mocked(api.getIssueAgentDeliveries).mock.calls) expect(call[2]?.signal).toBe(signal);
  });

  it("keeps comment projections but hides issue receipts if batch snapshots disagree", async () => {
    vi.mocked(api.getIssueAgentDeliveries).mockResolvedValueOnce(projection).mockResolvedValueOnce({ ...projection, issue: { ...projection.issue, source_revision: 9, title: "New" }, comments: [] });
    const result = await fetchIssueAgentDeliveries("issue", Array.from({ length: 2_001 }, (_, i) => String(i)), new AbortController().signal);
    expect(result.issue.source_revision).toBeNull();
    expect(result.issue.receipts).toEqual([]);
    expect(result.comments).toEqual(projection.comments);
  });

  it("cancels an old in-flight read before a scoped ACK hint and refreshes on reconnect", async () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    let finishOld!: (data: IssueAgentDeliveries) => void;
    let oldSignal: AbortSignal | undefined;
    vi.mocked(api.getIssueAgentDeliveries).mockImplementationOnce((_id, _ids, options) => {
      oldSignal = options?.signal;
      return new Promise((resolve) => { finishOld = resolve; });
    }).mockResolvedValue(projection);
    const { result, unmount } = renderHook(() => useIssueAgentDeliveries("ws", "issue", snapshot, comments), { wrapper });
    await waitFor(() => expect(api.getIssueAgentDeliveries).toHaveBeenCalledTimes(1));
    act(() => realtime.delivery?.({ issue_id: "unrelated" }));
    expect(api.getIssueAgentDeliveries).toHaveBeenCalledTimes(1);
    await act(async () => { realtime.delivery?.({ issue_id: "issue" }); });
    await waitFor(() => expect(result.current.data?.comments[0]?.receipts).toEqual([receipt]));
    expect(oldSignal?.aborted).toBe(true);
    await act(async () => finishOld({ ...projection, comments: [] }));
    expect(result.current.data?.comments).toEqual(projection.comments);
    act(() => realtime.reconnect?.());
    await waitFor(() => expect(api.getIssueAgentDeliveries).toHaveBeenCalledTimes(3));
    unmount(); client.clear();
  });
});

describe("optimistic comment edit delivery fences", () => {
  const original: TimelineEntry = {
    type: "comment", id: "comment", actor_type: "member", actor_id: "human",
    created_at: "2026-11-04T11:00:00Z", content: "Original", revision: 3, attachments: [],
  };

  it.each([["another-workspace", "issue"], ["ws", "another-issue"]])("ignores pending edits outside %s / %s", async (wsId, issueId) => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    let finish!: () => void;
    vi.mocked(api.getIssueAgentDeliveries).mockResolvedValue(projection);
    const { result, unmount } = renderHook(() => ({
      deliveries: useIssueAgentDeliveries("ws", "issue", snapshot, comments),
      foreignEdit: useMutation({
        mutationKey: issueKeys.commentUpdate(wsId!, issueId!),
        mutationFn: (_input: { commentId: string }) => new Promise<void>((resolve) => { finish = resolve; }),
      }),
    }), { wrapper });
    await waitFor(() => expect(result.current.deliveries.data?.comments[0]?.receipts).toEqual([receipt]));
    act(() => result.current.foreignEdit.mutate({ commentId: "comment" }));
    await waitFor(() => expect(result.current.foreignEdit.isPending).toBe(true));
    expect(result.current.deliveries.data?.comments[0]?.receipts).toEqual([receipt]);
    await act(async () => finish());
    unmount(); client.clear();
  });

  it.each(["success", "failure"] as const)("keeps old ACKs hidden until the %s edit's authoritative timeline read settles", async (outcome) => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    client.setQueryData(issueKeys.timeline("issue"), [original]);
    const wrapper = ({ children }: { children: ReactNode }) => <QueryClientProvider client={client}>{children}</QueryClientProvider>;
    let finishEdit!: (data: Comment) => void;
    let failEdit!: (reason: Error) => void;
    vi.mocked(api.updateComment).mockImplementationOnce(() => new Promise((resolve, reject) => { finishEdit = resolve; failEdit = reject; }));
    vi.mocked(api.getIssueAgentDeliveries).mockResolvedValue(projection);
    let finishRead!: (entries: TimelineEntry[]) => void;
    const readTimeline = vi.fn(() => new Promise<TimelineEntry[]>((resolve) => { finishRead = resolve; }));
    const { result, unmount } = renderHook(() => {
      const timeline = useQuery({ queryKey: issueKeys.timeline("issue"), queryFn: readTimeline, staleTime: Infinity });
      const deliveries = useIssueAgentDeliveries("ws", "issue", snapshot, timeline.data ?? []);
      return { edit: useUpdateComment("issue"), deliveries, timeline };
    }, { wrapper });
    await waitFor(() => expect(result.current.deliveries.data?.comments[0]?.receipts).toEqual([receipt]));
    act(() => result.current.edit.mutate({ commentId: "comment", content: "Optimistic", attachmentIds: [] }));
    await waitFor(() => expect(result.current.timeline.data?.[0]?.content).toBe("Optimistic"));
    expect(result.current.timeline.data?.[0]?.revision).toBe(3);
    await waitFor(() => expect(result.current.deliveries.data?.comments[0]?.receipts).toEqual([]));
    await act(async () => {
      if (outcome === "success") finishEdit({
        id: "comment", issue_id: "issue", author_type: "member", author_id: "human",
        content: "Canonical", type: "comment", parent_id: null, reactions: [], attachments: [],
        created_at: original.created_at, updated_at: original.created_at,
        resolved_at: null, resolved_by_type: null, resolved_by_id: null,
        revision: 4, issue_revision: 9,
      });
      else failEdit(new Error("Edit rejected"));
    });
    await waitFor(() => expect(readTimeline).toHaveBeenCalledOnce());
    expect(result.current.edit.isPending).toBe(true);
    expect(result.current.timeline.data?.[0]?.content).toBe(outcome === "success" ? "Canonical" : "Original");
    expect(result.current.deliveries.data?.comments[0]?.receipts ?? []).toEqual([]);
    const authoritative = outcome === "success" ? { ...original, content: "Canonical", revision: 4 } : original;
    await act(async () => finishRead([authoritative]));
    await waitFor(() => expect(result.current.edit.isPending).toBe(false));
    await waitFor(() => expect(result.current.deliveries.data?.comments[0]?.receipts).toEqual(outcome === "success" ? [] : [receipt]));
    if (outcome === "success") {
      const newReceipt = { ...receipt, source_revision: 4 };
      vi.mocked(api.getIssueAgentDeliveries).mockResolvedValue({ ...projection, comments: [{ comment_id: "comment", source_revision: 4, receipts: [newReceipt] }] });
      act(() => realtime.delivery?.({ issue_id: "issue" }));
      await waitFor(() => expect(result.current.deliveries.data?.comments[0]?.receipts).toEqual([newReceipt]));
    }
    unmount(); client.clear();
  });
});
