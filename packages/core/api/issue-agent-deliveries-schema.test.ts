// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { IssueAgentDeliveriesSchema } from "./issue-agent-deliveries-schema";

const issueId = "11111111-1111-4111-8111-111111111111";
const commentId = "22222222-2222-4222-8222-222222222222";
const receipt = {
  event_id: "33333333-3333-4333-8333-333333333333",
  session_id: "44444444-4444-4444-8444-444444444444",
  agent_id: "55555555-5555-4555-8555-555555555555",
  agent_name: "Issue owner",
  agent_avatar_url: "/api/agents/owner/avatar",
  source_id: commentId,
  source_revision: 3,
  status: "delivered",
  delivered_at: "2026-11-04T12:00:00Z",
};
const projection = {
  issue: { source_revision: null, title: "Read latest", description: null, status: "todo", stage: null, assignee_id: null, receipts: [] },
  comments: [{ comment_id: commentId, source_revision: 3, receipts: [receipt] }],
};

afterEach(() => vi.unstubAllGlobals());

describe("issue agent delivery response boundary", () => {
  it("keeps unknown delivery states and defaults absent receipt metadata conservatively", () => {
    const parsed = IssueAgentDeliveriesSchema.parse({
      issue: { ...projection.issue, receipts: undefined },
      comments: [{ ...projection.comments[0], receipts: [{ ...receipt, agent_name: undefined, agent_avatar_url: undefined, status: "future-state", delivered_at: undefined }] }],
    });
    expect(parsed.issue.receipts).toEqual([]);
    expect(parsed.comments[0]?.receipts[0]).toMatchObject({ agent_name: "", agent_avatar_url: null, status: "future-state", delivered_at: null });
  });

  it("preserves timestamp strings for the UI to validate without claiming receipt success", () => {
    const parsed = IssueAgentDeliveriesSchema.parse({ ...projection, comments: [{ ...projection.comments[0], receipts: [{ ...receipt, delivered_at: "invalid-date" }] }] });
    expect(parsed.comments[0]?.receipts[0]?.delivered_at).toBe("invalid-date");
  });

  it.each([
    { source_id: null }, { source_id: "not-a-uuid" }, { event_id: 12 },
    { source_revision: "3" }, { source_revision: 0 }, { source_revision: 1.5 }, { status: true },
    { agent_name: 42 }, { agent_avatar_url: false },
  ])("rejects malformed receipts without coercing delivery evidence: %j", (patch) => {
    expect(IssueAgentDeliveriesSchema.safeParse({ ...projection, comments: [{ ...projection.comments[0], receipts: [{ ...receipt, ...patch }] }] }).success).toBe(false);
  });

  it("requires complete snapshots and rejects malformed comment versions", () => {
    expect(IssueAgentDeliveriesSchema.safeParse({ ...projection, issue: { ...projection.issue, title: undefined } }).success).toBe(false);
    expect(IssueAgentDeliveriesSchema.safeParse({ ...projection, comments: [{ ...projection.comments[0], source_revision: "3" }] }).success).toBe(false);
    expect(IssueAgentDeliveriesSchema.safeParse({ ...projection, comments: {} }).success).toBe(false);
    expect(IssueAgentDeliveriesSchema.safeParse({ ...projection, issue: { ...projection.issue, stage: "2" } }).success).toBe(false);
  });

  it("posts only requested source IDs and forwards cancellation", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify(projection), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const controller = new AbortController();
    const result = await new ApiClient("https://api.example.test").getIssueAgentDeliveries(issueId, [commentId], { signal: controller.signal });
    expect(result).toEqual(projection);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe(`https://api.example.test/api/issues/${issueId}/agent-deliveries`);
    expect(fetchMock.mock.calls[0]?.[1]).toMatchObject({ method: "POST", signal: controller.signal });
    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toEqual({ comment_ids: [commentId] });
  });

  it("does not turn a malformed projection into an empty successful delivery read", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ issue: projection.issue, comments: "invalid" }), { status: 200 })));
    await expect(new ApiClient("https://api.example.test").getIssueAgentDeliveries(issueId, [])).rejects.toThrow("Invalid agent delivery response");
  });
});
