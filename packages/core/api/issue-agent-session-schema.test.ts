// @vitest-environment node
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiClient } from "./client";
import { AgentTaskSchema } from "./schemas";
import { IssueAgentSessionsSchema } from "./issue-agent-session-schema";

const session = {
  id: "session", workspace_id: "workspace", issue_id: "issue", agent_id: "agent", state: "ready",
  created_at: "2026-11-04T00:00:00Z", updated_at: "2026-11-04T00:00:00Z",
  events: [{ id: "event", session_id: "session", sequence: 1, kind: "comment_created", status: "pending",
    created_at: "2026-11-04T00:00:00Z", updated_at: "2026-11-04T00:00:00Z" }],
};

afterEach(() => vi.unstubAllGlobals());

describe("issue agent session API", () => {
  it("preserves unknown states and supplies defaults for optional metadata", () => {
    const parsed = IssueAgentSessionsSchema.parse([{ ...session, state: "future", events: [{ ...session.events[0], status: "future" }] }]);
    expect(parsed[0]).toMatchObject({ state: "future", runtime_id: null, failure_reason: null, events_truncated: false });
    expect(parsed[0]?.can_retry).toBeUndefined();
    expect(parsed[0]?.events[0]).toMatchObject({ status: "future", attempt_count: 0, payload: {}, source_revision: 0 });
  });

  it("preserves an explicit retry capability and never coerces malformed values", () => {
    for (const canRetry of [true, false]) {
      expect(IssueAgentSessionsSchema.parse([{ ...session, can_retry: canRetry }])[0]?.can_retry).toBe(canRetry);
    }
    for (const canRetry of ["true", "false", null, 1]) {
      expect(IssueAgentSessionsSchema.parse([{ ...session, can_retry: canRetry }])[0]?.can_retry).toBeUndefined();
    }
  });

  it("preserves session ownership on run responses and ignores malformed metadata", () => {
    expect(AgentTaskSchema.parse({ id: "task", issue_agent_session_id: "session" }).issue_agent_session_id).toBe("session");
    expect(AgentTaskSchema.parse({ id: "task", issue_agent_session_id: 42 }).issue_agent_session_id).toBeUndefined();
  });

  it("rejects malformed receipts instead of presenting them as delivered", () => {
    expect(IssueAgentSessionsSchema.safeParse([{ ...session, events: [{ ...session.events[0], sequence: "1" }] }]).success).toBe(false);
  });

  it("does not turn a malformed response into an empty successful list", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(JSON.stringify({ sessions: "invalid" }), { status: 200 })));
    await expect(new ApiClient("https://api.example.test").listIssueAgentSessions("issue")).rejects.toThrow("Invalid agent session response");
  });

  it("retries the same session event without creating a comment or task", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(JSON.stringify({ ok: true }), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    await new ApiClient("https://api.example.test").retryIssueAgentSessionEvent("issue", "session", "event");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0]?.[0]).toBe("https://api.example.test/api/issues/issue/agent-sessions/session/events/event/retry");
    expect(fetchMock.mock.calls[0]?.[1]).toMatchObject({ method: "POST" });
  });
});
