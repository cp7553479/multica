import { cleanup, fireEvent, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { afterEach, describe, expect, it, vi } from "vitest";
import { api } from "@multica/core/api";
import { issueAgentSessionKeys } from "@multica/core/issues/agent-sessions";
import type { IssueAgentSession } from "@multica/core/types";
import { renderWithI18n } from "../../test/i18n";
import { IssueAgentSessions } from "./issue-agent-sessions";

vi.mock("@multica/core/api", () => ({ api: { listIssueAgentSessions: vi.fn(), retryIssueAgentSessionEvent: vi.fn() } }));
vi.mock("@multica/core/hooks", () => ({ useWorkspaceId: () => "workspace" }));
vi.mock("@multica/core/workspace/hooks", () => ({ useActorName: () => ({ getActorName: () => "Lambda" }) }));

function session(overrides: Partial<IssueAgentSession> = {}): IssueAgentSession {
  return {
    id: "session", workspace_id: "workspace", issue_id: "issue", agent_id: "agent", runtime_id: "runtime",
    state: "ready", failure_reason: null, active_task_id: null, created_at: "2026-11-04T00:00:00Z", updated_at: "2026-11-04T00:00:00Z",
    events_truncated: false,
    events: [{ id: "event", session_id: "session", sequence: 3, kind: "comment_created", status: "failed", failure_reason: "steering_unsupported",
      attempt_count: 1, source_id: "comment", source_revision: 1, actor_type: "member", actor_id: "user", payload: {}, delivered_at: null,
      created_at: "2026-11-04T00:00:00Z", updated_at: "2026-11-04T00:00:00Z" }],
    ...overrides,
  };
}

function render(sessions: IssueAgentSession[]) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  client.setQueryData(issueAgentSessionKeys.list("workspace", "issue"), sessions);
  vi.mocked(api.listIssueAgentSessions).mockResolvedValue(sessions);
  renderWithI18n(<QueryClientProvider client={client}><IssueAgentSessions issueId="issue" /></QueryClientProvider>);
  return client;
}

afterEach(() => { cleanup(); vi.clearAllMocks(); });

describe("IssueAgentSessions", () => {
  it("shows a refused invocation without offering to retry unauthorized input", () => {
    const value = session();
    value.events[0]!.status = "ignored";
    value.events[0]!.failure_reason = "invocation_not_allowed";
    render([value]);
    expect(screen.getByRole("alert")).toHaveTextContent("You do not have permission to send to this agent.");
    expect(screen.queryByRole("button", { name: "Retry event 3" })).not.toBeInTheDocument();
  });
  it("shows failed and unsupported delivery, then retries the same event", async () => {
    vi.mocked(api.retryIssueAgentSessionEvent).mockResolvedValue();
    render([session()]);
    expect(screen.getByText("Lambda")).toBeInTheDocument();
    expect(screen.getByText("Failed")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("This runtime cannot receive messages while running.");
    fireEvent.click(screen.getByRole("button", { name: "Retry event 3" }));
    await waitFor(() => expect(api.retryIssueAgentSessionEvent).toHaveBeenCalledWith("issue", "session", "event"));
  });

  it("can resume a blocked session from its first pending event", async () => {
    const value = session({ state: "blocked", failure_reason: "task_failed" });
    value.events[0]!.status = "pending";
    value.events[0]!.failure_reason = null;
    for (let sequence = 4; sequence <= 15; sequence++) value.events.push({ ...value.events[0]!, id: `later-${sequence}`, sequence });
    vi.mocked(api.retryIssueAgentSessionEvent).mockResolvedValue();
    render([value]);
    expect(screen.queryByRole("button", { name: "Retry event 4" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Retry event 3" }));
    await waitFor(() => expect(api.retryIssueAgentSessionEvent).toHaveBeenCalledWith("issue", "session", "event"));
  });

  it("keeps the failed head visible ahead of more than ten pending events", async () => {
    const value = session({ state: "blocked", failure_reason: null, events_truncated: true });
    for (let sequence = 4; sequence <= 25; sequence++) value.events.push({
      ...value.events[0]!, id: `later-${sequence}`, sequence, status: "pending", failure_reason: null,
    });
    vi.mocked(api.retryIssueAgentSessionEvent).mockResolvedValue();
    render([value]);
    expect(screen.getByText("Failed")).toBeInTheDocument();
    expect(screen.getAllByRole("listitem")).toHaveLength(10);
    expect(screen.queryByRole("button", { name: "Retry event 4" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Retry event 3" }));
    await waitFor(() => expect(api.retryIssueAgentSessionEvent).toHaveBeenCalledWith("issue", "session", "event"));
  });

  it.each(["resume_unavailable", "runtime_changed"])("does not retry an unrecoverable %s session", (failure) => {
    const value = session({ state: "blocked", failure_reason: failure });
    value.events[0]!.status = "pending";
    render([value]);
    expect(screen.getByRole("button", { name: "Retry event 3" })).toBeDisabled();
    expect(screen.getByRole("alert")).toHaveTextContent("This session cannot be resumed.");
  });

  it("can retry the same event after the original runtime is restored from A to B to A", async () => {
    const value = session({ state: "blocked", failure_reason: "runtime_changed", can_retry: false });
    value.events[0]!.failure_reason = "runtime_changed";
    const client = render([value]);
    expect(screen.getByRole("button", { name: "Retry event 3" })).toBeDisabled();

    vi.mocked(api.retryIssueAgentSessionEvent).mockResolvedValue();
    const restored = { ...value, can_retry: true };
    vi.mocked(api.listIssueAgentSessions).mockResolvedValue([restored]);
    client.setQueryData(issueAgentSessionKeys.list("workspace", "issue"), [restored]);
    await waitFor(() => expect(screen.getByRole("button", { name: "Retry event 3" })).toBeEnabled());
    expect(screen.queryByText(/^This session cannot be resumed/)).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Retry event 3" }));
    await waitFor(() => expect(api.retryIssueAgentSessionEvent).toHaveBeenCalledWith("issue", "session", "event"));
    expect(api.retryIssueAgentSessionEvent).toHaveBeenCalledTimes(1);
  });

  it("permits explicit revalidation of known session storage without starting a fresh session", async () => {
    const value = session({ state: "blocked", failure_reason: "resume_unavailable", can_retry: true });
    value.events[0]!.failure_reason = "resume_unavailable";
    vi.mocked(api.retryIssueAgentSessionEvent).mockResolvedValue();
    render([value]);
    fireEvent.click(screen.getByRole("button", { name: "Retry event 3" }));
    await waitFor(() => expect(api.retryIssueAgentSessionEvent).toHaveBeenCalledWith("issue", "session", "event"));
  });

  it("does not retry when the first provider session ID is unknown", () => {
    const value = session({ state: "blocked", failure_reason: "resume_unavailable", can_retry: false });
    value.events[0]!.status = "pending";
    render([value]);
    fireEvent.click(screen.getByRole("button", { name: "Retry event 3" }));
    expect(screen.getByRole("button", { name: "Retry event 3" })).toBeDisabled();
    expect(screen.getByRole("alert")).toHaveTextContent("This session cannot be resumed.");
    expect(api.retryIssueAgentSessionEvent).not.toHaveBeenCalled();
  });

  it("respects an explicit retry refusal even for a previously retryable failure", () => {
    render([session({ can_retry: false })]);
    expect(screen.getByRole("button", { name: "Retry event 3" })).toBeDisabled();
  });

  it("does not assume an unknown blocked reason can be recovered without a retry capability", () => {
    const value = session({ state: "blocked", failure_reason: "future_failure" });
    value.events[0]!.status = "pending";
    render([value]);
    expect(screen.getByRole("button", { name: "Retry event 3" })).toBeDisabled();
  });

  it("never treats an unknown event status as delivered", () => {
    const value = session();
    value.events[0]!.status = "future_status";
    render([value]);
    expect(screen.getByText("Unknown state")).toBeInTheDocument();
    expect(screen.queryByText("Delivered")).not.toBeInTheDocument();
  });

  it("shows truncation and does not retry detached sessions", () => {
    render([session({ state: "detached", events_truncated: true })]);
    expect(screen.getByText("Showing 1 event from the session history.")).toBeInTheDocument();
    expect(screen.getByText("Reassigned / inactive")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Retry event 3" })).not.toBeInTheDocument();
  });
});
