import { act, renderHook, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider, useMutation } from "@tanstack/react-query";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Issue, UpdateIssueRequest } from "@multica/core/types";
import { toast } from "sonner";
import { useIssueActions } from "./use-issue-actions";
import { useIssueSurfaceActions } from "../surface/use-issue-surface-actions";
import { IssueSurfaceActionsProvider } from "../surface/actions-context";

const saveIssue = vi.hoisted(() => vi.fn());
vi.mock("@multica/core/issues/mutations", () => ({
  // Keep the real observer: consecutive mutate calls detach older per-call callbacks.
  useUpdateIssue: () => useMutation({ mutationFn: saveIssue }),
  useBatchUpdateIssues: () => ({ isPending: false, mutateAsync: vi.fn() }),
  useBatchDeleteIssues: () => ({ isPending: false, mutateAsync: vi.fn() }),
}));
vi.mock("@multica/core/hooks", () => ({ useWorkspaceId: () => "workspace" }));
vi.mock("@multica/core/auth", () => ({ useAuthStore: (selector: (value: unknown) => unknown) => selector({ user: null }) }));
vi.mock("@multica/core/paths", () => ({ useWorkspacePaths: () => ({ issueDetail: (id: string) => `/issues/${id}` }) }));
vi.mock("@multica/core/issue-statuses/hooks", () => ({ useIssueStatuses: () => ({ entryOf: () => undefined }) }));
vi.mock("@multica/core/modals", () => ({ useModalStore: Object.assign(
  (selector: (value: unknown) => unknown) => selector({ open: vi.fn() }), { getState: () => ({ open: vi.fn() }) },
) }));
vi.mock("@multica/core/pins", () => ({
  pinListOptions: () => ({ queryKey: ["pins"], queryFn: async () => [] }),
  useCreatePin: () => ({ mutate: vi.fn() }), useDeletePin: () => ({ mutate: vi.fn() }),
}));
vi.mock("../../navigation", () => ({ useNavigation: () => ({ getShareableUrl: (path: string) => path }) }));
vi.mock("../../i18n", () => ({ useT: () => ({ t: () => "Update failed" }) }));
vi.mock("sonner", () => ({ toast: { error: vi.fn() } }));

const issue = { id: "issue", title: "Original", status: "todo", assignee_type: null, assignee_id: null } as Issue;
type Write = { input: Partial<UpdateIssueRequest>; resolve: (issue: Issue) => void; reject: (error: Error) => void };
let writes: Write[];

beforeEach(() => {
  writes = [];
  vi.mocked(toast.error).mockClear();
  saveIssue.mockReset().mockImplementation((input: Partial<UpdateIssueRequest>) => new Promise<Issue>((resolve, reject) => {
    writes.push({ input, resolve, reject });
  }));
});

function Surface({ children }: { children: ReactNode }) {
  const { actions } = useIssueSurfaceActions({ createDefaults: {} });
  return <IssueSurfaceActionsProvider actions={actions}>{children}</IssueSurfaceActionsProvider>;
}

function renderActions(surface: boolean) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  return renderHook(() => useIssueActions(issue), {
    wrapper: ({ children }) => <QueryClientProvider client={client}>
      {surface ? <Surface>{children}</Surface> : children}
    </QueryClientProvider>,
  });
}

describe.each([false, true])("issue update settlement with surface=%s", (surface) => {
  it("notifies both overlapping title requests through the real mutation observer", async () => {
    const first = vi.fn();
    const second = vi.fn();
    const settled = vi.fn();
    const { result } = renderActions(surface);
    act(() => {
      result.current.updateField({ title: "First title" }, { onSuccess: first, onSettled: settled });
      result.current.updateField({ title: "Second title" }, { onSuccess: second, onSettled: settled });
    });
    await waitFor(() => expect(writes).toHaveLength(2));
    await act(async () => writes[0]!.resolve({ ...issue, title: "First title" }));
    await waitFor(() => expect(first).toHaveBeenCalledWith(expect.objectContaining({ title: "First title" })));
    expect(second).not.toHaveBeenCalled();
    await act(async () => writes[1]!.resolve({ ...issue, title: "Second title" }));
    await waitFor(() => expect(second).toHaveBeenCalledWith(expect.objectContaining({ title: "Second title" })));
    expect(settled).toHaveBeenCalledTimes(2);
  });

  it("settles an interleaved description and failed title request independently", async () => {
    const description = vi.fn();
    const titleError = vi.fn();
    const settled = vi.fn();
    const { result } = renderActions(surface);
    act(() => {
      result.current.updateField({ description: "New description" }, { onSuccess: description, onSettled: settled });
      result.current.updateField({ title: "New title" }, { onError: titleError, onSettled: settled });
    });
    await waitFor(() => expect(writes).toHaveLength(2));
    await act(async () => writes[0]!.resolve({ ...issue, description: "New description" }));
    await waitFor(() => expect(description).toHaveBeenCalledTimes(1));
    const error = new Error("Title rejected");
    await act(async () => writes[1]!.reject(error));
    await waitFor(() => expect(titleError).toHaveBeenCalledWith(error));
    expect(settled).toHaveBeenCalledTimes(2);
    expect(toast.error).toHaveBeenCalledWith("Title rejected");
  });
});
