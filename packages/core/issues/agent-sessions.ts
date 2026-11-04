import { queryOptions, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "../api";
import { issueKeys } from "./queries";

export const issueAgentSessionKeys = {
  list: (wsId: string, issueId: string) => [...issueKeys.all(wsId), issueId, "agent-sessions"] as const,
};

export function issueAgentSessionsOptions(wsId: string, issueId: string) {
  return queryOptions({
    queryKey: issueAgentSessionKeys.list(wsId, issueId),
    queryFn: () => api.listIssueAgentSessions(issueId),
    enabled: !!wsId && !!issueId,
    staleTime: 3_000,
    refetchInterval: 5_000,
    refetchOnWindowFocus: true,
  });
}

/** Requeues the same durable event; it never creates another comment. */
export function useRetryIssueAgentSessionEvent(wsId: string, issueId: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ sessionId, eventId }: { sessionId: string; eventId: string }) =>
      api.retryIssueAgentSessionEvent(issueId, sessionId, eventId),
    onSuccess: () => qc.invalidateQueries({ queryKey: issueAgentSessionKeys.list(wsId, issueId) }),
  });
}
