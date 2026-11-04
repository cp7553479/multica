"use client";

import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { AlertCircle, Loader2, RotateCcw } from "@multica/ui/icons";
import { Button } from "@multica/ui/components/ui/button";
import { issueAgentSessionsOptions, useRetryIssueAgentSessionEvent } from "@multica/core/issues/agent-sessions";
import { useWorkspaceId } from "@multica/core/hooks";
import { useActorName } from "@multica/core/workspace/hooks";
import type { IssueAgentSession, IssueAgentSessionEvent } from "@multica/core/types";
import { useT } from "../../i18n";
import { IssueSidebarSectionHeader } from "./issue-sidebar-section";

type IssuesT = ReturnType<typeof useT<"issues">>["t"];

function eventStatus(status: string, t: IssuesT) {
  switch (status) {
    case "pending": return t(($) => $.agent_sessions.pending);
    case "delivering": return t(($) => $.agent_sessions.delivering);
    case "delivered": return t(($) => $.agent_sessions.delivered);
    case "failed": return t(($) => $.agent_sessions.failed);
    case "ignored": return t(($) => $.agent_sessions.ignored);
    default: return t(($) => $.agent_sessions.unknown);
  }
}

function failureMessage(reason: string | null, t: IssuesT, canRetry = false) {
  if (reason?.includes("unsupported") || reason === "supplement_unavailable") {
    return t(($) => $.agent_sessions.unsupported);
  }
  switch (reason) {
    case "invocation_not_allowed": return t(($) => $.agent_sessions.invocation_not_allowed);
    case "delivery_unconfirmed": return t(($) => $.agent_sessions.delivery_unconfirmed);
    case "provider_rejected": return t(($) => $.agent_sessions.provider_rejected);
    case "turn_ended": return t(($) => $.agent_sessions.turn_ended);
    case "runtime_changed":
    case "resume_unavailable": return canRetry
      ? t(($) => $.agent_sessions.retry_original_session)
      : t(($) => $.agent_sessions.resume_unavailable);
    default: return t(($) => $.agent_sessions.blocked_hint);
  }
}

const KNOWN_RETRYABLE_FAILURES = new Set([
  "task_failed", "runtime_unbound", "issue_not_ready", "unsupported", "steering_unsupported",
  "supplement_unavailable", "delivery_unconfirmed", "provider_rejected", "turn_ended",
]);

function canRetryEvent(session: IssueAgentSession, event: IssueAgentSessionEvent) {
  if (session.can_retry !== undefined) return session.can_retry;
  // Older responses cannot establish recovery from an unknown blocked state.
  if (session.state === "blocked" && !session.failure_reason && !event.failure_reason) return false;
  return [session.failure_reason, event.failure_reason]
    .every((reason) => reason === null || KNOWN_RETRYABLE_FAILURES.has(reason));
}

/** Delivery belongs to the persistent session, independently of comment threads. */
export function IssueAgentSessions({ issueId }: { issueId: string }) {
  const { t } = useT("issues");
  const wsId = useWorkspaceId() ?? "";
  const { getActorName } = useActorName();
  const [open, setOpen] = useState(true);
  const sessions = useQuery(issueAgentSessionsOptions(wsId, issueId));
  const retry = useRetryIssueAgentSessionEvent(wsId, issueId);

  if (sessions.isPending || (!sessions.isError && !sessions.data?.length)) return null;

  const retryEvent = (event: IssueAgentSessionEvent) => {
    retry.mutate({ sessionId: event.session_id, eventId: event.id });
  };

  return (
    <section>
      <IssueSidebarSectionHeader title={t(($) => $.agent_sessions.title)} open={open} onOpenChange={setOpen}
        count={sessions.data?.length} />
      {open && <div className="space-y-3 px-4">
        {sessions.isError && <p role="alert" className="text-caption text-destructive">{t(($) => $.agent_sessions.load_failed)}</p>}
        {retry.isError && <p role="alert" className="text-caption text-destructive">{t(($) => $.agent_sessions.retry_failed)}</p>}
        {sessions.data?.map((session) => {
          const state = session.state === "ready"
            ? session.active_task_id ? t(($) => $.agent_sessions.working) : t(($) => $.agent_sessions.idle)
            : session.state === "blocked" ? t(($) => $.agent_sessions.blocked)
              : session.state === "paused" ? t(($) => $.agent_sessions.paused)
                : session.state === "detached" ? t(($) => $.agent_sessions.detached)
                : session.state === "closed" ? t(($) => $.agent_sessions.closed)
                  : t(($) => $.agent_sessions.unknown);
          const ordered = [...session.events].sort((a, b) => a.sequence - b.sequence);
          const firstUnsettled = ordered.find((event) => ["pending", "delivering", "failed"].includes(event.status));
          const recent = ordered.slice(-10);
          // Keep the recoverable head visible even when newer events fill the panel.
          const events = firstUnsettled && !recent.includes(firstUnsettled)
            ? [firstUnsettled, ...recent.slice(-9)] : recent;
          return (
            <div key={session.id} className="min-w-0 space-y-1.5">
              <p className="flex min-w-0 items-center justify-between gap-2 text-caption">
                <span className="truncate font-medium">{getActorName("agent", session.agent_id)}</span>
                <span className="shrink-0 text-muted-foreground">{state}</span>
              </p>
              {session.failure_reason && <p role="alert" className="text-caption text-destructive">{failureMessage(session.failure_reason, t, session.can_retry === true)}</p>}
              <ol className="space-y-1.5">
                {events.map((event) => (
                  <li key={event.id} className="space-y-1 text-caption">
                    <div className="flex items-center justify-between gap-2">
                      <span className="min-w-0 truncate text-muted-foreground">{t(($) => $.agent_sessions.event, { sequence: event.sequence })}</span>
                      <span className={event.status === "failed" ? "text-destructive" : "text-muted-foreground"}>
                        {eventStatus(event.status, t)}
                      </span>
                      {(event.status === "failed" || (session.state === "blocked" && event.status === "pending" && event.id === firstUnsettled?.id))
                        && (session.state === "ready" || session.state === "blocked") && (
                        <Button type="button" size="xs" variant="outline" disabled={retry.isPending || !canRetryEvent(session, event)}
                          aria-busy={retry.isPending || undefined}
                          aria-label={t(($) => $.agent_sessions.retry_event, { sequence: event.sequence })}
                          onClick={() => retryEvent(event)}>
                          {retry.isPending && retry.variables?.eventId === event.id
                            ? <Loader2 aria-hidden className="size-3 motion-safe:animate-spin" />
                            : <RotateCcw aria-hidden className="size-3" />}
                          {t(($) => $.agent_sessions.retry)}
                        </Button>
                      )}
                    </div>
                    {(event.status === "failed" || (event.status === "ignored" && event.failure_reason === "invocation_not_allowed")) && <p role="alert" className="flex items-start gap-1 text-destructive">
                      <AlertCircle aria-hidden className="mt-0.5 size-3 shrink-0" />
                      {failureMessage(event.failure_reason, t, session.can_retry === true)}
                    </p>}
                  </li>
                ))}
              </ol>
              {(session.events_truncated || session.events.length > events.length) &&
                <p className="text-caption text-muted-foreground">{t(($) => $.agent_sessions.truncated, { count: events.length })}</p>}
            </div>
          );
        })}
      </div>}
    </section>
  );
}
