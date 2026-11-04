"use client";

import { useMemo } from "react";
import { TriangleAlert, Users } from "@multica/ui/icons";
import type { CommentTriggerPreviewAgent, CommentTriggerOutcome } from "@multica/core/types";
import { mentionLabelsByTarget } from "@multica/core/issues/comment-trigger-outcomes";
import { resolvePublicFileUrl } from "@multica/core/workspace/avatar-url";
import { ActorAvatar } from "@multica/ui/components/common/actor-avatar";
import { Tooltip, TooltipContent, TooltipTrigger } from "@multica/ui/components/ui/tooltip";
import { useT } from "../../i18n";
import { blockedReasonLabel, blockedShortReasonLabel } from "../blocked-trigger-copy";

interface CommentTriggerChipsProps {
  recipients: CommentTriggerPreviewAgent[];
  blocked?: CommentTriggerOutcome[];
  hasAllMembersMention?: boolean;
  draftContent?: string;
}

/** The server owns routing; the composer only shows the receiving sessions. */
export function CommentTriggerChips({
  recipients,
  blocked = [],
  hasAllMembersMention = false,
  draftContent = "",
}: CommentTriggerChipsProps) {
  const { t } = useT("issues");
  const labels = useMemo(() => mentionLabelsByTarget(draftContent), [draftContent]);
  if (recipients.length === 0 && blocked.length === 0 && !hasAllMembersMention) return null;

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {hasAllMembersMention && (
        <span className="inline-flex h-6 min-w-0 items-center gap-1.5 px-1.5 text-micro text-muted-foreground">
          <Users aria-hidden className="size-3 shrink-0" />
          {t(($) => $.comment.all_members_notice)}
        </span>
      )}
      {recipients.map((agent) => (
        <span key={agent.id}
          aria-label={t(($) => $.comment.session_recipient, { name: agent.name })}
          className="inline-flex h-6 min-w-0 max-w-full items-center gap-1.5 rounded-md bg-brand/7 px-1.5 text-micro text-foreground">
          <ActorAvatar name={agent.name} initials="" avatarUrl={resolvePublicFileUrl(agent.avatar_url)} isAgent size="xs" />
          <span className="truncate">{t(($) => $.comment.session_recipient, { name: agent.name })}</span>
        </span>
      ))}
      {blocked.map((outcome) => {
        const label = labels.get(`${outcome.target_type}:${outcome.target_id}`);
        const reason = blockedShortReasonLabel(outcome.reason_code, t);
        return (
          <Tooltip key={`${outcome.target_type}:${outcome.target_id}`}>
            <TooltipTrigger render={
              <span className="inline-flex h-6 min-w-0 max-w-full items-center gap-1.5 rounded-md px-1.5 text-micro text-destructive"
                aria-label={label ? t(($) => $.comment.trigger_blocked_chip_aria, { name: label, reason }) : reason}>
                <TriangleAlert aria-hidden className="size-3 shrink-0" />
                {label && <span className="truncate">{label}</span>}
                <span>{reason}</span>
              </span>
            } />
            <TooltipContent side="top" className="max-w-72 text-caption">{blockedReasonLabel(outcome.reason_code, t)}</TooltipContent>
          </Tooltip>
        );
      })}
    </div>
  );
}
