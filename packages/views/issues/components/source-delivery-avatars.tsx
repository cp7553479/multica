"use client";

import type { IssueDeliveryReceipt } from "@multica/core/types";
import { useActorName } from "@multica/core/workspace/hooks";
import { Tooltip, TooltipContent, TooltipTrigger } from "@multica/ui/components/ui/tooltip";
import { cn } from "@multica/ui/lib/utils";
import { ActorAvatar } from "../../common/actor-avatar";
import { useT } from "../../i18n";

export const EMPTY_DELIVERY_RECEIPTS: readonly IssueDeliveryReceipt[] = [];

/** Only an acknowledgement for this displayed source version earns an avatar. */
export function SourceDeliveryAvatars({
  sourceId,
  sourceRevision,
  receipts = EMPTY_DELIVERY_RECEIPTS,
  className,
}: {
  sourceId: string;
  sourceRevision?: number;
  receipts?: readonly IssueDeliveryReceipt[];
  className?: string;
}) {
  const { t } = useT("issues");
  const { getActorName } = useActorName();
  const acknowledged = sourceRevision === undefined ? [] : receipts.filter((receipt) =>
    receipt.source_id === sourceId && receipt.source_revision === sourceRevision
    && receipt.status === "delivered" && receipt.delivered_at !== null
    && Number.isFinite(Date.parse(receipt.delivered_at)),
  );
  const delivered = [...new Map(acknowledged.map((receipt) => [receipt.agent_id, receipt])).values()];
  if (delivered.length === 0) return null;

  return (
    <div className={cn("mt-1.5 flex flex-wrap items-center gap-1", className)} data-delivery-avatars>
      {delivered.map((receipt) => {
        const name = receipt.agent_name || getActorName("agent", receipt.agent_id);
        const label = t(($) => $.delivery_receipts.delivered_to, { name });
        return (
          <Tooltip key={receipt.agent_id}>
            <TooltipTrigger tabIndex={0} render={
              <span role="img" aria-label={label} className="inline-flex rounded-full focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
                <ActorAvatar actorType="agent" actorId={receipt.agent_id} name={name}
                  avatarUrl={receipt.agent_avatar_url} size="xs" profileLink={false} showStatusDot={false} />
              </span>
            } />
            <TooltipContent side="bottom">{label}</TooltipContent>
          </Tooltip>
        );
      })}
    </div>
  );
}
