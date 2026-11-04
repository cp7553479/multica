// @vitest-environment node
import { describe, expect, it } from "vitest";
import { commentSupplementReceipts } from "./run-steering";

describe("commentSupplementReceipts", () => {
  it("returns the receipt list, or the single receipt an older server sent", () => {
    const receipts = [{ task_id: "a", agent_id: "x", status: "pending" as const }, { task_id: "b", status: "delivered" as const }];
    expect(commentSupplementReceipts({ supplements: receipts, supplement_task_id: "a", supplement_status: "pending" })).toBe(receipts);
    expect(commentSupplementReceipts({ supplement_task_id: "a", supplement_status: "failed", supplement_failure_reason: "turn_ended" }))
      .toEqual([{ task_id: "a", status: "failed", failure_reason: "turn_ended", delivered_at: undefined }]);
    expect(commentSupplementReceipts({})).toEqual([]);
  });
});
