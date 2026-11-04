// @vitest-environment jsdom
import { beforeEach, describe, expect, it } from "vitest";
import { useCommentComposerStore } from "./comment-composer-store";

describe("comment composer store", () => {
  beforeEach(() => useCommentComposerStore.setState({ sticky: true }));

  it("persists the sticky composer preference", () => {
    useCommentComposerStore.getState().toggleSticky();
    expect(useCommentComposerStore.getState().sticky).toBe(false);
    expect(JSON.parse(localStorage.getItem("multica_comment_composer")!).state).toEqual({ sticky: false });
  });

  it("discards a saved run-routing preference during hydration", async () => {
    localStorage.setItem("multica_comment_composer", JSON.stringify({ state: { sticky: false, runningAgentReply: "after_run" }, version: 0 }));
    await useCommentComposerStore.persist.rehydrate();
    expect(useCommentComposerStore.getState().sticky).toBe(false);
    expect(useCommentComposerStore.getState()).not.toHaveProperty("runningAgentReply");
  });
});
