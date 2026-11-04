import { describe, expect, it, vi } from "vitest";
import { screen } from "@testing-library/react";
import type { CommentTriggerPreviewAgent } from "@multica/core/types";
import { renderWithI18n } from "../../test/i18n";
import { CommentTriggerChips } from "./comment-trigger-chips";

vi.mock("@multica/core/api", () => ({ api: { getBaseUrl: () => "https://api.example.test" } }));

const walt: CommentTriggerPreviewAgent = { id: "agent-1", name: "Walt", source: "issue_assignee", reason: "" };

describe("CommentTriggerChips", () => {
  it("renders nothing without receiving sessions", () => {
    const { container } = renderWithI18n(<CommentTriggerChips recipients={[]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("shows the receiving session without per-message scheduling choices", () => {
    renderWithI18n(<CommentTriggerChips recipients={[walt]} />);
    expect(screen.getByText("Send to Walt’s session")).toBeInTheDocument();
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(screen.queryByRole("menu")).not.toBeInTheDocument();
    expect(screen.queryByText(/start after|start over|won't start/i)).not.toBeInTheDocument();
  });

  it("resolves uploaded avatars against the API origin", () => {
    renderWithI18n(<CommentTriggerChips recipients={[{ ...walt, avatar_url: "/uploads/custom.png" }]} />);
    expect(screen.getByRole("img", { name: "Walt" })).toHaveAttribute("src", "https://api.example.test/uploads/custom.png");
  });

  it("keeps the session recipient when a comment also broadcasts to members", () => {
    renderWithI18n(<CommentTriggerChips recipients={[walt]} hasAllMembersMention />);
    expect(screen.getByText("Member broadcast")).toBeInTheDocument();
    expect(screen.getByText("Send to Walt’s session")).toBeInTheDocument();
  });

  it("names a refused recipient and gives the actual permission reason", () => {
    renderWithI18n(<CommentTriggerChips recipients={[]} blocked={[
      { target_type: "agent", target_id: "deadbeef-0001", status: "blocked", reason_code: "invocation_not_allowed" },
    ]} draftContent="[@Go](mention://agent/deadbeef-0001) hi" />);
    expect(screen.getByText("Go")).toBeInTheDocument();
    expect(screen.getByText("Not found or no permission")).toBeInTheDocument();
  });
});
