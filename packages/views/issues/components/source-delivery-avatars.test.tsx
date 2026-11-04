import { fireEvent, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { IssueDeliveryReceipt } from "@multica/core/types";
import { renderWithI18n } from "../../test/i18n";
import { SourceDeliveryAvatars } from "./source-delivery-avatars";

const avatarProps = vi.hoisted(() => vi.fn());
vi.mock("../../common/actor-avatar", () => ({
  ActorAvatar: (props: Record<string, unknown>) => {
    avatarProps(props);
    return <span data-testid="delivery-avatar" />;
  },
}));
vi.mock("@multica/core/workspace/hooks", () => ({
  useActorName: () => ({ getActorName: (_type: string, id: string) => id === "agent-1" ? "Lambda" : "Orion" }),
}));

function receipt(overrides: Partial<IssueDeliveryReceipt> = {}): IssueDeliveryReceipt {
  return {
    event_id: "event-1", session_id: "session-1", agent_id: "agent-1", source_id: "comment-1", source_revision: 3,
    status: "delivered", delivered_at: "2026-11-04T00:00:00Z", ...overrides,
  };
}

beforeEach(() => avatarProps.mockClear());

describe("SourceDeliveryAvatars", () => {
  it.each(["pending", "delivering", "failed", "ignored", "future_status"])("does not present %s as delivered", (status) => {
    const { container } = renderWithI18n(
      <SourceDeliveryAvatars sourceId="comment-1" sourceRevision={3} receipts={[receipt({ status })]} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it.each([null, "", "not-a-date"])("requires a real acknowledgement timestamp (%s)", (delivered_at) => {
    const { container } = renderWithI18n(
      <SourceDeliveryAvatars sourceId="comment-1" sourceRevision={3} receipts={[receipt({ delivered_at })]} />,
    );
    expect(container).toBeEmptyDOMElement();
  });

  it("does not reuse an acknowledgement for another source or an older displayed revision", () => {
    const { container, rerender } = renderWithI18n(
      <SourceDeliveryAvatars sourceId="comment-2" sourceRevision={3} receipts={[receipt()]} />,
    );
    expect(container).toBeEmptyDOMElement();
    rerender(<SourceDeliveryAvatars sourceId="comment-1" sourceRevision={4} receipts={[receipt()]} />);
    expect(container).toBeEmptyDOMElement();
    rerender(<SourceDeliveryAvatars sourceId="comment-1" receipts={[receipt()]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("shows only tiny acknowledged receiver avatars without visible text or presence signals", () => {
    const { container } = renderWithI18n(
      <SourceDeliveryAvatars sourceId="comment-1" sourceRevision={3} receipts={[
        receipt(), receipt({ event_id: "event-2", session_id: "session-2", agent_id: "agent-2" }),
        receipt({ event_id: "event-3", session_id: "session-3" }),
      ]} />,
    );
    expect(screen.getAllByTestId("delivery-avatar")).toHaveLength(2);
    expect(container.textContent).toBe("");
    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(screen.getByLabelText("Delivered to Lambda")).toBeInTheDocument();
    expect(screen.getByLabelText("Delivered to Orion")).toBeInTheDocument();
    for (const [props] of avatarProps.mock.calls) {
      expect(props).toMatchObject({ actorType: "agent", size: "xs", profileLink: false, showStatusDot: false });
    }
  });

  it("renders each acknowledged agent once across duplicate events and sessions without truncating receivers", () => {
    renderWithI18n(<SourceDeliveryAvatars sourceId="comment-1" sourceRevision={3} receipts={[
      receipt(), receipt(), receipt({ event_id: "duplicate-ack", session_id: "other-session" }),
      ...Array.from({ length: 6 }, (_, index) => receipt({ event_id: `event-${index + 2}`, agent_id: `agent-${index + 2}` })),
    ]} />);
    expect(screen.getAllByTestId("delivery-avatar")).toHaveLength(7);
    expect(screen.getAllByLabelText("Delivered to Lambda")).toHaveLength(1);
  });

  it("explains delivery in its keyboard-accessible tooltip without claiming a read or completed turn", async () => {
    renderWithI18n(<SourceDeliveryAvatars sourceId="comment-1" sourceRevision={3} receipts={[receipt()]} />);
    const avatar = screen.getByLabelText("Delivered to Lambda");
    expect(avatar).toHaveAttribute("tabindex", "0");
    fireEvent.focus(avatar);
    expect(await screen.findByText("Delivered to Lambda")).toBeInTheDocument();
    expect(screen.queryByText(/read|processed|completed/i)).not.toBeInTheDocument();
  });

  it("keeps an archived receiver's hydrated identity when the live directory has changed", () => {
    renderWithI18n(<SourceDeliveryAvatars sourceId="comment-1" sourceRevision={3} receipts={[
      receipt({ agent_name: "Archived helper", agent_avatar_url: "/uploads/archived.png" }),
    ]} />);
    expect(screen.getByRole("img", { name: "Delivered to Archived helper" })).toBeInTheDocument();
    expect(avatarProps).toHaveBeenCalledWith(expect.objectContaining({ name: "Archived helper", avatarUrl: "/uploads/archived.png" }));
  });

  it.each([
    ["en", "Delivered to Lambda"], ["zh-Hans", "已送达 Lambda"], ["zh-Hant", "已送達 Lambda"],
    ["es", "Entregado a Lambda"], ["fr", "Remis à Lambda"], ["ja", "Lambda に配信済み"], ["ko", "Lambda에게 전달됨"],
  ] as const)("localizes the receipt for %s", (locale, label) => {
    renderWithI18n(<SourceDeliveryAvatars sourceId="comment-1" sourceRevision={3} receipts={[receipt()]} />, { locale });
    expect(screen.getByLabelText(label)).toBeInTheDocument();
  });
});
