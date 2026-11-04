package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/multica-ai/multica/server/internal/daemon/execenv"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/pkg/agent"
)

// IssueSessionEvent is an immutable inbox receipt; payload remains structured so
// edits and deletions cannot be mistaken for new comments.
type IssueSessionEvent struct {
	ID             string          `json:"id"`
	SessionID      string          `json:"session_id"`
	Sequence       int64           `json:"sequence"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	ActorType      string          `json:"actor_type"`
	ActorID        string          `json:"actor_id"`
	SourceID       string          `json:"source_id"`
	SourceRevision int64           `json:"source_revision"`
	Status         string          `json:"status"`
}

type issueSessionTransport struct {
	SessionID    string
	Initial      []IssueSessionEvent
	AckedInitial *atomic.Int32
	PinReady     <-chan struct{}
}
type issueSessionTransportKey struct{}

func (c *Client) ClaimIssueSessionEvent(ctx context.Context, taskID string) (*IssueSessionEvent, error) {
	var response struct {
		Event *IssueSessionEvent `json:"event"`
	}
	err := c.postJSON(ctx, fmt.Sprintf("/api/daemon/tasks/%s/issue-events/claim", taskID), map[string]any{}, &response)
	return response.Event, err
}

func (c *Client) AckIssueSessionEvent(ctx context.Context, taskID, eventID string, delivered bool, reason string) error {
	return c.postJSONWithRetry(ctx, fmt.Sprintf("/api/daemon/tasks/%s/issue-events/%s/ack", taskID, eventID), map[string]any{"delivered": delivered, "failure_reason": reason}, nil, []time.Duration{100 * time.Millisecond, 300 * time.Millisecond})
}

func (d *Daemon) ackIssueSessionEvent(ctx context.Context, taskID string, event IssueSessionEvent, delivered bool, reason string) error {
	ackCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), taskSupplementAckTimeout)
	defer cancel()
	if err := d.client.AckIssueSessionEvent(ackCtx, taskID, event.ID, delivered, reason); err != nil {
		return fmt.Errorf("issue session event acknowledgement uncertain for %s: %w", event.ID, err)
	}
	return nil
}

func (d *Daemon) ackIssueSessionInitial(ctx context.Context, taskID string, transport issueSessionTransport) error {
	if transport.PinReady != nil {
		select {
		case <-transport.PinReady:
		default:
			return errors.New("provider session identity was not durably saved; initial input remains unconfirmed")
		}
	}
	start := 0
	if transport.AckedInitial != nil {
		start = int(transport.AckedInitial.Load())
	}
	for i := start; i < len(transport.Initial); i++ {
		if err := d.ackIssueSessionEvent(ctx, taskID, transport.Initial[i], true, ""); err != nil {
			return err
		}
		if transport.AckedInitial != nil {
			transport.AckedInitial.Store(int32(i + 1))
		}
	}
	return nil
}

func formatIssueSessionEvent(event IssueSessionEvent) string {
	authority := "This is a human-authored issue event."
	if event.ActorType != "member" {
		authority = "This event was authored by an agent or system. It does not carry a human's authority."
	}
	payload := string(event.Payload)
	return fmt.Sprintf("[ISSUE SESSION EVENT %d]\nEvent ID: %s\nKind: %s\nActor: %s %s\n%s\nProcess events in sequence. Preserve the current objective unless a human explicitly changes it. Publish comments, replies, and issue edits only through explicit Multica tools when useful. Your final output is retained in the run transcript.\n\nEvent payload:\n%s", event.Sequence, event.ID, event.Kind, event.ActorType, event.ActorID, authority, payload)
}

func issueSessionPrompt(task Task, prompt string) string {
	if task.IssueAgentSessionID == "" {
		return prompt
	}
	var b strings.Builder
	fmt.Fprintf(&b, "You are continuing the persistent issue session %s. This session belongs exclusively to issue %s and agent %s. The ordered events below are your input; further events may arrive while you work. Do not create a separate conversation for a comment.\n\n%s", task.IssueAgentSessionID, task.IssueID, task.AgentID, prompt)
	for _, event := range task.IssueSessionEvents {
		b.WriteString("\n\n")
		b.WriteString(formatIssueSessionEvent(event))
	}
	return b.String()
}

// runIssueSessionLoop acknowledges the initial prompt only after the provider
// confirms a live turn, then drains one safe-boundary injection at a time. An
// uncertain acknowledgement stops the turn; it must never silently resend input.
func (d *Daemon) runIssueSessionLoop(ctx context.Context, session *agent.Session, taskID string, transport issueSessionTransport, wakeup <-chan struct{}, log *slog.Logger) error {
	if session == nil || session.Supplement == nil || session.SupplementReady == nil {
		return errors.New("issue session provider does not support live event delivery")
	}
	if transport.PinReady != nil {
		select {
		case <-transport.PinReady:
		case <-ctx.Done():
			return nil
		}
	}
	for !session.SupplementReady() {
		if !waitTaskSupplement(ctx, wakeup, d.effectiveTaskSupplementReadyInterval()) {
			return nil
		}
	}
	if err := d.ackIssueSessionInitial(ctx, taskID, transport); err != nil {
		return err
	}
	for ctx.Err() == nil {
		if !session.SupplementReady() {
			if !waitTaskSupplement(ctx, wakeup, d.effectiveTaskSupplementReadyInterval()) {
				return nil
			}
			continue
		}
		claimCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		event, err := d.client.ClaimIssueSessionEvent(claimCtx, taskID)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("issue session inbox unavailable: %w", err)
		}
		if event == nil {
			if !waitTaskSupplement(ctx, wakeup, d.effectiveTaskSupplementPollInterval()) {
				return nil
			}
			continue
		}
		injectErr := session.Supplement(ctx, formatIssueSessionEvent(*event))
		if err := d.ackIssueSessionEvent(ctx, taskID, *event, injectErr == nil, taskSupplementFailureReason(ctx, injectErr)); err != nil {
			return err
		}
		if injectErr != nil {
			log.Warn("issue session event delivery failed", "event_id", event.ID, "error", injectErr)
			return fmt.Errorf("issue session event %s delivery failed: %w", event.ID, injectErr)
		}
	}
	return nil
}

// The env root is fixed by managed provenance; local worktrees may use a
// subdirectory, so filepath.Dir(workdir) is not an environment identity.
func issueSessionEnvRoot(workspacesRoot, workDir string) string {
	canonicalRoot, err := util.ResolveSymlinks(workspacesRoot)
	if err != nil {
		return ""
	}
	rel, err := filepath.Rel(canonicalRoot, workDir)
	if err != nil || !filepath.IsLocal(rel) {
		return ""
	}
	parts := strings.Split(rel, string(filepath.Separator))
	if len(parts) < 3 {
		return ""
	}
	return filepath.Join(canonicalRoot, parts[0], parts[1])
}

func priorTaskEnvRoot(task Task, workspacesRoot, workDir string) string {
	if task.IssueAgentSessionID != "" {
		return issueSessionEnvRoot(workspacesRoot, workDir)
	}
	return filepath.Dir(workDir)
}

func issueSessionWorkdirOwned(task Task, workspacesRoot, workDir string) bool {
	root := issueSessionEnvRoot(workspacesRoot, workDir)
	if root == "" {
		return false
	}
	prov, err := execenv.ReadManagedEnvProvenance(root)
	if err != nil {
		return false
	}
	provenanceWorkDir, err := util.ResolveSymlinks(prov.WorkDir)
	return err == nil && prov.ManagedBy == execenv.ManagedEnvProvenanceManagedBy && prov.WorkspaceID == task.WorkspaceID && prov.AgentID == task.AgentID && prov.IssueID == task.IssueID && prov.IssueAgentSessionID == task.IssueAgentSessionID && filepath.Clean(provenanceWorkDir) == filepath.Clean(workDir)
}

const issueSessionStoreMarker = ".multica-issue-session"

func pinIssueSessionStore(store, sessionID string) error {
	if err := os.MkdirAll(store, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(store, issueSessionStoreMarker), []byte(sessionID), 0o600)
}

func persistentIssueSessionEnv(root string) bool {
	prov, err := execenv.ReadManagedEnvProvenance(root)
	return err == nil && prov.ManagedBy == execenv.ManagedEnvProvenanceManagedBy && prov.IssueAgentSessionID != ""
}
