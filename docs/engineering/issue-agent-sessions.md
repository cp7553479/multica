# Persistent issue agent sessions

This fork replaces comment-triggered execution routing with a persistent inbox
for the assigned agent. It is based on upstream commit
`16ce2df18c325cce1b3760704169373b4f741086` from `multica-ai/multica`.

## Ownership and identifiers

`(workspace_id, issue_id, agent_id)` identifies one logical session. Its UUID is
stable across executions. `provider_session_id` identifies the coding tool's
conversation, and each `agent_task_queue.id` identifies one execution turn. A
comment is an input event, not an execution identity.

The session stores its runtime, provider conversation, working directory and
active execution. The runtime is pinned once continuity exists. Missing local
history, a changed runtime or a rejected provider resume is a visible error;
these conditions do not start a fresh conversation automatically.

Automatic input routing follows the direct agent assignee. Mentions and reply
parents do not redirect the owner inbox. Additional agents must receive explicit
assignments, for example through assigned sub-issues. Existing squad assignment
and non-issue work remain separate features; implicit mention/comment dispatch
is removed. A comment's parent only controls where an explicitly published reply
appears in the discussion.

## Data flow

```mermaid
sequenceDiagram
    participant U as Human / task tool
    participant DB as Mutation transaction
    participant I as Session inbox
    participant S as Runtime scheduler
    participant D as Daemon
    participant A as Provider conversation
    U->>DB: Assign, comment, reply, edit or delete
    DB->>I: Capture immutable event and per-session sequence
    DB-->>U: Commit the content and event together
    S->>I: Lock owners, then session; inspect first unsettled event
    alt Session idle
        S->>D: One execution with initial ordered batch and saved pointers
        D->>A: Start or resume with initial events
    else Session busy
        S-->>D: Content-free wake hint
        D->>I: Claim next event (polling also recovers lost hints)
        D->>A: Inject at supported safe boundary
    end
    D->>I: Acknowledge provider acceptance
    A->>DB: Explicit comment / reply / issue-edit tool
    Note over DB,I: Own writes are audited as ignored inputs
    D->>S: Complete execution and persist continuity pointers
    S->>I: Check remaining pending events under the session lock
```

Migration 618 captures assignment, comment creation/reply, comment content edits
and deletion, and issue title/description/status changes in the mutation's own
PostgreSQL transaction. The event key is `kind:source_uuid:source_revision`.
The session row serializes sequence allocation. Rollback also rolls back the
event and sequence increment. Comment bodies are immutable event snapshots;
subsequent edits are new facts, not replacement execution plans.

The event states are `pending`, `delivering`, `delivered`, `failed`, and
`ignored`. A failed or unconfirmed head prevents later events from overtaking
it. The UI retains the true first unsettled event as well as recent history,
so a busy session cannot hide the event that requires recovery.

## Execution and parallelism

Only one execution consumes a logical session at a time. Multiple comments can
share the same queued or active execution. After successful completion, pending
events wake another turn of the same conversation. Runtime polling repairs a
lost notification or a crash between a committed input and its wake hint.

An execution carries one invocation principal. Inputs from another principal
remain pending until the next turn of the same logical session; they cannot
borrow the current turn's human delegation rights for agent-to-agent calls.

Different issues can execute concurrently with the same agent configuration.
Existing agent, runtime and daemon capacity limits remain enforced. To reproduce
the A1/B1/A2 acceptance case, both the agent limit and available runtime/daemon
capacity must be at least two. Enabling a worktree alone does not increase these
limits. This change does not establish the cause of any previous user's queue.

Issue sessions use isolated persistent working directories. A configured local
directory is used as the source for an isolated retained worktree, rather than
as a shared writable checkout. The environment records the session's provenance;
the source must be a Git repository or a directory inside one. A non-Git local
directory fails preparation explicitly instead of reverting to shared writes.
Resume refuses a different session's directory. Environments and supported
provider stores are pinned against ordinary TTL garbage collection. Deleting an
issue deletes its inbox and logical session, but local retained worktrees and
provider stores require deliberate operator cleanup after the daemon has stopped
using them; this change does not silently remove unfinished local work.

Media-gated channel issue creation retains its deadline in the session instead
of manufacturing a deferred comment task. Media readiness releases that session
inbox. Backlog and triage still prevent automatic execution.

Scheduled, event, manual and `child_done` wakeup rules also publish
`wakeup.fired` inputs into the same issue/agent inbox. Each input retains the
rule's instruction, revision, authorizing principal, receipt references and
causal chain. An explicitly configured auxiliary agent retains its own
issue/agent session; a rule never silently changes its target to the assignee.
Only the current active squad leader receives squad authority. Auxiliary
agents do not inherit the assignee's workflow permission.

Transferring rule receipts and appending the session event commit atomically.
A receipt's `processed_at` means the input was transferred to the durable inbox,
not accepted by a provider. Provider delivery is recorded on the event. Rules
retain expiry, self-write suppression, firing caps and loop/rate guards; their
history follows the execution that actually claims each event. Disabling,
updating or deleting a rule withdraws pending/failed inputs while preserving
already claimed inputs and other messages in the same session.

## Delivery guarantees and failures

Committed mutation capture is durable and event ingestion is idempotent for a
source version. Delivery acknowledgements are durable and idempotent for the
bound execution and event. `delivered` means the provider accepted the input; it
does not mean the requested work succeeded or that an external write occurred.

A process can fail after provider acceptance but before its acknowledgement is
recorded. This is `delivery_unconfirmed`, not a promise that the input was never
seen. It is blocked for explicit recovery rather than automatically replayed.
An operator retry can therefore repeat an instruction. External issue/tool
writes do not have an exactly-once guarantee from this transport.

Provider identity and the retained workdir are saved together before initial
input can be acknowledged. Supporting adapters report the real provider thread
as soon as their native protocol exposes it. A failed durable pin stops input
processing; final/late callbacks cannot replace an already saved identity.
Switching back to the original runtime permits an explicit retry, which
revalidates that original conversation and storage. If an execution started but
never exposed a recoverable identity, recovery remains visibly unavailable;
reassignment or another event cannot create a fresh replacement conversation.

The protocols have different first-identity boundaries. Cursor's native hook is
post-tool, so a hard crash before the CLI exposes its first conversation ID
cannot be guaranteed recoverable. Fake-provider subprocess tests cover a daemon
SIGKILL after durable pin and initial acknowledgement, followed by a new process
resuming the same ID and unfinished workdir. They do not establish hard-crash
behavior for authenticated real providers; those tests were not run.

The daemon advertises `issue-session-events-v1` and the existing safe input
capability. Unsupported providers, old daemons, missing conversation data and
provider rejection fail visibly. They never fall back to one task per comment.
No old/new route mode switch is provided. Installed clients' retired steer and
suppress parameters are accepted and ignored for agent-owned issues. Assignment
and promotion dialogs no longer offer a quiet-run option for these sessions;
existing explicit squad controls remain separate. Old supplement and issue-run retry
entry points return an actionable conflict instead of creating a parallel plan.

## Reassignment, stopping and authorization

Reassignment detaches the old agent's assignment session and ignores its
unconsumed assignment/comment/issue inputs. Explicit wakeup inputs retain their
configured target and are rechecked against the current rule and invocation
rights. An already running old execution may finish, but cannot consume the new
owner's events. The new agent has its own session, directory and provider
conversation. Returning to an earlier agent retains that agent's continuity
pointer and captures a new assignment snapshot; stale detached inputs are not
silently replayed.

Cancellation stops the execution and blocks its session for explicit recovery.
Unconfirmed inputs remain visible. Archiving the agent blocks further wakeups.
Deleting an issue, agent or workspace removes the corresponding database inbox
in application-owned trigger cleanup, without new foreign keys or cascades.

Every delivered event is checked against the existing agent invocation policy.
Unauthorized input is visibly refused as an ignored event, so a collaborator
without invocation rights cannot block later authorized owner messages.
Source actor and task lineage come from authenticated transaction-local values,
not message text. A terminal task cannot continue borrowing its former human's
private-agent invocation rights. Ordinary agent self-writes are recorded as
ignored, preventing a comment/edit feedback loop. Explicit assignment to another
issue and validated workflow handoffs remain actionable. A workflow event carries
the new brief and handoff note; only an acknowledged event that still matches the
current issue stage advances the execution's workflow permission. Existing scoped CLI tools continue to
enforce workspace, issue, parent-comment and field permissions.

Plugin `comments:write` and `issues:write` scopes do not grant invocation.
Plugin writes are captured as ignored audit events with
`plugin_write_requires_invoke`; they neither spend agent capacity nor block the
owner inbox. Ordinary plugin delivery hooks are not used as a message bus.

## Explicit output and APIs

The execution transcript retains final output. Completion and failure do not
automatically publish that output under a triggering comment. The agent chooses
to call `multica issue comment add`, supplies `--parent` only for an intentional
reply, or uses `multica issue update` for an allowed issue edit.

New authenticated endpoints:

- `GET /api/issues/{id}/agent-sessions`: authorized session and event receipts.
- `POST /api/issues/{id}/agent-sessions/{sessionId}/events/{eventId}/retry`:
  explicit retry of the same event, subject to current invocation permission.
- `POST /api/daemon/tasks/{taskId}/issue-events/claim`: next ordered input.
- `POST /api/daemon/tasks/{taskId}/issue-events/{eventId}/ack`: bounded stable
  receipt or failure code for that execution.

The claim response adds `issue_agent_session_id` and `issue_session_events`.
Model/provider diagnostics and credentials are not stored in event failure
codes. Session receipts are workspace-scoped and private-agent access is checked
before exposing their history.

## Upgrade and validation

This is a breaking routing change. Stop the old daemon, drain or deliberately
cancel its old queued/running issue tasks, back up the database, apply migrations
614–618 with the repository migrator, and start the matching server and daemon.
Do not run an old daemon alongside the new one. Existing assigned issues create
their logical session lazily on the next input; new assignments capture it in the
assignment transaction. Historical execution records remain inspectable.

No production deployment or merge is part of this work. The draft review should
include real database regressions for ordering, duplicate capture, rollback,
A/B parallelism, completion boundaries, restart recovery, self-writes,
reassignment, invocation permissions, plugin scopes and unknown delivery; fake
provider tests cover safe input boundaries and strict resume. Do not run
authenticated real-agent smoke tests without explicit authorization.

A stable logical session does not control a model provider's prompt-cache
expiration or guarantee a permanent cache hit.
