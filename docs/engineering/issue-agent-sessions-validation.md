# Issue session validation

Validated on 2026-11-04 from upstream
`16ce2df18c325cce1b3760704169373b4f741086`, using macOS arm64,
Go 1.26.9 and pnpm 10.28.2.

Database checks used a dedicated temporary PostgreSQL cluster on localhost,
with separate databases for behavioral tests and migration round trips. No
production database, installed agent account, or default Multica daemon was used.
Database test processes were serialized because separate handler test processes
share a suite fixture. A sandbox-denied localhost connection that makes TestMain
skip was not counted as a successful database test.

## Checks

| Scope | Check | Result |
| --- | --- | --- |
| Backend and CLI compilation | `go test ./... -run '^$' -count=1` | Passed |
| Complete handler suite | `go test ./internal/handler -count=1` | Passed, 54.145 s |
| Complete service suite | `go test ./internal/service -count=1` | Passed, 8.805 s |
| Migration tests | `go test ./internal/migrations -count=1` | Passed, 0.539 s |
| Session concurrency | `go test -race ./internal/service` with session and rule handoff/lock regression selectors | Passed, 1.969 s |
| Complete daemon suite | `go test ./internal/daemon -count=1` | Passed, 36.486 s |
| Execution environments | `go test ./internal/daemon/execenv -count=1` and retained-worktree regressions | Passed, 38.786 s |
| Complete native provider adapter suite | `go test ./pkg/agent -count=1`, using fake CLIs/RPC | Passed, 117.695 s |
| Channel media integration | Issue-command/router tests with session readiness release | Passed |
| HTTP router integration | Complete `cmd/server` suite after routing-contract migration | Passed, 3.874 s |
| Core logic | `vitest run` in `packages/core` | 207 files, 2675 tests passed |
| Shared views | `vitest run` in `packages/views` | 563 files, 7183 tests passed |
| Types | Core, views, web and both desktop typecheck scripts | Passed |
| Lint/format/static checks | Changed core/views files, all Go packages via `go vet ./...`, Go formatting and `git diff --check` | Passed |
| Schema installation | All migrations on an empty database; migrations 614–618 down/up | Passed |

Web typechecking includes the repository's `fumadocs-mdx` generation step.
Desktop typechecking uses the package scripts' `--composite false` option.

## Independent review regressions

All five identified routing/continuity issues were addressed before publication:

| Finding | Result and evidence |
| --- | --- |
| Rules could create legacy tasks outside the session inbox | Rules now transfer durable `wakeup.fired` events; actual claim, unsupported-daemon, one-consumer, rule withdrawal and auxiliary-agent permission tests pass. Old task creation and Join dispatch were removed. |
| Initial input could be acknowledged before provider identity was saved | The daemon requires a durable identity/workdir pin before ACK. Fake native adapters and a SIGKILL/restart subprocess regression pass. |
| Switching away from and back to the original runtime prevented retry | Database and HTTP regressions verify retry uses the saved original identity and storage; unknown first identities remain visibly unavailable. |
| Quick actions lost source-task delegation | Private and member-scoped cross-issue delegation cases preserve source task, authenticated actor and human lineage. |
| A late pin after cancellation did not repair logical-session continuity | Atomic task/session pin and late-cancel recovery regressions pass; terminal callbacks cannot replace pinned identity. |

The new rule integration also has real database regressions for same-rule
self-write suppression, receipt/task lock ordering and an unstarted consumer
whose first authorizing input is withdrawn. In the last case, the next sender's
input survives and receives its own authorization in the same logical session.
Rule history, check-in and workspace counters read actual event deliveries and
count a shared execution once.

Existing daemon lifecycle and retry machinery remain the execution substrate.
Identity and acknowledgement changes cover continuity regressions introduced by
the new routing model.

## Acceptance coverage

- Capacity two: A1 and B1 can run concurrently with one agent configuration;
  A2 is delivered to A's existing session without adding a comment task.
- Duplicate capture, ordered comment/reply/edit events and transactional rollback.
- Pending input at completion, explicit recovery, unknown acknowledgement and
  refusal to silently create a fresh provider conversation.
- Provider/workdir continuity, isolated retained Git worktrees and exact ownership.
- Reassignment isolation and terminal-task/cross-principal invocation boundaries.
- Self-authored comments/edits do not loop. Explicit cross-issue assignment and
  validated workflow handoffs remain actionable.
- Old clients' routing fields cannot redirect or suppress the owner inbox.
- Plugin write scopes do not acquire invocation rights; a later human edit is
  attributed to the human rather than the original plugin.
- Final output remains in the transcript; explicit comments retain the selected
  top-level/reply location and cross-issue parent validation.
- Workflow notes and briefs are durable input; permission advances only after
  the matching event is acknowledged. A real PostgreSQL barrier test verifies
  task-before-session lock ordering at ACK/completion.
- Historical receipts, revision CAS, stale claims, token rollback and concurrent
  retry atomicity remain tested after removing the obsolete routing contracts.

## Autopilot routing follow-up

Independent review identified an additional producer: Autopilot `create_issue`
created both the captured assignment input and an ordinary queued issue task.
The latter could conflict with the session consumer's pending-thread uniqueness
constraint during runtime wake. This is a queue/claim blockage, not evidence of
two simultaneous executions on the same issue and agent.

A temporary Go overlay of the old producer reproduced the ordinary queued task.
The current regression proves schedule/manual × single-then-batch/batch-then-single
claim combinations can dispatch the Autopilot issue and another issue on the same
runtime with capacity two. Initial input, run linkage, invocation attribution,
provider ID and retained directory remain associated with the same logical session.

Real database regressions also cover departed-member refusal, human reassignment,
terminal history, unrecorded historical dispatch links, post-commit wake failure,
webhook delivery idempotency and explicit same-event retry. The original failed
Autopilot run keeps its recorded dispatch outcome when a later session recovery
executes; a comment continuation cannot become that run's dispatch.

- Complete service suite: passed, 8.648 s.
- Autopilot and issue-session race regressions: passed, 2.172 s.
- Autopilot repair regressions: five cases passed, 0.582 s.
- HTTP listener regressions: 17 cases passed, 0.805 s.
- Webhook crash-window and adjacent regressions: four cases passed, 0.768 s.
- Complete handler suite: passed, 53.751 s.
- Complete server HTTP/listener suite: passed, 4.072 s.
- `go vet ./...`, regenerated sqlc and `git diff --check`: passed.

No new migration, frontend change or generic recovery infrastructure is required
for this producer fix. Existing squad and `run_only` dispatch retain their scope.

## Browser checks and limits

The real session panel and application styles were checked with synthetic API
records at desktop width and 390px Chinese dark mode. There were no page errors
or horizontal overflow. Retrying the failed head issued one request for that
same session/event and did not create a comment or legacy run.

- [Desktop light](../assets/issue-agent-sessions/desktop-light.png)
- [390px Chinese dark](../assets/issue-agent-sessions/mobile-dark-zh.png)

A fake-provider test kills the daemon helper after durable pin and initial ACK,
then starts a new process which resumes the same provider ID and unfinished
workdir. Authenticated real-provider hard-crash E2E and real-agent smoke tests
were not run. These are declared boundaries, not an additional delivery gate.
Cursor cannot expose its first identity before its native post-tool boundary;
a crash before the provider exposes a recoverable identity fails visibly.

Full application Playwright E2E and mobile UI testing were not run. The screenshots are component fixtures, not proof of a
live provider round trip. No merge or production deployment was performed.
