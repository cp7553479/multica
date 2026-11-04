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

## Simplification and cutover review

The completed routing change was reviewed for unused entry points, duplicated
state and reuse of existing Multica machinery. This pass removes code with
verified call-graph or constant-value evidence:

| Change | Reason |
| --- | --- |
| Remove test-only `PublishIssueAgentEvent`, its unused execution result and an immediately invoked closure | Production already captures mutations transactionally or appends a rule event inside the existing transaction. The deduplication test now exercises that actual append operation. |
| Remove unused cleanup/role queries, Handoff wrapper, mention-denial notice, parent-notice transaction and Squad self-trigger helper | All had zero production callers; actual deletion capture, runtime refusal notices, Squad dispatch and historical reads remain. |
| Remove Held/reserved/claimed/waiting input classification and its pagination key | These values were constant false after rules moved to durable events. The existing oldest/rule-ID pagination and time budget are unchanged. |
| Remove the write-only Handler notifier field and transport SessionID | Live notification uses the existing TaskService notifier; identity validation uses the task/provider/session binding. |
| Narrow the Autopilot bind result and combine duplicate missing-ID checks | Both bind callers discarded the returned session; identical terminal predicates now have one branch. |
| Use native Mutation error state and remove unused preview result | TanStack Query already resets errors between attempts. No consumer read preview.isCurrent; the effective query-key/debounce/placeholder safeguards remain. |

The pass also records two explicit correctness boundaries rather than hiding
them under a refactor:

- A database predating inbox capture may have a committed Autopilot issue/run
  without a session or initial assignment. Targeted missing/empty-session cases
  now produce the migration-required diagnostic, leave all data untouched and
  create no fallback task. The architecture document provides a read-only
  preflight plus a guarded, per-reviewed-run disposition example. Only SELECT
  and EXPLAIN were executed to validate those SQL examples; no operator UPDATE
  or production migration was performed.
- The prior Autopilot-origin guard also rejected an authorized human's first
  reassignment to a different agent. A failing regression reproduced this;
  normal member/delegation input policy now applies, and the unauthorized-member
  refusal remains tested. The original unbound Autopilot assignment still fails
  closed.

Verification in this pass:

- Complete service/handler/server suites: passed, 8.942 / 55.037 / 4.032 s.
- Session, Autopilot and lock-order race regressions: passed, 2.306 s.
- Existing wakeup/pagination and handler permission/receipt cases: 17 groups passed.
- Fake daemon delivery, initial ACK, pin and SIGKILL cases: passed, 1.930 s.
- Five frontend suites: 95 tests passed, including failed retry to pending/success
  state transitions; views types and changed-file ESLint passed.
- Regenerated sqlc, `go vet ./...` and `git diff --check`: passed.

Necessary complexity is retained: logical sessions are distinct from provider
threads and execution records; capture/ACK and identity pin protect different
transaction/protocol boundaries; current invocation rights are checked at wake
and delivery; and original worktree provenance, resource caps and native
provider input APIs remain in use. Existing scheduler, lifecycle, attribution,
query/schema and UI component mechanisms are reused. This review adds no new
framework, queue service, provider wrapper or generic recovery subsystem.

## Delivery avatar follow-up

The delivery UI reads durable ACK records rather than task creation, completion
or the bounded session history. It shows only a 16 px receiving-agent avatar at
the bottom left of the current issue description or comment, with an accessible
"Delivered to" tooltip. Pending and failed state remain in the existing session
panel. No placeholder turn or automatic comment is added.

Real database regressions cover pending-to-delivered, duplicate ACK, retry,
handler recreation, exact comment versions, historical records beyond 199,
multiple same-version receivers, private-agent VIEW permissions, cross-issue and
workspace isolation, global latest issue snapshots and request bounds. ACK hint
regressions observe committed rows from another connection, suppress rollback
and duplicate notifications, and cover retry before a subsequent wake failure.

| Scope | Result |
| --- | --- |
| Complete service, handler, migration and HTTP server suites | Passed, 11.618 / 54.148 / 0.568 / 3.935 s |
| Delivery projection handler regressions | Eight groups passed, 0.768 s |
| ACK/retry publication with race detector | Four groups passed, 1.428 s |
| Complete core suite | 209 files, 2713 tests passed |
| Complete shared views suite | 565 files, 7218 tests passed |
| Core/views, web and both desktop typechecks; changed-file ESLint; Go vet | Passed |
| Concurrent index migration registration checks | Passed, 0.533 s |
| Migration 619 down/up on separate test database | Valid and ready index; EXPLAIN uses the source-version index |

Core and view regressions also cover cancelled stale reads, five-second polling,
reconnect, batched timelines beyond 2000 IDs, per-agent deduplication, canonical
comment revisions and hiding old receipts across components during optimistic
edits. Issue draft and overlapping-save checks guard the same current-content
boundary. The existing update wrappers use each native mutation Promise for
completion callbacks. True overlapping saves test forward and reverse responses,
including an older conflict after the newer save succeeded; current canonical
cache state determines avatar eligibility. The final five focused suites passed
231 tests; independent review also passed eight component, four native observer
and four canonical-comment checks. The general comment revision also changes for some non-body edits;
an earlier avatar may conservatively disappear in that case. This change does
not invent a second content-version system.

Chrome checked the actual delivery-avatar component and core polling hook with
synthetic API records. It verified no avatar before ACK or for failure, arrival
after ACK, duplicate/multiple receivers, reload persistence, exact 16 px size,
bottom-left alignment, keyboard tooltip and 390 px Chinese dark layout. No page
errors or horizontal overflow occurred. The surrounding page is a component
fixture, not a complete live issue screen or a real-provider round trip.

- [Before ACK](../assets/issue-agent-sessions/delivery-avatars-pending.png)
- [After ACK, desktop light](../assets/issue-agent-sessions/delivery-avatars-desktop-light.png)
- [After ACK, 390px Chinese dark](../assets/issue-agent-sessions/delivery-avatars-mobile-dark-zh.png)

## Delivery avatar simplification review

A separate post-implementation review made three behavior-preserving changes:
keep the receipt schema and refresh helper private to their files; replace a
ternary whose two branches both equal the submitted description; and remove a
blur check already implied by the existing in-flight save flag. The only
non-null pending-description assignment occurs while that flag is true; reset,
success and failure clear the pending value synchronously. No editor state,
ACK/source-version fence, query, transport, permission gate or avatar component
was replaced.

Existing targeted regressions passed after the cleanup: four core files / 122
tests, including schema, query refresh, canonical comment mutation and realtime
scope; two views files / 124 tests, including description debounce/queued saves,
true overlapping responses and conflicts. Core/views typechecks, changed-file
ESLint and `git diff --check` passed. No new tests or generic abstraction were
added. Backend and browser components were unchanged, so their preceding checks
were not repeated. Full-application E2E, mobile UI and real-provider checks
remain unrun.

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
