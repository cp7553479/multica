package service

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

// drainTick runs one pass over the workspace with room to finish: these tests
// pin which rules a pass reaches, not how fast a loaded CI machine runs them.
func drainTick(t *testing.T, s *IssueWakeupService, ws pgtype.UUID) {
	t.Helper()
	if err := s.tickWith(context.Background(), []pgtype.UUID{ws}, tickLimits{budget: 5 * time.Minute, inputBudget: 5 * time.Minute}); err != nil {
		t.Fatal(err)
	}
}

// idleConditionRules adds n enabled condition rules whose condition never
// holds, all due and ordered ahead of everything else, the way a busy
// production scan looks to a freshly hinted rule. They are spread over issues
// to stay under the per-issue limit and carry the instruction "never".
func idleConditionRules(t *testing.T, f principalFixture, s *IssueWakeupService, agent string, n int) {
	t.Helper()
	const perIssue = 30
	for made := 0; made < n; made += perIssue {
		idleIssue := parseTestUUID(t, f.Issue(t, "Idle conditions"))
		f.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", idleIssue)
		f.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", idleIssue)
		f.Cleanup(t, "DELETE FROM activity_log WHERE issue_id=$1", idleIssue)
		seed := wakeCreate(t, f, s, idleIssue, WakeupInput{AgentID: agent, Kind: "event", Instruction: "never",
			Condition: condition(t, map[string]any{"type": "issue_field", "field": "status", "value": "blocked"})})
		cloneWakeup(t, f, seed.ID, min(perIssue, n-made)-1, false)
	}
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second', updated_at=now()-interval '1 hour' WHERE workspace_id=$1 AND instruction='never'", f.WorkspaceID)
}

// cloneWakeup copies a rule n times, optionally as turned-off rules.
func cloneWakeup(t *testing.T, f principalFixture, id pgtype.UUID, n int, off bool) {
	t.Helper()
	if n <= 0 {
		return
	}
	f.Exec(t, `INSERT INTO issue_wakeup
		SELECT (jsonb_populate_record(w, jsonb_build_object('id', gen_random_uuid())
			|| CASE WHEN $3 THEN jsonb_build_object('enabled', false, 'disabled_at', now()) ELSE '{}'::jsonb END)).*
		FROM issue_wakeup w, generate_series(1, $2::int) WHERE w.id=$1`, id, n, off)
}

// MUL-7933 / #9139: a status change behind more due condition checks than one
// batch used to wait several scheduler passes (minutes). The hinted rule must
// fire in the first pass, exactly once, after the coder's run has already
// ended, and the due checks must all still be visited in that pass.
func TestWakeupTickFiresStatusHintBehindManyDueChecks(t *testing.T) {
	f, s, issue, reviewer := conditionFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	idleConditionRules(t, f, s, reviewer, 2*wakeupTickBatch+30)

	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: reviewer, Kind: "event", Instruction: "Review it",
		Condition: condition(t, map[string]any{"type": "issue_field", "field": "status", "value": "in_review"})})
	// Not due by time and last in scan order: only its hint can reach it.
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()+interval '1 hour', updated_at=now() WHERE id=$1", w.ID)

	// The coder sets in_review from its run, and the run ends before any pass.
	var owner string
	f.QueryRow(t, "SELECT owner_id::text FROM agent WHERE id=$1", reviewer).Scan(&owner)
	coder := f.privateAgentOwnedBy(t, owner, "coder")
	run := f.Task(t, coder, testutil.Cols{"issue_id": issue, "status": "running", "started_at": testutil.Raw("now()"),
		"runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + coder + "')")})
	if _, err := f.q.UpdateIssueStatus(ctx, db.UpdateIssueStatusParams{ID: issue, Status: "in_review", WorkspaceID: ws, SourceTaskID: parseTestUUID(t, run)}); err != nil {
		t.Fatal(err)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt WHERE wakeup_id=$1 AND processed_at IS NULL AND event_type='issue.status_changed'", w.ID); n != 1 {
		t.Fatalf("pending status hints = %d, want 1", n)
	}
	f.Exec(t, "UPDATE agent_task_queue SET status='completed', completed_at=now() WHERE id=$1", run)

	drainTick(t, s, ws)
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 {
		t.Fatalf("reviewer inputs after one pass = %d, want 1", len(events))
	}
	// Every due check was visited in the same pass, across several batches.
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup WHERE workspace_id=$1 AND instruction='never' AND next_fire_at<=now()", f.WorkspaceID); n != 0 {
		t.Fatalf("%d due condition checks were left for a later pass", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_agent_event e JOIN issue_wakeup w ON e.payload->>'wakeup_id'=w.id::text WHERE e.kind='wakeup.fired' AND w.workspace_id=$1 AND w.instruction='never'", f.WorkspaceID); n != 0 {
		t.Fatalf("unmet conditions transferred %d inputs", n)
	}
	// Later passes do not fire the once rule again.
	f.Exec(t, "UPDATE issue_wakeup SET next_fire_at=now()-interval '1 second' WHERE id=$1 AND next_fire_at IS NOT NULL", w.ID)
	drainTick(t, s, ws)
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 {
		t.Fatalf("reviewer inputs after two passes = %d, want 1", len(events))
	}
}

// More rules with pending inputs than a whole batch and a full batch of due
// checks both drain in one pass: the input lane takes at most its share of
// each batch, the due lane the rest, and each rule is visited once.
func TestWakeupTickDrainsBothLanesInOnePass(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	idleConditionRules(t, f, s, agent, wakeupTickBatch)

	// More turned-off rules with pending inputs than a whole batch: dispatch
	// only drops their inputs. The lane still lists rules that are not
	// enabled, as it must for a stopped once rule's captured inputs.
	seed := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "noisy"})
	f.Exec(t, "UPDATE issue_wakeup SET enabled=false, disabled_at=now() WHERE id=$1", seed.ID)
	cloneWakeup(t, f, seed.ID, wakeupTickBatch+20-1, true)
	f.Exec(t, `INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload)
		SELECT gen_random_uuid(),id,revision,'k','comment.created','{}' FROM issue_wakeup WHERE issue_id=$1`, issue)

	inputs, err := f.q.ListInputWakeups(ctx, db.ListInputWakeupsParams{WorkspaceIds: []pgtype.UUID{ws}, SkipIds: []pgtype.UUID{}, RowLimit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs) != wakeupTickBatch+20 {
		t.Fatalf("input lane lists %d rules, want %d (disabled rules with inputs included)", len(inputs), wakeupTickBatch+20)
	}
	drainTick(t, s, ws)
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup_receipt r JOIN issue_wakeup w ON w.id=r.wakeup_id WHERE w.issue_id=$1 AND r.processed_at IS NULL", issue); n != 0 {
		t.Fatalf("%d inputs left pending after one pass", n)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup WHERE workspace_id=$1 AND instruction='never' AND next_fire_at<=now()", f.WorkspaceID); n != 0 {
		t.Fatalf("%d due checks were crowded out", n)
	}
}

// Slow inputs must not starve due checks (PR #9202 review): dispatches
// alternate between the lanes, and while due work remains the input lane
// stops at its share of the pass budget. Checked by dispatch order, so a slow
// machine cannot change the outcome.
func TestWakeupTickSlowInputsLeaveTimeForDueChecks(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	ws := parseTestUUID(t, f.WorkspaceID)
	const dueRules, slowInputs = 40, 10
	idleConditionRules(t, f, s, agent, dueRules)
	seed := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "noisy"})
	f.Exec(t, "UPDATE issue_wakeup SET enabled=false, disabled_at=now() WHERE id=$1", seed.ID)
	cloneWakeup(t, f, seed.ID, slowInputs-1, true)
	f.Exec(t, `INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload)
		SELECT gen_random_uuid(),id,revision,'k','comment.created','{}' FROM issue_wakeup WHERE issue_id=$1`, issue)

	// Each input takes at least 300ms; the input lane gets 500ms while due
	// checks remain, so at most two inputs may run before the last due check.
	var order []string
	err := s.tickWith(ctx, []pgtype.UUID{ws}, tickLimits{budget: 5 * time.Minute, inputBudget: 500 * time.Millisecond,
		beforeDispatch: func(w db.IssueWakeup) {
			order = append(order, w.Instruction)
			if w.Instruction == "noisy" {
				time.Sleep(300 * time.Millisecond)
			}
		}})
	if err != nil {
		t.Fatal(err)
	}
	lastDue, inputsBefore, inputs := -1, 0, 0
	for i, kind := range order {
		if kind == "never" {
			lastDue = i
		}
	}
	for i, kind := range order {
		if kind == "noisy" {
			inputs++
			if i < lastDue {
				inputsBefore++
			}
		}
	}
	if inputsBefore < 1 || inputsBefore > 2 {
		t.Fatalf("%d slow inputs ran before the last due check, want 1-2 (the input lane's share): %v", inputsBefore, order)
	}
	if order[0] != "noisy" || order[1] != "never" {
		t.Fatalf("lanes did not alternate: %v", order[:2])
	}
	// Once the due lane drained, the inputs used the rest of the pass.
	if inputs != slowInputs || len(order) != dueRules+slowInputs {
		t.Fatalf("dispatched %d inputs of %d and %d rules of %d", inputs, slowInputs, len(order), dueRules+slowInputs)
	}
	if n := f.Count(t, "SELECT count(*) FROM issue_wakeup WHERE workspace_id=$1 AND instruction='never' AND next_fire_at<=now()", f.WorkspaceID); n != 0 {
		t.Fatalf("%d due checks were left behind", n)
	}
}

// A queued or claimed execution cannot reserve a rule receipt. Only an atomic
// transfer into the durable inbox removes it from the scheduler's backlog.
func TestPendingWakeupInputsRemainBacklogUntilDurableTransfer(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	ws := []pgtype.UUID{parseTestUUID(t, f.WorkspaceID)}
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "follow up"})
	f.Comment(t, issue.String(), "hello")
	f.Exec(t, "UPDATE issue_wakeup_receipt SET created_at=now()-interval '10 minutes' WHERE wakeup_id=$1", w.ID)
	read := func() pendingInputSummary {
		t.Helper()
		p, err := s.pendingInputs(ctx, ws, wakeupPendingScanPage)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	backlog := func(why string) {
		t.Helper()
		p := read()
		if age, ok := p.age(time.Now()); !ok || age < 9*time.Minute || p.Held != 0 {
			t.Fatalf("%s: age=%v ok=%v held=%d, want ~10m backlog", why, age, ok, p.Held)
		}
	}
	backlog("unhandled input")
	run := f.Task(t, agent, testutil.Cols{"issue_id": issue.String(), "originator_user_id": f.UserID, "accountable_user_id": f.UserID,
		"runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agent + "')")})
	backlog("queued legacy execution does not reserve the receipt")
	for _, status := range []string{"dispatched", "cancelled", "failed", "running"} {
		f.Exec(t, "UPDATE agent_task_queue SET status=$2,started_at=CASE WHEN $2='running' THEN now() ELSE NULL END WHERE id=$1", run, status)
		f.Exec(t, "UPDATE issue_wakeup_receipt SET task_id=$2 WHERE wakeup_id=$1", w.ID, run)
		backlog("historical receipt task reference in state " + status)
	}
	// Drain the old execution before starting the new architecture's consumer.
	f.Exec(t, "UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1", run)
	wakeDispatch(t, s, w)
	if p := read(); p.OldestReady.Valid || p.Held != 0 {
		t.Fatalf("transferred receipt remains rule backlog: %+v", p)
	}
	if events := wakeupInbox(t, f, w.ID); len(events) != 1 || events[0].Status != "pending" || events[0].DeliveredAt.Valid {
		t.Fatalf("receipt transfer must retain the undelivered input: %+v", events)
	}
}

// Platform receipts remain backlog regardless of historical executions.
// Their target and principal are revalidated when transferred into a session.
func TestPendingWakeupInputsSystemRuleIsNotHeldByLegacyExecutions(t *testing.T) {
	f, s, issue, target := conditionFixture(t)
	ctx := context.Background()
	ws := []pgtype.UUID{parseTestUUID(t, f.WorkspaceID)}
	var owner string
	f.QueryRow(t, "SELECT owner_id::text FROM agent WHERE id=$1", target).Scan(&owner)
	other := f.privateAgentOwnedBy(t, owner, "other")
	f.Exec(t, "UPDATE issue SET assignee_type='agent', assignee_id=$2 WHERE id=$1", issue, target)
	rule, err := f.q.CreateSystemWakeup(ctx, db.CreateSystemWakeupParams{ID: dbid.NewV7(), WorkspaceID: ws[0], IssueID: issue,
		EventTypes: []string{"issue.status_changed"}, Condition: childDoneCondition, Enabled: true, SystemRule: systemRuleText(SystemRuleChildDone)})
	if err != nil {
		t.Fatal(err)
	}
	f.Exec(t, `INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload,created_at)
		VALUES(gen_random_uuid(),$1,$2,'k','condition.met','{}',now()-interval '10 minutes')`, rule.ID, rule.Revision)
	read := func() pendingInputSummary {
		t.Helper()
		p, err := s.pendingInputs(ctx, ws, wakeupPendingScanPage)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	queued := func(agent, runAs string) string {
		t.Helper()
		return f.Task(t, agent, testutil.Cols{"issue_id": issue.String(), "originator_user_id": runAs, "accountable_user_id": runAs,
			"runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agent + "')")})
	}
	issueRow, err := f.q.GetIssue(ctx, issue)
	if err != nil {
		t.Fatal(err)
	}
	targetAgent, err := f.q.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: parseTestUUID(t, target), WorkspaceID: ws[0]})
	if err != nil {
		t.Fatal(err)
	}
	runAs, err := s.childDoneRunAs(ctx, issueRow, targetAgent)
	if err != nil || !runAs.UserID.Valid {
		t.Fatalf("run-as for the target: %+v %v", runAs, err)
	}
	// Another agent's queued run cannot take a platform rule's input.
	queued(other, runAs.UserID.String())
	if p := read(); !p.OldestReady.Valid || p.Held != 0 {
		t.Fatalf("input with only another agent's run queued: %+v, want backlog", p)
	}
	// Nor can the target's run as somebody else.
	stranger := queued(target, f.member(t, "someone-else"))
	if p := read(); !p.OldestReady.Valid || p.Held != 0 {
		t.Fatalf("input with the target's run as another person: %+v, want backlog", p)
	}
	f.Exec(t, "UPDATE agent_task_queue SET status='cancelled', completed_at=now() WHERE id=$1", stranger)
	queued(target, runAs.UserID.String())
	if p := read(); !p.OldestReady.Valid || p.Held != 0 {
		t.Fatalf("input with the target's run queued: %+v, want backlog", p)
	}
}

// The scan must page across every untransferred receipt group. After the
// oldest page is consumed, the younger input still determines backlog age.
func TestPendingWakeupInputsScansEveryPageOfUntransferredGroups(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	ctx := context.Background()
	ws := []pgtype.UUID{parseTestUUID(t, f.WorkspaceID)}
	// A page of turned-off rules whose inputs must still be discarded by dispatch.
	seed := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "untransferred"})
	f.Exec(t, "UPDATE issue_wakeup SET enabled=false, disabled_at=now() WHERE id=$1", seed.ID)
	cloneWakeup(t, f, seed.ID, wakeupPendingScanPage-1, true)
	f.Exec(t, `INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload,created_at)
		SELECT gen_random_uuid(),id,revision,'k','comment.created','{}',now()-interval '20 minutes' FROM issue_wakeup WHERE issue_id=$1`, issue)
	f.Task(t, agent, testutil.Cols{"issue_id": issue.String(), "originator_user_id": f.UserID, "accountable_user_id": f.UserID,
		"runtime_id": testutil.Raw("(SELECT runtime_id FROM agent WHERE id='" + agent + "')")})
	// One ready input, younger than the full first page, on another issue.
	other := parseTestUUID(t, f.Issue(t, "Ready input"))
	f.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", other)
	f.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", other)
	f.Cleanup(t, "DELETE FROM activity_log WHERE issue_id=$1", other)
	ready := wakeCreate(t, f, s, other, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "ready"})
	f.Exec(t, `INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload,created_at)
		VALUES(gen_random_uuid(),$1,$2,'k','comment.created','{}',now()-interval '10 minutes')`, ready.ID, ready.Revision)

	for _, page := range []int{wakeupPendingScanPage, 7} {
		p, err := s.pendingInputs(ctx, ws, page)
		if err != nil {
			t.Fatal(err)
		}
		if age, ok := p.age(time.Now()); !ok || age < 19*time.Minute || age > 25*time.Minute || p.Held != 0 {
			t.Fatalf("page %d: oldest untransferred input: age=%v ok=%v held=%d", page, age, ok, p.Held)
		}
	}
	f.Exec(t, "UPDATE issue_wakeup_receipt SET processed_at=now() WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", issue)
	for _, page := range []int{wakeupPendingScanPage, 7} {
		p, err := s.pendingInputs(ctx, ws, page)
		if err != nil {
			t.Fatal(err)
		}
		if age, ok := p.age(time.Now()); !ok || age < 9*time.Minute || age > 15*time.Minute || p.Held != 0 {
			t.Fatalf("page %d: younger pending group after transfer: age=%v ok=%v held=%d", page, age, ok, p.Held)
		}
	}
}

// A scan that cannot finish says so instead of returning a silent summary.
func TestPendingWakeupInputsReportsAnIncompleteScan(t *testing.T) {
	f, s, issue, agent := conditionFixture(t)
	w := wakeCreate(t, f, s, issue, WakeupInput{AgentID: agent, Kind: "event", Mode: "continuous", EventTypes: []string{"comment.created"}, Instruction: "follow up"})
	f.Exec(t, `INSERT INTO issue_wakeup_receipt(id,wakeup_id,revision,event_key,event_type,payload)
		VALUES(gen_random_uuid(),$1,$2,'k','comment.created','{}')`, w.ID, w.Revision)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.pendingInputs(ctx, []pgtype.UUID{parseTestUUID(t, f.WorkspaceID)}, wakeupPendingScanPage); err == nil {
		t.Fatal("a scan whose context ended reported a complete result")
	}
}
