package handler

import (
	"net/http"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
)

// MUL-6490 / GH #7328 — the authorization chain must survive a cross-issue hop.
//
// The invariant under test (MUL-3963): a run always acts "on behalf of" exactly
// one human U, and every A2A delegation is judged by whether U is on the target's
// allow-list — never by the agent principal doing the asking. The chain's carrier
// is comment.source_task_id: an agent's comment records the run that wrote it, and
// the run that comment wakes inherits that run's originator.
//
// The bug: CreateComment only stamped source_task_id when the authoring run's task
// was on the SAME issue as the comment. An agent coordinating on an issue it just
// created wrote a NULL lineage, so the woken run resolved to unattributed and
// every @mention / assign / sub-issue inside it hit invocation_not_allowed — while
// the identical delegation on the originating issue succeeded.
//
// The fix propagates the chain unconditionally, which is monotonic: a run can only
// carry the human it ALREADY acts for, and each hop re-runs canInvokeAgent against
// that same human. What must stay impossible is SUBSTITUTING a human — falling
// back to the coordinator's owner, or adopting the target issue's originator —
// because that borrows a different person's authority. Both directions are pinned
// below, and the three entry points a woken run delegates through are driven
// through their real handlers: the risk this fix carries is precisely that one
// entry point resolves the chain differently from another.

// crossIssueChain is the {coordinator, two issues, one running task} shape every
// case here starts from. IssueX is where the coordinator's run lives; IssueY is
// the issue it coordinates on.
type crossIssueChain struct {
	CoordinatorID string
	IssueX        string
	IssueY        string
	TaskA         string // the coordinator's running task, on IssueX
}

// newCrossIssueChain seeds a coordinator agent owned by ownerUserID with a running
// task on IssueX whose originator is originatorUserID (pass nil for an
// unattributed run, e.g. a schedule/webhook autopilot dispatch).
func newCrossIssueChain(t *testing.T, ownerUserID string, originatorUserID any) crossIssueChain {
	t.Helper()
	coordinator := seedAllowListedAgent(t, "MUL-6490 coordinator", ownerUserID, "private")
	issueX := seedChainIssue(t, "MUL-6490 originating issue", coordinator)
	return crossIssueChain{
		CoordinatorID: coordinator,
		IssueX:        issueX,
		IssueY:        seedChainIssue(t, "MUL-6490 coordinated issue", coordinator),
		TaskA: dbfx.Task(t, coordinator, testutil.Cols{
			"runtime_id":          handlerTestRuntimeID(t),
			"issue_id":            issueX,
			"status":              "running",
			"originator_user_id":  originatorUserID,
			"accountable_user_id": originatorUserID,
		}),
	}
}

// seedChainIssue inserts an agent-created issue and registers teardown for the
// rows the HANDLERS will add to it. dbfx removes only what it inserted itself, and
// these tests exist to make real handlers write comments and enqueue tasks.
//
// The number comes from the workspace counter rather than dbfx's MAX(number)+1
// default: CreateIssue allocates from that counter, so a fixture issue that
// bypasses it collides with the sub-issue the test asks the real handler to make.
func seedChainIssue(t *testing.T, title, creatorAgentID string) string {
	t.Helper()
	issueID := dbfx.Issue(t, title, testutil.Cols{
		"creator_type": "agent",
		"creator_id":   creatorAgentID,
		"number":       nextWorkspaceIssueNumber(t),
	})
	dbfx.Cleanup(t, `DELETE FROM comment WHERE issue_id = $1`, issueID)
	dbfx.Cleanup(t, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	return issueID
}

// seedAllowListedAgent creates an agent with the given permission mode. Any
// memberTargets make it public_to and allow-list exactly those users — the
// minimum-privilege configuration the report ran ("only Bohan may invoke this").
// The grants are removed explicitly because the project forbids DB cascades, so
// they would outlive the agent row dbfx deletes.
func seedAllowListedAgent(t *testing.T, name, ownerUserID, mode string, memberTargets ...string) string {
	t.Helper()
	agentID := dbfx.Agent(t, name, handlerTestRuntimeID(t), testutil.Cols{
		"permission_mode":      mode,
		"owner_id":             ownerUserID,
		"max_concurrent_tasks": 5,
		"instructions":         "",
		"custom_env":           testutil.Raw("'{}'::jsonb"),
		"custom_args":          testutil.Raw("'[]'::jsonb"),
		"mcp_config":           testutil.Raw("'[]'::jsonb"),
	})
	for _, target := range memberTargets {
		dbfx.InsertNoID(t, "agent_invocation_target", testutil.Cols{
			"agent_id":    agentID,
			"target_type": "member",
			"target_id":   target,
		}, "agent_id = $1 AND target_type = 'member' AND target_id = $2", agentID, target)
	}
	return agentID
}

// ---- entry points ---------------------------------------------------------
//
// The three ways a run delegates. Each goes through the real handler, because the
// failure this fix repairs is entry-point wiring: the gate and the enqueue used to
// resolve the chain from different places, and only the wired path shows that.

// agentComments posts an explicit top-level comment or reply as the agent.
// taskID is the authenticated source of the event's human authorization.
func agentComments(t *testing.T, agentID, taskID, issueID, content, replyUnder string) *testutil.Response {
	t.Helper()
	body := map[string]any{"content": content}
	if replyUnder != "" {
		body["parent_id"] = replyUnder
	}
	return testutil.Call(t, testHandler.CreateComment, testutil.WithURLParams(
		asRun(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", body), agentID, taskID),
		"id", issueID,
	))
}

// agentAssigns points an existing issue at targetAgentID through UpdateIssue.
func agentAssigns(t *testing.T, agentID, taskID, issueID, targetAgentID string) *testutil.Response {
	t.Helper()
	return testutil.Call(t, testHandler.UpdateIssue, testutil.WithURLParams(
		asRun(newRequest(http.MethodPatch, "/api/issues/"+issueID, map[string]any{
			"assignee_type": "agent",
			"assignee_id":   targetAgentID,
		}), agentID, taskID),
		"id", issueID,
	))
}

// agentCreatesSubIssue creates a `todo` sub-issue assigned to targetAgentID, which
// dispatches that agent immediately — the third way the report saw the chain fail.
// The title is unique per target so the active-duplicate guard never masks the
// authorization outcome this asserts.
func agentCreatesSubIssue(t *testing.T, agentID, taskID, parentIssueID, targetAgentID string) *testutil.Response {
	t.Helper()
	resp := testutil.Call(t, testHandler.CreateIssue, asRun(
		newRequest(http.MethodPost, "/api/issues?workspace_id="+testWorkspaceID, map[string]any{
			"title":           "MUL-6490 delegated sub-issue for " + targetAgentID,
			"status":          "todo",
			"priority":        "medium",
			"assignee_type":   "agent",
			"assignee_id":     targetAgentID,
			"parent_issue_id": parentIssueID,
		}), agentID, taskID))
	if resp.Code == http.StatusCreated {
		var created struct {
			ID string `json:"id"`
		}
		resp.JSON(&created)
		dbfx.Cleanup(t, `DELETE FROM issue WHERE id = $1`, created.ID)
		dbfx.Cleanup(t, `DELETE FROM comment WHERE issue_id = $1`, created.ID)
		dbfx.Cleanup(t, `DELETE FROM agent_task_queue WHERE issue_id = $1`, created.ID)
	}
	return resp
}

// asRun makes a request speak as the agent from inside one of its runs. The
// member headers newRequest sets stay put on purpose: X-User-ID remains a
// workspace member while the ACTOR is the agent, which is exactly the
// confused-deputy shape the gate must judge by the chain's human, not the caller's
// session.
func asRun(req *http.Request, agentID, taskID string) *http.Request {
	return testutil.WithHeaders(req, "X-Agent-ID", agentID, "X-Task-ID", taskID)
}

// ---- reads ----------------------------------------------------------------

// queuedTaskFor returns the queued task for (issue, agent) and its originator.
// ok is false when the delegation was refused and nothing was enqueued.
func queuedTaskFor(t *testing.T, issueID, agentID string) (taskID string, originator pgtype.UUID, ok bool) {
	t.Helper()
	const where = `FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`
	if dbfx.Count(t, `SELECT count(*) `+where, issueID, agentID) == 0 {
		return "", pgtype.UUID{}, false
	}
	dbfx.QueryRow(t, `SELECT id, originator_user_id `+where, issueID, agentID).Scan(&taskID, &originator)
	return taskID, originator, true
}

func commentSourceTaskOf(t *testing.T, commentID string) pgtype.UUID {
	t.Helper()
	var sourceTaskID pgtype.UUID
	dbfx.QueryRow(t, `SELECT source_task_id FROM comment WHERE id = $1`, commentID).Scan(&sourceTaskID)
	return sourceTaskID
}

func lastCommentOn(t *testing.T, issueID string) string {
	t.Helper()
	var commentID string
	dbfx.QueryRow(t,
		`SELECT id FROM comment WHERE issue_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1`,
		issueID).Scan(&commentID)
	return commentID
}

func runTo(t *testing.T, taskID string) {
	t.Helper()
	dbfx.Exec(t, `UPDATE agent_task_queue SET status='running',started_at=COALESCE(started_at,now()) WHERE id=$1`, taskID)
}

func mention(agentID string) string {
	return "[@Worker](mention://agent/" + agentID + ") please take this"
}

// The same human authorization must reach an explicitly assigned owner whether
// the author writes on its source issue or on another issue.
func TestCrossIssueDelegation_OriginatorChainSurvivesTheHop(t *testing.T) {
	bohan := testUserID
	worker := seedAllowListedAgent(t, "MUL-6490 worker", bohan, "public_to", bohan)
	for _, tc := range []struct {
		name  string
		cross bool
	}{{"same issue", false}, {"cross issue", true}} {
		t.Run(tc.name, func(t *testing.T) {
			chain := newCrossIssueChain(t, bohan, bohan)
			target := chain.IssueX
			if tc.cross {
				target = chain.IssueY
			}
			// The issue chooses the owner. The mention is ordinary content;
			// this unattributed fixture assignment cannot authorize an execution.
			dbfx.Exec(t, `UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1`, target, worker)
			var posted CommentResponse
			agentComments(t, chain.CoordinatorID, chain.TaskA, target, mention(worker), "").Want(http.StatusCreated).JSON(&posted)
			if got := commentSourceTaskOf(t, posted.ID); uuidToString(got) != chain.TaskA {
				t.Fatalf("comment source=%s, want the cross-issue authoring task %s", uuidToString(got), chain.TaskA)
			}
			var capturedHuman, capturedTask, eventOwner string
			dbfx.QueryRow(t, `SELECT e.originator_user_id,e.source_task_id,s.agent_id FROM issue_agent_event e JOIN issue_agent_session s ON s.id=e.session_id WHERE e.source_id=$1 AND e.kind='comment.created'`, posted.ID).Scan(&capturedHuman, &capturedTask, &eventOwner)
			if capturedHuman != bohan || capturedTask != chain.TaskA || eventOwner != worker {
				t.Fatalf("captured chain human=%s task=%s owner=%s", capturedHuman, capturedTask, eventOwner)
			}
			taskID, originator, ok := queuedTaskFor(t, target, worker)
			if !ok || uuidToString(originator) != bohan {
				t.Fatalf("owner execution lost the authorized human: task=%s originator=%s present=%t", taskID, uuidToString(originator), ok)
			}
			runTo(t, taskID)
			second := seedAllowListedAgent(t, "MUL-6490 second worker", bohan, "public_to", bohan)
			nextIssue := seedChainIssue(t, "MUL-6490 next coordinated issue", worker)
			dbfx.Exec(t, `UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1`, nextIssue, second)
			var followup CommentResponse
			agentComments(t, worker, taskID, nextIssue, mention(second), "").Want(http.StatusCreated).JSON(&followup)
			if got := commentSourceTaskOf(t, followup.ID); uuidToString(got) != taskID {
				t.Fatalf("next event lost its active source task: %s", uuidToString(got))
			}
			if _, human, ok := queuedTaskFor(t, nextIssue, second); !ok || uuidToString(human) != bohan {
				t.Fatalf("next owner execution lost the same human: present=%t originator=%s", ok, uuidToString(human))
			}
		})
	}
}

func TestCrossIssueDelegation_EveryEntryPointJudgesTheSameHuman(t *testing.T) {
	bohan := testUserID
	_, _, alice := privateAgentTestFixture(t)
	firstHop := seedAllowListedAgent(t, "MUL-6490 shared worker", bohan, "public_to", bohan, alice)
	secondHop := seedAllowListedAgent(t, "MUL-6490 bohan-only worker", bohan, "public_to", bohan)
	for _, tc := range []struct {
		name, chainOf string
		admit         bool
	}{
		{"bohan's chain reaches a bohan-only agent", bohan, true},
		{"alice's chain does not become bohan's", alice, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			chain := newCrossIssueChain(t, bohan, tc.chainOf)
			dbfx.Exec(t, `UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1`, chain.IssueY, firstHop)
			agentComments(t, chain.CoordinatorID, chain.TaskA, chain.IssueY, mention(firstHop), "").Want(http.StatusCreated)
			firstTaskID, originator, ok := queuedTaskFor(t, chain.IssueY, firstHop)
			if !ok || uuidToString(originator) != tc.chainOf {
				t.Fatalf("first owner borrowed another human: task=%s originator=%s present=%t", firstTaskID, uuidToString(originator), ok)
			}
			runTo(t, firstTaskID)
			t.Run("comment to assigned owner", func(t *testing.T) {
				target := seedChainIssue(t, "Explicit second comment owner", firstHop)
				dbfx.Exec(t, `UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1`, target, secondHop)
				var posted CommentResponse
				agentComments(t, firstHop, firstTaskID, target, mention(secondHop), "").Want(http.StatusCreated).JSON(&posted)
				if _, _, got := queuedTaskFor(t, target, secondHop); got != tc.admit {
					t.Fatalf("assigned owner admitted=%t, want %t", got, tc.admit)
				}
				var human, status string
				dbfx.QueryRow(t, `SELECT originator_user_id,status FROM issue_agent_event WHERE source_id=$1 AND kind='comment.created'`, posted.ID).Scan(&human, &status)
				if human != tc.chainOf || (!tc.admit && status != "ignored") {
					t.Fatalf("event substituted authority: human=%s status=%s", human, status)
				}
			})
			t.Run("assign", func(t *testing.T) {
				want := http.StatusForbidden
				if tc.admit {
					want = http.StatusOK
				}
				response := agentAssigns(t, firstHop, firstTaskID, chain.IssueY, secondHop).Want(want)
				if !tc.admit {
					assertDenialReason(t, response, "you do not have permission to assign work to this agent")
				}
				if _, human, ok := queuedTaskFor(t, chain.IssueY, secondHop); ok != tc.admit || (ok && uuidToString(human) != tc.chainOf) {
					t.Fatalf("assignment substituted authority: present=%t human=%s", ok, uuidToString(human))
				}
			})
			t.Run("sub-issue", func(t *testing.T) {
				want := http.StatusForbidden
				if tc.admit {
					want = http.StatusCreated
				}
				response := agentCreatesSubIssue(t, firstHop, firstTaskID, chain.IssueY, secondHop).Want(want)
				if !tc.admit {
					assertDenialReason(t, response, "you do not have permission to assign work to this agent")
					return
				}
				var created struct {
					ID string `json:"id"`
				}
				response.JSON(&created)
				if _, human, ok := queuedTaskFor(t, created.ID, secondHop); !ok || uuidToString(human) != tc.chainOf {
					t.Fatalf("sub-issue substituted authority: present=%t human=%s", ok, uuidToString(human))
				}
			})
		})
	}
	for _, tc := range []struct {
		name     string
		terminal bool
	}{{"unattributed source", false}, {"terminal source cannot borrow its former human", true}} {
		t.Run(tc.name, func(t *testing.T) {
			var human any
			if tc.terminal {
				human = bohan
			}
			chain := newCrossIssueChain(t, bohan, human)
			if tc.terminal {
				dbfx.Exec(t, `UPDATE agent_task_queue SET status='completed',completed_at=now() WHERE id=$1`, chain.TaskA)
			}
			target := seedChainIssue(t, "Refused source comment owner", chain.CoordinatorID)
			dbfx.Exec(t, `UPDATE issue SET assignee_type='agent',assignee_id=$2 WHERE id=$1`, target, firstHop)
			var posted CommentResponse
			agentComments(t, chain.CoordinatorID, chain.TaskA, target, mention(firstHop), "").Want(http.StatusCreated).JSON(&posted)
			if _, _, ok := queuedTaskFor(t, target, firstHop); ok {
				t.Fatal("unattributed or terminal source acquired member-scoped authority")
			}
			var originator pgtype.UUID
			var status, reason string
			dbfx.QueryRow(t, `SELECT originator_user_id,status,failure_reason FROM issue_agent_event WHERE source_id=$1 AND kind='comment.created'`, posted.ID).Scan(&originator, &status, &reason)
			if originator.Valid || status != "ignored" || reason != "invocation_not_allowed" {
				t.Fatalf("invalid source lent authority: originator=%s status=%s reason=%s", uuidToString(originator), status, reason)
			}
			agentAssigns(t, chain.CoordinatorID, chain.TaskA, chain.IssueY, firstHop).Want(http.StatusForbidden)
			agentCreatesSubIssue(t, chain.CoordinatorID, chain.TaskA, chain.IssueY, firstHop).Want(http.StatusForbidden)
		})
	}
}

// TestCrossIssueDelegation_GateAndStampAgreeOnTheHuman closes the split-brain the
// bug created: the invoke gate resolved the human from the request's X-Task-ID
// (cross-issue OK) while the enqueued run resolved it from the stored comment
// (cross-issue NULL). The gate said yes and the run started with no authority, so
// the failure only surfaced one hop later. Both resolvers must answer identically
// for the same action.
func TestCrossIssueDelegation_GateAndStampAgreeOnTheHuman(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	bohan := testUserID
	worker := seedAllowListedAgent(t, "MUL-6490 agreement worker", bohan, "public_to", bohan)
	chain := newCrossIssueChain(t, bohan, bohan)

	agentComments(t, chain.CoordinatorID, chain.TaskA, chain.IssueY, mention(worker), "").
		Want(http.StatusCreated)
	commentID := lastCommentOn(t, chain.IssueY)

	// What the gate saw at admission time, from the request header.
	fromGate := testHandler.invokeOriginatorFromRequest(
		asRun(newRequest(http.MethodPost, "/api/issues/"+chain.IssueY+"/comments", nil), chain.CoordinatorID, chain.TaskA),
		"agent", chain.CoordinatorID)

	// What the enqueue path sees afterwards, from the persisted comment.
	fromStamp := uuidToString(testHandler.TaskService.ResolveOriginatorFromTriggerComment(
		t.Context(), parseUUID(testWorkspaceID), parseUUID(commentID)))

	if fromGate != fromStamp {
		t.Fatalf("gate resolved %q but the stored chain resolves %q — a run would start with authority the gate never granted",
			fromGate, fromStamp)
	}
	if fromGate != bohan {
		t.Fatalf("both resolvers agree on %q, want %q", fromGate, bohan)
	}
}
