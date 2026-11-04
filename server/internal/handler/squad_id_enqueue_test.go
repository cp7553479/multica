package handler

import (
	"context"
	"testing"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestCreateCommentSquadMentionCannotChangeTheOwnerSessionRole(t *testing.T) {
	issueID, ownerID, runtimeID := issueSessionCaptureFixture(t)
	leaderID := dbfx.Agent(t, "Mentioned squad leader", runtimeID)
	squadID := dbfx.Squad(t, "Mentioned squad role", leaderID)
	captureSessionComment(t, issueID, map[string]any{
		"content": "[@Squad](mention://squad/" + squadID + ") collaboration context",
	}, "", "")
	sessions := captureIssueSessions(t, issueID)
	if len(sessions) != 1 || sessions[0].AgentID != ownerID || sessions[0].ActiveTaskID == nil {
		t.Fatalf("squad mention changed the issue owner: %+v", sessions)
	}
	task, err := testHandler.Queries.GetAgentTask(context.Background(), parseUUID(*sessions[0].ActiveTaskID))
	if err != nil || task.IsLeaderTask || task.SquadID.Valid || uuidToString(task.AgentID) != ownerID {
		t.Fatalf("squad mention changed the execution role: %+v, %v", task, err)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2", issueID, leaderID); n != 0 {
		t.Fatalf("squad mention dispatched %d implicit leader tasks", n)
	}
}

// TestCreateRetryTask_InheritsSquadID locks the retry-clone contract for the
// MUL-3730 fix: a retried leader task must inherit squad_id from its parent so
// the squad-leader briefing keeps being injected across retries. Parallels
// TestCreateRetryTask_InheritsIsLeaderTask.
func TestCreateRetryTask_InheritsSquadID(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	fx := newSquadCommentTriggerFixture(t)
	issueID := uuidToString(fx.Issue.ID)

	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
	})

	var runtimeID string
	if err := testPool.QueryRow(ctx, `SELECT runtime_id FROM agent WHERE id = $1`, fx.LeaderID).Scan(&runtimeID); err != nil {
		t.Fatalf("load runtime: %v", err)
	}

	var parentID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, attempt, max_attempts, is_leader_task, squad_id)
		VALUES ($1, $2, $3, 'failed', 1, 3, TRUE, $4)
		RETURNING id
	`, fx.LeaderID, runtimeID, issueID, fx.SquadID).Scan(&parentID); err != nil {
		t.Fatalf("seed parent task: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE id = $1 OR parent_task_id = $1`, parentID)
	})

	child, err := testHandler.Queries.CreateRetryTask(ctx, db.CreateRetryTaskParams{ID: util.MustParseUUID(parentID)})
	if err != nil {
		t.Fatalf("CreateRetryTask: %v", err)
	}
	if !child.SquadID.Valid || util.UUIDToString(child.SquadID) != fx.SquadID {
		t.Fatalf("child.SquadID = %v (valid=%v), want %s", util.UUIDToString(child.SquadID), child.SquadID.Valid, fx.SquadID)
	}
	if !child.IsLeaderTask {
		t.Fatalf("child.IsLeaderTask = false, want true (provenance must survive retry)")
	}
}
