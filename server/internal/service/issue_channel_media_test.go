package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/analytics"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type afterCommitTxStarter struct {
	pool        *pgxpool.Pool
	afterCommit func()
}

func (s *afterCommitTxStarter) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &afterCommitTx{Tx: tx, afterCommit: s.afterCommit}, nil
}

type afterCommitTx struct {
	pgx.Tx
	afterCommit func()
}

type rejectIssueCommitTxStarter struct {
	pool *pgxpool.Pool
	err  error
}

func (s *rejectIssueCommitTxStarter) Begin(ctx context.Context) (pgx.Tx, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &rejectIssueCommitTx{Tx: tx, err: s.err}, nil
}

type rejectIssueCommitTx struct {
	pgx.Tx
	err error
}

type recordingIssueAnalytics struct {
	events []analytics.Event
}

func (a *recordingIssueAnalytics) Capture(event analytics.Event) {
	a.events = append(a.events, event)
}

func (*recordingIssueAnalytics) Close() {}

func (t *rejectIssueCommitTx) Commit(ctx context.Context) error {
	_ = t.Tx.Rollback(ctx)
	return t.err
}

func (t *afterCommitTx) Commit(ctx context.Context) error {
	if err := t.Tx.Commit(ctx); err != nil {
		return err
	}
	if t.afterCommit != nil {
		t.afterCommit()
		t.afterCommit = nil
	}
	return nil
}

func TestPublishAttachmentsChangedCarriesIssueScope(t *testing.T) {
	bus := events.New()
	svc := &IssueService{Bus: bus}
	workspaceID := util.MustParseUUID("11111111-1111-4111-8111-111111111111")
	issueID := util.MustParseUUID("22222222-2222-4222-8222-222222222222")
	actorID := util.MustParseUUID("33333333-3333-4333-8333-333333333333")
	var got events.Event
	bus.Subscribe(protocol.EventIssueAttachmentsChanged, func(e events.Event) { got = e })

	svc.PublishAttachmentsChanged(context.Background(), db.Issue{ID: issueID, WorkspaceID: workspaceID}, actorID)

	if got.Type != protocol.EventIssueAttachmentsChanged || got.WorkspaceID != util.UUIDToString(workspaceID) || got.ActorType != "member" || got.ActorID != util.UUIDToString(actorID) {
		t.Fatalf("event envelope = %+v", got)
	}
	payload, ok := got.Payload.(map[string]any)
	if !ok || payload["issue_id"] != util.UUIDToString(issueID) {
		t.Fatalf("event payload = %#v", got.Payload)
	}
}

func TestPublishAttachmentsChangedAlsoBroadcastsUpdatedDescription(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, _, issueID := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	issueUUID := util.MustParseUUID(issueID)
	actorID := util.MustParseUUID(userID)
	const description = "![](/api/attachments/22222222-2222-4222-8222-222222222222/download)"
	if _, err := pool.Exec(ctx, `UPDATE issue SET description = $1 WHERE id = $2`, description, issueUUID); err != nil {
		t.Fatalf("update issue description: %v", err)
	}
	issue, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID: issueUUID, WorkspaceID: workspaceUUID,
	})
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}

	bus := events.New()
	svc := &IssueService{Bus: bus, Queries: q}
	var updated events.Event
	var ordered []events.Event
	bus.Subscribe(protocol.EventIssueUpdated, func(e events.Event) { updated = e })
	bus.SubscribeAll(func(e events.Event) {
		if e.Type == protocol.EventIssueUpdated || e.Type == protocol.EventIssueAttachmentsChanged {
			ordered = append(ordered, e)
		}
	})

	svc.PublishAttachmentsChanged(ctx, issue, actorID)

	if updated.Type != protocol.EventIssueUpdated || updated.WorkspaceID != workspaceID || updated.ActorType != "member" || updated.ActorID != userID {
		t.Fatalf("issue update envelope = %+v", updated)
	}
	payload, ok := updated.Payload.(map[string]any)
	if !ok {
		t.Fatalf("issue update payload = %#v", updated.Payload)
	}
	issuePayload, ok := payload["issue"].(map[string]any)
	if !ok {
		t.Fatalf("issue update body = %#v", payload["issue"])
	}
	gotDescription, ok := issuePayload["description"].(*string)
	if !ok || gotDescription == nil || *gotDescription != description {
		t.Fatalf("broadcast description = %#v, want %q", issuePayload["description"], description)
	}
	for _, key := range []string{"assignee_changed", "status_changed", "project_changed"} {
		if changed, ok := payload[key].(bool); !ok || changed {
			t.Fatalf("%s = %#v, want false", key, payload[key])
		}
	}
	if len(ordered) != 2 || ordered[0].Type != protocol.EventIssueUpdated || ordered[1].Type != protocol.EventIssueAttachmentsChanged {
		t.Fatalf("event order = %#v, want issue:updated then issue_attachments:changed", ordered)
	}
	attachmentPayload, ok := ordered[1].Payload.(map[string]any)
	if !ok || attachmentPayload["issue_revision"] != issue.Revision {
		t.Fatalf("attachment event payload = %#v, want revision %d", ordered[1].Payload, issue.Revision)
	}
}

func TestCreateMediaGatedIssueCommitsDeferredSessionAtomicallyBeforeCreatedEvent(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)
	fireAt := time.Now().Add(time.Minute)

	bus := events.New()
	wakeup := &stubWakeup{}
	createdOverlay := json.RawMessage(`{"mcpServers":{"creator":{"url":"https://creator.example"}}}`)
	taskService := &TaskService{
		Queries: q, TxStarter: pool, Bus: bus,
		Composio:     &stubOverlayBuilder{resp: createdOverlay},
		FeatureFlags: composioMCPAppsTestFlags(true), Wakeup: wakeup,
		IssueSessionCanInvoke: func(_ context.Context, _ db.Agent, event db.IssueAgentEvent) bool {
			return event.ActorType == "member" && event.ActorID == userUUID
		},
	}
	var callbackErr error
	var committedSession db.IssueAgentSession
	txStarter := &afterCommitTxStarter{pool: pool, afterCommit: func() {
		var issueID pgtype.UUID
		if err := pool.QueryRow(ctx, `SELECT id FROM issue WHERE workspace_id=$1 AND title='Media-gated issue'`, workspaceUUID).Scan(&issueID); err != nil {
			callbackErr = err
			return
		}
		var err error
		committedSession, err = q.GetIssueAgentSessionForPair(ctx, db.GetIssueAgentSessionForPairParams{
			WorkspaceID: workspaceUUID, AgentID: agentUUID, IssueID: issueID,
		})
		if err != nil {
			callbackErr = fmt.Errorf("session not committed with issue: %w", err)
			return
		}
		if !committedSession.WakeAfter.Valid || committedSession.WakeAfter.Time.Before(fireAt.Add(-time.Millisecond)) {
			callbackErr = fmt.Errorf("session missing durable media deadline: %+v", committedSession)
			return
		}
		if task, err := taskService.WakeIssueAgentSession(ctx, committedSession.ID); err != nil || task != nil {
			callbackErr = fmt.Errorf("wake raced media gate: task=%v err=%v", task, err)
		}
	}}
	issueService := NewIssueService(q, txStarter, bus, nil, taskService)
	var followupID pgtype.UUID
	bus.Subscribe(protocol.EventIssueCreated, func(event events.Event) {
		if callbackErr != nil {
			return
		}
		if committedSession.ActiveTaskID.Valid {
			callbackErr = errors.New("session started before media was bound")
			return
		}
		if err := pool.QueryRow(ctx, `
			INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content)
			VALUES ($1, $2, 'member', $3, 'Immediate follow-up') RETURNING id`,
			committedSession.IssueID, workspaceUUID, userUUID).Scan(&followupID); err != nil {
			callbackErr = err
			return
		}
		if task, err := taskService.WakeIssueAgentSession(ctx, committedSession.ID); err != nil || task != nil {
			callbackErr = fmt.Errorf("follow-up escaped media gate: task=%v err=%v", task, err)
		}
	})
	result, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID: workspaceUUID, Title: "Media-gated issue", Status: "todo", Priority: "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true}, AssigneeID: agentUUID,
		CreatorType: "member", CreatorID: userUUID,
	}, IssueCreateOpts{AssignedAgentRunFireAt: fireAt})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	if !result.AssignedSessionID.Valid || result.AssignedSessionID != committedSession.ID || result.AssignedTaskID.Valid {
		t.Fatalf("media-gated create = session %v task %v", result.AssignedSessionID, result.AssignedTaskID)
	}
	if len(wakeup.calls) != 0 {
		t.Fatalf("media-gated session woke prematurely: %+v", wakeup.calls)
	}
	events, err := q.ListIssueAgentEvents(ctx, db.ListIssueAgentEventsParams{SessionID: result.AssignedSessionID, EventLimit: 10})
	if err != nil || len(events) != 2 || events[0].Kind != "assigned" || events[1].SourceID != followupID {
		t.Fatalf("durable events before release = %+v err=%v", events, err)
	}
	if err := taskService.ReleaseIssueAgentSession(ctx, result.AssignedSessionID); err != nil {
		t.Fatal(err)
	}
	session, err := q.GetIssueAgentSession(ctx, result.AssignedSessionID)
	if err != nil || !session.ActiveTaskID.Valid || session.WakeAfter.Valid {
		t.Fatalf("released session = %+v err=%v", session, err)
	}
	task, err := q.GetAgentTask(ctx, session.ActiveTaskID)
	if err != nil || task.TriggerCommentID.Valid || !IsIssueAgentSessionTask(task) {
		t.Fatalf("released execution = %+v err=%v", task, err)
	}
	var gotOverlay, wantOverlay any
	if err := json.Unmarshal(task.RuntimeMcpOverlay, &gotOverlay); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(createdOverlay, &wantOverlay); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotOverlay, wantOverlay) {
		t.Fatalf("execution overlay = %s, want %s", task.RuntimeMcpOverlay, createdOverlay)
	}
	if len(wakeup.calls) != 1 || wakeup.calls[0].taskID != util.UUIDToString(task.ID) {
		t.Fatalf("release wakeups = %+v", wakeup.calls)
	}
}

func TestCreateIssuePropertiesCommitFailureHasNoSideEffects(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)
	propertyID := pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Valid: true}
	if _, err := pool.Exec(ctx, `INSERT INTO issue_property (id, workspace_id, name, type, config) VALUES ($1, $2, 'Commit property', 'text', '{}')`, propertyID, workspaceUUID); err != nil {
		t.Fatalf("insert property: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM issue_property WHERE id = $1`, propertyID) })

	bus := events.New()
	createdEvents := 0
	bus.Subscribe(protocol.EventIssueCreated, func(events.Event) { createdEvents++ })
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	analyticsSink := &recordingIssueAnalytics{}
	injected := errors.New("injected issue commit failure")
	issueService := NewIssueService(q, &rejectIssueCommitTxStarter{pool: pool, err: injected}, bus, analyticsSink, taskService)
	var taskCountBefore int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM agent_task_queue WHERE agent_id = $1`, agentUUID).Scan(&taskCountBefore); err != nil {
		t.Fatalf("count tasks before create: %v", err)
	}
	if taskCountBefore != 0 {
		t.Fatalf("fixture agent has %d tasks before create, want 0", taskCountBefore)
	}

	_, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "properties commit failure",
		Status:       "todo",
		Priority:     "none",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		Properties: map[pgtype.UUID]json.RawMessage{
			propertyID: json.RawMessage(`"kept only if committed"`),
		},
	}, IssueCreateOpts{})
	if !errors.Is(err, injected) {
		t.Fatalf("Create error = %v, want injected commit failure", err)
	}
	if createdEvents != 0 {
		t.Fatalf("published %d issue:created events after failed commit", createdEvents)
	}
	if len(analyticsSink.events) != 0 {
		t.Fatalf("captured %d analytics events after failed commit", len(analyticsSink.events))
	}
	var issueCount, taskCountAfter int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM issue WHERE workspace_id = $1 AND title = 'properties commit failure'`, workspaceUUID).Scan(&issueCount); err != nil {
		t.Fatalf("count issues: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM agent_task_queue WHERE agent_id = $1`, agentUUID).Scan(&taskCountAfter); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if issueCount != 0 || taskCountAfter != 0 {
		t.Fatalf("failed commit left side effects: issues=%d tasks_before=%d tasks_after=%d", issueCount, taskCountBefore, taskCountAfter)
	}
}

func TestCreateIssuePropertiesAreVisibleAtCommitBeforeEnqueue(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)
	propertyID := pgtype.UUID{Bytes: [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}, Valid: true}
	if _, err := pool.Exec(ctx, `INSERT INTO issue_property (id, workspace_id, name, type, config) VALUES ($1, $2, 'Enqueue property', 'text', '{}')`, propertyID, workspaceUUID); err != nil {
		t.Fatalf("insert property: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DELETE FROM issue_property WHERE id = $1`, propertyID) })

	var atCommit map[string]any
	txStarter := &afterCommitTxStarter{pool: pool, afterCommit: func() {
		var raw []byte
		if err := pool.QueryRow(ctx, `SELECT properties FROM issue WHERE workspace_id = $1 AND title = 'properties before enqueue'`, workspaceUUID).Scan(&raw); err != nil {
			atCommit = map[string]any{"error": err.Error()}
			return
		}
		_ = json.Unmarshal(raw, &atCommit)
	}}
	bus := events.New()
	var createdEvents []events.Event
	propertyChangedEvents := 0
	bus.Subscribe(protocol.EventIssueCreated, func(event events.Event) {
		createdEvents = append(createdEvents, event)
	})
	bus.Subscribe(protocol.EventIssuePropertiesChanged, func(events.Event) {
		propertyChangedEvents++
	})
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus,
		IssueSessionCanInvoke: func(_ context.Context, _ db.Agent, event db.IssueAgentEvent) bool {
			return event.ActorType == "member" && event.ActorID == userUUID
		},
	}
	issueService := NewIssueService(q, txStarter, bus, nil, taskService)

	result, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "properties before enqueue",
		Status:       "todo",
		Priority:     "none",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		Properties: map[pgtype.UUID]json.RawMessage{
			propertyID: json.RawMessage(`"ready"`),
		},
	}, IssueCreateOpts{
		BroadcastPayload: func(issue db.Issue, _ []db.Attachment, _ []db.IssueLabel) map[string]any {
			var properties map[string]any
			if err := json.Unmarshal(issue.Properties, &properties); err != nil {
				return map[string]any{"error": err.Error()}
			}
			return map[string]any{"issue": map[string]any{"properties": properties}}
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if atCommit[util.UUIDToString(propertyID)] != "ready" {
		t.Fatalf("properties visible immediately after commit = %#v", atCommit)
	}
	if !result.AssignedTaskID.Valid {
		t.Fatal("assigned task was not enqueued after property-bearing commit")
	}
	if len(createdEvents) != 1 || propertyChangedEvents != 0 {
		t.Fatalf("create events = %d issue:created, %d property-changed; want 1 and 0", len(createdEvents), propertyChangedEvents)
	}
	payload, ok := createdEvents[0].Payload.(map[string]any)
	if !ok {
		t.Fatalf("issue:created payload = %#v", createdEvents[0].Payload)
	}
	issueSnapshot, ok := payload["issue"].(map[string]any)
	if !ok {
		t.Fatalf("issue:created issue snapshot = %#v", payload["issue"])
	}
	properties, ok := issueSnapshot["properties"].(map[string]any)
	if !ok || properties[util.UUIDToString(propertyID)] != "ready" {
		t.Fatalf("issue:created properties = %#v", issueSnapshot["properties"])
	}
}

// The channel router publishes this snapshot for the issue it created, but
// the media download gives others up to DefaultMediaTimeout to act on it. A
// duplicate mark made in that window is on the row the snapshot reads, and
// clients patch their cache with the snapshot, so it must carry the mark
// rather than a null that erases it (MUL-7349).
func TestPublishAttachmentsChangedKeepsDuplicateMark(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, _, issueID := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	created, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID: util.MustParseUUID(issueID), WorkspaceID: workspaceUUID,
	})
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	var originalID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO issue (workspace_id, number, title, creator_type, creator_id, priority, status)
		VALUES ($1, (SELECT COALESCE(MAX(number), 0) + 1 FROM issue WHERE workspace_id = $1),
			'attr original', 'member', $2, 'none', 'in_progress')
		RETURNING id`, workspaceID, userID).Scan(&originalID); err != nil {
		t.Fatalf("seed original: %v", err)
	}
	t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, originalID) })
	original, err := q.GetIssueInWorkspace(ctx, db.GetIssueInWorkspaceParams{
		ID: util.MustParseUUID(originalID), WorkspaceID: workspaceUUID,
	})
	if err != nil {
		t.Fatalf("load original: %v", err)
	}
	workspace, err := q.GetWorkspace(ctx, workspaceUUID)
	if err != nil {
		t.Fatalf("load workspace: %v", err)
	}

	for _, tc := range []struct {
		name   string
		status string
		want   bool
	}{
		{"marked during the download", "cancelled", true},
		// A pointer an older server left on a reopened issue is no mark.
		{"pointer left on a reopened issue", "todo", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `UPDATE issue SET status = $1, duplicate_of_issue_id = $2 WHERE id = $3`,
				tc.status, originalID, issueID); err != nil {
				t.Fatalf("mark: %v", err)
			}
			bus := events.New()
			svc := &IssueService{Bus: bus, Queries: q}
			var updated events.Event
			bus.Subscribe(protocol.EventIssueUpdated, func(e events.Event) { updated = e })

			svc.PublishAttachmentsChanged(ctx, created, util.MustParseUUID(userID))

			payload, _ := updated.Payload.(map[string]any)
			snapshot, ok := payload["issue"].(map[string]any)
			if !ok {
				t.Fatalf("issue update payload = %#v", updated.Payload)
			}
			value, present := snapshot["duplicate_of"]
			if !present {
				t.Fatal("snapshot has no duplicate_of key")
			}
			ref, _ := value.(map[string]any)
			if !tc.want {
				if ref != nil {
					t.Fatalf("duplicate_of = %v, want null", ref)
				}
				return
			}
			want := map[string]any{
				"id":         originalID,
				"identifier": IssueIdentifier(workspace.IssuePrefix, original.Number),
				"title":      "attr original",
				"status":     "in_progress",
			}
			if len(ref) != len(want) {
				t.Fatalf("duplicate_of = %v, want %v", ref, want)
			}
			for k, v := range want {
				if ref[k] != v {
					t.Fatalf("duplicate_of = %v, want %v", ref, want)
				}
			}
		})
	}
}
