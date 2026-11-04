package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/logger"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

type CommentResponse struct {
	ID             string  `json:"id"`
	IssueID        string  `json:"issue_id"`
	AuthorType     string  `json:"author_type"`
	AuthorID       string  `json:"author_id"`
	Content        string  `json:"content"`
	Type           string  `json:"type"`
	ParentID       *string `json:"parent_id"`
	CreatedAt      string  `json:"created_at"`
	UpdatedAt      string  `json:"updated_at"`
	Revision       int64   `json:"revision"`
	IssueRevision  int64   `json:"issue_revision,omitempty"`
	ResolvedAt     *string `json:"resolved_at"`
	ResolvedByType *string `json:"resolved_by_type"`
	ResolvedByID   *string `json:"resolved_by_id"`
	SourceTaskID   *string `json:"source_task_id,omitempty"`
	// DeletedAt marks a tombstone: a comment deleted while it still had
	// replies (#8296). Its content is empty and it carries no attachments or
	// reactions; it stays only so the replies keep their direct parent.
	// Omitted for every live comment.
	DeletedAt *string `json:"deleted_at,omitempty"`
	// QuickActionID marks a comment produced by a quick action run (MUL-5465).
	// The timeline renders those as a collapsed one-line card instead of the
	// raw prompt body. It is NOT settable through this endpoint — there is no
	// request field for it — which is exactly why the card keys off this id
	// rather than a `type` value the client controls.
	QuickActionID *string              `json:"quick_action_id,omitempty"`
	Reactions     []ReactionResponse   `json:"reactions"`
	Attachments   []AttachmentResponse `json:"attachments"`
	// Orientation stats — populated only on the roots_only path and omitted in
	// every other mode, so the default response shape stays byte-identical for
	// existing callers. ReplyCount is the number of descendants in the thread;
	// LastActivityAt is the MAX(created_at) across the whole subtree. Together
	// they let an agent triage which thread to drill into without fetching any
	// replies.
	ReplyCount     *int    `json:"reply_count,omitempty"`
	LastActivityAt *string `json:"last_activity_at,omitempty"`
	// Populated on roots_only for reply-resolved threads.
	ResolutionID               *string `json:"resolution_id,omitempty"`
	ResolutionContent          *string `json:"resolution_content,omitempty"`
	ResolutionContentTruncated *bool   `json:"resolution_content_truncated,omitempty"`
	// ContentTruncated is set only under summary=true: true when Content was
	// clipped to the summary budget, false when it fit. nil (omitted) means the
	// caller did not request a summary projection, so Content is verbatim.
	ContentTruncated *bool `json:"content_truncated,omitempty"`
	// Fold projection fields — populated only under fold=true, and only on the
	// thread ROOT of a resolved thread (mirrors the human timeline fold; see
	// foldResolvedThreads). Both are nil/omitted everywhere else: every comment
	// under an unresolved thread, and every non-root comment. ThreadResolved
	// marks a thread whose discussion was collapsed to its conclusion;
	// FoldedCount is how many comments in that thread were dropped from the
	// response (0 when the thread was already at root + conclusion). The reader
	// pulls the folded comments back with `comment list --full`.
	ThreadResolved *bool `json:"thread_resolved,omitempty"`
	FoldedCount    *int  `json:"folded_count,omitempty"`
	// TriggerOutcomes retains the installed-client response field, reporting
	// delivery to this issue's owner. Mentions cannot add execution targets.
	TriggerOutcomes []CommentTriggerOutcome `json:"trigger_outcomes,omitempty"`
	// Supplements lists every running turn this comment steered, one receipt
	// per run. The single supplement_* fields mirror the first receipt for
	// clients that predate multi-run steering.
	Supplements             []CommentSupplementResponse `json:"supplements,omitempty"`
	SupplementTaskID        string                      `json:"supplement_task_id,omitempty"`
	SupplementStatus        string                      `json:"supplement_status,omitempty"`
	SupplementFailureReason *string                     `json:"supplement_failure_reason,omitempty"`
	SupplementDeliveredAt   *string                     `json:"supplement_delivered_at,omitempty"`
}

// CommentTriggerOutcome reports the owner-session delivery outcome using
// the existing response shape. Reason codes never expose private agent details.
type CommentTriggerOutcome struct {
	TargetType string             `json:"target_type"` // "agent" | "squad"
	TargetID   string             `json:"target_id"`
	Status     DispatchStatus     `json:"status"` // queued | coalesced | deferred | blocked
	ReasonCode DispatchReasonCode `json:"reason_code"`
}

func commentToResponse(c db.Comment, reactions []ReactionResponse, attachments []AttachmentResponse) CommentResponse {
	if reactions == nil {
		reactions = []ReactionResponse{}
	}
	if attachments == nil {
		attachments = []AttachmentResponse{}
	}
	return CommentResponse{
		ID:             uuidToString(c.ID),
		IssueID:        uuidToString(c.IssueID),
		AuthorType:     c.AuthorType,
		AuthorID:       uuidToString(c.AuthorID),
		Content:        c.Content,
		Type:           c.Type,
		ParentID:       uuidToPtr(c.ParentID),
		CreatedAt:      timestampToString(c.CreatedAt),
		UpdatedAt:      timestampToString(c.UpdatedAt),
		Revision:       c.Revision,
		ResolvedAt:     timestampToPtr(c.ResolvedAt),
		ResolvedByType: textToPtr(c.ResolvedByType),
		ResolvedByID:   uuidToPtr(c.ResolvedByID),
		SourceTaskID:   uuidToPtr(c.SourceTaskID),
		DeletedAt:      timestampToPtr(c.DeletedAt),
		QuickActionID:  uuidToPtr(c.QuickActionID),
		Reactions:      reactions,
		Attachments:    attachments,
	}
}

// Shared bound for plugin comments and input sent to a running agent.
const maxCommentContentBytes = 64 * 1024

// summaryContentRunes bounds comment content under summary=true. 200 runes is
// enough to tell what a comment is about (its opening) while cutting the bulk
// of a long body out of an agent's context budget. Counted in runes, not bytes,
// so multi-byte (e.g. CJK) content is clipped on a character boundary.
const summaryContentRunes = 200

// summarizeContent clips content to summaryContentRunes for the summary
// projection. Returns the (possibly clipped) content and whether it was
// truncated. An ellipsis marks a clip so the reader knows more text exists.
//
// It scans by rune and stops at the (budget+1)th rune rather than allocating a
// full []rune for the whole body — so a pathologically long comment costs only
// the budget, not its full length, under summary mode.
func summarizeContent(content string) (string, bool) {
	count := 0
	for byteOffset := range content { // range over a string yields rune start offsets
		if count == summaryContentRunes {
			return content[:byteOffset] + "…", true
		}
		count++
	}
	return content, false
}

// foldStat is the per-thread fold annotation attached to a resolved thread's
// root under fold=true. FoldedCount is how many comments in the thread were
// dropped from the response. See CommentResponse.ThreadResolved / FoldedCount.
type foldStat struct {
	FoldedCount int
}

// foldResolvedThreads collapses every resolved thread in a COMPLETE-thread
// comment set down to the comments a reader actually needs, mirroring the human
// timeline fold (deriveThreadResolution in
// packages/views/issues/components/thread-utils.ts) so agents see what humans
// see:
//
//   - unresolved thread     → every comment kept, unchanged, no annotation.
//   - reply-resolved thread → keep the thread root + the resolution reply (the
//     conclusion), drop every other reply. Root + conclusion, never conclusion
//     alone: a conclusion is often referential ("go with what we said above")
//     and unreadable without the root question.
//   - root-resolved thread  → keep only the root, drop every reply (the whole
//     discussion was a settled dead-end; the root states the topic).
//
// The thread root of each resolved thread is annotated (foldStat keyed by the
// root's id string) so the response can mark it thread_resolved and report how
// many comments were folded; a reader that needs the dropped discussion pulls
// it back with `comment list --full`.
//
// Resolution is derived exactly as the UI does: the root wins if resolved;
// otherwise the reply with the latest resolved_at is THE resolution. The single
// resolved-per-thread invariant (ClearOtherThreadResolutions) normally makes
// this unambiguous; the latest-wins tiebreak keeps the projection total if an
// older/concurrent write ever left two.
//
// Callers MUST invoke this only on a set containing COMPLETE threads (the
// default list, --recent, and untailed --thread reads). Partial-thread reads
// (--since, --tail) are rejected upstream because a fold computed over a partial
// thread could drop a resolution that was never fetched. As defense in depth,
// any comment whose thread root is absent from the set is treated as its own
// root and kept verbatim, so a partial thread degrades to "unchanged" rather
// than to silent data loss.
func foldResolvedThreads(comments []db.Comment) ([]db.Comment, map[string]foldStat) {
	if len(comments) == 0 {
		return comments, nil
	}

	byID := make(map[string]db.Comment, len(comments))
	for _, c := range comments {
		byID[uuidToString(c.ID)] = c
	}

	// rootOf walks parent_id up to the thread root within this set. It stops at
	// the first comment whose parent is absent (in a complete-thread set that is
	// the real root; in a partial set it is the highest ancestor we hold). The
	// loop is bounded by len(comments) so an unexpected cycle cannot hang it —
	// the PK forbids real cycles, but never trust a graph walk over stored data.
	rootOf := func(c db.Comment) db.Comment {
		cur := c
		for i := 0; i < len(comments); i++ {
			if !cur.ParentID.Valid {
				return cur
			}
			parent, ok := byID[uuidToString(cur.ParentID)]
			if !ok {
				return cur
			}
			cur = parent
		}
		return cur
	}

	type thread struct {
		root    db.Comment
		replies []db.Comment // non-root, in input (chronological) order
	}
	threads := map[string]*thread{}
	for _, c := range comments {
		root := rootOf(c)
		rid := uuidToString(root.ID)
		th := threads[rid]
		if th == nil {
			th = &thread{root: root}
			threads[rid] = th
		}
		if uuidToString(c.ID) != rid {
			th.replies = append(th.replies, c)
		}
	}

	keep := make(map[string]bool, len(comments))
	stats := map[string]foldStat{}
	for rid, th := range threads {
		// Root-resolved: keep only the root.
		if th.root.ResolvedAt.Valid {
			keep[rid] = true
			stats[rid] = foldStat{FoldedCount: len(th.replies)}
			continue
		}
		// Reply-resolved: the latest-resolved reply is the conclusion.
		var resolution *db.Comment
		for i := range th.replies {
			r := &th.replies[i]
			if !r.ResolvedAt.Valid {
				continue
			}
			if resolution == nil || r.ResolvedAt.Time.After(resolution.ResolvedAt.Time) {
				resolution = r
			}
		}
		if resolution == nil {
			// Unresolved thread: keep everything, no annotation.
			keep[rid] = true
			for _, r := range th.replies {
				keep[uuidToString(r.ID)] = true
			}
			continue
		}
		keep[rid] = true
		keep[uuidToString(resolution.ID)] = true
		// Every reply except the conclusion is folded away.
		stats[rid] = foldStat{FoldedCount: len(th.replies) - 1}
	}

	out := make([]db.Comment, 0, len(comments))
	for _, c := range comments {
		if keep[uuidToString(c.ID)] {
			out = append(out, c)
		}
	}
	return out, stats
}

// commentHardCap bounds the comments returned per issue. Sized as a defensive
// safety net rather than a UX paging window: prod p99 is ~30 comments and
// the all-time max observed is ~1.1k, so 2000 leaves ~2x headroom while still
// preventing a runaway response if some user manages to accumulate a wild
// number of rows on a single issue.
//
// This and the thread-completion budgets below are variables only so the cap
// tests can run the same windowing without seeding thousands of rows per
// scenario; nothing outside tests assigns them.
var commentHardCap = 2000

// HeaderCommentsTruncated tells browser and CLI callers that a defensive
// comment-list cap omitted rows. It is deliberately separate from the timeline
// header: /comments has several projections (roots, one thread, since) whose
// truncation cannot be described as a timeline activity kind.
const HeaderCommentsTruncated = "X-Comments-Truncated"

// commentProbeLimit reads one row past the cap so a truncated read can be told
// apart from an issue holding exactly commentHardCap comments. The difference
// matters: exact-cap reads are complete and must not advertise data loss.
func commentProbeLimit() int32 { return int32(commentHardCap) + 1 }

// Budgets for thread completion (completeCommentThreads).
//
// commentThreadContextBudget caps ALL context added outside the newest window:
// ancestors first, then missing siblings/descendants for any thread whose root
// had to be restored. A response is therefore bounded by commentHardCap +
// commentThreadContextBudget comments regardless of thread shape.
// commentThreadMaxDepth bounds each direction of the walk.
//
// Both are needed because reply depth is genuinely unbounded in stored data: the
// general write path saves the exact comment being replied to, so chains can run
// far deeper than the two levels the UI usually renders. Without a budget the
// completion pass would defeat the row cap it is meant to preserve.
var (
	commentThreadContextBudget = 2000
	commentThreadMaxDepth      = 64
)

// ListComments returns chronological comments with bounded thread-aware reads.
// The default newest-comment cap restores complete threads within a context budget
// or drops each incomplete thread as a unit and sets HeaderCommentsTruncated.
//
// roots_only provides activity metadata; summary clips bodies. thread accepts a
// root or descendant anchor; tail limits replies while retaining the root. recent
// limits whole active threads, not individual comments. Resolved-thread folding
// requires complete threads and cannot combine with partial-thread projections.
//
// Cursors pair a timestamp with an ID for tie-breaking: recent pages by thread
// activity, while thread+tail pages by reply creation. Since filtering follows
// the thread window. The validation below enforces supported combinations.
func (h *Handler) ListComments(w http.ResponseWriter, r *http.Request) {
	issueID := chi.URLParam(r, "id")
	issue, ok := h.loadIssueForUser(w, r, issueID)
	if !ok {
		return
	}

	q := r.URL.Query()

	var sinceTime pgtype.Timestamptz
	if v := q.Get("since"); v != "" {
		// RFC3339Nano parsing also accepts timestamps without fractional seconds.
		t, err := time.Parse(time.RFC3339Nano, v)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid since parameter; expected RFC3339 format")
			return
		}
		sinceTime = pgtype.Timestamptz{Time: t, Valid: true}
	}

	threadStr := q.Get("thread")
	recentStr := q.Get("recent")
	tailStr := q.Get("tail")
	beforeTimeStr := q.Get("before")
	beforeIDStr := q.Get("before_id")
	if beforeIDStr == "" {
		// Accept hyphenated alias to match CLI flag convention.
		beforeIDStr = q.Get("before-id")
	}

	rootsOnlyStr := q.Get("roots_only")
	if rootsOnlyStr == "" {
		// Accept hyphenated alias to match CLI flag convention.
		rootsOnlyStr = q.Get("roots-only")
	}

	rootsOnly := false
	if rootsOnlyStr != "" {
		switch rootsOnlyStr {
		case "true":
			rootsOnly = true
		case "false":
		default:
			writeError(w, http.StatusBadRequest, "invalid roots_only parameter; expected boolean")
			return
		}
	}

	// summary=true is an orthogonal content projection: it clips each comment's
	// content to a fixed budget so an agent can scan a list without pulling full
	// bodies into context. It is intentionally NOT mutually exclusive with any
	// mode — it composes with the default list, since, thread, recent, and
	// roots_only alike.
	summary := false
	if summaryStr := q.Get("summary"); summaryStr != "" {
		switch summaryStr {
		case "true":
			summary = true
		case "false":
		default:
			writeError(w, http.StatusBadRequest, "invalid summary parameter; expected boolean")
			return
		}
	}

	// fold=true is a thread-level projection: it collapses every resolved thread
	// in the result to root + conclusion (reply-resolved) or root only
	// (root-resolved), reusing the human timeline's fold semantics so an agent
	// reading a long issue doesn't pay tokens for settled discussion. Unlike
	// summary (a per-comment content clip that composes with everything), fold
	// needs WHOLE threads to compute a resolution, so it is rejected on the
	// partial-thread reads (since / tail) and on roots_only (which carries no
	// replies to fold). It composes with summary, the default list, recent, and
	// untailed thread reads.
	fold := false
	if foldStr := q.Get("fold"); foldStr != "" {
		switch foldStr {
		case "true":
			fold = true
		case "false":
		default:
			writeError(w, http.StatusBadRequest, "invalid fold parameter; expected boolean")
			return
		}
	}

	// --- combination validation ----------------------------------------
	if fold && sinceTime.Valid {
		writeError(w, http.StatusBadRequest, "fold and since are mutually exclusive: since returns a partial thread, and a fold over a partial thread could hide a resolution that was not fetched")
		return
	}
	if fold && tailStr != "" {
		writeError(w, http.StatusBadRequest, "fold and tail are mutually exclusive: tail returns a partial thread, which cannot be folded safely")
		return
	}
	if fold && rootsOnly {
		writeError(w, http.StatusBadRequest, "fold and roots_only are mutually exclusive: roots_only returns no replies to fold")
		return
	}
	if rootsOnly && threadStr != "" {
		writeError(w, http.StatusBadRequest, "roots_only and thread are mutually exclusive")
		return
	}
	if rootsOnly && recentStr != "" {
		writeError(w, http.StatusBadRequest, "roots_only and recent are mutually exclusive")
		return
	}
	if rootsOnly && tailStr != "" {
		writeError(w, http.StatusBadRequest, "roots_only and tail are mutually exclusive")
		return
	}
	if rootsOnly && (beforeTimeStr != "" || beforeIDStr != "") {
		writeError(w, http.StatusBadRequest, "roots_only does not support before / before_id")
		return
	}
	if threadStr != "" && recentStr != "" {
		writeError(w, http.StatusBadRequest, "thread and recent are mutually exclusive")
		return
	}
	if tailStr != "" && threadStr == "" {
		writeError(w, http.StatusBadRequest, "tail requires thread (it is a thread-scoped limit)")
		return
	}
	if (beforeTimeStr == "") != (beforeIDStr == "") {
		writeError(w, http.StatusBadRequest, "before and before_id must be set together (composite cursor)")
		return
	}
	// Cursor needs either a recent window (thread cursor) or a tailed thread
	// (reply cursor). A bare cursor would otherwise fall through to the
	// default / since path — returning a full timeline that the caller did
	// not ask for. Reject loudly so the API surface matches the documented
	// semantics.
	if beforeTimeStr != "" && recentStr == "" && (threadStr == "" || tailStr == "") {
		writeError(w, http.StatusBadRequest, "before / before_id require recent (thread cursor) or thread + tail (reply cursor)")
		return
	}

	// --- parse cursor / recent ----------------------------------------
	var beforeCursor pgtype.Timestamptz
	var beforeUUID pgtype.UUID
	hasCursor := false
	if beforeTimeStr != "" {
		t, err := time.Parse(time.RFC3339Nano, beforeTimeStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid before parameter; expected RFC3339 format")
			return
		}
		beforeCursor = pgtype.Timestamptz{Time: t, Valid: true}
		uuid, perr := util.ParseUUID(beforeIDStr)
		if perr != nil {
			writeError(w, http.StatusBadRequest, "invalid before_id parameter; expected UUID")
			return
		}
		beforeUUID = uuid
		hasCursor = true
	}

	recentN := 0
	if recentStr != "" {
		n, err := strconv.Atoi(recentStr)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid recent parameter; expected positive integer")
			return
		}
		if n > commentHardCap {
			n = commentHardCap
		}
		recentN = n
	}

	// tail=0 is allowed (returns root only — useful for "what is this thread
	// about" lookups without dragging any replies into context). Negative
	// values are rejected because they'd round-trip to LIMIT -N which
	// PostgreSQL flags as a syntax error.
	threadTail := -1
	threadTailSet := false
	if tailStr != "" {
		n, err := strconv.Atoi(tailStr)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "invalid tail parameter; expected non-negative integer")
			return
		}
		if n > commentHardCap {
			n = commentHardCap
		}
		threadTail = n
		threadTailSet = true
	}

	result, err := h.fetchCommentsForList(r.Context(), fetchCommentsArgs{
		Issue:         issue,
		Since:         sinceTime,
		ThreadAnchor:  threadStr,
		ThreadTail:    threadTail,
		ThreadTailSet: threadTailSet,
		RecentN:       recentN,
		HasCursor:     hasCursor,
		BeforeAt:      beforeCursor,
		BeforeID:      beforeUUID,
		RootsOnly:     rootsOnly,
	})
	if err != nil {
		switch err {
		case errCommentThreadNotFound:
			writeError(w, http.StatusNotFound, "thread anchor not found in this issue")
			return
		case errCommentThreadBadID:
			writeError(w, http.StatusBadRequest, "invalid thread parameter; expected UUID")
			return
		default:
			writeError(w, http.StatusInternalServerError, "failed to list comments")
			return
		}
	}

	// Apply the resolve-aware fold before anything keys off the comment set
	// (reaction/attachment grouping, the response array): folding drops comments,
	// and the dropped ones should not pay a reactions/attachments round-trip or
	// appear in the response.
	//
	// fetchCommentsForList marks FoldUnsafe only when a returned thread is known
	// to be partial. The default capped path is safe: completeCommentThreads
	// either restores every affected thread or removes it as one unit. An
	// oversized untailed --thread read, however, intentionally keeps only the
	// newest replies, so folding that partial thread would invent resolution
	// state and folded_count.
	var foldInfo map[string]foldStat
	if fold && !result.FoldUnsafe {
		result.Comments, foldInfo = foldResolvedThreads(result.Comments)
	}

	commentIDs := make([]pgtype.UUID, len(result.Comments))
	for i, c := range result.Comments {
		commentIDs[i] = c.ID
	}
	grouped := h.groupReactions(r, commentIDs)
	groupedAtt := h.groupAttachments(r, commentIDs)
	supplements := h.listCommentSupplements(r.Context(), issue.WorkspaceID, commentIDs)

	resp := make([]CommentResponse, len(result.Comments))
	for i, c := range result.Comments {
		cid := uuidToString(c.ID)
		resp[i] = commentToResponse(c, grouped[cid], groupedAtt[cid])
		applyCommentSupplements(&resp[i], supplements[cid])
		// Attach roots_only orientation stats when present (nil map elsewhere).
		if st, ok := result.RootStats[cid]; ok {
			rc := st.ReplyCount
			resp[i].ReplyCount = &rc
			if st.LastActivityAt.Valid {
				la := timestampToString(st.LastActivityAt)
				resp[i].LastActivityAt = &la
			}
			if c.ResolvedAt.Valid || st.ResolutionID.Valid {
				resolved := true
				resp[i].ThreadResolved = &resolved
			}
			if !c.ResolvedAt.Valid && st.ResolutionID.Valid {
				resp[i].ResolutionID = uuidToPtr(st.ResolutionID)
				content := st.ResolutionContent.String
				if summary {
					clipped, truncated := summarizeContent(content)
					content = clipped
					resp[i].ResolutionContentTruncated = &truncated
				}
				resp[i].ResolutionContent = &content
			}
		}
		// Attach fold annotations on a resolved thread's root (nil map elsewhere;
		// keyed by root id, so only root comments match).
		if st, ok := foldInfo[cid]; ok {
			resolved := true
			resp[i].ThreadResolved = &resolved
			fc := st.FoldedCount
			resp[i].FoldedCount = &fc
		}
		// Apply the summary projection last so it clips whatever content the
		// chosen read mode produced, uniformly across every mode.
		if summary {
			clipped, truncated := summarizeContent(resp[i].Content)
			resp[i].Content = clipped
			resp[i].ContentTruncated = &truncated
		}
	}

	// Emit the next cursor as response headers when the page is likely not
	// the last one. The cursor's meaning is context-dependent: under recent
	// it points at the oldest thread in the page (next page = older threads);
	// under thread + tail it points at the oldest reply in the page (next
	// page = older replies in the same thread). Headers stay out of the JSON
	// body so the default flat-array response shape — which the desktop UI
	// and existing callers depend on — is unchanged.
	if result.NextBefore != "" && result.NextBeforeID != "" {
		w.Header().Set("X-Multica-Next-Before", result.NextBefore)
		w.Header().Set("X-Multica-Next-Before-Id", result.NextBeforeID)
	}
	if result.CommentsTruncated {
		w.Header().Set(HeaderCommentsTruncated, "true")
	}

	writeJSON(w, http.StatusOK, resp)
}

// fetchCommentsArgs bundles the parsed query params so fetchCommentsForList
// stays readable. Sentinel errors below let the caller turn DB-layer outcomes
// into the right HTTP status without leaking SQL details.
//
// ThreadTail is split into a value + a "set" flag because tail=0 is a
// meaningful caller intent (return just the root). A bare int would collapse
// "user did not pass --tail" and "user passed --tail 0" into the same state,
// which would silently downgrade the latter to the full-thread path.
type fetchCommentsArgs struct {
	Issue         db.Issue
	Since         pgtype.Timestamptz
	RootsOnly     bool
	ThreadAnchor  string
	ThreadTail    int
	ThreadTailSet bool
	RecentN       int
	HasCursor     bool
	BeforeAt      pgtype.Timestamptz
	BeforeID      pgtype.UUID
}

// fetchCommentsResult carries both the materialised comments and (for the
// recent/thread-grouped path) the cursor to use for the next page. Cursor
// fields are empty strings when there is no next page or the path does not
// support cursors.
type fetchCommentsResult struct {
	Comments     []db.Comment
	NextBefore   string
	NextBeforeID string
	// RootStats carries per-root orientation stats keyed by comment id string.
	// Populated only on the roots_only path; nil for every other mode.
	RootStats map[string]rootStat
	// CommentsTruncated reports that a defensive cap omitted comments. Derived
	// from a probe read, never from the final response length.
	CommentsTruncated bool
	// FoldUnsafe reports that the response contains a known-partial thread.
	// Truncation alone does not imply this: completeCommentThreads makes the
	// default newest window safe to fold by restoring or dropping whole threads.
	FoldUnsafe bool
}

// rootStat is the per-thread orientation metadata attached to each root comment
// on the roots_only path. See CommentResponse.ReplyCount / LastActivityAt.
type rootStat struct {
	ReplyCount        int
	LastActivityAt    pgtype.Timestamptz
	ResolutionID      pgtype.UUID
	ResolutionContent pgtype.Text
}

var (
	errCommentThreadNotFound = &commentFetchError{"thread anchor not found"}
	errCommentThreadBadID    = &commentFetchError{"invalid thread anchor id"}
)

type commentFetchError struct{ msg string }

func (e *commentFetchError) Error() string { return e.msg }

func (h *Handler) fetchCommentsForList(ctx context.Context, args fetchCommentsArgs) (fetchCommentsResult, error) {
	issue := args.Issue

	// Thread-scoped read. Server resolves the anchor → root via recursive
	// CTE, so we don't have to assume two-layer flat threads here.
	if args.ThreadAnchor != "" {
		anchor, err := util.ParseUUID(args.ThreadAnchor)
		if err != nil {
			return fetchCommentsResult{}, errCommentThreadBadID
		}
		// Tailed path: paged query that returns root + the @reply_limit
		// most recent replies (per (created_at, id)). The thread root is
		// always returned, so a reader can land on a long thread without
		// dragging hundreds of replies into context. The reply-internal
		// cursor (--before / --before-id under --thread + --tail) scrolls
		// to older replies inside the same thread.
		if args.ThreadTailSet {
			// Probe for has-more by asking the SQL for one extra reply
			// beyond what the caller wants. If we get back >tail replies
			// there is at least one older reply still on disk; if we get
			// back ≤tail the page is the tail of the thread and there is
			// nothing older to scroll to (so we must NOT emit a cursor —
			// otherwise the next page is wasted round-trip that returns
			// just the root). This is the exact-boundary fix called out
			// in the MUL-2421 review.
			rows, err := h.Queries.ListThreadCommentsForIssuePaged(ctx, db.ListThreadCommentsForIssuePagedParams{
				AnchorID:    anchor,
				IssueID:     issue.ID,
				WorkspaceID: issue.WorkspaceID,
				HasCursor:   args.HasCursor,
				BeforeAt:    args.BeforeAt,
				BeforeID:    args.BeforeID,
				ReplyLimit:  int32(args.ThreadTail) + 1,
			})
			if err != nil {
				return fetchCommentsResult{}, err
			}
			if len(rows) == 0 {
				return fetchCommentsResult{}, errCommentThreadNotFound
			}
			// Split the result into root + replies (ASC order preserved).
			// Root is identified by parent_id IS NULL and is always
			// present in the SQL output; we keep it out of the cursor /
			// tail-trim logic so the user always sees thread context.
			var rootComment *db.Comment
			replies := make([]db.Comment, 0, len(rows))
			for _, r := range rows {
				c := db.Comment{
					ID:             r.ID,
					IssueID:        r.IssueID,
					AuthorType:     r.AuthorType,
					AuthorID:       r.AuthorID,
					Content:        r.Content,
					Type:           r.Type,
					CreatedAt:      r.CreatedAt,
					UpdatedAt:      r.UpdatedAt,
					ParentID:       r.ParentID,
					WorkspaceID:    r.WorkspaceID,
					ResolvedAt:     r.ResolvedAt,
					ResolvedByType: r.ResolvedByType,
					ResolvedByID:   r.ResolvedByID,
					SourceTaskID:   r.SourceTaskID,
					QuickActionID:  r.QuickActionID,
					Revision:       r.Revision,
					DeletedAt:      r.DeletedAt,
				}
				if !r.ParentID.Valid {
					root := c
					rootComment = &root
					continue
				}
				replies = append(replies, c)
			}
			// Trim the probe overflow back to the caller's tail. The SQL
			// emits ASC, so the extra row is the oldest reply — dropping
			// it from the head is what aligns "newest N" with the user's
			// request.
			hasMore := len(replies) > args.ThreadTail
			if hasMore {
				replies = replies[1:]
			}
			out := make([]db.Comment, 0, len(replies)+1)
			if rootComment != nil {
				out = append(out, *rootComment)
			}
			for _, r := range replies {
				// since drops stale rows AFTER the tail / cursor cut.
				// The root is exempt (already appended above): a reader
				// who set --since to skip already-seen replies still
				// needs the root context if the page only contained
				// the root.
				if args.Since.Valid && !r.CreatedAt.Time.After(args.Since.Time) {
					continue
				}
				out = append(out, r)
			}
			// Emit a reply cursor only when we proved an older reply
			// exists (hasMore). On an exact-boundary page (replyCount
			// == tail with no overflow) hasMore is false and the cursor
			// stays empty.
			//
			// Additionally suppress the cursor when `since` is set and
			// the oldest retained reply on this page is already <= since.
			// The next page walks replies strictly older than that one,
			// so every older reply has created_at strictly less — if the
			// cursor target itself can't satisfy `> since`, no older
			// reply can either, and continuing to paginate would only
			// return root-only pages until the agent walks the entire
			// pre-`since` history. This mirrors the head-thread guard on
			// the recent + since path. Flagged by Elon's second review on
			// MUL-2421.
			res := fetchCommentsResult{Comments: out}
			emitCursor := hasMore && len(replies) > 0
			if emitCursor && args.Since.Valid && !replies[0].CreatedAt.Time.After(args.Since.Time) {
				emitCursor = false
			}
			if emitCursor {
				oldest := replies[0]
				res.NextBefore = oldest.CreatedAt.Time.UTC().Format(time.RFC3339Nano)
				res.NextBeforeID = uuidToString(oldest.ID)
			}
			return res, nil
		}
		// Untailed reads use the same newest-reply query as the paged path.
		// Probe with commentHardCap replies: because the root is unconditional,
		// 2000 replies proves the total thread exceeds the 2000-row response cap.
		// We then drop the oldest reply and retain root + newest 1999 replies.
		rows, err := h.Queries.ListThreadCommentsForIssuePaged(ctx, db.ListThreadCommentsForIssuePagedParams{
			AnchorID:    anchor,
			IssueID:     issue.ID,
			WorkspaceID: issue.WorkspaceID,
			ReplyLimit:  int32(commentHardCap),
		})
		if err != nil {
			return fetchCommentsResult{}, err
		}
		if len(rows) == 0 {
			return fetchCommentsResult{}, errCommentThreadNotFound
		}
		var rootComment *db.Comment
		replies := make([]db.Comment, 0, len(rows))
		for _, r := range rows {
			c := db.Comment{
				ID:             r.ID,
				IssueID:        r.IssueID,
				AuthorType:     r.AuthorType,
				AuthorID:       r.AuthorID,
				Content:        r.Content,
				Type:           r.Type,
				CreatedAt:      r.CreatedAt,
				UpdatedAt:      r.UpdatedAt,
				ParentID:       r.ParentID,
				WorkspaceID:    r.WorkspaceID,
				ResolvedAt:     r.ResolvedAt,
				ResolvedByType: r.ResolvedByType,
				ResolvedByID:   r.ResolvedByID,
				SourceTaskID:   r.SourceTaskID,
				QuickActionID:  r.QuickActionID,
				Revision:       r.Revision,
				DeletedAt:      r.DeletedAt,
			}
			if !r.ParentID.Valid {
				root := c
				rootComment = &root
				continue
			}
			replies = append(replies, c)
		}
		truncated := len(replies) >= commentHardCap
		if truncated {
			replies = replies[1:]
		}
		out := make([]db.Comment, 0, len(replies)+1)
		if rootComment != nil && (!args.Since.Valid || rootComment.CreatedAt.Time.After(args.Since.Time)) {
			out = append(out, *rootComment)
		}
		for _, reply := range replies {
			if args.Since.Valid && !reply.CreatedAt.Time.After(args.Since.Time) {
				continue
			}
			out = append(out, reply)
		}
		return fetchCommentsResult{
			Comments:          out,
			CommentsTruncated: truncated,
			FoldUnsafe:        truncated,
		}, nil
	}

	// Thread-grouped recent read: N most recently active threads.
	if args.RecentN > 0 {
		rows, err := h.Queries.ListRecentThreadCommentsForIssue(ctx, db.ListRecentThreadCommentsForIssueParams{
			IssueID:     issue.ID,
			WorkspaceID: issue.WorkspaceID,
			HasCursor:   args.HasCursor,
			BeforeAt:    args.BeforeAt,
			BeforeID:    args.BeforeID,
			ThreadLimit: int32(args.RecentN),
		})
		if err != nil {
			return fetchCommentsResult{}, err
		}

		// The SQL already orders rows by (last_activity_at ASC, root_id ASC,
		// created_at ASC, id ASC), so the OLDEST-active thread sits at the
		// head and the FRESHEST thread at the tail. Walk the rows once to:
		//   1. Strip the thread-metadata columns down to db.Comment for the
		//      caller (uniform shape across paths).
		//   2. Count distinct threads in the page so we know whether a "next
		//      older page" is likely to exist.
		//   3. Capture the head thread's (last_activity_at, root_id) — that
		//      is the cursor for the next page (next page = threads strictly
		//      less recent than this one).
		comments := make([]db.Comment, 0, len(rows))
		var headRoot pgtype.UUID
		var headLast pgtype.Timestamptz
		seenRoot := map[string]struct{}{}
		for _, r := range rows {
			if !headRoot.Valid {
				headRoot = r.ThreadRootID
				headLast = r.ThreadLastActivityAt
			}
			seenRoot[uuidToString(r.ThreadRootID)] = struct{}{}
			// Since filter on the recent path: drop comments older than
			// `since`. Done in-memory so we keep the thread-grouped
			// semantics from the query (don't pre-filter rows before the
			// MAX(created_at) ranking — that would silently downgrade a
			// thread whose most recent activity falls inside the window).
			if args.Since.Valid && !r.CreatedAt.Time.After(args.Since.Time) {
				continue
			}
			comments = append(comments, db.Comment{
				ID:             r.ID,
				IssueID:        r.IssueID,
				AuthorType:     r.AuthorType,
				AuthorID:       r.AuthorID,
				Content:        r.Content,
				Type:           r.Type,
				CreatedAt:      r.CreatedAt,
				UpdatedAt:      r.UpdatedAt,
				ParentID:       r.ParentID,
				WorkspaceID:    r.WorkspaceID,
				ResolvedAt:     r.ResolvedAt,
				ResolvedByType: r.ResolvedByType,
				ResolvedByID:   r.ResolvedByID,
				SourceTaskID:   r.SourceTaskID,
				QuickActionID:  r.QuickActionID,
				Revision:       r.Revision,
				DeletedAt:      r.DeletedAt,
			})
		}

		// Only emit a cursor when the page is full. Fewer threads than
		// requested ⇒ the SELECT exhausted matching threads, so there is
		// no older page to scroll to.
		//
		// Additionally suppress the cursor when `since` is set and the head
		// thread's last_activity_at is already <= since. The pagination
		// walks threads in strictly decreasing last_activity_at, so every
		// older page has last_activity_at strictly less than the head's —
		// if the head itself can't satisfy `> since`, no older thread can
		// either. Predicating on the head (not on whether `comments` is
		// empty) also catches the mixed case where this page keeps rows
		// from fresher threads but the head thread is already past `since`.
		// Flagged by Elon in #2787's second review (MUL-2340 nit).
		out := fetchCommentsResult{Comments: comments}
		emitCursor := len(seenRoot) >= args.RecentN && headRoot.Valid && headLast.Valid
		if emitCursor && args.Since.Valid && !headLast.Time.After(args.Since.Time) {
			emitCursor = false
		}
		if emitCursor {
			out.NextBefore = headLast.Time.UTC().Format(time.RFC3339Nano)
			out.NextBeforeID = uuidToString(headRoot)
		}
		return out, nil
	}

	if args.RootsOnly {
		// Root-only read for issue-level orientation. This intentionally
		// stays separate from thread/recent modes: callers get the global
		// top-level discussion first, then fetch a specific thread only when
		// they need reply context. Each root carries reply_count +
		// last_activity_at so the reader can triage which thread to drill into.
		stats := map[string]rootStat{}
		if args.Since.Valid {
			rows, err := h.Queries.ListRootCommentsSinceForIssue(ctx, db.ListRootCommentsSinceForIssueParams{
				IssueID:     issue.ID,
				WorkspaceID: issue.WorkspaceID,
				Since:       args.Since,
				RowLimit:    commentProbeLimit(),
			})
			if err != nil {
				return fetchCommentsResult{}, err
			}
			truncated := len(rows) > commentHardCap
			if truncated {
				// Since is an incremental stream: keep the first page after the
				// cursor so callers can advance without skipping a gap.
				rows = rows[:commentHardCap]
			}
			comments := make([]db.Comment, len(rows))
			for i, r := range rows {
				comments[i] = db.Comment{
					ID: r.ID, IssueID: r.IssueID, AuthorType: r.AuthorType, AuthorID: r.AuthorID,
					Content: r.Content, Type: r.Type, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
					ParentID: r.ParentID, WorkspaceID: r.WorkspaceID, ResolvedAt: r.ResolvedAt,
					ResolvedByType: r.ResolvedByType, ResolvedByID: r.ResolvedByID,
					SourceTaskID: r.SourceTaskID, QuickActionID: r.QuickActionID, Revision: r.Revision,
					DeletedAt: r.DeletedAt,
				}
				stats[uuidToString(r.ID)] = rootStat{
					ReplyCount:        int(r.ReplyCount),
					LastActivityAt:    r.LastActivityAt,
					ResolutionID:      r.ResolutionID,
					ResolutionContent: r.ResolutionContent,
				}
			}
			return fetchCommentsResult{
				Comments: comments, RootStats: stats, CommentsTruncated: truncated,
			}, nil
		}

		rows, err := h.Queries.ListRootCommentsForIssue(ctx, db.ListRootCommentsForIssueParams{
			IssueID:     issue.ID,
			WorkspaceID: issue.WorkspaceID,
			RowLimit:    commentProbeLimit(),
		})
		if err != nil {
			return fetchCommentsResult{}, err
		}
		truncated := len(rows) > commentHardCap
		if truncated {
			// The SQL returns selected roots chronologically after taking the
			// newest probe window, so the overflow row is the oldest.
			rows = rows[1:]
		}
		comments := make([]db.Comment, len(rows))
		for i, r := range rows {
			comments[i] = db.Comment{
				ID: r.ID, IssueID: r.IssueID, AuthorType: r.AuthorType, AuthorID: r.AuthorID,
				Content: r.Content, Type: r.Type, CreatedAt: r.CreatedAt, UpdatedAt: r.UpdatedAt,
				ParentID: r.ParentID, WorkspaceID: r.WorkspaceID, ResolvedAt: r.ResolvedAt,
				ResolvedByType: r.ResolvedByType, ResolvedByID: r.ResolvedByID,
				SourceTaskID: r.SourceTaskID, QuickActionID: r.QuickActionID, Revision: r.Revision,
				DeletedAt: r.DeletedAt,
			}
			stats[uuidToString(r.ID)] = rootStat{
				ReplyCount:        int(r.ReplyCount),
				LastActivityAt:    r.LastActivityAt,
				ResolutionID:      r.ResolutionID,
				ResolutionContent: r.ResolutionContent,
			}
		}
		return fetchCommentsResult{
			Comments: comments, RootStats: stats, CommentsTruncated: truncated,
		}, nil
	}

	// Since reads keep their chronological page shape. A probe makes the cap
	// visible without skipping the next page of the incremental stream.
	if args.Since.Valid {
		comments, err := h.Queries.ListCommentsSinceForIssue(ctx, db.ListCommentsSinceForIssueParams{
			IssueID:     issue.ID,
			WorkspaceID: issue.WorkspaceID,
			CreatedAt:   args.Since,
			Limit:       commentProbeLimit(),
		})
		if err != nil {
			return fetchCommentsResult{}, err
		}
		truncated := len(comments) > commentHardCap
		if truncated {
			comments = comments[:commentHardCap]
		}
		return fetchCommentsResult{Comments: comments, CommentsTruncated: truncated}, nil
	}
	comments, err := h.Queries.ListCommentsForIssue(ctx, db.ListCommentsForIssueParams{
		IssueID:     issue.ID,
		WorkspaceID: issue.WorkspaceID,
		Limit:       commentProbeLimit(),
	})
	if err != nil {
		return fetchCommentsResult{}, err
	}
	// Probe read: an issue holding exactly commentHardCap comments is complete,
	// and calling that truncated would needlessly suppress the thread fold below.
	truncated := len(comments) > commentHardCap
	if truncated {
		comments = comments[len(comments)-commentHardCap:]
		// The cap keeps the NEWEST comments (MUL-5492), so a thread can be cut
		// in half: an old root or sibling outside the window, a fresh reply
		// inside it. Complete every affected thread within the shared context
		// budget. If a thread cannot be completed, it is dropped as one unit.
		//
		comments, err = h.completeCommentThreads(ctx, issue.ID, issue.WorkspaceID, comments)
		if err != nil {
			return fetchCommentsResult{}, err
		}
	}
	return fetchCommentsResult{Comments: comments, CommentsTruncated: truncated}, nil
}

// completeCommentThreads restores complete renderable threads around a newest-N
// comment window.
//
// Why this is needed at all: capping with the OLDEST n comments could never
// orphan a reply, because a reply is always newer than its parent and so a
// prefix of the timeline is closed under "parent of". A newest-n window is a
// suffix and has no such property — an old thread root falls outside while a
// fresh reply to it stays inside (MUL-5492).
//
// Parent closure comes first because an orphaned reply is not merely mis-nested,
// it is invisible.
// The timeline builds its top level from "activities + comments with no
// parent_id" and renders replies by looking them up under their parent, so an
// orphan sits in the map with no card to render it — the exact shape of MUL-1847
// (1 root + 29 replies, root dropped, all 29 vanished from the UI while the API
// returned them).
//
// Parent closure alone is insufficient for the human timeline. Web and Desktop
// derive resolution bars and folded counts from the comments they receive, so a
// backfilled root plus only its newest replies would make a partial thread look
// complete. After reaching roots, this function walks DOWN from every restored
// root and adds all missing siblings/descendants. If the shared budget or depth
// bound cannot prove a whole affected thread complete, that thread is removed
// as one unit; old installed clients can therefore never fold partial data.
//
// Both walks share commentThreadContextBudget, so the returned set remains
// provably bounded by commentHardCap + commentThreadContextBudget. Descendants
// are fetched one level at a time for every affected root in one query, avoiding
// an N+1 query per thread. Each query carries issue + workspace predicates and a
// probe limit.
//
// When a budget is exhausted, a parent row is missing, or a parent belongs to
// another issue or workspace, keepRootConnected drops the affected replies. A
// descendant overflow drops every affected thread rather than guessing which
// root is complete. Returning fewer new replies is a conservative degradation
// that the caller already signals as truncated; leaking another tenant's
// comments, returning an unbounded response, or emitting partial threads are all
// worse.
func (h *Handler) completeCommentThreads(ctx context.Context, issueID, workspaceID pgtype.UUID, window []db.Comment) ([]db.Comment, error) {
	if len(window) == 0 {
		return window, nil
	}

	initialIDs := make(map[string]struct{}, len(window))
	byID := make(map[string]db.Comment, len(window)+commentThreadContextBudget/8)
	through := window[0]
	for _, c := range window {
		id := uuidToString(c.ID)
		initialIDs[id] = struct{}{}
		byID[id] = c
		if c.CreatedAt.Time.After(through.CreatedAt.Time) ||
			(c.CreatedAt.Time.Equal(through.CreatedAt.Time) &&
				bytes.Compare(c.ID.Bytes[:], through.ID.Bytes[:]) > 0) {
			through = c
		}
	}

	added := 0
	for depth := 0; depth < commentThreadMaxDepth; depth++ {
		// Distinct parents referenced by the current set but not in it. Dedup is
		// what keeps many replies sharing one ancestor from fetching it twice.
		var missing []pgtype.UUID
		seen := make(map[string]struct{})
		for _, c := range byID {
			if !c.ParentID.Valid {
				continue
			}
			pid := uuidToString(c.ParentID)
			if _, have := byID[pid]; have {
				continue
			}
			if _, dup := seen[pid]; dup {
				continue
			}
			seen[pid] = struct{}{}
			missing = append(missing, c.ParentID)
		}
		if len(missing) == 0 {
			break
		}
		if added+len(missing) > commentThreadContextBudget {
			break
		}

		// Tenant-scoped: a parent in another issue or workspace simply does not
		// come back, and keepRootConnected then prunes the reply that pointed at
		// it. The filter is on every level, not just the first.
		rows, err := h.Queries.ListCommentsByIDsForIssue(ctx, db.ListCommentsByIDsForIssueParams{
			Ids:         missing,
			IssueID:     issueID,
			WorkspaceID: workspaceID,
		})
		if err != nil {
			return nil, err
		}
		if len(rows) == 0 {
			break
		}
		for _, r := range rows {
			id := uuidToString(r.ID)
			if _, have := byID[id]; have {
				continue
			}
			byID[id] = r
			added++
		}
	}

	// Remove every chain that did not reach a same-tenant root within the
	// ancestor budget/depth. Rebuild the map so the descendant phase cannot
	// accidentally revive a disconnected chain.
	connected := keepRootConnected(byID)
	byID = make(map[string]db.Comment, len(connected)+commentThreadContextBudget-added)
	for _, c := range connected {
		byID[uuidToString(c.ID)] = c
	}

	// A restored root identifies a thread that the newest window cut. Threads
	// whose roots were already in the suffix are complete under the normal
	// parent-before-reply timestamp contract and need no extra query.
	affectedRoots := make(map[string]struct{})
	for id, c := range byID {
		if c.ParentID.Valid {
			continue
		}
		if _, wasInWindow := initialIDs[id]; !wasInWindow {
			affectedRoots[id] = struct{}{}
		}
	}

	if len(affectedRoots) > 0 {
		complete, err := h.addMissingCommentDescendants(
			ctx,
			issueID,
			workspaceID,
			byID,
			affectedRoots,
			through,
			&added,
		)
		if err != nil {
			return nil, err
		}
		if !complete {
			dropCommentThreads(byID, affectedRoots)
		}
	}

	out := make([]db.Comment, 0, len(byID))
	for _, c := range byID {
		out = append(out, c)
	}
	sortCommentsChronologically(out)
	return out, nil
}

// addMissingCommentDescendants walks every affected thread breadth-first. It
// returns false when the remaining context budget or depth bound cannot prove
// all affected threads complete; callers then drop those threads as one unit.
func (h *Handler) addMissingCommentDescendants(
	ctx context.Context,
	issueID, workspaceID pgtype.UUID,
	byID map[string]db.Comment,
	affectedRoots map[string]struct{},
	through db.Comment,
	added *int,
) (bool, error) {
	frontier := make(map[string]string, len(affectedRoots)) // comment id -> root id
	for rootID := range affectedRoots {
		frontier[rootID] = rootID
	}

	// Every returned child is unique in a tree walk. The scan budget includes
	// rows already present in the window plus the remaining add budget, so all
	// descendant queries together inspect at most the response bound (+1 probe).
	scanBudget := len(byID) + commentThreadContextBudget - *added
	for depth := 0; depth <= commentThreadMaxDepth; depth++ {
		parentKeys := make([]string, 0, len(frontier))
		for id := range frontier {
			parentKeys = append(parentKeys, id)
		}
		sort.Strings(parentKeys)

		parentIDs := make([]pgtype.UUID, len(parentKeys))
		for i, id := range parentKeys {
			parentIDs[i] = byID[id].ID
		}

		rows, err := h.Queries.ListChildCommentsForParents(ctx, db.ListChildCommentsForParentsParams{
			ParentIds:   parentIDs,
			IssueID:     issueID,
			WorkspaceID: workspaceID,
			ThroughAt:   through.CreatedAt,
			ThroughID:   through.ID,
			RowLimit:    int32(scanBudget + 1),
		})
		if err != nil {
			return false, err
		}
		if len(rows) == 0 {
			return true, nil
		}
		if len(rows) > scanBudget || depth == commentThreadMaxDepth {
			return false, nil
		}
		scanBudget -= len(rows)

		next := make(map[string]string, len(rows))
		for _, row := range rows {
			parentID := uuidToString(row.ParentID)
			rootID, ok := frontier[parentID]
			if !ok {
				// The query is constrained to frontier parent ids, so this is
				// defensive fail-closed handling for malformed query output.
				return false, nil
			}
			id := uuidToString(row.ID)
			next[id] = rootID
			if _, exists := byID[id]; exists {
				continue
			}
			if *added >= commentThreadContextBudget {
				return false, nil
			}
			byID[id] = row
			*added = *added + 1
		}
		frontier = next
	}
	return false, nil
}

// dropCommentThreads removes every comment whose root is in roots. It is used
// only after a bounded descendant walk could not prove the affected threads
// complete.
func dropCommentThreads(byID map[string]db.Comment, roots map[string]struct{}) {
	for id := range byID {
		rootID, ok := commentRootID(id, byID)
		if !ok {
			delete(byID, id)
			continue
		}
		if _, drop := roots[rootID]; drop {
			delete(byID, id)
		}
	}
}

func commentRootID(id string, byID map[string]db.Comment) (string, bool) {
	seen := make(map[string]struct{})
	for current := id; ; {
		if _, loop := seen[current]; loop {
			return "", false
		}
		seen[current] = struct{}{}

		comment, ok := byID[current]
		if !ok {
			return "", false
		}
		if !comment.ParentID.Valid {
			return current, true
		}
		current = uuidToString(comment.ParentID)
	}
}

func sortCommentsChronologically(comments []db.Comment) {
	sort.Slice(comments, func(i, j int) bool {
		if !comments[i].CreatedAt.Time.Equal(comments[j].CreatedAt.Time) {
			return comments[i].CreatedAt.Time.Before(comments[j].CreatedAt.Time)
		}
		return bytes.Compare(comments[i].ID.Bytes[:], comments[j].ID.Bytes[:]) < 0
	})
}

// keepRootConnected returns only the comments whose parent chain terminates at a
// root (parent_id IS NULL) inside the set.
//
// A comment whose chain leaves the set, loops, or dead-ends is unrenderable: the
// UI needs a top-level card to hang the thread from. Dropping such a comment also
// drops its descendants, because their chains run through it and therefore fail
// the same test — no separate descendant pass is needed.
//
// The walk is bounded by the visited set, so a cycle in stored data terminates
// instead of hanging. Real cycles are impossible via the write path, but a graph
// walk over stored data should never assume that.
func keepRootConnected(byID map[string]db.Comment) []db.Comment {
	connected := make(map[string]bool, len(byID))

	for id := range byID {
		if _, decided := connected[id]; decided {
			continue
		}
		var path []string
		visited := make(map[string]struct{})
		verdict := false
		for cur := id; ; {
			if v, decided := connected[cur]; decided {
				verdict = v
				break
			}
			if _, loop := visited[cur]; loop {
				break // cycle: nothing on this path reaches a root
			}
			c, present := byID[cur]
			if !present {
				break // chain leaves the set
			}
			visited[cur] = struct{}{}
			path = append(path, cur)
			if !c.ParentID.Valid {
				verdict = true // a real root, inside the set
				break
			}
			cur = uuidToString(c.ParentID)
		}
		for _, p := range path {
			connected[p] = verdict
		}
	}

	out := make([]db.Comment, 0, len(byID))
	for id, c := range byID {
		if connected[id] {
			out = append(out, c)
		}
	}
	return out
}

type CreateCommentRequest struct {
	Content          string   `json:"content"`
	Type             string   `json:"type"`
	ParentID         *string  `json:"parent_id"`
	AttachmentIDs    []string `json:"attachment_ids"`
	SuppressAgentIDs []string `json:"suppress_agent_ids"`
	// Retired scheduling inputs are accepted for installed-client compatibility
	// and ignored. All comments enter the owning issue session's inbox.
	SteerTaskIDs []string `json:"steer_task_ids"`
	Steer        bool     `json:"steer"`
}

type CommentTriggerPreviewRequest struct {
	Content          string  `json:"content"`
	ParentID         *string `json:"parent_id"`
	EditingCommentID *string `json:"editing_comment_id"`
}

type CommentTriggerPreviewResponse struct {
	Agents []CommentTriggerAgentResponse `json:"agents"`
	// Blocked reports an owner the current actor cannot invoke. Mentions do
	// not select additional execution targets.
	Blocked []CommentTriggerOutcome `json:"blocked,omitempty"`
}

type CommentTriggerAgentResponse struct {
	ID        string  `json:"id"`
	Name      string  `json:"name"`
	AvatarURL *string `json:"avatar_url,omitempty"`
	Source    string  `json:"source"`
	Reason    string  `json:"reason"`
}

func (h *Handler) PreviewCommentTriggers(w http.ResponseWriter, r *http.Request) {
	issue, ok := h.loadIssueForUser(w, r, chi.URLParam(r, "id"))
	if !ok {
		return
	}
	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	var req CommentTriggerPreviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	resp := CommentTriggerPreviewResponse{Agents: []CommentTriggerAgentResponse{}}
	if sanitizeNullBytes(req.Content) == "" || issue.AssigneeType.String != "agent" || !issue.AssigneeID.Valid {
		writeJSON(w, http.StatusOK, resp)
		return
	}
	actorType, actorID := h.resolveActor(r, userID, uuidToString(issue.WorkspaceID))
	if actorType == "agent" && actorID == uuidToString(issue.AssigneeID) {
		writeJSON(w, http.StatusOK, resp)
		return
	}
	agent, err := h.Queries.GetAgentInWorkspace(r.Context(), db.GetAgentInWorkspaceParams{ID: issue.AssigneeID, WorkspaceID: issue.WorkspaceID})
	if err == nil && h.canInvokeAgent(r.Context(), agent, actorType, actorID, h.invokeOriginatorFromRequest(r, actorType, actorID), uuidToString(issue.WorkspaceID)) {
		resp.Agents = append(resp.Agents, CommentTriggerAgentResponse{ID: uuidToString(agent.ID), Name: agent.Name, AvatarURL: h.resolveAvatarURLPtr(textToPtr(agent.AvatarUrl)), Source: "issue_assignee", Reason: "This message is delivered to the issue's persistent agent session."})
	} else {
		resp.Blocked = []CommentTriggerOutcome{{TargetType: "agent", TargetID: uuidToString(issue.AssigneeID), Status: DispatchBlocked, ReasonCode: ReasonInvocationNotAllowed}}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) CreateComment(w http.ResponseWriter, r *http.Request) {
	r = h.withWakeupActor(r)
	issueID := chi.URLParam(r, "id")
	issue, ok := h.loadIssueForUser(w, r, issueID)
	if !ok {
		return
	}

	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}

	var req CreateCommentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Strip bytes PostgreSQL's TEXT column rejects before the empty check. The
	// case reachable over the JSON API is an embedded NUL (0x00, SQLSTATE
	// 22021) that survives a JSON round trip; sanitizeNullBytes also drops
	// invalid UTF-8 as defense-in-depth. A stray such byte in agent-written
	// content (notably via --content-file) otherwise fails the INSERT with an
	// opaque 500 the CLI renders as "server unavailable" and retries forever —
	// the plausible cause of GH #5388. Mirrors the skill-import sanitization;
	// normalizing first means all-NUL content is correctly treated as empty.
	req.Content = sanitizeNullBytes(req.Content)
	if req.Content == "" {
		writeError(w, http.StatusBadRequest, "content is required")
		return
	}
	if req.Type == "" {
		req.Type = "comment"
	}
	// Validate rather than letting the DB CHECK reject it: an unknown type
	// previously surfaced as a 500 on a constraint violation, which reads as a
	// server fault for what is plainly bad input. This is also the explicit
	// refusal of a client trying to author a machine-written kind (MUL-5465
	// review): a member must not be able to post a comment that renders as
	// something the system generated.
	if !isClientAuthorableCommentType(req.Type) {
		writeError(w, http.StatusBadRequest, "invalid comment type")
		return
	}

	var parentID pgtype.UUID
	if req.ParentID != nil {
		var parsed pgtype.UUID
		parsed, ok = parseUUIDOrBadRequest(w, *req.ParentID, "parent_id")
		if !ok {
			return
		}
		parentID = parsed
		parent, err := h.Queries.GetComment(r.Context(), parentID)
		if err != nil || uuidToString(parent.IssueID) != uuidToString(issue.ID) {
			writeError(w, http.StatusBadRequest, "invalid parent comment")
			return
		}
	}

	attachmentIDs, ok := parseUUIDSliceOrBadRequest(w, req.AttachmentIDs, "attachment_ids")
	if !ok {
		return
	}
	// Installed clients may still send the retired routing fields. They do
	// not choose a turn, suppress the owner, or create a separate execution.
	// Determine author identity: agent (via X-Agent-ID header) or member.
	authorType, authorID := h.resolveActor(r, userID, uuidToString(issue.WorkspaceID))

	// Task-token middleware stamps the trusted authoring agent and task. Keep
	// that provenance for issue-session events, including writes on another
	// issue, without borrowing the target issue's human authority.
	var sourceTaskID pgtype.UUID
	if authorType == "agent" {
		if task, ok := h.taskFromRequestHeader(r); ok {
			if task.IssueID.Valid && uuidToString(task.IssueID) == uuidToString(issue.ID) {
				noAction, checkErr := service.HasSquadLeaderNoActionEvaluationForTask(r.Context(), h.Queries, task)
				if checkErr != nil {
					slog.Warn("checking squad leader no_action evaluation failed", append(logger.RequestAttrs(r),
						"error", checkErr,
						"task_id", uuidToString(task.ID),
						"issue_id", issueID,
					)...)
				} else if noAction {
					writeError(w, http.StatusConflict, "squad leader recorded no_action; comments are not allowed for this task")
					return
				}
			}
			sourceTaskID = task.ID
		}
	}

	// NOTE: Comment content is stored as Markdown source. XSS is handled at the
	// rendering layer (rehype-sanitize) and at the editor layer
	// (@tiptap/markdown with html:false). Running an HTML sanitizer here would
	// entity-encode Markdown syntax characters (>, ", &, <) and corrupt the
	// source. See issue #1303 / discussion in MUL-1119, MUL-1125.

	// parent_id stores the exact comment being replied to. Thread-level behavior
	// (for example auto-unresolving a resolved thread) resolves the root
	// separately so storing a reply-to-reply does not destroy the direct-parent
	// signal used by trigger decisions.
	var rootComment *db.Comment
	if parentID.Valid {
		if root, err := h.Queries.GetThreadRoot(r.Context(), db.GetThreadRootParams{
			CommentID:   parentID,
			WorkspaceID: issue.WorkspaceID,
		}); err == nil {
			rootComment = &root
		}
	}

	createParams := db.CreateCommentParams{
		ID:           dbid.NewV7(),
		IssueID:      issue.ID,
		WorkspaceID:  issue.WorkspaceID,
		AuthorType:   authorType,
		AuthorID:     parseUUID(authorID),
		Content:      req.Content,
		Type:         req.Type,
		ParentID:     parentID,
		SourceTaskID: sourceTaskID,
		// The author's "don't start" choices outlive this call: completion
		// replay skips the agents it names.
		SuppressedAgentIds: nil,
	}
	var created db.CreateCommentRow
	var err error
	if len(attachmentIDs) > 0 {
		// A comment and the attachments it was posted with are one visible
		// change, and one database outcome. Committing the comment first leaves
		// a window in which it can gain a reply and be deleted into a
		// tombstone, and the link would then bind uploaded objects to that
		// placeholder — invisible, and missed by the prune's storage cleanup
		// (#8296 review). Inside this transaction the row is not yet visible to
		// a delete, so there is no window at all. Mirrors the attachment-set
		// edit in UpdateComment.
		//
		// Owner first: the CreateComment statement takes the issue row, and only
		// then are the attachments locked — the issue -> comment -> child order
		// LockIssueForDelete, UpdateComment, LockLiveComment and DeleteAttachment
		// all share. Taking the attachments first inverts it against issue
		// teardown, which holds the issue and then reaches the same rows through
		// the issue_id cascade, and Postgres aborts one side. Locking them at all
		// still pins the requested set: an attachment deleted while this waited
		// is refused before the comment is committed, rather than the comment
		// committing without it.
		tx, beginErr := h.beginWakeupWrite(r.Context())
		if beginErr != nil {
			slog.Warn("create comment failed", append(logger.RequestAttrs(r), "error", beginErr, "issue_id", issueID)...)
			writeError(w, http.StatusInternalServerError, "failed to create comment")
			return
		}
		defer tx.Rollback(r.Context())
		qtx := h.Queries.WithTx(tx)
		created, err = qtx.CreateComment(r.Context(), createParams)
		if err == nil {
			var missing pgtype.UUID
			missing, err = lockCommentAttachments(r.Context(), qtx, issue.WorkspaceID, issue.ID, attachmentIDs)
			if err == nil && missing.Valid {
				writeError(w, http.StatusConflict, "attachment "+uuidToString(missing)+" is no longer available")
				return
			}
		}
		if err == nil {
			err = qtx.LinkAttachmentsToComment(r.Context(), db.LinkAttachmentsToCommentParams{
				CommentID: created.ID,
				IssueID:   issue.ID,
				Column3:   attachmentIDs,
			})
		}
		if err == nil {
			err = tx.Commit(r.Context())
		}
	} else {
		created, err = wakeupWrite(h, r, func(q *db.Queries) (db.CreateCommentRow, error) {
			return q.CreateComment(r.Context(), createParams)
		})
	}
	if errors.Is(err, pgx.ErrNoRows) {
		// The issue was deleted, possibly while this waited for its row lock.
		writeError(w, http.StatusNotFound, "issue not found")
		return
	}
	if err != nil {
		slog.Warn("create comment failed", append(logger.RequestAttrs(r), "error", err, "issue_id", issueID)...)
		writeError(w, http.StatusInternalServerError, "failed to create comment")
		return
	}
	comment := created.Comment()

	// Fetch linked attachments so the response includes them.
	groupedAtt := h.groupAttachments(r, []pgtype.UUID{comment.ID})
	resp := commentToResponse(comment, nil, groupedAtt[uuidToString(comment.ID)])
	resp.IssueRevision = created.IssueRevision
	slog.Info("comment created", append(logger.RequestAttrs(r), "comment_id", uuidToString(comment.ID), "issue_id", issueID)...)
	h.publish(protocol.EventCommentCreated, uuidToString(issue.WorkspaceID), authorType, authorID, map[string]any{
		"comment":             resp,
		"issue_title":         issue.Title,
		"issue_assignee_type": textToPtr(issue.AssigneeType),
		"issue_assignee_id":   uuidToPtr(issue.AssigneeID),
		"issue_status":        issue.Status,
		"issue_revision":      created.IssueRevision,
	})

	// A reply in a resolved thread re-opens it. Done after CreateComment commits
	// so the reply is visible regardless of the unresolve outcome. Shared with
	// the agent task path (TaskService.createAgentComment) — both reply paths
	// must keep the resolved root in sync.
	h.TaskService.AutoUnresolveThreadOnReply(r.Context(), rootComment, uuidToString(issue.WorkspaceID), authorType, authorID, h.wakeupSourceTaskID(r))

	originatorUserID := h.invokeOriginatorFromRequest(r, authorType, authorID)
	// Preserve the comment even when session delivery is blocked, and surface
	// the owner outcome privately to the author.
	resp.TriggerOutcomes = h.commentSessionOutcomes(r.Context(), issue, authorType, authorID, originatorUserID)

	writeJSON(w, http.StatusCreated, resp)
}

// clientAuthorableCommentTypes is what POST /comments accepts. `status_change`
// and `system` are platform-generated narration, so a client claiming them
// would be forging system output; they are deliberately absent.
var clientAuthorableCommentTypes = map[string]struct{}{
	"comment":         {},
	"progress_update": {},
}

func isClientAuthorableCommentType(t string) bool {
	_, ok := clientAuthorableCommentTypes[t]
	return ok
}

// commentSessionOutcomes reports delivery to the assigned issue session. The
// database mutation already appended the ordered event; this hook only wakes
// its runtime. Polling recovers a lost notification without replaying comments.
func (h *Handler) commentSessionOutcomes(ctx context.Context, issue db.Issue, actorType, actorID, originatorUserID string) []CommentTriggerOutcome {
	if issue.AssigneeType.String != "agent" || !issue.AssigneeID.Valid || (actorType == "agent" && actorID == uuidToString(issue.AssigneeID)) {
		return nil
	}
	outcome := CommentTriggerOutcome{TargetType: "agent", TargetID: uuidToString(issue.AssigneeID), Status: DispatchDeferred, ReasonCode: ReasonDeferred}
	agent, err := h.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: issue.AssigneeID, WorkspaceID: issue.WorkspaceID})
	if err != nil || !h.canInvokeAgent(ctx, agent, actorType, actorID, originatorUserID, uuidToString(issue.WorkspaceID)) {
		outcome.Status, outcome.ReasonCode = DispatchBlocked, ReasonInvocationNotAllowed
	}
	h.wakeAssignedIssueSession(ctx, issue)
	return []CommentTriggerOutcome{outcome}
}

func (h *Handler) wakeAssignedIssueSession(ctx context.Context, issue db.Issue) {
	if issue.AssigneeType.String != "agent" || !issue.AssigneeID.Valid {
		return
	}
	agent, err := h.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: issue.AssigneeID, WorkspaceID: issue.WorkspaceID})
	if err != nil || !agent.RuntimeID.Valid {
		return
	}
	if _, err := h.TaskService.WakeIssueAgentSessionsForRuntime(ctx, agent.RuntimeID, 32); err != nil {
		slog.Warn("wake issue agent session failed; inbox remains durable", "issue_id", uuidToString(issue.ID), "error", err)
	}
}

func (h *Handler) UpdateComment(w http.ResponseWriter, r *http.Request) {
	r = h.withWakeupActor(r)
	commentId := chi.URLParam(r, "commentId")

	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}
	commentUUID, ok := parseUUIDOrBadRequest(w, commentId, "comment id")
	if !ok {
		return
	}

	// Load comment scoped to current workspace.
	workspaceID := h.resolveWorkspaceID(r)
	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace id")
	if !ok {
		return
	}
	existing, err := h.Queries.GetCommentInWorkspace(r.Context(), db.GetCommentInWorkspaceParams{
		ID:          commentUUID,
		WorkspaceID: wsUUID,
	})
	// A deleted comment's tombstone has nothing left to edit.
	if err != nil || existing.DeletedAt.Valid {
		writeError(w, http.StatusNotFound, "comment not found")
		return
	}
	if bound, err := h.Queries.CommentHasTaskSupplement(r.Context(), db.CommentHasTaskSupplementParams{
		CommentID: existing.ID, WorkspaceID: wsUUID,
	}); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to verify additional message")
		return
	} else if bound {
		writeError(w, http.StatusConflict, "additional messages cannot be edited")
		return
	}

	member, ok := h.workspaceMember(w, r, workspaceID)
	if !ok {
		return
	}

	actorType, actorID := h.resolveActor(r, userID, workspaceID)
	isAuthor := existing.AuthorType == actorType && uuidToString(existing.AuthorID) == actorID
	isAdmin := roleAllowed(member.Role, "owner", "admin")
	if !isAuthor && !isAdmin {
		writeError(w, http.StatusForbidden, "only comment author or admin can edit")
		return
	}

	var req struct {
		Content          string    `json:"content"`
		ContentBase      *string   `json:"content_base,omitempty"`
		AttachmentIDs    *[]string `json:"attachment_ids"`
		SuppressAgentIDs []string  `json:"suppress_agent_ids"`
		ExpectedRevision *int64    `json:"expected_revision,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	// See CreateComment: strip NUL / invalid-UTF-8 bytes PostgreSQL's TEXT column
	// rejects before the empty check, so an edit that introduces such a byte
	// can't 500 (GH #5388).
	req.Content = sanitizeNullBytes(req.Content)
	if req.ContentBase != nil {
		sanitized := sanitizeNullBytes(*req.ContentBase)
		req.ContentBase = &sanitized
	}
	if req.Content == "" {
		writeError(w, http.StatusBadRequest, "content is required")
		return
	}
	if req.ExpectedRevision != nil {
		if *req.ExpectedRevision < 1 {
			writeError(w, http.StatusBadRequest, "expected_revision must be a positive integer")
			return
		}
		if existing.Revision != *req.ExpectedRevision {
			writeRevisionConflict(w, "comment", existing.ID, *req.ExpectedRevision, existing.Revision)
			return
		}
	}
	strictContentEdit := req.ContentBase != nil || req.ExpectedRevision != nil

	var attachmentIDs []pgtype.UUID
	replaceAttachments := req.AttachmentIDs != nil
	if replaceAttachments {
		var ok bool
		attachmentIDs, ok = parseUUIDSliceOrBadRequest(w, *req.AttachmentIDs, "attachment_ids")
		if !ok {
			return
		}
	}
	// NOTE: See CreateComment — Markdown is sanitized at render/edit time, not here.

	oldContent := existing.Content
	// Preserve the existing authority lineage by default — this path is taken only
	// for an UNCHANGED edit (no re-trigger). When the content changes below, the
	// lineage is re-derived from the EDIT action itself (MUL-4857), never carried
	// over from the comment's original authoring run.
	sourceTaskID := existing.SourceTaskID
	var triggerIssue *db.Issue
	if oldContent != req.Content {
		issue, err := h.Queries.GetIssue(r.Context(), existing.IssueID)
		if err != nil {
			slog.Warn("load issue for edit post-processing failed", "issue_id", uuidToString(existing.IssueID), "error", err)
			writeError(w, http.StatusInternalServerError, "failed to load issue")
			return
		}
		triggerIssue = &issue
		// A content edit is a NEW action, so its delegation lineage must key on THIS
		// edit. Only the AGENT author re-editing its OWN comment carries the lineage
		// forward (commentSourceTaskID re-stamps the current editing task) — so
		// preview, save, and the deferred completion-reconcile all resolve the
		// authority from this one action. Any OTHER editor — a workspace owner/admin
		// editing an AGENT's comment, or a member editing their own — CLEARS the
		// lineage so the deferred reconcile fails closed instead of resurrecting the
		// original autopilot run's creator authority. An admin holds manage rights
		// over the comment, not invoke rights over the author's private agents (Elon
		// must-fix, round 3).
		if actorType == "agent" && isAuthor {
			sourceTaskID = h.commentSourceTaskID(r)
		} else {
			sourceTaskID = pgtype.UUID{}
		}

	}

	updateParams := db.UpdateCommentParams{
		ID:           commentUUID,
		Content:      req.Content,
		SourceTaskID: sourceTaskID,
		// New text replaces the "don't start" choices completion replay keeps.
		SuppressedAgentIds: nil,
	}
	if req.ContentBase != nil {
		updateParams.ContentBase = pgtype.Text{String: *req.ContentBase, Valid: true}
	}
	if req.ExpectedRevision != nil {
		updateParams.ExpectedRevision = pgtype.Int8{Int64: *req.ExpectedRevision, Valid: true}
	}
	var comment db.Comment
	var issueRevision int64
	transactionalEdit := replaceAttachments || (oldContent != req.Content && strictContentEdit)
	if transactionalEdit {
		// Strict body edits, attachment-set edits, and cancellation of tasks built
		// from the old body are one database outcome. UpdateComment takes the row
		// lock before the attachment replacement, so two modern editors cannot
		// interleave their CAS check and attachment selection. A body + attachment
		// edit is one visible mutation and therefore bumps revision exactly once.
		tx, beginErr := h.beginWakeupWrite(r.Context())
		if beginErr != nil {
			writeError(w, http.StatusInternalServerError, "failed to prepare comment edit")
			return
		}
		defer tx.Rollback(r.Context())
		qtx := h.Queries.WithTx(tx)
		var updated db.UpdateCommentRow
		updated, err = qtx.UpdateComment(r.Context(), updateParams)
		if err == nil {
			comment = updated.Comment()
			issueRevision = updated.IssueRevision
		}

		if err == nil && replaceAttachments {
			var changed int64
			changed, err = qtx.ReplaceCommentAttachments(r.Context(), db.ReplaceCommentAttachmentsParams{
				CommentID:     comment.ID,
				IssueID:       existing.IssueID,
				AttachmentIds: attachmentIDs,
			})
			if err == nil && changed > 0 && oldContent == req.Content {
				comment, err = qtx.BumpCommentRevision(r.Context(), db.BumpCommentRevisionParams{
					ID:          comment.ID,
					WorkspaceID: existing.WorkspaceID,
				})
			}
		}
		if err == nil {
			err = tx.Commit(r.Context())
		}
	} else {
		var updated db.UpdateCommentRow
		updated, err = wakeupWrite(h, r, func(q *db.Queries) (db.UpdateCommentRow, error) {
			return q.UpdateComment(r.Context(), updateParams)
		})
		if err == nil {
			comment = updated.Comment()
			issueRevision = updated.IssueRevision
		}
	}
	if err != nil {
		slog.Warn("update comment failed", append(logger.RequestAttrs(r), "error", err, "comment_id", commentId)...)

		if errors.Is(err, pgx.ErrNoRows) {
			current, reloadErr := h.Queries.GetCommentInWorkspace(r.Context(), db.GetCommentInWorkspaceParams{ID: commentUUID, WorkspaceID: wsUUID})
			if errors.Is(reloadErr, pgx.ErrNoRows) || (reloadErr == nil && current.DeletedAt.Valid) {
				// Deleted while the edit waited for it.
				writeError(w, http.StatusNotFound, "comment not found")
				return
			}
			if reloadErr == nil && strictContentEdit {
				if req.ExpectedRevision != nil {
					writeRevisionConflict(w, "comment", current.ID, *req.ExpectedRevision, current.Revision)
				} else {
					writeEditConflict(w, "comment", current.ID)
				}
				return
			}
		}
		writeError(w, http.StatusInternalServerError, "failed to update comment")
		return
	}
	commentEventOutcomes := func() []CommentTriggerOutcome {
		if oldContent == comment.Content {
			return nil
		}
		return h.commentSessionOutcomes(r.Context(), *triggerIssue, actorType, actorID, h.invokeOriginatorFromRequest(r, actorType, actorID))
	}

	// Fetch reactions and attachments for the updated comment.
	grouped := h.groupReactions(r, []pgtype.UUID{comment.ID})
	groupedAtt := h.groupAttachments(r, []pgtype.UUID{comment.ID})
	cid := uuidToString(comment.ID)
	resp := commentToResponse(comment, grouped[cid], groupedAtt[cid])
	resp.IssueRevision = issueRevision
	slog.Info("comment updated", append(logger.RequestAttrs(r), "comment_id", commentId)...)
	eventPayload := map[string]any{"comment": resp}
	if issueRevision > 0 {
		eventPayload["issue_revision"] = issueRevision
	}
	h.publish(protocol.EventCommentUpdated, workspaceID, actorType, actorID, eventPayload)

	// The broadcast above intentionally omits trigger_outcomes — it is the
	// editor's private feedback, not shared timeline state (MUL-4525 §2).
	resp.TriggerOutcomes = commentEventOutcomes()

	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) DeleteComment(w http.ResponseWriter, r *http.Request) {
	r = h.withWakeupActor(r)
	commentId := chi.URLParam(r, "commentId")

	userID, ok := requireUserID(w, r)
	if !ok {
		return
	}

	commentUUID, ok := parseUUIDOrBadRequest(w, commentId, "comment id")
	if !ok {
		return
	}

	// Load comment scoped to current workspace.
	workspaceID := h.resolveWorkspaceID(r)
	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace id")
	if !ok {
		return
	}
	comment, err := h.Queries.GetCommentInWorkspace(r.Context(), db.GetCommentInWorkspaceParams{
		ID:          commentUUID,
		WorkspaceID: wsUUID,
	})
	if err != nil {
		writeError(w, http.StatusNotFound, "comment not found")
		return
	}

	member, ok := h.workspaceMember(w, r, workspaceID)
	if !ok {
		return
	}

	actorType, actorID := h.resolveActor(r, userID, workspaceID)
	isAuthor := comment.AuthorType == actorType && uuidToString(comment.AuthorID) == actorID
	isAdmin := roleAllowed(member.Role, "owner", "admin")
	if !isAuthor && !isAdmin {
		writeError(w, http.StatusForbidden, "only comment author or admin can delete")
		return
	}
	issue, err := h.Queries.GetIssue(r.Context(), comment.IssueID)
	hasIssue := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Warn("load issue for delete post-processing failed", "issue_id", uuidToString(comment.IssueID), "error", err)
		writeError(w, http.StatusInternalServerError, "failed to load issue")
		return
	}
	if errors.Is(err, pgx.ErrNoRows) {
		// The issue delete may have won the race after GetCommentInWorkspace.
		// Continue so DeleteComment can report whether the comment itself still
		// existed; touching issue activity is intentionally best effort.
		slog.Info("comment parent issue no longer exists", "issue_id", uuidToString(comment.IssueID), "comment_id", commentId)
	}

	if comment.DeletedAt.Valid {
		writeError(w, http.StatusNotFound, "comment not found")
		return
	}

	deleted, err := h.deleteComment(r.Context(), comment.ID, comment.WorkspaceID)
	if err != nil {
		slog.Warn("delete comment failed", append(logger.RequestAttrs(r), "error", err, "comment_id", commentId)...)

		if errors.Is(err, errTaskSupplementInFlight) {
			writeError(w, http.StatusConflict, "additional message is still being delivered")
		} else if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "comment not found")
		} else {
			writeError(w, http.StatusInternalServerError, "failed to delete comment")
		}
		return
	}

	h.deleteS3Objects(r.Context(), deleted.AttachmentURLs)
	slog.Info("comment deleted", append(logger.RequestAttrs(r),
		"comment_id", commentId,
		"issue_id", uuidToString(comment.IssueID),
		"tombstoned", deleted.Tombstone != nil,
		"removed_count", len(deleted.RemovedIDs),
	)...)
	// A tombstone is still part of the thread, so clients replace it in place
	// through the ordinary update event; removed rows get one delete event each.
	if deleted.Tombstone != nil {
		resp := commentToResponse(*deleted.Tombstone, nil, nil)
		resp.IssueRevision = deleted.IssueRevision
		eventPayload := map[string]any{"comment": resp}
		if deleted.IssueRevision > 0 {
			eventPayload["issue_revision"] = deleted.IssueRevision
		}
		h.publish(protocol.EventCommentUpdated, workspaceID, actorType, actorID, eventPayload)
	}
	for _, removedID := range deleted.RemovedIDs {
		eventPayload := map[string]any{
			"comment_id": uuidToString(removedID),
			"issue_id":   uuidToString(comment.IssueID),
		}
		if deleted.IssueRevision > 0 {
			eventPayload["issue_revision"] = deleted.IssueRevision
		}
		h.publish(protocol.EventCommentDeleted, workspaceID, actorType, actorID, eventPayload)
	}
	if hasIssue {
		h.wakeAssignedIssueSession(r.Context(), issue)
	}
	w.WriteHeader(http.StatusNoContent)
}

// commentDeletion is the committed outcome of deleteComment.
type commentDeletion struct {
	// Tombstone is the cleared row when the comment still had replies.
	Tombstone *db.Comment
	// RemovedIDs lists every row removed outright: the comment itself when it
	// had no replies, followed by each tombstone ancestor that lost its last
	// reply as a result, nearest first.
	RemovedIDs []pgtype.UUID
	// AttachmentURLs are the deleted comment's storage objects, for cleanup
	// after commit.
	AttachmentURLs []string
	IssueRevision  int64
}

// lockCommentAttachments locks the unbound issue attachments a comment is
// being created with and reports the first requested id that is not among
// them: never eligible, or deleted while the lock waited. The caller must
// already hold the issue's row lock — these are its children, and taking them
// first is the inversion CreateComment documents.
func lockCommentAttachments(ctx context.Context, qtx *db.Queries, workspaceID, issueID pgtype.UUID, ids []pgtype.UUID) (missing pgtype.UUID, err error) {
	locked, err := qtx.LockAttachmentsForCommentLink(ctx, db.LockAttachmentsForCommentLinkParams{
		WorkspaceID:   workspaceID,
		IssueID:       issueID,
		AttachmentIds: ids,
	})
	if err != nil {
		return pgtype.UUID{}, err
	}
	have := make(map[pgtype.UUID]struct{}, len(locked))
	for _, id := range locked {
		have[id] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := have[id]; !ok {
			return id, nil
		}
	}
	return pgtype.UUID{}, nil
}

// withLiveCommentLock runs write in a transaction that first locks the
// comment's issue and then the comment — the order every comment mutation
// takes — and only while the comment is live. Writes to a comment's children
// (reactions, attachments) go through it so none can land on a tombstone, even
// when the delete commits while this waits. Returns pgx.ErrNoRows when the
// comment is gone or deleted.
func (h *Handler) withLiveCommentLock(ctx context.Context, commentID, workspaceID pgtype.UUID, write func(*db.Queries) error) error {
	tx, err := h.beginWakeupWrite(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qtx := h.Queries.WithTx(tx)
	if _, err := qtx.LockLiveComment(ctx, db.LockLiveCommentParams{ID: commentID, WorkspaceID: workspaceID}); err != nil {
		return err
	}
	if err := write(qtx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// commentTombstonePruneDepth bounds the upward walk over tombstone ancestors,
// matching the depth bound of the ancestor path queries.
const commentTombstonePruneDepth = 256

var errTaskSupplementInFlight = errors.New("additional message is still being delivered")

// deleteComment deletes exactly one comment (#8296). A comment that still has
// replies becomes a tombstone — content, attachments, reactions and resolution
// cleared, row kept — so each reply stays attached to its direct parent. A
// comment without replies is removed, along with every tombstone ancestor
// that is left without replies. Nothing here relies on the parent_id cascade.
//
// Returns pgx.ErrNoRows when the comment is already gone or already deleted.
func (h *Handler) deleteComment(ctx context.Context, commentID, workspaceID pgtype.UUID) (commentDeletion, error) {
	var out commentDeletion
	tx, err := h.beginWakeupWrite(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(ctx)
	qtx := h.Queries.WithTx(tx)

	target, err := qtx.LockCommentForDelete(ctx, db.LockCommentForDeleteParams{
		ID:          commentID,
		WorkspaceID: workspaceID,
	})
	if err != nil {
		return out, err
	}
	// Keep receipt admission locked through deletion so a concurrent retry
	// cannot resurrect or deliver the content being removed.
	receipts, receiptErr := qtx.LockTaskSupplementsByComment(ctx, db.LockTaskSupplementsByCommentParams{
		CommentID: commentID, WorkspaceID: workspaceID,
	})
	if receiptErr != nil {
		return out, receiptErr
	}
	for _, receipt := range receipts {
		if receipt.Status == "pending" || receipt.Status == "delivering" {
			return out, errTaskSupplementInFlight
		}
	}
	if len(receipts) > 0 {
		if err := qtx.DeleteTaskSupplementByComment(ctx, db.DeleteTaskSupplementByCommentParams{
			CommentID: commentID, WorkspaceID: workspaceID,
		}); err != nil {
			return out, err
		}
	}
	// Separate statement on purpose: its snapshot postdates the locks above,
	// so it sees every committed reply, and none can be added while they are
	// held.
	hasReplies, err := qtx.CommentHasReplies(ctx, db.CommentHasRepliesParams{
		ID:          target.ID,
		WorkspaceID: target.WorkspaceID,
	})
	if err != nil {
		return out, err
	}
	out.AttachmentURLs, err = qtx.DeleteCommentAttachments(ctx, db.DeleteCommentAttachmentsParams{
		CommentID:   target.ID,
		WorkspaceID: target.WorkspaceID,
	})
	if err != nil {
		return out, err
	}
	if err := qtx.DeleteCommentReactions(ctx, db.DeleteCommentReactionsParams{
		CommentID:   target.ID,
		WorkspaceID: target.WorkspaceID,
	}); err != nil {
		return out, err
	}

	if hasReplies {
		tombstone, err := qtx.TombstoneComment(ctx, db.TombstoneCommentParams{
			ID:          target.ID,
			WorkspaceID: target.WorkspaceID,
		})
		if err != nil {
			return out, err
		}
		// A deleted delegated-failure recovery signal is withdrawn: settle it so
		// the recovery sweeper stops scanning its row. A no-op for every other
		// comment.
		if _, err := qtx.SettleDelegatedFailureRecoveryComment(ctx, target.ID); err != nil {
			return out, err
		}
		out.Tombstone = &tombstone
	} else {
		removed, err := qtx.DeleteLeafComment(ctx, db.DeleteLeafCommentParams{
			ID:          target.ID,
			WorkspaceID: target.WorkspaceID,
		})
		if err != nil {
			return out, err
		}
		out.RemovedIDs = append(out.RemovedIDs, removed.ID)
		parentID := removed.ParentID
		for depth := 0; parentID.Valid && depth < commentTombstonePruneDepth; depth++ {
			pruned, err := qtx.DeleteReplylessCommentTombstone(ctx, db.DeleteReplylessCommentTombstoneParams{
				ID:          parentID,
				WorkspaceID: target.WorkspaceID,
			})
			if errors.Is(err, pgx.ErrNoRows) {
				// A live comment, or a tombstone other replies still hold.
				break
			}
			if err != nil {
				return out, err
			}
			out.RemovedIDs = append(out.RemovedIDs, pruned.ID)
			parentID = pruned.ParentID
		}
	}

	out.IssueRevision, err = qtx.TouchIssueForCommentDelete(ctx, db.TouchIssueForCommentDeleteParams{
		IssueID:     target.IssueID,
		WorkspaceID: target.WorkspaceID,
	})
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	if err := tx.Commit(ctx); err != nil {
		return out, err
	}
	return out, nil
}

func (h *Handler) loadCommentForActor(w http.ResponseWriter, r *http.Request) (db.Comment, string, string, string, bool) {
	commentId := chi.URLParam(r, "commentId")
	userID, ok := requireUserID(w, r)
	if !ok {
		return db.Comment{}, "", "", "", false
	}
	commentUUID, ok := parseUUIDOrBadRequest(w, commentId, "comment id")
	if !ok {
		return db.Comment{}, "", "", "", false
	}
	workspaceID := h.resolveWorkspaceID(r)
	wsUUID, ok := parseUUIDOrBadRequest(w, workspaceID, "workspace id")
	if !ok {
		return db.Comment{}, "", "", "", false
	}
	if _, ok := h.workspaceMember(w, r, workspaceID); !ok {
		return db.Comment{}, "", "", "", false
	}
	comment, err := h.Queries.GetCommentInWorkspace(r.Context(), db.GetCommentInWorkspaceParams{
		ID:          commentUUID,
		WorkspaceID: wsUUID,
	})
	// A deleted comment's tombstone cannot be a thread's resolution.
	if err != nil || comment.DeletedAt.Valid {
		writeError(w, http.StatusNotFound, "comment not found")
		return db.Comment{}, "", "", "", false
	}
	actorType, actorID := h.resolveActor(r, userID, workspaceID)
	return comment, workspaceID, actorType, actorID, true
}

func (h *Handler) ResolveComment(w http.ResponseWriter, r *http.Request) {
	r = h.withWakeupActor(r)
	comment, workspaceID, actorType, actorID, ok := h.loadCommentForActor(w, r)
	if !ok {
		return
	}
	wasResolved := comment.ResolvedAt.Valid

	actorUUID, err := util.ParseUUID(actorID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid actor id")
		return
	}

	// Single-resolution invariant: a thread has at most one resolved comment, so
	// resolving this one must clear any other resolution in the same thread. Both
	// writes run in one tx — clearing the old resolution and setting the new one
	// is atomic, so a crash can never leave two resolutions (or none) visible.
	tx, err := h.beginWakeupWrite(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to resolve comment")
		return
	}
	defer tx.Rollback(r.Context())
	qtx := h.Queries.WithTx(tx)

	cleared, err := qtx.ClearOtherThreadResolutions(r.Context(), db.ClearOtherThreadResolutionsParams{
		TargetID:    comment.ID,
		IssueID:     comment.IssueID,
		WorkspaceID: comment.WorkspaceID,
	})
	if err != nil {
		slog.Warn("clear other thread resolutions failed", append(logger.RequestAttrs(r), "error", err, "comment_id", uuidToString(comment.ID))...)
		writeError(w, http.StatusInternalServerError, "failed to resolve comment")
		return
	}

	updated, err := qtx.ResolveComment(r.Context(), db.ResolveCommentParams{
		ID:             comment.ID,
		ResolvedByType: pgtype.Text{String: actorType, Valid: true},
		ResolvedByID:   actorUUID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// Deleted after it was loaded.
		writeError(w, http.StatusNotFound, "comment not found")
		return
	}
	if err != nil {
		slog.Warn("resolve comment failed", append(logger.RequestAttrs(r), "error", err, "comment_id", uuidToString(comment.ID))...)
		writeError(w, http.StatusInternalServerError, "failed to resolve comment")
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		slog.Warn("resolve comment commit failed", append(logger.RequestAttrs(r), "error", err, "comment_id", uuidToString(comment.ID))...)
		writeError(w, http.StatusInternalServerError, "failed to resolve comment")
		return
	}

	// Emit a comment:unresolved per cleared sibling so granular realtime
	// consumers (which patch a single comment in place) drop the stale
	// resolution instead of showing two. Published after commit so no event ever
	// describes an uncommitted state.
	for _, c := range cleared {
		clearedID := uuidToString(c.ID)
		clearedReactions := h.groupReactions(r, []pgtype.UUID{c.ID})
		clearedAtt := h.groupAttachments(r, []pgtype.UUID{c.ID})
		clearedResp := commentToResponse(c, clearedReactions[clearedID], clearedAtt[clearedID])
		slog.Info("comment unresolved (replaced)", append(logger.RequestAttrs(r), "comment_id", clearedID)...)
		h.publish(protocol.EventCommentUnresolved, workspaceID, actorType, actorID, map[string]any{"comment": clearedResp})
	}

	grouped := h.groupReactions(r, []pgtype.UUID{updated.ID})
	groupedAtt := h.groupAttachments(r, []pgtype.UUID{updated.ID})
	cid := uuidToString(updated.ID)
	resp := commentToResponse(updated, grouped[cid], groupedAtt[cid])

	// Suppress the target event on a re-resolve no-op so consumers do not
	// re-process an unchanged thread (notifications, log spam). Cleared siblings
	// still get their own events above — those rows did change.
	if !wasResolved {
		slog.Info("comment resolved", append(logger.RequestAttrs(r), "comment_id", cid)...)
		h.publish(protocol.EventCommentResolved, workspaceID, actorType, actorID, map[string]any{"comment": resp})
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) UnresolveComment(w http.ResponseWriter, r *http.Request) {
	r = h.withWakeupActor(r)
	comment, workspaceID, actorType, actorID, ok := h.loadCommentForActor(w, r)
	if !ok {
		return
	}
	wasResolved := comment.ResolvedAt.Valid

	updated, err := wakeupWrite(h, r, func(q *db.Queries) (db.Comment, error) {
		return q.UnresolveComment(r.Context(), comment.ID)
	})
	if err != nil {
		slog.Warn("unresolve comment failed", append(logger.RequestAttrs(r), "error", err, "comment_id", uuidToString(comment.ID))...)
		writeError(w, http.StatusInternalServerError, "failed to unresolve comment")
		return
	}

	grouped := h.groupReactions(r, []pgtype.UUID{updated.ID})
	groupedAtt := h.groupAttachments(r, []pgtype.UUID{updated.ID})
	cid := uuidToString(updated.ID)
	resp := commentToResponse(updated, grouped[cid], groupedAtt[cid])

	if wasResolved {
		slog.Info("comment unresolved", append(logger.RequestAttrs(r), "comment_id", cid)...)
		h.publish(protocol.EventCommentUnresolved, workspaceID, actorType, actorID, map[string]any{"comment": resp})
	}
	writeJSON(w, http.StatusOK, resp)
}
