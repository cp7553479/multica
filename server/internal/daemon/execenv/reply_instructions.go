package execenv

import "fmt"

// BuildNewCommentsHint supplies issue-wide catch-up facts and exact read commands
// for a resumed provider session. The count excludes the injected trigger and
// the agent's own comments; the unfiltered since read may return more rows.
//
// The issue-wide since read supplies the server-computed delta and satisfies the
// workflow's scan. This hint does not make that scan optional. Keep per-run state
// in the user message rather than the stable brief.
//
// The since read has a 2000-comment cap. Use tail for bounded thread reads because
// thread+since alone may omit the root. Empty inputs render nothing, allowing
// the caller to select resumed-empty or cold-start guidance instead.
func BuildNewCommentsHint(issueID, triggerCommentID, triggerThreadID, newCommentsSince string, newCommentCount int) string {
	if newCommentCount <= 0 || newCommentsSince == "" || issueID == "" {
		return ""
	}
	threadID := activeThreadID(triggerThreadID, triggerCommentID)
	// One command, not two: the server already computed this delta, so the
	// agent reads it instead of rediscovering it with a scan plus expansions
	// (MUL-7344). The optional second command is a THREAD read, offered only
	// for the reply itself — it is never the wide read.
	delta := fmt.Sprintf(
		"%d new comment(s) on this issue since your last run, across all threads — "+
			"the server computed this delta, and reading it is the scan workflow step 2 requires. "+
			"Read exactly those comments with "+
			"`multica issue comment list %s --since %s --compact --output json` "+
			"(every comment created after that anchor, in every thread — it also returns the triggering comment and your own replies, "+
			"which the count above excludes, so expect more rows than that number).",
		newCommentCount, issueID, newCommentsSince,
	)
	// Defensive: comment triggers always carry a trigger id, but if one is
	// missing there is no thread to anchor the optional full-thread read on.
	if threadID == "" {
		return delta + "\n\n"
	}
	return fmt.Sprintf(
		"%s Triggering thread in full, if resumed memory is not enough for the reply: "+
			"`multica issue comment list %s --thread %s --tail 30 --compact --output json`.\n\n",
		delta, issueID, threadID,
	)
}

// BuildResumedCommentsHint returns the comment-reading pointer for the WARM
// path where the server COMPUTED an issue-wide delta this claim and it came
// back empty: the daemon is resuming a prior provider session, the triggering
// comment body has already been injected into the per-turn prompt, and no
// other comment arrived since the last run (beyond that trigger and the
// agent's own replies).
//
// That zero is server-computed and issue-wide, so it IS the answer the scan in
// workflow step 2 exists to produce; the hint says so, which is the one way a
// per-turn message may satisfy the scan without a call. Re-reading the
// triggering thread in full stays the agent's own call: step 2 leaves "which
// threads to expand" to judgment, and this is that judgment, not a decision
// about whether the wide read happens (MUL-6984).
//
// The caller must reach this ONLY on Task.NewCommentsDeltaKnown. A zero count
// on its own does not mean the delta is empty — a failed anchor read, a failed
// count read, a cold start and an old server all produce the same zero, and
// none of them looked at the issue. Claiming the scan is answered from one of
// those would delete the very fallback the mandatory scan exists to be; that
// path renders BuildResumedUnknownDeltaCommentsHint instead.
func BuildResumedCommentsHint(issueID, triggerCommentID, triggerThreadID string) string {
	threadID := activeThreadID(triggerThreadID, triggerCommentID)
	if issueID == "" || threadID == "" {
		return ""
	}
	// No standalone anchor-restating sentence here (MUL-5721 OPT-1): the read
	// command below already carries the thread anchor, and the trigger comment
	// id reaches the agent as the reply cookbook's `--parent` value.
	return fmt.Sprintf(
		"You're resuming the prior session, and the triggering comment is already included above. "+
			"No other new comments on this issue since your last run — this turn's issue-wide delta is empty, "+
			"which answers the scan workflow step 2 requires. "+
			"Triggering thread in full, if resumed memory is not enough for the reply: "+
			"`multica issue comment list %s --thread %s --tail 30 --compact --output json`.\n\n",
		issueID, threadID,
	)
}

// BuildResumedUnknownDeltaCommentsHint returns the comment-reading pointer for
// a resumed run whose issue-wide delta this claim does NOT carry: the count
// query or its anchor lookup failed, or the server predates the delta fields.
//
// The session context is real, so the hint still says the trigger is injected
// and offers the thread read. What it must not do is imply anything about the
// rest of the issue: nothing here looked. Workflow step 2's scan is mandatory
// by default and only an affirmative server report may waive it, so this hint
// hands the scan over as a command instead of waiving it (MUL-6984).
func BuildResumedUnknownDeltaCommentsHint(issueID, triggerCommentID, triggerThreadID string) string {
	threadID := activeThreadID(triggerThreadID, triggerCommentID)
	if issueID == "" {
		return ""
	}
	if threadID == "" {
		return fmt.Sprintf(
			"You're resuming the prior session, and the triggering comment is already included above. "+
				"This turn carries no issue-wide comment delta, so nothing here answers the scan workflow step 2 requires — run it: "+
				"`multica issue comment list %s --roots-only --summary --compact --output json`.\n\n",
			issueID,
		)
	}
	return fmt.Sprintf(
		"You're resuming the prior session, and the triggering comment is already included above. "+
			"This turn carries no issue-wide comment delta, so nothing here answers the scan workflow step 2 requires — run it: "+
			"`multica issue comment list %s --roots-only --summary --compact --output json`, "+
			"and expand what its `last_activity_at` shows has moved. "+
			"Triggering thread in full, if resumed memory is not enough for the reply: "+
			"`multica issue comment list %s --thread %s --tail 30 --compact --output json`.\n\n",
		issueID, issueID, threadID,
	)
}

// BuildColdCommentsHint returns the comment-reading pointer for a run whose
// latest turn on this issue did not come back — its first run, a resume the
// daemon had to drop, an explicitly fresh rerun, or a MUL-5305 older-fallback
// session, where the server hands back an OLDER session with the continuity
// gap flagged and the daemon does resume it. The hint therefore states nothing
// about the provider session: "fresh" is false for the fallback case, and no
// decision depends on it — every one of these runs reconstructs from the issue
// record with the same two reads (MUL-6984 review). Instead of dumping the
// whole flat timeline (oldest-first, server cap 2000), point the agent at the
// triggering CONVERSATION: `--thread <trigger> --tail 30` returns that thread's
// root plus its 30 newest replies (root is always included, even at --tail 0)
// — the context the triggering comment actually needs. The scan workflow step 2
// requires is handed over as the wide read; the hint deliberately does NOT
// name `--recent`, whose saturation trap and pagination live once in the
// brief's `## Available Commands` (MUL-5372). Per-turn hints name only the
// reads they actually want the agent to run, and state no modality: the
// earlier "Need cross-thread background?" framing invited the agent to judge
// a need it had no data to judge (MUL-6984).
//
// Since MUL-5377 the per-turn prompt is the only caller (same as
// BuildNewCommentsHint). Returns "" when there is no triggering comment to
// thread from, so the caller can keep a final plain fallback.
func BuildColdCommentsHint(issueID, triggerCommentID, triggerThreadID string) string {
	threadID := activeThreadID(triggerThreadID, triggerCommentID)
	if issueID == "" || threadID == "" {
		return ""
	}
	// The roots scan is phrased as a flag swap on the thread command above, not
	// a second full command: the duplicate restated the issue UUID for no
	// routing value (MUL-5721 OPT-1).
	return fmt.Sprintf(
		"Triggering thread: "+
			"`multica issue comment list %s --thread %s --tail 30 --compact --output json` "+
			"(that thread's root + its 30 newest replies). "+
			"The scan workflow step 2 requires is the same command with `--roots-only --summary` in place of `--thread ... --tail 30`.\n\n",
		issueID, threadID,
	)
}

func activeThreadID(triggerThreadID, triggerCommentID string) string {
	if triggerThreadID != "" {
		return triggerThreadID
	}
	return triggerCommentID
}

// BuildCommentReplyInstructions offers an explicit reply action for a comment
// event. The agent chooses whether to publish, reply, or update the issue.
func BuildCommentReplyInstructions(provider, issueID, triggerCommentID string, squadLeader bool) string {
	if triggerCommentID == "" {
		return ""
	}
	lead := "Choose whether this event needs a public response. Publish a new top-level comment by omitting --parent, reply to a particular comment with --parent, or change the issue with `multica issue update`. Final output stays in the run transcript and is never posted automatically.\n\nIf you choose to reply to this event's comment, "
	// Cleanup is GATED on the post succeeding, never a separate statement.
	// Agents hand the whole snippet to one shell call, and an unconditional
	// cleanup line runs — and SUCCEEDS — after a failed post, so the call's
	// exit status becomes the cleanup's 0. Under `--output table` a failed
	// and a successful post both write nothing to stdout, which leaves the
	// exit status as the only machine-checkable signal; masking it lets an
	// agent end its turn believing an unposted result was delivered. The
	// gate also keeps ./reply.md on disk after a failure, so the retry does
	// not have to regenerate the body.
	if runtimeGOOS == "windows" {
		// The daemon knows the host OS, not the shell behind the agent's
		// command tool: Codex runs PowerShell on Windows, Claude Code runs
		// Git Bash for its Bash tool and (by default on claude.ai accounts)
		// a PowerShell tool the user can turn off with
		// CLAUDE_CODE_USE_POWERSHELL_TOOL=0 (#8627). A PowerShell-only
		// cookbook pasted into Git Bash fails at the `if (` line AFTER the
		// post succeeded, so the agent sees a failure and may re-post. Offer
		// one variant per shell and let the agent pick.
		//
		// PowerShell 5.1 has no `&&` (it landed in PowerShell 7), so that
		// variant checks $LASTEXITCODE — which carries the exit code of the
		// last NATIVE command — and propagates it instead.
		return fmt.Sprintf(
			lead+
				"do NOT reuse --parent values from previous turns in this session.\n\n"+
				"Write the body file first — never pipe via `--content-stdin` (PowerShell drops non-ASCII; full rules: ## Comment Formatting above). Use the variant for the shell your command tool runs:\n\n"+
				"PowerShell:\n\n"+
				"    multica issue comment add %[1]s --parent %[2]s --content-file ./reply.md --output table\n"+
				"    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }\n"+
				"    Remove-Item ./reply.md\n\n"+
				"Git Bash:\n\n"+
				"    multica issue comment add %[1]s --parent %[2]s --content-file ./reply.md --output table && rm ./reply.md\n\n"+
				"Do NOT drop the exit-code check or the `&&`: a bare cleanup after a failed post reports success and deletes the body.\n\n"+
				"Do NOT write literal `\\n` escapes to simulate line breaks; the file preserves real newlines.\n",
			issueID, triggerCommentID,
		)
	}
	return fmt.Sprintf(
		lead+
			"do NOT reuse --parent values from previous turns in this session.\n\n"+
			"Write the body file first (rules: ## Comment Formatting above — MUL-2904 / #4182):\n\n"+
			"    multica issue comment add %s --parent %s --content-file ./reply.md --output table && rm ./reply.md\n\n"+
			"Keep the `&&`: as two separate statements a failed post is masked by the cleanup's success, and the body file is deleted.\n\n"+
			"Do NOT write literal `\\n` escapes to simulate line breaks; the file preserves real newlines.\n",
		issueID, triggerCommentID,
	)
}

// ThreadReplyTarget identifies an event's original thread and optional reply target.
type ThreadReplyTarget struct {
	ThreadID string
	ParentID string
}

// BuildMultiThreadCommentReplyInstructions preserves source context for events
// from multiple threads without turning those events into required replies.
func BuildMultiThreadCommentReplyInstructions(issueID string, targets []ThreadReplyTarget, squadLeader bool) string {
	if issueID == "" || len(targets) < 2 {
		return ""
	}
	var targetLines string
	for i, tgt := range targets {
		targetLines += fmt.Sprintf("%d. thread %s → optional reply with `--parent %s`\n", i+1, tgt.ThreadID, tgt.ParentID)
	}
	return fmt.Sprintf(
		"These events came from %d DISTINCT threads. Choose which events need a response; posting once per event or thread is not required. You may publish a top-level comment or update the issue instead. Final output stays in the run transcript and is never posted automatically.\n\n"+
			"Source threads in event order — OLDEST thread first:\n%s\n"+
			"When replying, never reuse a `--parent` from an earlier turn. Follow `## Comment Formatting` and use a DISTINCT body file per thread you choose to answer.\n",
		len(targets), targetLines,
	)
}
