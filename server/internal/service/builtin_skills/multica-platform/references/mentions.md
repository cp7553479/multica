# Mentions, issue sessions, and delegation

Comments and issue changes are events for the issue's assigned agent session.
Mentions identify people, agents, squads, or resources; they do not choose the
agent that executes an issue event.

## Build links from real identifiers

Look up identifiers instead of guessing:

- person: `multica workspace member list --output json` → `user_id`
- agent: `multica agent list --output json` → `id`
- squad: `multica squad list --output json` → `id`

Use canonical inline links with visible label text:

    [@Alice](mention://member/<user-id>)
    [@Reviewer](mention://agent/<agent-id>)
    [@Team](mention://squad/<squad-id>)
    [MUL-123](mention://issue/<issue-id>)
    [Project](mention://project/<project-id>)
    [@all](mention://all/all)

The mention parser recognizes `(member|agent|squad|issue|all)/([0-9a-fA-F-]+|all)`.
A display name is not a UUID. Match the entity type to the identifier source;
member identifiers are user IDs, not workspace membership row IDs. If the name
is ambiguous, ask instead of choosing a different person.

Empty labels, invisible labels, reference links, images, autolinks, titles,
angle-bracket destinations, and text removed by HTML sanitization do not make
canonical mentions. Code examples are inert as mentions. Prose mentions in
blockquotes still count as references. Project links are rendered separately.

## Events belong to the assigned session

The session belongs to one issue and one assigned agent. A comment, a reply,
a comment edit, or an issue edit enters that session's ordered event inbox.
A reply's parent identifies its discussion context; it does not redirect
execution to the parent author's agent. `@agent`, `@squad`, `@member`, and `@all`
do not replace or suppress the assigned session's event delivery.

Different issue sessions may execute concurrently, including sessions using
the same agent definition. Each session processes its own ordered events.
When the provider supports live input, new events can enter a running session
at a supported safe boundary. Otherwise the capability or delivery failure is
reported explicitly; the platform does not silently turn each comment into a
separate run. An idle session resumes its own recorded conversation.

Do not use `--steer`, `--steer-active`, or per-comment recipient suppression to
route messages. The issue's assignment owns routing. Check the visible event
receipt or failure state before retrying an input; a stored pending event is
not proof the provider received it, and re-posting creates a different event.

## Delegate through assignment

A mention alone does not delegate work. To ask another agent to work, explicitly
assign an issue or create and assign a sub-issue:

    multica issue assign <issue-id> --to-id <agent-id>
    multica issue create --title "Review implementation" --parent <issue-id> --assignee-id <agent-id>

Read `references/sub-issues.md` before splitting work. Assignment changes the
responsible session; later events go to the new owner. Prior session history
remains attributed to its original issue and agent.

Assignments still require the caller's invocation authority. A task-scoped
agent acts on behalf of its recorded human originator; a mention cannot grant
additional permissions or borrow another person's credentials.

## Publish through explicit actions

Turn completion does not publish the final assistant output to the issue.
Final output stays in the run transcript. Choose the public action that helps:

- top-level comment: `multica issue comment add <issue-id> --content-file ./message.md`
- reply: add `--parent <comment-id>` to that command
- issue edit: `multica issue update <issue-id> --description-file ./description.md`

Follow the runtime brief's Comment Formatting rules and check the CLI result.
A turn does not require a public comment, and an incoming event does not require
one reply. The session's own comments and issue edits do not wake itself again.
