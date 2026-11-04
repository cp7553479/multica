CREATE TABLE IF NOT EXISTS issue_agent_session (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    issue_id UUID NOT NULL,
    agent_id UUID NOT NULL,
    runtime_id UUID,
    provider_session_id TEXT,
    work_dir TEXT,
    active_task_id UUID,
    wake_after TIMESTAMPTZ,
    state TEXT NOT NULL DEFAULT 'ready' CHECK (state IN ('ready', 'blocked', 'detached')),
    failure_reason TEXT,
    next_sequence BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS issue_agent_event (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL,
    session_id UUID NOT NULL,
    sequence BIGINT NOT NULL,
    event_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    source_id UUID NOT NULL,
    source_revision BIGINT NOT NULL,
    actor_type TEXT NOT NULL,
    actor_id UUID,
    originator_user_id UUID,
    source_task_id UUID,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivering', 'delivered', 'failed', 'ignored')),
    delivery_task_id UUID,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    failure_reason TEXT,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
