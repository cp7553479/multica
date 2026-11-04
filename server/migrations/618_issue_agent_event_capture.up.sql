-- Capture inputs in the mutation transaction. WebSocket hints and HTTP
-- responses are not the source of truth for a session's inbox.
CREATE OR REPLACE FUNCTION append_issue_agent_event(
    p_issue uuid, p_kind text, p_source uuid, p_revision bigint,
    p_actor_type text, p_actor_id uuid, p_source_task uuid, p_payload jsonb
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE i issue; a agent; s issue_agent_session; event_sequence bigint;
    event_status text; originator uuid; captured_key text;
BEGIN
    SELECT * INTO i FROM issue WHERE id=p_issue;
    IF NOT FOUND OR i.assignee_type IS DISTINCT FROM 'agent' OR i.assignee_id IS NULL THEN RETURN; END IF;
    SELECT * INTO a FROM agent WHERE id=i.assignee_id AND workspace_id=i.workspace_id;
    IF NOT FOUND THEN RETURN; END IF;

    INSERT INTO issue_agent_session(workspace_id,issue_id,agent_id,runtime_id,state,failure_reason)
    VALUES(i.workspace_id,i.id,a.id,a.runtime_id,
        CASE WHEN a.runtime_id IS NULL THEN 'blocked' ELSE 'ready' END,
        CASE WHEN a.runtime_id IS NULL THEN 'runtime_unbound' END)
    ON CONFLICT (workspace_id,issue_id,agent_id) DO NOTHING;
    SELECT * INTO s FROM issue_agent_session
    WHERE workspace_id=i.workspace_id AND issue_id=i.id AND agent_id=a.id FOR UPDATE;

    captured_key := p_kind||':'||p_source::text||':'||p_revision::text;
    IF EXISTS (SELECT 1 FROM issue_agent_event WHERE session_id=s.id AND issue_agent_event.event_key=captured_key) THEN RETURN; END IF;
    IF p_source_task IS NOT NULL THEN
        SELECT originator_user_id INTO originator FROM agent_task_queue
        WHERE id=p_source_task AND agent_id=p_actor_id
            AND status NOT IN ('completed','failed','cancelled');
    ELSIF p_actor_type='member' THEN
        originator := p_actor_id;
    END IF;
    event_status := CASE
        WHEN p_actor_type='plugin' OR p_payload->>'via_plugin_id' IS NOT NULL THEN 'ignored'
        WHEN p_actor_type='agent' AND p_actor_id=a.id
            AND NOT COALESCE(p_kind='issue.updated' AND p_payload->>'workflow_handoff'='true'
                AND (p_payload->>'status_changed'='true' OR p_payload->>'stage_changed'='true'),false)
            AND NOT (p_kind='assigned' AND NOT EXISTS (
                SELECT 1 FROM agent_task_queue source
                WHERE source.id=p_source_task AND source.issue_id=i.id
            )) THEN 'ignored'
        ELSE 'pending' END;
    UPDATE issue_agent_session SET next_sequence=next_sequence+1,updated_at=now()
    WHERE id=s.id RETURNING next_sequence INTO event_sequence;
    INSERT INTO issue_agent_event(workspace_id,session_id,sequence,event_key,kind,source_id,source_revision,
        actor_type,actor_id,source_task_id,originator_user_id,payload,status,failure_reason)
    VALUES(i.workspace_id,s.id,event_sequence,captured_key,p_kind,p_source,p_revision,
        p_actor_type,p_actor_id,p_source_task,originator,
        p_payload||jsonb_build_object('issue_id',i.id,'workspace_id',i.workspace_id,'source_task_id',p_source_task),
        event_status,CASE
            WHEN p_actor_type='plugin' OR p_payload->>'via_plugin_id' IS NOT NULL THEN 'plugin_write_requires_invoke'
            WHEN event_status='ignored' THEN 'self_authored' END);
END $$;

CREATE OR REPLACE FUNCTION capture_issue_agent_comment() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c comment; event_kind text; actor_kind text; actor uuid; source_task uuid;
BEGIN
    c := CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
    IF TG_OP='INSERT' THEN
        IF c.type NOT IN ('comment','progress_update') THEN RETURN c; END IF;
        event_kind := 'comment.created';
    ELSIF TG_OP='DELETE' THEN
        IF OLD.deleted_at IS NOT NULL THEN RETURN OLD; END IF;
        event_kind := 'comment.deleted';
    ELSIF OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL THEN
        event_kind := 'comment.deleted';
    ELSIF NEW.deleted_at IS NOT NULL OR NEW.content IS NOT DISTINCT FROM OLD.content THEN
        RETURN NEW;
    ELSE
        event_kind := 'comment.updated';
    END IF;
    source_task := NULLIF(current_setting('multica.source_task_id',true),'')::uuid;
    actor_kind := NULLIF(current_setting('multica.actor_type',true),'');
    actor := NULLIF(current_setting('multica.actor_id',true),'')::uuid;
    IF TG_OP='INSERT' THEN
        actor_kind := COALESCE(actor_kind,c.author_type);
        actor := COALESCE(actor,c.author_id);
        source_task := COALESCE(source_task,c.source_task_id);
    END IF;
    -- An edit's actor comes from the authenticated write transaction, never
    -- from the original author of the comment being edited.
    PERFORM append_issue_agent_event(c.issue_id,event_kind,c.id,c.revision,
        COALESCE(actor_kind,'system'),actor,source_task,
        jsonb_build_object('comment_id',c.id,'parent_comment_id',c.parent_id,
            'author_type',c.author_type,'author_id',c.author_id,
            'via_plugin_id',CASE WHEN TG_OP='INSERT' THEN c.via_plugin_id END,
            'content',CASE WHEN event_kind='comment.deleted' THEN NULL ELSE c.content END));
    RETURN c;
END $$;
CREATE TRIGGER capture_issue_agent_comment AFTER INSERT OR UPDATE OR DELETE ON comment
FOR EACH ROW EXECUTE FUNCTION capture_issue_agent_comment();

CREATE OR REPLACE FUNCTION capture_issue_agent_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE actor_kind text; actor uuid; source_task uuid; event_kind text; ownership_changed boolean;
    status_changed boolean; stage_changed boolean;
BEGIN
    IF TG_OP='DELETE' THEN
        DELETE FROM issue_agent_event WHERE session_id IN (SELECT id FROM issue_agent_session WHERE issue_id=OLD.id AND workspace_id=OLD.workspace_id);
        DELETE FROM issue_agent_session WHERE issue_id=OLD.id AND workspace_id=OLD.workspace_id;
        RETURN OLD;
    END IF;
    ownership_changed := TG_OP='INSERT';
    status_changed := TG_OP='INSERT';
    stage_changed := TG_OP='INSERT';
    IF TG_OP='UPDATE' THEN
        ownership_changed := NEW.assignee_type IS DISTINCT FROM OLD.assignee_type OR NEW.assignee_id IS DISTINCT FROM OLD.assignee_id;
        status_changed := NEW.status IS DISTINCT FROM OLD.status;
        stage_changed := NEW.stage IS DISTINCT FROM OLD.stage;
    END IF;
    IF ownership_changed THEN
        UPDATE issue_agent_session SET state='detached',failure_reason='reassigned',updated_at=now()
        WHERE issue_id=NEW.id AND workspace_id=NEW.workspace_id
            AND (NEW.assignee_type IS DISTINCT FROM 'agent' OR agent_id IS DISTINCT FROM NEW.assignee_id);
        UPDATE issue_agent_event SET status='ignored',failure_reason='reassigned',updated_at=now()
        WHERE status IN ('pending','failed') AND kind <> 'wakeup.fired' AND session_id IN (
            SELECT id FROM issue_agent_session WHERE issue_id=NEW.id AND workspace_id=NEW.workspace_id AND state='detached');
        UPDATE issue_agent_session SET state='ready',failure_reason=NULL,updated_at=now()
        WHERE issue_id=NEW.id AND workspace_id=NEW.workspace_id AND NEW.assignee_type='agent'
            AND agent_id=NEW.assignee_id AND state='detached';
    END IF;
    IF NEW.assignee_type IS DISTINCT FROM 'agent' OR NEW.assignee_id IS NULL THEN RETURN NEW; END IF;
    IF NOT ownership_changed AND NEW.title IS NOT DISTINCT FROM OLD.title
        AND NEW.description IS NOT DISTINCT FROM OLD.description AND NOT status_changed AND NOT stage_changed
        AND COALESCE(current_setting('multica.handoff_note',true),'')='' THEN RETURN NEW; END IF;

    actor_kind := NULLIF(current_setting('multica.actor_type',true),'');
    actor := NULLIF(current_setting('multica.actor_id',true),'')::uuid;
    source_task := NULLIF(current_setting('multica.source_task_id',true),'')::uuid;
    IF TG_OP='INSERT' THEN
        actor_kind := COALESCE(actor_kind,NEW.creator_type);
        actor := COALESCE(actor,NEW.creator_id);
        IF source_task IS NULL AND NEW.origin_type IN ('agent_create','quick_create') THEN source_task := NEW.origin_id; END IF;
    END IF;
    event_kind := CASE WHEN ownership_changed THEN 'assigned' ELSE 'issue.updated' END;
    PERFORM append_issue_agent_event(NEW.id,event_kind,NEW.id,NEW.revision,
        COALESCE(actor_kind,'system'),actor,source_task,
        jsonb_build_object('issue_id',NEW.id,'title',NEW.title,'description',NEW.description,
            'status',NEW.status,'prev_status',CASE WHEN TG_OP='UPDATE' THEN OLD.status END,
            'stage',NEW.stage,'prev_stage',CASE WHEN TG_OP='UPDATE' THEN OLD.stage END,
            'status_changed',status_changed,'stage_changed',stage_changed,
            'workflow_handoff',COALESCE(current_setting('multica.workflow_handoff',true),'')='true',
            'handoff_note',NULLIF(current_setting('multica.handoff_note',true),''),
            'workflow_brief',NULLIF(current_setting('multica.workflow_brief',true),''),
            'assignee_id',NEW.assignee_id));
    RETURN NEW;
END $$;
CREATE TRIGGER capture_issue_agent_change AFTER INSERT OR UPDATE OR DELETE ON issue
FOR EACH ROW EXECUTE FUNCTION capture_issue_agent_change();

CREATE OR REPLACE FUNCTION cleanup_issue_agent_principal() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME='agent' THEN
        DELETE FROM issue_agent_event WHERE session_id IN (SELECT id FROM issue_agent_session WHERE agent_id=OLD.id);
        DELETE FROM issue_agent_session WHERE agent_id=OLD.id;
    ELSE
        DELETE FROM issue_agent_event WHERE workspace_id=OLD.id;
        DELETE FROM issue_agent_session WHERE workspace_id=OLD.id;
    END IF;
    RETURN OLD;
END $$;
CREATE TRIGGER cleanup_issue_agent_principal BEFORE DELETE ON agent
FOR EACH ROW EXECUTE FUNCTION cleanup_issue_agent_principal();
CREATE TRIGGER cleanup_issue_agent_workspace BEFORE DELETE ON workspace
FOR EACH ROW EXECUTE FUNCTION cleanup_issue_agent_principal();
