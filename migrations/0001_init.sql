-- Core schema for the job scheduler.
--
-- Image: one `jobs` table is both the task store and the coordination layer.
-- Everything the workers need (due time, ownership, retry bookkeeping) lives here.

CREATE TABLE jobs (
    id                BIGSERIAL   PRIMARY KEY,
    org_id            BIGINT      NOT NULL,
    kind              TEXT        NOT NULL,
    queue             TEXT        NOT NULL DEFAULT 'default',
    payload           JSONB       NOT NULL DEFAULT '{}',
    priority          INT         NOT NULL DEFAULT 100,

    -- `scheduled_at` is the tenant's stated intent and is NEVER mutated.
    -- A retry moves `next_retry_at` instead, so we keep both facts.
    scheduled_at      TIMESTAMPTZ NOT NULL,
    next_retry_at     TIMESTAMPTZ,

    state             TEXT        NOT NULL DEFAULT 'pending',
    attempt           INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 3,

    -- Ownership. All three are set together at claim time and cleared together
    -- when the job leaves `running`, so a stale worker can never resurrect one.
    lease_id          UUID,
    locked_by         TEXT,
    lease_expires_at  TIMESTAMPTZ,

    last_error        TEXT,
    unique_key        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,

    CONSTRAINT jobs_state_chk CHECK (state IN ('pending', 'running', 'completed', 'failed')),
    CONSTRAINT jobs_running_has_owner_chk CHECK (
        state <> 'running' OR (lease_id IS NOT NULL AND locked_by IS NOT NULL AND lease_expires_at IS NOT NULL)
    )
);

-- Hot path index. Only pending rows are indexed, so the index stays small even
-- as completed rows pile up (the classic Postgres-queue MVCC pitfall). The
-- ordering key is the *effective* due time, matching the claim query exactly.
CREATE INDEX jobs_due_idx
    ON jobs (priority, COALESCE(next_retry_at, scheduled_at), id)
    WHERE state = 'pending';

-- Fairness index: lets the planner find one due job per organization quickly.
CREATE INDEX jobs_org_due_idx
    ON jobs (org_id, priority, COALESCE(next_retry_at, scheduled_at), id)
    WHERE state = 'pending';

-- Idempotency / dedup key. Only live jobs occupy a key, so a completed job with
-- the same key can be re-enqueued.
CREATE UNIQUE INDEX jobs_unique_key_idx
    ON jobs (unique_key)
    WHERE unique_key IS NOT NULL AND state IN ('pending', 'running');

-- Committed side effects. This is the *deduplication* table for database-local
-- effects: `job_id` is the primary key, so recording the same effect twice is a
-- no-op (`ON CONFLICT DO NOTHING`) rather than a second record. It is written in
-- the *same transaction* as the job's completion, so a crash can never leave an
-- effect committed while the job is still pending/running (or vice versa).
--
-- Duplicate *attempts* are tracked by the job's own `attempt` counter (bumped at
-- claim), which is instrumentation; the guarantee that an effect is applied
-- exactly once comes from this table plus the atomic completion.
CREATE TABLE effects (
    job_id     BIGINT      PRIMARY KEY,
    result     JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Set of organizations that currently have pending work, per queue.
--
-- The claim query uses this to look at each active tenant directly instead of
-- ranking the entire pending backlog with a window function. Without it, a
-- claim over a large backlog scans every pending row (O(pending)); with it, the
-- cost is O(active tenants) and stays flat as the backlog grows.
CREATE TABLE active_orgs (
    org_id BIGINT NOT NULL,
    queue  TEXT   NOT NULL,
    PRIMARY KEY (org_id, queue)
);

-- Keep `active_orgs` current no matter how jobs arrive (the CLI, a bulk load,
-- or a requeue), so callers never have to remember to maintain it. A
-- statement-level trigger uses the transition table to collapse a whole batch
-- into a handful of distinct upserts.
--
-- The shared advisory lock serializes this against pruning. Without it there is
-- a time-of-check/time-of-use race: the pruner's `NOT EXISTS` snapshot cannot
-- see another transaction's *uncommitted* pending job, so it can delete an org
-- whose membership this trigger just found already present (ON CONFLICT DO
-- NOTHING), stranding that job forever. Shared locks do not block each other, so
-- concurrent claims/enqueues stay parallel; only the pruner (which takes the
-- matching *exclusive* lock) waits for them.
CREATE FUNCTION mark_active_orgs() RETURNS trigger AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(0x6163746976650001);
    INSERT INTO active_orgs (org_id, queue)
    SELECT DISTINCT org_id, queue FROM changed WHERE state = 'pending'
    ON CONFLICT DO NOTHING;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER jobs_mark_active_insert
    AFTER INSERT ON jobs
    REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT
    EXECUTE FUNCTION mark_active_orgs();

CREATE TRIGGER jobs_mark_active_update
    AFTER UPDATE ON jobs
    REFERENCING NEW TABLE AS changed
    FOR EACH STATEMENT
    EXECUTE FUNCTION mark_active_orgs();
