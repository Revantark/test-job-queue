use chrono::{DateTime, Utc};
use job_core::{Job, JobError};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

/// Flat row representation mapped directly from the `jobs` table.
#[derive(Debug, FromRow)]
pub struct JobRow {
    pub id: i64,
    pub org_id: i64,
    pub kind: String,
    pub queue: String,
    pub payload: Value,
    pub priority: i32,
    pub scheduled_at: DateTime<Utc>,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub state: String,
    pub attempt: i32,
    pub max_attempts: i32,
    pub lease_id: Option<Uuid>,
    pub locked_by: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub unique_key: Option<String>,
    pub created_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl TryFrom<JobRow> for Job {
    type Error = JobError;

    fn try_from(r: JobRow) -> Result<Self, Self::Error> {
        Ok(Job {
            id: r.id,
            org_id: r.org_id,
            kind: r.kind,
            queue: r.queue,
            payload: r.payload,
            priority: r.priority,
            scheduled_at: r.scheduled_at,
            next_retry_at: r.next_retry_at,
            state: r.state.parse()?,
            attempt: u32::try_from(r.attempt)
                .map_err(|_| JobError::Invalid("negative attempt".into()))?,
            max_attempts: u32::try_from(r.max_attempts)
                .map_err(|_| JobError::Invalid("negative max_attempts".into()))?,
            lease_id: r.lease_id,
            locked_by: r.locked_by,
            lease_expires_at: r.lease_expires_at,
            last_error: r.last_error,
            unique_key: r.unique_key,
            created_at: r.created_at,
            finished_at: r.finished_at,
        })
    }
}

/// Insert a new job.
pub const ENQUEUE: &str = "INSERT INTO jobs \
    (org_id, kind, queue, payload, priority, scheduled_at, max_attempts, unique_key) \
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING id";

/// Take the exclusive side of the active-orgs lock so pruning cannot interleave
/// with job writes (which take it in shared mode). Must run inside a transaction.
pub const LOCK_ACTIVE_ORGS: &str = "SELECT pg_advisory_xact_lock(0x6163746976650001)";

/// Drop organizations that no longer have pending work (called by the reaper).
///
/// Takes the *exclusive* form of the lock the enqueue trigger takes in shared
/// mode, so pruning cannot run concurrently with a job write. This closes the
/// race where the pruner's snapshot misses an uncommitted pending job and
/// deletes an org membership as it is being created.
pub const PRUNE_ACTIVE_ORGS: &str = "DELETE FROM active_orgs a \
    WHERE NOT EXISTS ( \
        SELECT 1 FROM jobs j \
        WHERE j.org_id = a.org_id AND j.queue = a.queue AND j.state = 'pending' \
    )";

/// Claim a fair batch of due jobs.
///
/// Fairness: iterate the *active tenants* (`active_orgs`) rather than ranking
/// the whole pending backlog, so cost is O(active tenants) and stays flat as the
/// backlog grows. Each tenant contributes up to `$3` jobs, interleaved
/// round-robin via `pos`.
///
/// Priority: candidates within a tenant are ordered by (priority, effective
/// due), that order is carried out of the lateral and re-applied in the outer
/// sort — so a partial batch still prefers the higher-priority job, and
/// `random()` only breaks ties *between equal-priority tenants* (which is what
/// keeps a small tenant from being starved by a large one).
///
/// In-flight isolation: `$6` caps how many jobs an org may have *running*; the
/// per-tenant candidate limit shrinks by that org's current running count, so a
/// tenant holding long jobs cannot occupy the whole pool across claim rounds.
///
/// `FOR UPDATE SKIP LOCKED` must sit on the base `jobs` scan: it only skips rows
/// actually being locked, so two workers running this always get disjoint sets.
pub const CLAIM: &str = r"
WITH inflight AS (
    SELECT org_id, count(*) AS n
    FROM jobs
    WHERE state = 'running' AND queue = $1
    GROUP BY org_id
)
UPDATE jobs j
SET state = 'running',
    attempt = j.attempt + 1,
    locked_by = $4,
    lease_id = gen_random_uuid(),
    lease_expires_at = now() + make_interval(secs => $5)
FROM (
    SELECT id FROM (
        SELECT c.id,
               c.priority,
               c.due,
               row_number() OVER (PARTITION BY a.org_id ORDER BY c.priority, c.due, c.id) AS pos
        FROM active_orgs a
        LEFT JOIN inflight i ON i.org_id = a.org_id
        CROSS JOIN LATERAL (
            SELECT j2.id,
                   j2.priority,
                   COALESCE(j2.next_retry_at, j2.scheduled_at) AS due
            FROM jobs j2
            WHERE j2.org_id = a.org_id
              AND j2.queue = a.queue
              AND j2.state = 'pending'
              AND COALESCE(j2.next_retry_at, j2.scheduled_at) <= now()
            ORDER BY j2.priority, COALESCE(j2.next_retry_at, j2.scheduled_at), j2.id
            LIMIT LEAST(
                COALESCE($3, 4),
                GREATEST(0, COALESCE($6::bigint, 2147483647) - COALESCE(i.n, 0))
            )
            FOR UPDATE SKIP LOCKED
        ) c
        WHERE a.queue = $1
    ) ranked
    -- Round-robin across tenants (`pos`), then by priority so a tenant whose
    -- best job is more urgent goes first, then `random()` to break ties among
    -- equal-priority tenants (which is what rotates a partial batch fairly
    -- instead of always favouring the lowest org id). Note: `due` is
    -- deliberately NOT in this outer sort — it is already encoded inside each
    -- tenant's `pos`, and including it here would deterministically favour the
    -- earliest-inserted org.
    ORDER BY ranked.pos, ranked.priority, random()
    LIMIT $2
) picked
WHERE j.id = picked.id
RETURNING j.id, j.org_id, j.kind, j.queue, j.payload, j.priority,
          j.scheduled_at, j.next_retry_at, j.state, j.attempt, j.max_attempts,
          j.lease_id, j.locked_by, j.lease_expires_at, j.last_error,
          j.unique_key, j.created_at, j.finished_at";

/// Renew a lease. The extra predicates reject a renewal that arrives after the
/// lease has already expired or been stolen, so a stalled worker learns it lost
/// the job instead of clobbering its replacement.
pub const HEARTBEAT: &str = "
UPDATE jobs
SET lease_expires_at = now() + make_interval(secs => $2)
WHERE id = $1
  AND lease_id = $3
  AND state = 'running'
  AND lease_expires_at > now()
RETURNING id";

/// Terminal success, fenced by the lease.
pub const COMPLETE: &str = "
UPDATE jobs
SET state = 'completed',
    finished_at = now(),
    lease_id = NULL,
    locked_by = NULL,
    lease_expires_at = NULL,
    last_error = NULL
WHERE id = $1 AND lease_id = $2 AND state = 'running'
RETURNING id";

/// Record a failed attempt. Requeues with a backoff when attempts remain,
/// otherwise dead-letters. The `attempt < max_attempts` test lives in SQL so a
/// job cannot be requeued past its cap even if two writers race.
pub const FAIL: &str = r"
UPDATE jobs
SET state = CASE WHEN $3::boolean OR attempt >= max_attempts THEN 'failed' ELSE 'pending' END,
    next_retry_at = CASE
        WHEN $3::boolean OR attempt >= max_attempts THEN next_retry_at
        ELSE now() + make_interval(secs => $4)
    END,
    finished_at = CASE
        WHEN $3::boolean OR attempt >= max_attempts THEN now()
        ELSE finished_at
    END,
    last_error = $5,
    lease_id = NULL,
    locked_by = NULL,
    lease_expires_at = NULL
WHERE id = $1 AND lease_id = $2 AND state = 'running'
RETURNING state, attempt, max_attempts, org_id, queue";

/// Reclaim jobs whose lease lapsed. A crashed worker stops heartbeating, its
/// lease expires, and this returns the job to circulation (or dead-letters it).
/// The trigger on `jobs` re-marks the org active if the job is requeued.
pub const REAP: &str = r"
UPDATE jobs
SET state = CASE WHEN attempt >= max_attempts THEN 'failed' ELSE 'pending' END,
    next_retry_at = CASE
        WHEN attempt >= max_attempts THEN next_retry_at
        ELSE now() + make_interval(secs => $1)
    END,
    finished_at = CASE WHEN attempt >= max_attempts THEN now() ELSE finished_at END,
    last_error = COALESCE(last_error, 'reclaimed after lease expiry'),
    lease_id = NULL,
    locked_by = NULL,
    lease_expires_at = NULL
WHERE state = 'running' AND lease_expires_at < now()
RETURNING id, state";

/// Status counts, optionally scoped to one organization.
pub const STATUS: &str = "
SELECT
    count(*) FILTER (WHERE state = 'pending')   AS pending,
    count(*) FILTER (WHERE state = 'running')   AS running,
    count(*) FILTER (WHERE state = 'completed') AS completed,
    count(*) FILTER (WHERE state = 'failed')    AS failed
FROM jobs
WHERE $1::bigint IS NULL OR org_id = $1";

/// Soonest runnable job in a queue, used to sleep instead of busy-polling.
pub const NEXT_DUE: &str = "
SELECT min(COALESCE(next_retry_at, scheduled_at)) AS due_at
FROM jobs
WHERE state = 'pending' AND queue = $1";

/// Fetch one job by id.
pub const GET_JOB: &str = "
SELECT id, org_id, kind, queue, payload, priority, scheduled_at, next_retry_at,
       state, attempt, max_attempts, lease_id, locked_by, lease_expires_at,
       last_error, unique_key, created_at, finished_at
FROM jobs WHERE id = $1";

/// Record a committed side effect. Idempotent by construction: `job_id` is the
/// primary key, so a duplicate execution records nothing new (`RETURNING` yields
/// a row only on the first insert).
pub const RECORD_EFFECT: &str = "
INSERT INTO effects (job_id, result)
VALUES ($1, $2)
ON CONFLICT (job_id) DO NOTHING
RETURNING job_id";
