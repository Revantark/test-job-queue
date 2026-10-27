use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job_core::{ClaimedJob, Job, JobId, JobOutcome, Lease, NewJob, OrgId};
use job_store::{ClaimOptions, StatusCounts, Store, StoreError};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::queries as q;

/// Postgres-backed [`Store`].
///
/// Wraps a pool so a single instance is cheaply cloneable across the worker
/// pool (the pool itself is already reference-counted).
#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Build a store from an existing connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connect to `url` and return a ready store.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let pool = PgPool::connect(url)
            .await
            .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(Self { pool })
    }

    /// Access the underlying pool (used by the worker's reaper and tests).
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// How many times `job_id`'s effect has been committed (0 or 1).
    ///
    /// Because effects are keyed by job id and committed atomically, this is
    /// always exactly `1` for a completed job.
    pub async fn effect_committed(&self, job_id: JobId) -> Result<bool, StoreError> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT job_id FROM effects WHERE job_id = $1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        Ok(row.is_some())
    }

    /// Record a job's side effect **idempotently**.
    ///
    /// `job_id` is the primary key, so recording the same effect twice inserts
    /// nothing the second time. Returns `true` if this call created the effect,
    /// `false` if it was already present (i.e. a re-run after a crash). This is
    /// the real deduplication primitive: attempts are at-least-once, but a
    /// committed effect is recorded exactly once.
    pub async fn record_effect(&self, job_id: JobId, result: Value) -> Result<bool, StoreError> {
        let inserted: Option<(i64,)> = sqlx::query_as(q::RECORD_EFFECT)
            .bind(job_id)
            .bind(result)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        Ok(inserted.is_some())
    }
}

#[async_trait]
impl Store for PgStore {
    async fn enqueue(&self, job: NewJob) -> Result<JobId, StoreError> {
        job.validate()?;
        let (id,): (i64,) = sqlx::query_as(q::ENQUEUE)
            .bind(job.org_id)
            .bind(&job.kind)
            .bind(&job.queue)
            .bind(&job.payload)
            .bind(job.priority)
            .bind(job.scheduled_at)
            .bind(i32::try_from(job.max_attempts).unwrap_or(i32::MAX))
            .bind(&job.unique_key)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        // `active_orgs` is maintained by a trigger on `jobs`, so no bookkeeping
        // is needed here.
        Ok(id)
    }

    async fn claim(&self, queue: &str, opts: &ClaimOptions) -> Result<Vec<ClaimedJob>, StoreError> {
        let rows: Vec<q::JobRow> = sqlx::query_as(q::CLAIM)
            .bind(queue)
            .bind(opts.batch)
            .bind(opts.per_org_limit)
            .bind(&opts.worker)
            .bind(opts.lease.as_secs_f64())
            .bind(opts.per_org_inflight)
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;

        rows.into_iter().map(claim_from_row).collect()
    }

    async fn heartbeat(
        &self,
        job_id: JobId,
        lease_id: Uuid,
        lease: Duration,
    ) -> Result<bool, StoreError> {
        let row: Option<(i64,)> = sqlx::query_as(q::HEARTBEAT)
            .bind(job_id)
            .bind(lease.as_secs_f64())
            .bind(lease_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        Ok(row.is_some())
    }

    async fn complete(
        &self,
        job_id: JobId,
        lease_id: Uuid,
        _outcome: &JobOutcome,
    ) -> Result<bool, StoreError> {
        // Fenced terminal write. Handlers persist their own effects idempotently
        // (keyed by job_id), so the engine only flips state here. A duplicate
        // effect write after a crash is a no-op, giving exactly-once committed
        // effects even though attempts are at-least-once.
        let updated: Option<(i64,)> = sqlx::query_as(q::COMPLETE)
            .bind(job_id)
            .bind(lease_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        Ok(updated.is_some())
    }

    async fn fail(
        &self,
        job_id: JobId,
        lease_id: Uuid,
        outcome: &JobOutcome,
    ) -> Result<bool, StoreError> {
        let (error, fatal) = match outcome {
            JobOutcome::Retryable { error } => (error.clone(), false),
            JobOutcome::Fatal { error } => (error.clone(), true),
            JobOutcome::Success { .. } => {
                return Err(StoreError::Database(
                    "fail() called with a Success outcome".into(),
                ));
            }
        };

        let mut tx = self.pool.begin().await.map_err(db)?;

        // Backoff policy lives in `job-core::compute_backoff` (exponential with
        // jitter seeded by job id) so retries spread out instead of hammering a
        // shared dependency in lockstep. It needs the attempt count, which is a
        // cheap indexed read on the row we already hold.
        let (attempt,): (i32,) = sqlx::query_as("SELECT attempt FROM jobs WHERE id = $1")
            .bind(job_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(db)?;
        let backoff = job_core::compute_backoff(
            u32::try_from(attempt).unwrap_or(1),
            Duration::from_secs(1),
            Duration::from_secs(300),
            u64::try_from(job_id).unwrap_or(0),
        );

        let row: Option<(String, i32, i32, i64, String)> = sqlx::query_as(q::FAIL)
            .bind(job_id)
            .bind(lease_id)
            .bind(fatal)
            .bind(backoff.as_secs_f64())
            .bind(&error)
            .fetch_optional(&mut *tx)
            .await
            .map_err(db)?;

        tx.commit().await.map_err(db)?;
        Ok(row.is_some())
    }

    async fn reap(&self, grace: Duration) -> Result<u64, StoreError> {
        let rows: Vec<(i64, String)> = sqlx::query_as(q::REAP)
            .bind(grace.as_secs_f64())
            .fetch_all(&self.pool)
            .await
            .map_err(db)?;

        // Housekeeping: forget orgs that no longer have pending work, so the
        // active set (and thus the claim query) stays small. Serialized against
        // job writes via the exclusive advisory lock (see `mark_active_orgs`),
        // which is what makes the "no pending jobs" test race-free.
        let mut tx = self.pool.begin().await.map_err(db)?;
        sqlx::query(q::LOCK_ACTIVE_ORGS)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        sqlx::query(q::PRUNE_ACTIVE_ORGS)
            .execute(&mut *tx)
            .await
            .map_err(db)?;
        tx.commit().await.map_err(db)?;
        Ok(rows.len() as u64)
    }

    async fn status(&self, org_id: Option<OrgId>) -> Result<StatusCounts, StoreError> {
        let row: (i64, i64, i64, i64) = sqlx::query_as(q::STATUS)
            .bind(org_id)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(StatusCounts {
            pending: row.0,
            running: row.1,
            completed: row.2,
            failed: row.3,
        })
    }

    async fn get(&self, job_id: JobId) -> Result<Option<Job>, StoreError> {
        let row: Option<q::JobRow> = sqlx::query_as(q::GET_JOB)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(db)?;
        row.map(|r| Job::try_from(r).map_err(StoreError::from))
            .transpose()
    }

    async fn next_due_at(&self, queue: &str) -> Result<Option<DateTime<Utc>>, StoreError> {
        let row: (Option<DateTime<Utc>>,) = sqlx::query_as(q::NEXT_DUE)
            .bind(queue)
            .fetch_one(&self.pool)
            .await
            .map_err(db)?;
        Ok(row.0)
    }

    async fn migrate(&self) -> Result<(), StoreError> {
        crate::MIGRATOR
            .run(&self.pool)
            .await
            .map_err(|e| StoreError::Database(e.to_string()))?;
        Ok(())
    }
}

fn claim_from_row(row: q::JobRow) -> Result<ClaimedJob, StoreError> {
    let lease_id = row
        .lease_id
        .ok_or_else(|| StoreError::Database("claimed row missing lease_id".into()))?;
    let worker: String = row
        .locked_by
        .clone()
        .ok_or_else(|| StoreError::Database("claimed row missing locked_by".into()))?;
    let expires_at = row
        .lease_expires_at
        .ok_or_else(|| StoreError::Database("claimed row missing lease_expires_at".into()))?;

    let job = Job::try_from(row)?;
    Ok(ClaimedJob {
        job,
        lease: Lease {
            id: lease_id,
            worker,
            expires_at,
        },
    })
}

#[allow(clippy::needless_pass_by_value)]
fn db(e: sqlx::Error) -> StoreError {
    StoreError::Database(e.to_string())
}
