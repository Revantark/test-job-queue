//! Storage abstraction for the scheduler.
//!
//! Everything the worker needs is expressed as a small, focused trait
//! ([`Store`]); nothing here mentions SQL or Postgres. A concrete backend lives
//! in a sibling crate and is only selected in the binary's composition root.

#![warn(missing_docs)]

mod error;

pub use error::StoreError;

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use job_core::{ClaimedJob, Job, JobId, JobOutcome, NewJob, OrgId, WorkerId};

/// Aggregated counts used by the `status` command.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatusCounts {
    /// Jobs waiting to be claimed.
    pub pending: i64,
    /// Jobs currently claimed by a worker.
    pub running: i64,
    /// Jobs that finished successfully.
    pub completed: i64,
    /// Jobs that were dead-lettered.
    pub failed: i64,
}

/// Options for a single claim round.
#[derive(Debug, Clone)]
pub struct ClaimOptions {
    /// Maximum number of jobs to claim in one round.
    pub batch: i64,
    /// How long the lease stays valid without a heartbeat.
    pub lease: Duration,
    /// Identity of the claiming worker.
    pub worker: WorkerId,
    /// Maximum jobs contributed per organization within this batch. `None`
    /// disables the per-org cap.
    pub per_org_limit: Option<i64>,
    /// Maximum jobs an organization may have *running at once* (across all
    /// workers). `None` disables the in-flight cap. This is what stops a tenant
    /// with long jobs from occupying the whole pool before others submit work.
    pub per_org_inflight: Option<i64>,
}

/// The storage contract the worker depends on (dependency inversion).
///
/// Implementations must guarantee that a job claimed by one worker cannot be
/// claimed by another until its lease expires or it is released.
#[async_trait]
pub trait Store: Send + Sync + 'static {
    /// Persist a new job, returning its assigned id.
    async fn enqueue(&self, job: NewJob) -> Result<JobId, StoreError>;

    /// Atomically claim up to `opts.batch` due jobs for `opts.worker`.
    async fn claim(&self, queue: &str, opts: &ClaimOptions) -> Result<Vec<ClaimedJob>, StoreError>;

    /// Extend the lease of a running job by `lease`; returns `false` if it is no
    /// longer owned by this lease (i.e. it was reclaimed).
    async fn heartbeat(
        &self,
        job_id: JobId,
        lease_id: uuid::Uuid,
        lease: Duration,
    ) -> Result<bool, StoreError>;

    /// Mark a claimed job completed (fenced by lease).
    async fn complete(
        &self,
        job_id: JobId,
        lease_id: uuid::Uuid,
        outcome: &JobOutcome,
    ) -> Result<bool, StoreError>;

    /// Record a failed attempt: requeue with backoff or dead-letter.
    async fn fail(
        &self,
        job_id: JobId,
        lease_id: uuid::Uuid,
        outcome: &JobOutcome,
    ) -> Result<bool, StoreError>;

    /// Reclaim jobs whose lease expired. Returns how many were affected.
    async fn reap(&self, grace: Duration) -> Result<u64, StoreError>;

    /// Counts per state, optionally scoped to one organization.
    async fn status(&self, org_id: Option<OrgId>) -> Result<StatusCounts, StoreError>;

    /// Look up a job by id.
    async fn get(&self, job_id: JobId) -> Result<Option<Job>, StoreError>;

    /// Timestamp of the soonest runnable job, if any (used to sleep efficiently).
    async fn next_due_at(&self, queue: &str) -> Result<Option<DateTime<Utc>>, StoreError>;

    /// Run pending migrations.
    async fn migrate(&self) -> Result<(), StoreError>;
}
