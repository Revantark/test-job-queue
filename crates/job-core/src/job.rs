use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

/// Primary key of a job (`BIGSERIAL` in Postgres).
pub type JobId = i64;
/// Owning organization. Drives multi-tenant fairness.
pub type OrgId = i64;
/// Identifier of a consumer process, used only for diagnostics and ownership.
pub type WorkerId = String;

/// Errors raised while building or parsing domain values.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    /// A required field was empty or otherwise malformed.
    #[error("invalid job: {0}")]
    Invalid(String),
    /// A persisted state string did not match any known state.
    #[error("unknown job state: {0}")]
    UnknownState(String),
}

/// Lifecycle state of a job.
///
/// Kept as a flat enum (rather than a data-carrying one) so it maps cleanly to
/// the single `state` column; illegal combinations are instead prevented by the
/// DB check constraint and the `Job` validation helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobState {
    /// Due to be claimed when its effective time arrives.
    Pending,
    /// Claimed by a worker under a live lease.
    Running,
    /// Terminal success.
    Completed,
    /// Terminal failure (attempts exhausted, fatal error, or unknown kind).
    Failed,
}

impl JobState {
    /// The canonical lowercase string stored in the database.
    pub fn as_str(self) -> &'static str {
        match self {
            JobState::Pending => "pending",
            JobState::Running => "running",
            JobState::Completed => "completed",
            JobState::Failed => "failed",
        }
    }

    /// Whether the job has reached a state it can never leave.
    pub fn is_terminal(self) -> bool {
        matches!(self, JobState::Completed | JobState::Failed)
    }
}

impl fmt::Display for JobState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for JobState {
    type Err = JobError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(JobState::Pending),
            "running" => Ok(JobState::Running),
            "completed" => Ok(JobState::Completed),
            "failed" => Ok(JobState::Failed),
            other => Err(JobError::UnknownState(other.to_owned())),
        }
    }
}

/// A request to schedule a job. Carries no identity or lifecycle — the store
/// assigns those.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewJob {
    /// Tenant the job belongs to.
    pub org_id: OrgId,
    /// Handler name, e.g. `"send_email"`.
    pub kind: String,
    /// Logical queue for routing and capacity.
    pub queue: String,
    /// Handler-specific arguments; opaque to the scheduler.
    pub payload: Value,
    /// When the job becomes due.
    pub scheduled_at: DateTime<Utc>,
    /// Lower runs first.
    pub priority: i32,
    /// Maximum number of claims before the job is dead-lettered.
    pub max_attempts: u32,
    /// Optional dedup key; at most one live job may hold a given key.
    pub unique_key: Option<String>,
}

impl NewJob {
    /// Build a job with the standard defaults for queue, priority and retries.
    pub fn new(org_id: OrgId, kind: impl Into<String>, scheduled_at: DateTime<Utc>) -> Self {
        Self {
            org_id,
            kind: kind.into(),
            queue: "default".to_owned(),
            payload: Value::Object(Default::default()),
            scheduled_at,
            priority: 100,
            max_attempts: 3,
            unique_key: None,
        }
    }

    /// Set the queue.
    #[must_use]
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = queue.into();
        self
    }

    /// Set the payload.
    #[must_use]
    pub fn payload(mut self, payload: Value) -> Self {
        self.payload = payload;
        self
    }

    /// Set the priority (lower runs first).
    #[must_use]
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the maximum number of attempts.
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the dedup key.
    #[must_use]
    pub fn unique_key(mut self, key: impl Into<String>) -> Self {
        self.unique_key = Some(key.into());
        self
    }

    /// Validate invariants that the database would otherwise reject.
    pub fn validate(&self) -> Result<(), JobError> {
        if self.kind.trim().is_empty() {
            return Err(JobError::Invalid("kind must not be empty".into()));
        }
        if self.queue.trim().is_empty() {
            return Err(JobError::Invalid("queue must not be empty".into()));
        }
        if self.max_attempts == 0 {
            return Err(JobError::Invalid("max_attempts must be at least 1".into()));
        }
        Ok(())
    }
}

/// A persisted job row.
#[derive(Debug, Clone, PartialEq)]
pub struct Job {
    /// Primary key.
    pub id: JobId,
    /// Owning organization.
    pub org_id: OrgId,
    /// Handler selector.
    pub kind: String,
    /// Queue name.
    pub queue: String,
    /// Handler-specific arguments.
    pub payload: Value,
    /// Lower runs first.
    pub priority: i32,
    /// Tenant intent; never mutated after insert.
    pub scheduled_at: DateTime<Utc>,
    /// Set after a retryable failure or a crash recovery; `None` until then.
    pub next_retry_at: Option<DateTime<Utc>>,
    /// Current lifecycle state.
    pub state: JobState,
    /// How many times the job has been claimed so far.
    pub attempt: u32,
    /// Attempts allowed before dead-lettering.
    pub max_attempts: u32,
    /// Fencing token of the current claim, when running.
    pub lease_id: Option<Uuid>,
    /// Worker holding the current claim, when running.
    pub locked_by: Option<WorkerId>,
    /// When the current claim's lease lapses without renewal.
    pub lease_expires_at: Option<DateTime<Utc>>,
    /// Most recent failure message, if any.
    pub last_error: Option<String>,
    /// Dedup key, if set.
    pub unique_key: Option<String>,
    /// Insert time.
    pub created_at: DateTime<Utc>,
    /// Time the job reached a terminal state.
    pub finished_at: Option<DateTime<Utc>>,
}

impl Job {
    /// The time the job is next eligible to run: an active retry wins over the
    /// original schedule.
    pub fn effective_due_at(&self) -> DateTime<Utc> {
        self.next_retry_at.unwrap_or(self.scheduled_at)
    }

    /// Whether the running state carries a complete ownership triple.
    pub fn has_live_lease(&self) -> bool {
        self.lease_id.is_some() && self.locked_by.is_some() && self.lease_expires_at.is_some()
    }
}

/// Proof of ownership held by a worker for a claimed job.
///
/// The store renews the lease (heartbeat) and fences terminal writes with
/// [`Lease::id`], so a worker whose lease was already reclaimed cannot complete
/// the job it thinks it owns.
#[derive(Debug, Clone, PartialEq)]
pub struct Lease {
    /// Fencing token; a fresh value on every claim.
    pub id: Uuid,
    /// Worker holding the claim.
    pub worker: WorkerId,
    /// When the lease lapses without renewal.
    pub expires_at: DateTime<Utc>,
}

/// A job this worker currently owns. The only way to obtain one is to claim it,
/// which makes ownership a type-level guarantee rather than a convention.
#[derive(Debug, Clone)]
pub struct ClaimedJob {
    /// The claimed row (its state is `Running`).
    pub job: Job,
    /// The lease that proves ownership.
    pub lease: Lease,
}

impl ClaimedJob {
    /// Number of attempts including this one.
    pub fn attempt(&self) -> u32 {
        self.job.attempt
    }
}

/// The result of running a handler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum JobOutcome {
    /// The handler succeeded; persist the result.
    Success {
        /// Arbitrary JSON result, stored with the job's effect.
        result: Value,
    },
    /// Transient failure; retry if attempts remain, otherwise dead-letter.
    Retryable {
        /// Description of the failure.
        error: String,
    },
    /// Permanent failure; dead-letter immediately regardless of attempts left.
    Fatal {
        /// Description of the failure.
        error: String,
    },
}

impl JobOutcome {
    /// A successful outcome with a null result.
    pub fn success() -> Self {
        JobOutcome::Success {
            result: Value::Null,
        }
    }
}
