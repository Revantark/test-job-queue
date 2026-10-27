//! Pure domain model for the scheduler.
//!
//! This crate has **no I/O dependencies** on purpose: the worker, store and CLI
//! all depend *inward* on these types, so storage or transport concerns can
//! never leak into the domain.

#![warn(missing_docs)]

mod backoff;
mod job;

pub use backoff::compute_backoff;
pub use job::{
    ClaimedJob, Job, JobError, JobId, JobOutcome, JobState, Lease, NewJob, OrgId, WorkerId,
};
