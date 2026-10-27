//! Sample handlers used to exercise the engine.
//!
//! These are demonstrations, not domain logic: `sleep` simulates variable
//! durations, `fail` drives the retry path, and `db_effect` proves idempotency.

#![warn(missing_docs)]

mod db_effect;
mod fail;
mod sleep;

pub use db_effect::DbEffectHandler;
pub use fail::FailHandler;
pub use sleep::SleepHandler;

use job_core::JobId;

/// A place to record a job's side effect idempotently.
///
/// Kept as a trait so handlers never depend on a concrete store; the binary
/// wires in the Postgres-backed sink. Implementations must deduplicate on
/// `job_id`, so a re-run after a crash records nothing new.
#[async_trait::async_trait]
pub trait EffectSink: Send + Sync + 'static {
    /// Record the effect for `job_id`. Returns `true` if this call created the
    /// effect, `false` if it already existed (a deduplicated re-run).
    async fn record(&self, job_id: JobId)
    -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;
}

/// A sink that forgets everything; useful for `sleep`-only workers and tests.
#[derive(Default, Clone, Copy)]
pub struct NoopEffectSink;

#[async_trait::async_trait]
impl EffectSink for NoopEffectSink {
    async fn record(
        &self,
        _job_id: JobId,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        Ok(true)
    }
}
