use async_trait::async_trait;
use job_core::JobOutcome;
use job_worker::{Handler, JobContext};

/// Fails retryably until `fail_until_attempt`, then succeeds.
///
/// Drives the retry/backoff path. With `fail_until_attempt` beyond
/// `max_attempts` the job is dead-lettered instead, exercising that branch too.
pub struct FailHandler {
    fail_until_attempt: u32,
}

impl FailHandler {
    /// Fail every attempt below `fail_until_attempt`.
    pub fn new(fail_until_attempt: u32) -> Self {
        Self { fail_until_attempt }
    }
}

impl Default for FailHandler {
    fn default() -> Self {
        Self::new(2)
    }
}

#[async_trait]
impl Handler for FailHandler {
    fn kind(&self) -> &'static str {
        "fail"
    }

    async fn run(&self, ctx: JobContext) -> JobOutcome {
        if ctx.attempt < self.fail_until_attempt {
            JobOutcome::Retryable {
                error: format!("attempt {} below threshold", ctx.attempt),
            }
        } else {
            JobOutcome::success()
        }
    }
}
