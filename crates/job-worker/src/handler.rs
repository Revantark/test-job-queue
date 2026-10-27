use async_trait::async_trait;
use job_core::JobOutcome;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

/// Everything a handler needs to do its work.
#[derive(Debug, Clone)]
pub struct JobContext {
    /// Id of the job being run.
    pub job_id: job_core::JobId,
    /// Owning organization.
    pub org_id: job_core::OrgId,
    /// Which attempt this is (1 for the first).
    pub attempt: u32,
    /// Handler-specific arguments.
    pub payload: Value,
    /// Signalled when the worker loses its lease; handlers should check it at
    /// coarse checkpoints and abort, since another worker may already own the
    /// job.
    pub cancel: CancellationToken,
}

impl JobContext {
    /// Returns `true` once the worker has lost ownership of the job.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
}

/// Boxed future returned by a handler.
pub type HandlerFuture = std::pin::Pin<Box<dyn std::future::Future<Output = JobOutcome> + Send>>;

/// A unit of work, selected by the job's `kind`.
///
/// # Contract
///
/// Handlers **must be idempotent**: delivery is at-least-once, so a handler may
/// run again after a crash recovery. Effectful handlers should key their writes
/// on `job_id` (see the `db_effect` sample handler).
#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// The `kind` string this handler serves.
    fn kind(&self) -> &'static str;

    /// Execute the job. Use [`JobContext::is_cancelled`] at checkpoints.
    async fn run(&self, ctx: JobContext) -> JobOutcome;
}
