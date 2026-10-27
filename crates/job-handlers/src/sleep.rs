use async_trait::async_trait;
use job_core::JobOutcome;
use job_worker::{Handler, JobContext};
use rand::RngExt;
use tracing::info;

/// Sleeps for a random duration in `[min_ms, max_ms]`.
///
/// Exists so load tests can mix fast and slow jobs and show that one slow job
/// does not block the others sharing the pool.
pub struct SleepHandler {
    min_ms: u64,
    max_ms: u64,
}

impl SleepHandler {
    /// A handler sleeping between `min_ms` and `max_ms` milliseconds.
    pub fn new(min_ms: u64, max_ms: u64) -> Self {
        Self {
            min_ms,
            max_ms: max_ms.max(min_ms),
        }
    }
}

impl Default for SleepHandler {
    fn default() -> Self {
        Self::new(10, 500)
    }
}

#[async_trait]
impl Handler for SleepHandler {
    fn kind(&self) -> &'static str {
        "sleep"
    }

    async fn run(&self, ctx: JobContext) -> JobOutcome {
        // Payload may override the bounds so tests and load scripts can request
        // a specific (often long) duration.
        let min_ms = payload_u64(&ctx.payload, "min_ms").unwrap_or(self.min_ms);
        let max_ms = payload_u64(&ctx.payload, "max_ms")
            .unwrap_or(self.max_ms)
            .max(min_ms);

        let ms = if max_ms == min_ms {
            min_ms
        } else {
            rand::rng().random_range(min_ms..=max_ms)
        };

        // Abort cleanly if we lose the lease mid-sleep, rather than blocking the
        // pool on a job we no longer own.
        tokio::select! {
            () = tokio::time::sleep(std::time::Duration::from_millis(ms)) => {}
            () = ctx.cancel.cancelled() => {
                return JobOutcome::Retryable { error: "cancelled: lease lost".into() };
            }
        }

        info!(job_id = ctx.job_id, slept_ms = ms, "slept");
        JobOutcome::success()
    }
}

/// Read an unsigned integer field from a JSON payload, if present.
fn payload_u64(payload: &serde_json::Value, key: &str) -> Option<u64> {
    payload.get(key).and_then(serde_json::Value::as_u64)
}
