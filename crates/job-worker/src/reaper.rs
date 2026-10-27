use std::sync::Arc;
use std::time::Duration;

use job_store::Store;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Tunables for the reaper.
#[derive(Debug, Clone)]
pub struct ReaperConfig {
    /// How often to sweep.
    pub interval: Duration,
    /// Extra backoff applied to a reclaimed job before it becomes due again, so
    /// a flapping worker does not spin the job.
    pub requeue_backoff: Duration,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            requeue_backoff: Duration::from_secs(1),
        }
    }
}

/// Periodically reclaims jobs whose lease expired.
///
/// A crashed worker stops heartbeating; its lease lapses; the reaper returns the
/// job to `pending` (or dead-letters it past `max_attempts`). This is the
/// mechanism that makes "jobs must not be lost when a worker dies" true.
pub struct Reaper {
    store: Arc<dyn Store>,
    config: ReaperConfig,
}

impl Reaper {
    /// Build a reaper over any store.
    pub fn new(store: Arc<dyn Store>, config: ReaperConfig) -> Self {
        Self { store, config }
    }

    /// Run until `shutdown` is cancelled.
    pub async fn run(self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.config.interval);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    match self.store.reap(self.config.requeue_backoff).await {
                        Ok(0) => {}
                        Ok(n) => info!(reclaimed = n, "reaper requeued expired leases"),
                        Err(e) => warn!(error = %e, "reaper sweep failed"),
                    }
                }
                () = shutdown.cancelled() => return,
            }
        }
    }
}
