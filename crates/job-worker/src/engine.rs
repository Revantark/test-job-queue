use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use job_core::{ClaimedJob, JobOutcome};
use job_store::{ClaimOptions, Store};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::handler::JobContext;
use crate::registry::Registry;

/// Tunables for a worker process.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Queue to pull from.
    pub queue: String,
    /// Identity recorded on claimed rows.
    pub worker_id: String,
    /// Number of jobs executed concurrently.
    pub pool_size: usize,
    /// Jobs requested per claim round. Defaults to `pool_size` so one round
    /// trip fills the pool without stranding a large reserved batch.
    pub claim_batch: i64,
    /// Lease duration; the heartbeat renews it at `lease / heartbeat_divisor`.
    pub lease: Duration,
    /// Maximum jobs contributed per organization within a single claim round.
    pub per_org_limit: Option<i64>,
    /// Maximum jobs an organization may have running at once (across workers).
    /// Prevents a tenant with long jobs from occupying the whole pool.
    pub per_org_inflight: Option<i64>,
    /// How long to sleep when nothing is due.
    pub idle_sleep: Duration,
    /// Maximum time to wait for in-flight jobs to finish on shutdown before
    /// abandoning stuck ones (the reaper recovers them).
    pub drain_timeout: Duration,
}

impl WorkerConfig {
    /// Sensible defaults for a queue and worker id.
    pub fn new(queue: impl Into<String>, worker_id: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            worker_id: worker_id.into(),
            pool_size: 4,
            claim_batch: 4,
            lease: Duration::from_secs(30),
            per_org_limit: Some(2),
            per_org_inflight: Some(2),
            idle_sleep: Duration::from_millis(200),
            drain_timeout: Duration::from_secs(10),
        }
    }

    fn heartbeat_interval(&self) -> Duration {
        // Renew several times per lease so a single missed tick is harmless.
        (self.lease / 3).max(Duration::from_millis(250))
    }
}

/// A running worker: claims due jobs and executes them, renewing leases.
pub struct Worker {
    store: Arc<dyn Store>,
    registry: Registry,
    config: WorkerConfig,
}

impl Worker {
    /// Build a worker over any store implementation.
    pub fn new(store: Arc<dyn Store>, registry: Registry, config: WorkerConfig) -> Self {
        Self {
            store,
            registry,
            config,
        }
    }

    /// Run until `shutdown` is cancelled. Executes up to `pool_size` jobs
    /// concurrently.
    pub async fn run(self, shutdown: CancellationToken) {
        info!(queue = %self.config.queue, worker = %self.config.worker_id, "worker starting");
        let mut inflight: JoinSet<()> = JoinSet::new();

        loop {
            // Keep the pool topped up before claiming more work. Inspect the
            // finished task's result so a panicking execution task is surfaced
            // rather than silently dropped.
            while inflight.len() >= self.config.pool_size {
                match inflight.join_next().await {
                    Some(Ok(())) => {}
                    Some(Err(e)) if e.is_panic() => {
                        error!("execution task panicked: {e}");
                    }
                    Some(Err(e)) => warn!("execution task cancelled: {e}"),
                    None => break,
                }
            }

            if shutdown.is_cancelled() {
                break;
            }

            let free = self.config.pool_size - inflight.len();
            let free = i64::try_from(free).unwrap_or(i64::MAX);
            let batch = self.config.claim_batch.min(free).max(1);
            let opts = ClaimOptions {
                batch,
                lease: self.config.lease,
                worker: self.config.worker_id.clone(),
                per_org_limit: self.config.per_org_limit,
                per_org_inflight: self.config.per_org_inflight,
            };

            match self.store.claim(&self.config.queue, &opts).await {
                Ok(claimed) if !claimed.is_empty() => {
                    for job in claimed {
                        let store = Arc::clone(&self.store);
                        let registry = self.registry.clone();
                        let cfg = self.config.clone();
                        let shutdown = shutdown.clone();
                        inflight.spawn(async move {
                            execute(store, registry, cfg, job, shutdown).await;
                        });
                    }
                }
                Ok(_) => self.sleep_until_due(&shutdown).await,
                Err(e) => {
                    error!(error = %e, "claim failed; backing off");
                    tokio::time::sleep(self.config.idle_sleep).await;
                }
            }
        }

        info!("worker draining in-flight jobs before exit");
        // Drain in-flight jobs, but do not wait forever: a handler that ignores
        // cancellation (a "stuck" job) must not block shutdown indefinitely. Any
        // job still running when the deadline passes is left running; its lease
        // expires and the reaper recovers it.
        let drain_deadline = tokio::time::Instant::now() + self.config.drain_timeout;
        while !inflight.is_empty() {
            match tokio::time::timeout_at(drain_deadline, inflight.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(e))) if e.is_panic() => error!("execution task panicked: {e}"),
                Ok(Some(Err(e))) => warn!("execution task cancelled: {e}"),
                Ok(None) => break,
                Err(_) => {
                    warn!(
                        remaining = inflight.len(),
                        "drain timed out; abandoning stuck jobs (reaper will reclaim)"
                    );
                    inflight.abort_all();
                    break;
                }
            }
        }
        info!("worker stopped");
    }

    /// Sleep until the next job is due, capped at the idle interval so newly
    /// inserted immediate work is picked up promptly (a long sleep here is what
    /// lets a just-enqueued job drift). Lower latency still comes from
    /// `LISTEN/NOTIFY`, noted in the design doc.
    async fn sleep_until_due(&self, shutdown: &CancellationToken) {
        let delay = match self.store.next_due_at(&self.config.queue).await {
            Ok(Some(due)) => {
                let now = chrono::Utc::now();
                let until = due.signed_duration_since(now);
                if until.num_milliseconds() <= 0 {
                    self.config.idle_sleep
                } else {
                    Duration::from_millis(u64::try_from(until.num_milliseconds()).unwrap_or(0))
                        .min(self.config.idle_sleep)
                }
            }
            _ => self.config.idle_sleep,
        };
        tokio::select! {
            () = tokio::time::sleep(delay) => {}
            () = shutdown.cancelled() => {}
        }
    }
}

/// Execute one claimed job to completion, renewing its lease as it runs.
async fn execute(
    store: Arc<dyn Store>,
    registry: Registry,
    config: WorkerConfig,
    claimed: ClaimedJob,
    shutdown: CancellationToken,
) {
    let job_id = claimed.job.id;
    let lease_id = claimed.lease.id;
    let cancel = CancellationToken::new();

    // The heartbeat must run *concurrently* with the handler. Its lifetime is
    // tied to this function via an RAII guard, so even a panic in the handler
    // stops the heartbeat (a detached task would otherwise renew the lease
    // forever, and the reaper would never reclaim the job).
    let heartbeat = HeartbeatGuard::spawn(
        Arc::clone(&store),
        job_id,
        lease_id,
        config.lease,
        config.heartbeat_interval(),
        cancel.clone(),
    );

    let outcome = match registry.get(&claimed.job.kind) {
        Some(handler) => {
            let ctx = JobContext {
                job_id,
                org_id: claimed.job.org_id,
                attempt: claimed.job.attempt,
                payload: claimed.job.payload.clone(),
                cancel: cancel.clone(),
            };
            // A panicking handler must not kill the worker process or leave the
            // job wedged: catch it and treat the attempt as a (retryable)
            // failure. `AssertUnwindSafe` is sound here because we discard all
            // handler state on panic and only inspect `outcome` afterwards.
            tokio::select! {
                biased;
                // On shutdown, cancel the handler so a cooperative long-running
                // job can stop; the attempt is finalized as a retry.
                () = shutdown.cancelled() => {
                    cancel.cancel();
                    JobOutcome::Retryable { error: "worker shutting down".into() }
                }
                result = std::panic::AssertUnwindSafe(handler.run(ctx)).catch_unwind() => {
                    // Panics are per-attempt failures, not process death.
                    result.unwrap_or_else(|_| {
                        error!(job_id, "handler panicked; treating attempt as failed");
                        JobOutcome::Retryable { error: "handler panicked".into() }
                    })
                }
            }
        }
        None => {
            // An unregistered kind is a configuration error, not a transient
            // fault; retrying it forever would only waste attempts.
            JobOutcome::Fatal {
                error: format!("no handler registered for kind '{}'", claimed.job.kind),
            }
        }
    };

    // Stop the heartbeat before finalizing (also done on drop, belt-and-braces).
    heartbeat.stop().await;

    let result = match &outcome {
        JobOutcome::Success { .. } => store.complete(job_id, lease_id, &outcome).await,
        JobOutcome::Retryable { .. } | JobOutcome::Fatal { .. } => {
            store.fail(job_id, lease_id, &outcome).await
        }
    };

    match result {
        Ok(false) => warn!(job_id, "finalize fenced out; lease was reclaimed"),
        Ok(true) => {}
        Err(e) => error!(job_id, error = %e, "finalize failed"),
    }
}

/// Owns a heartbeat task and stops it on drop, so the task cannot outlive the
/// execution it belongs to — including when the handler panics.
struct HeartbeatGuard {
    handle: Option<tokio::task::JoinHandle<()>>,
    cancel: CancellationToken,
}

impl HeartbeatGuard {
    fn spawn(
        store: Arc<dyn Store>,
        job_id: job_core::JobId,
        lease_id: uuid::Uuid,
        lease: Duration,
        interval: Duration,
        cancel: CancellationToken,
    ) -> Self {
        let handle = tokio::spawn(heartbeat_loop(
            store,
            job_id,
            lease_id,
            lease,
            interval,
            cancel.clone(),
        ));
        Self {
            handle: Some(handle),
            cancel,
        }
    }

    /// Cancel the heartbeat and wait for it to finish.
    async fn stop(mut self) {
        self.cancel.cancel();
        // Take the handle out so `await`ing it does not move out of a `Drop`
        // type; the guard is about to be dropped anyway.
        if let Some(handle) = self.handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        // Runs even on panic/unwind. Abort as a last resort in case `stop` was
        // never reached; this guarantees no heartbeat outlives its job.
        self.cancel.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

/// Renew the lease on an interval until the job finishes.
///
/// If a renewal is rejected the worker has lost ownership: we cancel the job so
/// the handler stops, bounding any duplicate-execution window.
async fn heartbeat_loop(
    store: Arc<dyn Store>,
    job_id: job_core::JobId,
    lease_id: uuid::Uuid,
    lease: Duration,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match store.heartbeat(job_id, lease_id, lease).await {
                    Ok(true) => {}
                    Ok(false) => {
                        warn!(job_id, "lease lost during execution; cancelling handler");
                        cancel.cancel();
                        return;
                    }
                    Err(e) => error!(job_id, error = %e, "heartbeat error"),
                }
            }
            () = cancel.cancelled() => return,
        }
    }
}
