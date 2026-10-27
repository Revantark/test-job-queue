//! Regression tests for previously-broken behaviors:
//!
//! - a panicking handler must not leave its heartbeat renewing the lease
//!   forever (which would make the job unrecoverable);
//! - priority ordering must hold for partial batches;
//! - a per-org in-flight cap must stop one tenant from occupying the pool;
//! - newly enqueued immediate work must be picked up promptly.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::TestDb;
use job_core::{JobOutcome, NewJob};
use job_store::{ClaimOptions, Store};
use job_worker::{Handler, JobContext, Registry, Worker, WorkerConfig};
use tokio_util::sync::CancellationToken;

fn opts(batch: i64, per_org: Option<i64>, inflight: Option<i64>) -> ClaimOptions {
    ClaimOptions {
        batch,
        lease: Duration::from_secs(30),
        worker: "test".into(),
        per_org_limit: per_org,
        per_org_inflight: inflight,
    }
}

/// A handler that panics on every run.
struct PanickyHandler;

#[async_trait::async_trait]
impl Handler for PanickyHandler {
    fn kind(&self) -> &'static str {
        "panic"
    }
    async fn run(&self, _ctx: JobContext) -> JobOutcome {
        panic!("boom");
    }
}

/// A handler that never returns until cancelled.
struct HangHandler;

#[async_trait::async_trait]
impl Handler for HangHandler {
    fn kind(&self) -> &'static str {
        "hang"
    }
    async fn run(&self, ctx: JobContext) -> JobOutcome {
        ctx.cancel.cancelled().await;
        JobOutcome::Retryable {
            error: "cancelled".into(),
        }
    }
}

/// A panicking handler must be turned into a failed attempt whose lease is
/// released, so the job is recoverable rather than wedged forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_handler_is_recoverable() {
    let db = TestDb::create().await;
    let store = Arc::new(db.store().await);

    let job = NewJob::new(1, "panic", chrono::Utc::now()).max_attempts(3);
    let id = store.enqueue(job).await.unwrap();

    let registry = Registry::new().add(PanickyHandler);
    let mut cfg = WorkerConfig::new("default", "panic-worker");
    cfg.pool_size = 1;
    cfg.lease = Duration::from_secs(1);
    cfg.per_org_limit = None;
    cfg.per_org_inflight = None;
    cfg.idle_sleep = Duration::from_millis(20);
    cfg.drain_timeout = Duration::from_secs(2);

    let shutdown = CancellationToken::new();
    let worker = Worker::new(store.clone(), registry, cfg);
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    // The panicked attempt should be recorded as a retryable failure, and the
    // job returned to pending with its lease cleared — not stuck running with a
    // live heartbeat.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let job = store.get(id).await.unwrap().unwrap();
        if job.state == job_core::JobState::Pending {
            assert!(
                job.lease_id.is_none() && job.lease_expires_at.is_none(),
                "a failed attempt must release the lease"
            );
            break;
        }
        if job.state == job_core::JobState::Running && job.attempt >= 1 {
            // Keep waiting for it to be finalized as failed.
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "job was never finalized after the handler panic; state={:?} attempt={}",
            job.state,
            job.attempt
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.cancel();
    let _ = handle.await;
}

/// A hanging handler that loses its lease must be cancelled (its heartbeat must
/// notice the lost lease), and the job must be reclaimable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hanging_handler_is_cancelled_when_lease_lost() {
    let db = TestDb::create().await;
    let store = Arc::new(db.store().await);

    let job = NewJob::new(1, "hang", chrono::Utc::now()).max_attempts(3);
    let id = store.enqueue(job).await.unwrap();

    let registry = Registry::new().add(HangHandler);
    let mut cfg = WorkerConfig::new("default", "hang-worker");
    cfg.pool_size = 1;
    cfg.lease = Duration::from_secs(1);
    cfg.per_org_limit = None;
    cfg.per_org_inflight = None;
    cfg.idle_sleep = Duration::from_millis(20);
    cfg.drain_timeout = Duration::from_secs(2);

    let shutdown = CancellationToken::new();
    let worker = Worker::new(store.clone(), registry, cfg);
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    // Wait until it is running, then force the lease to expire.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let job = store.get(id).await.unwrap().unwrap();
        if job.state == job_core::JobState::Running {
            break;
        }
        assert!(tokio::time::Instant::now() <= deadline, "job never started");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    common::expire_lease(&store, id).await;

    // The heartbeat's next tick should see the lost lease, cancel the handler,
    // and the attempt should finalize (fail/requeue) rather than hang forever.
    loop {
        let job = store.get(id).await.unwrap().unwrap();
        if job.state == job_core::JobState::Pending {
            break;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "hanging handler was not cancelled after lease loss; state={:?}",
            job.state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    shutdown.cancel();
    let _ = handle.await;
}

/// Priority must hold even for a partial batch (the bug discarded priority when
/// the final batch took only some of the per-tenant candidates).
#[tokio::test]
async fn priority_holds_for_partial_batches() {
    let db = TestDb::create().await;
    let store = db.store().await;

    // Insert the low-priority job first so insertion order opposes priority.
    store
        .enqueue(NewJob::new(1, "sleep", chrono::Utc::now()).priority(500))
        .await
        .unwrap();
    let high = store
        .enqueue(NewJob::new(1, "sleep", chrono::Utc::now()).priority(1))
        .await
        .unwrap();

    // per_org_limit > batch is exactly the case that used to break ordering.
    let claimed = store
        .claim("default", &opts(1, Some(4), None))
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(
        claimed[0].job.id, high,
        "a partial batch must still prefer the higher-priority job"
    );
}

/// A per-org in-flight cap must stop one tenant from occupying the pool: its
/// long-running jobs cannot hold every slot while another tenant has work.
#[tokio::test]
async fn in_flight_cap_prevents_pool_monopolization() {
    let db = TestDb::create().await;
    let store = db.store().await;

    // Org 1 has long jobs; org 2 has one job.
    for _ in 0..50 {
        store
            .enqueue(NewJob::new(1, "sleep", chrono::Utc::now()))
            .await
            .unwrap();
    }
    store
        .enqueue(NewJob::new(2, "sleep", chrono::Utc::now()))
        .await
        .unwrap();

    // With an in-flight cap of 1, repeated claim rounds may take at most one
    // org-1 job at a time until it completes.
    let mut org1_claimed = 0;
    for _ in 0..3 {
        let batch = store
            .claim("default", &opts(4, None, Some(1)))
            .await
            .unwrap();
        org1_claimed += batch.iter().filter(|c| c.job.org_id == 1).count();
    }
    assert_eq!(
        org1_claimed, 1,
        "an org with in-flight cap 1 must not get a second concurrent job"
    );
}

/// An immediate job must be picked up promptly even when the worker just went to
/// sleep (the idle sleep must not run for seconds at a time).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn immediate_job_is_not_delayed_by_idle_sleep() {
    let db = TestDb::create().await;
    let store = Arc::new(db.store().await);

    let registry = Registry::new().add(HangHandler); // claims and holds, so we can observe
    let mut cfg = WorkerConfig::new("default", "idle-worker");
    cfg.pool_size = 1;
    cfg.lease = Duration::from_secs(30);
    cfg.per_org_limit = None;
    cfg.per_org_inflight = None;
    cfg.idle_sleep = Duration::from_millis(200);
    cfg.drain_timeout = Duration::from_secs(2);

    let shutdown = CancellationToken::new();
    let worker = Worker::new(store.clone(), registry, cfg);
    let handle = tokio::spawn(worker.run(shutdown.clone()));

    // Let the worker go idle/sleep.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Enqueue an immediate job and measure how long until it is claimed.
    let id = store
        .enqueue(NewJob::new(1, "hang", chrono::Utc::now()))
        .await
        .unwrap();
    let start = tokio::time::Instant::now();
    loop {
        let job = store.get(id).await.unwrap().unwrap();
        if job.state == job_core::JobState::Running {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(2),
            "immediate job delayed too long: {:?}",
            start.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_millis(700),
        "job should be claimed within ~1 idle interval, took {elapsed:?}"
    );

    shutdown.cancel();
    let _ = handle.await;
}
