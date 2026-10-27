//! Crash recovery: a job whose worker dies (lease stops being renewed) is
//! reclaimed by the reaper and completed exactly once.

mod common;

use std::time::Duration;

use common::{TestDb, claim_one, expire_lease, seed};
use job_store::Store;

#[tokio::test]
async fn expired_lease_is_reclaimed_and_not_lost() {
    let db = TestDb::create().await;
    let store = db.store().await;
    seed(&store, 7, "db_effect", 1).await;

    // A worker claims the job but then "crashes": it never completes or
    // heartbeats again.
    let claimed = claim_one(&store, "doomed-worker", Duration::from_secs(30)).await;
    assert_eq!(claimed.len(), 1);
    let job_id = claimed[0].job.id;

    let running = store.get(job_id).await.unwrap().unwrap();
    assert_eq!(running.state, job_core::JobState::Running);

    // Simulate the crash by expiring the lease, then reap. A zero requeue
    // backoff makes the job immediately eligible again for the test.
    expire_lease(&store, job_id).await;
    let reclaimed = store.reap(Duration::ZERO).await.unwrap();
    assert_eq!(reclaimed, 1, "the orphaned job must be reclaimed");

    let after = store.get(job_id).await.unwrap().unwrap();
    assert_eq!(
        after.state,
        job_core::JobState::Pending,
        "job returns to the queue"
    );
    assert!(
        after.next_retry_at.is_some(),
        "a requeued job gets a backoff"
    );
    assert_eq!(after.attempt, 1, "the crashed attempt still counts");

    // A new worker claims and completes it.
    let reclaimed_claim = store
        .claim(
            "default",
            &job_store::ClaimOptions {
                batch: 1,
                lease: Duration::from_secs(30),
                worker: "healthy-worker".into(),
                per_org_limit: None,
                per_org_inflight: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(reclaimed_claim.len(), 1);
    let c = &reclaimed_claim[0];
    let done = store
        .complete(c.job.id, c.lease.id, &job_core::JobOutcome::success())
        .await
        .unwrap();
    assert!(done, "the new owner completes the job");
    let finished = store.get(job_id).await.unwrap().unwrap();
    assert_eq!(finished.state, job_core::JobState::Completed);
    assert!(finished.finished_at.is_some());
}

#[tokio::test]
async fn stale_worker_cannot_complete_after_reclaim() {
    let db = TestDb::create().await;
    let store = db.store().await;
    seed(&store, 8, "db_effect", 1).await;

    let claimed = claim_one(&store, "slow-worker", Duration::from_secs(30)).await;
    let job_id = claimed[0].job.id;
    let stale_lease = claimed[0].lease.id;

    // The slow worker's lease lapses and the job is reaped and re-claimed.
    expire_lease(&store, job_id).await;
    store.reap(Duration::ZERO).await.unwrap();
    let fresh = claim_one(&store, "fresh-worker", Duration::from_secs(30)).await;
    assert_eq!(fresh.len(), 1);
    assert_ne!(
        fresh[0].lease.id, stale_lease,
        "a new claim mints a new lease"
    );

    // The zombie tries to complete with its old lease: it must be fenced out.
    let fenced = store
        .complete(job_id, stale_lease, &job_core::JobOutcome::success())
        .await
        .unwrap();
    assert!(
        !fenced,
        "a stale lease must not be able to complete the job"
    );
    let still_running = store.get(job_id).await.unwrap().unwrap();
    assert_eq!(
        still_running.state,
        job_core::JobState::Running,
        "the fenced write must not change state"
    );

    // The legitimate owner still completes exactly once.
    let ok = store
        .complete(job_id, fresh[0].lease.id, &job_core::JobOutcome::success())
        .await
        .unwrap();
    assert!(ok);
    let done = store.get(job_id).await.unwrap().unwrap();
    assert_eq!(done.state, job_core::JobState::Completed);
}
