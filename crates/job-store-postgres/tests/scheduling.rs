//! Scheduling: jobs run close to their `scheduled_at` and are never claimed
//! early.
#![allow(clippy::cast_possible_wrap)]

mod common;

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use common::{TestDb, claim_one};
use job_core::NewJob;
use job_store::Store;

#[tokio::test]
async fn future_jobs_are_not_claimed_early() {
    let db = TestDb::create().await;
    let store = db.store().await;

    let future = NewJob::new(1, "sleep", Utc::now() + ChronoDuration::seconds(60));
    let id = store.enqueue(future).await.unwrap();

    // Nothing is due yet.
    assert!(
        claim_one(&store, "w", Duration::from_secs(30))
            .await
            .is_empty()
    );
    let job = store.get(id).await.unwrap().unwrap();
    assert_eq!(job.state, job_core::JobState::Pending);
}

#[tokio::test]
async fn due_job_runs_within_tolerance() {
    let db = TestDb::create().await;
    let store = db.store().await;

    // A job due ~300ms from now must become claimable within a small tolerance.
    let target = Utc::now() + ChronoDuration::milliseconds(300);
    let id = store
        .enqueue(NewJob::new(1, "sleep", target))
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let claimed = claim_one(&store, "w", Duration::from_secs(30)).await;
        if claimed.iter().any(|c| c.job.id == id) {
            let drift = (Utc::now() - target).num_milliseconds();
            assert!(drift >= 0, "job must not run before its due time");
            assert!(drift < 1000, "job drifted too far: {drift}ms");
            return;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "job was never claimed after coming due"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn priority_ordering_is_respected() {
    let db = TestDb::create().await;
    let store = db.store().await;

    let low = store
        .enqueue(NewJob::new(1, "sleep", Utc::now()).priority(1))
        .await
        .unwrap();
    let high = store
        .enqueue(NewJob::new(1, "sleep", Utc::now()).priority(500))
        .await
        .unwrap();

    let claimed = claim_one(&store, "w", Duration::from_secs(30)).await;
    assert_eq!(claimed[0].job.id, low, "lower priority number runs first");
    assert_ne!(claimed[0].job.id, high);
}
