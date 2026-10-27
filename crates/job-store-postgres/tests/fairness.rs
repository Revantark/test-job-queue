//! Fairness: a tenant with many jobs must not starve a tenant with few.
#![allow(clippy::cast_possible_wrap)]

mod common;

use std::sync::Arc;

use common::{PgEffectSink, TestDb, seed};
use job_handlers::DbEffectHandler;
use job_store::Store;
use job_worker::Registry;

/// With a per-org cap of 1 and two workers, a single claim round can take at
/// most one job from each organization, so the small tenant is served even
/// while a large one has thousands pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_tenant_is_not_starved_by_large_tenant() {
    let db = TestDb::create().await;
    let store = db.store().await;

    let big_org = 100;
    let small_org = 200;
    seed(&store, big_org, "sleep", 1000).await;
    seed(&store, small_org, "sleep", 10).await;

    // The window-function cap: per claim round, at most one job per org.
    let opts = job_store::ClaimOptions {
        batch: 2,
        lease: std::time::Duration::from_secs(30),
        worker: "fairness".into(),
        per_org_limit: Some(1),
        per_org_inflight: None,
    };
    let claimed = store.claim("default", &opts).await.unwrap();
    assert_eq!(claimed.len(), 2);
    let mut orgs: Vec<i64> = claimed.iter().map(|c| c.job.org_id).collect();
    orgs.sort_unstable();
    assert_eq!(
        orgs,
        vec![big_org, small_org],
        "a single round must draw from both organizations"
    );
}

/// Regression: a partial claim (batch smaller than the number of active
/// tenants) must rotate across tenants, not always hand the slot to the lowest
/// org id. A deterministic tiebreak starved every tenant but the first.
#[tokio::test]
async fn partial_batch_rotates_across_orgs() {
    let db = TestDb::create().await;
    let store = db.store().await;

    // Five tenants, each with plenty of work.
    for org in 1..=5 {
        seed(&store, org, "sleep", 50).await;
    }

    // Claim one job at a time, many times; every org must appear.
    let mut seen = std::collections::HashSet::new();
    for _ in 0..40 {
        let opts = job_store::ClaimOptions {
            batch: 1,
            lease: std::time::Duration::from_secs(30),
            worker: "rotate".into(),
            per_org_limit: Some(1),
            per_org_inflight: None,
        };
        for c in store.claim("default", &opts).await.unwrap() {
            seen.insert(c.job.org_id);
        }
        tokio::task::yield_now().await;
    }

    assert_eq!(
        seen.len(),
        5,
        "a batch of 1 must rotate across all tenants, saw {seen:?}"
    );
}

/// End to end: the small tenant's jobs finish while the large tenant still has
/// a big backlog, because a per-org cap of 1 serves them round-robin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_tenant_completes_first() {
    let db = TestDb::create().await;
    let store = db.store().await;
    seed(&store, 1, "db_effect", 5000).await;
    seed(&store, 2, "db_effect", 10).await;

    let registry = Registry::new().add(DbEffectHandler::new(Arc::new(PgEffectSink::new(
        store.clone(),
    ))));
    let shared: Arc<dyn Store> = Arc::new(store.clone());

    let shutdown = tokio_util::sync::CancellationToken::new();
    let mut handles = Vec::new();
    for i in 0..3 {
        let cfg = job_worker::WorkerConfig {
            pool_size: 2,
            claim_batch: 2,
            lease: std::time::Duration::from_secs(5),
            per_org_limit: Some(1),
            per_org_inflight: None,
            idle_sleep: std::time::Duration::from_millis(20),
            ..job_worker::WorkerConfig::new("default", format!("fair-{i}"))
        };
        let worker = job_worker::Worker::new(Arc::clone(&shared), registry.clone(), cfg);
        handles.push(tokio::spawn(worker.run(shutdown.clone())));
    }

    // Wait until the small tenant is fully served.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    let big_done_when_small_finished = loop {
        let small_done: (i64,) =
            sqlx::query_as("SELECT count(*) FROM jobs WHERE org_id = 2 AND state = 'completed'")
                .fetch_one(store.pool())
                .await
                .unwrap();
        if small_done.0 >= 10 {
            let big_done: (i64,) = sqlx::query_as(
                "SELECT count(*) FROM jobs WHERE org_id = 1 AND state = 'completed'",
            )
            .fetch_one(store.pool())
            .await
            .unwrap();
            break big_done.0;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "small tenant starved: only {} completed",
            small_done.0
        );
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };

    // Fairness means interleaving, not "small tenant beats the big backlog to
    // the finish line" (which would only hold if the big one were stalled). With
    // a per-org cap of 1, each claim round serves roughly one job per org, so
    // when the small tenant's 10 jobs are done the large tenant should have
    // completed a comparable handful, nowhere near its 5000.
    assert!(
        big_done_when_small_finished < 200,
        "small tenant was served out of proportion: large tenant had {big_done_when_small_finished} done",
    );

    shutdown.cancel();
    for h in handles {
        let _ = h.await;
    }
}
