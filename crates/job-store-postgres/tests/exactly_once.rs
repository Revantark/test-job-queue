//! Exactly-once: no job is lost and none is executed twice, even with several
//! concurrent workers racing over the same rows.
#![allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]

mod common;

use std::sync::Arc;

use common::{PgEffectSink, TestDb, drain, seed};
use job_handlers::DbEffectHandler;
use job_store::Store;
use job_worker::Registry;

const JOBS: i64 = 200;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_job_runs_exactly_once() {
    let db = TestDb::create().await;
    let store = db.store().await;

    seed(&store, 1, "db_effect", JOBS as usize).await;

    let registry = Registry::new().add(DbEffectHandler::new(Arc::new(PgEffectSink::new(
        store.clone(),
    ))));
    let shared: Arc<dyn Store> = Arc::new(store.clone());
    drain(
        shared,
        registry,
        /* pool_size */ 4,
        /* workers   */ 4,
        Some(2),
        "default",
        JOBS,
    )
    .await;

    let counts = store.status(None).await.unwrap();
    assert_eq!(counts.completed, JOBS, "all jobs must complete");
    assert_eq!(counts.failed, 0);

    // The decisive assertion: exactly one committed effect per job. Since
    // `job_id` is the primary key of `effects`, a duplicate execution cannot
    // create a second row, and a *missing* row shows a job that never recorded
    // its effect.
    let total: (i64,) = sqlx::query_as("SELECT count(*) FROM effects")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(total.0, JOBS, "every job must record exactly one effect");

    let mismatched: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM jobs j \
         WHERE j.kind = 'db_effect' AND j.state = 'completed' \
           AND NOT EXISTS (SELECT 1 FROM effects e WHERE e.job_id = j.id)",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        mismatched.0, 0,
        "every completed db_effect job has its effect"
    );
}
