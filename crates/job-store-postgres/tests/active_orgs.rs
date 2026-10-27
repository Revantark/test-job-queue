//! Regression: a concurrent enqueue and prune must never strand a pending job.
//!
//! The `active_orgs` set is denormalized (the claim query scans it instead of
//! the whole backlog). It is maintained by a trigger on `jobs` and pruned by the
//! reaper. Without serialization there is a time-of-check/time-of-use race: the
//! pruner's snapshot cannot see another transaction's *uncommitted* pending job,
//! so it can delete a membership the producer's trigger just found already
//! present (`ON CONFLICT DO NOTHING`) — leaving a pending job with no
//! `active_orgs` row and therefore permanently unclaimable.
//!
//! These tests pin the guarantee that a pending job is always claimable,
//! regardless of interleaving.

mod common;

use std::time::Duration;

use common::TestDb;
use job_store::{ClaimOptions, Store};
use job_store_postgres::PgStore;

const ORGS: i64 = 12;
const ROUNDS: usize = 40;

fn claim_opts() -> ClaimOptions {
    ClaimOptions {
        batch: 1,
        lease: Duration::from_secs(30),
        worker: "race".into(),
        per_org_limit: None,
        per_org_inflight: None,
    }
}

/// The exact interleaving from the bug report: a stale membership exists, a
/// producer enqueues (trigger's upsert is a no-op because the row exists), and a
/// pruner runs between the producer's insert and its commit.
#[tokio::test]
async fn concurrent_enqueue_and_prune_does_not_strand_job() {
    let db = TestDb::create().await;
    let store = PgStore::connect(&db.url).await.expect("connect");
    store.migrate().await.expect("migrate");

    // Stale membership: org 5 is "active" but has no pending job.
    sqlx::query("INSERT INTO active_orgs (org_id, queue) VALUES (5, 'default')")
        .execute(store.pool())
        .await
        .unwrap();

    // Producer A: an open transaction that inserts a pending job for org 5. The
    // trigger fires but its upsert is a no-op (the row already exists).
    let mut conn_a = store.pool().acquire().await.unwrap();
    sqlx::query("BEGIN").execute(&mut *conn_a).await.unwrap();
    sqlx::query(
        "INSERT INTO jobs (org_id, kind, queue, scheduled_at) \
         VALUES (5, 'sleep', 'default', now())",
    )
    .execute(&mut *conn_a)
    .await
    .unwrap();

    // Pruner B: the reaper's prune, concurrently.
    let store_b = store.clone();
    let prune = tokio::spawn(async move {
        store_b.reap(Duration::ZERO).await.expect("reap");
    });

    // Let B reach the (exclusive) lock, which must block on A's shared lock.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // A commits; B may now proceed.
    sqlx::query("COMMIT").execute(&mut *conn_a).await.unwrap();
    prune.await.unwrap();

    let active: (i64,) = sqlx::query_as("SELECT count(*) FROM active_orgs WHERE org_id = 5")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(
        active.0, 1,
        "org membership must survive while a pending job exists"
    );

    let claimed = store.claim("default", &claim_opts()).await.unwrap();
    assert_eq!(
        claimed.len(),
        1,
        "the pending job must remain claimable after the race"
    );
}

/// A stronger, randomized version: hammer enqueue and prune from many tasks and
/// assert that every pending job is still claimable at the end. Any stranded job
/// shows up as a pending row with no matching `active_orgs` entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_enqueues_and_prunes_leave_no_orphans() {
    let db = TestDb::create().await;
    let store = PgStore::connect(&db.url).await.expect("connect");
    store.migrate().await.expect("migrate");

    let mut tasks = Vec::new();
    for round in 0..ROUNDS {
        // Producers.
        for org in 1..=ORGS {
            let s = store.clone();
            tasks.push(tokio::spawn(async move {
                let job = job_core::NewJob::new(org, "sleep", chrono::Utc::now());
                s.enqueue(job).await.expect("enqueue");
            }));
        }
        // A pruner on every few rounds, racing the producers.
        if round % 3 == 0 {
            let s = store.clone();
            tasks.push(tokio::spawn(async move {
                s.reap(Duration::ZERO).await.expect("reap");
            }));
        }
    }
    for t in tasks {
        t.await.unwrap();
    }

    // No pending job may be unrepresented in active_orgs.
    let orphans: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM jobs j WHERE j.state = 'pending' \
         AND NOT EXISTS (SELECT 1 FROM active_orgs a \
                         WHERE a.org_id = j.org_id AND a.queue = j.queue)",
    )
    .fetch_one(store.pool())
    .await
    .unwrap();
    assert_eq!(
        orphans.0, 0,
        "every pending job must have an active_orgs entry"
    );

    // And every job must be claimable (drain them all).
    let mut total = 0;
    loop {
        let batch = store.claim("default", &claim_opts()).await.unwrap();
        if batch.is_empty() {
            break;
        }
        total += batch.len();
    }
    let pending: (i64,) = sqlx::query_as("SELECT count(*) FROM jobs WHERE state = 'pending'")
        .fetch_one(store.pool())
        .await
        .unwrap();
    assert_eq!(pending.0, 0, "all jobs should have been claimable");
    assert!(total > 0);
}
