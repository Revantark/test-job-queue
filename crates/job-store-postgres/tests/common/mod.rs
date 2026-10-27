//! Shared test harness: a throwaway database per test run and helpers to drive
//! workers to completion.
//!
//! Each test binary pulls in the whole module, so helpers unused by a given
//! test are expected.
#![allow(dead_code, clippy::cast_possible_wrap)]

use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use job_core::{JobId, NewJob};
use job_handlers::EffectSink;
use job_store::{ClaimOptions, Store};
use job_store_postgres::PgStore;
use job_worker::{Registry, Worker, WorkerConfig};
use serde_json::Value;
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A database created for a single test and dropped when the value is dropped.
pub struct TestDb {
    name: String,
    admin_url: String,
    /// Connection string for the throwaway database.
    pub url: String,
}

impl TestDb {
    /// Create a uniquely named database, apply migrations, and return it.
    pub async fn create() -> Self {
        let base =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set for integration tests");
        let admin_url = replace_dbname(&base, "postgres");
        let name = format!(
            "hunar_test_{}_{}",
            process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );
        let url = replace_dbname(&base, &name);

        let pool = sqlx::PgPool::connect(&admin_url)
            .await
            .expect("connect to admin database");
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
            .execute(&pool)
            .await
            .expect("create test database");
        pool.close().await;

        let store = PgStore::connect(&url).await.expect("connect to test db");
        store.migrate().await.expect("migrate test db");

        Self {
            name,
            admin_url,
            url,
        }
    }

    /// A connected store for this database.
    pub async fn store(&self) -> PgStore {
        let store = PgStore::connect(&self.url).await.expect("connect");
        store.migrate().await.expect("migrate");
        store
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // Dropping happens inside the test's async context, so run the cleanup
        // on a fresh thread with its own runtime to avoid nesting executors.
        let name = self.name.clone();
        let admin_url = self.admin_url.clone();
        let _ = std::thread::spawn(move || {
            let rt = Runtime::new().expect("cleanup runtime");
            rt.block_on(async {
                if let Ok(pool) = sqlx::PgPool::connect(&admin_url).await {
                    let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                        "DROP DATABASE \"{name}\" WITH (FORCE)"
                    )))
                    .execute(&pool)
                    .await;
                    pool.close().await;
                }
            });
        })
        .join();
    }
}

/// A [`EffectSink`] backed by the Postgres `effects` table.
pub struct PgEffectSink {
    store: PgStore,
}

impl PgEffectSink {
    pub fn new(store: PgStore) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl EffectSink for PgEffectSink {
    async fn record(
        &self,
        job_id: JobId,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        self.store
            .record_effect(job_id, Value::Null)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Enqueue `count` identical jobs for `org`, due immediately.
pub async fn seed(store: &PgStore, org: i64, kind: &str, count: usize) {
    for _ in 0..count {
        let job = NewJob::new(org, kind, chrono::Utc::now());
        store.enqueue(job).await.expect("enqueue");
    }
}

/// Run `workers` worker processes with `registry` until `target` jobs reach a
/// terminal state (or the deadline passes), then shut them down.
pub async fn drain(
    store: Arc<dyn Store>,
    registry: Registry,
    pool_size: usize,
    workers: usize,
    per_org_limit: Option<i64>,
    queue: &str,
    target: i64,
) {
    let shutdown = CancellationToken::new();
    let mut handles = Vec::new();
    for i in 0..workers {
        let mut cfg = WorkerConfig::new(queue, format!("test-{i}"));
        cfg.pool_size = pool_size;
        cfg.claim_batch = pool_size as i64;
        cfg.lease = Duration::from_secs(5);
        cfg.per_org_limit = per_org_limit;
        cfg.idle_sleep = Duration::from_millis(20);
        let worker = Worker::new(Arc::clone(&store), registry.clone(), cfg);
        handles.push(tokio::spawn(worker.run(shutdown.clone())));
    }

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let counts = store.status(None).await.expect("status");
        if counts.completed + counts.failed >= target {
            break;
        }
        assert!(
            tokio::time::Instant::now() <= deadline,
            "drain timed out: completed={} failed={} target={target}",
            counts.completed,
            counts.failed
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    shutdown.cancel();
    for h in handles {
        let _ = h.await;
    }
}

/// A claim returning zero jobs uses this to mean "nothing due right now".
pub async fn claim_one(
    store: &PgStore,
    worker: &str,
    lease: Duration,
) -> Vec<job_core::ClaimedJob> {
    let opts = ClaimOptions {
        batch: 1,
        lease,
        worker: worker.to_owned(),
        per_org_limit: None,
        per_org_inflight: None,
    };
    store.claim("default", &opts).await.expect("claim")
}

/// Replace the database name (last path segment) in a Postgres URL.
pub fn replace_dbname(url: &str, dbname: &str) -> String {
    match url.rfind('/') {
        Some(idx) => {
            let (prefix, rest) = url.split_at(idx + 1);
            let query = rest.find('?').map_or("", |q| &rest[q..]);
            format!("{prefix}{dbname}{query}")
        }
        None => url.to_owned(),
    }
}

/// Whether a job's effect has been committed (0 or 1).
pub async fn effect_runs(store: &PgStore, job_id: JobId) -> i64 {
    let row: Option<(i64,)> = sqlx::query_as("SELECT count(*) FROM effects WHERE job_id = $1")
        .bind(job_id)
        .fetch_optional(store.pool())
        .await
        .expect("query effect");
    row.map_or(0, |(r,)| r)
}

/// Force a lease to expire so recovery paths can be exercised without waiting.
pub async fn expire_lease(store: &PgStore, job_id: JobId) {
    sqlx::query("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1")
        .bind(job_id)
        .execute(store.pool())
        .await
        .expect("expire lease");
}
