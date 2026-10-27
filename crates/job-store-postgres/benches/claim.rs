//! Micro-benchmark for the claim query across batch sizes.
//!
//! Demonstrates the batching tradeoff: larger `LIMIT` amortizes the round trip
//! but reserves more rows per claim.
//!
//! The benchmark creates its **own uniquely named database** (derived from the
//! admin URL), migrates it, and drops it afterwards, so it never touches the
//! database named in `DATABASE_URL` or any shared schema.
//!
//! Run with: `DATABASE_URL=postgres://user@localhost/postgres cargo bench -p job-store-postgres`

use std::process;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use job_core::NewJob;
use job_store::{ClaimOptions, Store};
use job_store_postgres::PgStore;
use tokio::runtime::Runtime;

fn bench_claim(c: &mut Criterion) {
    let Ok(admin_url) = std::env::var("DATABASE_URL") else {
        eprintln!("DATABASE_URL not set; skipping claim benchmark");
        return;
    };
    let name = format!("scheduler_bench_{}", process::id());
    let url = replace_dbname(&admin_url, &name);

    let rt = Runtime::new().expect("tokio runtime");
    let store = rt.block_on(async {
        // Create a disposable database (idempotently), migrate it.
        let admin = sqlx::PgPool::connect(&admin_url)
            .await
            .expect("connect to admin db");
        let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"
        )))
        .execute(&admin)
        .await;
        sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE \"{name}\"")))
            .execute(&admin)
            .await
            .expect("create bench database");
        admin.close().await;

        let store = PgStore::connect(&url).await.expect("connect");
        store.migrate().await.expect("migrate");
        store
    });

    let mut group = c.benchmark_group("claim");
    for batch in [1_i64, 10, 50] {
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            b.to_async(&rt).iter(|| {
                let store: Arc<PgStore> = Arc::new(store.clone());
                async move {
                    // Seed a fresh due job so every iteration has work to claim.
                    let job = NewJob::new(1, "sleep", Utc::now());
                    store.enqueue(job).await.expect("enqueue");
                    let opts = ClaimOptions {
                        batch,
                        lease: Duration::from_secs(30),
                        worker: "bench".into(),
                        per_org_limit: None,
                        per_org_inflight: None,
                    };
                    let claimed = store.claim("default", &opts).await.expect("claim");
                    assert!(!claimed.is_empty());
                    // Return the row so the next iteration has a clean slate.
                    for c in claimed {
                        store
                            .fail(
                                c.job.id,
                                c.lease.id,
                                &job_core::JobOutcome::Fatal {
                                    error: "bench".into(),
                                },
                            )
                            .await
                            .expect("fail");
                    }
                }
            });
        });
    }
    group.finish();

    // Drop the disposable database.
    rt.block_on(async {
        if let Ok(admin) = sqlx::PgPool::connect(&admin_url).await {
            let _ = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"
            )))
            .execute(&admin)
            .await;
            admin.close().await;
        }
    });
}

criterion_group!(benches, bench_claim);
criterion_main!(benches);

/// Replace the database name (last path segment) in a Postgres URL.
fn replace_dbname(url: &str, dbname: &str) -> String {
    match url.rfind('/') {
        Some(idx) => {
            let (prefix, rest) = url.split_at(idx + 1);
            let query = rest.find('?').map_or("", |q| &rest[q..]);
            format!("{prefix}{dbname}{query}")
        }
        None => url.to_owned(),
    }
}
