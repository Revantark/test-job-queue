use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use clap::{Parser, Subcommand};
use job_core::{NewJob, OrgId};
use job_handlers::{DbEffectHandler, EffectSink, FailHandler, SleepHandler};
use job_store::Store;
use job_store_postgres::PgStore;
use job_worker::{Reaper, ReaperConfig, Registry, Worker, WorkerConfig};
use serde_json::Value;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::Config;

/// Database-backed job scheduler and executor.
#[derive(Debug, Parser)]
#[command(name = "scheduler", version, about)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Apply database migrations.
    Migrate,

    /// Schedule a job.
    Enqueue {
        /// Owning organization id.
        #[arg(long)]
        org: OrgId,
        /// Handler kind (e.g. `sleep`, `fail`, `db_effect`).
        #[arg(long)]
        kind: String,
        /// JSON payload.
        #[arg(long, default_value = "{}")]
        payload: String,
        /// Queue name.
        #[arg(long, default_value = "default")]
        queue: String,
        /// Delay before the job runs, e.g. `5s`, `10m`, `2h`.
        #[arg(long, default_value = "0s")]
        delay: String,
        /// Lower runs first.
        #[arg(long, default_value_t = 100)]
        priority: i32,
        /// Maximum attempts before dead-lettering.
        #[arg(long, default_value_t = 3)]
        max_attempts: u32,
    },

    /// Run workers until interrupted (Ctrl-C).
    Run {
        /// Override the configured pool size.
        #[arg(long)]
        workers: Option<usize>,
    },

    /// Show job counts by state.
    Status {
        /// Restrict to one organization.
        #[arg(long)]
        org: Option<OrgId>,
    },
}

impl Cli {
    /// Parse arguments and execute the selected command.
    pub async fn run() -> Result<()> {
        let cli = Cli::parse();
        let mut config = Config::from_env()?;
        let store = Arc::new(
            PgStore::connect(&config.database_url)
                .await
                .context("connecting to Postgres")?,
        );

        match cli.command {
            Command::Migrate => {
                store.migrate().await.context("running migrations")?;
                info!("migrations applied");
            }
            Command::Enqueue {
                org,
                kind,
                payload,
                queue,
                delay,
                priority,
                max_attempts,
            } => {
                let payload: Value =
                    serde_json::from_str(&payload).context("payload must be valid JSON")?;
                let job = NewJob::new(org, kind, Utc::now() + parse_delay(&delay)?)
                    .queue(queue)
                    .payload(payload)
                    .priority(priority)
                    .max_attempts(max_attempts);
                let id = store.enqueue(job).await.context("enqueueing job")?;
                println!("enqueued job {id}");
            }
            Command::Run { workers } => {
                if let Some(n) = workers {
                    config.pool_size = n;
                }
                run_workers(&config, store).await?;
            }
            Command::Status { org } => {
                let counts = store.status(org).await.context("reading status")?;
                println!("pending   {}", counts.pending);
                println!("running   {}", counts.running);
                println!("completed {}", counts.completed);
                println!("failed    {}", counts.failed);
            }
        }
        Ok(())
    }
}

/// Wire handlers, the worker and the reaper, then run until Ctrl-C.
async fn run_workers(config: &Config, store: Arc<PgStore>) -> Result<()> {
    let registry = build_registry(store.clone());
    let shutdown = CancellationToken::new();
    spawn_signal_handler(shutdown.clone());

    let mut worker_cfg = WorkerConfig::new(
        config.queue.clone(),
        format!("{}-{}", hostname(), std::process::id()),
    );
    worker_cfg.pool_size = config.pool_size;
    worker_cfg.claim_batch = i64::try_from(config.pool_size).unwrap_or(i64::MAX);
    worker_cfg.lease = config.lease();
    worker_cfg.per_org_limit = config.per_org_limit;
    worker_cfg.per_org_inflight = config.per_org_inflight;
    worker_cfg.drain_timeout = std::time::Duration::from_secs(config.drain_timeout_secs);

    let worker_store: Arc<dyn Store> = store.clone();
    let worker = Worker::new(Arc::clone(&worker_store), registry, worker_cfg);
    let reaper = Reaper::new(
        Arc::clone(&worker_store),
        ReaperConfig {
            interval: config.lease() / 3,
            requeue_backoff: std::time::Duration::from_secs(config.lease_secs / 6),
        },
    );

    let worker_task = tokio::spawn(worker.run(shutdown.clone()));
    let reaper_task = tokio::spawn(reaper.run(shutdown.clone()));

    let _ = tokio::join!(reaper_task, worker_task);
    Ok(())
}

/// The handler registry. Swap these sample handlers for your own.
fn build_registry(store: Arc<PgStore>) -> Registry {
    let sink: Arc<dyn EffectSink> = Arc::new(PgEffectSink { store });
    Registry::new()
        .add(SleepHandler::default())
        .add(FailHandler::default())
        .add(DbEffectHandler::new(sink))
}

/// Records job effects in Postgres, making repeated runs harmless.
struct PgEffectSink {
    store: Arc<PgStore>,
}

#[async_trait::async_trait]
impl EffectSink for PgEffectSink {
    async fn record(
        &self,
        job_id: job_core::JobId,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        self.store
            .record_effect(job_id, Value::Null)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Parse `5s`, `10m`, `2h`, `500ms`, or `0` into a duration.
fn parse_delay(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() || s == "0" {
        return Ok(Duration::zero());
    }
    let (num, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: i64 = num
        .parse()
        .with_context(|| format!("invalid delay '{s}'"))?;
    let d = match unit {
        "" | "s" => Duration::seconds(n),
        "ms" => Duration::milliseconds(n),
        "m" => Duration::minutes(n),
        "h" => Duration::hours(n),
        "d" => Duration::days(n),
        other => anyhow::bail!("unknown delay unit '{other}'"),
    };
    Ok(d)
}

fn spawn_signal_handler(shutdown: CancellationToken) {
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown signal received");
        shutdown.cancel();
    });
}

fn hostname() -> String {
    std::env::var("HOSTNAME").unwrap_or_else(|_| "host".to_owned())
}

#[cfg(test)]
mod tests {
    use super::parse_delay;
    use chrono::Duration;

    #[test]
    fn parses_units() {
        assert_eq!(parse_delay("0").unwrap(), Duration::zero());
        assert_eq!(parse_delay("5s").unwrap(), Duration::seconds(5));
        assert_eq!(parse_delay("10m").unwrap(), Duration::minutes(10));
        assert_eq!(parse_delay("2h").unwrap(), Duration::hours(2));
        assert_eq!(parse_delay("500ms").unwrap(), Duration::milliseconds(500));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse_delay("5x").is_err());
        assert!(parse_delay("abc").is_err());
    }
}
