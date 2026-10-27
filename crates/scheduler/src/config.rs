use std::time::Duration;

use anyhow::{Context, Result};

/// Process configuration, read from the environment with sane defaults.
#[derive(Debug, Clone)]
pub struct Config {
    /// Postgres connection string.
    pub database_url: String,
    /// Queue consumed by `run`.
    pub queue: String,
    /// Concurrent jobs per worker process.
    pub pool_size: usize,
    /// Lease duration in seconds.
    pub lease_secs: u64,
    /// Maximum jobs contributed per organization within one claim round.
    pub per_org_limit: Option<i64>,
    /// Maximum jobs an organization may have running at once (across workers).
    pub per_org_inflight: Option<i64>,
    /// Maximum seconds to wait for in-flight jobs to finish on shutdown.
    pub drain_timeout_secs: u64,
}

impl Config {
    /// Read configuration from the environment.
    pub fn from_env() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL")
            .context("DATABASE_URL must be set (e.g. postgres://user@localhost/hunar_jobs)")?;
        Ok(Self {
            database_url,
            queue: env_or("SCHEDULER_QUEUE", "default"),
            pool_size: env_parse("SCHEDULER_POOL_SIZE", 4)?,
            lease_secs: env_parse("SCHEDULER_LEASE_SECS", 30)?,
            per_org_limit: env_opt_i64("SCHEDULER_PER_ORG_LIMIT", 2)?,
            per_org_inflight: env_opt_i64("SCHEDULER_PER_ORG_INFLIGHT", 2)?,
            drain_timeout_secs: env_parse("SCHEDULER_DRAIN_TIMEOUT_SECS", 10)?,
        })
    }

    /// The lease as a duration.
    pub fn lease(&self) -> Duration {
        Duration::from_secs(self.lease_secs)
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_owned())
}

/// Read an optional integer, defaulting to `default` when unset. Set the var to
/// `0` or leave it unset-and-`default`-`None` to disable the cap.
fn env_opt_i64(key: &str, default: i64) -> Result<Option<i64>> {
    match std::env::var(key) {
        Ok(v) => {
            let n: i64 = v
                .parse()
                .map_err(|e| anyhow::anyhow!("{key} is invalid: {e}"))?;
            Ok(if n <= 0 { None } else { Some(n) })
        }
        Err(_) => Ok(Some(default)),
    }
}

fn env_parse<T>(key: &str, default: T) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(v) => v
            .parse()
            .map_err(|e| anyhow::anyhow!("{key} is invalid: {e}")),
        Err(_) => Ok(default),
    }
}
