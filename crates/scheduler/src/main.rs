//! `scheduler` — a database-backed job scheduler and executor.

use anyhow::Result;
use tracing_subscriber::EnvFilter;

mod cli;
mod config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    cli::Cli::run().await
}
