//! Postgres implementation of [`job_store::Store`].
//!
//! This is the only crate that speaks SQL. It uses `SELECT ... FOR UPDATE SKIP
//! LOCKED` to hand disjoint sets of due jobs to concurrent workers, and fences
//! every mutation with the claim's `lease_id` so a reclaimed job cannot be
//! completed by a stale worker.

#![warn(missing_docs)]

mod pg;
mod queries;

pub use pg::PgStore;

/// Embedded migrations, applied by [`PgStore::migrate`].
pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");
