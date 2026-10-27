//! Worker runtime: the `Handler` contract, the registry, and the loop that
//! claims, executes and finalizes jobs.
//!
//! The runtime depends only on [`job_store::Store`], never on a concrete
//! backend, so the same worker drives Postgres today and anything else later.

#![warn(missing_docs)]

mod engine;
mod handler;
mod reaper;
mod registry;

pub use engine::{Worker, WorkerConfig};
pub use handler::{Handler, HandlerFuture, JobContext};
pub use reaper::{Reaper, ReaperConfig};
pub use registry::Registry;
