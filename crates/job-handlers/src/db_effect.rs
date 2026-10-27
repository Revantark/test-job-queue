use std::sync::Arc;

use async_trait::async_trait;
use job_core::JobOutcome;
use job_worker::{Handler, JobContext};
use serde_json::json;

use crate::EffectSink;

/// Records a side effect keyed on the job id.
///
/// This is the idempotency workhorse: because delivery is at-least-once, a
/// correct effectful handler must make repeated runs harmless. Writing keyed on
/// `job_id` (via a store-side upsert) does exactly that, and lets the
/// exactly-once test assert that each job's effect was applied once.
pub struct DbEffectHandler {
    sink: Arc<dyn EffectSink>,
}

impl DbEffectHandler {
    /// Build a handler that writes effects through `sink`.
    pub fn new(sink: Arc<dyn EffectSink>) -> Self {
        Self { sink }
    }
}

#[async_trait]
impl Handler for DbEffectHandler {
    fn kind(&self) -> &'static str {
        "db_effect"
    }

    async fn run(&self, ctx: JobContext) -> JobOutcome {
        match self.sink.record(ctx.job_id).await {
            Ok(created) => JobOutcome::Success {
                result: json!({ "effect_created": created }),
            },
            Err(e) => JobOutcome::Retryable {
                error: e.to_string(),
            },
        }
    }
}
