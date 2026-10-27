use std::collections::HashMap;
use std::sync::Arc;

use crate::handler::Handler;

/// Maps a job `kind` to the handler that serves it.
///
/// Open for extension (register more kinds) and closed for modification (the
/// engine never changes when a new kind is added).
#[derive(Default, Clone)]
pub struct Registry {
    handlers: HashMap<&'static str, Arc<dyn Handler>>,
}

impl Registry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a handler, returning `self` for chaining.
    ///
    /// A later registration for the same kind replaces the earlier one, which
    /// keeps tests simple and makes overrides explicit.
    #[must_use]
    #[allow(clippy::should_implement_trait)]
    pub fn add<H: Handler>(mut self, handler: H) -> Self {
        self.handlers.insert(handler.kind(), Arc::new(handler));
        self
    }

    /// Register a handler that is already behind an `Arc`.
    #[must_use]
    pub fn add_arc(mut self, kind: &'static str, handler: Arc<dyn Handler>) -> Self {
        self.handlers.insert(kind, handler);
        self
    }

    /// Look up the handler for `kind`.
    pub fn get(&self, kind: &str) -> Option<Arc<dyn Handler>> {
        self.handlers.get(kind).cloned()
    }

    /// Whether a handler is registered for `kind`.
    pub fn contains(&self, kind: &str) -> bool {
        self.handlers.contains_key(kind)
    }

    /// Number of registered kinds.
    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::JobContext;
    use job_core::JobOutcome;

    struct Dummy(&'static str);

    #[async_trait::async_trait]
    impl Handler for Dummy {
        fn kind(&self) -> &'static str {
            self.0
        }
        async fn run(&self, _ctx: JobContext) -> JobOutcome {
            JobOutcome::success()
        }
    }

    #[test]
    fn lookup_and_missing() {
        let reg = Registry::new().add(Dummy("a")).add(Dummy("b"));
        assert!(reg.contains("a"));
        assert!(reg.contains("b"));
        assert!(!reg.contains("c"));
        assert!(reg.get("a").is_some());
        assert!(reg.get("c").is_none());
        assert_eq!(reg.len(), 2);
    }
}
