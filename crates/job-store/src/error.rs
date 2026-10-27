use job_core::JobError;

/// Errors surfaced by a [`Store`](crate::Store) implementation.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The backing database rejected or could not complete the operation.
    #[error("database error: {0}")]
    Database(String),
    /// A domain invariant was violated while mapping rows.
    #[error("invalid data: {0}")]
    Domain(#[from] JobError),
    /// The requested job does not exist.
    #[error("job {0} not found")]
    NotFound(i64),
}
