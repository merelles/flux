use thiserror::Error;

#[derive(Error, Debug)]
pub enum RepositoryError {
    #[error("Entity not found")]
    NotFound,

    #[error("Invalid entity data: {0}")]
    InvalidData(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Concurrency error: entity was modified")]
    ConcurrencyConflict,

    #[error("Unsupported operation: {0}")]
    Unsupported(String),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

pub type Result<T> = std::result::Result<T, RepositoryError>;
