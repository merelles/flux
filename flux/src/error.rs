use thiserror::Error;

#[derive(Error, Debug)]
pub enum RepositoryError {
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),

    #[error("Entity not found")]
    NotFound,

    #[error("Invalid entity data: {0}")]
    InvalidData(String),

    #[error("Concurrency error: entity was modified")]
    ConcurrencyConflict,

    #[error("Operation failed: {0}")]
    OperationFailed(String),
}

pub type Result<T> = std::result::Result<T, RepositoryError>;
