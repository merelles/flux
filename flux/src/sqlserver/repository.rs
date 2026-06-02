//! SQL Server repository implementation using Tiberius
//!
//! Tiberius is an excellent choice for SQL Server - modern, async, and well-maintained.

use std::marker::PhantomData;
use std::result::Result;
use std::sync::Arc;

use async_trait::async_trait;
// Tiberius types
use tiberius::Client as SqlServerClient;
use uuid::Uuid;

use crate::{Entity, Repository as RepositoryTrait, RepositoryError};
// Note: Tiberius uses tokio

/// Generic SQL Server repository implementation
///
/// This provides a fully generic CRUD implementation for any Entity type.
/// All SQL is generated automatically based on the Entity metadata.
///
/// Uses Tiberius - an excellent async SQL Server driver.
#[derive(Clone)]
pub struct SqlServerRepository<T: Entity> {
    client: Arc<SqlServerClient>,
    _phantom: PhantomData<T>,
}

impl<T: Entity> SqlServerRepository<T> {
    /// Creates a new SqlServerRepository instance
    pub fn new(client: Arc<SqlServerClient>) -> Self {
        Self {
            client,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<T: Entity> RepositoryTrait<T> for SqlServerRepository<T> {
    async fn find_by_id(&self, id: Uuid) -> Result<T, RepositoryError> {
        println!(
            "[INFO] SQL Server: Buscando {} com {}: {}",
            T::table_name(),
            T::primary_key(),
            id
        );

        // TODO: Implement with Tiberius
        // Tiberius uses T-SQL syntax
        // Example: SELECT * FROM table WHERE pk = @P1

        println!("[WARN] SQL Server repository not yet implemented");
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }

    async fn find_all(&self) -> Result<Vec<T>, RepositoryError> {
        println!(
            "[INFO] SQL Server: Buscando todos os registros de {}",
            T::table_name()
        );
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }

    async fn save(&self, entity: T) -> Result<T, RepositoryError> {
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }

    async fn insert(&self, entity: T) -> Result<T, RepositoryError> {
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }

    async fn update(&self, entity: T) -> Result<T, RepositoryError> {
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }

    async fn delete(&self, id: Uuid) -> Result<bool, RepositoryError> {
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }

    async fn exists(&self, id: Uuid) -> Result<bool, RepositoryError> {
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }

    async fn count(&self) -> Result<u64, RepositoryError> {
        Err(RepositoryError::OperationFailed(
            "SQL Server repository implementation is pending".to_string(),
        ))
    }
}
