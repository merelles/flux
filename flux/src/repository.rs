use std::result::Result;

use async_trait::async_trait;
use uuid::Uuid;

use crate::entity::Entity;
use crate::error::RepositoryError;

/// Generic repository interface for CRUD operations
///
/// This trait provides a clean abstraction over data persistence.
/// Implementations should be database-agnostic, but we primarily use PostgreSQL.
#[async_trait]
pub trait Repository<T: Entity>: Send + Sync {
    /// Finds a single entity by its primary key
    ///
    /// # Errors
    /// Returns `RepositoryError::NotFound` if no entity exists with the given ID
    async fn find_by_id(&self, id: Uuid) -> Result<T, RepositoryError>;

    /// Finds multiple entities using a specification
    ///
    /// Returns an empty vector if no entities match the specification.
    async fn find_all(&self) -> Result<Vec<T>, RepositoryError>;

    /// Finds multiple entities using a filter
    ///
    /// Returns an empty vector if no entities match the filter.
    async fn find_all_with_filter(
        &self,
        filter: crate::filter::GenericFilter<T>,
    ) -> Result<Vec<T>, RepositoryError>;

    /// Saves an entity (insert or update)
    ///
    /// If the entity has an ID and exists in the database, it will be updated.
    /// Otherwise, a new entity will be inserted.
    ///
    /// # Returns
    /// Returns the saved entity with any generated fields filled in
    async fn save(&self, entity: T) -> Result<T, RepositoryError>;

    /// Inserts a new entity
    ///
    /// # Errors
    /// Returns an error if an entity with the same ID already exists
    async fn insert(&self, entity: T) -> Result<T, RepositoryError>;

    /// Updates an existing entity
    ///
    /// # Errors
    /// Returns `RepositoryError::NotFound` if the entity doesn't exist
    async fn update(&self, entity: T) -> Result<T, RepositoryError>;

    /// Deletes an entity by its primary key
    ///
    /// # Returns
    /// Returns `true` if an entity was deleted, `false` if no entity existed
    async fn delete(&self, id: Uuid) -> Result<bool, RepositoryError>;

    /// Checks if an entity exists with the given primary key
    async fn exists(&self, id: Uuid) -> Result<bool, RepositoryError>;

    /// Counts entities matching a specification
    async fn count(&self) -> Result<u64, RepositoryError>;

    /// Finds entities by a foreign key field
    ///
    /// Returns an empty vector if no entities match.
    async fn find_by_foreign_key(
        &self,
        field: &str,
        value: &Uuid,
    ) -> Result<Vec<T>, RepositoryError>;

    /// Deletes entities by a foreign key field
    ///
    /// # Returns
    /// Returns the number of entities deleted
    async fn delete_by_foreign_key(
        &self,
        field: &str,
        value: &Uuid,
    ) -> Result<u64, RepositoryError>;
}
