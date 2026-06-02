use std::error::Error;

use tokio_postgres::types::ToSql;
use tokio_postgres::Row;
use uuid::Uuid;

/// Core Entity trait for mapping database rows to Rust types
///
/// This trait must be implemented for all database entities.
pub trait Entity: Send + Sync + Sized + Clone {
    /// Returns the table name in the database
    fn table_name() -> &'static str;

    /// Returns the primary key column name
    fn primary_key() -> &'static str;

    /// Converts a database row into an entity
    ///
    /// # Errors
    /// Returns an error if the row data is invalid or mapping fails
    fn from_row(row: Row) -> std::result::Result<Self, Box<dyn Error + Send + Sync>>;

    /// Returns parameters for INSERT operations (all fields)
    fn to_insert_params(&self) -> Vec<&(dyn ToSql + Sync)>;

    /// Returns parameters for UPDATE operations (all fields except PK)
    fn to_update_params(&self) -> Vec<&(dyn ToSql + Sync)>;

    /// Returns the primary key value
    fn primary_key_value(&self) -> &(dyn ToSql + Sync);

    /// Returns all field names for this entity
    fn fields() -> Vec<&'static str>;

    /// Checks if this entity has a primary key value set
    fn has_id(&self) -> bool;
}

/// Helper extension trait for entities
pub trait EntityExt: Entity {
    /// Returns the primary key as Uuid
    fn get_id(&self) -> Option<Uuid>;

    /// Checks if this is a new entity (no ID set)
    fn is_new(&self) -> bool {
        !self.has_id()
    }
}

impl<T: Entity> EntityExt for T {
    fn get_id(&self) -> Option<Uuid> {
        // This is a placeholder - actual implementation will use the macro
        // to generate proper getter based on the PK field type
        None
    }
}

/// Aggregate Root trait for entities with related children
///
/// This trait enables automatic loading and saving of related entities
/// using #[has_many] and #[has_one] annotations.
///
/// The macro will generate metadata methods that PostgresRepository uses
/// to automatically load/save children.
pub trait AggregateRoot: Entity + Send + Sync + Sized + Clone {
    /// Checks if this aggregate has any children to load/save
    fn has_children(&self) -> bool;

    /// Returns the list of child relationship names
    fn child_names() -> Vec<&'static str>;

    /// Gets the foreign key column name for a child relationship
    fn child_foreign_key(child_name: &str) -> Option<&'static str>;

    /// Gets the child entity type name as string (for macro generation)
    fn child_type(child_name: &str) -> Option<&'static str>;

    /// Indicates if a child is a collection (has_many) or single (has_one)
    fn is_collection(child_name: &str) -> bool;
}
