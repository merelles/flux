pub mod config;
pub mod entity;
pub mod error;
pub mod filter;
pub mod repository;
pub mod specification;

// PostgreSQL implementation
#[cfg(feature = "postgres")]
pub mod postgres;

// SQL Server implementation
#[cfg(feature = "sqlserver")]
pub mod sqlserver;

// Re-export PostgreSQL types when feature is enabled
#[cfg(feature = "postgres")]
pub use postgres::{create_connection, PostgresRepository};
// Re-export SQL Server types when feature is enabled
#[cfg(feature = "sqlserver")]
pub use sqlserver::SqlServerRepository;
// Re-export common types
pub use uuid::Uuid;

pub use self::config::Config;
pub use self::entity::{AggregateRoot, Entity, EntityExt};
pub use self::error::RepositoryError;
pub use self::filter::{Filter, FilterBuilder, GenericFilter, OrderDirection};
pub use self::repository::Repository;

// Re-export proc macros (they're separate from the trait)
// Note: Users import these as flux_derive::{Entity, Enum}
