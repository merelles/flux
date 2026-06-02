pub mod aggregate;
pub mod entity;
pub mod error;
pub mod filter;
pub mod page;
pub mod repository;

pub use uuid::Uuid;

pub use self::aggregate::{
    AggregateRoot, CascadeAction, Include, OnReplace, RelationKind, RelationMetadata,
};
pub use self::entity::{Entity, EntityId};
pub use self::error::{RepositoryError, Result};
pub use self::filter::{
    FilterCondition, FilterExpr, FilterOp, FilterOperand, FilterValue, GenericFilter, OrderClause,
    OrderDirection,
};
pub use self::page::{Page, PageRequest};
pub use self::repository::{
    AggregateRepository, BulkRepository, GraphSaveMode, PageStream, ReadRepository,
    RelationRepository, Repository, StreamRepository, UnboundedReadRepository, WriteRepository,
};
