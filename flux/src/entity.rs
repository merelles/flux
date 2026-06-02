use std::{any::Any, fmt::Debug, hash::Hash};

/// Core entity contract shared by all backend adapters.
/// Backend-specific crates own table/document mapping.
pub trait Entity: Send + Sync + Sized + Clone {
    type Id: EntityId;

    /// Returns the entity identifier.
    fn id(&self) -> &Self::Id;

    /// Allows adapters to distinguish new entities when an ID type supports it.
    fn has_id(&self) -> bool;
}

/// Identifier types accepted by the core repository contracts.
pub trait EntityId: Any + Clone + Debug + Send + Sync + Eq + Hash + 'static {}

macro_rules! impl_entity_id {
    ($($ty:ty),* $(,)?) => {
        $(impl EntityId for $ty {})*
    };
}

impl_entity_id!(i16, i32, i64, u16, u32, u64, String, uuid::Uuid);
