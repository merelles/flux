use async_trait::async_trait;
use flux::{GraphSaveMode, Include, Result};

use crate::{MongoEntity, MongoRepository};

#[async_trait]
pub trait MongoAggregate: MongoEntity + flux::AggregateRoot + Sized {
    async fn load_relations(
        repository: &MongoRepository<Self>,
        aggregate: &mut Self,
        includes: &[Include<Self>],
    ) -> Result<()>;

    async fn insert_relations(repository: &MongoRepository<Self>, aggregate: &Self) -> Result<()>;

    async fn update_relations(
        repository: &MongoRepository<Self>,
        aggregate: &Self,
        mode: GraphSaveMode,
    ) -> Result<()>;

    async fn delete_relations(repository: &MongoRepository<Self>, id: &Self::Id) -> Result<()>;
}
