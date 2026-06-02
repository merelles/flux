use async_trait::async_trait;
use flux::{GraphSaveMode, Include, Result};

use crate::{PostgresRepository, SqlEntity};

#[async_trait]
pub trait PostgresAggregate: SqlEntity + flux::AggregateRoot + Sized {
    async fn load_relations(
        repository: &PostgresRepository<Self>,
        aggregate: &mut Self,
        includes: &[Include<Self>],
    ) -> Result<()>;

    async fn insert_relations(
        repository: &PostgresRepository<Self>,
        aggregate: &Self,
    ) -> Result<()>;

    async fn update_relations(
        repository: &PostgresRepository<Self>,
        aggregate: &Self,
        mode: GraphSaveMode,
    ) -> Result<()>;

    async fn delete_relations(repository: &PostgresRepository<Self>, id: &Self::Id) -> Result<()>;
}
