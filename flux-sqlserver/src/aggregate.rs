use async_trait::async_trait;
use flux::{GraphSaveMode, Include, Result};
use futures_util::io::{AsyncRead, AsyncWrite};

use crate::{SqlServerEntity, SqlServerRepository};

#[async_trait]
pub trait SqlServerAggregate: SqlServerEntity + flux::AggregateRoot + Sized {
    async fn load_relations<S>(
        repository: &SqlServerRepository<Self, S>,
        aggregate: &mut Self,
        includes: &[Include<Self>],
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync;

    async fn insert_relations<S>(
        repository: &SqlServerRepository<Self, S>,
        aggregate: &Self,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync;

    async fn update_relations<S>(
        repository: &SqlServerRepository<Self, S>,
        aggregate: &Self,
        mode: GraphSaveMode,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync;

    async fn delete_relations<S>(
        repository: &SqlServerRepository<Self, S>,
        id: &Self::Id,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + Sync;
}
