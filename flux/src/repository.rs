use std::future::Future;

use async_trait::async_trait;

use crate::{AggregateRoot, Entity, EntityId, GenericFilter, Include, Page, PageRequest, Result};

#[async_trait]
pub trait ReadRepository<T: Entity>: Send + Sync {
    async fn find_by_id(&self, id: &T::Id) -> Result<T>;

    async fn find_page(&self, page: PageRequest<T::Id>) -> Result<Page<T, T::Id>>;

    async fn find_page_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>;

    async fn find_all_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>;

    async fn exists(&self, id: &T::Id) -> Result<bool>;

    async fn count(&self) -> Result<u64>;
}

#[async_trait]
pub trait WriteRepository<T: Entity>: Send + Sync {
    async fn insert(&self, entity: &T) -> Result<T>;

    async fn update(&self, entity: &T) -> Result<T>;

    async fn save(&self, entity: &T) -> Result<T>;

    async fn delete(&self, id: &T::Id) -> Result<bool>;
}

#[async_trait]
pub trait BulkRepository<T: Entity>: Send + Sync {
    async fn insert_many(&self, entities: &[T]) -> Result<Vec<T>>;

    async fn update_many(&self, entities: &[T]) -> Result<Vec<T>>;

    async fn save_many(&self, entities: &[T]) -> Result<Vec<T>>;

    async fn delete_many(&self, ids: &[T::Id]) -> Result<u64>;
}

#[async_trait]
pub trait RelationRepository<T: Entity>: Send + Sync {
    async fn find_by_foreign_key<K>(
        &self,
        field: &str,
        value: &K,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>
    where
        K: EntityId;

    async fn delete_by_foreign_key<K>(&self, field: &str, value: &K) -> Result<u64>
    where
        K: EntityId;
}

pub trait Repository<T>:
    ReadRepository<T> + WriteRepository<T> + BulkRepository<T> + RelationRepository<T>
where
    T: Entity,
{
}

#[async_trait]
pub trait StreamRepository<T>: ReadRepository<T>
where
    T: Entity + 'static,
{
    async fn for_each_page<F, Fut>(&self, limit: u32, handler: F) -> Result<()>
    where
        F: FnMut(Page<T, T::Id>) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send;

    async fn for_each_page_with_filter<F, Fut>(
        &self,
        filter: GenericFilter<T>,
        limit: u32,
        handler: F,
    ) -> Result<()>
    where
        F: FnMut(Page<T, T::Id>) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send;
}

#[async_trait]
impl<T, R> StreamRepository<T> for R
where
    T: Entity + 'static,
    R: ReadRepository<T> + Sync,
{
    async fn for_each_page<F, Fut>(&self, limit: u32, mut handler: F) -> Result<()>
    where
        F: FnMut(Page<T, T::Id>) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        let mut after = None;

        loop {
            let page = self.find_page(PageRequest::cursor(limit, after)).await?;
            after = page.next_cursor.clone();
            let should_continue = after.is_some();
            handler(page).await?;

            if !should_continue {
                break;
            }
        }

        Ok(())
    }

    async fn for_each_page_with_filter<F, Fut>(
        &self,
        filter: GenericFilter<T>,
        limit: u32,
        mut handler: F,
    ) -> Result<()>
    where
        F: FnMut(Page<T, T::Id>) -> Fut + Send,
        Fut: Future<Output = Result<()>> + Send,
    {
        let mut after = None;

        loop {
            let page = self
                .find_page_with_filter(filter.clone(), PageRequest::cursor(limit, after))
                .await?;
            after = page.next_cursor.clone();
            let should_continue = after.is_some();
            handler(page).await?;

            if !should_continue {
                break;
            }
        }

        Ok(())
    }
}

impl<T, R> Repository<T> for R
where
    T: Entity,
    R: ReadRepository<T> + WriteRepository<T> + BulkRepository<T> + RelationRepository<T>,
{
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphSaveMode {
    AppendChildren,
    UpsertChildren,
    ReplaceChildren,
}

#[async_trait]
pub trait AggregateRepository<A: AggregateRoot>: Send + Sync {
    async fn find_graph_by_id(&self, id: &A::Id, includes: &[Include<A>]) -> Result<A>;

    async fn insert_graph(&self, aggregate: &A) -> Result<A>;

    async fn update_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A>;

    async fn save_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A>;

    async fn delete_graph(&self, id: &A::Id) -> Result<bool>;
}
