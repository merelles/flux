use std::marker::PhantomData;

use async_trait::async_trait;
use flux::{
    BulkRepository, EntityId, GenericFilter, Page, PageRequest, ReadRepository, RelationRepository,
    RepositoryError, Result, WriteRepository,
};
use futures_util::TryStreamExt;
use mongodb::{
    bson::{Bson, Document},
    Collection, Database,
};

use crate::{entity::unsupported_id, render_filter, MongoEntity, MongoId};

pub struct MongoRepository<T: MongoEntity> {
    database: Database,
    _marker: PhantomData<T>,
}

impl<T: MongoEntity> Clone for MongoRepository<T> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: MongoEntity> MongoRepository<T> {
    pub fn new(database: Database) -> Self {
        Self {
            database,
            _marker: PhantomData,
        }
    }

    pub fn collection(&self) -> Collection<Document> {
        self.database.collection::<Document>(T::collection_name())
    }

    async fn query_page(
        &self,
        mut filter: Document,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>
    where
        T::Id: MongoId,
    {
        let limit = page.limit();
        let collection = self.collection();

        let find = match page {
            PageRequest::Offset { offset, .. } => {
                collection.find(filter).limit(i64::from(limit)).skip(offset)
            }
            PageRequest::Cursor { after, .. } => {
                if let Some(after) = after {
                    let mut cursor_filter = Document::new();
                    cursor_filter.insert("$gt", after.to_bson()?);
                    filter.insert(T::id_field(), Bson::Document(cursor_filter));
                }
                collection.find(filter).limit(i64::from(limit))
            }
        };

        let mut cursor = find.await.map_err(map_error)?;
        let mut items = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(map_error)? {
            items.push(T::from_document(document)?);
        }

        let next_cursor = if items.len() == limit as usize {
            items.last().map(|item| item.id().clone())
        } else {
            None
        };

        Ok(Page::new(items, limit, next_cursor, None))
    }
}

#[async_trait]
impl<T> ReadRepository<T> for MongoRepository<T>
where
    T: MongoEntity,
    T::Id: MongoId,
{
    async fn find_by_id(&self, id: &T::Id) -> Result<T> {
        let filter = id_filter::<T>(id)?;
        let document = self
            .collection()
            .find_one(filter)
            .await
            .map_err(map_error)?
            .ok_or(RepositoryError::NotFound)?;
        T::from_document(document)
    }

    async fn find_page(&self, page: PageRequest<T::Id>) -> Result<Page<T, T::Id>> {
        self.query_page(Document::new(), page).await
    }

    async fn find_page_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>> {
        self.query_page(render_filter(&filter)?, page).await
    }

    async fn find_all_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>> {
        self.find_page_with_filter(filter, page).await
    }

    async fn exists(&self, id: &T::Id) -> Result<bool> {
        let filter = id_filter::<T>(id)?;
        let count = self
            .collection()
            .count_documents(filter)
            .await
            .map_err(map_error)?;
        Ok(count > 0)
    }

    async fn count(&self) -> Result<u64> {
        self.collection()
            .count_documents(Document::new())
            .await
            .map_err(map_error)
    }
}

#[async_trait]
impl<T> WriteRepository<T> for MongoRepository<T>
where
    T: MongoEntity,
    T::Id: MongoId,
{
    async fn insert(&self, entity: &T) -> Result<T> {
        self.collection()
            .insert_one(entity.to_document()?)
            .await
            .map_err(map_error)?;
        Ok(entity.clone())
    }

    async fn update(&self, entity: &T) -> Result<T> {
        let filter = id_filter::<T>(entity.id())?;
        let result = self
            .collection()
            .replace_one(filter, entity.to_document()?)
            .await
            .map_err(map_error)?;
        if result.matched_count == 0 {
            return Err(RepositoryError::NotFound);
        }
        Ok(entity.clone())
    }

    async fn save(&self, entity: &T) -> Result<T> {
        let filter = id_filter::<T>(entity.id())?;
        self.collection()
            .replace_one(filter, entity.to_document()?)
            .upsert(true)
            .await
            .map_err(map_error)?;
        Ok(entity.clone())
    }

    async fn delete(&self, id: &T::Id) -> Result<bool> {
        let filter = id_filter::<T>(id)?;
        let result = self
            .collection()
            .delete_one(filter)
            .await
            .map_err(map_error)?;
        Ok(result.deleted_count > 0)
    }
}

#[async_trait]
impl<T> BulkRepository<T> for MongoRepository<T>
where
    T: MongoEntity,
    T::Id: MongoId,
{
    async fn insert_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let documents = entities
            .iter()
            .map(T::to_document)
            .collect::<Result<Vec<_>>>()?;
        self.collection()
            .insert_many(documents)
            .await
            .map_err(map_error)?;
        Ok(entities.to_vec())
    }

    async fn update_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let models = self.replace_models(entities, false)?;
        let result = self
            .database
            .client()
            .bulk_write(models)
            .await
            .map_err(map_error)?;

        if result.matched_count < entities.len() as i64 {
            return Err(RepositoryError::NotFound);
        }

        Ok(entities.to_vec())
    }

    async fn save_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let models = self.replace_models(entities, true)?;
        self.database
            .client()
            .bulk_write(models)
            .await
            .map_err(map_error)?;

        Ok(entities.to_vec())
    }

    async fn delete_many(&self, ids: &[T::Id]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let values = ids
            .iter()
            .map(MongoId::to_bson)
            .collect::<Result<Vec<_>>>()?;
        let mut in_filter = Document::new();
        in_filter.insert("$in", Bson::Array(values));
        let mut filter = Document::new();
        filter.insert(T::id_field(), Bson::Document(in_filter));
        let result = self
            .collection()
            .delete_many(filter)
            .await
            .map_err(map_error)?;
        Ok(result.deleted_count)
    }
}

impl<T: MongoEntity> MongoRepository<T>
where
    T::Id: MongoId,
{
    fn replace_models(
        &self,
        entities: &[T],
        upsert: bool,
    ) -> Result<Vec<mongodb::options::ReplaceOneModel>> {
        let collection = self.collection();
        entities
            .iter()
            .map(|entity| {
                let filter = id_filter::<T>(entity.id())?;
                let document = entity.to_document()?;
                let mut model = collection
                    .replace_one_model(filter, &document)
                    .map_err(map_error)?;
                model.upsert = Some(upsert);
                Ok(model)
            })
            .collect()
    }
}

#[async_trait]
impl<T> RelationRepository<T> for MongoRepository<T>
where
    T: MongoEntity,
    T::Id: MongoId,
{
    async fn find_by_foreign_key<K>(
        &self,
        field: &str,
        value: &K,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>
    where
        K: EntityId,
    {
        let mut filter = Document::new();
        filter.insert(field, entity_id_to_bson(value)?);
        self.query_page(filter, page).await
    }

    async fn delete_by_foreign_key<K>(&self, field: &str, value: &K) -> Result<u64>
    where
        K: EntityId,
    {
        let mut filter = Document::new();
        filter.insert(field, entity_id_to_bson(value)?);
        let result = self
            .collection()
            .delete_many(filter)
            .await
            .map_err(map_error)?;
        Ok(result.deleted_count)
    }
}

fn entity_id_to_bson<I: EntityId>(id: &I) -> Result<Bson> {
    let any = id as &dyn std::any::Any;
    if let Some(value) = any.downcast_ref::<crate::MongoObjectId>() {
        Ok(Bson::ObjectId(value.0))
    } else if let Some(value) = any.downcast_ref::<String>() {
        Ok(Bson::String(value.clone()))
    } else if let Some(value) = any.downcast_ref::<i32>() {
        Ok(Bson::Int32(*value))
    } else if let Some(value) = any.downcast_ref::<i64>() {
        Ok(Bson::Int64(*value))
    } else if let Some(value) = any.downcast_ref::<uuid::Uuid>() {
        Ok(Bson::String(value.to_string()))
    } else {
        Err(unsupported_id::<I>())
    }
}

fn id_filter<T>(id: &T::Id) -> Result<Document>
where
    T: MongoEntity,
    T::Id: MongoId,
{
    let mut filter = Document::new();
    filter.insert(T::id_field(), id.to_bson()?);
    Ok(filter)
}

fn map_error(error: mongodb::error::Error) -> RepositoryError {
    RepositoryError::Backend(error.to_string())
}
