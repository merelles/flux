use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use async_trait::async_trait;
use flux::{
    AggregateRepository, BulkRepository, CascadeAction, EntityId, GenericFilter, GraphSaveMode,
    Include, OnReplace, Page, PageRequest, ReadRepository, RelationMetadata, RelationRepository,
    RepositoryError, Result, WriteRepository,
};
use futures_util::TryStreamExt;
use mongodb::{
    bson::{Bson, Document},
    ClientSession, Collection, Database,
};
use tokio::sync::Mutex;

use crate::{entity::unsupported_id, render_filter_parts, MongoAggregate, MongoEntity, MongoId};

const DEFAULT_RELATION_PAGE_LIMIT: u32 = 512;

pub type MongoTransactionFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

pub struct MongoRepository<T: MongoEntity> {
    database: Database,
    session: Option<Arc<Mutex<ClientSession>>>,
    _marker: PhantomData<T>,
}

impl<T: MongoEntity> Clone for MongoRepository<T> {
    fn clone(&self) -> Self {
        Self {
            database: self.database.clone(),
            session: self.session.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: MongoEntity> MongoRepository<T> {
    pub fn new(database: Database) -> Self {
        Self {
            database,
            session: None,
            _marker: PhantomData,
        }
    }

    fn with_session(database: Database, session: Arc<Mutex<ClientSession>>) -> Self {
        Self {
            database,
            session: Some(session),
            _marker: PhantomData,
        }
    }

    fn child_repository<C>(&self) -> MongoRepository<C>
    where
        C: MongoEntity,
    {
        match &self.session {
            Some(session) => {
                MongoRepository::<C>::with_session(self.database.clone(), Arc::clone(session))
            }
            None => MongoRepository::<C>::new(self.database.clone()),
        }
    }

    pub fn collection(&self) -> Collection<Document> {
        self.database.collection::<Document>(T::collection_name())
    }

    pub fn database(&self) -> &Database {
        &self.database
    }

    pub async fn with_transaction<R, F>(&self, operation: F) -> Result<R>
    where
        R: Send,
        F: for<'session> FnOnce(&'session mut ClientSession) -> MongoTransactionFuture<'session, R>
            + Send,
    {
        let mut session = self
            .database
            .client()
            .start_session()
            .await
            .map_err(map_error)?;
        session.start_transaction().await.map_err(map_error)?;

        match operation(&mut session).await {
            Ok(value) => {
                session.commit_transaction().await.map_err(map_error)?;
                Ok(value)
            }
            Err(error) => {
                if let Err(abort_error) = session.abort_transaction().await.map_err(map_error) {
                    return Err(RepositoryError::OperationFailed(format!(
                        "transaction failed: {error}; abort failed: {abort_error}"
                    )));
                }
                Err(error)
            }
        }
    }

    async fn start_transaction_repository(&self) -> Result<Self> {
        let mut session = self
            .database
            .client()
            .start_session()
            .await
            .map_err(map_error)?;
        session.start_transaction().await.map_err(map_error)?;
        Ok(Self::with_session(
            self.database.clone(),
            Arc::new(Mutex::new(session)),
        ))
    }

    async fn commit_transaction_repository(&self) -> Result<()> {
        let session = self.session.as_ref().ok_or_else(|| {
            RepositoryError::OperationFailed("missing Mongo transaction session".to_string())
        })?;
        let mut session = session.lock().await;
        session.commit_transaction().await.map_err(map_error)
    }

    async fn rollback_transaction_repository(&self) -> Result<()> {
        let session = self.session.as_ref().ok_or_else(|| {
            RepositoryError::OperationFailed("missing Mongo transaction session".to_string())
        })?;
        let mut session = session.lock().await;
        session.abort_transaction().await.map_err(map_error)
    }

    async fn insert_one_document(&self, document: Document) -> Result<Bson> {
        let result = if let Some(session) = &self.session {
            let mut session = session.lock().await;
            self.collection()
                .insert_one(document)
                .session(&mut *session)
                .await
                .map_err(map_error)?
        } else {
            self.collection()
                .insert_one(document)
                .await
                .map_err(map_error)?
        };
        Ok(result.inserted_id)
    }

    async fn insert_many_documents(&self, documents: Vec<Document>) -> Result<()> {
        if let Some(session) = &self.session {
            let mut session = session.lock().await;
            self.collection()
                .insert_many(documents)
                .session(&mut *session)
                .await
                .map_err(map_error)?;
        } else {
            self.collection()
                .insert_many(documents)
                .await
                .map_err(map_error)?;
        }
        Ok(())
    }

    async fn bulk_write_models(
        &self,
        models: Vec<mongodb::options::ReplaceOneModel>,
    ) -> Result<mongodb::results::SummaryBulkWriteResult> {
        let models = models
            .into_iter()
            .map(mongodb::options::WriteModel::ReplaceOne)
            .collect::<Vec<_>>();

        if let Some(session) = &self.session {
            let mut session = session.lock().await;
            self.database
                .client()
                .bulk_write(models)
                .session(&mut *session)
                .await
                .map_err(map_error)
        } else {
            self.database
                .client()
                .bulk_write(models)
                .await
                .map_err(map_error)
        }
    }

    async fn replace_one_document(
        &self,
        filter: Document,
        document: Document,
        upsert: bool,
    ) -> Result<mongodb::results::UpdateResult> {
        let collection = self.collection();
        let action = collection.replace_one(filter, document).upsert(upsert);
        if let Some(session) = &self.session {
            let mut session = session.lock().await;
            action.session(&mut *session).await.map_err(map_error)
        } else {
            action.await.map_err(map_error)
        }
    }

    async fn delete_one_document(
        &self,
        filter: Document,
    ) -> Result<mongodb::results::DeleteResult> {
        if let Some(session) = &self.session {
            let mut session = session.lock().await;
            self.collection()
                .delete_one(filter)
                .session(&mut *session)
                .await
                .map_err(map_error)
        } else {
            self.collection()
                .delete_one(filter)
                .await
                .map_err(map_error)
        }
    }

    async fn delete_many_documents(
        &self,
        filter: Document,
    ) -> Result<mongodb::results::DeleteResult> {
        if let Some(session) = &self.session {
            let mut session = session.lock().await;
            self.collection()
                .delete_many(filter)
                .session(&mut *session)
                .await
                .map_err(map_error)
        } else {
            self.collection()
                .delete_many(filter)
                .await
                .map_err(map_error)
        }
    }

    async fn update_many_documents(
        &self,
        filter: Document,
        update: Document,
    ) -> Result<mongodb::results::UpdateResult> {
        if let Some(session) = &self.session {
            let mut session = session.lock().await;
            self.collection()
                .update_many(filter, update)
                .session(&mut *session)
                .await
                .map_err(map_error)
        } else {
            self.collection()
                .update_many(filter, update)
                .await
                .map_err(map_error)
        }
    }

    pub async fn load_has_many<C, K>(
        &self,
        metadata: &RelationMetadata,
        source: &K,
    ) -> Result<Vec<C>>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        let foreign_key = required_relation_part(metadata, metadata.foreign_key, "foreign_key")?;
        let child_repo = self.child_repository::<C>();
        load_all_by_foreign_key(&child_repo, foreign_key, source).await
    }

    pub async fn load_has_one<C, K>(
        &self,
        metadata: &RelationMetadata,
        source: &K,
    ) -> Result<Option<C>>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        let foreign_key = required_relation_part(metadata, metadata.foreign_key, "foreign_key")?;
        let child_repo = self.child_repository::<C>();
        let page = child_repo
            .find_by_foreign_key(
                foreign_key,
                source,
                PageRequest::cursor(1, Option::<C::Id>::None),
            )
            .await?;
        Ok(page.items.into_iter().next())
    }

    pub async fn save_has_many<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
        mode: GraphSaveMode,
    ) -> Result<()>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        let foreign_key = required_relation_part(metadata, metadata.foreign_key, "foreign_key")?;
        let child_repo = self.child_repository::<C>();

        match mode {
            GraphSaveMode::AppendChildren => {
                child_repo.insert_many(children).await?;
            }
            GraphSaveMode::UpsertChildren => {
                child_repo.save_many(children).await?;
            }
            GraphSaveMode::ReplaceChildren => {
                match metadata.on_replace {
                    OnReplace::KeepMissing => {}
                    OnReplace::DeleteMissing => {
                        delete_missing_by_foreign_key(&child_repo, foreign_key, source, children)
                            .await?;
                    }
                    OnReplace::UnlinkMissing => {
                        unlink_missing_by_foreign_key(&child_repo, foreign_key, source, children)
                            .await?;
                    }
                }
                child_repo.save_many(children).await?;
            }
        }

        Ok(())
    }

    pub async fn save_has_one<C, K>(
        &self,
        metadata: &RelationMetadata,
        child: Option<&C>,
        source: &K,
        mode: GraphSaveMode,
    ) -> Result<()>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        let foreign_key = required_relation_part(metadata, metadata.foreign_key, "foreign_key")?;
        let child_repo = self.child_repository::<C>();

        match (mode, child) {
            (GraphSaveMode::AppendChildren, Some(child)) => {
                child_repo.insert(child).await?;
            }
            (GraphSaveMode::UpsertChildren | GraphSaveMode::ReplaceChildren, Some(child)) => {
                child_repo.save(child).await?;
            }
            (GraphSaveMode::ReplaceChildren, None) => match metadata.on_replace {
                OnReplace::KeepMissing => {}
                OnReplace::DeleteMissing => {
                    child_repo
                        .delete_by_foreign_key(foreign_key, source)
                        .await?;
                }
                OnReplace::UnlinkMissing => {
                    unlink_all_by_foreign_key(&child_repo, foreign_key, source).await?;
                }
            },
            _ => {}
        }

        Ok(())
    }

    pub async fn delete_relation<C, K>(&self, metadata: &RelationMetadata, source: &K) -> Result<()>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        if metadata.cascade != CascadeAction::Delete {
            return Ok(());
        }

        let foreign_key = required_relation_part(metadata, metadata.foreign_key, "foreign_key")?;
        let child_repo = self.child_repository::<C>();
        child_repo
            .delete_by_foreign_key(foreign_key, source)
            .await?;
        Ok(())
    }

    pub async fn load_many_to_many<C, K>(
        &self,
        metadata: &RelationMetadata,
        source: &K,
    ) -> Result<Vec<C>>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        let join_table = required_relation_part(metadata, metadata.join_table, "join_table")?;
        let source_key = required_relation_part(metadata, metadata.source_key, "source_key")?;
        let target_key = required_relation_part(metadata, metadata.target_key, "target_key")?;
        let target_primary_key = metadata.target_primary_key.unwrap_or_else(C::id_field);

        let join_collection = self.database.collection::<Document>(join_table);
        let mut join_filter = Document::new();
        join_filter.insert(source_key, entity_id_to_bson(source)?);
        let mut cursor = join_collection.find(join_filter).await.map_err(map_error)?;
        let mut target_values = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(map_error)? {
            if let Some(value) = document.get(target_key) {
                target_values.push(value.clone());
            }
        }

        if target_values.is_empty() {
            return Ok(Vec::new());
        }

        let child_repo = self.child_repository::<C>();
        let mut in_filter = Document::new();
        in_filter.insert("$in", Bson::Array(target_values));
        let mut target_filter = Document::new();
        target_filter.insert(target_primary_key, Bson::Document(in_filter));

        let mut cursor = child_repo
            .collection()
            .find(target_filter)
            .sort(default_sort::<C>())
            .await
            .map_err(map_error)?;
        let mut items = Vec::new();
        while let Some(document) = cursor.try_next().await.map_err(map_error)? {
            items.push(C::from_document(document)?);
        }
        Ok(items)
    }

    pub async fn save_many_to_many<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
        mode: GraphSaveMode,
    ) -> Result<()>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        let child_repo = self.child_repository::<C>();
        child_repo.save_many(children).await?;

        if mode == GraphSaveMode::ReplaceChildren && metadata.on_replace != OnReplace::KeepMissing {
            self.delete_missing_many_to_many_links(metadata, children, source)
                .await?;
        }

        self.upsert_many_to_many_links(metadata, children, source)
            .await
    }

    pub async fn delete_many_to_many_links<K>(
        &self,
        metadata: &RelationMetadata,
        source: &K,
    ) -> Result<()>
    where
        K: EntityId,
    {
        let join_table = required_relation_part(metadata, metadata.join_table, "join_table")?;
        let source_key = required_relation_part(metadata, metadata.source_key, "source_key")?;
        let mut filter = Document::new();
        filter.insert(source_key, entity_id_to_bson(source)?);
        delete_many_in_collection(&self.database, self.session.as_ref(), join_table, filter)
            .await?;
        Ok(())
    }

    async fn delete_missing_many_to_many_links<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
    ) -> Result<()>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        let join_table = required_relation_part(metadata, metadata.join_table, "join_table")?;
        let source_key = required_relation_part(metadata, metadata.source_key, "source_key")?;
        let target_key = required_relation_part(metadata, metadata.target_key, "target_key")?;
        let mut filter = Document::new();
        filter.insert(source_key, entity_id_to_bson(source)?);

        if !children.is_empty() {
            let keep_values = children
                .iter()
                .map(|child| child.id().to_bson())
                .collect::<Result<Vec<_>>>()?;
            let mut not_in = Document::new();
            not_in.insert("$nin", Bson::Array(keep_values));
            filter.insert(target_key, Bson::Document(not_in));
        }

        delete_many_in_collection(&self.database, self.session.as_ref(), join_table, filter)
            .await?;
        Ok(())
    }

    async fn upsert_many_to_many_links<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
    ) -> Result<()>
    where
        C: MongoEntity,
        C::Id: MongoId,
        K: EntityId,
    {
        if children.is_empty() {
            return Ok(());
        }

        let join_table = required_relation_part(metadata, metadata.join_table, "join_table")?;
        let source_key = required_relation_part(metadata, metadata.source_key, "source_key")?;
        let target_key = required_relation_part(metadata, metadata.target_key, "target_key")?;
        let join_collection = self.database.collection::<Document>(join_table);
        let source_value = entity_id_to_bson(source)?;
        let mut models = Vec::with_capacity(children.len());

        for child in children {
            let target_value = child.id().to_bson()?;
            let mut filter = Document::new();
            filter.insert(source_key, source_value.clone());
            filter.insert(target_key, target_value.clone());

            let mut document = Document::new();
            document.insert(source_key, source_value.clone());
            document.insert(target_key, target_value);

            let mut model = join_collection
                .replace_one_model(filter, &document)
                .map_err(map_error)?;
            model.upsert = Some(true);
            models.push(model);
        }

        self.bulk_write_models(models).await?;
        Ok(())
    }

    async fn query_page(
        &self,
        mut filter: Document,
        sort: Option<Document>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>
    where
        T::Id: MongoId,
    {
        let limit = page.limit();
        let collection = self.collection();
        let total = collection
            .count_documents(filter.clone())
            .await
            .map_err(map_error)?;

        let find = match page {
            PageRequest::Offset { offset, .. } => collection
                .find(filter)
                .sort(sort.unwrap_or_else(|| default_sort::<T>()))
                .limit(i64::from(limit))
                .skip(offset),
            PageRequest::Cursor { after, .. } => {
                if let Some(after) = after {
                    let mut cursor_filter = Document::new();
                    cursor_filter.insert("$gt", after.to_bson()?);
                    filter.insert(T::id_field(), Bson::Document(cursor_filter));
                }
                collection
                    .find(filter)
                    .sort(sort.unwrap_or_else(|| default_sort::<T>()))
                    .limit(i64::from(limit))
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

        Ok(Page::new(items, limit, next_cursor, Some(total)))
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
        self.query_page(Document::new(), None, page).await
    }

    async fn find_page_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>> {
        let rendered = render_filter_parts(&filter)?;
        self.query_page(rendered.filter, rendered.sort, page).await
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
        let inserted_id = self.insert_one_document(entity.to_document()?).await?;
        let mut saved = entity.clone();
        if !saved.has_id() {
            <T as flux::Entity>::set_id(&mut saved, bson_to_entity_id::<T::Id>(inserted_id)?);
        }
        Ok(saved)
    }

    async fn update(&self, entity: &T) -> Result<T> {
        let filter = id_filter::<T>(entity.id())?;
        let result = self
            .replace_one_document(filter, entity.to_document()?, false)
            .await?;
        if result.matched_count == 0 {
            return Err(RepositoryError::NotFound);
        }
        Ok(entity.clone())
    }

    async fn save(&self, entity: &T) -> Result<T> {
        let filter = id_filter::<T>(entity.id())?;
        self.replace_one_document(filter, entity.to_document()?, true)
            .await?;
        Ok(entity.clone())
    }

    async fn delete(&self, id: &T::Id) -> Result<bool> {
        let filter = id_filter::<T>(id)?;
        let result = self.delete_one_document(filter).await?;
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
        self.insert_many_documents(documents).await?;
        Ok(entities.to_vec())
    }

    async fn update_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let models = self.replace_models(entities, false)?;
        let result = self.bulk_write_models(models).await?;

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
        self.bulk_write_models(models).await?;

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
        let result = self.delete_many_documents(filter).await?;
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
        self.query_page(filter, None, page).await
    }

    async fn delete_by_foreign_key<K>(&self, field: &str, value: &K) -> Result<u64>
    where
        K: EntityId,
    {
        let mut filter = Document::new();
        filter.insert(field, entity_id_to_bson(value)?);
        let result = self.delete_many_documents(filter).await?;
        Ok(result.deleted_count)
    }
}

#[async_trait]
impl<A> AggregateRepository<A> for MongoRepository<A>
where
    A: MongoAggregate,
    A::Id: MongoId,
{
    async fn find_graph_by_id(&self, id: &A::Id, includes: &[Include<A>]) -> Result<A> {
        let mut aggregate = self.find_by_id(id).await?;
        A::load_relations(self, &mut aggregate, includes).await?;
        Ok(aggregate)
    }

    async fn insert_graph(&self, aggregate: &A) -> Result<A> {
        let tx_repo = self.start_transaction_repository().await?;
        let saved = match tx_repo.insert(aggregate).await {
            Ok(saved) => saved,
            Err(error) => {
                tx_repo.rollback_transaction_repository().await?;
                return Err(error);
            }
        };
        let mut aggregate = aggregate.clone();
        <A as flux::Entity>::set_id(&mut aggregate, saved.id().clone());

        if let Err(error) = A::insert_relations(&tx_repo, &aggregate).await {
            tx_repo.rollback_transaction_repository().await?;
            return Err(error);
        }
        tx_repo.commit_transaction_repository().await?;
        self.reload_all_relations(saved.id()).await
    }

    async fn update_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A> {
        let tx_repo = self.start_transaction_repository().await?;
        let saved = match tx_repo.update(aggregate).await {
            Ok(saved) => saved,
            Err(error) => {
                tx_repo.rollback_transaction_repository().await?;
                return Err(error);
            }
        };
        if let Err(error) = A::update_relations(&tx_repo, aggregate, mode).await {
            tx_repo.rollback_transaction_repository().await?;
            return Err(error);
        }
        tx_repo.commit_transaction_repository().await?;
        self.reload_all_relations(saved.id()).await
    }

    async fn save_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A> {
        let tx_repo = self.start_transaction_repository().await?;
        let saved = match tx_repo.save(aggregate).await {
            Ok(saved) => saved,
            Err(error) => {
                tx_repo.rollback_transaction_repository().await?;
                return Err(error);
            }
        };
        if let Err(error) = A::update_relations(&tx_repo, aggregate, mode).await {
            tx_repo.rollback_transaction_repository().await?;
            return Err(error);
        }
        tx_repo.commit_transaction_repository().await?;
        self.reload_all_relations(saved.id()).await
    }

    async fn delete_graph(&self, id: &A::Id) -> Result<bool> {
        let tx_repo = self.start_transaction_repository().await?;
        if let Err(error) = A::delete_relations(&tx_repo, id).await {
            tx_repo.rollback_transaction_repository().await?;
            return Err(error);
        }
        let deleted = match tx_repo.delete(id).await {
            Ok(deleted) => deleted,
            Err(error) => {
                tx_repo.rollback_transaction_repository().await?;
                return Err(error);
            }
        };
        tx_repo.commit_transaction_repository().await?;
        Ok(deleted)
    }
}

impl<T> MongoRepository<T>
where
    T: MongoAggregate,
    T::Id: MongoId,
{
    async fn reload_all_relations(&self, id: &T::Id) -> Result<T> {
        let includes = T::relations()
            .iter()
            .map(|relation| Include::new(relation.name))
            .collect::<Vec<_>>();
        self.find_graph_by_id(id, &includes).await
    }
}

async fn load_all_by_foreign_key<C, K>(
    repository: &MongoRepository<C>,
    field: &str,
    value: &K,
) -> Result<Vec<C>>
where
    C: MongoEntity,
    C::Id: MongoId,
    K: EntityId,
{
    let mut items = Vec::new();
    let mut after = None;

    loop {
        let page = repository
            .find_by_foreign_key(
                field,
                value,
                PageRequest::cursor(DEFAULT_RELATION_PAGE_LIMIT, after),
            )
            .await?;
        let next_cursor = page.next_cursor;
        items.extend(page.items);

        if next_cursor.is_none() {
            break;
        }
        after = next_cursor;
    }

    Ok(items)
}

async fn delete_missing_by_foreign_key<C, K>(
    repository: &MongoRepository<C>,
    field: &str,
    value: &K,
    children: &[C],
) -> Result<()>
where
    C: MongoEntity,
    C::Id: MongoId,
    K: EntityId,
{
    let mut filter = foreign_key_filter(field, value)?;

    if !children.is_empty() {
        let keep_values = children
            .iter()
            .map(|child| child.id().to_bson())
            .collect::<Result<Vec<_>>>()?;
        let mut not_in = Document::new();
        not_in.insert("$nin", Bson::Array(keep_values));
        filter.insert(C::id_field(), Bson::Document(not_in));
    }

    repository.delete_many_documents(filter).await?;
    Ok(())
}

async fn unlink_missing_by_foreign_key<C, K>(
    repository: &MongoRepository<C>,
    field: &str,
    value: &K,
    children: &[C],
) -> Result<()>
where
    C: MongoEntity,
    C::Id: MongoId,
    K: EntityId,
{
    let mut filter = foreign_key_filter(field, value)?;

    if !children.is_empty() {
        let keep_values = children
            .iter()
            .map(|child| child.id().to_bson())
            .collect::<Result<Vec<_>>>()?;
        let mut not_in = Document::new();
        not_in.insert("$nin", Bson::Array(keep_values));
        filter.insert(C::id_field(), Bson::Document(not_in));
    }

    unset_foreign_key(repository, filter, field).await
}

async fn unlink_all_by_foreign_key<C, K>(
    repository: &MongoRepository<C>,
    field: &str,
    value: &K,
) -> Result<()>
where
    C: MongoEntity,
    C::Id: MongoId,
    K: EntityId,
{
    let filter = foreign_key_filter(field, value)?;
    unset_foreign_key(repository, filter, field).await
}

async fn unset_foreign_key<C>(
    repository: &MongoRepository<C>,
    filter: Document,
    field: &str,
) -> Result<()>
where
    C: MongoEntity,
    C::Id: MongoId,
{
    let mut unset = Document::new();
    unset.insert(field, "");
    let mut update = Document::new();
    update.insert("$unset", Bson::Document(unset));
    repository.update_many_documents(filter, update).await?;
    Ok(())
}

async fn delete_many_in_collection(
    database: &Database,
    session: Option<&Arc<Mutex<ClientSession>>>,
    collection_name: &str,
    filter: Document,
) -> Result<mongodb::results::DeleteResult> {
    let collection = database.collection::<Document>(collection_name);
    if let Some(session) = session {
        let mut session = session.lock().await;
        collection
            .delete_many(filter)
            .session(&mut *session)
            .await
            .map_err(map_error)
    } else {
        collection.delete_many(filter).await.map_err(map_error)
    }
}

fn foreign_key_filter<K>(field: &str, value: &K) -> Result<Document>
where
    K: EntityId,
{
    let mut filter = Document::new();
    filter.insert(field, entity_id_to_bson(value)?);
    Ok(filter)
}

fn required_relation_part<'a>(
    metadata: &RelationMetadata,
    value: Option<&'a str>,
    name: &str,
) -> Result<&'a str> {
    value.ok_or_else(|| {
        RepositoryError::InvalidData(format!("relation {} is missing {name}", metadata.name))
    })
}

fn entity_id_to_bson<I: EntityId>(id: &I) -> Result<Bson> {
    let any = id as &dyn std::any::Any;
    if let Some(value) = any.downcast_ref::<crate::MongoObjectId>() {
        Ok(Bson::ObjectId(value.0))
    } else if let Some(value) = any.downcast_ref::<String>() {
        Ok(Bson::String(value.clone()))
    } else if let Some(value) = any.downcast_ref::<i16>() {
        Ok(Bson::Int32(i32::from(*value)))
    } else if let Some(value) = any.downcast_ref::<i32>() {
        Ok(Bson::Int32(*value))
    } else if let Some(value) = any.downcast_ref::<i64>() {
        Ok(Bson::Int64(*value))
    } else if let Some(value) = any.downcast_ref::<u16>() {
        Ok(Bson::Int32(i32::from(*value)))
    } else if let Some(value) = any.downcast_ref::<u32>() {
        Ok(Bson::Int64(i64::from(*value)))
    } else if let Some(value) = any.downcast_ref::<u64>() {
        Ok(Bson::Int64(i64::try_from(*value).map_err(|_| {
            RepositoryError::InvalidData(format!("u64 value exceeds i64 range: {value}"))
        })?))
    } else if let Some(value) = any.downcast_ref::<uuid::Uuid>() {
        Ok(Bson::String(value.to_string()))
    } else {
        Err(unsupported_id::<I>())
    }
}

fn bson_to_entity_id<I: EntityId>(value: Bson) -> Result<I> {
    let value: Box<dyn std::any::Any> = match value {
        Bson::ObjectId(value) => Box::new(crate::MongoObjectId(value)),
        Bson::String(value) => Box::new(value),
        Bson::Int32(value) => Box::new(value),
        Bson::Int64(value) => Box::new(value),
        other => {
            return Err(RepositoryError::Unsupported(format!(
                "unsupported generated Mongo id value: {other:?}"
            )));
        }
    };

    value.downcast::<I>().map(|value| *value).map_err(|_| {
        RepositoryError::Unsupported(format!(
            "generated Mongo id cannot be converted to {}",
            std::any::type_name::<I>()
        ))
    })
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

fn default_sort<T: MongoEntity>() -> Document {
    let mut sort = Document::new();
    sort.insert(T::id_field(), 1);
    sort
}

fn map_error(error: mongodb::error::Error) -> RepositoryError {
    RepositoryError::Backend(error.to_string())
}
