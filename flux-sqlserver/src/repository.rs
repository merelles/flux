use std::{any::Any, collections::HashSet, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use flux::{
    AggregateRepository, BulkRepository, CascadeAction, EntityId, GenericFilter, GraphSaveMode,
    Include, Page, PageRequest, ReadRepository, RelationMetadata, RelationRepository,
    RepositoryError, Result, WriteRepository,
};
use futures_util::io::{AsyncRead, AsyncWrite};
use tiberius::Client;
use tokio::sync::Mutex;

use crate::{filter::quote_path, render_filter, SqlServerAggregate, SqlServerEntity};

const DEFAULT_RELATION_PAGE_LIMIT: u32 = 512;
const SQLSERVER_MAX_BIND_PARAMS: usize = 2_000;

pub struct SqlServerRepository<T, S>
where
    T: SqlServerEntity,
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    client: Arc<Mutex<Client<S>>>,
    transaction_lock: Arc<Mutex<()>>,
    _marker: PhantomData<T>,
}

impl<T, S> Clone for SqlServerRepository<T, S>
where
    T: SqlServerEntity,
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            transaction_lock: Arc::clone(&self.transaction_lock),
            _marker: PhantomData,
        }
    }
}

impl<T, S> SqlServerRepository<T, S>
where
    T: SqlServerEntity,
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    pub fn new(client: Arc<Mutex<Client<S>>>) -> Self {
        Self {
            client,
            transaction_lock: Arc::new(Mutex::new(())),
            _marker: PhantomData,
        }
    }

    fn with_shared_state(client: Arc<Mutex<Client<S>>>, transaction_lock: Arc<Mutex<()>>) -> Self {
        Self {
            client,
            transaction_lock,
            _marker: PhantomData,
        }
    }

    pub fn client(&self) -> &Arc<Mutex<Client<S>>> {
        &self.client
    }

    pub async fn load_has_many<C, K>(
        &self,
        metadata: &RelationMetadata,
        source: &K,
    ) -> Result<Vec<C>>
    where
        C: SqlServerEntity,
        K: EntityId,
        S: Sync,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = SqlServerRepository::<C, S>::with_shared_state(
            Arc::clone(&self.client),
            Arc::clone(&self.transaction_lock),
        );
        load_all_by_foreign_key(&child_repo, foreign_key, source).await
    }

    pub async fn load_has_one<C, K>(
        &self,
        metadata: &RelationMetadata,
        source: &K,
    ) -> Result<Option<C>>
    where
        C: SqlServerEntity,
        K: EntityId,
        S: Sync,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = SqlServerRepository::<C, S>::with_shared_state(
            Arc::clone(&self.client),
            Arc::clone(&self.transaction_lock),
        );
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
        C: SqlServerEntity,
        K: EntityId,
        S: Sync,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = SqlServerRepository::<C, S>::with_shared_state(
            Arc::clone(&self.client),
            Arc::clone(&self.transaction_lock),
        );

        match mode {
            GraphSaveMode::AppendChildren => {
                child_repo.insert_many(children).await?;
            }
            GraphSaveMode::UpsertChildren => {
                child_repo.save_many(children).await?;
            }
            GraphSaveMode::ReplaceChildren => {
                match metadata.on_replace {
                    flux::OnReplace::KeepMissing => {}
                    flux::OnReplace::DeleteMissing => {
                        let delete_ids =
                            missing_child_ids(&child_repo, foreign_key, source, children).await?;
                        child_repo.delete_many(&delete_ids).await?;
                    }
                    flux::OnReplace::UnlinkMissing => {
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
        C: SqlServerEntity,
        K: EntityId,
        S: Sync,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = SqlServerRepository::<C, S>::with_shared_state(
            Arc::clone(&self.client),
            Arc::clone(&self.transaction_lock),
        );

        match (mode, child) {
            (GraphSaveMode::AppendChildren, Some(child)) => {
                child_repo.insert(child).await?;
            }
            (GraphSaveMode::UpsertChildren | GraphSaveMode::ReplaceChildren, Some(child)) => {
                child_repo.save(child).await?;
            }
            (GraphSaveMode::ReplaceChildren, None) => match metadata.on_replace {
                flux::OnReplace::KeepMissing => {}
                flux::OnReplace::DeleteMissing => {
                    child_repo
                        .delete_by_foreign_key(foreign_key, source)
                        .await?;
                }
                flux::OnReplace::UnlinkMissing => {
                    unlink_all_by_foreign_key(&child_repo, foreign_key, source).await?;
                }
            },
            _ => {}
        }

        Ok(())
    }

    pub async fn delete_relation<C, K>(&self, metadata: &RelationMetadata, source: &K) -> Result<()>
    where
        C: SqlServerEntity,
        K: EntityId,
        S: Sync,
    {
        if metadata.cascade != CascadeAction::Delete {
            return Ok(());
        }

        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = SqlServerRepository::<C, S>::with_shared_state(
            Arc::clone(&self.client),
            Arc::clone(&self.transaction_lock),
        );
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
        C: SqlServerEntity,
        K: EntityId,
    {
        let join_table = required_relation_part(metadata, metadata.join_table, "join_table")?;
        let source_key = required_relation_part(metadata, metadata.source_key, "source_key")?;
        let target_key = required_relation_part(metadata, metadata.target_key, "target_key")?;
        let target_primary_key = metadata.target_primary_key.unwrap_or_else(C::primary_key);

        let target_table = quote_path(C::table_name())?;
        let join_table = quote_path(join_table)?;
        let source_key = quote_path(source_key)?;
        let target_key = quote_path(target_key)?;
        let target_primary_key = quote_path(target_primary_key)?;
        let query = format!(
            "SELECT target.* FROM {target_table} AS target INNER JOIN {join_table} AS join_table ON target.{target_primary_key} = join_table.{target_key} WHERE join_table.{source_key} = @P1 ORDER BY target.{target_primary_key} ASC"
        );
        let mut params = Vec::new();
        push_entity_id_param(&mut params, source)?;
        let rows = self.query_owned(&query, &params).await?;
        rows.into_iter().map(C::from_row).collect()
    }

    pub async fn save_many_to_many<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
        mode: GraphSaveMode,
    ) -> Result<()>
    where
        C: SqlServerEntity,
        K: EntityId,
        S: Sync,
    {
        let child_repo = SqlServerRepository::<C, S>::with_shared_state(
            Arc::clone(&self.client),
            Arc::clone(&self.transaction_lock),
        );
        child_repo.save_many(children).await?;

        if mode == GraphSaveMode::ReplaceChildren
            && metadata.on_replace != flux::OnReplace::KeepMissing
        {
            self.delete_missing_many_to_many_links(metadata, children, source)
                .await?;
        }

        self.insert_many_to_many_links(metadata, children, source)
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
        let join_table = quote_path(required_relation_part(
            metadata,
            metadata.join_table,
            "join_table",
        )?)?;
        let source_key = quote_path(required_relation_part(
            metadata,
            metadata.source_key,
            "source_key",
        )?)?;
        let query = format!("DELETE FROM {join_table} WHERE {source_key} = @P1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, source)?;
        self.execute_owned(&query, &params).await?;
        Ok(())
    }

    async fn delete_missing_many_to_many_links<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
    ) -> Result<()>
    where
        C: SqlServerEntity,
        K: EntityId,
    {
        let join_table = quote_path(required_relation_part(
            metadata,
            metadata.join_table,
            "join_table",
        )?)?;
        let source_key = quote_path(required_relation_part(
            metadata,
            metadata.source_key,
            "source_key",
        )?)?;
        let target_key = quote_path(required_relation_part(
            metadata,
            metadata.target_key,
            "target_key",
        )?)?;

        let mut params = Vec::new();
        push_entity_id_param(&mut params, source)?;

        let query = if children.is_empty() {
            format!("DELETE FROM {join_table} WHERE {source_key} = @P1")
        } else {
            for child in children {
                push_entity_id_param(&mut params, child.id())?;
            }
            let placeholders = (2..=params.len())
                .map(|index| format!("@P{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "DELETE FROM {join_table} WHERE {source_key} = @P1 AND {target_key} NOT IN ({placeholders})"
            )
        };

        self.execute_owned(&query, &params).await?;
        Ok(())
    }

    async fn insert_many_to_many_links<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
    ) -> Result<()>
    where
        C: SqlServerEntity,
        K: EntityId,
    {
        if children.is_empty() {
            return Ok(());
        }

        let join_table = quote_path(required_relation_part(
            metadata,
            metadata.join_table,
            "join_table",
        )?)?;
        let source_key = quote_path(required_relation_part(
            metadata,
            metadata.source_key,
            "source_key",
        )?)?;
        let target_key = quote_path(required_relation_part(
            metadata,
            metadata.target_key,
            "target_key",
        )?)?;

        let values = values_clause(2, children.len(), 1);
        let query = format!(
            "MERGE INTO {join_table} AS target USING (VALUES {values}) AS source ({source_key}, {target_key}) ON target.{source_key} = source.{source_key} AND target.{target_key} = source.{target_key} WHEN NOT MATCHED THEN INSERT ({source_key}, {target_key}) VALUES (source.{source_key}, source.{target_key});"
        );
        let mut params = Vec::with_capacity(children.len() * 2);
        for child in children {
            push_entity_id_param(&mut params, source)?;
            push_entity_id_param(&mut params, child.id())?;
        }

        self.execute_owned(&query, &params).await?;
        Ok(())
    }

    async fn query_page(
        &self,
        filter: Option<GenericFilter<T>>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>> {
        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let mut where_parts = Vec::new();
        let mut owned_params: Vec<Box<dyn tiberius::ToSql>> = Vec::new();
        let mut order_by = None;

        if let Some(filter) = filter {
            let rendered = render_filter(&filter, owned_params.len() + 1)?;
            if let Some(where_clause) = rendered.where_clause {
                where_parts.push(where_clause);
            }
            order_by = rendered.order_by;
            owned_params.extend(rendered.params);
        }

        let total = self
            .query_count(&table, &where_parts, &owned_params)
            .await?;
        let limit = page.limit();

        match page {
            PageRequest::Offset { offset, .. } => {
                owned_params.push(Box::new(i64::try_from(offset).map_err(|_| {
                    RepositoryError::InvalidData(format!("offset exceeds i64 range: {offset}"))
                })?));
                let offset_placeholder = format!("@P{}", owned_params.len());
                owned_params.push(Box::new(i64::from(limit)));
                let limit_placeholder = format!("@P{}", owned_params.len());
                let query = build_select_query(
                    &table,
                    &where_parts,
                    order_by
                        .unwrap_or_else(|| format!("{primary_key} ASC"))
                        .as_str(),
                    &offset_placeholder,
                    &limit_placeholder,
                );
                let rows = self.query_owned(&query, &owned_params).await?;
                page_from_rows::<T>(rows, limit, Some(total))
            }
            PageRequest::Cursor { after, .. } => {
                if let Some(after) = after {
                    push_entity_id_param(&mut owned_params, &after)?;
                    where_parts.push(format!("{primary_key} > @P{}", owned_params.len()));
                }
                owned_params.push(Box::new(i64::from(limit)));
                let limit_placeholder = format!("@P{}", owned_params.len());
                let query = build_select_query(
                    &table,
                    &where_parts,
                    order_by
                        .unwrap_or_else(|| format!("{primary_key} ASC"))
                        .as_str(),
                    "0",
                    &limit_placeholder,
                );
                let rows = self.query_owned(&query, &owned_params).await?;
                page_from_rows::<T>(rows, limit, Some(total))
            }
        }
    }

    async fn query_owned(
        &self,
        query: &str,
        params: &[Box<dyn tiberius::ToSql>],
    ) -> Result<Vec<tiberius::Row>> {
        let refs = params
            .iter()
            .map(|param| param.as_ref() as &dyn tiberius::ToSql)
            .collect::<Vec<_>>();
        let mut client = self.client.lock().await;
        let stream = client
            .query(query, refs.as_slice())
            .await
            .map_err(map_error)?;
        let rows = stream.into_first_result().await.map_err(map_error)?;
        Ok(rows)
    }

    async fn query_params(
        &self,
        query: &str,
        params: &[&dyn tiberius::ToSql],
    ) -> Result<Vec<tiberius::Row>> {
        let mut client = self.client.lock().await;
        let stream = client.query(query, params).await.map_err(map_error)?;
        let rows = stream.into_first_result().await.map_err(map_error)?;
        Ok(rows)
    }

    async fn execute_owned(&self, query: &str, params: &[Box<dyn tiberius::ToSql>]) -> Result<u64> {
        let refs = params
            .iter()
            .map(|param| param.as_ref() as &dyn tiberius::ToSql)
            .collect::<Vec<_>>();
        let mut client = self.client.lock().await;
        let result = client
            .execute(query, refs.as_slice())
            .await
            .map_err(map_error)?;
        Ok(result.total())
    }

    async fn query_count(
        &self,
        table: &str,
        where_parts: &[String],
        params: &[Box<dyn tiberius::ToSql>],
    ) -> Result<u64> {
        let where_clause = if where_parts.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_parts.join(" AND "))
        };
        let query = format!("SELECT COUNT_BIG(*) AS count FROM {table}{where_clause}");
        let rows = self.query_owned(&query, params).await?;
        let row = rows.into_iter().next().ok_or_else(|| {
            RepositoryError::OperationFailed("count query returned no rows".to_string())
        })?;
        let count = row
            .get::<i64, _>("count")
            .ok_or_else(|| RepositoryError::InvalidData("invalid count result".to_string()))?;
        Ok(count as u64)
    }

    async fn execute_simple(&self, query: &str) -> Result<()> {
        let mut client = self.client.lock().await;
        client
            .simple_query(query)
            .await
            .map_err(map_error)?
            .into_results()
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn begin_transaction(&self) -> Result<()> {
        self.execute_simple("BEGIN TRANSACTION").await
    }

    async fn commit_transaction(&self) -> Result<()> {
        self.execute_simple("COMMIT TRANSACTION").await
    }

    async fn rollback_transaction(&self) -> Result<()> {
        self.execute_simple("ROLLBACK TRANSACTION").await
    }

    async fn reload_all_relations(&self, id: &T::Id) -> Result<T>
    where
        T: SqlServerAggregate,
        S: Sync,
    {
        let mut aggregate = self.find_by_id(id).await?;
        let includes = T::relations()
            .iter()
            .map(|relation| Include::new(relation.name))
            .collect::<Vec<_>>();
        T::load_relations(self, &mut aggregate, &includes).await?;
        Ok(aggregate)
    }
}

#[async_trait]
impl<T, S> ReadRepository<T> for SqlServerRepository<T, S>
where
    T: SqlServerEntity,
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    async fn find_by_id(&self, id: &T::Id) -> Result<T> {
        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let query = format!("SELECT TOP (1) * FROM {table} WHERE {primary_key} = @P1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, id)?;
        let rows = self.query_owned(&query, &params).await?;
        rows.into_iter()
            .next()
            .map(T::from_row)
            .transpose()?
            .ok_or(RepositoryError::NotFound)
    }

    async fn find_page(&self, page: PageRequest<T::Id>) -> Result<Page<T, T::Id>> {
        self.query_page(None, page).await
    }

    async fn find_page_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>> {
        self.query_page(Some(filter), page).await
    }

    async fn find_all_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>> {
        self.find_page_with_filter(filter, page).await
    }

    async fn exists(&self, id: &T::Id) -> Result<bool> {
        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let query =
            format!("SELECT TOP (1) 1 AS exists_flag FROM {table} WHERE {primary_key} = @P1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, id)?;
        Ok(!self.query_owned(&query, &params).await?.is_empty())
    }

    async fn count(&self) -> Result<u64> {
        let table = quote_path(T::table_name())?;
        self.query_count(&table, &[], &[]).await
    }
}

#[async_trait]
impl<T, S> WriteRepository<T> for SqlServerRepository<T, S>
where
    T: SqlServerEntity,
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    async fn insert(&self, entity: &T) -> Result<T> {
        self.insert_many(std::slice::from_ref(entity))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| RepositoryError::OperationFailed("insert returned no rows".to_string()))
    }

    async fn update(&self, entity: &T) -> Result<T> {
        self.update_many(std::slice::from_ref(entity))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| RepositoryError::OperationFailed("update returned no rows".to_string()))
    }

    async fn save(&self, entity: &T) -> Result<T> {
        self.save_many(std::slice::from_ref(entity))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| RepositoryError::OperationFailed("save returned no rows".to_string()))
    }

    async fn delete(&self, id: &T::Id) -> Result<bool> {
        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let query = format!("DELETE FROM {table} WHERE {primary_key} = @P1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, id)?;
        Ok(self.execute_owned(&query, &params).await? > 0)
    }
}

#[async_trait]
impl<T, S> BulkRepository<T> for SqlServerRepository<T, S>
where
    T: SqlServerEntity,
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    async fn insert_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let table = quote_path(T::table_name())?;
        let fields = T::fields();
        let field_list = quoted_fields(fields)?;
        let mut saved = Vec::with_capacity(entities.len());

        for chunk in entities.chunks(chunk_size(fields.len())) {
            let values = values_clause(fields.len(), chunk.len(), 1);
            let query =
                format!("INSERT INTO {table} ({field_list}) OUTPUT INSERTED.* VALUES {values}");
            let params = collect_insert_params::<T>(chunk)?;
            let rows = self.query_params(&query, params.as_slice()).await?;
            saved.extend(
                rows.into_iter()
                    .map(T::from_row)
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        Ok(saved)
    }

    async fn update_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let fields = T::fields();
        let source_columns = quoted_fields(fields)?;
        let update_fields = update_fields::<T>()?;
        let set_clause = update_fields
            .iter()
            .map(|field| {
                Ok(format!(
                    "target.{field} = source.{field}",
                    field = quote_path(field)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let mut saved = Vec::with_capacity(entities.len());

        for chunk in entities.chunks(chunk_size(fields.len())) {
            let values = values_clause(fields.len(), chunk.len(), 1);
            let query = format!(
                "UPDATE target SET {set_clause} OUTPUT INSERTED.* FROM {table} AS target INNER JOIN (VALUES {values}) AS source ({source_columns}) ON target.{primary_key} = source.{primary_key}"
            );
            let params = collect_insert_params::<T>(chunk)?;
            let rows = self.query_params(&query, params.as_slice()).await?;
            saved.extend(
                rows.into_iter()
                    .map(T::from_row)
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        Ok(saved)
    }

    async fn save_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let fields = T::fields();
        let field_list = quoted_fields(fields)?;
        let source_columns = quoted_fields(fields)?;
        let update_fields = update_fields::<T>()?;
        let update_clause = update_fields
            .iter()
            .map(|field| {
                Ok(format!(
                    "target.{field} = source.{field}",
                    field = quote_path(field)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let insert_values = fields
            .iter()
            .map(|field| Ok(format!("source.{}", quote_path(field)?)))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let mut saved = Vec::with_capacity(entities.len());

        for chunk in entities.chunks(chunk_size(fields.len())) {
            let values = values_clause(fields.len(), chunk.len(), 1);
            let query = format!(
                "MERGE INTO {table} AS target USING (VALUES {values}) AS source ({source_columns}) ON target.{primary_key} = source.{primary_key} WHEN MATCHED THEN UPDATE SET {update_clause} WHEN NOT MATCHED THEN INSERT ({field_list}) VALUES ({insert_values}) OUTPUT INSERTED.*;"
            );
            let params = collect_insert_params::<T>(chunk)?;
            let rows = self.query_params(&query, params.as_slice()).await?;
            saved.extend(
                rows.into_iter()
                    .map(T::from_row)
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        Ok(saved)
    }

    async fn delete_many(&self, ids: &[T::Id]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let mut affected = 0;

        for chunk in ids.chunks(SQLSERVER_MAX_BIND_PARAMS) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("@P{index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!("DELETE FROM {table} WHERE {primary_key} IN ({placeholders})");
            let mut params = Vec::new();
            for id in chunk {
                push_entity_id_param(&mut params, id)?;
            }
            affected += self.execute_owned(&query, &params).await?;
        }

        Ok(affected)
    }
}

#[async_trait]
impl<T, S> RelationRepository<T> for SqlServerRepository<T, S>
where
    T: SqlServerEntity,
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
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
        let filter = GenericFilter::<T>::new().eq(field, entity_id_to_filter_value(value)?);
        self.find_page_with_filter(filter, page).await
    }

    async fn delete_by_foreign_key<K>(&self, field: &str, value: &K) -> Result<u64>
    where
        K: EntityId,
    {
        let table = quote_path(T::table_name())?;
        let field = quote_path(field)?;
        let query = format!("DELETE FROM {table} WHERE {field} = @P1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, value)?;
        self.execute_owned(&query, &params).await
    }
}

#[async_trait]
impl<A, S> AggregateRepository<A> for SqlServerRepository<A, S>
where
    A: SqlServerAggregate,
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    async fn find_graph_by_id(&self, id: &A::Id, includes: &[Include<A>]) -> Result<A> {
        let mut aggregate = self.find_by_id(id).await?;
        A::load_relations(self, &mut aggregate, includes).await?;
        Ok(aggregate)
    }

    async fn insert_graph(&self, aggregate: &A) -> Result<A> {
        let _transaction_guard = self.transaction_lock.lock().await;
        self.begin_transaction().await?;

        let saved = match self.insert(aggregate).await {
            Ok(saved) => saved,
            Err(error) => {
                self.rollback_transaction().await?;
                return Err(error);
            }
        };

        if let Err(error) = A::insert_relations(self, aggregate).await {
            self.rollback_transaction().await?;
            return Err(error);
        }

        self.commit_transaction().await?;
        self.reload_all_relations(saved.id()).await
    }

    async fn update_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A> {
        let _transaction_guard = self.transaction_lock.lock().await;
        self.begin_transaction().await?;

        let saved = match self.update(aggregate).await {
            Ok(saved) => saved,
            Err(error) => {
                self.rollback_transaction().await?;
                return Err(error);
            }
        };

        if let Err(error) = A::update_relations(self, aggregate, mode).await {
            self.rollback_transaction().await?;
            return Err(error);
        }

        self.commit_transaction().await?;
        self.reload_all_relations(saved.id()).await
    }

    async fn save_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A> {
        let _transaction_guard = self.transaction_lock.lock().await;
        self.begin_transaction().await?;

        let saved = match self.save(aggregate).await {
            Ok(saved) => saved,
            Err(error) => {
                self.rollback_transaction().await?;
                return Err(error);
            }
        };

        if let Err(error) = A::update_relations(self, aggregate, mode).await {
            self.rollback_transaction().await?;
            return Err(error);
        }

        self.commit_transaction().await?;
        self.reload_all_relations(saved.id()).await
    }

    async fn delete_graph(&self, id: &A::Id) -> Result<bool> {
        let _transaction_guard = self.transaction_lock.lock().await;
        self.begin_transaction().await?;

        if let Err(error) = A::delete_relations(self, id).await {
            self.rollback_transaction().await?;
            return Err(error);
        }

        let deleted = match self.delete(id).await {
            Ok(deleted) => deleted,
            Err(error) => {
                self.rollback_transaction().await?;
                return Err(error);
            }
        };

        self.commit_transaction().await?;
        Ok(deleted)
    }
}

fn build_select_query(
    table: &str,
    where_parts: &[String],
    order_by: &str,
    offset_placeholder: &str,
    limit_placeholder: &str,
) -> String {
    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };
    format!(
        "SELECT * FROM {table}{where_clause} ORDER BY {order_by} OFFSET {offset_placeholder} ROWS FETCH NEXT {limit_placeholder} ROWS ONLY"
    )
}

async fn load_all_by_foreign_key<C, K, S>(
    repository: &SqlServerRepository<C, S>,
    field: &str,
    value: &K,
) -> Result<Vec<C>>
where
    C: SqlServerEntity,
    K: EntityId,
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
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

async fn missing_child_ids<C, K, S>(
    repository: &SqlServerRepository<C, S>,
    field: &str,
    value: &K,
    children: &[C],
) -> Result<Vec<C::Id>>
where
    C: SqlServerEntity,
    K: EntityId,
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    let existing = load_all_by_foreign_key(repository, field, value).await?;
    let current_ids = children
        .iter()
        .map(|child| child.id().clone())
        .collect::<HashSet<_>>();
    Ok(existing
        .iter()
        .filter(|child| !current_ids.contains(child.id()))
        .map(|child| child.id().clone())
        .collect())
}

async fn unlink_missing_by_foreign_key<C, K, S>(
    repository: &SqlServerRepository<C, S>,
    field: &str,
    value: &K,
    children: &[C],
) -> Result<()>
where
    C: SqlServerEntity,
    K: EntityId,
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = quote_path(C::table_name())?;
    let primary_key = quote_path(C::primary_key())?;
    let field = quote_path(field)?;
    let mut params = Vec::new();
    push_entity_id_param(&mut params, value)?;

    let query = if children.is_empty() {
        format!("UPDATE {table} SET {field} = NULL WHERE {field} = @P1")
    } else {
        for child in children {
            push_entity_id_param(&mut params, child.id())?;
        }
        let placeholders = (2..=params.len())
            .map(|index| format!("@P{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "UPDATE {table} SET {field} = NULL WHERE {field} = @P1 AND {primary_key} NOT IN ({placeholders})"
        )
    };

    repository.execute_owned(&query, &params).await?;
    Ok(())
}

async fn unlink_all_by_foreign_key<C, K, S>(
    repository: &SqlServerRepository<C, S>,
    field: &str,
    value: &K,
) -> Result<()>
where
    C: SqlServerEntity,
    K: EntityId,
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let table = quote_path(C::table_name())?;
    let field = quote_path(field)?;
    let query = format!("UPDATE {table} SET {field} = NULL WHERE {field} = @P1");
    let mut params = Vec::new();
    push_entity_id_param(&mut params, value)?;
    repository.execute_owned(&query, &params).await?;
    Ok(())
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

fn page_from_rows<T: SqlServerEntity>(
    rows: Vec<tiberius::Row>,
    limit: u32,
    total: Option<u64>,
) -> Result<Page<T, T::Id>> {
    let items = rows
        .into_iter()
        .map(T::from_row)
        .collect::<Result<Vec<_>>>()?;
    let next_cursor = if items.len() == limit as usize {
        items.last().map(|item| item.id().clone())
    } else {
        None
    };
    Ok(Page::new(items, limit, next_cursor, total))
}

fn collect_insert_params<T: SqlServerEntity>(entities: &[T]) -> Result<Vec<&dyn tiberius::ToSql>> {
    let fields_len = T::fields().len();
    let mut params = Vec::with_capacity(fields_len * entities.len());
    for entity in entities {
        let entity_params = entity.to_insert_params();
        if entity_params.len() != fields_len {
            return Err(RepositoryError::InvalidData(format!(
                "expected {fields_len} insert params, got {}",
                entity_params.len()
            )));
        }
        params.extend(entity_params);
    }
    Ok(params)
}

fn chunk_size(field_count: usize) -> usize {
    let field_count = field_count.max(1);
    (SQLSERVER_MAX_BIND_PARAMS / field_count).max(1)
}

fn values_clause(field_count: usize, entity_count: usize, start_index: usize) -> String {
    let mut param_index = start_index;
    (0..entity_count)
        .map(|_| {
            let placeholders = (0..field_count)
                .map(|_| {
                    let placeholder = format!("@P{param_index}");
                    param_index += 1;
                    placeholder
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("({placeholders})")
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn quoted_fields(fields: &[&str]) -> Result<String> {
    fields
        .iter()
        .map(|field| quote_path(field))
        .collect::<Result<Vec<_>>>()
        .map(|fields| fields.join(", "))
}

fn update_fields<T: SqlServerEntity>() -> Result<Vec<&'static str>> {
    let update_fields = T::fields()
        .iter()
        .copied()
        .filter(|field| *field != T::primary_key())
        .collect::<Vec<_>>();

    if update_fields.is_empty() {
        return Err(RepositoryError::InvalidData(
            "entity has no fields to update".to_string(),
        ));
    }

    Ok(update_fields)
}

fn push_entity_id_param<I: EntityId>(
    params: &mut Vec<Box<dyn tiberius::ToSql>>,
    id: &I,
) -> Result<()> {
    let any = id as &dyn Any;
    if let Some(value) = any.downcast_ref::<uuid::Uuid>() {
        params.push(Box::new(*value));
    } else if let Some(value) = any.downcast_ref::<String>() {
        params.push(Box::new(value.clone()));
    } else if let Some(value) = any.downcast_ref::<i16>() {
        params.push(Box::new(*value));
    } else if let Some(value) = any.downcast_ref::<i32>() {
        params.push(Box::new(*value));
    } else if let Some(value) = any.downcast_ref::<i64>() {
        params.push(Box::new(*value));
    } else if let Some(value) = any.downcast_ref::<u16>() {
        params.push(Box::new(i32::from(*value)));
    } else if let Some(value) = any.downcast_ref::<u32>() {
        params.push(Box::new(i64::from(*value)));
    } else if let Some(value) = any.downcast_ref::<u64>() {
        params.push(Box::new(i64::try_from(*value).map_err(|_| {
            RepositoryError::InvalidData(format!("u64 value exceeds i64 range: {value}"))
        })?));
    } else {
        return Err(RepositoryError::Unsupported(format!(
            "unsupported SQL Server id type: {}",
            std::any::type_name::<I>()
        )));
    }
    Ok(())
}

fn entity_id_to_filter_value<I: EntityId>(id: &I) -> Result<flux::FilterValue> {
    let any = id as &dyn Any;
    if let Some(value) = any.downcast_ref::<uuid::Uuid>() {
        Ok(flux::FilterValue::Uuid(*value))
    } else if let Some(value) = any.downcast_ref::<String>() {
        Ok(flux::FilterValue::String(value.clone()))
    } else if let Some(value) = any.downcast_ref::<i16>() {
        Ok(flux::FilterValue::I16(*value))
    } else if let Some(value) = any.downcast_ref::<i32>() {
        Ok(flux::FilterValue::I32(*value))
    } else if let Some(value) = any.downcast_ref::<i64>() {
        Ok(flux::FilterValue::I64(*value))
    } else if let Some(value) = any.downcast_ref::<u16>() {
        Ok(flux::FilterValue::U16(*value))
    } else if let Some(value) = any.downcast_ref::<u32>() {
        Ok(flux::FilterValue::U32(*value))
    } else if let Some(value) = any.downcast_ref::<u64>() {
        Ok(flux::FilterValue::U64(*value))
    } else {
        Err(RepositoryError::Unsupported(format!(
            "unsupported SQL Server id type: {}",
            std::any::type_name::<I>()
        )))
    }
}

fn map_error(error: tiberius::error::Error) -> RepositoryError {
    RepositoryError::Backend(error.to_string())
}
