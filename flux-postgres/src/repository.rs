use std::{any::Any, collections::HashSet, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use flux::{
    AggregateRepository, BulkRepository, CascadeAction, EntityId, GenericFilter, GraphSaveMode,
    Include, Page, PageRequest, ReadRepository, RelationMetadata, RelationRepository,
    RepositoryError, Result, WriteRepository,
};
use tokio::sync::Mutex;
use tokio_postgres::{types::ToSql, Client};

use crate::{filter::quote_path, render_filter, PostgresAggregate, SqlEntity};

const DEFAULT_RELATION_PAGE_LIMIT: u32 = 512;
const POSTGRES_MAX_BIND_PARAMS: usize = 60_000;

pub struct PostgresRepository<T: SqlEntity> {
    client: Arc<Client>,
    transaction_lock: Arc<Mutex<()>>,
    _marker: PhantomData<T>,
}

impl<T: SqlEntity> Clone for PostgresRepository<T> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            transaction_lock: Arc::clone(&self.transaction_lock),
            _marker: PhantomData,
        }
    }
}

impl<T: SqlEntity> PostgresRepository<T> {
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            transaction_lock: Arc::new(Mutex::new(())),
            _marker: PhantomData,
        }
    }

    fn with_shared_state(client: Arc<Client>, transaction_lock: Arc<Mutex<()>>) -> Self {
        Self {
            client,
            transaction_lock,
            _marker: PhantomData,
        }
    }

    pub fn client(&self) -> &Arc<Client> {
        &self.client
    }

    pub async fn load_has_many<C, K>(
        &self,
        metadata: &RelationMetadata,
        source: &K,
    ) -> Result<Vec<C>>
    where
        C: SqlEntity,
        K: EntityId,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = PostgresRepository::<C>::with_shared_state(
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
        C: SqlEntity,
        K: EntityId,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = PostgresRepository::<C>::with_shared_state(
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
        C: SqlEntity,
        K: EntityId,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = PostgresRepository::<C>::with_shared_state(
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
        C: SqlEntity,
        K: EntityId,
    {
        let foreign_key = metadata.foreign_key.ok_or_else(|| {
            RepositoryError::InvalidData(format!(
                "relation {} is missing foreign_key",
                metadata.name
            ))
        })?;
        let child_repo = PostgresRepository::<C>::with_shared_state(
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
        C: SqlEntity,
        K: EntityId,
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
        let child_repo = PostgresRepository::<C>::with_shared_state(
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
        C: SqlEntity,
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
            "SELECT target.* FROM {target_table} AS target INNER JOIN {join_table} AS join_table ON target.{target_primary_key} = join_table.{target_key} WHERE join_table.{source_key} = $1 ORDER BY target.{target_primary_key} ASC"
        );
        let mut params = Vec::new();
        push_entity_id_param(&mut params, source)?;
        let rows = self
            .client
            .query(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)?;
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
        C: SqlEntity,
        K: EntityId,
    {
        let child_repo = PostgresRepository::<C>::with_shared_state(
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
        let query = format!("DELETE FROM {join_table} WHERE {source_key} = $1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, source)?;
        self.client
            .execute(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn delete_missing_many_to_many_links<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
    ) -> Result<()>
    where
        C: SqlEntity,
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
            format!("DELETE FROM {join_table} WHERE {source_key} = $1")
        } else {
            for child in children {
                push_entity_id_param(&mut params, child.id())?;
            }
            let placeholders = (2..=params.len())
                .map(|index| format!("${index}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "DELETE FROM {join_table} WHERE {source_key} = $1 AND {target_key} NOT IN ({placeholders})"
            )
        };

        self.client
            .execute(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)?;
        Ok(())
    }

    async fn insert_many_to_many_links<C, K>(
        &self,
        metadata: &RelationMetadata,
        children: &[C],
        source: &K,
    ) -> Result<()>
    where
        C: SqlEntity,
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
            "INSERT INTO {join_table} ({source_key}, {target_key}) VALUES {values} ON CONFLICT DO NOTHING"
        );
        let mut params = Vec::with_capacity(children.len() * 2);
        for child in children {
            push_entity_id_param(&mut params, source)?;
            push_entity_id_param(&mut params, child.id())?;
        }

        self.client
            .execute(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)?;
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
        let mut owned_params: Vec<Box<dyn ToSql + Sync + Send>> = Vec::new();
        let mut order_by = None;

        if let Some(filter) = filter {
            let rendered = render_filter(&filter, owned_params.len() + 1)?;
            if let Some(where_clause) = rendered.where_clause {
                where_parts.push(where_clause);
            }
            order_by = rendered.order_by;
            owned_params.extend(rendered.params);
        }

        let limit = page.limit();
        let total = self
            .query_count(&table, &where_parts, &owned_params)
            .await?;

        match page {
            PageRequest::Offset { offset, .. } => {
                owned_params.push(Box::new(i64::from(limit)));
                let limit_placeholder = format!("${}", owned_params.len());
                owned_params.push(Box::new(i64::try_from(offset).map_err(|_| {
                    RepositoryError::InvalidData(format!("offset exceeds i64 range: {offset}"))
                })?));
                let offset_placeholder = format!("${}", owned_params.len());
                let query = build_select_query(
                    &table,
                    &where_parts,
                    order_by
                        .unwrap_or_else(|| format!("{primary_key} ASC"))
                        .as_str(),
                    &limit_placeholder,
                    Some(&offset_placeholder),
                );
                let rows = self.query_owned(&query, &owned_params).await?;
                page_from_rows::<T>(rows, limit, Some(total))
            }
            PageRequest::Cursor { after, .. } => {
                if let Some(after) = after {
                    push_entity_id_param(&mut owned_params, &after)?;
                    where_parts.push(format!("{primary_key} > ${}", owned_params.len()));
                }
                owned_params.push(Box::new(i64::from(limit)));
                let limit_placeholder = format!("${}", owned_params.len());
                let query = build_select_query(
                    &table,
                    &where_parts,
                    order_by
                        .unwrap_or_else(|| format!("{primary_key} ASC"))
                        .as_str(),
                    &limit_placeholder,
                    None,
                );
                let rows = self.query_owned(&query, &owned_params).await?;
                page_from_rows::<T>(rows, limit, Some(total))
            }
        }
    }

    async fn begin_transaction(&self) -> Result<()> {
        self.client.batch_execute("BEGIN").await.map_err(map_error)
    }

    async fn commit_transaction(&self) -> Result<()> {
        self.client.batch_execute("COMMIT").await.map_err(map_error)
    }

    async fn rollback_transaction(&self) -> Result<()> {
        self.client
            .batch_execute("ROLLBACK")
            .await
            .map_err(map_error)
    }

    async fn reload_all_relations(&self, id: &T::Id) -> Result<T>
    where
        T: PostgresAggregate,
    {
        let includes = T::relations()
            .iter()
            .map(|relation| Include::new(relation.name))
            .collect::<Vec<_>>();
        self.find_graph_by_id(id, &includes).await
    }

    async fn query_owned(
        &self,
        query: &str,
        params: &[Box<dyn ToSql + Sync + Send>],
    ) -> Result<Vec<tokio_postgres::Row>> {
        let refs = params
            .iter()
            .map(|param| param.as_ref() as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        self.client
            .query(query, refs.as_slice())
            .await
            .map_err(map_error)
    }

    async fn query_count(
        &self,
        table: &str,
        where_parts: &[String],
        params: &[Box<dyn ToSql + Sync + Send>],
    ) -> Result<u64> {
        let where_clause = if where_parts.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", where_parts.join(" AND "))
        };
        let query = format!("SELECT COUNT(*) FROM {table}{where_clause}");
        let row = self
            .client
            .query_one(&query, owned_refs(params).as_slice())
            .await
            .map_err(map_error)?;
        let count: i64 = row.get(0);
        Ok(count as u64)
    }
}

#[async_trait]
impl<T> ReadRepository<T> for PostgresRepository<T>
where
    T: SqlEntity,
{
    async fn find_by_id(&self, id: &T::Id) -> Result<T> {
        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let query = format!("SELECT * FROM {table} WHERE {primary_key} = $1 LIMIT 1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, id)?;
        let row = self
            .client
            .query_opt(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)?;

        row.map(T::from_row)
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
        let query = format!("SELECT 1 FROM {table} WHERE {primary_key} = $1 LIMIT 1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, id)?;
        let row = self
            .client
            .query_opt(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)?;
        Ok(row.is_some())
    }

    async fn count(&self) -> Result<u64> {
        let table = quote_path(T::table_name())?;
        let query = format!("SELECT COUNT(*) FROM {table}");
        let row = self
            .client
            .query_one(&query, &[])
            .await
            .map_err(map_error)?;
        let count: i64 = row.get(0);
        Ok(count as u64)
    }
}

#[async_trait]
impl<T> WriteRepository<T> for PostgresRepository<T>
where
    T: SqlEntity,
{
    async fn insert(&self, entity: &T) -> Result<T> {
        self.insert_many(std::slice::from_ref(entity))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| RepositoryError::OperationFailed("insert returned no rows".to_string()))
    }

    async fn update(&self, entity: &T) -> Result<T> {
        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let fields = T::fields();
        let update_fields = update_fields::<T>()?;
        let set_clause = update_fields
            .iter()
            .enumerate()
            .map(|(index, field)| Ok(format!("{} = ${}", quote_path(field)?, index + 1)))
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let query = format!(
            "UPDATE {table} SET {set_clause} WHERE {primary_key} = ${} RETURNING *",
            update_fields.len() + 1
        );

        let mut params = entity.to_update_params();
        if params.len() != fields.len() - 1 {
            return Err(RepositoryError::InvalidData(format!(
                "expected {} update params, got {}",
                fields.len() - 1,
                params.len()
            )));
        }
        params.push(entity.primary_key_param());
        let row = self
            .client
            .query_one(&query, params.as_slice())
            .await
            .map_err(map_error)?;
        T::from_row(row)
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
        let query = format!("DELETE FROM {table} WHERE {primary_key} = $1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, id)?;
        let affected = self
            .client
            .execute(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)?;
        Ok(affected > 0)
    }
}

#[async_trait]
impl<T> BulkRepository<T> for PostgresRepository<T>
where
    T: SqlEntity,
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
            let values_clause = values_clause(fields.len(), chunk.len(), 1);
            let query =
                format!("INSERT INTO {table} ({field_list}) VALUES {values_clause} RETURNING *");
            let params = collect_insert_params::<T>(chunk)?;
            let rows = self
                .client
                .query(&query, params.as_slice())
                .await
                .map_err(map_error)?;
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
        let update_fields = update_fields::<T>()?;
        let source_columns = quoted_fields(fields)?;
        let set_clause = update_fields
            .iter()
            .map(|field| {
                Ok(format!(
                    "{field} = source.{field}",
                    field = quote_path(field)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let mut saved = Vec::with_capacity(entities.len());

        for chunk in entities.chunks(chunk_size(fields.len())) {
            let values_clause = values_clause(fields.len(), chunk.len(), 1);
            let query = format!(
                "UPDATE {table} AS target SET {set_clause} FROM (VALUES {values_clause}) AS source ({source_columns}) WHERE target.{primary_key} = source.{primary_key} RETURNING target.*"
            );
            let params = collect_insert_params::<T>(chunk)?;
            let rows = self
                .client
                .query(&query, params.as_slice())
                .await
                .map_err(map_error)?;
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
        let update_fields = update_fields::<T>()?;
        let update_clause = update_fields
            .iter()
            .map(|field| {
                Ok(format!(
                    "{field} = EXCLUDED.{field}",
                    field = quote_path(field)?
                ))
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let mut saved = Vec::with_capacity(entities.len());

        for chunk in entities.chunks(chunk_size(fields.len())) {
            let values_clause = values_clause(fields.len(), chunk.len(), 1);
            let query = format!(
                "INSERT INTO {table} ({field_list}) VALUES {values_clause} ON CONFLICT ({primary_key}) DO UPDATE SET {update_clause} RETURNING *"
            );
            let params = collect_insert_params::<T>(chunk)?;
            let rows = self
                .client
                .query(&query, params.as_slice())
                .await
                .map_err(map_error)?;
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

        for chunk in ids.chunks(POSTGRES_MAX_BIND_PARAMS) {
            let placeholders = (1..=chunk.len())
                .map(|index| format!("${index}"))
                .collect::<Vec<_>>()
                .join(", ");
            let query = format!("DELETE FROM {table} WHERE {primary_key} IN ({placeholders})");
            let mut params = Vec::new();
            for id in chunk {
                push_entity_id_param(&mut params, id)?;
            }
            affected += self
                .client
                .execute(&query, owned_refs(&params).as_slice())
                .await
                .map_err(map_error)?;
        }

        Ok(affected)
    }
}

#[async_trait]
impl<T> RelationRepository<T> for PostgresRepository<T>
where
    T: SqlEntity,
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
        let query = format!("DELETE FROM {table} WHERE {field} = $1");
        let mut params = Vec::new();
        push_entity_id_param(&mut params, value)?;
        self.client
            .execute(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)
    }
}

#[async_trait]
impl<A> AggregateRepository<A> for PostgresRepository<A>
where
    A: PostgresAggregate,
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
    limit_placeholder: &str,
    offset_placeholder: Option<&str>,
) -> String {
    let where_clause = if where_parts.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_parts.join(" AND "))
    };
    let offset_clause = offset_placeholder
        .map(|placeholder| format!(" OFFSET {placeholder}"))
        .unwrap_or_default();
    format!("SELECT * FROM {table}{where_clause} ORDER BY {order_by} LIMIT {limit_placeholder}{offset_clause}")
}

async fn load_all_by_foreign_key<C, K>(
    repository: &PostgresRepository<C>,
    field: &str,
    value: &K,
) -> Result<Vec<C>>
where
    C: SqlEntity,
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

async fn missing_child_ids<C, K>(
    repository: &PostgresRepository<C>,
    field: &str,
    value: &K,
    children: &[C],
) -> Result<Vec<C::Id>>
where
    C: SqlEntity,
    K: EntityId,
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

async fn unlink_missing_by_foreign_key<C, K>(
    repository: &PostgresRepository<C>,
    field: &str,
    value: &K,
    children: &[C],
) -> Result<()>
where
    C: SqlEntity,
    K: EntityId,
{
    let table = quote_path(C::table_name())?;
    let primary_key = quote_path(C::primary_key())?;
    let field = quote_path(field)?;
    let mut params = Vec::new();
    push_entity_id_param(&mut params, value)?;

    let query = if children.is_empty() {
        format!("UPDATE {table} SET {field} = NULL WHERE {field} = $1")
    } else {
        for child in children {
            push_entity_id_param(&mut params, child.id())?;
        }
        let placeholders = (2..=params.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "UPDATE {table} SET {field} = NULL WHERE {field} = $1 AND {primary_key} NOT IN ({placeholders})"
        )
    };

    repository
        .client
        .execute(&query, owned_refs(&params).as_slice())
        .await
        .map_err(map_error)?;
    Ok(())
}

async fn unlink_all_by_foreign_key<C, K>(
    repository: &PostgresRepository<C>,
    field: &str,
    value: &K,
) -> Result<()>
where
    C: SqlEntity,
    K: EntityId,
{
    let table = quote_path(C::table_name())?;
    let field = quote_path(field)?;
    let query = format!("UPDATE {table} SET {field} = NULL WHERE {field} = $1");
    let mut params = Vec::new();
    push_entity_id_param(&mut params, value)?;
    repository
        .client
        .execute(&query, owned_refs(&params).as_slice())
        .await
        .map_err(map_error)?;
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

fn page_from_rows<T: SqlEntity>(
    rows: Vec<tokio_postgres::Row>,
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

fn collect_insert_params<T: SqlEntity>(entities: &[T]) -> Result<Vec<&(dyn ToSql + Sync)>> {
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
    (POSTGRES_MAX_BIND_PARAMS / field_count).max(1)
}

fn values_clause(field_count: usize, entity_count: usize, start_index: usize) -> String {
    let mut param_index = start_index;
    (0..entity_count)
        .map(|_| {
            let placeholders = (0..field_count)
                .map(|_| {
                    let placeholder = format!("${param_index}");
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

fn update_fields<T: SqlEntity>() -> Result<Vec<&'static str>> {
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

fn owned_refs(params: &[Box<dyn ToSql + Sync + Send>]) -> Vec<&(dyn ToSql + Sync)> {
    params
        .iter()
        .map(|param| param.as_ref() as &(dyn ToSql + Sync))
        .collect()
}

fn push_entity_id_param<I: EntityId>(
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
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
            "unsupported Postgres id type: {}",
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
            "unsupported Postgres id type: {}",
            std::any::type_name::<I>()
        )))
    }
}

fn map_error(error: tokio_postgres::Error) -> RepositoryError {
    RepositoryError::Backend(error.to_string())
}
