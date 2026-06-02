use std::{any::Any, marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use flux::{
    BulkRepository, EntityId, GenericFilter, Page, PageRequest, ReadRepository, RelationRepository,
    RepositoryError, Result, WriteRepository,
};
use tokio_postgres::{types::ToSql, Client};

use crate::{filter::quote_path, render_filter, SqlEntity};

pub struct PostgresRepository<T: SqlEntity> {
    client: Arc<Client>,
    _marker: PhantomData<T>,
}

impl<T: SqlEntity> Clone for PostgresRepository<T> {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
            _marker: PhantomData,
        }
    }
}

impl<T: SqlEntity> PostgresRepository<T> {
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            _marker: PhantomData,
        }
    }

    pub fn client(&self) -> &Arc<Client> {
        &self.client
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
                page_from_rows::<T>(rows, limit)
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
                page_from_rows::<T>(rows, limit)
            }
        }
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

    async fn find_all_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>> {
        self.query_page(Some(filter), page).await
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
        let values_clause = values_clause(fields.len(), entities.len(), 1);
        let query =
            format!("INSERT INTO {table} ({field_list}) VALUES {values_clause} RETURNING *");
        let params = collect_insert_params::<T>(entities)?;
        let rows = self
            .client
            .query(&query, params.as_slice())
            .await
            .map_err(map_error)?;
        rows.into_iter().map(T::from_row).collect()
    }

    async fn update_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let fields = T::fields();
        let update_fields = update_fields::<T>()?;
        let values_clause = values_clause(fields.len(), entities.len(), 1);
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
        let query = format!(
            "UPDATE {table} AS target SET {set_clause} FROM (VALUES {values_clause}) AS source ({source_columns}) WHERE target.{primary_key} = source.{primary_key} RETURNING target.*"
        );
        let params = collect_insert_params::<T>(entities)?;
        let rows = self
            .client
            .query(&query, params.as_slice())
            .await
            .map_err(map_error)?;
        rows.into_iter().map(T::from_row).collect()
    }

    async fn save_many(&self, entities: &[T]) -> Result<Vec<T>> {
        if entities.is_empty() {
            return Ok(Vec::new());
        }

        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let fields = T::fields();
        let field_list = quoted_fields(fields)?;
        let values_clause = values_clause(fields.len(), entities.len(), 1);
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
        let query = format!(
            "INSERT INTO {table} ({field_list}) VALUES {values_clause} ON CONFLICT ({primary_key}) DO UPDATE SET {update_clause} RETURNING *"
        );
        let params = collect_insert_params::<T>(entities)?;
        let rows = self
            .client
            .query(&query, params.as_slice())
            .await
            .map_err(map_error)?;
        rows.into_iter().map(T::from_row).collect()
    }

    async fn delete_many(&self, ids: &[T::Id]) -> Result<u64> {
        if ids.is_empty() {
            return Ok(0);
        }

        let table = quote_path(T::table_name())?;
        let primary_key = quote_path(T::primary_key())?;
        let placeholders = (1..=ids.len())
            .map(|index| format!("${index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let query = format!("DELETE FROM {table} WHERE {primary_key} IN ({placeholders})");
        let mut params = Vec::new();
        for id in ids {
            push_entity_id_param(&mut params, id)?;
        }
        self.client
            .execute(&query, owned_refs(&params).as_slice())
            .await
            .map_err(map_error)
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
        self.find_all_with_filter(filter, page).await
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

fn page_from_rows<T: SqlEntity>(
    rows: Vec<tokio_postgres::Row>,
    limit: u32,
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
    Ok(Page::new(items, limit, next_cursor, None))
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
