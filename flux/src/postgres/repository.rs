use std::marker::PhantomData;
use std::result::Result;
use std::sync::Arc;

use async_trait::async_trait;
use tokio_postgres::{Client, Error as PostgresError};
use uuid::Uuid;

use crate::{Entity, EntityExt, Filter, Repository as RepositoryTrait, RepositoryError};

/// Generic PostgreSQL repository implementation
///
/// This provides a fully generic CRUD implementation for any Entity type.
/// All SQL is generated automatically based on the Entity metadata.
#[derive(Clone)]
pub struct PostgresRepository<T: Entity> {
    client: Arc<Client>,
    _phantom: PhantomData<T>,
}

impl<T: Entity> PostgresRepository<T> {
    /// Creates a new PostgresRepository instance
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            _phantom: PhantomData,
        }
    }

    fn build_query_context(&self, query: &str, params_count: usize) -> String {
        let sanitized_query = query.replace('\n', " ").replace('"', "\\\"");
        format!(
            "table={} params={} query=\"{}\"",
            T::table_name(),
            params_count,
            sanitized_query
        )
    }

    fn log_db_error(
        &self,
        operation: &str,
        query_context: &str,
        err: PostgresError,
    ) -> RepositoryError {
        println!(
            "[ERROR] {} failed: {} | err: {}",
            operation, query_context, err
        );
        RepositoryError::OperationFailed(format!(
            "Database error during {}: {} | err: {}",
            operation, query_context, err
        ))
    }
}

#[async_trait]
impl<T: Entity> RepositoryTrait<T> for PostgresRepository<T> {
    async fn find_by_id(&self, id: Uuid) -> Result<T, RepositoryError> {
        let query = format!(
            "SELECT * FROM {} WHERE {} = $1",
            T::table_name(),
            T::primary_key()
        );

        println!(
            "[INFO] Buscando {} com {}: {}",
            T::table_name(),
            T::primary_key(),
            id
        );

        let query_context = self.build_query_context(&query, 1);

        let row = self
            .client
            .query_opt(&query, &[&id])
            .await
            .map_err(|err| self.log_db_error("find_by_id", &query_context, err))?;

        match row {
            Some(row) => {
                println!("[OK] {} encontrado com id: {}", T::table_name(), id);
                T::from_row(row).map_err(|e| RepositoryError::InvalidData(format!("{}", e)))
            }
            None => {
                println!("[WARN] {} não encontrado com id: {}", T::table_name(), id);
                Err(RepositoryError::NotFound)
            }
        }
    }

    async fn find_all(&self) -> Result<Vec<T>, RepositoryError> {
        let query = format!("SELECT * FROM {}", T::table_name());

        println!("[INFO] Buscando todos os registros de {}", T::table_name());

        let rows = self.client.query(&query, &[]).await?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            match T::from_row(row) {
                Ok(entity) => results.push(entity),
                Err(e) => {
                    println!("[WARN] Falha ao parsear linha: {}", e);
                    // Continue processing other rows
                }
            }
        }

        println!(
            "[OK] Encontrados {} registros de {}",
            results.len(),
            T::table_name()
        );
        Ok(results)
    }

    async fn find_all_with_filter(
        &self,
        filter: crate::filter::GenericFilter<T>,
    ) -> Result<Vec<T>, RepositoryError> {
        // Build query using filter
        let (where_clause, params): (
            String,
            Vec<std::sync::Arc<dyn tokio_postgres::types::ToSql + Sync + Send>>,
        ) = filter.build_query();

        let query = if where_clause.is_empty() {
            format!("SELECT * FROM {}", T::table_name())
        } else {
            format!("SELECT * FROM {} {}", T::table_name(), where_clause)
        };

        println!(
            "[INFO] Buscando registros de {} com filtro: {}",
            T::table_name(),
            where_clause
        );

        // Convert Arc<dyn ToSql> to &dyn ToSql for query execution
        // Note: tokio_postgres requires &[&dyn ToSql + Sync]
        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(
                |p: &std::sync::Arc<dyn tokio_postgres::types::ToSql + Sync + Send>| {
                    p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)
                },
            )
            .collect();

        let rows = self.client.query(&query, &param_refs[..]).await?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            match T::from_row(row) {
                Ok(entity) => results.push(entity),
                Err(e) => {
                    println!("[WARN] Falha ao parsear linha: {}", e);
                    // Continue processing other rows
                }
            }
        }

        println!(
            "[OK] Encontrados {} registros de {} com filtro",
            results.len(),
            T::table_name()
        );
        Ok(results)
    }

    async fn save(&self, entity: T) -> Result<T, RepositoryError> {
        // Check if entity exists to determine insert vs update
        if entity.has_id() {
            let id = entity.get_id().ok_or_else(|| {
                RepositoryError::InvalidData(
                    "Entity has_id() returns true but get_id() returns None".to_string(),
                )
            })?;
            if self.exists(id).await? {
                return self.update(entity).await;
            }
        }

        self.insert(entity).await
    }

    async fn insert(&self, entity: T) -> Result<T, RepositoryError> {
        let fields = T::fields();
        let placeholders: Vec<String> = (1..=fields.len()).map(|i| format!("${}", i)).collect();

        let query = format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING *",
            T::table_name(),
            fields.join(", "),
            placeholders.join(", ")
        );

        println!("[INFO] Inserindo novo registro em {}", T::table_name());

        let params = entity.to_insert_params();
        let query_context = self.build_query_context(&query, params.len());

        let row = self
            .client
            .query_one(&query, params.as_slice())
            .await
            .map_err(|err| self.log_db_error("insert", &query_context, err))?;

        let result =
            T::from_row(row).map_err(|e| RepositoryError::InvalidData(format!("{}", e)))?;

        println!(
            "[OK] Registro inserido em {} com id: {:?}",
            T::table_name(),
            result.get_id()
        );
        Ok(result)
    }

    async fn update(&self, entity: T) -> Result<T, RepositoryError> {
        let pk_name = T::primary_key();
        let fields = T::fields();

        // Filter out primary key from update fields
        let update_fields: Vec<_> = fields.iter().filter(|&&field| field != pk_name).collect();

        if update_fields.is_empty() {
            println!("[WARN] Nenhum campo para atualizar em {}", T::table_name());
            return Err(RepositoryError::InvalidData(
                "No fields to update (only primary key present)".to_string(),
            ));
        }

        let set_clause: Vec<String> = update_fields
            .iter()
            .enumerate()
            .map(|(i, field)| format!("{} = ${}", field, i + 1))
            .collect();

        let query = format!(
            "UPDATE {} SET {} WHERE {} = ${} RETURNING *",
            T::table_name(),
            set_clause.join(", "),
            pk_name,
            update_fields.len() + 1
        );

        println!("[INFO] Atualizando registro em {}", T::table_name());

        let mut params = entity.to_update_params();
        let pk_value = entity.primary_key_value();

        // Add primary key value to params
        params.push(pk_value);

        let query_context = self.build_query_context(&query, params.len());

        let row = self
            .client
            .query_one(&query, params.as_slice())
            .await
            .map_err(|err| self.log_db_error("update", &query_context, err))?;

        let result =
            T::from_row(row).map_err(|e| RepositoryError::InvalidData(format!("{}", e)))?;

        println!(
            "[OK] Registro atualizado em {} com id: {:?}",
            T::table_name(),
            result.get_id()
        );
        Ok(result)
    }

    async fn delete(&self, id: Uuid) -> Result<bool, RepositoryError> {
        let query = format!(
            "DELETE FROM {} WHERE {} = $1",
            T::table_name(),
            T::primary_key()
        );

        println!(
            "[INFO] Deletando registro em {} com id: {}",
            T::table_name(),
            id
        );

        let query_context = self.build_query_context(&query, 1);

        let rows_affected = self
            .client
            .execute(&query, &[&id])
            .await
            .map_err(|err| self.log_db_error("delete", &query_context, err))?;

        let deleted = rows_affected > 0;
        if deleted {
            println!(
                "[CLEAN] {} registro(s) deletado(s) de {}: {}",
                rows_affected,
                T::table_name(),
                id
            );
        } else {
            println!(
                "[WARN] Nenhum registro deletado de {} com id: {}",
                T::table_name(),
                id
            );
        }

        Ok(deleted)
    }

    async fn exists(&self, id: Uuid) -> Result<bool, RepositoryError> {
        let query = format!(
            "SELECT 1 FROM {} WHERE {} = $1 LIMIT 1",
            T::table_name(),
            T::primary_key()
        );

        let row = self.client.query_opt(&query, &[&id]).await?;

        Ok(row.is_some())
    }

    async fn count(&self) -> Result<u64, RepositoryError> {
        let query = format!("SELECT COUNT(*) FROM {}", T::table_name());

        let row = self.client.query_one(&query, &[]).await?;

        let count: i64 = row.get(0);
        Ok(count as u64)
    }

    async fn find_by_foreign_key(
        &self,
        field: &str,
        value: &Uuid,
    ) -> Result<Vec<T>, RepositoryError> {
        let query = format!("SELECT * FROM {} WHERE {} = $1", T::table_name(), field);

        println!(
            "[INFO] Buscando {} com {}: {}",
            T::table_name(),
            field,
            value
        );

        let rows = self.client.query(&query, &[value]).await?;

        let mut results = Vec::with_capacity(rows.len());
        for row in rows {
            match T::from_row(row) {
                Ok(entity) => results.push(entity),
                Err(e) => {
                    println!("[WARN] Falha ao parsear linha: {}", e);
                }
            }
        }

        println!(
            "[OK] Encontrados {} registros de {} com {}: {}",
            results.len(),
            T::table_name(),
            field,
            value
        );
        Ok(results)
    }

    async fn delete_by_foreign_key(
        &self,
        field: &str,
        value: &Uuid,
    ) -> Result<u64, RepositoryError> {
        let query = format!("DELETE FROM {} WHERE {} = $1", T::table_name(), field);

        println!(
            "[INFO] Deletando registros de {} onde {}: {}",
            T::table_name(),
            field,
            value
        );

        let rows_affected = self.client.execute(&query, &[value]).await?;

        println!(
            "[CLEAN] {} registro(s) deletado(s) de {} onde {}: {}",
            rows_affected,
            T::table_name(),
            field,
            value
        );
        Ok(rows_affected)
    }
}
