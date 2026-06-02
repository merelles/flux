use std::sync::Arc;

use tokio_postgres::types::ToSql;

use crate::entity::Entity;

/// Generic filter trait
pub trait Filter: Send + Sync {
    /// Builds a SQL query with this filter
    ///
    /// Returns a tuple of (SQL clause, parameters)
    fn build_query(&self) -> (String, Vec<Arc<dyn ToSql + Sync + Send>>);
}

/// Builder for creating filters fluently
pub struct FilterBuilder<T> {
    conditions: Vec<(String, Arc<dyn ToSql + Sync + Send>)>,
    order_by: Option<(String, OrderDirection)>,
    limit: Option<u64>,
    offset: Option<u64>,
    _marker: std::marker::PhantomData<T>,
}

pub enum OrderDirection {
    Asc,
    Desc,
}

impl<T> FilterBuilder<T> {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Adds an equality condition
    pub fn eq(mut self, field: &str, value: impl ToSql + Send + Sync + 'static) -> Self {
        self.conditions.push((field.to_string(), Arc::new(value)));
        self
    }

    /// Adds a condition (for compatibility with old API)
    pub fn with_condition(self, field: &str, value: impl ToSql + Send + Sync + 'static) -> Self {
        self.eq(field, value)
    }

    /// Adds an ordering clause
    pub fn order_by(mut self, field: &str, direction: OrderDirection) -> Self {
        self.order_by = Some((field.to_string(), direction));
        self
    }

    /// Sets the limit for results
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Sets the offset for results
    pub fn offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }
}

impl<T> Default for FilterBuilder<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Generic filter implementation for entities
pub struct GenericFilter<T> {
    conditions: Vec<(String, Arc<dyn ToSql + Sync + Send>)>,
    order_by: Option<(String, OrderDirection)>,
    limit: Option<u64>,
    offset: Option<u64>,
    _marker: std::marker::PhantomData<T>,
}

impl<T> GenericFilter<T> {
    /// Creates a new filter with no conditions
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            order_by: None,
            limit: None,
            offset: None,
            _marker: std::marker::PhantomData,
        }
    }

    /// Adds a condition for filtering
    pub fn with_condition(
        mut self,
        field: &str,
        value: impl ToSql + Send + Sync + 'static,
    ) -> Self {
        self.conditions.push((field.to_string(), Arc::new(value)));
        self
    }

    /// Adds ordering
    pub fn with_order_by(mut self, field: &str, direction: OrderDirection) -> Self {
        self.order_by = Some((field.to_string(), direction));
        self
    }

    /// Sets the limit
    pub fn with_limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Sets the offset
    pub fn with_offset(mut self, n: u64) -> Self {
        self.offset = Some(n);
        self
    }
}

impl<T> Default for GenericFilter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Entity> Filter for GenericFilter<T> {
    fn build_query(&self) -> (String, Vec<Arc<dyn ToSql + Sync + Send>>) {
        let mut where_clause = String::new();
        let mut params = Vec::new();

        if !self.conditions.is_empty() {
            where_clause = "WHERE ".to_string();
            let conditions: Vec<String> = self
                .conditions
                .iter()
                .enumerate()
                .map(|(i, (field, _))| format!("{} = ${}", field, i + 1))
                .collect();
            where_clause.push_str(&conditions.join(" AND "));

            params.extend(self.conditions.iter().map(|(_, val)| Arc::clone(val)));
        }

        // Add ORDER BY
        if let Some((field, direction)) = &self.order_by {
            let dir_str = match direction {
                OrderDirection::Asc => "ASC",
                OrderDirection::Desc => "DESC",
            };
            where_clause.push_str(&format!(" ORDER BY {} {}", field, dir_str));
        }

        // Add LIMIT
        if let Some(limit) = self.limit {
            params.push(Arc::new(limit as i64));
            where_clause.push_str(&format!(" LIMIT ${}", params.len()));
        }

        // Add OFFSET
        if let Some(offset) = self.offset {
            params.push(Arc::new(offset as i64));
            where_clause.push_str(&format!(" OFFSET ${}", params.len()));
        }

        (where_clause, params)
    }
}

impl<T> From<FilterBuilder<T>> for GenericFilter<T> {
    fn from(builder: FilterBuilder<T>) -> Self {
        Self {
            conditions: builder.conditions,
            order_by: builder.order_by,
            limit: builder.limit,
            offset: builder.offset,
            _marker: std::marker::PhantomData,
        }
    }
}
