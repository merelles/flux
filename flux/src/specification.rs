/// Specification pattern for building queries
///
/// This trait allows for flexible query construction.
/// Implementations should convert specifications into SQL queries.
pub trait Specification<T>: Send + Sync {
    /// Converts this specification into SQL
    ///
    /// Returns a tuple of (SQL WHERE clause, parameter values)
    fn to_sql(&self) -> (String, Vec<Box<dyn std::any::Any>>);

    /// Returns true if this is an empty specification (no filters)
    fn is_empty(&self) -> bool;
}

/// Empty specification that matches all entities
pub struct EmptySpecification;

impl<T> Specification<T> for EmptySpecification {
    fn to_sql(&self) -> (String, Vec<Box<dyn std::any::Any>>) {
        ("1=1".to_string(), Vec::new())
    }

    fn is_empty(&self) -> bool {
        true
    }
}

/// Specification that combines multiple conditions with AND
pub struct AndSpecification {
    conditions: Vec<String>,
}

impl AndSpecification {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    pub fn add(mut self, condition: String) -> Self {
        self.conditions.push(condition);
        self
    }
}

impl<T> Specification<T> for AndSpecification {
    fn to_sql(&self) -> (String, Vec<Box<dyn std::any::Any>>) {
        let sql = self.conditions.join(" AND ");
        (sql, Vec::new())
    }

    fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }
}

impl Default for AndSpecification {
    fn default() -> Self {
        Self::new()
    }
}

/// Specification that combines multiple conditions with OR
pub struct OrSpecification {
    conditions: Vec<String>,
}

impl OrSpecification {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
        }
    }

    pub fn add(mut self, condition: String) -> Self {
        self.conditions.push(condition);
        self
    }
}

impl<T> Specification<T> for OrSpecification {
    fn to_sql(&self) -> (String, Vec<Box<dyn std::any::Any>>) {
        let sql = self.conditions.join(" OR ");
        (sql, Vec::new())
    }

    fn is_empty(&self) -> bool {
        self.conditions.is_empty()
    }
}

impl Default for OrSpecification {
    fn default() -> Self {
        Self::new()
    }
}
