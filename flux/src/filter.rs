use std::marker::PhantomData;

use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderClause {
    pub field: String,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    U16(u16),
    U32(u32),
    U64(u64),
    F64(f64),
    String(String),
    Uuid(Uuid),
    Null,
}

macro_rules! impl_filter_value {
    ($($ty:ty => $variant:ident),* $(,)?) => {
        $(
            impl From<$ty> for FilterValue {
                fn from(value: $ty) -> Self {
                    Self::$variant(value)
                }
            }
        )*
    };
}

impl_filter_value!(
    bool => Bool,
    i16 => I16,
    i32 => I32,
    i64 => I64,
    u16 => U16,
    u32 => U32,
    u64 => U64,
    f64 => F64,
    String => String,
    Uuid => Uuid,
);

impl From<&str> for FilterValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    In,
    Like,
    IsNull,
    IsNotNull,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterOperand {
    None,
    Single(FilterValue),
    Many(Vec<FilterValue>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterCondition {
    pub field: String,
    pub op: FilterOp,
    pub operand: FilterOperand,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterExpr {
    Condition(FilterCondition),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
    Not(Box<FilterExpr>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct GenericFilter<T> {
    expressions: Vec<FilterExpr>,
    order_by: Vec<OrderClause>,
    _marker: PhantomData<T>,
}

impl<T> Default for GenericFilter<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GenericFilter<T> {
    pub fn new() -> Self {
        Self {
            expressions: Vec::new(),
            order_by: Vec::new(),
            _marker: PhantomData,
        }
    }

    pub fn expressions(&self) -> &[FilterExpr] {
        &self.expressions
    }

    pub fn order_by_clauses(&self) -> &[OrderClause] {
        &self.order_by
    }

    pub fn into_expr(self) -> Option<FilterExpr> {
        match self.expressions.len() {
            0 => None,
            1 => self.expressions.into_iter().next(),
            _ => Some(FilterExpr::And(self.expressions)),
        }
    }

    pub fn with_condition(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.eq(field, value)
    }

    pub fn eq(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.condition(field, FilterOp::Eq, FilterOperand::Single(value.into()))
    }

    pub fn ne(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.condition(field, FilterOp::Ne, FilterOperand::Single(value.into()))
    }

    pub fn gt(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.condition(field, FilterOp::Gt, FilterOperand::Single(value.into()))
    }

    pub fn gte(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.condition(field, FilterOp::Gte, FilterOperand::Single(value.into()))
    }

    pub fn lt(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.condition(field, FilterOp::Lt, FilterOperand::Single(value.into()))
    }

    pub fn lte(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.condition(field, FilterOp::Lte, FilterOperand::Single(value.into()))
    }

    pub fn like(self, field: &str, value: impl Into<FilterValue>) -> Self {
        self.condition(field, FilterOp::Like, FilterOperand::Single(value.into()))
    }

    pub fn is_null(self, field: &str) -> Self {
        self.condition(field, FilterOp::IsNull, FilterOperand::None)
    }

    pub fn is_not_null(self, field: &str) -> Self {
        self.condition(field, FilterOp::IsNotNull, FilterOperand::None)
    }

    pub fn in_list<I, V>(self, field: &str, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<FilterValue>,
    {
        let values = values.into_iter().map(Into::into).collect();
        self.condition(field, FilterOp::In, FilterOperand::Many(values))
    }

    pub fn with_order_by(self, field: &str, direction: OrderDirection) -> Self {
        self.order_by(field, direction)
    }

    pub fn order_by(mut self, field: &str, direction: OrderDirection) -> Self {
        self.order_by.push(OrderClause {
            field: field.to_string(),
            direction,
        });
        self
    }

    pub fn and<F>(mut self, build: F) -> Self
    where
        F: FnOnce(Self) -> Self,
    {
        if let Some(expr) = build(Self::new()).into_expr() {
            self.expressions.push(expr);
        }
        self
    }

    pub fn and_group<F>(self, build: F) -> Self
    where
        F: FnOnce(Self) -> Self,
    {
        self.and(build)
    }

    pub fn or<F>(mut self, build: F) -> Self
    where
        F: FnOnce(Self) -> Self,
    {
        if let Some(expr) = build(Self::new()).into_expr() {
            match self.expressions.last_mut() {
                Some(FilterExpr::Or(items)) => items.push(expr),
                _ => self.expressions.push(FilterExpr::Or(vec![expr])),
            }
        }
        self
    }

    pub fn not<F>(mut self, build: F) -> Self
    where
        F: FnOnce(Self) -> Self,
    {
        if let Some(expr) = build(Self::new()).into_expr() {
            self.expressions.push(FilterExpr::Not(Box::new(expr)));
        }
        self
    }

    fn condition(mut self, field: &str, op: FilterOp, operand: FilterOperand) -> Self {
        self.expressions
            .push(FilterExpr::Condition(FilterCondition {
                field: field.to_string(),
                op,
                operand,
            }));
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Product;

    #[test]
    fn builds_and_expression_from_multiple_conditions() {
        let expr = GenericFilter::<Product>::new()
            .eq("name", "Keyboard")
            .gte("price", 100)
            .into_expr()
            .expect("filter expression");

        match expr {
            FilterExpr::And(items) => assert_eq!(items.len(), 2),
            other => panic!("expected AND expression, got {other:?}"),
        }
    }

    #[test]
    fn builds_or_expression_from_group() {
        let expr = GenericFilter::<Product>::new()
            .or(|query| query.eq("status", "open"))
            .or(|query| query.eq("status", "paid"))
            .into_expr()
            .expect("filter expression");

        match expr {
            FilterExpr::Or(items) => assert_eq!(items.len(), 2),
            other => panic!("expected OR expression, got {other:?}"),
        }
    }
}
