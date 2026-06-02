use flux::{
    FilterExpr, FilterOp, FilterOperand, FilterValue, GenericFilter, OrderDirection,
    RepositoryError, Result,
};
use tokio_postgres::types::ToSql;

pub struct RenderedFilter {
    pub where_clause: Option<String>,
    pub order_by: Option<String>,
    pub params: Vec<Box<dyn ToSql + Sync + Send>>,
}

pub fn render_filter<T>(filter: &GenericFilter<T>, start_index: usize) -> Result<RenderedFilter> {
    let mut params = Vec::new();
    let mut expressions = Vec::new();

    for expr in filter.expressions() {
        expressions.push(render_expr(expr, &mut params, start_index)?);
    }

    let where_clause = if expressions.is_empty() {
        None
    } else {
        Some(expressions.join(" AND "))
    };

    let order_by = if filter.order_by_clauses().is_empty() {
        None
    } else {
        let clauses = filter
            .order_by_clauses()
            .iter()
            .map(|clause| {
                let direction = match clause.direction {
                    OrderDirection::Asc => "ASC",
                    OrderDirection::Desc => "DESC",
                };
                Ok(format!("{} {}", quote_path(&clause.field)?, direction))
            })
            .collect::<Result<Vec<_>>>()?;
        Some(clauses.join(", "))
    };

    Ok(RenderedFilter {
        where_clause,
        order_by,
        params,
    })
}

pub(crate) fn quote_path(path: &str) -> Result<String> {
    let parts = path
        .split('.')
        .map(quote_ident)
        .collect::<Result<Vec<_>>>()?;
    Ok(parts.join("."))
}

fn quote_ident(ident: &str) -> Result<String> {
    if ident.is_empty() {
        return Err(RepositoryError::InvalidData(
            "SQL identifier cannot be empty".to_string(),
        ));
    }

    if !ident
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(RepositoryError::InvalidData(format!(
            "Invalid SQL identifier: {ident}"
        )));
    }

    Ok(format!("\"{}\"", ident))
}

fn render_expr(
    expr: &FilterExpr,
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    start_index: usize,
) -> Result<String> {
    match expr {
        FilterExpr::Condition(condition) => {
            let field = quote_path(&condition.field)?;
            match (&condition.op, &condition.operand) {
                (FilterOp::Eq, FilterOperand::Single(FilterValue::Null))
                | (FilterOp::IsNull, FilterOperand::None) => Ok(format!("{field} IS NULL")),
                (FilterOp::Ne, FilterOperand::Single(FilterValue::Null))
                | (FilterOp::IsNotNull, FilterOperand::None) => Ok(format!("{field} IS NOT NULL")),
                (FilterOp::In, FilterOperand::Many(values)) => {
                    if values.is_empty() {
                        return Ok("FALSE".to_string());
                    }

                    let mut placeholders = Vec::with_capacity(values.len());
                    for value in values {
                        push_filter_value(params, value)?;
                        placeholders.push(format!("${}", start_index + params.len() - 1));
                    }
                    Ok(format!("{field} IN ({})", placeholders.join(", ")))
                }
                (op, FilterOperand::Single(value)) => {
                    push_filter_value(params, value)?;
                    let placeholder = format!("${}", start_index + params.len() - 1);
                    let sql_op = match op {
                        FilterOp::Eq => "=",
                        FilterOp::Ne => "<>",
                        FilterOp::Gt => ">",
                        FilterOp::Gte => ">=",
                        FilterOp::Lt => "<",
                        FilterOp::Lte => "<=",
                        FilterOp::Like => "LIKE",
                        _ => {
                            return Err(RepositoryError::Unsupported(format!(
                                "Unsupported filter operator: {op:?}"
                            )));
                        }
                    };
                    Ok(format!("{field} {sql_op} {placeholder}"))
                }
                _ => Err(RepositoryError::InvalidData(format!(
                    "Invalid filter operand for {:?}",
                    condition.op
                ))),
            }
        }
        FilterExpr::And(items) => render_group("AND", items, params, start_index),
        FilterExpr::Or(items) => render_group("OR", items, params, start_index),
        FilterExpr::Not(inner) => Ok(format!(
            "NOT ({})",
            render_expr(inner, params, start_index)?
        )),
    }
}

fn render_group(
    operator: &str,
    items: &[FilterExpr],
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    start_index: usize,
) -> Result<String> {
    let rendered = items
        .iter()
        .map(|expr| render_expr(expr, params, start_index))
        .collect::<Result<Vec<_>>>()?;
    Ok(format!("({})", rendered.join(&format!(" {operator} "))))
}

fn push_filter_value(
    params: &mut Vec<Box<dyn ToSql + Sync + Send>>,
    value: &FilterValue,
) -> Result<()> {
    match value {
        FilterValue::Bool(value) => params.push(Box::new(*value)),
        FilterValue::I16(value) => params.push(Box::new(*value)),
        FilterValue::I32(value) => params.push(Box::new(*value)),
        FilterValue::I64(value) => params.push(Box::new(*value)),
        FilterValue::U16(value) => params.push(Box::new(i32::from(*value))),
        FilterValue::U32(value) => params.push(Box::new(i64::from(*value))),
        FilterValue::U64(value) => params.push(Box::new(i64::try_from(*value).map_err(|_| {
            RepositoryError::InvalidData(format!("u64 value exceeds i64 range: {value}"))
        })?)),
        FilterValue::F64(value) => params.push(Box::new(*value)),
        FilterValue::String(value) => params.push(Box::new(value.clone())),
        FilterValue::Uuid(value) => params.push(Box::new(*value)),
        FilterValue::Null => {
            return Err(RepositoryError::InvalidData(
                "NULL filter value must be rendered as IS NULL or IS NOT NULL".to_string(),
            ));
        }
    }
    Ok(())
}
