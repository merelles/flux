use flux::{
    FilterExpr, FilterOp, FilterOperand, FilterValue, GenericFilter, RepositoryError, Result,
};
use mongodb::bson::{Bson, Document};

pub fn render_filter<T>(filter: &GenericFilter<T>) -> Result<Document> {
    let mut document = Document::new();
    let expressions = filter.expressions();

    if expressions.is_empty() {
        return Ok(document);
    }

    if expressions.len() == 1 {
        return render_expr(&expressions[0]);
    }

    let clauses = expressions
        .iter()
        .map(render_expr)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(Bson::Document)
        .collect::<Vec<_>>();
    document.insert("$and", Bson::Array(clauses));
    Ok(document)
}

fn render_expr(expr: &FilterExpr) -> Result<Document> {
    match expr {
        FilterExpr::Condition(condition) => render_condition(condition),
        FilterExpr::And(items) => render_group("$and", items),
        FilterExpr::Or(items) => render_group("$or", items),
        FilterExpr::Not(inner) => {
            let mut document = Document::new();
            document.insert(
                "$nor",
                Bson::Array(vec![Bson::Document(render_expr(inner)?)]),
            );
            Ok(document)
        }
    }
}

fn render_group(operator: &str, items: &[FilterExpr]) -> Result<Document> {
    let clauses = items
        .iter()
        .map(render_expr)
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(Bson::Document)
        .collect::<Vec<_>>();
    let mut document = Document::new();
    document.insert(operator, Bson::Array(clauses));
    Ok(document)
}

fn render_condition(condition: &flux::FilterCondition) -> Result<Document> {
    let field = &condition.field;
    match (&condition.op, &condition.operand) {
        (FilterOp::Eq, FilterOperand::Single(value)) => field_document(field, to_bson(value)?),
        (FilterOp::Ne, FilterOperand::Single(value)) => {
            operator_document(field, "$ne", to_bson(value)?)
        }
        (FilterOp::Gt, FilterOperand::Single(value)) => {
            operator_document(field, "$gt", to_bson(value)?)
        }
        (FilterOp::Gte, FilterOperand::Single(value)) => {
            operator_document(field, "$gte", to_bson(value)?)
        }
        (FilterOp::Lt, FilterOperand::Single(value)) => {
            operator_document(field, "$lt", to_bson(value)?)
        }
        (FilterOp::Lte, FilterOperand::Single(value)) => {
            operator_document(field, "$lte", to_bson(value)?)
        }
        (FilterOp::In, FilterOperand::Many(values)) => {
            let values = values.iter().map(to_bson).collect::<Result<Vec<_>>>()?;
            operator_document(field, "$in", Bson::Array(values))
        }
        (FilterOp::Like, FilterOperand::Single(FilterValue::String(value))) => {
            operator_document(field, "$regex", Bson::String(value.clone()))
        }
        (FilterOp::IsNull, FilterOperand::None) => field_document(field, Bson::Null),
        (FilterOp::IsNotNull, FilterOperand::None) => operator_document(field, "$ne", Bson::Null),
        _ => Err(RepositoryError::InvalidData(format!(
            "invalid Mongo filter condition: {:?}",
            condition.op
        ))),
    }
}

fn field_document(field: &str, value: Bson) -> Result<Document> {
    let mut document = Document::new();
    document.insert(field, value);
    Ok(document)
}

fn operator_document(field: &str, operator: &str, value: Bson) -> Result<Document> {
    let mut operator_doc = Document::new();
    operator_doc.insert(operator, value);
    field_document(field, Bson::Document(operator_doc))
}

fn to_bson(value: &FilterValue) -> Result<Bson> {
    match value {
        FilterValue::Bool(value) => Ok(Bson::Boolean(*value)),
        FilterValue::I16(value) => Ok(Bson::Int32(i32::from(*value))),
        FilterValue::I32(value) => Ok(Bson::Int32(*value)),
        FilterValue::I64(value) => Ok(Bson::Int64(*value)),
        FilterValue::U16(value) => Ok(Bson::Int32(i32::from(*value))),
        FilterValue::U32(value) => Ok(Bson::Int64(i64::from(*value))),
        FilterValue::U64(value) => Ok(Bson::Int64(i64::try_from(*value).map_err(|_| {
            RepositoryError::InvalidData(format!("u64 value exceeds i64 range: {value}"))
        })?)),
        FilterValue::F64(value) => Ok(Bson::Double(*value)),
        FilterValue::String(value) => Ok(Bson::String(value.clone())),
        FilterValue::Uuid(value) => Ok(Bson::String(value.to_string())),
        FilterValue::Null => Ok(Bson::Null),
    }
}
