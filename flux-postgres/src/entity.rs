use flux::Result;
use tokio_postgres::{types::ToSql, Row};

pub trait SqlEntity: flux::Entity {
    fn table_name() -> &'static str;

    fn primary_key() -> &'static str;

    fn fields() -> &'static [&'static str];

    fn from_row(row: Row) -> Result<Self>;

    fn to_insert_params(&self) -> Vec<&(dyn ToSql + Sync)>;

    fn to_update_params(&self) -> Vec<&(dyn ToSql + Sync)>;

    fn primary_key_param(&self) -> &(dyn ToSql + Sync);
}
