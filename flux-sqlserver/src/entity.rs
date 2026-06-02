use flux::Result;

pub trait SqlServerEntity: flux::Entity {
    fn table_name() -> &'static str;

    fn primary_key() -> &'static str;

    fn fields() -> &'static [&'static str];

    fn from_row(row: tiberius::Row) -> Result<Self>;

    fn to_insert_params(&self) -> Vec<&dyn tiberius::ToSql>;

    fn to_update_params(&self) -> Vec<&dyn tiberius::ToSql>;

    fn primary_key_param(&self) -> &dyn tiberius::ToSql;
}

pub trait SqlServerField: Sized {
    fn from_row(row: &tiberius::Row, column: &'static str) -> Result<Self>;
}

macro_rules! impl_sqlserver_field_copy {
    ($($ty:ty),* $(,)?) => {
        $(
            impl SqlServerField for $ty {
                fn from_row(row: &tiberius::Row, column: &'static str) -> Result<Self> {
                    row.try_get::<$ty, _>(column)
                        .map_err(|error| flux::RepositoryError::InvalidData(error.to_string()))?
                        .ok_or_else(|| {
                            flux::RepositoryError::InvalidData(format!(
                                "SQL Server column {column} is NULL"
                            ))
                        })
                }
            }
        )*
    };
}

impl_sqlserver_field_copy!(bool, i16, i32, i64, f32, f64, uuid::Uuid);

impl SqlServerField for String {
    fn from_row(row: &tiberius::Row, column: &'static str) -> Result<Self> {
        row.try_get::<&str, _>(column)
            .map_err(|error| flux::RepositoryError::InvalidData(error.to_string()))?
            .map(str::to_string)
            .ok_or_else(|| {
                flux::RepositoryError::InvalidData(format!("SQL Server column {column} is NULL"))
            })
    }
}

impl<T> SqlServerField for Option<T>
where
    T: SqlServerField,
{
    fn from_row(row: &tiberius::Row, column: &'static str) -> Result<Self> {
        match T::from_row(row, column) {
            Ok(value) => Ok(Some(value)),
            Err(flux::RepositoryError::InvalidData(message))
                if message == format!("SQL Server column {column} is NULL") =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }
}
