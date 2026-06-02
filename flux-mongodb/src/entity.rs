use flux::{EntityId, RepositoryError, Result};
use mongodb::bson::{oid::ObjectId, Bson, DateTime, Document};
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MongoObjectId(pub ObjectId);

impl EntityId for MongoObjectId {}

impl From<MongoObjectId> for flux::FilterValue {
    fn from(value: MongoObjectId) -> Self {
        Self::Backend {
            type_name: "mongodb.object_id",
            value: value.0.to_hex(),
        }
    }
}

impl From<ObjectId> for MongoObjectId {
    fn from(value: ObjectId) -> Self {
        Self(value)
    }
}

impl From<MongoObjectId> for ObjectId {
    fn from(value: MongoObjectId) -> Self {
        value.0
    }
}

pub trait MongoId: EntityId {
    fn to_bson(&self) -> Result<Bson>;
}

impl MongoId for MongoObjectId {
    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::ObjectId(self.0))
    }
}

impl MongoId for String {
    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::String(self.clone()))
    }
}

impl MongoId for i32 {
    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::Int32(*self))
    }
}

impl MongoId for i64 {
    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::Int64(*self))
    }
}

impl MongoId for uuid::Uuid {
    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::String(self.to_string()))
    }
}

pub trait MongoEntity: flux::Entity {
    fn collection_name() -> &'static str;

    fn id_field() -> &'static str {
        "_id"
    }

    fn from_document(document: Document) -> Result<Self>;

    fn to_document(&self) -> Result<Document>;
}

pub trait MongoField: Sized {
    fn from_bson(value: Bson) -> Result<Self>;

    fn to_bson(&self) -> Result<Bson>;
}

macro_rules! impl_mongo_field_int {
    ($($ty:ty),* $(,)?) => {
        $(
            impl MongoField for $ty {
                fn from_bson(value: Bson) -> Result<Self> {
                    match value {
                        Bson::Int32(value) => <$ty>::try_from(value).map_err(|_| {
                            RepositoryError::InvalidData(format!(
                                "BSON Int32 value is out of range for {}",
                                std::any::type_name::<$ty>()
                            ))
                        }),
                        Bson::Int64(value) => <$ty>::try_from(value).map_err(|_| {
                            RepositoryError::InvalidData(format!(
                                "BSON Int64 value is out of range for {}",
                                std::any::type_name::<$ty>()
                            ))
                        }),
                        other => Err(RepositoryError::InvalidData(format!(
                            "expected integer BSON value, got {other:?}"
                        ))),
                    }
                }

                fn to_bson(&self) -> Result<Bson> {
                    let value = i64::try_from(*self).map_err(|_| {
                        RepositoryError::InvalidData(format!(
                            "integer value is out of i64 range for {}",
                            std::any::type_name::<$ty>()
                        ))
                    })?;

                    match i32::try_from(value) {
                        Ok(value) => Ok(Bson::Int32(value)),
                        Err(_) => Ok(Bson::Int64(value)),
                    }
                }
            }
        )*
    };
}

impl_mongo_field_int!(i16, i32, i64, u16, u32, u64);

impl MongoField for MongoObjectId {
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::ObjectId(value) => Ok(Self(value)),
            other => Err(RepositoryError::InvalidData(format!(
                "expected ObjectId BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::ObjectId(self.0))
    }
}

impl MongoField for String {
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::String(value) => Ok(value),
            other => Err(RepositoryError::InvalidData(format!(
                "expected String BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::String(self.clone()))
    }
}

impl MongoField for bool {
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::Boolean(value) => Ok(value),
            other => Err(RepositoryError::InvalidData(format!(
                "expected Boolean BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::Boolean(*self))
    }
}

impl MongoField for f64 {
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::Double(value) => Ok(value),
            Bson::Int32(value) => Ok(f64::from(value)),
            Bson::Int64(value) => Ok(value as f64),
            other => Err(RepositoryError::InvalidData(format!(
                "expected numeric BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::Double(*self))
    }
}

impl MongoField for uuid::Uuid {
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::String(value) => uuid::Uuid::from_str(&value).map_err(|error| {
                RepositoryError::InvalidData(format!("invalid UUID string in BSON: {error}"))
            }),
            other => Err(RepositoryError::InvalidData(format!(
                "expected UUID string BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::String(self.to_string()))
    }
}

impl<T> MongoField for Option<T>
where
    T: MongoField,
{
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::Null => Ok(None),
            value => T::from_bson(value).map(Some),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        match self {
            Some(value) => value.to_bson(),
            None => Ok(Bson::Null),
        }
    }
}

impl MongoField for Bson {
    fn from_bson(value: Bson) -> Result<Self> {
        Ok(value)
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(self.clone())
    }
}

impl MongoField for Document {
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::Document(value) => Ok(value),
            other => Err(RepositoryError::InvalidData(format!(
                "expected Document BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::Document(self.clone()))
    }
}

impl MongoField for DateTime {
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::DateTime(value) => Ok(value),
            other => Err(RepositoryError::InvalidData(format!(
                "expected DateTime BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        Ok(Bson::DateTime(*self))
    }
}

impl<T> MongoField for Vec<T>
where
    T: MongoField,
{
    fn from_bson(value: Bson) -> Result<Self> {
        match value {
            Bson::Array(values) => values
                .into_iter()
                .map(T::from_bson)
                .collect::<Result<Vec<_>>>(),
            other => Err(RepositoryError::InvalidData(format!(
                "expected Array BSON value, got {other:?}"
            ))),
        }
    }

    fn to_bson(&self) -> Result<Bson> {
        self.iter()
            .map(MongoField::to_bson)
            .collect::<Result<Vec<_>>>()
            .map(Bson::Array)
    }
}

pub(crate) fn unsupported_id<I>() -> RepositoryError {
    RepositoryError::Unsupported(format!(
        "unsupported Mongo id type: {}",
        std::any::type_name::<I>()
    ))
}
