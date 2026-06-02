use flux::{EntityId, RepositoryError, Result};
use mongodb::bson::{oid::ObjectId, Bson, Document};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MongoObjectId(pub ObjectId);

impl EntityId for MongoObjectId {}

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

pub(crate) fn unsupported_id<I>() -> RepositoryError {
    RepositoryError::Unsupported(format!(
        "unsupported Mongo id type: {}",
        std::any::type_name::<I>()
    ))
}
