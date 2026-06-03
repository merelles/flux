# Flux Usage Guide and API Proposal

Status: this document separates the API that exists today from the API we should build next.

The intended workspace should be split into focused crates:

- `flux`: core traits, errors, filters, pagination, repository contracts, aggregate metadata
- `flux-derive`: derive macros for entities, SQL mappings, Mongo mappings, and aggregates
- `flux-postgres`: PostgreSQL adapter and SQL rendering
- `flux-mongodb`: MongoDB adapter and BSON rendering
- `flux-sqlserver`: future SQL Server adapter

The `flux` crate must not depend on database drivers. Backend-specific crates depend on `flux`, not the other way around.

The current CRUD API is useful for one table at a time. The missing part is aggregate persistence:

```rust
pub struct Order {
    pub order_oid: Uuid,
    pub customer_name: String,
    pub items: Vec<OrderItem>,
}
```

`items` is not a column in `orders`. It is a relation. Flux needs to understand that distinction.

## Current State

Available today:

- `#[derive(Entity)]`
- `#[table_name = "..."]`
- `#[primary_key]`
- `#[skip]`
- `PostgresRepository<T>` inside the current `flux` crate
- `find_by_id`
- `find_all`
- `insert`
- `update`
- `save`
- `delete`
- `exists`
- `count`
- `find_by_foreign_key`
- `delete_by_foreign_key`

Important current limitations:

- `GenericFilter` accepts several conditions, but today they are equality-only and joined with `AND`
- repository IDs are fixed to `Uuid`
- `find_all` is unbounded and can load too much data into memory

Not complete today:

- automatic relation loading
- automatic aggregate saving
- bulk insert/update/upsert
- `#[has_one]`
- `#[has_many]`
- `#[many_to_many]`
- `AggregateRoot` derive macro
- backend-neutral filters for SQL and Mongo
- separate adapter crates such as `flux-postgres` and `flux-mongodb`

## Design Direction

The current path is correct for simple table repositories, but not enough for aggregates.

Other ecosystems solved this by separating responsibilities:

- Ecto has `insert_all` for bulk writes and `Multi` for transactional workflows
- Prisma has nested writes and `createMany`
- SeaORM has `insert_many`
- SQLAlchemy and Hibernate support relationship metadata, cascades, and batch writes
- Diesel and SQLx keep SQL explicit and let the application choose the bulk strategy

The lesson for Flux:

- keep row persistence simple
- add bulk persistence explicitly
- add aggregate persistence explicitly
- do not make `insert` secretly traverse arbitrary object graphs
- do not expose unbounded reads as the default path
- do not bind the core repository abstraction to `Uuid`
- keep database drivers outside the core `flux` crate

## Proposed Crate Layout

```text
crates/
  flux/
    src/
      entity.rs
      error.rs
      filter.rs
      page.rs
      repository.rs
      aggregate.rs
  flux-derive/
    src/
      lib.rs
  flux-postgres/
    src/
      lib.rs
      repository.rs
      filter.rs
      entity.rs
  flux-mongodb/
    src/
      lib.rs
      repository.rs
      filter.rs
      entity.rs
  flux-sqlserver/
    src/
      lib.rs
```

Dependency direction:

```text
app
  -> flux
  -> flux-derive
  -> flux-postgres
  -> flux-mongodb

flux-postgres -> flux
flux-mongodb  -> flux
flux-derive   -> flux
flux          -> no database driver
```

This keeps the core small and avoids forcing Mongo projects to compile Postgres dependencies or SQL projects to compile Mongo dependencies.

## Proposed Dependencies

Postgres application:

```toml
[dependencies]
flux = { path = "crates/flux" }
flux-derive = { path = "crates/flux-derive" }
flux-postgres = { path = "crates/flux-postgres" }
tokio = { version = "1", features = ["full"] }
uuid = { version = "1", features = ["serde", "v4"] }
```

Mongo application:

```toml
[dependencies]
flux = { path = "crates/flux" }
flux-derive = { path = "crates/flux-derive" }
flux-mongodb = { path = "crates/flux-mongodb" }
tokio = { version = "1", features = ["full"] }
mongodb = "3"
```

## Proposed Interface Segregation

Flux should split repository behavior into small traits.

```rust
pub trait Entity: Send + Sync + Sized + Clone {
    type Id: EntityId;

    fn id(&self) -> &Self::Id;
}

pub trait EntityId: Clone + Send + Sync + Eq + 'static {}

impl EntityId for uuid::Uuid {}
impl EntityId for i16 {}
impl EntityId for i32 {}
impl EntityId for i64 {}
impl EntityId for u32 {}
impl EntityId for u64 {}
impl EntityId for String {}

pub enum PageRequest<Id> {
    Offset {
        limit: u32,
        offset: u64,
    },
    Cursor {
        limit: u32,
        after: Option<Id>,
    },
}

pub struct Page<T, Id> {
    pub items: Vec<T>,
    pub limit: u32,
    pub next_cursor: Option<Id>,
    pub total: Option<u64>,
}

#[async_trait]
pub trait ReadRepository<T: Entity>: Send + Sync {
    async fn find_by_id(&self, id: &T::Id) -> Result<T>;
    async fn find_page(&self, page: PageRequest<T::Id>) -> Result<Page<T, T::Id>>;
    async fn find_page_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>;
    async fn exists(&self, id: &T::Id) -> Result<bool>;
    async fn count(&self) -> Result<u64>;
}

#[async_trait]
pub trait WriteRepository<T: Entity>: Send + Sync {
    async fn insert(&self, entity: &T) -> Result<T>;
    async fn update(&self, entity: &T) -> Result<T>;
    async fn save(&self, entity: &T) -> Result<T>;
    async fn delete(&self, id: &T::Id) -> Result<bool>;
}

#[async_trait]
pub trait BulkRepository<T: Entity>: Send + Sync {
    async fn insert_many(&self, entities: &[T]) -> Result<Vec<T>>;
    async fn update_many(&self, entities: &[T]) -> Result<Vec<T>>;
    async fn save_many(&self, entities: &[T]) -> Result<Vec<T>>;
    async fn delete_many(&self, ids: &[T::Id]) -> Result<u64>;
}

#[async_trait]
pub trait RelationRepository<T: Entity>: Send + Sync {
    async fn find_by_foreign_key<K: EntityId>(
        &self,
        field: &str,
        value: &K,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>;
    async fn delete_by_foreign_key<K: EntityId>(&self, field: &str, value: &K) -> Result<u64>;
}

#[async_trait]
pub trait AggregateRepository<A: AggregateRoot>: Send + Sync {
    async fn find_graph_by_id(&self, id: &A::Id, includes: &[Include<A>]) -> Result<A>;
    async fn insert_graph(&self, aggregate: &A) -> Result<A>;
    async fn update_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A>;
    async fn save_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A>;
    async fn delete_graph(&self, id: &A::Id) -> Result<bool>;
}
```

Backend crates add their own ID support through local newtypes or conversion traits. A crate outside `flux` cannot implement `flux::EntityId` directly for `mongodb::bson::oid::ObjectId` because both the trait and the type are external to that adapter crate.

```rust
// flux-mongodb
pub struct MongoObjectId(pub mongodb::bson::oid::ObjectId);

impl flux::EntityId for MongoObjectId {}
```

The existing `Repository<T>` can remain as a convenience trait that combines the common traits.

The proposed write methods receive `&T` instead of `T`. That avoids moving aggregates out of the caller and lets graph operations reuse the same data when saving the root and its children.

```rust
pub trait Repository<T>:
    ReadRepository<T>
    + WriteRepository<T>
    + BulkRepository<T>
    + RelationRepository<T>
where
    T: Entity,
{
}
```

## Proposed Filter Syntax

Today `GenericFilter` can receive several conditions:

```rust
let filter = GenericFilter::<Order>::new()
    .with_condition("customer_name", "Alice")
    .with_condition("status", "open")
    .with_limit(20);
```

The generated SQL uses `AND`:

```sql
WHERE customer_name = $1 AND status = $2 LIMIT $3
```

`with_limit` exists today, but in the proposed API pagination should move to `PageRequest`.

That is useful, but too limited. The next version should support explicit operators and groups:

```rust
let filter = GenericFilter::<Order>::new()
    .eq("customer_name", "Alice")
    .in_list("status", ["open", "paid"])
    .gte("created_at", start_date)
    .lt("created_at", end_date)
    .order_by("created_at", OrderDirection::Desc);
```

For `OR`, the syntax should make the grouping visible:

```rust
let filter = GenericFilter::<Order>::new()
    .and(|q| q.eq("customer_name", "Alice"))
    .and_group(|q| {
        q.or(|q| q.eq("status", "open"))
            .or(|q| q.eq("status", "paid"))
    });
```

The internal representation should be backend-neutral:

```rust
pub enum FilterValue {
    Bool(bool),
    I16(i16),
    I32(i32),
    I64(i64),
    Uuid(uuid::Uuid),
    String(String),
    Null,
}

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
}
```

The Postgres adapter turns this into SQL and bind parameters. The Mongo adapter turns it into a BSON document.

Mongo-specific values should be adapted in `flux-mongodb`, not added as hard dependencies in `flux`. If `ObjectId` needs first-class filter support, the extension point should live in `flux-mongodb`.

## Proposed Entity Syntax

`Entity` belongs to `flux` and represents the domain identity. It should not know if the data comes from Postgres, Mongo, SQL Server, or another backend.

```rust
use flux::Entity;
use uuid::Uuid;

#[derive(Clone, Debug, Entity)]
pub struct Product {
    #[primary_key]
    pub product_oid: Uuid,
    pub name: String,
}
```

SQL mapping belongs to `flux-postgres`. The trait is defined by `flux-postgres`; the implementation can be generated by `flux-derive`.

```rust
use flux_derive::{Entity, SqlEntity};
use uuid::Uuid;

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "order_items"]
pub struct OrderItem {
    #[primary_key]
    pub item_oid: Uuid,
    pub order_oid: Uuid,
    pub product_name: String,
    pub quantity: i32,
}
```

Mongo mapping belongs to `flux-mongodb`. The trait is defined by `flux-mongodb`; the implementation can be generated by `flux-derive`.

```rust
use flux_derive::{Entity, MongoEntity};
use flux_mongodb::MongoObjectId;

#[derive(Clone, Debug, Entity, MongoEntity)]
#[collection_name = "customers"]
pub struct Customer {
    #[primary_key]
    pub id: MongoObjectId,
    pub name: String,
}
```

## Proposed Aggregate Syntax

`AggregateRoot` belongs to `flux` and maps relations. Relation fields are ignored by backend row/document mapping.

```rust
use flux_derive::{AggregateRoot, Entity, SqlEntity};
use uuid::Uuid;

#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "orders"]
pub struct Order {
    #[primary_key]
    pub order_oid: Uuid,
    pub customer_name: String,

    #[has_many(foreign_key = "order_oid", references = "order_oid", on_replace = "delete_missing")]
    pub items: Vec<OrderItem>,
}
```

This should generate metadata equivalent to:

- `items` is a child collection
- child table is `order_items`
- child foreign key is `order_oid`
- parent reference key is `orders.order_oid`
- `items` is not part of `INSERT INTO orders (...)`
- `items` is not part of `UPDATE orders SET ...`
- replacing an order can delete missing child rows

`flux-postgres` implements the SQL graph persistence. A future `flux-mongodb` implementation may store relations as references or embedded documents depending on Mongo mapping metadata.

## Repository Setup

Postgres:

```rust
use flux_postgres::PostgresRepository;

let order_repo = PostgresRepository::<Order>::new(pool.clone());
```

Mongo:

```rust
use flux_mongodb::MongoRepository;

let customer_repo = MongoRepository::<Customer>::new(database.clone());
```

## Select Syntax

Simple table read:

```rust
let order = order_repo.find_by_id(&order_id).await?;
```

Aggregate read with children:

```rust
let order = order_repo
    .find_graph_by_id(&order_id, &[Order::items()])
    .await?;
```

Aggregate read without children:

```rust
let order = order_repo.find_graph_by_id(&order_id, &[]).await?;
```

Paged read:

```rust
let page = order_repo
    .find_page(PageRequest::Cursor {
        limit: 50,
        after: None,
    })
    .await?;
```

Filtered paged read:

```rust
let filter = GenericFilter::<Order>::new()
    .with_condition("customer_name", "Alice")
    .with_order_by("customer_name", OrderDirection::Asc);

let page = order_repo
    .find_page_with_filter(
        filter,
        PageRequest::Cursor {
            limit: 50,
            after: None,
        },
    )
    .await?;
```

## Pagination Rules

The next API should not expose unbounded `find_all` as the normal path.

Current API:

```rust
let orders = order_repo.find_all().await?;
```

Problem:

- this can load the whole table into memory
- latency grows with table size
- it is hard to use safely in services and APIs

Proposed API:

```rust
let page = order_repo
    .find_page(PageRequest::Cursor {
        limit: 100,
        after: None,
    })
    .await?;
```

Next page:

```rust
let next_page = order_repo
    .find_page(PageRequest::Cursor {
        limit: 100,
        after: page.next_cursor,
    })
    .await?;
```

`find_all` should either be removed from the main trait or kept as an explicit unsafe/convenience method:

```rust
async fn find_all_unbounded(&self) -> Result<Vec<T>>;
```

The default repository interface should prefer:

- `find_page`
- `find_page_with_filter`
- future streaming APIs for batch jobs

Pagination should be separate from `GenericFilter`. Filters describe conditions. `PageRequest` describes how much data can be returned.

## Insert Syntax

Single row insert:

```rust
let saved_item = item_repo.insert(&item).await?;
```

Bulk insert:

```rust
let saved_items = item_repo.insert_many(&order.items).await?;
```

`&Vec<OrderItem>` coerces to `&[OrderItem]`, so the caller can pass a list or a slice.

Aggregate insert:

```rust
let saved_order = order_repo.insert_graph(&order).await?;
```

Expected behavior:

- starts one transaction
- inserts the root order
- inserts all `items` with one bulk operation per relation
- commits only if all operations succeed
- rolls back if any child insert fails

For Postgres, `insert_many` should generate one statement like:

```sql
INSERT INTO order_items (item_oid, order_oid, product_name, quantity)
VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)
RETURNING *
```

For very large lists, the repository can chunk internally.

## Update Syntax

Single row update:

```rust
let saved_order = order_repo.update(&order).await?;
```

Bulk update:

```rust
let saved_items = item_repo.update_many(&order.items).await?;
```

Aggregate update:

```rust
let saved_order = order_repo
    .update_graph(&order, GraphSaveMode::ReplaceChildren)
    .await?;
```

`GraphSaveMode::ReplaceChildren` means:

- update the root row
- upsert current child rows
- delete children that exist in the database but are missing from the aggregate

Other useful modes:

```rust
GraphSaveMode::AppendChildren
GraphSaveMode::UpsertChildren
GraphSaveMode::ReplaceChildren
```

## Save Syntax

Single row upsert:

```rust
let saved_item = item_repo.save(&item).await?;
```

Bulk upsert:

```rust
let saved_items = item_repo.save_many(&order.items).await?;
```

Aggregate upsert:

```rust
let saved_order = order_repo
    .save_graph(&order, GraphSaveMode::ReplaceChildren)
    .await?;
```

For Postgres, `save_many` should prefer:

```sql
INSERT INTO order_items (...)
VALUES (...)
ON CONFLICT (item_oid)
DO UPDATE SET ...
RETURNING *
```

## Delete Syntax

Single row delete:

```rust
let deleted = order_repo.delete(&order_id).await?;
```

Bulk delete:

```rust
let deleted_rows = item_repo.delete_many(&item_ids).await?;
```

Delete by relation:

```rust
let deleted_rows = item_repo
    .delete_by_foreign_key("order_oid", &order_id)
    .await?;
```

Aggregate delete:

```rust
let deleted = order_repo.delete_graph(&order_id).await?;
```

Aggregate delete should obey relation metadata. Child deletes should happen only when the relation explicitly allows cascade.

```rust
#[has_many(foreign_key = "order_oid", references = "order_oid", cascade_delete)]
pub items: Vec<OrderItem>,
```

## One To One

```rust
#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "users"]
pub struct User {
    #[primary_key]
    pub user_oid: Uuid,
    pub name: String,

    #[has_one(foreign_key = "user_oid", references = "user_oid")]
    pub profile: Option<UserProfile>,
}

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "user_profiles"]
pub struct UserProfile {
    #[primary_key]
    pub profile_oid: Uuid,
    pub user_oid: Uuid,
    pub bio: String,
}
```

Usage:

```rust
let user = user_repo
    .find_graph_by_id(&user_id, &[User::profile()])
    .await?;

let saved_user = user_repo.save_graph(&user, GraphSaveMode::UpsertChildren).await?;
```

## One To Many

```rust
#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "orders"]
pub struct Order {
    #[primary_key]
    pub order_oid: Uuid,
    pub customer_name: String,

    #[has_many(foreign_key = "order_oid", references = "order_oid", on_replace = "delete_missing")]
    pub items: Vec<OrderItem>,
}
```

Usage:

```rust
let order = order_repo
    .find_graph_by_id(&order_id, &[Order::items()])
    .await?;

let saved_order = order_repo
    .save_graph(&order, GraphSaveMode::ReplaceChildren)
    .await?;
```

No application-level loop is needed. The aggregate repository should call `insert_many`, `save_many`, or relation replacement internally.

## Many To Many

For a plain join table without extra fields:

```rust
#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "students"]
pub struct Student {
    #[primary_key]
    pub student_oid: Uuid,
    pub name: String,

    #[many_to_many(
        join_table = "enrollments",
        source_key = "student_oid",
        target_key = "course_oid",
        target_primary_key = "course_oid",
        on_replace = "delete_missing"
    )]
    pub courses: Vec<Course>,
}

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "courses"]
pub struct Course {
    #[primary_key]
    pub course_oid: Uuid,
    pub title: String,
}
```

Usage:

```rust
let student = student_repo
    .find_graph_by_id(&student_id, &[Student::courses()])
    .await?;

let saved_student = student_repo
    .save_graph(&student, GraphSaveMode::ReplaceChildren)
    .await?;
```

For a join table with extra fields, model the join row as its own entity.

```rust
#[derive(Clone, Debug, Entity, SqlEntity, AggregateRoot)]
#[table_name = "students"]
pub struct Student {
    #[primary_key]
    pub student_oid: Uuid,
    pub name: String,

    #[has_many(foreign_key = "student_oid", references = "student_oid")]
    pub enrollments: Vec<Enrollment>,
}

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "enrollments"]
pub struct Enrollment {
    #[primary_key]
    pub enrollment_oid: Uuid,
    pub student_oid: Uuid,
    pub course_oid: Uuid,
    pub enrolled_at: String,
}
```

This is more explicit and avoids hiding important data on the join table.

## Transaction Rules

Aggregate operations must be transactional.

```rust
let saved_order = order_repo
    .save_graph(&order, GraphSaveMode::ReplaceChildren)
    .await?;
```

Required behavior:

- start transaction
- save root
- save each relation using bulk operations
- apply delete rules for missing children
- commit on success
- rollback on failure

## ID Model

Flux should not require `Uuid`.

The core entity contract should use an associated type:

```rust
pub trait Entity: Send + Sync + Sized + Clone {
    type Id: EntityId;

    fn id(&self) -> &Self::Id;
}
```

That allows UUID, integers, strings, and Mongo IDs through adapter-owned newtypes:

```rust
#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "products"]
pub struct Product {
    #[primary_key]
    pub product_oid: Uuid,
    pub name: String,
}

#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "categories"]
pub struct Category {
    #[primary_key]
    pub category_id: i64,
    pub name: String,
}

#[derive(Clone, Debug, Entity, MongoEntity)]
#[collection_name = "customers"]
pub struct Customer {
    #[primary_key]
    pub id: flux_mongodb::MongoObjectId,
    pub name: String,
}
```

For SQL repositories, `T::Id` must be bindable as a SQL parameter. For Mongo repositories, `T::Id` must be convertible to BSON.

For relational databases, prefer `i32`, `i64`, `Uuid`, or `String` as IDs. Unsigned integers can exist in the core trait, but support depends on the target database and driver.

That means Flux should split backend-specific mapping:

```rust
pub trait SqlEntity: Entity {
    fn table_name() -> &'static str;
    fn primary_key() -> &'static str;
    fn from_row(row: tokio_postgres::Row) -> Result<Self>;
    fn to_insert_params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)>;
    fn to_update_params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)>;
}
```

`flux-mongodb` owns Mongo document mapping:

```rust
pub trait MongoEntity: Entity {
    fn collection_name() -> &'static str;
    fn from_document(document: mongodb::bson::Document) -> Result<Self>;
    fn to_document(&self) -> mongodb::bson::Document;
}
```

The first aggregate implementation can still assume client-generated IDs.

```rust
let order_oid = Uuid::new_v4();

let order = Order {
    order_oid,
    customer_name: "Alice".to_string(),
    items: vec![
        OrderItem {
            item_oid: Uuid::new_v4(),
            order_oid,
            product_name: "Keyboard".to_string(),
            quantity: 1,
        },
    ],
};
```

Database-generated IDs can be supported later, but they require the aggregate repository to copy the generated parent key into child foreign keys before bulk insert.

Implemented syntax for SQL-generated IDs:

```rust
#[derive(Clone, Debug, Entity, SqlEntity)]
#[table_name = "products"]
pub struct Product {
    #[primary_key]
    #[generated_id]
    pub product_id: i64,
    pub name: String,
}
```

For derived entities, `#[generated_id]` makes `has_id()` compare the primary key against `Default::default()`. Inserts with no ID omit the primary key column and use the row returned by the database to populate the entity ID. Aggregate inserts use the saved root ID when persisting relation fields, and generated relation metadata copies the parent key into `#[has_one]` and `#[has_many]` child foreign keys before saving children.

## Recommended Implementation Order

1. Split the workspace into `flux`, `flux-derive`, `flux-postgres`, and future adapter crates
2. Move Postgres-specific traits and repository code into `flux-postgres`
3. Keep `flux` limited to core traits, errors, filters, pagination, and repository contracts
4. Replace fixed `Uuid` repository signatures with `T::Id`
5. Fix entity ID generation in `flux-derive`
6. Replace unbounded `find_all` with paged reads
7. Move `GenericFilter` toward a backend-neutral filter AST
8. Add `BulkRepository<T>`
9. Implement Postgres `insert_many`
10. Implement Postgres `save_many` with `ON CONFLICT`
11. Add relation attributes to `flux-derive`
12. Generate `AggregateRoot` metadata
13. Add `AggregateRepository<A>`
14. Implement `insert_graph`
15. Implement `save_graph`
16. Add many-to-many support
17. Add `flux-mongodb` repository traits and adapter implementation

## API Position

Flux should not try to become a full ORM immediately.

The stronger path is:

- lightweight row repository
- explicit bulk repository
- aggregate repository for transactional graph persistence
- derive macros for metadata
- no hidden lazy loading
- no automatic N+1 queries

This gives good ergonomics for domain aggregates while keeping the database behavior visible.
