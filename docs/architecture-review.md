# Flux Architecture Review

This document consolidates a technical review of `docs/usage.md` from multiple engineering perspectives: junior developer, mid-level developer, senior developer, and architect.

## Executive Summary

The direction proposed in `usage.md` is technically sound: Flux should remain a lightweight repository library, not become a full ORM immediately.

The core decision is correct:

- keep row persistence separate from aggregate persistence
- add bulk operations explicitly
- make pagination the default read path
- replace fixed `Uuid` IDs with `T::Id`
- move filters toward a backend-neutral representation

The main risk is implementation scope. Generic IDs, pagination, filter AST, bulk writes, aggregate graph persistence, and Mongo support are all valid, but they should not be implemented as one large refactor.

Recommended strategy:

1. Stabilize the table-level repository contract
2. Add safe pagination and generic IDs
3. Add bulk writes
4. Add aggregate metadata and graph persistence
5. Add Mongo only after the core abstractions stop leaking Postgres types

## Junior Developer Review

The proposed syntax is readable and easy to understand.

Positive points:

- `insert_many(&items)` is obvious
- `save_graph(&order, GraphSaveMode::ReplaceChildren)` communicates intent
- `#[has_many(...)]` makes the relation visible on the aggregate
- `PageRequest::Cursor { limit, after }` makes pagination explicit

Potential confusion:

- `Entity`, `SqlEntity`, `MongoEntity`, `AggregateRoot`, and `Repository` may feel like too many traits at first
- `GraphSaveMode` needs very clear documentation and examples
- `find_page_with_filter(filter, page)` is clear, but the difference between filter and pagination must stay consistent

Recommendation:

- keep examples small and compileable
- provide one complete example for `Product`
- provide one complete aggregate example for `Order` and `OrderItem`

## Mid-Level Developer Review

The interface segregation is the strongest part of the proposal.

Recommended trait boundaries:

```rust
ReadRepository<T>
WriteRepository<T>
BulkRepository<T>
RelationRepository<T>
AggregateRepository<A>
```

This avoids forcing every backend and use case to implement graph persistence immediately.

Important corrections:

- `find_all` should not remain in the primary trait
- `GenericFilter` should not own pagination long term
- `Repository<T>` should be a convenience alias/combined trait, not the main design unit
- `WriteRepository` should receive `&T` instead of `T`

Open concern:

- returning `Vec<T>` from `insert_many` is ergonomic, but for very large batches this can still allocate heavily

Recommendation:

- keep `insert_many` returning `Vec<T>` for the first version
- add chunking internally
- later add a lower-level API returning affected row count or stream if needed

## Senior Developer Review

The current codebase leaks PostgreSQL into core traits.

Examples:

- `Entity` depends on `tokio_postgres::Row`
- `Entity` depends on `tokio_postgres::types::ToSql`
- `GenericFilter` stores `Arc<dyn ToSql + Sync + Send>`
- `Repository` uses `Uuid` directly

This prevents clean Mongo support and makes SQL Server harder than necessary.

Recommended correction:

- keep `Entity` backend-neutral
- move row conversion into `SqlEntity`
- move document conversion into `MongoEntity`
- move SQL bind values into the SQL adapter layer
- make `FilterValue` backend-neutral

Suggested core shape:

```rust
pub trait Entity: Send + Sync + Sized + Clone {
    type Id: EntityId;

    fn entity_name() -> &'static str;
    fn id(&self) -> &Self::Id;
}
```

Suggested SQL shape:

```rust
pub trait SqlEntity: Entity {
    fn table_name() -> &'static str;
    fn primary_key() -> &'static str;
    fn fields() -> Vec<&'static str>;
    fn from_row(row: tokio_postgres::Row) -> Result<Self>;
    fn to_insert_params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)>;
    fn to_update_params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)>;
}
```

Primary technical risk:

- trying to support SQL, Mongo, aggregates, and bulk writes with the same metadata model too early

Recommendation:

- design backend-neutral public traits
- implement only Postgres first
- leave Mongo as a future adapter once abstractions prove stable

## Architect Review

Flux should position itself as a repository and aggregate persistence toolkit, not a general ORM.

The architecture should optimize for:

- explicit database behavior
- predictable SQL generation
- no hidden lazy loading
- no accidental N+1 queries
- clear transaction boundaries
- composable repository traits

The aggregate model is the correct abstraction for `Order.items`.

Good aggregate syntax:

```rust
#[derive(Clone, Debug, Entity, AggregateRoot)]
#[table_name = "orders"]
pub struct Order {
    #[primary_key]
    pub order_oid: Uuid,
    pub customer_name: String,

    #[has_many(foreign_key = "order_oid", references = "order_oid", on_replace = "delete_missing")]
    pub items: Vec<OrderItem>,
}
```

The important semantic decision:

- `insert(&order)` saves only the `orders` row
- `insert_graph(&order)` saves the aggregate graph

This avoids surprising behavior.

## Key Decisions

Decision: `find_all` should not be part of the primary read interface.

Reason: unbounded reads can exhaust memory and create unpredictable latency.

Decision: pagination should be explicit through `PageRequest`.

Reason: filtering and result-size control are separate responsibilities.

Decision: IDs should be generic through `T::Id`.

Reason: `Uuid` is not universal. SQL projects often use `i32`/`i64`; Mongo uses `ObjectId`.

Decision: bulk writes should be first-class.

Reason: application-level loops with `await` per item create avoidable latency.

Decision: aggregate persistence should be separate from row persistence.

Reason: graph persistence requires transaction orchestration, relation metadata, cascade rules, and bulk operations.

Decision: Mongo support should not be added before core traits are backend-neutral.

Reason: adding Mongo too early will either leak SQL concepts into Mongo or force a broad redesign.

## Recommended Public API

The recommended API surface for the next phase:

```rust
pub trait Entity {
    type Id: EntityId;

    fn id(&self) -> &Self::Id;
}

pub trait ReadRepository<T: Entity> {
    async fn find_by_id(&self, id: &T::Id) -> Result<T>;
    async fn find_page(&self, page: PageRequest<T::Id>) -> Result<Page<T, T::Id>>;
    async fn find_page_with_filter(
        &self,
        filter: GenericFilter<T>,
        page: PageRequest<T::Id>,
    ) -> Result<Page<T, T::Id>>;
}

pub trait WriteRepository<T: Entity> {
    async fn insert(&self, entity: &T) -> Result<T>;
    async fn update(&self, entity: &T) -> Result<T>;
    async fn save(&self, entity: &T) -> Result<T>;
    async fn delete(&self, id: &T::Id) -> Result<bool>;
}

pub trait BulkRepository<T: Entity> {
    async fn insert_many(&self, entities: &[T]) -> Result<Vec<T>>;
    async fn save_many(&self, entities: &[T]) -> Result<Vec<T>>;
    async fn delete_many(&self, ids: &[T::Id]) -> Result<u64>;
}

pub trait AggregateRepository<A: AggregateRoot> {
    async fn find_graph_by_id(&self, id: &A::Id, includes: &[Include<A>]) -> Result<A>;
    async fn save_graph(&self, aggregate: &A, mode: GraphSaveMode) -> Result<A>;
}
```

## Delivery Plan

Phase 1: Repository safety

- introduce `Entity::Id`
- replace `Uuid` repository signatures with `T::Id`
- add `PageRequest` and `Page`
- replace primary `find_all` usage with `find_page`

Phase 2: Filter redesign

- replace `ToSql` inside `GenericFilter` with `FilterValue`
- support `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in_list`, `like`, `is_null`
- support explicit `AND` and `OR` groups
- implement Postgres filter rendering

Phase 3: Bulk operations

- add `BulkRepository<T>`
- implement Postgres `insert_many`
- implement Postgres `save_many` with `ON CONFLICT`
- add chunking for large batches

Phase 4: Aggregate persistence

- add relation attributes to `flux-derive`
- generate aggregate metadata
- implement `AggregateRepository`
- implement `insert_graph` and `save_graph` with transactions
- support `has_one` and `has_many`

Phase 5: Many-to-many and additional backends

- support plain join tables
- support explicit join entities with extra fields
- introduce `MongoEntity`
- implement Mongo repository adapter

## Final Recommendation

Proceed with the proposal, but keep the implementation staged.

The best next technical step is not aggregate persistence yet. The best next step is to fix the foundation:

1. make `Entity` ID generic
2. remove unbounded reads from the main API
3. separate backend-neutral entity/filter contracts from Postgres-specific mapping

After that, bulk operations and aggregate persistence will fit naturally without forcing a redesign.
