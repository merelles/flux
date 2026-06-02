# Flux

Flux is a Rust workspace for backend-neutral repository contracts and database adapters.

The project is intentionally split into a small core crate plus adapter crates:

- `flux`: core traits, filters, pagination, errors, repository contracts, aggregate metadata
- `flux-derive`: derive macros for core entity identity
- `flux-postgres`: PostgreSQL adapter, SQL entity mapping, SQL filter rendering, CRUD and bulk writes
- `flux-mongodb`: MongoDB adapter, BSON entity mapping, BSON filter rendering, CRUD and basic bulk operations

Flux is not trying to be a full ORM. The goal is to keep database behavior explicit while giving applications a clean repository API.

## Status

Implemented:

- backend-neutral `Entity` with generic `Entity::Id`
- `ReadRepository`, `WriteRepository`, `BulkRepository`, `RelationRepository`
- `PageRequest` and `Page`
- backend-neutral `GenericFilter` AST
- Postgres adapter crate
- MongoDB adapter crate
- basic aggregate metadata types

In progress / next steps:

- `SqlEntity` derive macro
- `MongoEntity` derive macro
- `AggregateRoot` derive macro
- `AggregateRepository` implementations
- Mongo bulk upsert using native bulk write operations
- many-to-many graph persistence

## Workspace

```text
flux/
  Cargo.toml
  flux/
    src/
      aggregate.rs
      entity.rs
      error.rs
      filter.rs
      page.rs
      repository.rs
  flux-derive/
    src/
      lib.rs
  flux-postgres/
    src/
      entity.rs
      filter.rs
      repository.rs
  flux-mongodb/
    src/
      entity.rs
      filter.rs
      repository.rs
  docs/
    usage.md
    architecture-review.md
```

## Architecture Flow

```text
Application code
      |
      v
Domain structs
      |
      +--------------------+
      |                    |
      v                    v
flux::Entity        flux::AggregateRoot
      |                    |
      v                    v
Repository traits   Aggregate metadata
      |
      +-----------------------------+
      |                             |
      v                             v
flux-postgres                 flux-mongodb
      |                             |
      v                             v
PostgreSQL                    MongoDB
```

Adapter dependency direction:

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

Read flow:

```text
repo.find_all_with_filter(filter, page)
      |
      v
GenericFilter AST + PageRequest
      |
      +----------------------------+
      |                            |
      v                            v
SQL renderer                  BSON renderer
      |                            |
      v                            v
SELECT ... LIMIT ...          find(...).limit(...)
      |                            |
      v                            v
Page<T, T::Id>                Page<T, T::Id>
```

Write flow:

```text
repo.save_many(&items)
      |
      v
BulkRepository<T>
      |
      +----------------------------+
      |                            |
      v                            v
Postgres INSERT ...           Mongo insert_many /
ON CONFLICT ...               replace_one upsert loop
      |
      v
Vec<T>
```

## Install

Use the core only:

```toml
[dependencies]
flux = { path = "flux" }
```

Use Postgres:

```toml
[dependencies]
flux = { path = "flux" }
flux-postgres = { path = "flux-postgres" }
tokio-postgres = "0.7"
tokio = { version = "1", features = ["full"] }
uuid = { version = "1", features = ["v4", "serde"] }
```

Use MongoDB:

```toml
[dependencies]
flux = { path = "flux" }
flux-mongodb = { path = "flux-mongodb" }
mongodb = "3"
tokio = { version = "1", features = ["full"] }
```

Use derive macros:

```toml
[dependencies]
flux = { path = "flux" }
flux-derive = { path = "flux-derive" }
```

## Core Entity

`flux::Entity` only describes identity. It does not know about tables, columns, collections, rows, documents, Postgres, or MongoDB.

```rust
use flux::{Entity, Uuid};

#[derive(Clone, Debug)]
pub struct Product {
    pub product_oid: Uuid,
    pub name: String,
    pub price: i32,
}

impl Entity for Product {
    type Id = Uuid;

    fn id(&self) -> &Self::Id {
        &self.product_oid
    }

    fn has_id(&self) -> bool {
        true
    }
}
```

With `flux-derive`, the identity implementation can be generated:

```rust
use flux::Uuid;
use flux_derive::Entity;

#[derive(Clone, Debug, Entity)]
pub struct Product {
    #[primary_key]
    pub product_oid: Uuid,
    pub name: String,
    pub price: i32,
}
```

## Filters

`GenericFilter<T>` is backend-neutral. It is an AST. Adapters decide how to render it.

```rust
use flux::{GenericFilter, OrderDirection};

let filter = GenericFilter::<Product>::new()
    .eq("name", "Keyboard")
    .gte("price", 100)
    .in_list("status", ["active", "pending"])
    .order_by("price", OrderDirection::Desc);
```

Grouped conditions:

```rust
let filter = GenericFilter::<Product>::new()
    .and(|query| query.eq("category", "hardware"))
    .and_group(|query| {
        query
            .or(|query| query.eq("status", "active"))
            .or(|query| query.eq("status", "pending"))
    });
```

Null checks:

```rust
let filter = GenericFilter::<Product>::new()
    .is_not_null("name")
    .is_null("deleted_at");
```

## Pagination

Flux avoids unbounded `find_all` in the core API. Reads should be paged.

Repository methods are provided by traits. Import the traits you use:

```rust
use flux::{BulkRepository, ReadRepository, RelationRepository, WriteRepository};
```

Cursor pagination:

```rust
use flux::PageRequest;

let page = repo
    .find_page(PageRequest::cursor(50, None))
    .await?;
```

Next page:

```rust
let next_page = repo
    .find_page(PageRequest::cursor(50, page.next_cursor))
    .await?;
```

Offset pagination:

```rust
let page = repo
    .find_page(PageRequest::offset(100, 200))
    .await?;
```

Filtered page:

```rust
let filter = GenericFilter::<Product>::new()
    .gte("price", 100)
    .order_by("price", OrderDirection::Asc);

let page = repo
    .find_all_with_filter(filter, PageRequest::cursor(50, None))
    .await?;
```

## Postgres Example

`flux-postgres` owns SQL mapping through `SqlEntity`.

Manual mapping today:

```rust
use flux::{Entity, Result, Uuid};
use flux_postgres::SqlEntity;
use tokio_postgres::{types::ToSql, Row};

#[derive(Clone, Debug)]
pub struct Product {
    pub product_oid: Uuid,
    pub name: String,
    pub price: i32,
}

impl Entity for Product {
    type Id = Uuid;

    fn id(&self) -> &Self::Id {
        &self.product_oid
    }

    fn has_id(&self) -> bool {
        true
    }
}

impl SqlEntity for Product {
    fn table_name() -> &'static str {
        "products"
    }

    fn primary_key() -> &'static str {
        "product_oid"
    }

    fn fields() -> &'static [&'static str] {
        &["product_oid", "name", "price"]
    }

    fn from_row(row: Row) -> Result<Self> {
        Ok(Self {
            product_oid: row.try_get("product_oid")
                .map_err(|err| flux::RepositoryError::Backend(err.to_string()))?,
            name: row.try_get("name")
                .map_err(|err| flux::RepositoryError::Backend(err.to_string()))?,
            price: row.try_get("price")
                .map_err(|err| flux::RepositoryError::Backend(err.to_string()))?,
        })
    }

    fn to_insert_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        vec![&self.product_oid, &self.name, &self.price]
    }

    fn to_update_params(&self) -> Vec<&(dyn ToSql + Sync)> {
        vec![&self.name, &self.price]
    }

    fn primary_key_param(&self) -> &(dyn ToSql + Sync) {
        &self.product_oid
    }
}
```

Create a Postgres repository:

```rust
use std::sync::Arc;

use flux_postgres::PostgresRepository;
use tokio_postgres::NoTls;

let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;

tokio::spawn(async move {
    if let Err(err) = connection.await {
        eprintln!("postgres connection error: {err}");
    }
});

let repo = PostgresRepository::<Product>::new(Arc::new(client));
```

Postgres read:

```rust
let product = repo.find_by_id(&product_id).await?;
```

Postgres insert:

```rust
let product = Product {
    product_oid: Uuid::new_v4(),
    name: "Keyboard".to_string(),
    price: 120,
};

let saved = repo.insert(&product).await?;
```

Postgres bulk insert:

```rust
let products = vec![
    Product {
        product_oid: Uuid::new_v4(),
        name: "Keyboard".to_string(),
        price: 120,
    },
    Product {
        product_oid: Uuid::new_v4(),
        name: "Mouse".to_string(),
        price: 80,
    },
];

let saved = repo.insert_many(&products).await?;
```

Postgres upsert many:

```rust
let saved = repo.save_many(&products).await?;
```

Postgres delete many:

```rust
let ids = products
    .iter()
    .map(|product| product.product_oid)
    .collect::<Vec<_>>();

let deleted = repo.delete_many(&ids).await?;
```

## MongoDB Example

`flux-mongodb` owns BSON mapping through `MongoEntity`.

```rust
use flux::{Entity, Result};
use flux_mongodb::{MongoEntity, MongoObjectId};
use mongodb::bson::{doc, oid::ObjectId, Document};

#[derive(Clone, Debug)]
pub struct Customer {
    pub id: MongoObjectId,
    pub name: String,
    pub email: String,
}

impl Entity for Customer {
    type Id = MongoObjectId;

    fn id(&self) -> &Self::Id {
        &self.id
    }

    fn has_id(&self) -> bool {
        true
    }
}

impl MongoEntity for Customer {
    fn collection_name() -> &'static str {
        "customers"
    }

    fn from_document(document: Document) -> Result<Self> {
        let id = document
            .get_object_id("_id")
            .map_err(|err| flux::RepositoryError::Backend(err.to_string()))?;

        let name = document
            .get_str("name")
            .map_err(|err| flux::RepositoryError::Backend(err.to_string()))?
            .to_string();

        let email = document
            .get_str("email")
            .map_err(|err| flux::RepositoryError::Backend(err.to_string()))?
            .to_string();

        Ok(Self {
            id: MongoObjectId(id),
            name,
            email,
        })
    }

    fn to_document(&self) -> Result<Document> {
        Ok(doc! {
            "_id": self.id.0,
            "name": &self.name,
            "email": &self.email,
        })
    }
}
```

Create a Mongo repository:

```rust
use flux_mongodb::MongoRepository;
use mongodb::Client;

let client = Client::with_uri_str("mongodb://localhost:27017").await?;
let database = client.database("app");
let repo = MongoRepository::<Customer>::new(database);
```

Mongo insert:

```rust
let customer = Customer {
    id: MongoObjectId(ObjectId::new()),
    name: "Alice".to_string(),
    email: "alice@example.com".to_string(),
};

let saved = repo.insert(&customer).await?;
```

Mongo paged read:

```rust
let page = repo
    .find_page(PageRequest::cursor(50, None))
    .await?;
```

Mongo filtered read:

```rust
let filter = GenericFilter::<Customer>::new()
    .eq("name", "Alice");

let page = repo
    .find_all_with_filter(filter, PageRequest::cursor(50, None))
    .await?;
```

Mongo insert many:

```rust
let customers = vec![
    Customer {
        id: MongoObjectId(ObjectId::new()),
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
    },
    Customer {
        id: MongoObjectId(ObjectId::new()),
        name: "Bob".to_string(),
        email: "bob@example.com".to_string(),
    },
];

let saved = repo.insert_many(&customers).await?;
```

## Aggregate Syntax

The core crate has metadata types for aggregates, but graph persistence is still a next step for adapter crates.

Target syntax:

```rust
use flux::Uuid;

#[derive(Clone, Debug)]
pub struct Order {
    pub order_oid: Uuid,
    pub customer_name: String,
    pub items: Vec<OrderItem>,
}

#[derive(Clone, Debug)]
pub struct OrderItem {
    pub item_oid: Uuid,
    pub order_oid: Uuid,
    pub product_name: String,
    pub quantity: i32,
}
```

The intended aggregate save flow:

```text
order_repo.save_graph(&order, GraphSaveMode::ReplaceChildren)
      |
      v
begin transaction
      |
      v
save orders row
      |
      v
save_many(order.items)
      |
      v
delete missing items if requested
      |
      v
commit
```

Until `AggregateRepository` is implemented in adapters, use explicit repositories:

```rust
let saved_order = order_repo.insert(&order).await?;
let saved_items = item_repo.insert_many(&order.items).await?;
```

## Development

Format:

```text
cargo fmt --all
```

Check:

```text
cargo check --workspace --all-targets
```

Test:

```text
cargo test --workspace --all-targets
```

## Design Notes

Important rules:

- `flux` must not depend on database drivers
- adapters depend on `flux`
- filters are backend-neutral
- pagination is explicit
- bulk writes are first-class
- aggregate graph persistence must be transactional
- `insert(&entity)` saves one row/document
- `save_graph(&aggregate)` saves a full aggregate graph

More detail:

- [Usage guide](docs/usage.md)
- [Architecture review](docs/architecture-review.md)

## License

MIT. See [LICENSE](LICENSE).
