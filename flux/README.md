# Flux

Generic repository pattern with support for multiple databases using Cargo features.

## Features

### ✅ PostgreSQL (Default)

Fully functional PostgreSQL implementation using `tokio-postgres`.

### 🚧 SQL Server

Placeholder for Tiberius-based SQL Server implementation (ready for development).

### 🔮 Future: Cassandra

Can be added when needed.

## Usage

### PostgreSQL (Default)

```toml
[dependencies]
flux = { path = "crates/flux" }
```

```rust
use flux::{Entity, PostgresRepository, Uuid};
use tokio_postgres::{Client, NoTls};
use std::sync::Arc;

// Define your entity
#[derive(Clone, Debug)]
pub struct Product {
    pub product_oid: Uuid,
    pub name: String,
    pub price: i32,
}

impl Entity for Product {
    fn table_name() -> &'static str {
        "products"
    }

    fn primary_key() -> &'static str {
        "product_oid"
    }

    // ... implement other trait methods
}

// Use the repository
let repo = PostgresRepository::<Product>::new(client);
let product = repo.find_by_id(id).await?;
```

### SQL Server (Coming Soon)

```toml
[dependencies]
flux = { path = "crates/flux", features = ["sqlserver"] }
```

```rust
use flux::{Entity, SqlServerRepository};

let repo = SqlServerRepository::<Product>::new(client);
let product = repo.find_by_id(id).await?;
```

## Architecture

```
crates/flux/
├── src/
│   ├── lib.rs              # Public API
│   ├── entity.rs           # Entity trait
│   ├── repository.rs       # Repository trait
│   ├── error.rs            # RepositoryError
│   ├── filter.rs           # Filter + GenericFilter
│   ├── specification.rs    # Specification pattern
│   ├── postgres/           # PostgreSQL implementation
│   │   └── repository.rs   # PostgresRepository<T>
│   └── sqlserver/          # SQL Server implementation
│       └── repository.rs   # SqlServerRepository<T>
```

## Logging Standard

All operations use standardized log symbols:

- `[INFO]` - Information, status, details
- `[OK]` - Success
- `[WARN]` - Warning
- `[CLEAN]` - Deletion/cleanup operations

## Status

- ✅ PostgreSQL: Fully implemented
- 🚧 SQL Server: Scaffold ready, implementation pending
- 🔮 Cassandra: Not started

## Development

See `examples/basic_usage.rs` for a complete example.

## License

MIT

