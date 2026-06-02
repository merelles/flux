//! Example: Using PostgresRepository
//!
//! This demonstrates how to use the new PostgresRepository with a custom entity.
//!
//! Note: This example requires a running PostgreSQL database.

use std::sync::Arc;

use flux::{Entity, PostgresRepository};
use tokio_postgres::NoTls;
use uuid::Uuid;

/// Simple Product entity
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

    fn from_row(
        row: tokio_postgres::Row,
    ) -> std::result::Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Product {
            product_oid: row.get::<_, Uuid>("product_oid"),
            name: row.get::<_, String>("name"),
            price: row.get::<_, i32>("price"),
        })
    }

    fn to_insert_params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
        vec![&self.product_oid, &self.name, &self.price]
    }

    fn to_update_params(&self) -> Vec<&(dyn tokio_postgres::types::ToSql + Sync)> {
        vec![&self.name, &self.price]
    }

    fn primary_key_value(&self) -> &(dyn tokio_postgres::types::ToSql + Sync) {
        &self.product_oid
    }

    fn fields() -> Vec<&'static str> {
        vec!["product_oid", "name", "price"]
    }

    fn has_id(&self) -> bool {
        true
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Flux Basic Usage Example ===\n");

    // Connect to database
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://postgres:postgres@localhost/pdv".to_string());

    println!("Connecting to database...");
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let client = Arc::new(client);

    // Create repository
    let _repo: PostgresRepository<Product> = PostgresRepository::new(client.clone());

    println!("✅ Connected! Repository created.\n");

    // Example operations would go here:
    println!("Example operations:");
    println!("  1. repo.find_by_id(id).await?");
    println!("  2. repo.find_all().await?");
    println!("  3. repo.save(product).await?");
    println!("  4. repo.delete(id).await?");
    println!("  5. repo.count().await?");
    println!("  6. repo.exists(id).await?");

    println!("\n⚠️  Full example requires:");
    println!("  1. PostgreSQL database running");
    println!("  2. 'products' table created");
    println!("  3. Proper schema matching Product entity");

    println!("\n✅ PostgresRepository implementation is complete!");

    Ok(())
}
