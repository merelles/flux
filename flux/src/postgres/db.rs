use std::sync::Arc;

use tokio_postgres::{Client, NoTls};

pub async fn create_connection(
    database_url: &str,
) -> Result<Arc<Client>, Box<dyn std::error::Error>> {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("[ERR] Database connection error: {}", e);
        }
    });

    println!("[INFO] Database connected successfully");
    Ok(Arc::new(client))
}
