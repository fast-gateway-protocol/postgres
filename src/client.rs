//! PostgreSQL client with connection pooling.

use anyhow::{Context, Result};
use deadpool_postgres::{Config, Pool, Runtime};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::types::ToSql;
use tokio_postgres::NoTls;

/// Connection configuration for PostgreSQL.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ConnectionConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: String,
    pub ssl: bool,
}

impl ConnectionConfig {
    /// Parse a DATABASE_URL into ConnectionConfig.
    pub fn from_url(url: &str) -> Result<Self> {
        let parsed = url::Url::parse(url).context("Invalid DATABASE_URL format")?;

        Ok(Self {
            host: parsed.host_str().unwrap_or("localhost").to_string(),
            port: parsed.port().unwrap_or(5432),
            user: parsed.username().to_string(),
            password: parsed.password().map(|s| s.to_string()),
            database: parsed.path().trim_start_matches('/').to_string(),
            ssl: parsed.query_pairs().any(|(k, v)| k == "sslmode" && v != "disable"),
        })
    }

    /// Build config from libpq-style environment variables.
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            host: std::env::var("PGHOST").unwrap_or_else(|_| "localhost".into()),
            port: std::env::var("PGPORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(5432),
            user: std::env::var("PGUSER").unwrap_or_else(|_| "postgres".into()),
            password: std::env::var("PGPASSWORD").ok(),
            database: std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".into()),
            ssl: std::env::var("PGSSLMODE")
                .map(|m| m != "disable")
                .unwrap_or(false),
        })
    }
}

/// PostgreSQL client with connection pooling.
pub struct PostgresClient {
    pool: Pool,
    config: ConnectionConfig,
}

impl PostgresClient {
    /// Create a new PostgreSQL client with connection pool.
    pub async fn new(config: ConnectionConfig) -> Result<Self> {
        let mut cfg = Config::new();
        cfg.host = Some(config.host.clone());
        cfg.port = Some(config.port);
        cfg.user = Some(config.user.clone());
        cfg.password = config.password.clone();
        cfg.dbname = Some(config.database.clone());

        // Create pool - using NoTls for simplicity, can add TLS support later
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1), NoTls)
            .context("Failed to create connection pool")?;

        Ok(Self { pool, config })
    }

    /// Get connection info for health checks.
    pub fn connection_info(&self) -> String {
        format!(
            "{}@{}:{}/{}",
            self.config.user, self.config.host, self.config.port, self.config.database
        )
    }

    /// Test database connectivity.
    pub async fn ping(&self) -> Result<bool> {
        let client = self.pool.get().await.context("Failed to get connection")?;
        let row = client.query_one("SELECT 1", &[]).await?;
        let val: i32 = row.get(0);
        Ok(val == 1)
    }

    /// Execute a SQL query and return results as JSON.
    pub async fn query(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Value> {
        let client = self.pool.get().await.context("Failed to get connection")?;
        let stmt = client.prepare(sql).await.context("Failed to prepare query")?;
        let rows = client.query(&stmt, params).await.context("Query failed")?;

        // Get column names
        let columns: Vec<&str> = stmt.columns().iter().map(|c| c.name()).collect();

        // Convert rows to JSON
        let mut results = Vec::new();
        for row in rows {
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let value = row_value_to_json(&row, i)?;
                obj.insert(col.to_string(), value);
            }
            results.push(Value::Object(obj));
        }

        Ok(json!({
            "rows": results,
            "row_count": results.len(),
            "columns": columns,
        }))
    }

    /// Execute a non-SELECT statement (INSERT, UPDATE, DELETE).
    pub async fn execute(&self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Value> {
        let client = self.pool.get().await.context("Failed to get connection")?;
        let rows_affected = client.execute(sql, params).await.context("Execute failed")?;

        Ok(json!({
            "rows_affected": rows_affected,
        }))
    }

    /// Execute multiple statements in a transaction.
    pub async fn transaction(&self, statements: &[String]) -> Result<Value> {
        let mut client = self.pool.get().await.context("Failed to get connection")?;
        let tx = client.transaction().await.context("Failed to start transaction")?;

        let mut results = Vec::new();
        for (i, sql) in statements.iter().enumerate() {
            let rows_affected = tx
                .execute(sql.as_str(), &[])
                .await
                .with_context(|| format!("Statement {} failed", i))?;
            results.push(json!({
                "statement": i,
                "rows_affected": rows_affected,
            }));
        }

        tx.commit().await.context("Failed to commit transaction")?;

        Ok(json!({
            "committed": true,
            "statements": results,
        }))
    }

    /// List tables in a schema.
    pub async fn list_tables(&self, schema: &str) -> Result<Value> {
        let sql = r#"
            SELECT
                table_name,
                table_type,
                (SELECT count(*) FROM information_schema.columns c
                 WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count
            FROM information_schema.tables t
            WHERE table_schema = $1
            ORDER BY table_name
        "#;

        self.query(sql, &[&schema]).await
    }

    /// Get table schema (columns, types, constraints).
    pub async fn table_schema(&self, table: &str, schema: &str) -> Result<Value> {
        let columns_sql = r#"
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
        "#;

        let constraints_sql = r#"
            SELECT
                tc.constraint_name,
                tc.constraint_type,
                kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = $1 AND tc.table_name = $2
        "#;

        let indexes_sql = r#"
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = $1 AND tablename = $2
        "#;

        let client = self.pool.get().await.context("Failed to get connection")?;

        // Get columns
        let columns = {
            let stmt = client.prepare(columns_sql).await?;
            let rows = client.query(&stmt, &[&schema, &table]).await?;
            rows_to_json(&rows, &stmt)?
        };

        // Get constraints
        let constraints = {
            let stmt = client.prepare(constraints_sql).await?;
            let rows = client.query(&stmt, &[&schema, &table]).await?;
            rows_to_json(&rows, &stmt)?
        };

        // Get indexes
        let indexes = {
            let stmt = client.prepare(indexes_sql).await?;
            let rows = client.query(&stmt, &[&schema, &table]).await?;
            rows_to_json(&rows, &stmt)?
        };

        Ok(json!({
            "table": table,
            "schema": schema,
            "columns": columns,
            "constraints": constraints,
            "indexes": indexes,
        }))
    }

    /// List schemas in the database.
    pub async fn list_schemas(&self) -> Result<Value> {
        let sql = r#"
            SELECT
                schema_name,
                schema_owner
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
        "#;

        self.query(sql, &[]).await
    }

    /// Get database statistics.
    pub async fn stats(&self) -> Result<Value> {
        let client = self.pool.get().await.context("Failed to get connection")?;

        // Database size
        let size_row = client
            .query_one("SELECT pg_database_size(current_database())", &[])
            .await?;
        let db_size: i64 = size_row.get(0);

        // Connection stats
        let conn_row = client
            .query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
                &[],
            )
            .await?;
        let connections: i64 = conn_row.get(0);

        // Table count
        let table_row = client
            .query_one(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
                &[],
            )
            .await?;
        let table_count: i64 = table_row.get(0);

        // Version
        let version_row = client.query_one("SELECT version()", &[]).await?;
        let version: String = version_row.get(0);

        Ok(json!({
            "database": self.config.database,
            "size_bytes": db_size,
            "size_human": format_bytes(db_size as u64),
            "active_connections": connections,
            "table_count": table_count,
            "version": version,
        }))
    }
}

/// Convert a row value at index to JSON Value.
fn row_value_to_json(row: &tokio_postgres::Row, idx: usize) -> Result<Value> {
    use tokio_postgres::types::Type;

    let col = row.columns().get(idx).unwrap();

    // Handle NULL values
    if row.try_get::<_, Option<&[u8]>>(idx).ok().flatten().is_none()
        && row.try_get::<_, Option<String>>(idx).ok().flatten().is_none()
        && row.try_get::<_, Option<i32>>(idx).ok().flatten().is_none()
    {
        // Try to get as Option<String> to check for NULL
        if let Ok(None) = row.try_get::<_, Option<String>>(idx) {
            return Ok(Value::Null);
        }
    }

    match *col.type_() {
        Type::BOOL => {
            let v: Option<bool> = row.get(idx);
            Ok(v.map(Value::Bool).unwrap_or(Value::Null))
        }
        Type::INT2 => {
            let v: Option<i16> = row.get(idx);
            Ok(v.map(|n| json!(n)).unwrap_or(Value::Null))
        }
        Type::INT4 => {
            let v: Option<i32> = row.get(idx);
            Ok(v.map(|n| json!(n)).unwrap_or(Value::Null))
        }
        Type::INT8 => {
            let v: Option<i64> = row.get(idx);
            Ok(v.map(|n| json!(n)).unwrap_or(Value::Null))
        }
        Type::FLOAT4 => {
            let v: Option<f32> = row.get(idx);
            Ok(v.map(|n| json!(n)).unwrap_or(Value::Null))
        }
        Type::FLOAT8 => {
            let v: Option<f64> = row.get(idx);
            Ok(v.map(|n| json!(n)).unwrap_or(Value::Null))
        }
        Type::JSON | Type::JSONB => {
            let v: Option<Value> = row.get(idx);
            Ok(v.unwrap_or(Value::Null))
        }
        Type::TIMESTAMPTZ | Type::TIMESTAMP => {
            let v: Option<chrono::NaiveDateTime> = row.get(idx);
            Ok(v.map(|dt: chrono::NaiveDateTime| json!(dt.to_string())).unwrap_or(Value::Null))
        }
        Type::DATE => {
            let v: Option<chrono::NaiveDate> = row.get(idx);
            Ok(v.map(|d: chrono::NaiveDate| json!(d.to_string())).unwrap_or(Value::Null))
        }
        Type::UUID => {
            // UUID needs to be converted to string
            let v: Option<String> = row.try_get(idx).ok().flatten();
            Ok(v.map(|s| json!(s)).unwrap_or(Value::Null))
        }
        _ => {
            // Default: try to get as string
            let v: Option<String> = row.try_get(idx).ok().flatten();
            Ok(v.map(|s| json!(s)).unwrap_or(Value::Null))
        }
    }
}

/// Convert rows to JSON array.
fn rows_to_json(rows: &[tokio_postgres::Row], stmt: &tokio_postgres::Statement) -> Result<Vec<Value>> {
    let columns: Vec<&str> = stmt.columns().iter().map(|c| c.name()).collect();
    let mut results = Vec::new();

    for row in rows {
        let mut obj = serde_json::Map::new();
        for (i, col) in columns.iter().enumerate() {
            let value = row_value_to_json(row, i)?;
            obj.insert(col.to_string(), value);
        }
        results.push(Value::Object(obj));
    }

    Ok(results)
}

/// Format bytes to human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}
