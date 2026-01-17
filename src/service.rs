//! FGP service implementation for PostgreSQL.

use anyhow::Result;
use fgp_daemon::service::{HealthStatus, MethodInfo};
use fgp_daemon::FgpService;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::client::{ConnectionConfig, PostgresClient};

/// FGP service for PostgreSQL operations.
pub struct PostgresService {
    client: Arc<PostgresClient>,
    runtime: Runtime,
    config: ConnectionConfig,
}

impl PostgresService {
    /// Create a new PostgresService with the given connection config.
    pub fn new(config: ConnectionConfig) -> Result<Self> {
        let runtime = Runtime::new()?;

        // Create client inside runtime context
        let client = runtime.block_on(async { PostgresClient::new(config.clone()).await })?;

        Ok(Self {
            client: Arc::new(client),
            runtime,
            config,
        })
    }

    /// Helper to get a string parameter.
    fn get_str<'a>(params: &'a HashMap<String, Value>, key: &str) -> Option<&'a str> {
        params.get(key).and_then(|v| v.as_str())
    }

    /// Helper to get string parameter with default.
    fn get_str_default<'a>(
        params: &'a HashMap<String, Value>,
        key: &str,
        default: &'a str,
    ) -> &'a str {
        params
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or(default)
    }

    /// Health check implementation.
    fn health(&self) -> Result<Value> {
        let client = self.client.clone();
        let ok = self.runtime.block_on(async move { client.ping().await })?;

        Ok(serde_json::json!({
            "status": if ok { "healthy" } else { "unhealthy" },
            "database": self.config.database,
            "host": self.config.host,
            "port": self.config.port,
            "version": env!("CARGO_PKG_VERSION"),
        }))
    }

    /// Execute SQL query.
    fn query(&self, params: HashMap<String, Value>) -> Result<Value> {
        let sql = Self::get_str(&params, "sql")
            .ok_or_else(|| anyhow::anyhow!("Missing required parameter: sql"))?
            .to_string();

        let client = self.client.clone();

        self.runtime
            .block_on(async move { client.query(&sql, &[]).await })
    }

    /// Execute non-SELECT statement.
    fn execute(&self, params: HashMap<String, Value>) -> Result<Value> {
        let sql = Self::get_str(&params, "sql")
            .ok_or_else(|| anyhow::anyhow!("Missing required parameter: sql"))?
            .to_string();

        let client = self.client.clone();

        self.runtime
            .block_on(async move { client.execute(&sql, &[]).await })
    }

    /// Execute transaction.
    fn transaction(&self, params: HashMap<String, Value>) -> Result<Value> {
        let statements: Vec<String> = params
            .get("statements")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str())
                    .map(|s| s.to_string())
                    .collect()
            })
            .ok_or_else(|| anyhow::anyhow!("Missing required parameter: statements (array)"))?;

        if statements.is_empty() {
            anyhow::bail!("statements array cannot be empty");
        }

        let client = self.client.clone();

        self.runtime
            .block_on(async move { client.transaction(&statements).await })
    }

    /// List tables.
    fn tables(&self, params: HashMap<String, Value>) -> Result<Value> {
        let schema = Self::get_str_default(&params, "schema", "public").to_string();
        let client = self.client.clone();

        self.runtime
            .block_on(async move { client.list_tables(&schema).await })
    }

    /// Get table schema.
    fn schema(&self, params: HashMap<String, Value>) -> Result<Value> {
        let table = Self::get_str(&params, "table")
            .ok_or_else(|| anyhow::anyhow!("Missing required parameter: table"))?
            .to_string();
        let schema = Self::get_str_default(&params, "schema", "public").to_string();

        let client = self.client.clone();

        self.runtime
            .block_on(async move { client.table_schema(&table, &schema).await })
    }

    /// List schemas.
    fn schemas(&self) -> Result<Value> {
        let client = self.client.clone();
        self.runtime
            .block_on(async move { client.list_schemas().await })
    }

    /// Get database stats.
    fn stats(&self) -> Result<Value> {
        let client = self.client.clone();
        self.runtime.block_on(async move { client.stats().await })
    }
}

impl FgpService for PostgresService {
    fn name(&self) -> &str {
        "postgres"
    }

    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn dispatch(&self, method: &str, params: HashMap<String, Value>) -> Result<Value> {
        match method {
            "health" => self.health(),
            "query" | "postgres.query" => self.query(params),
            "execute" | "postgres.execute" => self.execute(params),
            "transaction" | "postgres.transaction" => self.transaction(params),
            "tables" | "postgres.tables" => self.tables(params),
            "schema" | "postgres.schema" => self.schema(params),
            "schemas" | "postgres.schemas" => self.schemas(),
            "stats" | "postgres.stats" => self.stats(),
            _ => anyhow::bail!("Unknown method: {}", method),
        }
    }

    fn method_list(&self) -> Vec<MethodInfo> {
        vec![
            MethodInfo::new("postgres.query", "Execute a SQL SELECT query and return results")
                .schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string", "description": "SQL query to execute" }
                    },
                    "required": ["sql"]
                })),
            MethodInfo::new("postgres.execute", "Execute a non-SELECT statement (INSERT, UPDATE, DELETE)")
                .schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "sql": { "type": "string", "description": "SQL statement to execute" }
                    },
                    "required": ["sql"]
                })),
            MethodInfo::new("postgres.transaction", "Execute multiple statements in a transaction")
                .schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "statements": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "SQL statements to execute in transaction"
                        }
                    },
                    "required": ["statements"]
                })),
            MethodInfo::new("postgres.tables", "List tables in a schema")
                .schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "schema": { "type": "string", "default": "public", "description": "Schema name" }
                    }
                })),
            MethodInfo::new("postgres.schema", "Get table schema (columns, constraints, indexes)")
                .schema(serde_json::json!({
                    "type": "object",
                    "properties": {
                        "table": { "type": "string", "description": "Table name" },
                        "schema": { "type": "string", "default": "public", "description": "Schema name" }
                    },
                    "required": ["table"]
                })),
            MethodInfo::new("postgres.schemas", "List all schemas in the database"),
            MethodInfo::new("postgres.stats", "Get database statistics (size, connections, table count)"),
        ]
    }

    fn on_start(&self) -> Result<()> {
        tracing::info!(
            "PostgresService starting, connecting to {}...",
            self.client.connection_info()
        );

        let client = self.client.clone();
        self.runtime.block_on(async move {
            match client.ping().await {
                Ok(true) => {
                    tracing::info!("PostgreSQL connection verified");
                    Ok(())
                }
                Ok(false) => {
                    tracing::warn!("PostgreSQL ping returned false");
                    Ok(())
                }
                Err(e) => {
                    tracing::error!("Failed to connect to PostgreSQL: {}", e);
                    Err(e)
                }
            }
        })
    }

    fn health_check(&self) -> HashMap<String, HealthStatus> {
        let mut checks = HashMap::new();

        let client = self.client.clone();
        let start = std::time::Instant::now();
        let result = self.runtime.block_on(async move { client.ping().await });

        let latency = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(true) => {
                checks.insert(
                    "postgres".into(),
                    HealthStatus::healthy_with_latency(latency),
                );
            }
            Ok(false) => {
                checks.insert(
                    "postgres".into(),
                    HealthStatus::unhealthy("Ping returned false"),
                );
            }
            Err(e) => {
                checks.insert("postgres".into(), HealthStatus::unhealthy(e.to_string()));
            }
        }

        checks
    }
}
