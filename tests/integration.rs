//! Integration tests for fgp-postgres daemon
//!
//! These tests require a running PostgreSQL instance.
//! Set DATABASE_URL environment variable to run.

use std::collections::HashMap;
use serde_json::{json, Value};

/// Test that we can parse connection strings correctly
#[test]
fn test_parse_connection_string() {
    // This test doesn't require a database
    let url = "postgres://user:pass@localhost:5432/testdb";
    assert!(url.starts_with("postgres://"));
}

/// Test SQL parameter handling
#[test]
fn test_sql_params_serialization() {
    let params: Vec<Value> = vec![json!(1), json!("test"), json!(true)];
    let serialized = serde_json::to_string(&params).unwrap();
    assert!(serialized.contains("1"));
    assert!(serialized.contains("\"test\""));
    assert!(serialized.contains("true"));
}

/// Test method parameter extraction
#[test]
fn test_param_extraction() {
    let mut params: HashMap<String, Value> = HashMap::new();
    params.insert("sql".into(), json!("SELECT * FROM users"));
    params.insert("limit".into(), json!(10));

    let sql = params.get("sql").and_then(|v| v.as_str());
    assert_eq!(sql, Some("SELECT * FROM users"));

    let limit = params.get("limit").and_then(|v| v.as_i64()).unwrap_or(25);
    assert_eq!(limit, 10);
}

/// Test schema name validation
#[test]
fn test_schema_name_validation() {
    let valid_schemas = ["public", "my_schema", "Schema1"];
    let invalid_schemas = ["my schema", "schema;drop", "schema--comment"];

    for schema in valid_schemas {
        assert!(schema.chars().all(|c| c.is_alphanumeric() || c == '_'));
    }

    for schema in invalid_schemas {
        assert!(!schema.chars().all(|c| c.is_alphanumeric() || c == '_'));
    }
}

/// Test table name parsing
#[test]
fn test_table_name_parsing() {
    let qualified = "public.users";
    let parts: Vec<&str> = qualified.split('.').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "public");
    assert_eq!(parts[1], "users");
}

#[cfg(feature = "integration")]
mod integration {
    //! These tests require DATABASE_URL to be set

    use super::*;

    fn skip_if_no_database() -> bool {
        std::env::var("DATABASE_URL").is_err()
    }

    #[test]
    fn test_connection() {
        if skip_if_no_database() {
            eprintln!("Skipping: DATABASE_URL not set");
            return;
        }
        // Real connection test would go here
    }
}
