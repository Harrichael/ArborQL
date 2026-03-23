pub mod mysql;
pub mod sqlite;

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// A single cell value from the database.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(i) => write!(f, "{}", i),
            Value::Float(v) => write!(f, "{}", v),
            Value::Text(s) => write!(f, "{}", s),
            Value::Bytes(b) => write!(f, "<{} bytes>", b.len()),
        }
    }
}

/// A row returned from the database.
pub type Row = HashMap<String, Value>;

/// Column metadata.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub is_primary_key: bool,
}

/// Foreign-key relationship between two tables.
#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// Column in this table
    pub from_column: String,
    /// Referenced table
    pub to_table: String,
    /// Referenced column
    pub to_column: String,
}

/// Full metadata for one table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub foreign_keys: Vec<ForeignKey>,
}

/// Trait implemented by each database backend.
#[async_trait]
pub trait Database: Send + Sync {
    /// Return all table names.
    async fn list_tables(&self) -> Result<Vec<String>>;

    /// Return full metadata for a table.
    async fn describe_table(&self, table: &str) -> Result<TableInfo>;

    /// Execute a raw SELECT and return rows.
    async fn query(&self, sql: &str) -> Result<Vec<Row>>;
}

/// Connect to the given URL and return a boxed `Database`.
pub async fn connect(url: &str) -> Result<Box<dyn Database>> {
    if url.starts_with("mysql://") || url.starts_with("mysql+tls://") {
        Ok(Box::new(mysql::MysqlDb::connect(url).await?))
    } else if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
        Ok(Box::new(sqlite::SqliteDb::connect(url).await?))
    } else {
        anyhow::bail!("Unsupported database URL: {}", url)
    }
}
