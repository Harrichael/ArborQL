pub mod mysql;
pub mod sqlite;

use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

/// Which database backend to use for a connection.
#[derive(Debug, Clone, PartialEq)]
pub enum DbType {
    Sqlite,
    Mysql,
}

/// Parameters for a database connection (before it is actively connected).
/// The actual `Box<dyn Database>` is created by calling `connect_params`.
#[derive(Debug, Clone)]
pub struct ConnectionParams {
    /// User-defined name/alias for this connection.
    pub name: String,
    /// Which backend to use.
    pub db_type: DbType,
    /// SQLite: path to the `.db` file.
    pub sqlite_path: Option<String>,
    /// MySQL: hostname or IP address.
    pub mysql_host: Option<String>,
    /// MySQL: port number (default 3306).
    pub mysql_port: Option<u16>,
    /// MySQL: username.
    pub mysql_username: Option<String>,
    /// MySQL: password (may be empty).
    pub mysql_password: Option<String>,
    /// MySQL: database name.
    pub mysql_database: Option<String>,
}

impl ConnectionParams {
    /// Build the connection URL suitable for `connect()`.
    pub fn to_url(&self) -> String {
        match self.db_type {
            DbType::Sqlite => {
                let path = self.sqlite_path.as_deref().unwrap_or("");
                format!("sqlite://{}", path)
            }
            DbType::Mysql => {
                let host = self.mysql_host.as_deref().unwrap_or("localhost");
                let port = self.mysql_port.unwrap_or(3306);
                let user = self.mysql_username.as_deref().unwrap_or("");
                let password = self.mysql_password.as_deref().unwrap_or("");
                let database = self.mysql_database.as_deref().unwrap_or("");
                if password.is_empty() {
                    format!("mysql://{}@{}:{}/{}", user, host, port, database)
                } else {
                    format!("mysql://{}:{}@{}:{}/{}", user, password, host, port, database)
                }
            }
        }
    }

    /// A human-readable URL without the password.
    pub fn display_url(&self) -> String {
        match self.db_type {
            DbType::Sqlite => {
                let path = self.sqlite_path.as_deref().unwrap_or("");
                format!("sqlite://{}", path)
            }
            DbType::Mysql => {
                let host = self.mysql_host.as_deref().unwrap_or("localhost");
                let port = self.mysql_port.unwrap_or(3306);
                let user = self.mysql_username.as_deref().unwrap_or("");
                let database = self.mysql_database.as_deref().unwrap_or("");
                format!("mysql://{}@{}:{}/{}", user, host, port, database)
            }
        }
    }
}

/// Connect to a database using a `ConnectionParams` struct.
pub async fn connect_params(params: &ConnectionParams) -> Result<Box<dyn Database>> {
    connect(&params.to_url()).await
}

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
            Value::Bytes(b) => write!(f, "0x{}", b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>()),
        }
    }
}

/// A row returned from the database.
pub type Row = HashMap<String, Value>;

/// Column metadata.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    /// Short type name, e.g. `"binary"`, `"varchar"`, `"int"`.
    pub data_type: String,
    /// Full type declaration as reported by the database, e.g. `"binary(16)"`,
    /// `"varchar(255)"`.  Empty string for backends that don't expose this.
    pub column_type: String,
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

    /// Return metadata for ALL tables in one shot.
    /// Default implementation calls `describe_table` per table; backends can
    /// override to use a single batched query (important for MySQL performance).
    async fn describe_all_tables(&self, tables: &[String]) -> Result<Vec<TableInfo>> {
        let mut result = Vec::new();
        for t in tables {
            result.push(self.describe_table(t).await?);
        }
        Ok(result)
    }

    /// Execute a raw SELECT and return rows.
    async fn query(&self, sql: &str) -> Result<Vec<Row>>;

    /// Return true when this database supports the `UUID_TO_BIN` / `BIN_TO_UUID`
    /// functions (MySQL 8+).  Defaults to false.
    fn supports_uuid_functions(&self) -> bool {
        false
    }
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
