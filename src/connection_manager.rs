use crate::db::{self, Database, ForeignKey, Row, TableInfo};
use crate::schema::Schema;
use chrono::{DateTime, Local};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;

// ── Connection types ────────────────────────────────────────────────────────

/// Supported database backend types.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionType {
    Sqlite,
    Mysql,
}

impl ConnectionType {
    pub fn all() -> &'static [ConnectionType] {
        &[ConnectionType::Sqlite, ConnectionType::Mysql]
    }

    pub fn label(&self) -> &'static str {
        match self {
            ConnectionType::Sqlite => "SQLite",
            ConnectionType::Mysql => "MySQL",
        }
    }

    /// Field definitions for the connection creation form.
    pub fn fields(&self) -> Vec<ConnectionFieldDef> {
        match self {
            ConnectionType::Sqlite => vec![
                ConnectionFieldDef {
                    name: "alias".into(),
                    label: "Alias".into(),
                    placeholder: "e.g. local".into(),
                    required: true,
                },
                ConnectionFieldDef {
                    name: "path".into(),
                    label: "File Path".into(),
                    placeholder: "e.g. /path/to/db.sqlite3".into(),
                    required: true,
                },
            ],
            ConnectionType::Mysql => vec![
                ConnectionFieldDef {
                    name: "alias".into(),
                    label: "Alias".into(),
                    placeholder: "e.g. prd".into(),
                    required: true,
                },
                ConnectionFieldDef {
                    name: "host".into(),
                    label: "Host".into(),
                    placeholder: "localhost".into(),
                    required: true,
                },
                ConnectionFieldDef {
                    name: "port".into(),
                    label: "Port".into(),
                    placeholder: "3306".into(),
                    required: true,
                },
                ConnectionFieldDef {
                    name: "user".into(),
                    label: "Username".into(),
                    placeholder: "root".into(),
                    required: true,
                },
                ConnectionFieldDef {
                    name: "password".into(),
                    label: "Password".into(),
                    placeholder: "(may be blank)".into(),
                    required: false,
                },
                ConnectionFieldDef {
                    name: "database".into(),
                    label: "Database".into(),
                    placeholder: "mydb".into(),
                    required: true,
                },
            ],
        }
    }

    /// Build a connection URL from form field values.
    pub fn build_url(&self, values: &HashMap<String, String>) -> Result<String> {
        match self {
            ConnectionType::Sqlite => {
                let path = values
                    .get("path")
                    .filter(|s| !s.is_empty())
                    .context("File path is required")?;
                Ok(format!("sqlite://{}", path))
            }
            ConnectionType::Mysql => {
                let host = values
                    .get("host")
                    .filter(|s| !s.is_empty())
                    .context("Host is required")?;
                let port = values
                    .get("port")
                    .filter(|s| !s.is_empty())
                    .context("Port is required")?;
                let user = values
                    .get("user")
                    .filter(|s| !s.is_empty())
                    .context("Username is required")?;
                let password = values.get("password").cloned().unwrap_or_default();
                let database = values
                    .get("database")
                    .filter(|s| !s.is_empty())
                    .context("Database name is required")?;
                Ok(format!(
                    "mysql://{}:{}@{}:{}/{}",
                    user, password, host, port, database
                ))
            }
        }
    }

    /// Infer the connection type from a URL.
    pub fn from_url(url: &str) -> Option<Self> {
        if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
            Some(ConnectionType::Sqlite)
        } else if url.starts_with("mysql://") || url.starts_with("mysql+tls://") {
            Some(ConnectionType::Mysql)
        } else {
            None
        }
    }
}

/// Definition of a field in the connection creation form.
#[derive(Debug, Clone)]
pub struct ConnectionFieldDef {
    pub name: String,
    pub label: String,
    pub placeholder: String,
    pub required: bool,
}

// ── Connection status ────────────────────────────────────────────────────────

/// The status of a managed connection.
#[derive(Debug, Clone)]
pub enum ConnectionStatus {
    /// Connection is live and usable.
    Connected,
    /// User intentionally disconnected (or not yet connected).
    Disconnected,
    /// Connection attempt failed with an error message.
    Error(String),
}

impl ConnectionStatus {
    pub fn is_connected(&self) -> bool {
        matches!(self, ConnectionStatus::Connected)
    }
}

// ── Managed connection ──────────────────────────────────────────────────────

/// A single database connection managed by the ConnectionManager.
pub struct ManagedConnection {
    pub alias: String,
    pub conn_type: ConnectionType,
    pub url: String,
    pub status: ConnectionStatus,
    /// Active database handle. `None` when not connected.
    pub db: Option<Box<dyn Database>>,
    /// Cached table names from this connection (original, unqualified).
    pub original_tables: Vec<String>,
    /// Cached table infos from this connection (original, unqualified).
    pub original_table_infos: HashMap<String, TableInfo>,
    /// Table count from the last successful connection (persists across disconnect).
    pub last_table_count: usize,
    /// Timestamp of the last successful schema sync.
    pub last_synced: Option<DateTime<Local>>,
}

impl ManagedConnection {
    pub fn is_connected(&self) -> bool {
        self.status.is_connected()
    }
}

// ── Connection manager ──────────────────────────────────────────────────────

/// Manages multiple database connections and presents them as a single
/// `Database` to the engine via a merged, qualified schema.
pub struct ConnectionManager {
    pub connections: Vec<ManagedConnection>,
    /// Maps qualified table name → connection index.
    table_to_conn: HashMap<String, usize>,
    /// Maps qualified table name → original (unqualified) table name.
    qualified_to_original: HashMap<String, String>,
    /// The merged schema built from all connected databases.
    merged_schema: Schema,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            connections: Vec::new(),
            table_to_conn: HashMap::new(),
            qualified_to_original: HashMap::new(),
            merged_schema: Schema::default(),
        }
    }

    /// Add and connect a new database.
    ///
    /// On success the connection is added in `Connected` state.  On failure
    /// the connection is still added (in `Error` state) so the user can see
    /// it in the manager and retry.
    pub async fn add_connection(
        &mut self,
        alias: String,
        conn_type: ConnectionType,
        url: String,
    ) -> Result<()> {
        // Check for duplicate alias
        if self.connections.iter().any(|c| c.alias == alias) {
            anyhow::bail!("Connection alias '{}' already in use", alias);
        }

        match self.try_connect(&url).await {
            Ok((db, table_names, table_infos)) => {
                let count = table_names.len();
                self.connections.push(ManagedConnection {
                    alias,
                    conn_type,
                    url,
                    status: ConnectionStatus::Connected,
                    db: Some(db),
                    original_tables: table_names,
                    original_table_infos: table_infos,
                    last_table_count: count,
                    last_synced: Some(Local::now()),
                });
                self.rebuild();
                Ok(())
            }
            Err(e) => {
                let msg = e.to_string();
                self.connections.push(ManagedConnection {
                    alias: alias.clone(),
                    conn_type,
                    url,
                    status: ConnectionStatus::Error(msg.clone()),
                    db: None,
                    original_tables: Vec::new(),
                    original_table_infos: HashMap::new(),
                    last_table_count: 0,
                    last_synced: None,
                });
                self.rebuild();
                anyhow::bail!("Failed to connect '{}': {}", alias, msg)
            }
        }
    }

    /// Attempt to connect and load schema. Returns db handle + metadata.
    /// Times out after 10 seconds to avoid hanging the UI on bad hosts.
    async fn try_connect(
        &self,
        url: &str,
    ) -> Result<(Box<dyn Database>, Vec<String>, HashMap<String, TableInfo>)> {
        let url = url.to_string();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            async {
                let db = db::connect(&url).await?;
                let table_names = db.list_tables().await?;
                let table_infos_vec = db.describe_all_tables(&table_names).await?;
                let table_infos: HashMap<String, TableInfo> = table_infos_vec
                    .into_iter()
                    .map(|t| (t.name.clone(), t))
                    .collect();
                Ok::<_, anyhow::Error>((db, table_names, table_infos))
            },
        )
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => anyhow::bail!("Connection timed out after 10 seconds"),
        }
    }

    /// Disconnect a connection (keep it in the list but drop the handle).
    pub fn disconnect(&mut self, index: usize) {
        if let Some(conn) = self.connections.get_mut(index) {
            conn.db = None;
            conn.status = ConnectionStatus::Disconnected;
        }
        self.rebuild();
    }

    /// Reconnect a disconnected or errored connection.
    pub async fn reconnect(&mut self, index: usize) -> Result<()> {
        let url = {
            let conn = self
                .connections
                .get(index)
                .context("Invalid connection index")?;
            if conn.status.is_connected() {
                return Ok(()); // already connected
            }
            conn.url.clone()
        };

        match self.try_connect(&url).await {
            Ok((db, table_names, table_infos)) => {
                let count = table_names.len();
                let conn = &mut self.connections[index];
                conn.db = Some(db);
                conn.original_tables = table_names;
                conn.original_table_infos = table_infos;
                conn.status = ConnectionStatus::Connected;
                conn.last_table_count = count;
                conn.last_synced = Some(Local::now());
                self.rebuild();
                Ok(())
            }
            Err(e) => {
                let msg = e.to_string();
                self.connections[index].status = ConnectionStatus::Error(msg.clone());
                let alias = self.connections[index].alias.clone();
                self.rebuild();
                anyhow::bail!("Failed to reconnect '{}': {}", alias, msg)
            }
        }
    }

    /// Remove a connection entirely.
    pub fn remove_connection(&mut self, index: usize) {
        if index < self.connections.len() {
            self.connections.remove(index);
            self.rebuild();
        }
    }

    /// Rebuild the table map and merged schema from all connected databases.
    fn rebuild(&mut self) {
        self.table_to_conn.clear();
        self.qualified_to_original.clear();

        // Count how many connections each table name appears in.
        let mut name_counts: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, conn) in self.connections.iter().enumerate() {
            if !conn.status.is_connected() {
                continue;
            }
            for table in &conn.original_tables {
                name_counts
                    .entry(table.clone())
                    .or_default()
                    .push(idx);
            }
        }

        // Build the table-to-connection mapping.
        for (table_name, conn_indices) in &name_counts {
            if conn_indices.len() == 1 {
                // Unique: map both bare name and qualified name
                let idx = conn_indices[0];
                let alias = &self.connections[idx].alias;
                let qualified = format!("{}.{}", alias, table_name);
                self.table_to_conn.insert(table_name.clone(), idx);
                self.table_to_conn.insert(qualified.clone(), idx);
                self.qualified_to_original
                    .insert(table_name.clone(), table_name.clone());
                self.qualified_to_original
                    .insert(qualified, table_name.clone());
            } else {
                // Ambiguous: only map qualified names
                for &idx in conn_indices {
                    let alias = &self.connections[idx].alias;
                    let qualified = format!("{}.{}", alias, table_name);
                    self.table_to_conn.insert(qualified.clone(), idx);
                    self.qualified_to_original
                        .insert(qualified, table_name.clone());
                }
            }
        }

        // Build merged schema.
        self.merged_schema = self.build_merged_schema_inner();
    }

    /// Build a merged schema where ambiguous table names are qualified.
    fn build_merged_schema_inner(&self) -> Schema {
        // Determine which table names are ambiguous.
        let mut name_counts: HashMap<String, usize> = HashMap::new();
        for conn in &self.connections {
            if !conn.status.is_connected() {
                continue;
            }
            for table in &conn.original_tables {
                *name_counts.entry(table.clone()).or_default() += 1;
            }
        }

        let mut tables: HashMap<String, TableInfo> = HashMap::new();

        for conn in &self.connections {
            if !conn.status.is_connected() {
                continue;
            }
            for (orig_name, info) in &conn.original_table_infos {
                let is_ambiguous = name_counts.get(orig_name).copied().unwrap_or(0) > 1;
                let qualified_name = if is_ambiguous {
                    format!("{}.{}", conn.alias, orig_name)
                } else {
                    orig_name.clone()
                };

                // Rewrite FK references to use qualified names.
                let rewritten_fks: Vec<ForeignKey> = info
                    .foreign_keys
                    .iter()
                    .map(|fk| {
                        let target_ambiguous =
                            name_counts.get(&fk.to_table).copied().unwrap_or(0) > 1;
                        let qualified_target = if target_ambiguous {
                            // FK targets within the same connection get that
                            // connection's alias prefix.
                            format!("{}.{}", conn.alias, fk.to_table)
                        } else {
                            fk.to_table.clone()
                        };
                        ForeignKey {
                            from_column: fk.from_column.clone(),
                            to_table: qualified_target,
                            to_column: fk.to_column.clone(),
                        }
                    })
                    .collect();

                tables.insert(
                    qualified_name.clone(),
                    TableInfo {
                        name: qualified_name,
                        columns: info.columns.clone(),
                        foreign_keys: rewritten_fks,
                    },
                );
            }
        }

        Schema {
            tables,
            virtual_fks: Vec::new(),
        }
    }

    /// Get the merged schema.
    pub fn merged_schema(&self) -> &Schema {
        &self.merged_schema
    }

    /// Resolve a table name (bare or qualified) to (qualified_name, connection_index).
    pub fn resolve_table(&self, name: &str) -> Result<(String, usize)> {
        if let Some(&idx) = self.table_to_conn.get(name) {
            // Find the canonical qualified name (for bare names that auto-resolved).
            Ok((name.to_string(), idx))
        } else {
            // Check if it's an ambiguous bare name.
            let mut matches: Vec<String> = Vec::new();
            for conn in &self.connections {
                if !conn.status.is_connected() {
                    continue;
                }
                if conn.original_tables.contains(&name.to_string()) {
                    matches.push(format!("{}.{}", conn.alias, name));
                }
            }
            if matches.is_empty() {
                anyhow::bail!("Unknown table '{}'", name);
            } else {
                anyhow::bail!(
                    "Ambiguous table '{}' — qualify with connection alias: {}",
                    name,
                    matches.join(", ")
                );
            }
        }
    }

    /// Get the original (unqualified) table name for a qualified name.
    fn original_table_name(&self, qualified: &str) -> String {
        self.qualified_to_original
            .get(qualified)
            .cloned()
            .unwrap_or_else(|| qualified.to_string())
    }

    /// Derive an alias from a database URL.
    pub fn alias_from_url(url: &str) -> String {
        if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
            // Use filename without extension
            let path = url
                .strip_prefix("sqlite://")
                .or_else(|| url.strip_prefix("sqlite:"))
                .unwrap_or(url);
            std::path::Path::new(path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("db")
                .to_string()
        } else if url.starts_with("mysql://") || url.starts_with("mysql+tls://") {
            // Use database name (last path segment)
            url.rsplit('/')
                .next()
                .filter(|s| !s.is_empty())
                .unwrap_or("db")
                .to_string()
        } else {
            "db".to_string()
        }
    }

    /// Extract the table name from a simple SQL query (after FROM).
    fn extract_table_from_sql(sql: &str) -> Option<String> {
        let upper = sql.to_uppercase();
        let from_pos = upper.find("FROM ")?;
        let after_from = &sql[from_pos + 5..];
        let trimmed = after_from.trim_start();
        // Table name ends at space or end of string
        let end = trimmed
            .find(|c: char| c.is_whitespace())
            .unwrap_or(trimmed.len());
        let table = &trimmed[..end];
        if table.is_empty() {
            None
        } else {
            Some(table.to_string())
        }
    }

    /// Rewrite SQL by replacing the qualified table name with the original.
    fn rewrite_sql(&self, sql: &str, qualified: &str, original: &str) -> String {
        // Replace the first occurrence of the qualified name after FROM
        sql.replacen(qualified, original, 1)
    }

    /// Return summaries for the UI.
    pub fn connection_summaries(&self) -> Vec<ConnectionSummary> {
        self.connections
            .iter()
            .map(|c| ConnectionSummary {
                alias: c.alias.clone(),
                conn_type: c.conn_type.label().to_string(),
                url: c.url.clone(),
                status: c.status.clone(),
                table_count: c.original_tables.len(),
                last_table_count: c.last_table_count,
                last_synced: c.last_synced,
            })
            .collect()
    }
}

/// Summary of a connection for UI display.
#[derive(Debug, Clone)]
pub struct ConnectionSummary {
    pub alias: String,
    pub conn_type: String,
    pub url: String,
    pub status: ConnectionStatus,
    /// Current table count (live, 0 when disconnected).
    pub table_count: usize,
    /// Table count from last successful sync (persists across disconnect).
    pub last_table_count: usize,
    /// When the schema was last synced.
    pub last_synced: Option<DateTime<Local>>,
}

// ── Database trait implementation ───────────────────────────────────────────

#[async_trait]
impl Database for ConnectionManager {
    async fn list_tables(&self) -> Result<Vec<String>> {
        let mut names: Vec<String> = self.table_to_conn.keys().cloned().collect();
        // Deduplicate: for unique tables, both bare and qualified forms exist.
        // Keep the bare form for unique tables, qualified for ambiguous ones.
        let mut seen = std::collections::HashSet::new();
        names.retain(|name| {
            let original = self.original_table_name(name);
            // If this is a qualified form and the bare form also exists, skip it.
            if name.contains('.') && self.table_to_conn.contains_key(&original) {
                return false;
            }
            seen.insert(name.clone())
        });
        names.sort();
        Ok(names)
    }

    async fn describe_table(&self, table: &str) -> Result<TableInfo> {
        self.merged_schema
            .tables
            .get(table)
            .cloned()
            .context(format!("Table '{}' not found in merged schema", table))
    }

    async fn describe_all_tables(&self, tables: &[String]) -> Result<Vec<TableInfo>> {
        tables
            .iter()
            .map(|t| {
                self.merged_schema
                    .tables
                    .get(t)
                    .cloned()
                    .context(format!("Table '{}' not found in merged schema", t))
            })
            .collect()
    }

    async fn query(&self, sql: &str) -> Result<Vec<Row>> {
        let qualified_table = Self::extract_table_from_sql(sql)
            .context("Could not extract table name from SQL")?;

        let &conn_idx = self
            .table_to_conn
            .get(&qualified_table)
            .context(format!(
                "No connection found for table '{}'",
                qualified_table
            ))?;

        let conn = &self.connections[conn_idx];
        let db = conn
            .db
            .as_ref()
            .context(format!("Connection '{}' is disconnected", conn.alias))?;

        let original = self.original_table_name(&qualified_table);
        let rewritten = self.rewrite_sql(sql, &qualified_table, &original);

        db.query(&rewritten).await
    }

    fn supports_uuid_functions(&self) -> bool {
        // Return true if any connected backend supports it.
        // In practice, this is checked per-query via routing.
        self.connections
            .iter()
            .any(|c| c.db.as_ref().map(|d| d.supports_uuid_functions()).unwrap_or(false))
    }
}
