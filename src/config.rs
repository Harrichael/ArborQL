use anyhow::{Context, Result};
use rs_jsonnet::evaluate_with_filename;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use crate::connection_manager::{ConnectionType, ManagedConnection};
use crate::schema::VirtualFkDef;

#[derive(Debug, Clone)]
pub struct ColumnDefaults {
    pub global: Vec<String>,
    pub per_table: HashMap<String, Vec<String>>,
}

impl Default for ColumnDefaults {
    fn default() -> Self {
        Self {
            global: vec![],
            per_table: HashMap::new(),
        }
    }
}

impl ColumnDefaults {
    pub fn for_table(&self, table: &str) -> &[String] {
        self.per_table.get(table).unwrap_or(&self.global)
    }
}

/// A saved connection definition (no alias — that's ephemeral).
/// Stores structured params (host, port, etc.) rather than a flat URL.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SavedConnection {
    /// Stable identity linking saved entries to live connections.
    pub id: String,
    #[serde(rename = "type")]
    pub conn_type: String,
    /// Structured connection fields (e.g. host, port, user, database, path).
    /// Password is only included if the user explicitly opted in.
    #[serde(flatten)]
    pub params: HashMap<String, String>,
}

/// Full parsed config returned to callers.
#[derive(Debug, Default, Clone)]
pub struct AppConfig {
    pub columns: ColumnDefaults,
    pub virtual_fks: Vec<VirtualFkDef>,
    pub connections: Vec<SavedConnection>,
    /// Maximum number of history entries to keep in `~/.latticeql/history`.
    /// Defaults to 10 000 when not set in any config file.
    pub history_max_len: usize,
}

#[derive(Debug, Default, Deserialize)]
struct RawConfig {
    #[serde(default)]
    columns: RawColumnsConfig,
    #[serde(default)]
    virtual_fks: Vec<RawVirtualFk>,
    #[serde(default)]
    connections: Vec<SavedConnection>,
    /// Maximum history file length. `null` / absent means "use default".
    #[serde(default)]
    history_max_len: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct RawColumnsConfig {
    #[serde(default)]
    default: Option<Vec<String>>,
    #[serde(default)]
    tables: HashMap<String, RawTableColumnsConfig>,
}

#[derive(Debug, Default, Deserialize)]
struct RawTableColumnsConfig {
    #[serde(default)]
    default: Option<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RawVirtualFk {
    from_table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    type_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    type_value: Option<String>,
    id_column: String,
    to_table: String,
    to_column: String,
}

impl From<RawVirtualFk> for VirtualFkDef {
    fn from(r: RawVirtualFk) -> Self {
        VirtualFkDef {
            from_table: r.from_table,
            type_column: r.type_column,
            type_value: r.type_value,
            id_column: r.id_column,
            to_table: r.to_table,
            to_column: r.to_column,
        }
    }
}

impl From<&VirtualFkDef> for RawVirtualFk {
    fn from(v: &VirtualFkDef) -> Self {
        RawVirtualFk {
            from_table: v.from_table.clone(),
            type_column: v.type_column.clone(),
            type_value: v.type_value.clone(),
            id_column: v.id_column.clone(),
            to_table: v.to_table.clone(),
            to_column: v.to_column.clone(),
        }
    }
}

pub fn load_config() -> Result<AppConfig> {
    let path = home_config_path()?;
    let mut columns = ColumnDefaults::default();
    let mut virtual_fks: Vec<VirtualFkDef> = Vec::new();
    let mut connections: Vec<SavedConnection> = Vec::new();
    let mut history_max_len: Option<usize> = None;

    if path.is_file() {
        let raw = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        let rendered = evaluate_with_filename(raw.trim(), &path.display().to_string())
            .map_err(|e| anyhow::anyhow!("jsonnet eval failed for {}: {}", path.display(), e))?;
        let json = rendered.to_json_value();
        let parsed: RawConfig = serde_json::from_value(json).with_context(|| {
            format!(
                "failed to parse config JSON from jsonnet file: {}",
                path.display()
            )
        })?;
        apply_column_layer(&mut columns, &parsed);
        for raw_vfk in parsed.virtual_fks {
            let vfk = VirtualFkDef::from(raw_vfk);
            if !virtual_fks.contains(&vfk) {
                virtual_fks.push(vfk);
            }
        }
        connections = parsed.connections;
        if let Some(v) = parsed.history_max_len {
            history_max_len = Some(v as usize);
        }
    }

    Ok(AppConfig {
        columns,
        virtual_fks,
        connections,
        history_max_len: history_max_len.unwrap_or(10_000),
    })
}

/// Persist `vfks` into `~/.latticeql/default.jsonnet`.
/// Reads the existing file first so column settings are preserved; writes back
/// as plain JSON (valid jsonnet).
pub fn save_virtual_fks(vfks: &[VirtualFkDef]) -> Result<PathBuf> {
    let path = home_config_path()?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Read + evaluate existing config (if any) into a JSON Value so we can
    // update just the virtual_fks key without losing column settings.
    let mut root: serde_json::Value = if path.is_file() {
        let raw = fs::read_to_string(&path)?;
        let rendered = evaluate_with_filename(raw.trim(), &path.display().to_string())
            .map_err(|e| anyhow::anyhow!("jsonnet eval failed for {}: {}", path.display(), e))?;
        rendered.to_json_value()
    } else {
        serde_json::json!({})
    };

    let raw_vfks: Vec<RawVirtualFk> = vfks.iter().map(RawVirtualFk::from).collect();
    root["virtual_fks"] = serde_json::to_value(&raw_vfks)?;

    let json = serde_json::to_string_pretty(&root)?;
    fs::write(&path, json)?;
    Ok(path)
}

/// Save or update a single connection in `~/.latticeql/default.jsonnet`.
/// If a saved entry with the same `id` exists, it is replaced; otherwise appended.
/// Password is only included when `save_password` is true.
pub fn save_connection(
    conn: &ManagedConnection,
    existing_saved: &[SavedConnection],
    save_password: bool,
) -> Result<(PathBuf, Vec<SavedConnection>)> {
    let path = home_config_path()?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut root: serde_json::Value = if path.is_file() {
        let raw = fs::read_to_string(&path)?;
        let rendered = evaluate_with_filename(raw.trim(), &path.display().to_string())
            .map_err(|e| anyhow::anyhow!("jsonnet eval failed for {}: {}", path.display(), e))?;
        rendered.to_json_value()
    } else {
        serde_json::json!({})
    };

    let mut params = conn.params.clone();
    if !save_password {
        params.remove("password");
    }
    params.remove("alias");

    let new_entry = SavedConnection {
        id: conn.id.clone(),
        conn_type: match conn.conn_type {
            ConnectionType::Sqlite => "sqlite".to_string(),
            ConnectionType::Mysql => "mysql".to_string(),
        },
        params,
    };

    // Build updated list: replace existing entry by ID, or append.
    let mut updated: Vec<SavedConnection> = existing_saved
        .iter()
        .filter(|s| s.id != conn.id)
        .cloned()
        .collect();
    updated.push(new_entry);

    root["connections"] = serde_json::to_value(&updated)?;

    let json = serde_json::to_string_pretty(&root)?;
    fs::write(&path, json)?;
    Ok((path, updated))
}

/// Remove a saved connection by ID from `~/.latticeql/default.jsonnet`.
pub fn remove_saved_connection(
    id: &str,
    existing_saved: &[SavedConnection],
) -> Result<(PathBuf, Vec<SavedConnection>)> {
    let path = home_config_path()?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut root: serde_json::Value = if path.is_file() {
        let raw = fs::read_to_string(&path)?;
        let rendered = evaluate_with_filename(raw.trim(), &path.display().to_string())
            .map_err(|e| anyhow::anyhow!("jsonnet eval failed for {}: {}", path.display(), e))?;
        rendered.to_json_value()
    } else {
        serde_json::json!({})
    };

    let updated: Vec<SavedConnection> = existing_saved
        .iter()
        .filter(|s| s.id != id)
        .cloned()
        .collect();

    root["connections"] = serde_json::to_value(&updated)?;

    let json = serde_json::to_string_pretty(&root)?;
    fs::write(&path, json)?;
    Ok((path, updated))
}

/// Kept for backward-compat; callers that only need columns can use this.
pub fn load_column_defaults() -> Result<ColumnDefaults> {
    Ok(load_config()?.columns)
}

fn apply_column_layer(target: &mut ColumnDefaults, layer: &RawConfig) {
    if let Some(global) = &layer.columns.default {
        target.global = global.clone();
    }
    for (table, table_cfg) in &layer.columns.tables {
        if let Some(cols) = &table_cfg.default {
            target.per_table.insert(table.clone(), cols.clone());
        }
    }
}

fn home_config_path() -> Result<PathBuf> {
    Ok(home_dir()?.join(".latticeql").join("default.jsonnet"))
}

pub(crate) fn home_dir() -> Result<PathBuf> {
    std::env::var("HOME")
        .map(PathBuf::from)
        .context("HOME environment variable is not set")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    // Serialize tests that mutate the HOME environment variable so they don't
    // stomp on each other when the test runner uses multiple threads.
    static HOME_LOCK: Mutex<()> = Mutex::new(());

    fn temp_dir() -> PathBuf {
        let base = std::env::temp_dir().join(format!(
            "latticeql-config-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::create_dir_all(&base).unwrap();
        base
    }

    #[test]
    fn virtual_fks_loaded_from_home_config_and_deduped() {
        let _guard = HOME_LOCK.lock().unwrap();
        let home = temp_dir();
        fs::create_dir_all(home.join(".latticeql")).unwrap();

        let vfk_json = r#"{
          "from_table": "comments",
          "type_column": "commentable_type",
          "type_value": "Post",
          "id_column": "commentable_id",
          "to_table": "posts",
          "to_column": "id"
        }"#;
        // Two identical VFK entries in the same file — should be deduplicated.
        let cfg_with_dup = format!(
            r#"{{ columns: {{ default: ["id"] }}, virtual_fks: [{0}, {0}] }}"#,
            vfk_json
        );
        fs::write(home.join(".latticeql/default.jsonnet"), &cfg_with_dup).unwrap();

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &home);
        let cfg = load_config().unwrap();
        if let Some(old_home) = old_home {
            std::env::set_var("HOME", old_home);
        }

        assert_eq!(cfg.virtual_fks.len(), 1);
        assert_eq!(cfg.virtual_fks[0].from_table, "comments");
        assert_eq!(cfg.virtual_fks[0].to_table, "posts");
        assert_eq!(cfg.virtual_fks[0].type_column, Some("commentable_type".to_string()));
    }

    #[test]
    fn save_and_reload_virtual_fks() {
        let _guard = HOME_LOCK.lock().unwrap();
        let home = temp_dir();
        fs::create_dir_all(home.join(".latticeql")).unwrap();

        let vfks = vec![crate::schema::VirtualFkDef {
            from_table: "comments".to_string(),
            type_column: Some("commentable_type".to_string()),
            type_value: Some("Post".to_string()),
            id_column: "commentable_id".to_string(),
            to_table: "posts".to_string(),
            to_column: "id".to_string(),
        }];

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &home);
        save_virtual_fks(&vfks).unwrap();
        let cfg = load_config().unwrap();
        if let Some(old_home) = old_home {
            std::env::set_var("HOME", old_home);
        }

        assert_eq!(cfg.virtual_fks.len(), 1);
        assert_eq!(cfg.virtual_fks[0].from_table, "comments");
        assert_eq!(cfg.virtual_fks[0].type_column, Some("commentable_type".to_string()));
    }

    #[test]
    fn save_and_reload_simple_virtual_fk() {
        let _guard = HOME_LOCK.lock().unwrap();
        let home = temp_dir();
        fs::create_dir_all(home.join(".latticeql")).unwrap();

        // A simple FK with no type discriminator
        let vfks = vec![crate::schema::VirtualFkDef {
            from_table: "orders".to_string(),
            type_column: None,
            type_value: None,
            id_column: "customer_id".to_string(),
            to_table: "customers".to_string(),
            to_column: "id".to_string(),
        }];

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &home);
        save_virtual_fks(&vfks).unwrap();
        let cfg = load_config().unwrap();
        if let Some(old_home) = old_home {
            std::env::set_var("HOME", old_home);
        }

        assert_eq!(cfg.virtual_fks.len(), 1);
        assert_eq!(cfg.virtual_fks[0].from_table, "orders");
        assert_eq!(cfg.virtual_fks[0].to_table, "customers");
        assert!(cfg.virtual_fks[0].type_column.is_none());
        assert!(cfg.virtual_fks[0].type_value.is_none());
    }

    #[test]
    fn defaults_for_table_falls_back_to_global() {
        let cfg = ColumnDefaults::default();
        assert_eq!(cfg.for_table("users"), &Vec::<String>::new());
    }

    #[test]
    fn config_loads_columns_from_home() {
        let _guard = HOME_LOCK.lock().unwrap();
        let home = temp_dir();
        fs::create_dir_all(home.join(".latticeql")).unwrap();

        fs::write(
            home.join(".latticeql/default.jsonnet"),
            r#"{ columns: { default: ["id", "name"], tables: { orders: { default: ["id", "status"] } } } }"#,
        )
        .unwrap();

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &home);
        let cfg = load_column_defaults().unwrap();
        if let Some(old_home) = old_home {
            std::env::set_var("HOME", old_home);
        }

        assert_eq!(cfg.global, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(
            cfg.for_table("orders"),
            &vec!["id".to_string(), "status".to_string()]
        );
    }
}
