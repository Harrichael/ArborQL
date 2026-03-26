use anyhow::{Context, Result};
use rs_jsonnet::evaluate_with_filename;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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

/// Full parsed config returned to callers.
#[derive(Debug, Default, Clone)]
pub struct AppConfig {
    pub columns: ColumnDefaults,
    pub virtual_fks: Vec<VirtualFkDef>,
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
    #[serde(default)]
    type_column: Option<String>,
    #[serde(default)]
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

pub fn load_config(cwd: &Path) -> Result<AppConfig> {
    let config_files = discover_config_files(cwd)?;
    let mut columns = ColumnDefaults::default();
    let mut virtual_fks: Vec<VirtualFkDef> = Vec::new();
    let mut history_max_len: Option<usize> = None;

    // Apply most-generic first so more-specific configs win (last write wins).
    for file in config_files.iter().rev() {
        let raw = fs::read_to_string(file)
            .with_context(|| format!("failed to read config file: {}", file.display()))?;
        let rendered = evaluate_with_filename(raw.trim(), &file.display().to_string())
            .map_err(|e| anyhow::anyhow!("jsonnet eval failed for {}: {}", file.display(), e))?;
        let json = rendered.to_json_value();
        let parsed: RawConfig = serde_json::from_value(json).with_context(|| {
            format!(
                "failed to parse config JSON from jsonnet file: {}",
                file.display()
            )
        })?;
        apply_column_layer(&mut columns, &parsed);
        // Merge virtual FKs: add any not already present (deduplicate by all fields).
        for raw_vfk in parsed.virtual_fks {
            let vfk = VirtualFkDef::from(raw_vfk);
            if !virtual_fks.contains(&vfk) {
                virtual_fks.push(vfk);
            }
        }
        // Most-specific config wins (last write wins in rev iteration).
        if let Some(v) = parsed.history_max_len {
            history_max_len = Some(v as usize);
        }
    }

    Ok(AppConfig {
        columns,
        virtual_fks,
        history_max_len: history_max_len.unwrap_or(10_000),
    })
}

/// Persist `vfks` into the most-specific config file that was discovered for
/// `cwd` (i.e. the same file that was read). If no config file exists anywhere
/// in the search path, creates `.latticeql/default.jsonnet` in `cwd`.
/// Reads the existing file first so column settings are preserved; writes back
/// as plain JSON (valid jsonnet).
pub fn save_virtual_fks(cwd: &Path, vfks: &[VirtualFkDef]) -> Result<PathBuf> {
    // Use the first (most-specific) discovered file, or default to cwd.
    let path = discover_config_files(cwd)?
        .into_iter()
        .next()
        .unwrap_or_else(|| cwd.join(".latticeql").join("default.jsonnet"));

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

/// Kept for backward-compat; callers that only need columns can use this.
pub fn load_column_defaults(cwd: &Path) -> Result<ColumnDefaults> {
    Ok(load_config(cwd)?.columns)
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

fn discover_config_files(cwd: &Path) -> Result<Vec<PathBuf>> {
    let home = home_dir()?;
    let cwd = cwd.canonicalize().unwrap_or_else(|_| cwd.to_path_buf());

    // Walk from cwd upward — most specific (cwd) first, most generic (root) last.
    let mut found: Vec<PathBuf> = Vec::new();
    let mut current = cwd.clone();
    loop {
        let cfg = current.join(".latticeql").join("default.jsonnet");
        if cfg.is_file() {
            found.push(cfg);
        }
        if !current.pop() {
            break;
        }
    }

    // If home was not encountered in the walk (cwd is outside $HOME),
    // append it as the most generic fallback.
    let home_cfg = home.join(".latticeql").join("default.jsonnet");
    if home_cfg.is_file() && !found.contains(&home_cfg) {
        found.push(home_cfg);
    }

    Ok(found)
}

pub(crate) fn home_dir() -> Result<PathBuf> {
    std::env::var("HOME")
        .map(PathBuf::from)
        .context("HOME environment variable is not set")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

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
    fn virtual_fks_loaded_and_deduped() {
        let home = temp_dir();
        let cwd = home.join("work").join("repo");
        fs::create_dir_all(&cwd).unwrap();
        fs::create_dir_all(home.join(".latticeql")).unwrap();
        fs::create_dir_all(cwd.join(".latticeql")).unwrap();

        let vfk_json = r#"{
          "from_table": "comments",
          "type_column": "commentable_type",
          "type_value": "Post",
          "id_column": "commentable_id",
          "to_table": "posts",
          "to_column": "id"
        }"#;
        // Same VFK in both home and cwd — should appear only once after merge.
        let cfg_with_vfk = format!(r#"{{ columns: {{ default: ["id"] }}, virtual_fks: [{}] }}"#, vfk_json);
        fs::write(home.join(".latticeql/default.jsonnet"), &cfg_with_vfk).unwrap();
        fs::write(cwd.join(".latticeql/default.jsonnet"), &cfg_with_vfk).unwrap();

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &home);
        let cfg = load_config(&cwd).unwrap();
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
        let dir = temp_dir();
        fs::create_dir_all(dir.join(".latticeql")).unwrap();

        let vfks = vec![crate::schema::VirtualFkDef {
            from_table: "comments".to_string(),
            type_column: Some("commentable_type".to_string()),
            type_value: Some("Post".to_string()),
            id_column: "commentable_id".to_string(),
            to_table: "posts".to_string(),
            to_column: "id".to_string(),
        }];

        save_virtual_fks(&dir, &vfks).unwrap();

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &dir);
        let cfg = load_config(&dir).unwrap();
        if let Some(old_home) = old_home {
            std::env::set_var("HOME", old_home);
        }

        assert_eq!(cfg.virtual_fks.len(), 1);
        assert_eq!(cfg.virtual_fks[0].from_table, "comments");
        assert_eq!(cfg.virtual_fks[0].type_column, Some("commentable_type".to_string()));
    }

    #[test]
    fn save_and_reload_simple_virtual_fk() {
        let dir = temp_dir();
        fs::create_dir_all(dir.join(".latticeql")).unwrap();

        // A simple FK with no type discriminator
        let vfks = vec![crate::schema::VirtualFkDef {
            from_table: "orders".to_string(),
            type_column: None,
            type_value: None,
            id_column: "customer_id".to_string(),
            to_table: "customers".to_string(),
            to_column: "id".to_string(),
        }];

        save_virtual_fks(&dir, &vfks).unwrap();

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &dir);
        let cfg = load_config(&dir).unwrap();
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
    fn layered_config_merges_global_and_table_overrides() {
        let home = temp_dir();
        let cwd = home.join("work").join("repo");
        fs::create_dir_all(&cwd).unwrap();
        fs::create_dir_all(home.join(".latticeql")).unwrap();
        fs::create_dir_all(cwd.join(".latticeql")).unwrap();

        fs::write(
            home.join(".latticeql/default.jsonnet"),
            r#"{ columns: { default: ["id", "name"], tables: { orders: { default: ["id", "status"] } } } }"#,
        )
        .unwrap();
        fs::write(
            cwd.join(".latticeql/default.jsonnet"),
            r#"{ columns: { tables: { users: { default: ["id", "email"] } } } }"#,
        )
        .unwrap();

        let old_home = std::env::var("HOME").ok();
        std::env::set_var("HOME", &home);
        let cfg = load_column_defaults(&cwd).unwrap();
        if let Some(old_home) = old_home {
            std::env::set_var("HOME", old_home);
        }

        assert_eq!(cfg.global, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(
            cfg.for_table("orders"),
            &vec!["id".to_string(), "status".to_string()]
        );
        assert_eq!(
            cfg.for_table("users"),
            &vec!["id".to_string(), "email".to_string()]
        );
    }
}
