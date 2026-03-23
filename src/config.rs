use anyhow::{Context, Result};
use rs_jsonnet::evaluate_with_filename;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

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

#[derive(Debug, Default, Deserialize)]
struct RawConfig {
    #[serde(default)]
    columns: RawColumnsConfig,
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

pub fn load_column_defaults(cwd: &Path) -> Result<ColumnDefaults> {
    let config_files = discover_config_files(cwd)?;
    let mut merged = ColumnDefaults::default();

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
        apply_layer(&mut merged, parsed);
    }

    Ok(merged)
}

fn apply_layer(target: &mut ColumnDefaults, layer: RawConfig) {
    if let Some(global) = layer.columns.default {
        target.global = global;
    }
    for (table, table_cfg) in layer.columns.tables {
        if let Some(cols) = table_cfg.default {
            target.per_table.insert(table, cols);
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
        let cfg = current.join(".arborql").join("default.jsonnet");
        if cfg.is_file() {
            found.push(cfg);
        }
        if !current.pop() {
            break;
        }
    }

    // If home was not encountered in the walk (cwd is outside $HOME),
    // append it as the most generic fallback.
    let home_cfg = home.join(".arborql").join("default.jsonnet");
    if home_cfg.is_file() && !found.contains(&home_cfg) {
        found.push(home_cfg);
    }

    Ok(found)
}

fn home_dir() -> Result<PathBuf> {
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
            "arborql-config-test-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        fs::create_dir_all(&base).unwrap();
        base
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
        fs::create_dir_all(home.join(".arborql")).unwrap();
        fs::create_dir_all(cwd.join(".arborql")).unwrap();

        fs::write(
            home.join(".arborql/default.jsonnet"),
            r#"{ columns: { default: ["id", "name"], tables: { orders: { default: ["id", "status"] } } } }"#,
        )
        .unwrap();
        fs::write(
            cwd.join(".arborql/default.jsonnet"),
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
