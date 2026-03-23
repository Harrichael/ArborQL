use serde::Deserialize;
use std::collections::HashMap;

/// Top-level config file format (`arborql.toml`).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ArborConfig {
    #[serde(default)]
    pub polymorphic_associations: Vec<PolymorphicAssocConfig>,
}

/// One polymorphic association entry in the config file.
///
/// Example TOML:
/// ```toml
/// [[polymorphic_associations]]
/// table        = "comments"
/// type_column  = "commentable_type"
/// id_column    = "commentable_id"
/// type_map     = { Post = "posts", Photo = "photos" }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct PolymorphicAssocConfig {
    /// Table that owns the type/id column pair (e.g. `"comments"`).
    pub table: String,
    /// Column storing the discriminator value (e.g. `"commentable_type"`).
    pub type_column: String,
    /// Column storing the associated record's primary key (e.g. `"commentable_id"`).
    pub id_column: String,
    /// Primary-key column on the target tables. Defaults to `"id"`.
    #[serde(default = "default_id")]
    pub target_id_column: String,
    /// Maps discriminator string values to table names (e.g. `Post → "posts"`).
    #[serde(default)]
    pub type_map: HashMap<String, String>,
}

fn default_id() -> String {
    "id".to_string()
}

/// Load and parse an `ArborConfig` from a TOML file at `path`.
pub fn load(path: &str) -> anyhow::Result<ArborConfig> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Cannot read config '{}': {}", path, e))?;
    toml::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Config parse error in '{}': {}", path, e))
}
