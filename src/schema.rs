use crate::db::{Database, TableInfo};
use anyhow::Result;
use std::collections::HashMap;

/// A user-defined virtual foreign key.
///
/// Supports both simple direct foreign keys (no type discrimination) and
/// polymorphic (Rails-style) associations where a discriminator column
/// determines which target table the id column points to.
///
/// Stored in `Schema.virtual_fks` and treated natively alongside real FK edges
/// during path finding and traversal.
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualFkDef {
    /// Table that owns the id column (e.g. `"comments"`).
    pub from_table: String,
    /// Optional discriminator column for polymorphic associations
    /// (e.g. `"commentable_type"`). `None` for simple direct FKs.
    pub type_column: Option<String>,
    /// Value the type column must hold for this FK (e.g. `"Post"`).
    /// Only meaningful when `type_column` is `Some`.
    pub type_value: Option<String>,
    /// Column in `from_table` that holds the referenced PK (e.g. `"commentable_id"`).
    pub id_column: String,
    /// Target table (e.g. `"posts"`).
    pub to_table: String,
    /// PK column on the target table (usually `"id"`).
    pub to_column: String,
}

/// The full schema of a database, built by querying all tables.
#[derive(Debug, Default, Clone)]
pub struct Schema {
    pub tables: HashMap<String, TableInfo>,
    /// User-defined virtual FKs for polymorphic associations.
    /// Treated natively alongside real FK edges during path finding.
    pub virtual_fks: Vec<VirtualFkDef>,
}

impl Schema {
    /// Explore all tables in the database and build the schema.
    pub async fn explore(db: &dyn Database) -> Result<Self> {
        let table_names = db.list_tables().await?;
        eprintln!("Found {} tables, loading metadata…", table_names.len());
        let table_infos = db.describe_all_tables(&table_names).await?;
        let tables = table_infos.into_iter().map(|t| (t.name.clone(), t)).collect();
        Ok(Self { tables, virtual_fks: Vec::new() })
    }

    /// Return a sorted list of table names for display.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }
}

