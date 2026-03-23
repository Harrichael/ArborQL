use crate::db::{Database, TableInfo};
use anyhow::Result;
use std::collections::HashMap;

/// The full schema of a database, built by querying all tables.
#[derive(Debug, Default, Clone)]
pub struct Schema {
    pub tables: HashMap<String, TableInfo>,
}

impl Schema {
    /// Explore all tables in the database and build the schema.
    pub async fn explore(db: &dyn Database) -> Result<Self> {
        let table_names = db.list_tables().await?;
        let mut tables = HashMap::new();
        for name in table_names {
            let info = db.describe_table(&name).await?;
            tables.insert(name, info);
        }
        Ok(Self { tables })
    }

    /// Return a sorted list of table names for display.
    pub fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.keys().cloned().collect();
        names.sort();
        names
    }
}

/// One step in a relationship path (table A → table B via FK).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathStep {
    /// Source table
    pub from_table: String,
    /// Column in the source table
    pub from_column: String,
    /// Target table
    pub to_table: String,
    /// Column in the target table
    pub to_column: String,
}

impl std::fmt::Display for PathStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{} → {}.{}",
            self.from_table, self.from_column, self.to_table, self.to_column
        )
    }
}

/// An ordered sequence of steps describing a path between two tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TablePath {
    pub steps: Vec<PathStep>,
}

impl std::fmt::Display for TablePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = std::iter::once(self.steps[0].from_table.clone())
            .chain(self.steps.iter().map(|s| s.to_table.clone()))
            .collect();
        write!(f, "{}", parts.join(" → "))
    }
}

/// Find all simple paths between `from` and `to` in the schema graph.
///
/// The schema graph is undirected w.r.t. discovery (we traverse both FK
/// directions), but each `PathStep` records which direction was used.
pub fn find_paths(schema: &Schema, from: &str, to: &str) -> Vec<TablePath> {
    if !schema.tables.contains_key(from) || !schema.tables.contains_key(to) {
        return vec![];
    }
    if from == to {
        return vec![TablePath { steps: vec![] }];
    }

    let mut results = Vec::new();
    let mut visited = std::collections::HashSet::new();
    visited.insert(from.to_string());

    dfs(schema, from, to, &mut visited, &mut vec![], &mut results);
    results
}

fn dfs(
    schema: &Schema,
    current: &str,
    target: &str,
    visited: &mut std::collections::HashSet<String>,
    path: &mut Vec<PathStep>,
    results: &mut Vec<TablePath>,
) {
    // Edges from current table's foreign keys (forward direction)
    if let Some(info) = schema.tables.get(current) {
        for fk in &info.foreign_keys {
            if !visited.contains(&fk.to_table) {
                let step = PathStep {
                    from_table: current.to_string(),
                    from_column: fk.from_column.clone(),
                    to_table: fk.to_table.clone(),
                    to_column: fk.to_column.clone(),
                };
                path.push(step);
                if fk.to_table == target {
                    results.push(TablePath { steps: path.clone() });
                } else {
                    visited.insert(fk.to_table.clone());
                    dfs(schema, &fk.to_table, target, visited, path, results);
                    visited.remove(&fk.to_table);
                }
                path.pop();
            }
        }
    }

    // Reverse edges: other tables that have a FK pointing to `current`
    for (other_table, other_info) in &schema.tables {
        if visited.contains(other_table) {
            continue;
        }
        for fk in &other_info.foreign_keys {
            if fk.to_table == current {
                let step = PathStep {
                    from_table: current.to_string(),
                    from_column: fk.to_column.clone(),
                    to_table: other_table.clone(),
                    to_column: fk.from_column.clone(),
                };
                path.push(step);
                if other_table == target {
                    results.push(TablePath { steps: path.clone() });
                } else {
                    visited.insert(other_table.clone());
                    dfs(schema, other_table, target, visited, path, results);
                    visited.remove(other_table);
                }
                path.pop();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{ColumnInfo, ForeignKey, TableInfo};

    fn make_table(name: &str, fks: Vec<(&str, &str, &str)>) -> TableInfo {
        TableInfo {
            name: name.to_string(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "INTEGER".to_string(),
                nullable: false,
                is_primary_key: true,
            }],
            foreign_keys: fks
                .into_iter()
                .map(|(fc, tt, tc)| ForeignKey {
                    from_column: fc.to_string(),
                    to_table: tt.to_string(),
                    to_column: tc.to_string(),
                })
                .collect(),
        }
    }

    fn schema_from(tables: Vec<TableInfo>) -> Schema {
        let map = tables.into_iter().map(|t| (t.name.clone(), t)).collect();
        Schema { tables: map }
    }

    #[test]
    fn test_direct_path() {
        // users.location_id → locations.id
        let schema = schema_from(vec![
            make_table("users", vec![("location_id", "locations", "id")]),
            make_table("locations", vec![]),
        ]);
        let paths = find_paths(&schema, "users", "locations");
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].steps.len(), 1);
        assert_eq!(paths[0].steps[0].from_table, "users");
        assert_eq!(paths[0].steps[0].to_table, "locations");
    }

    #[test]
    fn test_indirect_path() {
        // users → user_groups → locations
        let schema = schema_from(vec![
            make_table("users", vec![("group_id", "user_groups", "id")]),
            make_table("user_groups", vec![("location_id", "locations", "id")]),
            make_table("locations", vec![]),
        ]);
        let paths = find_paths(&schema, "users", "locations");
        assert!(!paths.is_empty());
        let path = &paths[0];
        assert_eq!(path.steps.len(), 2);
    }

    #[test]
    fn test_no_path() {
        let schema = schema_from(vec![
            make_table("users", vec![]),
            make_table("locations", vec![]),
        ]);
        let paths = find_paths(&schema, "users", "locations");
        assert!(paths.is_empty());
    }

    #[test]
    fn test_same_table() {
        let schema = schema_from(vec![make_table("users", vec![])]);
        let paths = find_paths(&schema, "users", "users");
        assert_eq!(paths.len(), 1);
        assert!(paths[0].steps.is_empty());
    }

    #[test]
    fn test_multiple_paths() {
        // users can reach locations directly OR via assignments
        let schema = schema_from(vec![
            make_table(
                "users",
                vec![
                    ("location_id", "locations", "id"),
                    ("assignment_id", "assignments", "id"),
                ],
            ),
            make_table("assignments", vec![("location_id", "locations", "id")]),
            make_table("locations", vec![]),
        ]);
        let paths = find_paths(&schema, "users", "locations");
        assert!(paths.len() >= 2, "Expected multiple paths, got {}", paths.len());
    }
}
