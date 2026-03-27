use crate::schema::Schema;

/// One step in a relationship path (table A → table B via real or virtual FK).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PathStep {
    /// Source table
    pub from_table: String,
    /// Column in the source table
    pub from_column: String,
    /// Target table
    pub to_table: String,
    /// Column in the target table
    pub to_column: String,
    /// For polymorphic forward steps: only follow this step when the source
    /// node's named column equals the given value.
    pub source_type_filter: Option<(String, String)>,
    /// For polymorphic reverse steps: appended as `AND <str>` to the target query.
    pub target_extra_where: Option<String>,
}

impl std::fmt::Display for PathStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}.{} → {}.{}",
            self.from_table, self.from_column, self.to_table, self.to_column
        )?;
        if let Some((col, val)) = &self.source_type_filter {
            write!(f, " [when {}.{} = '{}']", self.from_table, col, val)?;
        }
        if let Some(extra) = &self.target_extra_where {
            write!(f, " [where {}]", extra)?;
        }
        Ok(())
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

/// Result of a path search — up to 10 paths plus a flag indicating more exist.
#[derive(Debug, Clone)]
pub struct PathSearchResult {
    pub paths: Vec<TablePath>,
    /// True when the search found more paths than returned and stopped early.
    pub has_more: bool,
    /// Depth to resume from when `has_more` is true (next unexplored depth level).
    pub next_depth: usize,
}

/// Find paths between `from` and `to` using iterative deepening DFS.
///
/// Searches depths `start_depth..=max_depth`, collecting paths shortest-first.
/// Once 10 or more paths have been accumulated, the current depth level is
/// finished and the search stops. If `via` is non-empty only paths that pass
/// through ALL of those intermediate tables are returned.
///
/// `has_more` is set when `max_depth` has not been exhausted, and `next_depth`
/// gives the depth to resume from in a subsequent call.
pub fn find_paths(
    schema: &Schema,
    from: &str,
    to: &str,
    via: &[String],
    start_depth: usize,
    max_depth: usize,
) -> PathSearchResult {
    const MAX_PATHS: usize = 10;

    if !schema.tables.contains_key(from) || !schema.tables.contains_key(to) {
        return PathSearchResult { paths: vec![], has_more: false, next_depth: max_depth + 1 };
    }
    if from == to {
        return PathSearchResult {
            paths: vec![TablePath { steps: vec![] }],
            has_more: false,
            next_depth: max_depth + 1,
        };
    }

    let mut results: Vec<TablePath> = Vec::new();
    let mut init_visited = std::collections::HashSet::new();
    init_visited.insert(from.to_string());
    let mut path_buf = Vec::new();

    for depth in start_depth..=max_depth {
        dfs_at_depth(
            schema,
            from,
            to,
            &mut path_buf,
            &mut init_visited,
            depth,
            via,
            &mut results,
        );

        if results.len() >= MAX_PATHS {
            return PathSearchResult {
                paths: results,
                has_more: depth < max_depth,
                next_depth: depth + 1,
            };
        }
    }

    PathSearchResult {
        paths: results,
        has_more: false,
        next_depth: max_depth + 1,
    }
}

/// Depth-limited DFS: find all paths of exactly `remaining_depth` steps from
/// `current` to `target`, appending valid ones to `results`.
///
/// Uses push/pop on mutable `path_so_far` and `visited` to avoid cloning at
/// every recursive call.
fn dfs_at_depth(
    schema: &Schema,
    current: &str,
    target: &str,
    path_so_far: &mut Vec<PathStep>,
    visited: &mut std::collections::HashSet<String>,
    remaining_depth: usize,
    via: &[String],
    results: &mut Vec<TablePath>,
) {
    if remaining_depth == 0 {
        if current == target {
            let candidate = TablePath { steps: path_so_far.clone() };
            if via_satisfied(&candidate, via) {
                results.push(candidate);
            }
        }
        return;
    }

    for (next_table, step) in edges_from(schema, current) {
        if visited.contains(&next_table) {
            continue;
        }
        path_so_far.push(step);
        visited.insert(next_table.clone());
        dfs_at_depth(
            schema,
            &next_table,
            target,
            path_so_far,
            visited,
            remaining_depth - 1,
            via,
            results,
        );
        visited.remove(&next_table);
        path_so_far.pop();
    }
}

/// Return all FK edges (forward, reverse, virtual) out of `table` as
/// `(next_table, step)` pairs.
fn edges_from(schema: &Schema, table: &str) -> Vec<(String, PathStep)> {
    let mut edges = Vec::new();

    // Forward edges: this table's own FKs
    if let Some(info) = schema.tables.get(table) {
        for fk in &info.foreign_keys {
            edges.push((fk.to_table.clone(), PathStep {
                from_table: table.to_string(),
                from_column: fk.from_column.clone(),
                to_table: fk.to_table.clone(),
                to_column: fk.to_column.clone(),
                ..Default::default()
            }));
        }
    }

    // Reverse edges: other tables whose FK points here
    for (other_table, other_info) in &schema.tables {
        if other_table == table { continue; }
        for fk in &other_info.foreign_keys {
            if fk.to_table == table {
                edges.push((other_table.clone(), PathStep {
                    from_table: table.to_string(),
                    from_column: fk.to_column.clone(),
                    to_table: other_table.clone(),
                    to_column: fk.from_column.clone(),
                    ..Default::default()
                }));
            }
        }
    }

    // Forward virtual FK edges
    for vfk in &schema.virtual_fks {
        if vfk.from_table == table {
            edges.push((vfk.to_table.clone(), PathStep {
                from_table: table.to_string(),
                from_column: vfk.id_column.clone(),
                to_table: vfk.to_table.clone(),
                to_column: vfk.to_column.clone(),
                source_type_filter: vfk.type_column.as_ref()
                    .zip(vfk.type_value.as_ref())
                    .map(|(col, val)| (col.clone(), val.clone())),
                ..Default::default()
            }));
        }
    }

    // Reverse virtual FK edges
    for vfk in &schema.virtual_fks {
        if vfk.to_table == table {
            let target_extra_where = vfk.type_column.as_ref()
                .zip(vfk.type_value.as_ref())
                .map(|(col, val)| format!("{} = '{}'", col, val.replace('\'', "''")));
            edges.push((vfk.from_table.clone(), PathStep {
                from_table: table.to_string(),
                from_column: vfk.to_column.clone(),
                to_table: vfk.from_table.clone(),
                to_column: vfk.id_column.clone(),
                target_extra_where,
                ..Default::default()
            }));
        }
    }

    edges
}

/// Return true if `path` passes through all `via` tables as intermediate nodes.
fn via_satisfied(path: &TablePath, via: &[String]) -> bool {
    if via.is_empty() {
        return true;
    }
    let intermediates: std::collections::HashSet<&str> = path
        .steps
        .iter()
        .skip(1)
        .map(|s| s.from_table.as_str())
        .collect();
    via.iter().all(|v| intermediates.contains(v.as_str()))
}

/// Build a `TablePath` from an explicit `via` list.
pub fn build_path_from_via(
    schema: &Schema,
    from: &str,
    to: &str,
    via: &[String],
) -> Option<TablePath> {
    // via contains intermediate tables; full sequence is: from → via[0] → via[1] → ... → to
    let sequence: Vec<&str> = std::iter::once(from)
        .chain(via.iter().map(|s| s.as_str()))
        .chain(std::iter::once(to))
        .collect();

    let mut steps = Vec::new();
    for window in sequence.windows(2) {
        let a = window[0];
        let b = window[1];
        // Find a FK between a and b
        if let Some(step) = find_step(schema, a, b) {
            steps.push(step);
        } else {
            return None;
        }
    }
    Some(TablePath { steps })
}

fn find_step(schema: &Schema, a: &str, b: &str) -> Option<PathStep> {
    if let Some(info) = schema.tables.get(a) {
        for fk in &info.foreign_keys {
            if fk.to_table == b {
                return Some(PathStep {
                    from_table: a.to_string(),
                    from_column: fk.from_column.clone(),
                    to_table: b.to_string(),
                    to_column: fk.to_column.clone(),
                    ..Default::default()
                });
            }
        }
    }
    // Reverse direction
    if let Some(info) = schema.tables.get(b) {
        for fk in &info.foreign_keys {
            if fk.to_table == a {
                return Some(PathStep {
                    from_table: a.to_string(),
                    from_column: fk.to_column.clone(),
                    to_table: b.to_string(),
                    to_column: fk.from_column.clone(),
                    ..Default::default()
                });
            }
        }
    }
    // Forward virtual FK: a owns the poly columns, b is the target
    for vfk in &schema.virtual_fks {
        if vfk.from_table == a && vfk.to_table == b {
            return Some(PathStep {
                from_table: a.to_string(),
                from_column: vfk.id_column.clone(),
                to_table: b.to_string(),
                to_column: vfk.to_column.clone(),
                source_type_filter: vfk.type_column.as_ref()
                    .zip(vfk.type_value.as_ref())
                    .map(|(col, val)| (col.clone(), val.clone())),
                ..Default::default()
            });
        }
    }
    // Reverse virtual FK: b owns the poly columns, a is the target
    for vfk in &schema.virtual_fks {
        if vfk.to_table == a && vfk.from_table == b {
            let target_extra_where = vfk.type_column.as_ref()
                .zip(vfk.type_value.as_ref())
                .map(|(col, val)| format!("{} = '{}'", col, val.replace('\'', "''")));
            return Some(PathStep {
                from_table: a.to_string(),
                from_column: vfk.to_column.clone(),
                to_table: b.to_string(),
                to_column: vfk.id_column.clone(),
                target_extra_where,
                ..Default::default()
            });
        }
    }
    None
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
                column_type: "INTEGER".to_string(),
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
        Schema { tables: map, virtual_fks: Vec::new() }
    }

    #[test]
    fn test_direct_path() {
        let schema = schema_from(vec![
            make_table("users", vec![("location_id", "locations", "id")]),
            make_table("locations", vec![]),
        ]);
        let r = find_paths(&schema, "users", "locations", &[], 1, 10);
        assert_eq!(r.paths.len(), 1);
        assert_eq!(r.paths[0].steps.len(), 1);
        assert_eq!(r.paths[0].steps[0].from_table, "users");
        assert_eq!(r.paths[0].steps[0].to_table, "locations");
    }

    #[test]
    fn test_indirect_path() {
        let schema = schema_from(vec![
            make_table("users", vec![("group_id", "user_groups", "id")]),
            make_table("user_groups", vec![("location_id", "locations", "id")]),
            make_table("locations", vec![]),
        ]);
        let r = find_paths(&schema, "users", "locations", &[], 1, 10);
        assert!(!r.paths.is_empty());
        let path = &r.paths[0];
        assert_eq!(path.steps.len(), 2);
    }

    #[test]
    fn test_no_path() {
        let schema = schema_from(vec![
            make_table("users", vec![]),
            make_table("locations", vec![]),
        ]);
        let r = find_paths(&schema, "users", "locations", &[], 1, 10);
        assert!(r.paths.is_empty());
    }

    #[test]
    fn test_same_table() {
        let schema = schema_from(vec![make_table("users", vec![])]);
        let r = find_paths(&schema, "users", "users", &[], 1, 10);
        assert_eq!(r.paths.len(), 1);
        assert!(r.paths[0].steps.is_empty());
    }

    #[test]
    fn test_multiple_paths() {
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
        let r = find_paths(&schema, "users", "locations", &[], 1, 10);
        assert!(r.paths.len() >= 2, "Expected multiple paths, got {}", r.paths.len());
    }
}
