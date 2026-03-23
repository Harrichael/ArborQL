use crate::db::{Database, Row};
use crate::rules::{Rule, conditions_to_sql};
use crate::schema::{Schema, TablePath};
use anyhow::Result;

/// A node in the hierarchical data tree.
#[derive(Debug, Clone)]
pub struct DataNode {
    pub table: String,
    pub row: Row,
    /// Columns to display (subset of row keys, in order).
    pub visible_columns: Vec<String>,
    /// Child nodes related to this row.
    pub children: Vec<DataNode>,
    /// Whether this node is collapsed in the UI.
    pub collapsed: bool,
}

impl DataNode {
    pub fn new(table: String, row: Row) -> Self {
        let mut visible_columns: Vec<String> = row.keys().cloned().collect();
        visible_columns.sort();
        Self {
            table,
            row,
            visible_columns,
            children: Vec::new(),
            collapsed: false,
        }
    }

    /// Return a short string summary for display (first pk-like column).
    pub fn summary(&self) -> String {
        let id_candidates = ["id", "name", "title", "label"];
        for candidate in &id_candidates {
            if let Some(val) = self.row.get(*candidate) {
                return format!("{}: {}", candidate, val);
            }
        }
        // Fall back to first visible column
        if let Some(col) = self.visible_columns.first() {
            if let Some(val) = self.row.get(col) {
                return format!("{}: {}", col, val);
            }
        }
        "(empty row)".to_string()
    }
}

/// The core data engine: holds the schema and the accumulated data tree.
pub struct Engine {
    pub schema: Schema,
    pub roots: Vec<DataNode>,
    pub rules: Vec<Rule>,
}

impl Engine {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            roots: Vec::new(),
            rules: Vec::new(),
        }
    }

    /// Execute a filter rule and add matching rows as root nodes.
    pub async fn apply_filter_rule(
        &mut self,
        db: &dyn Database,
        table: &str,
        conditions: &[crate::rules::Condition],
    ) -> Result<usize> {
        let where_clause = conditions_to_sql(conditions);
        let sql = if where_clause.is_empty() {
            format!("SELECT * FROM {}", table)
        } else {
            format!("SELECT * FROM {} WHERE {}", table, where_clause)
        };
        let rows = db.query(&sql).await?;
        let count = rows.len();
        for row in rows {
            self.roots.push(DataNode::new(table.to_string(), row));
        }
        Ok(count)
    }

    /// Execute a relation rule along a specific path. For each existing root
    /// node that belongs to `from_table`, follow the path and attach child
    /// nodes (fetching any missing intermediate/target rows).
    pub async fn apply_relation_rule(
        &mut self,
        db: &dyn Database,
        path: &TablePath,
    ) -> Result<usize> {
        if path.steps.is_empty() {
            return Ok(0);
        }
        let mut total = 0;
        // We iterate over root indices to avoid borrow issues
        let n = self.roots.len();
        for i in 0..n {
            if self.roots[i].table == path.steps[0].from_table {
                let added = self
                    .attach_path(db, i, path, 0)
                    .await?;
                total += added;
            }
        }
        Ok(total)
    }

    /// Recursively attach path steps to a node.
    async fn attach_path(
        &mut self,
        db: &dyn Database,
        node_idx: usize,
        path: &TablePath,
        step_idx: usize,
    ) -> Result<usize> {
        if step_idx >= path.steps.len() {
            return Ok(0);
        }
        let step = &path.steps[step_idx];
        // Get the FK value from this node
        let fk_val = match self.roots[node_idx].row.get(&step.from_column) {
            Some(v) => v.to_string(),
            None => return Ok(0),
        };
        // Fetch matching rows from the next table
        let sql = format!(
            "SELECT * FROM {} WHERE {} = '{}'",
            step.to_table,
            step.to_column,
            fk_val.replace('\'', "''")
        );
        let rows = db.query(&sql).await?;
        let count = rows.len();
        for row in rows {
            let child = DataNode::new(step.to_table.clone(), row);
            self.roots[node_idx].children.push(child);
        }
        // Recurse into newly added children for the next step
        if step_idx + 1 < path.steps.len() {
            let child_count = self.roots[node_idx].children.len();
            let start = child_count.saturating_sub(count);
            // We need to handle nested children differently since they're embedded
            // For simplicity, process children of the current node
            for ci in start..child_count {
                let next_fk_val = match self.roots[node_idx].children[ci]
                    .row
                    .get(&path.steps[step_idx + 1].from_column)
                {
                    Some(v) => v.to_string(),
                    None => continue,
                };
                let next_step = &path.steps[step_idx + 1];
                let next_sql = format!(
                    "SELECT * FROM {} WHERE {} = '{}'",
                    next_step.to_table,
                    next_step.to_column,
                    next_fk_val.replace('\'', "''")
                );
                let next_rows = db.query(&next_sql).await?;
                for row in next_rows {
                    let grandchild = DataNode::new(next_step.to_table.clone(), row);
                    self.roots[node_idx].children[ci].children.push(grandchild);
                }
            }
        }
        Ok(count)
    }

    /// Execute a rule (dispatching to filter or relation).
    /// Returns `Ok(None)` for filter rules or relation rules with a single path.
    /// Returns `Ok(Some(paths))` when multiple paths exist and user must choose.
    pub async fn execute_rule(
        &mut self,
        db: &dyn Database,
        rule: Rule,
    ) -> Result<Option<Vec<TablePath>>> {
        match &rule {
            Rule::Filter { table, conditions } => {
                let table = table.clone();
                let conditions = conditions.clone();
                self.apply_filter_rule(db, &table, &conditions).await?;
                self.rules.push(rule);
                Ok(None)
            }
            Rule::Relation {
                from_table,
                to_table,
                via,
            } => {
                if !via.is_empty() {
                    // User already specified the path
                    let path = build_path_from_via(
                        &self.schema,
                        from_table,
                        to_table,
                        via,
                    );
                    if let Some(path) = path {
                        self.apply_relation_rule(db, &path).await?;
                        self.rules.push(rule);
                        return Ok(None);
                    }
                }
                // Find all paths
                let paths =
                    crate::schema::find_paths(&self.schema, from_table, to_table);
                if paths.is_empty() {
                    anyhow::bail!(
                        "No path found between '{}' and '{}'",
                        from_table,
                        to_table
                    );
                } else if paths.len() == 1 {
                    self.apply_relation_rule(db, &paths[0]).await?;
                    self.rules.push(rule);
                    Ok(None)
                } else {
                    // Multiple paths — let the UI ask the user to pick
                    Ok(Some(paths))
                }
            }
        }
    }

    /// Re-execute all rules in order (used when rules are reordered).
    pub async fn reexecute_all(&mut self, db: &dyn Database) -> Result<()> {
        self.roots.clear();
        let rules = self.rules.clone();
        // Replay against a clean rules buffer so execute_rule doesn't append
        // duplicates during re-execution.
        self.rules.clear();
        for rule in rules {
            self.execute_rule(db, rule).await?;
        }
        Ok(())
    }
}

/// Build a `TablePath` from an explicit `via` list.
fn build_path_from_via(
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

fn find_step(schema: &Schema, a: &str, b: &str) -> Option<crate::schema::PathStep> {
    use crate::schema::PathStep;
    if let Some(info) = schema.tables.get(a) {
        for fk in &info.foreign_keys {
            if fk.to_table == b {
                return Some(PathStep {
                    from_table: a.to_string(),
                    from_column: fk.from_column.clone(),
                    to_table: b.to_string(),
                    to_column: fk.to_column.clone(),
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
                });
            }
        }
    }
    None
}


/// Flatten the data tree into a list of (depth, node_ref) for rendering.
pub fn flatten_tree(roots: &[DataNode]) -> Vec<(usize, &DataNode)> {
    let mut out = Vec::new();
    for node in roots {
        flatten_node(node, 0, &mut out);
    }
    out
}

fn flatten_node<'a>(
    node: &'a DataNode,
    depth: usize,
    out: &mut Vec<(usize, &'a DataNode)>,
) {
    out.push((depth, node));
    if !node.collapsed {
        for child in &node.children {
            flatten_node(child, depth + 1, out);
        }
    }
}

/// Collect all extra column names available for a node (those not in
/// visible_columns).
pub fn available_extra_columns(node: &DataNode) -> Vec<String> {
    node.row
        .keys()
        .filter(|k| !node.visible_columns.contains(k))
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Value;

    use std::collections::HashMap;

    fn create_test_node(table: &str, id: i64) -> DataNode {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(id));
        DataNode::new(table.to_string(), row)
    }

    #[test]
    fn test_flatten_tree_empty() {
        let roots: Vec<DataNode> = vec![];
        let flat = flatten_tree(&roots);
        assert!(flat.is_empty());
    }

    #[test]
    fn test_flatten_tree_nested() {
        let mut parent = create_test_node("users", 1);
        parent.children.push(create_test_node("orders", 10));
        parent.children.push(create_test_node("orders", 11));
        let roots = vec![parent];
        let flat = flatten_tree(&roots);
        assert_eq!(flat.len(), 3);
        assert_eq!(flat[0].0, 0);
        assert_eq!(flat[1].0, 1);
        assert_eq!(flat[2].0, 1);
    }

    #[test]
    fn test_flatten_collapsed() {
        let mut parent = create_test_node("users", 1);
        parent.collapsed = true;
        parent.children.push(create_test_node("orders", 10));
        let roots = vec![parent];
        let flat = flatten_tree(&roots);
        assert_eq!(flat.len(), 1); // children hidden
    }

    #[test]
    fn test_node_summary() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(42));
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        let node = DataNode::new("users".to_string(), row);
        // "id" comes before "name" in candidates
        assert!(node.summary().contains("id") || node.summary().contains("name"));
    }
}
