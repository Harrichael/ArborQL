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

    /// Attach path steps to a root node, recursively traversing all steps.
    async fn attach_path(
        &mut self,
        db: &dyn Database,
        node_idx: usize,
        path: &TablePath,
        step_idx: usize,
    ) -> Result<usize> {
        attach_path_to_node(db, &mut self.roots[node_idx], path, step_idx).await
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
        for rule in rules {
            self.execute_rule(db, rule).await?;
        }
        Ok(())
    }
}

/// Recursively attach path steps starting at `step_idx` to `node`, fetching
/// children from the database and recursing into each child for the next step.
/// Uses `Box::pin` to allow the async function to call itself recursively.
fn attach_path_to_node<'a>(
    db: &'a dyn Database,
    node: &'a mut DataNode,
    path: &'a TablePath,
    step_idx: usize,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize>> + 'a>> {
    Box::pin(async move {
        if step_idx >= path.steps.len() {
            return Ok(0);
        }
        let step = &path.steps[step_idx];
        // Get the FK value from this node
        let fk_val = match node.row.get(&step.from_column) {
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
            let mut child = DataNode::new(step.to_table.clone(), row);
            // Recursively attach subsequent path steps to this child
            attach_path_to_node(db, &mut child, path, step_idx + 1).await?;
            node.children.push(child);
        }
        Ok(count)
    })
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
    use crate::schema::PathStep;

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

    /// Create an in-memory SQLite database with a 3-table schema mirroring the
    /// users → orders → order_items → products chain.
    async fn setup_test_db() -> crate::db::sqlite::SqliteDb {
        use sqlx::SqlitePool;
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let stmts = [
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id))",
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES orders(id), product_id INTEGER NOT NULL REFERENCES products(id))",
            "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')",
            "INSERT INTO orders VALUES (10, 1), (11, 2)",
            "INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')",
            "INSERT INTO order_items VALUES (1000, 10, 100), (1001, 11, 101)",
        ];
        for stmt in &stmts {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        crate::db::sqlite::SqliteDb::from_pool(pool)
    }

    #[tokio::test]
    async fn test_apply_relation_rule_single_step() {
        // users → orders (reverse edge: orders.user_id → users.id)
        let db = setup_test_db().await;
        let path = TablePath {
            steps: vec![PathStep {
                from_table: "users".to_string(),
                from_column: "id".to_string(),
                to_table: "orders".to_string(),
                to_column: "user_id".to_string(),
            }],
        };
        let schema = crate::schema::Schema::default();
        let mut engine = Engine::new(schema);
        engine.roots.push(create_test_node("users", 1));
        engine.roots.push(create_test_node("users", 2));

        let count = engine.apply_relation_rule(&db, &path).await.unwrap();
        assert_eq!(count, 2); // one order per user
        assert_eq!(engine.roots[0].children.len(), 1);
        assert_eq!(engine.roots[1].children.len(), 1);
    }

    #[tokio::test]
    async fn test_apply_relation_rule_three_steps() {
        // users → orders → order_items → products (3 steps)
        let db = setup_test_db().await;
        let path = TablePath {
            steps: vec![
                PathStep {
                    from_table: "users".to_string(),
                    from_column: "id".to_string(),
                    to_table: "orders".to_string(),
                    to_column: "user_id".to_string(),
                },
                PathStep {
                    from_table: "orders".to_string(),
                    from_column: "id".to_string(),
                    to_table: "order_items".to_string(),
                    to_column: "order_id".to_string(),
                },
                PathStep {
                    from_table: "order_items".to_string(),
                    from_column: "product_id".to_string(),
                    to_table: "products".to_string(),
                    to_column: "id".to_string(),
                },
            ],
        };
        let schema = crate::schema::Schema::default();
        let mut engine = Engine::new(schema);
        engine.roots.push(create_test_node("users", 1));

        engine.apply_relation_rule(&db, &path).await.unwrap();

        // Alice has 1 order, that order has 1 order_item, that item links to 1 product
        assert_eq!(engine.roots[0].children.len(), 1, "user should have 1 order");
        let order = &engine.roots[0].children[0];
        assert_eq!(order.table, "orders");
        assert_eq!(order.children.len(), 1, "order should have 1 order_item");
        let item = &order.children[0];
        assert_eq!(item.table, "order_items");
        assert_eq!(item.children.len(), 1, "order_item should have 1 product");
        let product = &item.children[0];
        assert_eq!(product.table, "products");
        assert_eq!(product.children.len(), 0);
    }
}
