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

    /// Execute a relation rule along a specific path. For each node anywhere in
    /// the tree that belongs to `from_table`, follow the path and attach child
    /// nodes (fetching any missing intermediate/target rows).
    pub async fn apply_relation_rule(
        &mut self,
        db: &dyn Database,
        path: &TablePath,
    ) -> Result<usize> {
        if path.steps.is_empty() {
            return Ok(0);
        }
        let from_table = path.steps[0].from_table.clone();
        let mut total = 0;
        for root in &mut self.roots {
            total += attach_to_all_matching(db, root, &from_table, path).await?;
        }
        Ok(total)
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
                resolved_path,
            } => {
                // If a path was previously resolved (auto or manual), use it
                // directly so future virtual FKs can't create ambiguity.
                if let Some(path) = resolved_path {
                    let path = path.clone();
                    self.apply_relation_rule(db, &path).await?;
                    self.rules.push(rule);
                    return Ok(None);
                }
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
                    let path = paths.into_iter().next().unwrap();
                    self.apply_relation_rule(db, &path).await?;
                    // Store the resolved path so re-execution is deterministic.
                    let stored = Rule::Relation {
                        from_table: from_table.clone(),
                        to_table: to_table.clone(),
                        via: via.clone(),
                        resolved_path: Some(path),
                    };
                    self.rules.push(stored);
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

        // Polymorphic forward filter: skip if type column doesn't match expected value
        if let Some((type_col, expected)) = &step.source_type_filter {
            let actual = node.row.get(type_col).map(|v| v.to_string()).unwrap_or_default();
            if actual != *expected {
                return Ok(0);
            }
        }

        // Get the FK value from this node
        let fk_val = match node.row.get(&step.from_column) {
            Some(v) => v.to_string(),
            None => return Ok(0),
        };

        // Build SQL, optionally with extra WHERE clause for reverse poly steps
        let sql = if let Some(extra) = &step.target_extra_where {
            format!(
                "SELECT * FROM {} WHERE {} = '{}' AND {}",
                step.to_table,
                step.to_column,
                fk_val.replace('\'', "''"),
                extra
            )
        } else {
            format!(
                "SELECT * FROM {} WHERE {} = '{}'",
                step.to_table,
                step.to_column,
                fk_val.replace('\'', "''")
            )
        };

        let rows = db.query(&sql).await?;
        let count = rows.len();
        for row in rows {
            let mut child = DataNode::new(step.to_table.clone(), row);
            attach_path_to_node(db, &mut child, path, step_idx + 1).await?;
            node.children.push(child);
        }
        Ok(count)
    })
}

/// Walk `node` and all its descendants, calling `attach_path_to_node` for every
/// node whose table matches `from_table`. Once a match is found, the path
/// traversal handles descending into that subtree; we only recurse through
/// non-matching nodes to search deeper.
fn attach_to_all_matching<'a>(
    db: &'a dyn Database,
    node: &'a mut DataNode,
    from_table: &'a str,
    path: &'a TablePath,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize>> + 'a>> {
    Box::pin(async move {
        if node.table == from_table {
            attach_path_to_node(db, node, path, 0).await
        } else {
            let mut total = 0;
            for child in &mut node.children {
                total += attach_to_all_matching(db, child, from_table, path).await?;
            }
            Ok(total)
        }
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
                source_type_filter: Some((vfk.type_column.clone(), vfk.type_value.clone())),
                ..Default::default()
            });
        }
    }
    // Reverse virtual FK: b owns the poly columns, a is the target
    for vfk in &schema.virtual_fks {
        if vfk.to_table == a && vfk.from_table == b {
            let extra = format!("{} = '{}'", vfk.type_column, vfk.type_value.replace('\'', "''"));
            return Some(PathStep {
                from_table: a.to_string(),
                from_column: vfk.to_column.clone(),
                to_table: b.to_string(),
                to_column: vfk.id_column.clone(),
                target_extra_where: Some(extra),
                ..Default::default()
            });
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
    use crate::rules;
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

    #[test]
    fn test_available_extra_columns_when_row_has_more_fields() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::Text("Alice".to_string()));
        row.insert("email".to_string(), Value::Text("alice@example.com".to_string()));
        row.insert("created_at".to_string(), Value::Text("2026-01-01".to_string()));

        let node = DataNode::new("users".to_string(), row);
        let mut extras = available_extra_columns(&node);
        extras.sort();

        assert!(
            extras.is_empty(),
            "with all columns visible by default, node-level extras should be empty"
        );
    }

    /// Create an in-memory SQLite database with users/orders/products and
    /// departments -> users, users -> products relations.
    async fn setup_test_db() -> crate::db::sqlite::SqliteDb {
        use sqlx::SqlitePool;
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let stmts = [
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, department_id INTEGER NOT NULL REFERENCES departments(id))",
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL REFERENCES users(id))",
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER NOT NULL REFERENCES orders(id), product_id INTEGER NOT NULL REFERENCES products(id))",
            "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')",
            "INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2)",
            "INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')",
            "INSERT INTO orders VALUES (10, 1), (11, 2)",
            "INSERT INTO order_items VALUES (1000, 10, 100), (1001, 11, 101)",
        ];
        for stmt in &stmts {
            sqlx::query(stmt).execute(&pool).await.unwrap();
        }
        crate::db::sqlite::SqliteDb::from_pool(pool)
    }

    /// Dedicated DB for departments -> users -> products tests with an
    /// unambiguous users -> products path.
    async fn setup_departments_users_products_db() -> crate::db::sqlite::SqliteDb {
        use sqlx::SqlitePool;
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let stmts = [
            "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, department_id INTEGER NOT NULL REFERENCES departments(id), favorite_product_id INTEGER NOT NULL REFERENCES products(id))",
            "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')",
            "INSERT INTO products VALUES (100, 'Widget'), (101, 'Gadget')",
            "INSERT INTO users VALUES (1, 'Alice', 1, 100), (2, 'Bob', 2, 101)",
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
                ..Default::default()
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
                    ..Default::default()
                },
                PathStep {
                    from_table: "orders".to_string(),
                    from_column: "id".to_string(),
                    to_table: "order_items".to_string(),
                    to_column: "order_id".to_string(),
                    ..Default::default()
                },
                PathStep {
                    from_table: "order_items".to_string(),
                    from_column: "product_id".to_string(),
                    to_table: "products".to_string(),
                    to_column: "id".to_string(),
                    ..Default::default()
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

    fn tree_signature(nodes: &[DataNode]) -> Vec<String> {
        fn walk(nodes: &[DataNode], prefix: &str, out: &mut Vec<String>) {
            for (i, node) in nodes.iter().enumerate() {
                let id = node
                    .row
                    .get("id")
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "?".to_string());
                let here = format!("{}{}:{}#{}", prefix, i, node.table, id);
                out.push(here.clone());
                let child_prefix = format!("{}>", here);
                walk(&node.children, &child_prefix, out);
            }
        }

        let mut out = Vec::new();
        walk(nodes, "", &mut out);
        out
    }

    #[tokio::test]
    async fn test_apply_relation_rule_on_nested_nodes() {
        // Reproduces: relation rules only applied to root-level nodes, missing
        // deeper matches after a prior relation rule created children.
        let db = setup_test_db().await;
        let schema = crate::schema::Schema::explore(&db).await.unwrap();
        let mut engine = Engine::new(schema);

        // 1. Load users as roots.
        engine.apply_filter_rule(&db, "users", &[]).await.unwrap();
        assert_eq!(engine.roots.len(), 2);

        // 2. Attach orders to users (users → orders).
        let users_to_orders = TablePath {
            steps: vec![PathStep {
                from_table: "users".to_string(),
                from_column: "id".to_string(),
                to_table: "orders".to_string(),
                to_column: "user_id".to_string(),
                ..Default::default()
            }],
        };
        engine.apply_relation_rule(&db, &users_to_orders).await.unwrap();
        assert_eq!(engine.roots[0].children.len(), 1, "each user should have 1 order");

        // 3. Attach order_items to orders — orders are *children*, not roots.
        let orders_to_items = TablePath {
            steps: vec![PathStep {
                from_table: "orders".to_string(),
                from_column: "id".to_string(),
                to_table: "order_items".to_string(),
                to_column: "order_id".to_string(),
                ..Default::default()
            }],
        };
        engine.apply_relation_rule(&db, &orders_to_items).await.unwrap();

        let order = &engine.roots[0].children[0];
        assert_eq!(order.table, "orders");
        assert_eq!(order.children.len(), 1, "order should have 1 order_item child");
        assert_eq!(order.children[0].table, "order_items");
    }

    #[tokio::test]
    async fn test_reinsert_root_rule_before_relation_replays_consistently() {
        let db = setup_test_db().await;
        let schema = crate::schema::Schema::explore(&db).await.unwrap();
        let mut engine = Engine::new(schema);

        let users_rule = rules::parse_rule("users").unwrap();
        let relation_rule = rules::parse_rule("users to products").unwrap();

        engine.execute_rule(&db, users_rule.clone()).await.unwrap();
        engine
            .execute_rule(&db, relation_rule.clone())
            .await
            .unwrap();

        let baseline_sig = tree_signature(&engine.roots);
        assert!(
            baseline_sig.iter().any(|s| s.contains(":products#")),
            "baseline should include products"
        );

        // Simulate deleting the root-producing rule and applying reorder.
        engine.rules.retain(|r| r != &users_rule);
        engine.reexecute_all(&db).await.unwrap();
        let after_delete_sig = tree_signature(&engine.roots);
        assert!(
            !after_delete_sig.iter().any(|s| s.contains(":products#")),
            "without users rule, products should not remain from stale state"
        );

        // Simulate setting insertion to beginning and adding users back.
        engine.rules.insert(0, users_rule);
        engine.reexecute_all(&db).await.unwrap();
        let restored_sig = tree_signature(&engine.roots);

        assert!(
            restored_sig.iter().any(|s| s.contains(":products#")),
            "reinserted users rule should restore products through relation rule"
        );
        assert_eq!(
            restored_sig, baseline_sig,
            "restored response should match original baseline response"
        );
    }

    #[tokio::test]
    async fn test_departments_users_products_sequence_reaches_nested_users() {
        let db = setup_departments_users_products_db().await;
        let schema = crate::schema::Schema::explore(&db).await.unwrap();
        let mut engine = Engine::new(schema);

        let rule_departments = rules::parse_rule("departments").unwrap();
        let rule_departments_users = rules::parse_rule("departments to users").unwrap();
        let rule_users_products = rules::parse_rule("users to products").unwrap();

        engine.execute_rule(&db, rule_departments).await.unwrap();
        engine.execute_rule(&db, rule_departments_users).await.unwrap();
        engine.execute_rule(&db, rule_users_products).await.unwrap();

        let sig = tree_signature(&engine.roots);
        assert!(
            sig.iter().any(|s| s.contains(":users#")),
            "departments to users should attach users"
        );
        assert!(
            sig.iter().any(|s| s.contains(":products#")),
            "users to products should reach users nested under departments"
        );
    }
}
