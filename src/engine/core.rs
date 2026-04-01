use crate::db::{Database, Row, Value};
use crate::rules::{Rule, conditions_to_sql, row_matches_conditions};
use crate::schema::Schema;
use super::paths::{TablePath, PathSearchResult, find_paths, build_path_from_via, MAX_PATH_DEPTH};
use anyhow::Result;
use std::collections::HashMap;

/// A node in the hierarchical data tree.
#[derive(Debug, Clone)]
pub struct DataNode {
    pub table: String,
    pub row: Row,
    /// Child nodes related to this row.
    pub children: Vec<DataNode>,
    /// Whether this node is collapsed in the UI.
    pub collapsed: bool,
}

impl DataNode {
    pub fn new(table: String, row: Row) -> Self {
        Self {
            table,
            row,
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
        // Fall back to first column alphabetically
        let mut cols: Vec<&String> = self.row.keys().collect();
        cols.sort();
        if let Some(col) = cols.first() {
            if let Some(val) = self.row.get(*col) {
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

    /// Remove all nodes in the tree where the table matches and all conditions hold.
    /// Pruning is recursive: if a parent is pruned, its children are removed too.
    pub fn apply_prune_rule(
        &mut self,
        table: &str,
        conditions: &[crate::rules::Condition],
    ) {
        prune_nodes(&mut self.roots, table, conditions);
    }
    /// Follow the path and attach child nodes, batching SQL queries per step.
    ///
    /// For each step, collects FK values from all relevant frontier nodes,
    /// issues a single `WHERE col IN (...)` query, then distributes results
    /// back to the correct parent nodes.
    pub async fn apply_relation_rule(
        &mut self,
        db: &dyn Database,
        path: &TablePath,
    ) -> Result<usize> {
        if path.steps.is_empty() {
            return Ok(0);
        }
        let from_table = path.steps[0].from_table.clone();
        let mut frontier = find_matching_addrs(&self.roots, &from_table);
        let mut total = 0;

        for (step_idx, step) in path.steps.iter().enumerate() {
            // --- Collect phase (immutable) ---
            // Gather (frontier_index, sql_literal) for each eligible node.
            let mut fk_entries: Vec<(usize, String)> = Vec::new();
            for (i, addr) in frontier.iter().enumerate() {
                let node = node_at(&self.roots, addr);
                // Polymorphic forward filter
                if let Some((type_col, expected)) = &step.source_type_filter {
                    if node.row.get(type_col).map(|v| v.to_string()).unwrap_or_default() != *expected {
                        continue;
                    }
                }
                // Null FK check
                let fk_val = match node.row.get(&step.from_column) {
                    Some(Value::Null) | None => {
                        crate::log::info(format!(
                            "Traversal step {}: skipping node in '{}' — FK column '{}' is null/missing",
                            step_idx + 1, node.table, step.from_column
                        ));
                        continue;
                    }
                    Some(v) => v,
                };
                fk_entries.push((i, format_value_for_sql(fk_val)));
            }

            if fk_entries.is_empty() {
                break;
            }

            // --- Query phase (chunked to avoid exceeding DB limits) ---
            const CHUNK_SIZE: usize = 500;
            let unique_fks: Vec<String> = fk_entries
                .iter()
                .map(|(_, v)| v.clone())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect();

            let mut grouped: HashMap<Value, Vec<Row>> = HashMap::new();
            let mut step_row_count = 0;

            for chunk in unique_fks.chunks(CHUNK_SIZE) {
                let in_clause = chunk.join(", ");
                let sql = if let Some(extra) = &step.target_extra_where {
                    format!(
                        "SELECT * FROM {} WHERE {} IN ({}) AND {}",
                        step.to_table, step.to_column, in_clause, extra
                    )
                } else {
                    format!(
                        "SELECT * FROM {} WHERE {} IN ({})",
                        step.to_table, step.to_column, in_clause
                    )
                };
                let rows = db.query(&sql).await?;
                step_row_count += rows.len();
                for row in rows {
                    if let Some(key) = row.get(&step.to_column) {
                        grouped.entry(key.clone()).or_default().push(row);
                    }
                }
            }

            total += step_row_count;
            let chunks_used = (unique_fks.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;
            let id_summary = if unique_fks.len() <= 5 {
                unique_fks.join(", ")
            } else {
                format!(
                    "{}, ... +{} more",
                    unique_fks[..3].join(", "),
                    unique_fks.len() - 3
                )
            };
            crate::log::info(format!(
                "Traversal step {}/{}: {} → {} WHERE {} IN ({}) — {} ID(s), {} chunk(s), {} row(s)",
                step_idx + 1,
                path.steps.len(),
                step.from_table,
                step.to_table,
                step.to_column,
                id_summary,
                unique_fks.len(),
                chunks_used,
                step_row_count,
            ));

            // --- Attach phase (mutable) ---
            let mut next_frontier: Vec<NodeAddr> = Vec::new();
            for &(addr_idx, _) in &fk_entries {
                let addr = &frontier[addr_idx];
                let node = node_at_mut(&mut self.roots, addr);
                let fk_val = node.row.get(&step.from_column).unwrap();
                let child_start = node.children.len();
                if let Some(matching_rows) = grouped.get(fk_val) {
                    for row in matching_rows {
                        node.children
                            .push(DataNode::new(step.to_table.clone(), row.clone()));
                    }
                }
                for i in child_start..node.children.len() {
                    let mut child_addr = addr.clone();
                    child_addr.push(i);
                    next_frontier.push(child_addr);
                }
            }

            frontier = next_frontier;
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
    ) -> Result<Option<PathSearchResult>> {
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
                        crate::log::info(format!(
                            "Traversal: built explicit path {} via [{}]",
                            path.steps.iter().map(|s| format!("{}.{} → {}.{}", s.from_table, s.from_column, s.to_table, s.to_column)).collect::<Vec<_>>().join(", "),
                            via.join(", ")
                        ));
                        self.apply_relation_rule(db, &path).await?;
                        self.rules.push(rule);
                        return Ok(None);
                    } else {
                        crate::log::warn(format!(
                            "Traversal: could not build explicit path {} → {} via [{}] — no FK chain found; falling back to path search",
                            from_table, to_table, via.join(", ")
                        ));
                    }
                }
                let result =
                    find_paths(&self.schema, from_table, to_table, via, 1, MAX_PATH_DEPTH);
                if result.paths.is_empty() {
                    // Log which tables have FKs to help the user understand the schema
                    let schema_fk_summary: Vec<String> = self.schema.tables.iter()
                        .filter(|(_, info)| !info.foreign_keys.is_empty())
                        .map(|(name, info)| format!("{}: [{}]", name,
                            info.foreign_keys.iter().map(|fk| format!("{} → {}.{}", fk.from_column, fk.to_table, fk.to_column)).collect::<Vec<_>>().join(", ")))
                        .collect();
                    crate::log::warn(format!(
                        "No path found between '{}' and '{}' via [{}]. Known FK relationships: {}",
                        from_table, to_table, via.join(", "),
                        if schema_fk_summary.is_empty() { "none (no FK constraints in schema)".to_string() } else { schema_fk_summary.join("; ") }
                    ));
                    anyhow::bail!(
                        "No path found between '{}' and '{}' — check 'l' logs for schema FK details",
                        from_table,
                        to_table
                    );
                } else if result.paths.len() == 1 && !result.has_more {
                    let path = result.paths.into_iter().next().unwrap();
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
                    Ok(Some(result))
                }
            }
            Rule::Prune { table, conditions } => {
                let table = table.clone();
                let conditions = conditions.clone();
                self.apply_prune_rule(&table, &conditions);
                self.rules.push(rule);
                Ok(None)
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

/// Remove nodes matching `table` + `conditions` from the list, recursing into
/// children of non-matching nodes. A matched node is dropped with all its children.
fn prune_nodes(nodes: &mut Vec<DataNode>, table: &str, conditions: &[crate::rules::Condition]) {
    nodes.retain_mut(|node| {
        if node.table == table && row_matches_conditions(&node.row, conditions) {
            false // drop this node and all its children
        } else {
            prune_nodes(&mut node.children, table, conditions);
            true
        }
    });
}

/// Index-based address of a node in the tree.
/// `[0]` = `roots[0]`, `[0, 2]` = `roots[0].children[2]`, etc.
type NodeAddr = Vec<usize>;

/// Resolve an immutable node reference by address.
fn node_at<'a>(roots: &'a [DataNode], addr: &NodeAddr) -> &'a DataNode {
    let mut node = &roots[addr[0]];
    for &idx in &addr[1..] {
        node = &node.children[idx];
    }
    node
}

/// Resolve a mutable node reference by address.
fn node_at_mut<'a>(roots: &'a mut [DataNode], addr: &NodeAddr) -> &'a mut DataNode {
    let mut node = &mut roots[addr[0]];
    for &idx in &addr[1..] {
        node = &mut node.children[idx];
    }
    node
}

/// Walk the entire tree, collecting addresses of all nodes whose table matches.
/// Always recurses into children (even for matching nodes) so that nested
/// same-table nodes are found when rules are applied separately.
fn find_matching_addrs(roots: &[DataNode], table: &str) -> Vec<NodeAddr> {
    let mut addrs = Vec::new();
    for (i, root) in roots.iter().enumerate() {
        collect_addrs(root, table, &mut vec![i], &mut addrs);
    }
    addrs
}

fn collect_addrs(node: &DataNode, table: &str, addr: &mut Vec<usize>, out: &mut Vec<NodeAddr>) {
    if node.table == table {
        out.push(addr.clone());
    }
    for (i, child) in node.children.iter().enumerate() {
        addr.push(i);
        collect_addrs(child, table, addr, out);
        addr.pop();
    }
}

/// Format a `Value` as a SQL literal for use in queries.
fn format_value_for_sql(val: &Value) -> String {
    match val {
        Value::Integer(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bytes(b) => {
            let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
            format!("X'{}'", hex)
        }
        other => format!("'{}'", other.to_string().replace('\'', "''")),
    }
}




#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::paths::PathStep;
    use crate::db::Value;
    use crate::rules;

    use std::collections::HashMap;

    fn create_test_node(table: &str, id: i64) -> DataNode {
        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(id));
        DataNode::new(table.to_string(), row)
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

    #[tokio::test]
    async fn test_find_paths_via_single_table() {
        // Schema: users → orders → order_items → products
        // Via ["orders"] should find paths that pass through orders.
        let db = setup_test_db().await;
        let schema = crate::schema::Schema::explore(&db).await.unwrap();

        let via = vec!["orders".to_string()];
        let r = find_paths(&schema, "users", "products", &via, 1, MAX_PATH_DEPTH);

        assert!(!r.paths.is_empty(), "Should find at least one path via orders");
        for p in &r.paths {
            let tables: Vec<&str> = p.steps.iter().map(|s| s.from_table.as_str()).collect();
            assert!(
                tables.contains(&"orders"),
                "Every path must pass through 'orders', got: {:?}", tables
            );
        }
    }

    #[tokio::test]
    async fn test_find_paths_via_two_tables_order_sensitive() {
        // Schema: users → orders → order_items → products
        // The valid path traverses orders THEN order_items.
        let db = setup_test_db().await;
        let schema = crate::schema::Schema::explore(&db).await.unwrap();

        // Correct order: ["orders", "order_items"] — matches the traversal
        let via_correct = vec!["orders".to_string(), "order_items".to_string()];
        let r_correct = find_paths(&schema, "users", "products", &via_correct, 1, MAX_PATH_DEPTH);
        assert!(!r_correct.paths.is_empty(),
            "Should find paths when via tables are in correct traversal order");

        // Reversed order: ["order_items", "orders"] — does NOT match
        let via_reversed = vec!["order_items".to_string(), "orders".to_string()];
        let r_reversed = find_paths(&schema, "users", "products", &via_reversed, 1, MAX_PATH_DEPTH);
        assert!(r_reversed.paths.is_empty(),
            "Should find NO paths when via tables are in wrong order");
    }

    #[tokio::test]
    async fn test_single_traversal_products_only_on_inner_users() {
        // Single multi-step path: departments → users → products
        // Products should attach to the inner users (children of departments),
        // NOT to the top-level departments themselves.
        let db = setup_departments_users_products_db().await;
        let schema = crate::schema::Schema::explore(&db).await.unwrap();
        let mut engine = Engine::new(schema);

        // Load departments as roots
        engine.apply_filter_rule(&db, "departments", &[]).await.unwrap();
        assert_eq!(engine.roots.len(), 2);

        // Apply 2-step path: departments → users → products
        let path = crate::engine::paths::TablePath {
            steps: vec![
                crate::engine::paths::PathStep {
                    from_table: "departments".to_string(),
                    from_column: "id".to_string(),
                    to_table: "users".to_string(),
                    to_column: "department_id".to_string(),
                    ..Default::default()
                },
                crate::engine::paths::PathStep {
                    from_table: "users".to_string(),
                    from_column: "favorite_product_id".to_string(),
                    to_table: "products".to_string(),
                    to_column: "id".to_string(),
                    ..Default::default()
                },
            ],
        };
        engine.apply_relation_rule(&db, &path).await.unwrap();

        // Departments should have user children
        for dept in &engine.roots {
            assert_eq!(dept.table, "departments");
            assert!(!dept.children.is_empty(), "department should have user children");
            for user in &dept.children {
                assert_eq!(user.table, "users");
                // Users should have product children
                assert!(!user.children.is_empty(), "inner user should have product children");
                assert_eq!(user.children[0].table, "products");
            }
            // No products as direct children of departments
            assert!(
                !dept.children.iter().any(|c| c.table == "products"),
                "products should not be direct children of departments"
            );
        }
    }

    #[tokio::test]
    async fn test_split_traversals_products_on_all_users() {
        // Two separate traversals:
        // 1. departments to users (creates inner users under departments)
        // 2. users to products (should attach products to ALL users — both
        //    top-level and inner)
        let db = setup_departments_users_products_db().await;
        let schema = crate::schema::Schema::explore(&db).await.unwrap();
        let mut engine = Engine::new(schema);

        // Load both departments and users as roots
        engine.apply_filter_rule(&db, "departments", &[]).await.unwrap();
        engine.apply_filter_rule(&db, "users", &[]).await.unwrap();

        // Step 1: departments → users
        let dept_to_users = crate::engine::paths::TablePath {
            steps: vec![crate::engine::paths::PathStep {
                from_table: "departments".to_string(),
                from_column: "id".to_string(),
                to_table: "users".to_string(),
                to_column: "department_id".to_string(),
                ..Default::default()
            }],
        };
        engine.apply_relation_rule(&db, &dept_to_users).await.unwrap();

        // Step 2: users → products (should hit ALL users)
        let users_to_products = crate::engine::paths::TablePath {
            steps: vec![crate::engine::paths::PathStep {
                from_table: "users".to_string(),
                from_column: "favorite_product_id".to_string(),
                to_table: "products".to_string(),
                to_column: "id".to_string(),
                ..Default::default()
            }],
        };
        engine.apply_relation_rule(&db, &users_to_products).await.unwrap();

        // Check inner users (under departments) got products
        let dept_roots: Vec<&DataNode> = engine.roots.iter().filter(|r| r.table == "departments").collect();
        for dept in &dept_roots {
            for user in &dept.children {
                assert_eq!(user.table, "users");
                assert!(
                    user.children.iter().any(|c| c.table == "products"),
                    "inner user under department should have product children"
                );
            }
        }

        // Check top-level users also got products
        let user_roots: Vec<&DataNode> = engine.roots.iter().filter(|r| r.table == "users").collect();
        assert!(!user_roots.is_empty(), "should have top-level user roots");
        for user in &user_roots {
            assert!(
                user.children.iter().any(|c| c.table == "products"),
                "top-level user should also have product children"
            );
        }
    }
}
