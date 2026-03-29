use std::collections::{HashMap, HashSet};

use crate::app::model::SchemaNode;
use super::widget::ColumnManagerWidget;

/// Persistent service that owns all column visibility state.
/// Always exists. The widget is a temporary interaction view
/// created via `open_widget`.
pub struct ColumnManagerService {
    default_visible: Vec<String>,
    default_visible_by_node: HashMap<String, Vec<String>>,
    visible: HashMap<String, Vec<String>>,
    order: HashMap<String, Vec<String>>,
}

impl ColumnManagerService {
    pub fn new(
        default_visible: Vec<String>,
        default_visible_by_node: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            default_visible,
            default_visible_by_node,
            visible: HashMap::new(),
            order: HashMap::new(),
        }
    }

    /// Returns configured default columns for a node, falling back to global defaults.
    fn configured_defaults(&self, node_name: &str) -> &[String] {
        self.default_visible_by_node
            .get(node_name)
            .map(|v| v.as_slice())
            .unwrap_or(&self.default_visible)
    }

    /// Initialize visibility for a schema node using config defaults.
    /// Only initializes if the node hasn't been registered yet.
    pub fn register_node(&mut self, node: &SchemaNode) {
        let all_col_names: Vec<String> = node.columns.iter().map(|c| c.name.clone()).collect();
        let defaults = self.configured_defaults(&node.name).to_vec();

        self.visible.entry(node.name.clone()).or_insert_with(|| {
            defaults
                .iter()
                .filter(|d| all_col_names.contains(d))
                .cloned()
                .collect()
        });

        self.order.entry(node.name.clone()).or_insert_with(|| {
            let default_set: HashSet<String> = defaults.iter().cloned().collect();
            let mut ordered: Vec<String> = defaults
                .iter()
                .filter(|d| all_col_names.contains(d))
                .cloned()
                .collect();
            for c in &all_col_names {
                if !default_set.contains(c) {
                    ordered.push(c.clone());
                }
            }
            ordered
        });
    }

    /// Which columns are visible for a given node.
    pub fn visible_columns(&self, node_name: &str) -> &[String] {
        self.visible.get(node_name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Full column ordering for a given node.
    pub fn column_order(&self, node_name: &str) -> &[String] {
        self.order.get(node_name).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Create a widget for the TUI overlay. `available_columns` is the set of
    /// columns actually present in the data (may differ from schema if rows
    /// have extra/missing fields).
    pub fn open_widget(&self, node_name: &str, available_columns: &[String]) -> ColumnManagerWidget {
        let visible: Vec<String> = self.visible_columns(node_name).to_vec();
        let mut ordered: Vec<String> = self.column_order(node_name).to_vec();

        // Add any columns present in data but not yet in the order list.
        for c in available_columns {
            if !ordered.contains(c) {
                ordered.push(c.clone());
            }
        }
        // Remove columns not present in the available data.
        ordered.retain(|c| available_columns.contains(c));

        ColumnManagerWidget::new(node_name.to_string(), ordered, visible)
    }

    /// Apply confirmed widget results back into the service.
    pub fn apply_widget(&mut self, widget: &ColumnManagerWidget) {
        self.visible.insert(widget.table.clone(), widget.visible_columns());
        self.order.insert(widget.table.clone(), widget.column_order());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::model::{ColumnDef, SchemaNode};

    fn schema_node(name: &str, cols: &[&str]) -> SchemaNode {
        SchemaNode {
            name: name.to_string(),
            columns: cols.iter().map(|c| ColumnDef {
                name: c.to_string(),
                data_type: "text".to_string(),
                is_primary_key: c == &"id",
                nullable: true,
            }).collect(),
        }
    }

    #[test]
    fn register_uses_defaults() {
        let mut mgr = ColumnManagerService::new(
            vec!["id".into(), "name".into()],
            HashMap::new(),
        );
        mgr.register_node(&schema_node("users", &["id", "name", "email"]));

        assert_eq!(mgr.visible_columns("users"), &["id", "name"]);
        assert_eq!(mgr.column_order("users"), &["id", "name", "email"]);
    }

    #[test]
    fn register_uses_per_node_defaults() {
        let mut per_node = HashMap::new();
        per_node.insert("users".to_string(), vec!["id".into(), "email".into()]);

        let mut mgr = ColumnManagerService::new(vec!["id".into(), "name".into()], per_node);
        mgr.register_node(&schema_node("users", &["id", "name", "email"]));

        assert_eq!(mgr.visible_columns("users"), &["id", "email"]);
    }

    #[test]
    fn register_ignores_missing_columns() {
        let mut mgr = ColumnManagerService::new(
            vec!["id".into(), "nonexistent".into()],
            HashMap::new(),
        );
        mgr.register_node(&schema_node("users", &["id", "name"]));

        assert_eq!(mgr.visible_columns("users"), &["id"]);
    }

    #[test]
    fn register_does_not_overwrite() {
        let mut mgr = ColumnManagerService::new(vec!["id".into()], HashMap::new());
        mgr.register_node(&schema_node("users", &["id", "name"]));
        // Manually change visibility
        mgr.visible.insert("users".into(), vec!["name".into()]);
        // Re-register should NOT overwrite
        mgr.register_node(&schema_node("users", &["id", "name"]));
        assert_eq!(mgr.visible_columns("users"), &["name"]);
    }

    #[test]
    fn open_widget_builds_items() {
        let mut mgr = ColumnManagerService::new(vec!["id".into()], HashMap::new());
        mgr.register_node(&schema_node("users", &["id", "name", "email"]));

        let widget = mgr.open_widget("users", &["id".into(), "name".into(), "email".into()]);
        assert_eq!(widget.items.len(), 3);
        assert!(widget.items[0].enabled);   // id is visible
        assert!(!widget.items[1].enabled);  // name is not
        assert!(!widget.items[2].enabled);  // email is not
    }

    #[test]
    fn apply_widget_updates_state() {
        let mut mgr = ColumnManagerService::new(vec!["id".into()], HashMap::new());
        mgr.register_node(&schema_node("users", &["id", "name"]));

        let mut widget = mgr.open_widget("users", &["id".into(), "name".into()]);
        widget.items[1].enabled = true; // enable "name"
        mgr.apply_widget(&widget);

        assert_eq!(mgr.visible_columns("users"), &["id", "name"]);
    }

    #[test]
    fn unknown_node_returns_empty() {
        let mgr = ColumnManagerService::new(vec![], HashMap::new());
        assert!(mgr.visible_columns("unknown").is_empty());
        assert!(mgr.column_order("unknown").is_empty());
    }
}
