use crate::engine::DataNode;

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

/// Toggle the collapsed state of the node at `flat_idx` in the tree.
pub fn toggle_fold(roots: &mut [DataNode], flat_idx: usize) {
    let mut counter = 0usize;
    toggle_fold_recursive(roots, flat_idx, &mut counter);
}

fn toggle_fold_recursive(
    nodes: &mut [DataNode],
    target: usize,
    counter: &mut usize,
) -> bool {
    for node in nodes.iter_mut() {
        if *counter == target {
            node.collapsed = !node.collapsed;
            return true;
        }
        *counter += 1;
        if !node.collapsed && toggle_fold_recursive(&mut node.children, target, counter) {
            return true;
        }
    }
    false
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
    fn test_toggle_fold() {
        let mut parent = create_test_node("users", 1);
        parent.children.push(create_test_node("orders", 10));
        let mut roots = vec![parent];

        assert!(!roots[0].collapsed);
        toggle_fold(&mut roots, 0);
        assert!(roots[0].collapsed);
        toggle_fold(&mut roots, 0);
        assert!(!roots[0].collapsed);
    }
}
