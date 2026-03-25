# Data Viewing

The main screen is the Data Viewer — a scrollable, collapsible tree of database
rows. This manual explains how the tree is structured, what is shown, and how
to navigate it.


## Tree Structure

Each node in the tree represents a single database row. Nodes are indented
to show parent-child relationships established by Relation rules. A row from
`users` that has related `orders` looks like:

```
▼ [users] id: 1  name: Alice
    ▼ [orders] id: 10  status: pending
        ▼ [order_items] id: 100  product_id: 5
```

- `▼` means the node is expanded (children visible).
- `▶` means the node is collapsed (children hidden).
- Nodes with no children show no arrow.


## Summary Columns

Only a subset of each row's columns is shown in the tree line. These are the
"summary columns" for that table, controlled by:

1. The Column Manager overlay (`c` key) — per-session overrides.
2. `columns.tables.<table>.default` in `config.jsonnet` — table-specific defaults.
3. `columns.default` in `config.jsonnet` — global fallback.
4. Built-in fallback: `["id", "name"]` if nothing else is configured.

Columns are displayed as `column: value` pairs separated by `│`.


## Column Detail Bar

The bottom of the Data Viewer shows **all columns** for the currently selected
row. This is useful for inspecting values that are not in the summary columns.


## Navigation

| Key          | Action                          |
|--------------|---------------------------------|
| `j` / `↓`   | Move selection down             |
| `k` / `↑`   | Move selection up               |
| `f` / Enter  | Toggle fold/unfold node         |

The viewer auto-scrolls to keep the selected row visible.


## Schema Sidebar

Press `s` to toggle a sidebar listing all table names in the connected schema.
Width is fixed at 24 characters.


## Status Bar

At the bottom of the screen the status bar shows:

- The current mode name.
- In Normal mode: available key shortcuts.
- A warning indicator (`⚠`) if the log contains warnings or errors (press `l`
  to view them).


## Empty State

When no rules have been executed the tree is empty and the viewer shows:

```
Data Viewer (empty — type ':' to enter a command)
```

Enter a Filter rule (`:` then `<table> where ...`) to load data.


## Pruning Nodes

Press `x` on a selected node to remove it and its entire subtree. Internally
this inserts a Prune rule. The prune is applied in-memory immediately without
re-querying the database.
