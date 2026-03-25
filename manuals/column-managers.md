# Column Managers

Each table has its own set of visible columns in the tree. The Column Manager
overlay lets you choose which columns appear in the tree summary line and in
what order.


## Opening the Overlay

Navigate to any row in the tree, then press `c`. The overlay opens for that
row's table.


## Layout

The overlay shows an ordered list of all columns for the table. Each row has:

```
[x] column_name     (enabled — shown in tree)
[ ] other_column    (disabled — hidden in tree)
```


## Key Bindings

| Key       | Action                                            |
|-----------|---------------------------------------------------|
| `↑` / `k` | Move cursor up                                   |
| `↓` / `j` | Move cursor down                                 |
| `Space` / `x` | Toggle selected column on/off                |
| `u`       | Move selected column earlier in the list          |
| `d`       | Move selected column later in the list            |
| `/`       | Activate search filter                            |
| `Enter`   | Apply changes                                     |
| `Esc`     | Cancel (or clear search if search is active)      |

`u` and `d` (reordering) are disabled while the search filter is active.


## Search Filter

Press `/` to activate the search input. Type to filter the column list by
name. Press `Esc` once to deactivate search input (keep the filter text),
press `Esc` again to clear the filter entirely.


## Applying Changes

Press `Enter` to confirm. The tree immediately updates to show only the
enabled columns for that table. The column order is also saved and used for
future displays of the same table within this session.


## Default Columns

When a table is first encountered, the visible columns are seeded from:

1. `columns.tables.<table>.default` in `config.jsonnet`.
2. `columns.default` in `config.jsonnet` (global fallback).
3. Hard-coded fallback: `["id", "name"]`.

Only columns that actually exist in the row data are shown.


## Persistence

Column visibility and order are **session-only** — they reset when you
restart LatticeQL. To make defaults permanent, set them in `config.jsonnet`:

```jsonnet
{
  columns: {
    default: ["id", "name"],
    tables: {
      orders: { default: ["id", "status", "total_cents"] }
    }
  }
}
```
