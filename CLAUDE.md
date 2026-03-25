# LatticeQL — Agent Orientation

## Purpose

LatticeQL is a terminal-based tool for navigating relational datasets **without writing SQL**. Given a live database connection, it discovers the schema automatically and lets you build hierarchical views by declaring rules: "load these rows", "attach their related records", "drop nodes matching this criterion". The result is an interactive, collapsible tree rendered in the terminal.

The central insight is that relational data already has structure — foreign keys define parent-child relationships — and a good UI should make that structure explorable in real time rather than forcing the user to compose JOIN queries. LatticeQL also handles polymorphic (Rails-style) associations that lack formal FK constraints through user-defined **virtual foreign keys**.

## Tech Stack

- **Language**: Rust (async, Tokio)
- **TUI**: Ratatui 0.29 + Crossterm 0.28
- **Databases**: MySQL and SQLite3 via `sqlx`
- **Config**: Jsonnet (evaluated by `rs-jsonnet`)
- **CLI args**: Clap

## Source Layout

```
src/
  main.rs          — entry point, event loop, all keyboard handling
  engine.rs        — DataNode tree, rule execution, DB queries
  rules.rs         — rule parser/grammar, tokenizer, completion hints
  schema.rs        — schema discovery, virtual FK definitions, BFS path-finding
  config.rs        — config file loading and merging
  log.rs           — thread-safe log queue
  db/
    mod.rs         — Database trait, Value/Row/TableInfo types
    sqlite.rs      — SQLite backend
    mysql.rs       — MySQL backend (uses information_schema)
  ui/
    app.rs         — AppState struct, Mode enum, helper methods
    render.rs      — all Ratatui rendering functions
manuals/           — in-app help manuals (read with 'm' key)
samples/           — example SQL files and pre-built SQLite databases
```

## Key Concepts

**Rules** are the user's instructions to the engine. There are three kinds:
- `Filter` — load rows matching a WHERE clause as root nodes
- `Relation` — follow FK links and attach related rows as children
- `Prune` — remove matching nodes from the tree in-memory

The engine holds an ordered list of rules. Re-executing them in order from scratch always produces a deterministic tree.

**DataNode** is a tree node: it knows its table, its row data (as a `HashMap<String, Value>`), and its children. The tree is flattened to a `Vec<(depth, &DataNode)>` for rendering.

**Schema** is discovered once at startup: table names, column info, real FK constraints, and user-defined virtual FKs. Path-finding (BFS) over this graph produces `TablePath` objects that the engine uses to write JOIN-equivalent queries.

**AppState** owns all UI state: current mode, cursor positions, visible columns per table, column ordering, rules list, overlay data. It is passed (mutably) to both the keyboard handler and the renderer.

**Mode** (an enum in `app.rs`) drives all TUI branching. Each mode has its own keyboard handler branch in `main.rs` and its own render path in `render.rs`. Adding a new overlay means adding a `Mode` variant, a handler branch, and a render function.

## Keyboard Shortcut Map (Normal Mode)

| Key | Action |
|-----|--------|
| `:` | Enter command mode |
| `j` / `↓` | Select next row |
| `k` / `↑` | Select previous row |
| `f` / `Enter` | Toggle fold |
| `s` | Toggle schema sidebar |
| `c` | Open column manager |
| `r` | Open rule reorder overlay |
| `v` | Open virtual FK manager |
| `m` | Open manual browser |
| `l` | Open log viewer |
| `x` | Prune selected node |
| `q` | Quit |

## Config File

Searched upward from `cwd` then from `~/.latticeql/config.jsonnet`. Format is Jsonnet:

```jsonnet
{
  columns: {
    default: ["id", "name"],
    tables: { users: { default: ["id", "email"] } }
  },
  virtual_fks: [
    {
      from_table: "comments", type_column: "commentable_type",
      type_value: "Post", id_column: "commentable_id",
      to_table: "posts", to_column: "id"
    }
  ]
}
```

## Adding a New Overlay

1. Add a variant to `Mode` in `src/ui/app.rs`.
2. Add a handler `match` arm in `handle_key` in `src/main.rs`.
3. Add a `render_*` function in `src/ui/render.rs` and call it from the overlay dispatch in `render()`.
4. Update the Normal-mode command-bar hint string in `render_command_bar`.

## Testing

Run `cargo test`. Tests live next to source files (SQLite-backed integration tests in `engine.rs`, parser unit tests in `rules.rs`, schema tests in `schema.rs`).

## Manuals

In-app documentation lives in `manuals/`. Press `m` in Normal mode to browse and read them. They are embedded at compile time via `include_str!`.
