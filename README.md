# ArborQL 🌲

Navigate complex relational datasets intuitively in a unified terminal interface.

ArborQL connects to a SQL database, automatically explores the schema (tables, columns and foreign-key relationships), and lets you build up a hierarchical view of the data by typing simple rules in a command bar — no SQL required.

---

## Features

| Feature | Description |
|---------|-------------|
| **Auto schema discovery** | On startup, ArborQL reads every table's columns and foreign-key graph |
| **Filter rules** | Load rows matching a condition: `users where name startswith 'Rick'` |
| **Relation rules** | Traverse FK links between tables: `users to orders` |
| **Explicit path** | Pin the exact join path: `users to locations via departments` |
| **Path selection** | When multiple FK paths exist, ArborQL presents a list to pick from |
| **Tree view** | Related rows are shown as collapsible children (`▼`/`▶`) |
| **Column expansion** | Choose which columns to show for any individual row |
| **Rule reordering** | Drag rules up/down and replay them in a new order |
| **Schema sidebar** | Toggle a panel listing every table in the database |
| **MySQL + SQLite3** | Two database backends supported out of the box |

---

## Installation

```bash
# Clone and build (requires Rust 1.70+)
git clone https://github.com/Harrichael/ArborQL.git
cd ArborQL
cargo build --release
# Binary is now at  ./target/release/arborql
```

---

## Quick start with the sample databases

Two ready-made SQLite3 databases live in `samples/`.

**`ecommerce`** — a small e-commerce / team directory:

```
locations ◄─── departments ◄─── users ───► locations
                                  │
                                orders ────► order_items ◄──── products
                                                                   │
                                                             product_tags ◄── tags
```

**`blog`** — a social blogging platform with polymorphic associations:

```
users ───► posts ◄─── comments (polymorphic: Post / Photo)
      ───► photos ◄── likes    (polymorphic: Post / Photo / Comment)
                 ◄─── taggings (polymorphic: Post / Photo) ───► tags
```

**Start ArborQL:**

```bash
./target/release/arborql --database sqlite://samples/ecommerce.db
./target/release/arborql --database sqlite://samples/blog.db
```

### Example session (ecommerce)

Once the TUI is running, press `:` to enter a command:

```
# Load all users
:users

# Load only users whose name starts with 'Rick'
:users where name startswith 'Rick'

# Load all orders for those users (follows users.id → orders.user_id FK)
:users to orders

# Drill deeper – attach order items to every order
:orders to order_items

# Traverse from users all the way to locations via departments
:users to locations via departments

# Load products with a specific category
:products where category = 'Hardware'
```

---

## Usage

```
arborql --database <URL>
```

| Database | URL format |
|----------|-----------|
| SQLite3  | `sqlite://path/to/database.db` or `sqlite:///absolute/path.db` |
| MySQL    | `mysql://user:password@host/dbname` |

### Key bindings

| Key | Action |
|-----|--------|
| `:` | Enter command mode |
| `Esc` | Cancel / close overlay |
| `j` / `↓` | Move selection down |
| `k` / `↑` | Move selection up |
| `f` / `Enter` | Fold / unfold selected row's children |
| `s` | Toggle schema sidebar |
| `c` | Add a column to the selected row's display |
| `r` | Open rule-reorder overlay |
| `q` | Quit |

### Rule syntax

**Filter rule** — load rows from a table matching one or more conditions:

```
<table> where <column> <op> <value> [and <column> <op> <value> ...]
```

| Operator | Meaning |
|----------|---------|
| `=`          | equal |
| `!=`         | not equal |
| `<` `<=` `>` `>=` | numeric / lexicographic comparison |
| `startswith` | string prefix match |
| `endswith`   | string suffix match |
| `contains`   | substring match |

Examples:
```
users where role = 'admin'
products where price_cents > '5000'
users where name startswith 'Rick'
orders where status = 'pending' and total_cents > '10000'
```

**Relation rule** — follow foreign-key links to attach related rows as children:

```
<from_table> to <to_table>
<from_table> to <to_table> via <intermediate>[, <intermediate> ...]
```

If ArborQL finds more than one FK path between the two tables it presents a selection dialog; the chosen path is saved in the rule as a `via` clause so it can be replayed consistently.

Examples:
```
users to orders
orders to products
users to locations via departments
```

### Rule reorder overlay (`r`)

Press `r` to open the rule list.  Use `u` / `d` to swap a rule up or down, then press `Enter` to re-execute all rules in the new order (the data tree is rebuilt from scratch).  Press `Esc` to cancel without changes.

---

## Project layout

```
src/
├── main.rs        — CLI entry-point, tokio event loop, key handlers
├── db/
│   ├── mod.rs     — Database trait + Value / Row / TableInfo types
│   ├── sqlite.rs  — SQLite backend (PRAGMA table_info / foreign_key_list)
│   └── mysql.rs   — MySQL backend (information_schema)
├── schema.rs      — In-memory schema graph + DFS path-finder
├── rules.rs       — Rule parser (filter & relation)
├── engine.rs      — DataNode tree builder, rule executor
└── ui/
    ├── app.rs     — Application state & input helpers
    └── render.rs  — ratatui layout & widgets

samples/
├── ecommerce.db   — Pre-built SQLite3 database (e-commerce / org chart)
├── ecommerce.sql  — DDL + seed data (re-runnable)
├── blog.db        — Pre-built SQLite3 database (blog / polymorphic assocs)
└── blog.sql       — DDL + seed data (re-runnable)
```

---

## Contributing

Bug reports and pull requests are welcome on GitHub.  
Please open an issue first for larger feature ideas.

