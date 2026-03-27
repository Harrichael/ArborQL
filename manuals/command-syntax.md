# Command Querying Syntax

LatticeQL uses a concise, natural-language-like grammar instead of SQL. Commands
are entered by pressing `:` to open Command mode, typing a rule, then pressing
`Enter` to execute it.

There are three rule types: Filter, Relation, and Prune.


## Filter Rule

Load rows from a table as root nodes in the tree.

```
<table> where <column> <op> <value> [and <column> <op> <value> ...]
```

### Operators

| Operator     | Meaning                        |
|--------------|--------------------------------|
| =            | Equals                         |
| !=           | Not equals                     |
| <            | Less than                      |
| <=           | Less than or equal             |
| >            | Greater than                   |
| >=           | Greater than or equal          |
| startswith   | Value begins with string       |
| endswith     | Value ends with string         |
| contains     | Value contains substring       |

### Examples

```
users where name startswith 'Rick'
products where category = 'Hardware'
orders where status = 'pending' and total_cents > '10000'
```

### Notes

- Values are always quoted strings (e.g. `'42'`, `'pending'`).
- Multiple conditions are combined with `and` (all must match).
- The resulting rows become top-level nodes; you can add Relation rules to
  attach children to them.


## Relation Rule

Follow foreign key relationships and attach related rows as children.

```
<from_table> to <to_table>
<from_table> to <to_table> via <intermediate>[, <intermediate> ...]
```

### Examples

```
users to orders
orders to order_items
users to locations via departments
```

### How It Works

LatticeQL searches the schema graph (real FKs + virtual FKs) for paths from
`from_table` to `to_table`, shortest first. If exactly one path exists, it is
applied automatically. If multiple paths exist, a selection overlay appears so
you can choose — press `n` in the overlay to load additional paths if more
are available.

The `via` keyword filters paths to only those that pass through the listed
intermediate tables, **in the order specified**. For example,
`users to products via orders, order_items` only matches paths that reach
`orders` before `order_items`. The via tables need not be adjacent in the
path — they are a subsequence constraint.

Once applied, the engine queries the database and appends matching child rows
to every existing node of `from_table` type in the tree.


## Prune Rule

Remove nodes (and their entire subtree) from the current tree.

```
prune <table> where <column> <op> <value> [and ...]
```

### Example

```
prune orders where status = 'cancelled'
```

### Notes

- Prune is applied in-memory — it does not re-query the database.
- Pruning is recorded as a rule so the tree can be rebuilt deterministically
  when rules are reordered.


## Command Completion

While typing in Command mode a hint line below the input shows valid next
tokens. Up to 8 options are shown; press any listed token character to advance.
Table names and column names are drawn live from the connected schema.


## Execution Order

Rules are executed in the order they were entered. A Filter rule must come
before any Relation rule that references the same table. Use the Rule Reorder
overlay (`r` in Normal mode) to change the order and replay the tree.
