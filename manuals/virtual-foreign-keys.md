# Virtual Foreign Keys

## What Are They?

Virtual foreign keys let you define relationships that aren't captured by
formal foreign key constraints. Two common scenarios:

**1. Simple missing FKs** — a column holds a reference to another table but
no FK constraint was created (common in legacy databases or ORMs):

```sql
CREATE TABLE orders (
  id          INTEGER PRIMARY KEY,
  customer_id INTEGER   -- references customers.id, but no FK constraint
);
```

**2. Polymorphic associations** — a single table references multiple other
tables using a type discriminator column (Rails-style):

```sql
CREATE TABLE comments (
  id               INTEGER PRIMARY KEY,
  commentable_type VARCHAR(255),   -- "Post" or "Photo"
  commentable_id   INTEGER,        -- id in the referenced table
  body             TEXT
);
```

In both cases LatticeQL cannot discover the relationship automatically.
**Virtual foreign keys** let you define it manually so the engine can traverse
it just like a real FK.


## Virtual FK Manager

Press `v` in Normal mode to open the Virtual FK Manager overlay.

The manager lists all currently defined virtual FKs. Each entry shows:
```
comments.commentable_type='Post' → posts.id  (via commentable_id)
orders.customer_id → customers.id
```

### Key Bindings

| Key         | Action                              |
|-------------|-------------------------------------|
| `↑` / `k`   | Move cursor up                     |
| `↓` / `j`   | Move cursor down                   |
| `a`         | Open the Add form                  |
| `d` / `x`   | Delete selected virtual FK         |
| `/`         | Activate search filter             |
| `Ctrl+S`    | Save all FKs to config file        |
| `Esc`       | Close the manager                  |


## Adding a Virtual FK (Form)

Press `a` to open the single-screen form. All six fields are visible at once.
The **active field** (highlighted in yellow with `▶`) shows a dropdown list
of options below — use `↑`/`↓` or `j`/`k` to navigate, `Enter` to confirm.

### Navigation

| Key             | Action                                    |
|-----------------|-------------------------------------------|
| `Tab`           | Move to next field                        |
| `Shift+Tab`     | Move to previous field                    |
| `↑` / `k`       | Move up in the active field's dropdown    |
| `↓` / `j`       | Move down in the active field's dropdown  |
| `Enter`         | Confirm selection and advance to next     |
| `/`             | Search/filter the active dropdown         |
| `Ctrl+S`        | Commit and save the FK (when complete)    |
| `Esc`           | Cancel / go back to manager               |

### Fields

| Field          | Required | Description                                         |
|----------------|----------|-----------------------------------------------------|
| `from_table`   | Yes      | Table that owns the FK column                       |
| `id_column`    | Yes      | Column holding the foreign key value                |
| `type_column`  | Optional | Discriminator column for polymorphic FKs            |
| `type_value`   | Optional | Discriminator value for this specific association   |
| `to_table`     | Yes      | Target table being referenced                       |
| `to_column`    | Yes      | Primary key column on the target table              |

For **simple FKs** (no polymorphism): leave `type_column` blank by selecting
`(none — simple FK)`. The `type_value` field is then skipped automatically.

For **polymorphic FKs**: select the discriminator column and value.
LatticeQL queries the database live to show the available type values with
their row counts, so you can see which are most common.

The form completes when all required fields are filled. The FK is committed
when you `Enter` on `to_column` or press `Ctrl+S` from any field. `Ctrl+S`
also immediately persists the FK to the config file.


## How Virtual FKs Are Treated

Once defined, a virtual FK is:

- Added to the in-memory schema graph alongside real FKs.
- Used by BFS path-finding when you execute a Relation rule.
- Stored in `.latticeql/config.jsonnet` so it persists across sessions.

For polymorphic FKs the engine applies a WHERE filter on the type column when
following the path, so only the correct polymorphic type is joined. Simple FKs
are traversed without any extra condition.


## Persistence

Virtual FKs are saved to `.latticeql/default.jsonnet` in the nearest ancestor
directory (or `~/.latticeql/default.jsonnet` as fallback). They are loaded
automatically on the next startup.

```jsonnet
{
  virtual_fks: [
    // Polymorphic FK
    {
      from_table:   "comments",
      type_column:  "commentable_type",
      type_value:   "Post",
      id_column:    "commentable_id",
      to_table:     "posts",
      to_column:    "id"
    },
    // Simple FK (no type discriminator)
    {
      from_table:  "orders",
      id_column:   "customer_id",
      to_table:    "customers",
      to_column:   "id"
    }
  ]
}
```

You can also define them by hand in the config file instead of using the form.


## Example Workflow

1. You have `comments` and `posts` tables with the polymorphic pattern above.
2. Press `v` → `a` to open the form.
3. Select `comments` as `from_table`, press `Enter`.
4. Select `commentable_id` as `id_column`, press `Enter`.
5. Select `commentable_type` as `type_column`, press `Enter`.
6. Select `Post  (42)` as `type_value`, press `Enter`.
7. Select `posts` as `to_table`, press `Enter`.
8. Select `id` as `to_column`, press `Enter` — the FK is committed.
9. Press `Ctrl+S` in the manager to persist to config.
10. Run a filter rule: `posts where id = '1'`.
11. Run a relation rule: `posts to comments`.
12. LatticeQL traverses the virtual FK and attaches comments where
    `commentable_type = 'Post'` and `commentable_id = posts.id`.
