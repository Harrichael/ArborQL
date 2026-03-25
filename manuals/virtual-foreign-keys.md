# Virtual Foreign Keys

## What Are They?

Some databases use **polymorphic associations** — a single table references
multiple other tables using a type discriminator column and an integer id
column, without a formal foreign key constraint.

A classic example from Rails:

```sql
CREATE TABLE comments (
  id               INTEGER PRIMARY KEY,
  commentable_type VARCHAR(255),   -- "Post" or "Photo"
  commentable_id   INTEGER,        -- id in the referenced table
  body             TEXT
);
```

No FK constraint exists, so LatticeQL cannot discover this relationship
automatically. **Virtual foreign keys** let you define it manually so the
engine can traverse it just like a real FK.


## Virtual FK Manager

Press `v` in Normal mode to open the Virtual FK Manager overlay.

The manager lists all currently defined virtual FKs. Each entry shows:
```
comments.commentable_type='Post' → posts.id  (via commentable_id)
```

### Key Bindings

| Key       | Action                              |
|-----------|-------------------------------------|
| `↑` / `k` | Move cursor up                     |
| `↓` / `j` | Move cursor down                   |
| `a`       | Open the Add wizard                |
| `d` / `x` | Delete selected virtual FK         |
| `/`       | Activate search filter             |
| `Esc`     | Close the manager                  |


## Adding a Virtual FK (Wizard)

Press `a` to start the 6-step wizard:

### Step 1 — From Table
Choose the table that owns the type and id columns (e.g. `comments`).

### Step 2 — Type Column
Choose the discriminator column (e.g. `commentable_type`).

### Step 3 — Type Value
Choose the value of the discriminator that identifies this particular
association. LatticeQL samples live data and shows values with their counts,
so you can see which are most common. (e.g. `Post` with count 42).

### Step 4 — ID Column
Choose the column that holds the foreign key value (e.g. `commentable_id`).

### Step 5 — To Table
Choose the target table that is referenced (e.g. `posts`).

### Step 6 — To Column
Choose the primary key column on the target table (e.g. `id`).

Press `Esc` at any step to abort.


## How Virtual FKs Are Treated

Once defined, a virtual FK is:

- Added to the in-memory schema graph alongside real FKs.
- Used by BFS path-finding when you execute a Relation rule.
- Stored in `.latticeql/config.jsonnet` so it persists across sessions.

Internally the engine applies a WHERE filter on the type column when
following a virtual FK path, so only the correct polymorphic type is joined.


## Persistence

Virtual FKs are saved to `.latticeql/config.jsonnet` in the nearest ancestor
directory (or `~/.latticeql/config.jsonnet` as fallback). They are loaded
automatically on the next startup.

```jsonnet
{
  virtual_fks: [
    {
      from_table: "comments",
      type_column: "commentable_type",
      type_value:  "Post",
      id_column:   "commentable_id",
      to_table:    "posts",
      to_column:   "id"
    }
  ]
}
```

You can also define them by hand in the config file instead of using the
wizard.


## Example Workflow

1. You have `comments` and `posts` tables with the polymorphic pattern above.
2. Press `v` → `a` to open the wizard.
3. Select `comments` → `commentable_type` → `Post` → `commentable_id` → `posts` → `id`.
4. Press `Esc` to close the manager.
5. Run a filter rule: `posts where id = '1'`.
6. Run a relation rule: `posts to comments`.
7. LatticeQL traverses the virtual FK and attaches comments where
   `commentable_type = 'Post'` and `commentable_id = posts.id`.
