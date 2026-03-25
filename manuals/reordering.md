# Reordering Commands

Every command you run is recorded as a **rule**. Rules are executed in order
to build the tree: Filter rules load root nodes, Relation rules attach
children, Prune rules remove unwanted nodes. The order matters — a Relation
rule can only attach children to nodes that already exist.

The Rule Reorder overlay lets you change the order, delete rules, and set
where the next new rule will be inserted.


## Opening the Overlay

Press `r` in Normal mode. The overlay shows the current rule list with a
`next insertion` marker that indicates where the next command will be placed.


## Key Bindings

| Key      | Action                                              |
|----------|-----------------------------------------------------|
| `↑` / `k` | Move cursor up                                   |
| `↓` / `j` | Move cursor down                                 |
| `u`      | Swap selected rule with the one above (move earlier) |
| `d`      | Swap selected rule with the one below (move later)   |
| `x`      | Delete selected rule                                |
| `i`      | Set insertion point **before** the cursor position  |
| `o`      | Set insertion point **after** the cursor position   |
| `z`      | Undo last change                                    |
| `y`      | Redo last undone change                             |
| `Enter`  | Apply changes and rebuild the tree                  |
| `Esc`    | Cancel without applying changes                     |


## Insertion Point

The `next insertion` marker (shown with `→`) marks where new rules entered
via Command mode will be inserted. By default it advances to the end after
each new command. Use `i` and `o` in the reorder overlay to pin it to a
specific position.

This is useful when you want to add a Relation rule that should run before an
existing Prune rule, without having to reorder afterwards.


## Undo / Redo

Changes inside the overlay (swaps and deletes) can be undone with `z` and
redone with `y`. Undo/redo history is cleared when you press `Enter` to apply
or `Esc` to cancel.


## Applying Changes

When you press `Enter`, the engine discards the current tree and re-executes
all rules in the new order from scratch. This may trigger fresh database
queries for Filter and Relation rules.


## Example Workflow

Suppose you have:
```
1. users where status = 'active'
2. prune users where role = 'admin'
3. users to orders
```

Rule 2 runs before Rule 3, so pruned users never get their orders attached.
To attach orders first and then prune:

1. Press `r` to open the overlay.
2. Navigate to Rule 2 (`↓` once).
3. Press `d` to swap it down below Rule 3.
4. Press `Enter` to apply.
