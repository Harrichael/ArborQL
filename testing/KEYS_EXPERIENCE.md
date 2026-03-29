# Exhaustive Snapshot Tables

A testing pattern for functions that map one set of inputs to outputs across
multiple dimensions. Instead of writing individual assertions for specific
cases, generate a formatted table of every input x dimension combination at
runtime and compare it against a hardcoded expected string.

## When to use this

This pattern works well when:

- A function maps a **finite, enumerable** input set through **multiple
  independent dimensions** (modes, contexts, feature flags, roles)
- Individual unit tests would cover specific cells but **hide the full
  picture** — a reviewer can't see emergent behavior across dimensions
- Changes tend to have **cross-cutting effects** — modifying one dimension's
  logic can silently change behavior in combinations you didn't think to test
- The mapping is **the specification** — the table IS the thing stakeholders
  need to review, not a proxy for it

It works poorly when:

- The input space is continuous or very large (floats, free-form strings)
- The output is complex (large structs, side effects) rather than a short label
- The mapping changes frequently in ways that make snapshot maintenance noisy

## How it works

1. **Enumerate all inputs** (keys, commands, API calls, etc.)
2. **Enumerate all dimension permutations** (modes x roles, flags x states, etc.)
3. **Call the function under test** for every input x permutation combination
4. **Format results as a table** with inputs as rows and permutations as columns
5. **Compare against a hardcoded expected string** with a custom diff on failure

## Making the table readable

Raw exhaustive tables can be large. Three techniques keep them scannable:

**Collapse identical columns.** When multiple dimension values produce identical
results across all rows, merge them into one column. Label the merged column to
show what was collapsed (e.g. `*` for "all values identical"). This proves
dimension independence at a glance — if a future change breaks it, the `*`
splits and the diff shows exactly where.

**Collapse identical consecutive rows.** When consecutive inputs produce
identical results across all columns, merge them into a range (e.g. `b-c`,
`A-M`). This keeps alphabetic or numeric sweeps compact while still surfacing
individual inputs that have unique behavior.

**Use short, consistent labels.** Abbreviate output values to a fixed width so
columns align. Include enough information to identify the value without needing
to cross-reference (e.g. `Text(d)` not just `Text`, so you can see which
characters are captured as text input).

## Making failures useful

The default `assert_eq!` on large strings dumps both sides as escaped
single-line blobs. Replace it with a line-by-line diff:

```rust
fn assert_table_eq(actual: &str, expected: &str) {
    if actual == expected { return; }
    let actual_lines: Vec<&str> = actual.lines().collect();
    let expected_lines: Vec<&str> = expected.lines().collect();
    let max = actual_lines.len().max(expected_lines.len());

    let mut diff = String::new();
    diff.push_str("\n\nSnapshot mismatch:\n\n");
    for i in 0..max {
        let a = actual_lines.get(i).unwrap_or(&"<missing>");
        let e = expected_lines.get(i).unwrap_or(&"<missing>");
        if a == e {
            diff.push_str(&format!("  {}\n", a));
        } else {
            diff.push_str(&format!("- {}\n", e));
            diff.push_str(&format!("+ {}\n", a));
        }
    }
    panic!("{}", diff);
}
```

This produces output like:

```
  a         | Text(a)   | Text(a)   | Text(a)   | AddItem   | AddItem   | -         | Back
- d         | Text(d)   | Text(d)   | Text(d)   | Remove    | MoveDn    | -         | Back
+ d         | Text(d)   | Text(d)   | Text(d)   | -         | MoveDn    | -         | Back
  e-h       | Text(e-h) | Text(e-h) | Text(e-h) | -         | -         | -         | Back
```

Unchanged lines provide context. Changed lines show `-` (expected) / `+` (actual).
The reviewer sees exactly which cells changed and can verify intent at a glance.

## What it catches that individual tests miss

- **Unintended side effects**: changing one dimension's logic may alter cells in
  combinations you didn't write a test for
- **Dimension independence violations**: if two dimensions are supposed to be
  orthogonal, column collapsing proves it — breaking independence splits the
  collapsed column and the diff surfaces it
- **Coverage gaps**: there are no gaps. Every cell is tested. With individual
  tests, you choose which cases to cover and hope you picked the right ones
- **New variant omissions**: adding an enum variant without handling it changes
  cells or adds columns, forcing the snapshot to update

## Updating the snapshot

After an intentional change:

1. Run the test and read the diff
2. Verify every `-`/`+` line matches your intent
3. Copy the actual table into the expected string

The update is mechanical, but the review step is not — the diff is the code
review artifact. If a one-line logic change produces 15 changed cells in the
table, that's important information.

## Tradeoffs

**Pro**: one test replaces N individual tests, serves as documentation, catches
cross-cutting changes, and produces reviewer-friendly diffs.

**Con**: updating the snapshot after intentional changes requires copying the
new table. This is low effort but nonzero. If the mapping changes very
frequently (multiple times per day), the churn may not be worth it.

**Guideline**: if you find yourself writing more than ~5 individual assertions
for the same function across different dimension values, consider whether a
snapshot table would be clearer.

## Example in this codebase

`key_mapping_snapshot` in `src/ui/model/keys.rs` tests the `from_key_event`
function which maps `(KeyEvent, FocusLoci)` to `Option<UserKeyEvent>`.

- **Rows**: 17 special keys + 26 lowercase + 26 uppercase = 69 inputs
  (collapsed to ~43 displayed rows)
- **Columns**: `InputFocus(4) x EntityFocus(4)` = 16 permutations
  (collapsed to 7 displayed columns because Idle/Text/Search each collapse
  all EntityFocus values to `*`)
- **Table dimensions**: ~43 rows x 7 columns = ~301 cells tested
- **Replaces**: 11 individual unit tests that covered ~30 cells
