# UI Model — Domain-Specific Toolkit

This module (`ui/model/`) is a domain-specific toolkit for building terminal UIs
with clean input handling. It implements the **Command pattern** (Gang of Four):
raw platform key events are translated into semantic command objects
(`UserKeyEvent`), which are dispatched to handler methods on the focused widget
(`ControlPanel`). Widgets never see raw keys; the dispatch layer never knows
what widgets do with commands.

## Rules

- **Nothing in `ui/model/` references specific widgets, views, or app features.**
  This module defines the generic toolkit (events, focus model, dispatch trait).
  Widget implementations live outside this module.
- **`UserKeyEvent` variants are semantically generic.** Use names like `Remove`,
  `AddItem`, `NavigateUp` — never `PruneNode`, `OpenColumnManager`, or
  `ToggleSchema`. If a name only makes sense for one feature, it belongs in
  that feature's ControlPanel impl, not here.
- **`dispatch()` is the only place that matches on `UserKeyEvent`.** No widget,
  app layer, or other code should match on the enum directly.
- **`from_key_event()` is the only place that reads raw `KeyEvent` for semantic
  mapping.** Widgets receive `on_*()` calls, not raw keys. The exception is
  `on_text_input(KeyEvent)` which intentionally passes the raw event for text
  editing mechanics (backspace, cursor movement, etc.).
- **Default no-ops are mandatory.** Every `ControlPanel` method must have a
  default empty implementation. Widgets opt in to events, never opt out.

## Architecture

```
crossterm::KeyEvent
        │
        ▼
┌──────────────────┐
│  from_key_event  │  keys.rs — translates raw keys using UserFocusLoci
│                  │  (InputFocus × EntityFocus)
└────────┬─────────┘
         │ UserKeyEvent
         ▼
┌──────────────────┐
│    dispatch()    │  control_panel.rs — exhaustive match, routes to trait methods
└────────┬─────────┘
         │ on_*() call
         ▼
┌──────────────────┐
│  ControlPanel    │  widget implementation — handles only the events it cares about
│  (trait impl)    │
└──────────────────┘
```

## The three pieces

### 1. `keys.rs` — The translator

**Problem**: a physical key like `d` means "type the letter d" in a text field,
"delete" in a list overlay, and "move item down" in a reorderable list. The
meaning depends on context, not on the key itself.

**Solution**: `from_key_event(key, focus)` translates a raw `KeyEvent` into an
`Option<UserKeyEvent>` based on a `UserFocusLoci` — a two-dimensional focus
state:

- **`InputFocus`** — is a text buffer capturing keystrokes?
  - `None`: chars are action keys, mapped by EntityFocus
  - `Idle`: minimal nav (j/k/q), everything else becomes TextInput
  - `Text`: all remaining keys become TextInput
  - `Search`: same as Text (semantically distinct for future use)

- **`EntityFocus`** — what kind of entity has focus? (only consulted when
  InputFocus is None)
  - `Overlay`: standard list action keys
  - `Editable`: reorderable list (d→MoveItemDown, u→MoveItemUp)
  - `Confirm`: yes/no dialog
  - `Dismiss`: any key exits

The two dimensions are orthogonal. InputFocus short-circuits: when it's
Text/Search/Idle, EntityFocus is irrelevant. This is verified by the snapshot
test (collapsed `*` columns prove independence).

### 2. `control_panel.rs` — The dispatcher

**Problem**: a monolithic `handle_key` function with nested mode matches grows
without bound. Adding a new event or widget means touching a central function
that already handles every mode.

**Solution**: the Command pattern. `UserKeyEvent` is the command object.
`ControlPanel` is the receiver trait. `dispatch()` is the invoker.

- **`ControlPanel` trait**: one `on_*()` method per `UserKeyEvent` variant, all
  default no-ops. Widgets implement only what they handle.
- **`dispatch(panel, event)`**: the single exhaustive match on `UserKeyEvent`.
  No other code should match on `UserKeyEvent` directly.

Adding a new event is mechanical:
1. Add a variant to `UserKeyEvent`
2. Add a default no-op `on_*()` method to `ControlPanel`
3. Add an arm to `dispatch()`
4. The compiler catches any gaps (exhaustive match)
5. The snapshot test catches any mapping changes

### 3. Snapshot test in `keys.rs` — The safety net

An exhaustive table test generates every key × every `InputFocus × EntityFocus`
permutation, formats them as a readable table, and compares against a hardcoded
expected string. See `testing/KEYS_EXPERIENCE.md` for the full guide.

Key properties:
- Columns auto-collapse when an InputFocus makes EntityFocus irrelevant (shown as `*`)
- Consecutive identical alphabet rows collapse into ranges (e.g. `b-c`, `A-M`)
- Failures produce a line-by-line diff showing exactly which cells changed

## Design principles

**Semantic over physical.** `UserKeyEvent::Remove` not `KeyCode::Char('x')`.
Widgets express intent, not key codes. Key bindings can change without touching
widget code.

**No-op by default.** A widget that only cares about navigation implements
`on_navigate_up` and `on_navigate_down`. All 23 other events are silently
ignored. No boilerplate match arms.

**Single dispatch point.** Only `dispatch()` matches on `UserKeyEvent`. This
ensures every event routes through one place, making it easy to add logging,
undo tracking, or event recording later without changing widgets.

**Widget ignorance.** Widgets don't know about each other, about raw keys, or
about focus management. The app layer decides which widget has focus and routes
events to it. Widgets just respond to semantic commands.

**Two-dimensional focus, not a flat mode enum.** `InputFocus` and `EntityFocus`
are independent axes. A list overlay can be in Search or None input focus
without changing its entity type. This avoids combinatorial explosion of mode
variants and makes the overlay search sub-state natural to express.

## File inventory

| File | Role |
|------|------|
| `keys.rs` | `UserKeyEvent` enum, `UserFocusLoci` struct, `from_key_event()`, snapshot test |
| `control_panel.rs` | `ControlPanel` trait, `dispatch()` |

## How to add a new event

1. Add a variant to `UserKeyEvent` in `keys.rs`
2. Add a mapping in `from_key_event()` for the appropriate focus states
3. Add a default no-op `on_*(&mut self)` to the `ControlPanel` trait
4. Add a match arm in `dispatch()` calling the new method
5. Run `cargo test` — the snapshot test will fail with a diff showing the new
   mapping; update the expected table
6. Implement `on_*()` in whichever widgets need it

## How to add a new widget

1. Create a struct for your widget state
2. `impl ControlPanel for MyWidget { ... }` — implement only the `on_*` methods
   you need
3. The app layer creates `MyWidget` and routes events to it via `dispatch()`
