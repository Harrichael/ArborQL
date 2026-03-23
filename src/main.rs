mod db;
mod engine;
mod rules;
mod schema;
mod ui;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use engine::{Engine, flatten_tree};
use ratatui::{Terminal, backend::CrosstermBackend};
use schema::Schema;
use std::io;
use ui::app::{AppState, ColumnManagerItem, Mode, VirtualFkAddStep};
use schema::VirtualFkDef;

/// ArborQL — Navigate complex datasets from multiple sources intuitively.
#[derive(Parser, Debug)]
#[command(name = "arborql", version, about)]
struct Args {
    /// Database connection URL.
    ///
    /// Examples:
    ///   sqlite://path/to/db.sqlite3
    ///   mysql://user:password@localhost/dbname
    #[arg(short, long)]
    database: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Connect to the database
    eprintln!("Connecting to database…");
    let db = db::connect(&args.database).await?;

    // Explore schema
    eprintln!("Exploring schema…");
    let schema = Schema::explore(db.as_ref()).await?;
    let table_names = schema.table_names();

    let mut engine = Engine::new(schema);
    let mut state = AppState::new();
    state.table_names = table_names;
    // Build per-table column lists for command completion hints.
    state.table_columns = engine.schema.tables.iter().map(|(name, info)| {
        let cols = info.columns.iter().map(|c| c.name.clone()).collect();
        (name.clone(), cols)
    }).collect();

    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, &mut state, &mut engine, db.as_ref()).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
) -> Result<()> {
    // Pending paths waiting for user selection
    let mut pending_paths: Option<(rules::Rule, Vec<schema::TablePath>)> = None;

    loop {
        // Draw
        terminal.draw(|f| ui::render::render(f, state, &engine.roots))?;

        // Handle events (with a timeout so we can do async work)
        if event::poll(std::time::Duration::from_millis(50))? {
            let ev = event::read()?;
            match ev {
                Event::Key(key) => {
                    let handled = handle_key(
                        key,
                        state,
                        engine,
                        db,
                        &mut pending_paths,
                    )
                    .await?;
                    if !handled {
                        // Quit signal
                        break;
                    }
                }
                Event::Resize(_, _) => {} // terminal handles this automatically
                _ => {}
            }
        }
    }
    Ok(())
}

fn insert_rule_at_next_cursor(
    state: &mut AppState,
    engine: &mut Engine,
    rule: rules::Rule,
) -> bool {
    let idx = state.next_rule_cursor.min(engine.rules.len());
    let inserted_before_existing = idx < engine.rules.len();
    engine.rules.insert(idx, rule);
    state.next_rule_cursor = (idx + 1).min(engine.rules.len());
    inserted_before_existing
}

fn place_last_added_rule_at_next_cursor(state: &mut AppState, engine: &mut Engine) -> bool {
    if let Some(rule) = engine.rules.pop() {
        let idx = state.next_rule_cursor.min(engine.rules.len());
        let inserted_before_existing = idx < engine.rules.len();
        engine.rules.insert(idx, rule);
        state.next_rule_cursor = (idx + 1).min(engine.rules.len());
        return inserted_before_existing;
    }
    false
}

fn columns_for_table(roots: &[engine::DataNode], table: &str) -> Vec<String> {
    fn walk(nodes: &[engine::DataNode], table: &str, out: &mut Option<Vec<String>>) {
        for node in nodes {
            if node.table == table {
                let mut cols: Vec<String> = node.row.keys().cloned().collect();
                cols.sort();
                *out = Some(cols);
                return;
            }
            walk(&node.children, table, out);
            if out.is_some() {
                return;
            }
        }
    }

    let mut found = None;
    walk(roots, table, &mut found);
    found.unwrap_or_default()
}

fn ensure_tree_visibility_for_node(state: &mut AppState, node: &engine::DataNode) {
    fn default_tree_columns(node: &engine::DataNode) -> Vec<String> {
        let mut all_cols: Vec<String> = node.row.keys().cloned().collect();
        all_cols.sort();
        let preferred = ["id", "name", "title", "label"];
        let mut visible: Vec<String> = preferred
            .iter()
            .filter_map(|c| {
                if all_cols.iter().any(|k| k == c) {
                    Some((*c).to_string())
                } else {
                    None
                }
            })
            .collect();
        if visible.is_empty() && !all_cols.is_empty() {
            visible.push(all_cols[0].clone());
        }
        visible
    }

    state
        .tree_visible_columns
        .entry(node.table.clone())
        .or_insert_with(|| default_tree_columns(node));
    state
        .tree_column_order
        .entry(node.table.clone())
        .or_insert_with(|| {
            let mut all_cols: Vec<String> = node.row.keys().cloned().collect();
            all_cols.sort();
            let defaults = default_tree_columns(node);
            let default_set: std::collections::HashSet<String> =
                defaults.iter().cloned().collect();

            let mut ordered = defaults;
            for c in all_cols {
                if !default_set.contains(&c) {
                    ordered.push(c);
                }
            }
            ordered
        });
}

fn column_manager_items_for_table(
    state: &AppState,
    roots: &[engine::DataNode],
    table: &str,
) -> Vec<ColumnManagerItem> {
    let all_cols = columns_for_table(roots, table);
    let shown = state
        .tree_visible_columns
        .get(table)
        .cloned()
        .unwrap_or_default();
    let mut ordered = state
        .tree_column_order
        .get(table)
        .cloned()
        .unwrap_or_default();

    for c in &all_cols {
        if !ordered.contains(c) {
            ordered.push(c.clone());
        }
    }
    ordered.retain(|c| all_cols.contains(c));

    let shown_set: std::collections::HashSet<String> =
        shown.iter().cloned().collect();

    ordered
        .into_iter()
        .map(|name| ColumnManagerItem {
            enabled: shown_set.contains(&name),
            name,
        })
        .collect()
}

/// Returns `false` when the application should quit.
async fn handle_key(
    key: crossterm::event::KeyEvent,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
    pending_paths: &mut Option<(rules::Rule, Vec<schema::TablePath>)>,
) -> Result<bool> {
    // Column manager overlay has exclusive key handling while open.
    if state.column_add.is_some() {
        match key.code {
            KeyCode::Esc => {
                state.column_add = None;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if let Some((_, _, ref mut cursor)) = state.column_add {
                    if *cursor > 0 {
                        *cursor -= 1;
                    }
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if let Some((_, ref items, ref mut cursor)) = state.column_add {
                    if *cursor + 1 < items.len() {
                        *cursor += 1;
                    }
                }
            }
            KeyCode::Char('u') => {
                if let Some((_, ref mut items, ref mut cursor)) = state.column_add {
                    if *cursor > 0 {
                        items.swap(*cursor, *cursor - 1);
                        *cursor -= 1;
                    }
                }
            }
            KeyCode::Char('d') => {
                if let Some((_, ref mut items, ref mut cursor)) = state.column_add {
                    if *cursor + 1 < items.len() {
                        items.swap(*cursor, *cursor + 1);
                        *cursor += 1;
                    }
                }
            }
            KeyCode::Char(' ') | KeyCode::Char('x') => {
                if let Some((_, ref mut items, cursor)) = state.column_add {
                    if let Some(item) = items.get_mut(cursor) {
                        item.enabled = !item.enabled;
                    }
                }
            }
            KeyCode::Enter => {
                if let Some((table, ref items, _)) = state.column_add.clone() {
                    let enabled: Vec<String> = items
                        .iter()
                        .filter(|i| i.enabled)
                        .map(|i| i.name.clone())
                        .collect();
                    state.tree_visible_columns.insert(table.clone(), enabled);
                    state.tree_column_order.insert(
                        table,
                        items.iter().map(|i| i.name.clone()).collect(),
                    );
                }
                state.column_add = None;
            }
            _ => {}
        }
        return Ok(true);
    }

    match state.mode.clone() {
        // ── Normal mode ──────────────────────────────────────────────────
        Mode::Normal => {
            match key.code {
                KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(false),
                KeyCode::Char(':') => {
                    state.mode = Mode::Command;
                    state.clear_input();
                }
                KeyCode::Char('j') | KeyCode::Down => state.select_down(),
                KeyCode::Char('k') | KeyCode::Up => state.select_up(),
                KeyCode::Char('f') | KeyCode::Enter => {
                    // Toggle fold on selected node
                    let flat = flatten_tree(&engine.roots);
                    if state.selected_row < flat.len() {
                        toggle_fold(&mut engine.roots, state.selected_row);
                    }
                }
                KeyCode::Char('s') => {
                    state.show_schema = !state.show_schema;
                }
                KeyCode::Char('r') => {
                    if !engine.rules.is_empty() {
                        state.rules = engine.rules.clone();
                        state.rule_cursor = 0;
                        state.next_rule_cursor =
                            state.next_rule_cursor.min(state.rules.len());
                        state.rule_reorder_undo.clear();
                        state.rule_reorder_redo.clear();
                        state.mode = Mode::RuleReorder;
                    }
                }
                KeyCode::Char('c') => {
                    // Manage table-level tree columns for selected node's table.
                    let flat = flatten_tree(&engine.roots);
                    if state.selected_row < flat.len() {
                        let (_, node) = flat[state.selected_row];
                        ensure_tree_visibility_for_node(state, node);
                        let items = column_manager_items_for_table(
                            state,
                            &engine.roots,
                            &node.table,
                        );
                        if !items.is_empty() {
                            state.column_add = Some((node.table.clone(), items, 0));
                        }
                    }
                }
                KeyCode::Char('v') => {
                    state.mode = Mode::VirtualFkManager { cursor: 0 };
                }
                _ => {}
            }
        }

        // ── Command mode ─────────────────────────────────────────────────
        Mode::Command => {
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                    state.clear_input();
                }
                KeyCode::Enter => {
                    let cmd = state.input_text().trim().to_string();
                    state.mode = Mode::Normal;
                    state.clear_input();
                    if !cmd.is_empty() {
                        execute_command(cmd, state, engine, db, pending_paths).await?;
                    }
                }
                KeyCode::Char(c) => {
                    if key.modifiers.contains(KeyModifiers::CONTROL) && c == 'c' {
                        return Ok(false);
                    }
                    state.input_char(c);
                }
                KeyCode::Backspace => state.input_backspace(),
                KeyCode::Delete => state.input_delete(),
                KeyCode::Left => state.cursor_left(),
                KeyCode::Right => state.cursor_right(),
                _ => {}
            }
        }

        // ── Path selection overlay ────────────────────────────────────────
        Mode::PathSelection => {
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                    *pending_paths = None;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if state.path_cursor > 0 {
                        state.path_cursor -= 1;
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if state.path_cursor + 1 < state.paths.len() {
                        state.path_cursor += 1;
                    }
                }
                KeyCode::Enter => {
                    if let Some((rule, paths)) = pending_paths.take() {
                        let chosen = &paths[state.path_cursor];
                        // Apply the chosen path
                        engine.apply_relation_rule(db, chosen).await?;
                        // Update rule with the chosen path stored as resolved_path
                        let updated_rule = match rule {
                            rules::Rule::Relation { from_table, to_table, via, .. } => {
                                let extra_via: Vec<String> = chosen
                                    .steps
                                    .iter()
                                    .skip(1)
                                    .map(|s| s.from_table.clone())
                                    .collect();
                                rules::Rule::Relation {
                                    from_table,
                                    to_table,
                                    via: if via.is_empty() { extra_via } else { via },
                                    resolved_path: Some(chosen.clone()),
                                }
                            }
                            other => other,
                        };
                        if insert_rule_at_next_cursor(state, engine, updated_rule) {
                            engine.reexecute_all(db).await?;
                        }
                    }
                    state.mode = Mode::Normal;
                    state.paths.clear();
                }
                _ => {}
            }
        }

        // ── Rule reorder overlay ─────────────────────────────────────────
        Mode::RuleReorder => {
            let push_rule_reorder_undo = |state: &mut AppState| {
                state
                    .rule_reorder_undo
                    .push((
                        state.rules.clone(),
                        state.rule_cursor,
                        state.next_rule_cursor,
                    ));
                state.rule_reorder_redo.clear();
            };
            match key.code {
                KeyCode::Esc => {
                    state.rule_reorder_undo.clear();
                    state.rule_reorder_redo.clear();
                    state.mode = Mode::Normal;
                }
                KeyCode::Enter => {
                    // Apply reordered rules
                    engine.rules = state.rules.clone();
                    state.next_rule_cursor =
                        state.next_rule_cursor.min(engine.rules.len());
                    let _ = engine.reexecute_all(db).await;
                    state.rule_reorder_undo.clear();
                    state.rule_reorder_redo.clear();
                    state.mode = Mode::Normal;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if state.rule_cursor > 0 {
                        state.rule_cursor -= 1;
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if state.rule_cursor + 1 < state.rules.len() {
                        state.rule_cursor += 1;
                    }
                }
                KeyCode::Char('u') => {
                    // Swap up
                    if state.rule_cursor > 0 {
                        push_rule_reorder_undo(state);
                        state.rules.swap(state.rule_cursor, state.rule_cursor - 1);
                        state.rule_cursor -= 1;
                    }
                }
                KeyCode::Char('d') => {
                    // Swap down
                    if state.rule_cursor + 1 < state.rules.len() {
                        push_rule_reorder_undo(state);
                        state.rules.swap(state.rule_cursor, state.rule_cursor + 1);
                        state.rule_cursor += 1;
                    }
                }
                KeyCode::Char('x') => {
                    if !state.rules.is_empty() {
                        push_rule_reorder_undo(state);
                        state.rules.remove(state.rule_cursor);
                        if state.rules.is_empty() {
                            state.rule_cursor = 0;
                            state.next_rule_cursor = 0;
                        } else if state.rule_cursor >= state.rules.len() {
                            state.rule_cursor = state.rules.len() - 1;
                        }
                        state.next_rule_cursor = state.next_rule_cursor.min(state.rules.len());
                    }
                }
                KeyCode::Char('i') => {
                    state.next_rule_cursor = state.rule_cursor.min(state.rules.len());
                }
                KeyCode::Char('o') => {
                    state.next_rule_cursor = (state.rule_cursor + 1).min(state.rules.len());
                }
                KeyCode::Char('z') => {
                    if let Some((rules, cursor, next_cursor)) = state.rule_reorder_undo.pop() {
                        state
                            .rule_reorder_redo
                            .push((
                                state.rules.clone(),
                                state.rule_cursor,
                                state.next_rule_cursor,
                            ));
                        state.rules = rules;
                        state.rule_cursor = cursor.min(state.rules.len().saturating_sub(1));
                        state.next_rule_cursor = next_cursor.min(state.rules.len());
                    }
                }
                KeyCode::Char('y') => {
                    if let Some((rules, cursor, next_cursor)) = state.rule_reorder_redo.pop() {
                        state
                            .rule_reorder_undo
                            .push((
                                state.rules.clone(),
                                state.rule_cursor,
                                state.next_rule_cursor,
                            ));
                        state.rules = rules;
                        state.rule_cursor = cursor.min(state.rules.len().saturating_sub(1));
                        state.next_rule_cursor = next_cursor.min(state.rules.len());
                    }
                }
                _ => {}
            }
        }

        // ── Error / Info overlays — any key dismisses ────────────────────
        Mode::Error(_) | Mode::Info(_) => {
            state.mode = Mode::Normal;
        }

        // ── Virtual FK manager ───────────────────────────────────────────
        Mode::VirtualFkManager { cursor } => {
            match key.code {
                KeyCode::Esc => { state.mode = Mode::Normal; }
                KeyCode::Up | KeyCode::Char('k') => {
                    if cursor > 0 {
                        state.mode = Mode::VirtualFkManager { cursor: cursor - 1 };
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if cursor + 1 < state.virtual_fks.len() {
                        state.mode = Mode::VirtualFkManager { cursor: cursor + 1 };
                    }
                }
                KeyCode::Char('a') => {
                    state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickFromTable { cursor: 0 });
                }
                KeyCode::Char('d') | KeyCode::Char('x') => {
                    if cursor < state.virtual_fks.len() {
                        let removed = state.virtual_fks.remove(cursor);
                        engine.schema.virtual_fks.retain(|v| v != &removed);
                        let new_cursor = if cursor > 0 && cursor >= state.virtual_fks.len() {
                            cursor - 1
                        } else {
                            cursor
                        };
                        state.mode = Mode::VirtualFkManager { cursor: new_cursor };
                    }
                }
                _ => {}
            }
        }

        // ── Virtual FK creation wizard ───────────────────────────────────
        Mode::VirtualFkAdd(ref step) => {
            let step = step.clone();
            match step {
                VirtualFkAddStep::PickFromTable { cursor } => match key.code {
                    KeyCode::Esc => { state.mode = Mode::VirtualFkManager { cursor: 0 }; }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if cursor > 0 { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickFromTable { cursor: cursor - 1 }); }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if cursor + 1 < state.table_names.len() { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickFromTable { cursor: cursor + 1 }); }
                    }
                    KeyCode::Enter => {
                        if let Some(t) = state.table_names.get(cursor) {
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeColumn { from_table: t.clone(), cursor: 0 });
                        }
                    }
                    _ => {}
                },
                VirtualFkAddStep::PickTypeColumn { ref from_table, cursor } => {
                    let cols = state.table_columns.get(from_table).cloned().unwrap_or_default();
                    match key.code {
                        KeyCode::Esc => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickFromTable { cursor: 0 }); }
                        KeyCode::Up | KeyCode::Char('k') => {
                            if cursor > 0 { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeColumn { from_table: from_table.clone(), cursor: cursor - 1 }); }
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            if cursor + 1 < cols.len() { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeColumn { from_table: from_table.clone(), cursor: cursor + 1 }); }
                        }
                        KeyCode::Enter => {
                            if let Some(col) = cols.get(cursor) {
                                let options = query_type_options(db, from_table, col).await;
                                state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeValue {
                                    from_table: from_table.clone(),
                                    type_column: col.clone(),
                                    options,
                                    cursor: 0,
                                });
                            }
                        }
                        _ => {}
                    }
                },
                VirtualFkAddStep::PickTypeValue { ref from_table, ref type_column, ref options, cursor } => match key.code {
                    KeyCode::Esc => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeColumn { from_table: from_table.clone(), cursor: 0 }); }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if cursor > 0 { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeValue { from_table: from_table.clone(), type_column: type_column.clone(), options: options.clone(), cursor: cursor - 1 }); }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if cursor + 1 < options.len() { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeValue { from_table: from_table.clone(), type_column: type_column.clone(), options: options.clone(), cursor: cursor + 1 }); }
                    }
                    KeyCode::Enter => {
                        if let Some((type_value, _)) = options.get(cursor) {
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickIdColumn { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), cursor: 0 });
                        }
                    }
                    _ => {}
                },
                VirtualFkAddStep::PickIdColumn { ref from_table, ref type_column, ref type_value, cursor } => {
                    let cols = state.table_columns.get(from_table).cloned().unwrap_or_default();
                    match key.code {
                        KeyCode::Esc => {
                            // Re-query options to return to step 3.
                            let options = query_type_options(db, from_table, type_column).await;
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeValue { from_table: from_table.clone(), type_column: type_column.clone(), options, cursor: 0 });
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            if cursor > 0 { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickIdColumn { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), cursor: cursor - 1 }); }
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            if cursor + 1 < cols.len() { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickIdColumn { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), cursor: cursor + 1 }); }
                        }
                        KeyCode::Enter => {
                            if let Some(col) = cols.get(cursor) {
                                state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToTable { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), id_column: col.clone(), cursor: 0 });
                            }
                        }
                        _ => {}
                    }
                },
                VirtualFkAddStep::PickToTable { ref from_table, ref type_column, ref type_value, ref id_column, cursor } => match key.code {
                    KeyCode::Esc => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickIdColumn { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), cursor: 0 }); }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if cursor > 0 { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToTable { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), id_column: id_column.clone(), cursor: cursor - 1 }); }
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        if cursor + 1 < state.table_names.len() { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToTable { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), id_column: id_column.clone(), cursor: cursor + 1 }); }
                    }
                    KeyCode::Enter => {
                        if let Some(to_table) = state.table_names.get(cursor) {
                            // Default cursor to the "id" column if present.
                            let to_cols = state.table_columns.get(to_table).cloned().unwrap_or_default();
                            let default = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToColumn {
                                from_table: from_table.clone(),
                                type_column: type_column.clone(),
                                type_value: type_value.clone(),
                                id_column: id_column.clone(),
                                to_table: to_table.clone(),
                                cursor: default,
                            });
                        }
                    }
                    _ => {}
                },
                VirtualFkAddStep::PickToColumn { ref from_table, ref type_column, ref type_value, ref id_column, ref to_table, cursor } => {
                    let to_cols = state.table_columns.get(to_table).cloned().unwrap_or_default();
                    match key.code {
                        KeyCode::Esc => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToTable { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), id_column: id_column.clone(), cursor: 0 }); }
                        KeyCode::Up | KeyCode::Char('k') => {
                            if cursor > 0 { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToColumn { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), id_column: id_column.clone(), to_table: to_table.clone(), cursor: cursor - 1 }); }
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            if cursor + 1 < to_cols.len() { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToColumn { from_table: from_table.clone(), type_column: type_column.clone(), type_value: type_value.clone(), id_column: id_column.clone(), to_table: to_table.clone(), cursor: cursor + 1 }); }
                        }
                        KeyCode::Enter => {
                            if let Some(to_col) = to_cols.get(cursor) {
                                let vfk = VirtualFkDef {
                                    from_table: from_table.clone(),
                                    type_column: type_column.clone(),
                                    type_value: type_value.clone(),
                                    id_column: id_column.clone(),
                                    to_table: to_table.clone(),
                                    to_column: to_col.clone(),
                                };
                                state.virtual_fks.push(vfk.clone());
                                engine.schema.virtual_fks.push(vfk);
                                state.mode = Mode::VirtualFkManager { cursor: state.virtual_fks.len().saturating_sub(1) };
                            }
                        }
                        _ => {}
                    }
                },
            }
        }
    }

    Ok(true)
}

/// Query distinct values of `type_col` in `table`, ordered by frequency descending.
/// Returns a list of (value, count) pairs.
async fn query_type_options(db: &dyn db::Database, table: &str, type_col: &str) -> Vec<(String, i64)> {
    let sql = format!(
        "SELECT {} as type_val, COUNT(*) as cnt FROM {} GROUP BY {} ORDER BY cnt DESC",
        type_col, table, type_col
    );
    db.query(&sql).await.unwrap_or_default().iter().filter_map(|row| {
        let val = row.get("type_val")?.to_string();
        let cnt = match row.get("cnt")? {
            db::Value::Integer(n) => *n,
            _ => 0,
        };
        Some((val, cnt))
    }).collect()
}

/// Execute a command string entered in command mode.
async fn execute_command(
    cmd: String,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
    pending_paths: &mut Option<(rules::Rule, Vec<schema::TablePath>)>,
) -> Result<()> {
    match rules::parse_rule(&cmd) {
        Err(e) => {
            state.mode = Mode::Error(e);
        }
        Ok(rule) => {
            match engine.execute_rule(db, rule.clone()).await {
                Err(e) => {
                    state.mode = Mode::Error(e.to_string());
                }
                Ok(None) => {
                    if place_last_added_rule_at_next_cursor(state, engine) {
                        engine.reexecute_all(db).await?;
                    }
                }
                Ok(Some(paths)) => {
                    // Multiple paths — ask user to pick
                    state.paths = paths.clone();
                    state.path_cursor = 0;
                    state.mode = Mode::PathSelection;
                    *pending_paths = Some((rule, paths));
                }
            }
        }
    }
    Ok(())
}

/// Toggle the collapsed state of the node at `flat_idx` in the tree.
fn toggle_fold(roots: &mut [engine::DataNode], flat_idx: usize) {
    let mut counter = 0usize;
    toggle_fold_recursive(roots, flat_idx, &mut counter);
}

fn toggle_fold_recursive(
    nodes: &mut [engine::DataNode],
    target: usize,
    counter: &mut usize,
) -> bool {
    for node in nodes.iter_mut() {
        if *counter == target {
            node.collapsed = !node.collapsed;
            return true;
        }
        *counter += 1;
        if !node.collapsed && toggle_fold_recursive(&mut node.children, target, counter) {
            return true;
        }
    }
    false
}
