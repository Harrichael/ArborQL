mod command_history;
mod config;
mod db;
mod engine;
mod log;
mod rules;
mod schema;
mod ui;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use engine::{Engine, flatten_tree};
use ratatui::{Terminal, backend::CrosstermBackend};
use rules::Completion;
use schema::Schema;
use std::io;
use ui::app::{AppState, ColumnManagerItem, Mode, VirtualFkAddStep};
use schema::VirtualFkDef;

/// LatticeQL — Navigate complex datasets from multiple sources intuitively.
#[derive(Parser, Debug)]
#[command(name = "latticeql", version, about)]
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
    let defaults = config::load_config(&std::env::current_dir()?)?;
    state.default_visible_columns = defaults.columns.global;
    state.default_visible_columns_by_table = defaults.columns.per_table;
    let history_max_len = defaults.history_max_len;
    // Inject virtual FKs from config.
    for vfk in defaults.virtual_fks {
        state.virtual_fks.push(vfk.clone());
        engine.schema.virtual_fks.push(vfk);
    }
    // Build per-table column lists for command completion hints.
    state.table_columns = engine.schema.tables.iter().map(|(name, info)| {
        let cols = info.columns.iter().map(|c| c.name.clone()).collect();
        (name.clone(), cols)
    }).collect();

    // Load persisted command history from ~/.latticeql/history.
    let history_file = std::env::var("HOME")
        .ok()
        .map(std::path::PathBuf::from)
        .map(|h| h.join(".latticeql").join("history"));
    if let Some(ref path) = history_file {
        match command_history::CommandHistory::load_from_file(path, history_max_len) {
            Ok(h) => state.command_history = h,
            Err(e) => eprintln!("Warning: could not load command history: {}", e),
        }
    }

    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, &mut state, &mut engine, db.as_ref(), history_file).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;

    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
    history_file: Option<std::path::PathBuf>,
) -> Result<()> {
    // Pending paths waiting for user selection
    let mut pending_paths: Option<(rules::Rule, Vec<schema::TablePath>)> = None;

    loop {
        // Drain any log entries queued by background code (e.g. type decoder warnings).
        state.logs.extend(log::drain());

        // Draw
        terminal.draw(|f| ui::render::render(f, state, &engine.roots))?;

        // Handle Ctrl+Z suspend request (set by handle_key, consumed here so
        // that we have access to the terminal object).
        if state.should_suspend {
            state.should_suspend = false;
            #[cfg(unix)]
            {
                disable_raw_mode()?;
                execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
                terminal.show_cursor()?;
                // Send SIGTSTP to the current process, suspending it.
                unsafe { libc::raise(libc::SIGTSTP) };
                // Execution resumes here after the shell sends SIGCONT.
                enable_raw_mode()?;
                execute!(terminal.backend_mut(), EnterAlternateScreen)?;
                terminal.clear()?;
            }
        }

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
                        &history_file,
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
    fn default_tree_columns(
        configured_defaults: &[String],
        node: &engine::DataNode,
    ) -> Vec<String> {
        let mut all_cols: Vec<String> = node.row.keys().cloned().collect();
        all_cols.sort();
        let mut visible: Vec<String> = configured_defaults
            .iter()
            .filter_map(|c| {
                if all_cols.iter().any(|k| k == c) {
                    Some(c.clone())
                } else {
                    None
                }
            })
            .collect();
        visible
    }

    let configured_defaults = state
        .configured_defaults_for_table(&node.table)
        .to_vec();
    let default_cols = default_tree_columns(&configured_defaults, node);

    state
        .tree_visible_columns
        .entry(node.table.clone())
        .or_insert_with(|| default_cols.clone());
    state
        .tree_column_order
        .entry(node.table.clone())
        .or_insert_with(|| {
            let mut all_cols: Vec<String> = node.row.keys().cloned().collect();
            all_cols.sort();
            let defaults = default_cols.clone();
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
    history_file: &Option<std::path::PathBuf>,
) -> Result<bool> {
    // Column manager overlay has exclusive key handling while open.
    if state.column_add.is_some() {
        // Helper: get filtered indices for current search
        let filtered: Vec<usize> = if let Some((_, ref items, _)) = state.column_add {
            let q = state.overlay_search.to_lowercase();
            items.iter().enumerate()
                .filter(|(_, it)| q.is_empty() || it.name.to_lowercase().contains(&q))
                .map(|(i, _)| i)
                .collect()
        } else { vec![] };

        match key.code {
            // Navigation always fires
            KeyCode::Up | KeyCode::Char('k') => {
                if let Some((_, _, ref mut cursor)) = state.column_add {
                    if *cursor > 0 { *cursor -= 1; }
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if let Some((_, _, ref mut cursor)) = state.column_add {
                    let max = filtered.len().saturating_sub(1);
                    if *cursor < max { *cursor += 1; }
                }
            }
            KeyCode::Char('u') if state.overlay_search.is_empty() => {
                if let Some((_, ref mut items, ref mut cursor)) = state.column_add {
                    if *cursor > 0 { items.swap(*cursor, *cursor - 1); *cursor -= 1; }
                }
            }
            KeyCode::Char('d') if state.overlay_search.is_empty() => {
                if let Some((_, ref mut items, ref mut cursor)) = state.column_add {
                    if *cursor + 1 < items.len() { items.swap(*cursor, *cursor + 1); *cursor += 1; }
                }
            }
            KeyCode::Char(' ') | KeyCode::Char('x') => {
                if let Some((_, ref mut items, cursor)) = state.column_add {
                    if let Some(&orig_idx) = filtered.get(cursor) {
                        if let Some(item) = items.get_mut(orig_idx) { item.enabled = !item.enabled; }
                    }
                }
            }
            KeyCode::Enter => {
                if let Some((table, ref items, _)) = state.column_add.clone() {
                    let enabled: Vec<String> = items.iter().filter(|i| i.enabled).map(|i| i.name.clone()).collect();
                    state.tree_visible_columns.insert(table.clone(), enabled);
                    state.tree_column_order.insert(table, items.iter().map(|i| i.name.clone()).collect());
                }
                state.reset_overlay_search();
                state.column_add = None;
            }
            // Activate search
            KeyCode::Char('/') if !state.overlay_search_active => {
                state.overlay_search_active = true;
            }
            // Esc: 3-level exit
            KeyCode::Esc => {
                if state.overlay_search_active {
                    state.overlay_search_active = false;
                } else if !state.overlay_search.is_empty() {
                    state.overlay_search.clear();
                    state.overlay_scroll = 0;
                    if let Some((_, _, ref mut cursor)) = state.column_add { *cursor = 0; }
                } else {
                    state.reset_overlay_search();
                    state.column_add = None;
                }
            }
            // Search input when active
            KeyCode::Backspace if state.overlay_search_active => {
                state.overlay_search.pop();
                state.overlay_scroll = 0;
                if let Some((_, _, ref mut cursor)) = state.column_add { *cursor = 0; }
            }
            KeyCode::Char(c) if state.overlay_search_active => {
                state.overlay_search.push(c);
                state.overlay_scroll = 0;
                if let Some((_, _, ref mut cursor)) = state.column_add { *cursor = 0; }
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
                KeyCode::Char('z') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    state.should_suspend = true;
                }
                KeyCode::Char(':') => {
                    state.mode = Mode::Command;
                    state.clear_input();
                    state.history_cursor = None;
                    state.history_draft = String::new();
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
                KeyCode::Char('r') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    // Ctrl+R from Normal mode: jump straight into reverse command search.
                    state.clear_input();
                    state.history_cursor = None;
                    state.mode = Mode::CommandSearch {
                        query: String::new(),
                        match_cursor: 0,
                        saved_input: String::new(),
                    };
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
                            state.reset_overlay_search();
                            state.column_add = Some((node.table.clone(), items, 0));
                        }
                    }
                }
                KeyCode::Char('v') => {
                    state.reset_overlay_search();
                    state.mode = Mode::VirtualFkManager { cursor: 0 };
                }
                KeyCode::Char('x') => {
                    // Prune (remove) the currently selected node from the tree.
                    let flat = flatten_tree(&engine.roots);
                    if state.selected_row < flat.len() {
                        let (_, node) = flat[state.selected_row];
                        let table = node.table.clone();
                        // Find primary key column; fall back to "id".
                        let pk_col = engine
                            .schema
                            .tables
                            .get(&table)
                            .and_then(|info| {
                                info.columns.iter().find(|c| c.is_primary_key).map(|c| c.name.clone())
                            })
                            .unwrap_or_else(|| "id".to_string());
                        if let Some(pk_val) = node.row.get(&pk_col) {
                            let conditions = vec![rules::Condition {
                                column: pk_col,
                                op: rules::Op::Eq,
                                value: pk_val.to_string(),
                            }];
                            let rule = rules::Rule::Prune {
                                table: table.clone(),
                                conditions: conditions.clone(),
                            };
                            insert_rule_at_next_cursor(state, engine, rule);
                            // Prune is in-memory: apply directly without re-fetching from DB.
                            engine.apply_prune_rule(&table, &conditions);
                        }
                    }
                }
                KeyCode::Char('l') => {
                    state.mode = Mode::LogViewer { cursor: state.logs.len().saturating_sub(1) };
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
                    state.history_cursor = None;
                    state.history_draft = String::new();
                }
                KeyCode::Enter => {
                    let cmd = state.input_text().trim().to_string();
                    // Determine whether this command should be recorded.
                    // If the user navigated to a history entry and runs it
                    // unchanged, do not append it again.
                    // Both `cmd` and `e.text` are trimmed, so the comparison
                    // is between two normalised strings.
                    let navigated_unchanged = state
                        .history_cursor
                        .and_then(|i| state.command_history.entries().get(i))
                        .map(|e| e.text == cmd)
                        .unwrap_or(false);
                    if !navigated_unchanged {
                        if state.command_history.push(cmd.clone()) {
                            if let Some(ref path) = history_file {
                                if let Some(entry) = state.command_history.entries().last() {
                                    if let Err(e) = command_history::CommandHistory::append_to_file(entry, path) {
                                        crate::log::warn(format!("could not save command history: {}", e));
                                    }
                                }
                            }
                        }
                    }
                    state.history_cursor = None;
                    state.history_draft = String::new();
                    state.mode = Mode::Normal;
                    state.clear_input();
                    if !cmd.is_empty() {
                        execute_command(cmd, state, engine, db, pending_paths).await?;
                    }
                }
                // Up/Down: navigate command history.
                KeyCode::Up => state.history_up(),
                KeyCode::Down => state.history_down(),
                // Tab: apply single-option completion.
                KeyCode::Tab => {
                    let completions = rules::completions_at(
                        &state.input,
                        &state.table_names,
                        &state.table_columns,
                    );
                    if completions.len() == 1 {
                        if let Completion::Token(ref s) = completions[0] {
                            let (_, partial) =
                                rules::tokenize_partial(&state.input);
                            let prefix_len = state.input.len() - partial.len();
                            state.input =
                                format!("{}{} ", &state.input[..prefix_len], s);
                            state.cursor = state.input.len();
                            // Reset history browsing since the input changed.
                            state.history_cursor = None;
                        }
                    }
                }
                KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    match c {
                        'c' => return Ok(false),
                        'z' => state.should_suspend = true,
                        'r' => {
                            // Enter reverse-i-search mode.
                            let saved = state.input.clone();
                            state.mode = Mode::CommandSearch {
                                query: String::new(),
                                match_cursor: 0,
                                saved_input: saved,
                            };
                        }
                        _ => {}
                    }
                }
                KeyCode::Char(c) => {
                    state.input_char(c);
                    // Typing resets history browsing position; we're now on
                    // a modified (or new) command, no longer on the exact
                    // history entry.
                    state.history_cursor = None;
                }
                KeyCode::Backspace => {
                    if state.input.is_empty() {
                        // Backspace on empty input exits command mode.
                        state.mode = Mode::Normal;
                        state.history_cursor = None;
                        state.history_draft = String::new();
                    } else {
                        state.input_backspace();
                        state.history_cursor = None;
                    }
                }
                KeyCode::Delete => state.input_delete(),
                KeyCode::Left => state.cursor_left(),
                KeyCode::Right => state.cursor_right(),
                _ => {}
            }
        }

        // ── Reverse-i-search mode ─────────────────────────────────────────
        Mode::CommandSearch { query, match_cursor, saved_input } => {
            match key.code {
                KeyCode::Esc => {
                    // Cancel search: restore the saved input.
                    state.input = saved_input.clone();
                    state.cursor = state.input.len();
                    state.history_cursor = None;
                    state.mode = Mode::Command;
                }
                KeyCode::Enter => {
                    // Accept the current match and switch to Command mode.
                    // The matched command is already in state.input (set while
                    // rendering), so we just need to switch modes.
                    let matched = state
                        .command_history
                        .search_reverse(&query, match_cursor)
                        .and_then(|i| state.command_history.entries().get(i))
                        .map(|e| e.text.clone());
                    if let Some(text) = matched {
                        state.input = text;
                        state.cursor = state.input.len();
                    }
                    state.history_cursor = None;
                    state.mode = Mode::Command;
                }
                KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    match c {
                        'c' => return Ok(false),
                        'z' => state.should_suspend = true,
                        'r' => {
                            // Ctrl+R again: advance to the next older match.
                            state.mode = Mode::CommandSearch {
                                query,
                                match_cursor: match_cursor + 1,
                                saved_input,
                            };
                        }
                        _ => {}
                    }
                }
                KeyCode::Char(c) => {
                    // Append to query, reset to most-recent match.
                    let mut new_query = query.clone();
                    new_query.push(c);
                    state.mode = Mode::CommandSearch {
                        query: new_query,
                        match_cursor: 0,
                        saved_input,
                    };
                }
                KeyCode::Backspace => {
                    let mut new_query = query.clone();
                    new_query.pop();
                    state.mode = Mode::CommandSearch {
                        query: new_query,
                        match_cursor: 0,
                        saved_input,
                    };
                }
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

        // ── Log viewer ───────────────────────────────────────────────────
        Mode::LogViewer { cursor } => {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('l') => {
                    state.mode = Mode::Normal;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if cursor > 0 {
                        state.mode = Mode::LogViewer { cursor: cursor - 1 };
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if cursor + 1 < state.logs.len() {
                        state.mode = Mode::LogViewer { cursor: cursor + 1 };
                    }
                }
                _ => {}
            }
        }

        // ── Virtual FK manager ───────────────────────────────────────────
        Mode::VirtualFkManager { cursor } => {
            let filtered: Vec<usize> = {
                let q = state.overlay_search.to_lowercase();
                state.virtual_fks.iter().enumerate()
                    .filter(|(_, vfk)| q.is_empty() || vfk.from_table.to_lowercase().contains(&q) || vfk.to_table.to_lowercase().contains(&q) || vfk.type_value.to_lowercase().contains(&q))
                    .map(|(i, _)| i)
                    .collect()
            };
            match key.code {
                // Navigation always fires
                KeyCode::Up | KeyCode::Char('k') => {
                    if cursor > 0 { state.mode = Mode::VirtualFkManager { cursor: cursor - 1 }; }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let max = filtered.len().saturating_sub(1);
                    if cursor < max { state.mode = Mode::VirtualFkManager { cursor: cursor + 1 }; }
                }
                KeyCode::Char('a') => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickFromTable { cursor: 0 }); }
                KeyCode::Char('d') | KeyCode::Char('x') if !state.overlay_search_active => {
                    if let Some(&orig_idx) = filtered.get(cursor) {
                        let removed = state.virtual_fks.remove(orig_idx);
                        engine.schema.virtual_fks.retain(|v| v != &removed);
                        let new_cursor = cursor.saturating_sub(if cursor >= filtered.len().saturating_sub(1) { 1 } else { 0 });
                        state.mode = Mode::VirtualFkManager { cursor: new_cursor };
                    }
                }
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    match config::save_virtual_fks(&std::env::current_dir()?, &state.virtual_fks) {
                        Ok(path) => { state.mode = Mode::Info(format!("Virtual FKs saved to {}", path.display())); }
                        Err(e) => { state.mode = Mode::Error(format!("Save failed: {}", e)); }
                    }
                }
                // Activate search
                KeyCode::Char('/') if !state.overlay_search_active => {
                    state.overlay_search_active = true;
                }
                // Esc: 3-level exit
                KeyCode::Esc => {
                    if state.overlay_search_active {
                        state.overlay_search_active = false;
                    } else if !state.overlay_search.is_empty() {
                        state.overlay_search.clear();
                        state.overlay_scroll = 0;
                        state.mode = Mode::VirtualFkManager { cursor: 0 };
                    } else {
                        state.reset_overlay_search();
                        state.mode = Mode::Normal;
                    }
                }
                // Search input when active
                KeyCode::Backspace if state.overlay_search_active => {
                    state.overlay_search.pop();
                    state.overlay_scroll = 0;
                    state.mode = Mode::VirtualFkManager { cursor: 0 };
                }
                KeyCode::Char(c) if state.overlay_search_active => {
                    state.overlay_search.push(c);
                    state.overlay_scroll = 0;
                    state.mode = Mode::VirtualFkManager { cursor: 0 };
                }
                _ => {}
            }
        }

        // ── Virtual FK creation wizard ───────────────────────────────────
        Mode::VirtualFkAdd(ref step) => {
            let step = step.clone();

            // Helper macro: build filtered original-indices for a slice
            macro_rules! filtered_indices {
                ($items:expr) => {{
                    let q = state.overlay_search.to_lowercase();
                    $items.iter().enumerate()
                        .filter(|(_, s)| q.is_empty() || s.to_lowercase().contains(&q))
                        .map(|(i, _)| i)
                        .collect::<Vec<_>>()
                }};
            }

            // Navigation + special keys always fire first.
            // Printable chars only feed search when search is active.
            match (&step, key.code) {
                // ── Up/Down: navigate filtered list ──────────────────────
                (_, KeyCode::Up) | (_, KeyCode::Char('k')) => {
                    let c = state.wizard_cursor();
                    if c > 0 { state.wizard_set_cursor(c - 1); }
                }
                (_, KeyCode::Down) | (_, KeyCode::Char('j')) => {
                    let max = match &step {
                        VirtualFkAddStep::PickFromTable { .. } => filtered_indices!(state.table_names).len(),
                        VirtualFkAddStep::PickTypeColumn { from_table, .. } => filtered_indices!(state.table_columns.get(from_table).cloned().unwrap_or_default()).len(),
                        VirtualFkAddStep::PickTypeValue { options, .. } => { let l: Vec<String> = options.iter().map(|(v,c)| format!("{}  ({})", v, c)).collect(); filtered_indices!(l).len() }
                        VirtualFkAddStep::PickIdColumn { from_table, .. } => filtered_indices!(state.table_columns.get(from_table).cloned().unwrap_or_default()).len(),
                        VirtualFkAddStep::PickToTable { .. } => filtered_indices!(state.table_names).len(),
                        VirtualFkAddStep::PickToColumn { to_table, .. } => filtered_indices!(state.table_columns.get(to_table).cloned().unwrap_or_default()).len(),
                    };
                    let c = state.wizard_cursor();
                    if c + 1 < max { state.wizard_set_cursor(c + 1); }
                }

                // ── / : activate search input ─────────────────────────
                (_, KeyCode::Char('/')) if !state.overlay_search_active => {
                    state.overlay_search_active = true;
                    // don't clear existing search
                }

                // ── Esc: 3-level exit ─────────────────────────────────
                (_, KeyCode::Esc) => {
                    if state.overlay_search_active {
                        // Level 1: stop typing, keep filter
                        state.overlay_search_active = false;
                    } else if !state.overlay_search.is_empty() {
                        // Level 2: clear filter
                        state.overlay_search.clear();
                        state.overlay_scroll = 0;
                        state.wizard_set_cursor(0);
                    } else {
                        // Level 3: go back one step
                        state.reset_overlay_search();
                        match step {
                            VirtualFkAddStep::PickFromTable { .. } => { state.mode = Mode::VirtualFkManager { cursor: 0 }; }
                            VirtualFkAddStep::PickTypeColumn { .. } => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickFromTable { cursor: 0 }); }
                            VirtualFkAddStep::PickTypeValue { from_table, type_column, .. } => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeColumn { from_table, cursor: 0 }); }
                            VirtualFkAddStep::PickIdColumn { from_table, type_column, type_value, .. } => {
                                let options = query_type_options(db, &from_table, &type_column).await;
                                state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeValue { from_table, type_column, options, cursor: 0 });
                            }
                            VirtualFkAddStep::PickToTable { from_table, type_column, type_value, id_column, .. } => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickIdColumn { from_table, type_column, type_value, cursor: 0 }); }
                            VirtualFkAddStep::PickToColumn { from_table, type_column, type_value, id_column, .. } => { state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToTable { from_table, type_column, type_value, id_column, cursor: 0 }); }
                        }
                    }
                }

                // ── Enter: confirm selection ───────────────────────────
                (VirtualFkAddStep::PickFromTable { cursor }, KeyCode::Enter) => {
                    let cursor = *cursor;
                    let fi = filtered_indices!(state.table_names);
                    if let Some(&orig) = fi.get(cursor) {
                        if let Some(t) = state.table_names.get(orig) {
                            let t = t.clone(); state.reset_overlay_search();
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeColumn { from_table: t, cursor: 0 });
                        }
                    }
                }
                (VirtualFkAddStep::PickTypeColumn { from_table, cursor }, KeyCode::Enter) => {
                    let cursor = *cursor; let from_table = from_table.clone();
                    let cols = state.table_columns.get(&from_table).cloned().unwrap_or_default();
                    let fi = filtered_indices!(cols);
                    if let Some(&orig) = fi.get(cursor) {
                        if let Some(col) = cols.get(orig) {
                            let col = col.clone(); state.reset_overlay_search();
                            let options = query_type_options(db, &from_table, &col).await;
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickTypeValue { from_table, type_column: col, options, cursor: 0 });
                        }
                    }
                }
                (VirtualFkAddStep::PickTypeValue { from_table, type_column, options, cursor }, KeyCode::Enter) => {
                    let cursor = *cursor; let options = options.clone();
                    let labels: Vec<String> = options.iter().map(|(v,c)| format!("{}  ({})", v, c)).collect();
                    let fi = filtered_indices!(labels);
                    if let Some(&orig) = fi.get(cursor) {
                        if let Some((tv, _)) = options.get(orig) {
                            let tv = tv.clone(); let ft = from_table.clone(); let tc = type_column.clone();
                            state.reset_overlay_search();
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickIdColumn { from_table: ft, type_column: tc, type_value: tv, cursor: 0 });
                        }
                    }
                }
                (VirtualFkAddStep::PickIdColumn { from_table, type_column, type_value, cursor }, KeyCode::Enter) => {
                    let cursor = *cursor; let from_table = from_table.clone(); let type_column = type_column.clone(); let type_value = type_value.clone();
                    let cols = state.table_columns.get(&from_table).cloned().unwrap_or_default();
                    let fi = filtered_indices!(cols);
                    if let Some(&orig) = fi.get(cursor) {
                        if let Some(col) = cols.get(orig) {
                            let col = col.clone(); state.reset_overlay_search();
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToTable { from_table, type_column, type_value, id_column: col, cursor: 0 });
                        }
                    }
                }
                (VirtualFkAddStep::PickToTable { from_table, type_column, type_value, id_column, cursor }, KeyCode::Enter) => {
                    let cursor = *cursor; let from_table = from_table.clone(); let type_column = type_column.clone(); let type_value = type_value.clone(); let id_column = id_column.clone();
                    let fi = filtered_indices!(state.table_names);
                    if let Some(&orig) = fi.get(cursor) {
                        if let Some(to_table) = state.table_names.get(orig) {
                            let to_table = to_table.clone();
                            let to_cols = state.table_columns.get(&to_table).cloned().unwrap_or_default();
                            let default = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                            state.reset_overlay_search();
                            state.mode = Mode::VirtualFkAdd(VirtualFkAddStep::PickToColumn { from_table, type_column, type_value, id_column, to_table, cursor: default });
                        }
                    }
                }
                (VirtualFkAddStep::PickToColumn { from_table, type_column, type_value, id_column, to_table, cursor }, KeyCode::Enter) => {
                    let cursor = *cursor; let to_table = to_table.clone();
                    let to_cols = state.table_columns.get(&to_table).cloned().unwrap_or_default();
                    let fi = filtered_indices!(to_cols);
                    if let Some(&orig) = fi.get(cursor) {
                        if let Some(to_col) = to_cols.get(orig) {
                            let vfk = VirtualFkDef {
                                from_table: from_table.clone(), type_column: type_column.clone(),
                                type_value: type_value.clone(), id_column: id_column.clone(),
                                to_table: to_table.clone(), to_column: to_col.clone(),
                            };
                            state.virtual_fks.push(vfk.clone());
                            engine.schema.virtual_fks.push(vfk);
                            state.reset_overlay_search();
                            state.mode = Mode::VirtualFkManager { cursor: state.virtual_fks.len().saturating_sub(1) };
                        }
                    }
                }

                // ── Search input: printable chars when active ─────────
                (_, KeyCode::Backspace) if state.overlay_search_active => {
                    state.overlay_search.pop();
                    state.overlay_scroll = 0;
                    state.wizard_set_cursor(0);
                }
                (_, KeyCode::Char(c)) if state.overlay_search_active => {
                    state.overlay_search.push(c);
                    state.overlay_scroll = 0;
                    state.wizard_set_cursor(0);
                }

                _ => {}
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
                Ok(Some(result)) => {
                    // Multiple paths — ask user to pick
                    state.paths = result.paths.clone();
                    state.paths_has_more = result.has_more;
                    state.path_cursor = 0;
                    state.mode = Mode::PathSelection;
                    *pending_paths = Some((rule, result.paths));
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
