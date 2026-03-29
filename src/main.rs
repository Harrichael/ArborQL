mod app;
mod command_history;
mod config;
mod connection_manager;
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

use std::io;
use app::column_manager::service::ColumnManagerModule;
use app::model::SchemaNode;
use connection_manager::{ConnectionManager, ConnectionType};
use ui::app::{AppState, ConfirmAction, ConnectionForm, ConnectionManagerTab, Mode, PALETTE_COMMANDS, VirtualFkField, VirtualFkForm};
use ui::model::control_panel::{dispatch, ControlPanel};
use ui::model::keys::from_key_event;
use schema::VirtualFkDef;

/// LatticeQL — Navigate complex datasets from multiple sources intuitively.
#[derive(Parser, Debug)]
#[command(name = "latticeql", version, about)]
struct Args {
    /// Database connection URL (optional — can also add via the connection manager).
    ///
    /// Examples:
    ///   sqlite://path/to/db.sqlite3
    ///   mysql://user:password@localhost/dbname
    #[arg(short, long)]
    database: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut conn_mgr = ConnectionManager::new();

    // Load config first so we can restore saved connections.
    let defaults = config::load_config()?;

    // If a database URL was provided, add it as the first connection.
    if let Some(ref url) = args.database {
        let alias = ConnectionManager::alias_from_url(url);
        let conn_type = ConnectionType::from_url(url)
            .ok_or_else(|| anyhow::anyhow!("Unsupported database URL: {}", url))?;
        eprintln!("Connecting to database as '{}'…", alias);
        let params = ConnectionType::params_from_url(url);
        conn_mgr.add_connection(None, alias, conn_type, url.clone(), params).await?;
    }

    // Saved connections from config are loaded into state.saved_connections
    // (the Saved tab). They are NOT auto-connected — the user picks one and
    // provides an alias via the Connection Manager.

    let schema = conn_mgr.merged_schema().clone();
    let table_names = schema.table_names();

    let mut engine = Engine::new(schema);
    let mut state = AppState::new();
    state.table_names = table_names;
    state.saved_connections = defaults.connections;
    state.connections_summary = conn_mgr.connection_summaries(&saved_ids(&state));
    state.display_table_names = conn_mgr.display_table_names();
    state.display_name_map = conn_mgr.display_name_map();
    state.column_manager = ColumnManagerModule::new(defaults.columns.global, defaults.columns.per_table);
    // Register all known schema nodes with the column manager.
    for (_name, info) in &engine.schema.tables {
        state.column_manager.register_node(&SchemaNode::from_table_info(info));
    }
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
    let history_file = config::home_dir()
        .ok()
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

    let result = run_app(&mut terminal, &mut state, &mut engine, &mut conn_mgr, history_file).await;

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
    conn_mgr: &mut ConnectionManager,
    history_file: Option<std::path::PathBuf>,
) -> Result<()> {
    // Pending paths waiting for user selection
    let mut pending_paths: Option<(rules::Rule, Vec<engine::TablePath>)> = None;

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
                        conn_mgr,
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

/// Compute the set of saved connection IDs for is_saved checks.
fn saved_ids(state: &AppState) -> std::collections::HashSet<String> {
    state.saved_connections.iter().map(|s| s.id.clone()).collect()
}

/// Refresh engine schema and UI state after a connection change.
fn refresh_schema_from_conn_mgr(
    state: &mut AppState,
    engine: &mut Engine,
    conn_mgr: &ConnectionManager,
) {
    let mut schema = conn_mgr.merged_schema().clone();
    // Re-inject virtual FKs into the new schema.
    for vfk in &state.virtual_fks {
        schema.virtual_fks.push(vfk.clone());
    }
    engine.schema = schema;
    state.table_names = engine.schema.table_names();
    state.table_columns = engine
        .schema
        .tables
        .iter()
        .map(|(name, info)| {
            let cols = info.columns.iter().map(|c| c.name.clone()).collect();
            (name.clone(), cols)
        })
        .collect();
    // Register any new schema nodes with the column manager.
    for (_name, info) in &engine.schema.tables {
        state.column_manager.register_node(&SchemaNode::from_table_info(info));
    }
    state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
    state.display_table_names = conn_mgr.display_table_names();
    state.display_name_map = conn_mgr.display_name_map();
}

/// Returns `false` when the application should quit.
async fn handle_key(
    key: crossterm::event::KeyEvent,
    state: &mut AppState,
    engine: &mut Engine,
    conn_mgr: &mut ConnectionManager,
    pending_paths: &mut Option<(rules::Rule, Vec<engine::TablePath>)>,
    history_file: &Option<std::path::PathBuf>,
) -> Result<bool> {
    // The ConnectionManager implements Database, so we can use it as &dyn Database.
    let db: &dyn db::Database = conn_mgr;

    // Ctrl+Z suspends regardless of current mode.
    if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) {
        state.should_suspend = true;
        return Ok(true);
    }

    // Column manager overlay has exclusive key handling while open.
    if let Some(ref mut widget) = state.column_add {
        if let Some(event) = from_key_event(key, &widget.focus_loci()) {
            dispatch(widget, event);
        }
        if widget.closed {
            if widget.confirmed {
                state.column_manager.apply_widget(widget);
            }
            state.column_add = None;
        }
        return Ok(true);
    }

    match state.mode.clone() {
        // ── Normal mode ──────────────────────────────────────────────────
        Mode::Normal => {
            match key.code {
                KeyCode::Char(':') => {
                    state.mode = Mode::CommandPalette;
                    state.clear_input();
                }
                KeyCode::Char('j') | KeyCode::Down => state.select_down(),
                KeyCode::Char('k') | KeyCode::Up => state.select_up(),
                KeyCode::Enter => {
                    // Toggle fold on selected node
                    let flat = flatten_tree(&engine.roots);
                    if state.selected_row < flat.len() {
                        toggle_fold(&mut engine.roots, state.selected_row);
                    }
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
                KeyCode::Char(c) => {
                    // Any other character enters query mode with that char.
                    state.mode = Mode::Query;
                    state.clear_input();
                    state.input_char(c);
                    state.history_cursor = None;
                    state.history_draft = String::new();
                }
                _ => {}
            }
        }

        // ── Query mode ───────────────────────────────────────────────────
        Mode::Query => {
            match key.code {
                // ':' on empty input opens the command palette (same as Normal mode).
                KeyCode::Char(':') if state.input.is_empty() => {
                    state.mode = Mode::CommandPalette;
                    state.clear_input();
                }
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
                        &state.completion_table_names(),
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
                    state.mode = Mode::Query;
                }
                KeyCode::Enter => {
                    // Accept the current match and switch to Query mode.
                    // The matched text is already resolved below.
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
                    state.mode = Mode::Query;
                }
                KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    match c {
                        'c' => return Ok(false),
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

        // ── Command palette (`:` key) ────────────────────────────────────
        Mode::CommandPalette => {
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                    state.clear_input();
                }
                KeyCode::Enter => {
                    let filter = state.input_text().trim().to_lowercase();
                    state.clear_input();
                    // Exact shortcut match takes priority, otherwise require a unique name prefix match.
                    let shortcut_match = PALETTE_COMMANDS.iter()
                        .find(|(_, key, _)| *key == filter);
                    let matched = if let Some((name, _, _)) = shortcut_match {
                        Some(*name)
                    } else {
                        let name_matches: Vec<_> = PALETTE_COMMANDS.iter()
                            .filter(|(name, _, _)| name.starts_with(&filter))
                            .collect();
                        if name_matches.len() == 1 { Some(name_matches[0].0) } else { None }
                    };
                    match matched {
                        Some("quit") => return Ok(false),
                        Some("schema") => {
                            state.show_schema = !state.show_schema;
                            state.mode = Mode::Normal;
                        }
                        Some("columns") => {
                            let flat = flatten_tree(&engine.roots);
                            if state.selected_row < flat.len() {
                                let (_, node) = flat[state.selected_row];
                                let available = columns_for_table(&engine.roots, &node.table);
                                let widget = state.column_manager.open_widget(&node.table, &available);
                                if !widget.items.is_empty() {
                                    state.column_add = Some(widget);
                                }
                            }
                            state.mode = Mode::Normal;
                        }
                        Some("lattice") => {
                            state.reset_overlay_search();
                            state.mode = Mode::VirtualFkManager { cursor: 0 };
                        }
                        Some("rules") => {
                            if !engine.rules.is_empty() {
                                state.rules = engine.rules.clone();
                                state.rule_cursor = 0;
                                state.next_rule_cursor =
                                    state.next_rule_cursor.min(state.rules.len());
                                state.rule_reorder_undo.clear();
                                state.rule_reorder_redo.clear();
                                state.mode = Mode::RuleReorder;
                            } else {
                                state.mode = Mode::Normal;
                            }
                        }
                        Some("connections") => {
                            state.reset_overlay_search();
                            state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
                            state.mode = Mode::ConnectionManager {
                                tab: ConnectionManagerTab::Connections,
                                cursor: 0,
                            };
                        }
                        Some("logs") => {
                            state.mode = Mode::LogViewer { cursor: state.logs.len().saturating_sub(1) };
                        }
                        Some("manuals") => {
                            state.mode = Mode::ManualList { cursor: 0 };
                        }
                        Some("prune") => {
                            let flat = flatten_tree(&engine.roots);
                            if state.selected_row < flat.len() {
                                let (_, node) = flat[state.selected_row];
                                let table = node.table.clone();
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
                                    engine.apply_prune_rule(&table, &conditions);
                                }
                            }
                            state.mode = Mode::Normal;
                        }
                        _ => {
                            state.mode = Mode::Normal;
                        }
                    }
                }
                KeyCode::Backspace => {
                    if state.input.is_empty() {
                        state.mode = Mode::Normal;
                    } else {
                        state.input_backspace();
                    }
                }
                KeyCode::Char(c) => {
                    state.input_char(c);
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
                KeyCode::Char('n') if state.paths_has_more => {
                    if let Some((ref rule, ref mut paths)) = *pending_paths {
                        if let rules::Rule::Relation { from_table, to_table, via, .. } = rule {
                            let more = crate::engine::find_paths(
                                &engine.schema, from_table, to_table, via,
                                state.paths_next_depth, engine::MAX_PATH_DEPTH,
                            );
                            paths.extend(more.paths.iter().cloned());
                            state.paths.extend(more.paths);
                            state.paths_has_more = more.has_more;
                            state.paths_next_depth = more.next_depth;
                        }
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
                    .filter(|(_, vfk)| q.is_empty() || vfk.from_table.to_lowercase().contains(&q) || vfk.to_table.to_lowercase().contains(&q) || vfk.type_value.as_deref().unwrap_or("").to_lowercase().contains(&q))
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
                KeyCode::Char('a') => {
                    state.reset_overlay_search();
                    state.mode = Mode::VirtualFkAdd(VirtualFkForm::new());
                }
                KeyCode::Char('d') | KeyCode::Char('x') if !state.overlay_search_active => {
                    if let Some(&orig_idx) = filtered.get(cursor) {
                        let removed = state.virtual_fks.remove(orig_idx);
                        engine.schema.virtual_fks.retain(|v| v != &removed);
                        let new_cursor = cursor.saturating_sub(if cursor >= filtered.len().saturating_sub(1) { 1 } else { 0 });
                        state.mode = Mode::VirtualFkManager { cursor: new_cursor };
                    }
                }
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    match config::save_virtual_fks(&state.virtual_fks) {
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

        // ── Virtual FK creation form ────────────────────────────────────
        Mode::VirtualFkAdd(ref form_state) => {
            let form = form_state.clone();

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

            // Build the dropdown items for the currently active field.
            let dropdown_items: Vec<String> = match &form.active_field {
                VirtualFkField::FromTable | VirtualFkField::ToTable => state.display_table_names.clone(),
                VirtualFkField::IdColumn => {
                    state.table_columns.get(&form.from_table).cloned().unwrap_or_default()
                }
                VirtualFkField::TypeColumn => {
                    let mut cols = vec!["(none — simple FK)".to_string()];
                    cols.extend(state.table_columns.get(&form.from_table).cloned().unwrap_or_default());
                    cols
                }
                VirtualFkField::TypeValue => {
                    if form.type_column.is_empty() {
                        vec!["(no type_column set — skipping)".to_string()]
                    } else {
                        form.type_options.iter().map(|(v, c)| format!("{}  ({})", v, c)).collect()
                    }
                }
                VirtualFkField::ToColumn => {
                    state.table_columns.get(&form.to_table).cloned().unwrap_or_default()
                }
            };

            match key.code {
                // ── Tab: advance to next field ─────────────────────────
                KeyCode::Tab => {
                    if let Mode::VirtualFkAdd(f) = &mut state.mode {
                        let next = f.active_field.next(f.type_column.is_empty());
                        f.active_field = next;
                        f.cursor = 0;
                    }
                    state.overlay_search.clear();
                    state.overlay_search_active = false;
                    state.overlay_scroll = 0;
                    // Pre-select "id" when switching to ToColumn
                    if let Mode::VirtualFkAdd(f) = &mut state.mode {
                        if f.active_field == VirtualFkField::ToColumn {
                            let to_cols = state.table_columns.get(&f.to_table).cloned().unwrap_or_default();
                            f.cursor = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                        }
                    }
                    // Load type options when switching to TypeValue
                    if let Mode::VirtualFkAdd(f) = &state.mode {
                        if f.active_field == VirtualFkField::TypeValue && !f.type_column.is_empty() {
                            let tc = f.type_column.clone();
                            let ft = f.from_table.clone();
                            let options = query_type_options(db, &ft, &tc).await;
                            if let Mode::VirtualFkAdd(f) = &mut state.mode {
                                f.type_options = options;
                            }
                        }
                    }
                }

                // ── Shift+Tab: go to previous field ───────────────────
                KeyCode::BackTab => {
                    if let Mode::VirtualFkAdd(f) = &mut state.mode {
                        let prev = f.active_field.prev(f.type_column.is_empty());
                        f.active_field = prev;
                        f.cursor = 0;
                    }
                    state.overlay_search.clear();
                    state.overlay_search_active = false;
                    state.overlay_scroll = 0;
                    // Load type options when switching back to TypeValue
                    if let Mode::VirtualFkAdd(f) = &state.mode {
                        if f.active_field == VirtualFkField::TypeValue && !f.type_column.is_empty() {
                            let tc = f.type_column.clone();
                            let ft = f.from_table.clone();
                            let options = query_type_options(db, &ft, &tc).await;
                            if let Mode::VirtualFkAdd(f) = &mut state.mode {
                                f.type_options = options;
                            }
                        }
                    }
                }

                // ── Up/Down: navigate the active dropdown ──────────────
                // Arrow keys always navigate regardless of search state.
                // 'k'/'j' only navigate when search input is not active
                // (when search is active they fall through to the Char(c) handler).
                KeyCode::Up => {
                    let c = state.wizard_cursor();
                    if c > 0 { state.wizard_set_cursor(c - 1); }
                }
                KeyCode::Char('k') if !state.overlay_search_active => {
                    let c = state.wizard_cursor();
                    if c > 0 { state.wizard_set_cursor(c - 1); }
                }
                KeyCode::Down => {
                    let fi = filtered_indices!(dropdown_items);
                    let c = state.wizard_cursor();
                    if c + 1 < fi.len() { state.wizard_set_cursor(c + 1); }
                }
                KeyCode::Char('j') if !state.overlay_search_active => {
                    let fi = filtered_indices!(dropdown_items);
                    let c = state.wizard_cursor();
                    if c + 1 < fi.len() { state.wizard_set_cursor(c + 1); }
                }

                // ── / : activate search ───────────────────────────────
                KeyCode::Char('/') if !state.overlay_search_active => {
                    state.overlay_search_active = true;
                }

                // ── Esc: 3-level exit ─────────────────────────────────
                KeyCode::Esc => {
                    if state.overlay_search_active {
                        state.overlay_search_active = false;
                    } else if !state.overlay_search.is_empty() {
                        state.overlay_search.clear();
                        state.overlay_scroll = 0;
                        state.wizard_set_cursor(0);
                    } else {
                        state.reset_overlay_search();
                        state.mode = Mode::VirtualFkManager { cursor: 0 };
                    }
                }

                // ── Enter: confirm selection for active field ─────────
                KeyCode::Enter => {
                    let fi = filtered_indices!(dropdown_items);
                    if let Some(&orig) = fi.get(form.cursor) {
                        if let Some(raw_value) = dropdown_items.get(orig) {
                            let raw_value = raw_value.clone();
                            state.reset_overlay_search();

                            if let Mode::VirtualFkAdd(f) = &mut state.mode {
                                match &f.active_field {
                                    VirtualFkField::FromTable => {
                                        f.from_table = raw_value;
                                        // Reset dependent fields when source table changes
                                        f.id_column.clear();
                                        f.type_column.clear();
                                        f.type_value.clear();
                                        f.active_field = VirtualFkField::IdColumn;
                                        f.cursor = 0;
                                    }
                                    VirtualFkField::IdColumn => {
                                        f.id_column = raw_value;
                                        f.active_field = VirtualFkField::TypeColumn;
                                        f.cursor = 0;
                                    }
                                    VirtualFkField::TypeColumn => {
                                        if raw_value.starts_with("(none") {
                                            f.type_column.clear();
                                            f.type_value.clear();
                                            // Skip TypeValue — jump straight to ToTable
                                            f.active_field = VirtualFkField::ToTable;
                                        } else {
                                            f.type_column = raw_value;
                                            f.active_field = VirtualFkField::TypeValue;
                                        }
                                        f.cursor = 0;
                                    }
                                    VirtualFkField::TypeValue => {
                                        if !raw_value.starts_with("(no type_column") {
                                            // Strip the "  (count)" suffix
                                            let tv = raw_value
                                                .split("  (")
                                                .next()
                                                .unwrap_or(&raw_value)
                                                .to_string();
                                            f.type_value = tv;
                                        }
                                        f.active_field = VirtualFkField::ToTable;
                                        f.cursor = 0;
                                    }
                                    VirtualFkField::ToTable => {
                                        f.to_table = raw_value;
                                        f.to_column.clear();
                                        // Pre-select "id" column if it exists
                                        let to_cols = state.table_columns
                                            .get(&f.to_table).cloned().unwrap_or_default();
                                        f.cursor = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                                        f.active_field = VirtualFkField::ToColumn;
                                    }
                                    VirtualFkField::ToColumn => {
                                        f.to_column = raw_value;
                                        // Commit when all required fields are filled
                                        if f.is_complete() {
                                            let vfk = VirtualFkDef {
                                                from_table: f.from_table.clone(),
                                                type_column: if f.type_column.is_empty() { None } else { Some(f.type_column.clone()) },
                                                type_value: if f.type_value.is_empty() { None } else { Some(f.type_value.clone()) },
                                                id_column: f.id_column.clone(),
                                                to_table: f.to_table.clone(),
                                                to_column: f.to_column.clone(),
                                            };
                                            state.virtual_fks.push(vfk.clone());
                                            engine.schema.virtual_fks.push(vfk);
                                            state.reset_overlay_search();
                                            state.mode = Mode::VirtualFkManager {
                                                cursor: state.virtual_fks.len().saturating_sub(1),
                                            };
                                            return Ok(true);
                                        }
                                    }
                                }
                            }

                            // Load type options when switching to TypeValue
                            if let Mode::VirtualFkAdd(f) = &state.mode {
                                if f.active_field == VirtualFkField::TypeValue
                                    && !f.type_column.is_empty()
                                {
                                    let tc = f.type_column.clone();
                                    let ft = f.from_table.clone();
                                    let options = query_type_options(db, &ft, &tc).await;
                                    if let Mode::VirtualFkAdd(f) = &mut state.mode {
                                        f.type_options = options;
                                    }
                                }
                            }
                        }
                    }
                }

                // ── Ctrl+S: commit + save when form is complete ────────
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    if form.is_complete() {
                        let vfk = VirtualFkDef {
                            from_table: form.from_table.clone(),
                            type_column: if form.type_column.is_empty() { None } else { Some(form.type_column.clone()) },
                            type_value: if form.type_value.is_empty() { None } else { Some(form.type_value.clone()) },
                            id_column: form.id_column.clone(),
                            to_table: form.to_table.clone(),
                            to_column: form.to_column.clone(),
                        };
                        state.virtual_fks.push(vfk.clone());
                        engine.schema.virtual_fks.push(vfk);
                        match config::save_virtual_fks(&state.virtual_fks) {
                            Ok(path) => {
                                state.reset_overlay_search();
                                state.mode = Mode::Info(format!("Virtual FK saved to {}", path.display()));
                            }
                            Err(e) => {
                                state.mode = Mode::Error(format!("Save failed: {}", e));
                            }
                        }
                    }
                }

                // ── Search input: printable chars when active ──────────
                KeyCode::Backspace if state.overlay_search_active => {
                    state.overlay_search.pop();
                    state.overlay_scroll = 0;
                    state.wizard_set_cursor(0);
                }
                KeyCode::Char(c) if state.overlay_search_active => {
                    state.overlay_search.push(c);
                    state.overlay_scroll = 0;
                    state.wizard_set_cursor(0);
                }

                _ => {}
            }
        }

        // ── Confirm dialog ──────────────────────────────────────────────
        Mode::Confirm { tag, .. } => {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Char('n') | KeyCode::Char('N') => {
                    let save_pw = matches!(key.code, KeyCode::Char('y') | KeyCode::Char('Y'));
                    match tag {
                        ConfirmAction::SaveConnectionWithPassword { conn_index } => {
                            if conn_index < conn_mgr.connections.len() {
                                let conn = &conn_mgr.connections[conn_index];
                                let alias = conn.alias.clone();
                                match config::save_connection(conn, &state.saved_connections, save_pw) {
                                    Ok((path, updated)) => {
                                        state.saved_connections = updated;
                                        state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
                                        let pw_note = if save_pw { " (with password)" } else { "" };
                                        state.mode = Mode::Info(format!("Connection '{}' saved{} to {}", alias, pw_note, path.display()));
                                    }
                                    Err(e) => {
                                        state.mode = Mode::Error(format!("Save failed: {}", e));
                                    }
                                }
                            } else {
                                state.mode = Mode::Normal;
                            }
                        }
                    }
                }
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                }
                _ => {}
            }
        }

        // ── Connection manager overlay ─────────────────────────────────
        Mode::ConnectionManager { tab, cursor } => {
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                }
                KeyCode::Right | KeyCode::Tab => {
                    let new_tab = match tab {
                        ConnectionManagerTab::Connections => ConnectionManagerTab::Saved,
                        ConnectionManagerTab::Saved => ConnectionManagerTab::Connectors,
                        ConnectionManagerTab::Connectors => ConnectionManagerTab::Connections,
                    };
                    state.mode = Mode::ConnectionManager { tab: new_tab, cursor: 0 };
                }
                KeyCode::Left | KeyCode::BackTab => {
                    let new_tab = match tab {
                        ConnectionManagerTab::Connections => ConnectionManagerTab::Connectors,
                        ConnectionManagerTab::Saved => ConnectionManagerTab::Connections,
                        ConnectionManagerTab::Connectors => ConnectionManagerTab::Saved,
                    };
                    state.mode = Mode::ConnectionManager { tab: new_tab, cursor: 0 };
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if cursor > 0 {
                        state.mode = Mode::ConnectionManager { tab, cursor: cursor - 1 };
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let max = match &tab {
                        ConnectionManagerTab::Connections => {
                            state.connections_summary.len().saturating_sub(1)
                        }
                        ConnectionManagerTab::Saved => {
                            state.saved_connections.len().saturating_sub(1)
                        }
                        ConnectionManagerTab::Connectors => {
                            ConnectionType::all().len().saturating_sub(1)
                        }
                    };
                    if cursor < max {
                        state.mode = Mode::ConnectionManager { tab, cursor: cursor + 1 };
                    }
                }
                KeyCode::Enter => {
                    match &tab {
                        ConnectionManagerTab::Connectors => {
                            let types = ConnectionType::all();
                            if cursor < types.len() {
                                let ct = types[cursor].clone();
                                state.mode = Mode::ConnectionAdd(ConnectionForm::new(ct));
                            }
                        }
                        ConnectionManagerTab::Saved => {
                            if cursor < state.saved_connections.len() {
                                // Suggest an alias from the saved params.
                                let saved = &state.saved_connections[cursor];
                                let conn_type = match saved.conn_type.as_str() {
                                    "sqlite" => ConnectionType::Sqlite,
                                    _ => ConnectionType::Mysql,
                                };
                                let suggested = saved.params.get("path")
                                    .or_else(|| saved.params.get("database"))
                                    .cloned()
                                    .map(|s| {
                                        std::path::Path::new(&s)
                                            .file_stem()
                                            .and_then(|f| f.to_str())
                                            .unwrap_or(&s)
                                            .to_string()
                                    })
                                    .unwrap_or_else(|| format!("conn{}", cursor + 1));
                                let _ = conn_type; // used only for alias suggestion
                                state.mode = Mode::SavedConnectionAlias {
                                    saved_index: cursor,
                                    alias: suggested,
                                };
                            }
                        }
                        ConnectionManagerTab::Connections => {
                            // Toggle connect/disconnect (also retries on Error)
                            if cursor < conn_mgr.connections.len() {
                                if conn_mgr.connections[cursor].is_connected() {
                                    conn_mgr.disconnect(cursor);
                                } else {
                                    // Attempt reconnect; on failure the connection
                                    // stays in Error state which is visible in the list.
                                    let _ = conn_mgr.reconnect(cursor).await;
                                }
                                refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                                state.mode = Mode::ConnectionManager { tab, cursor };
                            }
                        }
                    }
                }
                KeyCode::Char('d') | KeyCode::Char('x') => {
                    if tab == ConnectionManagerTab::Saved && cursor < state.saved_connections.len() {
                        let removed_id = state.saved_connections[cursor].id.clone();
                        // Persist removal to config.
                        if let Ok((_path, updated)) = config::remove_saved_connection(&removed_id, &state.saved_connections) {
                            state.saved_connections = updated;
                        } else {
                            state.saved_connections.remove(cursor);
                        }
                        // Refresh summaries so is_saved updates for any live connection with this ID.
                        state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
                        let new_cursor = cursor.min(state.saved_connections.len().saturating_sub(1));
                        state.mode = Mode::ConnectionManager { tab, cursor: new_cursor };
                    } else if tab == ConnectionManagerTab::Connections && cursor < conn_mgr.connections.len() {
                        conn_mgr.remove_connection(cursor);
                        refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                        let new_cursor = cursor.min(conn_mgr.connections.len().saturating_sub(1));
                        state.mode = Mode::ConnectionManager { tab, cursor: new_cursor };
                    }
                }
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    if tab == ConnectionManagerTab::Connections && cursor < conn_mgr.connections.len() {
                        let conn = &conn_mgr.connections[cursor];
                        if conn.has_password() {
                            state.mode = Mode::Confirm {
                                message: format!(
                                    "Connection '{}' has a password. Save password to config file? (y/n)",
                                    conn.alias
                                ),
                                tag: ConfirmAction::SaveConnectionWithPassword { conn_index: cursor },
                            };
                        } else {
                            match config::save_connection(conn, &state.saved_connections, false) {
                                Ok((path, updated)) => {
                                    state.saved_connections = updated;
                                    state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
                                    state.mode = Mode::Info(format!("Connection '{}' saved to {}", conn.alias, path.display()));
                                }
                                Err(e) => {
                                    state.mode = Mode::Error(format!("Save failed: {}", e));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        // ── Saved connection alias prompt ────────────────────────────────
        Mode::SavedConnectionAlias { saved_index, ref alias } => {
            let saved_index = saved_index;
            let alias = alias.clone();
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::ConnectionManager {
                        tab: ConnectionManagerTab::Saved,
                        cursor: saved_index,
                    };
                }
                KeyCode::Enter => {
                    if !alias.is_empty() {
                        if let Some(saved) = state.saved_connections.get(saved_index) {
                            let inherited_id = saved.id.clone();
                            let conn_type = match saved.conn_type.as_str() {
                                "sqlite" => ConnectionType::Sqlite,
                                _ => ConnectionType::Mysql,
                            };
                            let params = saved.params.clone();
                            match conn_type.build_url(&params) {
                                Ok(url) => {
                                    let result = conn_mgr.add_connection(
                                        Some(inherited_id), alias.clone(), conn_type, url, params,
                                    ).await;
                                    refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                                    match result {
                                        Ok(()) => {
                                            state.mode = Mode::Info(format!(
                                                "Connected '{}'",
                                                alias,
                                            ));
                                        }
                                        Err(_) => {
                                            let conn_idx = conn_mgr.connections.len().saturating_sub(1);
                                            state.mode = Mode::ConnectionManager {
                                                tab: ConnectionManagerTab::Connections,
                                                cursor: conn_idx,
                                            };
                                        }
                                    }
                                }
                                Err(e) => {
                                    state.mode = Mode::Error(format!("Invalid params: {}", e));
                                }
                            }
                        }
                    }
                }
                KeyCode::Backspace => {
                    if let Mode::SavedConnectionAlias { ref mut alias, .. } = state.mode {
                        alias.pop();
                    }
                }
                KeyCode::Char(c) => {
                    if let Mode::SavedConnectionAlias { ref mut alias, .. } = state.mode {
                        alias.push(c);
                    }
                }
                _ => {}
            }
        }

        // ── Connection add form ─────────────────────────────────────────
        Mode::ConnectionAdd(ref form_state) => {
            let form = form_state.clone();
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::ConnectionManager {
                        tab: ConnectionManagerTab::Connectors,
                        cursor: 0,
                    };
                }
                KeyCode::Tab => {
                    if let Mode::ConnectionAdd(ref mut f) = state.mode {
                        f.active_field = (f.active_field + 1) % f.fields.len();
                    }
                }
                KeyCode::BackTab => {
                    if let Mode::ConnectionAdd(ref mut f) = state.mode {
                        if f.active_field == 0 {
                            f.active_field = f.fields.len() - 1;
                        } else {
                            f.active_field -= 1;
                        }
                    }
                }
                KeyCode::Enter | KeyCode::Char('s')
                    if key.code == KeyCode::Enter
                        || key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) =>
                {
                    if form.is_complete() {
                        let alias = form.alias().to_string();
                        let conn_type = form.conn_type.clone();
                        let params = form.values();
                        match conn_type.build_url(&params) {
                            Err(e) => {
                                state.mode = Mode::Error(format!("Invalid params: {}", e));
                            }
                            Ok(url) => {
                                // add_connection adds the connection regardless of
                                // success/failure (Error state on failure).
                                let result = conn_mgr.add_connection(None, alias.clone(), conn_type, url, params).await;
                                refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                                let conn_idx = conn_mgr.connections.len().saturating_sub(1);
                                match result {
                                    Ok(()) => {
                                        state.mode = Mode::Info(format!(
                                            "Connected '{}' ({} tables)",
                                            alias,
                                            conn_mgr
                                                .connections
                                                .last()
                                                .map(|c| c.original_tables.len())
                                                .unwrap_or(0)
                                        ));
                                    }
                                    Err(_) => {
                                        // Connection was added in Error state;
                                        // go to manager so user can see it and retry.
                                        state.mode = Mode::ConnectionManager {
                                            tab: ConnectionManagerTab::Connections,
                                            cursor: conn_idx,
                                        };
                                    }
                                }
                            }
                        }
                    }
                }
                KeyCode::Backspace => {
                    if let Mode::ConnectionAdd(ref mut f) = state.mode {
                        f.fields[f.active_field].value.pop();
                    }
                }
                KeyCode::Char(c) => {
                    if let Mode::ConnectionAdd(ref mut f) = state.mode {
                        f.fields[f.active_field].value.push(c);
                    }
                }
                _ => {}
            }
        }

        // ── Manual list ──────────────────────────────────────────────────
        Mode::ManualList { cursor } => {
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('m') => {
                    state.mode = Mode::Normal;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if cursor > 0 {
                        state.mode = Mode::ManualList { cursor: cursor - 1 };
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if cursor + 1 < ui::render::MANUALS.len() {
                        state.mode = Mode::ManualList { cursor: cursor + 1 };
                    }
                }
                KeyCode::Enter => {
                    state.mode = Mode::ManualView { index: cursor, scroll: 0 };
                }
                _ => {}
            }
        }

        // ── Manual viewer ────────────────────────────────────────────────
        Mode::ManualView { index, scroll } => {
            let line_count = ui::render::manual_line_count(index);
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    state.mode = Mode::ManualList { cursor: index };
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if scroll > 0 {
                        state.mode = Mode::ManualView { index, scroll: scroll - 1 };
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if scroll + 1 < line_count {
                        state.mode = Mode::ManualView { index, scroll: scroll + 1 };
                    }
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
    pending_paths: &mut Option<(rules::Rule, Vec<engine::TablePath>)>,
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
                    state.paths_next_depth = result.next_depth;
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
