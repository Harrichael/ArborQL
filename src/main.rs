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
use app::column_manager::module::ColumnManagerModule;
use app::model::SchemaNode;
use connection_manager::{ConnectionManager, ConnectionType};
use app::connection_manager::widget::{ConnManagerAction, ConnManagerWidget};
use app::virtual_fk_manager::widget::{VfkAction, VfkWidget};
use ui::app::{AppState, ConfirmAction, Mode, PALETTE_COMMANDS};
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

    // Manuals overlay has exclusive key handling while open.
    if let Some(ref mut widget) = state.manuals {
        if let Some(event) = from_key_event(key, &widget.focus_loci()) {
            dispatch(widget, event);
        }
        if widget.closed {
            state.manuals = None;
        }
        return Ok(true);
    }

    // Rule reorder overlay has exclusive key handling while open.
    if let Some(ref mut widget) = state.rules_reorder {
        if let Some(event) = from_key_event(key, &widget.focus_loci()) {
            dispatch(widget, event);
        }
        if widget.closed {
            if widget.confirmed {
                engine.rules = widget.rules.clone();
                state.next_rule_cursor = widget.next_cursor.min(engine.rules.len());
                let _ = engine.reexecute_all(conn_mgr).await;
            }
            state.rules_reorder = None;
        }
        return Ok(true);
    }

    // Connection manager overlay has exclusive key handling while open.
    if state.conn_manager.is_some() {
        // Dispatch key event
        if let Some(ref mut widget) = state.conn_manager {
            if let Some(event) = from_key_event(key, &widget.focus_loci()) {
                dispatch(widget, event);
            }
        }
        // Extract action (take ownership so we can act on it without borrowing widget)
        let action = state.conn_manager.as_mut()
            .map(|w| std::mem::replace(&mut w.action, ConnManagerAction::None))
            .unwrap_or(ConnManagerAction::None);
        match action {
            ConnManagerAction::None => {}
            ConnManagerAction::Connect { alias, conn_type, url, params, inherited_id } => {
                let result = conn_mgr.add_connection(
                    inherited_id, alias.clone(), conn_type, url, params,
                ).await;
                refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                match result {
                    Ok(()) => {
                        state.conn_manager = None;
                        state.mode = Mode::Info(format!("Connected '{}'", alias));
                    }
                    Err(_) => {
                        let ids = saved_ids(state);
                        if let Some(ref mut w) = state.conn_manager {
                            w.connections = conn_mgr.connection_summaries(&ids);
                            w.saved_connections = state.saved_connections.clone();
                            w.view = app::connection_manager::widget::ConnManagerView::Tabs;
                            w.tab = app::connection_manager::widget::ConnManagerTab::Connections;
                            w.cursor = conn_mgr.connections.len().saturating_sub(1);
                            w.focus.input = crate::ui::model::keys::InputFocus::None;
                        }
                    }
                }
            }
            ConnManagerAction::ToggleConnection(idx) => {
                if idx < conn_mgr.connections.len() {
                    if conn_mgr.connections[idx].is_connected() {
                        conn_mgr.disconnect(idx);
                    } else {
                        let _ = conn_mgr.reconnect(idx).await;
                    }
                    refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                    let ids = saved_ids(state);
                    if let Some(ref mut w) = state.conn_manager {
                        w.connections = conn_mgr.connection_summaries(&ids);
                    }
                }
            }
            ConnManagerAction::RemoveConnection(idx) => {
                if idx < conn_mgr.connections.len() {
                    conn_mgr.remove_connection(idx);
                    refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                    let ids = saved_ids(state);
                    if let Some(ref mut w) = state.conn_manager {
                        w.connections = conn_mgr.connection_summaries(&ids);
                        w.cursor = w.cursor.min(w.connections.len().saturating_sub(1));
                    }
                }
            }
            ConnManagerAction::RemoveSaved(id) => {
                if let Ok((_path, updated)) = config::remove_saved_connection(&id, &state.saved_connections) {
                    state.saved_connections = updated;
                } else {
                    state.saved_connections.retain(|s| s.id != id);
                }
                let ids = saved_ids(state);
                if let Some(ref mut w) = state.conn_manager {
                    w.connections = conn_mgr.connection_summaries(&ids);
                    w.saved_connections = state.saved_connections.clone();
                    w.cursor = w.cursor.min(w.saved_connections.len().saturating_sub(1));
                }
            }
            ConnManagerAction::SaveConnection { conn_index, needs_password_confirm } => {
                if conn_index < conn_mgr.connections.len() {
                    let conn = &conn_mgr.connections[conn_index];
                    if needs_password_confirm {
                        let msg = format!(
                            "Connection '{}' has a password. Save password to config file? (y/n)",
                            conn.alias
                        );
                        state.conn_manager = None;
                        state.mode = Mode::Confirm {
                            message: msg,
                            tag: ConfirmAction::SaveConnectionWithPassword { conn_index },
                        };
                    } else {
                        match config::save_connection(conn, &state.saved_connections, false) {
                            Ok((path, updated)) => {
                                let info = format!("Connection '{}' saved to {}", conn.alias, path.display());
                                state.saved_connections = updated;
                                state.conn_manager = None;
                                state.mode = Mode::Info(info);
                            }
                            Err(e) => {
                                state.conn_manager = None;
                                state.mode = Mode::Error(format!("Save failed: {}", e));
                            }
                        }
                    }
                }
            }
        }
        if state.conn_manager.as_ref().map_or(false, |w| w.closed) {
            state.conn_manager = None;
        }
        if state.conn_manager.is_some() {
            return Ok(true);
        }
    }

    // Virtual FK manager overlay has exclusive key handling while open.
    if state.vfk_manager.is_some() {
        if let Some(ref mut widget) = state.vfk_manager {
            if let Some(event) = from_key_event(key, &widget.focus_loci()) {
                dispatch(widget, event);
            }
        }
        let action = state.vfk_manager.as_mut()
            .map(|w| std::mem::replace(&mut w.action, VfkAction::None))
            .unwrap_or(VfkAction::None);
        match action {
            VfkAction::None => {}
            VfkAction::QueryTypeOptions { table, column } => {
                let options = query_type_options(conn_mgr, &table, &column).await;
                if let Some(ref mut w) = state.vfk_manager {
                    if let Some(ref mut form) = w.form {
                        form.type_options = options;
                    }
                }
            }
            VfkAction::AddToEngine(vfk) => {
                state.virtual_fks.push(vfk.clone());
                engine.schema.virtual_fks.push(vfk);
                if let Some(ref mut w) = state.vfk_manager {
                    w.virtual_fks = state.virtual_fks.clone();
                    w.cursor = w.virtual_fks.len().saturating_sub(1);
                }
            }
            VfkAction::RemoveFromEngine(idx) => {
                if idx < state.virtual_fks.len() {
                    let removed = state.virtual_fks.remove(idx);
                    engine.schema.virtual_fks.retain(|v| v != &removed);
                }
                if let Some(ref mut w) = state.vfk_manager {
                    w.virtual_fks = state.virtual_fks.clone();
                }
            }
            VfkAction::SaveToConfig => {
                match config::save_virtual_fks(&state.virtual_fks) {
                    Ok(path) => {
                        state.vfk_manager = None;
                        state.mode = Mode::Info(format!("Virtual FKs saved to {}", path.display()));
                    }
                    Err(e) => {
                        state.vfk_manager = None;
                        state.mode = Mode::Error(format!("Save failed: {}", e));
                    }
                }
            }
        }
        if state.vfk_manager.as_ref().map_or(false, |w| w.closed) {
            state.vfk_manager = None;
        }
        if state.vfk_manager.is_some() {
            return Ok(true);
        }
    }

    // The ConnectionManager implements Database, so we can use it as &dyn Database.
    // Placed after overlay handlers because those need &mut conn_mgr.
    let db: &dyn db::Database = conn_mgr;

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
                            state.vfk_manager = Some(VfkWidget::new(
                                state.virtual_fks.clone(),
                                state.display_table_names.clone(),
                                state.table_columns.clone(),
                            ));
                        }
                        Some("rules") => {
                            if !engine.rules.is_empty() {
                                state.rules_reorder = Some(
                                    app::query_rules_manager::widget::RulesWidget::new(
                                        engine.rules.clone(),
                                        state.next_rule_cursor,
                                    )
                                );
                            }
                        }
                        Some("connections") => {
                            let summaries = conn_mgr.connection_summaries(&saved_ids(state));
                            state.conn_manager = Some(ConnManagerWidget::new(
                                summaries,
                                state.saved_connections.clone(),
                            ));
                        }
                        Some("logs") => {
                            state.mode = Mode::LogViewer { cursor: state.logs.len().saturating_sub(1) };
                        }
                        Some("manuals") => {
                            state.manuals = Some(app::manuals_manager::widget::ManualsWidget::new());
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
