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
use engine::Engine;
use ui::tree_view::{flatten_tree, toggle_fold};
use ratatui::{Terminal, backend::CrosstermBackend};
use rules::Completion;

use std::io;
use connection_manager::{ConnectionManager, ConnectionType};
use ui::app::{AppState, ColumnManagerItem, ConfirmAction, ConnectionForm, ConnectionManagerTab, Mode, VirtualFkField, VirtualFkForm};
use ui::select_list::SelectList;

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
    if state.column_add.is_some() {
        let (filtered, flen, search_active) = if let Some((_, ref items, ref list)) = state.column_add {
            let q = list.search_query().to_lowercase();
            let f: Vec<usize> = items.iter().enumerate()
                .filter(|(_, it)| q.is_empty() || it.name.to_lowercase().contains(&q))
                .map(|(i, _)| i)
                .collect();
            let len = f.len();
            let sa = list.search_active();
            (f, len, sa)
        } else { (vec![], 0, false) };

        match key.code {
            KeyCode::Up => {
                if let Some((_, _, ref mut list)) = state.column_add { list.move_up(); }
            }
            KeyCode::Char('k') if !search_active => {
                if let Some((_, _, ref mut list)) = state.column_add { list.move_up(); }
            }
            KeyCode::Down => {
                if let Some((_, _, ref mut list)) = state.column_add { list.move_down(flen); }
            }
            KeyCode::Char('j') if !search_active => {
                if let Some((_, _, ref mut list)) = state.column_add { list.move_down(flen); }
            }
            KeyCode::Char('u') if !search_active => {
                if let Some((_, ref mut items, ref mut list)) = state.column_add {
                    if let Some((a, b)) = list.move_item_up() {
                        items.swap(a, b);
                    }
                }
            }
            KeyCode::Char('d') if !search_active => {
                if let Some((_, ref mut items, ref mut list)) = state.column_add {
                    let len = items.len();
                    if let Some((a, b)) = list.move_item_down(len) {
                        items.swap(a, b);
                    }
                }
            }
            KeyCode::Char(' ') | KeyCode::Char('x') => {
                if let Some((_, ref mut items, ref list)) = state.column_add {
                    if let Some(&orig_idx) = filtered.get(list.cursor) {
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
                state.column_add = None;
            }
            KeyCode::Char('/') if !search_active => {
                if let Some((_, _, ref mut list)) = state.column_add { list.activate_search(); }
            }
            KeyCode::Esc => {
                let close = if let Some((_, _, ref mut list)) = state.column_add {
                    list.handle_esc() == ui::select_list::EscAction::Close
                } else { true };
                if close {
                    state.column_add = None;
                }
            }
            KeyCode::Backspace if search_active => {
                if let Some((_, _, ref mut list)) = state.column_add { list.search_pop(); }
            }
            KeyCode::Char(c) if search_active => {
                if let Some((_, _, ref mut list)) = state.column_add { list.search_push(c); }
            }
            _ => {}
        }
        return Ok(true);
    }

    // ── Normal mode ──────────────────────────────────────────────────
    // Extracted before the mode match so we can call &mut self methods
    // on state (select_up, input_char, etc.) without conflicting borrows.
    if matches!(state.mode, Mode::Normal) {
        match key.code {
            KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(false),
            KeyCode::Char('/') => {
                state.mode = Mode::Command;
                state.clear_input();
                state.history_cursor = None;
                state.history_draft = String::new();
            }
            KeyCode::Char('j') | KeyCode::Down => state.select_down(),
            KeyCode::Char('k') | KeyCode::Up => state.select_up(),
            KeyCode::Char('f') | KeyCode::Enter => {
                let flat = flatten_tree(&engine.roots);
                if state.selected_row < flat.len() {
                    toggle_fold(&mut engine.roots, state.selected_row);
                }
            }
            KeyCode::Char('s') => {
                state.show_schema = !state.show_schema;
            }
            KeyCode::Char('r') if key.modifiers.contains(KeyModifiers::CONTROL) => {
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
                    state.next_rule_cursor =
                        state.next_rule_cursor.min(state.rules.len());
                    state.rule_reorder_undo.clear();
                    state.rule_reorder_redo.clear();
                    state.mode = Mode::RuleReorder { list: SelectList::new() };
                }
            }
            KeyCode::Char('c') => {
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
                        state.column_add = Some((node.table.clone(), items, SelectList::with_search()));
                    }
                }
            }
            KeyCode::Char('v') => {
                state.mode = Mode::VirtualFkManager { list: SelectList::with_search() };
            }
            KeyCode::Char('x') => {
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
            }
            KeyCode::Char('l') => {
                let mut list = SelectList::new();
                list.cursor = state.logs.len().saturating_sub(1);
                state.mode = Mode::LogViewer { list };
            }
            KeyCode::Char('m') => {
                state.mode = Mode::ManualList { list: SelectList::new() };
            }
            KeyCode::Char('+') => {
                state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
                state.mode = Mode::connection_manager(ConnectionManagerTab::Connections);
            }
            _ => {}
        }
        return Ok(true);
    }

    // ── Command mode ─────────────────────────────────────────────────
    if matches!(state.mode, Mode::Command) {
        match key.code {
            KeyCode::Esc => {
                state.mode = Mode::Normal;
                state.clear_input();
                state.history_cursor = None;
                state.history_draft = String::new();
            }
            KeyCode::Enter => {
                let cmd = state.input_text().trim().to_string();
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
            KeyCode::Up => state.history_up(),
            KeyCode::Down => state.history_down(),
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
                        state.history_cursor = None;
                    }
                }
            }
            KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                match c {
                    'c' => return Ok(false),
                    'r' => {
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
                state.history_cursor = None;
            }
            KeyCode::Backspace => {
                if state.input.is_empty() {
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
        return Ok(true);
    }

    match &mut state.mode {
        Mode::Normal | Mode::Command => unreachable!(), // handled above

        // ── Reverse-i-search mode ─────────────────────────────────────────
        Mode::CommandSearch { ref mut query, ref mut match_cursor, ref saved_input } => {
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc => {
                    state.input = saved_input.clone();
                    state.cursor = state.input.len();
                    state.history_cursor = None;
                    new_mode = Some(Mode::Command);
                }
                KeyCode::Enter => {
                    let matched = state
                        .command_history
                        .search_reverse(query, *match_cursor)
                        .and_then(|i| state.command_history.entries().get(i))
                        .map(|e| e.text.clone());
                    if let Some(text) = matched {
                        state.input = text;
                        state.cursor = state.input.len();
                    }
                    state.history_cursor = None;
                    new_mode = Some(Mode::Command);
                }
                KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    match c {
                        'c' => return Ok(false),
                        'r' => { *match_cursor += 1; }
                        _ => {}
                    }
                }
                KeyCode::Char(c) => {
                    query.push(c);
                    *match_cursor = 0;
                }
                KeyCode::Backspace => {
                    query.pop();
                    *match_cursor = 0;
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Path selection overlay ────────────────────────────────────────
        Mode::PathSelection { ref mut list } => {
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc => {
                    new_mode = Some(Mode::Normal);
                    *pending_paths = None;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    list.move_up();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    list.move_down(state.paths.len());
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
                    let cursor = list.cursor;
                    if let Some((rule, paths)) = pending_paths.take() {
                        let chosen = &paths[cursor];
                        engine.apply_relation_rule(db, chosen).await?;
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
                    new_mode = Some(Mode::Normal);
                    state.paths.clear();
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Rule reorder overlay ─────────────────────────────────────────
        Mode::RuleReorder { ref mut list } => {
            let mut new_mode: Option<Mode> = None;
            // Inline undo-push: captures disjoint fields directly.
            macro_rules! push_undo {
                ($cursor:expr) => {
                    state.rule_reorder_undo.push((
                        state.rules.clone(), $cursor, state.next_rule_cursor,
                    ));
                    state.rule_reorder_redo.clear();
                };
            }
            match key.code {
                KeyCode::Esc => {
                    state.rule_reorder_undo.clear();
                    state.rule_reorder_redo.clear();
                    new_mode = Some(Mode::Normal);
                }
                KeyCode::Enter => {
                    engine.rules = state.rules.clone();
                    state.next_rule_cursor =
                        state.next_rule_cursor.min(engine.rules.len());
                    let _ = engine.reexecute_all(db).await;
                    state.rule_reorder_undo.clear();
                    state.rule_reorder_redo.clear();
                    new_mode = Some(Mode::Normal);
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    list.move_up();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    list.move_down(state.rules.len());
                }
                KeyCode::Char('u') => {
                    if let Some((a, b)) = list.move_item_up() {
                        push_undo!(a + 1);
                        state.rules.swap(a, b);
                    }
                }
                KeyCode::Char('d') => {
                    let len = state.rules.len();
                    if let Some((a, b)) = list.move_item_down(len) {
                        push_undo!(a);
                        state.rules.swap(a, b);
                    }
                }
                KeyCode::Char('x') => {
                    if !state.rules.is_empty() {
                        push_undo!(list.cursor);
                        state.rules.remove(list.cursor);
                        if state.rules.is_empty() {
                            list.cursor = 0;
                            state.next_rule_cursor = 0;
                        } else {
                            list.clamp_cursor(state.rules.len());
                        }
                        state.next_rule_cursor = state.next_rule_cursor.min(state.rules.len());
                    }
                }
                KeyCode::Char('i') => {
                    state.next_rule_cursor = list.cursor.min(state.rules.len());
                }
                KeyCode::Char('o') => {
                    state.next_rule_cursor = (list.cursor + 1).min(state.rules.len());
                }
                KeyCode::Char('z') => {
                    if let Some((rules, cursor, next_cursor)) = state.rule_reorder_undo.pop() {
                        state.rule_reorder_redo.push((
                            state.rules.clone(), list.cursor, state.next_rule_cursor,
                        ));
                        state.rules = rules;
                        list.cursor = cursor.min(state.rules.len().saturating_sub(1));
                        state.next_rule_cursor = next_cursor.min(state.rules.len());
                    }
                }
                KeyCode::Char('y') => {
                    if let Some((rules, cursor, next_cursor)) = state.rule_reorder_redo.pop() {
                        state.rule_reorder_undo.push((
                            state.rules.clone(), list.cursor, state.next_rule_cursor,
                        ));
                        state.rules = rules;
                        list.cursor = cursor.min(state.rules.len().saturating_sub(1));
                        state.next_rule_cursor = next_cursor.min(state.rules.len());
                    }
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Error / Info overlays — any key dismisses ────────────────────
        Mode::Error(_) | Mode::Info(_) => {
            state.mode = Mode::Normal;
        }

        // ── Log viewer ───────────────────────────────────────────────────
        Mode::LogViewer { ref mut list } => {
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('l') => {
                    new_mode = Some(Mode::Normal);
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    list.move_up();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    list.move_down(state.logs.len());
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Virtual FK manager ───────────────────────────────────────────
        Mode::VirtualFkManager { ref mut list } => {
            let q = list.search_query().to_lowercase();
            let filtered: Vec<usize> = state.virtual_fks.iter().enumerate()
                .filter(|(_, vfk)| q.is_empty() || vfk.from_table.to_lowercase().contains(&q) || vfk.to_table.to_lowercase().contains(&q) || vfk.type_value.as_deref().unwrap_or("").to_lowercase().contains(&q))
                .map(|(i, _)| i)
                .collect();
            let flen = filtered.len();
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Up => { list.move_up(); }
                KeyCode::Char('k') if !list.search_active() => { list.move_up(); }
                KeyCode::Down => { list.move_down(flen); }
                KeyCode::Char('j') if !list.search_active() => { list.move_down(flen); }
                KeyCode::Char('a') if !list.search_active() => {
                    new_mode = Some(Mode::VirtualFkAdd(VirtualFkForm::new()));
                }
                KeyCode::Char('d') | KeyCode::Char('x') if !list.search_active() => {
                    if let Some(&orig_idx) = filtered.get(list.cursor) {
                        let removed = state.virtual_fks.remove(orig_idx);
                        engine.schema.virtual_fks.retain(|v| v != &removed);
                        list.clamp_cursor(filtered.len().saturating_sub(1));
                    }
                }
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    match config::save_virtual_fks(&state.virtual_fks) {
                        Ok(path) => { new_mode = Some(Mode::Info(format!("Virtual FKs saved to {}", path.display()))); }
                        Err(e) => { new_mode = Some(Mode::Error(format!("Save failed: {}", e))); }
                    }
                }
                KeyCode::Char('/') if !list.search_active() => {
                    list.activate_search();
                }
                KeyCode::Esc => {
                    if list.handle_esc() == ui::select_list::EscAction::Close {
                        new_mode = Some(Mode::Normal);
                    }
                }
                KeyCode::Backspace if list.search_active() => { list.search_pop(); }
                KeyCode::Char(c) if list.search_active() => { list.search_push(c); }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Virtual FK creation form ────────────────────────────────────
        Mode::VirtualFkAdd(ref mut form) => {
            // Clone form data for read-only use (dropdown items, filter count).
            // Mutations go through `form` directly since it's &mut.
            let form_snapshot = form.clone();

            let dropdown_items: Vec<String> = match &form_snapshot.active_field {
                VirtualFkField::FromTable | VirtualFkField::ToTable => state.display_table_names.clone(),
                VirtualFkField::IdColumn => {
                    state.table_columns.get(&form_snapshot.from_table).cloned().unwrap_or_default()
                }
                VirtualFkField::TypeColumn => {
                    let mut cols = vec!["(none — simple FK)".to_string()];
                    cols.extend(state.table_columns.get(&form_snapshot.from_table).cloned().unwrap_or_default());
                    cols
                }
                VirtualFkField::TypeValue => {
                    if form_snapshot.type_column.is_empty() {
                        vec!["(no type_column set — skipping)".to_string()]
                    } else {
                        form_snapshot.type_options.iter().map(|(v, c)| format!("{}  ({})", v, c)).collect()
                    }
                }
                VirtualFkField::ToColumn => {
                    state.table_columns.get(&form_snapshot.to_table).cloned().unwrap_or_default()
                }
            };

            let flen = {
                let q = form_snapshot.list.search_query().to_lowercase();
                dropdown_items.iter()
                    .filter(|s| q.is_empty() || s.to_lowercase().contains(&q))
                    .count()
            };
            let search_active = form_snapshot.list.search_active();

            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Tab => {
                    let next = form.active_field.next(form.type_column.is_empty());
                    form.active_field = next;
                    form.list.cursor = 0;
                    form.list.reset_search();
                    if form.active_field == VirtualFkField::ToColumn {
                        let to_cols = state.table_columns.get(&form.to_table).cloned().unwrap_or_default();
                        form.list.cursor = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                    }
                    if form.active_field == VirtualFkField::TypeValue && !form.type_column.is_empty() {
                        let tc = form.type_column.clone();
                        let ft = form.from_table.clone();
                        form.type_options = query_type_options(db, &ft, &tc).await;
                    }
                }
                KeyCode::BackTab => {
                    let prev = form.active_field.prev(form.type_column.is_empty());
                    form.active_field = prev;
                    form.list.cursor = 0;
                    form.list.reset_search();
                    if form.active_field == VirtualFkField::TypeValue && !form.type_column.is_empty() {
                        let tc = form.type_column.clone();
                        let ft = form.from_table.clone();
                        form.type_options = query_type_options(db, &ft, &tc).await;
                    }
                }
                KeyCode::Up => { form.list.move_up(); }
                KeyCode::Char('k') if !search_active => { form.list.move_up(); }
                KeyCode::Down => { form.list.move_down(flen); }
                KeyCode::Char('j') if !search_active => { form.list.move_down(flen); }
                KeyCode::Char('/') if !search_active => { form.list.activate_search(); }
                KeyCode::Esc => {
                    if form.list.handle_esc() == ui::select_list::EscAction::Close {
                        new_mode = Some(Mode::VirtualFkManager { list: SelectList::with_search() });
                    }
                }
                KeyCode::Enter => {
                    let fi = {
                        let q = form_snapshot.list.search_query().to_lowercase();
                        dropdown_items.iter().enumerate()
                            .filter(|(_, s)| q.is_empty() || s.to_lowercase().contains(&q))
                            .map(|(i, _)| i)
                            .collect::<Vec<_>>()
                    };
                    if let Some(&orig) = fi.get(form_snapshot.list.cursor) {
                        if let Some(raw_value) = dropdown_items.get(orig) {
                            let raw_value = raw_value.clone();
                            form.list.reset_search();
                            form.list.cursor = 0;
                            match &form.active_field.clone() {
                                VirtualFkField::FromTable => {
                                    form.from_table = raw_value;
                                    form.id_column.clear();
                                    form.type_column.clear();
                                    form.type_value.clear();
                                    form.active_field = VirtualFkField::IdColumn;
                                }
                                VirtualFkField::IdColumn => {
                                    form.id_column = raw_value;
                                    form.active_field = VirtualFkField::TypeColumn;
                                }
                                VirtualFkField::TypeColumn => {
                                    if raw_value.starts_with("(none") {
                                        form.type_column.clear();
                                        form.type_value.clear();
                                        form.active_field = VirtualFkField::ToTable;
                                    } else {
                                        form.type_column = raw_value;
                                        form.active_field = VirtualFkField::TypeValue;
                                    }
                                }
                                VirtualFkField::TypeValue => {
                                    if !raw_value.starts_with("(no type_column") {
                                        let tv = raw_value
                                            .split("  (")
                                            .next()
                                            .unwrap_or(&raw_value)
                                            .to_string();
                                        form.type_value = tv;
                                    }
                                    form.active_field = VirtualFkField::ToTable;
                                }
                                VirtualFkField::ToTable => {
                                    form.to_table = raw_value;
                                    form.to_column.clear();
                                    let to_cols = state.table_columns
                                        .get(&form.to_table).cloned().unwrap_or_default();
                                    form.list.cursor = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                                    form.active_field = VirtualFkField::ToColumn;
                                }
                                VirtualFkField::ToColumn => {
                                    form.to_column = raw_value;
                                    if form.is_complete() {
                                        let vfk = form.to_vfk_def();
                                        state.virtual_fks.push(vfk.clone());
                                        engine.schema.virtual_fks.push(vfk);
                                        let mut mgr_list = SelectList::with_search();
                                        mgr_list.cursor = state.virtual_fks.len().saturating_sub(1);
                                        new_mode = Some(Mode::VirtualFkManager { list: mgr_list });
                                    }
                                }
                            }
                            // Load type options when switching to TypeValue
                            if form.active_field == VirtualFkField::TypeValue
                                && !form.type_column.is_empty()
                                && new_mode.is_none()
                            {
                                let tc = form.type_column.clone();
                                let ft = form.from_table.clone();
                                form.type_options = query_type_options(db, &ft, &tc).await;
                            }
                        }
                    }
                }
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    if form_snapshot.is_complete() {
                        let vfk = form.to_vfk_def();
                        state.virtual_fks.push(vfk.clone());
                        engine.schema.virtual_fks.push(vfk);
                        match config::save_virtual_fks(&state.virtual_fks) {
                            Ok(path) => {
                                new_mode = Some(Mode::Info(format!("Virtual FK saved to {}", path.display())));
                            }
                            Err(e) => {
                                new_mode = Some(Mode::Error(format!("Save failed: {}", e)));
                            }
                        }
                    }
                }
                KeyCode::Backspace if search_active => { form.list.search_pop(); }
                KeyCode::Char(c) if search_active => { form.list.search_push(c); }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Confirm dialog ──────────────────────────────────────────────
        Mode::Confirm { ref tag, .. } => {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Char('n') | KeyCode::Char('N') => {
                    let save_pw = matches!(key.code, KeyCode::Char('y') | KeyCode::Char('Y'));
                    match tag {
                        ConfirmAction::SaveConnectionWithPassword { conn_index } => {
                            if *conn_index < conn_mgr.connections.len() {
                                let conn = &conn_mgr.connections[*conn_index];
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
        Mode::ConnectionManager { ref mut tab, ref mut connections_list, ref mut saved_list, ref mut connectors_list } => {
            let list = match tab {
                ConnectionManagerTab::Connections => &mut *connections_list,
                ConnectionManagerTab::Saved => &mut *saved_list,
                ConnectionManagerTab::Connectors => &mut *connectors_list,
            };
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc => {
                    new_mode = Some(Mode::Normal);
                }
                KeyCode::Right | KeyCode::Tab => {
                    *tab = match *tab {
                        ConnectionManagerTab::Connections => ConnectionManagerTab::Saved,
                        ConnectionManagerTab::Saved => ConnectionManagerTab::Connectors,
                        ConnectionManagerTab::Connectors => ConnectionManagerTab::Connections,
                    };
                }
                KeyCode::Left | KeyCode::BackTab => {
                    *tab = match *tab {
                        ConnectionManagerTab::Connections => ConnectionManagerTab::Connectors,
                        ConnectionManagerTab::Saved => ConnectionManagerTab::Connections,
                        ConnectionManagerTab::Connectors => ConnectionManagerTab::Saved,
                    };
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    list.move_up();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    let len = match *tab {
                        ConnectionManagerTab::Connections => state.connections_summary.len(),
                        ConnectionManagerTab::Saved => state.saved_connections.len(),
                        ConnectionManagerTab::Connectors => ConnectionType::all().len(),
                    };
                    list.move_down(len);
                }
                KeyCode::Enter => {
                    let cursor = list.cursor;
                    match *tab {
                        ConnectionManagerTab::Connectors => {
                            let types = ConnectionType::all();
                            if cursor < types.len() {
                                let ct = types[cursor].clone();
                                new_mode = Some(Mode::ConnectionAdd(ConnectionForm::new(ct)));
                            }
                        }
                        ConnectionManagerTab::Saved => {
                            if cursor < state.saved_connections.len() {
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
                                let _ = conn_type;
                                new_mode = Some(Mode::SavedConnectionAlias {
                                    saved_index: cursor,
                                    alias: suggested,
                                });
                            }
                        }
                        ConnectionManagerTab::Connections => {
                            if cursor < conn_mgr.connections.len() {
                                if conn_mgr.connections[cursor].is_connected() {
                                    conn_mgr.disconnect(cursor);
                                } else {
                                    let _ = conn_mgr.reconnect(cursor).await;
                                }
                                refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                            }
                        }
                    }
                }
                KeyCode::Char('d') | KeyCode::Char('x') => {
                    let cursor = list.cursor;
                    if *tab == ConnectionManagerTab::Saved && cursor < state.saved_connections.len() {
                        let removed_id = state.saved_connections[cursor].id.clone();
                        if let Ok((_path, updated)) = config::remove_saved_connection(&removed_id, &state.saved_connections) {
                            state.saved_connections = updated;
                        } else {
                            state.saved_connections.remove(cursor);
                        }
                        list.clamp_cursor(state.saved_connections.len());
                        // Inline saved_ids to avoid borrowing all of `state`.
                        let sids: std::collections::HashSet<String> =
                            state.saved_connections.iter().map(|s| s.id.clone()).collect();
                        state.connections_summary = conn_mgr.connection_summaries(&sids);
                    } else if *tab == ConnectionManagerTab::Connections && cursor < conn_mgr.connections.len() {
                        conn_mgr.remove_connection(cursor);
                        list.clamp_cursor(conn_mgr.connections.len());
                        // Inline refresh to avoid borrowing all of `state`.
                        let mut schema = conn_mgr.merged_schema().clone();
                        for vfk in &state.virtual_fks {
                            schema.virtual_fks.push(vfk.clone());
                        }
                        engine.schema = schema;
                        state.table_names = engine.schema.table_names();
                        state.table_columns = engine.schema.tables.iter().map(|(name, info)| {
                            let cols = info.columns.iter().map(|c| c.name.clone()).collect();
                            (name.clone(), cols)
                        }).collect();
                        let sids: std::collections::HashSet<String> =
                            state.saved_connections.iter().map(|s| s.id.clone()).collect();
                        state.connections_summary = conn_mgr.connection_summaries(&sids);
                        state.display_table_names = conn_mgr.display_table_names();
                        state.display_name_map = conn_mgr.display_name_map();
                    }
                }
                KeyCode::Char('s') if key.modifiers.contains(crossterm::event::KeyModifiers::CONTROL) => {
                    let cursor = list.cursor;
                    if *tab == ConnectionManagerTab::Connections && cursor < conn_mgr.connections.len() {
                        let conn = &conn_mgr.connections[cursor];
                        if conn.has_password() {
                            new_mode = Some(Mode::Confirm {
                                message: format!(
                                    "Connection '{}' has a password. Save password to config file? (y/n)",
                                    conn.alias
                                ),
                                tag: ConfirmAction::SaveConnectionWithPassword { conn_index: cursor },
                            });
                        } else {
                            match config::save_connection(conn, &state.saved_connections, false) {
                                Ok((path, updated)) => {
                                    state.saved_connections = updated;
                                    state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
                                    new_mode = Some(Mode::Info(format!("Connection '{}' saved to {}", conn.alias, path.display())));
                                }
                                Err(e) => {
                                    new_mode = Some(Mode::Error(format!("Save failed: {}", e)));
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Saved connection alias prompt ────────────────────────────────
        Mode::SavedConnectionAlias { saved_index, ref mut alias } => {
            let saved_index = *saved_index;
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc => {
                    let mut mode = Mode::connection_manager(ConnectionManagerTab::Saved);
                    if let Mode::ConnectionManager { ref mut saved_list, .. } = mode {
                        saved_list.cursor = saved_index;
                    }
                    new_mode = Some(mode);
                }
                KeyCode::Enter => {
                    if !alias.is_empty() {
                        if let Some(saved) = state.saved_connections.get(saved_index) {
                            let inherited_id = saved.id.clone();
                            let conn_type = match saved.conn_type.as_str() {
                                "sqlite" => ConnectionType::Sqlite,
                                _ => ConnectionType::Mysql,
                            };
                            let alias_str = alias.clone();
                            let params = saved.params.clone();
                            match conn_type.build_url(&params) {
                                Ok(url) => {
                                    let result = conn_mgr.add_connection(
                                        Some(inherited_id), alias_str.clone(), conn_type, url, params,
                                    ).await;
                                    refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                                    match result {
                                        Ok(()) => {
                                            new_mode = Some(Mode::Info(format!(
                                                "Connected '{}'",
                                                alias_str,
                                            )));
                                        }
                                        Err(_) => {
                                            let conn_idx = conn_mgr.connections.len().saturating_sub(1);
                                            let mut mode = Mode::connection_manager(ConnectionManagerTab::Connections);
                                            if let Mode::ConnectionManager { ref mut connections_list, .. } = mode {
                                                connections_list.cursor = conn_idx;
                                            }
                                            new_mode = Some(mode);
                                        }
                                    }
                                }
                                Err(e) => {
                                    new_mode = Some(Mode::Error(format!("Invalid params: {}", e)));
                                }
                            }
                        }
                    }
                }
                KeyCode::Backspace => { alias.pop(); }
                KeyCode::Char(c) => { alias.push(c); }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Connection add form ─────────────────────────────────────────
        Mode::ConnectionAdd(ref mut form) => {
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc => {
                    new_mode = Some(Mode::connection_manager(ConnectionManagerTab::Connectors));
                }
                KeyCode::Tab => {
                    form.active_field = (form.active_field + 1) % form.fields.len();
                }
                KeyCode::BackTab => {
                    if form.active_field == 0 {
                        form.active_field = form.fields.len() - 1;
                    } else {
                        form.active_field -= 1;
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
                                new_mode = Some(Mode::Error(format!("Invalid params: {}", e)));
                            }
                            Ok(url) => {
                                let result = conn_mgr.add_connection(None, alias.clone(), conn_type, url, params).await;
                                refresh_schema_from_conn_mgr(state, engine, conn_mgr);
                                let conn_idx = conn_mgr.connections.len().saturating_sub(1);
                                match result {
                                    Ok(()) => {
                                        new_mode = Some(Mode::Info(format!(
                                            "Connected '{}' ({} tables)",
                                            alias,
                                            conn_mgr
                                                .connections
                                                .last()
                                                .map(|c| c.original_tables.len())
                                                .unwrap_or(0)
                                        )));
                                    }
                                    Err(_) => {
                                        let mut mode = Mode::connection_manager(ConnectionManagerTab::Connections);
                                        if let Mode::ConnectionManager { ref mut connections_list, .. } = mode {
                                            connections_list.cursor = conn_idx;
                                        }
                                        new_mode = Some(mode);
                                    }
                                }
                            }
                        }
                    }
                }
                KeyCode::Backspace => {
                    form.fields[form.active_field].value.pop();
                }
                KeyCode::Char(c) => {
                    form.fields[form.active_field].value.push(c);
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Manual list ──────────────────────────────────────────────────
        Mode::ManualList { ref mut list } => {
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('m') => {
                    new_mode = Some(Mode::Normal);
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    list.move_up();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    list.move_down(ui::render::MANUALS.len());
                }
                KeyCode::Enter => {
                    new_mode = Some(Mode::ManualView { index: list.cursor, scroll: 0 });
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
        }

        // ── Manual viewer ────────────────────────────────────────────────
        Mode::ManualView { ref mut index, ref mut scroll } => {
            let idx = *index;
            let line_count = ui::render::manual_line_count(idx);
            let mut new_mode: Option<Mode> = None;
            match key.code {
                KeyCode::Esc | KeyCode::Char('q') => {
                    let mut list = SelectList::new();
                    list.cursor = idx;
                    new_mode = Some(Mode::ManualList { list });
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if *scroll > 0 { *scroll -= 1; }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if *scroll + 1 < line_count { *scroll += 1; }
                }
                _ => {}
            }
            if let Some(m) = new_mode { state.mode = m; }
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
                    state.mode = Mode::PathSelection { list: SelectList::new() };
                    *pending_paths = Some((rule, result.paths));
                }
            }
        }
    }
    Ok(())
}


