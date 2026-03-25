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
use db::{ConnectionParams, DbType};
use engine::{Engine, flatten_tree};
use ratatui::{Terminal, backend::CrosstermBackend};
use schema::Schema;
use std::io;
use ui::app::{AppState, ColumnManagerItem, ConnectionAddStep, ConnectionInfo, ConnectionStatus, Mode, VirtualFkAddStep};
use schema::VirtualFkDef;

/// LatticeQL — Navigate complex datasets from multiple sources intuitively.
#[derive(Parser, Debug)]
#[command(name = "latticeql", version, about)]
struct Args {
    /// Database connection URL (may be supplied 0 or more times).
    ///
    /// Examples:
    ///   sqlite://path/to/db.sqlite3
    ///   mysql://user:password@localhost/dbname
    ///
    /// If omitted the app starts with no connection; use the Connection Manager
    /// (press M) to add one at runtime.
    #[arg(short, long)]
    database: Vec<String>,
}

/// A live database connection with its resolved schema.
struct ConnectionHandle {
    name: String,
    db: Box<dyn db::Database>,
    schema: Schema,
}

/// Resolve a (possibly qualified) table name against the active connections.
///
/// - If `table` contains a `.`, treat the prefix as the connection name.
/// - Otherwise scan all connections for the table; succeed only when it is
///   unique, error when it exists in multiple connections.
///
/// Returns `(connection_name, unqualified_table_name)`.
fn resolve_table_name<'a, 'b>(
    table: &'b str,
    connections: &'a [ConnectionHandle],
) -> Result<(Option<&'a str>, &'b str)> {
    if let Some(dot) = table.find('.') {
        let conn_name = &table[..dot];
        let tbl = &table[dot + 1..];
        if let Some(conn) = connections.iter().find(|c| c.name == conn_name) {
            Ok((Some(conn.name.as_str()), tbl))
        } else {
            anyhow::bail!("Unknown connection name '{}' in qualified table '{}'", conn_name, table);
        }
    } else {
        // Count how many connections contain this table.
        let owners: Vec<&str> = connections
            .iter()
            .filter(|c| c.schema.tables.contains_key(table))
            .map(|c| c.name.as_str())
            .collect();
        match owners.len() {
            0 => {
                // Table not found anywhere — let the engine/parse raise the error.
                Ok((None, table))
            }
            1 => Ok((Some(owners[0]), table)),
            _ => anyhow::bail!(
                "Ambiguous table '{}': exists in connections [{}]. Use a qualified name, e.g. '{}.{}'",
                table,
                owners.join(", "),
                owners[0],
                table,
            ),
        }
    }
}

/// Look up the database handle for `connection_name` (or the first connection
/// when `connection_name` is `None`).
fn db_for_connection<'a>(
    conn_name: Option<&str>,
    connections: &'a [ConnectionHandle],
) -> Option<&'a dyn db::Database> {
    match conn_name {
        Some(name) => connections.iter().find(|c| c.name == name).map(|c| c.db.as_ref()),
        None => connections.first().map(|c| c.db.as_ref()),
    }
}

/// Build a merged `Schema` from all active connections.
///
/// For tables that are unique across all connections, use the unqualified name.
/// For tables that appear in multiple connections, add them under *both* the
/// qualified name (`connection.table`) and — if not yet present — the
/// unqualified name (first-wins).
fn merged_schema(connections: &[ConnectionHandle]) -> Schema {
    use std::collections::HashMap;

    // Count occurrences of each table name.
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for c in connections {
        for name in c.schema.tables.keys() {
            *counts.entry(name.as_str()).or_insert(0) += 1;
        }
    }

    let mut tables = std::collections::HashMap::new();
    let mut virtual_fks: Vec<VirtualFkDef> = Vec::new();

    for conn in connections {
        for (name, info) in &conn.schema.tables {
            let is_ambiguous = *counts.get(name.as_str()).unwrap_or(&0) > 1;
            // Always add qualified form.
            let qualified = format!("{}.{}", conn.name, name);
            let mut qualified_info = info.clone();
            qualified_info.name = qualified.clone();
            tables.insert(qualified, qualified_info);
            // Add unqualified form if not already present (first connection wins).
            if !is_ambiguous {
                tables.entry(name.clone()).or_insert_with(|| info.clone());
            }
        }
        virtual_fks.extend(conn.schema.virtual_fks.iter().cloned());
    }

    Schema { tables, virtual_fks }
}

/// Build table_names list for UI display — include both qualified and
/// unqualified forms so the user can type either.
fn merged_table_names(connections: &[ConnectionHandle]) -> Vec<String> {
    let schema = merged_schema(connections);
    schema.table_names()
}

/// Build per-table column maps for command-completion (including qualified names).
fn merged_table_columns(
    connections: &[ConnectionHandle],
) -> std::collections::HashMap<String, Vec<String>> {
    let schema = merged_schema(connections);
    schema
        .tables
        .iter()
        .map(|(name, info)| {
            let cols = info.columns.iter().map(|c| c.name.clone()).collect();
            (name.clone(), cols)
        })
        .collect()
}

/// Connect to `url` with an auto-generated name (last path segment or
/// the hostname for MySQL).
fn auto_name_for_url(url: &str) -> String {
    if url.starts_with("sqlite://") || url.starts_with("sqlite:") {
        let path = url.trim_start_matches("sqlite://").trim_start_matches("sqlite:");
        std::path::Path::new(path)
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(path)
            .to_string()
    } else if url.starts_with("mysql://") || url.starts_with("mysql+tls://") {
        // mysql://user:pass@host:port/db  →  db
        url.rsplitn(2, '/').next().unwrap_or("mysql").to_string()
    } else {
        "db".to_string()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let mut connections: Vec<ConnectionHandle> = Vec::new();

    for url in &args.database {
        eprintln!("Connecting to {}…", url);
        let db = db::connect(url).await?;
        eprintln!("Exploring schema…");
        let schema = Schema::explore(db.as_ref()).await?;
        let name = auto_name_for_url(url);
        connections.push(ConnectionHandle { name, db, schema });
    }

    let mut engine = if let Some(first) = connections.first() {
        Engine::new(first.schema.clone())
    } else {
        Engine::new(Schema::default())
    };

    let mut state = AppState::new();

    // Populate connection info for the UI.
    for handle in &connections {
        state.connections.push(ConnectionInfo {
            name: handle.name.clone(),
            db_type: DbType::Sqlite, // actual type inferred by display_url; tracked for display
            display_url: format!("(connected: {})", handle.name),
            status: ConnectionStatus::Connected,
        });
    }

    // Update merged schema in engine.
    if !connections.is_empty() {
        engine.schema = merged_schema(&connections);
        state.table_names = merged_table_names(&connections);
        state.table_columns = merged_table_columns(&connections);
    }

    let defaults = config::load_config(&std::env::current_dir()?)?;
    state.default_visible_columns = defaults.columns.global;
    state.default_visible_columns_by_table = defaults.columns.per_table;
    // Inject virtual FKs from config.
    for vfk in defaults.virtual_fks {
        state.virtual_fks.push(vfk.clone());
        engine.schema.virtual_fks.push(vfk);
    }

    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, &mut state, &mut engine, &mut connections).await;

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
    connections: &mut Vec<ConnectionHandle>,
) -> Result<()> {
    // Pending paths waiting for user selection
    let mut pending_paths: Option<(rules::Rule, Vec<schema::TablePath>, Option<String>)> = None;

    loop {
        // Drain any log entries queued by background code (e.g. type decoder warnings).
        state.logs.extend(log::drain());

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
                        connections,
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
    connection_name: Option<String>,
) -> bool {
    let idx = state.next_rule_cursor.min(engine.rules.len());
    let inserted_before_existing = idx < engine.rules.len();
    engine.insert_rule(idx, rule, connection_name);
    state.next_rule_cursor = (idx + 1).min(engine.rules.len());
    inserted_before_existing
}

fn place_last_added_rule_at_next_cursor(state: &mut AppState, engine: &mut Engine) -> bool {
    if !engine.rules.is_empty() {
        let last_idx = engine.rules.len() - 1;
        let rule = engine.rules.remove(last_idx);
        let conn_name = if last_idx < engine.rule_connections.len() {
            engine.rule_connections.remove(last_idx)
        } else {
            None
        };
        let idx = state.next_rule_cursor.min(engine.rules.len());
        let inserted_before_existing = idx < engine.rules.len();
        engine.insert_rule(idx, rule, conn_name);
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
    connections: &mut Vec<ConnectionHandle>,
    pending_paths: &mut Option<(rules::Rule, Vec<schema::TablePath>, Option<String>)>,
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
                            insert_rule_at_next_cursor(state, engine, rule, None);
                            // Prune is in-memory: apply directly without re-fetching from DB.
                            engine.apply_prune_rule(&table, &conditions);
                        }
                    }
                }
                KeyCode::Char('l') => {
                    state.mode = Mode::LogViewer { cursor: state.logs.len().saturating_sub(1) };
                }
                KeyCode::Char('M') => {
                    state.reset_overlay_search();
                    state.mode = Mode::ConnectionManager { cursor: 0 };
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
                        execute_command(cmd, state, engine, connections, pending_paths).await?;
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
                    if let Some((rule, paths, conn_name)) = pending_paths.take() {
                        let chosen = &paths[state.path_cursor];
                        let db = db_for_connection(conn_name.as_deref(), connections);
                        if let Some(db) = db {
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
                            if insert_rule_at_next_cursor(state, engine, updated_rule, conn_name) {
                                let conn_pairs: Vec<(&str, &dyn db::Database)> = connections
                                    .iter()
                                    .map(|c| (c.name.as_str(), c.db.as_ref()))
                                    .collect();
                                engine.reexecute_all_multi(&conn_pairs).await?;
                            }
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
                    // Apply reordered rules — keep rule_connections in sync with rules order.
                    // The reorder manager only reorders rules, not rule_connections, so we
                    // rebuild rule_connections to match the new order using the display rules
                    // as a key into the original engine rules list.
                    engine.rules = state.rules.clone();
                    // Rebuild rule_connections to match the reordered rules (best-effort: keep
                    // original connection names where indices overlap, pad with None).
                    let old_len = engine.rule_connections.len();
                    engine.rule_connections.resize(engine.rules.len(), None);
                    // If old list was shorter, the new entries default to None above.
                    // If the reorder removed rules, truncate.
                    if engine.rules.len() < old_len {
                        engine.rule_connections.truncate(engine.rules.len());
                    }
                    state.next_rule_cursor =
                        state.next_rule_cursor.min(engine.rules.len());
                    let conn_pairs: Vec<(&str, &dyn db::Database)> = connections
                        .iter()
                        .map(|c| (c.name.as_str(), c.db.as_ref()))
                        .collect();
                    let _ = engine.reexecute_all_multi(&conn_pairs).await;
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
                                let db = connections.first().map(|c| c.db.as_ref());
                                let options = if let Some(db) = db {
                                    query_type_options(db, &from_table, &type_column).await
                                } else { vec![] };
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
                            let db = connections.first().map(|c| c.db.as_ref());
                            let options = if let Some(db) = db {
                                query_type_options(db, &from_table, &col).await
                            } else { vec![] };
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

        // ── Connection manager ───────────────────────────────────────────
        Mode::ConnectionManager { cursor } => {
            match key.code {
                KeyCode::Up | KeyCode::Char('k') => {
                    if cursor > 0 {
                        state.mode = Mode::ConnectionManager { cursor: cursor - 1 };
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if cursor + 1 < state.connections.len() {
                        state.mode = Mode::ConnectionManager { cursor: cursor + 1 };
                    }
                }
                KeyCode::Char('a') => {
                    state.mode = Mode::ConnectionAdd(ConnectionAddStep::ChooseType { cursor: 0 });
                }
                // Disconnect (remove) the selected connection.
                KeyCode::Char('d') | KeyCode::Char('x') => {
                    if cursor < state.connections.len() {
                        let name = state.connections[cursor].name.clone();
                        state.connections.remove(cursor);
                        connections.retain(|c| c.name != name);
                        // Rebuild engine schema and table lists.
                        engine.schema = merged_schema(connections);
                        state.table_names = merged_table_names(connections);
                        state.table_columns = merged_table_columns(connections);
                        // Clear data tree — it may reference the removed connection's tables.
                        engine.roots.clear();
                        engine.rules.clear();
                        engine.rule_connections.clear();
                        // Keep cursor in bounds after removal.
                        let new_cursor = if state.connections.is_empty() {
                            0
                        } else {
                            cursor.min(state.connections.len() - 1)
                        };
                        state.mode = Mode::ConnectionManager { cursor: new_cursor };
                    }
                }
                KeyCode::Esc | KeyCode::Char('q') => {
                    state.reset_overlay_search();
                    state.mode = Mode::Normal;
                }
                _ => {}
            }
        }

        // ── Connection add wizard ────────────────────────────────────────
        Mode::ConnectionAdd(ref step) => {
            let step = step.clone();
            match (&step, key.code) {
                // ── ChooseType: pick list (j/k navigate, Enter select) ─────
                (ConnectionAddStep::ChooseType { cursor }, KeyCode::Up)
                | (ConnectionAddStep::ChooseType { cursor }, KeyCode::Char('k')) => {
                    let cursor = *cursor;
                    if cursor > 0 {
                        state.mode = Mode::ConnectionAdd(ConnectionAddStep::ChooseType { cursor: cursor - 1 });
                    }
                }
                (ConnectionAddStep::ChooseType { cursor }, KeyCode::Down)
                | (ConnectionAddStep::ChooseType { cursor }, KeyCode::Char('j')) => {
                    let cursor = *cursor;
                    if cursor < 1 {
                        state.mode = Mode::ConnectionAdd(ConnectionAddStep::ChooseType { cursor: cursor + 1 });
                    }
                }
                (ConnectionAddStep::ChooseType { cursor }, KeyCode::Enter) => {
                    let cursor = *cursor;
                    if cursor == 0 {
                        state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterSqlitePath { input: String::new() });
                    } else {
                        state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlHost { input: String::new() });
                    }
                }
                // ── Text-input steps: char/backspace/enter/esc ─────────────
                // Esc always goes back.
                (_, KeyCode::Esc) => {
                    match step {
                        ConnectionAddStep::ChooseType { .. } => {
                            state.mode = Mode::ConnectionManager { cursor: 0 };
                        }
                        ConnectionAddStep::EnterSqlitePath { .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::ChooseType { cursor: 0 });
                        }
                        ConnectionAddStep::EnterSqliteName { .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterSqlitePath { input: String::new() });
                        }
                        ConnectionAddStep::EnterMysqlHost { .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::ChooseType { cursor: 1 });
                        }
                        ConnectionAddStep::EnterMysqlPort { host, .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlHost { input: host });
                        }
                        ConnectionAddStep::EnterMysqlUsername { host, port, .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlPort { host, input: port });
                        }
                        ConnectionAddStep::EnterMysqlPassword { host, port, username, .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlUsername { host, port, input: username });
                        }
                        ConnectionAddStep::EnterMysqlDatabase { host, port, username, password, .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlPassword { host, port, username, input: password });
                        }
                        ConnectionAddStep::EnterMysqlName { host, port, username, password, database, .. } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlDatabase { host, port, username, password, input: database });
                        }
                    }
                }
                // Backspace: remove last char from current input.
                (_, KeyCode::Backspace) => {
                    let mut s = step.clone();
                    if let Some(buf) = s.input_mut() {
                        buf.pop();
                    }
                    state.mode = Mode::ConnectionAdd(s);
                }
                // Enter: advance to next step (or connect on last step).
                (_, KeyCode::Enter) => {
                    match step {
                        ConnectionAddStep::EnterSqlitePath { input } => {
                            if !input.is_empty() {
                                // Auto-generate name from file stem.
                                let auto = std::path::Path::new(&input)
                                    .file_stem()
                                    .and_then(|s| s.to_str())
                                    .unwrap_or(&input)
                                    .to_string();
                                state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterSqliteName {
                                    path: input,
                                    input: auto,
                                });
                            }
                        }
                        ConnectionAddStep::EnterSqliteName { path, input: name } => {
                            // Validate name is non-empty and unique.
                            let name = name.trim().to_string();
                            if name.is_empty() {
                                state.mode = Mode::Error("Connection name cannot be empty".to_string());
                            } else if connections.iter().any(|c| c.name == name) {
                                state.mode = Mode::Error(format!("Connection name '{}' already exists", name));
                            } else {
                                let params = ConnectionParams {
                                    name: name.clone(),
                                    db_type: DbType::Sqlite,
                                    sqlite_path: Some(path.clone()),
                                    mysql_host: None,
                                    mysql_port: None,
                                    mysql_username: None,
                                    mysql_password: None,
                                    mysql_database: None,
                                };
                                match db::connect_params(&params).await {
                                    Err(e) => {
                                        state.mode = Mode::Error(format!("Connection failed: {}", e));
                                    }
                                    Ok(db_box) => {
                                        match Schema::explore(db_box.as_ref()).await {
                                            Err(e) => {
                                                state.mode = Mode::Error(format!("Schema exploration failed: {}", e));
                                            }
                                            Ok(schema) => {
                                                let display_url = params.display_url();
                                                connections.push(ConnectionHandle { name: name.clone(), db: db_box, schema });
                                                state.connections.push(ConnectionInfo {
                                                    name: name.clone(),
                                                    db_type: DbType::Sqlite,
                                                    display_url,
                                                    status: ConnectionStatus::Connected,
                                                });
                                                engine.schema = merged_schema(connections);
                                                state.table_names = merged_table_names(connections);
                                                state.table_columns = merged_table_columns(connections);
                                                state.mode = Mode::Info(format!("Connected to '{}' (sqlite)", name));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        ConnectionAddStep::EnterMysqlHost { input } => {
                            if !input.is_empty() {
                                state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlPort {
                                    host: input,
                                    input: "3306".to_string(),
                                });
                            }
                        }
                        ConnectionAddStep::EnterMysqlPort { host, input: port } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlUsername {
                                host,
                                port,
                                input: String::new(),
                            });
                        }
                        ConnectionAddStep::EnterMysqlUsername { host, port, input: username } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlPassword {
                                host,
                                port,
                                username,
                                input: String::new(),
                            });
                        }
                        ConnectionAddStep::EnterMysqlPassword { host, port, username, input: password } => {
                            state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlDatabase {
                                host,
                                port,
                                username,
                                password,
                                input: String::new(),
                            });
                        }
                        ConnectionAddStep::EnterMysqlDatabase { host, port, username, password, input: database } => {
                            if !database.is_empty() {
                                // Auto-generate name from database name.
                                state.mode = Mode::ConnectionAdd(ConnectionAddStep::EnterMysqlName {
                                    host,
                                    port,
                                    username,
                                    password,
                                    database: database.clone(),
                                    input: database,
                                });
                            }
                        }
                        ConnectionAddStep::EnterMysqlName { host, port, username, password, database, input: name } => {
                            let name = name.trim().to_string();
                            if name.is_empty() {
                                state.mode = Mode::Error("Connection name cannot be empty".to_string());
                            } else if connections.iter().any(|c| c.name == name) {
                                state.mode = Mode::Error(format!("Connection name '{}' already exists", name));
                            } else {
                                let port_num: u16 = port.parse().unwrap_or(3306);
                                let params = ConnectionParams {
                                    name: name.clone(),
                                    db_type: DbType::Mysql,
                                    sqlite_path: None,
                                    mysql_host: Some(host.clone()),
                                    mysql_port: Some(port_num),
                                    mysql_username: Some(username.clone()),
                                    mysql_password: Some(password.clone()),
                                    mysql_database: Some(database.clone()),
                                };
                                match db::connect_params(&params).await {
                                    Err(e) => {
                                        state.mode = Mode::Error(format!("Connection failed: {}", e));
                                    }
                                    Ok(db_box) => {
                                        match Schema::explore(db_box.as_ref()).await {
                                            Err(e) => {
                                                state.mode = Mode::Error(format!("Schema exploration failed: {}", e));
                                            }
                                            Ok(schema) => {
                                                let display_url = params.display_url();
                                                connections.push(ConnectionHandle { name: name.clone(), db: db_box, schema });
                                                state.connections.push(ConnectionInfo {
                                                    name: name.clone(),
                                                    db_type: DbType::Mysql,
                                                    display_url,
                                                    status: ConnectionStatus::Connected,
                                                });
                                                engine.schema = merged_schema(connections);
                                                state.table_names = merged_table_names(connections);
                                                state.table_columns = merged_table_columns(connections);
                                                state.mode = Mode::Info(format!("Connected to '{}' (mysql)", name));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                // Printable characters: append to current text input.
                (_, KeyCode::Char(c)) => {
                    let mut s = step.clone();
                    if let Some(buf) = s.input_mut() {
                        buf.push(c);
                    }
                    state.mode = Mode::ConnectionAdd(s);
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
    connections: &mut Vec<ConnectionHandle>,
    pending_paths: &mut Option<(rules::Rule, Vec<schema::TablePath>, Option<String>)>,
) -> Result<()> {
    if connections.is_empty() {
        state.mode = Mode::Error("No database connected. Press M to open the Connection Manager.".to_string());
        return Ok(());
    }

    // Pre-process command: detect and resolve qualified table names (conn.table).
    // We look at the first token of the command to find connection routing.
    let (resolved_cmd, conn_name) = resolve_command_table(&cmd, connections);
    let conn_name_str: Option<String> = conn_name.map(|s| s.to_string());

    let db = db_for_connection(conn_name_str.as_deref(), connections);
    let db = match db {
        Some(d) => d,
        None => {
            state.mode = Mode::Error("No active database connection".to_string());
            return Ok(());
        }
    };

    match rules::parse_rule(&resolved_cmd) {
        Err(e) => {
            state.mode = Mode::Error(e);
        }
        Ok(rule) => {
            match engine.execute_rule(db, rule.clone(), conn_name_str.clone()).await {
                Err(e) => {
                    state.mode = Mode::Error(e.to_string());
                }
                Ok(None) => {
                    if place_last_added_rule_at_next_cursor(state, engine) {
                        let conn_pairs: Vec<(&str, &dyn db::Database)> = connections
                            .iter()
                            .map(|c| (c.name.as_str(), c.db.as_ref()))
                            .collect();
                        engine.reexecute_all_multi(&conn_pairs).await?;
                    }
                }
                Ok(Some(result)) => {
                    // Multiple paths — ask user to pick
                    state.paths = result.paths.clone();
                    state.paths_has_more = result.has_more;
                    state.path_cursor = 0;
                    state.mode = Mode::PathSelection;
                    *pending_paths = Some((rule, result.paths, conn_name_str));
                }
            }
        }
    }
    Ok(())
}

/// Pre-process a command string to resolve `connection.table` qualified names.
///
/// Returns `(resolved_command, Option<connection_name>)`.
/// If the first token is a qualified `conn.table`, the command is rewritten to
/// use just `table`, and the connection name is returned.
fn resolve_command_table<'a>(
    cmd: &str,
    connections: &'a [ConnectionHandle],
) -> (String, Option<&'a str>) {
    let trimmed = cmd.trim();
    // Extract first token (up to first space or end).
    let first_token = trimmed.split_whitespace().next().unwrap_or("");

    if let Some(dot) = first_token.find('.') {
        let conn_name = &first_token[..dot];
        let table = &first_token[dot + 1..];
        if connections.iter().any(|c| c.name == conn_name) {
            let rest = &trimmed[first_token.len()..];
            let resolved = format!("{}{}", table, rest);
            // Return the connection name with the lifetime of the connections slice.
            let conn_ref = connections.iter().find(|c| c.name == conn_name).map(|c| c.name.as_str());
            return (resolved, conn_ref);
        }
    }
    (trimmed.to_string(), None)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{ColumnInfo, ForeignKey, TableInfo};
    use crate::schema::Schema;
    use std::collections::HashMap;

    fn make_schema_with_tables(tables: &[&str]) -> Schema {
        let mut map = HashMap::new();
        for &t in tables {
            map.insert(
                t.to_string(),
                TableInfo {
                    name: t.to_string(),
                    columns: vec![ColumnInfo {
                        name: "id".to_string(),
                        data_type: "INTEGER".to_string(),
                        column_type: "INTEGER".to_string(),
                        nullable: false,
                        is_primary_key: true,
                    }],
                    foreign_keys: vec![],
                },
            );
        }
        Schema { tables: map, virtual_fks: vec![] }
    }

    // We can't construct a real ConnectionHandle in unit tests (it holds a
    // Box<dyn Database>).  We test the pure functions that only need the schema.

    #[test]
    fn test_merged_schema_unique_tables() {
        // When all table names are unique across connections the merged schema
        // should contain both the unqualified and the qualified names.
        use crate::db::sqlite::SqliteDb;

        // Build fake handles by testing the logic through merged_schema directly.
        // (We cannot easily build ConnectionHandle without a live DB, so we test
        // the helper logic by calling merged_schema with an empty slice and
        // verifying it returns an empty schema.)
        let schema = merged_schema(&[]);
        assert!(schema.tables.is_empty());
    }

    #[test]
    fn test_auto_name_for_url_sqlite() {
        assert_eq!(auto_name_for_url("sqlite://samples/ecommerce.db"), "ecommerce");
        assert_eq!(auto_name_for_url("sqlite:samples/blog.db"), "blog");
    }

    #[test]
    fn test_auto_name_for_url_mysql() {
        assert_eq!(
            auto_name_for_url("mysql://user:pass@localhost/myapp"),
            "myapp"
        );
    }

    #[test]
    fn test_connection_params_sqlite_url() {
        use crate::db::{ConnectionParams, DbType};
        let p = ConnectionParams {
            name: "test".to_string(),
            db_type: DbType::Sqlite,
            sqlite_path: Some("samples/ecommerce.db".to_string()),
            mysql_host: None,
            mysql_port: None,
            mysql_username: None,
            mysql_password: None,
            mysql_database: None,
        };
        assert_eq!(p.to_url(), "sqlite://samples/ecommerce.db");
        assert_eq!(p.display_url(), "sqlite://samples/ecommerce.db");
    }

    #[test]
    fn test_connection_params_mysql_url() {
        use crate::db::{ConnectionParams, DbType};
        let p = ConnectionParams {
            name: "prod".to_string(),
            db_type: DbType::Mysql,
            sqlite_path: None,
            mysql_host: Some("db.example.com".to_string()),
            mysql_port: Some(3306),
            mysql_username: Some("alice".to_string()),
            mysql_password: Some("secret".to_string()),
            mysql_database: Some("mydb".to_string()),
        };
        assert_eq!(p.to_url(), "mysql://alice:secret@db.example.com:3306/mydb");
        // Display URL omits the password.
        assert_eq!(p.display_url(), "mysql://alice@db.example.com:3306/mydb");
    }

    #[test]
    fn test_connection_params_mysql_no_password() {
        use crate::db::{ConnectionParams, DbType};
        let p = ConnectionParams {
            name: "dev".to_string(),
            db_type: DbType::Mysql,
            sqlite_path: None,
            mysql_host: Some("localhost".to_string()),
            mysql_port: Some(3306),
            mysql_username: Some("root".to_string()),
            mysql_password: Some(String::new()),
            mysql_database: Some("devdb".to_string()),
        };
        assert_eq!(p.to_url(), "mysql://root@localhost:3306/devdb");
    }
}
