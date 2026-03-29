use std::collections::HashSet;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::app::column_manager::module::ColumnManagerModule;
use crate::app::model::SchemaNode;
use crate::command_history;
use crate::config;
use crate::connection_manager::{ConnectionManager, ConnectionType};
use crate::db;
use crate::engine::Engine;
use crate::log;
use crate::rules;
use crate::engine;
use super::state::AppState;

use super::{DataPlayground, TickResult};

impl DataPlayground {
    /// Drain logs, poll for events, handle key input.
    /// Returns `Suspend` when Ctrl+Z was pressed, `Quit` to exit.
    pub async fn tick(&mut self) -> Result<TickResult> {
        self.state.logs.extend(log::drain());

        if event::poll(std::time::Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press || key.kind == KeyEventKind::Repeat => {
                    return self.handle_key(key).await;
                }
                _ => {}
            }
        }
        Ok(TickResult::Continue)
    }

    /// Render the current state to the given frame.
    pub fn render(&mut self, f: &mut ratatui::Frame) {
        super::render::render(f, &mut self.state, &self.engine.roots);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<TickResult> {
        // Ctrl+Z suspends regardless of current mode.
        if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) {
            return Ok(TickResult::Suspend);
        }

        // Try widget dispatch first (overlays have priority).
        if let Some(result) = super::widget_dispatch::dispatch_widgets(self, key).await? {
            return Ok(result);
        }

        // Fall through to mode-based key handling.
        super::key_handler::handle_mode_key(self, key).await
    }

    pub async fn new(database_url: Option<String>) -> Result<Self> {
        let mut conn_mgr = ConnectionManager::new();
        let defaults = config::load_config()?;

        if let Some(ref url) = database_url {
            let alias = ConnectionManager::alias_from_url(url);
            let conn_type = ConnectionType::from_url(url)
                .ok_or_else(|| anyhow::anyhow!("Unsupported database URL: {}", url))?;
            eprintln!("Connecting to database as '{}'…", alias);
            let params = ConnectionType::params_from_url(url);
            conn_mgr.add_connection(None, alias, conn_type, url.clone(), params).await?;
        }

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
        for (_name, info) in &engine.schema.tables {
            state.column_manager.register_node(&SchemaNode::from_table_info(info));
        }
        for vfk in defaults.virtual_fks {
            state.virtual_fks.push(vfk.clone());
            engine.schema.virtual_fks.push(vfk);
        }
        state.table_columns = engine.schema.tables.iter().map(|(name, info)| {
            let cols = info.columns.iter().map(|c| c.name.clone()).collect();
            (name.clone(), cols)
        }).collect();

        let history_file = config::home_dir()
            .ok()
            .map(|h| h.join(".latticeql").join("history"));
        if let Some(ref path) = history_file {
            match command_history::CommandHistory::load_from_file(path, defaults.history_max_len) {
                Ok(h) => state.command_history = h,
                Err(e) => eprintln!("Warning: could not load command history: {}", e),
            }
        }

        Ok(Self {
            state,
            engine,
            conn_mgr,
            pending_paths: None,
            history_file,
        })
    }
}

// ── Helpers used across the data_playground module ──────────────────────

pub(super) fn saved_ids(state: &AppState) -> HashSet<String> {
    state.saved_connections.iter().map(|s| s.id.clone()).collect()
}

pub(super) fn refresh_schema_from_conn_mgr(
    state: &mut AppState,
    engine: &mut Engine,
    conn_mgr: &ConnectionManager,
) {
    let mut schema = conn_mgr.merged_schema().clone();
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
    for (_name, info) in &engine.schema.tables {
        state.column_manager.register_node(&SchemaNode::from_table_info(info));
    }
    state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
    state.display_table_names = conn_mgr.display_table_names();
    state.display_name_map = conn_mgr.display_name_map();
}

pub(super) fn insert_rule_at_next_cursor(
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

pub(super) fn place_last_added_rule_at_next_cursor(state: &mut AppState, engine: &mut Engine) -> bool {
    if let Some(rule) = engine.rules.pop() {
        let idx = state.next_rule_cursor.min(engine.rules.len());
        let inserted_before_existing = idx < engine.rules.len();
        engine.rules.insert(idx, rule);
        state.next_rule_cursor = (idx + 1).min(engine.rules.len());
        return inserted_before_existing;
    }
    false
}

pub(super) fn columns_for_table(roots: &[engine::DataNode], table: &str) -> Vec<String> {
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

pub(super) async fn query_type_options(db: &dyn db::Database, table: &str, type_col: &str) -> Vec<(String, i64)> {
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

pub(super) async fn execute_command(
    cmd: String,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
    pending_paths: &mut Option<(rules::Rule, Vec<engine::TablePath>)>,
) -> Result<()> {
    match rules::parse_rule(&cmd) {
        Err(e) => {
            state.error_info = Some(
                super::widgets::error_info::ErrorInfoWidget::error(e)
            );
        }
        Ok(rule) => {
            match engine.execute_rule(db, rule.clone()).await {
                Err(e) => {
                    state.error_info = Some(
                        super::widgets::error_info::ErrorInfoWidget::error(e.to_string())
                    );
                }
                Ok(None) => {
                    if place_last_added_rule_at_next_cursor(state, engine) {
                        engine.reexecute_all(db).await?;
                    }
                }
                Ok(Some(result)) => {
                    state.paths = result.paths.clone();
                    state.paths_has_more = result.has_more;
                    state.paths_next_depth = result.next_depth;
                    state.path_cursor = 0;
                    state.mode = super::types::Mode::PathSelection;
                    *pending_paths = Some((rule, result.paths));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn toggle_fold(roots: &mut [engine::DataNode], flat_idx: usize) {
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
