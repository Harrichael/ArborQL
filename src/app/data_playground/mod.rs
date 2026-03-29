use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::app::column_manager::module::ColumnManagerModule;
use crate::app::connection_manager::widget::{ConnManagerAction, ConnManagerWidget};
use crate::app::model::SchemaNode;
use crate::app::virtual_fk_manager::widget::{VfkAction, VfkWidget};
use crate::command_history;
use crate::config;
use crate::connection_manager::{ConnectionManager, ConnectionType};
use crate::db;
use crate::engine::{self, Engine, flatten_tree};
use crate::log;
use crate::rules::{self, Completion};
use crate::schema::VirtualFkDef;
use crate::ui::app::{AppState, ConfirmAction, Mode, PALETTE_COMMANDS};
use crate::ui::model::control_panel::{dispatch, ControlPanel};
use crate::ui::model::keys::from_key_event;

pub enum TickResult {
    Continue,
    Suspend,
    Quit,
}

pub struct DataPlayground {
    pub state: AppState,
    pub engine: Engine,
    pub conn_mgr: ConnectionManager,
    pending_paths: Option<(rules::Rule, Vec<engine::TablePath>)>,
    history_file: Option<PathBuf>,
}

impl DataPlayground {
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

    /// Drain logs, poll for events, handle key input.
    /// Returns `Suspend` when Ctrl+Z was pressed, `Quit` to exit.
    pub async fn tick(&mut self) -> Result<TickResult> {
        // Drain log entries queued by background code.
        self.state.logs.extend(log::drain());

        // Poll for events with a short timeout for async responsiveness.
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
        crate::ui::render::render(f, &mut self.state, &self.engine.roots);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<TickResult> {
        let state = &mut self.state;
        let engine = &mut self.engine;
        let conn_mgr = &mut self.conn_mgr;
        let pending_paths = &mut self.pending_paths;
        let history_file = &self.history_file;

        // Ctrl+Z suspends regardless of current mode.
        if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) {
            return Ok(TickResult::Suspend);
        }

        // ── Overlay dispatch chain ──────────────────────────────────────
        // Each overlay has exclusive key handling while open.

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
            return Ok(TickResult::Continue);
        }

        if let Some(ref mut widget) = state.manuals {
            if let Some(event) = from_key_event(key, &widget.focus_loci()) {
                dispatch(widget, event);
            }
            if widget.closed {
                state.manuals = None;
            }
            return Ok(TickResult::Continue);
        }

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
            return Ok(TickResult::Continue);
        }

        if state.conn_manager.is_some() {
            if let Some(ref mut widget) = state.conn_manager {
                if let Some(event) = from_key_event(key, &widget.focus_loci()) {
                    dispatch(widget, event);
                }
            }
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
                                w.view = crate::app::connection_manager::widget::ConnManagerView::Tabs;
                                w.tab = crate::app::connection_manager::widget::ConnManagerTab::Connections;
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
                return Ok(TickResult::Continue);
            }
        }

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
                return Ok(TickResult::Continue);
            }
        }

        if let Some(ref mut widget) = state.log_viewer {
            if let Some(event) = from_key_event(key, &widget.focus_loci()) {
                dispatch(widget, event);
            }
            if widget.closed {
                state.log_viewer = None;
            }
            return Ok(TickResult::Continue);
        }

        // ── Mode-based key handling ─────────────────────────────────────
        let db: &dyn db::Database = conn_mgr;

        match state.mode.clone() {
            Mode::Normal => {
                match key.code {
                    KeyCode::Char(':') => {
                        state.mode = Mode::CommandPalette;
                        state.clear_input();
                    }
                    KeyCode::Char('j') | KeyCode::Down => state.select_down(),
                    KeyCode::Char('k') | KeyCode::Up => state.select_up(),
                    KeyCode::Enter => {
                        let flat = flatten_tree(&engine.roots);
                        if state.selected_row < flat.len() {
                            toggle_fold(&mut engine.roots, state.selected_row);
                        }
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
                    KeyCode::Char(c) => {
                        state.mode = Mode::Query;
                        state.clear_input();
                        state.input_char(c);
                        state.history_cursor = None;
                        state.history_draft = String::new();
                    }
                    _ => {}
                }
            }

            Mode::Query => {
                match key.code {
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
                                let (_, partial) = rules::tokenize_partial(&state.input);
                                let prefix_len = state.input.len() - partial.len();
                                state.input = format!("{}{} ", &state.input[..prefix_len], s);
                                state.cursor = state.input.len();
                                state.history_cursor = None;
                            }
                        }
                    }
                    KeyCode::Char(c) if key.modifiers.contains(KeyModifiers::CONTROL) => {
                        match c {
                            'c' => return Ok(TickResult::Quit),
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
            }

            Mode::CommandSearch { query, match_cursor, saved_input } => {
                match key.code {
                    KeyCode::Esc => {
                        state.input = saved_input.clone();
                        state.cursor = state.input.len();
                        state.history_cursor = None;
                        state.mode = Mode::Query;
                    }
                    KeyCode::Enter => {
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
                            'c' => return Ok(TickResult::Quit),
                            'r' => {
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

            Mode::CommandPalette => {
                match key.code {
                    KeyCode::Esc => {
                        state.mode = Mode::Normal;
                        state.clear_input();
                    }
                    KeyCode::Enter => {
                        let filter = state.input_text().trim().to_lowercase();
                        state.clear_input();
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
                            Some("quit") => return Ok(TickResult::Quit),
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
                                        crate::app::query_rules_manager::widget::RulesWidget::new(
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
                                state.log_viewer = Some(
                                    crate::app::log_viewer::widget::LogViewerWidget::new(state.logs.clone())
                                );
                            }
                            Some("manuals") => {
                                state.manuals = Some(crate::app::manuals_manager::widget::ManualsWidget::new());
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
                        state.mode = Mode::Normal;
                        state.paths.clear();
                    }
                    _ => {}
                }
            }

            Mode::Error(_) | Mode::Info(_) => {
                state.mode = Mode::Normal;
            }

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

        Ok(TickResult::Continue)
    }
}

// ── Private helpers ─────────────────────────────────────────────────────

fn saved_ids(state: &AppState) -> HashSet<String> {
    state.saved_connections.iter().map(|s| s.id.clone()).collect()
}

fn refresh_schema_from_conn_mgr(
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
