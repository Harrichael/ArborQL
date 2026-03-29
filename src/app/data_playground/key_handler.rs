use anyhow::Result;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::app::connection_manager::widget::ConnManagerWidget;
use crate::app::virtual_fk_manager::widget::VfkWidget;
use crate::command_history;
use crate::config;
use crate::db;
use crate::engine::{self, flatten_tree};
use crate::rules::{self, Completion};
use super::types::{Mode, PALETTE_COMMANDS};

use super::module::{
    columns_for_table, execute_command, insert_rule_at_next_cursor, saved_ids, toggle_fold,
};
use super::widgets::error_info::ErrorInfoWidget;
use super::{DataPlayground, TickResult};

/// Handle a key event in the context of the current Mode.
/// Called after overlays have been checked.
pub(super) async fn handle_mode_key(
    playground: &mut DataPlayground,
    key: KeyEvent,
) -> Result<TickResult> {
    let state = &mut playground.state;
    let engine = &mut playground.engine;
    let conn_mgr = &mut playground.conn_mgr;
    let pending_paths = &mut playground.pending_paths;
    let history_file = &playground.history_file;

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
    }

    Ok(TickResult::Continue)
}
