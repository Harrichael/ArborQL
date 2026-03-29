use anyhow::Result;
use crossterm::event::KeyEvent;

use crate::app::connection_manager::widget::ConnManagerAction;
use crate::app::virtual_fk_manager::widget::VfkAction;
use crate::config;
use super::types::ConfirmAction;
use crate::app::tui::control_panel::{dispatch, ControlPanel};
use crate::app::tui::keys::from_key_event;

use super::module::{query_type_options, refresh_schema_from_conn_mgr, saved_ids};
use super::{DataPlayground, TickResult};
use super::widgets::error_info::ErrorInfoWidget;

/// Dispatch a key event to the active widget overlay, if any.
/// Returns `Some(TickResult)` if a widget consumed the event,
/// `None` if no widget is active and the caller should handle the key.
pub(super) async fn dispatch_widgets(
    playground: &mut DataPlayground,
    key: KeyEvent,
) -> Result<Option<TickResult>> {
    let state = &mut playground.state;
    let engine = &mut playground.engine;
    let conn_mgr = &mut playground.conn_mgr;

    // Error/Info overlay — any key dismisses
    if let Some(ref mut widget) = state.error_info {
        if let Some(event) = from_key_event(key, &widget.focus_loci()) {
            dispatch(widget, event);
        }
        if widget.closed {
            state.error_info = None;
        }
        return Ok(Some(TickResult::Continue));
    }

    // Confirm dialog overlay
    if let Some(ref mut widget) = state.confirm {
        if let Some(event) = from_key_event(key, &widget.focus_loci()) {
            dispatch(widget, event);
        }
    }
    if state.confirm.as_ref().map_or(false, |w| w.closed) {
        let widget = state.confirm.take().unwrap();
        if let Some(confirmed) = widget.confirmed {
            match widget.tag {
                ConfirmAction::SaveConnectionWithPassword { conn_index } => {
                    if conn_index < conn_mgr.connections.len() {
                        let conn = &conn_mgr.connections[conn_index];
                        let alias = conn.alias.clone();
                        match config::save_connection(conn, &state.saved_connections, confirmed) {
                            Ok((path, updated)) => {
                                state.saved_connections = updated;
                                state.connections_summary = conn_mgr.connection_summaries(&saved_ids(state));
                                let pw_note = if confirmed { " (with password)" } else { "" };
                                state.error_info = Some(ErrorInfoWidget::info(
                                    format!("Connection '{}' saved{} to {}", alias, pw_note, path.display())
                                ));
                            }
                            Err(e) => {
                                state.error_info = Some(ErrorInfoWidget::error(
                                    format!("Save failed: {}", e)
                                ));
                            }
                        }
                    }
                }
            }
        }
        return Ok(Some(TickResult::Continue));
    }
    if state.confirm.is_some() {
        return Ok(Some(TickResult::Continue));
    }

    // Column manager overlay
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
        return Ok(Some(TickResult::Continue));
    }

    // Manuals overlay
    if let Some(ref mut widget) = state.manuals {
        if let Some(event) = from_key_event(key, &widget.focus_loci()) {
            dispatch(widget, event);
        }
        if widget.closed {
            state.manuals = None;
        }
        return Ok(Some(TickResult::Continue));
    }

    // Rules reorder overlay
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
        return Ok(Some(TickResult::Continue));
    }

    // Connection manager overlay
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
                        state.error_info = Some(ErrorInfoWidget::info(format!("Connected '{}'", alias)));
                    }
                    Err(_) => {
                        let ids = saved_ids(state);
                        if let Some(ref mut w) = state.conn_manager {
                            w.connections = conn_mgr.connection_summaries(&ids);
                            w.saved_connections = state.saved_connections.clone();
                            w.view = crate::app::connection_manager::widget::ConnManagerView::Tabs;
                            w.tab = crate::app::connection_manager::widget::ConnManagerTab::Connections;
                            w.cursor = conn_mgr.connections.len().saturating_sub(1);
                            w.focus.input = crate::app::tui::keys::InputFocus::None;
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
                        state.confirm = Some(
                            super::widgets::confirm::ConfirmWidget::new(
                                msg,
                                ConfirmAction::SaveConnectionWithPassword { conn_index },
                            )
                        );
                    } else {
                        match config::save_connection(conn, &state.saved_connections, false) {
                            Ok((path, updated)) => {
                                let info = format!("Connection '{}' saved to {}", conn.alias, path.display());
                                state.saved_connections = updated;
                                state.conn_manager = None;
                                state.error_info = Some(ErrorInfoWidget::info(info));
                            }
                            Err(e) => {
                                state.conn_manager = None;
                                state.error_info = Some(ErrorInfoWidget::error(format!("Save failed: {}", e)));
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
            return Ok(Some(TickResult::Continue));
        }
    }

    // Virtual FK manager overlay
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
                        state.error_info = Some(ErrorInfoWidget::info(
                            format!("Virtual FKs saved to {}", path.display())
                        ));
                    }
                    Err(e) => {
                        state.vfk_manager = None;
                        state.error_info = Some(ErrorInfoWidget::error(
                            format!("Save failed: {}", e)
                        ));
                    }
                }
            }
        }
        if state.vfk_manager.as_ref().map_or(false, |w| w.closed) {
            state.vfk_manager = None;
        }
        if state.vfk_manager.is_some() {
            return Ok(Some(TickResult::Continue));
        }
    }

    // Log viewer overlay
    if let Some(ref mut widget) = state.log_viewer {
        if let Some(event) = from_key_event(key, &widget.focus_loci()) {
            dispatch(widget, event);
        }
        if widget.closed {
            state.log_viewer = None;
        }
        return Ok(Some(TickResult::Continue));
    }

    // No overlay consumed the key
    Ok(None)
}
