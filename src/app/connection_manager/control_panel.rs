use crossterm::event::{KeyCode, KeyEvent};

use super::widget::{
    ConnManagerAction, ConnManagerTab, ConnManagerView, ConnManagerWidget,
    ConnectionForm,
};
use crate::connection_manager::ConnectionType;
use crate::app::tui::control_panel::ControlPanel;
use crate::app::tui::keys::{EntityFocus, FocusLoci, InputFocus};

impl ControlPanel for ConnManagerWidget {
    fn focus_loci(&self) -> FocusLoci {
        self.focus
    }

    fn on_navigate_up(&mut self) {
        match self.view {
            ConnManagerView::Tabs => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                }
            }
            _ => {}
        }
    }

    fn on_navigate_down(&mut self) {
        match self.view {
            ConnManagerView::Tabs => {
                let max = self.tab_item_count().saturating_sub(1);
                if self.cursor < max {
                    self.cursor += 1;
                }
            }
            _ => {}
        }
    }

    fn on_next_field(&mut self) {
        match self.view {
            ConnManagerView::Tabs => {
                self.tab = match self.tab {
                    ConnManagerTab::Connections => ConnManagerTab::Saved,
                    ConnManagerTab::Saved => ConnManagerTab::Connectors,
                    ConnManagerTab::Connectors => ConnManagerTab::Connections,
                };
                self.cursor = 0;
            }
            ConnManagerView::AddForm => {
                if let Some(ref mut form) = self.form {
                    form.active_field = (form.active_field + 1) % form.fields.len();
                }
            }
            _ => {}
        }
    }

    fn on_prev_field(&mut self) {
        match self.view {
            ConnManagerView::Tabs => {
                self.tab = match self.tab {
                    ConnManagerTab::Connections => ConnManagerTab::Connectors,
                    ConnManagerTab::Saved => ConnManagerTab::Connections,
                    ConnManagerTab::Connectors => ConnManagerTab::Saved,
                };
                self.cursor = 0;
            }
            ConnManagerView::AddForm => {
                if let Some(ref mut form) = self.form {
                    if form.active_field == 0 {
                        form.active_field = form.fields.len() - 1;
                    } else {
                        form.active_field -= 1;
                    }
                }
            }
            _ => {}
        }
    }

    fn on_confirm(&mut self) {
        match self.view {
            ConnManagerView::Tabs => {
                match self.tab {
                    ConnManagerTab::Connections => {
                        if self.cursor < self.connections.len() {
                            self.action = ConnManagerAction::ToggleConnection(self.cursor);
                        }
                    }
                    ConnManagerTab::Saved => {
                        if self.cursor < self.saved_connections.len() {
                            let saved = &self.saved_connections[self.cursor];
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
                                .unwrap_or_else(|| format!("conn{}", self.cursor + 1));
                            self.alias = suggested;
                            self.alias_saved_index = self.cursor;
                            self.view = ConnManagerView::AliasPrompt;
                            self.focus.input = InputFocus::Text;
                        }
                    }
                    ConnManagerTab::Connectors => {
                        let types = ConnectionType::all();
                        if self.cursor < types.len() {
                            let ct = types[self.cursor].clone();
                            self.form = Some(ConnectionForm::new(ct));
                            self.view = ConnManagerView::AddForm;
                            self.focus.input = InputFocus::Text;
                        }
                    }
                }
            }
            ConnManagerView::AddForm => {
                if let Some(ref form) = self.form {
                    if form.is_complete() {
                        let alias = form.alias().to_string();
                        let conn_type = form.conn_type.clone();
                        let params = form.values();
                        match conn_type.build_url(&params) {
                            Ok(url) => {
                                self.action = ConnManagerAction::Connect {
                                    alias,
                                    conn_type,
                                    url,
                                    params,
                                    inherited_id: None,
                                };
                            }
                            Err(_) => {
                                // URL build failed — stay on form
                            }
                        }
                    }
                }
            }
            ConnManagerView::AliasPrompt => {
                if !self.alias.is_empty() {
                    if let Some(saved) = self.saved_connections.get(self.alias_saved_index) {
                        let inherited_id = saved.id.clone();
                        let conn_type = match saved.conn_type.as_str() {
                            "sqlite" => ConnectionType::Sqlite,
                            _ => ConnectionType::Mysql,
                        };
                        let params = saved.params.clone();
                        match conn_type.build_url(&params) {
                            Ok(url) => {
                                self.action = ConnManagerAction::Connect {
                                    alias: self.alias.clone(),
                                    conn_type,
                                    url,
                                    params,
                                    inherited_id: Some(inherited_id),
                                };
                            }
                            Err(_) => {
                                // URL build failed — stay on prompt
                            }
                        }
                    }
                }
            }
        }
    }

    fn on_remove(&mut self) {
        if self.view != ConnManagerView::Tabs {
            return;
        }
        match self.tab {
            ConnManagerTab::Saved => {
                if self.cursor < self.saved_connections.len() {
                    let id = self.saved_connections[self.cursor].id.clone();
                    self.action = ConnManagerAction::RemoveSaved(id);
                }
            }
            ConnManagerTab::Connections => {
                if self.cursor < self.connections.len() {
                    self.action = ConnManagerAction::RemoveConnection(self.cursor);
                }
            }
            _ => {}
        }
    }

    fn on_save(&mut self) {
        if self.view == ConnManagerView::Tabs && self.tab == ConnManagerTab::Connections {
            if self.cursor < self.connections.len() {
                let has_password = self.connections[self.cursor].url.contains("password")
                    || self.connections[self.cursor].url.contains('@');
                self.action = ConnManagerAction::SaveConnection {
                    conn_index: self.cursor,
                    needs_password_confirm: has_password,
                };
            }
        }
    }

    fn on_back(&mut self) {
        match self.view {
            ConnManagerView::Tabs => {
                self.closed = true;
            }
            ConnManagerView::AddForm => {
                self.form = None;
                self.view = ConnManagerView::Tabs;
                self.tab = ConnManagerTab::Connectors;
                self.cursor = 0;
                self.focus.input = InputFocus::None;
            }
            ConnManagerView::AliasPrompt => {
                self.alias.clear();
                self.view = ConnManagerView::Tabs;
                self.tab = ConnManagerTab::Saved;
                self.focus.input = InputFocus::None;
            }
        }
    }

    fn on_text_input(&mut self, key: KeyEvent) {
        match self.view {
            ConnManagerView::AddForm => {
                if let Some(ref mut form) = self.form {
                    match key.code {
                        KeyCode::Char(c) => {
                            form.fields[form.active_field].value.push(c);
                        }
                        KeyCode::Backspace => {
                            form.fields[form.active_field].value.pop();
                        }
                        _ => {}
                    }
                }
            }
            ConnManagerView::AliasPrompt => {
                match key.code {
                    KeyCode::Char(c) => {
                        self.alias.push(c);
                    }
                    KeyCode::Backspace => {
                        self.alias.pop();
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_widget() -> ConnManagerWidget {
        ConnManagerWidget::new(vec![], vec![])
    }

    #[test]
    fn focus_loci_tabs() {
        let w = empty_widget();
        let f = w.focus_loci();
        assert_eq!(f.input, InputFocus::None);
        assert_eq!(f.entity, EntityFocus::Overlay);
    }

    #[test]
    fn tab_cycling() {
        let mut w = empty_widget();
        assert_eq!(w.tab, ConnManagerTab::Connections);
        w.on_next_field();
        assert_eq!(w.tab, ConnManagerTab::Saved);
        w.on_next_field();
        assert_eq!(w.tab, ConnManagerTab::Connectors);
        w.on_next_field();
        assert_eq!(w.tab, ConnManagerTab::Connections);

        w.on_prev_field();
        assert_eq!(w.tab, ConnManagerTab::Connectors);
    }

    #[test]
    fn back_from_tabs_closes() {
        let mut w = empty_widget();
        w.on_back();
        assert!(w.closed);
    }

    #[test]
    fn connectors_tab_confirm_opens_form() {
        let mut w = empty_widget();
        w.tab = ConnManagerTab::Connectors;
        w.cursor = 0;
        w.on_confirm();
        assert_eq!(w.view, ConnManagerView::AddForm);
        assert!(w.form.is_some());
        assert_eq!(w.focus.input, InputFocus::Text);
    }

    #[test]
    fn back_from_form_returns_to_tabs() {
        let mut w = empty_widget();
        w.tab = ConnManagerTab::Connectors;
        w.cursor = 0;
        w.on_confirm(); // enter form
        w.on_back();
        assert_eq!(w.view, ConnManagerView::Tabs);
        assert_eq!(w.tab, ConnManagerTab::Connectors);
        assert!(w.form.is_none());
        assert_eq!(w.focus.input, InputFocus::None);
        assert!(!w.closed);
    }

    #[test]
    fn back_from_alias_returns_to_tabs() {
        let mut w = empty_widget();
        w.view = ConnManagerView::AliasPrompt;
        w.focus.input = InputFocus::Text;
        w.alias = "test".into();
        w.on_back();
        assert_eq!(w.view, ConnManagerView::Tabs);
        assert_eq!(w.tab, ConnManagerTab::Saved);
        assert!(w.alias.is_empty());
        assert_eq!(w.focus.input, InputFocus::None);
    }

    #[test]
    fn form_field_cycling() {
        let mut w = empty_widget();
        w.tab = ConnManagerTab::Connectors;
        w.cursor = 0;
        w.on_confirm(); // enter form
        let field_count = w.form.as_ref().unwrap().fields.len();
        assert!(field_count > 1);

        w.on_next_field();
        assert_eq!(w.form.as_ref().unwrap().active_field, 1);

        for _ in 0..field_count - 1 {
            w.on_next_field();
        }
        assert_eq!(w.form.as_ref().unwrap().active_field, 0); // wrapped

        w.on_prev_field();
        assert_eq!(w.form.as_ref().unwrap().active_field, field_count - 1);
    }
}
