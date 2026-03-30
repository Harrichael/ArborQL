use crossterm::event::{KeyCode, KeyEvent};

use super::widget::{
    VfkAction, VfkView, VfkWidget, VirtualFkField, VirtualFkForm,
};
use crate::schema::VirtualFkDef;
use crate::app::tui::control_panel::ControlPanel;
use crate::app::tui::keys::{FocusLoci, InputFocus};

impl ControlPanel for VfkWidget {
    fn focus_loci(&self) -> FocusLoci {
        self.focus
    }

    fn on_navigate_up(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    fn on_navigate_down(&mut self) {
        match self.view {
            VfkView::List => {
                let filtered = self.filtered_vfk_indices();
                if self.cursor + 1 < filtered.len() {
                    self.cursor += 1;
                }
            }
            VfkView::Form => {
                let items = self.dropdown_items();
                if self.cursor + 1 < items.len() {
                    self.cursor += 1;
                }
            }
        }
    }

    fn on_add_item(&mut self) {
        if self.view == VfkView::List {
            self.search.clear();
            self.search_cursor = 0;
            self.focus.input = InputFocus::None;
            self.form = Some(VirtualFkForm::new());
            self.view = VfkView::Form;
            self.cursor = 0;
            self.scroll = 0;
        }
    }

    fn on_remove(&mut self) {
        if self.view != VfkView::List {
            return;
        }
        let filtered = self.filtered_vfk_indices();
        if let Some(&orig_idx) = filtered.get(self.cursor) {
            self.action = VfkAction::RemoveFromEngine(orig_idx);
            self.cursor = self.cursor.saturating_sub(
                if self.cursor >= filtered.len().saturating_sub(1) { 1 } else { 0 }
            );
        }
    }

    fn on_save(&mut self) {
        self.action = VfkAction::SaveToConfig;
    }

    fn on_start_search(&mut self) {
        self.focus.input = InputFocus::Search;
    }

    fn on_next_field(&mut self) {
        if self.view == VfkView::Form {
            if let Some(ref mut form) = self.form {
                let next = form.active_field.next(form.type_column.is_empty());
                form.active_field = next;
                self.cursor = 0;
                self.scroll = 0;
                self.search.clear();
                self.search_cursor = 0;
                self.focus.input = InputFocus::None;

                // Pre-select "id" when switching to ToColumn
                if form.active_field == VirtualFkField::ToColumn {
                    if let Some(to_cols) = self.table_columns.get(&form.to_table) {
                        self.cursor = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                    }
                }

                // Request type options when switching to TypeValue
                if form.active_field == VirtualFkField::TypeValue && !form.type_column.is_empty() {
                    self.action = VfkAction::QueryTypeOptions {
                        table: form.from_table.clone(),
                        column: form.type_column.clone(),
                    };
                }
            }
        }
    }

    fn on_prev_field(&mut self) {
        if self.view == VfkView::Form {
            if let Some(ref mut form) = self.form {
                let prev = form.active_field.prev(form.type_column.is_empty());
                form.active_field = prev;
                self.cursor = 0;
                self.scroll = 0;
                self.search.clear();
                self.search_cursor = 0;
                self.focus.input = InputFocus::None;

                // Request type options when switching to TypeValue
                if form.active_field == VirtualFkField::TypeValue && !form.type_column.is_empty() {
                    self.action = VfkAction::QueryTypeOptions {
                        table: form.from_table.clone(),
                        column: form.type_column.clone(),
                    };
                }
            }
        }
    }

    fn on_confirm(&mut self) {
        if self.view != VfkView::Form {
            return;
        }
        let items = self.dropdown_items();
        let Some(raw_value) = items.get(self.cursor).cloned() else { return };

        self.search.clear();
        self.search_cursor = 0;
        self.focus.input = InputFocus::None;
        self.scroll = 0;

        let form = match &mut self.form {
            Some(f) => f,
            None => return,
        };

        match &form.active_field {
            VirtualFkField::FromTable => {
                form.from_table = raw_value;
                form.id_column.clear();
                form.type_column.clear();
                form.type_value.clear();
                form.active_field = VirtualFkField::IdColumn;
                self.cursor = 0;
            }
            VirtualFkField::IdColumn => {
                form.id_column = raw_value;
                form.active_field = VirtualFkField::TypeColumn;
                self.cursor = 0;
            }
            VirtualFkField::TypeColumn => {
                if raw_value.starts_with("(none") {
                    form.type_column.clear();
                    form.type_value.clear();
                    form.active_field = VirtualFkField::ToTable;
                } else {
                    form.type_column = raw_value;
                    form.active_field = VirtualFkField::TypeValue;
                    // Request type options
                    self.action = VfkAction::QueryTypeOptions {
                        table: form.from_table.clone(),
                        column: form.type_column.clone(),
                    };
                }
                self.cursor = 0;
            }
            VirtualFkField::TypeValue => {
                if !raw_value.starts_with("(no type_column") {
                    let tv = raw_value.split("  (").next().unwrap_or(&raw_value).to_string();
                    form.type_value = tv;
                }
                form.active_field = VirtualFkField::ToTable;
                self.cursor = 0;
            }
            VirtualFkField::ToTable => {
                form.to_table = raw_value;
                form.to_column.clear();
                // Pre-select "id" column
                if let Some(to_cols) = self.table_columns.get(&form.to_table) {
                    self.cursor = to_cols.iter().position(|c| c == "id").unwrap_or(0);
                } else {
                    self.cursor = 0;
                }
                form.active_field = VirtualFkField::ToColumn;
            }
            VirtualFkField::ToColumn => {
                form.to_column = raw_value;
                if form.is_complete() {
                    let vfk = VirtualFkDef {
                        from_table: form.from_table.clone(),
                        type_column: if form.type_column.is_empty() { None } else { Some(form.type_column.clone()) },
                        type_value: if form.type_value.is_empty() { None } else { Some(form.type_value.clone()) },
                        id_column: form.id_column.clone(),
                        to_table: form.to_table.clone(),
                        to_column: form.to_column.clone(),
                    };
                    self.action = VfkAction::AddToEngine(vfk);
                    self.form = None;
                    self.view = VfkView::List;
                    self.cursor = self.virtual_fks.len(); // will point to newly added
                }
            }
        }
    }

    fn on_back(&mut self) {
        match self.view {
            VfkView::List => {
                if self.focus.input == InputFocus::Search {
                    self.focus.input = InputFocus::None;
                } else if !self.search.is_empty() {
                    self.search.clear();
                    self.search_cursor = 0;
                    self.scroll = 0;
                    self.cursor = 0;
                } else {
                    self.closed = true;
                }
            }
            VfkView::Form => {
                if self.focus.input == InputFocus::Search {
                    self.focus.input = InputFocus::None;
                } else if !self.search.is_empty() {
                    self.search.clear();
                    self.search_cursor = 0;
                    self.scroll = 0;
                    self.cursor = 0;
                } else {
                    self.form = None;
                    self.view = VfkView::List;
                    self.cursor = 0;
                    self.scroll = 0;
                }
            }
        }
    }

    fn on_text_input(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Char(c) => {
                self.search.push(c);
                self.search_cursor = self.search.len();
                self.scroll = 0;
                self.cursor = 0;
            }
            KeyCode::Backspace => {
                self.search.pop();
                self.search_cursor = self.search.len();
                self.scroll = 0;
                self.cursor = 0;
            }
            _ => {}
        }
    }

    fn on_toggle_item(&mut self) {
        // Space in Search mode — no-op for VFK manager
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::tui::keys::EntityFocus;
    use std::collections::HashMap;

    fn empty_widget() -> VfkWidget {
        VfkWidget::new(vec![], vec![], HashMap::new())
    }

    #[test]
    fn focus_loci_default() {
        let w = empty_widget();
        let f = w.focus_loci();
        assert_eq!(f.input, InputFocus::None);
        assert_eq!(f.entity, EntityFocus::Overlay);
    }

    #[test]
    fn back_three_level() {
        let mut w = empty_widget();
        w.focus.input = InputFocus::Search;
        w.search = "foo".into();

        w.on_back(); // deactivate search
        assert_eq!(w.focus.input, InputFocus::None);
        assert!(!w.closed);

        w.on_back(); // clear search
        assert!(w.search.is_empty());
        assert!(!w.closed);

        w.on_back(); // close
        assert!(w.closed);
    }

    #[test]
    fn add_item_opens_form() {
        let mut w = empty_widget();
        w.on_add_item();
        assert_eq!(w.view, VfkView::Form);
        assert!(w.form.is_some());
    }

    #[test]
    fn back_from_form_returns_to_list() {
        let mut w = empty_widget();
        w.on_add_item();
        w.on_back();
        assert_eq!(w.view, VfkView::List);
        assert!(w.form.is_none());
        assert!(!w.closed);
    }

    #[test]
    fn search_activates_and_deactivates() {
        let mut w = empty_widget();
        w.on_start_search();
        assert_eq!(w.focus.input, InputFocus::Search);
        w.on_back();
        assert_eq!(w.focus.input, InputFocus::None);
    }
}
