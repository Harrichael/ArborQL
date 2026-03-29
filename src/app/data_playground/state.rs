use crate::command_history::CommandHistory;
use crate::engine::TablePath;
use crate::schema::VirtualFkDef;
use std::collections::HashMap;

use super::types::Mode;

/// Application state, passed to the renderer.
pub struct AppState {
    pub mode: Mode,
    /// Flat index of the currently selected tree row.
    pub selected_row: usize,
    /// Number of visible rows in last render (for scroll bounds).
    pub visible_row_count: usize,
    /// Vertical scroll offset for the data viewer.
    pub scroll_offset: usize,
    /// Current command input buffer.
    pub input: String,
    /// Cursor position within `input`.
    pub cursor: usize,
    /// Paths presented to the user for selection (PathSelection mode).
    pub paths: Vec<TablePath>,
    /// True when the search found more paths than returned.
    pub paths_has_more: bool,
    /// Depth to resume pathfinding from when `paths_has_more` is true.
    pub paths_next_depth: usize,
    /// Currently highlighted path index.
    pub path_cursor: usize,
    /// Table names from the schema, for display.
    pub table_names: Vec<String>,
    /// Next insertion position for newly added rules.
    pub next_rule_cursor: usize,
    /// Rule reorder overlay state, if open.
    pub rules_reorder: Option<crate::app::query_rules_manager::widget::RulesWidget>,
    /// Whether to show the schema sidebar.
    pub show_schema: bool,
    /// Column names per table, for command completion hints.
    pub table_columns: HashMap<String, Vec<String>>,
    /// Column visibility manager (persistent service).
    pub column_manager: crate::app::column_manager::module::ColumnManagerModule,
    /// Column manager overlay state, if open.
    pub column_add: Option<crate::app::column_manager::widget::ColumnManagerWidget>,
    /// Manuals overlay state, if open.
    pub manuals: Option<crate::app::manuals_manager::widget::ManualsWidget>,
    /// Connection manager overlay state, if open.
    pub conn_manager: Option<crate::app::connection_manager::widget::ConnManagerWidget>,
    /// Virtual FK manager overlay state, if open.
    pub vfk_manager: Option<crate::app::virtual_fk_manager::widget::VfkWidget>,
    /// Log viewer overlay state, if open.
    pub log_viewer: Option<crate::app::log_viewer::widget::LogViewerWidget>,
    /// Error/Info message overlay, if open.
    pub error_info: Option<super::widgets::error_info::ErrorInfoWidget>,
    /// Confirmation dialog overlay, if open.
    pub confirm: Option<super::widgets::confirm::ConfirmWidget>,
    /// Virtual FK definitions managed by the user.
    pub virtual_fks: Vec<VirtualFkDef>,
    /// Internal log history (warnings, errors, info messages).
    pub logs: Vec<crate::log::LogEntry>,
    /// Scroll offset shared by all list overlays (column manager, FK manager, etc.).
    pub overlay_scroll: usize,
    /// Live search/filter string for list overlays. Empty = no filter.
    pub overlay_search: String,
    /// Whether the search input is currently active (accepting keystrokes).
    pub overlay_search_active: bool,
    /// Entered command history (append-only).
    pub command_history: CommandHistory,
    /// Index into `command_history` while browsing with Up/Down (None = not browsing).
    pub history_cursor: Option<usize>,
    /// Input buffer saved when the user first enters history-browsing mode.
    pub history_draft: String,
    /// Connection summaries for the connection manager overlay.
    pub connections_summary: Vec<crate::connection_manager::ConnectionSummary>,
    /// Saved connection configs from the config file.
    pub saved_connections: Vec<crate::config::SavedConnection>,
    /// Fully-qualified table names for display (always prefixed when multi-connection).
    pub display_table_names: Vec<String>,
    /// Maps engine table names to display-qualified names.
    pub display_name_map: HashMap<String, String>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            mode: Mode::Normal,
            selected_row: 0,
            visible_row_count: 0,
            scroll_offset: 0,
            input: String::new(),
            cursor: 0,
            paths: Vec::new(),
            paths_has_more: false,
            paths_next_depth: 1,
            path_cursor: 0,
            table_names: Vec::new(),
            next_rule_cursor: 0,
            rules_reorder: None,
            show_schema: false,
            table_columns: HashMap::new(),
            column_manager: crate::app::column_manager::module::ColumnManagerModule::new(vec![], std::collections::HashMap::new()),
            column_add: None,
            manuals: None,
            conn_manager: None,
            vfk_manager: None,
            log_viewer: None,
            error_info: None,
            confirm: None,
            virtual_fks: Vec::new(),
            logs: Vec::new(),
            overlay_scroll: 0,
            overlay_search: String::new(),
            overlay_search_active: false,
            command_history: CommandHistory::new(),
            history_cursor: None,
            history_draft: String::new(),
            connections_summary: Vec::new(),
            saved_connections: Vec::new(),
            display_table_names: Vec::new(),
            display_name_map: HashMap::new(),
        }
    }

    pub fn completion_table_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.table_names.clone();
        for dn in &self.display_table_names {
            if !names.contains(dn) {
                names.push(dn.clone());
            }
        }
        names.sort();
        names
    }

    pub fn display_name<'a>(&'a self, table: &'a str) -> &'a str {
        self.display_name_map
            .get(table)
            .map(|s| s.as_str())
            .unwrap_or(table)
    }

    pub fn select_up(&mut self) {
        if self.selected_row > 0 {
            self.selected_row -= 1;
            self.clamp_scroll();
        }
    }

    pub fn select_down(&mut self) {
        if self.selected_row + 1 < self.visible_row_count {
            self.selected_row += 1;
            self.clamp_scroll();
        }
    }

    fn clamp_scroll(&mut self) {
        if self.selected_row < self.scroll_offset {
            self.scroll_offset = self.selected_row;
        }
    }

    pub fn input_char(&mut self, ch: char) {
        self.input.insert(self.cursor, ch);
        self.cursor += 1;
    }

    pub fn input_backspace(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            self.input.remove(self.cursor);
        }
    }

    pub fn input_delete(&mut self) {
        if self.cursor < self.input.len() {
            self.input.remove(self.cursor);
        }
    }

    pub fn cursor_left(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    pub fn cursor_right(&mut self) {
        if self.cursor < self.input.len() {
            self.cursor += 1;
        }
    }

    pub fn clear_input(&mut self) {
        self.input.clear();
        self.cursor = 0;
    }

    pub fn history_up(&mut self) {
        let len = self.command_history.len();
        if len == 0 {
            return;
        }
        match self.history_cursor {
            None => {
                self.history_draft = self.input.clone();
                self.history_cursor = Some(len - 1);
                self.input = self.command_history.entries()[len - 1].text.clone();
                self.cursor = self.input.len();
            }
            Some(i) if i > 0 => {
                self.history_cursor = Some(i - 1);
                self.input = self.command_history.entries()[i - 1].text.clone();
                self.cursor = self.input.len();
            }
            _ => {}
        }
    }

    pub fn history_down(&mut self) {
        match self.history_cursor {
            None => {}
            Some(i) => {
                let len = self.command_history.len();
                if i + 1 < len {
                    self.history_cursor = Some(i + 1);
                    self.input = self.command_history.entries()[i + 1].text.clone();
                    self.cursor = self.input.len();
                } else {
                    self.history_cursor = None;
                    self.input = self.history_draft.clone();
                    self.cursor = self.input.len();
                }
            }
        }
    }

    pub fn reset_overlay_search(&mut self) {
        self.overlay_search.clear();
        self.overlay_search_active = false;
        self.overlay_scroll = 0;
    }

    pub fn input_text(&self) -> &str {
        &self.input
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
