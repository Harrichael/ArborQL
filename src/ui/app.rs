use crate::rules::Rule;
use crate::schema::TablePath;
use std::collections::HashMap;

/// All possible modes the UI can be in.
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// Normal navigation mode.
    Normal,
    /// User is typing a command.
    Command,
    /// User is being asked to pick among multiple paths.
    PathSelection,
    /// User is reordering rules.
    RuleReorder,
    /// Error message displayed.
    Error(String),
    /// Informational message displayed.
    Info(String),
}

/// Working item in column manager overlay.
#[derive(Debug, Clone)]
pub struct ColumnManagerItem {
    pub name: String,
    pub enabled: bool,
}

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
    /// Currently highlighted path index.
    pub path_cursor: usize,
    /// Table names from the schema, for display.
    pub table_names: Vec<String>,
    /// Rules list.
    pub rules: Vec<Rule>,
    /// Selected rule index (for reorder mode).
    pub rule_cursor: usize,
    /// Next insertion position for newly added rules.
    pub next_rule_cursor: usize,
    /// Undo stack for rule reorder mode snapshots: (rules, cursor, next cursor).
    pub rule_reorder_undo: Vec<(Vec<Rule>, usize, usize)>,
    /// Redo stack for rule reorder mode snapshots: (rules, cursor, next cursor).
    pub rule_reorder_redo: Vec<(Vec<Rule>, usize, usize)>,
    /// Whether to show the schema sidebar.
    pub show_schema: bool,
    /// Tree-level visible columns by table.
    pub tree_visible_columns: HashMap<String, Vec<String>>,
    /// Full tree-level column ordering by table (enabled + disabled).
    pub tree_column_order: HashMap<String, Vec<String>>,
    /// Column manager mode: table, editable list (ordered + enabled), cursor.
    pub column_add: Option<(String, Vec<ColumnManagerItem>, usize)>,
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
            path_cursor: 0,
            table_names: Vec::new(),
            rules: Vec::new(),
            rule_cursor: 0,
            next_rule_cursor: 0,
            rule_reorder_undo: Vec::new(),
            rule_reorder_redo: Vec::new(),
            show_schema: false,
            tree_visible_columns: HashMap::new(),
            tree_column_order: HashMap::new(),
            column_add: None,
        }
    }

    /// Move selection up.
    pub fn select_up(&mut self) {
        if self.selected_row > 0 {
            self.selected_row -= 1;
            self.clamp_scroll();
        }
    }

    /// Move selection down.
    pub fn select_down(&mut self) {
        if self.selected_row + 1 < self.visible_row_count {
            self.selected_row += 1;
            self.clamp_scroll();
        }
    }

    fn clamp_scroll(&mut self) {
        // Keep selected row visible
        if self.selected_row < self.scroll_offset {
            self.scroll_offset = self.selected_row;
        }
    }

    /// Insert a character at the cursor.
    pub fn input_char(&mut self, ch: char) {
        self.input.insert(self.cursor, ch);
        self.cursor += 1;
    }

    /// Delete character before cursor.
    pub fn input_backspace(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            self.input.remove(self.cursor);
        }
    }

    /// Delete character at cursor.
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

    /// Clear the input buffer.
    pub fn clear_input(&mut self) {
        self.input.clear();
        self.cursor = 0;
    }

    /// Get text entered so far.
    pub fn input_text(&self) -> &str {
        &self.input
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
