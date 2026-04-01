use crate::command_history::CommandHistory;
use crate::connection_manager::ConnectionType;
use crate::rules::Rule;
use crate::engine::TablePath;
use crate::schema::{self, VirtualFkDef};
use crate::ui::select_list::SelectList;
use std::collections::HashMap;

/// Which field is active in the virtual FK creation form.
#[derive(Debug, Clone, PartialEq)]
pub enum VirtualFkField {
    FromTable,
    IdColumn,
    TypeColumn,
    TypeValue,
    ToTable,
    ToColumn,
}

impl VirtualFkField {
    /// Return the next field in Tab order.
    pub fn next(&self, type_column_empty: bool) -> Self {
        match self {
            Self::FromTable => Self::IdColumn,
            Self::IdColumn => Self::TypeColumn,
            // Skip TypeValue if no type_column is set
            Self::TypeColumn => {
                if type_column_empty { Self::ToTable } else { Self::TypeValue }
            }
            Self::TypeValue => Self::ToTable,
            Self::ToTable => Self::ToColumn,
            Self::ToColumn => Self::FromTable,
        }
    }

    /// Return the previous field in Shift+Tab order.
    pub fn prev(&self, type_column_empty: bool) -> Self {
        match self {
            Self::FromTable => Self::ToColumn,
            Self::IdColumn => Self::FromTable,
            Self::TypeColumn => Self::IdColumn,
            // Skip TypeValue if no type_column is set
            Self::TypeValue => Self::TypeColumn,
            Self::ToTable => {
                if type_column_empty { Self::TypeColumn } else { Self::TypeValue }
            }
            Self::ToColumn => Self::ToTable,
        }
    }

    /// Human-readable label for the field.
    pub fn label(&self) -> &'static str {
        match self {
            Self::FromTable => "from_table",
            Self::IdColumn => "id_column",
            Self::TypeColumn => "type_column",
            Self::TypeValue => "type_value",
            Self::ToTable => "to_table",
            Self::ToColumn => "to_column",
        }
    }
}

/// State for the virtual FK creation form.
///
/// A single-screen wizard where Tab/Shift+Tab moves between fields.
/// All fields are visible at once; the active field shows a dropdown list.
/// `type_column` and `type_value` are optional (empty = no discriminator).
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualFkForm {
    /// Currently active field (receiving input / showing dropdown).
    pub active_field: VirtualFkField,
    /// Selected from_table value (empty = not yet chosen).
    pub from_table: String,
    /// Selected id_column value (empty = not yet chosen).
    pub id_column: String,
    /// Selected type_column value (empty = no discriminator — simple FK).
    pub type_column: String,
    /// Selected type_value (empty = not applicable or not chosen).
    pub type_value: String,
    /// Selected to_table value (empty = not yet chosen).
    pub to_table: String,
    /// Selected to_column value (empty = not yet chosen).
    pub to_column: String,
    /// Selection state for the active field's dropdown list.
    pub list: SelectList,
    /// Live type-value options loaded from the DB when TypeValue is active.
    pub type_options: Vec<(String, i64)>,
}

impl VirtualFkForm {
    pub fn new() -> Self {
        Self {
            active_field: VirtualFkField::FromTable,
            from_table: String::new(),
            id_column: String::new(),
            type_column: String::new(),
            type_value: String::new(),
            to_table: String::new(),
            to_column: String::new(),
            list: SelectList::with_search(),
            type_options: Vec::new(),
        }
    }

    /// Returns `true` when all required fields are filled.
    pub fn is_complete(&self) -> bool {
        !self.from_table.is_empty()
            && !self.id_column.is_empty()
            && !self.to_table.is_empty()
            && !self.to_column.is_empty()
    }

    /// Build a `VirtualFkDef` from the form's current field values.
    /// Empty type_column/type_value are mapped to `None`.
    pub fn to_vfk_def(&self) -> schema::VirtualFkDef {
        VirtualFkDef {
            from_table: self.from_table.clone(),
            type_column: if self.type_column.is_empty() { None } else { Some(self.type_column.clone()) },
            type_value: if self.type_value.is_empty() { None } else { Some(self.type_value.clone()) },
            id_column: self.id_column.clone(),
            to_table: self.to_table.clone(),
            to_column: self.to_column.clone(),
        }
    }
}

impl Default for VirtualFkForm {
    fn default() -> Self {
        Self::new()
    }
}

/// All possible modes the UI can be in.
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// Normal navigation mode.
    Normal,
    /// User is typing a command.
    Command,
    /// User is being asked to pick among multiple paths.
    PathSelection { list: SelectList },
    /// User is reordering rules.
    RuleReorder { list: SelectList },
    /// Error message displayed.
    Error(String),
    /// Informational message displayed.
    Info(String),
    /// User is managing virtual FK definitions.
    VirtualFkManager { list: SelectList },
    /// User is filling the virtual FK creation form (single-screen, Tab-navigable).
    VirtualFkAdd(VirtualFkForm),
    /// User is viewing the internal log history.
    LogViewer { list: SelectList },
    /// User is browsing the list of available manuals.
    ManualList { list: SelectList },
    /// User is reading a specific manual (index into MANUALS slice, scroll offset).
    ManualView { index: usize, scroll: usize },
    /// User is doing a reverse-i-search through command history.
    CommandSearch {
        /// The search query typed so far.
        query: String,
        /// How many times Ctrl+R has been pressed to scan further back.
        match_cursor: usize,
        /// Input buffer saved before entering search mode (restored on Esc).
        saved_input: String,
    },
    /// Confirmation dialog: user must pick y/n.
    Confirm {
        message: String,
        /// What to do on Yes/No — stored as an opaque tag the handler interprets.
        tag: ConfirmAction,
    },
    /// User is browsing the connection manager.
    ConnectionManager {
        tab: ConnectionManagerTab,
        list: SelectList,
    },
    /// User is filling the connection creation form.
    ConnectionAdd(ConnectionForm),
    /// User is entering an alias for a saved connection before connecting.
    SavedConnectionAlias {
        /// Index into `state.saved_connections`.
        saved_index: usize,
        /// The alias being typed.
        alias: String,
    },
}

/// Actions that can follow a confirmation dialog.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfirmAction {
    /// Save a single connection — user decides whether to include the password.
    SaveConnectionWithPassword { conn_index: usize },
}

/// Which tab is active in the connection manager.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectionManagerTab {
    /// List of active/disconnected connections.
    Connections,
    /// Saved connection configs (need alias before connecting).
    Saved,
    /// List of connector types (to start a wizard).
    Connectors,
}

/// State for the connection creation form (single-screen, Tab-navigable fields).
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionForm {
    pub conn_type: ConnectionType,
    pub fields: Vec<ConnectionFormField>,
    pub active_field: usize,
}

impl ConnectionForm {
    pub fn new(conn_type: ConnectionType) -> Self {
        let defs = conn_type.fields();
        let fields = defs
            .into_iter()
            .map(|d| ConnectionFormField {
                name: d.name,
                label: d.label,
                value: String::new(),
                placeholder: d.placeholder,
                required: d.required,
            })
            .collect();
        Self {
            conn_type,
            fields,
            active_field: 0,
        }
    }

    /// Returns true when all required fields have values.
    pub fn is_complete(&self) -> bool {
        self.fields
            .iter()
            .all(|f| !f.required || !f.value.is_empty())
    }

    /// Collect field values into a HashMap for URL building.
    pub fn values(&self) -> std::collections::HashMap<String, String> {
        self.fields
            .iter()
            .map(|f| (f.name.clone(), f.value.clone()))
            .collect()
    }

    /// Get the alias field value.
    pub fn alias(&self) -> &str {
        self.fields
            .iter()
            .find(|f| f.name == "alias")
            .map(|f| f.value.as_str())
            .unwrap_or("")
    }
}

/// A single field in the connection creation form.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectionFormField {
    pub name: String,
    pub label: String,
    pub value: String,
    pub placeholder: String,
    pub required: bool,
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
    /// True when the search found more paths than returned.
    pub paths_has_more: bool,
    /// Depth to resume pathfinding from when `paths_has_more` is true.
    pub paths_next_depth: usize,
    /// Table names from the schema, for display.
    pub table_names: Vec<String>,
    /// Rules list.
    pub rules: Vec<Rule>,
    /// Next insertion position for newly added rules.
    pub next_rule_cursor: usize,
    /// Undo stack for rule reorder mode snapshots: (rules, cursor, next cursor).
    pub rule_reorder_undo: Vec<(Vec<Rule>, usize, usize)>,
    /// Redo stack for rule reorder mode snapshots: (rules, cursor, next cursor).
    pub rule_reorder_redo: Vec<(Vec<Rule>, usize, usize)>,
    /// Whether to show the schema sidebar.
    pub show_schema: bool,
    /// Column names per table, for command completion hints.
    pub table_columns: HashMap<String, Vec<String>>,
    /// Tree-level visible columns by table.
    pub tree_visible_columns: HashMap<String, Vec<String>>,
    /// Full tree-level column ordering by table (enabled + disabled).
    pub tree_column_order: HashMap<String, Vec<String>>,
    /// Column manager mode: table, editable list (ordered + enabled), selection state.
    pub column_add: Option<(String, Vec<ColumnManagerItem>, SelectList)>,
    /// Config-driven default visible columns.
    pub default_visible_columns: Vec<String>,
    /// Config-driven table-specific default visible columns.
    pub default_visible_columns_by_table: HashMap<String, Vec<String>>,
    /// Virtual FK definitions managed by the user.
    pub virtual_fks: Vec<VirtualFkDef>,
    /// Internal log history (warnings, errors, info messages).
    pub logs: Vec<crate::log::LogEntry>,
    /// Entered command history (append-only).
    pub command_history: CommandHistory,
    /// Index into `command_history` while browsing with Up/Down (None = not browsing).
    pub history_cursor: Option<usize>,
    /// Input buffer saved when the user first enters history-browsing mode
    /// (restored when they press Down past the most recent entry).
    pub history_draft: String,
    /// Set to true by the key handler to request a Ctrl+Z terminal suspend.
    pub should_suspend: bool,
    /// Connection summaries for the connection manager overlay.
    pub connections_summary: Vec<crate::connection_manager::ConnectionSummary>,
    /// Saved connection configs from the config file.
    pub saved_connections: Vec<crate::config::SavedConnection>,
    /// Fully-qualified table names for display (always prefixed when multi-connection).
    pub display_table_names: Vec<String>,
    /// Maps engine table names to display-qualified names (e.g. "users" → "ecommerce.users").
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
            table_names: Vec::new(),
            rules: Vec::new(),
            next_rule_cursor: 0,
            rule_reorder_undo: Vec::new(),
            rule_reorder_redo: Vec::new(),
            show_schema: false,
            table_columns: HashMap::new(),
            tree_visible_columns: HashMap::new(),
            tree_column_order: HashMap::new(),
            column_add: None,
            default_visible_columns: vec![],
            default_visible_columns_by_table: HashMap::new(),
            virtual_fks: Vec::new(),
            logs: Vec::new(),
            command_history: CommandHistory::new(),
            history_cursor: None,
            history_draft: String::new(),
            should_suspend: false,
            connections_summary: Vec::new(),
            saved_connections: Vec::new(),
            display_table_names: Vec::new(),
            display_name_map: HashMap::new(),
        }
    }

    /// Return table names for command completion: includes both engine names
    /// and display-qualified names (deduplicated, sorted).
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

    /// Return the display-qualified form of a table name.
    pub fn display_name<'a>(&'a self, table: &'a str) -> &'a str {
        self.display_name_map
            .get(table)
            .map(|s| s.as_str())
            .unwrap_or(table)
    }

    pub fn configured_defaults_for_table(&self, table: &str) -> &[String] {
        self.default_visible_columns_by_table
            .get(table)
            .unwrap_or(&self.default_visible_columns)
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

    /// Navigate to an older history entry (Up arrow behaviour in Command mode).
    ///
    /// Saves the current draft input on the first call so it can be restored
    /// with [`history_down`] later.
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
            _ => {} // already at oldest entry
        }
    }

    /// Navigate to a newer history entry, or restore the saved draft when the
    /// user moves past the most recent entry (Down arrow behaviour in Command mode).
    pub fn history_down(&mut self) {
        match self.history_cursor {
            None => {} // not currently browsing history
            Some(i) => {
                let len = self.command_history.len();
                if i + 1 < len {
                    self.history_cursor = Some(i + 1);
                    self.input = self.command_history.entries()[i + 1].text.clone();
                    self.cursor = self.input.len();
                } else {
                    // Past the end: restore the draft the user was typing.
                    self.history_cursor = None;
                    self.input = self.history_draft.clone();
                    self.cursor = self.input.len();
                }
            }
        }
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
