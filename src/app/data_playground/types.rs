use std::path::PathBuf;

use crate::connection_manager::ConnectionManager;
use crate::engine;
use crate::rules;
use crate::engine::Engine;

use super::state::AppState;

pub enum TickResult {
    Continue,
    Suspend,
    Quit,
}

pub struct DataPlayground {
    pub state: AppState,
    pub engine: Engine,
    pub conn_mgr: ConnectionManager,
    pub(super) pending_paths: Option<(rules::Rule, Vec<engine::TablePath>)>,
    pub(super) history_file: Option<PathBuf>,
}

/// All possible modes the UI can be in.
#[derive(Debug, Clone, PartialEq)]
pub enum Mode {
    /// Normal navigation mode — also shows the query bar (ready for input).
    Normal,
    /// User is typing a query (rule).
    Query,
    /// User is browsing the command palette (`:` key).
    CommandPalette,
    /// User is being asked to pick among multiple paths.
    PathSelection,
    /// User is doing a reverse-i-search through command history.
    CommandSearch {
        query: String,
        match_cursor: usize,
        saved_input: String,
    },
}

/// Actions that can follow a confirmation dialog.
#[derive(Debug, Clone, PartialEq)]
pub enum ConfirmAction {
    /// Save a single connection — user decides whether to include the password.
    SaveConnectionWithPassword { conn_index: usize },
}

/// Commands available in the command palette (`:` key).
/// Each entry is (name, shortcut key or "", description).
pub const PALETTE_COMMANDS: &[(&str, &str, &str)] = &[
    ("connections", "+", "Connection manager"),
    ("columns",     "c", "Column Manager"),
    ("lattice",     "v", "Manage virtual lattice keys"),
    ("rules",       "r", "Query Rules"),
    ("prune",       "x", "Remove selected node from Data Playground"),
    ("manuals",     "m", "Browse manuals"),
    ("logs",        "l", "View log messages"),
    ("quit",        "q", "Exit application"),
    ("schema",      "s", "Toggle schema sidebar"),
];
