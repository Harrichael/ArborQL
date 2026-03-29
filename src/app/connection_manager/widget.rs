use std::collections::HashMap;

use crate::config::SavedConnection;
use crate::connection_manager::{ConnectionSummary, ConnectionType};
use crate::app::tui::keys::{EntityFocus, FocusLoci, InputFocus};

#[derive(Debug, Clone, PartialEq)]
pub enum ConnManagerView {
    Tabs,
    AddForm,
    AliasPrompt,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConnManagerTab {
    Connections,
    Saved,
    Connectors,
}

pub enum ConnManagerAction {
    None,
    Connect {
        alias: String,
        conn_type: ConnectionType,
        url: String,
        params: HashMap<String, String>,
        inherited_id: Option<String>,
    },
    ToggleConnection(usize),
    RemoveConnection(usize),
    RemoveSaved(String),
    SaveConnection {
        conn_index: usize,
        needs_password_confirm: bool,
    },
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

/// State for the connection creation form.
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

    pub fn is_complete(&self) -> bool {
        self.fields
            .iter()
            .all(|f| !f.required || !f.value.is_empty())
    }

    pub fn values(&self) -> HashMap<String, String> {
        self.fields
            .iter()
            .map(|f| (f.name.clone(), f.value.clone()))
            .collect()
    }

    pub fn alias(&self) -> &str {
        self.fields
            .iter()
            .find(|f| f.name == "alias")
            .map(|f| f.value.as_str())
            .unwrap_or("")
    }
}

pub struct ConnManagerWidget {
    pub view: ConnManagerView,
    pub tab: ConnManagerTab,
    pub cursor: usize,
    pub focus: FocusLoci,
    pub closed: bool,
    pub action: ConnManagerAction,

    pub connections: Vec<ConnectionSummary>,
    pub saved_connections: Vec<SavedConnection>,

    pub form: Option<ConnectionForm>,

    pub alias: String,
    pub alias_saved_index: usize,
}

impl ConnManagerWidget {
    pub fn new(
        connections: Vec<ConnectionSummary>,
        saved_connections: Vec<SavedConnection>,
    ) -> Self {
        Self {
            view: ConnManagerView::Tabs,
            tab: ConnManagerTab::Connections,
            cursor: 0,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Overlay,
            },
            closed: false,
            action: ConnManagerAction::None,
            connections,
            saved_connections,
            form: None,
            alias: String::new(),
            alias_saved_index: 0,
        }
    }

    pub fn tab_item_count(&self) -> usize {
        match self.tab {
            ConnManagerTab::Connections => self.connections.len(),
            ConnManagerTab::Saved => self.saved_connections.len(),
            ConnManagerTab::Connectors => ConnectionType::all().len(),
        }
    }
}
