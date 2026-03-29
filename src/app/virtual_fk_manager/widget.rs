use std::collections::HashMap;

use crate::schema::VirtualFkDef;
use crate::app::tui::keys::{EntityFocus, FocusLoci, InputFocus};

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
    pub fn next(&self, type_column_empty: bool) -> Self {
        match self {
            Self::FromTable => Self::IdColumn,
            Self::IdColumn => Self::TypeColumn,
            Self::TypeColumn => {
                if type_column_empty { Self::ToTable } else { Self::TypeValue }
            }
            Self::TypeValue => Self::ToTable,
            Self::ToTable => Self::ToColumn,
            Self::ToColumn => Self::FromTable,
        }
    }

    pub fn prev(&self, type_column_empty: bool) -> Self {
        match self {
            Self::FromTable => Self::ToColumn,
            Self::IdColumn => Self::FromTable,
            Self::TypeColumn => Self::IdColumn,
            Self::TypeValue => Self::TypeColumn,
            Self::ToTable => {
                if type_column_empty { Self::TypeColumn } else { Self::TypeValue }
            }
            Self::ToColumn => Self::ToTable,
        }
    }

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
#[derive(Debug, Clone, PartialEq)]
pub struct VirtualFkForm {
    pub active_field: VirtualFkField,
    pub from_table: String,
    pub id_column: String,
    pub type_column: String,
    pub type_value: String,
    pub to_table: String,
    pub to_column: String,
    pub cursor: usize,
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
            cursor: 0,
            type_options: Vec::new(),
        }
    }

    pub fn is_complete(&self) -> bool {
        !self.from_table.is_empty()
            && !self.id_column.is_empty()
            && !self.to_table.is_empty()
            && !self.to_column.is_empty()
    }
}

impl Default for VirtualFkForm {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum VfkView {
    List,
    Form,
}

pub enum VfkAction {
    None,
    QueryTypeOptions {
        table: String,
        column: String,
    },
    SaveToConfig,
    AddToEngine(VirtualFkDef),
    RemoveFromEngine(usize),
}

pub struct VfkWidget {
    pub view: VfkView,
    pub cursor: usize,
    pub scroll: usize,
    pub search: String,
    pub search_cursor: usize,
    pub focus: FocusLoci,
    pub closed: bool,
    pub action: VfkAction,

    pub virtual_fks: Vec<VirtualFkDef>,
    pub form: Option<VirtualFkForm>,

    // Data for dropdowns (populated by app layer)
    pub display_table_names: Vec<String>,
    pub table_columns: HashMap<String, Vec<String>>,
}

impl VfkWidget {
    pub fn new(
        virtual_fks: Vec<VirtualFkDef>,
        display_table_names: Vec<String>,
        table_columns: HashMap<String, Vec<String>>,
    ) -> Self {
        Self {
            view: VfkView::List,
            cursor: 0,
            scroll: 0,
            search: String::new(),
            search_cursor: 0,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Overlay,
            },
            closed: false,
            action: VfkAction::None,
            virtual_fks,
            form: None,
            display_table_names,
            table_columns,
        }
    }

    /// Return display strings for each VFK, filtered by search.
    pub fn filtered_vfk_indices(&self) -> Vec<usize> {
        let query = self.search.to_lowercase();
        self.virtual_fks
            .iter()
            .enumerate()
            .filter(|(_, vfk)| {
                if query.is_empty() {
                    return true;
                }
                let display = vfk_display_string(vfk);
                display.to_lowercase().contains(&query)
            })
            .map(|(i, _)| i)
            .collect()
    }

    /// Items for the current form dropdown, filtered by search.
    pub fn dropdown_items(&self) -> Vec<String> {
        let form = match &self.form {
            Some(f) => f,
            None => return vec![],
        };
        let all = match form.active_field {
            VirtualFkField::FromTable | VirtualFkField::ToTable => {
                self.display_table_names.clone()
            }
            VirtualFkField::IdColumn => {
                self.table_columns
                    .get(&form.from_table)
                    .cloned()
                    .unwrap_or_default()
            }
            VirtualFkField::TypeColumn => {
                let mut items = vec!["(none — simple FK)".to_string()];
                if let Some(cols) = self.table_columns.get(&form.from_table) {
                    items.extend(cols.iter().cloned());
                }
                items
            }
            VirtualFkField::TypeValue => {
                form.type_options
                    .iter()
                    .map(|(val, count)| format!("{} ({})", val, count))
                    .collect()
            }
            VirtualFkField::ToColumn => {
                self.table_columns
                    .get(&form.to_table)
                    .cloned()
                    .unwrap_or_default()
            }
        };
        if self.search.is_empty() {
            all
        } else {
            let q = self.search.to_lowercase();
            all.into_iter().filter(|s| s.to_lowercase().contains(&q)).collect()
        }
    }
}

pub fn vfk_display_string(vfk: &VirtualFkDef) -> String {
    if let (Some(tc), Some(tv)) = (&vfk.type_column, &vfk.type_value) {
        format!(
            "{}.{} = '{}' → {}.{} (via {}.{})",
            vfk.from_table, tc, tv, vfk.to_table, vfk.to_column,
            vfk.from_table, vfk.id_column
        )
    } else {
        format!(
            "{}.{} → {}.{}",
            vfk.from_table, vfk.id_column, vfk.to_table, vfk.to_column
        )
    }
}
