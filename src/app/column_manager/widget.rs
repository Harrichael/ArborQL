use std::collections::HashSet;

use crate::ui::model::keys::{EntityFocus, InputFocus, FocusLoci};

/// Working item in the column manager overlay.
#[derive(Debug, Clone)]
pub struct ColumnManagerItem {
    pub name: String,
    pub enabled: bool,
}

/// Temporary TUI overlay state for the column manager interaction.
/// Created by `ColumnManagerModule::open_widget()`, lives while the overlay is open.
pub struct ColumnManagerWidget {
    pub table: String,
    pub items: Vec<ColumnManagerItem>,
    pub cursor: usize,
    pub search: String,
    pub search_cursor: usize,
    pub focus: FocusLoci,
    pub scroll: usize,
    pub viewport_height: Option<usize>,
    pub confirmed: bool,
    pub closed: bool,
}

impl ColumnManagerWidget {
    pub fn new(table: String, ordered: Vec<String>, visible: Vec<String>) -> Self {
        let shown: HashSet<String> = visible.into_iter().collect();
        let items = ordered
            .into_iter()
            .map(|name| ColumnManagerItem {
                enabled: shown.contains(&name),
                name,
            })
            .collect();
        Self {
            table,
            items,
            cursor: 0,
            search: String::new(),
            search_cursor: 0,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Editable,
            },
            scroll: 0,
            viewport_height: None,
            confirmed: false,
            closed: false,
        }
    }

    /// Indices into `items` that match the current search filter.
    pub(crate) fn filtered_indices(&self) -> Vec<usize> {
        if self.search.is_empty() {
            (0..self.items.len()).collect()
        } else {
            let query = self.search.to_lowercase();
            self.items
                .iter()
                .enumerate()
                .filter(|(_, item)| item.name.to_lowercase().contains(&query))
                .map(|(i, _)| i)
                .collect()
        }
    }

    /// The enabled column names in current order (result after confirm).
    pub fn visible_columns(&self) -> Vec<String> {
        self.items
            .iter()
            .filter(|i| i.enabled)
            .map(|i| i.name.clone())
            .collect()
    }

    /// All column names in current order (result after confirm).
    pub fn column_order(&self) -> Vec<String> {
        self.items.iter().map(|i| i.name.clone()).collect()
    }
}
