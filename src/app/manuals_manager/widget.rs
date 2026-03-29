use crate::app::tui::keys::{EntityFocus, FocusLoci, InputFocus};

/// Embedded manuals: (title, content).
pub const MANUALS: &[(&str, &str)] = &[
    ("Command Querying Syntax", include_str!("../../../manuals/command-syntax.md")),
    ("Data Viewing",            include_str!("../../../manuals/data-viewing.md")),
    ("Reordering Commands",     include_str!("../../../manuals/reordering.md")),
    ("Column Managers",         include_str!("../../../manuals/column-managers.md")),
    ("Virtual Foreign Keys",    include_str!("../../../manuals/virtual-foreign-keys.md")),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManualsView {
    List,
    Viewer,
}

pub struct ManualsWidget {
    pub view: ManualsView,
    pub cursor: usize,
    pub scroll: usize,
    pub viewport_height: Option<usize>,
    pub focus: FocusLoci,
    pub closed: bool,
}

impl ManualsWidget {
    pub fn new() -> Self {
        Self {
            view: ManualsView::List,
            cursor: 0,
            scroll: 0,
            viewport_height: None,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Overlay,
            },
            closed: false,
        }
    }

    pub fn max_scroll(&self) -> usize {
        let line_count = self.manual_line_count();
        let vh = self.viewport_height.unwrap_or(0);
        line_count.saturating_sub(vh)
    }

    pub fn manual_line_count(&self) -> usize {
        MANUALS.get(self.cursor)
            .map(|(_, content)| content.lines().count())
            .unwrap_or(0)
    }
}
