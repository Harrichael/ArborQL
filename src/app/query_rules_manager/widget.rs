use crate::rules::Rule;
use crate::app::tui::keys::{EntityFocus, FocusLoci, InputFocus};

pub struct RulesWidget {
    pub rules: Vec<Rule>,
    pub cursor: usize,
    pub next_cursor: usize,
    pub focus: FocusLoci,
    pub confirmed: bool,
    pub closed: bool,
    pub(crate) undo: Vec<(Vec<Rule>, usize, usize)>,
    pub(crate) redo: Vec<(Vec<Rule>, usize, usize)>,
}

impl RulesWidget {
    pub fn new(rules: Vec<Rule>, next_cursor: usize) -> Self {
        let next_cursor = next_cursor.min(rules.len());
        Self {
            rules,
            cursor: 0,
            next_cursor,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Editable,
            },
            confirmed: false,
            closed: false,
            undo: Vec::new(),
            redo: Vec::new(),
        }
    }
}
