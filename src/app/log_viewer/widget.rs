use crate::log::LogEntry;
use crate::app::tui::keys::{EntityFocus, FocusLoci, InputFocus};

pub struct LogViewerWidget {
    pub logs: Vec<LogEntry>,
    pub cursor: usize,
    pub focus: FocusLoci,
    pub closed: bool,
}

impl LogViewerWidget {
    pub fn new(logs: Vec<LogEntry>) -> Self {
        let cursor = logs.len().saturating_sub(1);
        Self {
            logs,
            cursor,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Overlay,
            },
            closed: false,
        }
    }
}
