use super::widget::LogViewerWidget;
use crate::ui::model::control_panel::ControlPanel;
use crate::ui::model::keys::FocusLoci;

impl ControlPanel for LogViewerWidget {
    fn focus_loci(&self) -> FocusLoci {
        self.focus
    }

    fn on_navigate_up(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    fn on_navigate_down(&mut self) {
        if self.cursor + 1 < self.logs.len() {
            self.cursor += 1;
        }
    }

    fn on_back(&mut self) {
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::{LogEntry, LogLevel};
    use crate::ui::model::keys::{EntityFocus, InputFocus};

    fn entry(msg: &str) -> LogEntry {
        LogEntry { level: LogLevel::Info, message: msg.to_string(), timestamp: 0 }
    }

    #[test]
    fn focus_loci_is_overlay() {
        let w = LogViewerWidget::new(vec![entry("a")]);
        let f = w.focus_loci();
        assert_eq!(f.input, InputFocus::None);
        assert_eq!(f.entity, EntityFocus::Overlay);
    }

    #[test]
    fn starts_at_last_entry() {
        let w = LogViewerWidget::new(vec![entry("a"), entry("b"), entry("c")]);
        assert_eq!(w.cursor, 2);
    }

    #[test]
    fn navigate_clamps() {
        let mut w = LogViewerWidget::new(vec![entry("a"), entry("b")]);
        assert_eq!(w.cursor, 1);
        w.on_navigate_down(); // at end
        assert_eq!(w.cursor, 1);
        w.on_navigate_up();
        assert_eq!(w.cursor, 0);
        w.on_navigate_up(); // at start
        assert_eq!(w.cursor, 0);
    }

    #[test]
    fn back_closes() {
        let mut w = LogViewerWidget::new(vec![]);
        w.on_back();
        assert!(w.closed);
    }
}
