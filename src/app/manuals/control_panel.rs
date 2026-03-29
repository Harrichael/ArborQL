use super::widget::{ManualsView, ManualsWidget, MANUALS};
use crate::ui::model::control_panel::ControlPanel;
use crate::ui::model::keys::FocusLoci;

impl ControlPanel for ManualsWidget {
    fn focus_loci(&self) -> FocusLoci {
        self.focus
    }

    fn on_navigate_up(&mut self) {
        match self.view {
            ManualsView::List => {
                if self.cursor > 0 {
                    self.cursor -= 1;
                }
            }
            ManualsView::Viewer => {
                if self.scroll > 0 {
                    self.scroll -= 1;
                }
            }
        }
    }

    fn on_navigate_down(&mut self) {
        match self.view {
            ManualsView::List => {
                if self.cursor + 1 < MANUALS.len() {
                    self.cursor += 1;
                }
            }
            ManualsView::Viewer => {
                if self.scroll < self.max_scroll() {
                    self.scroll += 1;
                }
            }
        }
    }

    fn on_confirm(&mut self) {
        if self.view == ManualsView::List {
            self.view = ManualsView::Viewer;
            self.scroll = 0;
        }
    }

    fn on_back(&mut self) {
        match self.view {
            ManualsView::Viewer => {
                self.view = ManualsView::List;
            }
            ManualsView::List => {
                self.closed = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_navigation() {
        let mut w = ManualsWidget::new();
        assert_eq!(w.cursor, 0);

        w.on_navigate_up(); // already at 0
        assert_eq!(w.cursor, 0);

        for _ in 0..MANUALS.len() {
            w.on_navigate_down();
        }
        assert_eq!(w.cursor, MANUALS.len() - 1); // clamped
    }

    #[test]
    fn confirm_opens_viewer() {
        let mut w = ManualsWidget::new();
        w.cursor = 2;
        w.on_confirm();
        assert_eq!(w.view, ManualsView::Viewer);
        assert_eq!(w.scroll, 0);
        assert_eq!(w.cursor, 2); // preserved
    }

    #[test]
    fn back_from_viewer_returns_to_list() {
        let mut w = ManualsWidget::new();
        w.cursor = 1;
        w.on_confirm(); // enter viewer
        w.on_back();
        assert_eq!(w.view, ManualsView::List);
        assert_eq!(w.cursor, 1); // preserved
        assert!(!w.closed);
    }

    #[test]
    fn back_from_list_closes() {
        let mut w = ManualsWidget::new();
        w.on_back();
        assert!(w.closed);
    }

    #[test]
    fn viewer_scroll_with_viewport() {
        let mut w = ManualsWidget::new();
        w.on_confirm(); // enter viewer for first manual
        w.viewport_height = Some(10);
        let line_count = w.manual_line_count();
        assert!(line_count > 10);

        w.on_navigate_up(); // already at 0
        assert_eq!(w.scroll, 0);

        w.on_navigate_down();
        assert_eq!(w.scroll, 1);

        // Scroll to end
        for _ in 0..line_count + 10 {
            w.on_navigate_down();
        }
        // Stops when last line fills the bottom of viewport
        assert_eq!(w.scroll, line_count - 10);
    }

    #[test]
    fn focus_loci_is_overlay() {
        use crate::ui::model::keys::{EntityFocus, InputFocus};
        let w = ManualsWidget::new();
        let f = w.focus_loci();
        assert_eq!(f.input, InputFocus::None);
        assert_eq!(f.entity, EntityFocus::Overlay);

        // Focus doesn't change between views
        let mut w = ManualsWidget::new();
        w.on_confirm();
        let f = w.focus_loci();
        assert_eq!(f.input, InputFocus::None);
        assert_eq!(f.entity, EntityFocus::Overlay);
    }

    #[test]
    fn viewer_scroll_without_viewport() {
        let mut w = ManualsWidget::new();
        w.on_confirm();
        let line_count = w.manual_line_count();

        // Without viewport, max_scroll is line_count (saturating_sub(0))
        for _ in 0..line_count + 10 {
            w.on_navigate_down();
        }
        assert_eq!(w.scroll, line_count);
    }
}
