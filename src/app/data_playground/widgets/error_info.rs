use crate::ui::model::control_panel::ControlPanel;
use crate::ui::model::keys::{EntityFocus, FocusLoci, InputFocus};

/// Widget for Error and Info overlay messages. Any key dismisses.
pub struct ErrorInfoWidget {
    pub message: String,
    pub is_error: bool,
    pub focus: FocusLoci,
    pub closed: bool,
}

impl ErrorInfoWidget {
    pub fn error(message: String) -> Self {
        Self {
            message,
            is_error: true,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Dismiss,
            },
            closed: false,
        }
    }

    pub fn info(message: String) -> Self {
        Self {
            message,
            is_error: false,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Dismiss,
            },
            closed: false,
        }
    }
}

impl ControlPanel for ErrorInfoWidget {
    fn focus_loci(&self) -> FocusLoci {
        self.focus
    }

    // EntityFocus::Dismiss maps all keys to Back
    fn on_back(&mut self) {
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn any_key_closes_error() {
        let mut w = ErrorInfoWidget::error("oops".into());
        assert!(!w.closed);
        w.on_back(); // Dismiss maps everything to Back
        assert!(w.closed);
    }

    #[test]
    fn any_key_closes_info() {
        let mut w = ErrorInfoWidget::info("done".into());
        w.on_back();
        assert!(w.closed);
    }

    #[test]
    fn focus_is_dismiss() {
        let w = ErrorInfoWidget::error("test".into());
        assert_eq!(w.focus_loci().entity, EntityFocus::Dismiss);
    }
}
