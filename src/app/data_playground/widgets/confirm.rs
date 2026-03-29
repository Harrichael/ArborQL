use crate::ui::app::ConfirmAction;
use crate::ui::model::control_panel::ControlPanel;
use crate::ui::model::keys::{EntityFocus, FocusLoci, InputFocus};

/// Widget for a Yes/No confirmation dialog.
pub struct ConfirmWidget {
    pub message: String,
    pub tag: ConfirmAction,
    pub focus: FocusLoci,
    pub closed: bool,
    pub confirmed: Option<bool>,
}

impl ConfirmWidget {
    pub fn new(message: String, tag: ConfirmAction) -> Self {
        Self {
            message,
            tag,
            focus: FocusLoci {
                input: InputFocus::None,
                entity: EntityFocus::Confirm,
            },
            closed: false,
            confirmed: None,
        }
    }
}

impl ControlPanel for ConfirmWidget {
    fn focus_loci(&self) -> FocusLoci {
        self.focus
    }

    fn on_confirm_yes(&mut self) {
        self.confirmed = Some(true);
        self.closed = true;
    }

    fn on_confirm_no(&mut self) {
        self.confirmed = Some(false);
        self.closed = true;
    }

    fn on_back(&mut self) {
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn widget() -> ConfirmWidget {
        ConfirmWidget::new(
            "Save?".into(),
            ConfirmAction::SaveConnectionWithPassword { conn_index: 0 },
        )
    }

    #[test]
    fn yes_confirms() {
        let mut w = widget();
        w.on_confirm_yes();
        assert!(w.closed);
        assert_eq!(w.confirmed, Some(true));
    }

    #[test]
    fn no_confirms() {
        let mut w = widget();
        w.on_confirm_no();
        assert!(w.closed);
        assert_eq!(w.confirmed, Some(false));
    }

    #[test]
    fn esc_closes_without_answer() {
        let mut w = widget();
        w.on_back();
        assert!(w.closed);
        assert_eq!(w.confirmed, None);
    }

    #[test]
    fn focus_is_confirm() {
        let w = widget();
        assert_eq!(w.focus_loci().entity, EntityFocus::Confirm);
    }
}
