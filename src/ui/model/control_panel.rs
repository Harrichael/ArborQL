use crossterm::event::KeyEvent;

use super::keys::{FocusLoci, UserKeyEvent};

/// A widget's key event handler. All methods have default no-op implementations;
/// widgets implement only the events they care about.
///
/// `focus_loci` has no default — every widget must declare its focus state.
pub trait ControlPanel {
    /// Returns the current focus state for key→event translation.
    /// Called before each key event to determine how raw keys map to semantic events.
    fn focus_loci(&self) -> FocusLoci;

    // ── Global ──────────────────────────────────────────────────────
    fn on_suspend(&mut self) {}
    fn on_back(&mut self) {}
    fn on_confirm(&mut self) {}
    fn on_force_quit(&mut self) {}
    fn on_save(&mut self) {}
    fn on_reverse_search(&mut self) {}
    fn on_navigate_up(&mut self) {}
    fn on_navigate_down(&mut self) {}
    fn on_next_field(&mut self) {}
    fn on_prev_field(&mut self) {}

    // ── Actions ─────────────────────────────────────────────────────
    fn on_quit(&mut self) {}
    fn on_start_search(&mut self) {}
    fn on_remove(&mut self) {}
    fn on_add_item(&mut self) {}
    fn on_insert_before(&mut self) {}
    fn on_insert_after(&mut self) {}
    fn on_undo(&mut self) {}
    fn on_toggle_item(&mut self) {}
    fn on_redo(&mut self) {}
    fn on_load_more(&mut self) {}
    fn on_move_item_up(&mut self) {}
    fn on_move_item_down(&mut self) {}

    // ── Confirm ─────────────────────────────────────────────────────
    fn on_confirm_yes(&mut self) {}
    fn on_confirm_no(&mut self) {}

    // ── Text input ──────────────────────────────────────────────────
    fn on_text_input(&mut self, _key: KeyEvent) {}
}

/// The single place that matches on `UserKeyEvent` and routes to trait methods.
/// Adding a new event variant requires adding a default no-op to `ControlPanel`
/// and a new arm here — the compiler enforces exhaustiveness.
pub fn dispatch(ctrl_panel: &mut dyn ControlPanel, event: UserKeyEvent) {
    match event {
        UserKeyEvent::Suspend => ctrl_panel.on_suspend(),
        UserKeyEvent::Back => ctrl_panel.on_back(),
        UserKeyEvent::Confirm => ctrl_panel.on_confirm(),
        UserKeyEvent::ForceQuit => ctrl_panel.on_force_quit(),
        UserKeyEvent::Save => ctrl_panel.on_save(),
        UserKeyEvent::ReverseSearch => ctrl_panel.on_reverse_search(),
        UserKeyEvent::NavigateUp => ctrl_panel.on_navigate_up(),
        UserKeyEvent::NavigateDown => ctrl_panel.on_navigate_down(),
        UserKeyEvent::NextField => ctrl_panel.on_next_field(),
        UserKeyEvent::PrevField => ctrl_panel.on_prev_field(),
        UserKeyEvent::Quit => ctrl_panel.on_quit(),
        UserKeyEvent::StartSearch => ctrl_panel.on_start_search(),
        UserKeyEvent::Remove => ctrl_panel.on_remove(),
        UserKeyEvent::AddItem => ctrl_panel.on_add_item(),
        UserKeyEvent::InsertBefore => ctrl_panel.on_insert_before(),
        UserKeyEvent::InsertAfter => ctrl_panel.on_insert_after(),
        UserKeyEvent::Undo => ctrl_panel.on_undo(),
        UserKeyEvent::ToggleItem => ctrl_panel.on_toggle_item(),
        UserKeyEvent::Redo => ctrl_panel.on_redo(),
        UserKeyEvent::LoadMore => ctrl_panel.on_load_more(),
        UserKeyEvent::MoveItemUp => ctrl_panel.on_move_item_up(),
        UserKeyEvent::MoveItemDown => ctrl_panel.on_move_item_down(),
        UserKeyEvent::ConfirmYes => ctrl_panel.on_confirm_yes(),
        UserKeyEvent::ConfirmNo => ctrl_panel.on_confirm_no(),
        UserKeyEvent::TextInput(key) => ctrl_panel.on_text_input(key),
    }
}
