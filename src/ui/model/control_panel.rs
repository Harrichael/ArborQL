use crossterm::event::KeyEvent;

use super::keys::UserKeyEvent;

/// A widget's key event handler. All methods have default no-op implementations;
/// widgets implement only the events they care about.
pub trait ControlPanel {
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
pub fn dispatch(panel: &mut dyn ControlPanel, event: UserKeyEvent) {
    match event {
        UserKeyEvent::Suspend => panel.on_suspend(),
        UserKeyEvent::Back => panel.on_back(),
        UserKeyEvent::Confirm => panel.on_confirm(),
        UserKeyEvent::ForceQuit => panel.on_force_quit(),
        UserKeyEvent::Save => panel.on_save(),
        UserKeyEvent::ReverseSearch => panel.on_reverse_search(),
        UserKeyEvent::NavigateUp => panel.on_navigate_up(),
        UserKeyEvent::NavigateDown => panel.on_navigate_down(),
        UserKeyEvent::NextField => panel.on_next_field(),
        UserKeyEvent::PrevField => panel.on_prev_field(),
        UserKeyEvent::Quit => panel.on_quit(),
        UserKeyEvent::StartSearch => panel.on_start_search(),
        UserKeyEvent::Remove => panel.on_remove(),
        UserKeyEvent::AddItem => panel.on_add_item(),
        UserKeyEvent::InsertBefore => panel.on_insert_before(),
        UserKeyEvent::InsertAfter => panel.on_insert_after(),
        UserKeyEvent::Undo => panel.on_undo(),
        UserKeyEvent::ToggleItem => panel.on_toggle_item(),
        UserKeyEvent::Redo => panel.on_redo(),
        UserKeyEvent::LoadMore => panel.on_load_more(),
        UserKeyEvent::MoveItemUp => panel.on_move_item_up(),
        UserKeyEvent::MoveItemDown => panel.on_move_item_down(),
        UserKeyEvent::ConfirmYes => panel.on_confirm_yes(),
        UserKeyEvent::ConfirmNo => panel.on_confirm_no(),
        UserKeyEvent::TextInput(key) => panel.on_text_input(key),
    }
}
