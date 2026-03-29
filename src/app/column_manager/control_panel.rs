use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::widget::ColumnManagerWidget;
use crate::ui::model::control_panel::ControlPanel;
use crate::ui::model::keys::{EntityFocus, InputFocus, UserFocusLoci};

/// Move cursor left by one char boundary.
fn move_left(s: &str, cursor: usize) -> usize {
    if cursor == 0 { return 0; }
    let mut pos = cursor - 1;
    while pos > 0 && !s.is_char_boundary(pos) {
        pos -= 1;
    }
    pos
}

/// Move cursor right by one char boundary.
fn move_right(s: &str, cursor: usize) -> usize {
    if cursor >= s.len() { return s.len(); }
    let mut pos = cursor + 1;
    while pos < s.len() && !s.is_char_boundary(pos) {
        pos += 1;
    }
    pos
}

/// Move cursor to start of previous word.
fn move_word_left(s: &str, cursor: usize) -> usize {
    let bytes = s.as_bytes();
    let mut pos = cursor;
    while pos > 0 && !bytes[pos - 1].is_ascii_alphanumeric() {
        pos -= 1;
    }
    while pos > 0 && bytes[pos - 1].is_ascii_alphanumeric() {
        pos -= 1;
    }
    pos
}

/// Move cursor to end of next word.
fn move_word_right(s: &str, cursor: usize) -> usize {
    let bytes = s.as_bytes();
    let len = s.len();
    let mut pos = cursor;
    while pos < len && !bytes[pos].is_ascii_alphanumeric() {
        pos += 1;
    }
    while pos < len && bytes[pos].is_ascii_alphanumeric() {
        pos += 1;
    }
    pos
}

/// Adjust scroll so the cursor stays within the visible viewport.
/// No-op until render has reported the viewport height.
fn clamp_scroll(w: &mut ColumnManagerWidget) {
    if let Some(vh) = w.viewport_height {
        if w.cursor < w.scroll {
            w.scroll = w.cursor;
        } else if w.cursor >= w.scroll + vh {
            w.scroll = w.cursor + 1 - vh;
        }
    }
}

impl ControlPanel for ColumnManagerWidget {
    fn focus_loci(&self) -> UserFocusLoci {
        UserFocusLoci {
            input: if self.search_active { InputFocus::Search } else { InputFocus::None },
            entity: EntityFocus::Editable,
        }
    }

    fn on_navigate_up(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
            clamp_scroll(self);
        }
    }

    fn on_navigate_down(&mut self) {
        let filtered = self.filtered_indices();
        if self.cursor + 1 < filtered.len() {
            self.cursor += 1;
            clamp_scroll(self);
        }
    }

    fn on_move_item_up(&mut self) {
        let filtered = self.filtered_indices();
        if self.cursor > 0 && self.cursor < filtered.len() {
            self.items.swap(filtered[self.cursor], filtered[self.cursor - 1]);
            self.cursor -= 1;
            clamp_scroll(self);
        }
    }

    fn on_move_item_down(&mut self) {
        let filtered = self.filtered_indices();
        if self.cursor + 1 < filtered.len() {
            self.items.swap(filtered[self.cursor], filtered[self.cursor + 1]);
            self.cursor += 1;
            clamp_scroll(self);
        }
    }

    fn on_toggle_item(&mut self) {
        let filtered = self.filtered_indices();
        if self.cursor < filtered.len() {
            let idx = filtered[self.cursor];
            self.items[idx].enabled = !self.items[idx].enabled;
        }
    }

    fn on_remove(&mut self) {
        // x key in Editable context — same as toggle for column manager.
        self.on_toggle_item();
    }

    fn on_start_search(&mut self) {
        self.search_active = true;
    }

    fn on_back(&mut self) {
        if self.search_active {
            self.search_active = false;
        } else if !self.search.is_empty() {
            self.search.clear();
            self.search_cursor = 0;
            self.scroll = 0;
            self.cursor = 0;
        } else {
            self.closed = true;
        }
    }

    fn on_confirm(&mut self) {
        self.confirmed = true;
        self.closed = true;
    }

    fn on_text_input(&mut self, key: KeyEvent) {
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        match key.code {
            KeyCode::Char(c) => {
                self.search.insert(self.search_cursor, c);
                self.search_cursor += c.len_utf8();
                self.scroll = 0;
                self.cursor = 0;
            }
            KeyCode::Backspace => {
                if self.search_cursor > 0 {
                    self.search_cursor = move_left(&self.search, self.search_cursor);
                    self.search.remove(self.search_cursor);
                    self.scroll = 0;
                    self.cursor = 0;
                }
            }
            KeyCode::Delete => {
                if self.search_cursor < self.search.len() {
                    self.search.remove(self.search_cursor);
                    self.scroll = 0;
                    self.cursor = 0;
                }
            }
            KeyCode::Left if ctrl => self.search_cursor = move_word_left(&self.search, self.search_cursor),
            KeyCode::Right if ctrl => self.search_cursor = move_word_right(&self.search, self.search_cursor),
            KeyCode::Left => self.search_cursor = move_left(&self.search, self.search_cursor),
            KeyCode::Right => self.search_cursor = move_right(&self.search, self.search_cursor),
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn widget(names: &[&str]) -> ColumnManagerWidget {
        let ordered: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        let visible = ordered.clone();
        ColumnManagerWidget::new("test_table".into(), ordered, visible)
    }

    #[test]
    fn navigate_clamps_to_bounds() {
        let mut p = widget(&["id", "name", "email"]);
        p.on_navigate_up(); // already at 0
        assert_eq!(p.cursor, 0);
        p.on_navigate_down();
        p.on_navigate_down();
        assert_eq!(p.cursor, 2);
        p.on_navigate_down(); // at end
        assert_eq!(p.cursor, 2);
    }

    #[test]
    fn navigate_respects_search_filter() {
        let mut p = widget(&["id", "name", "email"]);
        p.search = "a".into(); // matches "name"(1), "email"(2)
        p.on_navigate_down();
        assert_eq!(p.cursor, 1); // only 2 filtered items
        p.on_navigate_down();
        assert_eq!(p.cursor, 1); // can't go past end of filtered list
    }

    #[test]
    fn toggle_flips_enabled() {
        let mut p = widget(&["id", "name"]);
        assert!(p.items[0].enabled);
        p.on_toggle_item();
        assert!(!p.items[0].enabled);
        p.on_toggle_item();
        assert!(p.items[0].enabled);
    }

    #[test]
    fn remove_behaves_like_toggle() {
        let mut p = widget(&["id", "name"]);
        p.on_remove();
        assert!(!p.items[0].enabled);
    }

    #[test]
    fn toggle_targets_filtered_item() {
        let mut p = widget(&["id", "name", "email"]);
        p.search = "a".into(); // filtered: name(1), email(2)
        p.cursor = 1; // points to email in filtered view
        p.on_toggle_item();
        assert!(p.items[0].enabled); // id untouched
        assert!(p.items[1].enabled); // name untouched
        assert!(!p.items[2].enabled); // email toggled
    }

    #[test]
    fn move_item_reorders() {
        let mut p = widget(&["id", "name", "email"]);
        p.cursor = 1; // on "name"
        p.on_move_item_up();
        assert_eq!(p.items[0].name, "name");
        assert_eq!(p.items[1].name, "id");
        assert_eq!(p.cursor, 0);
    }

    #[test]
    fn move_item_down_reorders() {
        let mut p = widget(&["id", "name", "email"]);
        p.cursor = 1; // on "name"
        p.on_move_item_down();
        assert_eq!(p.items[1].name, "email");
        assert_eq!(p.items[2].name, "name");
        assert_eq!(p.cursor, 2);
    }

    #[test]
    fn three_level_back() {
        let mut p = widget(&["id"]);
        p.search_active = true;
        p.search = "foo".into();

        p.on_back(); // level 1: deactivate search
        assert!(!p.search_active);
        assert_eq!(p.search, "foo");
        assert!(!p.closed);

        p.on_back(); // level 2: clear search text
        assert!(p.search.is_empty());
        assert_eq!(p.cursor, 0);
        assert!(!p.closed);

        p.on_back(); // level 3: close
        assert!(p.closed);
    }

    #[test]
    fn confirm_sets_flags_and_results() {
        let mut p = widget(&["id", "name", "email"]);
        p.items[1].enabled = false; // disable "name"
        p.on_confirm();
        assert!(p.confirmed);
        assert!(p.closed);
        assert_eq!(p.visible_columns(), vec!["id", "email"]);
        assert_eq!(p.column_order(), vec!["id", "name", "email"]);
    }

    #[test]
    fn text_input_updates_search() {
        let mut p = widget(&["id", "name"]);
        p.search_active = true;

        let char_n = KeyEvent::new(KeyCode::Char('n'), crossterm::event::KeyModifiers::NONE);
        let backspace = KeyEvent::new(KeyCode::Backspace, crossterm::event::KeyModifiers::NONE);

        p.on_text_input(char_n);
        assert_eq!(p.search, "n");

        p.on_text_input(backspace);
        assert!(p.search.is_empty());
    }

    #[test]
    fn start_search_activates() {
        let mut p = widget(&["id"]);
        assert!(!p.search_active);
        p.on_start_search();
        assert!(p.search_active);
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, crossterm::event::KeyModifiers::NONE)
    }

    fn ctrl_key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, crossterm::event::KeyModifiers::CONTROL)
    }

    #[test]
    fn text_insert_at_cursor() {
        let mut p = widget(&["id"]);
        p.on_text_input(key(KeyCode::Char('a')));
        p.on_text_input(key(KeyCode::Char('b')));
        p.on_text_input(key(KeyCode::Char('c')));
        assert_eq!(p.search, "abc");
        assert_eq!(p.search_cursor, 3);

        // Move left, insert in the middle
        p.on_text_input(key(KeyCode::Left));
        p.on_text_input(key(KeyCode::Left));
        assert_eq!(p.search_cursor, 1);
        p.on_text_input(key(KeyCode::Char('x')));
        assert_eq!(p.search, "axbc");
        assert_eq!(p.search_cursor, 2);
    }

    #[test]
    fn text_cursor_left_right_bounds() {
        let mut p = widget(&["id"]);
        // Left on empty does nothing
        p.on_text_input(key(KeyCode::Left));
        assert_eq!(p.search_cursor, 0);

        p.on_text_input(key(KeyCode::Char('a')));
        // Right past end does nothing
        p.on_text_input(key(KeyCode::Right));
        assert_eq!(p.search_cursor, 1);

        p.on_text_input(key(KeyCode::Left));
        assert_eq!(p.search_cursor, 0);
    }

    #[test]
    fn text_backspace_at_cursor() {
        let mut p = widget(&["id"]);
        p.search = "abcd".into();
        p.search_cursor = 2; // between b and c

        p.on_text_input(key(KeyCode::Backspace));
        assert_eq!(p.search, "acd");
        assert_eq!(p.search_cursor, 1);

        // Backspace at start does nothing
        p.search_cursor = 0;
        p.on_text_input(key(KeyCode::Backspace));
        assert_eq!(p.search, "acd");
        assert_eq!(p.search_cursor, 0);
    }

    #[test]
    fn text_delete_at_cursor() {
        let mut p = widget(&["id"]);
        p.search = "abcd".into();
        p.search_cursor = 1; // on 'b'

        p.on_text_input(key(KeyCode::Delete));
        assert_eq!(p.search, "acd");
        assert_eq!(p.search_cursor, 1); // cursor stays

        // Delete at end does nothing
        p.search_cursor = p.search.len();
        p.on_text_input(key(KeyCode::Delete));
        assert_eq!(p.search, "acd");
    }

    #[test]
    fn text_ctrl_left_right_word_movement() {
        let mut p = widget(&["id"]);
        p.search = "foo bar baz".into();
        p.search_cursor = p.search.len(); // at end

        p.on_text_input(ctrl_key(KeyCode::Left)); // to start of "baz"
        assert_eq!(p.search_cursor, 8);

        p.on_text_input(ctrl_key(KeyCode::Left)); // to start of "bar"
        assert_eq!(p.search_cursor, 4);

        p.on_text_input(ctrl_key(KeyCode::Left)); // to start of "foo"
        assert_eq!(p.search_cursor, 0);

        p.on_text_input(ctrl_key(KeyCode::Left)); // already at start
        assert_eq!(p.search_cursor, 0);

        p.on_text_input(ctrl_key(KeyCode::Right)); // to end of "foo"
        assert_eq!(p.search_cursor, 3);

        p.on_text_input(ctrl_key(KeyCode::Right)); // to end of "bar"
        assert_eq!(p.search_cursor, 7);

        p.on_text_input(ctrl_key(KeyCode::Right)); // to end of "baz"
        assert_eq!(p.search_cursor, 11);
    }

    #[test]
    fn scroll_clamps_on_navigate() {
        let mut p = widget(&["a", "b", "c", "d", "e"]);
        p.viewport_height = Some(2);

        p.on_navigate_down(); // cursor=1, scroll should stay 0
        assert_eq!(p.cursor, 1);
        assert_eq!(p.scroll, 0);

        p.on_navigate_down(); // cursor=2, should scroll
        assert_eq!(p.cursor, 2);
        assert_eq!(p.scroll, 1);

        p.on_navigate_up();
        p.on_navigate_up(); // cursor=0, scroll should follow
        assert_eq!(p.cursor, 0);
        assert_eq!(p.scroll, 0);
    }

    #[test]
    fn scroll_noop_without_viewport() {
        let mut p = widget(&["a", "b", "c"]);
        // viewport_height is None
        p.on_navigate_down();
        p.on_navigate_down();
        assert_eq!(p.cursor, 2);
        assert_eq!(p.scroll, 0); // no clamping happened
    }
}
