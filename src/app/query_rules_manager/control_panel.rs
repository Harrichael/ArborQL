use super::widget::RulesWidget;
use crate::app::tui::control_panel::ControlPanel;
use crate::app::tui::keys::FocusLoci;

fn push_undo(w: &mut RulesWidget) {
    w.undo.push((w.rules.clone(), w.cursor, w.next_cursor));
    w.redo.clear();
}

impl ControlPanel for RulesWidget {
    fn focus_loci(&self) -> FocusLoci {
        self.focus
    }

    fn on_navigate_up(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    fn on_navigate_down(&mut self) {
        if self.cursor + 1 < self.rules.len() {
            self.cursor += 1;
        }
    }

    fn on_move_item_up(&mut self) {
        if self.cursor > 0 {
            push_undo(self);
            self.rules.swap(self.cursor, self.cursor - 1);
            self.cursor -= 1;
        }
    }

    fn on_move_item_down(&mut self) {
        if self.cursor + 1 < self.rules.len() {
            push_undo(self);
            self.rules.swap(self.cursor, self.cursor + 1);
            self.cursor += 1;
        }
    }

    fn on_remove(&mut self) {
        if !self.rules.is_empty() {
            push_undo(self);
            self.rules.remove(self.cursor);
            if self.rules.is_empty() {
                self.cursor = 0;
                self.next_cursor = 0;
            } else if self.cursor >= self.rules.len() {
                self.cursor = self.rules.len() - 1;
            }
            self.next_cursor = self.next_cursor.min(self.rules.len());
        }
    }

    fn on_insert_before(&mut self) {
        self.next_cursor = self.cursor.min(self.rules.len());
    }

    fn on_insert_after(&mut self) {
        self.next_cursor = (self.cursor + 1).min(self.rules.len());
    }

    fn on_undo(&mut self) {
        if let Some((rules, cursor, next_cursor)) = self.undo.pop() {
            self.redo.push((self.rules.clone(), self.cursor, self.next_cursor));
            self.rules = rules;
            self.cursor = cursor.min(self.rules.len().saturating_sub(1));
            self.next_cursor = next_cursor.min(self.rules.len());
        }
    }

    fn on_redo(&mut self) {
        if let Some((rules, cursor, next_cursor)) = self.redo.pop() {
            self.undo.push((self.rules.clone(), self.cursor, self.next_cursor));
            self.rules = rules;
            self.cursor = cursor.min(self.rules.len().saturating_sub(1));
            self.next_cursor = next_cursor.min(self.rules.len());
        }
    }

    fn on_confirm(&mut self) {
        self.confirmed = true;
        self.closed = true;
    }

    fn on_back(&mut self) {
        self.closed = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::Rule;
    use crate::app::tui::keys::{EntityFocus, InputFocus};

    fn filter_rule(table: &str) -> Rule {
        Rule::Filter {
            table: table.to_string(),
            conditions: vec![],
        }
    }

    fn widget(tables: &[&str]) -> RulesWidget {
        let rules: Vec<Rule> = tables.iter().map(|t| filter_rule(t)).collect();
        RulesWidget::new(rules, 0)
    }

    #[test]
    fn focus_loci_is_editable() {
        let w = widget(&["users"]);
        let f = w.focus_loci();
        assert_eq!(f.input, InputFocus::None);
        assert_eq!(f.entity, EntityFocus::Editable);
    }

    #[test]
    fn navigate_clamps() {
        let mut w = widget(&["a", "b", "c"]);
        w.on_navigate_up(); // at 0
        assert_eq!(w.cursor, 0);
        w.on_navigate_down();
        w.on_navigate_down();
        assert_eq!(w.cursor, 2);
        w.on_navigate_down(); // at end
        assert_eq!(w.cursor, 2);
    }

    #[test]
    fn swap_up_down() {
        let mut w = widget(&["a", "b", "c"]);
        w.cursor = 1;
        w.on_move_item_up();
        assert_eq!(w.rules[0].to_string(), "b");
        assert_eq!(w.rules[1].to_string(), "a");
        assert_eq!(w.cursor, 0);

        w.on_move_item_down();
        assert_eq!(w.rules[0].to_string(), "a");
        assert_eq!(w.rules[1].to_string(), "b");
        assert_eq!(w.cursor, 1);
    }

    #[test]
    fn remove_clamps_cursor() {
        let mut w = widget(&["a", "b"]);
        w.cursor = 1;
        w.on_remove();
        assert_eq!(w.rules.len(), 1);
        assert_eq!(w.cursor, 0); // clamped

        w.on_remove();
        assert!(w.rules.is_empty());
        assert_eq!(w.cursor, 0);
        assert_eq!(w.next_cursor, 0);
    }

    #[test]
    fn insert_before_after() {
        let mut w = widget(&["a", "b", "c"]);
        w.cursor = 1;
        w.on_insert_before();
        assert_eq!(w.next_cursor, 1);
        w.on_insert_after();
        assert_eq!(w.next_cursor, 2);
    }

    #[test]
    fn undo_redo() {
        let mut w = widget(&["a", "b", "c"]);
        w.cursor = 0;
        w.on_move_item_down(); // a↔b → [b, a, c], cursor=1
        assert_eq!(w.rules[0].to_string(), "b");

        w.on_undo();
        assert_eq!(w.rules[0].to_string(), "a");
        assert_eq!(w.cursor, 0);

        w.on_redo();
        assert_eq!(w.rules[0].to_string(), "b");
        assert_eq!(w.cursor, 1);
    }

    #[test]
    fn undo_on_remove() {
        let mut w = widget(&["a", "b"]);
        w.cursor = 0;
        w.on_remove(); // removes "a"
        assert_eq!(w.rules.len(), 1);
        assert_eq!(w.rules[0].to_string(), "b");

        w.on_undo();
        assert_eq!(w.rules.len(), 2);
        assert_eq!(w.rules[0].to_string(), "a");
    }

    #[test]
    fn confirm_sets_flags() {
        let mut w = widget(&["a"]);
        w.on_confirm();
        assert!(w.confirmed);
        assert!(w.closed);
    }

    #[test]
    fn back_closes_without_confirm() {
        let mut w = widget(&["a"]);
        w.on_back();
        assert!(w.closed);
        assert!(!w.confirmed);
    }

    #[test]
    fn new_clamps_next_cursor() {
        let w = RulesWidget::new(vec![filter_rule("a")], 100);
        assert_eq!(w.next_cursor, 1); // clamped to rules.len()
    }
}
