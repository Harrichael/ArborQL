/// Result of pressing Esc on a SelectList.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EscAction {
    /// Search input was active; it has been deactivated (filter text remains).
    SearchDeactivated,
    /// Search filter was non-empty; it has been cleared (cursor/scroll reset).
    SearchCleared,
    /// No search state to unwind; the caller should close the overlay.
    Close,
}

/// Lightweight cursor + scroll + optional search state for a scrollable list.
///
/// `SelectList` does not own items. The caller provides the item count (or
/// filtered count) at each call site.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectList {
    /// Index into the (possibly filtered) list.
    pub cursor: usize,
    /// First visible row; updated by [`visible_window`].
    pub scroll: usize,
    search: String,
    search_active: bool,
    search_supported: bool,
}

impl SelectList {
    /// Create a new SelectList without search support.
    pub fn new() -> Self {
        Self {
            cursor: 0,
            scroll: 0,
            search: String::new(),
            search_active: false,
            search_supported: false,
        }
    }

    /// Create a new SelectList with search support enabled.
    pub fn with_search() -> Self {
        Self {
            search_supported: true,
            ..Self::new()
        }
    }

    // ── Navigation ──────────────────────────────────────────────────────

    /// Move cursor up by 1. Returns true if cursor moved.
    pub fn move_up(&mut self) -> bool {
        if self.cursor > 0 {
            self.cursor -= 1;
            true
        } else {
            false
        }
    }

    /// Move cursor down by 1 within `len` items. Returns true if cursor moved.
    pub fn move_down(&mut self, len: usize) -> bool {
        if self.cursor + 1 < len {
            self.cursor += 1;
            true
        } else {
            false
        }
    }

    /// Move the item at cursor up (for reorder). Returns the pair of indices
    /// that should be swapped, or `None` if already at the top.
    /// Adjusts cursor to follow the moved item.
    pub fn move_item_up(&mut self) -> Option<(usize, usize)> {
        if self.cursor > 0 {
            let pair = (self.cursor - 1, self.cursor);
            self.cursor -= 1;
            Some(pair)
        } else {
            None
        }
    }

    /// Move the item at cursor down (for reorder). Returns the pair of indices
    /// that should be swapped, or `None` if already at the bottom.
    /// Adjusts cursor to follow the moved item.
    pub fn move_item_down(&mut self, len: usize) -> Option<(usize, usize)> {
        if self.cursor + 1 < len {
            let pair = (self.cursor, self.cursor + 1);
            self.cursor += 1;
            Some(pair)
        } else {
            None
        }
    }

    /// Clamp cursor to be within `[0, len)`. Useful after the underlying list
    /// shrinks (e.g. item deleted).
    pub fn clamp_cursor(&mut self, len: usize) {
        if len == 0 {
            self.cursor = 0;
        } else if self.cursor >= len {
            self.cursor = len - 1;
        }
    }

    // ── Search ──────────────────────────────────────────────────────────

    /// Whether search input is currently receiving keystrokes.
    pub fn search_active(&self) -> bool {
        self.search_active
    }

    /// The current search query.
    pub fn search_query(&self) -> &str {
        &self.search
    }

    /// Whether the search bar should be rendered (active or has a non-empty query).
    pub fn has_search_visible(&self) -> bool {
        self.search_supported && (self.search_active || !self.search.is_empty())
    }

    /// Activate search mode. No-op if search is not supported.
    pub fn activate_search(&mut self) {
        if self.search_supported {
            self.search_active = true;
        }
    }

    /// Append a character to the search query. Resets cursor and scroll.
    pub fn search_push(&mut self, ch: char) {
        if self.search_active {
            self.search.push(ch);
            self.cursor = 0;
            self.scroll = 0;
        }
    }

    /// Remove the last character from the search query. Resets cursor and scroll.
    pub fn search_pop(&mut self) {
        if self.search_active {
            self.search.pop();
            self.cursor = 0;
            self.scroll = 0;
        }
    }

    /// Handle Esc with 3-level logic:
    /// 1. If search is active → deactivate it (keep filter text)
    /// 2. If search has a query → clear it (reset cursor/scroll)
    /// 3. Otherwise → return Close
    pub fn handle_esc(&mut self) -> EscAction {
        if self.search_active {
            self.search_active = false;
            EscAction::SearchDeactivated
        } else if !self.search.is_empty() {
            self.search.clear();
            self.scroll = 0;
            self.cursor = 0;
            EscAction::SearchCleared
        } else {
            EscAction::Close
        }
    }

    /// Clear all search state. Call when closing the overlay or switching context.
    pub fn reset_search(&mut self) {
        self.search.clear();
        self.search_active = false;
        self.scroll = 0;
    }

    // ── Rendering helpers ───────────────────────────────────────────────

    /// Update scroll to keep cursor visible within `height` rows, then return
    /// `(skip, take)` for the caller's `.skip().take()` chain.
    pub fn visible_window(&mut self, height: usize) -> (usize, usize) {
        if height == 0 {
            return (0, 0);
        }
        if self.cursor < self.scroll {
            self.scroll = self.cursor;
        } else if self.cursor >= self.scroll + height {
            self.scroll = self.cursor + 1 - height;
        }
        (self.scroll, height)
    }
}

impl Default for SelectList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_up_down_bounds() {
        let mut list = SelectList::new();
        assert!(!list.move_up()); // already at 0
        assert!(list.move_down(5));
        assert_eq!(list.cursor, 1);
        list.cursor = 4;
        assert!(!list.move_down(5)); // at last item
        assert!(list.move_up());
        assert_eq!(list.cursor, 3);
    }

    #[test]
    fn move_down_empty() {
        let mut list = SelectList::new();
        assert!(!list.move_down(0));
    }

    #[test]
    fn clamp_cursor_shrink() {
        let mut list = SelectList::new();
        list.cursor = 10;
        list.clamp_cursor(5);
        assert_eq!(list.cursor, 4);
        list.clamp_cursor(0);
        assert_eq!(list.cursor, 0);
    }

    #[test]
    fn move_item_up_down() {
        let mut list = SelectList::new();
        list.cursor = 2;
        assert_eq!(list.move_item_up(), Some((1, 2)));
        assert_eq!(list.cursor, 1);
        assert_eq!(list.move_item_down(5), Some((1, 2)));
        assert_eq!(list.cursor, 2);
        list.cursor = 0;
        assert_eq!(list.move_item_up(), None);
        list.cursor = 4;
        assert_eq!(list.move_item_down(5), None);
    }

    #[test]
    fn search_not_supported() {
        let mut list = SelectList::new();
        list.activate_search();
        assert!(!list.search_active());
        assert!(!list.has_search_visible());
    }

    #[test]
    fn search_lifecycle() {
        let mut list = SelectList::with_search();
        assert!(!list.search_active());

        list.activate_search();
        assert!(list.search_active());
        assert!(list.has_search_visible());

        list.search_push('a');
        list.search_push('b');
        assert_eq!(list.search_query(), "ab");
        assert_eq!(list.cursor, 0); // reset on push

        list.cursor = 5;
        list.search_pop();
        assert_eq!(list.search_query(), "a");
        assert_eq!(list.cursor, 0); // reset on pop
    }

    #[test]
    fn three_level_esc() {
        let mut list = SelectList::with_search();
        list.activate_search();
        list.search_push('x');

        // Level 1: deactivate search input
        assert_eq!(list.handle_esc(), EscAction::SearchDeactivated);
        assert!(!list.search_active());
        assert_eq!(list.search_query(), "x"); // filter remains

        // Level 2: clear filter
        assert_eq!(list.handle_esc(), EscAction::SearchCleared);
        assert_eq!(list.search_query(), "");
        assert_eq!(list.cursor, 0);

        // Level 3: close
        assert_eq!(list.handle_esc(), EscAction::Close);
    }

    #[test]
    fn esc_no_search() {
        let mut list = SelectList::new();
        assert_eq!(list.handle_esc(), EscAction::Close);
    }

    #[test]
    fn visible_window_scroll() {
        let mut list = SelectList::new();
        list.cursor = 0;
        assert_eq!(list.visible_window(5), (0, 5));

        list.cursor = 7;
        assert_eq!(list.visible_window(5), (3, 5)); // 7 + 1 - 5 = 3

        list.cursor = 2;
        assert_eq!(list.visible_window(5), (2, 5)); // cursor < scroll, snap

        list.cursor = 4;
        assert_eq!(list.visible_window(5), (2, 5)); // still in window, no change
    }

    #[test]
    fn visible_window_zero_height() {
        let mut list = SelectList::new();
        assert_eq!(list.visible_window(0), (0, 0));
    }

    #[test]
    fn reset_search_clears_all() {
        let mut list = SelectList::with_search();
        list.activate_search();
        list.search_push('x');
        list.scroll = 10;
        list.reset_search();
        assert!(!list.search_active());
        assert_eq!(list.search_query(), "");
        assert_eq!(list.scroll, 0);
    }
}
