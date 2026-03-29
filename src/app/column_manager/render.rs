use ratatui::{
    layout::Rect,
    style::{Color, Style},
    widgets::{Block, Borders, Clear, List, ListItem},
    Frame,
};

use super::widget::ColumnManagerWidget;
use crate::app::tui::keys::InputFocus;
use crate::app::tui::render::{centered_rect, render_search_bar};

pub fn render(f: &mut Frame, widget: &mut ColumnManagerWidget) {
    let area = centered_rect(50, 70, f.area());
    f.render_widget(Clear, area);

    // Reserve last row for search bar
    let is_searching = widget.focus.input == InputFocus::Search;
    let has_search = is_searching || !widget.search.is_empty();
    let list_area = if has_search {
        Rect { height: area.height.saturating_sub(3), ..area }
    } else {
        area
    };

    let inner_height = list_area.height.saturating_sub(2) as usize;

    // Report viewport size so control widget can clamp scroll on input.
    widget.viewport_height = Some(inner_height);

    let filtered = widget.filtered_indices();
    let offset = widget.scroll;

    let list_items: Vec<ListItem> = filtered
        .iter()
        .enumerate()
        .skip(offset)
        .take(inner_height)
        .map(|(fi, &idx)| {
            let col = &widget.items[idx];
            let marker = if col.enabled { "[x]" } else { "[ ]" };
            let item = ListItem::new(format!("{} {}", marker, col.name));
            if fi == widget.cursor {
                item.style(Style::default().bg(Color::Green).fg(Color::Black))
            } else {
                item
            }
        })
        .collect();

    let match_info = if !widget.search.is_empty() {
        format!("  ({} matches)", filtered.len())
    } else {
        String::new()
    };
    let reorder_hint = if widget.search.is_empty() { "  u/d reorder" } else { "" };
    let list = List::new(list_items).block(
        Block::default()
            .title(format!(
                " Columns for '{}'{} (↑↓ nav · space/x toggle{}· /search · Enter apply · Esc) ",
                widget.table, match_info, reorder_hint
            ))
            .borders(Borders::ALL),
    );
    f.render_widget(list, list_area);

    // Search bar
    if has_search {
        let search_area = Rect { y: list_area.y + list_area.height, height: 3, ..area };
        render_search_bar(f, search_area, &widget.search, is_searching, widget.search_cursor);
    }
}
