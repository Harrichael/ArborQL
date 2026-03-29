use ratatui::{
    style::{Color, Style},
    widgets::{Block, Borders, Clear, List, ListItem},
    Frame,
};

use super::widget::RulesWidget;
use crate::app::tui::render::centered_rect;

pub fn render(f: &mut Frame, widget: &RulesWidget) {
    let area = centered_rect(60, 50, f.area());
    f.render_widget(Clear, area);

    let mut items: Vec<ListItem> = Vec::new();
    const SLOT_LABEL: &str = "next insertion";

    if widget.rules.is_empty() {
        items.push(ListItem::new(format!("→ {}", SLOT_LABEL)).style(Style::default().fg(Color::DarkGray)));
    } else {
        for (i, r) in widget.rules.iter().enumerate() {
            if i == widget.next_cursor {
                items.push(
                    ListItem::new(format!("→ {}", SLOT_LABEL))
                        .style(Style::default().fg(Color::DarkGray)),
                );
            }

            let text = format!("   {}. {}", i + 1, r);
            let item = ListItem::new(text);
            if i == widget.cursor {
                items.push(item.style(Style::default().bg(Color::Blue).fg(Color::White)));
            } else {
                items.push(item);
            }
        }
        if widget.next_cursor == widget.rules.len() {
            items.push(
                ListItem::new(format!("→ {}", SLOT_LABEL))
                    .style(Style::default().fg(Color::DarkGray)),
            );
        }
    }

    let list = List::new(items).block(
        Block::default()
            .title(" Rules (↑↓ move cursor, u/d swap, x delete, i/o set insertion, z undo, y redo, Enter apply, Esc cancel) ")
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Yellow)),
    );
    f.render_widget(list, area);
}
