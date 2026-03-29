use ratatui::{
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState},
    Frame,
};

use super::widget::LogViewerWidget;
use crate::app::tui::render::centered_rect;

pub fn render(f: &mut Frame, widget: &LogViewerWidget) {
    let area = centered_rect(80, 70, f.area());
    f.render_widget(Clear, area);

    let items: Vec<ListItem> = widget
        .logs
        .iter()
        .enumerate()
        .map(|(i, entry)| {
            let level_color = match entry.level {
                crate::log::LogLevel::Error => Color::Red,
                crate::log::LogLevel::Warn => Color::Yellow,
                crate::log::LogLevel::Info => Color::White,
            };
            let line = Line::from(Span::styled(
                entry.to_string(),
                Style::default().fg(level_color),
            ));
            if i == widget.cursor {
                ListItem::new(line).style(Style::default().bg(Color::DarkGray))
            } else {
                ListItem::new(line)
            }
        })
        .collect();

    let title = if widget.logs.is_empty() {
        " Log History — empty (Esc close) ".to_string()
    } else {
        format!(
            " Log History ({}/{})  ↑↓/jk navigate  Esc close ",
            widget.cursor + 1,
            widget.logs.len()
        )
    };

    let list = List::new(items).block(
        Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow)),
    );

    let inner_height = area.height.saturating_sub(2) as usize;
    let offset = if widget.logs.is_empty() {
        0
    } else if widget.cursor + 1 > inner_height {
        widget.cursor + 1 - inner_height
    } else {
        0
    };

    let mut list_state = ListState::default();
    list_state.select(Some(widget.cursor));
    *list_state.offset_mut() = offset;

    f.render_stateful_widget(list, area, &mut list_state);
}
