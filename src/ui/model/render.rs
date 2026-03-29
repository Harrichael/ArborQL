use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Frame,
};

/// Render a search/filter bar. When `active`, a cursor is shown at `cursor`
/// (a byte offset into `query`).
pub fn render_search_bar(f: &mut Frame, area: Rect, query: &str, active: bool, cursor: usize) {
    let (border_color, title) = if active {
        (Color::Yellow, " Search (Esc to stop typing, keep filter) ")
    } else {
        (Color::DarkGray, " Filter active (/ to edit, Esc to clear) ")
    };
    let text_style = Style::default().fg(Color::White).add_modifier(Modifier::BOLD);
    let spans = if active {
        let cursor = cursor.min(query.len());
        let before = &query[..cursor];
        let at = query[cursor..].chars().next()
            .map(|c| c.len_utf8())
            .unwrap_or(0);
        let after = &query[cursor + at..];
        let cursor_char = if at == 0 { " " } else { &query[cursor..cursor + at] };
        vec![
            Span::styled("/ ", Style::default().fg(Color::Yellow)),
            Span::styled(before, text_style),
            Span::styled(cursor_char, Style::default().bg(Color::Yellow).fg(Color::Black)),
            Span::styled(after, text_style),
        ]
    } else {
        vec![
            Span::styled("/ ", Style::default().fg(Color::Yellow)),
            Span::styled(query, text_style),
        ]
    };
    f.render_widget(
        Paragraph::new(Line::from(spans)).block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color)),
        ),
        area,
    );
}

/// Create a centered rectangle as a percentage of the parent area.
pub fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}
