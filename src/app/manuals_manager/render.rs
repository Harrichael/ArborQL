use ratatui::{
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame,
};

use super::widget::{ManualsView, ManualsWidget, MANUALS};
use crate::app::tui::render::centered_rect;

pub fn render(f: &mut Frame, widget: &mut ManualsWidget) {
    match widget.view {
        ManualsView::List => render_list(f, widget),
        ManualsView::Viewer => render_viewer(f, widget),
    }
}

fn render_list(f: &mut Frame, widget: &ManualsWidget) {
    let area = centered_rect(60, 50, f.area());
    f.render_widget(Clear, area);

    let items: Vec<ListItem> = MANUALS
        .iter()
        .enumerate()
        .map(|(i, (title, _))| {
            let line = Line::from(Span::raw(format!("  {}", title)));
            if i == widget.cursor {
                ListItem::new(line).style(Style::default().bg(Color::Blue).fg(Color::White).add_modifier(Modifier::BOLD))
            } else {
                ListItem::new(line)
            }
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Manuals — ↑↓/jk navigate  Enter open  Esc close ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(list, area);
}

fn render_viewer(f: &mut Frame, widget: &mut ManualsWidget) {
    let Some((title, content)) = MANUALS.get(widget.cursor) else { return };

    let area = centered_rect(85, 85, f.area());
    f.render_widget(Clear, area);

    let inner_height = area.height.saturating_sub(2) as usize;
    widget.viewport_height = Some(inner_height);
    let total_lines = content.lines().count();

    let lines: Vec<Line> = content
        .lines()
        .skip(widget.scroll)
        .take(inner_height)
        .map(md_line_to_ratatui)
        .collect();

    let title_str = format!(
        " {} ({}/{})  ↑↓/jk scroll  Esc back ",
        title,
        widget.scroll + 1,
        total_lines
    );

    let para = Paragraph::new(Text::from(lines))
        .block(
            Block::default()
                .title(title_str)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Cyan)),
        )
        .wrap(Wrap { trim: false });
    f.render_widget(para, area);
}

/// Convert a markdown line into a styled ratatui `Line`.
fn md_line_to_ratatui(raw: &str) -> Line<'static> {
    let s = raw.to_string();
    if s.starts_with("# ") {
        Line::from(Span::styled(
            s[2..].to_string(),
            Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD),
        ))
    } else if s.starts_with("## ") {
        Line::from(Span::styled(
            s[3..].to_string(),
            Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
        ))
    } else if s.starts_with("### ") {
        Line::from(Span::styled(
            s[4..].to_string(),
            Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
        ))
    } else if s.starts_with("```") || s == "```" {
        Line::from(Span::styled(s, Style::default().fg(Color::DarkGray)))
    } else if s.starts_with("| ") || s.starts_with("|--") || s.starts_with("|---") {
        Line::from(Span::styled(s, Style::default().fg(Color::White)))
    } else if s.starts_with("- ") || s.starts_with("* ") {
        Line::from(vec![
            Span::styled("• ", Style::default().fg(Color::Yellow)),
            Span::raw(s[2..].to_string()),
        ])
    } else {
        Line::from(Span::raw(s))
    }
}
