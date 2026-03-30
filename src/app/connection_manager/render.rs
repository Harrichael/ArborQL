use ratatui::{
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
    Frame,
};

use super::widget::{ConnManagerTab, ConnManagerView, ConnManagerWidget};
use crate::connection_manager::{ConnectionStatus, ConnectionType};
use crate::app::tui::render::centered_rect;

pub fn render(f: &mut Frame, widget: &ConnManagerWidget) {
    match widget.view {
        ConnManagerView::Tabs => render_tabs(f, widget),
        ConnManagerView::AddForm => render_add_form(f, widget),
        ConnManagerView::AliasPrompt => render_alias_prompt(f, widget),
    }
}

fn render_tabs(f: &mut Frame, widget: &ConnManagerWidget) {
    let area = centered_rect(70, 60, f.area());
    f.render_widget(Clear, area);

    let block = Block::default()
        .title(" Connection Manager  (←→/Tab: switch tab · ↑↓: navigate · Enter: select · Ctrl+S: save · Esc: close) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(inner);

    let tab_area = sections[0];
    let list_area = sections[1];

    let conn_style = if widget.tab == ConnManagerTab::Connections {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let saved_style = if widget.tab == ConnManagerTab::Saved {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let types_style = if widget.tab == ConnManagerTab::Connectors {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let tab_line = Line::from(vec![
        Span::styled("  [Connections]", conn_style),
        Span::raw("    "),
        Span::styled("[Saved]", saved_style),
        Span::raw("    "),
        Span::styled("[Connectors]", types_style),
    ]);
    f.render_widget(Paragraph::new(vec![tab_line, Line::from(Span::styled(
        "─".repeat(inner.width as usize),
        Style::default().fg(Color::DarkGray),
    ))]), tab_area);

    match widget.tab {
        ConnManagerTab::Connections => {
            let summaries = &widget.connections;
            if summaries.is_empty() {
                let items = vec![ListItem::new("  (no connections — switch to Connectors tab to add one)")
                    .style(Style::default().fg(Color::DarkGray))];
                f.render_widget(List::new(items), list_area);
            } else {
                let max_alias = summaries.iter().map(|s| s.alias.len()).max().unwrap_or(4).max(4);
                let max_type = summaries.iter().map(|s| s.conn_type.len()).max().unwrap_or(4).max(4);
                let avail_url = (list_area.width as usize)
                    .saturating_sub(2 + 2 + 2 + max_alias + 2 + max_type + 2 + 14 + 12);

                let items: Vec<ListItem> = summaries
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        let is_selected = i == widget.cursor;

                        let (status_str, status_color) = match &s.status {
                            ConnectionStatus::Connected => ("●", Color::Green),
                            ConnectionStatus::Disconnected => ("○", Color::Red),
                            ConnectionStatus::Error(_) => ("✗", Color::Red),
                        };

                        let url_or_err = match &s.status {
                            ConnectionStatus::Error(msg) => {
                                if msg.len() > avail_url && avail_url > 3 {
                                    format!("{}…", &msg[..avail_url - 1])
                                } else {
                                    msg.clone()
                                }
                            }
                            _ => {
                                let url = &s.url;
                                if url.len() > avail_url && avail_url > 3 {
                                    format!("{}…", &url[..avail_url - 1])
                                } else {
                                    url.clone()
                                }
                            }
                        };

                        let tables_str = if s.status.is_connected() {
                            format!("{} tables", s.table_count)
                        } else if s.last_table_count > 0 {
                            let synced = s.last_synced
                                .map(|t| t.format("%Y-%m-%d %H:%M:%S").to_string())
                                .unwrap_or_else(|| "unknown".into());
                            format!("{} tables last synced {}", s.last_table_count, synced)
                        } else {
                            "—".to_string()
                        };

                        let fg_url = if matches!(&s.status, ConnectionStatus::Error(_)) {
                            Color::Red
                        } else if is_selected {
                            Color::Gray
                        } else {
                            Color::DarkGray
                        };
                        let is_stale = !s.status.is_connected() && s.last_table_count > 0;
                        let fg_tables = if is_stale {
                            Color::Yellow
                        } else if is_selected {
                            Color::Gray
                        } else {
                            Color::DarkGray
                        };

                        let save_indicator = if s.is_saved { "  " } else { "* " };

                        let line = Line::from(vec![
                            Span::raw("  "),
                            Span::styled(status_str, Style::default().fg(status_color)),
                            Span::raw("  "),
                            Span::styled(save_indicator, Style::default().fg(Color::Yellow)),
                            Span::styled(
                                format!("{:<width$}", s.alias, width = max_alias),
                                Style::default().fg(Color::White),
                            ),
                            Span::raw("  "),
                            Span::styled(
                                format!("{:<width$}", s.conn_type, width = max_type),
                                Style::default().fg(Color::Cyan),
                            ),
                            Span::raw("  "),
                            Span::styled(url_or_err, Style::default().fg(fg_url)),
                            Span::raw("  "),
                            Span::styled(tables_str, Style::default().fg(fg_tables)),
                        ]);
                        let item = ListItem::new(line);
                        if is_selected {
                            item.style(Style::default().bg(Color::Blue))
                        } else {
                            item
                        }
                    })
                    .collect();

                let list = List::new(items).block(
                    Block::default()
                        .title_bottom(Line::from(Span::styled(
                            " Enter: toggle connect/disconnect · d/x: remove ",
                            Style::default().fg(Color::DarkGray),
                        )))
                );
                f.render_widget(list, list_area);
            }
        }
        ConnManagerTab::Saved => {
            let saved = &widget.saved_connections;
            if saved.is_empty() {
                let items = vec![ListItem::new("  (no saved connections — use Ctrl+S to save current connections)")
                    .style(Style::default().fg(Color::DarkGray))];
                f.render_widget(List::new(items), list_area);
            } else {
                let items: Vec<ListItem> = saved
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        let type_label = s.conn_type.to_uppercase();
                        let detail = match s.conn_type.as_str() {
                            "sqlite" => {
                                s.params.get("path").cloned().unwrap_or_default()
                            }
                            "mysql" => {
                                let host = s.params.get("host").cloned().unwrap_or_default();
                                let db = s.params.get("database").cloned().unwrap_or_default();
                                let user = s.params.get("user").cloned().unwrap_or_default();
                                format!("{}@{}/{}", user, host, db)
                            }
                            _ => String::new(),
                        };
                        let text = format!("  {:<8} {}", type_label, detail);
                        let item = ListItem::new(text);
                        if i == widget.cursor {
                            item.style(Style::default().bg(Color::Blue).fg(Color::White))
                        } else {
                            item
                        }
                    })
                    .collect();
                let list = List::new(items).block(
                    Block::default()
                        .title_bottom(Line::from(Span::styled(
                            " Enter: connect with alias · d/x: remove ",
                            Style::default().fg(Color::DarkGray),
                        )))
                );
                f.render_widget(list, list_area);
            }
        }
        ConnManagerTab::Connectors => {
            let types = ConnectionType::all();
            let items: Vec<ListItem> = types
                .iter()
                .enumerate()
                .map(|(i, ct)| {
                    let text = format!("  {} — create a new {} connection", ct.label(), ct.label());
                    let item = ListItem::new(text);
                    if i == widget.cursor {
                        item.style(Style::default().bg(Color::Blue).fg(Color::White))
                    } else {
                        item
                    }
                })
                .collect();
            let list = List::new(items).block(
                Block::default()
                    .title_bottom(Line::from(Span::styled(
                        " Enter: start connection wizard ",
                        Style::default().fg(Color::DarkGray),
                    )))
            );
            f.render_widget(list, list_area);
        }
    }
}

fn render_add_form(f: &mut Frame, widget: &ConnManagerWidget) {
    let Some(ref form) = widget.form else { return };

    let area = centered_rect(60, 60, f.area());
    f.render_widget(Clear, area);

    let complete = form.is_complete();
    let hint = if complete {
        " Enter/Ctrl+S: connect  "
    } else {
        " Fill required fields then press Enter  "
    };

    let block = Block::default()
        .title(format!(
            " New {} Connection  (Tab/Shift+Tab: field · Enter: connect · Esc: cancel) ",
            form.conn_type.label()
        ))
        .title_bottom(Line::from(Span::styled(hint, Style::default().fg(Color::DarkGray))))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let active_label_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    let label_style = Style::default().fg(Color::White);
    let value_style = Style::default().fg(Color::Green);
    let active_value_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    let placeholder_style = Style::default().fg(Color::DarkGray);
    let optional_label_style = Style::default().fg(Color::Cyan);

    let lines: Vec<Line> = form
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| {
            let is_active = i == form.active_field;
            let cursor_str = if is_active { "▶ " } else { "  " };
            let req_tag = if !field.required { " (opt)" } else { "" };
            let label_text = format!("{:<12}{}", field.label, req_tag);

            let ls = if is_active {
                active_label_style
            } else if !field.required {
                optional_label_style
            } else {
                label_style
            };

            let (display_val, vs) = if field.value.is_empty() {
                (field.placeholder.clone(), placeholder_style)
            } else if is_active {
                (format!("{}▌", field.value), active_value_style)
            } else {
                let display = if field.name == "password" {
                    "•".repeat(field.value.len())
                } else {
                    field.value.clone()
                };
                (display, value_style)
            };

            Line::from(vec![
                Span::raw(cursor_str.to_string()),
                Span::styled(format!("{}: ", label_text), ls),
                Span::styled(display_val, vs),
            ])
        })
        .collect();

    f.render_widget(Paragraph::new(lines), inner);
}

fn render_alias_prompt(f: &mut Frame, widget: &ConnManagerWidget) {
    let area = centered_rect(50, 20, f.area());
    f.render_widget(Clear, area);

    let block = Block::default()
        .title(" Enter alias for this connection (Enter: connect · Esc: cancel) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let line = Line::from(vec![
        Span::styled("  Alias: ", Style::default().fg(Color::White)),
        Span::styled(widget.alias.clone(), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::styled("▌", Style::default().fg(Color::Yellow)),
    ]);
    f.render_widget(Paragraph::new(line), inner);
}
