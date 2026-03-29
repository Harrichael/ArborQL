use crate::engine::{flatten_tree, DataNode};
use crate::rules::{completions_at, Completion};
use crate::ui::app::{AppState, Mode, PALETTE_COMMANDS};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame,
};

/// Main render entry point.
pub fn render(f: &mut Frame, state: &mut AppState, roots: &[DataNode]) {
    let size = f.area();

    // Layout: optional schema sidebar | data viewer | command bar at bottom
    let (schema_area, main_area) = if state.show_schema {
        let horiz = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Length(24), Constraint::Min(0)])
            .split(size);
        (Some(horiz[0]), horiz[1])
    } else {
        (None, size)
    };

    // Split main_area into data viewer + command bar.
    // In Command mode and CommandSearch mode we use an extra row for hints/search.
    let cmd_height: u16 = if matches!(state.mode, Mode::Normal | Mode::Query | Mode::CommandPalette | Mode::CommandSearch { .. }) { 4 } else { 3 };
    let vert = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(cmd_height)])
        .split(main_area);

    let viewer_area = vert[0];
    let cmd_area = vert[1];

    // Render schema sidebar
    if let Some(area) = schema_area {
        render_schema(f, state, area);
    }

    // Render data viewer
    render_data_viewer(f, state, roots, viewer_area);

    // Render command bar
    render_command_bar(f, state, cmd_area);

    // Render overlays
    match &state.mode {
        Mode::PathSelection => render_path_selection(f, state),
        Mode::LogViewer { .. } => render_log_viewer(f, state),
        Mode::Confirm { message, .. } => {
            let message = message.clone();
            render_overlay_message(f, &message, Color::Yellow);
        }
        Mode::Error(msg) => {
            let msg = msg.clone();
            render_overlay_message(f, &format!("Error: {}", msg), Color::Red);
        }
        Mode::Info(msg) => {
            let msg = msg.clone();
            render_overlay_message(f, &msg, Color::Green);
        }
        _ => {}
    }

    // Render column-add overlay
    if state.column_add.is_some() {
        render_column_add(f, state);
    }

    // Render manuals overlay
    if let Some(ref mut widget) = state.manuals {
        crate::app::manuals_manager::render::render(f, widget);
    }

    // Render rule reorder overlay
    if let Some(ref widget) = state.rules_reorder {
        crate::app::query_rules_manager::render::render(f, widget);
    }

    // Render connection manager overlay
    if let Some(ref widget) = state.conn_manager {
        crate::app::connection_manager::render::render(f, widget);
    }

    // Render virtual FK manager overlay
    if let Some(ref widget) = state.vfk_manager {
        crate::app::virtual_fk_manager::render::render(f, widget);
    }
}

fn render_schema(f: &mut Frame, state: &AppState, area: Rect) {
    let items: Vec<ListItem> = state
        .display_table_names
        .iter()
        .map(|t| ListItem::new(Span::raw(t.clone())))
        .collect();
    let list = List::new(items)
        .block(Block::default().title("Schema").borders(Borders::ALL))
        .style(Style::default().fg(Color::Cyan));
    f.render_widget(list, area);
}

fn render_data_viewer(
    f: &mut Frame,
    state: &mut AppState,
    roots: &[DataNode],
    area: Rect,
) {
    let flat = flatten_tree(roots);
    state.visible_row_count = flat.len();

    // Subtract 2 for block borders and 2 for the column detail bar at the bottom.
    let inner_height = area.height.saturating_sub(4) as usize;

    // Adjust scroll so the selected row is always visible
    if state.selected_row >= state.scroll_offset + inner_height {
        state.scroll_offset = state.selected_row + 1 - inner_height;
    }
    if state.selected_row < state.scroll_offset {
        state.scroll_offset = state.selected_row;
    }

    let items: Vec<ListItem> = flat
        .iter()
        .enumerate()
        .skip(state.scroll_offset)
        .take(inner_height)
        .map(|(idx, (depth, node))| {
            let is_selected = idx == state.selected_row;
            let indent = "  ".repeat(*depth);
            let arrow = if !node.children.is_empty() {
                if node.collapsed { "▶ " } else { "▼ " }
            } else {
                "  "
            };
            let summary_cols = state.column_manager.visible_columns(&node.table).to_vec();
            let summary = summary_cols
                .iter()
                .map(|c| {
                    let v = node.row.get(c).map(|v| v.to_string()).unwrap_or_default();
                    format!("{}: {}", c, v)
                })
                .collect::<Vec<_>>()
                .join("  │  ");
            let table_label = format!("[{}]", state.display_name(&node.table));
            let line = Line::from(vec![
                Span::raw(indent),
                Span::raw(arrow),
                Span::styled(table_label, Style::default().fg(Color::Yellow)),
                Span::raw(" "),
                Span::raw(summary),
            ]);
            let item = ListItem::new(line);
            if is_selected {
                item.style(
                    Style::default()
                        .bg(Color::Blue)
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                )
            } else {
                item
            }
        })
        .collect();

    // Show all columns for selected node
    let col_info = if !flat.is_empty() && state.selected_row < flat.len() {
        let (_, node) = flat[state.selected_row];
        let mut all_cols: Vec<String> = node.row.keys().cloned().collect();
        all_cols.sort();
        let cols: Vec<String> = all_cols
            .iter()
            .map(|c| {
                let v = node.row.get(c).map(|v| v.to_string()).unwrap_or_default();
                format!("{}: {}", c, v)
            })
            .collect();
        cols.join("  │  ")
    } else {
        String::new()
    };

    let title = if flat.is_empty() {
        " Data Playground ".to_string()
    } else {
        format!(
            " Data Playground [{}/{}] ",
            state.selected_row + 1,
            flat.len()
        )
    };

    let block = Block::default().title(title).borders(Borders::ALL);

    // Split viewer into list + column detail
    let inner = block.inner(area);
    f.render_widget(block, area);

    let vert = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(2)])
        .split(inner);

    let list = List::new(items);
    f.render_widget(list, vert[0]);

    // Column detail bar
    if !col_info.is_empty() {
        let detail = Paragraph::new(col_info)
            .style(Style::default().fg(Color::DarkGray))
            .wrap(Wrap { trim: true });
        f.render_widget(detail, vert[1]);
    }
}

fn render_command_bar(f: &mut Frame, state: &AppState, area: Rect) {
    if matches!(state.mode, Mode::Normal | Mode::Query) {
        let block = Block::default().title(" Query ").borders(Borders::ALL);
        let inner = block.inner(area);
        f.render_widget(block, area);

        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(inner);

        // Input line (empty in Normal mode, has text in Query mode)
        let cmd_para = Paragraph::new(state.input.as_str())
            .style(Style::default().fg(Color::White));
        f.render_widget(cmd_para, rows[0]);

        // Completion hint line
        let completions = completions_at(&state.input, &state.completion_table_names(), &state.table_columns);
        if !completions.is_empty() {
            let hint = format_completions(&completions);
            let hint_para = Paragraph::new(hint)
                .style(Style::default().fg(Color::DarkGray));
            f.render_widget(hint_para, rows[1]);
        }

        // Show cursor on the input line.
        f.set_cursor_position((
            area.x + 1 + state.cursor as u16, // +1 border
            area.y + 1,
        ));
    } else if state.mode == Mode::CommandPalette {
        let block = Block::default().title(" Commands ").borders(Borders::ALL);
        let inner = block.inner(area);
        f.render_widget(block, area);

        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(inner);

        // Input line with ":" prefix
        let cmd_para = Paragraph::new(format!(":{}", state.input))
            .style(Style::default().fg(Color::White));
        f.render_widget(cmd_para, rows[0]);

        // Filtered command list
        let filter = state.input.to_lowercase();
        let filtered: Vec<(&str, &str, &str)> = PALETTE_COMMANDS.iter()
            .filter(|(name, key, _)| filter.is_empty() || name.starts_with(&filter) || *key == filter)
            .copied()
            .collect();
        if !filtered.is_empty() {
            let hint = if filtered.len() <= 2 {
                let parts: Vec<String> = filtered.iter()
                    .map(|(name, key, desc)| if key.is_empty() {
                        format!("{} — {}", name, desc)
                    } else {
                        format!("{} ({}) — {}", name, key, desc)
                    })
                    .collect();
                format!(" {}", parts.join("  ·  "))
            } else {
                let parts: Vec<String> = filtered.iter()
                    .map(|(name, key, _)| if key.is_empty() { name.to_string() } else { format!("{} ({})", name, key) })
                    .collect();
                format!(" {}", parts.join("  ·  "))
            };
            let hint_para = Paragraph::new(hint)
                .style(Style::default().fg(Color::DarkGray));
            f.render_widget(hint_para, rows[1]);
        }

        f.set_cursor_position((
            area.x + 1 + 1 + state.cursor as u16, // +1 border, +1 for ":"
            area.y + 1,
        ));
    } else if let Mode::CommandSearch { ref query, match_cursor, .. } = state.mode {
        let block = Block::default()
            .title(" Reverse Search (Ctrl+R: older match, Esc: cancel) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Yellow));
        let inner = block.inner(area);
        f.render_widget(block, area);

        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(inner);

        // Search prompt line
        let prompt = Paragraph::new(Line::from(vec![
            Span::styled("(reverse-i-search): ", Style::default().fg(Color::Yellow)),
            Span::styled(query.clone(), Style::default().fg(Color::White)),
            Span::styled("▌", Style::default().fg(Color::Yellow)),
        ]));
        f.render_widget(prompt, rows[0]);

        // Matched command line – resolve the match once and reuse it for both
        // the display text and the colour selection.
        let matched = state
            .command_history
            .search_reverse(query, match_cursor)
            .and_then(|i| state.command_history.entries().get(i))
            .map(|e| e.text.clone());
        let (match_text, match_style) = match matched {
            Some(text) => (text, Style::default().fg(Color::White)),
            None => ("(no result)".to_string(), Style::default().fg(Color::Red)),
        };
        let match_para = Paragraph::new(Line::from(vec![
            Span::styled("→ ", Style::default().fg(Color::Yellow)),
            Span::styled(match_text, match_style),
        ]));
        f.render_widget(match_para, rows[1]);
    } else {
        let has_warn_or_error = state.logs.iter().any(|e| {
            matches!(e.level, crate::log::LogLevel::Warn | crate::log::LogLevel::Error)
        });
        let alert = if has_warn_or_error { " ⚠ " } else { "" };
        let (title, display) = (" LatticeQL ", "");
        let full_title = format!("{}{}", alert, title);
        let title_style = if has_warn_or_error {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default().fg(Color::White)
        };
        let block = Block::default()
            .title(full_title)
            .title_style(title_style)
            .borders(Borders::ALL);
        let para = Paragraph::new(display)
            .block(block)
            .style(Style::default().fg(Color::White));
        f.render_widget(para, area);
    }
}

/// Format a list of completions into a single hint string, capped at 8 items.
fn format_completions(completions: &[Completion]) -> String {
    const MAX_SHOW: usize = 8;
    let total = completions.len();
    let parts: Vec<String> = completions
        .iter()
        .take(MAX_SHOW)
        .map(|c| match c {
            Completion::Token(s) => s.clone(),
            Completion::QuotedValue => "'<value>'".to_string(),
        })
        .collect();
    let mut text = parts.join("  ·  ");
    if total > MAX_SHOW {
        text.push_str(&format!("  +{} more", total - MAX_SHOW));
    }
    text
}

fn render_path_selection(f: &mut Frame, state: &AppState) {
    let area = centered_rect(70, 60, f.area());
    f.render_widget(Clear, area);

    let inner_height = area.height.saturating_sub(2) as usize;
    let offset = if state.path_cursor >= inner_height {
        state.path_cursor + 1 - inner_height
    } else {
        0
    };

    let items: Vec<ListItem> = state
        .paths
        .iter()
        .enumerate()
        .skip(offset)
        .take(inner_height)
        .map(|(i, p)| {
            let selected = i == state.path_cursor;
            let summary_style = if selected {
                Style::default().bg(Color::Blue).fg(Color::White)
            } else {
                Style::default()
            };

            if selected {
                // Build a multi-line item: summary on first line, then one
                // line per step showing the full column-level detail.
                let mut lines = vec![Line::styled(format!(" {}", p), summary_style)];
                for step in &p.steps {
                    let detail = format!(
                        "   {}.{} → {}.{}",
                        step.from_table, step.from_column,
                        step.to_table,   step.to_column,
                    );
                    lines.push(Line::styled(
                        detail,
                        Style::default().bg(Color::Blue).fg(Color::Cyan),
                    ));
                }
                ListItem::new(Text::from(lines))
            } else {
                ListItem::new(Text::from(Line::styled(
                    format!(" {}", p),
                    summary_style,
                )))
            }
        })
        .collect();

    let count_label = if state.paths_has_more {
        format!(" {} paths (more available) ", state.paths.len())
    } else {
        format!(" {} paths ", state.paths.len())
    };

    let mut block = Block::default()
        .title(" ↑↓ navigate, Enter select, Esc cancel ")
        .title_bottom(Line::styled(count_label, Style::default().fg(Color::White)))
        .borders(Borders::ALL)
        .style(Style::default().fg(Color::Cyan));

    if state.paths_has_more {
        block = block.title_bottom(
            Line::styled(" n — load more ", Style::default().fg(Color::Yellow)),
        );
    }

    let list = List::new(items).block(block);
    f.render_widget(list, area);
}

fn render_column_add(f: &mut Frame, state: &mut AppState) {
    if let Some(ref mut widget) = state.column_add {
        crate::app::column_manager::render::render(f, widget);
    }
}

fn render_overlay_message(f: &mut Frame, message: &str, color: Color) {
    let area = centered_rect(60, 20, f.area());
    f.render_widget(Clear, area);
    let para = Paragraph::new(message)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .style(Style::default().fg(color)),
        )
        .alignment(Alignment::Center)
        .wrap(Wrap { trim: true });
    f.render_widget(para, area);
}



/// Compute a centered rect that is `percent_x`% wide and `percent_y`% tall.
// Shared render utilities re-exported from ui::model::render.
use crate::ui::model::render::{centered_rect, render_search_bar};

fn render_log_viewer(f: &mut Frame, state: &AppState) {
    let area = centered_rect(80, 70, f.area());
    f.render_widget(Clear, area);

    let cursor = match &state.mode {
        Mode::LogViewer { cursor } => *cursor,
        _ => 0,
    };

    let items: Vec<ListItem> = state
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
            if i == cursor {
                ListItem::new(line)
                    .style(Style::default().bg(Color::DarkGray))
            } else {
                ListItem::new(line)
            }
        })
        .collect();

    let title = if state.logs.is_empty() {
        " Log History — empty (Esc close) ".to_string()
    } else {
        format!(
            " Log History ({}/{})  ↑↓/jk navigate  Esc close ",
            cursor + 1,
            state.logs.len()
        )
    };

    let list = List::new(items)
        .block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::Yellow)),
        );

    // Scroll so cursor is visible
    let inner_height = area.height.saturating_sub(2) as usize;
    let offset = if state.logs.is_empty() {
        0
    } else if cursor + 1 > inner_height {
        cursor + 1 - inner_height
    } else {
        0
    };

    use ratatui::widgets::ListState;
    let mut list_state = ListState::default();
    list_state.select(Some(cursor));
    *list_state.offset_mut() = offset;

    f.render_stateful_widget(list, area, &mut list_state);
}

