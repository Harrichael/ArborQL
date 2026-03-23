use crate::engine::{flatten_tree, DataNode};
use crate::ui::app::{AppState, Mode};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
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

    // Split main_area into data viewer + command bar
    let vert = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(3), Constraint::Length(3)])
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
        Mode::RuleReorder => render_rule_reorder(f, state),
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
}

fn render_schema(f: &mut Frame, state: &AppState, area: Rect) {
    let items: Vec<ListItem> = state
        .table_names
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

    let inner_height = area.height.saturating_sub(2) as usize;

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
            let mut default_cols: Vec<String> = node.row.keys().cloned().collect();
            default_cols.sort();
            let summary_cols = state
                .tree_visible_columns
                .get(&node.table)
                .cloned()
                .unwrap_or_else(|| {
                    let preferred = ["id", "name", "title", "label"];
                    let mut cols: Vec<String> = preferred
                        .iter()
                        .filter_map(|c| {
                            if default_cols.iter().any(|k| k == c) {
                                Some((*c).to_string())
                            } else {
                                None
                            }
                        })
                        .collect();
                    if cols.is_empty() && !default_cols.is_empty() {
                        cols.push(default_cols[0].clone());
                    }
                    cols
                });
            let summary = if summary_cols.is_empty() {
                node.summary()
            } else {
                let parts: Vec<String> = summary_cols
                    .iter()
                    .map(|c| {
                        let v = node.row.get(c).map(|v| v.to_string()).unwrap_or_default();
                        format!("{}: {}", c, v)
                    })
                    .collect();
                parts.join("  │  ")
            };
            let table_label = format!("[{}]", node.table);
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
        " Data Viewer (empty — type ':' to enter a command) ".to_string()
    } else {
        format!(
            " Data Viewer [{}/{}] ",
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
    let (title, hint) = match &state.mode {
        Mode::Command => (
            " Command ",
            " Enter command • Esc: cancel",
        ),
        Mode::Normal => (
            " ArborQL ",
            " ':' command  'j/k' navigate  'f' fold  's' schema  'c' add column  'r' reorder  'q' quit",
        ),
        _ => (" ArborQL ", ""),
    };

    let display = match &state.mode {
        Mode::Command => format!(":{}", state.input),
        _ => hint.to_string(),
    };

    let block = Block::default().title(title).borders(Borders::ALL);
    let para = Paragraph::new(display)
        .block(block)
        .style(Style::default().fg(Color::White));
    f.render_widget(para, area);

    // Show cursor in command mode
    if state.mode == Mode::Command {
        f.set_cursor_position((
            area.x + 1 + 1 + state.cursor as u16, // +1 border, +1 for ':'
            area.y + 1,
        ));
    }
}

fn render_path_selection(f: &mut Frame, state: &AppState) {
    let area = centered_rect(70, 60, f.area());
    f.render_widget(Clear, area);

    let items: Vec<ListItem> = state
        .paths
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let text = p.to_string();
            let item = ListItem::new(text);
            if i == state.path_cursor {
                item.style(Style::default().bg(Color::Blue).fg(Color::White))
            } else {
                item
            }
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Multiple paths found — choose one (↑↓ navigate, Enter select, Esc cancel) ")
            .borders(Borders::ALL)
            .style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(list, area);
}

fn render_rule_reorder(f: &mut Frame, state: &AppState) {
    let area = centered_rect(60, 50, f.area());
    f.render_widget(Clear, area);

    let mut items: Vec<ListItem> = Vec::new();
    const SLOT_LABEL: &str = "next insertion";

    if state.rules.is_empty() {
        items.push(ListItem::new(format!("→ {}", SLOT_LABEL)).style(Style::default().fg(Color::DarkGray)));
    } else {
        for (i, r) in state.rules.iter().enumerate() {
            if i == state.next_rule_cursor {
                items.push(
                    ListItem::new(format!("→ {}", SLOT_LABEL))
                        .style(Style::default().fg(Color::DarkGray)),
                );
            }

            let text = format!("   {}. {}", i + 1, r);
            let item = ListItem::new(text);
            if i == state.rule_cursor {
                items.push(item.style(Style::default().bg(Color::Blue).fg(Color::White)));
            } else {
                items.push(item);
            }
        }
        if state.next_rule_cursor == state.rules.len() {
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

fn render_column_add(f: &mut Frame, state: &AppState) {
    if let Some((ref table, ref items, cursor)) = state.column_add {
        let area = centered_rect(50, 40, f.area());
        f.render_widget(Clear, area);

        let list_items: Vec<ListItem> = items
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let marker = if col.enabled { "[x]" } else { "[ ]" };
                let item = ListItem::new(format!("{} {}", marker, col.name));
                if i == cursor {
                    item.style(Style::default().bg(Color::Green).fg(Color::Black))
                } else {
                    item
                }
            })
            .collect();

        let list = List::new(list_items).block(
            Block::default()
                .title(format!(
                    " Columns for '{}' (↑↓ nav, space/x toggle, u/d reorder, Enter apply, Esc cancel) ",
                    table
                ))
                .borders(Borders::ALL),
        );
        f.render_widget(list, area);
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
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
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
