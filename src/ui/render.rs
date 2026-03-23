use crate::engine::{flatten_tree, DataNode};
use crate::rules::{completions_at, Completion};
use crate::ui::app::{AppState, Mode, VirtualFkAddStep};
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
    // In Command mode we use an extra row to show next-token hints.
    let cmd_height: u16 = if state.mode == Mode::Command { 4 } else { 3 };
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
        Mode::RuleReorder => render_rule_reorder(f, state),
        Mode::VirtualFkManager { .. } => render_virtual_fk_manager(f, state),
        Mode::VirtualFkAdd(_) => render_virtual_fk_add(f, state),
        Mode::LogViewer { .. } => render_log_viewer(f, state),
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
            let mut default_cols: Vec<String> = node.row.keys().cloned().collect();
            default_cols.sort();
            let summary_cols = state
                .tree_visible_columns
                .get(&node.table)
                .cloned()
                .unwrap_or_else(|| {
                    state
                        .configured_defaults_for_table(&node.table)
                        .iter()
                        .filter(|c| default_cols.iter().any(|k| k == *c))
                        .cloned()
                        .collect()
                });
            let summary = summary_cols
                .iter()
                .map(|c| {
                    let v = node.row.get(c).map(|v| v.to_string()).unwrap_or_default();
                    format!("{}: {}", c, v)
                })
                .collect::<Vec<_>>()
                .join("  │  ");
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
    if state.mode == Mode::Command {
        let block = Block::default().title(" Command ").borders(Borders::ALL);
        let inner = block.inner(area);
        f.render_widget(block, area);

        // Split inner into: command input line | next-token hint line.
        let rows = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Length(1)])
            .split(inner);

        // Input line
        let cmd_para = Paragraph::new(format!(":{}", state.input))
            .style(Style::default().fg(Color::White));
        f.render_widget(cmd_para, rows[0]);

        // Completion hint line
        let completions = completions_at(&state.input, &state.table_names, &state.table_columns);
        if !completions.is_empty() {
            let hint = format_completions(&completions);
            let hint_para = Paragraph::new(hint)
                .style(Style::default().fg(Color::DarkGray));
            f.render_widget(hint_para, rows[1]);
        }

        // Place the cursor on the input line.
        f.set_cursor_position((
            area.x + 1 + 1 + state.cursor as u16, // +1 border, +1 for ':'
            area.y + 1,
        ));
    } else {
        let has_warn_or_error = state.logs.iter().any(|e| {
            matches!(e.level, crate::log::LogLevel::Warn | crate::log::LogLevel::Error)
        });
        let alert = if has_warn_or_error { " ⚠ " } else { "" };
        let (title, display) = match &state.mode {
            Mode::Normal => (
                " LatticeQL ",
                " ':' command  'j/k' navigate  'f' fold  's' schema  'c' columns  'v' virtual FKs  'r' reorder  'l' logs  'q' quit",
            ),
            _ => (" LatticeQL ", ""),
        };
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
    let mut text = format!(" {}", parts.join("  ·  "));
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

    let mut items: Vec<ListItem> = state
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

    if state.paths_has_more && items.len() < inner_height {
        items.push(
            ListItem::new("  … (more paths exist)")
                .style(Style::default().fg(Color::DarkGray)),
        );
    }

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

fn render_column_add(f: &mut Frame, state: &mut AppState) {
    if let Some((ref table, ref items, cursor)) = state.column_add.clone() {
        let area = centered_rect(50, 70, f.area());
        f.render_widget(Clear, area);

        // Reserve last row for search bar
        let has_search = state.overlay_search_active || !state.overlay_search.is_empty();
        let list_area = if has_search {
            Rect { height: area.height.saturating_sub(3), ..area }
        } else {
            area
        };

        let inner_height = list_area.height.saturating_sub(2) as usize;

        // Apply search filter — cursor is index into filtered list
        let q = state.overlay_search.to_lowercase();
        let filtered: Vec<(usize, &crate::ui::app::ColumnManagerItem)> = items.iter()
            .enumerate()
            .filter(|(_, it)| q.is_empty() || it.name.to_lowercase().contains(&q))
            .collect();

        // Clamp scroll: scroll only when cursor leaves visible window
        if cursor < state.overlay_scroll {
            state.overlay_scroll = cursor;
        } else if cursor >= state.overlay_scroll + inner_height {
            state.overlay_scroll = cursor + 1 - inner_height;
        }
        let offset = state.overlay_scroll;

        let list_items: Vec<ListItem> = filtered
            .iter()
            .enumerate()
            .skip(offset)
            .take(inner_height)
            .map(|(fi, (_, col))| {
                let marker = if col.enabled { "[x]" } else { "[ ]" };
                let item = ListItem::new(format!("{} {}", marker, col.name));
                if fi == cursor {
                    item.style(Style::default().bg(Color::Green).fg(Color::Black))
                } else {
                    item
                }
            })
            .collect();

        let match_info = if !state.overlay_search.is_empty() {
            format!("  ({} matches)", filtered.len())
        } else {
            String::new()
        };
        let reorder_hint = if state.overlay_search.is_empty() { "  u/d reorder" } else { "" };
        let list = List::new(list_items).block(
            Block::default()
                .title(format!(
                    " Columns for '{}'{} (↑↓ nav · space/x toggle{}· /search · Enter apply · Esc) ",
                    table, match_info, reorder_hint
                ))
                .borders(Borders::ALL),
        );
        f.render_widget(list, list_area);

        // Search bar
        if has_search {
            let search_area = Rect { y: list_area.y + list_area.height, height: 3, ..area };
            render_search_bar(f, search_area, &state.overlay_search.clone(), state.overlay_search_active);
        }
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

fn render_virtual_fk_manager(f: &mut Frame, state: &mut AppState) {
    let area = centered_rect(72, 70, f.area());
    f.render_widget(Clear, area);

    let cursor = if let Mode::VirtualFkManager { cursor } = state.mode { cursor } else { 0 };

    // Reserve last row for search bar
    let has_search = state.overlay_search_active || !state.overlay_search.is_empty();
    let list_area = if has_search {
        Rect { height: area.height.saturating_sub(3), ..area }
    } else {
        area
    };

    let inner_height = list_area.height.saturating_sub(2) as usize;

    // Apply search filter
    let q = state.overlay_search.to_lowercase();
    let filtered: Vec<(usize, &crate::schema::VirtualFkDef)> = state.virtual_fks.iter()
        .enumerate()
        .filter(|(_, vfk)| {
            q.is_empty()
                || vfk.from_table.to_lowercase().contains(&q)
                || vfk.to_table.to_lowercase().contains(&q)
                || vfk.type_value.to_lowercase().contains(&q)
        })
        .collect();

    // Clamp scroll: only move when cursor leaves visible window
    if cursor < state.overlay_scroll {
        state.overlay_scroll = cursor;
    } else if inner_height > 0 && cursor >= state.overlay_scroll + inner_height {
        state.overlay_scroll = cursor + 1 - inner_height;
    }
    let offset = state.overlay_scroll;

    let items: Vec<ListItem> = if state.virtual_fks.is_empty() {
        vec![ListItem::new("  (none — press 'a' to add one)")
            .style(Style::default().fg(Color::DarkGray))]
    } else if filtered.is_empty() {
        vec![ListItem::new("  (no matches)")
            .style(Style::default().fg(Color::DarkGray))]
    } else {
        filtered
            .iter()
            .enumerate()
            .skip(offset)
            .take(inner_height)
            .map(|(fi, (_, vfk))| {
                let text = format!(
                    "  {}.{} = '{}' → {}.{}  (via {}.{})",
                    vfk.from_table, vfk.type_column, vfk.type_value,
                    vfk.to_table, vfk.to_column,
                    vfk.from_table, vfk.id_column,
                );
                let item = ListItem::new(text);
                if fi == cursor {
                    item.style(Style::default().bg(Color::Blue).fg(Color::White))
                } else {
                    item
                }
            })
            .collect()
    };

    let match_info = if !state.overlay_search.is_empty() {
        format!("  ({} matches)", filtered.len())
    } else {
        String::new()
    };
    let list = List::new(items).block(
        Block::default()
            .title(format!(" Virtual FK Manager{}  (↑↓ navigate · a add · d/x delete · /search · Ctrl+S save · Esc) ", match_info))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(list, list_area);

    // Search bar
    if has_search {
        let search_area = Rect { y: list_area.y + list_area.height, height: 3, ..area };
        render_search_bar(f, search_area, &state.overlay_search.clone(), state.overlay_search_active);
    }
}

fn render_virtual_fk_add(f: &mut Frame, state: &mut AppState) {
    let area = centered_rect(60, 70, f.area());
    f.render_widget(Clear, area);

    let step = if let Mode::VirtualFkAdd(ref s) = state.mode { s.clone() } else { return };

    match step {
        VirtualFkAddStep::PickFromTable { cursor } => {
            render_pick_list(f, state, &state.table_names.clone(), cursor, area, Block::default()
                .title(" Step 1/5: Table that owns the type+id columns  (↑↓ · / search · Enter · Esc) ")
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Yellow)));
        }
        VirtualFkAddStep::PickTypeColumn { from_table, cursor } => {
            let cols = state.table_columns.get(&from_table).cloned().unwrap_or_default();
            render_pick_list(f, state, &cols, cursor, area, Block::default()
                .title(format!(
                    " Step 2/5: Type discriminator column in '{}'  (↑↓ · / search · Enter · Esc) ",
                    from_table
                ))
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Yellow)));
        }
        VirtualFkAddStep::PickTypeValue { from_table, type_column, options, cursor } => {
            let option_strings: Vec<String> = options
                .iter()
                .map(|(val, cnt)| format!("{}  ({})", val, cnt))
                .collect();
            render_pick_list(f, state, &option_strings, cursor, area, Block::default()
                .title(format!(
                    " Step 3/5: Select value of {}.{}  (↑↓ · / search · Enter · Esc) ",
                    from_table, type_column
                ))
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Yellow)));
        }
        VirtualFkAddStep::PickIdColumn { from_table, type_column, type_value, cursor } => {
            let cols = state.table_columns.get(&from_table).cloned().unwrap_or_default();
            render_pick_list(f, state, &cols, cursor, area, Block::default()
                .title(format!(
                    " Step 4/5: ID column in '{}' (holds FK when {}='{}')  (↑↓ · / search · Enter · Esc) ",
                    from_table, type_column, type_value
                ))
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Yellow)));
        }
        VirtualFkAddStep::PickToTable { type_column, type_value, id_column, cursor, .. } => {
            render_pick_list(f, state, &state.table_names.clone(), cursor, area, Block::default()
                .title(format!(
                    " Step 5/6: Target table for {}='{}' via {}  (↑↓ · / search · Enter · Esc) ",
                    type_column, type_value, id_column
                ))
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Yellow)));
        }
        VirtualFkAddStep::PickToColumn { type_column, type_value, to_table, cursor, .. } => {
            let to_cols = state.table_columns.get(&to_table).cloned().unwrap_or_default();
            render_pick_list(f, state, &to_cols, cursor, area, Block::default()
                .title(format!(
                    " Step 6/6: Join column on '{}' for {}='{}'  (↑↓ · / search · Enter · Esc) ",
                    to_table, type_column, type_value
                ))
                .borders(Borders::ALL)
                .style(Style::default().fg(Color::Yellow)));
        }
    }
}

/// Render a scrollable pick list with search support.
/// `cursor` is the index into the *filtered* list.
/// Updates `state.overlay_scroll` to keep cursor in view.
fn render_pick_list(
    f: &mut ratatui::Frame,
    state: &mut AppState,
    items: &[String],
    cursor: usize,
    area: Rect,
    block: Block,
) {
    let has_search = state.overlay_search_active || !state.overlay_search.is_empty();
    let list_area = if has_search {
        Rect { height: area.height.saturating_sub(3), ..area }
    } else {
        area
    };
    let inner_height = list_area.height.saturating_sub(2) as usize;

    let q = state.overlay_search.to_lowercase();
    let filtered: Vec<(usize, &String)> = items.iter().enumerate()
        .filter(|(_, s)| q.is_empty() || s.to_lowercase().contains(&q))
        .collect();

    // Clamp scroll: peripheral — only move when cursor leaves window
    if cursor < state.overlay_scroll {
        state.overlay_scroll = cursor;
    } else if inner_height > 0 && cursor >= state.overlay_scroll + inner_height {
        state.overlay_scroll = cursor + 1 - inner_height;
    }

    let visible: Vec<ListItem> = filtered.iter()
        .enumerate()
        .skip(state.overlay_scroll)
        .take(inner_height)
        .map(|(fi, (_, s))| {
            let item = ListItem::new(format!("  {}", s));
            if fi == cursor {
                item.style(Style::default().bg(Color::Blue).fg(Color::White))
            } else {
                item
            }
        })
        .collect();

    let match_info = if !q.is_empty() { format!("  ({} matches)", filtered.len()) } else { String::new() };
    let block = block.title_bottom(Line::from(Span::styled(
        format!("  {}/{}{} ", cursor + 1, filtered.len(), match_info),
        Style::default().fg(Color::DarkGray),
    )));
    f.render_widget(List::new(visible).block(block), list_area);

    if has_search {
        let search_area = Rect { y: list_area.y + list_area.height, height: 3, ..area };
        render_search_bar(f, search_area, &state.overlay_search.clone(), state.overlay_search_active);
    }
}

/// Compute a centered rect that is `percent_x`% wide and `percent_y`% tall.
fn render_search_bar(f: &mut Frame, area: Rect, query: &str, active: bool) {
    let (border_color, title) = if active {
        (Color::Yellow, " Search (Esc to stop typing, keep filter) ")
    } else {
        (Color::DarkGray, " Filter active (/ to edit, Esc to clear) ")
    };
    let text = Line::from(vec![
        Span::styled("/ ", Style::default().fg(Color::Yellow)),
        Span::styled(query, Style::default().fg(Color::White).add_modifier(Modifier::BOLD)),
        if active { Span::styled("▌", Style::default().fg(Color::Yellow)) } else { Span::raw("") },
    ]);
    f.render_widget(
        Paragraph::new(text).block(
            Block::default()
                .title(title)
                .borders(Borders::ALL)
                .border_style(Style::default().fg(border_color)),
        ),
        area,
    );
}

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
