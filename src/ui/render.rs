use crate::connection_manager::{ConnectionStatus, ConnectionType};
use crate::engine::{flatten_tree, DataNode};
use crate::rules::{completions_at, Completion};
use crate::ui::app::{AppState, ConnectionManagerTab, Mode, VirtualFkField, VirtualFkForm};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame,
};

/// Embedded manuals: (title, content).
pub const MANUALS: &[(&str, &str)] = &[
    ("Command Querying Syntax", include_str!("../../manuals/command-syntax.md")),
    ("Data Viewing",            include_str!("../../manuals/data-viewing.md")),
    ("Reordering Commands",     include_str!("../../manuals/reordering.md")),
    ("Column Managers",         include_str!("../../manuals/column-managers.md")),
    ("Virtual Foreign Keys",    include_str!("../../manuals/virtual-foreign-keys.md")),
];

/// Return the number of lines in a manual (for scroll bounds).
pub fn manual_line_count(index: usize) -> usize {
    MANUALS.get(index).map(|(_, content)| content.lines().count()).unwrap_or(0)
}

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
    let cmd_height: u16 = if matches!(state.mode, Mode::Command | Mode::CommandSearch { .. }) { 4 } else { 3 };
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
        Mode::ManualList { .. } => render_manual_list(f, state),
        Mode::ManualView { .. } => render_manual_view(f, state),
        Mode::Confirm { message, .. } => {
            let message = message.clone();
            render_overlay_message(f, &message, Color::Yellow);
        }
        Mode::ConnectionManager { .. } => render_connection_manager(f, state),
        Mode::ConnectionAdd(_) => render_connection_add(f, state),
        Mode::SavedConnectionAlias { ref alias, .. } => {
            let alias = alias.clone();
            render_alias_prompt(f, &alias);
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
        let completions = completions_at(&state.input, &state.completion_table_names(), &state.table_columns);
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
        let (title, display) = match &state.mode {
            Mode::Normal => (
                " LatticeQL ",
                " ':' command  'j/k' navigate  'f' fold  's' schema  'c' columns  'v' virtual FKs  'r' reorder  '+' connections  'm' manuals  'l' logs  'q' quit",
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
                || vfk.type_value.as_deref().unwrap_or("").to_lowercase().contains(&q)
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
                let from = state.display_name(&vfk.from_table);
                let to = state.display_name(&vfk.to_table);
                let text = if let (Some(tc), Some(tv)) = (&vfk.type_column, &vfk.type_value) {
                    format!(
                        "  {}.{} = '{}' → {}.{}  (via {}.{})",
                        from, tc, tv,
                        to, vfk.to_column,
                        from, vfk.id_column,
                    )
                } else {
                    format!(
                        "  {}.{} → {}.{}",
                        from, vfk.id_column,
                        to, vfk.to_column,
                    )
                };
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
    let area = centered_rect(76, 88, f.area());
    f.render_widget(Clear, area);

    let form = if let Mode::VirtualFkAdd(ref form) = state.mode { form.clone() } else { return };

    // Outer block
    let complete = form.is_complete();
    let hint = if complete {
        " Ctrl+S: save & commit  "
    } else {
        " Fill required fields then press Ctrl+S or Enter on to_column  "
    };
    let block = Block::default()
        .title(format!(
            " Add Virtual FK  (Tab/Shift+Tab: field · ↑↓/j/k: select · Enter: confirm · /: search · Esc: cancel){} ",
            if complete { "· Ctrl+S: save " } else { "" }
        ))
        .title_bottom(Line::from(Span::styled(hint, Style::default().fg(Color::DarkGray))))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));

    let inner = block.inner(area);
    f.render_widget(block, area);

    // Layout inside block: header (7 rows) + dropdown (remaining)
    // The dropdown delegates search-bar rendering to render_pick_list, so no
    // extra height reservation is needed here.
    let header_height: u16 = 7;
    let dropdown_height = inner.height.saturating_sub(header_height);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_height),
            Constraint::Length(dropdown_height),
        ])
        .split(inner);

    let header_area = sections[0];
    let dropdown_area = sections[1];

    // ── Header: show all 6 fields + values ──────────────────────────────
    render_vfk_form_header(f, &form, header_area);

    // ── Dropdown: pick-list for the active field (handles search bar too) ──
    render_vfk_form_dropdown(f, state, &form, dropdown_area);
}

/// Render the 6-field summary header showing current values for every field.
fn render_vfk_form_header(f: &mut Frame, form: &VirtualFkForm, area: Rect) {
    let active_label_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    let label_style = Style::default().fg(Color::White);
    let set_value_style = Style::default().fg(Color::Green);
    let active_value_style = Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD);
    let unset_style = Style::default().fg(Color::DarkGray);
    let optional_label_style = Style::default().fg(Color::Cyan);

    let field_line = |field: &VirtualFkField, value: &str, optional: bool| -> Line<'static> {
        let is_active = &form.active_field == field;
        let cursor_str = if is_active { "▶ " } else { "  " };
        let opt_tag = if optional { " (opt)" } else { "" };
        let label_text = format!("{:<12}{}", field.label(), opt_tag);
        let colon_val = if value.is_empty() {
            if optional { "(optional)".to_string() } else { "(not set)".to_string() }
        } else {
            value.to_string()
        };
        let ls = if is_active {
            active_label_style
        } else if optional {
            optional_label_style
        } else {
            label_style
        };
        let vs = if value.is_empty() {
            unset_style
        } else if is_active {
            active_value_style
        } else {
            set_value_style
        };
        Line::from(vec![
            Span::raw(cursor_str.to_string()),
            Span::styled(format!("{}: ", label_text), ls),
            Span::styled(colon_val, vs),
        ])
    };

    let type_col_val = if form.type_column.is_empty() { "" } else { &form.type_column };
    let type_val_val = if form.type_value.is_empty() { "" } else { &form.type_value };

    let lines: Vec<Line> = vec![
        field_line(&VirtualFkField::FromTable, &form.from_table, false),
        field_line(&VirtualFkField::IdColumn, &form.id_column, false),
        field_line(&VirtualFkField::TypeColumn, type_col_val, true),
        field_line(&VirtualFkField::TypeValue, type_val_val, true),
        field_line(&VirtualFkField::ToTable, &form.to_table, false),
        field_line(&VirtualFkField::ToColumn, &form.to_column, false),
    ];

    // Separator line
    let separator = Line::from(Span::styled(
        "─".repeat(area.width as usize),
        Style::default().fg(Color::DarkGray),
    ));

    let mut all_lines = lines;
    all_lines.push(separator);

    f.render_widget(Paragraph::new(all_lines), area);
}

/// Render the dropdown list for the currently active field.
fn render_vfk_form_dropdown(f: &mut Frame, state: &mut AppState, form: &VirtualFkForm, area: Rect) {
    // Build the items for the active field
    let items: Vec<String> = match &form.active_field {
        VirtualFkField::FromTable | VirtualFkField::ToTable => state.display_table_names.clone(),
        VirtualFkField::IdColumn => {
            state.table_columns.get(&form.from_table).cloned().unwrap_or_default()
        }
        VirtualFkField::TypeColumn => {
            let mut cols = vec!["(none — simple FK)".to_string()];
            cols.extend(state.table_columns.get(&form.from_table).cloned().unwrap_or_default());
            cols
        }
        VirtualFkField::TypeValue => {
            if form.type_column.is_empty() {
                vec!["(no type_column set — skipping)".to_string()]
            } else {
                form.type_options.iter().map(|(v, c)| format!("{}  ({})", v, c)).collect()
            }
        }
        VirtualFkField::ToColumn => {
            state.table_columns.get(&form.to_table).cloned().unwrap_or_default()
        }
    };

    let field_label = form.active_field.label();
    let is_optional = matches!(form.active_field, VirtualFkField::TypeColumn | VirtualFkField::TypeValue);
    let opt_suffix = if is_optional { " (optional)" } else { "" };

    let block = Block::default()
        .title(format!(" {} {}", field_label, opt_suffix))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    render_pick_list(f, state, &items, form.cursor, area, block);
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

fn render_connection_manager(f: &mut Frame, state: &mut AppState) {
    let (tab, cursor) = match &state.mode {
        Mode::ConnectionManager { tab, cursor } => (tab.clone(), *cursor),
        _ => return,
    };

    let area = centered_rect(70, 60, f.area());
    f.render_widget(Clear, area);

    let block = Block::default()
        .title(" Connection Manager  (←→/Tab: switch tab · ↑↓: navigate · Enter: select · Ctrl+S: save · Esc: close) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(area);
    f.render_widget(block, area);

    // Tab header (2 rows)
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Min(1)])
        .split(inner);

    let tab_area = sections[0];
    let list_area = sections[1];

    // Render tab headers
    let conn_style = if tab == ConnectionManagerTab::Connections {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let saved_style = if tab == ConnectionManagerTab::Saved {
        Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::DarkGray)
    };
    let types_style = if tab == ConnectionManagerTab::Connectors {
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

    match tab {
        ConnectionManagerTab::Connections => {
            let summaries = &state.connections_summary;
            if summaries.is_empty() {
                let items = vec![ListItem::new("  (no connections — switch to Connectors tab to add one)")
                    .style(Style::default().fg(Color::DarkGray))];
                f.render_widget(List::new(items), list_area);
            } else {
                // Compute column widths for alignment.
                // +2 for "* " unsaved indicator before alias.
                let max_alias = summaries.iter().map(|s| s.alias.len()).max().unwrap_or(4).max(4);
                let max_type = summaries.iter().map(|s| s.conn_type.len()).max().unwrap_or(4).max(4);
                let avail_url = (list_area.width as usize)
                    .saturating_sub(2 + 2 + 2 + max_alias + 2 + max_type + 2 + 14 + 12);

                let items: Vec<ListItem> = summaries
                    .iter()
                    .enumerate()
                    .map(|(i, s)| {
                        let is_selected = i == cursor;

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
        ConnectionManagerTab::Saved => {
            let saved = &state.saved_connections;
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
                        if i == cursor {
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
        ConnectionManagerTab::Connectors => {
            let types = ConnectionType::all();
            let items: Vec<ListItem> = types
                .iter()
                .enumerate()
                .map(|(i, ct)| {
                    let text = format!("  {} — create a new {} connection", ct.label(), ct.label());
                    let item = ListItem::new(text);
                    if i == cursor {
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

fn render_connection_add(f: &mut Frame, state: &mut AppState) {
    let form = match &state.mode {
        Mode::ConnectionAdd(form) => form.clone(),
        _ => return,
    };

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
                // Show cursor indicator
                (format!("{}▌", field.value), active_value_style)
            } else {
                // Mask password fields
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

fn render_alias_prompt(f: &mut Frame, alias: &str) {
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
        Span::styled(alias.to_string(), Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD)),
        Span::styled("▌", Style::default().fg(Color::Yellow)),
    ]);
    f.render_widget(Paragraph::new(line), inner);
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

fn render_manual_list(f: &mut Frame, state: &AppState) {
    let area = centered_rect(60, 50, f.area());
    f.render_widget(Clear, area);

    let cursor = match &state.mode {
        Mode::ManualList { cursor } => *cursor,
        _ => 0,
    };

    let items: Vec<ListItem> = MANUALS
        .iter()
        .enumerate()
        .map(|(i, (title, _))| {
            let line = Line::from(Span::raw(format!("  {}", title)));
            if i == cursor {
                ListItem::new(line).style(Style::default().bg(Color::Blue).fg(Color::White).add_modifier(Modifier::BOLD))
            } else {
                ListItem::new(line)
            }
        })
        .collect();

    let list = List::new(items).block(
        Block::default()
            .title(" Manuals — ↑↓/jk navigate  Enter open  Esc/q/m close ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan)),
    );
    f.render_widget(list, area);
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

fn render_manual_view(f: &mut Frame, state: &AppState) {
    let (index, scroll) = match &state.mode {
        Mode::ManualView { index, scroll } => (*index, *scroll),
        _ => return,
    };

    let Some((title, content)) = MANUALS.get(index) else { return };

    let area = centered_rect(85, 85, f.area());
    f.render_widget(Clear, area);

    let inner_height = area.height.saturating_sub(2) as usize;
    let total_lines = content.lines().count();

    let lines: Vec<Line> = content
        .lines()
        .skip(scroll)
        .take(inner_height)
        .map(md_line_to_ratatui)
        .collect();

    let title_str = format!(
        " {} ({}/{})  ↑↓/jk scroll  Esc/q back ",
        title,
        scroll + 1,
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
