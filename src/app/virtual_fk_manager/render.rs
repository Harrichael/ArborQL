use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
    Frame,
};

use super::widget::{VfkView, VfkWidget, VirtualFkField, vfk_display_string};
use crate::app::tui::keys::InputFocus;
use crate::app::tui::render::{centered_rect, render_search_bar};

pub fn render(f: &mut Frame, widget: &VfkWidget) {
    match widget.view {
        VfkView::List => render_list(f, widget),
        VfkView::Form => render_form(f, widget),
    }
}

fn render_list(f: &mut Frame, widget: &VfkWidget) {
    let area = centered_rect(72, 70, f.area());
    f.render_widget(Clear, area);

    let is_searching = widget.focus.input == InputFocus::Search;
    let has_search = is_searching || !widget.search.is_empty();
    let list_area = if has_search {
        Rect { height: area.height.saturating_sub(3), ..area }
    } else {
        area
    };

    let inner_height = list_area.height.saturating_sub(2) as usize;
    let filtered = widget.filtered_vfk_indices();

    let items: Vec<ListItem> = if widget.virtual_fks.is_empty() {
        vec![ListItem::new("  (none — press 'a' to add one)")
            .style(Style::default().fg(Color::DarkGray))]
    } else if filtered.is_empty() {
        vec![ListItem::new("  (no matches)")
            .style(Style::default().fg(Color::DarkGray))]
    } else {
        filtered
            .iter()
            .enumerate()
            .skip(widget.scroll)
            .take(inner_height)
            .map(|(fi, &idx)| {
                let vfk = &widget.virtual_fks[idx];
                let text = format!("  {}", vfk_display_string(vfk));
                let item = ListItem::new(text);
                if fi == widget.cursor {
                    item.style(Style::default().bg(Color::Blue).fg(Color::White))
                } else {
                    item
                }
            })
            .collect()
    };

    let match_info = if !widget.search.is_empty() {
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

    if has_search {
        let search_area = Rect { y: list_area.y + list_area.height, height: 3, ..area };
        render_search_bar(f, search_area, &widget.search, is_searching, widget.search_cursor);
    }
}

fn render_form(f: &mut Frame, widget: &VfkWidget) {
    let form = match &widget.form {
        Some(f) => f,
        None => return,
    };

    let area = centered_rect(76, 88, f.area());
    f.render_widget(Clear, area);

    let complete = form.is_complete();
    let hint = if complete {
        " Ctrl+S: save & commit  "
    } else {
        " Fill required fields then press Ctrl+S or Enter on to_column  "
    };
    let block = Block::default()
        .title(format!(
            " Add Virtual FK  (Tab/Shift+Tab: field · ↑↓: select · Enter: confirm · /: search · Esc: cancel){} ",
            if complete { "· Ctrl+S: save " } else { "" }
        ))
        .title_bottom(Line::from(Span::styled(hint, Style::default().fg(Color::DarkGray))))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Yellow));

    let inner = block.inner(area);
    f.render_widget(block, area);

    let header_height: u16 = 7;
    let dropdown_height = inner.height.saturating_sub(header_height);

    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(header_height),
            Constraint::Length(dropdown_height),
        ])
        .split(inner);

    render_form_header(f, form, sections[0]);
    render_form_dropdown(f, widget, sections[1]);
}

fn render_form_header(f: &mut Frame, form: &super::widget::VirtualFkForm, area: Rect) {
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

    let mut lines: Vec<Line> = vec![
        field_line(&VirtualFkField::FromTable, &form.from_table, false),
        field_line(&VirtualFkField::IdColumn, &form.id_column, false),
        field_line(&VirtualFkField::TypeColumn, type_col_val, true),
        field_line(&VirtualFkField::TypeValue, type_val_val, true),
        field_line(&VirtualFkField::ToTable, &form.to_table, false),
        field_line(&VirtualFkField::ToColumn, &form.to_column, false),
    ];

    lines.push(Line::from(Span::styled(
        "─".repeat(area.width as usize),
        Style::default().fg(Color::DarkGray),
    )));

    f.render_widget(Paragraph::new(lines), area);
}

fn render_form_dropdown(f: &mut Frame, widget: &VfkWidget, area: Rect) {
    let form = match &widget.form {
        Some(f) => f,
        None => return,
    };

    let items = widget.dropdown_items();

    let field_label = form.active_field.label();
    let is_optional = matches!(form.active_field, VirtualFkField::TypeColumn | VirtualFkField::TypeValue);
    let opt_suffix = if is_optional { " (optional)" } else { "" };

    let is_searching = widget.focus.input == InputFocus::Search;
    let has_search = is_searching || !widget.search.is_empty();
    let list_area = if has_search {
        Rect { height: area.height.saturating_sub(3), ..area }
    } else {
        area
    };
    let inner_height = list_area.height.saturating_sub(2) as usize;

    let visible: Vec<ListItem> = items
        .iter()
        .enumerate()
        .skip(widget.scroll)
        .take(inner_height)
        .map(|(fi, s)| {
            let item = ListItem::new(format!("  {}", s));
            if fi == widget.cursor {
                item.style(Style::default().bg(Color::Blue).fg(Color::White))
            } else {
                item
            }
        })
        .collect();

    let match_info = if !widget.search.is_empty() {
        format!("  ({} matches)", items.len())
    } else {
        String::new()
    };
    let block = Block::default()
        .title(format!(" {} {}", field_label, opt_suffix))
        .title_bottom(Line::from(Span::styled(
            format!("  {}/{}{} ", widget.cursor + 1, items.len(), match_info),
            Style::default().fg(Color::DarkGray),
        )))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));
    f.render_widget(List::new(visible).block(block), list_area);

    if has_search {
        let search_area = Rect { y: list_area.y + list_area.height, height: 3, ..area };
        render_search_bar(f, search_area, &widget.search, is_searching, widget.search_cursor);
    }
}
