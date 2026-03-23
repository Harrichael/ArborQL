mod db;
mod engine;
mod rules;
mod schema;
mod ui;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use engine::{Engine, available_extra_columns, flatten_tree};
use ratatui::{Terminal, backend::CrosstermBackend};
use schema::Schema;
use std::io;
use ui::app::{AppState, Mode};

/// ArborQL — Navigate complex datasets from multiple sources intuitively.
#[derive(Parser, Debug)]
#[command(name = "arborql", version, about)]
struct Args {
    /// Database connection URL.
    ///
    /// Examples:
    ///   sqlite://path/to/db.sqlite3
    ///   mysql://user:password@localhost/dbname
    #[arg(short, long)]
    database: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Connect to the database
    eprintln!("Connecting to database…");
    let db = db::connect(&args.database).await?;

    // Explore schema
    eprintln!("Exploring schema…");
    let schema = Schema::explore(db.as_ref()).await?;
    let table_names = schema.table_names();

    let mut engine = Engine::new(schema);
    let mut state = AppState::new();
    state.table_names = table_names;

    // Set up terminal
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_app(&mut terminal, &mut state, &mut engine, db.as_ref()).await;

    // Restore terminal
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
) -> Result<()> {
    // Pending paths waiting for user selection
    let mut pending_paths: Option<(rules::Rule, Vec<schema::TablePath>)> = None;

    loop {
        // Draw
        terminal.draw(|f| ui::render::render(f, state, &engine.roots))?;

        // Handle events (with a timeout so we can do async work)
        if event::poll(std::time::Duration::from_millis(50))? {
            let ev = event::read()?;
            match ev {
                Event::Key(key) => {
                    let handled = handle_key(
                        key,
                        state,
                        engine,
                        db,
                        &mut pending_paths,
                    )
                    .await?;
                    if !handled {
                        // Quit signal
                        break;
                    }
                }
                Event::Resize(_, _) => {} // terminal handles this automatically
                _ => {}
            }
        }
    }
    Ok(())
}

/// Returns `false` when the application should quit.
async fn handle_key(
    key: crossterm::event::KeyEvent,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
    pending_paths: &mut Option<(rules::Rule, Vec<schema::TablePath>)>,
) -> Result<bool> {
    match state.mode.clone() {
        // ── Normal mode ──────────────────────────────────────────────────
        Mode::Normal => {
            match key.code {
                KeyCode::Char('q') | KeyCode::Char('Q') => return Ok(false),
                KeyCode::Char(':') => {
                    state.mode = Mode::Command;
                    state.clear_input();
                }
                KeyCode::Char('j') | KeyCode::Down => state.select_down(),
                KeyCode::Char('k') | KeyCode::Up => state.select_up(),
                KeyCode::Char('f') | KeyCode::Enter => {
                    // Toggle fold on selected node
                    let flat = flatten_tree(&engine.roots);
                    if state.selected_row < flat.len() {
                        toggle_fold(&mut engine.roots, state.selected_row);
                    }
                }
                KeyCode::Char('s') => {
                    state.show_schema = !state.show_schema;
                }
                KeyCode::Char('r') => {
                    if !engine.rules.is_empty() {
                        state.rules = engine.rules.clone();
                        state.rule_cursor = 0;
                        state.mode = Mode::RuleReorder;
                    }
                }
                KeyCode::Char('c') => {
                    // Add column to selected node
                    let flat = flatten_tree(&engine.roots);
                    if state.selected_row < flat.len() {
                        let (_, node) = flat[state.selected_row];
                        let extras = available_extra_columns(node);
                        if !extras.is_empty() {
                            let mut sorted_extras = extras;
                            sorted_extras.sort();
                            state.column_add = Some((state.selected_row, sorted_extras, 0));
                        } else {
                            state.mode = Mode::Info("No extra columns available".to_string());
                        }
                    }
                }
                _ => {}
            }
        }

        // ── Command mode ─────────────────────────────────────────────────
        Mode::Command => {
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                    state.clear_input();
                }
                KeyCode::Enter => {
                    let cmd = state.input_text().trim().to_string();
                    state.mode = Mode::Normal;
                    state.clear_input();
                    if !cmd.is_empty() {
                        execute_command(cmd, state, engine, db, pending_paths).await?;
                    }
                }
                KeyCode::Char(c) => {
                    if key.modifiers.contains(KeyModifiers::CONTROL) && c == 'c' {
                        return Ok(false);
                    }
                    state.input_char(c);
                }
                KeyCode::Backspace => state.input_backspace(),
                KeyCode::Delete => state.input_delete(),
                KeyCode::Left => state.cursor_left(),
                KeyCode::Right => state.cursor_right(),
                _ => {}
            }
        }

        // ── Path selection overlay ────────────────────────────────────────
        Mode::PathSelection => {
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                    *pending_paths = None;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if state.path_cursor > 0 {
                        state.path_cursor -= 1;
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if state.path_cursor + 1 < state.paths.len() {
                        state.path_cursor += 1;
                    }
                }
                KeyCode::Enter => {
                    if let Some((rule, paths)) = pending_paths.take() {
                        let chosen = &paths[state.path_cursor];
                        // Apply the chosen path
                        engine.apply_relation_rule(db, chosen).await?;
                        // Update rule with the via path
                        let updated_rule = match rule {
                            rules::Rule::Relation { from_table, to_table, .. } => {
                                let via: Vec<String> = chosen
                                    .steps
                                    .iter()
                                    .skip(1)
                                    .map(|s| s.from_table.clone())
                                    .collect();
                                rules::Rule::Relation { from_table, to_table, via }
                            }
                            other => other,
                        };
                        engine.rules.push(updated_rule);
                    }
                    state.mode = Mode::Normal;
                    state.paths.clear();
                }
                _ => {}
            }
        }

        // ── Rule reorder overlay ─────────────────────────────────────────
        Mode::RuleReorder => {
            match key.code {
                KeyCode::Esc => {
                    state.mode = Mode::Normal;
                }
                KeyCode::Enter => {
                    // Apply reordered rules
                    engine.rules = state.rules.clone();
                    let _ = engine.reexecute_all(db).await;
                    state.mode = Mode::Normal;
                }
                KeyCode::Up | KeyCode::Char('k') => {
                    if state.rule_cursor > 0 {
                        state.rule_cursor -= 1;
                    }
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    if state.rule_cursor + 1 < state.rules.len() {
                        state.rule_cursor += 1;
                    }
                }
                KeyCode::Char('u') => {
                    // Swap up
                    if state.rule_cursor > 0 {
                        state.rules.swap(state.rule_cursor, state.rule_cursor - 1);
                        state.rule_cursor -= 1;
                    }
                }
                KeyCode::Char('d') => {
                    // Swap down
                    if state.rule_cursor + 1 < state.rules.len() {
                        state.rules.swap(state.rule_cursor, state.rule_cursor + 1);
                        state.rule_cursor += 1;
                    }
                }
                _ => {}
            }
        }

        // ── Error / Info overlays — any key dismisses ────────────────────
        Mode::Error(_) | Mode::Info(_) => {
            state.mode = Mode::Normal;
        }
    }

    // Handle column-add overlay (independent of mode)
    if state.column_add.is_some() {
        match key.code {
            KeyCode::Esc => {
                state.column_add = None;
            }
            KeyCode::Up | KeyCode::Char('k') => {
                if let Some((_, _, ref mut cursor)) = state.column_add {
                    if *cursor > 0 {
                        *cursor -= 1;
                    }
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if let Some((_, ref cols, ref mut cursor)) = state.column_add {
                    if *cursor + 1 < cols.len() {
                        *cursor += 1;
                    }
                }
            }
            KeyCode::Enter => {
                if let Some((row_idx, ref cols, cursor)) = state.column_add.clone() {
                    let col_name = cols[cursor].clone();
                    add_column_to_node(&mut engine.roots, row_idx, &col_name);
                }
                state.column_add = None;
            }
            _ => {}
        }
    }

    Ok(true)
}

/// Execute a command string entered in command mode.
async fn execute_command(
    cmd: String,
    state: &mut AppState,
    engine: &mut Engine,
    db: &dyn db::Database,
    pending_paths: &mut Option<(rules::Rule, Vec<schema::TablePath>)>,
) -> Result<()> {
    match rules::parse_rule(&cmd) {
        Err(e) => {
            state.mode = Mode::Error(e);
        }
        Ok(rule) => {
            match engine.execute_rule(db, rule.clone()).await {
                Err(e) => {
                    state.mode = Mode::Error(e.to_string());
                }
                Ok(None) => {
                    // Success
                }
                Ok(Some(paths)) => {
                    // Multiple paths — ask user to pick
                    state.paths = paths.clone();
                    state.path_cursor = 0;
                    state.mode = Mode::PathSelection;
                    *pending_paths = Some((rule, paths));
                }
            }
        }
    }
    Ok(())
}

/// Toggle the collapsed state of the node at `flat_idx` in the tree.
fn toggle_fold(roots: &mut [engine::DataNode], flat_idx: usize) {
    let mut counter = 0usize;
    toggle_fold_recursive(roots, flat_idx, &mut counter);
}

fn toggle_fold_recursive(
    nodes: &mut [engine::DataNode],
    target: usize,
    counter: &mut usize,
) -> bool {
    for node in nodes.iter_mut() {
        if *counter == target {
            node.collapsed = !node.collapsed;
            return true;
        }
        *counter += 1;
        if !node.collapsed && toggle_fold_recursive(&mut node.children, target, counter) {
            return true;
        }
    }
    false
}

/// Add `col_name` to the visible columns of the node at `flat_idx`.
fn add_column_to_node(roots: &mut [engine::DataNode], flat_idx: usize, col_name: &str) {
    let mut counter = 0usize;
    add_column_recursive(roots, flat_idx, col_name, &mut counter);
}

fn add_column_recursive(
    nodes: &mut [engine::DataNode],
    target: usize,
    col_name: &str,
    counter: &mut usize,
) -> bool {
    for node in nodes.iter_mut() {
        if *counter == target {
            if !node.visible_columns.contains(&col_name.to_string()) {
                node.visible_columns.push(col_name.to_string());
            }
            return true;
        }
        *counter += 1;
        if add_column_recursive(&mut node.children, target, col_name, counter) {
            return true;
        }
    }
    false
}
