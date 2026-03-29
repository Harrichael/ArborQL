mod key_handler;
mod module;
mod overlay_dispatch;
pub mod widgets;

use std::path::PathBuf;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers};

use crate::connection_manager::ConnectionManager;
use crate::engine;
use crate::log;
use crate::rules;
use crate::engine::Engine;
use crate::ui::app::AppState;

pub enum TickResult {
    Continue,
    Suspend,
    Quit,
}

pub struct DataPlayground {
    pub state: AppState,
    pub engine: Engine,
    pub conn_mgr: ConnectionManager,
    pending_paths: Option<(rules::Rule, Vec<engine::TablePath>)>,
    history_file: Option<PathBuf>,
}

impl DataPlayground {
    /// Drain logs, poll for events, handle key input.
    /// Returns `Suspend` when Ctrl+Z was pressed, `Quit` to exit.
    pub async fn tick(&mut self) -> Result<TickResult> {
        self.state.logs.extend(log::drain());

        if event::poll(std::time::Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) if key.kind == KeyEventKind::Press || key.kind == KeyEventKind::Repeat => {
                    return self.handle_key(key).await;
                }
                _ => {}
            }
        }
        Ok(TickResult::Continue)
    }

    /// Render the current state to the given frame.
    pub fn render(&mut self, f: &mut ratatui::Frame) {
        crate::ui::render::render(f, &mut self.state, &self.engine.roots);
    }

    async fn handle_key(&mut self, key: KeyEvent) -> Result<TickResult> {
        // Ctrl+Z suspends regardless of current mode.
        if key.code == KeyCode::Char('z') && key.modifiers.contains(KeyModifiers::CONTROL) {
            return Ok(TickResult::Suspend);
        }

        // Try overlay dispatch first.
        if let Some(result) = overlay_dispatch::dispatch_overlay(self, key).await? {
            return Ok(result);
        }

        // Fall through to mode-based key handling.
        key_handler::handle_mode_key(self, key).await
    }
}
