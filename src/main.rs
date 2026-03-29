mod app;
mod command_history;
mod config;
mod connection_manager;
mod db;
mod engine;
mod log;
mod rules;
mod schema;

use anyhow::Result;
use clap::Parser;
use crossterm::{
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{Terminal, backend::CrosstermBackend};

use std::io;
use app::data_playground::{DataPlayground, TickResult};

/// LatticeQL — Navigate complex datasets from multiple sources intuitively.
#[derive(Parser, Debug)]
#[command(name = "latticeql", version, about)]
struct Args {
    /// Database connection URL (optional — can also add via the connection manager).
    ///
    /// Examples:
    ///   sqlite://path/to/db.sqlite3
    ///   mysql://user:password@localhost/dbname
    #[arg(short, long)]
    database: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut playground = DataPlayground::new(args.database).await?;

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;

    let result = loop {
        terminal.draw(|f| playground.render(f))?;

        match playground.tick().await? {
            TickResult::Continue => {}
            TickResult::Suspend => {
                #[cfg(unix)]
                {
                    disable_raw_mode()?;
                    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
                    terminal.show_cursor()?;
                    unsafe { libc::raise(libc::SIGTSTP) };
                    enable_raw_mode()?;
                    execute!(terminal.backend_mut(), EnterAlternateScreen)?;
                    terminal.clear()?;
                }
            }
            TickResult::Quit => break Ok(()),
        }
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}
