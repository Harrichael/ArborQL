//! Thread-safe global logger with two sinks: an in-memory queue (snapshotable
//! by an in-app viewer) and an optional file sink (initialized via `init`,
//! writes happen on a background thread).
//!
//! ```ignore
//! let _guard = logger::init("/var/log/myapp")?;
//! log!(Level::Info, "loaded {} rows", row_count);
//! let recent = logger::snapshot();
//! ```

mod dispatch;
mod file;
mod mem;
mod record;

pub use dispatch::log_record;
pub use file::{Guard, InitError, init};
pub use mem::snapshot;
pub use record::{Level, Record};
