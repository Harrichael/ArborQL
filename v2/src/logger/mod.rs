//! Thread-safe global log queue. Records are pushed via the `log!` macro
//! (which captures file/line at the call site) and read back as a snapshot
//! by consumers like an in-app viewer or a file sink.
//!
//! ```ignore
//! log!(Level::Info, "loaded {} rows", row_count);
//! let recent = logger::snapshot();
//! ```

mod queue;

pub use queue::{Level, Record, log_record, snapshot};
