use std::fmt;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogLevel {
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Info  => write!(f, "INFO "),
            LogLevel::Warn  => write!(f, "WARN "),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub level: LogLevel,
    pub message: String,
    /// Seconds since Unix epoch (UTC).
    pub timestamp: u64,
}

impl fmt::Display for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let secs = self.timestamp;
        let h = (secs / 3600) % 24;
        let m = (secs / 60) % 60;
        let s = secs % 60;
        write!(f, "{:02}:{:02}:{:02} [{}] {}", h, m, s, self.level, self.message)
    }
}

static LOG_QUEUE: Mutex<Vec<LogEntry>> = Mutex::new(Vec::new());

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn log(level: LogLevel, message: impl Into<String>) {
    if let Ok(mut q) = LOG_QUEUE.lock() {
        q.push(LogEntry { level, message: message.into(), timestamp: now_secs() });
    }
}

pub fn info(message: impl Into<String>)  { log(LogLevel::Info,  message); }
pub fn warn(message: impl Into<String>)  { log(LogLevel::Warn,  message); }
pub fn error(message: impl Into<String>) { log(LogLevel::Error, message); }

/// Drain all pending log entries into the caller's buffer (called each event-loop tick).
pub fn drain() -> Vec<LogEntry> {
    LOG_QUEUE.lock().map(|mut q| q.drain(..).collect()).unwrap_or_default()
}
