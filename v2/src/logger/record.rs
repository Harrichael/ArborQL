use std::thread::ThreadId;
use std::time::SystemTime;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Debug, Clone)]
pub struct Record {
    pub timestamp: SystemTime,
    pub level: Level,
    pub file: &'static str,
    pub line: u32,
    pub thread_id: ThreadId,
    pub message: String,
}
