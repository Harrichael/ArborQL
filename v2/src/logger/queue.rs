use std::sync::Mutex;
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

static LOG: Mutex<Vec<Record>> = Mutex::new(Vec::new());

pub fn log_record(record: Record) {
    // Logging must never panic, so a poisoned mutex is recovered rather
    // than propagated. A poisoned log is still a usable log.
    let mut guard = LOG.lock().unwrap_or_else(|p| p.into_inner());
    guard.push(record);
}

pub fn snapshot() -> Vec<Record> {
    let guard = LOG.lock().unwrap_or_else(|p| p.into_inner());
    guard.clone()
}

#[macro_export]
macro_rules! log {
    ($level:expr, $($arg:tt)*) => {
        $crate::logger::log_record($crate::logger::Record {
            timestamp: ::std::time::SystemTime::now(),
            level: $level,
            file: ::std::file!(),
            line: ::std::line!(),
            thread_id: ::std::thread::current().id(),
            message: ::std::format!($($arg)*),
        })
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn concurrent_pushes_all_recorded_with_call_site() {
        // Exercises the macro, thread-safety, and snapshot together — the
        // three things a logger is actually responsible for.
        let before = snapshot().len();

        let handles: Vec<_> = (0..8)
            .map(|i| {
                thread::spawn(move || {
                    for j in 0..50 {
                        log!(Level::Info, "thread {} msg {}", i, j);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let after = snapshot();
        assert_eq!(after.len() - before, 8 * 50);

        let new_records = &after[before..];
        assert!(new_records.iter().all(|r| r.file.ends_with("queue.rs")));
        assert!(new_records.iter().all(|r| r.line > 0));
        assert!(new_records.iter().all(|r| matches!(r.level, Level::Info)));

        let distinct_threads: std::collections::HashSet<_> =
            new_records.iter().map(|r| r.thread_id).collect();
        assert_eq!(distinct_threads.len(), 8);
    }
}
