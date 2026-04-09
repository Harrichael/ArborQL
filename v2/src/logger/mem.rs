use std::sync::Mutex;

use super::record::Record;

static LOG: Mutex<Vec<Record>> = Mutex::new(Vec::new());

pub fn push(record: Record) {
    // Logging must never panic, so a poisoned mutex is recovered rather
    // than propagated. A poisoned log is still a usable log.
    let mut guard = LOG.lock().unwrap_or_else(|p| p.into_inner());
    guard.push(record);
}

pub fn snapshot() -> Vec<Record> {
    let guard = LOG.lock().unwrap_or_else(|p| p.into_inner());
    guard.clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log;
    use crate::logger::record::Level;
    use std::thread;

    #[test]
    fn concurrent_pushes_all_recorded_with_call_site() {
        // Exercises the macro, thread-safety, and snapshot together — the
        // three things the in-memory sink is responsible for.
        //
        // The global queue is shared across the whole test binary, so we
        // identify our records by a unique marker rather than counting the
        // raw delta in `snapshot().len()`.
        const MARKER: &str = "MEMTEST_MARKER_concurrent";

        let handles: Vec<_> = (0..8)
            .map(|i| {
                thread::spawn(move || {
                    for j in 0..50 {
                        log!(Level::Info, "{} thread {} msg {}", MARKER, i, j);
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        let mine: Vec<_> = snapshot()
            .into_iter()
            .filter(|r| r.message.contains(MARKER))
            .collect();
        assert_eq!(mine.len(), 8 * 50);
        assert!(mine.iter().all(|r| r.file.ends_with("mem.rs")));
        assert!(mine.iter().all(|r| r.line > 0));
        assert!(mine.iter().all(|r| matches!(r.level, Level::Info)));

        let distinct_threads: std::collections::HashSet<_> =
            mine.iter().map(|r| r.thread_id).collect();
        assert_eq!(distinct_threads.len(), 8);
    }
}
