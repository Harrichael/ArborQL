use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// A single command entered by the user.
#[derive(Debug, Clone)]
pub struct Command {
    /// The command text (trimmed).
    pub text: String,
    /// Unix timestamp (seconds since epoch) when the command was entered.
    pub timestamp: u64,
}

/// Append-only command history.
///
/// Empty commands are silently ignored.  Consecutive identical commands are
/// deduplicated so that rapidly re-entering the same command does not flood the
/// history with redundant entries.  Non-consecutive duplicates are always
/// recorded — the "don't re-add" logic for navigated-history re-runs is handled
/// at the call site in the key handler, not here.
#[derive(Debug, Default)]
pub struct CommandHistory {
    entries: Vec<Command>,
}

impl CommandHistory {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    /// Load history from a file, keeping at most `max_len` most-recent entries.
    ///
    /// Each line in the file has the format `<timestamp>\t<command text>`.
    /// Lines that cannot be parsed are silently skipped.  If the file does not
    /// exist an empty history is returned.
    pub fn load_from_file(path: &Path, max_len: usize) -> io::Result<Self> {
        if !path.exists() {
            return Ok(Self::new());
        }
        let file = fs::File::open(path)?;
        let reader = io::BufReader::new(file);
        let mut entries: Vec<Command> = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            // Format: "<timestamp>\t<text>"
            // Split on the first tab only so text may contain tabs.
            if let Some(tab_pos) = line.find('\t') {
                let ts_str = &line[..tab_pos];
                let text = line[tab_pos + 1..].to_string();
                let timestamp = ts_str.parse::<u64>().unwrap_or(0);
                if !text.is_empty() {
                    entries.push(Command { text, timestamp });
                }
            }
        }
        // Keep only the most recent `max_len` entries.
        if entries.len() > max_len {
            entries.drain(..entries.len() - max_len);
        }
        Ok(Self { entries })
    }

    /// Append a single `Command` to the history file.
    ///
    /// Creates the file (and parent directories) if they do not yet exist.
    pub fn append_to_file(entry: &Command, path: &Path) -> io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        writeln!(file, "{}\t{}", entry.timestamp, entry.text)?;
        Ok(())
    }

    /// Append a command to the history.
    ///
    /// Rules:
    /// - Blank (whitespace-only) commands are ignored.
    /// - If the new text is identical to the most recent entry it is not added
    ///   again (consecutive-duplicate suppression).
    ///
    /// Returns `true` if the entry was actually added, `false` if it was
    /// suppressed (empty or consecutive duplicate).
    pub fn push(&mut self, text: impl Into<String>) -> bool {
        let text = text.into();
        let text = text.trim().to_string();
        if text.is_empty() {
            return false;
        }
        if self.entries.last().map(|e| e.text.as_str()) == Some(text.as_str()) {
            return false;
        }
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.entries.push(Command { text, timestamp });
        true
    }

    /// All entries, ordered from oldest (index 0) to most recent.
    pub fn entries(&self) -> &[Command] {
        &self.entries
    }

    /// Number of recorded commands.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Return the index of the (skip+1)-th most-recent entry whose text
    /// contains `query` (case-insensitive). Returns `None` when no such entry
    /// exists.
    pub fn search_reverse(&self, query: &str, skip: usize) -> Option<usize> {
        let q = query.to_lowercase();
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, e)| e.text.to_lowercase().contains(&q))
            .rev()
            .nth(skip)
            .map(|(i, _)| i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_history_path() -> std::path::PathBuf {
        let base = std::env::temp_dir().join(format!(
            "latticeql-history-test-{}.txt",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        base
    }

    #[test]
    fn empty_commands_not_added() {
        let mut h = CommandHistory::new();
        assert!(!h.push(""));
        assert!(!h.push("   "));
        assert!(h.is_empty());
    }

    #[test]
    fn consecutive_duplicates_suppressed() {
        let mut h = CommandHistory::new();
        assert!(h.push("users"));
        assert!(!h.push("users"));
        assert_eq!(h.len(), 1);
    }

    #[test]
    fn non_consecutive_duplicates_allowed() {
        let mut h = CommandHistory::new();
        h.push("users");
        h.push("orders");
        h.push("users");
        assert_eq!(h.len(), 3);
    }

    #[test]
    fn search_reverse_finds_most_recent_first() {
        let mut h = CommandHistory::new();
        h.push("users where id = 1");
        h.push("orders");
        h.push("users where id = 2");
        // Most recent match for "users" is index 2.
        assert_eq!(h.search_reverse("users", 0), Some(2));
        // Second most recent is index 0.
        assert_eq!(h.search_reverse("users", 1), Some(0));
        // No third match.
        assert_eq!(h.search_reverse("users", 2), None);
    }

    #[test]
    fn search_reverse_no_match() {
        let mut h = CommandHistory::new();
        h.push("orders");
        assert_eq!(h.search_reverse("users", 0), None);
    }

    #[test]
    fn append_and_load_roundtrip() {
        let path = temp_history_path();
        let cmd = Command { text: "users where id = 1".to_string(), timestamp: 1_700_000_000 };
        CommandHistory::append_to_file(&cmd, &path).unwrap();
        let cmd2 = Command { text: "orders".to_string(), timestamp: 1_700_000_001 };
        CommandHistory::append_to_file(&cmd2, &path).unwrap();

        let h = CommandHistory::load_from_file(&path, 10_000).unwrap();
        assert_eq!(h.len(), 2);
        assert_eq!(h.entries()[0].text, "users where id = 1");
        assert_eq!(h.entries()[0].timestamp, 1_700_000_000);
        assert_eq!(h.entries()[1].text, "orders");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_respects_max_len() {
        let path = temp_history_path();
        for i in 0..20u64 {
            let cmd = Command { text: format!("cmd{}", i), timestamp: i };
            CommandHistory::append_to_file(&cmd, &path).unwrap();
        }
        let h = CommandHistory::load_from_file(&path, 5).unwrap();
        assert_eq!(h.len(), 5);
        // Should keep the 5 most recent.
        assert_eq!(h.entries()[0].text, "cmd15");
        assert_eq!(h.entries()[4].text, "cmd19");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn load_missing_file_returns_empty() {
        let path = std::path::Path::new("/tmp/latticeql-nonexistent-history-file.txt");
        let h = CommandHistory::load_from_file(path, 10_000).unwrap();
        assert!(h.is_empty());
    }
}
