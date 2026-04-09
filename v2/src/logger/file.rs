//! File sink for the logger.
//!
//! `init(dir)` opens a fresh, uniquely-named log file inside `dir` and spawns
//! a tokio task that owns the file handle. `log_record` (in `dispatch.rs`)
//! hands records over via an unbounded mpsc channel — the send is sync and
//! cheap, so log call sites never block on disk I/O.
//!
//! `init` must be called from within a tokio runtime, since it calls
//! `tokio::spawn`. The returned `Guard` should be drained explicitly via
//! `guard.shutdown().await` to flush in-flight records before exit. Dropping
//! the guard without `shutdown` is best-effort: the channel closes and the
//! task will eventually finish, but the caller does not wait.
//!
//! ```ignore
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let guard = logger::init("/var/log/myapp")?;
//!     log!(Level::Info, "started");
//!     guard.shutdown().await;
//!     Ok(())
//! }
//! ```

use std::fs::OpenOptions;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;

use super::record::{Level, Record};

const FILENAME_RETRY_LIMIT: usize = 5;

static SENDER: Mutex<Option<UnboundedSender<Record>>> = Mutex::new(None);

#[derive(Debug)]
pub enum InitError {
    AlreadyInitialized,
    Io(io::Error),
}

impl From<io::Error> for InitError {
    fn from(e: io::Error) -> Self {
        InitError::Io(e)
    }
}

pub struct Guard {
    join: Option<JoinHandle<()>>,
}

impl Guard {
    // Deterministic flush + join. Prefer this over relying on Drop when you
    // need the file to reflect everything logged so far (e.g. before exiting
    // main, or in tests).
    pub async fn shutdown(mut self) {
        {
            let mut slot = SENDER.lock().unwrap_or_else(|p| p.into_inner());
            slot.take();
        }
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        // Best-effort: drop the static sender so the writer task's recv()
        // returns None and the task exits naturally. We can't .await the
        // JoinHandle from a sync Drop, so the join handle is detached. If
        // determinism matters, callers should use `Guard::shutdown` instead.
        let mut slot = SENDER.lock().unwrap_or_else(|p| p.into_inner());
        slot.take();
    }
}

pub fn init(dir: impl AsRef<Path>) -> Result<Guard, InitError> {
    let dir = dir.as_ref();

    // Check init-once before doing any I/O so a failed re-init doesn't
    // create a stray file.
    {
        let slot = SENDER.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_some() {
            return Err(InitError::AlreadyInitialized);
        }
    }

    let (std_file, _path) = open_unique_file(dir)?;
    let file = File::from_std(std_file);

    let (tx, rx) = mpsc::unbounded_channel::<Record>();
    let join = tokio::spawn(writer_loop(file, rx));

    {
        let mut slot = SENDER.lock().unwrap_or_else(|p| p.into_inner());
        // Race: another caller may have init'd between our check and here.
        // If so, abandon what we just built. The task will exit on its own
        // once `tx` (held only here) is dropped.
        if slot.is_some() {
            drop(tx);
            join.abort();
            return Err(InitError::AlreadyInitialized);
        }
        *slot = Some(tx);
    }

    Ok(Guard { join: Some(join) })
}

pub fn push(record: Record) {
    // Short critical section: clone the sender out, then send without
    // holding the mutex. If the sink isn't initialized this is a no-op.
    let sender = {
        let slot = SENDER.lock().unwrap_or_else(|p| p.into_inner());
        slot.clone()
    };
    if let Some(tx) = sender {
        let _ = tx.send(record);
    }
}

async fn writer_loop(mut file: File, mut rx: UnboundedReceiver<Record>) {
    while let Some(record) = rx.recv().await {
        let _ = write_record(&mut file, &record).await;
        // Drain pending records to batch the flush.
        while let Ok(more) = rx.try_recv() {
            let _ = write_record(&mut file, &more).await;
        }
        let _ = file.flush().await;
    }
    let _ = file.flush().await;
}

async fn write_record(file: &mut File, r: &Record) -> io::Result<()> {
    let (date, hms, millis) = format_timestamp(r.timestamp);
    let line = format!(
        "{date}T{hms}.{millis:03}Z {level:<5} {src}:{ln} [{tid:?}] {msg}\n",
        level = level_str(r.level),
        src = r.file,
        ln = r.line,
        tid = r.thread_id,
        msg = r.message,
    );
    file.write_all(line.as_bytes()).await
}

fn level_str(l: Level) -> &'static str {
    match l {
        Level::Error => "ERROR",
        Level::Warn => "WARN",
        Level::Info => "INFO",
        Level::Debug => "DEBUG",
        Level::Trace => "TRACE",
    }
}

fn open_unique_file(dir: &Path) -> io::Result<(std::fs::File, PathBuf)> {
    let (date, hms, _) = format_timestamp(SystemTime::now());
    let hms_compact: String = hms.chars().filter(|c| *c != ':').collect();
    let mut last_err = None;
    for _ in 0..FILENAME_RETRY_LIMIT {
        let id = random_hex_id()?;
        let path = dir.join(format!("{date}-{hms_compact}-{id}.log"));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(file) => return Ok((file, path)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                last_err = Some(e);
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_err.unwrap_or_else(|| io::Error::other("failed to create unique log file")))
}

fn random_hex_id() -> io::Result<String> {
    let mut buf = [0u8; 3];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut buf)?;
    Ok(format!("{:02x}{:02x}{:02x}", buf[0], buf[1], buf[2]))
}

// Returns (YYYY-MM-DD, HH:MM:SS, milliseconds-of-second).
fn format_timestamp(t: SystemTime) -> (String, String, u32) {
    let dur = t.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO);
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();

    let days = (secs / 86_400) as i64;
    let sod = (secs % 86_400) as u32;
    let hh = sod / 3600;
    let mm = (sod % 3600) / 60;
    let ss = sod % 60;

    let (y, mo, d) = days_to_ymd(days);
    (
        format!("{y:04}-{mo:02}-{d:02}"),
        format!("{hh:02}:{mm:02}:{ss:02}"),
        millis,
    )
}

// Howard Hinnant's days-from-civil algorithm, inverted.
// Input: days since 1970-01-01. Output: (year, month [1-12], day [1-31]).
fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log;
    use std::fs;

    #[test]
    fn days_to_ymd_known_dates() {
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
        assert_eq!(days_to_ymd(31), (1970, 2, 1));
        assert_eq!(days_to_ymd(365), (1971, 1, 1));
        // 2020-02-29 is day 18321 from 1970-01-01.
        assert_eq!(days_to_ymd(18_321), (2020, 2, 29));
    }

    #[tokio::test]
    async fn init_writes_records_and_rejects_double_init() {
        // SENDER is process-global, so this test is the sole owner of the
        // file sink for the duration of each iteration. Other tests in the
        // same binary may concurrently log via the in-mem sink; their
        // records will land in our file too, so we identify our own records
        // by a unique marker substring rather than counting line totals.
        //
        // The body is looped so a single `cargo test` run repeatedly
        // exercises the init → log → shutdown → drain path under
        // concurrent test load. If draining is racy, marker counts will
        // miss inside one of the iterations.
        const ITERATIONS: usize = 25;
        const RECORDS_PER_ITER: usize = 20;

        for iter in 0..ITERATIONS {
            let dir = make_temp_dir(&format!("file_sink_basic_{iter}"));

            let guard = init(&dir).expect("first init should succeed");
            assert!(matches!(
                init(&dir),
                Err(InitError::AlreadyInitialized)
            ));

            let marker = format!("FILESINK_MARKER_basic_{iter}");
            for i in 0..RECORDS_PER_ITER {
                log!(Level::Info, "{} idx {}", marker, i);
            }

            guard.shutdown().await;

            let entries: Vec<PathBuf> = fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap().path())
                .collect();
            assert_eq!(
                entries.len(),
                1,
                "iter {iter}: expected one log file, got {entries:?}"
            );
            let path = &entries[0];
            let name = path.file_name().unwrap().to_str().unwrap();
            // YYYY-MM-DD-HHMMSS-xxxxxx.log == 10+1+6+1+6+4 = 28 chars.
            assert_eq!(name.len(), 28, "iter {iter}: filename shape: {name}");
            assert!(name.ends_with(".log"));

            let contents = fs::read_to_string(path).unwrap();
            let marker_count = contents.matches(marker.as_str()).count();
            assert_eq!(
                marker_count, RECORDS_PER_ITER,
                "iter {iter}: expected {RECORDS_PER_ITER} marker lines, got {marker_count}"
            );
            assert!(contents.contains("INFO"));

            // After shutdown, init must succeed again on a fresh dir.
            let dir2 = make_temp_dir(&format!("file_sink_basic_{iter}_reinit"));
            let guard2 = init(&dir2).expect("re-init after shutdown should succeed");
            guard2.shutdown().await;
            let _ = fs::remove_dir_all(&dir2);

            let _ = fs::remove_dir_all(&dir);
        }
    }

    fn make_temp_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir()
            .join(format!("latticeql_test_{}_{}_{}", label, std::process::id(), nanos));
        fs::create_dir_all(&dir).unwrap();
        dir
    }
}
