use super::file;
use super::mem;
use super::record::Record;

pub fn log_record(record: Record) {
    // Clone for the in-mem sink; the file sink takes ownership. Records are
    // small (one String) so the clone cost is acceptable for v2.
    mem::push(record.clone());
    file::push(record);
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
