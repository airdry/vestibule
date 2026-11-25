//! Structured JSON logging: every runtime log record is emitted as one
//! JSON object to stderr, with the level filtered by the hot-reloadable
//! `observability.log_level` (read from the shared settings snapshot on
//! every record, so reloads apply to the next line).

use std::fmt;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::config::{LogLevel, SharedSettings};

/// The shared settings store installed once at startup; `None` before
/// `init` (tests, early startup) defaults the filter to `Info`.
static SETTINGS: OnceLock<SharedSettings> = OnceLock::new();

/// Installs the shared settings store used for level filtering.
pub fn init(settings: SharedSettings) {
    let _ = SETTINGS.set(settings);
}

/// The canonical name of a level, used as the JSON `level` field.
pub fn level_name(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Trace => "trace",
        LogLevel::Debug => "debug",
        LogLevel::Info => "info",
        LogLevel::Warn => "warn",
        LogLevel::Error => "error",
    }
}

/// The severity rank of a level; higher is more severe.
fn rank(level: LogLevel) -> u8 {
    match level {
        LogLevel::Trace => 0,
        LogLevel::Debug => 1,
        LogLevel::Info => 2,
        LogLevel::Warn => 3,
        LogLevel::Error => 4,
    }
}

/// `true` when a record at `record` severity passes `configured`.
pub fn should_emit(configured: LogLevel, record: LogLevel) -> bool {
    rank(record) >= rank(configured)
}

/// The currently configured level (hot-reloadable).
fn configured_level() -> LogLevel {
    SETTINGS
        .get()
        .map(|settings| settings.load().observability.log_level)
        .unwrap_or(LogLevel::Info)
}

/// Renders one JSON log record (pure, for unit tests).
pub fn record_json(level: LogLevel, target: &str, message: &str, timestamp: &str) -> String {
    serde_json::json!({
        "timestamp": timestamp,
        "level": level_name(level),
        "target": target,
        "message": message,
    })
    .to_string()
}

/// Emits one record to stderr when it passes the configured level.
pub fn emit(level: LogLevel, target: &str, message: fmt::Arguments<'_>) {
    if !should_emit(configured_level(), level) {
        return;
    }
    eprintln!(
        "{}",
        record_json(
            level,
            target,
            &message.to_string(),
            &iso8601(SystemTime::now()),
        )
    );
}

/// Renders `now` as an ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SSZ`).
pub fn iso8601(now: SystemTime) -> String {
    let seconds = now
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or_default();
    let (year, month, day) = civil_from_days((seconds / 86_400) as i64);
    let time_of_day = seconds % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time_of_day / 3_600,
        (time_of_day / 60) % 60,
        time_of_day % 60
    )
}

/// Converts days since the Unix epoch to a proleptic Gregorian date
/// (Howard Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    (if month <= 2 { year + 1 } else { year }, month as u32, day)
}

/// Emits an `error`-level record.
#[macro_export]
macro_rules! log_error {
    ($target:expr, $($arg:tt)*) => {
        $crate::logging::emit($crate::config::LogLevel::Error, $target, format_args!($($arg)*))
    };
}

/// Emits a `warn`-level record.
#[macro_export]
macro_rules! log_warn {
    ($target:expr, $($arg:tt)*) => {
        $crate::logging::emit($crate::config::LogLevel::Warn, $target, format_args!($($arg)*))
    };
}

/// Emits an `info`-level record.
#[macro_export]
macro_rules! log_info {
    ($target:expr, $($arg:tt)*) => {
        $crate::logging::emit($crate::config::LogLevel::Info, $target, format_args!($($arg)*))
    };
}

/// Emits a `debug`-level record.
#[macro_export]
macro_rules! log_debug {
    ($target:expr, $($arg:tt)*) => {
        $crate::logging::emit($crate::config::LogLevel::Debug, $target, format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_records_by_the_configured_level() {
        assert!(should_emit(LogLevel::Info, LogLevel::Error));
        assert!(should_emit(LogLevel::Info, LogLevel::Warn));
        assert!(should_emit(LogLevel::Info, LogLevel::Info));
        assert!(!should_emit(LogLevel::Info, LogLevel::Debug));
        assert!(!should_emit(LogLevel::Warn, LogLevel::Info));
        assert!(should_emit(LogLevel::Error, LogLevel::Error));
    }

    #[test]
    fn renders_json_records_with_escaping() {
        let record = record_json(
            LogLevel::Warn,
            "delivery",
            "dropped \"records\" after 400",
            "2025-01-02T03:04:05Z",
        );

        let parsed: serde_json::Value =
            serde_json::from_str(&record).expect("the record is one JSON object");
        assert_eq!(parsed["level"], "warn");
        assert_eq!(parsed["target"], "delivery");
        assert_eq!(parsed["timestamp"], "2025-01-02T03:04:05Z");
        assert_eq!(parsed["message"], "dropped \"records\" after 400");
    }

    #[test]
    fn renders_iso8601_timestamps() {
        assert_eq!(iso8601(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            iso8601(SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000)),
            "2023-11-14T22:13:20Z"
        );
    }
}
