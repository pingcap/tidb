// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::any::Any;
use std::backtrace::Backtrace;
use std::panic::{catch_unwind, AssertUnwindSafe};

use chrono::{DateTime, FixedOffset};
use thiserror::Error;

use crate::trace::TraceContext;

pub const GC_TIME_FORMAT: &str = "20060102-15:04:05.000 -0700";
const BYTE_SIZE_GB: i64 = 1 << 30;
const BYTE_SIZE_MB: i64 = 1 << 20;
const BYTE_SIZE_KB: i64 = 1 << 10;

struct SessionIdKey;

#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("string \"{value}\" doesn't has a prefix that matches format \"{GC_TIME_FORMAT}\"")]
pub struct GcTimeParseError {
    value: String,
}

/// Parse the persisted GC timestamp, accepting one legacy trailing zone token.
pub fn compatible_parse_gc_time(value: &str) -> Result<DateTime<FixedOffset>, GcTimeParseError> {
    fn parse(value: &str) -> Option<DateTime<FixedOffset>> {
        DateTime::parse_from_str(value, "%Y%m%d-%H:%M:%S%.f %z")
            .or_else(|_| DateTime::parse_from_str(value, "%Y%m%d-%H:%M:%S %z"))
            .ok()
    }

    parse(value)
        .or_else(|| value.rsplit_once(' ').and_then(|(prefix, _)| parse(prefix)))
        .ok_or_else(|| GcTimeParseError {
            value: value.to_owned(),
        })
}

/// Execute a recoverable task, invoke its recovery hook in both outcomes, and
/// swallow/log an unwinding panic like client-go's goroutine helper.
pub fn with_recovery<E, R>(exec: E, recover_fn: Option<R>)
where
    E: FnOnce(),
    R: FnOnce(Option<&(dyn Any + Send)>),
{
    let outcome = catch_unwind(AssertUnwindSafe(exec));
    if let Some(recover_fn) = recover_fn {
        recover_fn(outcome.as_ref().err().map(|panic| panic.as_ref()));
    }
    if let Err(panic) = outcome {
        let panic = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic");
        log::error!(
            "panic in the recoverable task, r: {panic}, stack trace: {}",
            Backtrace::force_capture()
        );
    }
}

pub fn with_session_id(context: &TraceContext, session_id: u64) -> TraceContext {
    context.with_value::<SessionIdKey, _>(session_id)
}

pub fn session_id(context: &TraceContext) -> Option<u64> {
    context.value::<SessionIdKey, u64>().copied()
}

pub fn format_bytes(num_bytes: i64) -> String {
    if num_bytes <= BYTE_SIZE_KB {
        return bytes_to_string(num_bytes);
    }
    let (unit, unit_name) = byte_unit(num_bytes);
    if unit == 1 {
        return bytes_to_string(num_bytes);
    }
    let value = num_bytes as f64 / unit as f64;
    let decimals = if num_bytes % unit == 0 {
        0
    } else if value < 10.0 {
        2
    } else {
        1
    };
    format!("{value:.decimals$} {unit_name}")
}

fn byte_unit(bytes: i64) -> (i64, &'static str) {
    if bytes > BYTE_SIZE_GB {
        (BYTE_SIZE_GB, "GB")
    } else if bytes > BYTE_SIZE_MB {
        (BYTE_SIZE_MB, "MB")
    } else if bytes > BYTE_SIZE_KB {
        (BYTE_SIZE_KB, "KB")
    } else {
        (1, "Bytes")
    }
}

pub fn bytes_to_string(num_bytes: i64) -> String {
    let gb = num_bytes as f64 / BYTE_SIZE_GB as f64;
    if gb > 1.0 {
        return format!("{gb} GB");
    }
    let mb = num_bytes as f64 / BYTE_SIZE_MB as f64;
    if mb > 1.0 {
        return format!("{mb} MB");
    }
    let kb = num_bytes as f64 / BYTE_SIZE_KB as f64;
    if kb > 1.0 {
        return format!("{kb} KB");
    }
    format!("{num_bytes} Bytes")
}

pub fn get_max_start_key<'a>(left: &'a [u8], right: &'a [u8]) -> &'a [u8] {
    if left > right {
        left
    } else {
        right
    }
}

pub fn get_min_end_key<'a>(left: &'a [u8], right: &'a [u8]) -> &'a [u8] {
    if right.is_empty() {
        left
    } else if left.is_empty() || left >= right {
        right
    } else {
        left
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};

    use super::*;

    #[test]
    fn compatible_gc_time_matrix_matches_source() {
        let valid = [
            "20181218-19:53:37 +0800 CST",
            "20181218-19:53:37 +0800 MST",
            "20181218-19:53:37 +0800 FOO",
            "20181218-19:53:37 +0800 +08",
            "20181218-19:53:37 +0800",
            "20181218-19:53:37 +0800 ",
            "20181218-11:53:37 +0000",
            "20181218-11:53:37.000 +0000",
            "20181218-19:53:37.000 +0800 +08",
        ];
        let expected = Utc.with_ymd_and_hms(2018, 12, 18, 11, 53, 37).unwrap();
        for value in valid {
            assert_eq!(compatible_parse_gc_time(value).unwrap().to_utc(), expected);
        }
        for value in [
            "",
            " ",
            "foo",
            "20181218-11:53:37",
            "20181218-19:53:37 +0800CST",
            "20181218-19:53:37 +0800 FOO BAR",
            "20181218-19:53:37 +0800FOOOOOOO BAR",
            "20181218-19:53:37 ",
        ] {
            assert!(compatible_parse_gc_time(value).is_err(), "{value}");
        }
    }

    #[test]
    fn source_byte_and_range_helpers() {
        assert_eq!(format_bytes(1_024), "1024 Bytes");
        assert_eq!(format_bytes(1_025), "1.00 KB");
        assert_eq!(format_bytes(10_752), "10.5 KB");
        assert_eq!(format_bytes(1 << 20), "1024 KB");
        assert_eq!(format_bytes((1 << 20) + 1), "1.00 MB");
        assert_eq!(bytes_to_string(2 << 20), "2 MB");
        assert_eq!(get_max_start_key(b"", b"a"), b"a");
        assert_eq!(get_max_start_key(b"b", b"a"), b"b");
        assert_eq!(get_min_end_key(b"", b"a"), b"a");
        assert_eq!(get_min_end_key(b"a", b""), b"a");
        assert_eq!(get_min_end_key(b"b", b"a"), b"a");
    }

    #[test]
    fn recovery_hook_runs_for_success_and_panic() {
        let calls = Arc::new(AtomicUsize::new(0));
        let success = calls.clone();
        with_recovery(
            || {},
            Some(move |panic: Option<&(dyn Any + Send)>| {
                assert!(panic.is_none());
                success.fetch_add(1, Ordering::Relaxed);
            }),
        );
        let failure = calls.clone();
        with_recovery(
            || panic!("boom"),
            Some(move |panic: Option<&(dyn Any + Send)>| {
                assert!(panic.is_some());
                failure.fetch_add(1, Ordering::Relaxed);
            }),
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn session_id_is_typed_and_context_local() {
        let base = TraceContext::new();
        let derived = with_session_id(&base, 42);
        assert_eq!(session_id(&base), None);
        assert_eq!(session_id(&derived), Some(42));
    }
}
