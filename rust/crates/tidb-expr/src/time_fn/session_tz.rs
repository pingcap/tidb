// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! The session-timezone time builtins: `FROM_UNIXTIME`, `UNIX_TIMESTAMP`,
//! and `TIDB_PARSE_TSO`, transcreated from `evalFromUnixTime`,
//! `builtinUnixTimestamp*Sig`/`goTimeToMysqlUnixTimestamp`, and
//! `builtinTidbParseTsoSig` in `pkg/expression/builtin_time.go`.
//!
//! These render or interpret wall-clock time in the session `time_zone`
//! ([`Columns::time_zone`]); the trait's default is the goeval oracle's
//! pinned `UTC+11`, keeping the golden corpus deterministic.
//!
//! Contracts pinned against goeval, not assumed:
//! - `FROM_UNIXTIME`'s fsp comes from the argument's type: integers 0,
//!   decimals their (capped) scale, strings/floats 6; the value is ROUNDED
//!   half-up at fsp (`.1234567` → `.123457`), the range is
//!   `[0, 32536771199]`, and out-of-range is NULL.
//! - `UNIX_TIMESTAMP`'s fsp comes from the argument's fractional digits;
//!   the value is TRUNCATED; datetimes outside
//!   `['1970-01-01 00:00:01', '3001-01-18 23:59:59.999999']` UTC are 0;
//!   invalid/zero datetimes are NULL; fsp 0 yields an integer, otherwise a
//!   decimal.
//! - `TIDB_PARSE_TSO` renders `tso >> 18` milliseconds since epoch at full
//!   (6-digit) precision; a non-positive tso is NULL.
//! - The zero-argument `UNIX_TIMESTAMP()` needs the statement clock and
//!   declines when [`Columns::now`] is absent.

use chrono::{Datelike, LocalResult, NaiveDateTime, TimeZone as _, Timelike, Utc};

use super::calendar::date_format;
use super::convert_tz::parse_datetime;
use crate::coerce::coerce_str;
use crate::context::SessionTimeZone;
use crate::{Columns, Datum, Decimal, EvalError};

/// MySQL 8.0.28's maximum unix timestamp: '3001-01-18 23:59:59' UTC.
const MAX_UNIX_SECS: i64 = 32_536_771_199;
const MAX_UNIX_MICROS: i64 = 32_536_771_199_999_999;

/// Renders the instant `secs`+`micros` (unix epoch) as a local wall clock in
/// the session zone.
fn instant_to_local(secs: i64, micros: u32, tz: &SessionTimeZone) -> Option<NaiveDateTime> {
    let utc = chrono::DateTime::<Utc>::from_timestamp(secs, micros * 1000)?;
    Some(match tz {
        SessionTimeZone::Fixed { offset_secs, .. } => {
            (utc + chrono::Duration::seconds(i64::from(*offset_secs))).naive_utc()
        }
        SessionTimeZone::Named(tz) => utc.with_timezone(tz).naive_local(),
    })
}

fn format_local(local: NaiveDateTime, fsp: usize) -> String {
    let mut out = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        local.year(),
        local.month(),
        local.day(),
        local.hour(),
        local.minute(),
        local.second()
    );
    if fsp > 0 {
        let micros = local.and_utc().timestamp_subsec_micros();
        let shown = micros / 10_u32.pow(6 - fsp as u32);
        out.push('.');
        out.push_str(&format!("{shown:0fsp$}"));
    }
    out
}

/// The unix-seconds argument as `(total_nanoseconds, fsp)`; `None` is NULL.
/// Go derives fsp from the argument TYPE: int 0, decimal its capped scale,
/// real/string `MaxFsp`; the nanoseconds keep the full written fraction so
/// rounding at fsp happens on the complete value, as in `evalFromUnixTime`.
fn unix_arg_nanos(value: &Datum) -> Result<Option<(i128, usize)>, EvalError> {
    let (text, fsp) = match value {
        Datum::Null => return Ok(None),
        Datum::Int(v) => (v.to_string(), 0),
        Datum::UInt(v) => (v.to_string(), 0),
        Datum::Decimal(d) => {
            let text = d.to_string();
            let scale = text.split_once('.').map_or(0, |(_, f)| f.len()).min(6);
            (text, scale)
        }
        Datum::Real(v) => (format!("{v:.9}"), 6),
        other => {
            let Some(text) = coerce_str(other)? else {
                return Ok(None);
            };
            (text, 6)
        }
    };

    let text = text.trim();
    let (int_part, frac_part) = text.split_once('.').unwrap_or((text, ""));
    let Ok(int_part): Result<i64, _> = int_part.parse() else {
        return Ok(None);
    };
    if int_part < 0 || frac_part.starts_with('-') {
        return Ok(None);
    }
    let frac_digits: String = frac_part.chars().take(9).collect();
    if !frac_digits.bytes().all(|b| b.is_ascii_digit()) {
        return Ok(None);
    }
    let frac_nanos: i128 = if frac_digits.is_empty() {
        0
    } else {
        format!("{frac_digits:0<9}").parse().unwrap()
    };
    Ok(Some((
        i128::from(int_part) * 1_000_000_000 + frac_nanos,
        fsp,
    )))
}

/// `FROM_UNIXTIME(unix[, format])`.
pub(super) fn from_unixtime(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if !(1..=2).contains(&vals.len()) {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some((total_nanos, fsp)) = unix_arg_nanos(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let integral = total_nanos / 1_000_000_000;
    if integral > i128::from(MAX_UNIX_SECS) {
        return Ok(Datum::Null);
    }

    // Round half-up at fsp over the complete value (convertTimeToMysqlTime
    // with ModeHalfUp), carrying into the seconds when the fraction rolls.
    let factor = 10_i128.pow(9 - fsp as u32);
    let rounded = (total_nanos + factor / 2) / factor * factor;
    let secs = (rounded / 1_000_000_000) as i64;
    let micros = ((rounded % 1_000_000_000) / 1000) as u32;

    let Some(local) = instant_to_local(secs, micros, &cols.time_zone()) else {
        return Ok(Datum::Null);
    };
    let formatted = format_local(local, fsp);
    if vals.len() == 2 {
        return date_format(&Datum::new_string(formatted), &vals[1]);
    }
    Ok(Datum::new_string(formatted))
}

/// `UNIX_TIMESTAMP([datetime])`.
pub(super) fn unix_timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    match vals.len() {
        0 => {
            // The statement clock; absent outside a session.
            let Some((utc_secs, nanos, _)) = cols.now() else {
                return Err(EvalError::Unsupported("session clock"));
            };
            let micros = i64::from(nanos / 1000) + utc_secs * 1_000_000;
            return Ok(unix_result(micros, 0));
        }
        1 => {}
        _ => return Err(EvalError::Unsupported("bad function arity")),
    }

    let Some(text) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some((naive, frac)) = parse_datetime(&text) else {
        return Ok(Datum::Null);
    };
    let fsp = frac.len().min(6);

    let instant = match &cols.time_zone() {
        SessionTimeZone::Fixed { offset_secs, .. } => {
            Some(naive.and_utc() - chrono::Duration::seconds(i64::from(*offset_secs)))
        }
        SessionTimeZone::Named(tz) => match tz.from_local_datetime(&naive) {
            LocalResult::Single(t) => Some(t.with_timezone(&Utc)),
            LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
            // A nonexistent local time is a GoTime error: result 0.
            LocalResult::None => None,
        },
    };
    let Some(instant) = instant else {
        return Ok(unix_result(0, fsp));
    };
    Ok(unix_result(instant.timestamp_micros(), fsp))
}

/// Builds `UNIX_TIMESTAMP`'s result from epoch microseconds: TRUNCATED at
/// fsp, integer when fsp is 0, out-of-range as 0.
fn unix_result(micros: i64, fsp: usize) -> Datum {
    let micros = if (1_000_000..=MAX_UNIX_MICROS).contains(&micros) {
        micros
    } else {
        0
    };
    if fsp == 0 {
        return Datum::Int(micros / 1_000_000);
    }
    let secs = micros / 1_000_000;
    let frac = (micros % 1_000_000) / 10_i64.pow(6 - fsp as u32);
    Datum::Decimal(Decimal::from_literal(&format!("{secs}.{frac:0fsp$}")))
}

/// `TIDB_PARSE_TSO(tso)`: the physical half, rendered at full precision in
/// the session zone.
pub(super) fn tidb_parse_tso(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.len() != 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(tso) = super::int_arg(&vals[0])? else {
        return Ok(Datum::Null);
    };
    if tso <= 0 {
        return Ok(Datum::Null);
    }
    let physical_ms = tso >> 18;
    let secs = physical_ms.div_euclid(1000);
    let micros = (physical_ms.rem_euclid(1000) * 1000) as u32;
    let Some(local) = instant_to_local(secs, micros, &cols.time_zone()) else {
        return Ok(Datum::Null);
    };
    Ok(Datum::new_string(format_local(local, 6)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::NoColumns;

    fn call(f: fn(&[Datum], &dyn Columns) -> Result<Datum, EvalError>, vals: &[Datum]) -> Datum {
        f(vals, &NoColumns).unwrap()
    }

    fn s(v: &str) -> Datum {
        Datum::new_string(v.to_string())
    }

    fn dec(v: &str) -> Datum {
        Datum::Decimal(Decimal::from_literal(v))
    }

    /// Every vector is goeval output under its pinned UTC+11 session zone.
    #[test]
    fn from_unixtime_goeval_vectors() {
        let cases: &[(Datum, &str)] = &[
            (Datum::Int(0), "1970-01-01 11:00:00"),
            (Datum::Int(1), "1970-01-01 11:00:01"),
            (Datum::Int(1_447_430_881), "2015-11-14 03:08:01"),
            (dec("1447430881.123456"), "2015-11-14 03:08:01.123456"),
            (dec("1447430881.999999"), "2015-11-14 03:08:01.999999"),
            // Literal scale 7 rounds half-up into fsp 6.
            (dec("1447430881.1234567"), "2015-11-14 03:08:01.123457"),
            (dec("1447430881.12"), "2015-11-14 03:08:01.12"),
            (Datum::Int(MAX_UNIX_SECS), "3001-01-19 10:59:59"),
            // A string argument carries MaxFsp.
            (s("1447430881.5"), "2015-11-14 03:08:01.500000"),
        ];
        for (arg, want) in cases {
            assert_eq!(
                call(from_unixtime, std::slice::from_ref(arg)),
                s(want),
                "FROM_UNIXTIME({arg:?})"
            );
        }
        assert_eq!(call(from_unixtime, &[Datum::Int(-1)]), Datum::Null);
        assert_eq!(
            call(from_unixtime, &[Datum::Int(MAX_UNIX_SECS + 1)]),
            Datum::Null
        );
        assert_eq!(call(from_unixtime, &[Datum::Null]), Datum::Null);

        // Two-argument form composes with DATE_FORMAT.
        assert_eq!(
            call(from_unixtime, &[Datum::Int(1_447_430_881), s("%H")]),
            s("03")
        );
    }

    #[test]
    fn unix_timestamp_goeval_vectors() {
        let cases: &[(&str, Datum)] = &[
            ("2015-11-13 10:20:19", Datum::Int(1_447_370_419)),
            ("2015-11-13 10:20:19.012", dec("1447370419.012")),
            ("1970-01-01 00:00:00", Datum::Int(0)),
            ("1969-12-31 23:59:59", Datum::Int(0)),
            ("3001-01-18 23:59:59", Datum::Int(32_536_731_599)),
            ("2038-01-19 03:14:07", Datum::Int(2_147_444_047)),
        ];
        for (arg, want) in cases {
            assert_eq!(
                call(unix_timestamp, &[s(arg)]),
                *want,
                "UNIX_TIMESTAMP({arg})"
            );
        }
        assert_eq!(
            call(unix_timestamp, &[s("0000-00-00 00:00:00")]),
            Datum::Null
        );
        assert_eq!(call(unix_timestamp, &[s("not-a-date")]), Datum::Null);
        assert_eq!(call(unix_timestamp, &[Datum::Null]), Datum::Null);
        // The zero-argument form needs the statement clock.
        assert!(unix_timestamp(&[], &NoColumns).is_err());
    }

    #[test]
    fn tidb_parse_tso_goeval_vectors() {
        assert_eq!(
            call(tidb_parse_tso, &[Datum::Int(424_930_234_047_906_595)]),
            s("2021-05-14 19:16:41.903000")
        );
        assert_eq!(call(tidb_parse_tso, &[Datum::Int(0)]), Datum::Null);
        assert_eq!(call(tidb_parse_tso, &[Datum::Null]), Datum::Null);
    }
}
