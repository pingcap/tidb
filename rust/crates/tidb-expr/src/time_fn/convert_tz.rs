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

//! `CONVERT_TZ(dt, from_tz, to_tz)`, transcreated from `builtinConvertTzSig`
//! (`pkg/expression/builtin_time.go`), `expression.timeZone2int`
//! (`util.go`), and `CoreTime.GoTime`/`AdjustedGoTime`
//! (`pkg/types/core_time.go`). Named zones use `chrono-tz`'s compiled IANA
//! data — the same tzdata Go's `time.LoadLocation` reads.
//!
//! Behavior pinned against goeval (TiDB's production engine), not assumed:
//! - the fractional-seconds text passes through verbatim (offsets are whole
//!   minutes, so the fraction is timezone-invariant);
//! - an AMBIGUOUS local time (DST fall-back) resolves to the EARLIEST
//!   instant, matching Go's `time.Date`;
//! - a NONEXISTENT local time (DST spring-forward gap) resolves to the
//!   transition instant itself — Go's `AdjustedGoTime` picks the closest
//!   zone bound, and inside a normal (≤4h) gap that is the transition;
//!   a gap wider than 4 hours is an error, surfaced as NULL like every
//!   other conversion failure in the source evaluator;
//! - `''`/unknown zones, out-of-range offsets (`+14:01`, `+13:60`), NULL
//!   arguments, and zero/invalid datetimes are all NULL;
//! - `SYSTEM` maps to Go's process-local zone — session state this
//!   value-only evaluator does not have, so it declines loudly instead of
//!   producing a machine-dependent value.

use std::str::FromStr;
use std::sync::LazyLock;

use chrono::{
    DateTime, Duration, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone as _,
    Utc,
};
use chrono_tz::Tz;
use regex::Regex;

use super::calendar::{parse_date_ymd, parse_time_with_fraction};
use crate::coerce::coerce_str;
use crate::{Datum, EvalError};

/// Go `convertTzFunctionClass`'s `tzRegex`.
static TZ_OFFSET_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(^[-+](0?[0-9]|1[0-3]):[0-5]?\d$)|(^\+14:00?$)").unwrap());

enum ConvTz {
    Fixed(i32),
    Named(Tz),
}

/// Resolves a CONVERT_TZ zone argument; `None` means the Go evaluator would
/// return NULL.
fn parse_conv_tz(s: &str) -> Result<Option<ConvTz>, EvalError> {
    if s.is_empty() {
        return Ok(None);
    }
    if TZ_OFFSET_RE.is_match(s) {
        // Go `timeZone2int`.
        let sign = if s.starts_with('-') { -1 } else { 1 };
        let body = &s[1..];
        let (h, m) = body.split_once(':').expect("regex guarantees a colon");
        let h: i32 = h.parse().expect("regex guarantees digits");
        let m: i32 = m.parse().expect("regex guarantees digits");
        return Ok(Some(ConvTz::Fixed(sign * (h * 3600 + m * 60))));
    }
    if s.eq_ignore_ascii_case("SYSTEM") {
        // Go maps SYSTEM to the process-local zone; producing a
        // machine-dependent value here would silently diverge.
        return Err(EvalError::Unsupported("session time zone"));
    }
    Ok(Tz::from_str(s).ok().map(ConvTz::Named))
}

/// Interprets `naive` as a local time in `tz`, resolving DST ambiguity and
/// gaps the way Go's `GoTime`/`AdjustedGoTime` do; `None` is NULL.
fn local_to_instant(naive: NaiveDateTime, tz: &ConvTz) -> Option<DateTime<Utc>> {
    match tz {
        ConvTz::Fixed(offset) => {
            let offset = FixedOffset::east_opt(*offset)?;
            Some(
                offset
                    .from_local_datetime(&naive)
                    .single()?
                    .with_timezone(&Utc),
            )
        }
        ConvTz::Named(tz) => match tz.from_local_datetime(&naive) {
            LocalResult::Single(t) => Some(t.with_timezone(&Utc)),
            // Fall-back: Go's time.Date lands on the earlier instant.
            LocalResult::Ambiguous(earliest, _) => Some(earliest.with_timezone(&Utc)),
            // Spring-forward gap: the closest zone bound is the transition.
            LocalResult::None => gap_transition_instant(*tz, naive),
        },
    }
}

/// Locates the DST transition instant containing the nonexistent local time
/// `naive` by bisecting the surrounding ±5h window on the UTC axis. Mirrors
/// Go `AdjustedGoTime`'s closest-zone-bound choice, including its refusal of
/// transitions wider than 4 hours.
fn gap_transition_instant(tz: Tz, naive: NaiveDateTime) -> Option<DateTime<Utc>> {
    let window = Duration::hours(5);
    let mut lo = tz
        .from_local_datetime(&(naive - window))
        .earliest()?
        .with_timezone(&Utc);
    let mut hi = tz
        .from_local_datetime(&(naive + window))
        .latest()?
        .with_timezone(&Utc);

    let off_lo = tz.offset_from_utc_datetime(&lo.naive_utc()).fix();
    let off_hi = tz.offset_from_utc_datetime(&hi.naive_utc()).fix();
    let gap_secs = i64::from(off_hi.local_minus_utc()) - i64::from(off_lo.local_minus_utc());
    if !(1..=4 * 3600).contains(&gap_secs) {
        return None;
    }

    // First UTC instant whose offset equals the post-transition offset.
    while hi - lo > Duration::seconds(1) {
        let mid = lo + (hi - lo) / 2;
        if tz.offset_from_utc_datetime(&mid.naive_utc()).fix() == off_hi {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    Some(hi)
}

/// Parses `YYYY-MM-DD[ HH:MM[:SS[.frac]]]` into calendar fields plus the
/// verbatim fraction text; the crate's canonical datetime-string forms.
pub(super) fn parse_datetime(s: &str) -> Option<(NaiveDateTime, String)> {
    let input = s.trim();
    let (year, month, day) = parse_date_ymd(input)?;
    let date = NaiveDate::from_ymd_opt(i32::try_from(year).ok()?, month, day)?;

    let time_text = input
        .split_once(char::is_whitespace)
        .map(|(_, time)| time.trim());
    let (h, mi, sec, frac) = match time_text {
        None | Some("") => (0, 0, 0, String::new()),
        Some(t) => parse_time_with_fraction(t)?,
    };
    let micros: u32 = if frac.is_empty() {
        0
    } else {
        format!("{frac:0<6}").parse().ok()?
    };
    Some((date.and_hms_micro_opt(h, mi, sec, micros)?, frac))
}

/// `CONVERT_TZ(dt, from_tz, to_tz)`.
pub(super) fn convert_tz(vals: &[Datum]) -> Result<Datum, EvalError> {
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(dt), Some(from_s), Some(to_s)) = (
        coerce_str(&vals[0])?,
        coerce_str(&vals[1])?,
        coerce_str(&vals[2])?,
    ) else {
        return Ok(Datum::Null);
    };

    let Some((naive, frac)) = parse_datetime(&dt) else {
        return Ok(Datum::Null);
    };
    let (Some(from_tz), Some(to_tz)) = (parse_conv_tz(&from_s)?, parse_conv_tz(&to_s)?) else {
        return Ok(Datum::Null);
    };

    let Some(instant) = local_to_instant(naive, &from_tz) else {
        return Ok(Datum::Null);
    };

    let local = match &to_tz {
        ConvTz::Fixed(offset) => {
            let Some(offset) = FixedOffset::east_opt(*offset) else {
                return Ok(Datum::Null);
            };
            instant.with_timezone(&offset).naive_local()
        }
        ConvTz::Named(tz) => instant.with_timezone(tz).naive_local(),
    };

    use chrono::{Datelike, Timelike};
    let mut out = format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        local.year(),
        local.month(),
        local.day(),
        local.hour(),
        local.minute(),
        local.second()
    );
    if !frac.is_empty() {
        out.push('.');
        out.push_str(&frac);
    }
    Ok(Datum::new_string(out))
}

#[cfg(test)]
mod tests {
    use super::convert_tz;
    use crate::Datum;

    fn s(v: &str) -> Datum {
        Datum::new_string(v.to_string())
    }

    fn call(dt: &str, from: &str, to: &str) -> Datum {
        convert_tz(&[s(dt), s(from), s(to)]).unwrap()
    }

    /// Every vector here is goeval output from TiDB's production engine.
    #[test]
    fn goeval_pinned_vectors() {
        let cases: &[(&str, &str, &str, &str)] = &[
            (
                "2004-01-01 12:00:00",
                "+00:00",
                "+10:00",
                "2004-01-01 22:00:00",
            ),
            (
                "2004-01-01 12:00:00",
                "-01:00",
                "-10:32",
                "2004-01-01 02:28:00",
            ),
            (
                "2004-01-01 12:00:00.25",
                "+00:00",
                "+10:00",
                "2004-01-01 22:00:00.25",
            ),
            (
                "2004-01-01 12:00:00.123456",
                "+00:00",
                "+00:30",
                "2004-01-01 12:30:00.123456",
            ),
            // Spring-forward gap resolves to the transition instant.
            (
                "2007-03-11 02:30:00",
                "US/Eastern",
                "UTC",
                "2007-03-11 07:00:00",
            ),
            // Fall-back ambiguity resolves to the earliest instant.
            (
                "2007-11-04 01:30:00",
                "US/Eastern",
                "UTC",
                "2007-11-04 05:30:00",
            ),
            (
                "2004-07-01 12:00:00",
                "Europe/Berlin",
                "Asia/Shanghai",
                "2004-07-01 18:00:00",
            ),
            (
                "2004-01-01 12:00:00",
                "+14:00",
                "+00:00",
                "2003-12-31 22:00:00",
            ),
            ("2004-01-01", "+00:00", "+10:00", "2004-01-01 10:00:00"),
            ("2004-01-01 12:00:00", "MET", "UTC", "2004-01-01 11:00:00"),
            (
                "2004-01-01 12:00:00",
                "+0:9",
                "+00:00",
                "2004-01-01 11:51:00",
            ),
        ];
        for (dt, from, to, want) in cases {
            assert_eq!(
                call(dt, from, to),
                s(want),
                "CONVERT_TZ({dt}, {from}, {to})"
            );
        }
    }

    #[test]
    fn goeval_pinned_nulls() {
        for (dt, from, to) in [
            ("2004-01-01 12:00:00", "+14:01", "+00:00"),
            ("2004-01-01 12:00:00", "+13:60", "+00:00"),
            ("2004-01-01 12:00:00", "", "UTC"),
            ("2004-01-01 12:00:00", "bogus/zone", "UTC"),
            ("0000-00-00", "+00:00", "+10:00"),
            ("not-a-date", "+00:00", "+10:00"),
        ] {
            assert_eq!(
                call(dt, from, to),
                Datum::Null,
                "CONVERT_TZ({dt}, {from}, {to})"
            );
        }
        assert_eq!(
            convert_tz(&[Datum::Null, s("+00:00"), s("+10:00")]).unwrap(),
            Datum::Null
        );
        // SYSTEM needs session state: decline, never a machine-dependent value.
        assert!(convert_tz(&[s("2004-01-01 12:00:00"), s("SYSTEM"), s("UTC")]).is_err());
    }
}
