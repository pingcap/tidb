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
//! - a wall clock the DST fall-back REPEATS names two instants, and Go
//!   picks neither "the earlier" nor "the later" as a rule — it runs
//!   `time.Date`, whose answer is the earlier instant in `US/Eastern` and
//!   the later one in `Europe/Paris`;
//! - a NONEXISTENT local time (DST spring-forward gap) resolves to the
//!   transition instant itself — Go's `AdjustedGoTime` picks the closest
//!   zone bound, and inside a normal (≤4h) gap that is the transition;
//!   a gap wider than 4 hours is an error, surfaced as NULL like every
//!   other conversion failure in the source evaluator;
//! - `''`/unknown zones, out-of-range offsets (`+14:01`, `+13:60`), NULL
//!   arguments, and zero/invalid datetimes are all NULL;
//! - `SYSTEM` maps to the process-local zone, matching Go's `time.Local`.

use std::str::FromStr;
use std::sync::LazyLock;

use chrono::{
    DateTime, FixedOffset, Local, LocalResult, NaiveDate, NaiveDateTime, TimeZone as _, Utc,
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
    System,
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
        return Ok(Some(ConvTz::System));
    }
    Ok(Tz::from_str(s).ok().map(ConvTz::Named))
}

/// Interprets `naive` as a local time in `tz`; `None` is NULL.
///
/// A named zone is Go's `time.Date`, which
/// [`super::session_tz::local_to_instant`] already models as the single rule
/// it is; a fixed offset has no transitions, so the wall clock names exactly
/// one instant.
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
        ConvTz::Named(tz) => super::session_tz::local_to_instant(tz, &naive),
        ConvTz::System => match Local.from_local_datetime(&naive) {
            LocalResult::Single(value) => Some(value.with_timezone(&Utc)),
            // Go's `time.Date` selects the post-transition occurrence for
            // the process-local fall-back overlap (the same normal-time
            // choice its Europe/Amsterdam source rows pin).
            LocalResult::Ambiguous(_, later) => Some(later.with_timezone(&Utc)),
            LocalResult::None => None,
        },
    }
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
        ConvTz::System => instant.with_timezone(&Local).naive_local(),
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
    use chrono::{Local, LocalResult, NaiveDateTime, TimeZone as _};
    use chrono_tz::Tz;

    fn s(v: &str) -> Datum {
        Datum::new_string(v.to_string())
    }

    fn call(dt: &str, from: &str, to: &str) -> Datum {
        convert_tz(&[s(dt), s(from), s(to)]).unwrap()
    }

    /// Exact Go `TestConvertTz` scalar matrix. The two `SYSTEM` rows below
    /// are computed separately from the process-local zone, just as the Go
    /// test computes them with `time.LoadLocation("Local")`.
    #[test]
    fn test_convert_tz() {
        let rows = [
            (
                s("2004-01-01 12:00:00.111"),
                s("-00:00"),
                s("+12:34"),
                Some("2004-01-02 00:34:00.111"),
            ),
            (
                s("2004-01-01 12:00:00.11"),
                s("+00:00"),
                s("+12:34"),
                Some("2004-01-02 00:34:00.11"),
            ),
            (
                s("2004-01-01 12:00:00.11111111111"),
                s("-00:00"),
                s("+12:34"),
                Some("2004-01-02 00:34:00.111111"),
            ),
            (
                s("2004-01-01 12:00:00"),
                s("GMT"),
                s("MET"),
                Some("2004-01-01 13:00:00"),
            ),
            (
                s("2004-01-01 12:00:00"),
                s("-01:00"),
                s("-12:00"),
                Some("2004-01-01 01:00:00"),
            ),
            (
                s("2004-01-01 12:00:00"),
                s("-00:00"),
                s("+13:00"),
                Some("2004-01-02 01:00:00"),
            ),
            (
                s("2004-01-01 12:00:00"),
                s("-00:00"),
                s("-13:00"),
                Some("2003-12-31 23:00:00"),
            ),
            (s("2004-01-01 12:00:00"), s("-00:00"), s("-12:88"), None),
            (s("2004-01-01 12:00:00"), s("+10:82"), s("GMT"), None),
            (
                s("2004-01-01 12:00:00"),
                s("+00:00"),
                s("GMT"),
                Some("2004-01-01 12:00:00"),
            ),
            (
                s("2004-01-01 12:00:00"),
                s("GMT"),
                s("+00:00"),
                Some("2004-01-01 12:00:00"),
            ),
            (
                Datum::Int(20_040_101),
                s("+00:00"),
                s("+10:32"),
                Some("2004-01-01 10:32:00"),
            ),
            (
                Datum::Real(314_159.0 / 100_000.0),
                s("+00:00"),
                s("+10:32"),
                None,
            ),
            (s("2004-01-01 12:00:00"), s(""), s("GMT"), None),
            (s("2004-01-01 12:00:00"), s("GMT"), s(""), None),
            (s("2004-01-01 12:00:00"), s("a"), s("GMT"), None),
            (s("2004-01-01 12:00:00"), s("0"), s("GMT"), None),
            (s("2004-01-01 12:00:00"), s("GMT"), s("a"), None),
            (s("2004-01-01 12:00:00"), s("GMT"), s("0"), None),
            (Datum::Null, s("GMT"), s("+00:00"), None),
            (s("2004-01-01 12:00:00"), Datum::Null, s("+00:00"), None),
            (s("2004-01-01 12:00:00"), s("GMT"), Datum::Null, None),
            (
                s("2004-01-01 12:00:00"),
                s("GMT"),
                s("+10:00"),
                Some("2004-01-01 22:00:00"),
            ),
            (
                s("2004-01-01 12:00:00"),
                s("+00:00"),
                s("MET"),
                Some("2004-01-01 13:00:00"),
            ),
            (
                s("2004-01-01 12:00:00"),
                s("+00:00"),
                s("+14:00"),
                Some("2004-01-02 02:00:00"),
            ),
            (
                s("2021-10-31 02:59:59"),
                s("+02:00"),
                s("Europe/Amsterdam"),
                Some("2021-10-31 02:59:59"),
            ),
            (
                s("2021-10-31 03:00:00"),
                s("+01:00"),
                s("Europe/Amsterdam"),
                Some("2021-10-31 03:00:00"),
            ),
            (
                s("2021-10-31 02:00:00"),
                s("+02:00"),
                s("Europe/Amsterdam"),
                Some("2021-10-31 02:00:00"),
            ),
            (
                s("2021-10-31 02:59:59"),
                s("+02:00"),
                s("Europe/Amsterdam"),
                Some("2021-10-31 02:59:59"),
            ),
            (
                s("2021-10-31 03:00:00"),
                s("+02:00"),
                s("Europe/Amsterdam"),
                Some("2021-10-31 02:00:00"),
            ),
            (
                s("2021-10-31 02:30:00"),
                s("+01:00"),
                s("Europe/Amsterdam"),
                Some("2021-10-31 02:30:00"),
            ),
            (
                s("2021-10-31 03:00:00"),
                s("+01:00"),
                s("Europe/Amsterdam"),
                Some("2021-10-31 03:00:00"),
            ),
            (
                s("2021-10-31 02:00:00"),
                s("Europe/Amsterdam"),
                s("+02:00"),
                Some("2021-10-31 03:00:00"),
            ),
            (
                s("2021-10-31 02:59:59"),
                s("Europe/Amsterdam"),
                s("+02:00"),
                Some("2021-10-31 03:59:59"),
            ),
            (
                s("2021-10-31 02:00:00"),
                s("Europe/Amsterdam"),
                s("+01:00"),
                Some("2021-10-31 02:00:00"),
            ),
            (
                s("2021-10-31 03:00:00"),
                s("Europe/Amsterdam"),
                s("+01:00"),
                Some("2021-10-31 03:00:00"),
            ),
            (
                s("2021-03-28 02:30:00"),
                s("Europe/Amsterdam"),
                s("UTC"),
                Some("2021-03-28 01:00:00"),
            ),
            (
                s("2007-03-11 2:00:00"),
                s("America/New_York"),
                s("America/Chicago"),
                Some("2007-03-11 01:00:00"),
            ),
            (
                s("2007-03-11 3:00:00"),
                s("America/New_York"),
                s("America/Chicago"),
                Some("2007-03-11 01:00:00"),
            ),
            (s("2004-10-00 12:00:00"), s("GMT"), s("MET"), None),
            (s("2004-00-01 12:00:00"), s("GMT"), s("MET"), None),
        ];
        for (date, from, to, expected) in rows {
            let actual = convert_tz(&[date, from, to]).unwrap();
            assert_eq!(
                actual,
                expected.map_or(Datum::Null, s),
                "expected {expected:?}"
            );
        }

        let wall =
            NaiveDateTime::parse_from_str("2021-10-22 10:00:00", "%Y-%m-%d %H:%M:%S").unwrap();
        let tallinn: Tz = "Europe/Tallinn".parse().unwrap();
        let tallinn_instant = tallinn.from_local_datetime(&wall).single().unwrap();
        let to_system = tallinn_instant
            .with_timezone(&Local)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        assert_eq!(
            call("2021-10-22 10:00:00", "Europe/Tallinn", "SYSTEM"),
            s(&to_system)
        );

        let local_instant = match Local.from_local_datetime(&wall) {
            LocalResult::Single(value) => value,
            LocalResult::Ambiguous(_, later) => later,
            LocalResult::None => panic!("source SYSTEM test wall clock must exist"),
        };
        let from_system = local_instant
            .with_timezone(&tallinn)
            .format("%Y-%m-%d %H:%M:%S")
            .to_string();
        assert_eq!(
            call("2021-10-22 10:00:00", "SYSTEM", "Europe/Tallinn"),
            s(&from_system)
        );
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
            // A repeated wall clock: `time.Date` answers the EARLIER of the
            // two instants here...
            (
                "2007-11-04 01:30:00",
                "US/Eastern",
                "UTC",
                "2007-11-04 05:30:00",
            ),
            (
                "2021-11-07 01:30:00",
                "America/Los_Angeles",
                "UTC",
                "2021-11-07 08:30:00",
            ),
            // ...and the LATER one here, which is why "take the earliest"
            // is not the rule. Zones east of UTC read the wall clock as UTC
            // into the post-transition period and land on the second pass.
            (
                "2025-10-26 02:30:00",
                "Europe/Paris",
                "UTC",
                "2025-10-26 01:30:00",
            ),
            (
                "2025-10-26 02:30:00.5",
                "Europe/Paris",
                "UTC",
                "2025-10-26 01:30:00.5",
            ),
            (
                "2021-10-31 01:30:00",
                "Europe/London",
                "UTC",
                "2021-10-31 01:30:00",
            ),
            (
                "2021-04-04 02:30:00",
                "Australia/Sydney",
                "UTC",
                "2021-04-03 16:30:00",
            ),
            // Controls: unrepeated wall clocks either side of that same
            // Paris fall-back, and one nowhere near a transition.
            (
                "2025-10-26 01:30:00",
                "Europe/Paris",
                "UTC",
                "2025-10-25 23:30:00",
            ),
            (
                "2025-10-26 03:00:00",
                "Europe/Paris",
                "UTC",
                "2025-10-26 02:00:00",
            ),
            (
                "2025-06-15 02:30:00",
                "Europe/Paris",
                "UTC",
                "2025-06-15 00:30:00",
            ),
            // Spring-forward gaps in both hemispheres.
            (
                "2025-03-30 02:30:00",
                "Europe/Paris",
                "UTC",
                "2025-03-30 01:00:00",
            ),
            (
                "2021-10-03 02:30:00",
                "Australia/Sydney",
                "UTC",
                "2021-10-02 16:00:00",
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
    }
}
