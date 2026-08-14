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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The typed temporal LITERALS `DATE 'lit'` and `TIMESTAMP 'lit'` (and their
//! ODBC spellings `{d 'lit'}` / `{ts 'lit'}`), which are NOT a
//! `CAST(lit AS DATE/DATETIME)`.
//!
//! Go builds a dedicated function class for each
//! (`pkg/expression/builtin_time.go`'s `dateLiteralFunctionClass` and
//! `timestampLiteralFunctionClass`), and the three ways they differ from the
//! cast are all observable:
//!
//! 1. A REGEX GATE runs before any parsing. `timestampPattern` requires a
//!    date AND an hour, so `TIMESTAMP '2024-01-01'` -- a perfectly good
//!    `CAST` input -- is `ErrWrongValue2` (1525). `datePattern` refuses
//!    anything carrying a time, so `{d '2024-01-01 01:12:31'}` is
//!    `ErrWrongValue` (1292).
//! 2. A PARSE FAILURE IS A HARD ERROR, where the cast reports a warning and
//!    answers `NULL` (`crate::cast::invalid_time_warning`). `TIMESTAMP
//!    '2024-01-01 14:00:00+14:01'` fails the statement.
//! 3. THE LITERAL'S OWN FRACTIONAL PRECISION SURVIVES.
//!    `types.GetFsp(str)` picks the fsp and `setDecimalAndFlenForDatetime`
//!    puts it in the result type, so `TIMESTAMP '2024-01-01 14:00:00.010'`
//!    prints `14:00:00.010`. `CAST(... AS DATETIME)` has decimal 0 and
//!    prints no fraction at all -- which is why the fraction cannot be
//!    restored by changing the cast.
//! 4. THE LITERAL IS TEMPORALLY TYPED. Go's two function classes declare
//!    `types.ETDatetime` and then `setDecimalAndFlenForDate` /
//!    `setDecimalAndFlenForDatetime(tm.Fsp())`, so `DATE 'lit'` reports
//!    `mysql.TypeDate` and `TIMESTAMP 'lit'` `mysql.TypeDatetime` -- exactly
//!    the types a `date`/`datetime` COLUMN reports. Every consumer that
//!    branches on "is this argument temporal?" therefore treats the literal
//!    and the column alike. This module returns that `FieldType` beside the
//!    value for that reason; folding to a `VarString` (what it used to do)
//!    made a literal invisible to `resolveType4Extremum`, to comparison
//!    refinement, and to anything else that reads
//!    `types.IsTypeTemporal(arg.GetType())`.
//!
//! Go does all three while BUILDING the expression, and so does this: the
//! literal folds to its formatted value here, and no per-row work remains.
//!
//! The parse runs in the SESSION's time zone, as Go's does: the rewriter
//! hands it down through [`crate::rewriter::ColumnResolver::time_zone`], the
//! fold-time sibling of the [`crate::Columns::time_zone`] the cast path
//! consults at eval time. A literal carrying an explicit offset
//! (`'... 14:00:00+02:00'`) normalizes into that zone, and a fractional
//! carry rounds the INSTANT in it (see [`parse`]).
//!
//! The SQL mode follows the statement too. `ALLOW_INVALID_DATES` controls
//! calendar validation while `DATE` applies `NO_ZERO_DATE` and
//! `NO_ZERO_IN_DATE` after parsing, in the same order as Go's literal
//! function.

use crate::EvalError;
use regex::Regex;
use std::sync::OnceLock;
use tidb_datatype::{FieldType, FieldTypeCode, Time, TimeType};

/// Go `mysql.MaxDateWidth`: `'YYYY-MM-DD'`.
const MAX_DATE_WIDTH: i64 = 10;
/// Go `mysql.MaxDatetimeWidthNoFsp`: `'YYYY-MM-DD HH:MM:SS'`.
const MAX_DATETIME_WIDTH_NO_FSP: i64 = 19;

/// Go's `timestampPattern`, transcreated character for character from
/// `pkg/expression/builtin_time.go`. `\d` and `\s` are spelled out as their
/// ASCII classes because RE2's are ASCII-only while Rust's are Unicode-aware.
fn timestamp_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(concat!(
            r"^",
            // Skip any spaces or zeros
            r"[\t\n\x0C\r ]*0*",
            // Year 1-4 digits
            r"[0-9]{1,4}",
            // 1 or 2 digit Month and Day, any non-digit as separator
            r"([^0-9]0*[0-9]{1,2}){2}",
            // At least one space between Date and Time parts
            r"[\t\n\x0C\r ]+",
            // Hour is mandatory
            r"0*[0-9]{1,2}",
            // Minutes or Minutes:Seconds are optional
            r"([^0-9]0*[0-9]{1,2}){0,2}",
            // Optional fractional seconds
            r"(\.[0-9]*)?",
            // Optional time zone offset, must be +/-HH:MM format
            r"([+-][0-9]{2}[:][0-9]{2})?",
            // Optionally ending with spaces
            r"[\t\n\x0C\r ]*$",
        ))
        .expect("timestampPattern is a valid regex")
    })
}

/// Go's `datePattern`, same source and same ASCII-class spelling.
fn date_pattern() -> &'static Regex {
    static PATTERN: OnceLock<Regex> = OnceLock::new();
    PATTERN.get_or_init(|| {
        Regex::new(
            r"^[\t\n\x0C\r ]*((0*[0-9]{1,4}([^0-9]0*[0-9]{1,2}){2})|([0-9]{2,4}([0-9]{2}){2}))[\t\n\x0C\r ]*$",
        )
        .expect("datePattern is a valid regex")
    })
}

/// Go `builtinDateLiteralSig`: the value of `DATE 'lit'`, or the error that
/// rejects the whole statement.
///
/// `zone` is inert here in practice -- `date_pattern` refuses a time part, so
/// neither a fractional carry nor an explicit offset can reach the parse --
/// but Go's `getFunction` passes its ctx location all the same, and so does
/// this, so the two literals cannot drift apart.
pub(crate) fn date_literal(
    text: &str,
    zone: &tidb_datatype::SessionTimeZone,
    modes: tidb_datatype::DateModes,
) -> Result<(Time, FieldType), EvalError> {
    if !date_pattern().is_match(text) {
        return Err(wrong_value(1292, "date", text));
    }
    let time = parse(text, TimeType::Date, 0, zone, modes.allow_invalid_dates)
        .map_err(|()| wrong_value(1292, "datetime", text))?;
    if modes.no_zero_date && time.is_zero() {
        return Err(wrong_value(1292, "date", text));
    }
    if modes.no_zero_in_date && time.invalid_zero() && !time.is_zero() {
        return Err(wrong_value(1292, "date", text));
    }
    // Go `setDecimalAndFlenForDate` (`pkg/expression/builtin.go:1065`):
    // `SetDecimal(0)`, `SetFlen(mysql.MaxDateWidth)`, `SetType(mysql.TypeDate)`.
    let mut ft = FieldType::new(FieldTypeCode::Date);
    ft.set_decimal(0);
    ft.set_flen(MAX_DATE_WIDTH);
    Ok((time, ft))
}

/// Go `builtinTimestampLiteralSig`: the value of `TIMESTAMP 'lit'`, or the
/// error that rejects the whole statement.
///
/// The two codes are Go's own and are NOT interchangeable: the regex gate is
/// `ErrWrongValue2` (1525) and the parse failure is `ErrWrongValue` (1292),
/// which is why the recorded topic carries both against this one syntax.
pub(crate) fn timestamp_literal(
    text: &str,
    zone: &tidb_datatype::SessionTimeZone,
    modes: tidb_datatype::DateModes,
) -> Result<(Time, FieldType), EvalError> {
    if !timestamp_pattern().is_match(text) {
        return Err(wrong_value(1525, "datetime", text));
    }
    let fsp = i64::from(tidb_datatype::get_fsp(text));
    let time = parse(
        text,
        TimeType::DateTime,
        fsp,
        zone,
        modes.allow_invalid_dates,
    )
    .map_err(|()| wrong_value(1292, "datetime", text))?;
    // Go `setDecimalAndFlenForDatetime(tm.Fsp())`
    // (`pkg/expression/builtin.go:1056`): the base type for the `ETDatetime`
    // return is already `mysql.TypeDatetime`, and only the scale and width
    // move -- `MaxDatetimeWidthNoFsp + fsp`, plus one for the `.` separator
    // when there is a fraction at all.
    let fsp = i64::from(time.fsp());
    let mut ft = FieldType::new(FieldTypeCode::Datetime);
    ft.set_decimal_under_limit(fsp);
    ft.set_flen_under_limit(MAX_DATETIME_WIDTH_NO_FSP + fsp + i64::from(fsp > 0));
    Ok((time, ft))
}

/// The parse runs in the SESSION's zone, as Go's does.
///
/// Go resolves `DATE 'lit'`/`TIMESTAMP 'lit'` in `getFunction`, whose `ctx`
/// carries the session location, and a literal whose fraction is wider than
/// `fsp` ROUNDS -- with the carry applied to the INSTANT in that zone. So
/// when the carry lands on a DST transition the two answers differ.
/// CAPTURED from real TiDB:
///
/// ```text
/// select timestamp '2011-03-13 01:59:59.9999999'
///   time_zone='UTC'                 2011-03-13 02:00:00.000000
///   time_zone='America/Los_Angeles' 2011-03-13 03:00:00.000000
/// select timestamp '2011-11-06 01:59:59.9999999'
///   time_zone='UTC'                 2011-11-06 02:00:00.000000
///   time_zone='America/Los_Angeles' 2011-11-06 01:00:00.000000
/// ```
///
/// This used to hardcode `chrono::Utc` -- the dropped-Context seam -- and
/// answered the UTC row for every session. The zone now arrives from
/// [`crate::rewriter::ColumnResolver::time_zone`], the fold-time sibling of
/// the [`crate::Columns::time_zone`] the eval-time cast already consults, so
/// the same statement rounds identically whichever of the two paths builds
/// it. An explicit `+HH:MM` offset in the literal likewise normalizes into
/// this zone rather than into UTC.
fn parse(
    text: &str,
    kind: TimeType,
    fsp: i64,
    zone: &tidb_datatype::SessionTimeZone,
    allow_invalid_dates: bool,
) -> Result<Time, ()> {
    tidb_datatype::parse_time(text, kind, fsp, false, true, allow_invalid_dates, zone)
        .map(|parsed| parsed.time)
        .map_err(|_| ())
}

/// Go `types.ErrWrongValue`/`ErrWrongValue2`, whose message names the target
/// type in lower case and quotes the offending literal.
fn wrong_value(code: u16, kind: &str, text: &str) -> EvalError {
    EvalError::WrongTemporalLiteral {
        code,
        message: format!("Incorrect {kind} value: '{text}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The printed value, which is all the pre-typing form of this module
    /// returned. The TYPE half is asserted separately in
    /// [`the_literal_reports_gos_own_temporal_type`].
    fn shown(result: Result<(Time, FieldType), EvalError>) -> String {
        result.unwrap().0.to_string()
    }

    /// The three properties this module exists for, one case each, taken from
    /// `tests/integrationtest/r/types/time.result`.
    #[test]
    fn literal_gates_and_fraction_follow_the_recording() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let modes = tidb_datatype::DateModes::default();
        // The regex gate: a date with no time is a 1525 for TIMESTAMP even
        // though CAST accepts it.
        assert!(matches!(
            timestamp_literal("2024-01-01", &utc, modes),
            Err(EvalError::WrongTemporalLiteral { code: 1525, .. })
        ));
        // A parse failure is a hard error, not a warning plus NULL.
        assert!(matches!(
            timestamp_literal("2024-01-01 14:00:00+14:01", &utc, modes),
            Err(EvalError::WrongTemporalLiteral { code: 1292, .. })
        ));
        // The literal's own fsp survives into the printed value.
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00.010", &utc, modes)),
            "2024-01-01 14:00:00.010"
        );
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00", &utc, modes)),
            "2024-01-01 14:00:00"
        );
        // DATE refuses a literal carrying a time part.
        assert!(matches!(
            date_literal("2024-01-01 01:12:31", &utc, modes),
            Err(EvalError::WrongTemporalLiteral { code: 1292, .. })
        ));
        assert_eq!(shown(date_literal("2024-01-01", &utc, modes)), "2024-01-01");
    }

    #[test]
    fn date_literal_uses_the_statement_sql_mode() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let permissive = tidb_datatype::DateModes::default();
        assert_eq!(
            shown(date_literal("0000-00-00", &utc, permissive)),
            "0000-00-00"
        );
        assert_eq!(
            shown(date_literal("2007-10-00", &utc, permissive)),
            "2007-10-00"
        );

        let no_zero_date = tidb_datatype::DateModes {
            no_zero_date: true,
            ..permissive
        };
        assert!(matches!(
            date_literal("0000-00-00", &utc, no_zero_date),
            Err(EvalError::WrongTemporalLiteral { code: 1292, ref message })
                if message == "Incorrect date value: '0000-00-00'"
        ));
        assert_eq!(
            shown(date_literal("2007-10-00", &utc, no_zero_date)),
            "2007-10-00"
        );

        let no_zero_in_date = tidb_datatype::DateModes {
            no_zero_in_date: true,
            ..permissive
        };
        assert_eq!(
            shown(date_literal("0000-00-00", &utc, no_zero_in_date)),
            "0000-00-00"
        );
        assert!(matches!(
            date_literal("2007-10-00", &utc, no_zero_in_date),
            Err(EvalError::WrongTemporalLiteral { code: 1292, ref message })
                if message == "Incorrect date value: '2007-10-00'"
        ));

        assert!(matches!(
            date_literal("2017-2-31", &utc, permissive),
            Err(EvalError::WrongTemporalLiteral { code: 1292, ref message })
                if message == "Incorrect datetime value: '2017-2-31'"
        ));
        assert_eq!(
            shown(date_literal(
                "2017-2-31",
                &utc,
                tidb_datatype::DateModes {
                    allow_invalid_dates: true,
                    ..permissive
                }
            )),
            "2017-02-31"
        );
    }

    /// The fold rounds in the SESSION zone, not in UTC: the capture in
    /// [`parse`]'s doc, replayed against both zones. The instants are DST
    /// TRANSITIONS on purpose -- a probe over ordinary instants shows no
    /// difference in ANY zone and is a false negative (the same trap the
    /// sibling test in `crate::cast` documents).
    #[test]
    fn the_fractional_carry_rounds_in_the_session_zone() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let la = tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let modes = tidb_datatype::DateModes::default();
        for (input, in_utc, in_la) in [
            (
                "2011-03-13 01:59:59.9999999",
                "2011-03-13 02:00:00.000000",
                "2011-03-13 03:00:00.000000",
            ),
            (
                "2011-11-06 01:59:59.9999999",
                "2011-11-06 02:00:00.000000",
                "2011-11-06 01:00:00.000000",
            ),
        ] {
            assert_eq!(
                shown(timestamp_literal(input, &utc, modes)),
                in_utc,
                "{input}"
            );
            assert_eq!(
                shown(timestamp_literal(input, &la, modes)),
                in_la,
                "{input}"
            );
        }
        // An explicit offset in the literal normalizes into the session zone
        // (the divergence the module doc used to pin as UTC-only): 14:00 at
        // +02:00 is 12:00 UTC and 04:00 in Los Angeles (PST, -08:00).
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00+02:00", &utc, modes)),
            "2024-01-01 12:00:00"
        );
        assert_eq!(
            shown(timestamp_literal("2024-01-01 14:00:00+02:00", &la, modes)),
            "2024-01-01 04:00:00"
        );
    }

    /// Go's `setDecimalAndFlenForDate` / `setDecimalAndFlenForDatetime`, the
    /// half a printed VALUE cannot show. `DATE 'lit'` is `mysql.TypeDate`
    /// with `MaxDateWidth` and scale 0; `TIMESTAMP 'lit'` is
    /// `mysql.TypeDatetime` whose scale is the LITERAL TEXT's own fsp and
    /// whose width grows by that fsp plus the `.` separator.
    ///
    /// The scale is read off the parsed `Time`, not off the text, so a
    /// literal whose fraction is wider than `MaxFsp` reports the CLAMPED 6 --
    /// the value rounds to six digits and the declared scale must agree with
    /// it, or the chunk cell and the header disagree.
    #[test]
    fn the_literal_reports_gos_own_temporal_type() {
        let utc = tidb_datatype::SessionTimeZone::utc();
        let modes = tidb_datatype::DateModes::default();
        let (_, date) = date_literal("2024-01-01", &utc, modes).unwrap();
        assert_eq!(date.code(), FieldTypeCode::Date);
        assert_eq!((date.flen(), date.decimal()), (10, 0));

        for (text, decimal, flen) in [
            ("2024-01-01 14:00:00", 0, 19),
            ("2024-01-01 14:00:00.010", 3, 23),
            ("2024-01-01 14:00:00.123456", 6, 26),
            // Wider than `MaxFsp`: the parse rounds to six digits, so the
            // reported scale is six and not the nine the text carries.
            ("2024-01-01 14:00:00.123456789", 6, 26),
        ] {
            let (time, ft) = timestamp_literal(text, &utc, modes).unwrap();
            assert_eq!(ft.code(), FieldTypeCode::Datetime, "{text}");
            assert_eq!((ft.flen(), ft.decimal()), (flen, decimal), "{text}");
            assert_eq!(i64::from(time.fsp()), decimal, "{text}");
        }
    }
}
