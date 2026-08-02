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
//!
//! Go does all three while BUILDING the expression, and so does this: the
//! literal folds to its formatted value here, and no per-row work remains.
//!
//! DOCUMENTED DIVERGENCE, shared with `crate::cast::cast_to_time`: the parse
//! runs against UTC rather than the session's time zone, so a literal that
//! carries an explicit offset (`'... 14:00:00+02:00'`) is normalized into UTC
//! instead of into the session zone. The rewriter has no session context to
//! consult; threading one is a separate change, and this is the SAME zone the
//! cast path already uses, so no statement's answer moves because of this
//! module.
//!
//! LIKEWISE the SQL mode: Go consults the statement's `TypeCtx` flags, and
//! this parses at the MOST PERMISSIVE setting (zero and invalid dates
//! allowed). That can only accept where Go rejects, never the reverse, so
//! adding this gate cannot turn a statement that works today into an error
//! for a reason Go would not raise.

use crate::EvalError;
use regex::Regex;
use std::sync::OnceLock;
use tidb_datatype::TimeType;

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
pub(crate) fn date_literal(text: &str) -> Result<String, EvalError> {
    if !date_pattern().is_match(text) {
        return Err(wrong_value(1292, "date", text));
    }
    parse(text, TimeType::Date, 0).map_err(|()| wrong_value(1292, "date", text))
}

/// Go `builtinTimestampLiteralSig`: the value of `TIMESTAMP 'lit'`, or the
/// error that rejects the whole statement.
///
/// The two codes are Go's own and are NOT interchangeable: the regex gate is
/// `ErrWrongValue2` (1525) and the parse failure is `ErrWrongValue` (1292),
/// which is why the recorded topic carries both against this one syntax.
pub(crate) fn timestamp_literal(text: &str) -> Result<String, EvalError> {
    if !timestamp_pattern().is_match(text) {
        return Err(wrong_value(1525, "datetime", text));
    }
    let fsp = i64::from(tidb_datatype::get_fsp(text));
    parse(text, TimeType::DateTime, fsp).map_err(|()| wrong_value(1292, "datetime", text))
}

fn parse(text: &str, kind: TimeType, fsp: i64) -> Result<String, ()> {
    tidb_datatype::parse_time(text, kind, fsp, false, true, true, &chrono::Utc)
        .map(|parsed| parsed.time.to_string())
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

    /// The three properties this module exists for, one case each, taken from
    /// `tests/integrationtest/r/types/time.result`.
    #[test]
    fn literal_gates_and_fraction_follow_the_recording() {
        // The regex gate: a date with no time is a 1525 for TIMESTAMP even
        // though CAST accepts it.
        assert!(matches!(
            timestamp_literal("2024-01-01"),
            Err(EvalError::WrongTemporalLiteral { code: 1525, .. })
        ));
        // A parse failure is a hard error, not a warning plus NULL.
        assert!(matches!(
            timestamp_literal("2024-01-01 14:00:00+14:01"),
            Err(EvalError::WrongTemporalLiteral { code: 1292, .. })
        ));
        // The literal's own fsp survives into the printed value.
        assert_eq!(
            timestamp_literal("2024-01-01 14:00:00.010").unwrap(),
            "2024-01-01 14:00:00.010"
        );
        assert_eq!(
            timestamp_literal("2024-01-01 14:00:00").unwrap(),
            "2024-01-01 14:00:00"
        );
        // DATE refuses a literal carrying a time part.
        assert!(matches!(
            date_literal("2024-01-01 01:12:31"),
            Err(EvalError::WrongTemporalLiteral { code: 1292, .. })
        ));
        assert_eq!(date_literal("2024-01-01").unwrap(), "2024-01-01");
    }
}
