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

//! `ADDTIME`, `SUBTIME`, `TIMESTAMP`, `TIMESTAMPADD` and `SYSDATE`, from
//! `pkg/expression/builtin_time.go`.
//!
//! # What makes these five one module
//!
//! Go picks their SIGNATURE from the argument `FieldType`s at build time.
//! `addTimeFunctionClass.getFunction` is a twelve-way switch over the
//! `(tp1, tp2)` cross product, and the arms differ in more than bookkeeping:
//! a DATETIME second argument makes the whole call NULL whatever the values
//! are, and the result fsp comes from a different operand in each arm.
//! [`TemporalKind`] is that switch, and [`add_sub_time`] is the twelve arms.
//!
//! # The two tiers, and Go's own row/vec split
//!
//! Go carries TWO bodies per signature: `evalString`/`evalTime` (the row
//! path, which is also what CONSTANT FOLDING runs) and `vecEvalString`
//! (the vectorized path a real column takes). They are not the same
//! function, and the difference is observable. Captured:
//!
//! ```text
//! -- both operands constant, so Go folds and takes the ROW path
//! select addtime('2020-01-01 10:00:00','2020-01-01 10:00:00')  NULL
//! -- the same values in a VARCHAR column, so Go takes the VEC path
//! select addtime(a,b) from u  -- a=b='2020-01-01 10:00:00'     2020-01-01 20:00:00
//! ```
//!
//! `builtinAddStringAndStringSig.evalString` ends with a `parser.Number` /
//! `parser.Char('-')` guard that nulls a second argument shaped
//! `<digits>-<more>`; `builtinAddStringAndStringSig.vecEvalString`
//! (`builtin_time_vec_generated.go:370`) simply does not have it. SUBTIME's
//! row body does not have it either, which is why the same pair of constants
//! answers a real value under `SUBTIME`. [`add_sub_time`]'s `row_path` flag
//! is that guard, and nothing else.
//!
//! # SYSDATE clock selection
//!
//! `builtinSysDateWithoutFspSig` calls `time.Now()` per evaluation, where
//! `NOW` returns the one statement timestamp. `tidb_sysdate_is_now` changes
//! `SYSDATE` into the latter before evaluation.

use tidb_datatype::{Datum, FieldType, FieldTypeCode};

use super::duration_parse::{
    self, fsp_for_time_add_sub, get_fsp, is_duration, parse_datetime, parse_duration, GoDateTime,
    GoDuration, Truncated, MAX_FSP, MIN_FSP,
};
use crate::coerce::coerce_str;
use crate::{Columns, EvalError};

/// The three temporal branches `getBf4TimeAddSub` reads off an argument's
/// `FieldType`, plus the `default` arm that covers everything else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TemporalKind {
    /// `mysql.TypeDatetime` / `mysql.TypeTimestamp`.
    Datetime,
    /// `mysql.TypeDate`.
    Date,
    /// `mysql.TypeDuration`.
    Duration,
    /// Go's `default`: a string, a number, anything else.
    Other,
}

/// The argument branch, taken from the static `FieldType` where the chunk
/// tier has one and from the DATUM otherwise. The AST tier has no field
/// types at all, so a plain string literal lands on `Other` -- which is the
/// arm Go itself selects for a string constant.
pub(crate) fn kind_of(field_type: Option<&FieldType>, value: &Datum) -> TemporalKind {
    if let Some(ft) = field_type {
        return match ft.code() {
            FieldTypeCode::Datetime | FieldTypeCode::Timestamp => TemporalKind::Datetime,
            FieldTypeCode::Date | FieldTypeCode::NewDate => TemporalKind::Date,
            FieldTypeCode::Duration => TemporalKind::Duration,
            _ => TemporalKind::Other,
        };
    }
    match value {
        Datum::Time(_) => TemporalKind::Datetime,
        Datum::Duration(_) => TemporalKind::Duration,
        _ => TemporalKind::Other,
    }
}

fn truncated_time_warning(cols: &dyn Columns, value: &str) -> Datum {
    cols.append_warning(1292, &format!("Truncated incorrect time value: '{value}'"));
    Datum::Null
}

/// `ADDTIME`/`SUBTIME` where no static argument type is available: the AST
/// tier, and the chunk tier's fallback. Both arguments take Go's `default`
/// branch unless the DATUM itself is temporal, and the ROW body applies --
/// which is the body Go's constant folding runs for a literal call.
pub(crate) fn add_sub_untyped(
    name: &str,
    vals: &[Datum],
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let kinds = [kind_of(None, &vals[0]), kind_of(None, &vals[1])];
    let sign = if name.eq_ignore_ascii_case("SUBTIME") {
        -1
    } else {
        1
    };
    add_sub_time(vals, kinds, sign, true, cols)
}

/// Go's `getBf4TimeAddSub` + `addTimeFunctionClass.getFunction` /
/// `subTimeFunctionClass.getFunction`, evaluated.
///
/// `sign` is `1` for `ADDTIME` and `-1` for `SUBTIME`; `row_path` selects
/// Go's `evalString` body over its `vecEvalString` one (see the module doc).
pub(crate) fn add_sub_time(
    vals: &[Datum],
    kinds: [TemporalKind; 2],
    sign: i64,
    row_path: bool,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    if vals.len() != 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    // Every `...Null` signature: a DATETIME/TIMESTAMP second argument makes
    // the result NULL whatever the first argument is.
    if kinds[1] == TemporalKind::Datetime {
        return Ok(Datum::Null);
    }
    let (Some(left), Some(right)) = (coerce_str(&vals[0])?, coerce_str(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    match kinds[0] {
        // `...DatetimeAnd*`: the result is a DATETIME whose fsp is the FIRST
        // argument's. The vectorized arm hands `Time.Add` a
        // `Duration{Fsp: -1}`, so the duration never raises it.
        TemporalKind::Datetime => {
            let Some(delta) = second_as_duration(&right, kinds[1], cols)? else {
                return Ok(Datum::Null);
            };
            datetime_result(&left, GoDuration { fsp: -1, ..delta }, sign)
        }
        // `...DateAnd*`: `arg0.SetType(TypeDatetime)` first, so a DATE reads
        // as midnight; the result is a STRING and the DATE's own fsp is 0,
        // which leaves the duration's fsp deciding.
        TemporalKind::Date => {
            let Some(delta) = second_as_duration(&right, kinds[1], cols)? else {
                return Ok(Datum::Null);
            };
            datetime_result(&left, delta, sign)
        }
        // `...DurationAnd*`: both operands are durations and so is the
        // result, at the larger of the two fsps.
        // The first operand is a TIME column here, so its fsp is its own
        // (Go's `EvalDuration` reads the column type's decimal), NOT MaxFsp
        // the way the string arm's `strDurationAddDuration` parses it.
        TemporalKind::Duration => {
            let Ok(first) = parse_duration(&left, get_fsp(&left)) else {
                return Ok(truncated_time_warning(cols, &left));
            };
            let Some(delta) = second_as_duration(&right, kinds[1], cols)? else {
                return Ok(Datum::Null);
            };
            Ok(Datum::new_string(first.combine(delta, sign).format()))
        }
        // `...StringAnd*`: the ONE arm that decides between the duration and
        // the datetime reading at RUNTIME, from the first argument's text.
        TemporalKind::Other => {
            let delta = match kinds[1] {
                TemporalKind::Duration => match parse_duration(&right, MAX_FSP) {
                    Ok(delta) => delta,
                    Err(Truncated) => return Ok(truncated_time_warning(cols, &right)),
                },
                _ => {
                    // `builtinAddStringAndStringSig`: the second argument's
                    // fsp comes from `getFsp4TimeAddSub`, not `GetFsp`.
                    match parse_duration(&right, fsp_for_time_add_sub(&right)) {
                        Ok(delta) => delta,
                        Err(Truncated) => return Ok(truncated_time_warning(cols, &right)),
                    }
                }
            };
            // ADDTIME only (`sign > 0`): `builtinSubStringAndStringSig` has
            // no such guard, which is why the same constant pair answers
            // NULL under ADDTIME and a real value under SUBTIME.
            if row_path
                && sign > 0
                && kinds[1] != TemporalKind::Duration
                && trailing_dash_group(&right)
            {
                return Ok(Datum::Null);
            }
            if is_duration(&left) {
                let Ok(first) = parse_duration(&left, MAX_FSP) else {
                    return Ok(truncated_time_warning(cols, &left));
                };
                let sum = first.combine(delta, sign);
                let fsp = if sum.micro_second() == 0 {
                    MIN_FSP
                } else {
                    MAX_FSP
                };
                return Ok(Datum::new_string(GoDuration { fsp, ..sum }.format()));
            }
            // `strDatetimeAddDuration`/`strDatetimeSubDuration`: the datetime
            // is parsed at MaxFsp and the RESULT's fsp is MaxFsp only when
            // the sum carries a microsecond.
            str_datetime_add_duration(&left, delta, sign, cols)
        }
    }
}

/// The second argument as a duration, for every arm whose second operand is
/// evaluated as one. `None` means the whole call is NULL.
fn second_as_duration(
    text: &str,
    kind: TemporalKind,
    cols: &dyn Columns,
) -> Result<Option<GoDuration>, EvalError> {
    if kind != TemporalKind::Duration && !is_duration(text) {
        // `builtin...AndStringSig`: a second argument that is not
        // duration-shaped is NULL without a warning.
        return Ok(None);
    }
    match parse_duration(text, get_fsp(text)) {
        Ok(duration) => Ok(Some(duration)),
        Err(Truncated) => {
            truncated_time_warning(cols, text);
            Ok(None)
        }
    }
}

/// `builtinAdd{Datetime,Date}And{Duration,String}Sig`: the first argument is
/// evaluated as a DATETIME, and a zero one makes the result NULL.
fn datetime_result(text: &str, delta: GoDuration, sign: i64) -> Result<Datum, EvalError> {
    let Some(first) = parse_datetime(text) else {
        return Ok(Datum::Null);
    };
    if first.is_zero() {
        return Ok(Datum::Null);
    }
    let signed = GoDuration {
        micros: delta.micros * sign,
        ..delta
    };
    match first.add(signed) {
        Some(result) if result.in_range() => Ok(Datum::new_string(result.format())),
        _ => Ok(Datum::Null),
    }
}

/// Go `strDatetimeAddDuration`/`strDatetimeSubDuration`.
fn str_datetime_add_duration(
    text: &str,
    delta: GoDuration,
    sign: i64,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    let Some(first) = parse_datetime(text) else {
        // Go appends the parse error as a warning "regardless of the
        // sql_mode, this is compatible with MySQL" and answers NULL.
        cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
        return Ok(Datum::Null);
    };
    let first = GoDateTime {
        fsp: MAX_FSP,
        ..first
    };
    let signed = GoDuration {
        micros: delta.micros * sign,
        ..delta
    };
    let Some(result) = first.add(signed) else {
        return Ok(Datum::Null);
    };
    if !result.in_range() {
        return Ok(Datum::Null);
    }
    let fsp = if result.micros == 0 { MIN_FSP } else { MAX_FSP };
    Ok(Datum::new_string(GoDateTime { fsp, ..result }.format()))
}

/// The tail of `builtinAddStringAndStringSig.evalString`: a second argument
/// that reads as `<digits>-<something>` makes the result NULL. Only ADDTIME,
/// only the row path (see the module doc).
fn trailing_dash_group(text: &str) -> bool {
    let trimmed = text.trim_start_matches(|c: char| c.is_ascii_whitespace());
    let digits = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    if digits == 0 {
        return false;
    }
    matches!(trimmed[digits..].strip_prefix('-'), Some(rest) if !rest.is_empty())
}

/// `timestampFunctionClass`: `builtinTimestamp1ArgSig` /
/// `builtinTimestamp2ArgsSig`. The result is a DATETIME whose fsp is the
/// argument's own; the second argument is a DURATION added to it, and it is
/// rejected outright when it carries a date part.
pub(crate) fn timestamp(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.is_empty() || vals.len() > 2 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let Some(text) = coerce_str(&vals[0])? else {
        return Ok(Datum::Null);
    };
    let Some(base) = parse_datetime(&text) else {
        cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
        return Ok(Datum::Null);
    };
    let base = GoDateTime {
        fsp: get_fsp(&text),
        ..base
    };
    if vals.len() == 1 {
        return Ok(Datum::new_string(base.format()));
    }
    let Some(second) = coerce_str(&vals[1])? else {
        return Ok(Datum::Null);
    };
    // `builtinTimestamp2ArgsSig`: a second argument that is not
    // duration-shaped is NULL before any parse is attempted, and so is a
    // first argument with a zero year ("MySQL won't evaluate add for date
    // with zero year").
    if base.year == 0 || !is_duration(&second) {
        return Ok(Datum::Null);
    }
    let Ok(delta) = parse_duration(&second, get_fsp(&second)) else {
        return Ok(Datum::Null);
    };
    match base.add(delta) {
        Some(result) if result.in_range() => Ok(Datum::new_string(
            GoDateTime {
                fsp: base.fsp.max(delta.fsp),
                ..result
            }
            .format(),
        )),
        _ => Ok(Datum::Null),
    }
}

/// `builtinTimestampAddSig.evalString` + `addUnitToTime`.
pub(crate) fn timestamp_add(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if vals.len() != 3 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let (Some(unit), Some(amount)) = (coerce_str(&vals[0])?, number_of(&vals[1])?) else {
        return Ok(Datum::Null);
    };
    let Some(text) = coerce_str(&vals[2])? else {
        return Ok(Datum::Null);
    };
    let Some(base) = parse_datetime(&text) else {
        cols.append_warning(1292, &format!("Incorrect datetime value: '{text}'"));
        return Ok(Datum::Null);
    };
    let unit = unit.to_ascii_uppercase();
    let Some(result) = add_unit_to_time(&unit, base, amount) else {
        return Err(EvalError::Unsupported("TIMESTAMPADD unit"));
    };
    let Some(result) = result else {
        return Ok(Datum::Null);
    };
    if !result.in_range() {
        cols.append_warning(
            1292,
            &format!(
                "Incorrect time value: '{{{} {} {} {} {} {} {}}}'",
                result.year,
                result.month,
                result.day,
                result.hour,
                result.minute,
                result.second,
                result.micros
            ),
        );
        return Ok(Datum::Null);
    }
    // Go: `fsp := types.DefaultFsp`, raised to `MaxFsp` when the result
    // carries a microsecond.
    let fsp = if result.micros == 0 { MIN_FSP } else { MAX_FSP };
    Ok(Datum::new_string(GoDateTime { fsp, ..result }.format()))
}

/// Go `addUnitToTime`. The outer `None` is an unknown unit (Go's
/// `ErrWrongValue`); the inner `None` is its `overflow` return.
fn add_unit_to_time(unit: &str, base: GoDateTime, amount: f64) -> Option<Option<GoDateTime>> {
    // Go computes BOTH: `s` is the truncated microsecond count, used only by
    // SECOND, and `v` is the rounded whole count every other unit uses.
    let truncated_micros = (amount * 1_000_000.0).trunc();
    let rounded = amount.round();
    let micros = match unit {
        "MICROSECOND" => rounded,
        "SECOND" => truncated_micros,
        "MINUTE" => rounded * 60_000_000.0,
        "HOUR" => rounded * 3_600_000_000.0,
        "DAY" => rounded * 86_400_000_000.0,
        "WEEK" => rounded * 7.0 * 86_400_000_000.0,
        "MONTH" => return Some(add_months(base, rounded, true)),
        "QUARTER" => return Some(add_months(base, rounded * 3.0, false)),
        "YEAR" => return Some(add_months(base, rounded * 12.0, false)),
        _ => return None,
    };
    if !micros.is_finite() || micros.abs() > 9e18 {
        return Some(None);
    }
    Some(base.add(GoDuration {
        micros: micros as i64,
        fsp: base.fsp,
    }))
}

/// The MONTH/QUARTER/YEAR arms of `addUnitToTime`. Go's MONTH arm goes
/// through `types.AddDate`, which CLAMPS to the target month's last day
/// (`2020-01-31 + 1 MONTH` is `2020-02-29`), while its QUARTER and YEAR arms
/// go through Go's own `time.Time.AddDate`, which OVERFLOWS
/// (`2020-02-29 + 1 YEAR` is `2021-03-01`). Both were captured.
fn add_months(base: GoDateTime, months: f64, clamp: bool) -> Option<GoDateTime> {
    if !months.is_finite() || months.abs() > 1e6 {
        return None;
    }
    let total = base.year * 12 + i64::from(base.month) - 1 + months as i64;
    if total < 0 {
        return None;
    }
    let year = total / 12;
    let month = (total % 12 + 1) as u32;
    let day = if clamp {
        base.day.min(last_day_of_month(year, month))
    } else {
        base.day
    };
    // A day past the target month's end rolls into the next month here,
    // which is exactly what Go's `time.Time.AddDate` normalization does.
    let (year, month, day) =
        duration_parse::date_from_daynr(duration_parse::daynr(year, month, 1) + i64::from(day) - 1);
    Some(GoDateTime {
        year,
        month,
        day,
        ..base
    })
}

fn last_day_of_month(year: i64, month: u32) -> u32 {
    let next = if month == 12 {
        duration_parse::daynr(year + 1, 1, 1)
    } else {
        duration_parse::daynr(year, month + 1, 1)
    };
    (next - duration_parse::daynr(year, month, 1)) as u32
}

fn number_of(value: &Datum) -> Result<Option<f64>, EvalError> {
    Ok(match value {
        Datum::Null => None,
        Datum::Int(v) => Some(*v as f64),
        Datum::UInt(v) => Some(*v as f64),
        Datum::Real(v) => Some(*v),
        Datum::Float32(v) => Some(*v),
        Datum::Decimal(d) => Some(d.to_f64()),
        _ => coerce_str(value)?.map(|text| text.trim().parse::<f64>().unwrap_or(0.0)),
    })
}

/// `builtinSysDateWithFspSig`/`builtinSysDateWithoutFspSig`: `time.Now()` in
/// the session zone, ROUNDED half-up to `fsp` digits -- not the statement
/// clock `NOW` reads, which is why two `SYSDATE()` calls in one statement can
/// differ and `SYSDATE() = NOW()` is `0` on a session whose statement clock
/// was taken earlier. With `tidb_sysdate_is_now=ON`, Go builds `NOW` instead,
/// including its truncating FSP behavior.
pub(crate) fn sysdate(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    if cols.sysdate_is_now() {
        return super::now(vals, cols);
    }
    if vals.len() > 1 {
        return Err(EvalError::Unsupported("bad function arity"));
    }
    let fsp = match vals.first() {
        None | Some(Datum::Null) => 0,
        Some(Datum::Int(value)) if (0..=i64::from(MAX_FSP)).contains(value) => *value as u32,
        Some(Datum::UInt(value)) if *value <= MAX_FSP as u64 => *value as u32,
        Some(value) => {
            let converted = value
                .to_i64()
                .map_err(|_| EvalError::Unsupported("bad fractional-seconds-precision argument"))?;
            if !(0..=i64::from(MAX_FSP)).contains(&converted.value) {
                return Err(EvalError::Unsupported(
                    "bad fractional-seconds-precision argument",
                ));
            }
            converted.value as u32
        }
    };
    // Only the ZONE comes from the statement clock; the instant does not.
    let (_, _, tz_offset) = cols.now().ok_or(EvalError::Unsupported(
        "SYSDATE needs the session clock, which is not wired here",
    ))?;
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| EvalError::Unsupported("the host clock is before the Unix epoch"))?;
    let secs = elapsed.as_secs() as i64 + i64::from(tz_offset);
    Ok(Datum::new_string(super::format_datetime(
        secs,
        elapsed.subsec_nanos(),
        fsp,
        true,
    )))
}

#[cfg(test)]
mod sysdate_source_tests {
    use super::sysdate;
    use crate::{Columns, Datum, EvalError};

    struct StatementClock(i64);

    impl Columns for StatementClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((self.0, 0, 0))
        }
    }

    struct AliasedStatementClock;

    impl Columns for AliasedStatementClock {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn now(&self) -> Option<(i64, u32, i32)> {
            Some((1_700_000_000, 654_999_999, 8 * 60 * 60))
        }

        fn sysdate_is_now(&self) -> bool {
            true
        }
    }

    fn host_now(fsp: u32) -> String {
        let elapsed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        super::super::format_datetime(elapsed.as_secs() as i64, elapsed.subsec_nanos(), fsp, true)
    }

    /// Go `TestSysDate`: the function reads the host clock rather than the
    /// statement `timestamp`, accepts FSP 0 through 6, and rejects a negative
    /// constant. Rust has one evaluator, so the source's row/vector loops
    /// converge on this same boundary.
    #[test]
    fn test_sys_date() {
        for statement_timestamp in [1_234, 0] {
            let before = host_now(0);
            let result = sysdate(&[], &StatementClock(statement_timestamp)).unwrap();
            let after = host_now(0);
            let Datum::String(result) = result else {
                panic!("SYSDATE must return its datetime string");
            };
            let result = result.as_utf8().unwrap();
            assert!(before.as_str() <= result && result <= after.as_str());
        }

        for fsp in 0..=6 {
            let before = host_now(fsp);
            let result = sysdate(&[Datum::Int(i64::from(fsp))], &StatementClock(0)).unwrap();
            let after = host_now(fsp);
            let Datum::String(result) = result else {
                panic!("SYSDATE({fsp}) must return its datetime string");
            };
            let result = result.as_utf8().unwrap();
            assert!(
                before.as_str() <= result && result <= after.as_str(),
                "fsp={fsp}"
            );
        }

        assert_eq!(
            sysdate(&[Datum::Int(-2)], &StatementClock(0)),
            Err(EvalError::Unsupported(
                "bad fractional-seconds-precision argument"
            ))
        );
    }

    #[test]
    fn sysdate_is_now_uses_the_statement_clock_and_now_rounding() {
        assert_eq!(
            sysdate(&[], &AliasedStatementClock).unwrap(),
            Datum::new_string("2023-11-15 06:13:20")
        );
        assert_eq!(
            sysdate(&[Datum::Int(3)], &AliasedStatementClock).unwrap(),
            Datum::new_string("2023-11-15 06:13:20.654")
        );
        assert_eq!(
            sysdate(&[Datum::Int(6)], &AliasedStatementClock).unwrap(),
            Datum::new_string("2023-11-15 06:13:20.654999")
        );
    }
}
