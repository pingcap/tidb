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

//! `TIMEDIFF(expr1, expr2)`, transcreated from `timeDiffFunctionClass` and its
//! eight signatures (`pkg/expression/builtin_time.go`).
//!
//! Go picks the signature from the ARGUMENTS' eval types
//! (`getArgEvalTp`): a DURATION, DATETIME or TIMESTAMP argument keeps its
//! type and everything else is read as a string. A duration beside a
//! datetime is `builtinNullTimeDiffSig`; a string argument goes through
//! `convertStringToDuration` (`types.StrToDuration`), which answers a TIME
//! for most text and a DATETIME only for a twelve-digit-or-longer value, and
//! the call is NULL when the two sides land on different kinds.

use tidb_datatype::{Datum, DurationOrTime, EvalType, FieldType, MySqlDuration, Time, MAX_FSP};

use crate::{Columns, EvalError};

/// `timeDiffFunctionClass.getArgEvalTp`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TimeDiffArg {
    Duration,
    Time,
    String,
}

/// The kind Go's `getArgEvalTp` gives an argument of static type `field`.
/// Without a static type the datum's own kind stands in for it.
pub(crate) fn arg_kind(field: Option<&FieldType>, value: &Datum) -> TimeDiffArg {
    match field.map(FieldType::eval_type) {
        Some(EvalType::Duration) => TimeDiffArg::Duration,
        Some(EvalType::Datetime | EvalType::Timestamp) => TimeDiffArg::Time,
        Some(_) => TimeDiffArg::String,
        None => match value {
            Datum::Duration(_) => TimeDiffArg::Duration,
            Datum::Time(_) => TimeDiffArg::Time,
            _ => TimeDiffArg::String,
        },
    }
}

/// One side after its signature read it.
enum Operand {
    Duration(MySqlDuration),
    Time(Time),
}

/// Evaluates `TIMEDIFF` through the signature `kinds` selects. `fsp` is the
/// result type's decimal (`max(getExpressionFsp(args))`), which is also the
/// precision `convertStringToDuration` parses a string side at.
pub(crate) fn time_diff(
    vals: &[Datum],
    kinds: [TimeDiffArg; 2],
    fsp: i64,
    cols: &dyn Columns,
) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    // `builtinNullTimeDiffSig` answers NULL without reading either side.
    if matches!(
        kinds,
        [TimeDiffArg::Duration, TimeDiffArg::Time] | [TimeDiffArg::Time, TimeDiffArg::Duration]
    ) {
        return Ok(Datum::Null);
    }
    let Some(left) = operand(left, kinds[0], fsp, cols)? else {
        return Ok(Datum::Null);
    };
    let Some(right) = operand(right, kinds[1], fsp, cols)? else {
        return Ok(Datum::Null);
    };
    let difference = match (left, right) {
        (Operand::Duration(left), Operand::Duration(right)) => {
            let difference = left
                .checked_sub(right)
                .map_err(|_| EvalError::IntOverflow)?;
            truncate_overflow(difference, cols)?
        }
        (Operand::Time(left), Operand::Time(right)) => {
            let difference = left
                .sub(right, &cols.time_zone())
                .map_err(|_| EvalError::Unsupported("TIMEDIFF over an unconvertible TIMESTAMP"))?;
            truncate_overflow(difference, cols)?
        }
        // A string side that parsed to the other kind: `lhsIsDuration !=
        // rhsIsDuration`, or the `!isDuration` / `isDuration` exits of the
        // mixed string signatures.
        _ => return Ok(Datum::Null),
    };
    // The chunk column carries the result type's decimal, which is what Go
    // reads the value back with.
    Ok(Datum::Duration(MySqlDuration::from_raw_parts(
        difference.nanoseconds(),
        if fsp >= 0 { fsp } else { difference.fsp() },
    )))
}

/// `TIMEDIFF` for a tier without static argument types: the kinds come from
/// the datums, and the precision from each value the way `getExpressionFsp`
/// reads a constant (`types.GetFsp` of its string form).
pub(crate) fn time_diff_untyped(vals: &[Datum], cols: &dyn Columns) -> Result<Datum, EvalError> {
    let [left, right] = vals else {
        return Err(EvalError::Unsupported("bad function arity"));
    };
    let fsp = value_fsp(left)?.max(value_fsp(right)?);
    time_diff(
        vals,
        [arg_kind(None, left), arg_kind(None, right)],
        fsp,
        cols,
    )
}

fn value_fsp(value: &Datum) -> Result<i64, EvalError> {
    Ok(match value {
        Datum::Duration(duration) => duration.fsp().clamp(0, MAX_FSP),
        Datum::Time(time) => i64::from(time.fsp()),
        _ => crate::coerce::coerce_str(value)?
            .map_or(0, |text| i64::from(tidb_datatype::get_fsp(&text))),
    })
}

/// Reads one side as its signature does: `EvalDuration` / `EvalTime` for a
/// typed side, `EvalString` plus `convertStringToDuration` for the rest.
/// `None` is SQL NULL.
fn operand(
    value: &Datum,
    kind: TimeDiffArg,
    fsp: i64,
    cols: &dyn Columns,
) -> Result<Option<Operand>, EvalError> {
    if value.is_null() {
        return Ok(None);
    }
    match (kind, value) {
        (TimeDiffArg::Duration, Datum::Duration(duration)) => {
            Ok(Some(Operand::Duration(*duration)))
        }
        (TimeDiffArg::Time, Datum::Time(time)) => Ok(Some(Operand::Time(*time))),
        (TimeDiffArg::Duration, _) => Ok(
            match crate::cast::cast_arg_as_duration(value, None, cols)? {
                Datum::Duration(duration) => Some(Operand::Duration(duration)),
                _ => None,
            },
        ),
        (TimeDiffArg::Time, _) => Ok(
            match crate::cast::cast_arg_as_datetime(value, None, cols)? {
                Datum::Time(time) => Some(Operand::Time(time)),
                _ => None,
            },
        ),
        (TimeDiffArg::String, _) => {
            let Some(text) =
                crate::coerce::coerce_str(&crate::cast::cast_arg_as_string(value, None, cols)?)?
            else {
                return Ok(None);
            };
            convert_string_to_duration(&text, fsp, cols).map(Some)
        }
    }
}

/// Go `convertStringToDuration` + `types.StrToDuration`.
fn convert_string_to_duration(
    text: &str,
    fsp: i64,
    cols: &dyn Columns,
) -> Result<Operand, EvalError> {
    let mut fsp = fsp;
    if let Some(dot) = text.find('.') {
        let fraction_len = (text.len() - dot - 1) as i64;
        if fraction_len <= MAX_FSP {
            fsp = fsp.max(fraction_len);
        }
    }
    let trimmed = text.trim();
    let zone = cols.time_zone();
    match tidb_datatype::str_to_duration(trimmed, fsp, &zone) {
        Ok(converted) => match converted.value {
            // `StrToDateTime` succeeded; its own truncation is a plain
            // warning `parseDatetime` appends.
            DurationOrTime::Time(time) => {
                if converted.event.is_some() {
                    cols.append_warning(
                        1292,
                        &format!("Truncated incorrect datetime value: '{trimmed}'"),
                    );
                }
                Ok(Operand::Time(time))
            }
            DurationOrTime::Duration(duration) => {
                if converted.event.is_some() {
                    truncated_time(trimmed, cols)?;
                }
                Ok(Operand::Duration(duration))
            }
        },
        // `ParseDuration`'s rejected input is `ZeroDuration` beside
        // `ErrTruncatedWrongVal`, and `StrToDuration` keeps reading it as a
        // duration once the truncation is handled.
        Err(_) => {
            truncated_time(trimmed, cols)?;
            Ok(Operand::Duration(MySqlDuration::from_raw_parts(
                0,
                fsp.clamp(0, MAX_FSP),
            )))
        }
    }
}

fn truncated_time(text: &str, cols: &dyn Columns) -> Result<(), EvalError> {
    cols.handle_truncate(&format!("Truncated incorrect time value: '{text}'"))
}

/// The tail of `calculateTimeDiff` / `calculateDurationTimeDiff`:
/// `TruncateOverflowMySQLTime` clamps to the TIME range, and its
/// `ErrTruncatedWrongVal` names the unclamped value as a Go `time.Duration`.
fn truncate_overflow(
    difference: MySqlDuration,
    cols: &dyn Columns,
) -> Result<MySqlDuration, EvalError> {
    let clamped = tidb_datatype::truncate_overflow_mysql_time(difference.nanoseconds());
    if clamped.overflow().is_some() {
        cols.handle_truncate(&format!(
            "Truncated incorrect time value: '{}'",
            tidb_model::go_duration::format_go_duration(difference.nanoseconds())
        ))?;
    }
    Ok(MySqlDuration::from_raw_parts(
        clamped.value(),
        difference.fsp(),
    ))
}
