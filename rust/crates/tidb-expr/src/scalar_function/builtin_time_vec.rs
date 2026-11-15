// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_time_vec.go`: temporal argument columns and datatype kernels.

use super::{vec_eval, ScalarFunction};
mod date_arithmetic;
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType};
use tidb_proto::tipb::ScalarFuncSig;

/// Go temporal `vectorized` signatures whose column kernels are implemented here.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    if matches!(
        function.sig,
        crate::builtin::SigFields::TimeLiteral { .. }
            | crate::builtin::SigFields::DurationLiteral { .. }
    ) {
        return true;
    }
    matches!(
        function.pb_builtin.kernel,
        super::Kernel::DateArithmetic { .. }
    ) || matches!(
        function.pb_code(),
        Month
            | Year
            | Date
            | FromUnixTime1Arg
            | FromUnixTime2Arg
            | ExtractDatetime
            | ExtractDuration
            | DayName
            | WeekDay
            | TimeFormat
            | UtcTimeWithArg
            | UtcTimeWithoutArg
            | UnixTimestampCurrent
            | YearWeekWithoutMode
            | YearWeekWithMode
            | PeriodDiff
            | PeriodAdd
            | NowWithArg
            | NowWithoutArg
            | GetFormat
            | LastDay
            | StrToDateDate
            | StrToDateDatetime
            | StrToDateDuration
            | FromDays
            | MicroSecond
            | Quarter
            | WeekWithMode
            | WeekWithoutMode
            | ToSeconds
            | Minute
            | Second
            | MakeDate
            | WeekOfYear
            | UtcTimestampWithArg
            | UtcTimestampWithoutArg
            | TimeToSec
            | UtcDate
            | UnixTimestampDec
            | UnixTimestampInt
            | TimestampAdd
            | ToDays
            | DateFormatSig
            | Hour
            | SecToTime
            | DateDiff
            | CurrentDate
            | MakeTime
            | DayOfYear
            | TimestampDiff
            | CurrentTime0Arg
            | CurrentTime1Arg
            | Time
            | MonthName
            | SysDateWithFsp
            | SysDateWithoutFsp
            | DayOfWeek
            | ConvertTz
            | Timestamp1Arg
            | Timestamp2Args
            | DayOfMonth
            | DurationDurationTimeDiff
            | DurationStringTimeDiff
            | StringDurationTimeDiff
            | StringStringTimeDiff
            | StringTimeTimeDiff
            | TimeStringTimeDiff
            | TimeTimeTimeDiff
            | NullTimeDiff
    ) || crate::time_fn::add_sub::kinds_of_signature(function.pb_code()).is_some()
        || matches!(
            function.func_name.lowercase(),
            "tidb_bounded_staleness"
                | "tidb_parse_tso"
                | "period_add"
                | "period_diff"
                | "curdate"
                | "current_date"
                | "utc_date"
        )
}

/// Go `builtin{Month,Date,Week,Timestamp,Extract,...}Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    if let super::Kernel::DateArithmetic { subtract } = function.pb_builtin.kernel {
        return date_arithmetic::eval(function, subtract, ctx, input, result);
    }
    if matches!(sig, CurrentTime1Arg | UtcTimeWithArg) {
        return clock_duration(function, ctx, input, result);
    }
    if matches!(sig, SysDateWithFsp | SysDateWithoutFsp) {
        return sysdate(function, ctx, input, result);
    }
    if function.func_name.lowercase() == "tidb_bounded_staleness" {
        return bounded_staleness(function, ctx, input, result);
    }
    if matches!(
        sig,
        NullTimeDiff
            | AddTimeDateTimeNull
            | AddTimeStringNull
            | AddTimeDurationNull
            | SubTimeDateTimeNull
            | SubTimeStringNull
            | SubTimeDurationNull
    ) {
        let domain = function
            .get_static_type()
            .expect("time result type")
            .eval_type();
        crate::chunk_executor::allocate(domain, input.num_rows(), result);
        for row in 0..input.num_rows() {
            vec_eval::put(result, row, domain, &Datum::Null)?;
        }
        return Ok(());
    }
    if matches!(
        sig,
        Hour | Minute | Second | MicroSecond | TimeFormat | UnixTimestampInt | UnixTimestampDec
    ) {
        return with_row_fallback(function, ctx, input, result);
    }
    if function.args.is_empty() {
        let value = function.pb_builtin.eval_values(function, Vec::new(), ctx)?;
        let domain = function
            .get_static_type()
            .expect("time result type")
            .eval_type();
        crate::chunk_executor::allocate(domain, input.num_rows(), result);
        for row in 0..input.num_rows() {
            vec_eval::put(result, row, domain, &value)?;
        }
        return Ok(());
    }
    let add_sub_reuse =
        crate::time_fn::add_sub::kinds_of_signature(sig).is_some_and(|[first, second]| {
            use crate::time_fn::add_sub::TemporalKind;
            matches!(first, TemporalKind::Datetime | TemporalKind::Duration)
                && second != TemporalKind::Datetime
        });
    let reuse = (add_sub_reuse
        || matches!(
            sig,
            Date | LastDay | PeriodAdd | PeriodDiff | ConvertTz | MakeTime
        )
        || matches!(function.func_name.lowercase(), "period_add" | "period_diff"))
    .then_some(0);
    vec_eval::map_arguments(function, ctx, input, result, reuse, |mut values| {
        if sig == UtcTimestampWithArg && values[0].is_null() {
            return Ok(Datum::Null);
        }
        if sig == MakeTime {
            // Go `getVecIntParam`: REAL hours and minutes truncate to int64.
            for value in &mut values[..2] {
                if let Datum::Real(number) = value {
                    *value = Datum::Int(number.trunc() as i64);
                }
            }
        }
        // Go `builtin{Add,Sub}*Sig.vecEval*`: their own string parsing, not
        // the row form's (`add_sub_time`'s `row_path`).
        if let Some(sign) = crate::time_fn::add_sub::sign_of_signature(sig) {
            return super::pb_builtin::add_sub_time_of(function, &values, sign, false, ctx);
        }
        if sig == ConvertTz {
            return super::pb_builtin::temporal_of_ret_type(
                function,
                crate::time_fn::convert_tz::convert_tz(&values)?,
                ctx,
            );
        }
        function.pb_builtin.eval_values(function, values, ctx)
    })
}

/// Go `builtin{Hour,Minute,Second,MicroSecond,TimeFormat,UnixTimestamp*}Sig`:
/// only an error in the first argument's typed column evaluation uses rows.
fn with_row_fallback(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let domain = function
        .get_static_type()
        .expect("time result type")
        .eval_type();
    let first = &function.args[0];
    let first_type = first
        .static_type()
        .expect("typed temporal argument")
        .eval_type();
    let buffer = match vec_eval::argument(first, first_type, ctx, input) {
        Ok(buffer) => buffer,
        Err(_) => return super::builtin_control_vec::fallback(function, ctx, input, result),
    };
    let format = if function.args.len() == 2 {
        Some(vec_eval::argument(
            &function.args[1],
            EvalType::String,
            ctx,
            input,
        )?)
    } else {
        None
    };
    crate::chunk_executor::allocate(domain, input.num_rows(), result);
    for row in 0..input.num_rows() {
        let mut values = vec![vec_eval::value(
            &buffer,
            row,
            first_type,
            first.static_type(),
        )];
        if let Some(format) = &format {
            values.push(vec_eval::value(
                format,
                row,
                EvalType::String,
                function.args[1].static_type(),
            ));
        }
        let value = function.pb_builtin.eval_values(function, values, ctx)?;
        vec_eval::put(result, row, domain, &value)?;
    }
    Ok(())
}

/// Go `builtinSysDate{With,Without}FspSig.vecEvalTime`: one host instant,
/// rounded independently for each row's requested fractional precision.
fn sysdate(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use chrono::{Offset, TimeZone};
    let precision = function
        .args
        .first()
        .map(|arg| vec_eval::argument(arg, EvalType::Int, ctx, input))
        .transpose()?;
    let now = chrono::Utc::now();
    let offset = ctx
        .time_zone()
        .offset_from_utc_datetime(&now.naive_utc())
        .fix()
        .local_minus_utc();
    result.resize_time(input.num_rows(), false);
    if let Some(buffer) = &precision {
        result.merge_nulls(&[buffer]);
    }
    for row in 0..input.num_rows() {
        if result.is_null(row) {
            continue;
        }
        let fsp = precision.as_ref().map_or(0, |buffer| buffer.get_int64(row));
        let fsp =
            tidb_datatype::check_fsp(fsp).map_err(|error| EvalError::Raw(error.to_string()))?;
        let text = crate::time_fn::format_datetime(
            now.timestamp() + i64::from(offset),
            now.timestamp_subsec_nanos(),
            fsp as u32,
            true,
        );
        let value = crate::cast::parse_computed_time(
            &Datum::new_string_datum(text),
            ctx,
            tidb_datatype::TimeType::DateTime,
            Some(fsp),
        )?;
        vec_eval::put(result, row, EvalType::Datetime, &value)?;
    }
    Ok(())
}

/// Go `builtinTiDBBoundedStalenessSig.vecEvalTime`: one SafeTS snapshot
/// after both argument columns, and both invalid endpoint diagnostics.
fn bounded_staleness(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let left = vec_eval::argument(&function.args[0], EvalType::Datetime, ctx, input)?;
    let right = vec_eval::argument(&function.args[1], EvalType::Datetime, ctx, input)?;
    let safe = crate::time_fn::session_tz::get_time_from_ts(ctx.get_stmt_min_safe_ts(), ctx)?;
    result.resize_time(input.num_rows(), false);
    result.merge_nulls(&[&left, &right]);
    for row in 0..input.num_rows() {
        if result.is_null(row) {
            continue;
        }
        let (left, right) = (left.get_time(row), right.get_time(row));
        if left.invalid_zero() || right.invalid_zero() {
            let mut error = Ok(());
            for time in [left, right] {
                if time.invalid_zero() {
                    error = crate::expression_errors::handle_truncate(
                        ctx,
                        &format!("Incorrect datetime value: '{time}'"),
                    );
                }
            }
            error?;
            result.set_null(row, true);
            continue;
        }
        if left.compare(right).is_gt() || safe.is_none() {
            result.set_null(row, true);
            continue;
        }
        let mut value =
            crate::time_fn::cal_appropriate_time(left, right, safe.expect("checked SafeTS"));
        value.set_kind(tidb_datatype::TimeType::DateTime);
        value.set_fsp(3).expect("valid fsp");
        vec_eval::put(result, row, EvalType::Datetime, &Datum::Time(value))?;
    }
    Ok(())
}

/// Go `builtinDayNameSig.vecEvalIndex`, `vecEvalInt`, and `vecEvalReal`.
pub(super) fn dayname_index(
    function: &ScalarFunction,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let dates = vec_eval::argument(&function.args[0], EvalType::Datetime, ctx, input)?;
    crate::chunk_executor::allocate(target, input.num_rows(), result);
    result.merge_nulls(&[&dates]);
    for row in 0..input.num_rows() {
        if result.is_null(row) {
            continue;
        }
        let value = crate::time_fn::weekday(&[Datum::Time(dates.get_time(row))], ctx)?;
        let value = if target == EvalType::Real {
            match value {
                Datum::Int(value) => Datum::Real(value as f64),
                value => value,
            }
        } else {
            value
        };
        vec_eval::put(result, row, target, &value)?;
    }
    Ok(())
}

/// Go `builtinCurrentTime1ArgSig.vecEvalDuration` and
/// `builtinUTCTimeWithArgSig.vecEvalDuration`: one statement-time snapshot,
/// formatted to microseconds before each precision is applied.
fn clock_duration(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let precision = vec_eval::argument(&function.args[0], EvalType::Int, ctx, input)?;
    let (seconds, nanos, offset) = ctx
        .now()
        .ok_or(EvalError::Unsupported("no session clock (SET timestamp)"))?;
    let utc = function.pb_code() == ScalarFuncSig::UtcTimeWithArg;
    let text = crate::time_fn::format_time_only(
        seconds + if utc { 0 } else { i64::from(offset) },
        nanos,
        6,
        false,
    );
    crate::chunk_executor::allocate(EvalType::Duration, input.num_rows(), result);
    for row in 0..input.num_rows() {
        if utc && precision.is_null(row) {
            result.set_null(row, true);
            continue;
        }
        let fsp = precision.get_int64(row);
        if utc {
            crate::time_fn::parse_fsp_for(&[Datum::Int(fsp)], "utc_time")?;
        }
        let parsed = tidb_datatype::parse_mysql_duration(
            &text,
            fsp,
            &ctx.time_zone(),
            true,
            ctx.date_modes().allow_invalid_dates,
        )
        .map_err(|error| EvalError::Raw(error.to_string()))?;
        let value =
            tidb_datatype::MySqlDuration::from_nanoseconds(parsed.nanoseconds(), parsed.fsp())
                .map_err(|error| EvalError::Raw(error.to_string()))?;
        vec_eval::put(result, row, EvalType::Duration, &Datum::Duration(value))?;
    }
    Ok(())
}
