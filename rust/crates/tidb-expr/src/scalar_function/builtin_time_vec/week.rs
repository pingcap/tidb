// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin{WeekDay,WeekWithMode,WeekWithoutMode,YearWeekWithMode,WeekOfYear}Sig.vecEvalInt`.
//!
//! Unlike their row forms, which reject `IsZero() || InvalidZero()`, these
//! kernels reject only a zero date: a date with a zero month or day computes
//! on (`Weekday` normalizing it as Go's `time.Date` does, `Week` answering 0).

use super::super::{vec_eval, ScalarFunction};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::EvalType;
use tidb_proto::tipb::ScalarFuncSig;

/// Whether `function` is one of this file's signatures.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        WeekDay | WeekWithMode | WeekWithoutMode | YearWeekWithMode | WeekOfYear
    )
}

/// Go `handleInvalidTimeError(ctx, ErrWrongValue.GenWithStackByArgs(DateTimeStr, t))`.
fn zero_date(ctx: &dyn Columns, time: tidb_datatype::Time) -> Result<(), EvalError> {
    crate::expression_errors::handle_truncate(ctx, &format!("Incorrect datetime value: '{time}'"))
}

pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let n = input.num_rows();
    let sig = function.pb_code();
    let buf1 = vec_eval::argument(&function.args[0], EvalType::Datetime, ctx, input)?;
    let buf2 = match sig {
        WeekWithMode | YearWeekWithMode => {
            Some(vec_eval::argument(&function.args[1], EvalType::Int, ctx, input)?)
        }
        _ => None,
    };
    // Go `builtinWeekWithoutModeSig`: `default_week_format`, once per chunk.
    let default_mode = ctx.default_week_format();
    result.resize_int64(n, false);
    // `builtinWeekWithModeSig` tests the date column itself; the others merge it.
    if sig != WeekWithMode {
        result.merge_nulls(&[&buf1]);
    }
    for i in 0..n {
        if buf1.is_null(i) {
            result.set_null(i, true);
            continue;
        }
        let date = buf1.get_time(i);
        if date.is_zero() {
            zero_date(ctx, date)?;
            result.set_null(i, true);
            continue;
        }
        // A NULL mode is mode 0.
        let mode = buf2
            .as_ref()
            .map_or(default_mode, |modes| if modes.is_null(i) { 0 } else { modes.get_int64(i) });
        let core = date.core_time();
        let value = match sig {
            WeekDay => (i64::from(core.weekday().sunday_index()) + 6) % 7,
            WeekWithMode | WeekWithoutMode => i64::from(core.week(mode as u8)),
            WeekOfYear => i64::from(core.week(3)),
            YearWeekWithMode => {
                let (year, week) = core.year_week(mode as u8);
                let value = i64::from(week) + i64::from(year) * 100;
                if value < 0 { i64::from(u32::MAX) } else { value }
            }
            _ => unreachable!("not a week kernel: {sig:?}"),
        };
        result.with_int64s_mut(|i64s| i64s[i] = value);
    }
    Ok(())
}
