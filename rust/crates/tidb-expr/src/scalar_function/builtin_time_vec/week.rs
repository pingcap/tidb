// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin{WeekDay,WeekWithMode,WeekWithoutMode,YearWeekWithMode,WeekOfYear}Sig.vecEvalInt`.
//!
//! Unlike their row forms, which reject `IsZero() || InvalidZero()`, these
//! kernels reject only a zero date: a date with a zero month or day computes
//! on (`Weekday` normalizing it as Go's `time.Date` does, `Week` answering 0).

use super::super::{ScalarFunction, vec_eval};
use crate::expression::Expression;
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType, Time};
use tidb_proto::tipb::ScalarFuncSig;

/// Whether `function` is one of this file's signatures.
pub(super) fn supported(function: &ScalarFunction) -> bool {
    use ScalarFuncSig::*;
    matches!(
        function.pb_code(),
        WeekDay
            | WeekWithMode
            | WeekWithoutMode
            | YearWeekWithoutMode
            | YearWeekWithMode
            | WeekOfYear
    )
}

/// Go `handleInvalidTimeError(ctx, ErrWrongValue.GenWithStackByArgs(DateTimeStr, t))`.
pub(super) fn invalid_date(ctx: &dyn Columns, time: Time) -> Result<(), EvalError> {
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
    let buf1 = vec_eval::read_argument(&function.args[0], EvalType::Datetime, ctx, input)?
        .into_contiguous();
    let mode_arg = function.args.get(1);
    let literal_mode = mode_arg.and_then(|argument| {
        let Expression::Constant(constant) = argument else {
            return None;
        };
        match constant.literal_value()? {
            Datum::Int(value) => Some(*value),
            Datum::UInt(value) => Some(*value as i64),
            Datum::Null => Some(0),
            _ => None,
        }
    });
    let buf2 = if literal_mode.is_none() {
        mode_arg
            .map(|argument| vec_eval::read_argument(argument, EvalType::Int, ctx, input))
            .transpose()?
    } else {
        None
    };
    // Go `builtinWeekWithoutModeSig`: `default_week_format`, once per chunk.
    let default_mode = literal_mode.unwrap_or_else(|| {
        if sig == WeekWithoutMode {
            ctx.default_week_format()
        } else {
            0
        }
    });
    result.resize_int64(n, false);
    // `builtinWeekWithModeSig` tests the date column itself; the others merge it.
    if sig != WeekWithMode {
        result.merge_nulls(&[&buf1]);
    }
    let dates = buf1.raw_cells();
    let modes = buf2.as_ref().map(vec_eval::ReadArgument::cells);
    let read_mode = modes.as_ref().map(|cells| cells.fixed::<8>());
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (i, (dest, &raw)) in cells.iter_mut().zip(dates.fixed_cells()).enumerate() {
            let bit = 1 << (i & 7);
            let mask = &mut bitmap[i >> 3];
            if *mask & bit == 0 || (sig == WeekWithMode && dates.is_null(i)) {
                *mask &= !bit;
                continue;
            }
            let date = tidb_datatype::Time::from_go_raw_like_go(u64::from_ne_bytes(raw));
            let invalid = if sig == YearWeekWithoutMode {
                date.invalid_zero()
            } else {
                date.is_zero()
            };
            if invalid {
                invalid_date(ctx, date)?;
                *mask &= !bit;
                continue;
            }
            // A NULL mode is mode 0.
            let mode = read_mode.as_ref().map_or(default_mode, |read| {
                read(i).map_or(0, |bits| i64::from_ne_bytes(*bits))
            });
            let core = date.core_time();
            let value = match sig {
                WeekDay => (i64::from(core.weekday().sunday_index()) + 6) % 7,
                WeekWithMode | WeekWithoutMode => i64::from(core.week(mode as u8)),
                WeekOfYear => i64::from(core.week(3)),
                YearWeekWithMode | YearWeekWithoutMode => {
                    let (year, week) = core.year_week(mode as u8);
                    let value = i64::from(week) + i64::from(year) * 100;
                    if value < 0 {
                        i64::from(u32::MAX)
                    } else {
                        value
                    }
                }
                _ => unreachable!("not a week kernel: {sig:?}"),
            };
            *dest = value.to_ne_bytes();
        }
        Ok(())
    })
}
