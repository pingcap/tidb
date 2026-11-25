// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go date-part and duration signatures from builtin_time_vec.go.

use super::super::{ScalarFunction, vec_eval};
use super::week::invalid_date;
use crate::expression::Expression;
use crate::{Columns, EvalError};
use std::borrow::Cow;
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{CoreTime, Datum, EvalType, MySqlDuration, Time, TimeType, TimestampInterval};
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn supported(sig: ScalarFuncSig) -> bool {
    use ScalarFuncSig::*;
    matches!(
        sig,
        Year | Month
            | DayOfMonth
            | Quarter
            | DayOfWeek
            | DayOfYear
            | ToDays
            | ToSeconds
            | Date
            | LastDay
            | DayName
            | MonthName
            | Hour
            | Minute
            | Second
            | MicroSecond
            | TimeToSec
            | ExtractDatetime
            | ExtractDuration
    )
}

pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    match sig {
        Hour | Minute | Second | MicroSecond | TimeToSec => {
            duration_part(function, ctx, input, result)
        }
        Date | LastDay => date_result(function, ctx, input, result),
        DayName | MonthName => name(function, ctx, input, result),
        ExtractDatetime | ExtractDuration => extract(function, ctx, input, result),
        _ => date_part(function, ctx, input, result),
    }
}

fn map_integer(
    function: &ScalarFunction,
    domain: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    project: impl Fn([u8; 8]) -> i64,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        domain,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let values = cells.fixed_cells::<8>();
            result.write_int64_rows((0..input.num_rows()).map(|row| {
                let row = selection.map_or(row, |selection| selection[row]);
                (!cells.is_null(row)).then(|| project(values[row]))
            }));
        },
    )
}

fn duration_part(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    let status = map_integer(function, EvalType::Duration, ctx, input, result, |raw| {
        let nanoseconds = i64::from_ne_bytes(raw);
        let duration = MySqlDuration::from_raw_parts(nanoseconds, 0);
        match sig {
            Hour => duration.hour(),
            Minute => duration.minute(),
            Second => duration.second(),
            MicroSecond => duration.microsecond(),
            TimeToSec => nanoseconds / 1_000_000_000,
            _ => unreachable!(),
        }
    });
    if status.is_err() && sig != TimeToSec {
        return super::super::builtin_control_vec::fallback(function, ctx, input, result);
    }
    status
}

fn date_part(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    use ScalarFuncSig::*;
    let sig = function.pb_code();
    if matches!(sig, Year | Month | DayOfMonth | Quarter) {
        return map_integer(function, EvalType::Datetime, ctx, input, result, |raw| {
            let date = CoreTime::from_raw(u64::from_ne_bytes(raw));
            match sig {
                Year => i64::from(date.year()),
                Month => i64::from(date.month()),
                DayOfMonth => i64::from(date.day()),
                Quarter => i64::from(date.month().div_ceil(3)),
                _ => unreachable!(),
            }
        });
    }
    let dates = vec_eval::read_argument(&function.args[0], EvalType::Datetime, ctx, input)?
        .into_contiguous();
    result.resize_int64(input.num_rows(), false);
    result.merge_nulls(&[&dates]);
    let dates = dates.raw_cells();
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, (dest, &raw)) in cells.iter_mut().zip(dates.fixed_cells()).enumerate() {
            let bit = 1 << (row & 7);
            let mask = &mut bitmap[row >> 3];
            if *mask & bit == 0 {
                continue;
            }
            let time = tidb_datatype::Time::from_go_raw_like_go(u64::from_ne_bytes(raw));
            let core = time.core_time();
            let value = match sig {
                DayOfWeek | DayOfYear if time.invalid_zero() => None,
                DayOfWeek => Some(i64::from(core.weekday().sunday_index()) + 1),
                DayOfYear => Some(i64::from(core.year_day())),
                ToDays | ToSeconds => {
                    let unit = if sig == ToDays {
                        TimestampInterval::Day
                    } else {
                        TimestampInterval::Second
                    };
                    let difference = CoreTime::default().timestamp_diff(core, unit);
                    (difference != 0).then_some(difference)
                }
                _ => unreachable!(),
            };
            if let Some(value) = value {
                *dest = value.to_ne_bytes();
            } else {
                invalid_date(ctx, time)?;
                *mask &= !bit;
            }
        }
        Ok(())
    })
}

fn date_result(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::eval_into(&function.args[0], EvalType::Datetime, ctx, input, result)?;
    let make_date = function.pb_code() == ScalarFuncSig::Date;
    let modes = ctx.date_modes();
    result.try_map_fixed_rows(|raw| {
        let time = Time::from_go_raw_like_go(u64::from_ne_bytes(raw));
        let core = time.core_time();
        let invalid = if make_date {
            if time.is_zero() {
                modes.no_zero_date
            } else {
                time.invalid_zero() && modes.no_zero_in_date
            }
        } else {
            core.month() == 0 || core.day() == 0 && modes.no_zero_date
        };
        if invalid {
            invalid_date(ctx, time)?;
            return Ok(None);
        }
        let day = if make_date {
            core.day()
        } else {
            tidb_datatype::get_last_day(core.year(), core.month())
        };
        let date = CoreTime::from_date(core.year() as u16, core.month(), day, 0, 0, 0, 0);
        let date = Time::new(date, TimeType::Date, 0).expect("DATE has no fractional precision");
        Ok(Some(date.go_raw().to_ne_bytes()))
    })
}

fn name(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let day_name = function.pb_code() == ScalarFuncSig::DayName;
    let rows = input.num_rows();
    if day_name {
        result.reserve_string(rows);
    }
    let argument = vec_eval::read_argument(&function.args[0], EvalType::Datetime, ctx, input)?;
    if !day_name {
        result.reserve_string(rows);
    }
    let cells = argument.cells();
    let read = cells.fixed::<8>();
    let no_zero_date = ctx.date_modes().no_zero_date;
    for row in 0..rows {
        let Some(raw) = read(row) else {
            result.append_null();
            continue;
        };
        let date = Time::from_go_raw_like_go(u64::from_ne_bytes(*raw));
        let core = date.core_time();
        let value = if day_name {
            if date.invalid_zero() {
                invalid_date(ctx, date)?;
                None
            } else {
                Some(crate::time_fn::WEEKDAY_NAMES[usize::from(core.weekday().sunday_index())])
            }
        } else {
            let month = usize::from(core.month());
            if (date.is_zero() && no_zero_date) || month > crate::time_fn::MONTH_NAMES.len() {
                invalid_date(ctx, date)?;
                None
            } else {
                month
                    .checked_sub(1)
                    .map(|index| crate::time_fn::MONTH_NAMES[index])
            }
        };
        if let Some(value) = value {
            result.append_string(value);
        } else {
            result.append_null();
        }
    }
    Ok(())
}

pub(super) fn dayname_index(
    function: &ScalarFunction,
    target: EvalType,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    crate::chunk_executor::allocate(target, input.num_rows(), result);
    let argument = vec_eval::read_argument(&function.args[0], EvalType::Datetime, ctx, input)?;
    let cells = argument.cells();
    let read = cells.fixed::<8>();
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, dest) in cells.iter_mut().enumerate() {
            let invalid = if let Some(raw) = read(row) {
                let date = Time::from_go_raw_like_go(u64::from_ne_bytes(*raw));
                if date.invalid_zero() {
                    invalid_date(ctx, date)?;
                    true
                } else {
                    let index = (i64::from(date.core_time().weekday().sunday_index()) + 6) % 7;
                    *dest = if target == EvalType::Real {
                        (index as f64).to_ne_bytes()
                    } else {
                        index.to_ne_bytes()
                    };
                    false
                }
            } else {
                true
            };
            if invalid {
                bitmap[row >> 3] &= !(1 << (row & 7));
            }
        }
        Ok(())
    })
}

fn extract(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let duration = function.pb_code() == ScalarFuncSig::ExtractDuration;
    let domain = if duration {
        EvalType::Duration
    } else {
        EvalType::Datetime
    };
    let unit_arg = &function.args[0];
    let literal_unit = match unit_arg {
        Expression::Constant(constant) => match constant.literal_value() {
            Some(value @ (Datum::String(_) | Datum::Bytes(_))) => {
                value.as_raw_bytes().map(String::from_utf8_lossy)
            }
            _ => None,
        },
        _ => None,
    };
    let unit = if literal_unit.is_none() {
        Some(vec_eval::read_argument(unit_arg, EvalType::String, ctx, input)?.into_contiguous())
    } else {
        None
    };
    let values = vec_eval::read_argument(&function.args[1], domain, ctx, input)?.into_contiguous();
    result.resize_int64(input.num_rows(), false);
    result.merge_nulls(&[&values]);
    if let Some(unit) = &unit {
        result.merge_nulls(&[unit]);
    }
    let units = unit.as_ref().map(|unit| unit.raw_cells());
    let values = values.raw_cells();
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, (dest, &raw)) in cells.iter_mut().zip(values.fixed_cells()).enumerate() {
            if bitmap[row >> 3] & (1 << (row & 7)) == 0 {
                continue;
            }
            let unit = if let Some(unit) = &literal_unit {
                Cow::Borrowed(unit.as_ref())
            } else {
                String::from_utf8_lossy(units.as_ref().expect("unit column").get(row))
            };
            let value = if duration {
                let time = MySqlDuration::from_raw_parts(i64::from_ne_bytes(raw), 0);
                tidb_datatype::extract_duration_num(time, &unit)
            } else {
                let time = Time::from_go_raw_like_go(u64::from_ne_bytes(raw));
                tidb_datatype::extract_datetime_num(time, &unit)
            };
            *dest = value
                .map_err(|error| {
                    EvalError::Conversion(tidb_terror::TerrorError::compatible(
                        tidb_terror::TerrorCode::new(1105),
                        error.to_string(),
                    ))
                })?
                .to_ne_bytes();
        }
        Ok(())
    })
}
