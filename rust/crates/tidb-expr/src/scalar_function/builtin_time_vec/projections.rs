// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go's typed DATE_FORMAT, DATEDIFF and TIMESTAMPDIFF vector evaluators.

use super::super::{vec_eval, ScalarFunction};
use super::week::invalid_date;
use crate::expression::Expression;
use crate::{Columns, EvalError};
use std::borrow::Cow;
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{Datum, EvalType, MySqlDuration, Time, TimeType, TimestampInterval};
use tidb_proto::tipb::ScalarFuncSig;

fn literal_string(expression: &Expression) -> Option<Option<&[u8]>> {
    let Expression::Constant(constant) = expression else {
        return None;
    };
    match constant.literal_value()? {
        value @ (Datum::String(_) | Datum::Bytes(_)) => Some(Some(value.go_bytes())),
        Datum::Null => Some(None),
        _ => None,
    }
}

fn interval(unit: &[u8]) -> Option<TimestampInterval> {
    use TimestampInterval::*;
    Some(match unit {
        b"YEAR" => Year,
        b"QUARTER" => Quarter,
        b"MONTH" => Month,
        b"WEEK" => Week,
        b"DAY" => Day,
        b"HOUR" => Hour,
        b"MINUTE" => Minute,
        b"SECOND" => Second,
        b"MICROSECOND" => Microsecond,
        _ => return None,
    })
}

fn invalid_dates(ctx: &dyn Columns, left: Time, right: Time) -> Result<(), EvalError> {
    // Go handles both arguments, in order, and returns the last error.
    let mut error = Ok(());
    if left.invalid_zero() {
        error = invalid_date(ctx, left);
    }
    if right.invalid_zero() {
        error = invalid_date(ctx, right);
    }
    error
}

#[inline]
fn project(
    ctx: &dyn Columns,
    left: [u8; 8],
    right: [u8; 8],
    compute: impl FnOnce(tidb_datatype::CoreTime, tidb_datatype::CoreTime) -> i64,
) -> Result<Option<i64>, EvalError> {
    let left = Time::from_go_raw_like_go(u64::from_ne_bytes(left));
    let right = Time::from_go_raw_like_go(u64::from_ne_bytes(right));
    if left.invalid_zero() || right.invalid_zero() {
        invalid_dates(ctx, left, right)?;
        return Ok(None);
    }
    Ok(Some(compute(left.core_time(), right.core_time())))
}

pub(in crate::scalar_function) fn date_diff_row(
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let [Datum::Time(left), Datum::Time(right)] = values else {
        return crate::time_fn::calendar::date_diff(values);
    };
    if left.invalid_zero() || right.invalid_zero() {
        invalid_dates(ctx, *left, *right)?;
        return Ok(Datum::Null);
    }
    Ok(Datum::Int(i64::from(
        left.core_time().date_diff(right.core_time()),
    )))
}

pub(in crate::scalar_function) fn timestamp_diff_row(
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let [unit @ (Datum::String(_) | Datum::Bytes(_)), Datum::Time(left), Datum::Time(right)] =
        values
    else {
        return crate::time_fn::calendar::timestamp_diff(values);
    };
    if left.invalid_zero() || right.invalid_zero() {
        invalid_dates(ctx, *left, *right)?;
        return Ok(Datum::Null);
    }
    Ok(Datum::Int(interval(unit.go_bytes()).map_or(0, |unit| {
        left.core_time().timestamp_diff(right.core_time(), unit)
    })))
}

pub(super) fn difference(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    if function.pb_code() == ScalarFuncSig::DateDiff {
        return vec_eval::with_argument_pair(
            &function.args[0],
            &function.args[1],
            EvalType::Datetime,
            ctx,
            input,
            |left, right| write_difference::<false>(ctx, input, result, left, right, None, None),
        );
    }
    let literal = literal_string(&function.args[0]);
    let units = if literal.is_none() {
        Some(vec_eval::read_argument(
            &function.args[0],
            EvalType::String,
            ctx,
            input,
        )?)
    } else {
        None
    };
    vec_eval::with_argument_pair(
        &function.args[1],
        &function.args[2],
        EvalType::Datetime,
        ctx,
        input,
        |left, right| write_difference::<true>(ctx, input, result, left, right, units, literal),
    )
}

fn write_difference<const TIMESTAMP: bool>(
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    left: vec_eval::ReadArgument<'_>,
    right: vec_eval::ReadArgument<'_>,
    units: Option<vec_eval::ReadArgument<'_>>,
    literal: Option<Option<&[u8]>>,
) -> Result<(), EvalError> {
    result.resize_int64(input.num_rows(), literal == Some(None));
    if literal == Some(None) {
        return Ok(());
    }
    if input.sel().is_some() {
        let (left_cells, right_cells) = (left.cells(), right.cells());
        let (left, right) = (left_cells.fixed::<8>(), right_cells.fixed::<8>());
        let units = units.as_ref().map(|units| units.cells());
        let literal_interval = literal.flatten().and_then(interval);
        return result.with_fixed_cells_mut(|output, bitmap| {
            for (row, dest) in output.iter_mut().enumerate() {
                let unit = if let Some(units) = &units {
                    let Some(unit) = units.bytes(row) else {
                        bitmap[row >> 3] &= !(1 << (row & 7));
                        continue;
                    };
                    interval(unit)
                } else {
                    literal_interval
                };
                let value = match (left(row), right(row)) {
                    (Some(&left), Some(&right)) => project(ctx, left, right, |left, right| {
                        if TIMESTAMP {
                            unit.map_or(0, |unit| left.timestamp_diff(right, unit))
                        } else {
                            i64::from(left.date_diff(right))
                        }
                    })?,
                    _ => None,
                };
                if let Some(value) = value {
                    *dest = value.to_ne_bytes();
                } else {
                    bitmap[row >> 3] &= !(1 << (row & 7));
                }
            }
            Ok(())
        });
    }
    let (left, _) = left.source();
    let (right, _) = right.source();
    let units = units.as_ref().map(|units| units.source().0);
    if let Some(units) = &units {
        result.merge_nulls(&[left, right, units]);
    } else {
        result.merge_nulls(&[left, right]);
    }

    let left = left.raw_cells();
    let right = right.raw_cells();
    let units = units.as_ref().map(|column| column.raw_cells());
    let literal_interval = literal.flatten().and_then(interval);
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, (dest, (&left, &right))) in output
            .iter_mut()
            .zip(left.fixed_cells().iter().zip(right.fixed_cells()))
            .enumerate()
        {
            let bit = 1 << (row & 7);
            let mask = &mut bitmap[row >> 3];
            if *mask & bit == 0 {
                continue;
            }
            let value = project(ctx, left, right, |left, right| {
                if TIMESTAMP {
                    let unit = units
                        .as_ref()
                        .map_or(literal_interval, |units| interval(units.get(row)));
                    unit.map_or(0, |unit| left.timestamp_diff(right, unit))
                } else {
                    i64::from(left.date_diff(right))
                }
            })?;
            if let Some(value) = value {
                *dest = value.to_ne_bytes();
            } else {
                *mask &= !bit;
            }
        }
        Ok(())
    })
}

pub(super) fn utf8_layout(layout: &[u8]) -> Cow<'_, str> {
    match std::str::from_utf8(layout) {
        Ok(layout) => Cow::Borrowed(layout),
        Err(_) => Cow::Owned(tidb_goruntime::GoString::from_bytes(layout).to_utf8_lossy_go()),
    }
}

fn format_date(
    date: Time,
    layout: &str,
    ctx: &dyn Columns,
    output: &mut String,
) -> Result<bool, EvalError> {
    if layout == "0" {
        output.push('0');
        return Ok(true);
    }
    if date.invalid_zero() {
        let core = date.core_time();
        let numeric_zero = date.is_zero()
            || date.kind() == TimeType::Date
                && core.year() == 0
                && core.month() == 0
                && core.day() == 0;
        if date.fsp() != 0 || !numeric_zero {
            invalid_date(ctx, date)?;
        }
        return Ok(false);
    }
    date.append_date_format(layout, output).map_err(|_| {
        EvalError::TruncatedWrongValue(format!(
            "Incorrect time value: '{}'",
            date.core_time().month()
        ))
    })?;
    Ok(true)
}

pub(in crate::scalar_function) fn format_row(
    values: &[Datum],
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let [date, layout] = values else {
        return Err(EvalError::WrongParameterCount("date_format"));
    };
    if let (Datum::Time(date), layout @ (Datum::String(_) | Datum::Bytes(_))) = (date, layout) {
        let mut output = String::new();
        let layout = utf8_layout(layout.go_bytes());
        if format_date(*date, &layout, ctx, &mut output)? {
            return Ok(Datum::new_string_datum(output));
        }
        return Ok(Datum::Null);
    }
    crate::time_fn::calendar::date_format_in(date, layout, Some(ctx))
}

pub(super) fn format(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let dates = vec_eval::read_argument(&function.args[0], EvalType::Datetime, ctx, input)?;
    let literal = literal_string(&function.args[1]).map(|layout| layout.map(utf8_layout));
    let layouts = if literal.is_none() {
        Some(vec_eval::read_argument(
            &function.args[1],
            EvalType::String,
            ctx,
            input,
        )?)
    } else {
        None
    };

    let dates = dates.cells();
    let dates = dates.fixed::<8>();
    let layouts = layouts.as_ref().map(|column| column.cells());
    let mut scratch = String::new();
    let rows = input.num_rows();
    result.reserve_string(rows);
    result.try_append_byte_rows(rows, |row, output| {
        let layout = match &literal {
            Some(layout) => layout.as_deref().map(Cow::Borrowed),
            None => layouts
                .as_ref()
                .and_then(|layouts| layouts.bytes(row))
                .map(utf8_layout),
        };
        let (Some(&raw), Some(layout)) = (dates(row), layout) else {
            return Ok(false);
        };
        let date = Time::from_go_raw_like_go(u64::from_ne_bytes(raw));
        scratch.clear();
        if !format_date(date, &layout, ctx, &mut scratch)? {
            return Ok(false);
        }
        output.append(scratch.as_bytes());
        Ok(true)
    })
}

pub(super) fn time_format(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let durations = match vec_eval::read_argument(&function.args[0], EvalType::Duration, ctx, input)
    {
        Ok(column) => column,
        Err(_) => return super::super::builtin_control_vec::fallback(function, ctx, input, result),
    };
    let layouts = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
    let durations = durations.cells();
    let durations = durations.fixed::<8>();
    let layouts = layouts.cells();
    let mut scratch = String::new();
    let rows = input.num_rows();
    result.reserve_string(rows);
    result.try_append_byte_rows(rows, |row, output| {
        let (Some(&nanos), Some(layout)) = (durations(row), layouts.bytes(row)) else {
            return Ok(false);
        };
        if layout.is_empty() {
            return Ok(false);
        }
        scratch.clear();
        MySqlDuration::from_raw_parts(i64::from_ne_bytes(nanos), 0)
            .append_duration_format(&utf8_layout(layout), &mut scratch);
        output.append(scratch.as_bytes());
        Ok(true)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::Column as ExpressionColumn, evaluator::EvaluatorSuite,
        new_function::new_function_base, NoColumns,
    };
    use tidb_datatype::{CoreTime, FieldType, FieldTypeCode};

    #[test]
    fn timestamp_diff_uses_go_units_in_rows_and_selected_batches() {
        let text = FieldType::new(FieldTypeCode::VarString);
        let datetime = FieldType::new(FieldTypeCode::Datetime);
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut input =
            Chunk::new_chunk_with_capacity(&[text.clone(), datetime.clone(), datetime.clone()], 3);
        let time = |day| {
            Time::new(
                CoreTime::from_date(2024, 2, day, 0, 0, 0, 0),
                TimeType::DateTime,
                0,
            )
            .unwrap()
        };
        for unit in ["DAY", "day", "unknown"] {
            input.append_string(0, unit);
            input.append_time(1, time(28));
            input.append_time(2, time(29));
        }
        let args = [text, datetime.clone(), datetime]
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                let mut column = ExpressionColumn::new(index as i64 + 1, field);
                column.index = index as i64;
                Expression::Column(column)
            })
            .collect();
        let expr = new_function_base(&NoColumns, "timestampdiff", integer.clone(), args).unwrap();
        assert_eq!(
            expr.eval(&NoColumns, input.get_row(1)).unwrap(),
            Datum::Int(0)
        );
        input.set_sel(Some(vec![1, 0, 2, 1]));
        let suite = EvaluatorSuite::new(vec![expr], false);
        let mut output = Chunk::new_chunk_with_capacity(&[integer], 4);
        suite.run(&NoColumns, &mut input, &mut output).unwrap();
        let column = output.column(0);
        assert_eq!(output.num_rows(), 4);
        assert_eq!(
            (0..output.num_rows())
                .map(|row| column.get_int64(row))
                .collect::<Vec<_>>(),
            [0, 1, 0, 0]
        );
    }

    #[test]
    fn date_format_reads_mask_bytes_using_go_rune_rules() {
        let date = Time::new(
            CoreTime::from_date(2024, 2, 29, 0, 0, 0, 0),
            TimeType::DateTime,
            0,
        )
        .unwrap();
        let value = format_row(
            &[
                Datum::Time(date),
                Datum::new_string_datum(b"%Y %\xf0\x9f%".to_vec()),
            ],
            &NoColumns,
        )
        .unwrap();
        assert_eq!(value.go_bytes(), "2024 ��".as_bytes());
    }
}
