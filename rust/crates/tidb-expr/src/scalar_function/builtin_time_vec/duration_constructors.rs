// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{ScalarFunction, vec_eval};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{EvalType, MAX_TIME_NANOS, TIME_MAX_HOUR, parse_frac};
use tidb_goruntime::go_strconv::append_float;
use tidb_proto::tipb::ScalarFuncSig;

const NANOS_PER_SECOND: i64 = 1_000_000_000;

pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Option<Result<(), EvalError>> {
    let sig = function.pb_code();
    let fsp = function.get_static_type()?.decimal();
    if !(0..=6).contains(&fsp) {
        return None;
    }
    if sig == ScalarFuncSig::SecToTime {
        return Some(sec_to_time(function, ctx, input, result, fsp));
    }
    let hour = function.args[0].static_type()?;
    if hour.eval_type() != EvalType::Int
        || function.args[1].static_type()?.eval_type() != EvalType::Int
    {
        return None;
    }
    Some(maketime(
        function,
        ctx,
        input,
        result,
        fsp,
        hour.is_unsigned(),
    ))
}

fn sec_to_time(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    fsp: i64,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Real,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let values = cells.fixed_cells::<8>();
            let mut text = Vec::with_capacity(24);
            result.resize_go_duration(input.num_rows(), false);
            result.with_fixed_cells_mut(|output, bitmap| {
                for (row, output) in output.iter_mut().enumerate() {
                    let source = selection.map_or(row, |selection| selection[row]);
                    if cells.is_null(source) {
                        bitmap[row / 8] &= !(1 << (row % 8));
                        continue;
                    }
                    let seconds = f64::from_ne_bytes(values[source]);
                    let negative = seconds < 0.0;
                    let magnitude = seconds.abs();
                    let whole = magnitude as u64;
                    if whole / 3600 > TIME_MAX_HOUR as u64 {
                        crate::time_fn::sec_to_time_overflow(magnitude, ctx)?;
                        *output = (if negative {
                            -MAX_TIME_NANOS
                        } else {
                            MAX_TIME_NANOS
                        })
                        .to_ne_bytes();
                        continue;
                    }
                    let second = (whole % 60) as f64 + (magnitude - whole as f64);
                    let nanos =
                        rounded_duration((whole / 60 * 60) as i64, second, fsp, b'f', &mut text)
                            .ok_or_else(|| {
                                let sign = if negative { "-" } else { "" };
                                invalid_duration(
                                    format_args!("{sign}{:02}", whole / 3600),
                                    (whole / 60 % 60) as i64,
                                    &text,
                                )
                            })?;
                    *output = (if negative { -nanos } else { nanos }).to_ne_bytes();
                }
                Ok(())
            })
        },
    )?
}

fn maketime(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    fsp: i64,
    hour_unsigned: bool,
) -> Result<(), EvalError> {
    vec_eval::with_argument_pair(
        &function.args[0],
        &function.args[1],
        EvalType::Int,
        ctx,
        input,
        |hours, minutes| {
            let seconds = vec_eval::read_argument(&function.args[2], EvalType::Real, ctx, input)?;
            let (hours, minutes, seconds) = (hours.cells(), minutes.cells(), seconds.cells());
            let (hours, minutes, seconds) = (
                hours.fixed::<8>(),
                minutes.fixed::<8>(),
                seconds.fixed::<8>(),
            );
            let mut text = Vec::with_capacity(24);
            result.resize_go_duration(input.num_rows(), false);
            result.with_fixed_cells_mut(|output, bitmap| {
                for (row, output) in output.iter_mut().enumerate() {
                    if let (Some(&hour), Some(&minute), Some(&second)) =
                        (hours(row), minutes(row), seconds(row))
                    {
                        let hour = i64::from_ne_bytes(hour);
                        let minute = i64::from_ne_bytes(minute);
                        let second = f64::from_ne_bytes(second);
                        // Ordered comparisons match Go, including NaN reaching the parser.
                        if (0..60).contains(&minute) && !(second < 0.0 || second >= 60.0) {
                            let negative = hour < 0 && !hour_unsigned;
                            let magnitude = hour.unsigned_abs();
                            let nanos = if (hour_unsigned && hour < 0)
                                || magnitude > TIME_MAX_HOUR as u64
                                || (magnitude == TIME_MAX_HOUR as u64
                                    && minute == 59
                                    && second > 59.0)
                            {
                                MAX_TIME_NANOS
                            } else {
                                rounded_duration(
                                    magnitude as i64 * 3600 + minute * 60,
                                    second,
                                    fsp,
                                    b'g',
                                    &mut text,
                                )
                                .ok_or_else(|| {
                                    invalid_duration(format_args!("{hour:02}"), minute, &text)
                                })?
                            };
                            *output = (if negative { -nanos } else { nanos }).to_ne_bytes();
                            continue;
                        }
                    }
                    bitmap[row / 8] &= !(1 << (row % 8));
                }
                Ok(())
            })
        },
    )
}

/// Both Go signatures format the seconds field before rounding its decimal
/// fraction. Keep the same spelling and carry without reparsing hours/minutes.
fn rounded_duration(
    base: i64,
    second: f64,
    fsp: i64,
    format: u8,
    text: &mut Vec<u8>,
) -> Option<i64> {
    text.clear();
    append_float(text, second, format, -1, 64);
    let dot = text.iter().position(|&b| b == b'.');
    let (whole, fraction) =
        dot.map_or((text.as_slice(), &[][..]), |i| (&text[..i], &text[i + 1..]));
    if !whole.iter().chain(fraction).all(u8::is_ascii_digit) {
        return None;
    }
    let second = whole
        .iter()
        .fold(0, |n, digit| n * 10 + i64::from(digit - b'0'));
    let (micros, carry) = parse_frac(fraction, fsp).ok()?;
    let nanos = (base + second + i64::from(carry)) * NANOS_PER_SECOND + micros * 1000;
    (nanos <= MAX_TIME_NANOS).then_some(nanos)
}

#[cold]
fn invalid_duration(hour: std::fmt::Arguments<'_>, minute: i64, seconds: &[u8]) -> EvalError {
    let seconds = std::str::from_utf8(seconds).expect("formatted float is ASCII");
    EvalError::Conversion(
        tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL.generate(format!(
            "Truncated incorrect time value: '{hour}:{minute:02}:{seconds}'"
        )),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        EvalErrorSql, NoColumns,
        column::Column as ExpressionColumn,
        evaluator::{EvaluatorError, EvaluatorSuite},
        expression::Expression,
        new_function::new_function_base,
    };
    use tidb_datatype::{FieldType, FieldTypeCode};

    #[test]
    fn sec_to_time_carries_rounded_seconds_across_minutes() {
        let mut real = FieldType::new(FieldTypeCode::Double);
        real.set_decimal(6);
        let duration = FieldType::new(FieldTypeCode::Duration);
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&real), 3);
        input.append_float64(0, 59.999_999_5);
        input.append_float64(0, -3_599.999_999_6);
        input.append_null(0);
        input.set_sel(Some(vec![2, 1, 0, 0]));
        let mut argument = ExpressionColumn::new(1, real);
        argument.index = 0;
        let expression = new_function_base(
            &NoColumns,
            "sec_to_time",
            duration.clone(),
            vec![Expression::Column(argument)],
        )
        .unwrap();
        let mut output = Chunk::new_chunk_with_capacity(&[duration], 4);
        EvaluatorSuite::new(vec![expression], false)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        let result = output.column(0);
        assert!(result.is_null(0));
        assert_eq!(result.get_int64(1), -3_600_000_000_000);
        assert_eq!(result.get_int64(2), 60_000_000_000);
        assert_eq!(result.get_int64(3), 60_000_000_000);
    }

    #[test]
    fn sec_to_time_clips_go_overflow_diagnostic() {
        struct Strict;
        impl Columns for Strict {
            fn get(&self, _: &[String]) -> Option<tidb_datatype::Datum> {
                None
            }

            fn truncate_level(&self) -> crate::ErrorLevel {
                crate::ErrorLevel::Error
            }
        }

        let error = crate::time_fn::sec_to_time_overflow(1e300, &Strict)
            .unwrap_err()
            .to_sql_error();
        assert_eq!(error.code, 1292);
        assert_eq!(
            error.message,
            format!("Truncated incorrect time value: '1{}'", "0".repeat(127))
        );
    }

    #[test]
    fn maketime_preserves_go_error_for_exponent_spelling() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let mut real = FieldType::new(FieldTypeCode::Double);
        real.set_decimal(6);
        let duration = FieldType::new(FieldTypeCode::Duration);
        let fields = [integer.clone(), integer, real];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
        input.append_int64(0, -1);
        input.append_int64(1, 0);
        input.append_float64(2, 0.000_000_5);
        let arguments = fields
            .into_iter()
            .enumerate()
            .map(|(index, field)| {
                let mut argument = ExpressionColumn::new(index as i64 + 1, field);
                argument.index = index as i64;
                Expression::Column(argument)
            })
            .collect();
        let expression =
            new_function_base(&NoColumns, "maketime", duration.clone(), arguments).unwrap();
        let mut output = Chunk::new_chunk_with_capacity(&[duration], 1);
        let error = EvaluatorSuite::new(vec![expression], false)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap_err();
        let EvaluatorError::Eval(error) = error else {
            panic!("expected SQL evaluation error, got {error:?}");
        };
        let error = error.to_sql_error();
        assert_eq!(error.code, 1292);
        assert_eq!(
            error.message,
            "Truncated incorrect time value: '-1:00:5e-07'"
        );
    }
}
