// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{vec_eval, ScalarFunction};
use crate::{time_fn, Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{EvalType, MyDecimal, RoundMode, SessionTimeZone, Time};
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn from_days(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::with_argument_column(
        &function.args[0],
        EvalType::Int,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let values = cells.fixed_cells::<8>();
            let convert = |row| {
                (!cells.is_null(row)).then(|| {
                    let day = i64::from_ne_bytes(values[row]);
                    // Go's vector signature accepts the whole TimeFromDays
                    // interval, including the narrow band in year 10000.
                    tidb_datatype::time_from_days(day).go_raw() as i64
                })
            };
            if let Some(selection) = selection {
                result.write_int64_rows(selection.iter().copied().map(convert));
            } else {
                result.write_int64_rows((0..column.rows()).map(convert));
            }
        },
    )
}

pub(super) fn makedate(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    binary_int(function, ctx, input, result, |year, day| {
        Ok(time_fn::makedate_int(year, day).map(|time| time.go_raw().to_ne_bytes()))
    })
}

pub(super) fn unix_timestamp(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Option<Result<(), EvalError>> {
    let zone = ctx.time_zone();
    if matches!(zone, SessionTimeZone::Local) {
        return None;
    }
    let first = &function.args[0];
    if first.static_type()?.eval_type() != EvalType::Datetime {
        return None;
    }
    let fsp = u32::try_from(function.get_static_type()?.decimal()).ok()?;
    if fsp > 6 {
        return None;
    }
    let integer_result = function.pb_code() == ScalarFuncSig::UnixTimestampInt;
    let decoded = vec_eval::with_argument_column(
        first,
        EvalType::Datetime,
        ctx,
        input,
        |column, selection| {
            let cells = column.raw_cells();
            let values = cells.fixed_cells::<8>();
            let micros = |row| {
                let core = Time::from_go_raw_like_go(u64::from_ne_bytes(values[row])).core_time();
                match &zone {
                    SessionTimeZone::Fixed { offset_secs, .. } => {
                        time_fn::session_tz::fixed_unix_micros(core, *offset_secs)
                    }
                    SessionTimeZone::Named(tz) => {
                        time_fn::session_tz::named_unix_micros(core, tz, integer_result)
                    }
                    SessionTimeZone::Local => unreachable!("Local zone uses the row fallback"),
                }
            };
            if integer_result {
                let convert = |row| (!cells.is_null(row)).then(|| micros(row) / 1_000_000);
                if let Some(selection) = selection {
                    result.write_int64_rows(selection.iter().copied().map(convert));
                } else {
                    result.write_int64_rows((0..column.rows()).map(convert));
                }
            } else {
                let divisor = 10_i64.pow(6 - fsp);
                result.resize_decimal(input.num_rows(), false);
                result.with_fixed_cells_mut(|output, bitmap| {
                    for (row, output) in output.iter_mut().enumerate() {
                        let source = selection.map_or(row, |selection| selection[row]);
                        if cells.is_null(source) {
                            bitmap[row / 8] &= !(1 << (row % 8));
                        } else {
                            let micros = micros(source);
                            if micros != 0 {
                                let mut decimal = if micros % divisor == 0 {
                                    MyDecimal::from_scaled_i128(
                                        i128::from(micros / divisor),
                                        fsp,
                                        fsp,
                                    )
                                } else {
                                    MyDecimal::from_scaled_i128(i128::from(micros), 6, 6)
                                }
                                .expect("valid Unix timestamp fits MyDecimal");
                                // Shift discards trailing zeroes before Go's Round.
                                // When truncating significant digits, Round also
                                // updates the packed integer digit count.
                                if micros % divisor != 0 {
                                    let error =
                                        decimal.round_in_place(fsp as i32, RoundMode::Truncate);
                                    debug_assert!(error.is_none());
                                }
                                *output = decimal.to_raw_bytes();
                            }
                        }
                    }
                });
            }
        },
    );
    // Go falls back to scalar evaluation only when the first argument fails.
    Some(
        decoded
            .or_else(|_| super::super::builtin_control_vec::fallback(function, ctx, input, result)),
    )
}

pub(super) fn period(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    if function.pb_code() == ScalarFuncSig::PeriodAdd
        || function.func_name.lowercase() == "period_add"
    {
        binary_int(function, ctx, input, result, |left, right| {
            time_fn::period_add_int(left, right).map(|value| Some(value.to_ne_bytes()))
        })
    } else {
        binary_int(function, ctx, input, result, |left, right| {
            time_fn::period_diff_int(left, right).map(|value| Some(value.to_ne_bytes()))
        })
    }
}

fn binary_int(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    map: impl Fn(i64, i64) -> Result<Option<[u8; 8]>, EvalError>,
) -> Result<(), EvalError> {
    vec_eval::with_argument_pair(
        &function.args[0],
        &function.args[1],
        EvalType::Int,
        ctx,
        input,
        |left, right| vec_eval::write_fixed_pair(left, right, result, input.num_rows(), map),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::Column as ExpressionColumn, evaluator::EvaluatorSuite, expression::Expression,
        new_function::new_function_base, NoColumns,
    };
    use tidb_datatype::{CoreTime, Datum, FieldType, FieldTypeCode, TimeType};

    #[test]
    fn from_days_preserves_go_vector_year_10000() {
        let integer = FieldType::new(FieldTypeCode::LongLong);
        let date = FieldType::new(FieldTypeCode::Date);
        let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&integer), 3);
        input.append_int64(0, 3_652_425);
        input.append_int64(0, 3_652_500);
        input.append_null(0);
        input.set_sel(Some(vec![2, 0, 1, 0]));
        let mut arg = ExpressionColumn::new(1, integer);
        arg.index = 0;
        let expr = new_function_base(
            &NoColumns,
            "from_days",
            date.clone(),
            vec![Expression::Column(arg)],
        )
        .unwrap();
        let mut output = Chunk::new_chunk_with_capacity(&[date], 4);
        EvaluatorSuite::new(vec![expr], false)
            .run(&NoColumns, &mut input, &mut output)
            .unwrap();
        let result = output.column(0);
        assert!(result.is_null(0));
        let date = CoreTime::from_date(10_000, 1, 1, 0, 0, 0, 0);
        assert_eq!(result.get_time(1).core_time(), date);
        assert_eq!(result.get_time(2).core_time(), CoreTime::default());
        assert_eq!(result.get_time(3).core_time(), date);
    }

    #[test]
    fn unix_timestamp_decimal_packs_go_zero_and_applies_fixed_offset() {
        struct FixedOffset;
        impl Columns for FixedOffset {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn time_zone(&self) -> SessionTimeZone {
                SessionTimeZone::Fixed {
                    name: "+05:30".to_owned(),
                    offset_secs: 19_800,
                }
            }
        }

        for (fsp, coefficient, digits_int) in [(6, 1_123_456, 1), (3, 1_123, 2)] {
            let mut datetime = FieldType::new(FieldTypeCode::Datetime);
            datetime.set_decimal(fsp);
            let decimal = FieldType::new(FieldTypeCode::NewDecimal);
            let mut input = Chunk::new_chunk_with_capacity(std::slice::from_ref(&datetime), 3);
            for second in [0, 1] {
                let core = CoreTime::from_date(1970, 1, 1, 5, 30, second, 123_456);
                input.append_time(0, Time::new(core, TimeType::DateTime, fsp).unwrap());
            }
            input.append_null(0);
            let mut arg = ExpressionColumn::new(1, datetime);
            arg.index = 0;
            let expr = new_function_base(
                &FixedOffset,
                "unix_timestamp",
                decimal.clone(),
                vec![Expression::Column(arg)],
            )
            .unwrap();
            let mut output = Chunk::new_chunk_with_capacity(&[decimal], 3);
            EvaluatorSuite::new(vec![expr], false)
                .run(&FixedOffset, &mut input, &mut output)
                .unwrap();
            let result = output.column(0);
            assert_eq!(result.get_raw(0), [0; 40]);
            let mut expected = MyDecimal::from_scaled_i128(coefficient, fsp as u32, fsp as u32)
                .unwrap()
                .to_raw_bytes();
            expected[0] = digits_int;
            assert_eq!(result.get_raw(1), expected);
            assert!(result.is_null(2));
        }
    }
}
