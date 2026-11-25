// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

use super::{projections::utf8_layout, vec_eval, ScalarFunction};
use crate::expression::Expression;
use crate::scalar_function::Kernel;
use crate::time_fn::{convert_tz as tz, session_tz};
use crate::{Columns, EvalError};
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{types_errors, EvalType, FieldTypeCode, MyDecimal, SessionTimeZone, Time};
use tidb_proto::tipb::ScalarFuncSig;

pub(super) fn convert_tz(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    vec_eval::eval_into(&function.args[0], EvalType::Datetime, ctx, input, result)?;
    let from = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
    let to = vec_eval::read_argument(&function.args[2], EvalType::String, ctx, input)?;
    let from = from.cells();
    let to = to.cells();
    let fsp = function
        .get_static_type()
        .expect("CONVERT_TZ type")
        .decimal();
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            let bit = 1 << (row & 7);
            let presence = &mut bitmap[row >> 3];
            if *presence & bit == 0 {
                continue;
            }
            if let (Some(from), Some(to)) = (from.bytes(row), to.bytes(row)) {
                let from = std::str::from_utf8(from).ok().and_then(tz::parse_conv_tz);
                let to = std::str::from_utf8(to).ok().and_then(tz::parse_conv_tz);
                if let (Some(from), Some(to)) = (from, to) {
                    let time = Time::from_go_raw_like_go(u64::from_ne_bytes(*output));
                    if let Some(time) = tz::convert_packed(time, &from, &to, fsp) {
                        *output = time.go_raw().to_ne_bytes();
                        continue;
                    }
                }
            }
            *presence &= !bit;
        }
        Ok(())
    })
}

pub(super) fn from_unixtime(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let fsp = function
        .get_static_type()
        .expect("FROM_UNIXTIME type")
        .decimal();
    let fsp = if fsp < 0 { 6 } else { fsp.min(6) };
    let zone = ctx.time_zone();
    let rows = input.num_rows();
    if function.pb_code() == ScalarFuncSig::FromUnixTime1Arg {
        if let Some(arg) = lossless_bigint_cast(&function.args[0]) {
            return vec_eval::with_argument_column(
                arg,
                EvalType::Int,
                ctx,
                input,
                |column, selection| {
                    let cells = column.raw_cells();
                    let values = cells.fixed_cells::<8>();
                    let convert = |row| {
                        if cells.is_null(row) {
                            return None;
                        }
                        let secs = i64::from_ne_bytes(values[row]);
                        session_tz::time_from_epoch(secs, 0, fsp, &zone)
                            .map(|time| time.go_raw() as i64)
                    };
                    if let Some(selection) = selection {
                        result.write_int64_rows(selection.iter().copied().map(convert));
                    } else {
                        result.write_int64_rows((0..column.rows()).map(convert));
                    }
                },
            );
        }
    }
    let values = vec_eval::read_argument(&function.args[0], EvalType::Decimal, ctx, input)?;
    let values = values.cells();
    let values = values.fixed::<40>();
    if function.pb_code() == ScalarFuncSig::FromUnixTime2Arg {
        let layout = vec_eval::read_argument(&function.args[1], EvalType::String, ctx, input)?;
        let layout = layout.cells();
        let mut scratch = String::new();
        result.reserve_string(rows);
        return result.try_append_byte_rows(rows, |row, output| {
            let (Some(value), Some(layout)) = (values(row), layout.bytes(row)) else {
                return Ok(false);
            };
            let Some(time) = epoch_decimal(value, fsp, &zone)? else {
                return Ok(false);
            };
            scratch.clear();
            time.append_date_format(&utf8_layout(layout), &mut scratch)
                .map_err(|_| EvalError::Unsupported("FROM_UNIXTIME format"))?;
            output.append(scratch.as_bytes());
            Ok(true)
        });
    }
    result.resize_time(rows, false);
    result.with_fixed_cells_mut(|output, bitmap| {
        for (row, output) in output.iter_mut().enumerate() {
            if let Some(value) = values(row) {
                if let Some(time) = epoch_decimal(value, fsp, &zone)? {
                    *output = time.go_raw().to_ne_bytes();
                    continue;
                }
            }
            bitmap[row / 8] &= !(1 << (row % 8));
        }
        Ok(())
    })
}

/// The standard BIGINT-to-DECIMAL wrapper introduces neither diagnostics
/// nor scale changes. A wrapped unsigned value above i64::MAX is outside
/// FROM_UNIXTIME's range under either interpretation.
fn lossless_bigint_cast(arg: &Expression) -> Option<&Expression> {
    let Expression::ScalarFunction(cast) = arg else {
        return None;
    };
    if !matches!(
        cast.pb_builtin.kernel,
        Kernel::Cast {
            source: EvalType::Int,
            target: EvalType::Decimal,
            in_union: false
        }
    ) {
        return None;
    }
    let input = &cast.args[0];
    let source = input.static_type()?;
    let target = cast.get_static_type()?;
    (source.code() == FieldTypeCode::LongLong
        && source.is_unsigned() == target.is_unsigned()
        && target.decimal() == 0
        && target.flen() >= 20)
        .then_some(input)
}

fn epoch_decimal(
    value: &[u8; 40],
    fsp: i64,
    zone: &SessionTimeZone,
) -> Result<Option<Time>, EvalError> {
    if value[3] != 0 {
        return Ok(None);
    }
    let integer_words = usize::from(value[0]).div_ceil(9);
    if integer_words <= 2 && value[1] <= 18 {
        // One MyDecimal word is nine decimal digits. Its first fractional
        // word already has the scale of a Go nanosecond count.
        let words = value[4..].as_chunks::<4>().0;
        let read = |idx| i64::from(i32::from_ne_bytes(words[idx]));
        let secs = match integer_words {
            0 => 0,
            1 => read(0),
            _ => read(0) * 1_000_000_000 + read(1),
        };
        let nanos = if value[1] == 0 {
            0
        } else {
            read(integer_words)
        };
        return Ok(session_tz::time_from_epoch(secs, nanos, fsp, zone));
    }
    wide_epoch_decimal(value, fsp, zone)
}

#[cold]
fn wide_epoch_decimal(
    raw: &[u8; 40],
    fsp: i64,
    zone: &SessionTimeZone,
) -> Result<Option<Time>, EvalError> {
    let value = MyDecimal::from_raw_bytes_like_go(*raw);
    let (secs, _) = value.to_int();
    if !(0..=session_tz::MAX_UNIX_SECS).contains(&secs) {
        return Ok(None);
    }
    let words = raw[4..].as_chunks::<4>().0;
    let integer_words = usize::from(raw[0]).div_ceil(9);
    let fraction_words = usize::from(raw[1]).div_ceil(9);
    let fraction = &words[integer_words..integer_words + fraction_words];
    // Go DecimalSub keeps the significant integer word count of the
    // minuend, unless the result is zero. DecimalMul reserves two more
    // words for its multiplier (1e9) even though the sum fits in one word.
    let total_words = if fraction.iter().all(|word| *word == [0; 4]) {
        usize::from(raw[2]).div_ceil(9) + 2
    } else {
        let significant_words = if secs == 0 {
            0
        } else if secs < 1_000_000_000 {
            1
        } else {
            2
        };
        significant_words + fraction_words + 2
    };
    if total_words > words.len() {
        return Err(EvalError::Conversion(types_errors::ERR_TRUNCATED.clone()));
    }
    let nanos = fraction
        .first()
        .map_or(0, |word| i64::from(i32::from_ne_bytes(*word)));
    Ok(session_tz::time_from_epoch(secs, nanos, fsp, zone))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        column::Column as ExpressionColumn, evaluator::EvaluatorSuite,
        new_function::new_function_base, NoColumns,
    };
    use tidb_datatype::{CoreTime, Datum, FieldType, TimeType};

    struct Paris;
    impl Columns for Paris {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn time_zone(&self) -> SessionTimeZone {
            SessionTimeZone::Named(chrono_tz::Europe::Paris)
        }
    }

    fn suite(name: &str, fields: &[FieldType], output: &FieldType) -> EvaluatorSuite {
        let args = fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let mut column = ExpressionColumn::new(i as i64 + 1, field.clone());
                column.index = i as i64;
                Expression::Column(column)
            })
            .collect();
        EvaluatorSuite::new(
            vec![new_function_base(&NoColumns, name, output.clone(), args).unwrap()],
            false,
        )
    }

    #[test]
    fn unix_timestamp_named_gap_preserves_both_go_signature_outcomes() {
        for (fsp, kind) in [(0, FieldTypeCode::LongLong), (6, FieldTypeCode::NewDecimal)] {
            let mut date = FieldType::new(FieldTypeCode::Datetime);
            date.set_decimal(fsp);
            let fields = [date];
            let output_type = FieldType::new(kind);
            let mut input = Chunk::new_chunk_with_capacity(&fields, 1);
            let wall = CoreTime::from_date(2025, 3, 30, 2, 30, 0, 0);
            input.append_time(0, Time::new(wall, TimeType::DateTime, fsp).unwrap());
            let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&output_type), 1);
            suite("unix_timestamp", &fields, &output_type)
                .run(&Paris, &mut input, &mut output)
                .unwrap();
            let result = output.column(0);
            assert!(!result.is_null(0));
            if fsp == 0 {
                assert_eq!(result.get_int64(0), 1_743_296_400);
            } else {
                assert_eq!(result.get_raw(0), [0; 40]);
            }
        }
    }

    #[test]
    fn convert_tz_packs_exact_gap_boundary_and_uses_future_zone_rules() {
        let mut date = FieldType::new(FieldTypeCode::Datetime);
        date.set_decimal(6);
        let zone = FieldType::new(FieldTypeCode::VarString);
        let fields = [date.clone(), zone.clone(), zone];
        let mut input = Chunk::new_chunk_with_capacity(&fields, 2);
        for core in [
            CoreTime::from_date(2025, 3, 30, 2, 30, 0, 987_654),
            CoreTime::from_date(2200, 7, 8, 12, 0, 0, 123_456),
        ] {
            input.append_time(0, Time::new(core, TimeType::DateTime, 6).unwrap());
            input.append_string(1, "Europe/Paris");
            input.append_string(2, "UTC");
        }
        let mut output = Chunk::new_chunk_with_capacity(std::slice::from_ref(&date), 2);
        suite("convert_tz", &fields, &date)
            .run(&Paris, &mut input, &mut output)
            .unwrap();
        let result = output.column(0);
        assert_eq!(
            result.get_time(0).core_time(),
            CoreTime::from_date(2025, 3, 30, 1, 0, 0, 0)
        );
        assert_eq!(
            result.get_time(1).core_time(),
            CoreTime::from_date(2200, 7, 8, 10, 0, 0, 123_456)
        );
    }

    #[test]
    fn from_unixtime_wide_fraction_preserves_go_decimal_mul_truncation() {
        let source = format!("1.{}9", "4".repeat(61));
        let (value, error) = MyDecimal::from_string(source.as_bytes());
        assert!(error.is_none());
        let error = epoch_decimal(&value.to_raw_bytes(), 6, &SessionTimeZone::utc()).unwrap_err();
        assert!(matches!(error, EvalError::Conversion(error) if error.code().value() == 1265));
    }
}
