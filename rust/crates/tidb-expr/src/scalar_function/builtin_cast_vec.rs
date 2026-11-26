// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

//! Go `builtin_cast_vec.go`: typed source column followed by conversion.

use super::{Kernel, ScalarFunction, vec_eval};
use crate::{Columns, EvalError};
use std::io::Write;
use tidb_chunk::{chunk::Chunk, column::Column};
use tidb_datatype::{
    Datum, Decimal, DecimalError, EvalType, FieldType, FieldTypeCode, MYDECIMAL_STRUCT_SIZE,
    MyDecimal, RoundMode,
};

/// Go `builtinCast*As*Sig.vecEval*`.
pub(super) fn eval(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
) -> Result<(), EvalError> {
    let Kernel::Cast {
        source,
        target,
        in_union,
    } = function.pb_builtin.kernel
    else {
        unreachable!()
    };
    let arg = &function.args[0];
    if source == EvalType::String
        && matches!(target, EvalType::Int | EvalType::Real | EvalType::Decimal)
        && super::is_numeric_binary_literal(arg)
    {
        return vec_eval::eval_into(arg, target, ctx, input, result);
    }
    if source == EvalType::String
        && target == EvalType::Int
        && arg.static_type().is_some_and(|f| f.is_hybrid())
    {
        return vec_eval::eval_into(arg, EvalType::Int, ctx, input, result);
    }
    if source == EvalType::String && matches!(target, EvalType::Int | EvalType::Real)
        && matches!(arg, crate::expression::Expression::ScalarFunction(child) if child.pb_code() == tidb_proto::tipb::ScalarFuncSig::DayName) {
        return vec_eval::eval_into(arg, target, ctx, input, result);
    }
    let field = function.get_static_type().expect("cast type");
    let source_unsigned = arg.static_type().is_some_and(FieldType::is_unsigned);
    let unsigned = field.is_unsigned();
    let clamp_negative = in_union && unsigned;
    match (source, target) {
        (EvalType::Int, EvalType::Int) => {
            vec_eval::eval_into(arg, source, ctx, input, result)?;
            if clamp_negative {
                result.with_fixed_cells_mut(|cells, _| {
                    for cell in cells {
                        *cell = i64::from_ne_bytes(*cell).max(0).to_ne_bytes();
                    }
                });
            }
            return Ok(());
        }
        (EvalType::Real, EvalType::Real) => {
            vec_eval::eval_into(arg, source, ctx, input, result)?;
            if clamp_negative {
                result.map_float64_rows(|value| Some(if value < 0.0 { 0.0 } else { value }));
            }
            return Ok(());
        }
        (EvalType::String, EvalType::String) if field.flen() < 0 => {
            return vec_eval::eval_into(arg, source, ctx, input, result);
        }
        (EvalType::Int | EvalType::Real | EvalType::Decimal, EvalType::String) => {
            let argument = vec_eval::read_argument(arg, source, ctx, input)?;
            let cells = argument.cells();
            let rows = input.num_rows();
            result.reserve_string(rows);
            match source {
                EvalType::Int => {
                    let cell = cells.fixed::<8>();
                    let year = arg
                        .static_type()
                        .is_some_and(|ty| ty.code() == FieldTypeCode::Year);
                    write_text(result, rows, field, ctx, cell, |bytes, text| {
                        let number = i64::from_ne_bytes(*bytes);
                        if year && number == 0 {
                            text.extend_from_slice(b"0000");
                        } else if source_unsigned {
                            write!(text, "{}", number as u64).unwrap();
                        } else {
                            write!(text, "{number}").unwrap();
                        }
                    })?;
                }
                EvalType::Real => {
                    let cell = cells.fixed::<8>();
                    let bits = if arg
                        .static_type()
                        .is_some_and(|ty| ty.code() == FieldTypeCode::Float)
                    {
                        32
                    } else {
                        64
                    };
                    write_text(result, rows, field, ctx, cell, |bytes, text| {
                        tidb_goruntime::go_strconv::append_float(
                            text,
                            f64::from_ne_bytes(*bytes),
                            b'f',
                            -1,
                            bits,
                        );
                    })?;
                }
                EvalType::Decimal => {
                    write_text(
                        result,
                        rows,
                        field,
                        ctx,
                        cells.fixed::<MYDECIMAL_STRUCT_SIZE>(),
                        |bytes, text| {
                            decode_decimal(*bytes).append_to_string_bytes(text);
                        },
                    )?;
                }
                _ => unreachable!(),
            }
            return Ok(());
        }
        (EvalType::Int, EvalType::Real) => {
            return cast_fixed(function, ctx, input, result, |bytes, dest| {
                let value = i64::from_ne_bytes(bytes);
                let value = if clamp_negative && !source_unsigned && value < 0 {
                    0.0
                } else if source_unsigned || unsigned {
                    (value as u64) as f64
                } else {
                    value as f64
                };
                *dest = value.to_ne_bytes();
                Ok(true)
            });
        }
        (EvalType::Int, EvalType::Decimal) => {
            return cast_fixed(function, ctx, input, result, |bytes, dest| {
                let value = i64::from_ne_bytes(bytes);
                let decimal = if clamp_negative && !source_unsigned && value < 0 {
                    MyDecimal::from_uint(0)
                } else if source_unsigned || unsigned {
                    MyDecimal::from_uint(value as u64)
                } else {
                    MyDecimal::from_int(value)
                };
                *dest = produce_decimal(field, decimal, ctx)?;
                Ok(true)
            });
        }
        (EvalType::Real, EvalType::Decimal) => {
            return cast_fixed(function, ctx, input, result, |bytes, dest| {
                let value = f64::from_ne_bytes(bytes);
                let (decimal, error) = if !in_union || value >= 0.0 {
                    MyDecimal::from_float64(value)
                } else {
                    (MyDecimal::default(), None)
                };
                // Go parses Float64 into the result cell before applying the
                // target's precision and scale.
                *dest = decimal.to_raw_bytes();
                match error {
                    Some(DecimalError::Overflow) => match ctx.truncate_level() {
                        crate::ErrorLevel::Error => {
                            return Err(decimal_cast_error(DecimalError::Overflow));
                        }
                        crate::ErrorLevel::Warn => {
                            let expression = super::string_with_ctx(arg, ctx).unwrap_or_default();
                            ctx.append_go_warning(
                                &tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL,
                                &format!("Truncated incorrect DECIMAL value: '{expression}'"),
                            );
                        }
                        crate::ErrorLevel::Ignore => {}
                    },
                    Some(DecimalError::TruncatedWrongValue) => {
                        return Err(EvalError::Conversion(
                            tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL.generate(format!(
                                "Truncated incorrect DECIMAL value: '{}'",
                                tidb_datatype::format_float_g_shortest(value),
                            )),
                        ));
                    }
                    Some(DecimalError::BadNumber) => {
                        return Err(decimal_cast_error(DecimalError::BadNumber));
                    }
                    Some(DecimalError::Truncated) | None => {}
                }
                *dest = produce_decimal(field, decimal, ctx)?;
                Ok(true)
            });
        }
        (EvalType::Decimal, EvalType::Decimal) => {
            vec_eval::eval_into(arg, source, ctx, input, result)?;
            return result.try_map_fixed_rows(|cell| {
                let mut decimal = decode_decimal(cell);
                if clamp_negative && decimal.is_negative() {
                    decimal = MyDecimal::default();
                }
                produce_decimal(field, decimal, ctx).map(Some)
            });
        }
        (EvalType::Decimal, EvalType::Real) => {
            return cast_fixed(function, ctx, input, result, |bytes, dest| {
                let decimal = decode_decimal(bytes);
                if clamp_negative && decimal.is_negative() {
                    return Ok(true);
                }
                let (value, error) = decimal.to_float64();
                if let Some(error) = error {
                    if error != DecimalError::Overflow {
                        return Err(decimal_cast_error(error));
                    }
                    ctx.handle_truncate_error(tidb_datatype::types_errors::ERR_OVERFLOW.clone())?;
                    return Ok(false);
                }
                *dest = value.to_ne_bytes();
                Ok(true)
            });
        }
        (EvalType::Decimal, EvalType::Int) => {
            return cast_fixed(function, ctx, input, result, |bytes, dest| {
                let decimal = decode_decimal(bytes);
                let mut rounded = decimal;
                if let Some(error) = decimal.round(&mut rounded, 0, RoundMode::HalfUp) {
                    return Err(decimal_cast_error(error));
                }
                let (value, error) = if !unsigned {
                    rounded.to_int()
                } else if clamp_negative && rounded.is_negative() {
                    (0, None)
                } else {
                    let (bits, error) = rounded.to_uint();
                    (bits as i64, error)
                };
                *dest = value.to_ne_bytes();
                if let Some(error) = error {
                    match ctx.truncate_level() {
                        crate::ErrorLevel::Error => return Err(decimal_cast_error(error)),
                        crate::ErrorLevel::Warn => {
                            ctx.append_go_warning(
                                &tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL,
                                &decimal_int_overflow_warning(bytes),
                            );
                        }
                        crate::ErrorLevel::Ignore => {}
                    }
                }
                Ok(true)
            });
        }
        (EvalType::String, EvalType::Decimal) => {
            return cast_text(function, ctx, input, result, |bytes, dest| {
                if let Ok(text) = std::str::from_utf8(bytes) {
                    let text = text.trim();
                    if clamp_negative && text.starts_with('-') {
                        return Ok(());
                    }
                    let (decimal, error) = MyDecimal::from_string(text.as_bytes());
                    if error.is_none() {
                        *dest = produce_decimal(field, decimal, ctx)?;
                        return Ok(());
                    }
                }
                let value = super::cast_string_as_decimal(bytes, field, ctx, true)?;
                let Datum::Decimal(value) = value else {
                    unreachable!()
                };
                *dest = value.to_chunk_my_decimal_lossy().to_raw_bytes();
                Ok(())
            });
        }
        (EvalType::String, EvalType::Int) => {
            return cast_text(function, ctx, input, result, |bytes, dest| {
                if let Ok(text) = std::str::from_utf8(bytes) {
                    let text = text.trim();
                    let negative = text.starts_with('-');
                    let direct = if unsigned {
                        if clamp_negative && negative && text.len() > 1 {
                            Some(0)
                        } else {
                            text.parse::<u64>().ok().map(|value| value as i64)
                        }
                    } else {
                        text.parse::<i64>().ok()
                    };
                    if let Some(value) = direct {
                        *dest = value.to_ne_bytes();
                        return Ok(());
                    }
                }
                let value = string_datum(bytes, arg.static_type());
                let value = function
                    .pb_builtin
                    .cast_value_in(function, value, true, ctx)?;
                *dest = crate::arg_eval_type::eval_int(&value)?
                    .expect("non-null cast")
                    .to_ne_bytes();
                Ok(())
            });
        }
        (EvalType::String, EvalType::Real)
            if field.code() == FieldTypeCode::Double
                && !unsigned
                && (field.flen() < 0 || field.decimal() < 0) =>
        {
            return cast_text(function, ctx, input, result, |bytes, dest| {
                if let Ok(text) = std::str::from_utf8(bytes) {
                    if let Ok(value) = text.trim().parse::<f64>() {
                        if value.is_finite() {
                            *dest = value.to_ne_bytes();
                            return Ok(());
                        }
                    }
                }
                let value = string_datum(bytes, arg.static_type());
                let value = function
                    .pb_builtin
                    .cast_value_in(function, value, true, ctx)?;
                let Datum::Real(value) = value else {
                    unreachable!()
                };
                *dest = value.to_ne_bytes();
                Ok(())
            });
        }
        (EvalType::Real, EvalType::Int) => {
            let flags = ctx.type_flags();
            return cast_fixed(function, ctx, input, result, |bytes, dest| {
                let value = f64::from_ne_bytes(bytes);
                let converted = if !unsigned {
                    tidb_datatype::convert_float_to_int(
                        value,
                        i64::MIN,
                        i64::MAX,
                        FieldTypeCode::LongLong,
                    )
                } else if clamp_negative && value < 0.0 {
                    Ok(0)
                } else {
                    tidb_datatype::convert_float_to_uint(
                        flags,
                        value,
                        u64::MAX,
                        FieldTypeCode::LongLong,
                    )
                    .map(|value| value as i64)
                    .map_err(|(value, error)| (value as i64, error))
                };
                match converted {
                    Ok(value) => *dest = value.to_ne_bytes(),
                    Err((saturated, _)) => {
                        *dest = saturated.to_ne_bytes();
                        let rounded = tidb_datatype::round_float(value);
                        ctx.handle_truncate_error(
                            tidb_datatype::types_errors::ERR_OVERFLOW.generate(format!(
                                "constant {} overflows bigint",
                                tidb_datatype::format_float_g_shortest(rounded)
                            )),
                        )?;
                    }
                }
                Ok(true)
            });
        }
        _ => {}
    }
    let reuse = source == target && source != EvalType::String;
    let mut buffer = Column::default();
    if reuse {
        vec_eval::eval_into(arg, source, ctx, input, result)?;
    } else {
        vec_eval::eval_into(arg, source, ctx, input, &mut buffer)?;
        crate::chunk_executor::allocate(target, input.num_rows(), result);
    }
    // Go CastJSONAsJSON delegates directly, retaining the source column.
    if source == EvalType::Json && target == EvalType::Json {
        return Ok(());
    }
    for i in 0..input.num_rows() {
        let source_column = if reuse { &*result } else { &buffer };
        let value = vec_eval::value(source_column, i, source, arg.static_type());
        let value = function
            .pb_builtin
            .cast_value_in(function, value, true, ctx)?;
        vec_eval::put(result, i, target, &value)?;
    }
    Ok(())
}

fn cast_fixed<const INPUT: usize, const OUTPUT: usize>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    mut convert: impl FnMut([u8; INPUT], &mut [u8; OUTPUT]) -> Result<bool, EvalError>,
) -> Result<(), EvalError> {
    let Kernel::Cast { source, target, .. } = function.pb_builtin.kernel else {
        unreachable!()
    };
    let source = vec_eval::read_argument(&function.args[0], source, ctx, input)?.into_contiguous();
    crate::chunk_executor::allocate(target, input.num_rows(), result);
    result.merge_nulls(&[&source]);
    let source = source.raw_cells();
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, (dest, &value)) in cells.iter_mut().zip(source.fixed_cells()).enumerate() {
            let bit = 1 << (row & 7);
            let mask = &mut bitmap[row >> 3];
            if *mask & bit != 0 && !convert(value, dest)? {
                *mask &= !bit;
            }
        }
        Ok(())
    })
}

fn cast_text<const N: usize>(
    function: &ScalarFunction,
    ctx: &dyn Columns,
    input: &Chunk,
    result: &mut Column,
    mut convert: impl FnMut(&[u8], &mut [u8; N]) -> Result<(), EvalError>,
) -> Result<(), EvalError> {
    let source =
        vec_eval::read_argument(&function.args[0], EvalType::String, ctx, input)?.into_contiguous();
    let target = function.get_static_type().expect("cast type").eval_type();
    crate::chunk_executor::allocate(target, input.num_rows(), result);
    result.merge_nulls(&[&source]);
    let source = source.raw_cells();
    result.with_fixed_cells_mut(|cells, bitmap| {
        for (row, dest) in cells.iter_mut().enumerate() {
            if bitmap[row >> 3] & (1 << (row & 7)) != 0 {
                convert(source.get(row), dest)?;
            }
        }
        Ok(())
    })
}

fn write_text<'a, const N: usize>(
    result: &mut Column,
    rows: usize,
    field: &FieldType,
    ctx: &dyn Columns,
    cell: impl Fn(usize) -> Option<&'a [u8; N]>,
    format: impl Fn(&[u8; N], &mut Vec<u8>),
) -> Result<(), EvalError> {
    let mut text = Vec::with_capacity(24);
    let width = usize::try_from(field.flen()).ok();
    let pad = field.code() == FieldTypeCode::String && field.is_binary_string();
    for row in 0..rows {
        if let Some(value) = cell(row) {
            text.clear();
            format(value, &mut text);
            if width.is_some_and(|width| text.len() > width || pad && text.len() < width) {
                let value = crate::cast::finish_string_cast(text.clone(), field, true, ctx)?;
                if let Some(bytes) = value.as_raw_bytes() {
                    result.append_bytes(bytes);
                } else {
                    result.append_null();
                }
            } else {
                result.append_bytes(&text);
            }
        } else {
            result.append_null();
        }
    }
    Ok(())
}

fn decode_decimal(bytes: [u8; MYDECIMAL_STRUCT_SIZE]) -> MyDecimal {
    MyDecimal::from_raw_bytes(bytes).expect("chunk decimal cell holds a valid MyDecimal")
}

fn string_datum(bytes: &[u8], field: Option<&FieldType>) -> Datum {
    if field.is_some_and(FieldType::is_binary_string) {
        Datum::new_bytes_datum(bytes.to_vec())
    } else {
        Datum::new_string_datum(bytes.to_vec())
    }
}

fn decimal_cast_error(error: DecimalError) -> EvalError {
    use tidb_datatype::types_errors::{ERR_BAD_NUMBER, ERR_OVERFLOW, ERR_TRUNCATED};
    EvalError::Conversion(match error {
        DecimalError::Overflow => ERR_OVERFLOW.clone(),
        DecimalError::Truncated => ERR_TRUNCATED.clone(),
        _ => ERR_BAD_NUMBER.clone(),
    })
}

fn decimal_int_overflow_warning(bytes: [u8; MYDECIMAL_STRUCT_SIZE]) -> String {
    use std::fmt::Write as _;

    // Go's vector signature passes MyDecimal by value to %-.128s.
    // String has a pointer receiver, so fmt applies the verb to the fields.
    let [integer, fraction, result_fraction, negative]: [u8; 4] =
        bytes[..4].try_into().expect("decimal header");
    let mut out = format!(
        "Truncated incorrect DECIMAL value: '{{%!s(int8={integer:0128}) %!s(int8={fraction:0128}) %!s(int8={result_fraction:0128}) %!s(bool={}) [",
        negative != 0
    );
    for (i, word) in bytes[4..].as_chunks::<4>().0.iter().enumerate() {
        if i != 0 {
            out.push(' ');
        }
        write!(out, "%!s(int32={:0128})", i32::from_ne_bytes(*word)).unwrap();
    }
    out.push_str("]}'");
    out
}

/// The warning-free part of Go `ProduceDecWithSpecifiedTp`, on packed words.
/// Reuse the shared producer for precision errors and scale-loss diagnostics.
fn produce_decimal(
    field: &FieldType,
    original: MyDecimal,
    ctx: &dyn Columns,
) -> Result<[u8; MYDECIMAL_STRUCT_SIZE], EvalError> {
    let mut decimal = original;
    let flen = field.flen();
    let scale = field.decimal();
    let fits = if flen == -1 || scale == -1 {
        true
    } else if let Ok(scale) = i8::try_from(scale) {
        if flen >= i64::from(scale) && scale >= 0 {
            let fraction = decimal.digits_frac();
            let rounded = fraction == scale
                || decimal
                    .round_in_place(i32::from(scale), RoundMode::HalfUp)
                    .is_none();
            rounded
                && i64::from(decimal.significant_integer_digits()) <= flen - i64::from(scale)
                && (fraction <= scale || decimal.compare(&original).is_eq())
        } else {
            false
        }
    } else {
        false
    };
    if fits {
        if field.is_unsigned() && decimal.is_negative() {
            decimal = MyDecimal::from_uint(0);
        }
        return Ok(decimal.to_raw_bytes());
    }
    let warnings = crate::constant::ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let converted = tidb_datatype::produce_dec_with_specified_tp(
        Decimal::from_my_decimal(&original),
        field,
        &context,
    );
    if let Some(error) = context.handle_truncate(converted.error) {
        return Err(EvalError::Conversion(error));
    }
    match converted.value {
        Datum::Decimal(value) => Ok(value.to_chunk_my_decimal_lossy().to_raw_bytes()),
        _ => Err(EvalError::Unsupported("decimal cast result type")),
    }
}
