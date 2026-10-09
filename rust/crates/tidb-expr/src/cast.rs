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

//! SQL CAST and typed protobuf cast signatures. String production shares the
//! datatype owner; AST binary arguments retain their explicit decoding boundary.
//! JSON and DATE/DATETIME targets retain their native datum domains. Source
//! signatures select numeric parsing, rounding and binary padding policy.

use crate::coerce::coerce_str;
use crate::time_fn::calendar::parse_date_ymd;
use crate::Decimal;
use crate::{Datum, EvalError};
use tidb_ast::CastType;
use tidb_datatype::{
    find_encoding, number_to_duration, ConversionFlags, DatumValueError, EvalType, FieldType,
    FieldTypeCode, ScalarConversionEvent, TransformOp, JSON_TYPE_CODE_DATE,
    JSON_TYPE_CODE_DATETIME, JSON_TYPE_CODE_DURATION, JSON_TYPE_CODE_STRING,
    JSON_TYPE_CODE_TIMESTAMP,
};

/// Go's protobuf cast-as-string signatures retain the complete wire field
/// type. Producing the string and padding fixed binary values are separate
/// operations; variable-width binary strings truncate but never pad.
pub(crate) fn eval_string_cast_with_type(
    value: Datum,
    source: Option<&FieldType>,
    target: &FieldType,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let value = match value {
        Datum::Int(value) if source.is_some_and(FieldType::is_unsigned) => {
            Datum::UInt(value as u64)
        }
        value => value,
    };
    let bytes = match year_zero_string(&value, source) {
        Some(text) => text.into_bytes(),
        None => datum_binary_bytes(&value)?,
    };
    let warnings = crate::constant::ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let converted =
        tidb_datatype::produce_string_with_type_in_context(bytes, target, false, &context)
            .map_err(|_| EvalError::Unsupported("protobuf string production"))?;
    if let Some(error) = converted.error {
        return Err(EvalError::Conversion(error));
    }
    let bytes = converted
        .value
        .as_raw_bytes()
        .ok_or(EvalError::Unsupported("produced string datum"))?;
    // JSON/vector signatures return after production; the other signatures
    // pad TypeString with binary collation and enforce the padding packet limit.
    if !matches!(value, Datum::Json(_) | Datum::VectorFloat32(_))
        && target.code() == FieldTypeCode::String
        && target.is_binary_string()
        && target.flen() > bytes.len() as i64
    {
        if target.flen() as u64 > ctx.max_allowed_packet() {
            ctx.handle_allowed_packet_overflowed("cast_as_binary")?;
            return Ok(Datum::Null);
        }
        let mut padded = bytes.to_vec();
        padded.resize(target.flen() as usize, 0);
        return Ok(Datum::new_bytes(padded));
    }
    Ok(converted.value)
}

/// Evaluate numeric cast signatures without reducing their wire FieldType to
/// an AST type. Go's source conversion and result production are separate steps.
pub(crate) fn eval_numeric_cast_with_type(
    value: Datum,
    source: EvalType,
    source_field: Option<&FieldType>,
    target: &FieldType,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    use tidb_datatype::{DecimalError, MyDecimal};
    let warnings = crate::constant::ConversionWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let handle = |error| {
        context
            .handle_truncate(error)
            .map_or(Ok(()), |error| Err(EvalError::Conversion(error)))
    };
    let value = match value {
        Datum::Int(value)
            if source == EvalType::Int
                && matches!(target.eval_type(), EvalType::Real | EvalType::Decimal)
                && (target.is_unsigned() || source_field.is_some_and(FieldType::is_unsigned)) =>
        {
            Datum::UInt(value as u64)
        }
        value => value,
    };
    match target.eval_type() {
        EvalType::Decimal => {
            let decimal = if source == EvalType::String {
                let bytes = value
                    .as_raw_bytes()
                    .ok_or(EvalError::Unsupported("decimal cast string"))?;
                let text = String::from_utf8_lossy(bytes);
                let text = text.trim();
                let (decimal, error) = MyDecimal::from_string(text.as_bytes());
                let error = error.map(|error| match error {
                    DecimalError::Truncated => tidb_datatype::ERR_TRUNCATED_WRONG_VALUE
                        .generate(format!("Truncated incorrect DECIMAL value: '{text}'")),
                    DecimalError::Overflow => tidb_datatype::ERR_OVERFLOW.clone(),
                    DecimalError::BadNumber => tidb_datatype::ERR_BAD_NUMBER.clone(),
                    DecimalError::TruncatedWrongValue => tidb_datatype::ERR_TRUNCATED_WRONG_VALUE
                        .generate(format!("Truncated incorrect DECIMAL value: '{text}'")),
                });
                handle(error)?;
                Decimal::from_my_decimal(&decimal)
            } else if let Datum::Real(value) | Datum::Float32(value) = value {
                let (decimal, error) = MyDecimal::from_float64(value);
                match error {
                    // The cast signature explicitly ignores fraction loss.
                    None | Some(DecimalError::Truncated) => {}
                    Some(DecimalError::Overflow) => match ctx.truncate_level() {
                        crate::ErrorLevel::Error => {
                            return Err(EvalError::Conversion(tidb_datatype::ERR_OVERFLOW.clone()))
                        }
                        crate::ErrorLevel::Warn => ctx.append_warning(
                            1292,
                            &format!(
                                "Truncated incorrect DECIMAL value: '{}'",
                                tidb_datatype::format_float_g_shortest(value)
                            ),
                        ),
                        crate::ErrorLevel::Ignore => {}
                    },
                    Some(_) => {
                        return Err(EvalError::Conversion(tidb_datatype::ERR_BAD_NUMBER.clone()))
                    }
                }
                Decimal::from_my_decimal(&decimal)
            } else {
                let (decimal, error) = value
                    .to_decimal_with_context(&context)
                    .map_err(|_| EvalError::Unsupported("numeric cast decimal source"))?;
                // JSON conversion already applies its type context; numeric
                // sources return their original diagnostic to the expression.
                handle(error)?;
                decimal
            };
            let result =
                tidb_datatype::produce_decimal_with_type_in_context(decimal, target, &context);
            handle(result.error)?;
            Ok(result.value)
        }
        EvalType::Real => {
            if source == EvalType::String {
                let result = str_to_real_for_cast(&value, ctx)?;
                let result =
                    tidb_datatype::produce_float_with_type_in_context(result, target, &context);
                return result
                    .error
                    .map_or(Ok(result.value), |error| Err(EvalError::Conversion(error)));
            }
            if matches!(value, Datum::Json(_)) {
                let converted = value
                    .to_f64_in_context(&context)
                    .map_err(|_| EvalError::Unsupported("JSON real conversion"))?;
                return converted.error.map_or(Ok(converted.value), |error| {
                    Err(EvalError::Conversion(error))
                });
            }
            let converted = value
                .to_f64()
                .map_err(|_| EvalError::Unsupported("real cast source"))?;
            if converted.event.is_some() {
                handle(Some(tidb_datatype::ERR_TRUNCATED_WRONG_VALUE.generate(
                    format!(
                        "Truncated incorrect FLOAT value: '{}'",
                        datum_sql_string(&value)?
                    ),
                )))?;
            }
            Ok(Datum::Real(converted.value))
        }
        EvalType::Int => {
            let unsigned = target.is_unsigned();
            let integer = |value: i64| {
                if unsigned {
                    Datum::UInt(value as u64)
                } else {
                    Datum::Int(value)
                }
            };
            match value {
                Datum::Int(value) => Ok(integer(value)),
                Datum::UInt(value) => Ok(integer(value as i64)),
                Datum::Real(value) | Datum::Float32(value) => {
                    let result = if unsigned {
                        tidb_datatype::convert_float_to_uint(
                            ctx.type_flags(),
                            value,
                            u64::MAX,
                            FieldTypeCode::LongLong,
                        )
                        .map(|value| value as i64)
                        .map_err(|(value, error)| (value as i64, error))
                    } else {
                        tidb_datatype::convert_float_to_int(
                            value,
                            i64::MIN,
                            i64::MAX,
                            FieldTypeCode::LongLong,
                        )
                    };
                    // Numeric conversion primitives return a saturated value beside the error.
                    match result {
                        Ok(value) => Ok(integer(value)),
                        Err((converted, _error)) => {
                            handle(Some(tidb_datatype::ERR_OVERFLOW.generate(format!(
                                "constant {} overflows bigint",
                                tidb_datatype::format_float_g_shortest(tidb_datatype::round_float(
                                    value
                                )),
                            ))))?;
                            Ok(integer(converted))
                        }
                    }
                }
                Datum::Decimal(value) => {
                    let rounded = value.round_to_scale(0);
                    let (result, error) = if unsigned {
                        let (value, error) = rounded.to_u64_trunc();
                        (value as i64, error)
                    } else {
                        rounded.to_i64_trunc()
                    };
                    if error.is_some() {
                        let error = tidb_datatype::ERR_OVERFLOW.clone();
                        match ctx.truncate_level() {
                            crate::ErrorLevel::Error => return Err(EvalError::Conversion(error)),
                            crate::ErrorLevel::Warn => ctx.append_warning(
                                1292,
                                &format!("Truncated incorrect DECIMAL value: '{value}'"),
                            ),
                            crate::ErrorLevel::Ignore => {}
                        }
                    }
                    Ok(integer(result))
                }
                value @ Datum::Json(_) => {
                    let converted = value
                        .convert_to_in_context(
                            &FieldType::new(FieldTypeCode::LongLong).with_unsigned(unsigned),
                            &context,
                            &zone,
                        )
                        .map_err(|_| EvalError::Unsupported("JSON integer conversion"))?;
                    handle(converted.error)?;
                    Ok(converted.value)
                }
                value => eval_cast_value(
                    if unsigned {
                        &CastType::Unsigned
                    } else {
                        &CastType::Signed
                    },
                    value,
                    source_field,
                    ctx,
                ),
            }
        }
        _ => Err(EvalError::Unsupported("non-numeric cast result")),
    }
}

/// Internal marker used when a wrapper carries Go's `UnspecifiedLength`
/// decimal scale through the AST-facing `CastType::Decimal` (whose fields are
/// unsigned).  A wrapper cast with an unspecified scale must preserve the
/// source value; mapping `-1` to the ordinary `0` scale would round every
/// fractional value to an integer before Go's constant-refinement step.
pub(crate) const UNSPECIFIED_CAST_SCALE: u32 = u32::MAX;

/// Evaluates a [`CastType`] against an already-evaluated, non-`NULL`
/// operand (`NULL` is handled by the caller — every target type maps
/// `NULL` to `NULL`, so there's no per-type NULL case to write here).
///
/// `source` is the operand's static `FieldType` where the caller knows it, and
/// `None` where it does not. Go picks the cast SIGNATURE from that type
/// (`builtinCastIntAsTimeSig` vs `builtinCastStringAsTimeSig` vs ...), and the
/// datum kind is only a proxy for it — a proxy with exactly one hole, `YEAR`,
/// whose values are ordinary `Datum::Int`s that Go nonetheless converts by a
/// rule of their own. See [`cast_to_time`].
pub(crate) fn eval_cast(
    cast_type: &CastType,
    v: Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let mut target = match cast_type {
        CastType::Signed | CastType::Unsigned => FieldType::new(FieldTypeCode::LongLong),
        CastType::UnsignedInUnion if matches!(v, Datum::Json(_)) => {
            FieldType::new(FieldTypeCode::LongLong)
        }
        CastType::Double => FieldType::new(FieldTypeCode::Double),
        CastType::Decimal { .. } => FieldType::new(FieldTypeCode::NewDecimal),
        _ => return eval_cast_value(cast_type, v, source, ctx),
    };
    if matches!(cast_type, CastType::Unsigned | CastType::UnsignedInUnion) {
        target.add_flags(tidb_datatype::FieldTypeFlags::UNSIGNED);
    }
    if let CastType::Decimal { flen, scale } = cast_type {
        target.set_flen(if *flen == 0 {
            tidb_datatype::UNSPECIFIED_LENGTH
        } else {
            i64::from(*flen)
        });
        target.set_decimal(if *scale == UNSPECIFIED_CAST_SCALE {
            tidb_datatype::UNSPECIFIED_LENGTH
        } else {
            i64::from(*scale)
        });
    }
    // Go's binary-literal real/decimal signatures return the argument's
    // numeric evaluation directly, without destination width production.
    if matches!(v, Datum::BinaryLiteral(_))
        && matches!(target.eval_type(), EvalType::Real | EvalType::Decimal)
    {
        let warnings = crate::constant::ConversionWarnings(ctx);
        let zone = ctx.time_zone();
        let context = tidb_datatype::ConversionContext::new(
            ctx.type_flags(),
            tidb_datatype::ConversionLocation::from_time_zone(&zone),
            &warnings,
        );
        let Datum::BinaryLiteral(value) = v else {
            unreachable!()
        };
        let (integer, error) = value.to_int_with_context(&context);
        if let Some(error) = error {
            return Err(EvalError::Conversion(error));
        }
        return Ok(if target.eval_type() == EvalType::Real {
            Datum::Real(integer as f64)
        } else {
            Datum::Decimal(Decimal::from_uint(integer))
        });
    }
    // AST callers may already have applied UNION's negative-to-zero rule.
    // Go selects numeric signatures for hybrid types and binary literals.
    let inferred = tidb_datatype::infer_param_type_from_datum(&v);
    let domain = match &v {
        Datum::BinaryLiteral(_) | Datum::Bit(_) => EvalType::Decimal,
        Datum::Enum(..) | Datum::Set(..) => EvalType::Int,
        _ => inferred.eval_type(),
    };
    eval_numeric_cast_with_type(v, domain, Some(source.unwrap_or(&inferred)), &target, ctx)
}

fn eval_cast_value(
    cast_type: &CastType,
    v: Datum,
    source: Option<&FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if v.is_range_sentinel() {
        return Err(EvalError::Unsupported("range sentinel cast operand"));
    }
    if matches!(v, Datum::VectorFloat32(_))
        && !matches!(
            cast_type,
            CastType::Char { .. } | CastType::Binary { .. } | CastType::Vector { .. }
        )
    {
        return Err(EvalError::Unsupported(
            "a vector can only be cast to string or vector",
        ));
    }
    match cast_type {
        CastType::Signed => Ok(Datum::Int(to_i64_signed_with_warnings(&v, ctx)?)),
        CastType::Unsigned => {
            report_int_truncation(&v, ctx)?;
            report_negative_string_unsigned(&v, ctx);
            Ok(Datum::UInt(to_u64_unsigned(&v, ctx)))
        }
        CastType::UnsignedInUnion => {
            // Every numeric/string `castAsInt` signature has an `inUnion`
            // negative-to-zero branch in Go. Temporal signatures do not: a
            // TIME/DATETIME value is first rendered as an integer and then
            // reinterpreted by the ordinary unsigned path. Check the source
            // eval family, not just the datum shape, before applying the
            // branch so string warnings are not emitted for a value Go drops.
            if union_unsigned_clamps_negative(&v, source) {
                Ok(Datum::UInt(0))
            } else {
                report_int_truncation(&v, ctx)?;
                report_negative_string_unsigned(&v, ctx);
                Ok(Datum::UInt(to_u64_unsigned(&v, ctx)))
            }
        }
        CastType::Char { len, charset } => {
            let (connection_charset, connection_collation) = ctx.connection_charset_info();
            let target_charset = charset
                .as_deref()
                .unwrap_or(connection_charset)
                .to_ascii_lowercase();
            let target_collation = match charset {
                Some(_) => tidb_datatype::get_default_collation(&target_charset)
                    .map_err(|_| EvalError::Unsupported("CAST charset"))?,
                None => connection_collation.to_owned(),
            };
            let target = FieldType::new(FieldTypeCode::VarString)
                .with_flen(
                    len.map(i64::from)
                        .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                )
                .with_charset_name(&target_charset)
                .with_collation_name(&target_collation);
            // Go inserts from_binary at the AST argument boundary. Protobuf
            // signatures carry that wrapper explicitly, so decoding stays here.
            if source.is_some_and(FieldType::is_binary_string) && target_charset != "binary" {
                let bytes = datum_binary_bytes(&v)?;
                let (decoded, error) = find_encoding(&target_charset)
                    .transform(&bytes, TransformOp::DECODE)
                    .into_parts();
                if error.is_some() {
                    let hex = bytes
                        .iter()
                        .map(|byte| format!("{byte:02X}"))
                        .collect::<String>();
                    ctx.append_warning(
                        3854,
                        &format!("Cannot convert string '{hex}' from binary to {target_charset}"),
                    );
                }
                eval_string_cast_with_type(Datum::new_bytes(decoded), None, &target, ctx)
            } else {
                eval_string_cast_with_type(v, source, &target, ctx)
            }
        }
        CastType::Binary { len } => {
            let target = FieldType::new(FieldTypeCode::String)
                .with_flen(
                    len.map(i64::from)
                        .unwrap_or(tidb_datatype::UNSPECIFIED_LENGTH),
                )
                .with_charset_name("binary")
                .with_collation_name("binary");
            eval_string_cast_with_type(v, source, &target, ctx)
        }
        CastType::Decimal { .. } | CastType::Double => eval_cast(cast_type, v, source, ctx),
        CastType::Date => cast_to_time(&v, source, ctx, tidb_datatype::TimeType::Date, 0),
        CastType::DateTime { fsp } => cast_to_time(
            &v,
            source,
            ctx,
            tidb_datatype::TimeType::DateTime,
            i64::from(fsp.unwrap_or(0)),
        ),
        CastType::Year => cast_to_year(&v, ctx),
        // go's FLOAT cast narrows through float32, and the two operand kinds
        // diverge (captured on the oracle): a TEXT value beyond the float32
        // range is `types.ErrOverflow`'s "constant 1e+300 overflows float"
        // (1690 / 22003), while a REAL constant's conversion answers 0
        // (`CAST(1e300 AS FLOAT)` -> 0). Within range both answer the value.
        CastType::Float => match v {
            Datum::Real(x) | Datum::Float32(x) => {
                let narrowed = x as f32;
                if narrowed.is_infinite() {
                    Ok(Datum::Real(0.0))
                } else {
                    Ok(Datum::Real(f64::from(narrowed)))
                }
            }
            other => {
                let converted = str_to_real_for_cast(&other, ctx)?;
                if converted.abs() > f64::from(f32::MAX) {
                    return Err(EvalError::ConstantFloatCastOverflow {
                        value: tidb_datatype::format_float_g_shortest(converted),
                    });
                }
                Ok(Datum::Real(converted))
            }
        },
        CastType::Vector { dimensions } => {
            let mut target = FieldType::new(FieldTypeCode::VectorFloat32);
            if let Some(dimensions) = dimensions {
                target.set_flen(i64::from(*dimensions));
            }
            let source_name = source
                .map(|field_type| tidb_datatype::type_str(field_type.code()))
                .unwrap_or("unspecified");
            v.convert_to(&target, ConversionFlags::default())
                .map(|converted| converted.value)
                .map_err(|error| match error {
                    DatumValueError::Unsupported(_, _) => {
                        EvalError::Vector(format!("cannot cast from {source_name} to vector"))
                    }
                    error => EvalError::Vector(error.to_string()),
                })
        }
        CastType::Time { fsp } => cast_to_duration(&v, source, ctx, i64::from(fsp.unwrap_or(0))),
        CastType::Json => crate::builtin_ext::cast_as_json(&v),
    }
}

fn union_unsigned_clamps_negative(
    value: &Datum,
    source: Option<&tidb_datatype::FieldType>,
) -> bool {
    let source_eval_type = source.map(FieldType::eval_type);
    match source_eval_type {
        Some(EvalType::Int) => matches!(value, Datum::Int(number) if *number < 0),
        Some(EvalType::Real) => {
            matches!(value, Datum::Real(number) if *number < 0.0)
                || matches!(value, Datum::Float32(number) if *number < 0.0)
        }
        Some(EvalType::Decimal) => {
            matches!(value, Datum::Decimal(decimal) if decimal.round_to_i64_saturating() < 0)
        }
        Some(EvalType::String) | None => match value {
            Datum::String(text) => text
                .as_utf8()
                .is_ok_and(|text| text.trim().len() > 1 && text.trim().starts_with('-')),
            Datum::Bytes(bytes) => std::str::from_utf8(bytes)
                .is_ok_and(|text| text.trim().len() > 1 && text.trim().starts_with('-')),
            Datum::Int(number) => *number < 0,
            Datum::Real(number) => *number < 0.0,
            Datum::Float32(number) => *number < 0.0,
            Datum::Decimal(decimal) => decimal.round_to_i64_saturating() < 0,
            _ => false,
        },
        Some(EvalType::Datetime | EvalType::Timestamp | EvalType::Duration)
        | Some(EvalType::VectorFloat32 | EvalType::Json) => false,
    }
}

/// `CAST(expr AS TIME[(fsp)])` returns TiDB's elapsed-time datum, not a
/// calendar string. The source type selects the Go signature: numeric inputs
/// turn a truncation/overflow into `NULL`, while string inputs retain the
/// parser's best-effort duration after reporting the truncation event.
fn cast_to_duration(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    fsp: i64,
) -> Result<Datum, EvalError> {
    let input = v.sql_string().unwrap_or_else(|_| "<binary>".to_owned());
    let target = FieldType::new(FieldTypeCode::Duration).with_decimal(fsp);
    let source_eval_type = source.map(FieldType::eval_type);

    if let Datum::Json(value) = v {
        match value.type_code() {
            JSON_TYPE_CODE_DURATION => {
                return value
                    .as_duration()
                    .map(Datum::Duration)
                    .map_err(|_| EvalError::Unsupported("JSON duration payload"))
            }
            JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => {
                let time = value
                    .as_time(fsp)
                    .map_err(|_| EvalError::Unsupported("JSON time payload"))?;
                let duration = time
                    .to_duration()
                    .map_err(|_| EvalError::Unsupported("JSON time conversion"))?;
                return duration
                    .round_frac(fsp)
                    .map(Datum::Duration)
                    .map_err(|_| EvalError::Unsupported("JSON time precision"));
            }
            JSON_TYPE_CODE_STRING => {
                let text = value
                    .unquote()
                    .map_err(|_| EvalError::Unsupported("JSON string payload"))?;
                let parsed = tidb_datatype::parse_duration_with_flags(
                    &text,
                    fsp,
                    ctx.type_flags(),
                    &ctx.time_zone(),
                );
                let value = match parsed {
                    Ok(parsed) if parsed.event.is_none() => {
                        return Ok(Datum::Duration(parsed.value))
                    }
                    Ok(parsed) => parsed.value,
                    Err(_) => tidb_datatype::MySqlDuration::from_raw_parts(0, 0),
                };
                ctx.handle_truncate(&format!(
                    "Truncated incorrect time value: '{}'",
                    tidb_datatype::warning_subject_byte_cap(&text)
                ))?;
                return Ok(Datum::Duration(value));
            }
            _ => {
                ctx.handle_truncate(&format!(
                    "Truncated incorrect TIME value: '{}'",
                    tidb_datatype::warning_subject_byte_cap(&input)
                ))?;
                return Ok(Datum::Null);
            }
        }
    }

    let numeric = matches!(
        source_eval_type,
        Some(EvalType::Int | EvalType::Real | EvalType::Decimal)
    ) || (source_eval_type.is_none()
        && matches!(
            v,
            Datum::Int(_) | Datum::UInt(_) | Datum::Real(_) | Datum::Float32(_) | Datum::Decimal(_)
        ));
    let converted = if source_eval_type == Some(EvalType::Int)
        || (source_eval_type.is_none() && matches!(v, Datum::Int(_) | Datum::UInt(_)))
    {
        let number = match v {
            Datum::Int(value) => *value,
            // Go's ETInt ABI is `int64`: an unsigned source reaches this
            // signature through the same low-64-bit representation.
            Datum::UInt(value) => *value as i64,
            _ => return Err(EvalError::Unsupported("CAST AS TIME integer datum")),
        };
        number_to_duration(number, fsp)
            .map(|converted| (Datum::new_duration(converted.value), converted.event))
            .map_err(|error| DatumValueError::Comparison(error.to_string()))
    } else if matches!(v, Datum::Time(_) | Datum::Duration(_)) {
        v.convert_to_in(&target, ctx.type_flags(), &ctx.time_zone())
            .map(|converted| (converted.value, converted.event))
    } else {
        tidb_datatype::parse_duration_with_flags(&input, fsp, ctx.type_flags(), &ctx.time_zone())
            .map(|converted| (Datum::Duration(converted.value), converted.event))
            .map_err(|error| DatumValueError::Comparison(error.to_string()))
    };

    match converted {
        Ok((value, None | Some(ScalarConversionEvent::RoundedToScale))) => Ok(value),
        Ok((value, Some(_))) => {
            ctx.handle_truncate(&format!(
                "Truncated incorrect time value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&input)
            ))?;
            Ok(if numeric { Datum::Null } else { value })
        }
        Err(DatumValueError::Unsupported(_, _)) => {
            Err(EvalError::Unsupported("CAST AS TIME source datum"))
        }
        Err(_) => {
            ctx.handle_truncate(&format!(
                "Truncated incorrect time value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&input)
            ))?;
            Ok(Datum::Null)
        }
    }
}

/// Go `WrapWithCastAsDuration` applied to one builtin argument value.
///
/// A duration expression is already in the requested domain. Calendar values
/// preserve their declared fractional precision; every other source receives
/// Go's `MaxFsp` target before the cast signature parses it.
pub(crate) fn cast_arg_as_duration(
    value: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(value, Datum::Duration(_) | Datum::Null) {
        return Ok(value.clone());
    }
    let fsp = source
        .filter(|field_type| {
            matches!(
                field_type.code(),
                FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
            )
        })
        .map_or(6, FieldType::decimal);
    cast_to_duration(value, source, ctx, fsp)
}

fn datum_sql_string(value: &Datum) -> Result<String, EvalError> {
    value
        .sql_string()
        .map_err(|_| EvalError::Unsupported("invalid UTF-8 string coercion"))
}

/// Go `builtinCastIntAsStringSig.evalString`'s last rendering rule
/// (`pkg/expression/builtin_cast.go:1098`):
///
/// ```go
/// if tp.GetType() == mysql.TypeYear && res == "0" {
///     res = "0000"
/// }
/// ```
///
/// `tp` is `b.args[0].GetType(ctx)` -- the SOURCE's static type, not the
/// datum's. A zero YEAR is a `Datum::Int(0)` indistinguishable from a
/// `BIGINT` zero, and the two render differently: `CAST(y AS CHAR)` is
/// `'0000'` where `CAST(i AS CHAR)` is `'0'`. Captured over
/// `t(y year, i int)` holding `(0, 0)`:
///
/// ```text
/// select cast(y as char), length(cast(y as char)), cast(i as char) from t;
/// 0000    4    0
/// ```
///
/// `Some` only for that one value: every other YEAR is already its own four
/// digits (the domain is `0` and `1901..=2155`), which is why Go tests the
/// RENDERED text rather than the integer.
fn year_zero_string(value: &Datum, source: Option<&tidb_datatype::FieldType>) -> Option<String> {
    if source.map(tidb_datatype::FieldType::code) != Some(tidb_datatype::FieldTypeCode::Year) {
        return None;
    }
    matches!(value, Datum::Int(0) | Datum::UInt(0)).then(|| "0000".to_owned())
}

/// Returns the byte payload used by Go's `builtinCast*AsStringSig` binary
/// target.  String/bytes datums already carry the source bytes; only numeric
/// values need SQL stringification first.
fn datum_binary_bytes(value: &Datum) -> Result<Vec<u8>, EvalError> {
    match value {
        Datum::String(value) => Ok(value.bytes().to_vec()),
        Datum::Bytes(value) => Ok(value.clone()),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => Ok(value.as_bytes().to_vec()),
        _ => Ok(datum_sql_string(value)?.into_bytes()),
    }
}

/// `SIGNED`'s own coercion: `Int` is unchanged; `Decimal`/`Float` round to
/// the nearest integer (ties away from zero for `Decimal`, ties to EVEN
/// for `Float` — a real asymmetry, matching the `~` bitwise operator's own
/// established rule, confirmed via `goeval`: `CAST(2.5e0 AS SIGNED)` is
/// `2`, `CAST(1.5 AS SIGNED)` — a `DECIMAL` literal — is also `2`, but
/// `CAST(0.5e0 AS SIGNED)` is `0`, the even neighbor); either CLAMPS
/// (never errors) on overflow past `i64`, confirmed via `goeval`:
/// `CAST(1e300 AS SIGNED)` is `9223372036854775807`. `Str` parses a
/// leading `[+-]?digits` prefix ONLY (no `.`, no exponent — confirmed via
/// `goeval`: `CAST('3.5abc' AS SIGNED)` sees just `3`, `CAST('.5' AS
/// SIGNED)` sees no digits at all), defaulting to `0` if no digit is
/// found. A nonnegative integer prefix is parsed through the full `u64`
/// domain and then converted to `i64`, preserving Go's negative-complement
/// result above `i64::MAX`; true `u64` overflow returns `-1`. Negative
/// overflow clamps to `i64::MIN`.
pub(crate) fn to_i64_signed(v: &Datum) -> i64 {
    to_i64_signed_in(v, &tidb_datatype::SessionTimeZone::utc())
}

/// [`to_i64_signed`] with the session's `time_zone`, which Go's
/// `toSignedInteger` hands to `Time.RoundFrac` -- load-bearing only when a
/// DATETIME's fractional carry lands on a DST transition instant.
pub(crate) fn to_i64_signed_in(v: &Datum, zone: &tidb_datatype::SessionTimeZone) -> i64 {
    match v {
        Datum::Int(i) => *i,
        Datum::UInt(i) => *i as i64,
        Datum::Decimal(d) => d.round_to_i64_saturating(),
        Datum::Real(f) => f.round_ties_even() as i64,
        Datum::String(s) => s.as_utf8().map(str_int_prefix).unwrap_or(0),
        Datum::Bytes(s) => std::str::from_utf8(s).map(str_int_prefix).unwrap_or(0),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other.to_i64_in(zone).map_or(0, |converted| converted.value),
    }
}

/// Signed integer coercion plus the warnings produced by Go's cast signature.
///
/// Builtins whose arguments are wrapped with `WrapWithCastAsInt` must use this
/// boundary rather than the value-only helper so their statement warning list
/// remains identical to an explicit `CAST(... AS SIGNED)`.
pub(crate) fn to_i64_signed_with_warnings(
    v: &Datum,
    ctx: &dyn crate::Columns,
) -> Result<i64, EvalError> {
    if matches!(v, Datum::Json(_)) {
        return match eval_numeric_cast_with_type(
            v.clone(),
            EvalType::Json,
            None,
            &FieldType::new(FieldTypeCode::LongLong),
            ctx,
        )? {
            Datum::Int(value) => Ok(value),
            _ => Err(EvalError::Unsupported("JSON signed conversion result")),
        };
    }
    report_int_truncation(v, ctx)?;
    report_signed_overflow(v, ctx);
    Ok(to_i64_signed_in(v, &ctx.time_zone()))
}

/// `UNSIGNED`'s own coercion. Integer and integer-string sources preserve
/// the low 64 bits, so `CAST(-5 AS UNSIGNED)` is the genuine
/// `18446744073709551611` UInt64 value.
///
/// The DECIMAL and the FLOAT source do NOT agree about a negative value, and
/// that disagreement is Go's, not an inconsistency to smooth over:
///
///  * `MyDecimal.ToUint` (`ConvertDecimalToUint`) returns 0 for a negative
///    value, plus a truncation event. Captured: `cast(-1.5 as unsigned)` is 0
///    with `1292 Truncated incorrect DECIMAL value: '-1.5'`.
///  * `ConvertFloatToUint` (`pkg/types/convert.go:169-183`) rounds first, and
///    for a negative result takes the `AllowNegativeToUnsigned` arm --
///    `return uint64(int64(val))`, the low 64 bits, exactly like the integer
///    source above -- beside an overflow event. Captured:
///    `cast(-1.5e0 as unsigned)` is 18446744073709551614 with `1690 constant
///    -2 overflows bigint`, `cast(-1e0 as unsigned)` is 18446744073709551615,
///    and `cast(-1e300 as unsigned)` is 9223372036854775808 (Go's
///    out-of-range `int64(...)` conversion lands on `i64::MIN`, which is what
///    Rust's saturating `as i64` gives too).
///
/// `-0.4` is the boundary the two share: it ROUNDS to `-0.0`, which is not
/// `< 0`, so both answer 0 with no event at all.
///
/// The result is [`Datum::UInt`], so downstream comparisons and arithmetic
/// retain the domain instead of silently reinterpreting it as signed display
/// text.
fn to_u64_unsigned(v: &Datum, ctx: &dyn crate::Columns) -> u64 {
    match v {
        // TiDB's integer cast reuses the low 64 bits for an ETInt source.
        // That is observable for `CAST(-5 AS UNSIGNED)`, which is
        // 18446744073709551611 rather than an error or a display-only wrap.
        // Go's `builtinCastTimeAsIntSig`/`builtinCastDurationAsIntSig`
        // produce a plain `int64` and the UNSIGNED target only reinterprets
        // its bits, so a temporal source takes the SIGNED path -- including
        // its `RoundFrac(DefaultFsp)`, which `convertToUint`'s own temporal
        // arm (a different caller) does NOT do.
        Datum::Int(_)
        | Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Time(_)
        | Datum::Duration(_) => to_i64_signed_in(v, &ctx.time_zone()) as u64,
        Datum::UInt(i) => *i,
        // A decimal rounds half-up then converts through the full u64 range
        // (Go `MyDecimal.ToUint`): a negative value becomes 0, and a magnitude in
        // `(i64::MAX, u64::MAX]` — the upper half of `UNSIGNED BIGINT` — is kept
        // rather than saturated at `i64::MAX` by the signed path.
        Datum::Decimal(d) => {
            // Go's `convertDecimalStrToUint` reports the clamp, and the
            // message carries the decimal's ORIGINAL text -- `'-2.0'`, not the
            // rounded `-2`. The test is on the ROUNDED value, which is why
            // `cast(-0.4 as unsigned)` is a silent 0 (it rounds to `-0`) while
            // `cast(-1.5 as unsigned)` warns. Captured (`gorun`, default
            // sql_mode): `select cast(-2.0 as unsigned)` -> 0 with
            // `1292 Truncated incorrect DECIMAL value: '-2.0'`;
            // `cast(1.5 as unsigned)` -> 2 with no warning at all.
            if d.round_to_i64_saturating() < 0 {
                ctx.append_warning(1292, &format!("Truncated incorrect DECIMAL value: '{d}'"));
            }
            d.round_to_u64_saturating()
        }
        // A real rounds half-to-even then converts across the full u64 range
        // (Go `ConvertFloatToUint`), so its own upper half is kept too -- and
        // its NEGATIVE half is kept as the low 64 bits rather than clamped.
        Datum::Real(f) | Datum::Float32(f) => real_to_u64_saturating(*f, ctx),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other
            .to_decimal()
            .map_or(0, |converted| converted.value.round_to_u64_saturating()),
    }
}

/// `CAST(real AS UNSIGNED)`: round half to even (Go `RoundFloat` =
/// `math.RoundToEven`, the same rounding the signed real path uses), then Go
/// `ConvertFloatToUint` across the full `u64` range. A magnitude past
/// `u64::MAX` saturates to `u64::MAX` and reports overflow
/// (`ConvertFloatToUint`'s `upperBound` clamp). Routing through the signed
/// path instead would lose the upper half of `UNSIGNED BIGINT` at `i64::MAX`.
///
/// A NEGATIVE rounded value takes Go's `AllowNegativeToUnsigned` arm
/// (`convert.go:171-176`): `uint64(int64(val))`, the SAME low-64-bit
/// reinterpretation an integer source gets -- see [`to_u64_unsigned`]'s doc
/// for the captures, and for why the DECIMAL source really does answer 0 here
/// while this one does not.
fn real_to_u64_saturating(f: f64, ctx: &dyn crate::Columns) -> u64 {
    let rounded = f.round_ties_even();
    if rounded < 0.0 {
        // Go raises the overflow event on this arm and returns the value
        // anyway. `overflow(val, tp)` prints the ROUNDED value with `%v`,
        // which for a float64 is `strconv.FormatFloat(f, 'g', -1, 64)`.
        // Captured under the DEFAULT (strict) sql_mode, where this is still a
        // WARNING rather than a statement error, because
        // `builtinCastRealAsIntSig` routes it through `HandleOverflow`.
        ctx.append_warning(
            1690,
            &format!(
                "constant {} overflows bigint",
                tidb_datatype::format_float_g_shortest(rounded)
            ),
        );
        // Rust's saturating `as i64` reproduces Go's out-of-range `int64(...)`
        // landing on `i64::MIN`, which is what makes `cast(-1e300 as
        // unsigned)` 9223372036854775808 rather than 0.
        (rounded as i64) as u64
    } else if !rounded.is_finite() || rounded >= (u64::MAX as f64) {
        // Go's big.Float.Uint64 returns the upper bound and an overflow
        // status for +Inf and every rounded value at or beyond 2^64.  The
        // statement context turns that status into the 1690 warning used by
        // `builtinCastRealAsIntSig`.
        ctx.append_warning(
            1690,
            &format!(
                "constant {} overflows bigint",
                tidb_datatype::format_float_g_shortest(rounded)
            ),
        );
        u64::MAX
    } else {
        // Rust's float-to-int cast is exact for the remaining in-range
        // integral values, including the full upper half of UNSIGNED BIGINT.
        rounded as u64
    }
}

/// Scans a MySQL-style INTEGER numeric prefix: optional leading
/// whitespace, optional sign, then a run of ASCII digits — stopping at
/// the first non-digit (no `.`, no exponent; see [`to_i64_signed`]'s own
/// doc for the confirming probe). `0` if no digit is found. Saturates to
/// `i64::MIN`/`MAX` on overflow rather than replicating real TiDB's own
/// exotic bit-reinterpretation for a string whose digit run exceeds even
/// `u64` range (confirmed via `goeval`: `CAST('99999999999999999999' AS
/// SIGNED)` — twenty `9`s — is `-1` in real TiDB, a `u64::MAX` value
/// bit-reinterpreted as `i64`; this project deliberately does not
/// replicate that, saturating to `i64::MAX` instead — a principled,
/// documented divergence for a value nobody writes intentionally, not an
/// oversight).
/// Go `types.getValidIntPrefix`'s `isFuncCast` arm, reporting ONLY whether
/// the scan consumed the whole string. Go scans BYTES and advances the valid
/// length only on a digit, so a lone sign leaves length zero:
/// `[+-]?` at offset 0 is skipped without counting, every following ASCII
/// digit sets the length to `i + 1`, and the first other byte stops the scan.
///
/// Returned separately from [`str_int_prefix`] because the two answers have
/// different lifetimes in Go too: the prefix VALUE is returned to the caller
/// unconditionally, while the truncation event goes through
/// `Context.HandleTruncate` and may be discarded, warned, or raised.
pub(crate) fn int_prefix_consumed_all(s: &str) -> bool {
    // Go `StrToInt`/`StrToUint` trim BOTH ends before scanning, so trailing
    // space is not a truncation; `CAST('  12  ' AS SIGNED)` is exact.
    let trimmed = s.trim();
    let mut valid_len = 0;
    for (i, byte) in trimmed.bytes().enumerate() {
        if (byte == b'+' || byte == b'-') && i == 0 {
            continue;
        }
        if byte.is_ascii_digit() {
            valid_len = i + 1;
            continue;
        }
        break;
    }
    valid_len != 0 && valid_len == trimmed.len()
}

/// Applies the statement's truncation level when `CAST(<string> AS
/// SIGNED/UNSIGNED)` did not consume the whole operand, which is the point
/// Go's `getValidIntPrefix` calls `Context.HandleTruncate`.
///
/// The `CAST(<number> AS SIGNED)` clamp's own warning, which is the only
/// thing that says the value saturated:
///
///  * `builtinCastRealAsIntSig` (`builtin_cast.go:1367`) returns
///    `ConvertFloatToInt`'s `ErrOverflow` (1690)
///    "constant %v overflows bigint" -- printing the ROUNDED value.
///  * `builtinCastDecimalAsIntSig` (`:1566`) aliases its own `ErrOverflow`
///    to `ErrTruncatedWrongVal` (1292)
///    "Truncated incorrect DECIMAL value: '%v'" -- printing the ORIGINAL
///    decimal, not the rounded one.
///
/// Both are WARNINGS in the default (strict) sql_mode, captured: reads never
/// fail. Go compares against `float64(upperBound)`, so the check is exact
/// only in `f64`: `CAST(9223372036854775806.9e0 AS SIGNED)` is `i64::MAX`
/// with NO warning, because the bound itself rounds up to the same `f64`.
/// Go's `val >= float64(upperBound)` spares exactly that equal case and
/// `val < float64(lowerBound)` is strict, which is why the range below is
/// INCLUSIVE at both ends.
///
/// `RoundFloat` is `math.RoundToEven`, mirrored here, but NO input can
/// observe it: an `f64` keeps a fractional part only below 2^52, while this
/// arm fires only past 2^63, so the rounding is the identity for both the
/// comparison and the printed text. It is kept because Go rounds; a fixture
/// that pins it cannot exist.
///
/// SIGNED only. Go's UNSIGNED target takes the other branch of the same
/// signature (`ConvertFloatToUint`/`MyDecimal.ToUint`), whose warning
/// [`to_u64_unsigned`] already raises -- calling both would double it.
fn report_signed_overflow(v: &Datum, ctx: &dyn crate::Columns) {
    match v {
        Datum::Real(value) | Datum::Float32(value) => {
            let rounded = value.round_ties_even();
            if !(i64::MIN as f64..=i64::MAX as f64).contains(&rounded) {
                ctx.append_warning(
                    1690,
                    &format!(
                        "constant {} overflows bigint",
                        tidb_datatype::format_float_g_shortest(rounded)
                    ),
                );
            }
        }
        Datum::Decimal(value) if value.round_to_i64().is_none() => ctx.append_warning(
            1292,
            &format!("Truncated incorrect DECIMAL value: '{value}'"),
        ),
        Datum::String(value) => {
            if let Ok(text) = value.as_utf8() {
                report_positive_string_signed_complement(text, ctx);
            }
        }
        Datum::Bytes(value) => {
            if let Ok(text) = std::str::from_utf8(value) {
                report_positive_string_signed_complement(text, ctx);
            }
        }
        _ => {}
    }
}

fn report_positive_string_signed_complement(text: &str, ctx: &dyn crate::Columns) {
    if !int_prefix_consumed_all(text) {
        return;
    }
    let trimmed = text.trim();
    if trimmed.starts_with('-') {
        return;
    }
    let digits = trimmed.strip_prefix('+').unwrap_or(trimmed);
    if digits
        .parse::<u64>()
        .is_ok_and(|value| value > i64::MAX as u64)
    {
        ctx.append_warning(
            8030,
            "Cast to signed converted positive out-of-range integer to its negative complement",
        );
    }
}

/// Only a string-valued operand reaches Go's `builtinCastStringAsIntSig`;
/// the numeric signatures have their own, overflow-shaped diagnostic, in
/// [`report_signed_overflow`].
pub(crate) fn report_int_truncation(v: &Datum, ctx: &dyn crate::Columns) -> Result<(), EvalError> {
    // go re-reads a JSON document's MarshalJSON text through the same
    // string-integer scanner (`builtinCastJSONAsIntSig`'s StrToInt), so a
    // document without an integer prefix warns exactly like a string one
    // (captured: CAST(a AS UNSIGNED) over the object document warns
    // `Truncated incorrect INTEGER value: '{...}'`).
    let json_text;
    let text = match v {
        Datum::String(value) => value.as_utf8().ok(),
        Datum::Bytes(value) => std::str::from_utf8(value).ok(),
        // Only the STRUCTURED documents (object/array) re-read through the
        // string scanner: go converts a JSON boolean/number directly (the
        // `false` document casts to 0 silently -- captured g-json2), so
        // stringifying those over-warned.
        Datum::Json(value)
            if matches!(
                value.type_code(),
                tidb_datatype::JSON_TYPE_CODE_OBJECT | tidb_datatype::JSON_TYPE_CODE_ARRAY
            ) =>
        {
            json_text = value.to_string();
            Some(json_text.as_str())
        }
        _ => None,
    };
    match text {
        Some(text)
            if !int_prefix_consumed_all(text) || signed_string_integer_parse_overflows(text) =>
        {
            ctx.handle_truncate(&format!(
                "Truncated incorrect INTEGER value: '{}'",
                tidb_datatype::warning_subject_byte_cap(text.trim())
            ))
        }
        _ => Ok(()),
    }
}

/// Reports Go's `ErrCastNegIntAsUnsigned` for a negative integer string.
///
/// `builtinCastStringAsIntSig` emits this advisory only after `StrToInt`
/// succeeds. A malformed or out-of-range prefix therefore keeps the normal
/// truncation/overflow warning and does not add a second 8031 event.
fn report_negative_string_unsigned(v: &Datum, ctx: &dyn crate::Columns) {
    let text = match v {
        Datum::String(value) => value.as_utf8().ok(),
        Datum::Bytes(value) => std::str::from_utf8(value).ok(),
        _ => None,
    };
    let Some(text) = text else {
        return;
    };
    let trimmed = text.trim();
    if trimmed.len() <= 1
        || !trimmed.starts_with('-')
        || !int_prefix_consumed_all(trimmed)
        || trimmed.parse::<i64>().is_err()
    {
        return;
    }
    ctx.append_warning(
        8031,
        "Cast to unsigned converted negative integer to it's positive complement",
    );
}

/// Maps `MyDecimal.FromString`'s non-overflow parse dispositions to the
/// warning emitted by Go's string-to-decimal cast signature. The parsed value
/// is still retained (including a valid prefix); only a completely invalid or
/// truncated suffix contributes this statement warning.
pub(crate) fn report_decimal_input_truncation(v: &Datum, ctx: &dyn crate::Columns) {
    let text = match v {
        Datum::String(value) => value.as_utf8().ok(),
        Datum::Bytes(value) => std::str::from_utf8(value).ok(),
        _ => None,
    };
    let Some(text) = text else {
        return;
    };
    let trimmed = text.trim();
    let (_, parse_error) = Decimal::parse_mysql(trimmed);
    if matches!(
        parse_error,
        Some(tidb_datatype::DecimalParseError::Overflow)
    ) {
        // go's string-to-decimal sig hands the parse's ErrOverflow straight
        // to `ec.HandleError` without format args, so the appended row keeps
        // the raw "%s value is out of range in '%s'" template (oracle:
        // CAST('1e300' AS DECIMAL)). The value is still retained (saturated);
        // the production clamp below reports its own formatted row.
        ctx.append_warning(1690, "%s value is out of range in '%s'");
    }
    if matches!(
        parse_error,
        Some(
            tidb_datatype::DecimalParseError::Truncated
                | tidb_datatype::DecimalParseError::BadNumber
                | tidb_datatype::DecimalParseError::TruncatedWrongValue
        )
    ) {
        ctx.append_warning(
            1292,
            &format!(
                "Truncated incorrect DECIMAL value: '{}'",
                tidb_datatype::warning_subject_byte_cap(trimmed)
            ),
        );
    }
}

fn signed_string_integer_parse_overflows(text: &str) -> bool {
    if !int_prefix_consumed_all(text) {
        return false;
    }
    let trimmed = text.trim();
    if trimmed.starts_with('-') {
        trimmed.parse::<i64>().is_err()
    } else {
        trimmed
            .strip_prefix('+')
            .unwrap_or(trimmed)
            .parse::<u64>()
            .is_err()
    }
}

pub(crate) fn str_int_prefix(s: &str) -> i64 {
    let s = s.trim_start();
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return 0;
    }
    if negative {
        format!("-{digits}").parse::<i64>().unwrap_or(i64::MIN)
    } else {
        digits.parse::<u64>().map_or(-1, |value| value as i64)
    }
}

fn decimal_prefix(s: &str) -> Decimal {
    let s = s.trim_start();
    let (negative, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s.strip_prefix('+').unwrap_or(s)),
    };
    let int_digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    let after_int = &rest[int_digits.len()..];
    let (frac_digits, after_frac) = match after_int.strip_prefix('.') {
        Some(r) => {
            let f: String = r.chars().take_while(char::is_ascii_digit).collect();
            let len = f.len();
            (f, &r[len..])
        }
        None => (String::new(), after_int),
    };
    if int_digits.is_empty() && frac_digits.is_empty() {
        return Decimal::from_int(0);
    }
    let base = if int_digits.is_empty() {
        format!("0.{frac_digits}")
    } else if frac_digits.is_empty() {
        int_digits.clone()
    } else {
        format!("{int_digits}.{frac_digits}")
    };
    let exponent = exponent_prefix(after_frac);
    if exponent != 0 {
        let base_f: f64 = base.parse().unwrap_or(0.0);
        let sign = if negative { -1.0 } else { 1.0 };
        let scaled = sign * base_f * 10f64.powi(exponent);
        // `f64`'s own `Display` never uses scientific notation, so this
        // recursive call always lands on the `exponent == 0` fast path
        // above — no risk of looping.
        return decimal_prefix(&scaled.to_string());
    }
    let mut d = Decimal::from_literal(&base);
    if negative {
        d = d.negate();
    }
    d
}

/// Scans an optional `e`/`E` exponent suffix (`[eE][+-]?digits`),
/// returning `0` if the text doesn't start with one — [`decimal_prefix`]'s
/// own helper.
fn exponent_prefix(s: &str) -> i32 {
    let Some(rest) = s.strip_prefix(['e', 'E']) else {
        return 0;
    };
    let (negative, rest) = match rest.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, rest.strip_prefix('+').unwrap_or(rest)),
    };
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return 0;
    }
    let mag: i32 = digits.parse().unwrap_or(0);
    if negative {
        -mag
    } else {
        mag
    }
}

/// `DOUBLE`/`FLOAT`'s own coercion: `Int`/`Decimal`/`Float` promote the
/// same way `crate::ops::to_f64` already does for binary arithmetic; a
/// `Str` source reuses [`decimal_prefix`]'s own numeric-prefix scan
/// (matching `DECIMAL`'s own string coercion, NOT `SIGNED`'s narrower
/// digit-run-only one — confirmed via `goeval`: `CAST('3.5e1abc' AS
/// DOUBLE)` is `35`, consuming the `.` and exponent `SIGNED`'s own scan
/// would stop before).
/// Go `builtinCastStringAsRealSig.evalReal`
/// (`pkg/expression/builtin_cast.go:1839`): the string operand goes through
/// `types.StrToFloat(ctx, val, true)`, whose trailing-garbage scan raises 1292
/// `Truncated incorrect DOUBLE value: '<trimmed>'` through the statement's
/// truncate policy -- once per evaluated row, exactly where the coercion
/// happens. Every non-string operand keeps the silent numeric conversion
/// below; Go reaches this signature only for a string-eval-type source.
///
/// `is_function_cast=true` is what makes an EMPTY string parse as `0`
/// silently (`getValidFloatPrefix`'s early return), matching the explicit
/// `CAST` this arm implements.
fn str_to_real_for_cast(v: &Datum, ctx: &dyn crate::Columns) -> Result<f64, EvalError> {
    if matches!(v, Datum::Json(_)) {
        let warnings = crate::constant::ConversionWarnings(ctx);
        let zone = ctx.time_zone();
        let context = tidb_datatype::ConversionContext::new(
            ctx.type_flags(),
            tidb_datatype::ConversionLocation::from_time_zone(&zone),
            &warnings,
        );
        let converted = v
            .to_f64_in_context(&context)
            .map_err(|_| EvalError::Unsupported("JSON real conversion"))?;
        if let Some(error) = converted.error {
            return Err(EvalError::Conversion(error));
        }
        return match converted.value {
            Datum::Real(value) => Ok(value),
            _ => Err(EvalError::Unsupported("JSON real conversion result")),
        };
    }
    let (text, type_word) = match v {
        Datum::String(value) => (
            String::from_utf8_lossy(value.bytes()).into_owned(),
            "DOUBLE",
        ),
        Datum::Bytes(value) => (String::from_utf8_lossy(value).into_owned(), "DOUBLE"),
        _ => return Ok(to_f64_for_cast(v)),
    };
    let converted = tidb_datatype::str_to_float(&text, true);
    if converted.event.is_some() {
        ctx.handle_truncate(&format!(
            "Truncated incorrect {} value: '{}'",
            type_word,
            tidb_datatype::float_warning_input(&text)
        ))?;
    }
    Ok(converted.value)
}

pub(crate) fn to_f64_for_cast(v: &Datum) -> f64 {
    match v {
        Datum::Int(i) => *i as f64,
        Datum::UInt(i) => *i as f64,
        Datum::Decimal(d) => d.to_f64(),
        Datum::Real(f) => *f,
        Datum::String(s) => s.as_utf8().map(decimal_prefix).map_or(0.0, |d| d.to_f64()),
        Datum::Bytes(s) => std::str::from_utf8(s)
            .map(decimal_prefix)
            .map_or(0.0, |d| d.to_f64()),
        Datum::Null | Datum::MinNotNull | Datum::MaxValue => unreachable!("guarded by caller"),
        other => other.to_f64().map_or(0.0, |converted| converted.value),
    }
}

/// `CAST(... AS DATE)` and `CAST(... AS DATETIME)`: Go
/// `builtinCastStringAsTimeSig.evalTime`.
///
/// The whole body is Go's, in order: `types.ParseTime` under the STATEMENT's
/// type flags, then `handleInvalidTimeError` on failure, then the separate
/// `NO_ZERO_DATE` rejection of an all-zero result, then the DATE truncation
/// of the clock fields.
///
/// # Why this does not use this crate's own date parser
///
/// It used to, and that was two sources of truth for one table. Go asks
/// `Time.Check` the zero-in-date and invalid-date questions with the
/// statement's flags; `time_fn::calendar::parse_date_ymd` asks NEITHER and
/// rejects a zero month unconditionally, so `CAST('2024-00-01' AS DATE)`
/// answered NULL where TiDB answers `2024-00-01`, and every failing cast
/// answered NULL with NO warning where TiDB warns 1292.
/// `tidb_datatype::parse_time` is the faithful port of Go's `ParseTime`,
/// flags included, and is the same parser the WRITE path converts through.
/// `parse_date_ymd` stays strict for its own callers, which Go does NOT
/// relax (see its doc).
///
/// # The flags are the READ path's, not the write path's
///
/// Go `ResetContextOfStmt`'s `*ast.SelectStmt` arm sets `IgnoreZeroInDate`
/// UNCONDITIONALLY -- a zero-in-date reads back intact even under the default
/// mode that refuses to STORE one -- and takes `IgnoreInvalidDateErr` from
/// `ALLOW_INVALID_DATES` alone. With `TruncateAsWarning` also set, a bad
/// value is a warning plus NULL, never a statement failure: READS NEVER FAIL,
/// in any sql_mode.
fn cast_to_time(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: i64,
) -> Result<Datum, EvalError> {
    let Some(time) = cast_to_time_value(v, source, ctx, kind, Some(fsp))? else {
        return Ok(Datum::Null);
    };
    Ok(Datum::Time(time))
}

/// Go's repeated DATE-target rule: preserve the calendar fields and clear the
/// clock before the typed value leaves the cast signature.
fn truncate_clock_for_date(
    mut time: tidb_datatype::Time,
    kind: tidb_datatype::TimeType,
) -> tidb_datatype::Time {
    if kind != tidb_datatype::TimeType::Date {
        return time;
    }
    let core = time.core_time();
    time.set_core_time(tidb_datatype::CoreTime::from_date(
        core.year() as u16,
        core.month(),
        core.day(),
        0,
        0,
        0,
        0,
    ));
    time
}

/// The `types.Time` Go's chosen `builtinCast*AsTimeSig` produces. `None` is
/// Go's NULL (any warning already raised). Both explicit CAST and the
/// argument-cast seam retain this native temporal value.
fn cast_to_time_value(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
) -> Result<Option<tidb_datatype::Time>, EvalError> {
    let json_string;
    let is_json_string =
        matches!(v, Datum::Json(value) if value.type_code() == JSON_TYPE_CODE_STRING);
    let v = if let Datum::Json(value) = v {
        match value.type_code() {
            JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => {
                // Go's native JSON calendar signature only changes type/FSP.
                // It neither rounds the packed clock nor runs Time.Convert.
                let mut time = value
                    .as_time(fsp.unwrap_or(6))
                    .map_err(|_| EvalError::Unsupported("JSON calendar payload"))?;
                time.set_kind(kind);
                return Ok(Some(truncate_clock_for_date(time, kind)));
            }
            JSON_TYPE_CODE_DURATION => {
                let duration = value
                    .as_duration()
                    .map_err(|_| EvalError::Unsupported("JSON duration payload"))?;
                return cast_to_time_value(&Datum::Duration(duration), None, ctx, kind, fsp);
            }
            JSON_TYPE_CODE_STRING => {
                json_string = Datum::new_string(
                    value
                        .unquote()
                        .map_err(|_| EvalError::Unsupported("JSON string payload"))?,
                );
                &json_string
            }
            _ => {
                let label = match kind {
                    tidb_datatype::TimeType::Date => "date",
                    tidb_datatype::TimeType::DateTime => "datetime",
                    tidb_datatype::TimeType::Timestamp => "timestamp",
                };
                ctx.handle_truncate(&format!("Truncated incorrect {label} value: '{value}'"))?;
                return Ok(None);
            }
        }
    } else {
        v
    };
    if let Datum::Time(value) = v {
        let flags = ctx.type_flags();
        let zone = ctx.time_zone();
        let (time, adjusted) = match value.convert_kind(
            kind,
            flags.ignore_zero_in_date_err(),
            flags.ignore_invalid_date_err(),
            &zone,
        ) {
            Ok(value) => value,
            Err(_) => {
                ctx.handle_truncate(&format!("Incorrect time value: '{:?}'", value.core_time()))?;
                return Ok(None);
            }
        };
        if adjusted {
            ctx.append_warning(8179, &format!("Timestamp is not valid, since it is in Daylight Saving Time transition '{:?}' for time zone '{}'", value.core_time(), zone.dag_zone().0));
        }
        let time = match fsp {
            Some(fsp) => time
                .round_frac(fsp, &zone)
                .map_err(|_| EvalError::Unsupported("temporal cast rounding"))?,
            None => time,
        };
        return Ok(Some(truncate_clock_for_date(time, kind)));
    }
    // A `YEAR` source is the one case the datum kind cannot speak for. Go
    // `builtinCastIntAsTimeSig.evalTime` (`builtin_cast.go:1127-1131`) asks the
    // ARGUMENT'S TYPE, not the integer's digits:
    //
    //   if b.args[0].GetType(ctx).GetType() == mysql.TypeYear {
    //       res, err = types.ParseTimeFromYear(val)
    //   } else {
    //       res, err = types.ParseTimeFromNum(typeCtx(ctx), val, ...)
    //   }
    //
    // and `types.ParseTimeFromYear` (`time.go:2072-2081`) INJECTS the value as
    // the year FIELD -- `FromDate(int(year), 0, 0, 0, 0, 0, 0)`, so `2018` is
    // `2018-00-00 00:00:00` -- with `0` mapping to the zero date typed
    // `mysql.TypeDate`. Routing that same `2018` through `ParseTimeFromNum`,
    // which reads an int as a packed `YYYYMMDD`, FAILS and yields NULL. Every
    // other INT source keeps `ParseTimeFromNum` below.
    if let Some(year) = year_source_value(v, source) {
        let time = tidb_datatype::parse_time_from_year(year)
            .map_err(|_| EvalError::Unsupported("a YEAR value outside the year range"))?;
        return Ok(Some(time));
    }
    // A DURATION source is the second kind whose text cannot speak for it. Go
    // `builtinCastDurationAsTimeSig.evalTime` (`builtin_cast.go:2275-2291`)
    // never parses `20:00:01` as a wall clock; it calls
    // `val.ConvertToTimeWithTimestamp(tc, b.tp.GetType(), ts)`, which takes
    // the CALENDAR DATE of the statement's own timestamp and mixes the
    // elapsed time into it (`types/time.go:1500-1507`). Routing the text
    // through `ParseTime` instead reads the `20` as a YEAR.
    //
    // Neither half of this is visible in the recorded corpus: every recorded
    // statement that reaches it has the OTHER argument winning, so any wrong
    // conversion still prints the recorded answer. Both are pinned by
    // `a_duration_beside_a_temporal_literal_lands_on_the_statement_date` in
    // `tidb-session`, which puts the duration on the winning side and then
    // moves the session zone across the date line.
    //
    // The two date-mode flags SURVIVED their own mutation (hardcoding both to
    // `false` moves nothing): `mixDateAndDuration` always starts from a real
    // calendar date, so no zero or invalid component can arise for them to
    // rule on. They are passed because Go passes its `ctx`, not because a
    // value distinguishes them.
    if let Datum::Duration(duration) = v {
        let modes = ctx.date_modes();
        let (utc_secs, nanos, tz_offset) = ctx
            .now()
            .ok_or(EvalError::Unsupported("no statement clock for a TIME cast"))?;
        // Go reads the calendar date of `ts.In(ctx.Location())`; `now`'s third
        // field is that location's offset AT that instant, so a fixed offset
        // names the same civil day without re-resolving the zone.
        let zone = chrono::FixedOffset::east_opt(tz_offset).ok_or(EvalError::Unsupported(
            "session time-zone offset out of range",
        ))?;
        let Some(stamp) = chrono::DateTime::from_timestamp(utc_secs, nanos) else {
            return Ok(None);
        };
        return Ok(duration
            .convert_to_time(
                stamp.with_timezone(&zone),
                kind,
                !modes.no_zero_in_date,
                modes.allow_invalid_dates,
            )
            .and_then(|time| match fsp {
                Some(fsp) => time.round_frac(fsp, &ctx.time_zone()),
                None => Ok(time),
            })
            .ok());
    }
    let Some(s) = coerce_str(v)? else {
        return Ok(None);
    };
    let modes = ctx.date_modes();
    // Go routes each source TYPE to its own parser, not its text: only the
    // STRING/BYTES signatures (`builtinCastStringAsTimeSig`) parse the wall-
    // clock text through `ParseTime`. An INT source takes `ParseTimeFromNum`,
    // and a REAL/DECIMAL source takes `ParseTimeFromFloatString`, both of which
    // read the value as TiDB's packed `YYYYMMDD[HHMMSS]` NUMBER -- not as a
    // free-form date string. Funnelling a decimal through the string parser is
    // what made `cast(121212.1111 as datetime)` absorb `.1111` as a clock
    // (`2012-12-12 11:11:00`) and `cast(111.1 as datetime)` fail outright,
    // where TiDB answers `2012-12-12 00:00:00` and `2000-01-11 00:00:00`
    // (`expression/cast`). The parser choice mirrors `Datum::convert_to_time`,
    // the faithful write-path port.
    let parsed = parse_time_by_source(v, &s, kind, fsp, ctx.type_flags(), &ctx.time_zone());
    let Ok((time, truncated, dst_adjusted)) = parsed else {
        // go routes each source TYPE to its own parser and its own warning:
        // the STRING sources warn `Incorrect datetime value: '<text>'`; the
        // numeric sources parse the INT64/FLOAT reinterpretations and warn
        // `Incorrect time value: '<int64>'` (a u64 overflow reads -1).
        if matches!(v, Datum::String(_) | Datum::Bytes(_)) {
            invalid_time_warning(ctx, &s, fsp.unwrap_or(0));
        } else if matches!(v, Datum::Decimal(_) | Datum::Real(_) | Datum::Float32(_)) {
            // The DECIMAL/REAL sources read the value through
            // `ParseTimeFromFloatString` -- the same wall-clock TEXT parser
            // the string sources use -- so a failure names the value with
            // the DATETIME word and the full decimal text (`cast(2.5 as
            // datetime)` warns `Incorrect datetime value: '2.5'`, not a
            // truncated integer).
            invalid_time_warning(ctx, &s, fsp.unwrap_or(0));
        } else {
            // go ParseTimeFromNum consumes the number's INT64 WRAP: a u64
            // overflow wraps to -1 (not the saturating i64::MAX).
            let signed = match v {
                Datum::UInt(n) => format!("{}", *n as i64),
                _ => v
                    .to_i64()
                    .map(|converted| format!("{}", converted.value))
                    .unwrap_or_else(|_| s.clone()),
            };
            ctx.append_warning(1292, &format!("Incorrect time value: '{signed}'"));
        }
        return Ok(None);
    };
    if truncated {
        ctx.append_warning(
            1292,
            &format!(
                "Truncated incorrect datetime value: '{}'",
                tidb_datatype::warning_subject_byte_cap(&s)
            ),
        );
    }
    if dst_adjusted {
        // handleInvalidTimeError does not downgrade the DST error. Unlike
        // Time.Convert's warning, ParseTime's returned error fails this cast.
        return Err(EvalError::Conversion(tidb_datatype::ERR_TIMESTAMP_IN_DST_TRANSITION.generate(format!(
            "Timestamp is not valid, since it is in Daylight Saving Time transition '{}' for time zone '{}'", s, ctx.time_zone().dag_zone().0))));
    }
    // Go's SECOND check is the STRING signature's ALONE
    // (`builtinCastStringAsTimeSig`: `res.IsZero() && HasNoZeroDateMode()`).
    // The INT/REAL/DECIMAL signatures have no such rejection -- a numeric zero
    // is the zero time, not NULL (Go `#11203`), so `cast(0 as datetime)` and a
    // `0`-valued double/decimal column read `0000-00-00 00:00:00`, matching
    // `expression/cast`. Gating this on the text sources keeps the numeric
    // sources on Go's own no-rejection path.
    if !is_json_string
        && matches!(v, Datum::String(_) | Datum::Bytes(_))
        && time.is_zero()
        && modes.no_zero_date
    {
        // Only the string signature performs this SQL-mode check, using
        // the parsed target's own type and precision in its diagnostic.
        ctx.handle_truncate(&format!("Incorrect datetime value: '{time}'"))?;
        return Ok(None);
    }
    Ok(Some(truncate_clock_for_date(time, kind)))
}

pub(crate) fn parse_computed_time(
    value: &Datum,
    ctx: &dyn crate::Columns,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
) -> Result<Datum, EvalError> {
    Ok(cast_to_time_value(value, None, ctx, kind, fsp)?.map_or(Datum::Null, Datum::Time))
}

pub(crate) fn parse_computed_duration(
    value: &Datum,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    let fsp = value.sql_string().ok().map_or(0, |text| {
        text.rsplit_once('.')
            .map_or(0, |(_, fraction)| fraction.len().min(6) as i64)
    });
    cast_to_duration(value, None, ctx, fsp)
}

/// Go's `WrapWithCastAsTime(ctx, expr, types.NewFieldType(mysql.TypeDatetime))`
/// (`pkg/expression/builtin_cast.go:2817`), applied to an argument's VALUE
/// because this tier has no build-time expression rewrite to hang the cast on.
///
/// Go's early return is the whole of the special-casing:
///
/// ```go
/// exprTp := expr.GetType(ctx.GetEvalCtx()).GetType()
/// if tp.GetType() == exprTp {
///     return expr
/// } else if (exprTp == mysql.TypeDate || exprTp == mysql.TypeTimestamp) && tp.GetType() == mysql.TypeDatetime {
///     return expr
/// }
/// ```
///
/// -- i.e. a DATE, DATETIME or TIMESTAMP expression is handed through
/// untouched. In this tier those three are exactly the expressions that
/// evaluate to a [`Datum::Time`], so ONE pass-through arm covers all of Go's
/// early return; everything else goes through the cast Go builds, which is
/// [`cast_to_time_value`] (the same function `CAST(x AS DATETIME)` uses,
/// YEAR hole and all).
pub(crate) fn cast_arg_as_datetime(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(v, Datum::Time(_) | Datum::Null) {
        return Ok(v.clone());
    }
    Ok(
        cast_to_time_value(v, source, ctx, tidb_datatype::TimeType::DateTime, None)?
            .map_or(Datum::Null, Datum::Time),
    )
}

/// Go `WrapWithCastAsInt(ctx, expr, nil)` (`builtin_cast.go:2666-2698`),
/// applied to an argument's VALUE for the same reason
/// [`cast_arg_as_datetime`] is.
///
/// Go's own body, in full:
///
/// ```go
/// if expr.GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeEnum {
///     ... expr.GetType(ctx.GetEvalCtx()).AddFlag(mysql.EnumSetAsIntFlag)
/// }
/// if expr.GetType(ctx.GetEvalCtx()).EvalType() == types.ETInt {
///     return expr
/// }
/// tp := types.NewFieldType(mysql.TypeLonglong)
/// ...
/// if targetType == nil {
///     tp.AddFlag(expr.GetType(ctx.GetEvalCtx()).GetFlag() & mysql.UnsignedFlag)
/// }
/// return BuildCastFunction(ctx, expr, tp)
/// ```
///
/// Three of Go's rules land here, and every one of them is a KIND test, not a
/// per-builtin condition:
///
///  * **The early return.** `EvalType() == types.ETInt` covers the integer
///    types and, through `FieldType.EvalType`'s own switch
///    (`pkg/parser/types/field_type.go:417-441`), `mysql.TypeBit` and
///    `mysql.TypeYear` as well. In this tier those are exactly the arguments
///    that evaluate to an [`Datum::Int`]/[`Datum::UInt`] — a `YEAR` reaches
///    the signature as its integer either way, so unlike the ETDatetime rung
///    the static type buys NOTHING here.
///
///  * **The hybrid short-circuit.** `mysql.TypeEnum` gets
///    `EnumSetAsIntFlag`, which flips its `EvalType()` to `ETInt` and takes
///    the early return with the ORDINAL. `mysql.TypeSet` does NOT get the
///    flag, but the cast Go then builds routes it right back to the same
///    reading: `castAsIntFunctionClass.getFunction` opens with
///    `if args[0].GetType(ctx.GetEvalCtx()).Hybrid() || IsBinaryLiteral(args[0]) {
///    sig = &builtinCastIntAsIntSig{bf} }` (`builtin_cast.go:146-147`), whose
///    body is `b.args[0].EvalInt`. ENUM, SET, BIT and a bit/hex LITERAL
///    therefore all reach the signature as their ordinal or bit integer, and
///    [`Datum::to_i64_in`](tidb_datatype::Datum::to_i64_in) already reads all
///    four that way -- so the ordinary cast below is already Go's answer for
///    them and needs no arm of its own.
///
///  * **The unsigned inheritance.** `targetType` is `nil` at every
///    `newBaseBuiltinFuncWithTp` call site (`builtin.go:202`), so the built
///    cast is `UNSIGNED` exactly when the SOURCE type is, and that flag is
///    what `builtinTruncateIntSig` reads back out of
///    `b.args[1].GetType(ctx).GetFlag()` (`builtin_math.go:2166`). A tier
///    without the source type therefore answers SIGNED, which is Go's answer
///    for every argument that is not an unsigned non-integer.
///
/// Confirmed against real TiDB (`gorun`) over an `enum('x','y','z')` holding
/// `'y'`, a `set('a','b','c')` holding `'a,c'` and a `bit(8)` holding
/// `b'00000011'`: `make_set(e,'p','q','r')` is `q` (ordinal 2),
/// `make_set(s,'p','q','r')` is `p,r` (bits 5) and `make_set(b,'p','q','r')`
/// is `p,q` (bits 3).
pub(crate) fn cast_arg_as_int(
    v: &Datum,
    source: Option<&tidb_datatype::FieldType>,
    ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    if matches!(v, Datum::Int(_) | Datum::UInt(_) | Datum::Null) {
        return Ok(v.clone());
    }
    let cast = if source.is_some_and(tidb_datatype::FieldType::is_unsigned) {
        CastType::Unsigned
    } else {
        CastType::Signed
    };
    eval_cast(&cast, v.clone(), source, ctx)
}

/// Go `WrapWithCastAsString(ctx, expr)` (`builtin_cast.go:2769-2813`), applied
/// to an argument's VALUE for the same reason [`cast_arg_as_datetime`] is.
///
/// Go's body is one early return and then a pile of RESULT-TYPE arithmetic:
///
/// ```go
/// exprTp := expr.GetType(ctx.GetEvalCtx())
/// if exprTp.EvalType() == types.ETString {
///     return expr
/// }
/// argLen := exprTp.GetFlen()
/// ... // argLen adjustments, then charset/collation on the built `tp`
/// return BuildCastFunction(ctx, expr, tp)
/// ```
///
/// Everything after the early return sets `tp`'s FLEN and CHARSET, which are
/// metadata: none of the `argLen` arms can truncate, because every one of them
/// is at least as wide as the rendering it describes (`mysql.MaxIntWidth` for
/// an integer, `GetFlen()+3` for a decimal, `-1` -- unspecified -- for a
/// float). So at the VALUE seam this cast is the early return plus "render
/// the value's text", and the two things worth transcribing are which values
/// take the early return and what BIT renders as.
///
///  * **The early return** is `EvalType() == types.ETString`, and
///    `FieldType.EvalType` (`pkg/parser/types/field_type.go:436-441`) puts
///    `mysql.TypeEnum` and `mysql.TypeSet` there unless they carry
///    `EnumSetAsIntFlag` -- a flag only `WrapWithCastAsInt` ever adds. An
///    ENUM or SET argument is therefore NOT wrapped, and the signature body
///    reads it with `EvalString`, which is its NAME. Captured from real TiDB
///    (`gorun`) over an `enum('{}','[1]','x')` holding `'{}'`: `quote(e)` is
///    `'{}'` and `ltrim(e)` is `{}` -- the name, never the ordinal `1`. This
///    is the exact OPPOSITE of [`cast_arg_as_int`]'s hybrid arm, where the
///    same column reaches the signature as its ordinal.
///
///  * **BIT is the one hybrid that is NOT string-typed**: `mysql.TypeBit` is
///    `ETInt`, so it does not take the early return. The cast Go then builds
///    lands on `castAsStringFunctionClass.getFunction`'s own hybrid arm
///    (`builtin_cast.go:315-321`), whose `castBitAsUnBinary` test is false
///    here because `WrapWithCastAsString` already set the target charset to
///    `charset.CharsetBin` for `TypeBit` (`:2801-2804`) -- so the signature
///    is `builtinCastStringAsStringSig` and the value is the bit's RAW BYTES,
///    not its decimal digits. Captured over a `bit(8)` holding `b'11111111'`:
///    `hex(ltrim(b))` is `FF`, and `hex(quote(b))` is `27EFBFBD27` -- one
///    0xFF byte that `Quote`'s own `[]rune` conversion then replaces.
///
/// `source` is unused: unlike [`cast_arg_as_datetime`]'s `YEAR` and
/// [`cast_arg_as_int`]'s `UNSIGNED`, nothing this cast produces depends on a
/// fact the datum does not already carry.
pub(crate) fn cast_arg_as_string(
    v: &Datum,
    _source: Option<&tidb_datatype::FieldType>,
    _ctx: &dyn crate::Columns,
) -> Result<Datum, EvalError> {
    match v {
        // Go's early return: every `types.ETString` eval type, which is every
        // string kind plus the two string-typed hybrids. Passing the datum
        // through UNCHANGED (rather than flattening it to bytes here) is what
        // keeps a `Datum::String`'s collation and a `Datum::Bytes`'s binary
        // signature readable by the body -- see `crate::string_signature`.
        Datum::Null
        | Datum::String(_)
        | Datum::Bytes(_)
        | Datum::Enum(..)
        | Datum::Set(..)
        | Datum::BinaryLiteral(_) => Ok(v.clone()),
        // The BIT arm above: raw bytes under the binary charset Go's `tp`
        // was given, which in this tier is `Datum::Bytes`.
        Datum::Bit(bits) => Ok(Datum::new_bytes(bits.as_bytes().to_vec())),
        // Everything else takes one of `castAsStringFunctionClass`'s
        // per-source signatures, all of which render the value's own text
        // under the connection charset -- which is exactly what
        // `crate::coerce::coerce_str_bytes` already is.
        _ => Ok(crate::coerce::coerce_str_bytes(v)?.map_or(Datum::Null, Datum::new_string)),
    }
}

/// The result type Go's `WrapWithCastAsString` assigns to `source`.
///
/// String-typed arguments take the source function's early return unchanged.
/// Every other type becomes `VAR_STRING`: an explicit collation survives,
/// BIT stays binary, and all remaining values use the connection charset.
/// The width is the same source-backed calculation used by CONCAT metadata.
pub(crate) fn cast_arg_as_string_type(
    source: &tidb_datatype::FieldType,
    explicit_collation: bool,
    connection: (&str, &str),
) -> tidb_datatype::FieldType {
    if source.eval_type() == tidb_datatype::EvalType::String {
        return source.clone();
    }
    let mut target = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    if explicit_collation {
        target.set_charset_name(source.charset_name());
        target.set_collation_name(source.collation_name());
    } else if source.code() == tidb_datatype::FieldTypeCode::Bit {
        target.set_charset_name("binary");
        target.set_collation_name("binary");
    } else {
        let (charset, collation) = connection;
        target.set_charset_name(charset);
        target.set_collation_name(collation);
    }
    target.set_flen(crate::rewriter::result_type::string_cast_flen(source));
    target.set_decimal(tidb_datatype::UNSPECIFIED_LENGTH);
    target
}

/// The integer a `YEAR`-typed operand carries, or `None` when the operand is
/// not a `YEAR` at all.
///
/// The type test is Go's own (`b.args[0].GetType(ctx).GetType() ==
/// mysql.TypeYear`); the kind test is this tier's, because a `YEAR` expression
/// always evaluates to an integer and anything else under a `YEAR` field type
/// is a value this tier produced, not one Go's `EvalInt` could have returned.
fn year_source_value(v: &Datum, source: Option<&tidb_datatype::FieldType>) -> Option<i64> {
    if source?.code() != tidb_datatype::FieldTypeCode::Year {
        return None;
    }
    match v {
        Datum::Int(value) => Some(*value),
        Datum::UInt(value) => i64::try_from(*value).ok(),
        _ => None,
    }
}

/// Parses one cast operand into a `Time`, choosing the parser by SOURCE TYPE
/// the way Go's per-signature `builtinCast*AsTimeSig` split does (see
/// [`cast_to_time`]'s doc for why the text is not enough). The read-path flags
/// are the string signature's own: `allow_zero_in_date` is UNCONDITIONALLY
/// `true` (a SELECT reads a zero-in-date back intact), and `allow_invalid_date`
/// follows `ALLOW_INVALID_DATES`. `Err(())` is Go's parse failure, which the
/// caller turns into a 1292 warning plus NULL.
///
/// The parser routing mirrors `Datum::convert_to_time`, the faithful write-path
/// port: INT/UINT -> `parse_time_from_num`, DECIMAL -> `parse_time_from_decimal`,
/// REAL/FLOAT -> `parse_time_from_float64`. A UINT beyond `i64::MAX` cannot be a
/// packed datetime and is a parse failure. The float/decimal parsers classify
/// DATE-vs-DATETIME by digit count, so the target `kind` is re-imposed with
/// `set_kind` -- exactly what `convert_to_time` does after those two parsers.
fn parse_time_by_source(
    v: &Datum,
    text: &str,
    kind: tidb_datatype::TimeType,
    fsp: Option<i64>,
    flags: ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<(tidb_datatype::Time, bool, bool), ()> {
    match v {
        Datum::Int(value) => tidb_datatype::parse_time_from_num(
            *value,
            kind,
            fsp.unwrap_or(0),
            flags.ignore_zero_in_date_err(),
            flags.ignore_invalid_date_err(),
            flags.ignore_zero_date_err(),
            zone,
        )
        .map(|parsed| (parsed.time, false, parsed.dst_adjusted))
        .map_err(|_| ()),
        Datum::UInt(value) => {
            let signed = i64::try_from(*value).map_err(|_| ())?;
            tidb_datatype::parse_time_from_num(
                signed,
                kind,
                fsp.unwrap_or(0),
                flags.ignore_zero_in_date_err(),
                flags.ignore_invalid_date_err(),
                flags.ignore_zero_date_err(),
                zone,
            )
            .map(|parsed| (parsed.time, false, parsed.dst_adjusted))
            .map_err(|_| ())
        }
        Datum::Decimal(value) => {
            let mut time = tidb_datatype::parse_time_from_decimal(
                value,
                flags.ignore_zero_in_date_err(),
                flags.ignore_invalid_date_err(),
                zone,
            )
            .map_err(|_| ())?;
            time.set_kind(kind);
            match fsp {
                Some(fsp) => time
                    .round_frac(fsp, zone)
                    .map(|time| (time, false, false))
                    .map_err(|_| ()),
                None => Ok((time, false, false)),
            }
        }
        Datum::Real(value) => real_to_time(*value, kind, fsp.unwrap_or(0), flags, zone)
            .map(|time| (time, false, false)),
        Datum::Float32(value) => real_to_time(*value, kind, fsp.unwrap_or(0), flags, zone)
            .map(|time| (time, false, false)),
        // STRING/BYTES and every other coercible source keep Go's
        // `builtinCastStringAsTimeSig` path: parse the wall-clock TEXT.
        //
        // The zone is the SESSION's, as Go's `builtinCastStringAsTimeSig` passes
        // `ctx.TypeCtx()`. It does more than TIMESTAMP's range check: a literal
        // whose fraction is wider than `fsp` ROUNDS, and Go applies that carry to
        // the INSTANT in `ctx.Location()`, so a carry landing on a DST transition
        // moves the wall clock by the offset change too. CAPTURED from real TiDB:
        // `cast('2011-03-13 01:59:59.9999999' as datetime)` is
        // `2011-03-13 02:00:00` under `time_zone='UTC'` and `03:00:00` under
        // `'America/Los_Angeles'` (02:00 does not exist there), and
        // `cast('2011-11-06 01:59:59.9999999' as datetime)` is `02:00:00` under
        // UTC and `01:00:00` there (the repeated hour). Hardcoding UTC returned
        // the UTC answer for every session.
        _ => tidb_datatype::parse_time(
            text,
            kind,
            fsp.unwrap_or_else(|| i64::from(tidb_datatype::get_fsp(text))),
            false,
            flags.ignore_zero_in_date_err(),
            flags.ignore_invalid_date_err(),
            zone,
        )
        .map(|parsed| (parsed.time, parsed.truncated, parsed.dst_adjusted))
        .map_err(|_| ()),
    }
}

/// REAL/FLOAT source shared by `Real` and `Float32`: Go
/// `builtinCastRealAsTimeSig` reads the float's packed-number form. `0.0` is
/// the zero time rather than a failure (Go's `#11203` guard); the float parser
/// already returns the zero time for a `0` integer part, so no special case is
/// needed here.
fn real_to_time(
    value: f64,
    kind: tidb_datatype::TimeType,
    fsp: i64,
    flags: ConversionFlags,
    zone: &tidb_datatype::SessionTimeZone,
) -> Result<tidb_datatype::Time, ()> {
    let mut time = tidb_datatype::parse_time_from_float64(
        value,
        flags.ignore_zero_in_date_err(),
        flags.ignore_invalid_date_err(),
        zone,
    )
    .map_err(|_| ())?;
    time.set_kind(kind);
    time.round_frac(fsp, zone).map_err(|_| ())
}

/// Go `handleInvalidTimeError` on the read path: `ErrWrongValue` (1292)
/// becomes a warning and the cast yields NULL.
fn invalid_time_warning(ctx: &dyn crate::Columns, input: &str, fsp: i64) {
    // go splits the failure classes by SHAPE (oracle-captured):
    //  * the ZERO date renders through the parsed value with its fsp
    //    (`'0000-00-00 00:00:00.000000'` for fsp 6 -- g-fsp);
    //  * a VALID calendar date prefix followed by trailing characters raises
    //    the VALUE class 8034 with the raw text (`CAST('2020-01-01x' AS
    //    DATE)` -- m17);
    //  * a calendar-INVALID date shape renders through its parsed parts
    //    without zero padding under the truncation class 1292
    //    (`'2020-2-30'` -- m4);
    //  * everything else is 1292 with the raw text (`'abc'` -- m1).
    let trimmed = input.trim();
    let head: Vec<&str> = trimmed.splitn(3, '-').collect();
    if head.len() == 3
        && !head[0].is_empty()
        && !head[1].is_empty()
        && head[0].bytes().all(|b| b.is_ascii_digit())
        && head[1].bytes().all(|b| b.is_ascii_digit())
    {
        let digits = |part: &str| -> i64 {
            part.bytes()
                .take_while(|byte| byte.is_ascii_digit())
                .fold(0i64, |acc, byte| acc * 10 + i64::from(byte - b'0'))
        };
        let (year, month) = (digits(head[0]), digits(head[1]));
        let day_digits = head[2]
            .bytes()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        let day = digits(&head[2][..day_digits]);
        if year == 0 && month == 0 && day == 0 {
            let fraction = if fsp > 0 {
                format!(".{:0width$}", 0, width = fsp as usize)
            } else {
                String::new()
            };
            ctx.append_warning(
                1292,
                &format!("Incorrect datetime value: '0000-00-00 00:00:00{fraction}'"),
            );
            return;
        }
        // Go `checkMonthDay` renders a month past 12 from the parsed parts,
        // unpadded, like an out-of-range day (`'2024-13-1'`).
        if month > 12 && head[2].bytes().all(|byte| byte.is_ascii_digit()) {
            let rendered = format!("{year}-{month}-{day}");
            ctx.append_warning(1292, &format!("Incorrect datetime value: '{rendered}'"));
            return;
        }
        if (1..=12).contains(&month) && (1..=31).contains(&day) {
            let days_in_month = match month {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                _ => {
                    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
                    if leap {
                        29
                    } else {
                        28
                    }
                }
            };
            if day <= days_in_month {
                // A valid calendar date prefix plus trailing characters.
                ctx.append_warning(8034, &format!("Incorrect datetime value: '{input}'"));
                return;
            }
            if head[2].bytes().all(|byte| byte.is_ascii_digit()) {
                let rendered = format!("{year}-{month}-{day}");
                ctx.append_warning(1292, &format!("Incorrect datetime value: '{rendered}'"));
                return;
            }
        }
    }
    ctx.append_warning(1292, &format!("Incorrect datetime value: '{input}'"));
}

/// `CAST(... AS YEAR)`: the operand's calendar year if it parses as a
/// date-shaped string (confirmed via `goeval`: `CAST('2021-01-01' AS YEAR)`
/// is `2021`), else a plain `SIGNED`-style integer coercion (confirmed via
/// `goeval`: `CAST('99' AS YEAR)` is `99` — NOT the two-digit-year century
/// pivot the `YEAR` COLUMN TYPE applies at storage time, a genuinely separate
/// rule this scalar CAST does not share).
///
/// A DURATION operand is the one exception to the datum-kind fallback. Go's
/// `builtinCastDurationAsIntSig` calls `Duration.ConvertToYear`, which mixes
/// the elapsed time into the statement clock's local calendar date. The
/// previous Rust path treated the duration as its packed integer (`125959`),
/// so it never observed either `ctx.now()` or the session time zone.
fn cast_to_year(v: &Datum, ctx: &dyn crate::Columns) -> Result<Datum, EvalError> {
    if let Datum::Duration(duration) = v {
        let (utc_secs, nanos, _) = ctx
            .now()
            .ok_or(EvalError::Unsupported("no statement clock for a YEAR cast"))?;
        let now = chrono::DateTime::<chrono::Utc>::from_timestamp(utc_secs, nanos)
            .ok_or(EvalError::Unsupported("statement clock is out of range"))?
            .with_timezone(&ctx.time_zone());
        let year = duration
            .convert_to_year(now, ctx.cast_time_to_year_through_concat())
            .map_err(|_| EvalError::Unsupported("duration to YEAR conversion"))?;
        return Ok(Datum::Int(year));
    }
    if let Some(s) = coerce_str(v)? {
        if let Some((y, _, _)) = parse_date_ymd(&s) {
            return Ok(Datum::Int(y));
        }
    }
    Ok(Datum::Int(to_i64_signed(v)))
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::*;
    use crate::context::NoColumns;

    struct WarningContext(RefCell<Vec<(u16, String)>>);

    impl crate::Columns for WarningContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn append_warning(&self, code: u16, message: &str) {
            self.0.borrow_mut().push((code, message.to_owned()));
        }
    }

    #[test]
    fn numeric_production_batch_binary_decimal_skips_target_fitting() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let value = Datum::BinaryLiteral(tidb_datatype::BinaryLiteral::from(vec![0xff, 0xff]));
        let result =
            eval_cast(&CastType::Decimal { flen: 1, scale: 0 }, value, None, &ctx).unwrap();
        assert_eq!(result.sql_string().unwrap(), "65535");
        assert!(ctx.0.borrow().is_empty());
    }

    #[test]
    fn string_owner_batch_json_binary_cast_does_not_pad() {
        let value = Datum::Json(tidb_datatype::BinaryJSON::parse("{}").unwrap());
        assert_eq!(
            eval_cast(&CastType::Binary { len: Some(8) }, value, None, &NoColumns)
                .unwrap()
                .as_raw_bytes(),
            Some(b"{}".as_slice())
        );
    }

    #[test]
    fn string_owner_batch_vector_binary_cast_does_not_pad() {
        let value =
            Datum::new_vector_float32(tidb_datatype::VectorFloat32::parse("[1,2]").unwrap());
        assert_eq!(
            eval_cast(&CastType::Binary { len: Some(8) }, value, None, &NoColumns)
                .unwrap()
                .as_raw_bytes(),
            Some(b"[1,2]".as_slice())
        );
    }

    #[test]
    fn string_owner_batch_typed_json_and_vector_skip_padding_and_packet_gate() {
        struct SmallPacket;
        impl crate::Columns for SmallPacket {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn max_allowed_packet(&self) -> u64 {
                4
            }
        }
        let target = FieldType::new(FieldTypeCode::String)
            .with_flen(8)
            .with_charset_name("binary")
            .with_collation_name("binary");
        for (value, expected) in [
            (
                Datum::Json(tidb_datatype::BinaryJSON::parse("{}").unwrap()),
                b"{}".as_slice(),
            ),
            (
                Datum::new_vector_float32(tidb_datatype::VectorFloat32::parse("[1,2]").unwrap()),
                b"[1,2]".as_slice(),
            ),
        ] {
            assert_eq!(
                eval_string_cast_with_type(value, None, &target, &SmallPacket)
                    .unwrap()
                    .as_raw_bytes(),
                Some(expected)
            );
        }
    }

    fn string_cast_policy(cast: CastType) {
        struct Policy(crate::context::ErrorLevel, RefCell<Vec<u16>>);
        impl crate::Columns for Policy {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn truncate_level(&self) -> crate::context::ErrorLevel {
                self.0
            }
            fn append_warning(&self, code: u16, _: &str) {
                self.1.borrow_mut().push(code);
            }
        }
        for level in [
            crate::context::ErrorLevel::Error,
            crate::context::ErrorLevel::Warn,
            crate::context::ErrorLevel::Ignore,
        ] {
            let ctx = Policy(level, RefCell::new(Vec::new()));
            let result = eval_cast(&cast, Datum::new_string("abc"), None, &ctx);
            if level == crate::context::ErrorLevel::Error {
                assert!(
                    matches!(result, Err(EvalError::Conversion(ref e)) if e.to_sql_error().code == 1406),
                    "{result:?}"
                );
            } else {
                assert_eq!(result.unwrap().as_raw_bytes(), Some(b"ab".as_slice()));
            }
            assert_eq!(
                *ctx.1.borrow(),
                if level == crate::context::ErrorLevel::Warn {
                    vec![1406]
                } else {
                    vec![]
                }
            );
        }
    }

    #[test]
    fn string_owner_batch_char_cast_obeys_statement_truncation_policy() {
        string_cast_policy(CastType::Char {
            len: Some(2),
            charset: None,
        });
    }

    #[test]
    fn string_owner_batch_binary_cast_obeys_statement_truncation_policy() {
        string_cast_policy(CastType::Binary { len: Some(2) });
    }

    #[test]
    fn truncated_datetime_cast_keeps_the_value_and_warns() {
        for (input, expected) in [
            ("1701020304.111", "2017-01-02 03:04:11"),
            ("150101.a", "2015-01-01 00:00:00"),
            ("150101.1a", "2015-01-01 01:00:00"),
            ("150101.1a1", "2015-01-01 01:00:00"),
            ("1101010101.111", "2011-01-01 01:01:11"),
            ("1101010101.11aaaaa", "2011-01-01 01:01:11"),
            ("1101010101.a1aaaaa", "2011-01-01 01:01:00"),
            ("970101.111a1", "1997-01-01 11:01:00"),
        ] {
            let ctx = WarningContext(RefCell::new(Vec::new()));
            let got = eval_cast(
                &CastType::DateTime { fsp: Some(0) },
                Datum::new_string(input),
                None,
                &ctx,
            )
            .expect("a truncated datetime remains a successful read cast");
            assert_eq!(render_time(&got), expected, "{input}");
            assert_eq!(
                ctx.0.borrow().as_slice(),
                &[(
                    1292,
                    format!("Truncated incorrect datetime value: '{input}'")
                )],
                "{input}"
            );
        }
    }

    #[test]
    fn signed_string_overflow_keeps_go_complement_and_warning_semantics() {
        for (input, expected, warning) in [
            (
                "18446744073709551614",
                -2,
                (
                    8030,
                    "Cast to signed converted positive out-of-range integer to its negative complement",
                ),
            ),
            (
                "18446744073709551616",
                -1,
                (1292, "Truncated incorrect INTEGER value: '18446744073709551616'"),
            ),
            (
                "-9223372036854775809",
                i64::MIN,
                (1292, "Truncated incorrect INTEGER value: '-9223372036854775809'"),
            ),
        ] {
            let ctx = WarningContext(RefCell::new(Vec::new()));
            assert_eq!(
                eval_cast(
                    &CastType::Signed,
                    Datum::new_string(input),
                    None,
                    &ctx,
                )
                .unwrap(),
                Datum::Int(expected),
                "{input}",
            );
            assert_eq!(
                ctx.0.borrow().as_slice(),
                &[(warning.0, warning.1.to_owned())],
                "{input}",
            );
        }
    }

    /// The string-to-JSON literal parse routes a `{}` OBJECT document
    /// through `builtinCastJSONAsIntSig`'s StrToInt re-read when cast to an
    /// integer — the object text has no integer prefix, so the 1292 row is
    /// mandatory (`g-t3`: `CAST(j AS SIGNED)` over the object column).
    #[test]
    fn json_object_to_int_cast_warns_truncation_like_string() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let json = Datum::new_json(tidb_datatype::BinaryJSON::parse("{}").expect("json"));
        let got = eval_cast(&CastType::Signed, json, None, &ctx)
            .expect("a truncated JSON int cast remains a successful read");
        assert_eq!(got, Datum::Int(0));
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1292, "Truncated incorrect INTEGER value: '{}'".to_owned())],
        );
    }

    /// Go's `ErrTruncatedWrongVal` template truncates the quoted value at
    /// 128 bytes (`"Truncated incorrect %-.64s value: '%-.128s'"`).
    #[test]
    fn double_cast_warning_value_truncates_at_128_bytes_like_go() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let long = format!("x{margin}", margin = "9".repeat(300));
        let got = eval_cast(
            &CastType::Double,
            Datum::new_string(long.clone()),
            None,
            &ctx,
        )
        .expect("a truncated DOUBLE cast remains a successful read");
        assert_eq!(got, Datum::Real(0.0));
        let warnings = ctx.0.borrow();
        let (code, text) = &warnings[0];
        assert_eq!(*code, 1292);
        // The quoted subject is the first 128 bytes of the input, not all 300.
        assert!(
            text.contains(&format!("value: '{}'", &long[..128])),
            "{text}"
        );
        assert!(
            !text.contains(&format!("value: '{}'", &long[..129])),
            "{text}"
        );
    }

    #[test]
    fn double_cast_warning_subject_stops_at_nul_like_go() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let got = eval_cast(&CastType::Double, Datum::new_string("\0 12"), None, &ctx)
            .expect("a truncated DOUBLE cast remains a successful read");
        assert_eq!(got, Datum::Real(0.0));
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1292, "Truncated incorrect DOUBLE value: ''".to_owned())]
        );
    }

    #[test]
    fn cast_decimal_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The wired bug: routing a decimal through the signed path saturated the
        // upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807). Go
        // rounds half-up then MyDecimal.ToUint, keeping the full u64 range.
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("10000000000000000000")),
                &NoColumns
            ),
            10_000_000_000_000_000_000,
            "one past i64::MAX is kept, not saturated"
        );
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("18446744073709551615")),
                &NoColumns
            ),
            u64::MAX
        );
        // Half-up rounding and the negative-to-zero rule are unchanged.
        assert_eq!(
            to_u64_unsigned(&Datum::Decimal(Decimal::from_literal("5.6")), &NoColumns),
            6
        );
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("5.6").negate()),
                &NoColumns
            ),
            0
        );
        // A signed-integer source still reinterprets its low 64 bits:
        // CAST(-5 AS UNSIGNED) stays 18446744073709551611, unaffected by the fix.
        assert_eq!(
            to_u64_unsigned(&Datum::Int(-5), &NoColumns),
            18_446_744_073_709_551_611
        );
    }

    #[test]
    fn cast_real_as_unsigned_keeps_the_upper_half_of_unsigned_bigint() {
        // The sibling wired bug: a real routed through the signed path saturated
        // the upper half of UNSIGNED BIGINT at i64::MAX (9223372036854775807).
        // Go rounds half-to-even (RoundFloat) then ConvertFloatToUint across the
        // full u64 range. 1e19 is exactly representable in f64.
        assert_eq!(
            to_u64_unsigned(&Datum::Real(1.0e19), &NoColumns),
            10_000_000_000_000_000_000,
            "a real past i64::MAX is kept, not saturated at i64::MAX"
        );
        // A magnitude past u64::MAX saturates to MaxUint64 (upperBound clamp).
        assert_eq!(to_u64_unsigned(&Datum::Real(1.0e30), &NoColumns), u64::MAX);
        // Half-to-even rounding (Go RoundFloat = math.RoundToEven), the same rule
        // the signed real path uses: 2.5 -> 2, 3.5 -> 4.
        assert_eq!(to_u64_unsigned(&Datum::Real(2.5), &NoColumns), 2);
        assert_eq!(to_u64_unsigned(&Datum::Real(3.5), &NoColumns), 4);
        // A negative real does NOT clamp to zero: Go's
        // `AllowNegativeToUnsigned` arm returns `uint64(int64(val))`, the same
        // low-64-bit reinterpretation the integer source above gets. This
        // assertion used to read `, 0)` and pinned the WRONG answer.
        // Captured (`goeval`): `cast(-1.5e0 as unsigned)` ->
        // 18446744073709551614, `cast(-1e0 as unsigned)` ->
        // 18446744073709551615, `cast(-1e300 as unsigned)` ->
        // 9223372036854775808.
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.5), &NoColumns),
            18_446_744_073_709_551_614
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.0), &NoColumns),
            18_446_744_073_709_551_615
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-5.6), &NoColumns),
            18_446_744_073_709_551_610
        );
        assert_eq!(
            to_u64_unsigned(&Datum::Real(-1.0e300), &NoColumns),
            9_223_372_036_854_775_808
        );
        // -0.4 ROUNDS to -0.0, which is not `< 0`, so it is the one negative
        // input that really is 0 -- with no warning either.
        assert_eq!(to_u64_unsigned(&Datum::Real(-0.4), &NoColumns), 0);
        // The DECIMAL source keeps Go's own opposite rule: negative -> 0.
        assert_eq!(
            to_u64_unsigned(
                &Datum::Decimal(Decimal::from_literal("1.5").negate()),
                &NoColumns
            ),
            0
        );
    }

    #[test]
    fn float32_unsigned_cast_reports_negative_overflow() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        assert_eq!(
            eval_cast(&CastType::Unsigned, Datum::Float32(-1.5), None, &ctx).unwrap(),
            Datum::UInt(18_446_744_073_709_551_614)
        );
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1690, "constant -2 overflows bigint".to_owned())]
        );
    }

    #[test]
    fn real_unsigned_cast_reports_positive_overflow() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        assert_eq!(
            eval_cast(&CastType::Unsigned, Datum::Real(1.0e30), None, &ctx).unwrap(),
            Datum::UInt(u64::MAX)
        );
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(1690, "constant 1e+30 overflows bigint".to_owned())]
        );
    }

    /// `CAST(str AS DATETIME)` rounds in the SESSION zone, not in UTC.
    ///
    /// Go's `builtinCastStringAsTimeSig` passes `ctx.TypeCtx()`, whose
    /// location the fractional-carry arm of `parseDatetime` applies the carry
    /// in. CAPTURED from real TiDB, both instants chosen so the carry lands
    /// exactly on a DST transition:
    ///
    /// ```text
    /// select cast('2011-03-13 01:59:59.9999999' as datetime)
    ///   time_zone='UTC'                 2011-03-13 02:00:00
    ///   time_zone='America/Los_Angeles' 2011-03-13 03:00:00
    /// select cast('2011-11-06 01:59:59.9999999' as datetime)
    ///   time_zone='UTC'                 2011-11-06 02:00:00
    ///   time_zone='America/Los_Angeles' 2011-11-06 01:00:00
    /// ```
    ///
    /// A four-zone probe over ordinary instants shows NO difference at all,
    /// which is why this pin uses the transition instants: an invariance
    /// probe here is a false negative.
    #[test]
    fn a_string_cast_to_datetime_rounds_in_the_session_zone() {
        use crate::Columns as _;
        struct Zoned(tidb_datatype::SessionTimeZone);
        impl crate::Columns for Zoned {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                self.0.clone()
            }
        }
        let utc = Zoned(tidb_datatype::SessionTimeZone::utc());
        let la = Zoned(tidb_datatype::SessionTimeZone::Named(
            chrono_tz::America::Los_Angeles,
        ));
        for (input, in_utc, in_la) in [
            (
                "2011-03-13 01:59:59.9999999",
                "2011-03-13 02:00:00",
                "2011-03-13 03:00:00",
            ),
            (
                "2011-11-06 01:59:59.9999999",
                "2011-11-06 02:00:00",
                "2011-11-06 01:00:00",
            ),
        ] {
            for (ctx, expected) in [(&utc, in_utc), (&la, in_la)] {
                let got = cast_to_time(
                    &Datum::new_string(input.to_string()),
                    None,
                    ctx,
                    tidb_datatype::TimeType::DateTime,
                    0,
                )
                .unwrap_or_else(|error| panic!("{input}: {error:?}"));
                assert_eq!(
                    render_time(&got),
                    expected,
                    "{input} in {:?}",
                    ctx.time_zone()
                );
            }
        }
    }

    #[test]
    fn a_string_cast_to_timestamp_propagates_dst_gap_error() {
        struct ZonedWarnings {
            zone: tidb_datatype::SessionTimeZone,
            warnings: RefCell<Vec<(u16, String)>>,
        }
        impl crate::Columns for ZonedWarnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
                self.zone.clone()
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.warnings.borrow_mut().push((code, message.to_owned()));
            }
        }

        let ctx = ZonedWarnings {
            zone: tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            warnings: RefCell::new(Vec::new()),
        };
        let got = cast_to_time(
            &Datum::new_string("2018-03-11 02:00:16".to_owned()),
            None,
            &ctx,
            tidb_datatype::TimeType::Timestamp,
            0,
        )
        .expect_err("Go handleInvalidTimeError preserves the DST error");
        let EvalError::Conversion(error) = got else {
            panic!("typed DST error")
        };
        assert_eq!(error.to_sql_error().code, 8179);
        assert!(error
            .to_sql_error()
            .message
            .contains("Daylight Saving Time transition '2018-03-11 02:00:16'"));
        assert!(ctx.warnings.borrow().is_empty());
    }

    fn render_time(v: &Datum) -> String {
        match v {
            Datum::Time(time) => time.to_string(),
            Datum::String(text) => crate::coerce::string_text(text)
                .expect("temporal CAST text is valid UTF-8")
                .to_owned(),
            Datum::Null => "NULL".to_owned(),
            other => panic!("a temporal cast produced an unexpected {other:?}"),
        }
    }

    fn datetime_fsp(v: Datum, fsp: i64) -> String {
        render_time(
            &cast_to_time(&v, None, &NoColumns, tidb_datatype::TimeType::DateTime, fsp)
                .expect("cast"),
        )
    }

    fn datetime(v: Datum) -> String {
        datetime_fsp(v, 0)
    }

    /// A DECIMAL source is read as TiDB's packed `YYYYMMDD[HHMMSS]` NUMBER
    /// (Go `builtinCastDecimalAsTimeSig` -> `ParseTimeFromFloatString`), NOT as
    /// wall-clock text. Funnelling `121212.1111` through the STRING parser made
    /// it absorb the `.1111` fraction as a clock (`2012-12-12 11:11:00`) where
    /// TiDB reads the whole-date number `121212` and answers midnight
    /// (`expression/cast`: `cast(d2 as datetime)` over `121212.1111`).
    #[test]
    fn a_decimal_source_reads_the_packed_number_not_the_wall_clock_text() {
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("121212.1111"))),
            "2012-12-12 00:00:00",
        );
        // A number shorter than a full date is zero-padded YYMMDD, so `111`
        // is `00-01-11` -> `2000-01-11`; the string parser rejected it as NULL.
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("111.1"))),
            "2000-01-11 00:00:00",
        );
        // A month of 13 is still an invalid date -> NULL, unchanged.
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("1311.1"))),
            "NULL",
        );
    }

    /// A REAL/DOUBLE source takes the same packed-number reading
    /// (Go `builtinCastRealAsTimeSig`), so `1122.1` is `00-11-22`.
    #[test]
    fn a_real_source_reads_the_packed_number() {
        assert_eq!(datetime(Datum::Real(1122.1)), "2000-11-22 00:00:00",);
        assert_eq!(datetime(Datum::Float32(1122.1)), "2000-11-22 00:00:00",);
    }

    /// An INTEGER source is `ParseTimeFromNum` (Go `builtinCastIntAsTimeSig`):
    /// `20170118` is the packed date, no fractional text to misread.
    #[test]
    fn an_integer_source_reads_the_packed_number() {
        assert_eq!(datetime(Datum::Int(20_170_118)), "2017-01-18 00:00:00",);
    }

    /// Go's numeric cast signatures have NO `NO_ZERO_DATE` rejection (the
    /// `#11203` guard: a zero number is the zero time, never NULL), unlike the
    /// STRING signature. Under the default SQL mode -- which DOES carry
    /// `NO_ZERO_DATE` ([`NoColumns`] answers `TIDB_DEFAULT_SQL_MODE`) -- a zero
    /// INT/REAL/DECIMAL therefore reads `0000-00-00 00:00:00`, matching
    /// `expression/cast`'s `(0, 0, 0)` row, while a zero-date STRING is still
    /// rejected to NULL.
    #[test]
    fn a_numeric_zero_is_the_zero_time_not_null() {
        let zero = "0000-00-00 00:00:00";
        assert_eq!(datetime(Datum::Int(0)), zero, "int 0");
        assert_eq!(datetime(Datum::UInt(0)), zero, "uint 0");
        assert_eq!(datetime(Datum::Real(0.0)), zero, "real 0");
        assert_eq!(
            datetime(Datum::Decimal(Decimal::from_literal("0"))),
            zero,
            "decimal 0"
        );
        // The STRING signature keeps Go's zero-date rejection under NO_ZERO_DATE.
        assert_eq!(
            datetime(Datum::new_string("0000-00-00 00:00:00".to_string())),
            "NULL",
            "a zero-date STRING is still rejected"
        );
    }

    /// The STRING path is unchanged: a wall-clock literal still parses as text.
    #[test]
    fn a_string_source_is_unchanged() {
        assert_eq!(
            datetime(Datum::new_string("2017-01-18 12:34:56".to_string())),
            "2017-01-18 12:34:56",
        );
    }

    #[test]
    fn temporal_target_fsp_rounds_at_boundaries() {
        let text = |s: &str| Datum::new_string(s.to_owned());
        assert_eq!(
            datetime_fsp(text("2020-02-03 11:22:33.987654"), 3),
            "2020-02-03 11:22:33.988"
        );
        assert_eq!(
            datetime_fsp(text("2020-01-01 23:59:59.5"), 0),
            "2020-01-02 00:00:00"
        );
        assert_eq!(
            datetime_fsp(text("2020-01-01 23:59:59.5"), 1),
            "2020-01-01 23:59:59.5"
        );
        let through_dispatch = eval_cast(
            &CastType::DateTime { fsp: Some(3) },
            text("2020-02-03 11:22:33.987654"),
            None,
            &NoColumns,
        )
        .expect("CAST dispatch");
        assert_eq!(render_time(&through_dispatch), "2020-02-03 11:22:33.988");
    }

    #[test]
    fn temporal_cast_preserves_value_and_zero_date_fields() {
        let got = cast_to_time(
            &Datum::new_string("0000-01-02 03:04:05".to_owned()),
            None,
            &NoColumns,
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .expect("cast");
        assert!(matches!(got, Datum::Time(_)));
        assert_eq!(render_time(&got), "0000-01-02 03:04:05");

        let date = cast_to_time(
            &Datum::new_string("2020-01-01 10:30:00".to_owned()),
            None,
            &NoColumns,
            tidb_datatype::TimeType::Date,
            0,
        )
        .expect("cast");
        assert_eq!(render_time(&date), "2020-01-01");
    }

    #[test]
    fn duration_to_date_keeps_go_visible_date() {
        struct AtMidnight;
        impl crate::Columns for AtMidnight {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                Some((1_785_974_400, 0, 0))
            }
        }
        let duration = Datum::Duration(
            tidb_datatype::MySqlDuration::new(12, 34, 56, 789_000, 3).expect("duration"),
        );
        let date = cast_to_time(
            &duration,
            None,
            &AtMidnight,
            tidb_datatype::TimeType::Date,
            0,
        )
        .expect("cast");
        assert_eq!(render_time(&date), "2026-08-06");
    }

    #[test]
    fn cast_time_dispatches_every_supported_source_domain() {
        use std::cell::RefCell;

        struct Warnings(RefCell<Vec<(u16, String)>>);
        impl crate::Columns for Warnings {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }

            fn append_warning(&self, code: u16, message: &str) {
                self.0.borrow_mut().push((code, message.to_owned()));
            }
        }

        let cast = CastType::Time { fsp: Some(3) };
        let int_type = FieldType::new(FieldTypeCode::LongLong);
        let decimal_type = FieldType::new(FieldTypeCode::NewDecimal);
        let string_type = FieldType::new(FieldTypeCode::VarString);
        let duration_type = FieldType::new(FieldTypeCode::Duration);
        let warnings = Warnings(RefCell::new(Vec::new()));

        for (value, source, expected) in [
            (Datum::Int(125_959), &int_type, "12:59:59.000"),
            (
                Datum::Decimal(Decimal::from_literal("125959")),
                &decimal_type,
                "12:59:59.000",
            ),
            (
                Datum::new_string("12:59:59".to_owned()),
                &string_type,
                "12:59:59.000",
            ),
            (
                Datum::new_duration(
                    tidb_datatype::MySqlDuration::new(12, 59, 59, 987_654, 6).expect("duration"),
                ),
                &duration_type,
                "12:59:59.988",
            ),
        ] {
            let got = eval_cast(&cast, value, Some(source), &warnings).expect("CAST AS TIME");
            assert_eq!(got.sql_string().expect("duration string"), expected);
        }

        // The Go string signature preserves the parser's best effort beside
        // a truncation warning, whereas the numeric signature returns NULL.
        let text = eval_cast(
            &CastType::Time { fsp: None },
            Datum::new_string("1x".to_owned()),
            Some(&string_type),
            &warnings,
        )
        .expect("string truncation is a warning");
        assert_eq!(text.sql_string().expect("duration string"), "00:00:01");
        let numeric = eval_cast(
            &CastType::Time { fsp: None },
            Datum::Int(126_060),
            Some(&int_type),
            &warnings,
        )
        .expect("numeric truncation is a warning");
        assert_eq!(numeric, Datum::Null);
        assert_eq!(warnings.0.borrow().len(), 2);

        // Go's JSON duration signature accepts only temporal/string JSON;
        // scalar numeric JSON is a NULL plus the same truncation warning.
        let json_type = FieldType::new(FieldTypeCode::Json);
        let json = eval_cast(
            &CastType::Time { fsp: None },
            Datum::new_json(tidb_datatype::BinaryJSON::parse("123").expect("json")),
            Some(&json_type),
            &warnings,
        )
        .expect("JSON mismatch is a warning");
        assert_eq!(json, Datum::Null);
        assert_eq!(warnings.0.borrow().len(), 3);
    }
    #[test]
    fn duration_batch_json_duration_preserves_embedded_precision() {
        let duration = tidb_datatype::MySqlDuration::new(12, 34, 56, 123_456, 6).unwrap();
        let json = Datum::Json(tidb_datatype::BinaryJSON::from_duration(duration));
        let result = eval_cast(
            &CastType::Time { fsp: Some(0) },
            json,
            Some(&FieldType::new(FieldTypeCode::Json)),
            &NoColumns,
        )
        .unwrap();
        assert_eq!(result, Datum::Duration(duration));
    }

    #[test]
    fn duration_batch_json_string_parse_error_keeps_zero_value() {
        let json = Datum::Json(tidb_datatype::BinaryJSON::parse("\"bad\"").unwrap());
        let result = eval_cast(
            &CastType::Time { fsp: Some(3) },
            json,
            Some(&FieldType::new(FieldTypeCode::Json)),
            &NoColumns,
        )
        .unwrap();
        assert!(matches!(result, Datum::Duration(_)), "{result:?}");
        assert_eq!(result.sql_string().unwrap(), "00:00:00");
    }

    #[test]
    fn duration_batch_string_calendar_fallback_keeps_midnight_carry() {
        let result = eval_cast(
            &CastType::Time { fsp: Some(0) },
            Datum::new_string("2024-01-01 23:59:59.999999".to_owned()),
            Some(&FieldType::new(FieldTypeCode::VarString)),
            &NoColumns,
        )
        .unwrap();
        assert_eq!(result.sql_string().unwrap(), "24:00:00");
    }
    #[test]
    fn calendar_batch_json_calendar_preserves_native_fields() {
        let time =
            tidb_datatype::parse_datetime("2024-01-02 03:04:05.999999", &chrono::Utc, true, false)
                .unwrap()
                .time;
        let json = Datum::Json(tidb_datatype::BinaryJSON::from_time(time));
        let result = cast_to_time(
            &json,
            None,
            &NoColumns,
            tidb_datatype::TimeType::DateTime,
            0,
        )
        .unwrap();
        let Datum::Time(value) = result else {
            panic!("native JSON calendar")
        };
        assert_eq!(value.core_time(), time.core_time());
        assert_eq!(value.fsp(), 0);
    }

    #[test]
    fn calendar_batch_json_duration_uses_statement_clock() {
        struct Clock;
        impl crate::Columns for Clock {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn now(&self) -> Option<(i64, u32, i32)> {
                Some((1_785_974_400, 0, 0))
            }
        }
        let duration = tidb_datatype::MySqlDuration::new(12, 34, 56, 789_000, 3).unwrap();
        let json = Datum::Json(tidb_datatype::BinaryJSON::from_duration(duration));
        let value =
            cast_to_time(&json, None, &Clock, tidb_datatype::TimeType::DateTime, 0).unwrap();
        assert_eq!(value.sql_string().unwrap(), "2026-08-06 12:34:57");
    }

    #[test]
    fn calendar_batch_json_source_admission_keeps_numeric_null() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let json = Datum::Json(tidb_datatype::BinaryJSON::parse("20240102").unwrap());
        let value = cast_to_time(&json, None, &ctx, tidb_datatype::TimeType::DateTime, 0).unwrap();
        assert!(value.is_null());
        assert_eq!(
            ctx.0.borrow().as_slice(),
            &[(
                1292,
                "Truncated incorrect datetime value: '20240102'".to_owned()
            )]
        );
        struct NoZero(RefCell<Vec<(u16, String)>>);
        impl crate::Columns for NoZero {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn date_modes(&self) -> tidb_datatype::DateModes {
                tidb_datatype::DateModes {
                    no_zero_date: true,
                    ..Default::default()
                }
            }
            fn append_warning(&self, code: u16, message: &str) {
                self.0.borrow_mut().push((code, message.to_owned()));
            }
        }
        let zero_ctx = NoZero(RefCell::new(Vec::new()));
        let json = Datum::Json(tidb_datatype::BinaryJSON::parse("\"0000-00-00\"").unwrap());
        let value =
            cast_to_time(&json, None, &zero_ctx, tidb_datatype::TimeType::DateTime, 3).unwrap();
        assert_eq!(value.sql_string().unwrap(), "0000-00-00 00:00:00.000");
        assert!(zero_ctx.0.borrow().is_empty());
        let value = cast_to_time(
            &Datum::new_string("0000-00-00"),
            None,
            &zero_ctx,
            tidb_datatype::TimeType::DateTime,
            3,
        )
        .unwrap();
        assert!(value.is_null());
        assert_eq!(
            zero_ctx.0.borrow().as_slice(),
            &[(
                1292,
                "Incorrect datetime value: '0000-00-00 00:00:00.000'".to_owned()
            )]
        );
    }

    #[test]
    fn calendar_batch_typed_time_validates_timestamp_target() {
        let ctx = WarningContext(RefCell::new(Vec::new()));
        let time = tidb_datatype::parse_datetime("1960-01-01 12:34:56", &chrono::Utc, true, false)
            .unwrap()
            .time;
        let value = cast_to_time(
            &Datum::Time(time),
            None,
            &ctx,
            tidb_datatype::TimeType::Timestamp,
            0,
        )
        .unwrap();
        assert!(value.is_null());
        assert_eq!(ctx.0.borrow().len(), 1);
        assert_eq!(ctx.0.borrow()[0].0, 1292);
    }
}
