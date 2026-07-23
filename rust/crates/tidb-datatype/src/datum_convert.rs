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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-shaped `pkg/types/datum.go::Datum.ConvertTo` implementation.
//!
//! Conversion produces a value and an event separately. Go routes the same
//! event through statement flags to become an error, warning, or ignored
//! condition. Keeping that policy out of the representation layer prevents
//! executor/session state from leaking into the datatype crate.

use chrono::Utc;

use crate::{
    adjust_year, convert_decimal_to_uint, convert_float_to_int, convert_float_to_uint,
    convert_int_to_int, convert_int_to_uint, convert_uint_to_int, convert_uint_to_uint,
    integer_signed_lower_bound, integer_signed_upper_bound, integer_unsigned_upper_bound,
    json_to_int, parse_enum, parse_enum_value, parse_set, parse_set_value, parse_time,
    parse_time_from_decimal, parse_time_from_num, str_to_duration, truncate_float, BinaryJSON,
    BinaryLiteral, BinaryLiteralWidth, Charset, Collation, ConversionFlags, Converted, CoreTime,
    Datum, DatumValueError, Decimal, DurationOrTime, FieldType, FieldTypeCode, MySqlDuration,
    ScalarConversionError, ScalarConversionEvent, Time, TimeType, VectorFloat32,
    UNSPECIFIED_LENGTH,
};

/// Direction used by reverse expression evaluation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoundingType {
    /// Round toward positive infinity.
    Ceiling,
    /// Round toward negative infinity.
    Floor,
}

impl Datum {
    /// Converts this datum into the target MySQL field domain.
    ///
    /// This is the pure value/event half of Go `Datum.ConvertTo`. Callers own
    /// statement warning/error policy and consume [`Converted::event`].
    pub fn convert_to(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        if self.is_null() || matches!(target.code(), FieldTypeCode::Null) {
            return Ok(exact(Self::Null));
        }
        match target.code() {
            FieldTypeCode::Tiny
            | FieldTypeCode::Short
            | FieldTypeCode::Int24
            | FieldTypeCode::Long
            | FieldTypeCode::LongLong => {
                if target.is_unsigned() {
                    self.convert_to_unsigned(target.code(), flags)
                        .map(map_converted(Self::UInt))
                } else {
                    self.convert_to_signed(target.code(), flags)
                        .map(map_converted(Self::Int))
                }
            }
            FieldTypeCode::Float | FieldTypeCode::Double => {
                let converted = self.to_f64()?;
                let produced = produce_float_with_type(converted.value, target);
                let event = prefer_event(converted.event, produced.event);
                Ok(Converted {
                    value: if matches!(target.code(), FieldTypeCode::Float) {
                        Self::Float32(f64::from(produced.value as f32))
                    } else {
                        Self::Real(produced.value)
                    },
                    event,
                })
            }
            FieldTypeCode::String
            | FieldTypeCode::Varchar
            | FieldTypeCode::VarString
            | FieldTypeCode::Blob
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob
            | FieldTypeCode::LongBlob => {
                let bytes = self.string_conversion_bytes(target, flags)?;
                let produced = produce_string_with_type(bytes, target, true)?;
                Ok(Converted {
                    value: if target.charset() == Charset::Binary {
                        Self::new_bytes(produced.value)
                    } else {
                        Self::new_collation_string(produced.value, target.collation())
                    },
                    event: produced.event,
                })
            }
            FieldTypeCode::NewDecimal => self.convert_to_decimal_target(target),
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp => {
                self.convert_to_time_target(target)
            }
            FieldTypeCode::Duration => self.convert_to_duration_target(target),
            FieldTypeCode::Year => self.convert_to_year(flags),
            FieldTypeCode::Enum => self.convert_to_enum(target, flags),
            FieldTypeCode::Set => self.convert_to_set(target, flags),
            FieldTypeCode::Bit => self.convert_to_bit(target, flags),
            FieldTypeCode::Json => self.convert_to_json_target(),
            FieldTypeCode::VectorFloat32 => self.convert_to_vector(target),
            other => Err(DatumValueError::Unsupported(
                self.kind(),
                field_target_name(other),
            )),
        }
    }

    fn string_conversion_bytes(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Vec<u8>, DatumValueError> {
        if matches!(self, Self::String(_) | Self::Bytes(_)) {
            let from_binary = self.collation() == Some(Collation::Binary);
            let to_binary = target.charset() == Charset::Binary;
            if from_binary && to_binary {
                return Ok(self.as_raw_bytes().unwrap().to_vec());
            }
            let transformed = if from_binary {
                self.binary_string_decoded(flags, target.charset().name())
            } else if to_binary {
                return Ok(self.binary_string_encoded().unwrap());
            } else {
                self.string_with_check(flags, target.charset().name())
            }
            .unwrap();
            let (bytes, error) = transformed.into_parts();
            if let Some(error) = error {
                return Err(DatumValueError::Comparison(error.to_string()));
            }
            return Ok(bytes);
        }
        if let Self::BinaryLiteral(value) = self {
            let transformed = crate::find_encoding(target.charset().name())
                .transform(value.as_bytes(), crate::TransformOp::DECODE);
            let (bytes, error) = transformed.into_parts();
            if let Some(error) = error {
                return Err(DatumValueError::Comparison(error.to_string()));
            }
            return Ok(bytes);
        }
        self.to_bytes().map_err(|error| {
            DatumValueError::Comparison(format!("string conversion failed: {error}"))
        })
    }

    fn convert_to_signed(
        &self,
        target: FieldTypeCode,
        flags: ConversionFlags,
    ) -> Result<Converted<i64>, DatumValueError> {
        let lower = integer_signed_lower_bound(target);
        let upper = integer_signed_upper_bound(target);
        let converted = match self {
            Self::Int(value) => numeric_outcome(convert_int_to_int(*value, lower, upper, target)),
            Self::UInt(value) => numeric_outcome(convert_uint_to_int(*value, upper, target)),
            Self::Real(value) | Self::Float32(value) => {
                numeric_outcome(convert_float_to_int(*value, lower, upper, target))
            }
            Self::String(value) => {
                let parsed = crate::str_to_int(value.as_utf8()?, false);
                let bounded =
                    numeric_outcome(convert_int_to_int(parsed.value, lower, upper, target));
                Converted {
                    value: bounded.value,
                    event: prefer_event(parsed.event, bounded.event),
                }
            }
            Self::Bytes(value) => {
                let parsed = crate::str_to_int(std::str::from_utf8(value)?, false);
                let bounded =
                    numeric_outcome(convert_int_to_int(parsed.value, lower, upper, target));
                Converted {
                    value: bounded.value,
                    event: prefer_event(parsed.event, bounded.event),
                }
            }
            Self::Time(value) => decimal_to_signed(&value.to_number(), lower, upper, target),
            Self::Duration(value) => decimal_to_signed(&value.to_number(), lower, upper, target),
            Self::Decimal(value) => decimal_to_signed(value, lower, upper, target),
            Self::Enum(value, _) => numeric_outcome(convert_float_to_int(
                value.to_number(),
                lower,
                upper,
                target,
            )),
            Self::Set(value, _) => numeric_outcome(convert_float_to_int(
                value.to_number(),
                lower,
                upper,
                target,
            )),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let literal = value.to_int();
                let bounded = numeric_outcome(convert_uint_to_int(literal.value(), upper, target));
                Converted {
                    value: bounded.value,
                    event: prefer_event(
                        literal
                            .is_truncated()
                            .then_some(ScalarConversionEvent::Truncated),
                        bounded.event,
                    ),
                }
            }
            Self::Json(value) => json_to_int(value, false, target, flags),
            _ => return Err(DatumValueError::Unsupported(self.kind(), "signed integer")),
        };
        Ok(converted)
    }

    fn convert_to_unsigned(
        &self,
        target: FieldTypeCode,
        flags: ConversionFlags,
    ) -> Result<Converted<u64>, DatumValueError> {
        let upper = integer_unsigned_upper_bound(target);
        let converted = match self {
            Self::Int(value) => numeric_outcome(convert_int_to_uint(flags, *value, upper, target)),
            Self::UInt(value) => numeric_outcome(convert_uint_to_uint(*value, upper, target)),
            Self::Real(value) | Self::Float32(value) => {
                numeric_outcome(convert_float_to_uint(flags, *value, upper, target))
            }
            Self::String(value) => string_to_unsigned(value.as_utf8()?, upper, target),
            Self::Bytes(value) => string_to_unsigned(std::str::from_utf8(value)?, upper, target),
            Self::Time(value) => decimal_to_unsigned(&value.to_number(), upper, target),
            Self::Duration(value) => decimal_to_unsigned(&value.to_number(), upper, target),
            Self::Decimal(value) => decimal_to_unsigned(value, upper, target),
            Self::Enum(value, _) => numeric_outcome(convert_float_to_uint(
                flags,
                value.to_number(),
                upper,
                target,
            )),
            Self::Set(value, _) => numeric_outcome(convert_float_to_uint(
                flags,
                value.to_number(),
                upper,
                target,
            )),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let literal = value.to_int();
                let bounded = numeric_outcome(convert_uint_to_uint(literal.value(), upper, target));
                Converted {
                    value: bounded.value,
                    event: prefer_event(
                        literal
                            .is_truncated()
                            .then_some(ScalarConversionEvent::Truncated),
                        bounded.event,
                    ),
                }
            }
            Self::Json(value) => {
                let converted = json_to_int(value, true, target, flags);
                Converted {
                    value: converted.value as u64,
                    event: converted.event,
                }
            }
            _ => {
                return Err(DatumValueError::Unsupported(
                    self.kind(),
                    "unsigned integer",
                ))
            }
        };
        Ok(converted)
    }

    fn convert_to_decimal_target(
        &self,
        target: &FieldType,
    ) -> Result<Converted<Self>, DatumValueError> {
        let converted = self.to_decimal()?;
        let original = converted.value;
        let mut value = original.clone();
        let mut event = converted.event;
        if target.flen() != UNSPECIFIED_LENGTH && target.decimal() != UNSPECIFIED_LENGTH {
            if target.flen() < target.decimal() {
                return Err(DatumValueError::Comparison(
                    "For float(M,D), double(M,D) or decimal(M,D), M must be >= D".to_owned(),
                ));
            }
            let rounded = value.round_to_scale(target.decimal() as i32);
            let fitted = rounded
                .fit_precision_scale(target.flen().max(0) as u32, target.decimal().max(0) as u32);
            let overflowed = fitted.is_none();
            value = fitted.unwrap_or_else(|| {
                Decimal::from_signed_literal(&format!(
                    "{}{}",
                    if rounded.is_negative() { "-" } else { "" },
                    max_decimal_text(target.flen() as usize, target.decimal() as usize)
                ))
            });
            if overflowed {
                event = Some(overflow_event(value.to_string(), target.code()));
            } else if value != original {
                event = event.or(Some(ScalarConversionEvent::Truncated));
            }
        }
        if target.is_unsigned() && value.is_negative() {
            value = Decimal::from_int(0);
            event = Some(overflow_event(original.to_string(), target.code()));
        }
        Ok(Converted {
            value: Self::new_decimal(value),
            event,
        })
    }

    fn convert_to_time_target(
        &self,
        target: &FieldType,
    ) -> Result<Converted<Self>, DatumValueError> {
        let kind = match target.code() {
            FieldTypeCode::Date => TimeType::Date,
            FieldTypeCode::Datetime => TimeType::DateTime,
            FieldTypeCode::Timestamp => TimeType::Timestamp,
            _ => unreachable!(),
        };
        let fsp = if target.decimal() == UNSPECIFIED_LENGTH {
            0
        } else {
            target.decimal()
        };
        let mut event = None;
        let time = match self {
            Self::Time(value) => {
                let (converted, adjusted) = value
                    .convert_kind(kind, true, false, &Utc)
                    .map_err(conversion_error)?;
                if adjusted {
                    event = Some(ScalarConversionEvent::Truncated);
                }
                converted.round_frac(fsp, &Utc).map_err(conversion_error)?
            }
            Self::Duration(value) => value
                .convert_to_time(Utc::now(), kind, true, false)
                .and_then(|time| time.round_frac(fsp, &Utc))
                .map_err(conversion_error)?,
            Self::String(value) => {
                parse_time(value.as_utf8()?, kind, fsp, false, true, false, &Utc)
                    .map_err(conversion_error)?
                    .time
            }
            Self::Bytes(value) => {
                parse_time(
                    std::str::from_utf8(value)?,
                    kind,
                    fsp,
                    false,
                    true,
                    false,
                    &Utc,
                )
                .map_err(conversion_error)?
                .time
            }
            Self::Int(value) => {
                parse_time_from_num(*value, kind, fsp, true, false, &Utc)
                    .map_err(conversion_error)?
                    .time
            }
            Self::UInt(value) if *value <= i64::MAX as u64 => {
                parse_time_from_num(*value as i64, kind, fsp, true, false, &Utc)
                    .map_err(conversion_error)?
                    .time
            }
            Self::Decimal(value) => {
                let mut time =
                    parse_time_from_decimal(value, true, false, &Utc).map_err(conversion_error)?;
                time.set_kind(kind);
                time.round_frac(fsp, &Utc).map_err(conversion_error)?
            }
            Self::Json(value) => {
                parse_time(&value.unquote()?, kind, fsp, false, true, false, &Utc)
                    .map_err(conversion_error)?
                    .time
            }
            _ => return Err(DatumValueError::Unsupported(self.kind(), "time")),
        };
        Ok(Converted {
            value: Self::new_time(time),
            event,
        })
    }

    fn convert_to_duration_target(
        &self,
        target: &FieldType,
    ) -> Result<Converted<Self>, DatumValueError> {
        let fsp = if target.decimal() == UNSPECIFIED_LENGTH {
            0
        } else {
            target.decimal()
        };
        let converted = match self {
            Self::Time(value) => exact(value.to_duration().map_err(conversion_error)?),
            Self::Duration(value) => exact(value.round_frac(fsp).map_err(conversion_error)?),
            Self::String(value) => duration_from_text(value.as_utf8()?, fsp)?,
            Self::Bytes(value) => duration_from_text(std::str::from_utf8(value)?, fsp)?,
            Self::Int(_) | Self::UInt(_) | Self::Real(_) | Self::Float32(_) | Self::Decimal(_) => {
                duration_from_text(
                    &self.sql_string().map_err(|error| {
                        DatumValueError::Comparison(format!("duration conversion failed: {error}"))
                    })?,
                    fsp,
                )?
            }
            Self::Json(value) => duration_from_text(&value.unquote()?, fsp)?,
            _ => return Err(DatumValueError::Unsupported(self.kind(), "duration")),
        };
        Ok(map_converted(Self::new_duration)(converted))
    }

    fn convert_to_year(&self, flags: ConversionFlags) -> Result<Converted<Self>, DatumValueError> {
        let (year, adjust_zero, event) = match self {
            Self::String(value) => year_from_text(value.as_utf8()?)?,
            Self::Bytes(value) => year_from_text(std::str::from_utf8(value)?)?,
            Self::Time(value) => (i64::from(value.core_time().year()), false, None),
            Self::Duration(value) => (
                value
                    .convert_to_year(Utc::now(), false)
                    .map_err(conversion_error)?,
                false,
                None,
            ),
            Self::Json(value) => {
                let converted = crate::json_to_int64(value, false, crate::DEFAULT_STATEMENT_FLAGS);
                (converted.value, false, converted.event)
            }
            _ => {
                let converted = self.convert_to_signed(FieldTypeCode::LongLong, flags)?;
                (converted.value, false, converted.event)
            }
        };
        let year = adjust_year(year, adjust_zero).map_err(conversion_error)?;
        Ok(Converted {
            value: Self::Int(year),
            event,
        })
    }

    fn convert_to_enum(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        let value = match self {
            Self::String(value) => parse_enum(target.elems(), value.as_utf8()?, target.collation()),
            Self::Bytes(value) => parse_enum(
                target.elems(),
                std::str::from_utf8(value)?,
                target.collation(),
            ),
            Self::BinaryLiteral(value) => parse_enum(
                target.elems(),
                std::str::from_utf8(value.as_bytes())?,
                target.collation(),
            ),
            Self::Enum(value, _) if value.value() == 0 => Ok(crate::MysqlEnum::new("", 0)),
            Self::Enum(value, _) => parse_enum(target.elems(), value.name(), target.collation()),
            Self::Set(value, _) => parse_enum(target.elems(), value.name(), target.collation()),
            _ => {
                let number = self.convert_to_unsigned(FieldTypeCode::LongLong, flags)?;
                parse_enum_value(target.elems(), number.value)
            }
        }
        .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
        Ok(exact(Self::new_enum(value, target.collation())))
    }

    fn convert_to_set(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        let value = match self {
            Self::String(value) => parse_set(target.elems(), value.as_utf8()?, target.collation()),
            Self::Bytes(value) => parse_set(
                target.elems(),
                std::str::from_utf8(value)?,
                target.collation(),
            ),
            Self::BinaryLiteral(value) => parse_set(
                target.elems(),
                std::str::from_utf8(value.as_bytes())?,
                target.collation(),
            ),
            Self::Enum(value, _) => parse_set(target.elems(), value.name(), target.collation()),
            Self::Set(value, _) => parse_set(target.elems(), value.name(), target.collation()),
            Self::VectorFloat32(_) => return Err(DatumValueError::Unsupported(self.kind(), "set")),
            _ => {
                let number = self.convert_to_unsigned(FieldTypeCode::LongLong, flags)?;
                parse_set_value(target.elems(), number.value)
            }
        }
        .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
        Ok(exact(Self::new_set(value, target.collation())))
    }

    fn convert_to_bit(
        &self,
        target: &FieldType,
        flags: ConversionFlags,
    ) -> Result<Converted<Self>, DatumValueError> {
        let flen = target.flen();
        if !(1..=64).contains(&flen) {
            return Err(DatumValueError::Comparison(format!(
                "Data Too Long, field len {flen}"
            )));
        }
        let mut event = None;
        let mut value = match self {
            Self::String(value) => value_to_literal_uint(value.bytes(), &mut event),
            Self::Bytes(value) => value_to_literal_uint(value, &mut event),
            Self::Int(value) => *value as u64,
            _ => {
                let converted = self.convert_to_unsigned(target.code(), flags)?;
                event = converted.event;
                converted.value
            }
        };
        if flen < 64 {
            let upper = (1_u64 << flen) - 1;
            if value > upper {
                value = upper;
                event = Some(ScalarConversionEvent::Truncated);
            }
        }
        let width = BinaryLiteralWidth::try_from(((flen + 7) / 8) as u8)
            .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
        Ok(Converted {
            value: Self::new_mysql_bit(BinaryLiteral::from_uint(value, Some(width))),
            event,
        })
    }

    fn convert_to_json_target(&self) -> Result<Converted<Self>, DatumValueError> {
        let json = match self {
            Self::String(value) => BinaryJSON::parse(value.as_utf8()?)?,
            Self::Bytes(value) => BinaryJSON::parse(std::str::from_utf8(value)?)?,
            Self::Enum(value, _) => BinaryJSON::parse(value.name())?,
            Self::Set(value, _) => BinaryJSON::parse(value.name())?,
            Self::BinaryLiteral(_) => {
                return Err(DatumValueError::Comparison(
                    "Cannot create a JSON value from a string with CHARACTER SET 'binary'"
                        .to_owned(),
                ))
            }
            Self::Json(value) => value.clone(),
            _ => self.to_mysql_json()?,
        };
        Ok(exact(Self::new_json(json)))
    }

    fn convert_to_vector(&self, target: &FieldType) -> Result<Converted<Self>, DatumValueError> {
        let value = match self {
            Self::VectorFloat32(value) => value.clone(),
            Self::String(value) => VectorFloat32::parse(value.as_utf8()?)
                .map_err(|error| DatumValueError::Comparison(error.to_string()))?,
            Self::Bytes(value) => VectorFloat32::parse(std::str::from_utf8(value)?)
                .map_err(|error| DatumValueError::Comparison(error.to_string()))?,
            _ => return Err(DatumValueError::Unsupported(self.kind(), "vector float32")),
        };
        let expected = (target.flen() != UNSPECIFIED_LENGTH)
            .then(|| usize::try_from(target.flen()).unwrap_or(usize::MAX));
        value
            .check_dims_fit_column(expected)
            .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
        Ok(exact(Self::new_vector_float32(value)))
    }
}

/// Source `ProduceFloatWithSpecifiedTp`.
pub fn produce_float_with_type(value: f64, target: &FieldType) -> Converted<f64> {
    if value.is_nan() {
        return Converted {
            value: 0.0,
            event: Some(overflow_event(value.to_string(), target.code())),
        };
    }
    if value.is_infinite() {
        return Converted {
            value,
            event: Some(overflow_event(value.to_string(), target.code())),
        };
    }
    let mut value = value;
    let mut event = None;
    if target.flen() != UNSPECIFIED_LENGTH && target.decimal() != UNSPECIFIED_LENGTH {
        match truncate_float(value, target.flen() as i32, target.decimal() as i32) {
            Ok(produced) => value = produced,
            Err((produced, error)) => {
                value = produced;
                event = Some(ScalarConversionEvent::Overflow(
                    ScalarConversionError::Overflow {
                        value: error.to_string(),
                        target: target.code(),
                    },
                ));
            }
        }
    }
    if target.is_unsigned() && value < 0.0 {
        return Converted {
            value: 0.0,
            event: Some(overflow_event(value.to_string(), target.code())),
        };
    }
    if matches!(target.code(), FieldTypeCode::Float)
        && !(-f64::from(f32::MAX)..=f64::from(f32::MAX)).contains(&value)
    {
        let source = value;
        value = if value.is_sign_positive() {
            f64::from(f32::MAX)
        } else {
            -f64::from(f32::MAX)
        };
        event = Some(overflow_event(source.to_string(), target.code()));
    }
    Converted { value, event }
}

/// Source `ProduceStrWithSpecifiedTp`, retaining truncation as an event.
pub fn produce_string_with_type(
    mut value: Vec<u8>,
    target: &FieldType,
    pad_zero: bool,
) -> Result<Converted<Vec<u8>>, DatumValueError> {
    let flen = target.flen();
    if flen < 0 {
        return Ok(exact(value));
    }
    let flen = flen as usize;
    let binary = target.charset() == Charset::Binary;
    let byte_limited = binary || target.code().is_type_blob();
    let split = if byte_limited {
        (value.len() > flen).then_some(flen)
    } else {
        utf8_split_at(&value, flen)?
    };
    let mut event = None;
    if let Some(split) = split {
        let overflow = value.split_off(split);
        if !overflow
            .iter()
            .all(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'))
            || !target.code().is_type_char()
            || matches!(target.code(), FieldTypeCode::Varchar)
        {
            event = Some(ScalarConversionEvent::Truncated);
        }
    }
    if pad_zero && binary && matches!(target.code(), FieldTypeCode::String) && value.len() < flen {
        value.resize(flen, 0);
    }
    Ok(Converted { value, event })
}

/// Source `GetMaxValue`.
pub fn get_max_value(target: &FieldType) -> Datum {
    match target.code() {
        code @ (FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong) => {
            if target.is_unsigned() {
                Datum::UInt(integer_unsigned_upper_bound(code))
            } else {
                Datum::Int(integer_signed_upper_bound(code))
            }
        }
        FieldTypeCode::Float => Datum::Float32(f64::from(crate::get_max_float(
            target.flen() as i32,
            target.decimal() as i32,
        ) as f32)),
        FieldTypeCode::Double => Datum::Real(crate::get_max_float(
            target.flen() as i32,
            target.decimal() as i32,
        )),
        FieldTypeCode::String
        | FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => Datum::new_collation_string([250], target.collation()),
        FieldTypeCode::NewDecimal => {
            Datum::new_decimal(Decimal::from_signed_literal(&max_decimal_text(
                target.flen().max(0) as usize,
                target.decimal().max(0) as usize,
            )))
        }
        FieldTypeCode::Duration => {
            Datum::new_duration(MySqlDuration::maximum(0).expect("FSP zero is valid"))
        }
        FieldTypeCode::Date | FieldTypeCode::Datetime => Datum::new_time(
            Time::new(
                CoreTime::from_date(9999, 12, 31, 23, 59, 59, 999_999),
                if matches!(target.code(), FieldTypeCode::Date) {
                    TimeType::Date
                } else {
                    TimeType::DateTime
                },
                0,
            )
            .expect("source maximum datetime is valid"),
        ),
        FieldTypeCode::Timestamp => Datum::new_time(
            Time::new(
                CoreTime::from_date(2038, 1, 19, 3, 14, 7, 999_999),
                TimeType::Timestamp,
                0,
            )
            .expect("source maximum timestamp is valid"),
        ),
        _ => Datum::Null,
    }
}

/// Source `GetMinValue`.
pub fn get_min_value(target: &FieldType) -> Datum {
    match target.code() {
        code @ (FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong) => {
            if target.is_unsigned() {
                Datum::UInt(0)
            } else {
                Datum::Int(integer_signed_lower_bound(code))
            }
        }
        FieldTypeCode::Float => Datum::Float32(-f64::from(crate::get_max_float(
            target.flen() as i32,
            target.decimal() as i32,
        ) as f32)),
        FieldTypeCode::Double => Datum::Real(-crate::get_max_float(
            target.flen() as i32,
            target.decimal() as i32,
        )),
        FieldTypeCode::String
        | FieldTypeCode::Varchar
        | FieldTypeCode::VarString
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => Datum::new_collation_string([1], target.collation()),
        FieldTypeCode::NewDecimal => Datum::new_decimal(Decimal::from_signed_literal(&format!(
            "-{}",
            max_decimal_text(
                target.flen().max(0) as usize,
                target.decimal().max(0) as usize
            )
        ))),
        FieldTypeCode::Duration => Datum::new_duration(
            MySqlDuration::from_nanoseconds(crate::MIN_TIME_NANOS, 0)
                .expect("source minimum duration is valid"),
        ),
        FieldTypeCode::Date | FieldTypeCode::Datetime => Datum::new_time(
            Time::new(
                CoreTime::from_date(1, 1, 1, 0, 0, 0, 0),
                if matches!(target.code(), FieldTypeCode::Date) {
                    TimeType::Date
                } else {
                    TimeType::DateTime
                },
                0,
            )
            .expect("source minimum datetime is valid"),
        ),
        FieldTypeCode::Timestamp => Datum::new_time(
            Time::new(
                CoreTime::from_date(1970, 1, 1, 0, 0, 1, 0),
                TimeType::Timestamp,
                0,
            )
            .expect("source minimum timestamp is valid"),
        ),
        _ => Datum::Null,
    }
}

/// Source `ChangeReverseResultByUpperLowerBound`.
pub fn change_reverse_result_by_bound(
    target: &FieldType,
    result: &Datum,
    rounding: RoundingType,
    flags: ConversionFlags,
) -> Result<Converted<Datum>, DatumValueError> {
    let mut converted = result.convert_to(target, flags)?;
    if matches!(converted.event, Some(ScalarConversionEvent::Overflow(_))) {
        return Ok(converted);
    }
    let source_bound = source_kind_bound(result, rounding);
    if converted
        .value
        .compare(
            &source_bound,
            source_bound.collation().unwrap_or(Collation::Binary),
        )?
        .is_eq()
    {
        converted.value = match rounding {
            RoundingType::Ceiling => get_max_value(target),
            RoundingType::Floor => get_min_value(target),
        };
    } else if matches!(rounding, RoundingType::Ceiling) {
        converted.value = increment_for_reverse(converted.value, target);
    }
    Ok(converted)
}

fn source_kind_bound(source: &Datum, rounding: RoundingType) -> Datum {
    let maximum = matches!(rounding, RoundingType::Ceiling);
    match source {
        Datum::Int(_) => Datum::Int(if maximum { i64::MAX } else { i64::MIN }),
        Datum::UInt(_) => Datum::UInt(if maximum { u64::MAX } else { 0 }),
        Datum::Float32(_) => Datum::Float32(if maximum {
            f64::from(f32::MAX)
        } else {
            -f64::from(f32::MAX)
        }),
        Datum::Real(_) => Datum::Real(if maximum { f64::MAX } else { -f64::MAX }),
        Datum::Decimal(value) => {
            let digits = value.coefficient_digits().len().max(1);
            let scale = value.scale() as usize;
            let text = max_decimal_text(digits, scale);
            let signed = if maximum { text } else { format!("-{text}") };
            Datum::new_decimal(Decimal::from_signed_literal(&signed))
        }
        _ => {
            if maximum {
                Datum::MaxValue
            } else {
                Datum::MinNotNull
            }
        }
    }
}

fn increment_for_reverse(value: Datum, target: &FieldType) -> Datum {
    match value {
        Datum::Int(value) => Datum::Int(
            value
                .checked_add(1)
                .filter(|next| *next <= integer_signed_upper_bound(target.code()))
                .unwrap_or(value),
        ),
        Datum::UInt(value) => Datum::UInt(
            value
                .checked_add(1)
                .filter(|next| *next <= integer_unsigned_upper_bound(target.code()))
                .unwrap_or(value),
        ),
        Datum::Float32(value) => Datum::Float32(if value < f64::from(f32::MAX) {
            value + 1.0
        } else {
            value
        }),
        Datum::Real(value) => Datum::Real(if value < f64::MAX { value + 1.0 } else { value }),
        Datum::Decimal(value) => {
            let maximum = get_max_value(target);
            if maximum
                .compare(&Datum::new_decimal(value.clone()), Collation::Binary)
                .is_ok_and(|ordering| ordering.is_eq())
            {
                Datum::new_decimal(value)
            } else {
                Datum::new_decimal(value.add(&Decimal::from_int(1)))
            }
        }
        other => other,
    }
}

fn string_to_unsigned(text: &str, upper: u64, target: FieldTypeCode) -> Converted<u64> {
    let parsed = crate::str_to_uint(text, false);
    let bounded = numeric_outcome(convert_uint_to_uint(parsed.value, upper, target));
    Converted {
        value: bounded.value,
        event: prefer_event(parsed.event, bounded.event),
    }
}

fn decimal_to_signed(
    value: &Decimal,
    lower: i64,
    upper: i64,
    target: FieldTypeCode,
) -> Converted<i64> {
    let rounded = value.round_to_i64();
    let raw = rounded.unwrap_or_else(|| value.round_to_i64_saturating());
    let bounded = numeric_outcome(convert_int_to_int(raw, lower, upper, target));
    Converted {
        value: bounded.value,
        event: if rounded.is_none() {
            Some(overflow_event(value.to_string(), target))
        } else {
            bounded.event
        },
    }
}

fn decimal_to_unsigned(value: &Decimal, upper: u64, target: FieldTypeCode) -> Converted<u64> {
    let converted = convert_decimal_to_uint(value, upper, target);
    numeric_outcome(converted)
}

fn duration_from_text(text: &str, fsp: i64) -> Result<Converted<MySqlDuration>, DatumValueError> {
    let converted = str_to_duration(text, fsp, &Utc).map_err(conversion_error)?;
    let value = match converted.value {
        DurationOrTime::Duration(value) => value,
        DurationOrTime::Time(value) => value.to_duration().map_err(conversion_error)?,
    };
    Ok(Converted {
        value,
        event: converted.event,
    })
}

fn year_from_text(
    text: &str,
) -> Result<(i64, bool, Option<ScalarConversionEvent>), DatumValueError> {
    let trimmed = text.trim();
    let converted = crate::str_to_int(trimmed, false);
    let adjust_zero = text.len() != 4 && converted.value == 0 && trimmed.starts_with('0');
    Ok((converted.value, adjust_zero, converted.event))
}

fn value_to_literal_uint(bytes: &[u8], event: &mut Option<ScalarConversionEvent>) -> u64 {
    let literal = BinaryLiteral::from(bytes);
    let outcome = literal.to_int();
    if outcome.is_truncated() {
        *event = Some(ScalarConversionEvent::Truncated);
    }
    outcome.value()
}

fn utf8_split_at(bytes: &[u8], flen: usize) -> Result<Option<usize>, DatumValueError> {
    let text = std::str::from_utf8(bytes)?;
    let mut chars = text.char_indices();
    for _ in 0..flen {
        if chars.next().is_none() {
            return Ok(None);
        }
    }
    Ok(chars.next().map(|(index, _)| index))
}

fn max_decimal_text(flen: usize, scale: usize) -> String {
    let integer = flen.saturating_sub(scale);
    if scale == 0 {
        return "9".repeat(integer);
    }
    format!("{}.{}", "9".repeat(integer.max(1)), "9".repeat(scale))
}

fn numeric_outcome<T>(result: Result<T, (T, ScalarConversionError)>) -> Converted<T> {
    match result {
        Ok(value) => exact(value),
        Err((value, error)) => Converted {
            value,
            event: Some(ScalarConversionEvent::Overflow(error)),
        },
    }
}

fn exact<T>(value: T) -> Converted<T> {
    Converted { value, event: None }
}

fn map_converted<T, U>(map: impl FnOnce(T) -> U) -> impl FnOnce(Converted<T>) -> Converted<U> {
    move |converted| Converted {
        value: map(converted.value),
        event: converted.event,
    }
}

fn prefer_event(
    first: Option<ScalarConversionEvent>,
    second: Option<ScalarConversionEvent>,
) -> Option<ScalarConversionEvent> {
    second.or(first)
}

fn overflow_event(value: String, target: FieldTypeCode) -> ScalarConversionEvent {
    ScalarConversionEvent::Overflow(ScalarConversionError::Overflow { value, target })
}

fn conversion_error(error: impl std::fmt::Display) -> DatumValueError {
    DatumValueError::Comparison(error.to_string())
}

const fn field_target_name(code: FieldTypeCode) -> &'static str {
    match code {
        FieldTypeCode::Unspecified => "unspecified",
        FieldTypeCode::NewDate => "new date",
        FieldTypeCode::Geometry => "geometry",
        FieldTypeCode::Unknown(_) => "unknown",
        _ => "field type",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_enum_value, parse_set_value, FieldTypeFlags};

    #[test]
    fn source_convert_to_integer_float_string_decimal_rows() {
        let signed_tiny = FieldType::new(FieldTypeCode::Tiny);
        let unsigned_tiny =
            FieldType::new(FieldTypeCode::Tiny).with_added_flags(FieldTypeFlags::UNSIGNED);
        assert_eq!(
            Datum::Int(128)
                .convert_to(&signed_tiny, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::Int(127)
        );
        assert_eq!(
            Datum::Int(-1)
                .convert_to(&unsigned_tiny, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::UInt(255)
        );

        let float = FieldType::new(FieldTypeCode::Float)
            .with_flen(5)
            .with_decimal(2);
        assert_eq!(
            Datum::Real(123.456)
                .convert_to(&float, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::Float32(f64::from(123.46_f32))
        );

        let string = FieldType::new(FieldTypeCode::Varchar)
            .with_flen(3)
            .with_collation(Collation::Utf8Mb4Bin);
        let converted = Datum::new_string("abcd")
            .convert_to(&string, crate::DEFAULT_STATEMENT_FLAGS)
            .unwrap();
        assert_eq!(converted.value.as_raw_bytes(), Some(&b"abc"[..]));
        assert!(converted.event.is_some());

        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(5)
            .with_decimal(2);
        assert_eq!(
            Datum::new_decimal(Decimal::from_signed_literal("12.345"))
                .convert_to(&decimal, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_decimal(Decimal::from_signed_literal("12.35"))
        );
    }

    #[test]
    fn source_convert_to_binary_string_rows() {
        let utf8 = "你好".as_bytes().to_vec();
        let gbk = vec![0xC4, 0xE3, 0xBA, 0xC3];
        let invalid_utf8 = [utf8.as_slice(), &[0x81]].concat();
        let invalid_gbk = [gbk.as_slice(), &[0x81]].concat();

        for (input, input_collation, output_collation, expected) in [
            (
                utf8.clone(),
                Collation::Utf8Bin,
                Collation::Utf8Bin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::Utf8Mb4Bin,
                Collation::Utf8Mb4Bin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::GbkBin,
                Collation::Utf8Bin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::GbkBin,
                Collation::GbkBin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::Binary,
                Collation::Utf8Mb4Bin,
                Some(utf8.clone()),
            ),
            (
                gbk.clone(),
                Collation::Binary,
                Collation::GbkBin,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::Utf8Bin,
                Collation::Binary,
                Some(utf8.clone()),
            ),
            (
                utf8.clone(),
                Collation::GbkBin,
                Collation::Binary,
                Some(gbk.clone()),
            ),
            (invalid_utf8, Collation::Utf8Bin, Collation::Utf8Bin, None),
            (invalid_gbk, Collation::GbkBin, Collation::GbkBin, None),
        ] {
            let input = Datum::new_collation_string(input, input_collation);
            let target = FieldType::new(FieldTypeCode::Varchar)
                .with_flen(255)
                .with_collation(output_collation);
            let converted = input.convert_to(&target, crate::DEFAULT_STATEMENT_FLAGS);
            match expected {
                Some(expected) => {
                    assert_eq!(
                        converted.unwrap().value.as_raw_bytes(),
                        Some(expected.as_slice())
                    );
                }
                None => assert!(converted.is_err()),
            }
        }
    }

    #[test]
    fn source_convert_to_enum_set_bit_json_vector_and_temporal_rows() {
        let enum_type = FieldType::new(FieldTypeCode::Enum)
            .with_elems(["a", "b"])
            .with_collation(Collation::Binary);
        assert_eq!(
            Datum::new_string("b")
                .convert_to(&enum_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_enum(parse_enum_value(&["a", "b"], 2).unwrap(), Collation::Binary)
        );

        let set_type = FieldType::new(FieldTypeCode::Set)
            .with_elems(["a", "b"])
            .with_collation(Collation::Binary);
        assert_eq!(
            Datum::UInt(3)
                .convert_to(&set_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_set(parse_set_value(&["a", "b"], 3).unwrap(), Collation::Binary)
        );

        let bit_type = FieldType::new(FieldTypeCode::Bit).with_flen(9);
        assert_eq!(
            Datum::UInt(0x101)
                .convert_to(&bit_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_mysql_bit(BinaryLiteral::from(&[0x01, 0x01]))
        );

        let json_type = FieldType::new(FieldTypeCode::Json);
        assert_eq!(
            Datum::new_string(r#"{"a":1}"#)
                .convert_to(&json_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_json(BinaryJSON::parse(r#"{"a":1}"#).unwrap())
        );

        let vector_type = FieldType::new(FieldTypeCode::VectorFloat32).with_flen(2);
        assert_eq!(
            Datum::new_string("[1,2]")
                .convert_to(&vector_type, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value,
            Datum::new_vector_float32(VectorFloat32::parse("[1,2]").unwrap())
        );

        let datetime = FieldType::new(FieldTypeCode::Datetime).with_decimal(0);
        assert_eq!(
            Datum::new_string("2011-01-01 11:11:11")
                .convert_to(&datetime, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value
                .sql_string()
                .unwrap(),
            "2011-01-01 11:11:11"
        );
        let duration = FieldType::new(FieldTypeCode::Duration).with_decimal(0);
        assert_eq!(
            Datum::new_string("12:34:56")
                .convert_to(&duration, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap()
                .value
                .sql_string()
                .unwrap(),
            "12:34:56"
        );
    }

    #[test]
    fn source_integer_uint32_and_bit_tables() {
        let uint32 = FieldType::new(FieldTypeCode::Long).with_unsigned(true);
        for (input, expected, overflow) in [
            (Datum::Int(5_000_000_000), u32::MAX as u64, true),
            (Datum::Int(-1), u32::MAX as u64, true),
            (Datum::new_string("5000000000"), u32::MAX as u64, true),
            (Datum::Int(12_345), 12_345, false),
            (Datum::Int(0), 0, false),
            (Datum::Int(2_147_483_648), 2_147_483_648, false),
        ] {
            let converted = input
                .convert_to(&uint32, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            assert_eq!(converted.value, Datum::UInt(expected));
            assert_eq!(converted.event.is_some(), overflow);
        }

        for (text, flen, expected, truncated) in [
            ("true", 1, vec![1], true),
            ("true", 32, b"true".to_vec(), false),
            ("false", 1, vec![1], true),
            ("false", 40, b"false".to_vec(), false),
            ("1", 1, vec![1], true),
            ("1", 8, vec![0x31], false),
            ("0", 1, vec![1], true),
            ("0", 8, vec![0x30], false),
            ("b'1'", 32, b"b'1'".to_vec(), false),
            ("b'0'", 32, b"b'0'".to_vec(), false),
        ] {
            let target = FieldType::new(FieldTypeCode::Bit).with_flen(flen);
            let converted = Datum::new_string(text)
                .convert_to(&target, crate::DEFAULT_STATEMENT_FLAGS)
                .unwrap();
            let Datum::Bit(value) = converted.value else {
                panic!("BIT conversion returned another datum kind");
            };
            assert_eq!(value.as_bytes(), expected, "{text} BIT({flen})");
            assert_eq!(converted.event.is_some(), truncated, "{text} BIT({flen})");
        }
    }

    #[test]
    fn source_max_min_and_reverse_bound_rows() {
        let unsigned = FieldType::new(FieldTypeCode::LongLong).with_unsigned(true);
        assert_eq!(get_min_value(&unsigned), Datum::UInt(0));
        assert_eq!(get_max_value(&unsigned), Datum::UInt(u64::MAX));

        for (input, target, rounding, expected) in [
            (
                Datum::Int(1),
                unsigned.clone(),
                RoundingType::Ceiling,
                Datum::UInt(2),
            ),
            (
                Datum::Int(1),
                unsigned.clone(),
                RoundingType::Floor,
                Datum::UInt(1),
            ),
            (
                Datum::Int(i64::MAX),
                unsigned.clone(),
                RoundingType::Ceiling,
                Datum::UInt(u64::MAX),
            ),
            (
                Datum::Int(i64::MAX),
                unsigned,
                RoundingType::Floor,
                Datum::UInt(i64::MAX as u64),
            ),
            (
                Datum::Int(1),
                FieldType::new(FieldTypeCode::Double)
                    .with_flen(22)
                    .with_decimal(UNSPECIFIED_LENGTH),
                RoundingType::Ceiling,
                Datum::Real(2.0),
            ),
            (
                Datum::Int(1),
                FieldType::new(FieldTypeCode::Double)
                    .with_flen(22)
                    .with_decimal(UNSPECIFIED_LENGTH),
                RoundingType::Floor,
                Datum::Real(1.0),
            ),
        ] {
            assert_eq!(
                change_reverse_result_by_bound(
                    &target,
                    &input,
                    rounding,
                    crate::DEFAULT_STATEMENT_FLAGS
                )
                .unwrap()
                .value,
                expected
            );
        }
    }
}
