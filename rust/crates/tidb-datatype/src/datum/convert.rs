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

//! Datum-to-scalar conversions.
//!
//! Mirrors the `ToBool` / `ToInt64` / `ToFloat64` / `ToDecimal` / `ToBytes` /
//! `ToHashKey` / `ToMysqlJSON` block of `pkg/types/datum.go`, plus the
//! aggregate-side opaque-JSON rule of `getRealJSONValue`
//! (`pkg/executor/aggfuncs/func_json_objectagg.go`).

use std::cmp::Ordering;

use super::{decimal_from_bytes, Datum, DatumStringError, DatumValueError};
use crate::{
    compare_binary_json, json_to_decimal, json_to_float, json_to_int64, str_to_float, str_to_int,
    BinaryJSON, BinaryJSONValue, Collation, Converted, Decimal, ScalarConversionEvent,
    DEFAULT_STATEMENT_FLAGS,
};

impl Datum {
    /// Source `Datum.ToBool`, retaining conversion warning/error disposition.
    pub fn to_bool(&self) -> Result<Converted<i64>, DatumValueError> {
        let converted = match self {
            Self::Int(value) => Converted {
                value: i64::from(*value != 0),
                event: None,
            },
            Self::UInt(value) => Converted {
                value: i64::from(*value != 0),
                event: None,
            },
            Self::Real(value) | Self::Float32(value) => Converted {
                value: i64::from(*value != 0.0),
                event: None,
            },
            Self::String(value) => {
                let parsed = str_to_float(value.as_utf8()?, false);
                Converted {
                    value: i64::from(parsed.value != 0.0),
                    event: parsed.event,
                }
            }
            Self::Bytes(value) => {
                let parsed = str_to_float(std::str::from_utf8(value)?, false);
                Converted {
                    value: i64::from(parsed.value != 0.0),
                    event: parsed.event,
                }
            }
            Self::Time(value) => Converted {
                value: i64::from(!value.is_zero()),
                event: None,
            },
            Self::Duration(value) => Converted {
                value: i64::from(value.nanoseconds() != 0),
                event: None,
            },
            Self::Decimal(value) => Converted {
                value: i64::from(!value.is_zero()),
                event: None,
            },
            Self::Enum(value, _) => Converted {
                value: i64::from(value.value() != 0),
                event: None,
            },
            Self::Set(value, _) => Converted {
                value: i64::from(value.value() != 0),
                event: None,
            },
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let outcome = value.to_int();
                Converted {
                    value: i64::from(outcome.value() != 0),
                    event: outcome
                        .is_truncated()
                        .then_some(ScalarConversionEvent::Truncated),
                }
            }
            Self::Json(value) => {
                let zero = BinaryJSON::parse("0")?;
                Converted {
                    value: i64::from(compare_binary_json(value, &zero) != Ordering::Equal),
                    event: None,
                }
            }
            Self::VectorFloat32(value) => Converted {
                value: i64::from(!value.is_zero_value()),
                event: None,
            },
            other => return Err(DatumValueError::Unsupported(other.kind(), "bool")),
        };
        Ok(converted)
    }

    /// Source `Datum.ToInt64`.
    ///
    /// Go's takes a `types.Context`, whose LOCATION reaches
    /// `Time.RoundFrac`. This overload supplies UTC for the zone-free
    /// callers, exactly as [`Datum::convert_to`] does for
    /// [`Datum::convert_to_in`]; a caller that owns a session zone must use
    /// [`Datum::to_i64_in`].
    pub fn to_i64(&self) -> Result<Converted<i64>, DatumValueError> {
        self.to_i64_in(&crate::SessionTimeZone::utc())
    }

    /// Source `Datum.ToInt64` = `toSignedInteger(ctx, TypeLonglong)` with the
    /// statement's own `ctx.Location()`.
    pub fn to_i64_in(
        &self,
        zone: &crate::SessionTimeZone,
    ) -> Result<Converted<i64>, DatumValueError> {
        let converted = match self {
            Self::Int(value) => Converted {
                value: *value,
                event: None,
            },
            Self::UInt(value) => Converted {
                value: (*value).min(i64::MAX as u64) as i64,
                event: (*value > i64::MAX as u64).then_some(ScalarConversionEvent::Truncated),
            },
            Self::Real(value) | Self::Float32(value) => {
                let rounded = crate::round_float(*value);
                Converted {
                    value: rounded.clamp(i64::MIN as f64, i64::MAX as f64) as i64,
                    event: (!(i64::MIN as f64..=i64::MAX as f64).contains(&rounded))
                        .then_some(ScalarConversionEvent::Truncated),
                }
            }
            Self::String(value) => str_to_int(value.as_utf8()?, false),
            Self::Bytes(value) => str_to_int(std::str::from_utf8(value)?, false),
            // Go `toSignedInteger`'s temporal arms round the TEMPORAL value
            // to `DefaultFsp` FIRST and only then render it as a number, so a
            // fractional carry propagates through the sexagesimal fields
            // instead of landing on an impossible seconds digit. Its own
            // comment states the contract: `11:59:59.999999 -> 120000`, not
            // `115960`. The zone is load-bearing on the DATETIME arm the same
            // way it is for `convert_to_signed` -- a carry that lands on a DST
            // transition instant reads back the SESSION zone's wall clock.
            Self::Time(value) => decimal_to_i64(
                value
                    .round_frac(crate::DEFAULT_FSP, zone)
                    .map_err(|error| DatumValueError::Comparison(error.to_string()))?
                    .to_number(),
            ),
            Self::Duration(value) => decimal_to_i64(
                value
                    .round_frac(crate::DEFAULT_FSP)
                    .map_err(|error| DatumValueError::Comparison(error.to_string()))?
                    .to_number(),
            ),
            Self::Decimal(value) => decimal_to_i64(value.clone()),
            Self::Enum(value, _) => Converted {
                value: value.value().min(i64::MAX as u64) as i64,
                event: None,
            },
            Self::Set(value, _) => Converted {
                value: value.value().min(i64::MAX as u64) as i64,
                event: None,
            },
            Self::Json(value) => json_to_int64(value, false, DEFAULT_STATEMENT_FLAGS),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let outcome = value.to_int();
                Converted {
                    value: outcome.value() as i64,
                    event: outcome
                        .is_truncated()
                        .then_some(ScalarConversionEvent::Truncated),
                }
            }
            other => return Err(DatumValueError::Unsupported(other.kind(), "int64")),
        };
        Ok(converted)
    }

    /// Source `Datum.ToFloat64`.
    pub fn to_f64(&self) -> Result<Converted<f64>, DatumValueError> {
        let converted = match self {
            Self::Int(value) => Converted {
                value: *value as f64,
                event: None,
            },
            Self::UInt(value) => Converted {
                value: *value as f64,
                event: None,
            },
            Self::Real(value) => Converted {
                value: *value,
                event: None,
            },
            Self::Float32(value) => Converted {
                value: f64::from(*value as f32),
                event: None,
            },
            Self::String(value) => str_to_float(value.as_utf8()?, false),
            Self::Bytes(value) => str_to_float(std::str::from_utf8(value)?, false),
            Self::Time(value) => Converted {
                value: value.to_number().to_f64(),
                event: None,
            },
            Self::Duration(value) => Converted {
                value: value.to_number().to_f64(),
                event: None,
            },
            Self::Decimal(value) => Converted {
                value: value.to_f64(),
                event: None,
            },
            Self::Enum(value, _) => Converted {
                value: value.to_number(),
                event: None,
            },
            Self::Set(value, _) => Converted {
                value: value.to_number(),
                event: None,
            },
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let outcome = value.to_int();
                Converted {
                    value: outcome.value() as f64,
                    event: outcome
                        .is_truncated()
                        .then_some(ScalarConversionEvent::Truncated),
                }
            }
            Self::Json(value) => json_to_float(value),
            other => return Err(DatumValueError::Unsupported(other.kind(), "float64")),
        };
        Ok(converted)
    }

    /// Source `Datum.ToDecimal`.
    pub fn to_decimal(&self) -> Result<Converted<Decimal>, DatumValueError> {
        let converted = match self {
            Self::Int(value) => Converted {
                value: Decimal::from_int(*value),
                event: None,
            },
            Self::UInt(value) => Converted {
                value: Decimal::from_uint(*value),
                event: None,
            },
            Self::Real(value) | Self::Float32(value) => Converted {
                value: Decimal::from_signed_literal(&value.to_string()),
                event: None,
            },
            Self::String(value) => decimal_from_bytes(value.bytes())?,
            Self::Bytes(value) => decimal_from_bytes(value)?,
            Self::Time(value) => Converted {
                value: value.to_number(),
                event: None,
            },
            Self::Duration(value) => Converted {
                value: value.to_number(),
                event: None,
            },
            Self::Decimal(value) => Converted {
                value: value.clone(),
                event: None,
            },
            Self::Enum(value, _) => Converted {
                value: Decimal::from_uint(value.value()),
                event: None,
            },
            Self::Set(value, _) => Converted {
                value: Decimal::from_uint(value.value()),
                event: None,
            },
            Self::Json(value) => json_to_decimal(value),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                let outcome = value.to_int();
                Converted {
                    value: Decimal::from_uint(outcome.value()),
                    event: outcome
                        .is_truncated()
                        .then_some(ScalarConversionEvent::Truncated),
                }
            }
            other => return Err(DatumValueError::Unsupported(other.kind(), "decimal")),
        };
        Ok(converted)
    }

    /// Source `Datum.ToBytes`, whose default arm is `ToString`.
    ///
    /// `ToString`'s `KindBinaryLiteral`/`KindMysqlBit` arm is
    /// `d.GetBinaryLiteral().ToString()`, which is `string(b)` -- a Go string
    /// conversion, so the OCTETS pass through unvalidated, exactly as they do
    /// for `KindString`/`KindBytes`. `sql_string` cannot serve that arm here
    /// because a Rust `String` must be UTF-8, and refusing `0xAABBCCDDEEFF`
    /// is not something Go ever does (`UNCOMPRESSED_LENGTH(0xAABBCCDDEEFF)`
    /// is 3721182122, not an error).
    pub fn to_bytes(&self) -> Result<Vec<u8>, DatumStringError> {
        match self {
            Self::String(value) => Ok(value.bytes().to_vec()),
            Self::Bytes(value) => Ok(value.clone()),
            Self::BinaryLiteral(value) | Self::Bit(value) => Ok(value.as_bytes().to_vec()),
            _ => self.sql_string().map(String::into_bytes),
        }
    }

    /// Source `Datum.ToHashKey`.
    pub fn to_hash_key(&self) -> Result<Vec<u8>, DatumStringError> {
        let bytes = self.to_bytes()?;
        Ok(self.collation().unwrap_or(Collation::Binary).key(&bytes))
    }

    /// Source `Datum.ToMysqlJSON`.
    pub fn to_mysql_json(&self) -> Result<BinaryJSON, DatumValueError> {
        let value = match self {
            Self::Json(value) => return Ok(value.clone()),
            Self::Int(value) => BinaryJSONValue::Int64(*value),
            Self::UInt(value) => BinaryJSONValue::Uint64(*value),
            Self::Real(value) | Self::Float32(value) => BinaryJSONValue::Float64(*value),
            Self::Decimal(value) => BinaryJSONValue::Float64(value.to_f64()),
            Self::String(value) => BinaryJSONValue::String(value.as_utf8()?.to_owned()),
            Self::Bytes(value) => BinaryJSONValue::String(std::str::from_utf8(value)?.to_owned()),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                BinaryJSONValue::String(std::str::from_utf8(value.as_bytes())?.to_owned())
            }
            Self::Null => BinaryJSONValue::Null,
            Self::Time(value) => BinaryJSONValue::Time(*value),
            Self::Duration(value) => BinaryJSONValue::Duration(*value),
            _ => BinaryJSONValue::String(
                self.sql_string()
                    .map_err(|_| DatumValueError::Unsupported(self.kind(), "json"))?,
            ),
        };
        BinaryJSON::from_typed_value(&value).map_err(Into::into)
    }

    /// As [`Self::to_mysql_json`], but a `Bytes` payload -- and a `String`
    /// payload whose `field_type` is BINARY-charset -- embeds
    /// `field_type`'s own MySQL type code as a JSON `Opaque` value instead
    /// of an ordinary JSON string. Go's `getRealJSONValue`
    /// (`pkg/executor/aggfuncs/func_json_objectagg.go`), the value rule
    /// shared by `JSON_ARRAYAGG` and `JSON_OBJECTAGG`, wraps `KindBytes`
    /// unconditionally (a byte datum has no other charset) and `KindString`
    /// only when its field type's charset is `binary`.
    ///
    /// A fixed-length `BINARY(n)` column (`FieldTypeCode::String`) pads the
    /// embedded buffer to `flen` bytes before encoding, matching Go's own
    /// tailing-zero rule (captured: `BINARY(3)` holding `"ab"` renders
    /// `base64:type254:YWIA`, the trailing NUL included). Every other datum
    /// kind defers to `to_mysql_json` unchanged.
    pub fn to_mysql_json_with_source_type(
        &self,
        field_type: &crate::FieldType,
    ) -> Result<BinaryJSON, DatumValueError> {
        let buf = match self {
            Self::Bytes(value) => Some(value.clone()),
            Self::String(value) if field_type.is_binary_string() => Some(value.bytes().to_vec()),
            _ => None,
        };
        let Some(mut buf) = buf else {
            return self.to_mysql_json();
        };
        if field_type.code() == crate::FieldTypeCode::String {
            let flen = field_type.flen();
            if flen > 0 {
                buf.resize(flen as usize, 0);
            }
        }
        let opaque = crate::Opaque {
            type_code: field_type.code().mysql_type(),
            bytes: buf,
        };
        BinaryJSON::from_typed_value(&BinaryJSONValue::Opaque(opaque)).map_err(Into::into)
    }
}

fn decimal_to_i64(decimal: Decimal) -> Converted<i64> {
    match decimal.round_to_i64() {
        Some(value) => Converted { value, event: None },
        None => Converted {
            value: decimal.round_to_i64_saturating(),
            event: Some(ScalarConversionEvent::Truncated),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::Datum;
    use crate::{BinaryJSON, BinaryLiteral, Collation, Decimal, MySqlDuration, TimeType};

    #[test]
    fn source_to_bool_rows() {
        for (datum, expected) in [
            (Datum::Int(0), 0),
            (Datum::UInt(0), 0),
            (Datum::Float32(0.1), 1),
            (Datum::Real(0.499), 1),
            (Datum::new_string(""), 0),
            (Datum::new_string("0.1"), 1),
            (Datum::new_bytes([]), 0),
            (Datum::new_bytes(b"0.1"), 1),
            (
                Datum::new_binary_literal(BinaryLiteral::from_uint(0, None)),
                0,
            ),
            (Datum::new_json(BinaryJSON::parse("1").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("0").unwrap()), 0),
            (Datum::new_json(BinaryJSON::parse("\"0\"").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("null").unwrap()), 1),
            (Datum::new_json(BinaryJSON::parse("false").unwrap()), 1),
        ] {
            assert_eq!(datum.to_bool().unwrap().value, expected, "{datum:?}");
        }
        let time = crate::parse_time(
            "2011-11-10 11:11:11.999999",
            TimeType::Timestamp,
            6,
            false,
            true,
            false,
            &chrono_tz::UTC,
        )
        .unwrap()
        .time;
        assert_eq!(Datum::new_time(time).to_bool().unwrap().value, 1);
        let duration = MySqlDuration::new(11, 11, 11, 999_999, 6).unwrap();
        assert_eq!(Datum::new_duration(duration).to_bool().unwrap().value, 1);
        assert_eq!(
            Datum::new_decimal(Decimal::from_signed_literal("0.14159"))
                .to_bool()
                .unwrap()
                .value,
            1
        );
    }

    #[test]
    fn source_to_int_float_decimal_and_bytes_rows() {
        for (datum, expected) in [
            (Datum::new_string("0"), 0),
            (Datum::Int(0), 0),
            (Datum::UInt(0), 0),
            (Datum::Float32(3.1), 3),
            (Datum::Real(3.1), 3),
            (
                Datum::new_binary_literal(BinaryLiteral::from_uint(100, None)),
                100,
            ),
            (Datum::new_json(BinaryJSON::parse("3").unwrap()), 3),
            (
                Datum::new_decimal(Decimal::from_signed_literal("3.1415926")),
                3,
            ),
        ] {
            assert_eq!(datum.to_i64().unwrap().value, expected, "{datum:?}");
        }

        for (datum, expected) in [
            (Datum::Int(-3), -3.0),
            (Datum::UInt(3), 3.0),
            (Datum::Float32(3.1), f64::from(3.1_f32)),
            (Datum::Real(3.1), 3.1),
            (Datum::new_string("3.25"), 3.25),
            (
                Datum::new_decimal(Decimal::from_signed_literal("-4.5")),
                -4.5,
            ),
            (Datum::new_json(BinaryJSON::parse("4.5").unwrap()), 4.5),
        ] {
            assert_eq!(datum.to_f64().unwrap().value, expected, "{datum:?}");
        }

        for (datum, expected) in [
            (Datum::Int(1), b"1".as_slice()),
            (Datum::new_decimal(Decimal::from_int(1)), b"1".as_slice()),
            (Datum::Real(1.23), b"1.23".as_slice()),
            (Datum::new_string("abc"), b"abc".as_slice()),
            (Datum::Null, b"".as_slice()),
        ] {
            assert_eq!(datum.to_bytes().unwrap(), expected, "{datum:?}");
        }

        // `MyDecimal.FromString` keeps the accepted `1.1` prefix and reports
        // ErrTruncated for the trailing `.1`; it does not fall back to zero.
        let malformed = Datum::new_string("1.1.1").to_decimal().unwrap();
        assert_eq!(malformed.value, Decimal::from_signed_literal("1.1"));
        assert_eq!(
            malformed.event,
            Some(crate::ScalarConversionEvent::Truncated)
        );
    }

    /// `Datum::to_mysql_json_with_source_type`: a BINARY-charset argument
    /// embeds the source column's own MySQL type code as a JSON `Opaque`
    /// value, Go's `getRealJSONValue`
    /// (`pkg/executor/aggfuncs/func_json_objectagg.go`), the value rule
    /// `JSON_ARRAYAGG`/`JSON_OBJECTAGG` share.
    ///
    /// Every expected string below is captured verbatim from a real TiDB
    /// server (`zz_dump_opaque_test.go`, `TestZZDumpOpaque`):
    /// `SELECT JSON_ARRAYAGG(col) FROM t` over one-column tables of each
    /// listed type, each holding the two-byte string `"ab"`.
    #[test]
    fn to_mysql_json_with_source_type_matches_captured_opaque_rendering() {
        use crate::{FieldType, FieldTypeCode};

        // VARBINARY(10): mysql.TypeVarchar (15) -- VARBINARY and VARCHAR
        // share this parse-time code, so the binary distinction rides the
        // collation, not the code, at DDL time.
        let varbinary = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        assert_eq!(
            Datum::new_bytes(*b"ab")
                .to_mysql_json_with_source_type(&varbinary)
                .unwrap()
                .to_string(),
            "\"base64:type15:YWI=\""
        );

        // BINARY(3): mysql.TypeString (254), fixed-length and zero-padded to
        // `flen` before encoding -- the captured `YWIA` decodes to
        // `61 62 00` (`ab\0`), the tailing pad byte included.
        let mut binary = FieldType::new(FieldTypeCode::String);
        binary.set_flen(3);
        assert_eq!(
            Datum::new_bytes(*b"ab")
                .to_mysql_json_with_source_type(&binary)
                .unwrap()
                .to_string(),
            "\"base64:type254:YWIA\""
        );

        // TINYBLOB/BLOB/MEDIUMBLOB/LONGBLOB: mysql.Type{Tiny,Medium,Long}Blob
        // and mysql.TypeBlob (249/250/251/252), never padded.
        for (code, expected) in [
            (FieldTypeCode::TinyBlob, "\"base64:type249:YWI=\""),
            (FieldTypeCode::MediumBlob, "\"base64:type250:YWI=\""),
            (FieldTypeCode::LongBlob, "\"base64:type251:YWI=\""),
            (FieldTypeCode::Blob, "\"base64:type252:YWI=\""),
        ] {
            let field_type = FieldType::new(code);
            assert_eq!(
                Datum::new_bytes(*b"ab")
                    .to_mysql_json_with_source_type(&field_type)
                    .unwrap()
                    .to_string(),
                expected,
                "{code:?}"
            );
        }

        // `CAST(x AS BINARY)`: mysql.TypeVarString (253), captured from
        // `JSON_ARRAY(CAST('ab' AS BINARY))` = `["base64:type253:YWI="]`.
        let cast_binary = FieldType::new(FieldTypeCode::VarString);
        assert_eq!(
            Datum::new_bytes(*b"ab")
                .to_mysql_json_with_source_type(&cast_binary)
                .unwrap()
                .to_string(),
            "\"base64:type253:YWI=\""
        );

        // A non-binary-charset argument (an ordinary VARCHAR column) is
        // unaffected: it stays a plain JSON string, matching
        // `to_mysql_json`.
        let varchar = FieldType::new(FieldTypeCode::Varchar);
        assert_eq!(
            Datum::new_string("ab")
                .to_mysql_json_with_source_type(&varchar)
                .unwrap()
                .to_string(),
            "\"ab\""
        );
    }

    /// `JSON_TYPE()` of a BINARY-charset opaque value reports `"BLOB"`, not
    /// `"OPAQUE"` -- captured: `JSON_TYPE(JSON_EXTRACT(arrayagg_result,
    /// '$[0]'))` over a VARBINARY-sourced element is `"BLOB"`.
    #[test]
    fn opaque_json_type_of_binary_charset_value_is_blob() {
        use crate::{FieldType, FieldTypeCode};

        let varbinary = FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Binary);
        let opaque = Datum::new_bytes(*b"ab")
            .to_mysql_json_with_source_type(&varbinary)
            .unwrap();
        assert_eq!(opaque.type_name().unwrap(), "BLOB");
    }
}
