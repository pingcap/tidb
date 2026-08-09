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

//! The `jsonDatum` persistence envelope.
//!
//! Mirrors `Datum.MarshalJSON` / `Datum.UnmarshalJSON` and the `jsonDatum`
//! struct of `pkg/types/datum.go`, including Go's kind codes and the
//! base64 byte encoding `encoding/json` applies to a `[]byte` field.

use super::{Datum, DatumKind, DatumValueError};
use crate::{
    BinaryJSON, BinaryLiteral, Collation, Decimal, MySqlDuration, MysqlEnum, MysqlSet, Time,
};

impl Datum {
    /// Source `Datum.MarshalJSON` persistence shape.
    ///
    /// The Rust enum eliminates Go's empty metadata slots, so only fields
    /// owned by the active variant are emitted. Field names and byte-base64
    /// encoding remain compatible with Go's `jsonDatum` envelope.
    pub fn marshal_json(&self) -> Result<Vec<u8>, DatumValueError> {
        let mut object = serde_json::Map::new();
        object.insert(
            "k".to_owned(),
            serde_json::Value::from(kind_code(self.kind())),
        );
        match self {
            Self::Null | Self::MinNotNull | Self::MaxValue => {}
            Self::Int(value) => insert_i64(&mut object, *value),
            Self::UInt(value) => insert_i64(&mut object, *value as i64),
            Self::Real(value) | Self::Float32(value) => {
                insert_i64(&mut object, value.to_bits() as i64)
            }
            Self::String(value) => {
                insert_bytes(&mut object, value.bytes());
                insert_collation(&mut object, value.collation());
            }
            Self::Bytes(value) => {
                insert_bytes(&mut object, value);
                insert_collation(&mut object, Collation::Binary);
            }
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                insert_bytes(&mut object, value.as_bytes())
            }
            Self::Decimal(value) => {
                object.insert("mydecimal".to_owned(), value.mysql_json_value());
            }
            Self::Duration(value) => {
                insert_i64(&mut object, value.nanoseconds());
                object.insert("decimal".to_owned(), serde_json::Value::from(value.fsp()));
            }
            Self::Enum(value, collation) => {
                insert_i64(&mut object, value.value() as i64);
                insert_bytes(&mut object, value.name().as_bytes());
                insert_collation(&mut object, *collation);
            }
            Self::Set(value, collation) => {
                insert_i64(&mut object, value.value() as i64);
                insert_bytes(&mut object, value.name().as_bytes());
                insert_collation(&mut object, *collation);
            }
            Self::Time(value) => {
                object.insert("time".to_owned(), serde_json::Value::from(value.go_raw()));
            }
            Self::Json(value) => {
                insert_i64(&mut object, i64::from(value.type_code()));
                insert_bytes(&mut object, value.value());
            }
            Self::Raw(value) => insert_bytes(&mut object, value),
            Self::VectorFloat32(value) => insert_bytes(&mut object, &value.serialize()),
        }
        serde_json::to_vec(&serde_json::Value::Object(object))
            .map_err(|error| DatumValueError::Comparison(error.to_string()))
    }

    /// Source `Datum.UnmarshalJSON` persistence shape.
    pub fn unmarshal_json(data: &[u8]) -> Result<Self, DatumValueError> {
        let value: serde_json::Value = serde_json::from_slice(data)
            .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
        let object = value.as_object().ok_or_else(|| {
            DatumValueError::Comparison("datum JSON must be an object".to_owned())
        })?;
        let kind = object
            .get("k")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| DatumValueError::Comparison("datum JSON is missing k".to_owned()))?
            as u8;
        let i = object
            .get("i")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        let bytes = object
            .get("b")
            .and_then(serde_json::Value::as_str)
            .map(decode_base64)
            .transpose()?
            .unwrap_or_default();
        let collation = object
            .get("collation")
            .and_then(serde_json::Value::as_str)
            .and_then(Collation::from_name)
            .unwrap_or(Collation::Binary);
        match kind {
            0 => Ok(Self::Null),
            1 => Ok(Self::Int(i)),
            2 => Ok(Self::UInt(i as u64)),
            3 => Ok(Self::Float32(f64::from_bits(i as u64))),
            4 => Ok(Self::Real(f64::from_bits(i as u64))),
            5 => Ok(Self::new_collation_string(bytes, collation)),
            6 => Ok(Self::new_bytes(bytes)),
            7 => Ok(Self::new_binary_literal(BinaryLiteral::from(bytes))),
            8 => object
                .get("mydecimal")
                .ok_or_else(|| {
                    DatumValueError::Comparison(
                        "decimal datum JSON is missing mydecimal".to_owned(),
                    )
                })
                .and_then(|value| {
                    Decimal::from_mysql_json_value(value)
                        .map(Self::new_decimal)
                        .map_err(DatumValueError::Comparison)
                }),
            9 => MySqlDuration::from_nanoseconds(
                i,
                object
                    .get("decimal")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0),
            )
            .map(Self::new_duration)
            .map_err(|error| DatumValueError::Comparison(error.to_string())),
            10 => Ok(Self::new_enum(MysqlEnum::new(bytes, i as u64), collation)),
            11 => Ok(Self::new_mysql_bit(BinaryLiteral::from(bytes))),
            12 => Ok(Self::new_set(MysqlSet::new(bytes, i as u64), collation)),
            13 => object
                .get("time")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| {
                    DatumValueError::Comparison("time datum JSON is missing time".to_owned())
                })
                .and_then(|raw| {
                    Time::from_go_raw(raw)
                        .map(Self::new_time)
                        .map_err(|error| DatumValueError::Comparison(error.to_string()))
                }),
            15 => Ok(Self::MinNotNull),
            16 => Ok(Self::MaxValue),
            17 => Ok(Self::new_raw(bytes)),
            18 => Ok(Self::new_json(BinaryJSON::from_binary_parts(
                i as u8, bytes,
            ))),
            19 => crate::deserialize_vector_float32(&bytes)
                .map(|(value, _)| Self::new_vector_float32(value))
                .map_err(|error| DatumValueError::Comparison(error.to_string())),
            other => Err(DatumValueError::Comparison(format!(
                "unsupported datum kind: {other}"
            ))),
        }
    }
}

const fn kind_code(kind: DatumKind) -> u8 {
    match kind {
        DatumKind::Null => 0,
        DatumKind::Int => 1,
        DatumKind::UInt => 2,
        DatumKind::Float32 => 3,
        DatumKind::Real => 4,
        DatumKind::String => 5,
        DatumKind::Bytes => 6,
        DatumKind::BinaryLiteral => 7,
        DatumKind::Decimal => 8,
        DatumKind::Duration => 9,
        DatumKind::Enum => 10,
        DatumKind::Bit => 11,
        DatumKind::Set => 12,
        DatumKind::Time => 13,
        DatumKind::MinNotNull => 15,
        DatumKind::MaxValue => 16,
        DatumKind::Raw => 17,
        DatumKind::Json => 18,
        DatumKind::VectorFloat32 => 19,
    }
}

fn insert_i64(object: &mut serde_json::Map<String, serde_json::Value>, value: i64) {
    if value != 0 {
        object.insert("i".to_owned(), serde_json::Value::from(value));
    }
}

fn insert_bytes(object: &mut serde_json::Map<String, serde_json::Value>, value: &[u8]) {
    if !value.is_empty() {
        object.insert(
            "b".to_owned(),
            serde_json::Value::String(encode_base64(value)),
        );
    }
}

fn insert_collation(object: &mut serde_json::Map<String, serde_json::Value>, collation: Collation) {
    object.insert(
        "collation".to_owned(),
        serde_json::Value::String(collation.name().to_owned()),
    );
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(chunk.get(1).copied().unwrap_or(0)) << 8)
            | u32::from(chunk.get(2).copied().unwrap_or(0));
        output.push(TABLE[((value >> 18) & 63) as usize] as char);
        output.push(TABLE[((value >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((value >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(value & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

fn decode_base64(text: &str) -> Result<Vec<u8>, DatumValueError> {
    if !text.len().is_multiple_of(4) {
        return Err(DatumValueError::Comparison(
            "invalid base64 datum bytes".to_owned(),
        ));
    }
    let mut output = Vec::with_capacity(text.len() / 4 * 3);
    for chunk in text.as_bytes().chunks_exact(4) {
        let a = base64_digit(chunk[0])?;
        let b = base64_digit(chunk[1])?;
        let c = if chunk[2] == b'=' {
            0
        } else {
            base64_digit(chunk[2])?
        };
        let d = if chunk[3] == b'=' {
            0
        } else {
            base64_digit(chunk[3])?
        };
        let value =
            (u32::from(a) << 18) | (u32::from(b) << 12) | (u32::from(c) << 6) | u32::from(d);
        output.push((value >> 16) as u8);
        if chunk[2] != b'=' {
            output.push((value >> 8) as u8);
        }
        if chunk[3] != b'=' {
            output.push(value as u8);
        }
    }
    Ok(output)
}

fn base64_digit(byte: u8) -> Result<u8, DatumValueError> {
    match byte {
        b'A'..=b'Z' => Ok(byte - b'A'),
        b'a'..=b'z' => Ok(byte - b'a' + 26),
        b'0'..=b'9' => Ok(byte - b'0' + 52),
        b'+' => Ok(62),
        b'/' => Ok(63),
        _ => Err(DatumValueError::Comparison(
            "invalid base64 datum bytes".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::Datum;
    use crate::{
        BinaryJSON, BinaryLiteral, Collation, CoreTime, Decimal, MySqlDuration, Time, TimeType,
    };

    /// Source: `pkg/types/datum_test.go::TestMarshalDatum`.
    #[test]
    fn test_marshal_datum() {
        let time = Time::new(
            CoreTime::from_date(2018, 3, 8, 16, 1, 0, 315_313),
            TimeType::Timestamp,
            6,
        )
        .unwrap();
        let values = vec![
            Datum::Int(1),
            Datum::UInt(72),
            Datum::Float32(f64::from(1.23_f32)),
            Datum::Real(1.23),
            Datum::Real(f64::NEG_INFINITY),
            Datum::new_decimal(Decimal::from_signed_literal("1.2345")),
            Datum::new_string("abcde"),
            Datum::new_collation_string("abcde", Collation::Binary),
            Datum::new_duration(MySqlDuration::from_nanoseconds(1, 0).unwrap()),
            Datum::new_time(time),
            Datum::new_bytes(b"abcde"),
            Datum::new_binary_literal(BinaryLiteral::from(&[0x81])),
            Datum::new_mysql_bit(BinaryLiteral::from(&[0x98, 0x76, 0x54, 0x32])),
            Datum::new_enum(crate::MysqlEnum::new("a", 1), Collation::DEFAULT),
            Datum::new_enum(crate::MysqlEnum::new("a", 1), Collation::AsciiBin),
            Datum::new_set(crate::MysqlSet::new("a", 1), Collation::GbkBin),
            Datum::new_json(BinaryJSON::parse("1").unwrap()),
            Datum::MinNotNull,
            Datum::MaxValue,
        ];
        assert_eq!(values.len(), 19, "one entry per Go source row");
        for (index, value) in values.into_iter().enumerate() {
            let encoded = value.marshal_json().unwrap();
            let decoded = Datum::unmarshal_json(&encoded).unwrap();
            assert_eq!(decoded, value, "round-trip row {index}: {encoded:?}");
        }
    }

    #[test]
    fn extended_kind_round_trips() {
        for value in [
            Datum::new_raw(b"raw"),
            Datum::new_vector_float32(crate::VectorFloat32::parse("[1,2]").unwrap()),
        ] {
            let encoded = value.marshal_json().unwrap();
            assert_eq!(Datum::unmarshal_json(&encoded).unwrap(), value);
        }
    }
}
