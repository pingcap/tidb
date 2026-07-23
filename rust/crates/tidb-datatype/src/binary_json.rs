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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt;

use serde_json::{Map, Number, Value};

use crate::{CoreTime, MySqlDuration, Time, TimeType};

/// BinaryJSON object type code.
pub const JSON_TYPE_CODE_OBJECT: u8 = 0x01;
/// BinaryJSON array type code.
pub const JSON_TYPE_CODE_ARRAY: u8 = 0x03;
/// BinaryJSON literal type code.
pub const JSON_TYPE_CODE_LITERAL: u8 = 0x04;
/// BinaryJSON signed integer type code.
pub const JSON_TYPE_CODE_INT64: u8 = 0x09;
/// BinaryJSON unsigned integer type code.
pub const JSON_TYPE_CODE_UINT64: u8 = 0x0a;
/// BinaryJSON double type code.
pub const JSON_TYPE_CODE_FLOAT64: u8 = 0x0b;
/// BinaryJSON string type code.
pub const JSON_TYPE_CODE_STRING: u8 = 0x0c;
/// BinaryJSON opaque type code.
pub const JSON_TYPE_CODE_OPAQUE: u8 = 0x0d;
/// BinaryJSON DATE type code.
pub const JSON_TYPE_CODE_DATE: u8 = 0x0e;
/// BinaryJSON DATETIME type code.
pub const JSON_TYPE_CODE_DATETIME: u8 = 0x0f;
/// BinaryJSON TIMESTAMP type code.
pub const JSON_TYPE_CODE_TIMESTAMP: u8 = 0x10;
/// BinaryJSON TIME/duration type code.
pub const JSON_TYPE_CODE_DURATION: u8 = 0x11;

/// BinaryJSON null literal.
pub const JSON_LITERAL_NULL: u8 = 0;
/// BinaryJSON true literal.
pub const JSON_LITERAL_TRUE: u8 = 1;
/// BinaryJSON false literal.
pub const JSON_LITERAL_FALSE: u8 = 2;

const HEADER_SIZE: usize = 8;
const KEY_ENTRY_SIZE: usize = 6;
const VALUE_ENTRY_SIZE: usize = 5;
const MAX_JSON_DEPTH: usize = 100;

/// Source-compatible `type code + value bytes` BinaryJSON representation.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BinaryJSON {
    type_code: u8,
    value: Vec<u8>,
}

/// Lossless logical tree used by binary JSON operations.
///
/// Container structure is decoded, while every scalar remains an exact
/// `type code + payload` value. This preserves opaque and temporal tags across
/// extract/modify/merge operations without inventing a serde sentinel format.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum JSONNode {
    Scalar(BinaryJSON),
    Array(Vec<JSONNode>),
    Object(Vec<(String, JSONNode)>),
}

/// Raw MySQL value embedded in binary JSON.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Opaque {
    /// MySQL field type code.
    pub type_code: u8,
    /// Uninterpreted field payload.
    pub bytes: Vec<u8>,
}

/// The complete set of source values accepted by `CreateBinaryJSON`.
///
/// A typed enum replaces Go's runtime `any` switch, making unsupported inputs
/// unrepresentable while retaining `json.Number`'s signed-integer-then-double
/// conversion rule.
#[derive(Clone, Debug, PartialEq)]
pub enum BinaryJSONValue {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// Signed integer.
    Int64(i64),
    /// Unsigned integer.
    Uint64(u64),
    /// Double-precision number.
    Float64(f64),
    /// Source `json.Number` text.
    Number(String),
    /// UTF-8 JSON string.
    String(String),
    /// Already encoded binary JSON.
    Binary(BinaryJSON),
    /// Ordered JSON array.
    Array(Vec<BinaryJSONValue>),
    /// JSON object; keys encode in bytewise order.
    Object(BTreeMap<String, BinaryJSONValue>),
    /// MySQL opaque value.
    Opaque(Opaque),
    /// MySQL date, datetime, or timestamp.
    Time(Time),
    /// MySQL TIME duration.
    Duration(MySqlDuration),
}

/// Invalid JSON text or binary representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BinaryJSONError {
    /// The JSON document contains no value.
    EmptyDocument,
    /// A complete root value is followed by another token.
    TrailingValues,
    /// Text is not valid JSON.
    InvalidText,
    /// Binary bytes violate the source layout.
    InvalidBinary,
    /// Document nesting exceeds TiDB's limit.
    TooDeep,
    /// An object key cannot fit the source uint16 key length.
    KeyTooLong,
    /// One UTF-16 high or low surrogate appeared without its pair.
    LoneSurrogate,
    /// A modifying operation received a wildcard, range, recursive, or root path.
    InvalidPath,
}

impl fmt::Display for BinaryJSONError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyDocument => formatter.write_str("The document is empty"),
            Self::TrailingValues => {
                formatter.write_str("The document root must not be followed by other values.")
            }
            Self::InvalidText => formatter.write_str("invalid JSON text"),
            Self::InvalidBinary => formatter.write_str("invalid binary JSON"),
            Self::TooDeep => formatter.write_str("JSON document is too deep"),
            Self::KeyTooLong => formatter.write_str(
                "[types:8129]TiDB does not yet support JSON objects with the key length >= 65536",
            ),
            Self::LoneSurrogate => formatter.write_str("invalid lone UTF-16 surrogate"),
            Self::InvalidPath => formatter.write_str("invalid JSON path for modification"),
        }
    }
}

impl std::error::Error for BinaryJSONError {}

impl BinaryJSON {
    /// Reconstructs one value from TiDB's persisted `type code + value`
    /// representation.
    ///
    /// Rowcodec already owns the exact value boundary through its offset
    /// table, so this constructor deliberately preserves the bytes without
    /// reparsing or normalizing the document.
    pub fn from_encoded_parts(type_code: u8, value: impl Into<Vec<u8>>) -> Self {
        Self {
            type_code,
            value: value.into(),
        }
    }

    /// Reconstructs the exact internal type-code/payload pair stored by
    /// `Datum.UnmarshalJSON`.
    pub(crate) fn from_binary_parts(type_code: u8, value: Vec<u8>) -> Self {
        Self::from_encoded_parts(type_code, value)
    }

    /// Constructs from an exact source type code and value payload.
    pub fn from_raw(type_code: u8, value: Vec<u8>) -> Result<Self, BinaryJSONError> {
        let json = Self { type_code, value };
        json.to_node()?;
        Ok(json)
    }

    /// Builds a source-layout opaque JSON value.
    pub fn from_opaque(value: Opaque) -> Self {
        let mut bytes = Vec::with_capacity(2 + value.bytes.len());
        bytes.push(value.type_code);
        encode_uvarint(value.bytes.len(), &mut bytes);
        bytes.extend_from_slice(&value.bytes);
        Self {
            type_code: JSON_TYPE_CODE_OPAQUE,
            value: bytes,
        }
    }

    /// Embeds a MySQL date, datetime, or timestamp.
    pub fn from_time(value: Time) -> Self {
        let type_code = match value.kind() {
            TimeType::Date => JSON_TYPE_CODE_DATE,
            TimeType::DateTime => JSON_TYPE_CODE_DATETIME,
            TimeType::Timestamp => JSON_TYPE_CODE_TIMESTAMP,
        };
        Self {
            type_code,
            value: value.core_time().raw().to_le_bytes().to_vec(),
        }
    }

    /// Decodes an embedded MySQL date, datetime, or timestamp.
    pub fn as_time(&self, fsp: i64) -> Result<Time, BinaryJSONError> {
        let kind = match self.type_code {
            JSON_TYPE_CODE_DATE => TimeType::Date,
            JSON_TYPE_CODE_DATETIME => TimeType::DateTime,
            JSON_TYPE_CODE_TIMESTAMP => TimeType::Timestamp,
            _ => return Err(BinaryJSONError::InvalidBinary),
        };
        let raw = u64::from_le_bytes(
            self.value
                .as_slice()
                .try_into()
                .map_err(|_| BinaryJSONError::InvalidBinary)?,
        );
        Time::new(CoreTime::from_raw(raw), kind, fsp).map_err(|_| BinaryJSONError::InvalidBinary)
    }

    /// Embeds a MySQL TIME duration.
    pub fn from_duration(value: MySqlDuration) -> Self {
        let mut bytes = value.nanoseconds().to_le_bytes().to_vec();
        bytes.extend_from_slice(&u32::from(value.fsp()).to_le_bytes());
        Self {
            type_code: JSON_TYPE_CODE_DURATION,
            value: bytes,
        }
    }

    /// Decodes an embedded MySQL TIME duration.
    pub fn as_duration(&self) -> Result<MySqlDuration, BinaryJSONError> {
        if self.type_code != JSON_TYPE_CODE_DURATION || self.value.len() != 12 {
            return Err(BinaryJSONError::InvalidBinary);
        }
        let nanoseconds = i64::from_le_bytes(self.value[..8].try_into().unwrap());
        let fsp = u32::from_le_bytes(self.value[8..].try_into().unwrap());
        MySqlDuration::from_nanoseconds(nanoseconds, i64::from(fsp))
            .map_err(|_| BinaryJSONError::InvalidBinary)
    }

    /// Parses JSON text and builds TiDB's binary representation.
    pub fn parse(text: &str) -> Result<Self, BinaryJSONError> {
        if text.trim().is_empty() {
            return Err(BinaryJSONError::EmptyDocument);
        }
        let mut value: Value = serde_json::from_str(text).map_err(|error| {
            if error.to_string().contains("trailing characters") {
                BinaryJSONError::TrailingValues
            } else {
                BinaryJSONError::InvalidText
            }
        })?;
        normalize_parsed_numbers(&mut value)?;
        Self::from_value(&value)
    }

    /// Builds a BinaryJSON value from a validated JSON tree.
    pub fn from_value(value: &Value) -> Result<Self, BinaryJSONError> {
        encode_value(value, 0)
    }

    /// Builds a BinaryJSON value from every source-supported input type.
    pub fn from_typed_value(value: &BinaryJSONValue) -> Result<Self, BinaryJSONError> {
        Self::from_node(&typed_value_to_node(value)?)
    }

    /// Calculates the source binary payload size for a typed input.
    pub fn calculate_typed_size(value: &BinaryJSONValue) -> Result<usize, BinaryJSONError> {
        Self::from_typed_value(value).map(|value| value.value.len())
    }

    /// Returns the source type code.
    pub const fn type_code(&self) -> u8 {
        self.type_code
    }

    /// Returns the exact bytes after the type code.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns `type code + value` for the TiDB codec boundary.
    pub fn encoded(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(1 + self.value.len());
        encoded.push(self.type_code);
        encoded.extend_from_slice(&self.value);
        encoded
    }

    /// Decodes into a JSON tree without losing signed/unsigned integer text.
    pub fn to_value(&self) -> Result<Value, BinaryJSONError> {
        decode_value(self.type_code, &self.value, 0)
    }

    pub(crate) fn to_node(&self) -> Result<JSONNode, BinaryJSONError> {
        decode_node(self, 0)
    }

    pub(crate) fn from_node(node: &JSONNode) -> Result<Self, BinaryJSONError> {
        encode_node(node, 0)
    }

    /// Returns a signed integer payload.
    pub fn as_i64(&self) -> Option<i64> {
        if self.type_code != JSON_TYPE_CODE_INT64 {
            return None;
        }
        Some(i64::from_le_bytes(self.value.as_slice().try_into().ok()?))
    }

    /// Returns an unsigned integer payload.
    pub fn as_u64(&self) -> Option<u64> {
        if self.type_code != JSON_TYPE_CODE_UINT64 {
            return None;
        }
        Some(u64::from_le_bytes(self.value.as_slice().try_into().ok()?))
    }

    /// Returns a double payload.
    pub fn as_f64(&self) -> Option<f64> {
        if self.type_code != JSON_TYPE_CODE_FLOAT64 {
            return None;
        }
        Some(f64::from_bits(u64::from_le_bytes(
            self.value.as_slice().try_into().ok()?,
        )))
    }

    /// Returns string bytes.
    pub fn as_string(&self) -> Option<&[u8]> {
        if self.type_code != JSON_TYPE_CODE_STRING {
            return None;
        }
        let (length, prefix) = decode_uvarint(&self.value).ok()?;
        self.value.get(prefix..prefix + length)
    }

    /// Decodes an opaque value without changing its bytes.
    pub fn opaque(&self) -> Result<Opaque, BinaryJSONError> {
        if self.type_code != JSON_TYPE_CODE_OPAQUE {
            return Err(BinaryJSONError::InvalidBinary);
        }
        let (&type_code, payload) = self
            .value
            .split_first()
            .ok_or(BinaryJSONError::InvalidBinary)?;
        let (length, prefix) = decode_uvarint(payload)?;
        let bytes = payload
            .get(prefix..prefix + length)
            .ok_or(BinaryJSONError::InvalidBinary)?;
        if prefix + length != payload.len() {
            return Err(BinaryJSONError::InvalidBinary);
        }
        Ok(Opaque {
            type_code,
            bytes: bytes.to_vec(),
        })
    }

    /// Returns MySQL's JSON_TYPE name.
    pub fn type_name(&self) -> Result<&'static str, BinaryJSONError> {
        match self.type_code {
            JSON_TYPE_CODE_OBJECT => Ok("OBJECT"),
            JSON_TYPE_CODE_ARRAY => Ok("ARRAY"),
            JSON_TYPE_CODE_LITERAL if self.value == [JSON_LITERAL_NULL] => Ok("NULL"),
            JSON_TYPE_CODE_LITERAL => Ok("BOOLEAN"),
            JSON_TYPE_CODE_INT64 => Ok("INTEGER"),
            JSON_TYPE_CODE_UINT64 => Ok("UNSIGNED INTEGER"),
            JSON_TYPE_CODE_FLOAT64 => Ok("DOUBLE"),
            JSON_TYPE_CODE_STRING => Ok("STRING"),
            JSON_TYPE_CODE_OPAQUE => match self.opaque()?.type_code {
                0x0f | 0xf9..=0xfe => Ok("BLOB"),
                0x10 => Ok("BIT"),
                _ => Ok("OPAQUE"),
            },
            JSON_TYPE_CODE_DATE => Ok("DATE"),
            JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => Ok("DATETIME"),
            JSON_TYPE_CODE_DURATION => Ok("TIME"),
            _ => Err(BinaryJSONError::InvalidBinary),
        }
    }

    /// Implements JSON_UNQUOTE for one binary JSON value.
    pub fn unquote(&self) -> Result<String, BinaryJSONError> {
        match self.as_string() {
            Some(bytes) => {
                let text =
                    std::str::from_utf8(bytes).map_err(|_| BinaryJSONError::InvalidBinary)?;
                unquote_string(text)
            }
            None => Ok(self.to_string()),
        }
    }
}

fn typed_value_to_node(value: &BinaryJSONValue) -> Result<JSONNode, BinaryJSONError> {
    let scalar = |value| Ok(JSONNode::Scalar(value));
    match value {
        BinaryJSONValue::Null => scalar(literal(JSON_LITERAL_NULL)),
        BinaryJSONValue::Bool(true) => scalar(literal(JSON_LITERAL_TRUE)),
        BinaryJSONValue::Bool(false) => scalar(literal(JSON_LITERAL_FALSE)),
        BinaryJSONValue::Int64(value) => scalar(BinaryJSON {
            type_code: JSON_TYPE_CODE_INT64,
            value: value.to_le_bytes().to_vec(),
        }),
        BinaryJSONValue::Uint64(value) => scalar(BinaryJSON {
            type_code: JSON_TYPE_CODE_UINT64,
            value: value.to_le_bytes().to_vec(),
        }),
        BinaryJSONValue::Float64(value) => Number::from_f64(*value)
            .ok_or(BinaryJSONError::InvalidText)
            .and_then(|value| encode_number(&value))
            .and_then(scalar),
        BinaryJSONValue::Number(value) => {
            if let Ok(value) = value.parse::<i64>() {
                typed_value_to_node(&BinaryJSONValue::Int64(value))
            } else {
                let value = value
                    .parse::<f64>()
                    .map_err(|_| BinaryJSONError::InvalidText)?;
                typed_value_to_node(&BinaryJSONValue::Float64(value))
            }
        }
        BinaryJSONValue::String(value) => {
            encode_value(&Value::String(value.clone()), 0).and_then(scalar)
        }
        BinaryJSONValue::Binary(value) => value.to_node(),
        BinaryJSONValue::Array(values) => values
            .iter()
            .map(typed_value_to_node)
            .collect::<Result<Vec<_>, _>>()
            .map(JSONNode::Array),
        BinaryJSONValue::Object(values) => values
            .iter()
            .map(|(key, value)| Ok((key.clone(), typed_value_to_node(value)?)))
            .collect::<Result<Vec<_>, _>>()
            .map(JSONNode::Object),
        BinaryJSONValue::Opaque(value) => scalar(BinaryJSON::from_opaque(value.clone())),
        BinaryJSONValue::Time(value) => scalar(BinaryJSON::from_time(*value)),
        BinaryJSONValue::Duration(value) => scalar(BinaryJSON::from_duration(*value)),
    }
}

fn normalize_parsed_numbers(value: &mut Value) -> Result<(), BinaryJSONError> {
    match value {
        Value::Number(number) if number.as_u64().is_some_and(|value| value > i64::MAX as u64) => {
            let value = number.as_u64().ok_or(BinaryJSONError::InvalidText)? as f64;
            *number = Number::from_f64(value).ok_or(BinaryJSONError::InvalidText)?;
        }
        Value::Array(values) => {
            for value in values {
                normalize_parsed_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                normalize_parsed_numbers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl fmt::Display for BinaryJSON {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Ok(opaque) = self.opaque() {
            return write!(
                formatter,
                "\"base64:type{}:{}\"",
                opaque.type_code,
                encode_base64(&opaque.bytes)
            );
        }
        if let Ok(mut time) = self.as_time(6) {
            let _ = time.set_fsp(6);
            return formatter.write_str(&quote_json_string(&time.to_string()));
        }
        if let Ok(duration) = self.as_duration() {
            let duration = MySqlDuration::from_nanoseconds(duration.nanoseconds(), 6)
                .map_err(|_| fmt::Error)?;
            return formatter.write_str(&quote_json_string(&duration.to_string()));
        }
        if let Some(value) = self.as_f64() {
            return formatter.write_str(&format_float64(value).ok_or(fmt::Error)?);
        }
        if matches!(self.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT) {
            return match self.to_node() {
                Ok(node) => formatter.write_str(&format_node(&node)),
                Err(_) => Ok(()),
            };
        }
        match self.to_value() {
            Ok(value) => formatter.write_str(&format_value(&value)),
            Err(_) => Ok(()),
        }
    }
}

/// Formats a JSON double using TiDB's MySQL-compatible exponent boundary.
///
/// This is the source `marshalFloat64To` rule: fixed notation is used in
/// `[1e-15, 1e15)`, scientific notation outside it, and fixed integral doubles
/// retain `.0`.
fn format_float64(value: f64) -> Option<String> {
    if !value.is_finite() {
        return None;
    }
    let absolute = value.abs();
    if absolute != 0.0 && !(1e-15..1e15).contains(&absolute) {
        return Some(format!("{value:e}"));
    }
    let mut output = value.to_string();
    if !output.contains('.') {
        output.push_str(".0");
    }
    Some(output)
}

/// Removes surrounding quotes and MySQL JSON escape sequences.
pub fn unquote_string(text: &str) -> Result<String, BinaryJSONError> {
    if text.len() >= 2 && text.starts_with('"') && text.ends_with('"') {
        return unquote_json_string(&text[1..text.len() - 1]);
    }
    Ok(text.to_owned())
}

/// Quotes a JSON path key, leaving an unescaped ECMAScript identifier bare.
pub fn quote_json_string(text: &str) -> String {
    let quoted = serde_json::to_string(text).expect("Rust string is valid JSON text");
    if is_ecmascript_identifier(text)
        && quoted.as_bytes()[1..quoted.len() - 1] == text.as_bytes()[..]
    {
        text.to_owned()
    } else {
        quoted
    }
}

/// Decodes MySQL's JSON_UNQUOTE escape syntax.
pub fn unquote_json_string(text: &str) -> Result<String, BinaryJSONError> {
    let mut output = String::with_capacity(text.len());
    let mut chars = text.char_indices().peekable();
    while let Some((_, ch)) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let (_, escaped) = chars.next().ok_or(BinaryJSONError::InvalidText)?;
        match escaped {
            '"' => output.push('"'),
            'b' => output.push('\u{8}'),
            'f' => output.push('\u{c}'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            '\\' => output.push('\\'),
            'u' => {
                let mut first = [0_u8; 4];
                for byte in &mut first {
                    *byte = chars
                        .next()
                        .and_then(|(_, ch)| ch.is_ascii().then_some(ch as u8))
                        .ok_or(BinaryJSONError::InvalidText)?;
                }
                let first = decode_hex_u16(&first)?;
                let scalar = if (0xd800..=0xdbff).contains(&first) {
                    if chars.next().map(|(_, ch)| ch) != Some('\\')
                        || chars.next().map(|(_, ch)| ch) != Some('u')
                    {
                        return Err(BinaryJSONError::InvalidText);
                    }
                    let mut second = [0_u8; 4];
                    for byte in &mut second {
                        *byte = chars
                            .next()
                            .and_then(|(_, ch)| ch.is_ascii().then_some(ch as u8))
                            .ok_or(BinaryJSONError::InvalidText)?;
                    }
                    let second = decode_hex_u16(&second)?;
                    if !(0xdc00..=0xdfff).contains(&second) {
                        return Err(BinaryJSONError::InvalidText);
                    }
                    0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
                } else if (0xdc00..=0xdfff).contains(&first) {
                    return Err(BinaryJSONError::InvalidText);
                } else {
                    u32::from(first)
                };
                output.push(char::from_u32(scalar).ok_or(BinaryJSONError::InvalidText)?);
            }
            other => output.push(other),
        }
    }
    Ok(output)
}

/// Decodes one four- or eight-hex-digit escaped Unicode value.
pub fn decode_escaped_unicode(hex: &[u8]) -> Result<([u8; 4], usize, bool), BinaryJSONError> {
    if hex.len() != 4 && hex.len() != 8 {
        return Err(BinaryJSONError::InvalidText);
    }
    let first = decode_hex_u16(hex.get(..4).ok_or(BinaryJSONError::InvalidText)?)?;
    let scalar = if hex.len() == 8 {
        let second = decode_hex_u16(hex.get(4..).ok_or(BinaryJSONError::InvalidText)?)?;
        if !(0xd800..=0xdbff).contains(&first) || !(0xdc00..=0xdfff).contains(&second) {
            return Err(BinaryJSONError::InvalidText);
        }
        0x10000 + ((u32::from(first) - 0xd800) << 10) + (u32::from(second) - 0xdc00)
    } else if (0xd800..=0xdfff).contains(&first) {
        return Err(BinaryJSONError::LoneSurrogate);
    } else {
        u32::from(first)
    };
    let ch = char::from_u32(scalar).ok_or(BinaryJSONError::InvalidText)?;
    let mut output = [0_u8; 4];
    let size = ch.encode_utf8(&mut output).len();
    Ok((output, size, false))
}

fn decode_hex_u16(hex: &[u8]) -> Result<u16, BinaryJSONError> {
    if hex.len() != 4 {
        return Err(BinaryJSONError::InvalidText);
    }
    hex.iter().try_fold(0_u16, |value, byte| {
        let digit = (*byte as char)
            .to_digit(16)
            .ok_or(BinaryJSONError::InvalidText)?;
        Ok((value << 4) | digit as u16)
    })
}

fn is_ecmascript_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    let Some(&first) = bytes.first() else {
        return false;
    };
    let is_letter = |byte: u8| {
        byte.is_ascii_alphabetic()
            || matches!(
                byte,
                0xAA | 0xB5 | 0xBA | 0xC0..=0xD6 | 0xD8..=0xF6 | 0xF8..=0xFF
            )
    };
    (is_letter(first) || first == b'$' || first == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| is_letter(*byte) || byte.is_ascii_digit() || matches!(byte, b'$' | b'_'))
}

fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((value >> 18) & 0x3f) as usize] as char);
        output.push(TABLE[((value >> 12) & 0x3f) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((value >> 6) & 0x3f) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(value & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

/// Compares two binary JSON values using TiDB's JSON precedence and scalar rules.
pub fn compare_binary_json(left: &BinaryJSON, right: &BinaryJSON) -> Ordering {
    let left_rank = json_precedence(left);
    let right_rank = json_precedence(right);
    if left_rank != right_rank {
        return left_rank.cmp(&right_rank);
    }
    if left.type_code == JSON_TYPE_CODE_OPAQUE && right.type_code == JSON_TYPE_CODE_OPAQUE {
        return match (left.opaque(), right.opaque()) {
            (Ok(left), Ok(right)) => left.bytes.cmp(&right.bytes),
            _ => Ordering::Equal,
        };
    }
    if matches!(
        left.type_code,
        JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP
    ) && matches!(
        right.type_code,
        JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP
    ) {
        return match (left.as_time(0), right.as_time(0)) {
            (Ok(left), Ok(right)) => left.compare(right),
            _ => Ordering::Equal,
        };
    }
    if left.type_code == JSON_TYPE_CODE_DURATION && right.type_code == JSON_TYPE_CODE_DURATION {
        return match (left.as_duration(), right.as_duration()) {
            (Ok(left), Ok(right)) => left.nanoseconds().cmp(&right.nanoseconds()),
            _ => Ordering::Equal,
        };
    }
    if matches!(left.type_code, JSON_TYPE_CODE_ARRAY | JSON_TYPE_CODE_OBJECT)
        && left.type_code == right.type_code
    {
        return match (left.to_node(), right.to_node()) {
            (Ok(left), Ok(right)) => compare_container_nodes(&left, &right),
            _ => Ordering::Equal,
        };
    }
    match (left.to_value(), right.to_value()) {
        (Ok(left), Ok(right)) => compare_json_value(&left, &right),
        _ => left.value.cmp(&right.value),
    }
}

fn compare_container_nodes(left: &JSONNode, right: &JSONNode) -> Ordering {
    match (left, right) {
        (JSONNode::Array(left), JSONNode::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_nodes(left, right))
            .find(|ordering| !ordering.is_eq())
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        (JSONNode::Object(left), JSONNode::Object(right)) => {
            let count = left.len().cmp(&right.len());
            if !count.is_eq() {
                return count;
            }
            let mut left = left.iter().collect::<Vec<_>>();
            let mut right = right.iter().collect::<Vec<_>>();
            left.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            right.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            left.into_iter()
                .zip(right)
                .find_map(|((left_key, left_value), (right_key, right_value))| {
                    let key = left_key.as_bytes().cmp(right_key.as_bytes());
                    if !key.is_eq() {
                        return Some(key);
                    }
                    let value = compare_nodes(left_value, right_value);
                    (!value.is_eq()).then_some(value)
                })
                .unwrap_or(Ordering::Equal)
        }
        _ => Ordering::Equal,
    }
}

fn compare_nodes(left: &JSONNode, right: &JSONNode) -> Ordering {
    match (left, right) {
        (JSONNode::Scalar(left), JSONNode::Scalar(right)) => compare_binary_json(left, right),
        (JSONNode::Array(_), JSONNode::Array(_)) | (JSONNode::Object(_), JSONNode::Object(_)) => {
            compare_container_nodes(left, right)
        }
        _ => {
            let left = BinaryJSON::from_node(left).expect("decoded JSON node must re-encode");
            let right = BinaryJSON::from_node(right).expect("decoded JSON node must re-encode");
            json_precedence(&left).cmp(&json_precedence(&right))
        }
    }
}

fn json_precedence(value: &BinaryJSON) -> i8 {
    match value.type_code {
        JSON_TYPE_CODE_OPAQUE => match value.type_name() {
            Ok("BLOB") => 14,
            Ok("BIT") => 13,
            _ => 12,
        },
        JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => 11,
        JSON_TYPE_CODE_DURATION => 10,
        JSON_TYPE_CODE_DATE => 9,
        JSON_TYPE_CODE_LITERAL if value.value != [JSON_LITERAL_NULL] => 8,
        JSON_TYPE_CODE_ARRAY => 7,
        JSON_TYPE_CODE_OBJECT => 6,
        JSON_TYPE_CODE_STRING => 5,
        JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64 | JSON_TYPE_CODE_FLOAT64 => 4,
        JSON_TYPE_CODE_LITERAL => 3,
        _ => 0,
    }
}

fn compare_json_value(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Number(left), Value::Number(right)) => compare_json_number(left, right),
        (Value::String(left), Value::String(right)) => left.as_bytes().cmp(right.as_bytes()),
        (Value::Array(left), Value::Array(right)) => left
            .iter()
            .zip(right)
            .map(|(left, right)| compare_json_value(left, right))
            .find(|ordering| !ordering.is_eq())
            .unwrap_or_else(|| left.len().cmp(&right.len())),
        (Value::Object(left), Value::Object(right)) => {
            let count = left.len().cmp(&right.len());
            if !count.is_eq() {
                return count;
            }
            let mut left = left.iter().collect::<Vec<_>>();
            let mut right = right.iter().collect::<Vec<_>>();
            left.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            right.sort_unstable_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
            left.into_iter()
                .zip(right)
                .find_map(|((left_key, left_value), (right_key, right_value))| {
                    let key = left_key.as_bytes().cmp(right_key.as_bytes());
                    if !key.is_eq() {
                        return Some(key);
                    }
                    let value = compare_json_value(left_value, right_value);
                    (!value.is_eq()).then_some(value)
                })
                .unwrap_or(Ordering::Equal)
        }
        _ => value_precedence(left).cmp(&value_precedence(right)),
    }
}

fn value_precedence(value: &Value) -> i8 {
    match value {
        Value::Null => 3,
        Value::Number(_) => 4,
        Value::String(_) => 5,
        Value::Object(_) => 6,
        Value::Array(_) => 7,
        Value::Bool(_) => 8,
    }
}

fn compare_json_number(left: &Number, right: &Number) -> Ordering {
    match (
        left.as_i64(),
        left.as_u64(),
        left.as_f64(),
        right.as_i64(),
        right.as_u64(),
        right.as_f64(),
    ) {
        (Some(left), _, _, Some(right), _, _) => left.cmp(&right),
        (Some(left), _, _, None, Some(right), _) => {
            if left < 0 {
                Ordering::Less
            } else {
                (left as u64).cmp(&right)
            }
        }
        (None, Some(left), _, Some(right), _, _) => {
            if right < 0 {
                Ordering::Greater
            } else {
                left.cmp(&(right as u64))
            }
        }
        (None, Some(left), _, None, Some(right), _) => left.cmp(&right),
        (_, _, Some(left), _, _, Some(right)) => {
            if (left - right).abs() < 1e-8 {
                Ordering::Equal
            } else {
                left.partial_cmp(&right).unwrap_or(Ordering::Greater)
            }
        }
        _ => Ordering::Equal,
    }
}

fn encode_node(node: &JSONNode, depth: usize) -> Result<BinaryJSON, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match node {
        JSONNode::Scalar(value) => Ok(value.clone()),
        JSONNode::Array(values) => {
            let values = values
                .iter()
                .map(|value| encode_node(value, depth + 1))
                .collect::<Result<Vec<_>, _>>()?;
            encode_binary_array(&values)
        }
        JSONNode::Object(values) => {
            let values = values
                .iter()
                .map(|(key, value)| Ok((key.as_str(), encode_node(value, depth + 1)?)))
                .collect::<Result<Vec<_>, BinaryJSONError>>()?;
            encode_binary_object(&values)
        }
    }
}

fn decode_node(value: &BinaryJSON, depth: usize) -> Result<JSONNode, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match value.type_code {
        JSON_TYPE_CODE_ARRAY => {
            let (count, size) = read_header(&value.value)?;
            if size != value.value.len() || HEADER_SIZE + count * VALUE_ENTRY_SIZE > size {
                return Err(BinaryJSONError::InvalidBinary);
            }
            let mut values = Vec::with_capacity(count);
            for index in 0..count {
                let entry = HEADER_SIZE + index * VALUE_ENTRY_SIZE;
                values.push(decode_node(
                    &decode_binary_entry(
                        value.value[entry],
                        &value.value[entry + 1..entry + 5],
                        &value.value,
                    )?,
                    depth + 1,
                )?);
            }
            Ok(JSONNode::Array(values))
        }
        JSON_TYPE_CODE_OBJECT => {
            let (count, size) = read_header(&value.value)?;
            let value_entries = HEADER_SIZE + count * KEY_ENTRY_SIZE;
            if size != value.value.len() || value_entries + count * VALUE_ENTRY_SIZE > size {
                return Err(BinaryJSONError::InvalidBinary);
            }
            let mut values = Vec::with_capacity(count);
            for index in 0..count {
                let key_entry = HEADER_SIZE + index * KEY_ENTRY_SIZE;
                let key_offset =
                    u32::from_le_bytes(value.value[key_entry..key_entry + 4].try_into().unwrap())
                        as usize;
                let key_length = u16::from_le_bytes(
                    value.value[key_entry + 4..key_entry + 6]
                        .try_into()
                        .unwrap(),
                ) as usize;
                let key = std::str::from_utf8(
                    value
                        .value
                        .get(key_offset..key_offset + key_length)
                        .ok_or(BinaryJSONError::InvalidBinary)?,
                )
                .map_err(|_| BinaryJSONError::InvalidBinary)?
                .to_owned();
                let entry = value_entries + index * VALUE_ENTRY_SIZE;
                let child = decode_binary_entry(
                    value.value[entry],
                    &value.value[entry + 1..entry + 5],
                    &value.value,
                )?;
                values.push((key, decode_node(&child, depth + 1)?));
            }
            Ok(JSONNode::Object(values))
        }
        _ => {
            validate_scalar(value)?;
            Ok(JSONNode::Scalar(value.clone()))
        }
    }
}

fn validate_scalar(value: &BinaryJSON) -> Result<(), BinaryJSONError> {
    match value.type_code {
        JSON_TYPE_CODE_OPAQUE => {
            value.opaque()?;
            Ok(())
        }
        JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP
            if value.value.len() == 8 =>
        {
            Ok(())
        }
        JSON_TYPE_CODE_DURATION if value.value.len() == 12 => Ok(()),
        JSON_TYPE_CODE_OBJECT | JSON_TYPE_CODE_ARRAY => Err(BinaryJSONError::InvalidBinary),
        _ => value.to_value().map(|_| ()),
    }
}

fn decode_binary_entry(
    type_code: u8,
    entry: &[u8],
    container: &[u8],
) -> Result<BinaryJSON, BinaryJSONError> {
    if type_code == JSON_TYPE_CODE_LITERAL {
        return BinaryJSON::from_raw(type_code, vec![entry[0]]);
    }
    let offset = u32::from_le_bytes(
        entry
            .try_into()
            .map_err(|_| BinaryJSONError::InvalidBinary)?,
    ) as usize;
    let value = container
        .get(offset..)
        .ok_or(BinaryJSONError::InvalidBinary)?;
    let length = value_length(type_code, value)?;
    BinaryJSON::from_raw(
        type_code,
        value
            .get(..length)
            .ok_or(BinaryJSONError::InvalidBinary)?
            .to_vec(),
    )
}

fn encode_binary_array(values: &[BinaryJSON]) -> Result<BinaryJSON, BinaryJSONError> {
    let data_start = HEADER_SIZE + values.len() * VALUE_ENTRY_SIZE;
    let mut output = vec![0; data_start];
    let mut payload = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let entry = HEADER_SIZE + index * VALUE_ENTRY_SIZE;
        output[entry] = value.type_code;
        if value.type_code == JSON_TYPE_CODE_LITERAL {
            output[entry + 1] = *value.value.first().ok_or(BinaryJSONError::InvalidBinary)?;
        } else {
            let offset = u32::try_from(data_start + payload.len())
                .map_err(|_| BinaryJSONError::InvalidBinary)?;
            output[entry + 1..entry + 5].copy_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(&value.value);
        }
    }
    output.extend_from_slice(&payload);
    write_header(&mut output, values.len())?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_ARRAY,
        value: output,
    })
}

fn encode_binary_object(values: &[(&str, BinaryJSON)]) -> Result<BinaryJSON, BinaryJSONError> {
    let mut values = values.to_vec();
    values.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    let key_entry_start = HEADER_SIZE;
    let value_entry_start = key_entry_start + values.len() * KEY_ENTRY_SIZE;
    let key_data_start = value_entry_start + values.len() * VALUE_ENTRY_SIZE;
    let key_bytes = values.iter().try_fold(0_usize, |total, (key, _)| {
        if key.len() > u16::MAX as usize {
            Err(BinaryJSONError::KeyTooLong)
        } else {
            total
                .checked_add(key.len())
                .ok_or(BinaryJSONError::InvalidBinary)
        }
    })?;
    let value_data_start = key_data_start + key_bytes;
    let mut output = vec![0; key_data_start];
    let mut keys = Vec::with_capacity(key_bytes);
    let mut payload = Vec::new();
    for (index, (key, value)) in values.iter().enumerate() {
        let key_entry = key_entry_start + index * KEY_ENTRY_SIZE;
        let key_offset = u32::try_from(key_data_start + keys.len())
            .map_err(|_| BinaryJSONError::InvalidBinary)?;
        output[key_entry..key_entry + 4].copy_from_slice(&key_offset.to_le_bytes());
        output[key_entry + 4..key_entry + 6].copy_from_slice(&(key.len() as u16).to_le_bytes());
        keys.extend_from_slice(key.as_bytes());

        let value_entry = value_entry_start + index * VALUE_ENTRY_SIZE;
        output[value_entry] = value.type_code;
        if value.type_code == JSON_TYPE_CODE_LITERAL {
            output[value_entry + 1] = *value.value.first().ok_or(BinaryJSONError::InvalidBinary)?;
        } else {
            let offset = u32::try_from(value_data_start + payload.len())
                .map_err(|_| BinaryJSONError::InvalidBinary)?;
            output[value_entry + 1..value_entry + 5].copy_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(&value.value);
        }
    }
    output.extend_from_slice(&keys);
    output.extend_from_slice(&payload);
    write_header(&mut output, values.len())?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_OBJECT,
        value: output,
    })
}

fn encode_value(value: &Value, depth: usize) -> Result<BinaryJSON, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match value {
        Value::Null => Ok(literal(JSON_LITERAL_NULL)),
        Value::Bool(true) => Ok(literal(JSON_LITERAL_TRUE)),
        Value::Bool(false) => Ok(literal(JSON_LITERAL_FALSE)),
        Value::Number(number) => encode_number(number),
        Value::String(text) => {
            let mut bytes = Vec::new();
            encode_uvarint(text.len(), &mut bytes);
            bytes.extend_from_slice(text.as_bytes());
            Ok(BinaryJSON {
                type_code: JSON_TYPE_CODE_STRING,
                value: bytes,
            })
        }
        Value::Array(values) => encode_array(values, depth + 1),
        Value::Object(values) => encode_object(values, depth + 1),
    }
}

fn literal(value: u8) -> BinaryJSON {
    BinaryJSON {
        type_code: JSON_TYPE_CODE_LITERAL,
        value: vec![value],
    }
}

fn encode_number(number: &Number) -> Result<BinaryJSON, BinaryJSONError> {
    if let Some(value) = number.as_i64() {
        Ok(BinaryJSON {
            type_code: JSON_TYPE_CODE_INT64,
            value: value.to_le_bytes().to_vec(),
        })
    } else if let Some(value) = number.as_u64() {
        Ok(BinaryJSON {
            type_code: JSON_TYPE_CODE_UINT64,
            value: value.to_le_bytes().to_vec(),
        })
    } else {
        let value = number.as_f64().ok_or(BinaryJSONError::InvalidText)?;
        Ok(BinaryJSON {
            type_code: JSON_TYPE_CODE_FLOAT64,
            value: value.to_bits().to_le_bytes().to_vec(),
        })
    }
}

fn encode_array(values: &[Value], depth: usize) -> Result<BinaryJSON, BinaryJSONError> {
    let entry_start = HEADER_SIZE;
    let data_start = entry_start + values.len() * VALUE_ENTRY_SIZE;
    let mut output = vec![0; data_start];
    let mut payload = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let encoded = encode_value(value, depth)?;
        let entry = entry_start + index * VALUE_ENTRY_SIZE;
        output[entry] = encoded.type_code;
        if encoded.type_code == JSON_TYPE_CODE_LITERAL {
            output[entry + 1] = encoded.value[0];
        } else {
            let offset = data_start
                .checked_add(payload.len())
                .and_then(|offset| u32::try_from(offset).ok())
                .ok_or(BinaryJSONError::InvalidBinary)?;
            output[entry + 1..entry + 5].copy_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(&encoded.value);
        }
    }
    output.extend_from_slice(&payload);
    write_header(&mut output, values.len())?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_ARRAY,
        value: output,
    })
}

fn encode_object(values: &Map<String, Value>, depth: usize) -> Result<BinaryJSON, BinaryJSONError> {
    let mut entries: Vec<_> = values.iter().collect();
    entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    let key_entry_start = HEADER_SIZE;
    let value_entry_start = key_entry_start + entries.len() * KEY_ENTRY_SIZE;
    let key_data_start = value_entry_start + entries.len() * VALUE_ENTRY_SIZE;
    let key_bytes = entries.iter().try_fold(0_usize, |total, (key, _)| {
        if key.len() > u16::MAX as usize {
            Err(BinaryJSONError::KeyTooLong)
        } else {
            total
                .checked_add(key.len())
                .ok_or(BinaryJSONError::InvalidBinary)
        }
    })?;
    let value_data_start = key_data_start + key_bytes;
    let mut output = vec![0; key_data_start];
    let mut keys = Vec::with_capacity(key_bytes);
    let mut payload = Vec::new();

    for (index, (key, value)) in entries.into_iter().enumerate() {
        let key_entry = key_entry_start + index * KEY_ENTRY_SIZE;
        let key_offset = u32::try_from(key_data_start + keys.len())
            .map_err(|_| BinaryJSONError::InvalidBinary)?;
        output[key_entry..key_entry + 4].copy_from_slice(&key_offset.to_le_bytes());
        output[key_entry + 4..key_entry + 6].copy_from_slice(&(key.len() as u16).to_le_bytes());
        keys.extend_from_slice(key.as_bytes());

        let encoded = encode_value(value, depth)?;
        let value_entry = value_entry_start + index * VALUE_ENTRY_SIZE;
        output[value_entry] = encoded.type_code;
        if encoded.type_code == JSON_TYPE_CODE_LITERAL {
            output[value_entry + 1] = encoded.value[0];
        } else {
            let offset = u32::try_from(value_data_start + payload.len())
                .map_err(|_| BinaryJSONError::InvalidBinary)?;
            output[value_entry + 1..value_entry + 5].copy_from_slice(&offset.to_le_bytes());
            payload.extend_from_slice(&encoded.value);
        }
    }
    output.extend_from_slice(&keys);
    output.extend_from_slice(&payload);
    write_header(&mut output, values.len())?;
    Ok(BinaryJSON {
        type_code: JSON_TYPE_CODE_OBJECT,
        value: output,
    })
}

fn write_header(output: &mut [u8], count: usize) -> Result<(), BinaryJSONError> {
    let count = u32::try_from(count).map_err(|_| BinaryJSONError::InvalidBinary)?;
    let size = u32::try_from(output.len()).map_err(|_| BinaryJSONError::InvalidBinary)?;
    output[..4].copy_from_slice(&count.to_le_bytes());
    output[4..8].copy_from_slice(&size.to_le_bytes());
    Ok(())
}

fn decode_value(type_code: u8, bytes: &[u8], depth: usize) -> Result<Value, BinaryJSONError> {
    if depth > MAX_JSON_DEPTH {
        return Err(BinaryJSONError::TooDeep);
    }
    match type_code {
        JSON_TYPE_CODE_LITERAL if bytes == [JSON_LITERAL_NULL] => Ok(Value::Null),
        JSON_TYPE_CODE_LITERAL if bytes == [JSON_LITERAL_TRUE] => Ok(Value::Bool(true)),
        JSON_TYPE_CODE_LITERAL if bytes == [JSON_LITERAL_FALSE] => Ok(Value::Bool(false)),
        JSON_TYPE_CODE_INT64 if bytes.len() == 8 => Ok(Value::Number(
            i64::from_le_bytes(bytes.try_into().expect("length checked")).into(),
        )),
        JSON_TYPE_CODE_UINT64 if bytes.len() == 8 => Ok(Value::Number(
            u64::from_le_bytes(bytes.try_into().expect("length checked")).into(),
        )),
        JSON_TYPE_CODE_FLOAT64 if bytes.len() == 8 => {
            let value = f64::from_bits(u64::from_le_bytes(
                bytes.try_into().expect("length checked"),
            ));
            Number::from_f64(value)
                .map(Value::Number)
                .ok_or(BinaryJSONError::InvalidBinary)
        }
        JSON_TYPE_CODE_STRING => {
            let (length, prefix) = decode_uvarint(bytes)?;
            let text = std::str::from_utf8(
                bytes
                    .get(prefix..prefix + length)
                    .ok_or(BinaryJSONError::InvalidBinary)?,
            )
            .map_err(|_| BinaryJSONError::InvalidBinary)?;
            if prefix + length != bytes.len() {
                return Err(BinaryJSONError::InvalidBinary);
            }
            Ok(Value::String(text.to_owned()))
        }
        JSON_TYPE_CODE_ARRAY => decode_array(bytes, depth + 1),
        JSON_TYPE_CODE_OBJECT => decode_object(bytes, depth + 1),
        _ => Err(BinaryJSONError::InvalidBinary),
    }
}

fn decode_array(bytes: &[u8], depth: usize) -> Result<Value, BinaryJSONError> {
    let (count, size) = read_header(bytes)?;
    if size != bytes.len() || HEADER_SIZE + count * VALUE_ENTRY_SIZE > bytes.len() {
        return Err(BinaryJSONError::InvalidBinary);
    }
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let entry = HEADER_SIZE + index * VALUE_ENTRY_SIZE;
        values.push(decode_entry(
            bytes[entry],
            &bytes[entry + 1..entry + 5],
            bytes,
            depth,
        )?);
    }
    Ok(Value::Array(values))
}

fn decode_object(bytes: &[u8], depth: usize) -> Result<Value, BinaryJSONError> {
    let (count, size) = read_header(bytes)?;
    let value_entries = HEADER_SIZE + count * KEY_ENTRY_SIZE;
    if size != bytes.len() || value_entries + count * VALUE_ENTRY_SIZE > bytes.len() {
        return Err(BinaryJSONError::InvalidBinary);
    }
    let mut values = Map::new();
    for index in 0..count {
        let key_entry = HEADER_SIZE + index * KEY_ENTRY_SIZE;
        let key_offset =
            u32::from_le_bytes(bytes[key_entry..key_entry + 4].try_into().unwrap()) as usize;
        let key_length =
            u16::from_le_bytes(bytes[key_entry + 4..key_entry + 6].try_into().unwrap()) as usize;
        let key = std::str::from_utf8(
            bytes
                .get(key_offset..key_offset + key_length)
                .ok_or(BinaryJSONError::InvalidBinary)?,
        )
        .map_err(|_| BinaryJSONError::InvalidBinary)?;
        let entry = value_entries + index * VALUE_ENTRY_SIZE;
        values.insert(
            key.to_owned(),
            decode_entry(bytes[entry], &bytes[entry + 1..entry + 5], bytes, depth)?,
        );
    }
    Ok(Value::Object(values))
}

fn decode_entry(
    type_code: u8,
    entry: &[u8],
    container: &[u8],
    depth: usize,
) -> Result<Value, BinaryJSONError> {
    if type_code == JSON_TYPE_CODE_LITERAL {
        return decode_value(type_code, &entry[..1], depth);
    }
    let offset = u32::from_le_bytes(entry.try_into().unwrap()) as usize;
    let value = container
        .get(offset..)
        .ok_or(BinaryJSONError::InvalidBinary)?;
    let length = value_length(type_code, value)?;
    decode_value(type_code, &value[..length], depth)
}

fn value_length(type_code: u8, bytes: &[u8]) -> Result<usize, BinaryJSONError> {
    match type_code {
        JSON_TYPE_CODE_OBJECT | JSON_TYPE_CODE_ARRAY => {
            let (_, size) = read_header(bytes)?;
            Ok(size)
        }
        JSON_TYPE_CODE_INT64 | JSON_TYPE_CODE_UINT64 | JSON_TYPE_CODE_FLOAT64 => Ok(8),
        JSON_TYPE_CODE_DATE | JSON_TYPE_CODE_DATETIME | JSON_TYPE_CODE_TIMESTAMP => Ok(8),
        JSON_TYPE_CODE_DURATION => Ok(12),
        JSON_TYPE_CODE_OPAQUE => {
            let payload = bytes.get(1..).ok_or(BinaryJSONError::InvalidBinary)?;
            let (length, prefix) = decode_uvarint(payload)?;
            Ok(1 + prefix + length)
        }
        JSON_TYPE_CODE_STRING => {
            let (length, prefix) = decode_uvarint(bytes)?;
            Ok(prefix + length)
        }
        _ => Err(BinaryJSONError::InvalidBinary),
    }
}

fn read_header(bytes: &[u8]) -> Result<(usize, usize), BinaryJSONError> {
    let header = bytes
        .get(..HEADER_SIZE)
        .ok_or(BinaryJSONError::InvalidBinary)?;
    let count = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
    let size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    Ok((count, size))
}

fn encode_uvarint(mut value: usize, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn decode_uvarint(bytes: &[u8]) -> Result<(usize, usize), BinaryJSONError> {
    let mut value = 0_usize;
    for (index, byte) in bytes.iter().copied().enumerate().take(10) {
        value |= usize::from(byte & 0x7f) << (index * 7);
        if byte < 0x80 {
            return Ok((value, index + 1));
        }
    }
    Err(BinaryJSONError::InvalidBinary)
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => serde_json::to_string(value).expect("valid Rust string"),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(format_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(key, value)| format!(
                        "{}: {}",
                        serde_json::to_string(key).expect("valid Rust string"),
                        format_value(value)
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}

fn format_node(value: &JSONNode) -> String {
    match value {
        JSONNode::Scalar(value) => value.to_string(),
        JSONNode::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(format_node)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        JSONNode::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!(
                    "{}: {}",
                    serde_json::to_string(key).expect("valid Rust string"),
                    format_node(value)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_json_marshal_unmarshal() {
        for text in [
            "null",
            "true",
            "false",
            "-1",
            "18446744073709551615",
            "1.5",
            "\"hello\"",
            "[1, true, \"x\", {\"b\": 2, \"a\": null}]",
            "{\"b\": [2, 3], \"a\": 1}",
        ] {
            let binary = BinaryJSON::parse(text).unwrap();
            let decoded = binary.to_value().unwrap();
            let rebuilt = BinaryJSON::from_value(&decoded).unwrap();
            assert_eq!(binary, rebuilt, "{text}");
        }
        assert_eq!(
            BinaryJSON::parse("").unwrap_err(),
            BinaryJSONError::EmptyDocument
        );
        assert_eq!(
            BinaryJSON::parse(r#""a"""#).unwrap_err(),
            BinaryJSONError::TrailingValues
        );
    }

    #[test]
    fn test_binary_json_string_and_types() {
        assert_eq!(
            BinaryJSON::parse("{\"b\":2,\"a\":[1,true]}")
                .unwrap()
                .to_string(),
            "{\"a\": [1, true], \"b\": 2}"
        );
        assert_eq!(BinaryJSON::parse("-2").unwrap().as_i64(), Some(-2));
        assert_eq!(
            BinaryJSON::from_value(&Value::Number(Number::from(u64::MAX)))
                .unwrap()
                .as_u64(),
            Some(u64::MAX)
        );
        assert_eq!(
            BinaryJSON::parse("18446744073709551615")
                .unwrap()
                .type_name()
                .unwrap(),
            "DOUBLE"
        );
        assert_eq!(
            BinaryJSON::parse("\"TiDB\"").unwrap().as_string(),
            Some(&b"TiDB"[..])
        );

        for (value, expected) in [
            (123_456_789.123_456_7, "123456789.1234567"),
            (0.000_000_01, "0.00000001"),
            (1e-20, "1e-20"),
            (1e15, "1e15"),
            (1e14, "100000000000000.0"),
            (9.0, "9.0"),
            (-0.0, "-0.0"),
        ] {
            let value =
                BinaryJSON::from_value(&Value::Number(Number::from_f64(value).unwrap())).unwrap();
            assert_eq!(value.to_string(), expected);
            let array =
                BinaryJSON::from_value(&Value::Array(vec![value.to_value().unwrap()])).unwrap();
            assert_eq!(array.to_string(), format!("[{expected}]"));
        }

        for (input, expected_type) in [
            (BinaryJSONValue::Int64(1_i64 << 62), JSON_TYPE_CODE_INT64),
            (
                BinaryJSONValue::Float64(123_456_789.123_456_7),
                JSON_TYPE_CODE_FLOAT64,
            ),
            (
                BinaryJSONValue::Float64(0.000_000_01),
                JSON_TYPE_CODE_FLOAT64,
            ),
            (
                BinaryJSONValue::Number(u64::MAX.to_string()),
                JSON_TYPE_CODE_FLOAT64,
            ),
        ] {
            let value = BinaryJSON::from_typed_value(&input).unwrap();
            assert_eq!(value.type_code(), expected_type);
            assert_eq!(
                BinaryJSON::calculate_typed_size(&input).unwrap(),
                value.value().len()
            );
        }

        let opaque = Opaque {
            type_code: 233,
            bytes: vec![1, 2, 3],
        };
        let input = BinaryJSONValue::Object(BTreeMap::from([(
            "values".to_owned(),
            BinaryJSONValue::Array(vec![
                BinaryJSONValue::Bool(true),
                BinaryJSONValue::Opaque(opaque.clone()),
            ]),
        )]));
        let value = BinaryJSON::from_typed_value(&input).unwrap();
        assert_eq!(
            value.to_node().unwrap(),
            JSONNode::Object(vec![(
                "values".to_owned(),
                JSONNode::Array(vec![
                    JSONNode::Scalar(literal(JSON_LITERAL_TRUE)),
                    JSONNode::Scalar(BinaryJSON::from_opaque(opaque)),
                ]),
            )])
        );
    }

    #[test]
    fn test_decode_escaped_unicode() {
        for (input, expected, size, surrogate, valid) in [
            ("597d", "好\0", 3, false, true),
            ("fffd", "�\0", 3, false, true),
            ("D83DDE0A", "😊", 4, false, true),
            ("D83D", "", 0, true, false),
            ("D83D11", "", 0, false, false),
            ("ZZZZ", "", 0, false, false),
            ("D83DDE0A597d", "", 0, false, false),
        ] {
            let result = decode_escaped_unicode(input.as_bytes());
            assert_eq!(result.is_ok(), valid, "{input}");
            assert_eq!(
                matches!(&result, Err(BinaryJSONError::LoneSurrogate)),
                surrogate,
                "{input}"
            );
            if let Ok((bytes, actual_size, _)) = result {
                assert_eq!(std::str::from_utf8(&bytes).unwrap(), expected, "{input}");
                assert_eq!(actual_size, size, "{input}");
            }
        }
    }

    #[test]
    fn test_unquote_json_string() {
        for (input, expected, valid) in [
            ("\\b", "\u{8}", true),
            ("\\f", "\u{c}", true),
            ("\\n", "\n", true),
            ("\\r", "\r", true),
            ("\\t", "\t", true),
            ("\\\\", "\\", true),
            ("\\u597d", "好", true),
            ("0\\u597d0", "0好0", true),
            ("\\a", "a", true),
            ("[", "[", true),
            ("\\ud83e\\udd21", "🤡", true),
            ("\\ufffd", "�", true),
            ("\\", "", false),
            ("\\u59", "", false),
        ] {
            let result = unquote_json_string(input);
            assert_eq!(result.is_ok(), valid, "{input}");
            if let Ok(result) = result {
                assert_eq!(result, expected, "{input}");
            }
        }

        for (raw, quoted) in [
            ("3", "\"3\""),
            (
                "hello, \"escaped quotes\" world",
                r#""hello, \"escaped quotes\" world""#,
            ),
            ("你", "\"你\""),
            ("true", "true"),
            ("null", "null"),
            ("\"", r#""\"""#),
            ("'", "\"'\""),
            ("", "\"\""),
            ("\\ \" \u{8} \u{c} \n \r \t", r#""\\ \" \b \f \n \r \t""#),
        ] {
            assert_eq!(quote_json_string(raw), quoted, "{raw:?}");
        }
    }

    #[test]
    fn test_binary_compare_and_opaque() {
        assert_eq!(
            compare_binary_json(
                &BinaryJSON::from_value(&Value::String("a".to_owned())).unwrap(),
                &BinaryJSON::from_value(&Value::String("b".to_owned())).unwrap()
            ),
            Ordering::Less
        );

        let long = BinaryJSON::from_opaque(Opaque {
            type_code: 0,
            bytes: vec![0, 1, 2, 3],
        });
        let short = BinaryJSON::from_opaque(Opaque {
            type_code: 0,
            bytes: vec![0, 1, 2],
        });
        assert_eq!(compare_binary_json(&long, &short), Ordering::Greater);
        assert_eq!(long.type_name().unwrap(), "OPAQUE");
        assert_eq!(long.opaque().unwrap().bytes, vec![0, 1, 2, 3]);

        let display = BinaryJSON::from_opaque(Opaque {
            type_code: 233,
            bytes: vec![b'9'],
        });
        assert_eq!(display.to_string(), "\"base64:type233:OQ==\"");
        let long_display = BinaryJSON::from_opaque(Opaque {
            type_code: 233,
            bytes: vec![0; 128],
        });
        assert_eq!(long_display.opaque().unwrap().bytes, vec![0; 128]);
        assert_eq!(
            long_display.to_string(),
            "\"base64:type233:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\""
        );
        assert_eq!(
            compare_binary_json(
                &BinaryJSON::from_opaque(Opaque {
                    type_code: 0,
                    bytes: vec![0, 1, 2, 3],
                }),
                &BinaryJSON::from_opaque(Opaque {
                    type_code: 0,
                    bytes: vec![0, 2, 1],
                }),
            ),
            Ordering::Less
        );
        assert_eq!(
            compare_binary_json(
                &BinaryJSON::from_typed_value(&BinaryJSONValue::String("test".to_owned())).unwrap(),
                &BinaryJSON::from_opaque(Opaque {
                    type_code: 0,
                    bytes: vec![0, 2, 1],
                }),
            ),
            Ordering::Less
        );

        let int = |value| BinaryJSON::from_value(&Value::Number(Number::from(value))).unwrap();
        let uint = |value| BinaryJSON::from_value(&Value::Number(Number::from(value))).unwrap();
        let real = |value| {
            BinaryJSON::from_value(&Value::Number(Number::from_f64(value).unwrap())).unwrap()
        };
        for (left, right, expected) in [
            (int(-1), uint(u64::MAX), Ordering::Less),
            (
                int(922_337_203_685_477_580),
                int(922_337_203_685_477_580),
                Ordering::Equal,
            ),
            (
                int(922_337_203_685_477_580),
                int(922_337_203_685_477_581),
                Ordering::Less,
            ),
            (
                int(922_337_203_685_477_581),
                int(922_337_203_685_477_580),
                Ordering::Greater,
            ),
            (
                int(922_337_203_685_477_580),
                uint(922_337_203_685_477_581),
                Ordering::Less,
            ),
            (int(2), uint(1), Ordering::Greater),
            (int(i64::MAX), uint(i64::MAX as u64), Ordering::Equal),
            (uint(u64::MAX), int(-1), Ordering::Greater),
            (
                uint(922_337_203_685_477_581),
                int(922_337_203_685_477_580),
                Ordering::Greater,
            ),
            (uint(1), int(2), Ordering::Less),
            (uint(i64::MAX as u64), int(i64::MAX), Ordering::Equal),
            (real(9.0), int(9), Ordering::Equal),
            (real(8.9), int(9), Ordering::Less),
            (real(9.1), int(9), Ordering::Greater),
            (real(9.0), uint(9), Ordering::Equal),
            (real(8.9), uint(9), Ordering::Less),
            (real(9.1), uint(9), Ordering::Greater),
            (int(9), real(9.0), Ordering::Equal),
            (int(9), real(8.9), Ordering::Greater),
            (int(9), real(9.1), Ordering::Less),
            (uint(9), real(9.0), Ordering::Equal),
            (uint(9), real(8.9), Ordering::Greater),
            (uint(9), real(9.1), Ordering::Less),
        ] {
            assert_eq!(compare_binary_json(&left, &right), expected);
        }

        for (left, right) in [
            ("null", "3"),
            ("3", r#""hello""#),
            (r#""hello""#, r#""hello, world""#),
            (r#""hello, world""#, r#"{"a":"b"}"#),
            (r#"{"a":"b"}"#, r#"["a","b"]"#),
            (r#"["a","b"]"#, r#"["a","c"]"#),
            (r#"["a","c"]"#, "false"),
            ("false", "true"),
        ] {
            assert_eq!(
                compare_binary_json(
                    &BinaryJSON::parse(left).unwrap(),
                    &BinaryJSON::parse(right).unwrap()
                ),
                Ordering::Less,
                "{left} < {right}"
            );
        }

        let time =
            Time::from_date_checked(2020, 2, 3, 4, 5, 6, 123_456, TimeType::DateTime, 3).unwrap();
        let encoded_time = BinaryJSON::from_time(time);
        assert_eq!(encoded_time.as_time(3).unwrap(), time);
        assert_eq!(encoded_time.to_string(), r#""2020-02-03 04:05:06.123456""#);

        let duration = MySqlDuration::new(12, 34, 56, 123_456, 3).unwrap();
        let encoded_duration = BinaryJSON::from_duration(duration);
        assert_eq!(encoded_duration.as_duration().unwrap(), duration);
        assert_eq!(encoded_duration.to_string(), r#""12:34:56.123456""#);

        let container = BinaryJSON::from_node(&JSONNode::Array(vec![
            JSONNode::Scalar(encoded_time),
            JSONNode::Scalar(encoded_duration),
            JSONNode::Scalar(display),
        ]))
        .unwrap();
        assert_eq!(
            container.to_string(),
            r#"["2020-02-03 04:05:06.123456", "12:34:56.123456", "base64:type233:OQ=="]"#
        );
        assert_eq!(
            BinaryJSON::from_raw(container.type_code(), container.value().to_vec()).unwrap(),
            container
        );
    }
}
