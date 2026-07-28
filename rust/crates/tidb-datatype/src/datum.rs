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
use std::fmt;

use crate::{
    compare_binary_json, json_to_decimal, json_to_float, json_to_int64, parse_datetime,
    parse_duration, str_to_float, str_to_int, BinaryJSON, BinaryJSONError, BinaryJSONValue,
    BinaryLiteral, Charset, Collation, ConversionFlags, Converted, Decimal, Encoding,
    EncodingResult, EncodingType, MySqlDuration, MysqlEnum, MysqlSet, ScalarConversionEvent, Time,
    TransformOp, VectorFloat32, DEFAULT_STATEMENT_FLAGS,
};

/// Go TiDB's complete datum-kind domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatumKind {
    /// SQL NULL.
    Null,
    /// The range sentinel below every non-NULL datum (`KindMinNotNull`).
    MinNotNull,
    /// The range sentinel above every other datum (`KindMaxValue`).
    MaxValue,
    /// A signed 64-bit integer.
    Int,
    /// An unsigned 64-bit integer.
    UInt,
    /// An exact fixed-point decimal.
    Decimal,
    /// A double-precision binary floating-point number.
    Real,
    /// A source `KindFloat32` value, stored through a float64 bit payload in Go.
    Float32,
    /// Raw string bytes with a registered collation.
    String,
    /// Raw bytes with binary semantics.
    Bytes,
    /// A bit/hex literal.
    BinaryLiteral,
    /// A MySQL `TIME` duration.
    Duration,
    /// A MySQL ENUM plus collation metadata.
    Enum,
    /// A MySQL BIT column value.
    Bit,
    /// A MySQL SET plus collation metadata.
    Set,
    /// A MySQL date/datetime/timestamp.
    Time,
    /// A MySQL binary JSON value.
    Json,
    /// Raw internal bytes.
    Raw,
    /// A TiDB float32 vector.
    VectorFloat32,
}

/// A byte-preserving SQL string and its registered collation.
///
/// Go strings can contain arbitrary bytes. Consequently the payload is not a
/// Rust `String` and no UTF-8 validity is assumed here. Decoding belongs to a
/// charset-aware expression operation with an explicit error policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StringDatum {
    bytes: Vec<u8>,
    collation: Collation,
}

/// Failure to render a datum through Go `Datum.ToString` semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatumStringError {
    /// A byte-preserving string payload is not valid UTF-8.
    InvalidUtf8(std::str::Utf8Error),
    /// Range sentinels are bounds, not SQL scalar strings. Go stores no
    /// interface payload for either sentinel, so both produce the same exact
    /// `Datum.ToString` error.
    RangeSentinel(DatumKind),
}

impl fmt::Display for DatumStringError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUtf8(error) => error.fmt(formatter),
            Self::RangeSentinel(_) => {
                formatter.write_str("cannot convert <nil>(type <nil>) to string")
            }
        }
    }
}

impl std::error::Error for DatumStringError {}

/// Failure to convert a datum to another scalar domain.
#[derive(Debug, Clone, PartialEq)]
pub enum DatumValueError {
    /// The datum kind has no source conversion for the requested target.
    Unsupported(DatumKind, &'static str),
    /// A byte-backed string is not UTF-8.
    InvalidUtf8(std::str::Utf8Error),
    /// Binary JSON construction failed.
    Json(BinaryJSONError),
    /// A source comparison conversion failed.
    Comparison(String),
}

impl fmt::Display for DatumValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported(kind, target) => {
                write!(formatter, "cannot convert {kind:?} to {target}")
            }
            Self::InvalidUtf8(error) => error.fmt(formatter),
            Self::Json(error) => error.fmt(formatter),
            Self::Comparison(error) => formatter.write_str(error),
        }
    }
}

impl std::error::Error for DatumValueError {}

impl From<std::str::Utf8Error> for DatumValueError {
    fn from(error: std::str::Utf8Error) -> Self {
        Self::InvalidUtf8(error)
    }
}

impl From<BinaryJSONError> for DatumValueError {
    fn from(error: BinaryJSONError) -> Self {
        Self::Json(error)
    }
}

impl StringDatum {
    /// Creates a string payload from raw bytes and a registered collation.
    pub fn new(bytes: impl Into<Vec<u8>>, collation: Collation) -> Self {
        Self {
            bytes: bytes.into(),
            collation,
        }
    }

    /// Returns the unchanged stored byte sequence.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Decodes the payload as UTF-8 without replacement.
    ///
    /// The datum remains byte-preserving when this fails. Callers that need
    /// character semantics must choose how the resulting error or warning is
    /// exposed; this representation layer never silently changes octets.
    pub fn as_utf8(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.bytes)
    }

    /// Consumes the string payload and returns its unchanged bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    /// Returns the string's registered collation.
    pub const fn collation(&self) -> Collation {
        self.collation
    }

    /// Derives the string's character set from the collation registry.
    pub fn charset(&self) -> Charset {
        self.collation.charset()
    }
}

/// A byte-preserving `pkg/types/datum.go::Datum` value.
///
/// Unlike Go's tagged storage box, the enum makes kind/payload disagreement
/// impossible.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum Datum {
    /// SQL NULL and the zero/default datum state.
    #[default]
    Null,
    /// The range sentinel below every non-NULL datum (`KindMinNotNull`).
    MinNotNull,
    /// The range sentinel above every other datum (`KindMaxValue`).
    MaxValue,
    /// A signed 64-bit integer (`KindInt64`).
    Int(i64),
    /// An unsigned 64-bit integer (`KindUint64`).
    UInt(u64),
    /// An exact fixed-point decimal (`KindMysqlDecimal`).
    Decimal(Decimal),
    /// A double-precision floating-point number (`KindFloat64`).
    Real(f64),
    /// A single-precision value retaining Go's distinct `KindFloat32`.
    Float32(f64),
    /// Byte-preserving string data (`KindString`).
    String(StringDatum),
    /// Arbitrary octets with binary semantics (`KindBytes`).
    Bytes(Vec<u8>),
    /// Bit/hex literal (`KindBinaryLiteral`).
    BinaryLiteral(BinaryLiteral),
    /// MySQL duration (`KindMysqlDuration`).
    Duration(MySqlDuration),
    /// MySQL ENUM and its collation.
    Enum(MysqlEnum, Collation),
    /// MySQL BIT column value (`KindMysqlBit`).
    Bit(BinaryLiteral),
    /// MySQL SET and its collation.
    Set(MysqlSet, Collation),
    /// MySQL date/datetime/timestamp (`KindMysqlTime`).
    Time(Time),
    /// MySQL binary JSON (`KindMysqlJSON`).
    Json(BinaryJSON),
    /// Internal raw bytes (`KindRaw`).
    Raw(Vec<u8>),
    /// TiDB vector (`KindVectorFloat32`).
    VectorFloat32(VectorFloat32),
}

impl Datum {
    /// Equivalent to Go `MinNotNullDatum`.
    pub const fn min_not_null() -> Self {
        Self::MinNotNull
    }

    /// Equivalent to Go `MaxValueDatum`.
    pub const fn max_value() -> Self {
        Self::MaxValue
    }

    /// Creates a signed integer datum.
    pub fn new_int(value: i64) -> Self {
        Self::Int(value)
    }

    /// Creates an unsigned integer datum without narrowing through `i64`.
    pub fn new_uint(value: u64) -> Self {
        Self::UInt(value)
    }

    /// Creates an exact decimal datum.
    pub fn new_decimal(value: Decimal) -> Self {
        Self::Decimal(value)
    }

    /// Creates a double-precision real datum.
    pub fn new_real(value: f64) -> Self {
        Self::Real(value)
    }

    /// Creates a source `KindFloat32`, preserving the supplied float64 payload.
    pub fn new_float32_from_f64(value: f64) -> Self {
        Self::Float32(value)
    }

    /// Creates a binary literal datum.
    pub fn new_binary_literal(value: BinaryLiteral) -> Self {
        Self::BinaryLiteral(value)
    }

    /// Creates a MySQL BIT datum.
    pub fn new_mysql_bit(value: BinaryLiteral) -> Self {
        Self::Bit(value)
    }

    /// Creates a MySQL duration datum.
    pub fn new_duration(value: MySqlDuration) -> Self {
        Self::Duration(value)
    }

    /// Creates a MySQL ENUM datum.
    pub fn new_enum(value: MysqlEnum, collation: Collation) -> Self {
        Self::Enum(value, collation)
    }

    /// Creates a MySQL SET datum.
    pub fn new_set(value: MysqlSet, collation: Collation) -> Self {
        Self::Set(value, collation)
    }

    /// Creates a MySQL temporal datum.
    pub fn new_time(value: Time) -> Self {
        Self::Time(value)
    }

    /// Creates a MySQL binary JSON datum.
    pub fn new_json(value: BinaryJSON) -> Self {
        Self::Json(value)
    }

    /// Creates internal raw bytes.
    pub fn new_raw(value: impl Into<Vec<u8>>) -> Self {
        Self::Raw(value.into())
    }

    /// Creates a vector datum.
    pub fn new_vector_float32(value: VectorFloat32) -> Self {
        Self::VectorFloat32(value)
    }

    /// Equivalent to Go `NewStringDatum`: the payload uses TiDB's default
    /// collation. `Into<Vec<u8>>` deliberately accepts raw bytes as well as
    /// Rust strings because a Go string is not required to be valid UTF-8.
    pub fn new_string(bytes: impl Into<Vec<u8>>) -> Self {
        Self::new_collation_string(bytes, Collation::DEFAULT)
    }

    /// Equivalent to Go `NewCollationStringDatum` / `SetString`, except the
    /// typed collation makes an unregistered name impossible.
    pub fn new_collation_string(bytes: impl Into<Vec<u8>>, collation: Collation) -> Self {
        Self::String(StringDatum::new(bytes, collation))
    }

    /// Equivalent to Go `NewBytesDatum` / `SetBytes`. A byte datum always has
    /// the binary collation; there is no metadata field that can contradict
    /// that invariant.
    pub fn new_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Bytes(bytes.into())
    }

    /// Returns the datum's source-level kind.
    pub const fn kind(&self) -> DatumKind {
        match self {
            Self::Null => DatumKind::Null,
            Self::MinNotNull => DatumKind::MinNotNull,
            Self::MaxValue => DatumKind::MaxValue,
            Self::Int(_) => DatumKind::Int,
            Self::UInt(_) => DatumKind::UInt,
            Self::Decimal(_) => DatumKind::Decimal,
            Self::Real(_) => DatumKind::Real,
            Self::Float32(_) => DatumKind::Float32,
            Self::String(_) => DatumKind::String,
            Self::Bytes(_) => DatumKind::Bytes,
            Self::BinaryLiteral(_) => DatumKind::BinaryLiteral,
            Self::Duration(_) => DatumKind::Duration,
            Self::Enum(_, _) => DatumKind::Enum,
            Self::Bit(_) => DatumKind::Bit,
            Self::Set(_, _) => DatumKind::Set,
            Self::Time(_) => DatumKind::Time,
            Self::Json(_) => DatumKind::Json,
            Self::Raw(_) => DatumKind::Raw,
            Self::VectorFloat32(_) => DatumKind::VectorFloat32,
        }
    }

    /// Returns whether this datum is SQL NULL.
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Returns whether this datum is Go TiDB's minimum non-NULL range sentinel.
    pub const fn is_min_not_null(&self) -> bool {
        matches!(self, Self::MinNotNull)
    }

    /// Returns whether this datum is Go TiDB's maximum range sentinel.
    pub const fn is_max_value(&self) -> bool {
        matches!(self, Self::MaxValue)
    }

    /// Returns whether this datum is a non-scalar range endpoint.
    pub const fn is_range_sentinel(&self) -> bool {
        matches!(self, Self::MinNotNull | Self::MaxValue)
    }

    /// Compares the source-defined NULL/range-sentinel part of `Datum.Compare`.
    ///
    /// Go TiDB orders these four classes as
    /// `NULL < MinNotNull < ordinary scalar < MaxValue`. If both operands are
    /// ordinary scalars, this returns `None`: their comparison needs the
    /// caller's statement context and collator and must not be guessed by the
    /// dependency-leaf representation crate.
    pub fn compare_sentinel_order(&self, other: &Self) -> Option<Ordering> {
        match (sentinel_rank(self), sentinel_rank(other)) {
            (Some(left), Some(right)) => Some(left.cmp(&right)),
            (Some(SentinelRank::MaxValue), None) => Some(Ordering::Greater),
            (Some(_), None) => Some(Ordering::Less),
            (None, Some(SentinelRank::MaxValue)) => Some(Ordering::Less),
            (None, Some(_)) => Some(Ordering::Greater),
            (None, None) => None,
        }
    }

    /// Source `Datum.Compare` with an explicit collator.
    ///
    /// This follows Go's right-hand-kind dispatch. That detail matters for
    /// mixed numeric, string, temporal, enum/set, binary-literal, and JSON
    /// comparisons because the right operand selects the conversion domain.
    pub fn compare(&self, other: &Self, comparer: Collation) -> Result<Ordering, DatumValueError> {
        if matches!(self, Self::Json(_)) && !matches!(other, Self::Json(_)) {
            return other.compare(self, comparer).map(Ordering::reverse);
        }
        if let Self::Json(value) = other {
            return self.compare_json(value);
        }
        if let Some(ordering) = self.compare_sentinel_order(other) {
            return Ok(ordering);
        }
        match other {
            Self::Int(value) => self.compare_i64(*value),
            Self::UInt(value) => self.compare_u64(*value),
            Self::Real(value) | Self::Float32(value) => self.compare_f64(*value),
            Self::String(value) => self.compare_string(value.bytes(), comparer),
            Self::Bytes(value) => self.compare_string(value, comparer),
            Self::Decimal(value) => self.compare_decimal(value),
            Self::Duration(value) => self.compare_duration(*value),
            Self::Enum(value, _) => self.compare_enum(value, comparer),
            Self::BinaryLiteral(value) | Self::Bit(value) => {
                self.compare_binary_literal(value, comparer)
            }
            Self::Set(value, _) => self.compare_set(value, comparer),
            Self::Json(_) => unreachable!("JSON comparison returned above"),
            Self::Time(value) => self.compare_time(*value),
            Self::VectorFloat32(value) => self.compare_vector(value),
            Self::Raw(_) => Ok(Ordering::Equal),
            Self::Null | Self::MinNotNull | Self::MaxValue => {
                unreachable!("sentinel comparison returned above")
            }
        }
    }

    fn compare_i64(&self, value: i64) -> Result<Ordering, DatumValueError> {
        match self {
            Self::Int(left) => Ok(left.cmp(&value)),
            Self::UInt(left) => Ok(if value < 0 {
                Ordering::Greater
            } else {
                left.cmp(&(value as u64))
            }),
            _ => self.compare_f64(value as f64),
        }
    }

    fn compare_u64(&self, value: u64) -> Result<Ordering, DatumValueError> {
        match self {
            Self::Int(left) => Ok(if *left < 0 {
                Ordering::Less
            } else {
                (*left as u64).cmp(&value)
            }),
            Self::UInt(left) => Ok(left.cmp(&value)),
            _ => self.compare_f64(value as f64),
        }
    }

    fn compare_f64(&self, value: f64) -> Result<Ordering, DatumValueError> {
        let left = match self {
            Self::Int(left) => *left as f64,
            Self::UInt(left) => *left as f64,
            Self::Real(left) | Self::Float32(left) => *left,
            Self::String(left) => numeric_bytes_to_float(left.bytes())?,
            Self::Bytes(left) => numeric_bytes_to_float(left)?,
            Self::Decimal(left) => left.to_f64(),
            Self::Duration(left) => left.nanoseconds() as f64 / 1_000_000_000.0,
            Self::Enum(left, _) => left.to_number(),
            Self::BinaryLiteral(left) | Self::Bit(left) => left.to_int().value() as f64,
            Self::Set(left, _) => left.to_number(),
            Self::Time(left) => left.to_number().to_f64(),
            _ => return Ok(Ordering::Less),
        };
        Ok(float_order(left, value))
    }

    fn compare_string(
        &self,
        value: &[u8],
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value)),
            Self::Bytes(left) => Ok(comparer.compare(left, value)),
            Self::Decimal(left) => Ok(left.cmp(&decimal_from_bytes(value)?.value)),
            Self::Time(left) => {
                let text = std::str::from_utf8(value)?;
                let parsed = parse_datetime(text, &chrono_tz::UTC, true, false)
                    .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
                Ok(left.compare(parsed.time))
            }
            Self::Duration(left) => {
                let parsed = parse_duration(value, 6)
                    .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
                Ok(left.nanoseconds().cmp(&parsed.nanoseconds()))
            }
            Self::Set(left, _) => Ok(comparer.compare(left.name().as_bytes(), value)),
            Self::Enum(left, _) => Ok(comparer.compare(left.name().as_bytes(), value)),
            Self::BinaryLiteral(left) | Self::Bit(left) => {
                Ok(comparer.compare(left.as_bytes(), value))
            }
            _ => self.compare_f64(numeric_bytes_to_float(value)?),
        }
    }

    fn compare_decimal(&self, value: &Decimal) -> Result<Ordering, DatumValueError> {
        let left = match self {
            Self::Decimal(left) => left.clone(),
            Self::String(left) => decimal_from_bytes(left.bytes())?.value,
            Self::Bytes(left) => decimal_from_bytes(left)?.value,
            _ => self.to_decimal()?.value,
        };
        Ok(left.cmp(value))
    }

    fn compare_duration(&self, value: MySqlDuration) -> Result<Ordering, DatumValueError> {
        match self {
            Self::Duration(left) => Ok(left.compare(value)),
            Self::String(left) => compare_duration_bytes(left.bytes(), value),
            Self::Bytes(left) => compare_duration_bytes(left, value),
            _ => self.compare_f64(value.nanoseconds() as f64 / 1_000_000_000.0),
        }
    }

    fn compare_enum(
        &self,
        value: &MysqlEnum,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value.name().as_bytes())),
            Self::Bytes(left) => Ok(comparer.compare(left, value.name().as_bytes())),
            Self::Enum(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            Self::Set(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            _ => self.compare_f64(value.to_number()),
        }
    }

    fn compare_binary_literal(
        &self,
        value: &BinaryLiteral,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value.as_bytes())),
            Self::Bytes(left) => Ok(comparer.compare(left, value.as_bytes())),
            Self::BinaryLiteral(left) | Self::Bit(left) => {
                Ok(comparer.compare(left.as_bytes(), value.as_bytes()))
            }
            _ => self.compare_f64(value.to_int().value() as f64),
        }
    }

    fn compare_set(
        &self,
        value: &MysqlSet,
        comparer: Collation,
    ) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => Ok(comparer.compare(left.bytes(), value.name().as_bytes())),
            Self::Bytes(left) => Ok(comparer.compare(left, value.name().as_bytes())),
            Self::Enum(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            Self::Set(left, _) => {
                Ok(comparer.compare(left.name().as_bytes(), value.name().as_bytes()))
            }
            _ => self.compare_f64(value.to_number()),
        }
    }

    fn compare_json(&self, value: &BinaryJSON) -> Result<Ordering, DatumValueError> {
        if matches!(self, Self::Null) {
            return Ok(Ordering::Greater);
        }
        Ok(compare_binary_json(&self.to_mysql_json()?, value))
    }

    fn compare_time(&self, value: Time) -> Result<Ordering, DatumValueError> {
        match self {
            Self::String(left) => compare_time_bytes(left.bytes(), value),
            Self::Bytes(left) => compare_time_bytes(left, value),
            Self::Time(left) => Ok(left.compare(value)),
            _ => self.compare_f64(value.to_number().to_f64()),
        }
    }

    fn compare_vector(&self, value: &VectorFloat32) -> Result<Ordering, DatumValueError> {
        match self {
            Self::VectorFloat32(left) => Ok(left.compare(value)),
            _ => Err(DatumValueError::Comparison(
                "cannot compare vector and non-vector, cast is required".to_owned(),
            )),
        }
    }

    /// Returns the signed payload when this is [`Datum::Int`].
    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    /// Returns the unsigned payload when this is [`Datum::UInt`].
    pub const fn as_uint(&self) -> Option<u64> {
        match self {
            Self::UInt(value) => Some(*value),
            _ => None,
        }
    }

    /// Borrows the decimal payload when this is [`Datum::Decimal`].
    pub const fn as_decimal(&self) -> Option<&Decimal> {
        match self {
            Self::Decimal(value) => Some(value),
            _ => None,
        }
    }

    /// Returns the floating-point payload when this is [`Datum::Real`].
    pub const fn as_real(&self) -> Option<f64> {
        match self {
            Self::Real(value) | Self::Float32(value) => Some(*value),
            _ => None,
        }
    }

    /// Borrows the binary JSON payload when this is [`Datum::Json`].
    pub const fn as_json(&self) -> Option<&BinaryJSON> {
        match self {
            Self::Json(value) => Some(value),
            _ => None,
        }
    }

    /// Borrows the string payload and its collation metadata.
    pub const fn as_string(&self) -> Option<&StringDatum> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// Mirrors the byte-level behavior shared by Go `GetString` and
    /// `GetBytes`: string, bytes, and internal raw datums expose the stored
    /// payload without validating, replacing, or truncating it.
    pub fn as_raw_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::String(value) => Some(value.bytes()),
            Self::Bytes(value) | Self::Raw(value) => Some(value),
            _ => None,
        }
    }

    /// Consumes a string, bytes, or raw datum and returns its unchanged payload.
    pub fn into_raw_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::String(value) => Some(value.into_bytes()),
            Self::Bytes(value) | Self::Raw(value) => Some(value),
            _ => None,
        }
    }

    /// Go `SetBytes` records `charset.CollationBin`; string collation is kept
    /// on its payload. Numeric and NULL datums have no string collation.
    pub const fn collation(&self) -> Option<Collation> {
        match self {
            Self::String(value) => Some(value.collation()),
            Self::Bytes(_) => Some(Collation::Binary),
            Self::Enum(_, collation) | Self::Set(_, collation) => Some(*collation),
            _ => None,
        }
    }

    /// Derives the string datum's character set from its collation.
    pub fn charset(&self) -> Option<Charset> {
        self.collation().map(Collation::charset)
    }

    /// Source `Datum.GetBinaryStringEncoded`.
    pub fn binary_string_encoded(&self) -> Option<Vec<u8>> {
        let bytes = self.as_raw_bytes()?;
        let charset = self.charset()?;
        Some(
            crate::find_encoding_take_utf8_as_noop(charset.name())
                .transform(bytes, TransformOp::ENCODE_NO_ERR)
                .into_parts()
                .0,
        )
    }

    /// Source `Datum.GetBinaryStringDecoded`.
    pub fn binary_string_decoded(
        &self,
        flags: ConversionFlags,
        charset: &str,
    ) -> Option<EncodingResult> {
        let bytes = self.as_raw_bytes()?;
        let Some(encoding) = datum_encoding(flags, charset) else {
            return Some(Encoding::Binary.transform(bytes, TransformOp::DECODE));
        };
        Some(encoding.transform(bytes, TransformOp::DECODE))
    }

    /// Source `Datum.GetStringWithCheck`.
    pub fn string_with_check(
        &self,
        flags: ConversionFlags,
        charset: &str,
    ) -> Option<EncodingResult> {
        let bytes = self.as_raw_bytes()?;
        let Some(encoding) = datum_encoding(flags, charset) else {
            return Some(Encoding::Binary.transform(bytes, TransformOp::REPLACE));
        };
        if encoding.is_valid(bytes) {
            Some(Encoding::Binary.transform(bytes, TransformOp::REPLACE))
        } else {
            Some(encoding.transform(bytes, TransformOp::REPLACE))
        }
    }

    /// Replaces the datum with SQL NULL.
    pub fn set_null(&mut self) {
        *self = Self::Null;
    }

    /// Equivalent to Go `Datum.SetMinNotNull`.
    pub fn set_min_not_null(&mut self) {
        *self = Self::MinNotNull;
    }

    /// Replaces the datum with raw string bytes and a registered collation.
    pub fn set_string(&mut self, bytes: impl Into<Vec<u8>>, collation: Collation) {
        *self = Self::new_collation_string(bytes, collation);
    }

    /// Replaces the datum with arbitrary binary bytes.
    pub fn set_bytes(&mut self, bytes: impl Into<Vec<u8>>) {
        *self = Self::new_bytes(bytes);
    }

    /// Renders the scalar in the existing Go-oracle label format.
    ///
    /// Every valid UTF-8 payload keeps the historical `STR:<text>` bytes,
    /// including embedded NUL and other control characters used by the Go
    /// differential oracle. Only invalid UTF-8 uses an uppercase hexadecimal
    /// suffix because it cannot be represented by a Rust [`String`].
    pub fn label(&self) -> String {
        match self {
            Self::Int(value) => format!("INT:{value}"),
            Self::UInt(value) => format!("UINT:{value}"),
            Self::Decimal(value) => format!("DEC:{value}"),
            Self::Real(value) => format!("FLOAT:{value}"),
            Self::Float32(value) => format!("FLOAT:{value}"),
            Self::String(value) => label_bytes("STR", value.bytes()),
            Self::Bytes(value) => label_bytes("STR", value),
            Self::BinaryLiteral(value) | Self::Bit(value) => label_bytes("STR", value.as_bytes()),
            Self::Duration(value) => format!("DUR:{value}"),
            Self::Enum(value, _) => format!("ENUM:{value}"),
            Self::Set(value, _) => format!("SET:{value}"),
            Self::Time(value) => format!("TIME:{value}"),
            Self::Json(value) => format!("JSON:{value}"),
            Self::Raw(value) => label_bytes("RAW", value),
            Self::VectorFloat32(value) => format!("VECTOR:{value}"),
            Self::Null => "NULL".to_string(),
            Self::MinNotNull => "SKIP:15".to_string(),
            Self::MaxValue => "SKIP:16".to_string(),
        }
    }

    /// Renders TiDB's result-set string form without lossy UTF-8 decoding.
    ///
    /// Existing valid UTF-8 behavior is unchanged. Invalid UTF-8 is returned
    /// to the caller as an error so a semantic coercion cannot silently turn
    /// raw bytes into replacement text or a hexadecimal diagnostic label.
    pub fn sql_string(&self) -> Result<String, DatumStringError> {
        match self {
            Self::Int(value) => Ok(value.to_string()),
            Self::UInt(value) => Ok(value.to_string()),
            Self::Decimal(value) => Ok(value.to_string()),
            Self::Real(value) => Ok(value.to_string()),
            Self::Float32(value) => Ok((*value as f32).to_string()),
            Self::String(value) => decode_bytes(value.bytes()),
            Self::Bytes(value) => decode_bytes(value),
            Self::BinaryLiteral(value) | Self::Bit(value) => decode_bytes(value.as_bytes()),
            Self::Duration(value) => Ok(value.to_string()),
            Self::Enum(value, _) => Ok(value.to_string()),
            Self::Set(value, _) => Ok(value.to_string()),
            Self::Time(value) => Ok(value.to_string()),
            Self::Json(value) => Ok(value.to_string()),
            Self::Raw(value) => decode_bytes(value),
            Self::VectorFloat32(value) => Ok(value.to_string()),
            Self::Null => Ok(String::new()),
            Self::MinNotNull => Err(DatumStringError::RangeSentinel(DatumKind::MinNotNull)),
            Self::MaxValue => Err(DatumStringError::RangeSentinel(DatumKind::MaxValue)),
        }
    }

    /// Source `Datum.TruncatedStringify` used by EXPLAIN and diagnostics.
    pub fn truncated_stringify(&self) -> Result<Vec<u8>, DatumStringError> {
        let bytes = match self {
            Self::String(value) => value.bytes().to_vec(),
            Self::Bytes(value) => value.clone(),
            Self::Json(value) => value.to_string().into_bytes(),
            Self::VectorFloat32(value) => return Ok(value.truncated_string().into_bytes()),
            Self::Int(value) => return Ok(value.to_string().into_bytes()),
            Self::UInt(value) => return Ok(value.to_string().into_bytes()),
            other => other.sql_string()?.into_bytes(),
        };
        Ok(truncate_diagnostic_bytes(bytes))
    }

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
    pub fn to_i64(&self) -> Result<Converted<i64>, DatumValueError> {
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
            Self::Time(value) => decimal_to_i64(value.to_number()),
            Self::Duration(value) => decimal_to_i64(value.to_number()),
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

    /// Source `Datum.ToBytes`.
    pub fn to_bytes(&self) -> Result<Vec<u8>, DatumStringError> {
        match self {
            Self::String(value) => Ok(value.bytes().to_vec()),
            Self::Bytes(value) => Ok(value.clone()),
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

    /// Owned memory estimate for the Rust enum representation.
    pub fn estimated_mem_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::String(value) => value.bytes().len(),
                Self::Bytes(value) | Self::Raw(value) => value.len(),
                Self::BinaryLiteral(value) | Self::Bit(value) => value.as_bytes().len(),
                Self::Json(value) => value.value().len(),
                Self::VectorFloat32(value) => value.estimated_mem_usage(),
                _ => 0,
            }
    }

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
            10 => Ok(Self::new_enum(
                MysqlEnum::new(std::str::from_utf8(&bytes)?.to_owned(), i as u64),
                collation,
            )),
            11 => Ok(Self::new_mysql_bit(BinaryLiteral::from(bytes))),
            12 => Ok(Self::new_set(
                MysqlSet::new(std::str::from_utf8(&bytes)?.to_owned(), i as u64),
                collation,
            )),
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

fn datum_encoding(flags: ConversionFlags, charset: &str) -> Option<Encoding> {
    let mut encoding = crate::find_encoding(charset);
    if (encoding.encoding_type() == EncodingType::Utf8 && flags.skip_utf8_check())
        || (encoding.encoding_type() == EncodingType::Ascii && flags.skip_ascii_check())
    {
        return None;
    }
    if charset == "utf8" && !flags.skip_utf8mb4_check() {
        encoding = Encoding::Utf8Mb3Strict;
    }
    Some(encoding)
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

fn decimal_from_bytes(bytes: &[u8]) -> Result<Converted<Decimal>, DatumValueError> {
    let text = std::str::from_utf8(bytes)?;
    Ok(match crate::convert::decimal_text(text) {
        Some(value) => Converted { value, event: None },
        _ => Converted {
            value: Decimal::from_int(0),
            event: Some(ScalarConversionEvent::Truncated),
        },
    })
}

fn numeric_bytes_to_float(bytes: &[u8]) -> Result<f64, DatumValueError> {
    Ok(str_to_float(std::str::from_utf8(bytes)?, false).value)
}

fn compare_duration_bytes(bytes: &[u8], value: MySqlDuration) -> Result<Ordering, DatumValueError> {
    let parsed =
        parse_duration(bytes, 6).map_err(|error| DatumValueError::Comparison(error.to_string()))?;
    Ok(parsed.nanoseconds().cmp(&value.nanoseconds()))
}

fn compare_time_bytes(bytes: &[u8], value: Time) -> Result<Ordering, DatumValueError> {
    let text = std::str::from_utf8(bytes)?;
    let parsed = parse_datetime(text, &chrono_tz::UTC, true, false)
        .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
    Ok(parsed.time.compare(value))
}

fn float_order(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right).unwrap_or_else(|| {
        if left.is_nan() {
            if right.is_nan() {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        } else {
            Ordering::Greater
        }
    })
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

/// Deep-copies a datum row.
pub fn clone_row(row: &[Datum]) -> Vec<Datum> {
    row.to_vec()
}

/// Returns whether any datum is SQL NULL.
pub fn datums_contain_null(datums: &[Datum]) -> bool {
    datums.iter().any(Datum::is_null)
}

/// Source `SortDatums`, using each right-hand datum's collation.
pub fn sort_datums(datums: &mut [Datum]) -> Result<(), DatumValueError> {
    for index in 1..datums.len() {
        let mut current = index;
        while current > 0 {
            let collation = datums[current].collation().unwrap_or(Collation::Binary);
            if datums[current - 1].compare(&datums[current], collation)? != Ordering::Greater {
                break;
            }
            datums.swap(current - 1, current);
            current -= 1;
        }
    }
    Ok(())
}

/// Estimates memory for a one- or two-dimensional datum array.
pub fn estimated_mem_usage(datums: &[Datum], rows: usize) -> usize {
    if rows == 0 {
        return 0;
    }
    datums.iter().map(Datum::estimated_mem_usage).sum::<usize>() * rows
}

/// Source `DatumsToString`.
pub fn datums_to_string(
    datums: &[Datum],
    handle_special_values: bool,
    binary_as_hex: bool,
) -> Result<String, DatumStringError> {
    use fmt::Write;

    let mut output = String::new();
    if datums.len() > 1 {
        output.push('(');
    }
    for (index, datum) in datums.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        if handle_special_values {
            match datum {
                Datum::Null => {
                    output.push_str("NULL");
                    continue;
                }
                Datum::MinNotNull => {
                    output.push_str("-inf");
                    continue;
                }
                Datum::MaxValue => {
                    output.push_str("+inf");
                    continue;
                }
                _ => {}
            }
        }
        let mut text = datum.sql_string()?;
        let original_length = (text.len() > 2048).then_some(text.len());
        if original_length.is_some() {
            text.truncate(2048);
        }
        if matches!(datum, Datum::String(_)) {
            if binary_as_hex && !is_printable(text.as_bytes()) {
                write!(output, "0x{}", encode_hex(text.as_bytes()))
                    .expect("writing to String cannot fail");
            } else {
                write!(output, "\"{text}\"").expect("writing to String cannot fail");
            }
        } else {
            output.push_str(&text);
        }
        if let Some(length) = original_length {
            write!(output, " len({length})").expect("writing to String cannot fail");
        }
    }
    if datums.len() > 1 {
        output.push(')');
    }
    Ok(output)
}

/// Source `DatumsToStrNoErr`.
pub fn datums_to_string_no_error(datums: &[Datum]) -> String {
    datums_to_string(datums, true, false).unwrap_or_default()
}

/// Source `DatumsToStrNoErrSmart`.
pub fn datums_to_string_no_error_smart(datums: &[Datum]) -> String {
    datums_to_string(datums, true, true).unwrap_or_default()
}

/// Source printable-string predicate.
pub fn is_printable(value: &[u8]) -> bool {
    std::str::from_utf8(value).is_ok_and(|text| !text.chars().any(char::is_control))
}

fn truncate_diagnostic_bytes(mut value: Vec<u8>) -> Vec<u8> {
    const MAX_LEN: usize = 64;
    if value.len() <= MAX_LEN {
        return value;
    }
    let original_len = value.len();
    value.truncate(MAX_LEN);
    value.extend_from_slice(b"...(len:");
    value.extend_from_slice(original_len.to_string().as_bytes());
    value.push(b')');
    value
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SentinelRank {
    Null,
    MinNotNull,
    MaxValue,
}

const fn sentinel_rank(datum: &Datum) -> Option<SentinelRank> {
    match datum {
        Datum::Null => Some(SentinelRank::Null),
        Datum::MinNotNull => Some(SentinelRank::MinNotNull),
        Datum::MaxValue => Some(SentinelRank::MaxValue),
        Datum::Int(_)
        | Datum::UInt(_)
        | Datum::Decimal(_)
        | Datum::Real(_)
        | Datum::Float32(_)
        | Datum::String(_)
        | Datum::Bytes(_)
        | Datum::BinaryLiteral(_)
        | Datum::Duration(_)
        | Datum::Enum(_, _)
        | Datum::Bit(_)
        | Datum::Set(_, _)
        | Datum::Time(_)
        | Datum::Json(_)
        | Datum::Raw(_)
        | Datum::VectorFloat32(_) => None,
    }
}

fn label_bytes(kind: &str, bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => format!("{kind}:{text}"),
        Err(_) => format!("{kind}_HEX:{}", encode_hex(bytes)),
    }
}

fn decode_bytes(bytes: &[u8]) -> Result<String, DatumStringError> {
    std::str::from_utf8(bytes)
        .map(str::to_string)
        .map_err(DatumStringError::InvalidUtf8)
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02X}").expect("writing to String cannot fail");
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::{
        clone_row, datums_contain_null, datums_to_string, datums_to_string_no_error,
        datums_to_string_no_error_smart, estimated_mem_usage, is_printable, sort_datums, Datum,
        DatumKind,
    };
    use crate::{
        parse_datetime, parse_enum_value, parse_set_value, BinaryJSON, BinaryLiteral, Charset,
        Collation, ConversionFlags, Decimal, MySqlDuration, TimeType,
    };

    /// Source: `pkg/types/datum.go` (`NewStringDatum`,
    /// `NewCollationStringDatum`, `GetString`, `GetBytes`, and `SetBytes`).
    #[test]
    fn go_string_and_bytes_contract_keeps_arbitrary_octets() {
        let raw = vec![b'a', 0, 0xff, 0xfe, b'z'];

        let string = Datum::new_collation_string(raw.clone(), Collation::Utf8Mb4Bin);
        assert_eq!(string.kind(), DatumKind::String);
        assert_eq!(string.as_raw_bytes(), Some(raw.as_slice()));
        assert_eq!(string.collation(), Some(Collation::Utf8Mb4Bin));
        assert_eq!(string.charset(), Some(Charset::Utf8Mb4));
        assert!(std::str::from_utf8(string.as_raw_bytes().unwrap()).is_err());

        let bytes = Datum::new_bytes(raw.clone());
        assert_eq!(bytes.kind(), DatumKind::Bytes);
        assert_eq!(bytes.as_raw_bytes(), Some(raw.as_slice()));
        assert_eq!(bytes.collation(), Some(Collation::Binary));
        assert_eq!(bytes.charset(), Some(Charset::Binary));
    }

    #[test]
    fn source_binary_string_encoding_and_validation_rows() {
        let utf8 = "你好".as_bytes();
        let gbk = [0xC4, 0xE3, 0xBA, 0xC3];

        for (datum, expected) in [
            (
                Datum::new_collation_string(utf8, Collation::Utf8Bin),
                utf8.to_vec(),
            ),
            (
                Datum::new_collation_string(utf8, Collation::Utf8Mb4Bin),
                utf8.to_vec(),
            ),
            (
                Datum::new_collation_string(utf8, Collation::GbkBin),
                gbk.to_vec(),
            ),
            (Datum::new_bytes(gbk), gbk.to_vec()),
        ] {
            assert_eq!(datum.binary_string_encoded().unwrap(), expected);
        }

        let decoded = Datum::new_bytes(gbk)
            .binary_string_decoded(ConversionFlags::default(), "gbk")
            .unwrap();
        assert_eq!(decoded.bytes(), utf8);
        assert!(decoded.error().is_none());

        let invalid_utf8 = Datum::new_string([utf8, &[0x81]].concat());
        let checked = invalid_utf8
            .string_with_check(ConversionFlags::default(), "utf8mb4")
            .unwrap();
        assert!(checked.error().is_some());
        assert_eq!(checked.bytes(), [utf8, b"?"].concat());

        let skipped = invalid_utf8
            .string_with_check(
                ConversionFlags::default().with_skip_utf8_check(true),
                "utf8mb4",
            )
            .unwrap();
        assert!(skipped.error().is_none());
        assert_eq!(skipped.bytes(), [utf8, &[0x81]].concat());

        let utf8mb4 = Datum::new_string("𠆢");
        assert!(utf8mb4
            .string_with_check(ConversionFlags::default(), "utf8")
            .unwrap()
            .error()
            .is_some());
        assert!(utf8mb4
            .string_with_check(
                ConversionFlags::default().with_skip_utf8mb4_check(true),
                "utf8",
            )
            .unwrap()
            .error()
            .is_none());
    }

    /// Source: `pkg/types/datum_test.go::TestDatum` and the constructor block
    /// in `pkg/types/datum.go`.
    #[test]
    fn go_supported_scalar_constructor_vectors() {
        assert!(Datum::default().is_null());
        assert_eq!(Datum::new_int(-1).as_int(), Some(-1));
        assert_eq!(Datum::new_uint(u64::MAX).as_uint(), Some(u64::MAX));
        assert_eq!(Datum::new_real(1.25).as_real(), Some(1.25));

        let decimal = Decimal::from_literal("72.5");
        assert_eq!(
            Datum::new_decimal(decimal.clone()).as_decimal(),
            Some(&decimal)
        );

        let string = Datum::new_string("abc");
        assert_eq!(string.as_raw_bytes(), Some(&b"abc"[..]));
        assert_eq!(string.collation(), Some(Collation::DEFAULT));
    }

    /// Replacing a datum through Go's setters must replace kind-specific
    /// metadata as well. The enum assignment makes that transition atomic.
    #[test]
    fn setters_replace_kind_and_string_metadata_together() {
        let mut datum = Datum::new_bytes(vec![0xff, 0]);
        datum.set_string(vec![0xfe, 0], Collation::Utf8Mb4Bin);
        assert_eq!(datum.kind(), DatumKind::String);
        assert_eq!(datum.collation(), Some(Collation::Utf8Mb4Bin));

        datum.set_bytes(vec![0xfd, 0]);
        assert_eq!(datum.kind(), DatumKind::Bytes);
        assert_eq!(datum.collation(), Some(Collation::Binary));

        datum.set_null();
        assert!(datum.is_null());
        assert_eq!(datum.as_raw_bytes(), None);
        assert_eq!(datum.collation(), None);
    }

    /// Source: `pkg/types/datum.go::GetString` / `GetBytes`. Diagnostics may
    /// encode arbitrary octets, but semantic stringification must reject
    /// invalid UTF-8 instead of replacing or reinterpreting it.
    #[test]
    fn diagnostic_labels_are_lossless_but_sql_stringification_is_checked() {
        assert_eq!(Datum::new_string("TiDB").label(), "STR:TiDB");

        let invalid = Datum::new_bytes(vec![0xff, 0, b'A']);
        assert_eq!(invalid.label(), "STR_HEX:FF0041");
        assert!(invalid.sql_string().is_err());

        let embedded_nul = Datum::new_string(vec![b'a', 0, b'b']);
        assert_eq!(embedded_nul.label().as_bytes(), b"STR:a\0b");
        assert_eq!(embedded_nul.sql_string().unwrap().as_bytes(), b"a\0b");
    }

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

        let malformed = Datum::new_string("1.1.1").to_decimal().unwrap();
        assert_eq!(malformed.value, Decimal::from_int(0));
        assert_eq!(
            malformed.event,
            Some(crate::ScalarConversionEvent::Truncated)
        );
    }

    #[test]
    fn source_clone_memory_string_and_null_rows() {
        let row = vec![
            Datum::Int(72),
            Datum::UInt(72),
            Datum::new_string("abcd"),
            Datum::new_bytes(b"abcd"),
            Datum::new_raw(b"raw"),
        ];
        let cloned = clone_row(&row);
        assert_eq!(cloned, row);
        assert_ne!(
            cloned[2].as_raw_bytes().unwrap().as_ptr(),
            row[2].as_raw_bytes().unwrap().as_ptr()
        );
        assert_eq!(estimated_mem_usage(&row, 0), 0);
        assert_eq!(
            estimated_mem_usage(&row, 10),
            row.iter().map(Datum::estimated_mem_usage).sum::<usize>() * 10
        );
        assert!(datums_contain_null(&[Datum::Int(1), Datum::Null]));
        assert!(!datums_contain_null(&[Datum::Int(1), Datum::UInt(2)]));

        let datums = [
            Datum::Int(1),
            Datum::UInt(2),
            Datum::Float32(-3.1111111),
            Datum::Real(4.123),
            Datum::new_decimal(Decimal::from_signed_literal("6.6")),
            Datum::new_string("abc"),
            Datum::MinNotNull,
            Datum::MaxValue,
        ];
        assert_eq!(
            datums_to_string(&datums, true, false).unwrap(),
            "(1, 2, -3.1111112, 4.123, 6.6, \"abc\", -inf, +inf)"
        );
        assert!(is_printable(b"abc"));
        assert!(!is_printable(b"a\0bc"));
        assert!(is_printable("abcé".as_bytes()));
        assert!(!is_printable(&[b'a', b'b', b'c', 0xc3]));

        let mut sortable = vec![Datum::Int(3), Datum::Int(-1), Datum::Int(2)];
        sort_datums(&mut sortable).unwrap();
        assert_eq!(sortable, vec![Datum::Int(-1), Datum::Int(2), Datum::Int(3)]);
        assert_eq!(
            datums_to_string_no_error(&[Datum::Int(1), Datum::UInt(2)]),
            "(1, 2)"
        );
        assert_eq!(
            datums_to_string_no_error_smart(&[Datum::new_string("a\0b")]),
            "0x610062"
        );
    }

    /// Source `BenchmarkDatumTruncatedStringify` inputs plus the byte-boundary
    /// contract exercised by the implementation.
    #[test]
    fn source_datum_truncated_stringify_rows() {
        let long = Datum::new_string("1".repeat(128));
        assert_eq!(
            long.truncated_stringify().unwrap(),
            format!("{}...(len:128)", "1".repeat(64)).into_bytes()
        );
        assert_eq!(
            Datum::new_int(2).truncated_stringify().unwrap(),
            b"2".to_vec()
        );
        let split_utf8 = Datum::new_string(format!("{}é", "a".repeat(63)));
        assert_eq!(
            split_utf8.truncated_stringify().unwrap(),
            [
                "a".repeat(63).into_bytes(),
                vec![0xc3],
                b"...(len:65)".to_vec()
            ]
            .concat()
        );
    }

    /// Source: `pkg/types/compare_test.go::TestCompare` and
    /// `TestCompareDatum`. Each row is also checked in reverse because the Go
    /// table makes antisymmetry part of the contract.
    #[test]
    fn source_compare_cross_kind_rows() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        let decimal_one = || Datum::new_decimal(Decimal::from_int(1));
        let literal = |value| Datum::new_binary_literal(BinaryLiteral::from_uint(value, None));
        let enum_one = || Datum::new_enum(parse_enum_value(&["a"], 1).unwrap(), Collation::Binary);
        let set_one = || Datum::new_set(parse_set_value(&["a"], 1).unwrap(), Collation::Binary);
        let zero_time = Datum::new_time(
            parse_datetime("0000-00-00 00:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let later_time = Datum::new_time(
            parse_datetime("2011-01-01 11:11:11", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let zero_duration = Datum::new_duration(MySqlDuration::from_nanoseconds(0, 0).unwrap());
        let positive_duration =
            Datum::new_duration(MySqlDuration::from_nanoseconds(29_034, 2).unwrap());

        let rows = vec![
            (Datum::Real(1.0), Datum::new_string("1"), Equal),
            (Datum::Int(-1), Datum::UInt(1), Less),
            (Datum::Int(-1), Datum::new_string("-1"), Equal),
            (Datum::UInt(1), Datum::new_string("1"), Equal),
            (decimal_one(), Datum::new_string("1"), Equal),
            (decimal_one(), Datum::new_bytes(b"1"), Equal),
            (Datum::new_string("1"), Datum::Int(-1), Greater),
            (Datum::new_string("1"), Datum::Real(2.0), Less),
            (
                Datum::new_string("hello"),
                Datum::new_decimal(Decimal::from_int(0)),
                Equal,
            ),
            (Datum::Null, Datum::Int(2), Less),
            (Datum::new_string(""), Datum::Null, Greater),
            (Datum::new_string("aasf"), Datum::new_string("4"), Greater),
            (Datum::new_bytes(b"abc"), Datum::new_bytes(b"ab"), Greater),
            (Datum::new_bytes(b"123"), Datum::Int(1234), Less),
            (literal(1), Datum::Int(1), Equal),
            (
                Datum::new_binary_literal(BinaryLiteral::from_uint(0x004D_7953_514C, None)),
                Datum::new_string("MySQL"),
                Equal,
            ),
            (literal(0), Datum::UInt(10), Less),
            (literal(1), decimal_one(), Equal),
            (enum_one(), Datum::Int(1), Equal),
            (enum_one(), Datum::new_string("a"), Equal),
            (enum_one(), Datum::UInt(10), Less),
            (enum_one(), literal(2), Less),
            (set_one(), Datum::Int(1), Equal),
            (set_one(), Datum::new_string("a"), Equal),
            (set_one(), enum_one(), Equal),
            (zero_time.clone(), later_time.clone(), Less),
            (
                later_time,
                Datum::new_string("0000-00-00 00:00:00"),
                Greater,
            ),
            (zero_duration.clone(), positive_duration, Less),
            (Datum::new_string("12:00:00"), zero_duration, Greater),
            (Datum::MaxValue, Datum::new_string("00:00:00"), Greater),
            (Datum::MinNotNull, Datum::new_string("00:00:00"), Less),
            (Datum::Null, Datum::MinNotNull, Less),
            (Datum::MinNotNull, Datum::MaxValue, Less),
            (
                Datum::new_json(BinaryJSON::parse("1").unwrap()),
                Datum::Null,
                Less,
            ),
        ];

        for (index, (left, right, expected)) in rows.into_iter().enumerate() {
            assert_eq!(
                left.compare(&right, Collation::Binary).unwrap(),
                expected,
                "forward row {index}: {left:?} versus {right:?}"
            );
            assert_eq!(
                right.compare(&left, Collation::Binary).unwrap(),
                expected.reverse(),
                "reverse row {index}: {right:?} versus {left:?}"
            );
        }
    }

    /// Source: `pkg/types/datum_test.go::TestMarshalDatum`.
    #[test]
    fn source_marshal_datum_round_trips_every_stored_kind() {
        let time = parse_datetime("2018-03-08 16:01:00.315313", &chrono_tz::UTC, true, false)
            .unwrap()
            .time;
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
            Datum::new_raw(b"raw"),
            Datum::new_vector_float32(crate::VectorFloat32::parse("[1,2]").unwrap()),
            Datum::MinNotNull,
            Datum::MaxValue,
        ];
        for (index, value) in values.into_iter().enumerate() {
            let encoded = value.marshal_json().unwrap();
            let decoded = Datum::unmarshal_json(&encoded).unwrap();
            assert_eq!(decoded, value, "round-trip row {index}: {encoded:?}");
        }
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
