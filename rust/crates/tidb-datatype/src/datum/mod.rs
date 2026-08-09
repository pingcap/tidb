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

//! The `Datum` value domain: kinds, payloads, constructors, and accessors.
//!
//! Mirrors the storage half of `pkg/types/datum.go` -- the `Datum` struct's
//! kind tag and payload slots, its `NewXxxDatum` constructors, `GetXxx`
//! accessors, `SetXxx` setters, charset/encoding views, `MemUsage`, and the
//! whole-row helpers `CloneRow`, `DatumsContainNull`, and `SortDatums`.
//! Behavior that is large enough to stand on its own lives beside this file:
//! comparison in [`compare`], scalar conversion in [`convert`], text
//! rendering in [`stringify`], and the persistence envelope in
//! [`json_envelope`].

use std::cmp::Ordering;
use std::fmt;

use crate::{
    BinaryJSON, BinaryJSONError, BinaryLiteral, Charset, Collation, ConversionFlags, Converted,
    Decimal, Encoding, EncodingResult, EncodingType, MySqlDuration, MysqlEnum, MysqlSet, Time,
    TransformOp, VectorFloat32,
};

mod compare;
mod convert;
mod json_envelope;
mod stringify;

pub use stringify::{
    datums_to_string, datums_to_string_no_error, datums_to_string_no_error_smart, is_printable,
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

/// Failure at a Rust textual projection of Go `Datum.ToString` bytes, or at
/// one of Go's non-string range sentinel branches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatumStringError {
    /// Byte-authoritative source output is not valid Rust UTF-8.
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
    /// A DATE/DATETIME/TIMESTAMP source did not form a value the target
    /// accepts -- Go `types.ErrWrongValue`, MySQL 1292.
    ///
    /// It carries the value Go returns BESIDE the error: `convertToMysqlTime`
    /// and `convertToMysqlTimestamp` both do `ret.SetMysqlTime(t)` before
    /// returning, and `table.CastValue`'s `handleZeroDatetime` reads that
    /// datum on the very path where the error is downgraded to a warning. An
    /// error with no value would leave the non-strict write path with nothing
    /// to store; every caller that only wants the failure can still ignore
    /// the payload.
    IncorrectTemporal(Time),
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
            Self::IncorrectTemporal(_) => formatter.write_str("incorrect temporal value"),
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

    /// Source `Datum.GetBytes`, which reads the datum's `b` field whatever the
    /// kind is and answers an empty slice when nothing was stored there. Every
    /// setter that writes `b` is covered: `SetString`/`SetBytes`/`SetRaw`,
    /// `SetBinaryLiteral`/`SetMysqlBit`, and `SetMysqlEnum`/`SetMysqlSet`,
    /// which both store the member NAME.
    ///
    /// `as_raw_bytes` is the narrower accessor for callers that must
    /// distinguish "no payload" from "empty payload"; anything transcreating a
    /// Go function whose body says `d.GetBytes()` wants this one, because
    /// Go has no such distinction to make.
    pub fn go_bytes(&self) -> &[u8] {
        match self {
            Self::String(value) => value.bytes(),
            Self::Bytes(value) | Self::Raw(value) => value,
            Self::BinaryLiteral(value) | Self::Bit(value) => value.as_bytes(),
            Self::Enum(value, _) => value.name().as_bytes(),
            Self::Set(value, _) => value.name().as_bytes(),
            _ => &[],
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

    /// Source `Datum.GetBinaryStringDecoded`, which reads `GetBytes` and so
    /// applies to every kind that stores a payload -- including the
    /// `BinaryLiteral` and `Bit` kinds that are the ONLY ones Go's
    /// `convertToString` and `pkg/ddl`'s `getDefaultValue` call it on.
    pub fn binary_string_decoded(&self, flags: ConversionFlags, charset: &str) -> EncodingResult {
        let bytes = self.go_bytes();
        let Some(encoding) = datum_encoding(flags, charset) else {
            return Encoding::Binary.transform(bytes, TransformOp::DECODE);
        };
        encoding.transform(bytes, TransformOp::DECODE)
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
    /// Go `Datum.MemUsage`: `EmptyDatumSize + cap(d.b) + len(d.collation)`.
    ///
    /// Go keeps every variable-length payload in the single `d.b` byte slice
    /// and every fixed-width one (int, real, decimal, time, duration) inline,
    /// so walking this enum's variants reproduces the same division. Two named
    /// departures, both forced by the representation: the constant term is
    /// this enum's size rather than Go's struct's, and the payload term is a
    /// LENGTH where Go reads a capacity -- every payload here is built
    /// exactly-sized, so a datum whose buffer was over-reserved and then
    /// shortened is under-reported by the slack.
    pub fn estimated_mem_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::String(value) => value.bytes().len() + value.collation().name().len(),
                Self::Bytes(value) | Self::Raw(value) => value.len(),
                Self::BinaryLiteral(value) | Self::Bit(value) => value.as_bytes().len(),
                Self::Json(value) => value.value().len(),
                Self::VectorFloat32(value) => value.estimated_mem_usage(),
                Self::Enum(value, collation) => value.name().len() + collation.name().len(),
                Self::Set(value, collation) => value.name().len() + collation.name().len(),
                _ => 0,
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

pub(super) fn decimal_from_bytes(bytes: &[u8]) -> Result<Converted<Decimal>, DatumValueError> {
    let text = std::str::from_utf8(bytes)?;
    Ok(crate::convert::decimal_from_text(text))
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

#[cfg(test)]
mod tests {
    use super::{
        clone_row, datums_contain_null, datums_to_string, datums_to_string_no_error,
        datums_to_string_no_error_smart, estimated_mem_usage, is_printable, sort_datums, Datum,
        DatumKind,
    };
    use crate::{
        parse_datetime, parse_enum_value, BinaryJSON, BinaryLiteral, Charset, Collation,
        ConversionFlags, Decimal, MySqlDuration, Time,
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

        let decoded =
            Datum::new_bytes(gbk).binary_string_decoded(ConversionFlags::default(), "gbk");
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

    #[test]
    fn test_convert_to_string_with_check_source_rows() {
        let utf8 = "你好".as_bytes();
        let utf8mb4 = "你好👋".as_bytes();
        let invalid_utf8 = [utf8, &[0x81]].concat();
        for (input, charset, flags, valid) in [
            (utf8, "utf8mb4", ConversionFlags::default(), true),
            (utf8mb4, "utf8mb4", ConversionFlags::default(), true),
            (
                utf8,
                "utf8mb4",
                ConversionFlags::default().with_skip_utf8_check(true),
                true,
            ),
            (
                utf8mb4,
                "utf8mb4",
                ConversionFlags::default().with_skip_utf8_check(true),
                true,
            ),
            (
                invalid_utf8.as_slice(),
                "utf8mb4",
                ConversionFlags::default().with_skip_utf8_check(true),
                true,
            ),
            (
                invalid_utf8.as_slice(),
                "utf8mb4",
                ConversionFlags::default(),
                false,
            ),
            (
                invalid_utf8.as_slice(),
                "ascii",
                ConversionFlags::default(),
                false,
            ),
            (
                invalid_utf8.as_slice(),
                "ascii",
                ConversionFlags::default().with_skip_ascii_check(true),
                true,
            ),
            (utf8mb4, "utf8", ConversionFlags::default(), false),
            (
                utf8mb4,
                "utf8",
                ConversionFlags::default().with_skip_utf8mb4_check(true),
                true,
            ),
        ] {
            let checked = Datum::new_string(input)
                .string_with_check(flags, charset)
                .unwrap();
            assert_eq!(checked.error().is_none(), valid, "{charset} {input:?}");
            if valid {
                assert_eq!(checked.bytes(), input, "{charset} {input:?}");
            }
        }
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

    #[test]
    fn test_is_null() {
        for (datum, expected) in [
            (Datum::Null, true),
            (Datum::Int(0), false),
            (Datum::Int(1), false),
            (Datum::Real(1.1), false),
            (Datum::new_string("string"), false),
            (Datum::new_string(""), false),
        ] {
            assert_eq!(datum.is_null(), expected, "{datum:?}");
        }
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

    #[test]
    fn test_clone_datum() {
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
        assert_ne!(
            cloned[3].as_raw_bytes().unwrap().as_ptr(),
            row[3].as_raw_bytes().unwrap().as_ptr()
        );
        assert_ne!(
            cloned[4].as_raw_bytes().unwrap().as_ptr(),
            row[4].as_raw_bytes().unwrap().as_ptr()
        );
    }

    #[test]
    fn test_estimated_mem_usage() {
        // The Go source row measures a 72-byte Datum, 40-byte MyDecimal,
        // 8-byte Time, and 5,530 bytes for ten copies. Rust deliberately owns
        // a different enum/heap representation, so exact byte parity is not a
        // meaningful contract; pin both measured layouts and the Rust formula.
        assert_eq!(
            (
                std::mem::size_of::<Datum>(),
                std::mem::size_of::<Decimal>(),
                std::mem::size_of::<Time>()
            ),
            (64, 64, 16)
        );

        let bytes = b"abcd";
        let row = vec![
            Datum::Int(1),
            Datum::Real(1.0),
            Datum::Float32(1.0),
            Datum::new_string(bytes),
            Datum::new_bytes(bytes),
            Datum::new_decimal(Decimal::from_signed_literal("1234.1234")),
            Datum::new_enum(parse_enum_value(&["a"], 1).unwrap(), Collation::Binary),
        ];
        assert_eq!(
            estimated_mem_usage(&row, 10),
            row.iter().map(Datum::estimated_mem_usage).sum::<usize>() * 10
        );
        assert_eq!(estimated_mem_usage(&row, 0), 0);
        assert_ne!(estimated_mem_usage(&row, 10), 5_530);
    }

    #[test]
    fn test_datums_to_string() {
        let zero_time = Datum::new_time(
            parse_datetime("0000-00-00 00:00:00", &chrono_tz::UTC, true, false)
                .unwrap()
                .time,
        );
        let datums = [
            Datum::Int(1),
            Datum::UInt(2),
            Datum::Float32(-3.1111111),
            Datum::Real(4.123),
            Datum::Real(f64::INFINITY),
            Datum::new_decimal(Decimal::from_signed_literal("6.6")),
            Datum::new_string("abc"),
            Datum::new_collation_string("", Collation::Binary),
            Datum::new_duration(MySqlDuration::from_nanoseconds(11_111, 0).unwrap()),
            zero_time,
            Datum::new_bytes(b"xxx"),
            Datum::new_binary_literal(BinaryLiteral::from(Vec::<u8>::new())),
            Datum::new_json(BinaryJSON::parse("null").unwrap()),
            Datum::MinNotNull,
            Datum::MaxValue,
        ];
        assert_eq!(
            datums_to_string(&datums, true, false).unwrap(),
            "(1, 2, -3.1111112, 4.123, +Inf, 6.6, \"abc\", \"\", 00:00:00, \
             0000-00-00 00:00:00, xxx, , null, -inf, +inf)"
        );
    }

    #[test]
    fn datum_row_helpers() {
        assert!(datums_contain_null(&[Datum::Int(1), Datum::Null]));
        assert!(!datums_contain_null(&[Datum::Int(1), Datum::UInt(2)]));

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

    #[test]
    fn test_is_printable() {
        for (input, expected) in [
            (b"abc".as_slice(), true),
            (b"a\0bc".as_slice(), false),
            ("abcé".as_bytes(), true),
            (&[b'a', b'b', b'c', 0xc3], false),
        ] {
            assert_eq!(is_printable(input), expected, "{input:?}");
        }
    }
}
