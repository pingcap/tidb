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

use crate::{Charset, Collation, Decimal};

/// The currently ported subset of Go TiDB's datum kinds.
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
    /// Raw string bytes with a registered collation.
    String,
    /// Raw bytes with binary semantics.
    Bytes,
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

/// A byte-preserving subset of `pkg/types/datum.go::Datum`.
///
/// Unlike Go's tagged storage box, the enum makes kind/payload disagreement
/// impossible. No placeholder temporal or JSON cases are present: those
/// variants arrive only with their source-backed representations.
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
    /// Byte-preserving string data (`KindString`).
    String(StringDatum),
    /// Arbitrary octets with binary semantics (`KindBytes`).
    Bytes(Vec<u8>),
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
            Self::String(_) => DatumKind::String,
            Self::Bytes(_) => DatumKind::Bytes,
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
            Self::Real(value) => Some(*value),
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
    /// `GetBytes`: both string kinds expose the stored payload without
    /// validating, replacing, or truncating it.
    pub fn as_raw_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::String(value) => Some(value.bytes()),
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }

    /// Consumes a string or bytes datum and returns its unchanged payload.
    pub fn into_raw_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::String(value) => Some(value.into_bytes()),
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }

    /// Go `SetBytes` records `charset.CollationBin`; string collation is kept
    /// on its payload. Numeric and NULL datums have no string collation.
    pub const fn collation(&self) -> Option<Collation> {
        match self {
            Self::String(value) => Some(value.collation()),
            Self::Bytes(_) => Some(Collation::Binary),
            _ => None,
        }
    }

    /// Derives the string datum's character set from its collation.
    pub fn charset(&self) -> Option<Charset> {
        self.collation().map(Collation::charset)
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
            Self::String(value) => label_bytes("STR", value.bytes()),
            Self::Bytes(value) => label_bytes("STR", value),
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
            Self::String(value) => decode_bytes(value.bytes()),
            Self::Bytes(value) => decode_bytes(value),
            Self::Null => Ok(String::new()),
            Self::MinNotNull => Err(DatumStringError::RangeSentinel(DatumKind::MinNotNull)),
            Self::MaxValue => Err(DatumStringError::RangeSentinel(DatumKind::MaxValue)),
        }
    }
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
        | Datum::String(_)
        | Datum::Bytes(_) => None,
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
    use super::{Datum, DatumKind};
    use crate::{Charset, Collation, Decimal};

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
}
