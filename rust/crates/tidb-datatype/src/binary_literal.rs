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

use tidb_error::mysql::{errcode, FormatArg, SqlError};
use tidb_error::terror::{TerrorClass, TerrorCode, TerrorError};
use tidb_error::tidb::errname;

use crate::{ConversionContext, TruncationPolicy};

/// TiDB's byte-preserving internal representation for bit and hexadecimal
/// literals (`pkg/types/binary_literal.go::BinaryLiteral`).
///
/// Leading zero bytes are significant for rendering and raw-string access.
/// Numeric conversion and comparison ignore them, exactly as the Go source
/// does.
#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub struct BinaryLiteral(Vec<u8>);

impl BinaryLiteral {
    /// The zero-length literal used by empty bit and hexadecimal syntax.
    pub const ZERO: Self = Self(Vec::new());

    /// Creates a literal from a big-endian unsigned value.
    ///
    /// `None` is Go's `byteSize == -1`: redundant leading zero bytes are
    /// removed, while numeric zero remains one zero byte. A typed width keeps
    /// Go's panic-only invalid sizes out of this operation entirely.
    pub fn from_uint(value: u64, width: Option<BinaryLiteralWidth>) -> Self {
        let encoded = value.to_be_bytes();
        match width {
            Some(width) => Self(encoded[encoded.len() - usize::from(width.get())..].to_vec()),
            None => Self(trim_leading_zero_bytes(&encoded).to_vec()),
        }
    }

    /// Returns the literal's unchanged bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the literal and returns its unchanged bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Go `ToString` returns an arbitrary-byte string. Rust strings require
    /// UTF-8, so this is the lossless equivalent at the same representation
    /// boundary.
    pub fn to_raw_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    /// Returns Go `ToBitLiteralString`'s SQL bit-literal representation.
    pub fn to_bit_literal_string(&self, trim_leading_zero: bool) -> String {
        if self.0.is_empty() {
            return "b''".to_owned();
        }

        let mut bits = String::with_capacity(self.0.len() * 8);
        for byte in &self.0 {
            use fmt::Write;
            write!(bits, "{byte:08b}").expect("writing to String cannot fail");
        }
        let bits = if trim_leading_zero {
            let trimmed = bits.trim_start_matches('0');
            if trimmed.is_empty() {
                "0"
            } else {
                trimmed
            }
        } else {
            &bits
        };
        format!("b'{bits}'")
    }

    /// Converts the big-endian literal to an unsigned integer before any
    /// statement-context warning/error policy is applied.
    ///
    /// Go returns `math.MaxUint64` together with `ErrTruncatedWrongVal` for a
    /// non-zero payload wider than eight bytes. The typed outcome preserves
    /// both pieces without inventing a warning sink in the datatype layer.
    pub fn to_int(&self) -> BinaryLiteralIntOutcome {
        let bytes = trim_leading_zero_bytes(&self.0);
        if bytes.len() > 8 {
            return BinaryLiteralIntOutcome::Truncated { value: u64::MAX };
        }
        let value = bytes
            .iter()
            .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte));
        BinaryLiteralIntOutcome::Exact(value)
    }

    /// Go `BinaryLiteral.ToInt(ctx)`, routed through the shared truncation
    /// policy while preserving Go's `(value, error)` pair.
    pub fn to_int_with_policy(
        &self,
        policy: TruncationPolicy,
        append_warning: impl FnMut(SqlError),
    ) -> (u64, Option<SqlError>) {
        let outcome = self.to_int();
        let error = outcome
            .is_truncated()
            .then(|| self.truncated_wrong_value_error().to_sql_error());
        (outcome.value(), policy.handle(error, append_warning))
    }

    /// Source `BinaryLiteral.ToInt(ctx)` through the authoritative conversion
    /// context and shared ClassTypes terror identity.
    pub fn to_int_with_context(
        &self,
        context: &ConversionContext<'_>,
    ) -> (u64, Option<TerrorError>) {
        let outcome = self.to_int();
        let error = outcome
            .is_truncated()
            .then(|| self.truncated_wrong_value_error());
        (outcome.value(), context.handle_truncate(error))
    }

    fn truncated_wrong_value_error(&self) -> TerrorError {
        TerrorError::registered_standard(
            TerrorClass::Types,
            TerrorCode::new(
                isize::try_from(errcode::ErrTruncatedWrongValue)
                    .expect("MySQL error code must fit the source int domain"),
            ),
            errname::ErrTruncatedWrongValue,
        )
        .fast_generate(
            errname::ErrTruncatedWrongValue.raw,
            &[FormatArg::from("BINARY"), FormatArg::from(self.to_string())],
        )
    }

    /// Compares two literals as unsigned big-endian integers after removing
    /// redundant leading zero bytes.
    pub fn compare(&self, other: &Self) -> Ordering {
        let left = trim_leading_zero_bytes(&self.0);
        let right = trim_leading_zero_bytes(&other.0);
        left.len().cmp(&right.len()).then_with(|| left.cmp(right))
    }
}

impl From<Vec<u8>> for BinaryLiteral {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for BinaryLiteral {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for BinaryLiteral {
    fn from(bytes: &[u8; N]) -> Self {
        Self(bytes.to_vec())
    }
}

impl AsRef<[u8]> for BinaryLiteral {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for BinaryLiteral {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        formatter.write_str("0x")?;
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A bit literal parsed from `b'...'`, `B'...'`, or `0b...` syntax.
#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub struct BitLiteral(BinaryLiteral);

impl BitLiteral {
    /// Parses the complete Go `NewBitLiteral` syntax and validation boundary.
    pub fn parse(input: &str) -> Result<Self, BinaryLiteralParseError> {
        parse_bit_str(input).map(Self)
    }

    /// Returns the raw literal bytes (Go `BitLiteral.ToString`).
    pub fn to_raw_bytes(&self) -> &[u8] {
        self.0.to_raw_bytes()
    }

    /// Borrows the shared binary-literal representation.
    pub const fn as_binary_literal(&self) -> &BinaryLiteral {
        &self.0
    }
}

/// A hexadecimal literal parsed from `x'...'`, `X'...'`, or `0x...` syntax.
#[derive(Debug, Clone, Default, Eq, Hash, PartialEq)]
pub struct HexLiteral(BinaryLiteral);

impl HexLiteral {
    /// Parses the complete Go `NewHexLiteral` syntax and validation boundary.
    pub fn parse(input: &str) -> Result<Self, BinaryLiteralParseError> {
        parse_hex_str(input).map(Self)
    }

    /// Returns the raw literal bytes (Go `HexLiteral.ToString`).
    pub fn to_raw_bytes(&self) -> &[u8] {
        self.0.to_raw_bytes()
    }

    /// Borrows the shared binary-literal representation.
    pub const fn as_binary_literal(&self) -> &BinaryLiteral {
        &self.0
    }
}

/// The result of converting a literal before statement-context error policy.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum BinaryLiteralIntOutcome {
    /// Conversion was exact.
    Exact(u64),
    /// Significant bytes exceeded the unsigned 64-bit boundary.
    Truncated {
        /// Go's returned value (`math.MaxUint64`).
        value: u64,
    },
}

impl BinaryLiteralIntOutcome {
    /// Returns the value produced alongside the exact/truncated disposition.
    pub const fn value(self) -> u64 {
        match self {
            Self::Exact(value) | Self::Truncated { value } => value,
        }
    }

    /// Returns whether Go would pass `ErrTruncatedWrongVal` to its context.
    pub const fn is_truncated(self) -> bool {
        matches!(self, Self::Truncated { .. })
    }
}

/// A fixed output width accepted by Go `NewBinaryLiteralFromUint`.
///
/// The private field guarantees the value is in `1..=8`, eliminating the
/// source panic state from the constructor that consumes this type.
#[derive(Debug, Clone, Copy, Eq, Hash, PartialEq)]
pub struct BinaryLiteralWidth(u8);

impl BinaryLiteralWidth {
    /// Returns the validated byte width.
    pub const fn get(self) -> u8 {
        self.0
    }

    fn try_new(byte_size: i16) -> Result<Self, InvalidBinaryLiteralWidth> {
        match byte_size {
            1..=8 => Ok(Self(byte_size as u8)),
            _ => Err(InvalidBinaryLiteralWidth::new(byte_size)),
        }
    }
}

impl TryFrom<u8> for BinaryLiteralWidth {
    type Error = InvalidBinaryLiteralWidth;

    fn try_from(byte_size: u8) -> Result<Self, Self::Error> {
        Self::try_new(i16::from(byte_size))
    }
}

impl TryFrom<i8> for BinaryLiteralWidth {
    type Error = InvalidBinaryLiteralWidth;

    fn try_from(byte_size: i8) -> Result<Self, Self::Error> {
        Self::try_new(i16::from(byte_size))
    }
}

/// An invalid fixed byte width. Go panics on this input; Rust rejects it
/// before a literal-construction call can be formed.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct InvalidBinaryLiteralWidth {
    byte_size: i16,
}

impl InvalidBinaryLiteralWidth {
    /// Creates the error used by width conversion.
    pub const fn new(byte_size: i16) -> Self {
        Self { byte_size }
    }

    /// Returns the rejected source byte size.
    pub const fn byte_size(self) -> i16 {
        self.byte_size
    }
}

impl fmt::Display for InvalidBinaryLiteralWidth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid binary literal byte size {}",
            self.byte_size
        )
    }
}

impl std::error::Error for InvalidBinaryLiteralWidth {}

/// A bit/hex parsing failure at the same boundary as the Go parser helpers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum BinaryLiteralParseError {
    /// The bit parser received an empty input string.
    EmptyBit,
    /// The input used no supported bit-literal prefix.
    InvalidBitFormat(String),
    /// The bit payload contained a byte other than `0` or `1`.
    InvalidBitDigit(u8),
    /// The hexadecimal parser received an empty input string.
    EmptyHex,
    /// Quoted hexadecimal syntax contained an odd number of digits.
    OddQuotedHexDigits(usize),
    /// The input used no supported hexadecimal-literal prefix.
    InvalidHexFormat(String),
    /// The hexadecimal payload contained a non-hexadecimal byte.
    InvalidHexDigit(u8),
}

impl fmt::Display for BinaryLiteralParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBit => formatter.write_str("invalid empty string for parsing bit type"),
            Self::InvalidBitFormat(input) => write!(formatter, "invalid bit type format {input}"),
            Self::InvalidBitDigit(byte) => {
                write!(formatter, "invalid bit digit 0x{byte:02x}")
            }
            Self::EmptyHex => {
                formatter.write_str("invalid empty string for parsing hexadecimal literal")
            }
            Self::OddQuotedHexDigits(digits) => write!(
                formatter,
                "invalid hexadecimal format, must even numbers, but {digits}"
            ),
            Self::InvalidHexFormat(input) => {
                write!(formatter, "invalid hexadecimal format {input}")
            }
            Self::InvalidHexDigit(byte) => {
                write!(formatter, "invalid hexadecimal digit 0x{byte:02x}")
            }
        }
    }
}

impl std::error::Error for BinaryLiteralParseError {}

/// Parses `b'...'`, `B'...'`, or lower-case-prefix `0b...` syntax.
pub fn parse_bit_str(input: &str) -> Result<BinaryLiteral, BinaryLiteralParseError> {
    let bytes = input.as_bytes();
    let Some(first) = bytes.first() else {
        return Err(BinaryLiteralParseError::EmptyBit);
    };
    let payload = if matches!(*first, b'b' | b'B') {
        trim_apostrophes(&bytes[1..])
    } else if bytes.starts_with(b"0b") {
        &bytes[2..]
    } else {
        return Err(BinaryLiteralParseError::InvalidBitFormat(input.to_owned()));
    };
    if payload.is_empty() {
        return Ok(BinaryLiteral::ZERO);
    }

    let aligned_len = (payload.len() + 7) & !7;
    let padding = aligned_len - payload.len();
    let mut output = vec![0_u8; aligned_len / 8];
    for (index, byte) in payload.iter().copied().enumerate() {
        let bit = match byte {
            b'0' => 0,
            b'1' => 1,
            _ => return Err(BinaryLiteralParseError::InvalidBitDigit(byte)),
        };
        let aligned_index = padding + index;
        output[aligned_index / 8] |= bit << (7 - aligned_index % 8);
    }
    Ok(BinaryLiteral(output))
}

/// Parses `x'...'`, `X'...'`, or lower-case-prefix `0x...` syntax.
pub fn parse_hex_str(input: &str) -> Result<BinaryLiteral, BinaryLiteralParseError> {
    let bytes = input.as_bytes();
    let Some(first) = bytes.first() else {
        return Err(BinaryLiteralParseError::EmptyHex);
    };
    let quoted = matches!(*first, b'x' | b'X');
    let payload = if quoted {
        trim_apostrophes(&bytes[1..])
    } else if bytes.starts_with(b"0x") {
        &bytes[2..]
    } else {
        return Err(BinaryLiteralParseError::InvalidHexFormat(input.to_owned()));
    };
    if quoted && payload.len() % 2 != 0 {
        return Err(BinaryLiteralParseError::OddQuotedHexDigits(payload.len()));
    }
    if payload.is_empty() {
        return Ok(BinaryLiteral::ZERO);
    }

    let mut output = Vec::with_capacity(payload.len().div_ceil(2));
    let mut index = 0;
    if payload.len() % 2 != 0 {
        output.push(hex_nibble(payload[0])?);
        index = 1;
    }
    while index < payload.len() {
        output.push((hex_nibble(payload[index])? << 4) | hex_nibble(payload[index + 1])?);
        index += 2;
    }
    Ok(BinaryLiteral(output))
}

fn hex_nibble(byte: u8) -> Result<u8, BinaryLiteralParseError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(BinaryLiteralParseError::InvalidHexDigit(byte)),
    }
}

fn trim_apostrophes(mut bytes: &[u8]) -> &[u8] {
    while bytes.first() == Some(&b'\'') {
        bytes = &bytes[1..];
    }
    while bytes.last() == Some(&b'\'') {
        bytes = &bytes[..bytes.len() - 1];
    }
    bytes
}

fn trim_leading_zero_bytes(bytes: &[u8]) -> &[u8] {
    if bytes.is_empty() {
        return bytes;
    }
    let first_non_zero = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len() - 1);
    &bytes[first_non_zero..]
}

#[cfg(test)]
#[path = "binary_literal_tests.rs"]
mod tests;
