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

use tidb_goruntime::go_hex;
use tidb_mysql::errors::{errcode, FormatArg, SqlError};
use tidb_terror::{TerrorClass, TerrorCode, TerrorError};
use tidb_errno::errname;

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

    /// Source `Datum.GetBinaryLiteral4Cmp`: the bytes a comparison sees.
    ///
    /// A `BIT(n)` payload is stored zero-padded to its declared width, so the
    /// stored bytes of `b'1'` differ by width while the value does not.
    /// Comparison therefore drops redundant leading zero bytes -- all-zero
    /// keeps one, so a zero literal stays a byte string rather than becoming
    /// the empty one.
    pub fn get_binary_literal4_cmp(&self) -> &[u8] {
        trim_leading_zero_bytes(&self.0)
    }

    /// As [`Self::compare_bytes`], for a payload Go reaches through a
    /// `KindString`/`KindBytes` datum's own buffer rather than a literal.
    pub fn compare_bytes_of(bytes: &[u8]) -> &[u8] {
        trim_leading_zero_bytes(bytes)
    }

    /// Go `ToString` returns an arbitrary-byte string. Rust strings require
    /// UTF-8, so this is the lossless equivalent at the same representation
    /// boundary.
    pub fn to_raw_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    /// Returns the hexadecimal form for callers that need owned bytes.
    pub(crate) fn to_hex_bytes(&self) -> Vec<u8> {
        if self.0.is_empty() {
            return Vec::new();
        }
        let mut encoded = vec![0; 2 + self.0.len() * 2];
        encoded[..2].copy_from_slice(b"0x");
        go_hex::encode(&mut encoded[2..], self.as_bytes());
        encoded
    }

    /// Returns Go `ToBitLiteralString`'s SQL bit-literal representation.
    pub fn to_bit_literal_string(&self, trim_leading_zero: bool) -> String {
        let bytes = if trim_leading_zero {
            trim_leading_zero_bytes(&self.0)
        } else {
            &self.0
        };
        let Some((&first, rest)) = bytes.split_first() else {
            return "b''".to_owned();
        };
        let skip = if trim_leading_zero {
            first.leading_zeros().min(7) as usize
        } else {
            0
        };
        let mut output = String::with_capacity(3 + bytes.len() * 8 - skip);
        output.push_str("b'");
        output.push_str(&BIT_TEXT[usize::from(first)][skip..]);
        for &byte in rest {
            output.push_str(BIT_TEXT[usize::from(byte)]);
        }
        output.push('\'');
        output
    }

    /// Converts the big-endian literal to an unsigned integer before any
    /// statement-context warning/error policy is applied.
    ///
    /// Go returns `math.MaxUint64` together with `ErrTruncatedWrongVal` for a
    /// non-zero payload wider than eight bytes. The typed outcome preserves
    /// both pieces without inventing a warning sink in the datatype layer.
    pub fn to_int(&self) -> BinaryLiteralIntOutcome {
        let bytes = if self.0.len() <= 8 {
            self.as_bytes()
        } else {
            trim_leading_zero_bytes(&self.0)
        };
        if bytes.len() > 8 {
            return BinaryLiteralIntOutcome::Truncated { value: u64::MAX };
        }
        let value = match bytes.first_chunk::<8>() {
            Some(word) => u64::from_be_bytes(*word),
            None => bytes
                .iter()
                .fold(0_u64, |value, byte| (value << 8) | u64::from(*byte)),
        };
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
        .fast_gen(
            errname::ErrTruncatedWrongValue.raw,
            &[FormatArg::from("BINARY"), FormatArg::from(self.to_string())],
        )
    }

    /// Compares two literals as unsigned big-endian integers after removing
    /// redundant leading zero bytes.
    #[inline]
    pub fn compare(&self, other: &Self) -> Ordering {
        Self::compare_bytes(&self.0, &other.0)
    }

    /// Compares borrowed binary literals with the same unsigned ordering.
    pub fn compare_bytes(left: &[u8], right: &[u8]) -> Ordering {
        if left.len() == right.len() {
            if left.is_empty() {
                return Ordering::Equal;
            }
            return left.cmp(right);
        }
        let left = trim_leading_zero_bytes(left);
        let right = trim_leading_zero_bytes(right);
        left.len().cmp(&right.len()).then_with(|| left.cmp(right))
    }
}

static BIT_DIGITS: [[u8; 8]; 256] = {
    let mut digits = [[b'0'; 8]; 256];
    let mut byte = 0;
    while byte < digits.len() {
        let mut bit = 0;
        while bit < 8 {
            digits[byte][bit] += ((byte >> (7 - bit)) & 1) as u8;
            bit += 1;
        }
        byte += 1;
    }
    digits
};

static BIT_TEXT: [&str; 256] = {
    let mut text = [""; 256];
    let mut byte = 0;
    while byte < text.len() {
        text[byte] = match std::str::from_utf8(&BIT_DIGITS[byte]) {
            Ok(text) => text,
            Err(_) => panic!("bit digits are ASCII"),
        };
        byte += 1;
    }
    text
};

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

static HEX_BYTE: [[u8; 4]; 256] = {
    let digits = b"0123456789abcdef";
    let mut bytes = [[b'0', b'x', 0, 0]; 256];
    let mut i = 0;
    while i < bytes.len() {
        bytes[i][2] = digits[i >> 4];
        bytes[i][3] = digits[i & 15];
        i += 1;
    }
    bytes
};

static HEX_BYTE_TEXT: [&str; 256] = {
    let mut values = [""; 256];
    let mut i = 0;
    while i < values.len() {
        values[i] = match std::str::from_utf8(&HEX_BYTE[i]) {
            Ok(text) => text,
            Err(_) => panic!("hex digits are ASCII"),
        };
        i += 1;
    }
    values
};

impl fmt::Display for BinaryLiteral {
    #[inline]
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_bytes() {
            [] => Ok(()),
            [byte] => formatter.write_str(HEX_BYTE_TEXT[*byte as usize]),
            bytes if bytes.len() <= 32 => format_small_hex(bytes, formatter),
            bytes => format_hex_chunks(bytes, formatter),
        }
    }
}

// Keep the empty and one-byte paths small enough to inline into callers.
#[inline(never)]
fn format_small_hex(bytes: &[u8], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    if let [_, _, _, _, _, _, _, _] = bytes {
        let mut encoded = [0; 18];
        encoded[..2].copy_from_slice(b"0x");
        go_hex::encode(&mut encoded[2..], bytes);
        return formatter.write_str(std::str::from_utf8(&encoded).expect("hex digits are ASCII"));
    }

    let mut encoded = [0; 66];
    encoded[..2].copy_from_slice(b"0x");
    let end = 2 + bytes.len() * 2;
    for (out, &byte) in encoded[2..end].as_chunks_mut::<2>().0.iter_mut().zip(bytes) {
        let [_, _, high, low] = HEX_BYTE[byte as usize];
        *out = [high, low];
    }
    formatter.write_str(std::str::from_utf8(&encoded[..end]).expect("hex digits are ASCII"))
}

fn format_hex_chunks(bytes: &[u8], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    const CHUNK_SIZE: usize = 512;
    let mut encoded = [0; 2 + CHUNK_SIZE * 2];
    encoded[..2].copy_from_slice(b"0x");
    let mut start = 0;
    for chunk in bytes.chunks(CHUNK_SIZE) {
        let end = 2 + go_hex::encode(&mut encoded[2..], chunk);
        let text = std::str::from_utf8(&encoded[start..end]).expect("hex digits are ASCII");
        formatter.write_str(text)?;
        start = 2;
    }
    Ok(())
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

    let (prefix, aligned) = payload.split_at(payload.len() % 8);
    let mut output = Vec::with_capacity(payload.len().div_ceil(8));
    if !prefix.is_empty() {
        output.push(parse_bit_digits(prefix)?);
    }
    const LOW_BITS: u64 = 0x0101_0101_0101_0101;
    const ASCII_ZEROES: u64 = u64::from_le_bytes([b'0'; 8]);
    for digits in aligned.as_chunks::<8>().0 {
        let word = u64::from_le_bytes(*digits);
        let invalid_bits = (word & !LOW_BITS) ^ ASCII_ZEROES;
        if invalid_bits != 0 {
            let first_invalid = invalid_bits.trailing_zeros() as usize / 8;
            return Err(BinaryLiteralParseError::InvalidBitDigit(digits[first_invalid]));
        }
        // Multiplying gathers each byte's low bit into the high byte,
        // with the first input digit becoming the most significant bit.
        let byte = ((word & LOW_BITS).wrapping_mul(0x8040_2010_0804_0201) >> 56) as u8;
        output.push(byte);
    }
    Ok(BinaryLiteral(output))
}

fn parse_bit_digits(digits: &[u8]) -> Result<u8, BinaryLiteralParseError> {
    let mut output = 0;
    for &digit in digits {
        match digit {
            b'0' | b'1' => output = (output << 1) | (digit - b'0'),
            _ => return Err(BinaryLiteralParseError::InvalidBitDigit(digit)),
        }
    }
    Ok(output)
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

    let (head, pairs) = payload.as_rchunks::<2>();
    let mut output = vec![0; payload.len().div_ceil(2)];
    if let [digit] = *head {
        go_hex::decode_pairs(&mut output[..1], &[[b'0', digit]])
            .map_err(BinaryLiteralParseError::InvalidHexDigit)?;
    }
    go_hex::decode_pairs(&mut output[head.len()..], pairs)
        .map_err(BinaryLiteralParseError::InvalidHexDigit)?;
    Ok(BinaryLiteral(output))
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
    if bytes.first().is_none_or(|&byte| byte != 0) {
        return bytes;
    }

    let mut remaining = bytes;
    while let Some((word, tail)) = remaining.split_first_chunk::<8>() {
        let word = u64::from_be_bytes(*word);
        if word != 0 {
            return &remaining[word.leading_zeros() as usize / 8..];
        }
        remaining = tail;
    }
    match remaining.iter().position(|&byte| byte != 0) {
        Some(first) => &remaining[first..],
        None => &bytes[bytes.len() - 1..],
    }
}

#[cfg(test)]
#[path = "binary_literal_tests.rs"]
mod tests;
