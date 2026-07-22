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

//! Complete encoding dispatch from `pkg/parser/charset/encoding*.go`.

use std::fmt;

use encoding_rs::{EncoderResult, GB18030, GBK};

use crate::ascii_encoding::ASCII_ENCODING;
use crate::charset::{CaseRange, GB18030_BY_BYTES, GB18030_BY_RUNE, GB18030_CASES, GBK_CASES};
use crate::encoding_base::{TransformOp, TransformPolicy, TransformResult};
use crate::utf8_encoding::{UTF8_ENCODING, UTF8_MB3_STRICT_ENCODING};

/// Source `EncodingTp` values.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[repr(i8)]
pub enum EncodingType {
    /// No encoding.
    #[default]
    None = 0,
    /// Four-byte UTF-8.
    Utf8,
    /// Strict three-byte UTF-8.
    Utf8Mb3Strict,
    /// ASCII.
    Ascii,
    /// TiDB-compatible Latin-1.
    Latin1,
    /// Binary.
    Binary,
    /// GBK.
    Gbk,
    /// GB18030-2022.
    Gb18030,
}

/// A stateless supported encoding implementation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    /// Four-byte UTF-8.
    Utf8,
    /// Strict three-byte UTF-8.
    Utf8Mb3Strict,
    /// ASCII.
    Ascii,
    /// Byte-preserving Latin-1 compatibility behavior.
    Latin1,
    /// Byte-preserving binary behavior.
    Binary,
    /// GBK.
    Gbk,
    /// GB18030-2022.
    Gb18030,
}

/// The first invalid group returned with transformed bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingError {
    charset: &'static str,
    invalid: Vec<u8>,
}

impl EncodingError {
    /// Returns the canonical charset name.
    pub const fn charset(&self) -> &'static str {
        self.charset
    }

    /// Returns the exact invalid source group.
    pub fn invalid_bytes(&self) -> &[u8] {
        &self.invalid
    }
}

impl fmt::Display for EncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Invalid {} character string: '", self.charset)?;
        for byte in &self.invalid {
            write!(formatter, "{byte:02X}")?;
        }
        formatter.write_str("'")
    }
}

impl std::error::Error for EncodingError {}

/// Bytes and the optional first source conversion error.
pub type EncodingResult = TransformResult<EncodingError>;

impl Encoding {
    /// Returns the source registry name.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Utf8 | Self::Utf8Mb3Strict => "utf8mb4",
            Self::Ascii => "ascii",
            Self::Latin1 => "latin1",
            Self::Binary => "binary",
            Self::Gbk => "gbk",
            Self::Gb18030 => "gb18030",
        }
    }

    /// Returns the source encoding type.
    pub const fn encoding_type(self) -> EncodingType {
        match self {
            Self::Utf8 => EncodingType::Utf8,
            Self::Utf8Mb3Strict => EncodingType::Utf8Mb3Strict,
            Self::Ascii => EncodingType::Ascii,
            Self::Latin1 => EncodingType::Latin1,
            Self::Binary => EncodingType::Binary,
            Self::Gbk => EncodingType::Gbk,
            Self::Gb18030 => EncodingType::Gb18030,
        }
    }

    /// Returns the next encoded character group.
    pub fn peek(self, source: &[u8]) -> &[u8] {
        match self {
            Self::Utf8 | Self::Utf8Mb3Strict => UTF8_ENCODING.peek(source),
            Self::Ascii | Self::Latin1 | Self::Binary => source.get(..1).unwrap_or(source),
            Self::Gbk => source
                .get(
                    ..source
                        .len()
                        .min(if source.first().is_some_and(|b| *b >= 0x80) {
                            2
                        } else {
                            1
                        }),
                )
                .unwrap_or(source),
            Self::Gb18030 => peek_gb18030(source),
        }
    }

    /// Returns a multibyte width, or zero for single-byte/invalid input.
    pub fn mb_len(self, source: &[u8]) -> usize {
        match self {
            Self::Utf8 | Self::Utf8Mb3Strict => UTF8_ENCODING.mb_len(source),
            Self::Gbk => gbk_mb_len(source),
            Self::Gb18030 => gb18030_mb_len(source),
            Self::Ascii | Self::Latin1 | Self::Binary => 0,
        }
    }

    /// Checks whether UTF-8 input can be represented by this encoding.
    pub fn is_valid(self, source: &[u8]) -> bool {
        match self {
            Self::Utf8 => UTF8_ENCODING.is_valid(source),
            Self::Utf8Mb3Strict => UTF8_MB3_STRICT_ENCODING.is_valid(source),
            Self::Ascii => ASCII_ENCODING.is_valid(source),
            Self::Latin1 | Self::Binary => true,
            Self::Gbk | Self::Gb18030 => {
                let mut valid = true;
                self.foreach(source, TransformOp::FROM_UTF8, |_, _, ok| {
                    valid = ok;
                    ok
                });
                valid
            }
        }
    }

    /// Visits source groups in order.
    pub fn foreach<F>(self, source: &[u8], operation: TransformOp, mut visit: F)
    where
        F: FnMut(&[u8], &[u8], bool) -> bool,
    {
        match self {
            Self::Utf8 => UTF8_ENCODING.foreach(source, visit),
            Self::Utf8Mb3Strict => UTF8_MB3_STRICT_ENCODING.foreach(source, visit),
            Self::Ascii => ASCII_ENCODING.foreach(source, visit),
            Self::Latin1 | Self::Binary => {
                for byte in source {
                    let group = std::slice::from_ref(byte);
                    if !visit(group, group, true) {
                        break;
                    }
                }
            }
            Self::Gbk | Self::Gb18030 => {
                let from_utf8 = operation.contains(TransformOp::FROM_UTF8);
                let mut offset = 0;
                while offset < source.len() {
                    let width = if from_utf8 {
                        UTF8_ENCODING.peek(&source[offset..]).len()
                    } else {
                        self.peek(&source[offset..]).len()
                    };
                    let width = width.max(1).min(source.len() - offset);
                    let group = &source[offset..offset + width];
                    let converted = if from_utf8 {
                        encode_group(self, group)
                    } else {
                        decode_group(self, group)
                    };
                    let (bytes, valid) = converted
                        .map(|bytes| (bytes, true))
                        .unwrap_or_else(|| (b"\xEF\xBF\xBD".to_vec(), false));
                    if !visit(group, &bytes, valid) {
                        break;
                    }
                    offset += width;
                }
            }
        }
    }

    /// Applies the source transform policy.
    pub fn transform(self, source: &[u8], operation: TransformOp) -> EncodingResult {
        match self {
            Self::Latin1 | Self::Binary => TransformResult::new(source.to_vec(), None),
            _ => {
                let mut policy =
                    TransformPolicy::new(source.len(), operation, |invalid| EncodingError {
                        charset: self.name(),
                        invalid: invalid.to_vec(),
                    });
                self.foreach(source, operation, |from, to, valid| {
                    policy.push(from, to, valid)
                });
                policy.finish()
            }
        }
    }

    /// Applies source-compatible upper-case mapping.
    pub fn to_upper(self, source: &str) -> String {
        match self {
            Self::Gbk => map_case(source, GBK_CASES, Case::Upper),
            Self::Gb18030 => map_case(source, GB18030_CASES, Case::Upper),
            _ => source.to_uppercase(),
        }
    }

    /// Applies source-compatible lower-case mapping.
    pub fn to_lower(self, source: &str) -> String {
        match self {
            Self::Gbk => map_case(source, GBK_CASES, Case::Lower),
            Self::Gb18030 => map_case(source, GB18030_CASES, Case::Lower),
            _ => source.to_lowercase(),
        }
    }
}

/// Checks whether a name has a complete encoding implementation.
pub fn is_supported_encoding(charset: &str) -> bool {
    matches!(
        charset,
        "utf8mb4" | "utf8" | "gbk" | "latin1" | "binary" | "ascii" | "gb18030"
    )
}

/// Finds an encoding; empty and unknown names use binary, exactly as Go does.
pub fn find_encoding(charset: &str) -> Encoding {
    match charset {
        "utf8mb4" | "utf8" => Encoding::Utf8,
        "gbk" => Encoding::Gbk,
        "latin1" => Encoding::Latin1,
        "binary" | "" => Encoding::Binary,
        "ascii" => Encoding::Ascii,
        "gb18030" => Encoding::Gb18030,
        _ => Encoding::Binary,
    }
}

/// Finds an encoding while treating UTF-8 as byte-preserving binary.
pub fn find_encoding_take_utf8_as_noop(charset: &str) -> Encoding {
    let encoding = find_encoding(charset);
    if encoding.encoding_type() == EncodingType::Utf8 {
        Encoding::Binary
    } else {
        encoding
    }
}

/// Counts the valid UTF-8 prefix representable by an encoding.
pub fn count_valid_bytes(encoding: Encoding, source: &[u8]) -> usize {
    count_valid(encoding, source, TransformOp::FROM_UTF8)
}

/// Counts the valid encoded prefix decodable to UTF-8.
pub fn count_valid_bytes_decode(encoding: Encoding, source: &[u8]) -> usize {
    count_valid(encoding, source, TransformOp::TO_UTF8)
}

fn count_valid(encoding: Encoding, source: &[u8], operation: TransformOp) -> usize {
    let mut count = 0;
    encoding.foreach(source, operation, |from, _, valid| {
        if valid {
            count += from.len();
        }
        valid
    });
    count
}

fn encode_group(encoding: Encoding, source: &[u8]) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(source).ok()?;
    if text.chars().count() != 1 {
        return None;
    }
    let character = text.chars().next()?;
    if encoding == Encoding::Gbk && character == '€' {
        return None;
    }
    if encoding == Encoding::Gb18030 {
        if let Ok(index) = GB18030_BY_RUNE.binary_search_by_key(&character, |row| row.0) {
            return Some(integer_bytes(GB18030_BY_RUNE[index].1));
        }
    }
    let codec = if encoding == Encoding::Gbk {
        GBK
    } else {
        GB18030
    };
    let mut output = [0_u8; 8];
    let (result, read, written) =
        codec
            .new_encoder()
            .encode_from_utf8_without_replacement(text, &mut output, true);
    if result == EncoderResult::InputEmpty && read == source.len() {
        Some(output[..written].to_vec())
    } else {
        None
    }
}

fn decode_group(encoding: Encoding, source: &[u8]) -> Option<Vec<u8>> {
    if source.first() == Some(&0x80) {
        return None;
    }
    if encoding == Encoding::Gb18030 {
        let encoded = bytes_integer(source);
        if let Ok(index) = GB18030_BY_BYTES.binary_search_by_key(&encoded, |row| row.0) {
            let mut buffer = [0_u8; 4];
            return Some(
                GB18030_BY_BYTES[index]
                    .1
                    .encode_utf8(&mut buffer)
                    .as_bytes()
                    .to_vec(),
            );
        }
        if source == [0x84, 0x31, 0xA4, 0x37] {
            return Some(b"\xEF\xBF\xBD".to_vec());
        }
    }
    let codec = if encoding == Encoding::Gbk {
        GBK
    } else {
        GB18030
    };
    let decoded = codec
        .decode_without_bom_handling_and_without_replacement(source)
        .map(|text| text.into_owned())?;
    // WHATWG GBK exposes pointer values as private-use characters where
    // Go's x/text GBK decoder reports an invalid sequence. TiDB follows the
    // latter and replaces the whole source group.
    if encoding == Encoding::Gbk
        && decoded
            .chars()
            .any(|character| ('\u{E000}'..='\u{F8FF}').contains(&character))
    {
        None
    } else {
        Some(decoded.into_bytes())
    }
}

fn peek_gb18030(source: &[u8]) -> &[u8] {
    let Some(&first) = source.first() else {
        return source;
    };
    if first == 0x80 || first == 0xFF || first <= 0x7F {
        return &source[..1];
    }
    if !(0x81..=0xFE).contains(&first) || source.len() < 2 {
        return &source[..1];
    }
    let second = source[1];
    if (0x40..0x7F).contains(&second) || (0x80..=0xFE).contains(&second) {
        return &source[..2];
    }
    if source.len() >= 4
        && (0x30..=0x39).contains(&second)
        && (0x81..=0xFE).contains(&source[2])
        && (0x30..=0x39).contains(&source[3])
    {
        return &source[..4];
    }
    &source[..1]
}

fn gbk_mb_len(source: &[u8]) -> usize {
    if source.len() >= 2
        && (0x81..=0xFE).contains(&source[0])
        && ((0x40..=0x7E).contains(&source[1]) || (0x80..=0xFE).contains(&source[1]))
    {
        2
    } else {
        0
    }
}

fn gb18030_mb_len(source: &[u8]) -> usize {
    if gbk_mb_len(source) == 2 {
        return 2;
    }
    if source.len() >= 4
        && (0x81..=0xFE).contains(&source[0])
        && (0x30..=0x39).contains(&source[1])
        && (0x81..=0xFE).contains(&source[2])
        && (0x30..=0x39).contains(&source[3])
    {
        4
    } else {
        0
    }
}

fn bytes_integer(bytes: &[u8]) -> u32 {
    bytes
        .iter()
        .fold(0, |value, byte| (value << 8) | u32::from(*byte))
}

fn integer_bytes(value: u32) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let first = bytes.iter().position(|byte| *byte != 0).unwrap_or(3);
    bytes[first..].to_vec()
}

enum Case {
    Upper,
    Lower,
}

fn map_case(source: &str, cases: &[CaseRange], requested: Case) -> String {
    let mut output = String::with_capacity(source.len());
    for character in source.chars() {
        let codepoint = u32::from(character);
        if let Some(range) = cases
            .iter()
            .find(|range| range.lo <= codepoint && codepoint <= range.hi)
        {
            let delta = match requested {
                Case::Upper => range.upper,
                Case::Lower => range.lower,
            };
            output.push(
                char::from_u32(codepoint.wrapping_add_signed(delta))
                    .expect("source special-case delta is a Unicode scalar"),
            );
        } else {
            match requested {
                Case::Upper => output.extend(character.to_uppercase()),
                Case::Lower => output.extend(character.to_lowercase()),
            }
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_validation_rows() {
        for (charset, source, expected, valid_prefix, valid) in [
            ("ascii", b"qwerty".as_slice(), b"qwerty".as_slice(), 6, true),
            ("ascii", "qwÊrty".as_bytes(), b"qw?rty", 2, false),
            ("utf8", "😂".as_bytes(), "😂".as_bytes(), 4, true),
            ("gbk", "À".as_bytes(), b"?", 0, false),
            ("gb18030", "À".as_bytes(), "À".as_bytes(), 2, true),
            ("gb18030", "😂".as_bytes(), "😂".as_bytes(), 4, true),
        ] {
            let encoding = find_encoding(charset);
            assert_eq!(encoding.is_valid(source), valid, "{charset}");
            assert_eq!(
                encoding
                    .transform(source, TransformOp::REPLACE_NO_ERR)
                    .bytes(),
                expected,
                "{charset}"
            );
            assert_eq!(
                count_valid_bytes(encoding, source),
                valid_prefix,
                "{charset}"
            );
        }
        assert!(!Encoding::Utf8Mb3Strict.is_valid("😂".as_bytes()));
    }

    #[test]
    fn source_gbk_and_gb18030_vectors() {
        let gbk = Encoding::Gbk;
        let encoded = gbk.transform("一二三".as_bytes(), TransformOp::ENCODE_REPLACE);
        assert_eq!(encoded.bytes(), b"\xD2\xBB\xB6\xFE\xC8\xFD");
        assert!(encoded.error().is_none());
        let euro = gbk.transform("€a".as_bytes(), TransformOp::ENCODE_REPLACE);
        assert_eq!(euro.bytes(), b"?a");
        assert!(euro.error().is_some());

        let gb18030 = Encoding::Gb18030;
        for (text, expected) in [
            ("🀁", b"\x94\x38\xE1\x31".as_slice()),
            ("€", b"\xA2\xE3"),
            ("ḿ", b"\xA8\xBC"),
        ] {
            let result = gb18030.transform(text.as_bytes(), TransformOp::ENCODE_REPLACE);
            assert_eq!(result.bytes(), expected, "{text}");
            assert!(result.error().is_none());
        }
        let decoded = gb18030.transform(
            b"\xB0\xB2\x84\x31\xA4\x37\x30\x84\x31\xA4\x37\x32",
            TransformOp::DECODE_REPLACE,
        );
        assert_eq!(decoded.bytes(), "安�0�2".as_bytes());
        assert!(decoded.error().is_none());
    }
}
