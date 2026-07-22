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

//! HTML encoding labels from `pkg/parser/charset/encoding_table.go`.

use std::fmt;

use encoding_rs::{EncoderResult, GBK};

#[derive(Debug, Clone, Copy)]
struct EncodingLabel {
    label: &'static str,
    canonical: &'static str,
}

include!("encoding_labels.rs");

/// One of the encoding implementations referenced by the source label table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HtmlCodec {
    Nop,
    Replacement,
    Utf16Be,
    Utf16Le,
    HzGb2312,
    EncodingRs(&'static encoding_rs::Encoding),
}

/// A resolved HTML encoding label and its source-defined canonical name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HtmlEncoding {
    canonical_name: &'static str,
    codec: HtmlCodec,
}

impl HtmlEncoding {
    /// Returns the canonical name recorded by the Go package.
    pub const fn canonical_name(self) -> &'static str {
        self.canonical_name
    }

    /// Decodes bytes to UTF-8, replacing malformed input as the Go decoders do.
    pub fn decode(self, source: &[u8]) -> Vec<u8> {
        match self.codec {
            HtmlCodec::Nop => source.to_vec(),
            HtmlCodec::Replacement => "\u{fffd}".as_bytes().to_vec(),
            HtmlCodec::Utf16Be => decode_utf16(source, true),
            HtmlCodec::Utf16Le => decode_utf16(source, false),
            HtmlCodec::HzGb2312 => decode_hz_gb2312(source),
            HtmlCodec::EncodingRs(encoding) => {
                let (decoded, _) = encoding.decode_without_bom_handling(source);
                decoded.into_owned().into_bytes()
            }
        }
    }

    /// Encodes UTF-8 bytes, returning the first unrepresentable character.
    ///
    /// Like Go's `encoding.Encoder`, malformed UTF-8 is first converted to
    /// U+FFFD. The source `encoding.Nop` entry remains byte-preserving.
    pub fn encode(self, source: &[u8]) -> Result<Vec<u8>, HtmlEncodingError> {
        if self.codec == HtmlCodec::Nop {
            return Ok(source.to_vec());
        }
        let source = String::from_utf8_lossy(source);
        match self.codec {
            HtmlCodec::Nop => unreachable!(),
            HtmlCodec::Replacement => Ok(source.into_owned().into_bytes()),
            HtmlCodec::Utf16Be => Ok(encode_utf16(&source, true)),
            HtmlCodec::Utf16Le => Ok(encode_utf16(&source, false)),
            HtmlCodec::HzGb2312 => encode_hz_gb2312(&source),
            HtmlCodec::EncodingRs(encoding) => encode_with_encoding_rs(encoding, &source),
        }
    }
}

/// An HTML encoding encoder encountered an unrepresentable character.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HtmlEncodingError {
    character: char,
}

impl HtmlEncodingError {
    /// Returns the unrepresentable character.
    pub const fn character(self) -> char {
        self.character
    }
}

impl fmt::Display for HtmlEncodingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "character {:?} is not representable",
            self.character
        )
    }
}

impl std::error::Error for HtmlEncodingError {}

/// Resolves an HTML encoding label using the exact source alias table.
///
/// Matching is case-insensitive and ignores only the ASCII whitespace trimmed
/// by the Go implementation.
pub fn lookup_encoding(label: &str) -> Option<HtmlEncoding> {
    let normalized = label
        .trim_matches(['\t', '\n', '\r', '\u{c}', ' '])
        .to_ascii_lowercase();
    let row = ENCODING_LABELS.iter().find(|row| row.label == normalized)?;
    let codec = match row.canonical {
        "utf-8" | "binary" => HtmlCodec::Nop,
        "replacement" => HtmlCodec::Replacement,
        "utf-16be" => HtmlCodec::Utf16Be,
        "utf-16le" => HtmlCodec::Utf16Le,
        "hz-gb-2312" => HtmlCodec::HzGb2312,
        canonical => HtmlCodec::EncodingRs(encoding_rs::Encoding::for_label(canonical.as_bytes())?),
    };
    Some(HtmlEncoding {
        canonical_name: row.canonical,
        codec,
    })
}

fn encode_with_encoding_rs(
    encoding: &'static encoding_rs::Encoding,
    source: &str,
) -> Result<Vec<u8>, HtmlEncodingError> {
    let mut encoder = encoding.new_encoder();
    let capacity = encoder
        .max_buffer_length_from_utf8_without_replacement(source.len())
        .unwrap_or(source.len());
    let mut output = Vec::with_capacity(capacity);
    let (result, _) =
        encoder.encode_from_utf8_to_vec_without_replacement(source, &mut output, true);
    match result {
        EncoderResult::InputEmpty => Ok(output),
        EncoderResult::Unmappable(character) => Err(HtmlEncodingError { character }),
        EncoderResult::OutputFull => unreachable!("capacity came from encoding_rs"),
    }
}

fn decode_utf16(source: &[u8], big_endian: bool) -> Vec<u8> {
    let units = source.chunks(2).map(|bytes| match bytes {
        [first, second] if big_endian => u16::from_be_bytes([*first, *second]),
        [first, second] => u16::from_le_bytes([*first, *second]),
        _ => 0xFFFD,
    });
    char::decode_utf16(units)
        .map(|result| result.unwrap_or(char::REPLACEMENT_CHARACTER))
        .collect::<String>()
        .into_bytes()
}

fn encode_utf16(source: &str, big_endian: bool) -> Vec<u8> {
    source
        .encode_utf16()
        .flat_map(|unit| {
            if big_endian {
                unit.to_be_bytes()
            } else {
                unit.to_le_bytes()
            }
        })
        .collect()
}

fn decode_hz_gb2312(source: &[u8]) -> Vec<u8> {
    let mut output = String::new();
    let mut in_gb = false;
    let mut offset = 0;
    while offset < source.len() {
        let first = source[offset];
        if first >= 0x80 {
            output.push(char::REPLACEMENT_CHARACTER);
            offset += 1;
            continue;
        }
        if first == b'~' {
            let Some(second) = source.get(offset + 1).copied() else {
                output.push(char::REPLACEMENT_CHARACTER);
                break;
            };
            offset += 2;
            match second {
                b'{' => in_gb = true,
                b'}' => in_gb = false,
                b'~' => output.push('~'),
                b'\n' => {}
                _ => output.push(char::REPLACEMENT_CHARACTER),
            }
            continue;
        }
        if !in_gb {
            output.push(char::from(first));
            offset += 1;
            continue;
        }
        let Some(second) = source.get(offset + 1).copied() else {
            output.push(char::REPLACEMENT_CHARACTER);
            break;
        };
        if !(0x21..0x7E).contains(&first) || !(0x21..0x7F).contains(&second) {
            output.push(char::REPLACEMENT_CHARACTER);
            offset += if second > 0x80 { 1 } else { 2 };
            continue;
        }
        let bytes = [first + 0x80, second + 0x80];
        let (decoded, had_errors) = GBK.decode_without_bom_handling(&bytes);
        if had_errors {
            output.push(char::REPLACEMENT_CHARACTER);
        } else {
            output.push_str(&decoded);
        }
        offset += 2;
    }
    output.into_bytes()
}

fn encode_hz_gb2312(source: &str) -> Result<Vec<u8>, HtmlEncodingError> {
    let mut output = Vec::with_capacity(source.len());
    let mut in_gb = false;
    for character in source.chars() {
        if character.is_ascii() {
            if character == '~' {
                output.extend_from_slice(b"~~");
                continue;
            }
            if in_gb {
                output.extend_from_slice(b"~}");
                in_gb = false;
            }
            output.push(character as u8);
            continue;
        }
        let encoded = encode_with_encoding_rs(GBK, &character.to_string())?;
        if encoded.len() != 2
            || !(0xA1..0xFE).contains(&encoded[0])
            || !(0xA1..0xFF).contains(&encoded[1])
        {
            return Err(HtmlEncodingError { character });
        }
        if !in_gb {
            output.extend_from_slice(b"~{");
            in_gb = true;
        }
        output.extend_from_slice(&[encoded[0] - 0x80, encoded[1] - 0x80]);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_table_is_complete_and_resolvable() {
        assert_eq!(ENCODING_LABELS.len(), 218);
        for row in ENCODING_LABELS {
            let encoding = lookup_encoding(row.label).unwrap_or_else(|| panic!("{}", row.label));
            assert_eq!(encoding.canonical_name(), row.canonical, "{}", row.label);
        }
    }

    #[test]
    fn lookup_normalizes_like_go() {
        assert_eq!(
            lookup_encoding(" \tGbK\r\n").map(HtmlEncoding::canonical_name),
            Some("gbk")
        );
        assert!(lookup_encoding("not-an-encoding").is_none());
        assert!(lookup_encoding("\u{a0}gbk").is_none());
    }

    #[test]
    fn special_codecs_match_source_semantics() {
        let nop = lookup_encoding("utf8mb4").unwrap();
        assert_eq!(nop.encode(b"a\xFF").unwrap(), b"a\xFF");
        assert_eq!(nop.decode(b"a\xFF"), b"a\xFF");

        let replacement = lookup_encoding("iso-2022-kr").unwrap();
        assert_eq!(replacement.decode(b"anything"), "\u{fffd}".as_bytes());
        assert_eq!(
            replacement.encode(b"AB\x80YZ").unwrap(),
            "AB\u{fffd}YZ".as_bytes()
        );

        let utf16 = lookup_encoding("utf-16be").unwrap();
        assert_eq!(utf16.encode("A中".as_bytes()).unwrap(), b"\x00A\x4E\x2D");
        assert_eq!(utf16.decode(b"\x00A\x4E\x2D"), "A中".as_bytes());
    }

    #[test]
    fn hz_gb2312_switches_between_ascii_and_gb_states() {
        let encoding = lookup_encoding("hz-gb-2312").unwrap();
        let encoded = encoding.encode("A中文~B".as_bytes()).unwrap();
        assert_eq!(encoded, b"A~{VPND~~~}B");
        assert_eq!(encoding.decode(&encoded), "A中文~B".as_bytes());
        assert_eq!(encoding.decode(b"~x"), "\u{fffd}".as_bytes());
        assert_eq!(encoding.decode(b"~{ A"), "\u{fffd}".as_bytes());
        assert_eq!(
            encoding.decode(b"~{ \x81A"),
            "\u{fffd}\u{fffd}\u{fffd}".as_bytes()
        );
    }

    #[test]
    fn encoding_rs_aliases_transform_and_reject_unrepresentable_input() {
        let windows = lookup_encoding("latin1").unwrap();
        assert_eq!(windows.encode("€".as_bytes()).unwrap(), b"\x80");
        assert_eq!(windows.decode(b"\x80"), "€".as_bytes());
        assert_eq!(
            windows.encode("中".as_bytes()).unwrap_err().character(),
            '中'
        );
    }
}
