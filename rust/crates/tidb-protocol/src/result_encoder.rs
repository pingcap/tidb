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

//! Source-shaped result charset policy.
//!
//! This leaf ports the decision boundary in
//! `pkg/format/textrow/result_encoder.go`.  It deliberately does not pretend
//! to be a general character-set conversion library: the currently registered
//! binary/UTF-8/Latin-1 paths are byte preserving (which is also what TiDB's
//! Go implementations do for those paths), while ASCII and GBK use the
//! source-shaped replacement policies. Unknown session or column charset IDs
//! are explicit errors instead of silently falling back to binary.

use std::fmt;

use encoding_rs::{EncoderResult, GBK};
use tidb_datatype::ascii_encoding::ASCII_ENCODING;
use tidb_datatype::{Charset, TransformOp};

use crate::result::is_string_column_type as source_is_string_column_type;

/// MySQL's default collation IDs used by TiDB's result metadata path.
///
/// These are collation IDs rather than character-set IDs because the Go
/// `ResultEncoder` receives `dumpCharset`/`UpdateDataEncoding` values from
/// column metadata and resolves them through `GetCharsetInfoByID`.
pub const UTF8MB4_DEFAULT_COLLATION_ID: u16 = 46;
/// The `utf8mb4_general_ci` collation ID.
pub const UTF8MB4_GENERAL_CI_COLLATION_ID: u16 = 45;
/// The `utf8mb4_unicode_ci` collation ID.
pub const UTF8MB4_UNICODE_CI_COLLATION_ID: u16 = 224;
/// The default collation ID for `latin1`.
pub const LATIN1_DEFAULT_COLLATION_ID: u16 = 47;
/// The default collation ID for `ascii`.
pub const ASCII_DEFAULT_COLLATION_ID: u16 = 65;
/// The default collation ID for `utf8`/`utf8mb3`.
pub const UTF8_DEFAULT_COLLATION_ID: u16 = 83;
/// The `utf8_general_ci` collation ID.
pub const UTF8_GENERAL_CI_COLLATION_ID: u16 = 33;
/// The `utf8_unicode_ci` collation ID.
pub const UTF8_UNICODE_CI_COLLATION_ID: u16 = 192;
/// The default collation ID for `binary`.
pub const BINARY_DEFAULT_COLLATION_ID: u16 = 63;
/// The default `gbk_bin` collation ID.
pub const GBK_DEFAULT_COLLATION_ID: u16 = 28;

/// Errors for result charset state that is outside the currently ported
/// registry boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultEncoderError {
    /// A session result charset name is not registered by the datatype leaf.
    UnsupportedCharsetName(String),
    /// A column collation ID is not registered by the datatype leaf.
    UnsupportedCollationId(u16),
    /// Data encoding was requested before `update_data_encoding` supplied a
    /// column collation.
    DataEncodingUnset,
}

/// Character sets that can be emitted by the result protocol.
///
/// GBK intentionally lives here rather than in `tidb-datatype`: result
/// encoding needs a byte conversion table, while the shared datatype
/// registry is still limited to the charsets needed by expression and
/// metadata code. Keeping this enum local prevents an incomplete registry
/// entry from being mistaken for full GBK support elsewhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultCharset {
    /// Byte-oriented binary result data.
    Binary,
    /// Seven-bit ASCII result data.
    Ascii,
    /// Single-byte Latin-1 result data.
    Latin1,
    /// Legacy three-byte UTF-8 result data.
    Utf8,
    /// Four-byte UTF-8 result data.
    Utf8Mb4,
    /// GBK/CP936 result data.
    Gbk,
}

impl ResultCharset {
    fn from_datatype(charset: Charset) -> Self {
        match charset {
            Charset::Binary => Self::Binary,
            Charset::Ascii => Self::Ascii,
            Charset::Latin1 => Self::Latin1,
            Charset::Utf8 => Self::Utf8,
            Charset::Utf8Mb4 => Self::Utf8Mb4,
        }
    }
}

impl fmt::Display for ResultEncoderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCharsetName(name) => {
                write!(f, "unsupported result charset '{name}'")
            }
            Self::UnsupportedCollationId(id) => write!(f, "unsupported result collation id {id}"),
            Self::DataEncodingUnset => f.write_str("result data charset was not initialized"),
        }
    }
}

impl std::error::Error for ResultEncoderError {}

/// Returns whether a MySQL field type participates in result charset
/// rewriting.  The type partition is shared with the text-row metadata
/// primitive so BIT/blob/enum/set/JSON/vector columns follow source behavior.
pub fn is_string_column_type(type_code: u8) -> bool {
    source_is_string_column_type(type_code)
}

/// Encodes text-protocol metadata and row bytes using TiDB's result charset
/// policy.
///
/// `result_charset` is the session `@@character_set_results` value.  An empty
/// value models Go's `isNull` state and therefore leaves data in its column
/// charset.  Callers must call [`Self::update_data_encoding`] once per column
/// before encoding a non-null/non-binary result.
#[derive(Debug, Clone, Copy)]
pub struct ResultEncoder {
    result_charset: Option<ResultCharset>,
    data_charset: Option<ResultCharset>,
    data_is_binary: bool,
}

impl ResultEncoder {
    /// Creates an encoder from a registered session charset name.
    ///
    /// The empty string is the source null-result state.  Charset aliases
    /// such as `utf8mb3` are resolved through the datatype registry.
    pub fn new(result_charset: &str) -> Result<Self, ResultEncoderError> {
        let result_charset = if result_charset.is_empty() {
            None
        } else {
            Some(match result_charset.to_ascii_lowercase().as_str() {
                "gbk" => ResultCharset::Gbk,
                name => {
                    ResultCharset::from_datatype(Charset::from_name(name).ok_or_else(|| {
                        ResultEncoderError::UnsupportedCharsetName(result_charset.to_owned())
                    })?)
                }
            })
        };
        Ok(Self {
            result_charset,
            data_charset: None,
            data_is_binary: false,
        })
    }

    /// Returns the registered session result charset, or `None` for Go's
    /// `@@character_set_results = NULL` state.
    pub const fn result_charset(&self) -> Option<ResultCharset> {
        self.result_charset
    }

    /// Returns the charset ID advertised in a text-protocol column definition.
    ///
    /// This is the source `ColumnCharsetID` rule: null/empty results,
    /// non-string columns, and binary columns retain `dump_charset`; otherwise
    /// the session result charset's default collation is advertised.
    pub fn column_charset_id(&self, dump_charset: u16, is_string_col: bool) -> u16 {
        let Some(result_charset) = self.result_charset else {
            return dump_charset;
        };
        if !is_string_col || dump_charset == BINARY_DEFAULT_COLLATION_ID {
            return dump_charset;
        }
        charset_default_collation_id(result_charset)
    }

    /// Updates the data charset from a source column collation ID.
    ///
    /// TiDB's current result registry supports the binary/ASCII/Latin-1,
    /// UTF-8, and GBK collation rows represented here. Other IDs return an
    /// explicit error until their registry rows and conversion
    /// implementations are ported.
    pub fn update_data_encoding(&mut self, collation_id: u16) -> Result<(), ResultEncoderError> {
        let charset = charset_from_collation_id(collation_id)
            .ok_or(ResultEncoderError::UnsupportedCollationId(collation_id))?;
        self.data_is_binary = charset == ResultCharset::Binary;
        self.data_charset = Some(charset);
        Ok(())
    }

    /// Encodes metadata bytes with `@@character_set_results`.
    ///
    /// Binary and UTF-8 are source no-op paths. ASCII uses Go's replacement
    /// mode (`OpEncodeReplace`) and therefore emits `?` for an invalid group;
    /// the Go implementation logs the conversion error and still returns the
    /// replacement bytes, so this API preserves that behavior.
    pub fn encode_meta(&self, src: &[u8]) -> Result<Vec<u8>, ResultEncoderError> {
        let charset = self.result_charset.unwrap_or(ResultCharset::Binary);
        Ok(encode_with_charset(src, charset))
    }

    /// Encodes row data using the source column/session precedence rule.
    ///
    /// A null result, a binary session result, or a binary column uses the
    /// column charset. All other columns use the session result charset.
    pub fn encode_data(&self, src: &[u8]) -> Result<Vec<u8>, ResultEncoderError> {
        let Some(data_charset) = self.data_charset else {
            return Err(ResultEncoderError::DataEncodingUnset);
        };
        let use_data_charset = self.result_charset.is_none()
            || self.result_charset == Some(ResultCharset::Binary)
            || self.data_is_binary;
        let charset = if use_data_charset {
            data_charset
        } else {
            self.result_charset.expect("non-null result charset")
        };
        Ok(encode_with_charset(src, charset))
    }
}

fn charset_default_collation_id(charset: ResultCharset) -> u16 {
    match charset {
        ResultCharset::Utf8Mb4 => UTF8MB4_DEFAULT_COLLATION_ID,
        ResultCharset::Latin1 => LATIN1_DEFAULT_COLLATION_ID,
        ResultCharset::Ascii => ASCII_DEFAULT_COLLATION_ID,
        ResultCharset::Utf8 => UTF8_DEFAULT_COLLATION_ID,
        ResultCharset::Binary => BINARY_DEFAULT_COLLATION_ID,
        ResultCharset::Gbk => GBK_DEFAULT_COLLATION_ID,
    }
}

fn charset_from_collation_id(id: u16) -> Option<ResultCharset> {
    match id {
        UTF8MB4_DEFAULT_COLLATION_ID
        | UTF8MB4_GENERAL_CI_COLLATION_ID
        | UTF8MB4_UNICODE_CI_COLLATION_ID => Some(ResultCharset::Utf8Mb4),
        LATIN1_DEFAULT_COLLATION_ID => Some(ResultCharset::Latin1),
        ASCII_DEFAULT_COLLATION_ID => Some(ResultCharset::Ascii),
        UTF8_DEFAULT_COLLATION_ID | UTF8_GENERAL_CI_COLLATION_ID | UTF8_UNICODE_CI_COLLATION_ID => {
            Some(ResultCharset::Utf8)
        }
        BINARY_DEFAULT_COLLATION_ID => Some(ResultCharset::Binary),
        GBK_DEFAULT_COLLATION_ID => Some(ResultCharset::Gbk),
        _ => None,
    }
}

fn encode_with_charset(src: &[u8], charset: ResultCharset) -> Vec<u8> {
    match charset {
        // Go's FindEncodingTakeUTF8AsNoop deliberately uses the binary
        // encoder for UTF-8, and TiDB's Latin-1 compatibility encoder is also
        // a byte-preserving NOP. Keep all of these paths allocation-only.
        ResultCharset::Binary
        | ResultCharset::Latin1
        | ResultCharset::Utf8
        | ResultCharset::Utf8Mb4 => src.to_vec(),
        ResultCharset::Ascii => ASCII_ENCODING
            .transform(src, TransformOp::ENCODE_REPLACE)
            .bytes()
            .to_vec(),
        ResultCharset::Gbk => encode_gbk(src),
    }
}

/// Encode UTF-8 bytes with TiDB's custom GBK policy.
///
/// `encoding_rs` supplies the same GBK/CP936 mapping as Go's
/// `simplifiedchinese.GBK`. TiDB's wrapper differs in one important way: the
/// euro sign is deliberately rejected and replacement mode emits `?`, not
/// the WHATWG encoder's HTML escape. Encoding one Unicode scalar at a time
/// lets us preserve that replacement boundary and also mirrors Go's
/// `utf8.DecodeRune` behavior for malformed input (one `?` per bad byte).
fn encode_gbk(src: &[u8]) -> Vec<u8> {
    let mut output = Vec::with_capacity(src.len());
    let mut remaining = src;
    while !remaining.is_empty() {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                encode_valid_gbk(valid, &mut output);
                break;
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if valid_up_to != 0 {
                    // `valid_up_to` is always a UTF-8 scalar boundary.
                    encode_valid_gbk(
                        std::str::from_utf8(&remaining[..valid_up_to])
                            .expect("the UTF-8 error boundary is a valid prefix"),
                        &mut output,
                    );
                    remaining = &remaining[valid_up_to..];
                }
                // Go's DecodeRune consumes only the invalid leading byte,
                // even when the malformed sequence contains continuations.
                output.push(b'?');
                remaining = &remaining[1..];
            }
        }
    }
    output
}

fn encode_valid_gbk(src: &str, output: &mut Vec<u8>) {
    for ch in src.chars() {
        // pkg/parser/charset/encoding_gbk.go intentionally rejects `€` even
        // though Windows-936/WHATWG GBK has a euro extension.
        if ch == '\u{20ac}' {
            output.push(b'?');
            continue;
        }
        let mut input_buf = [0u8; 4];
        let input = ch.encode_utf8(&mut input_buf);
        let mut bytes = [0u8; 8];
        let (result, read, written) = GBK
            .new_encoder()
            .encode_from_utf8_without_replacement(input, &mut bytes, true);
        match result {
            EncoderResult::InputEmpty => {
                debug_assert_eq!(read, input.len());
                output.extend_from_slice(&bytes[..written]);
            }
            EncoderResult::Unmappable(_) => output.push(b'?'),
            EncoderResult::OutputFull => unreachable!("GBK scalar exceeds eight output bytes"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        is_string_column_type, ResultEncoder, ResultEncoderError, ASCII_DEFAULT_COLLATION_ID,
        BINARY_DEFAULT_COLLATION_ID, GBK_DEFAULT_COLLATION_ID, LATIN1_DEFAULT_COLLATION_ID,
        UTF8MB4_DEFAULT_COLLATION_ID,
    };

    #[test]
    fn source_column_charset_id_preserves_binary_and_non_string_columns() {
        let encoder = ResultEncoder::new("utf8mb4").expect("registered charset");
        assert_eq!(
            encoder.column_charset_id(47, true),
            UTF8MB4_DEFAULT_COLLATION_ID
        );
        assert_eq!(encoder.column_charset_id(47, false), 47);
        assert_eq!(
            encoder.column_charset_id(BINARY_DEFAULT_COLLATION_ID, true),
            BINARY_DEFAULT_COLLATION_ID
        );
        let null_encoder = ResultEncoder::new("").expect("null result charset");
        assert_eq!(null_encoder.column_charset_id(47, true), 47);
    }

    #[test]
    fn source_string_type_partition_matches_result_metadata() {
        for type_code in [
            0x0f, 0xfd, 0xfe, 0x10, 0xf9, 0xfa, 0xfb, 0xfc, 0xf7, 0xf8, 0xf5, 0xe1,
        ] {
            assert!(is_string_column_type(type_code), "type {type_code:#x}");
        }
        assert!(!is_string_column_type(0x03));
    }

    #[test]
    fn source_meta_and_utf8_data_are_byte_preserving() {
        let mut encoder = ResultEncoder::new("utf8mb4").expect("registered charset");
        encoder
            .update_data_encoding(47)
            .expect("latin1 registry row");
        let bytes = [0xff, 0x00, 0xc3, 0x28];
        assert_eq!(encoder.encode_meta(&bytes).unwrap(), bytes);
        assert_eq!(encoder.encode_data(&bytes).unwrap(), bytes);
    }

    #[test]
    fn source_binary_result_uses_column_charset_and_ascii_replaces_invalid() {
        let mut binary = ResultEncoder::new("binary").expect("registered charset");
        binary
            .update_data_encoding(ASCII_DEFAULT_COLLATION_ID)
            .expect("ascii registry row");
        // Go's UTF-8 lead-byte grouping treats the truncated `0xffa` suffix
        // as one invalid group, so replacement emits one `?` for the pair.
        assert_eq!(binary.encode_data(b"\xffa").unwrap(), b"?");

        let mut session_utf8 = ResultEncoder::new("utf8mb4").expect("registered charset");
        session_utf8
            .update_data_encoding(BINARY_DEFAULT_COLLATION_ID)
            .expect("binary registry row");
        assert_eq!(session_utf8.encode_data(b"\xffa").unwrap(), b"\xffa");
    }

    #[test]
    fn source_gbk_meta_and_column_precedence_match_go() {
        let encoder = ResultEncoder::new("gbk").expect("GBK is a supported result charset");
        assert_eq!(
            encoder.column_charset_id(47, true),
            GBK_DEFAULT_COLLATION_ID
        );
        assert_eq!(encoder.column_charset_id(47, false), 47);
        assert_eq!(
            encoder.column_charset_id(BINARY_DEFAULT_COLLATION_ID, true),
            63
        );
        assert_eq!(encoder.encode_meta("一".as_bytes()).unwrap(), [0xd2, 0xbb]);
    }

    #[test]
    fn source_gbk_replaces_unmappable_euro_and_malformed_utf8() {
        let encoder = ResultEncoder::new("gbk").expect("GBK is a supported result charset");
        assert_eq!(
            encoder.encode_meta("一二三123".as_bytes()).unwrap(),
            [0xd2, 0xbb, 0xb6, 0xfe, 0xc8, 0xfd, b'1', b'2', b'3']
        );
        assert_eq!(encoder.encode_meta("€aÀ".as_bytes()).unwrap(), b"?a?");
        assert_eq!(encoder.encode_meta(b"a\x80b").unwrap(), b"a?b");
    }

    #[test]
    fn source_gbk_data_obeys_session_and_column_precedence() {
        let mut session_gbk = ResultEncoder::new("gbk").expect("GBK is a supported result charset");
        session_gbk
            .update_data_encoding(LATIN1_DEFAULT_COLLATION_ID)
            .expect("latin1 column");
        assert_eq!(
            session_gbk.encode_data("一".as_bytes()).unwrap(),
            [0xd2, 0xbb]
        );

        let mut binary_session = ResultEncoder::new("binary").expect("binary charset");
        binary_session
            .update_data_encoding(GBK_DEFAULT_COLLATION_ID)
            .expect("GBK column");
        assert_eq!(
            binary_session.encode_data("一".as_bytes()).unwrap(),
            [0xd2, 0xbb]
        );

        let mut null_session = ResultEncoder::new("").expect("NULL result charset");
        null_session
            .update_data_encoding(GBK_DEFAULT_COLLATION_ID)
            .expect("GBK column");
        assert_eq!(
            null_session.encode_data("一".as_bytes()).unwrap(),
            [0xd2, 0xbb]
        );
    }

    #[test]
    fn unsupported_state_is_explicit() {
        let mut encoder = ResultEncoder::new("utf8").expect("registered charset");
        encoder
            .update_data_encoding(33)
            .expect("registered utf8 general-ci row");
        assert_eq!(
            encoder.update_data_encoding(999),
            Err(ResultEncoderError::UnsupportedCollationId(999))
        );
        let unset = ResultEncoder::new("utf8").expect("registered charset");
        assert_eq!(
            unset.encode_data(b"x"),
            Err(ResultEncoderError::DataEncodingUnset)
        );
    }
}
