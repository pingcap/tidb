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

//! Source-shaped ASCII encoding operations.
//!
//! This is intentionally a byte API.  Go's charset implementation accepts
//! arbitrary octets in a string and uses the UTF-8 lead-byte width when it
//! reports an invalid sequence.  Converting the input to `str` would lose
//! that contract before validation starts.

use crate::encoding_base::{TransformOp, TransformPolicy, TransformResult};
use std::fmt;

/// ASCII keeps the source operation name while sharing the single operation
/// vocabulary with UTF-8 and later charset leaves.
pub type AsciiOp = TransformOp;

/// TiDB's seven-bit ASCII encoding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AsciiEncoding;

/// The one stateless ASCII encoder instance.  A zero-sized value avoids a
/// registry or dispatcher dependency while retaining the source singleton's
/// call shape for leaf tests.
pub const ASCII_ENCODING: AsciiEncoding = AsciiEncoding;

/// The first invalid byte group reported by source `generateEncodingErr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AsciiTransformError {
    invalid: Vec<u8>,
}

impl AsciiTransformError {
    /// Returns the exact invalid source group passed to the Go error helper.
    pub fn invalid_bytes(&self) -> &[u8] {
        &self.invalid
    }
}

impl fmt::Display for AsciiTransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invalid ascii character string: '{}'",
            hex_bytes(&self.invalid)
        )
    }
}

impl std::error::Error for AsciiTransformError {}

/// Bytes and the optional first invalid-group error returned by Transform.
///
/// Go returns both values when replacement is requested without suppressing
/// errors; a `Result<Vec<u8>, E>` would incorrectly discard those bytes.
pub type AsciiTransformResult = TransformResult<AsciiTransformError>;

impl AsciiEncoding {
    /// Returns the source registry name.
    pub const fn name(self) -> &'static str {
        "ascii"
    }

    /// Returns the next ASCII byte, preserving the input slice lifetime.
    pub fn peek(self, src: &[u8]) -> &[u8] {
        if src.is_empty() {
            src
        } else {
            &src[..1]
        }
    }

    /// Returns true only when every input octet is seven-bit ASCII.
    pub fn is_valid(self, src: &[u8]) -> bool {
        src.iter().all(|byte| *byte <= 0x7f)
    }

    /// Visits source groups using the exact ASCII/UTF-8 lead-byte grouping
    /// from `encoding_ascii.go`.  `to` aliases `from` because ASCII has no
    /// conversion table.
    pub fn foreach<F>(self, src: &[u8], mut f: F)
    where
        F: FnMut(&[u8], &[u8], bool) -> bool,
    {
        let mut i = 0;
        while i < src.len() {
            let mut width = 1;
            let mut ok = true;
            if src[i] > 0x7f {
                width = utf8_peek(&src[i..]).len();
                ok = false;
            }
            let group = &src[i..i + width];
            if !f(group, group, ok) {
                return;
            }
            i += width;
        }
    }

    /// Applies the source `encodingBase.Transform` collection and error
    /// policy while retaining bytes throughout.  Valid input follows the Go
    /// collection path as well; the operation bits always decide which bytes
    /// are emitted.
    pub fn transform(self, src: &[u8], op: AsciiOp) -> AsciiTransformResult {
        // `encodingASCII.Transform` has a source-level valid-input fast path:
        // it returns the original bytes regardless of the operation bits.
        if self.is_valid(src) {
            return TransformResult::new(src.to_vec(), None);
        }
        let mut policy = TransformPolicy::new(src.len(), op, |invalid| AsciiTransformError {
            invalid: invalid.to_vec(),
        });
        self.foreach(src, |from, to, ok| policy.push(from, to, ok));
        policy.finish()
    }
}

/// Mirrors `encodingUTF8.Peek` without decoding or manufacturing a `str`.
fn utf8_peek(src: &[u8]) -> &[u8] {
    if src.is_empty() {
        return src;
    }
    let expected = if src[0] < 0x80 {
        1
    } else if src[0] < 0xe0 {
        2
    } else if src[0] < 0xf0 {
        3
    } else {
        4
    };
    &src[..expected.min(src.len())]
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use fmt::Write;
        write!(&mut result, "{byte:02X}").expect("writing to String cannot fail");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{AsciiEncoding, AsciiOp, ASCII_ENCODING};

    #[test]
    fn source_ascii_rows_from_test_encoding_validate() {
        let rows: &[(&[u8], &[u8], usize, bool)] = &[
            (b"", b"", 0, true),
            (b"qwerty", b"qwerty", 6, true),
            (b"qw\xC3\x8Arty", b"qw?rty", 2, false),
            ("中文".as_bytes(), b"??", 0, false),
            ("中文?qwert".as_bytes(), b"???qwert", 0, false),
        ];
        for &(src, expected, valid_bytes, expected_valid) in rows {
            assert_eq!(ASCII_ENCODING.is_valid(src), expected_valid, "{src:?}");
            let mut count = 0;
            ASCII_ENCODING.foreach(src, |from, _, ok| {
                if ok {
                    count += from.len();
                }
                ok
            });
            assert_eq!(count, valid_bytes, "{src:?}");
            let transformed = ASCII_ENCODING.transform(src, AsciiOp::REPLACE_NO_ERR);
            assert_eq!(transformed.bytes(), expected, "{src:?}");
            assert!(transformed.error().is_none());
        }
    }

    #[test]
    fn foreach_preserves_utf8_lead_width_for_arbitrary_bytes() {
        let src = [b'a', 0xc3, b'b', 0xff, b'c', 0xf0, 0x80, 0x80, b'd'];
        let mut groups = Vec::new();
        ASCII_ENCODING.foreach(&src, |from, to, ok| {
            groups.push((from.to_vec(), to.to_vec(), ok));
            true
        });
        assert_eq!(
            groups,
            vec![
                (b"a".to_vec(), b"a".to_vec(), true),
                (vec![0xc3, b'b'], vec![0xc3, b'b'], false),
                (
                    vec![0xff, b'c', 0xf0, 0x80],
                    vec![0xff, b'c', 0xf0, 0x80],
                    false
                ),
                (vec![0x80, b'd'], vec![0x80, b'd'], false),
            ]
        );
    }

    #[test]
    fn transform_preserves_source_truncate_replace_and_error_modes() {
        let valid_fast_path = ASCII_ENCODING.transform(b"abc", AsciiOp::DECODE_NO_ERR);
        assert_eq!(valid_fast_path.bytes(), b"abc");
        assert!(valid_fast_path.error().is_none());

        let src = b"a\xC3\x8Ab";
        let replaced = ASCII_ENCODING.transform(src, AsciiOp::REPLACE_NO_ERR);
        assert_eq!(replaced.bytes(), b"a?b");
        assert!(replaced.error().is_none());
        for (op, expected) in [
            (AsciiOp::REPLACE, b"a?b".as_slice()),
            (AsciiOp::ENCODE, b"a".as_slice()),
            (AsciiOp::ENCODE_REPLACE, b"a?b".as_slice()),
            (AsciiOp::DECODE_REPLACE, b"a?b".as_slice()),
        ] {
            let transformed = ASCII_ENCODING.transform(src, op);
            assert_eq!(transformed.bytes(), expected);
            assert_eq!(
                transformed.error().map(|error| error.invalid_bytes()),
                Some(&[0xc3, 0x8a][..])
            );
        }
    }

    #[test]
    fn transform_trims_at_invalid_group_and_keeps_raw_bytes() {
        let src = [b'a', 0xff, b'b'];
        let encoded = ASCII_ENCODING.transform(&src, AsciiOp::ENCODE_NO_ERR);
        assert_eq!(encoded.bytes(), b"a");
        assert!(encoded.error().is_none());
        let decoded = ASCII_ENCODING.transform(&src, AsciiOp::DECODE_NO_ERR);
        assert_eq!(decoded.bytes(), b"a");
        assert!(decoded.error().is_none());
        let src = [0xff, 0xfe, 0xfd];
        let transformed = ASCII_ENCODING.transform(&src, AsciiOp::REPLACE);
        assert_eq!(transformed.bytes(), b"?");
        assert_eq!(
            transformed.error().map(|error| error.invalid_bytes()),
            Some(&[0xff, 0xfe, 0xfd][..])
        );
    }

    #[test]
    fn metadata_and_peek_match_source() {
        let encoding = AsciiEncoding;
        assert_eq!(encoding.name(), "ascii");
        assert_eq!(encoding.peek(b""), b"");
        assert_eq!(encoding.peek(b"abc"), b"a");
        assert_eq!(encoding.peek(&[0xff, 0x00]), &[0xff]);
    }
}
