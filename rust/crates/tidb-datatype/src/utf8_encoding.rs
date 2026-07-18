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

//! Source-shaped UTF-8 charset leaves.
//!
//! This module deliberately stays below the charset registry.  It ports the
//! byte-level behavior in `encoding_utf8.go` so later registry work can use a
//! tested authority instead of teaching every caller its own UTF-8 rules.
//! Inputs remain byte slices: Go strings may contain arbitrary octets and the
//! invalid-group boundaries are part of the `Foreach`/`Transform` contract.

use crate::encoding_base::{TransformOp, TransformPolicy, TransformResult};
use std::fmt;

/// UTF-8 keeps the source operation name while sharing one policy with all
/// byte-preserving charset leaves.
pub type Utf8Op = TransformOp;

/// The normal four-byte UTF-8 encoding (`utf8mb4` in TiDB's source registry).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Utf8Encoding;

/// The strict legacy three-byte UTF-8 encoding (`utf8`/`utf8mb3`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Utf8Mb3StrictEncoding;

/// Stateless source-shaped UTF-8 encoder instance.
pub const UTF8_ENCODING: Utf8Encoding = Utf8Encoding;

/// Stateless source-shaped strict `utf8mb3` encoder instance.
pub const UTF8_MB3_STRICT_ENCODING: Utf8Mb3StrictEncoding = Utf8Mb3StrictEncoding;

/// The first invalid source group reported by `generateEncodingErr`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Utf8TransformError {
    invalid: Vec<u8>,
}

impl Utf8TransformError {
    /// Returns the exact invalid source group passed to the source error
    /// helper.
    pub fn invalid_bytes(&self) -> &[u8] {
        &self.invalid
    }
}

impl fmt::Display for Utf8TransformError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invalid utf8 character string: '{}'",
            hex_bytes(&self.invalid)
        )
    }
}

impl std::error::Error for Utf8TransformError {}

/// Bytes and the optional first invalid-group error returned by Transform.
pub type Utf8TransformResult = TransformResult<Utf8TransformError>;

impl Utf8Encoding {
    /// Returns the source registry name.
    pub const fn name(self) -> &'static str {
        "utf8mb4"
    }

    /// Returns the source `EncodingUTF8.Peek` grouping.  This is intentionally
    /// a lead-byte width operation, not validation.
    pub fn peek(self, src: &[u8]) -> &[u8] {
        utf8_peek(src)
    }

    /// Returns the byte width of the first valid non-ASCII rune, or zero for
    /// ASCII and invalid input, matching `utf8.DecodeRuneInString`.
    pub fn mb_len(self, src: &[u8]) -> usize {
        valid_utf8_width(src)
            .filter(|width| *width > 1)
            .unwrap_or(0)
    }

    /// Returns true when every byte is valid four-byte UTF-8.
    pub fn is_valid(self, src: &[u8]) -> bool {
        all_valid(src, false)
    }

    /// Visits decoded UTF-8 groups.  `from` and `to` alias the original bytes
    /// because UTF-8 is a no-op conversion in the source implementation.
    pub fn foreach<F>(self, src: &[u8], mut f: F)
    where
        F: FnMut(&[u8], &[u8], bool) -> bool,
    {
        foreach_utf8(src, false, &mut f);
    }

    /// Applies source `encodingBase.Transform` error, truncation, and
    /// replacement policy while retaining bytes throughout.
    pub fn transform(self, src: &[u8], op: Utf8Op) -> Utf8TransformResult {
        transform_utf8(src, op, false)
    }
}

impl Utf8Mb3StrictEncoding {
    /// The strict source implementation inherits `encodingUTF8.Name`.
    pub const fn name(self) -> &'static str {
        UTF8_ENCODING.name()
    }

    /// Returns the same lead-byte grouping as the embedded UTF-8 encoding.
    pub fn peek(self, src: &[u8]) -> &[u8] {
        UTF8_ENCODING.peek(src)
    }

    /// `MbLen` is inherited and reports valid UTF-8 width even for a rune
    /// that strict validation later rejects as a four-byte character.
    pub fn mb_len(self, src: &[u8]) -> usize {
        UTF8_ENCODING.mb_len(src)
    }

    /// Returns true only for valid UTF-8 whose runes are at most three bytes.
    pub fn is_valid(self, src: &[u8]) -> bool {
        all_valid(src, true)
    }

    /// Visits groups and marks valid four-byte runes invalid, as in the
    /// source `encodingUTF8MB3Strict.Foreach`.
    pub fn foreach<F>(self, src: &[u8], mut f: F)
    where
        F: FnMut(&[u8], &[u8], bool) -> bool,
    {
        foreach_utf8(src, true, &mut f);
    }

    /// Applies source transform behavior with strict three-byte validation.
    pub fn transform(self, src: &[u8], op: Utf8Op) -> Utf8TransformResult {
        transform_utf8(src, op, true)
    }
}

fn all_valid(src: &[u8], strict_mb3: bool) -> bool {
    let mut valid = true;
    foreach_utf8(src, strict_mb3, &mut |_, _, ok| {
        valid = ok;
        ok
    });
    valid
}

fn foreach_utf8<F>(src: &[u8], strict_mb3: bool, f: &mut F)
where
    F: FnMut(&[u8], &[u8], bool) -> bool,
{
    let mut offset = 0;
    while offset < src.len() {
        let (width, valid) = decode_utf8_group(&src[offset..]);
        let end = offset + width;
        let ok = valid && (!strict_mb3 || width <= 3);
        if !f(&src[offset..end], &src[offset..end], ok) {
            return;
        }
        offset = end;
    }
}

fn transform_utf8(src: &[u8], op: Utf8Op, strict_mb3: bool) -> Utf8TransformResult {
    // Both Go UTF-8 implementations return valid input unchanged before the
    // shared encoding-base policy sees the operation bits.
    if all_valid(src, strict_mb3) {
        return TransformResult::new(src.to_vec(), None);
    }
    let mut policy = TransformPolicy::new(src.len(), op, |invalid| Utf8TransformError {
        invalid: invalid.to_vec(),
    });
    foreach_utf8(src, strict_mb3, &mut |from, to, ok| {
        policy.push(from, to, ok)
    });
    policy.finish()
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

/// Returns the valid UTF-8 width for the first rune.  Go's decoder reports an
/// invalid or truncated sequence as `RuneError, 1`, so invalid groups advance
/// one byte even when their lead byte suggests a wider sequence.
fn valid_utf8_width(src: &[u8]) -> Option<usize> {
    let (width, valid) = decode_utf8_group(src);
    valid.then_some(width)
}

fn decode_utf8_group(src: &[u8]) -> (usize, bool) {
    if src.is_empty() {
        return (0, true);
    }
    let first = src[0];
    if first < 0x80 {
        return (1, true);
    }
    let width = match first {
        0xc2..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf4 => 4,
        _ => return (1, false),
    };
    if src.len() < width {
        return (1, false);
    }
    if src[1..width].iter().any(|byte| *byte & 0xc0 != 0x80) {
        return (1, false);
    }
    if (first == 0xe0 && src[1] < 0xa0)
        || (first == 0xed && src[1] >= 0xa0)
        || (first == 0xf0 && src[1] < 0x90)
        || (first == 0xf4 && src[1] >= 0x90)
    {
        return (1, false);
    }
    (width, true)
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
    use super::{Utf8Op, UTF8_ENCODING, UTF8_MB3_STRICT_ENCODING};

    #[test]
    fn source_test_encoding_validate_utf8_rows() {
        let mb4_rows: &[(&[u8], &[u8], bool)] = &[
            (b"", b"", true),
            (b"qwerty", b"qwerty", true),
            ("qwÊrty".as_bytes(), "qwÊrty".as_bytes(), true),
            ("qwÊ合法字符串".as_bytes(), "qwÊ合法字符串".as_bytes(), true),
            ("😂".as_bytes(), "😂".as_bytes(), true),
            (&[0xff, 0xfe, 0xfd], b"???", false),
            (
                &[0xe4, 0xb8, 0xad, 0xff, 0xfe, 0xfd],
                "中???".as_bytes(),
                false,
            ),
            ("�".as_bytes(), "�".as_bytes(), true),
        ];
        for &(src, expected, valid) in mb4_rows {
            assert_eq!(UTF8_ENCODING.is_valid(src), valid, "{src:?}");
            let transformed = UTF8_ENCODING.transform(src, Utf8Op::REPLACE_NO_ERR);
            assert_eq!(transformed.bytes(), expected, "{src:?}");
            assert!(transformed.error().is_none(), "{src:?}");
        }

        let mb3_rows: &[(&[u8], &[u8], bool)] = &[
            (b"", b"", true),
            (b"qwerty", b"qwerty", true),
            ("qwÊrty".as_bytes(), "qwÊrty".as_bytes(), true),
            ("qwÊ合法字符串".as_bytes(), "qwÊ合法字符串".as_bytes(), true),
            ("😂".as_bytes(), b"?", false),
            ("valid_str😂".as_bytes(), b"valid_str?", false),
            (&[0xff, 0xfe, 0xfd], b"???", false),
            ("�".as_bytes(), "�".as_bytes(), true),
        ];
        for &(src, expected, valid) in mb3_rows {
            assert_eq!(UTF8_MB3_STRICT_ENCODING.is_valid(src), valid, "{src:?}");
            let transformed = UTF8_MB3_STRICT_ENCODING.transform(src, Utf8Op::REPLACE_NO_ERR);
            assert_eq!(transformed.bytes(), expected, "{src:?}");
            assert!(transformed.error().is_none(), "{src:?}");
        }
    }

    #[test]
    fn peek_mb_len_and_foreach_match_go_decoder_boundaries() {
        let src = [b'a', 0xc3, 0x8a, 0xff, 0xf0, 0x9f, 0x98, 0x82, b'z'];
        assert_eq!(UTF8_ENCODING.peek(&src), b"a");
        assert_eq!(UTF8_ENCODING.peek(&src[1..]), &[0xc3, 0x8a]);
        assert_eq!(UTF8_ENCODING.peek(&src[3..]), &[0xff, 0xf0, 0x9f, 0x98]);
        assert_eq!(UTF8_ENCODING.mb_len("Ê".as_bytes()), 2);
        assert_eq!(UTF8_ENCODING.mb_len(b"a"), 0);
        assert_eq!(UTF8_ENCODING.mb_len(&[0xc3]), 0);

        let mut groups = Vec::new();
        UTF8_ENCODING.foreach(&src, |from, to, ok| {
            groups.push((from.to_vec(), to.to_vec(), ok));
            true
        });
        assert_eq!(groups[0], (b"a".to_vec(), b"a".to_vec(), true));
        assert_eq!(groups[1], (vec![0xc3, 0x8a], vec![0xc3, 0x8a], true));
        assert_eq!(groups[2], (vec![0xff], vec![0xff], false));
        assert_eq!(
            groups[3],
            ("😂".as_bytes().to_vec(), "😂".as_bytes().to_vec(), true)
        );
        assert_eq!(groups[4], (b"z".to_vec(), b"z".to_vec(), true));

        let mut strict_groups = Vec::new();
        UTF8_MB3_STRICT_ENCODING.foreach("a😂z".as_bytes(), |from, _, ok| {
            strict_groups.push((from.to_vec(), ok));
            true
        });
        assert_eq!(strict_groups[1], ("😂".as_bytes().to_vec(), false));
    }

    #[test]
    fn transform_preserves_source_error_and_truncation_modes() {
        let src = [b'a', 0xc3, 0x8a, b'b'];
        let valid_fast_path = UTF8_ENCODING.transform("é".as_bytes(), Utf8Op::DECODE_NO_ERR);
        assert_eq!(valid_fast_path.bytes(), "é".as_bytes());
        assert!(valid_fast_path.error().is_none());

        let replaced = UTF8_ENCODING.transform(&src, Utf8Op::REPLACE_NO_ERR);
        assert_eq!(replaced.bytes(), src);
        assert!(replaced.error().is_none());

        let invalid = [b'a', 0xff, b'b'];
        for (op, expected) in [
            (Utf8Op::REPLACE, b"a?b".as_slice()),
            (Utf8Op::ENCODE, b"a".as_slice()),
            (Utf8Op::ENCODE_REPLACE, b"a?b".as_slice()),
            (Utf8Op::DECODE_REPLACE, b"a?b".as_slice()),
        ] {
            let transformed = UTF8_ENCODING.transform(&invalid, op);
            assert_eq!(transformed.bytes(), expected);
            assert_eq!(
                transformed.error().map(|error| error.invalid_bytes()),
                Some(&[0xff][..])
            );
        }
        let trimmed = UTF8_ENCODING.transform(&invalid, Utf8Op::DECODE_NO_ERR);
        assert_eq!(trimmed.bytes(), b"a");
        assert!(trimmed.error().is_none());
    }
}
