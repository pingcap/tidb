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

//! Go `string` bytes carried inside a Rust `str`.
//!
//! A Go `string` is an arbitrary byte sequence: TiDB scans COM_QUERY bytes
//! as-is (`hack.String`), so a `latin1`/`binary`/`utf8mb4` client's SQL text
//! and its string literals may hold bytes that are not UTF-8, and those bytes
//! reach storage unchanged. This crate's SQL text is a `str`, so every byte
//! that is not part of valid UTF-8 travels as one ESCAPE character from the
//! last 128 code points of Unicode (U+10FF80..=U+10FFFF, private use and
//! noncharacters): escape `U+10FF00 + b` stands for the raw byte `b`
//! (`0x80..=0xFF`; a byte below `0x80` is always valid UTF-8).
//!
//! The mapping is lossless in both directions. A VALID character inside the
//! escape range is itself escaped byte by byte by [`bytes_to_str`], so
//! [`str_to_bytes`] restores exactly the bytes that came in. Text that is
//! valid UTF-8 and holds no escape-range character -- every ordinary
//! statement -- is borrowed unchanged by both functions.
//!
//! The consequence for the rest of the stack: inside SQL text and string
//! literal values, an escape character MEANS its raw byte. Code that turns a
//! literal into datum bytes, or SQL-derived text into wire bytes, converts
//! with [`str_to_bytes`].

use std::borrow::Cow;

/// The code point whose low byte is the escaped raw byte.
const ESCAPE_BASE: u32 = 0x10_FF00;
/// The first escape character, standing for raw byte `0x80`.
const FIRST_ESCAPE: u32 = ESCAPE_BASE + 0x80;

/// Whether `c` is an escape character, which stands for one raw byte.
#[must_use]
pub const fn is_escape(c: char) -> bool {
    c as u32 >= FIRST_ESCAPE
}

/// Whether `text` holds any escape character. Every escape is encoded as
/// `F4 8F BE xx` or `F4 8F BF xx`, so a byte scan decides it.
#[must_use]
pub fn contains_escapes(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes
        .windows(3)
        .any(|window| window[0] == 0xF4 && window[1] == 0x8F && window[2] >= 0xBE)
}

/// Carries Go string bytes in a `str`: valid UTF-8 is kept, and each byte of
/// an invalid sequence (or of an escape-range character) becomes its escape.
#[must_use]
pub fn bytes_to_str(bytes: &[u8]) -> Cow<'_, str> {
    if let Ok(text) = std::str::from_utf8(bytes) {
        if !contains_escapes(text) {
            return Cow::Borrowed(text);
        }
    }
    let mut out = String::with_capacity(bytes.len() + bytes.len() / 2);
    for chunk in bytes.utf8_chunks() {
        for c in chunk.valid().chars() {
            if is_escape(c) {
                let mut buffer = [0u8; 4];
                for byte in c.encode_utf8(&mut buffer).as_bytes() {
                    out.push(escape(*byte));
                }
            } else {
                out.push(c);
            }
        }
        for byte in chunk.invalid() {
            out.push(escape(*byte));
        }
    }
    Cow::Owned(out)
}

/// The exact Go string bytes a `str` produced by [`bytes_to_str`] carries:
/// each escape character becomes its raw byte, everything else its UTF-8.
#[must_use]
pub fn str_to_bytes(text: &str) -> Cow<'_, [u8]> {
    if !contains_escapes(text) {
        return Cow::Borrowed(text.as_bytes());
    }
    let mut out = Vec::with_capacity(text.len());
    for c in text.chars() {
        if is_escape(c) {
            // The escape range holds exactly the bytes 0x80..=0xFF.
            out.push((c as u32 - ESCAPE_BASE) as u8);
        } else {
            let mut buffer = [0u8; 4];
            out.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
        }
    }
    Cow::Owned(out)
}

/// The escape character for a raw byte `0x80..=0xFF`.
fn escape(byte: u8) -> char {
    debug_assert!(byte >= 0x80, "an ASCII byte is valid UTF-8");
    char::from_u32(ESCAPE_BASE + u32::from(byte)).expect("U+10FF80..=U+10FFFF are scalar values")
}

#[cfg(test)]
mod tests {
    use super::{bytes_to_str, contains_escapes, is_escape, str_to_bytes};
    use std::borrow::Cow;

    #[test]
    fn valid_utf8_without_escapes_is_borrowed_both_ways() {
        let text = "SELECT HEX('└─é')";
        assert!(matches!(bytes_to_str(text.as_bytes()), Cow::Borrowed(t) if t == text));
        assert!(matches!(str_to_bytes(text), Cow::Borrowed(b) if b == text.as_bytes()));
    }

    #[test]
    fn invalid_bytes_round_trip_exactly() {
        for bytes in [
            &b"SELECT '\xe9'"[..],
            b"a\xe9b\xff\x80",
            b"\xc3",
            b"\xe2\x94",
            b"\xf4\x8f\xbe\x85",
            b"\xf4\x8f\xbf\xbf\xe9\xf4\x8f",
            "é\u{10ff80}\u{10ffff}x".as_bytes(),
        ] {
            let text = bytes_to_str(bytes);
            assert_eq!(str_to_bytes(&text).as_ref(), bytes, "{bytes:?}");
        }
    }

    #[test]
    fn each_invalid_byte_is_one_escape_character() {
        let text = bytes_to_str(b"a\xe9b");
        let chars: Vec<char> = text.chars().collect();
        assert_eq!(chars.len(), 3);
        assert_eq!(chars[0], 'a');
        assert!(is_escape(chars[1]));
        assert_eq!(chars[2], 'b');
        assert!(contains_escapes(&text));
        assert!(!contains_escapes("é\u{10ff7f}"));
    }
}
