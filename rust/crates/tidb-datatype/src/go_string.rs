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

//! Immutable, byte-preserving Go string headers.
//!
//! Go strings may contain arbitrary bytes. Copying a string copies its
//! `(data,len)` header and retains the byte backing, while APIs such as
//! `stringutil.Copy` explicitly allocate a fresh backing. Rust's `String`
//! cannot represent that domain, so metadata and ENUM/SET values use this
//! dependency-leaf representation instead.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A Go string header retaining arbitrary bytes and immutable backing identity.
#[derive(Default)]
pub struct GoString {
    backing: Option<Arc<[u8]>>,
    start: usize,
    len: usize,
}

/// Byte view plus source-shaped interface-copy conversion used by APIs that
/// accept Go strings. A [`GoString`] implementation copies only its header;
/// UTF-8 and byte-container conveniences allocate a new immutable backing.
pub trait GoStringSource {
    /// Returns the unchanged Go string bytes.
    fn as_go_bytes(&self) -> &[u8];

    /// Copies the source string header/value into the native representation.
    fn to_go_string(&self) -> GoString;
}

impl GoString {
    /// Constructs a string from an owned byte allocation.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes.into();
        let len = bytes.len();
        if len == 0 {
            return Self::default();
        }
        Self {
            backing: Some(Arc::from(bytes)),
            start: 0,
            len,
        }
    }

    /// Returns the unchanged source bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.backing {
            Some(backing) => &backing[self.start..self.start + self.len],
            None => &[],
        }
    }

    /// Returns the source byte length.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the source byte length is zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Decodes this byte string as UTF-8 without replacement.
    pub fn as_utf8(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(self.as_bytes())
    }

    /// Returns a substring header sharing this string's immutable backing.
    ///
    /// Bounds are byte offsets, exactly as Go string slicing. Invalid or
    /// reversed bounds panic before constructing the result.
    #[must_use]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index.checked_add(1).expect("Go string slice overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(index) => index.checked_add(1).expect("Go string slice overflow"),
            Bound::Excluded(index) => *index,
            Bound::Unbounded => self.len,
        };
        assert!(start <= end, "Go string slice starts after its end");
        assert!(end <= self.len, "Go string slice index out of bounds");
        Self {
            backing: self.backing.clone(),
            start: self.start + start,
            len: end - start,
        }
    }

    /// Allocates a fresh copy of non-empty bytes, matching
    /// `stringutil.Copy`. Go returns the canonical empty string without an
    /// allocation when the input is empty.
    #[must_use]
    pub fn deep_copy(&self) -> Self {
        if self.is_empty() {
            Self::default()
        } else {
            Self::from_bytes(self.as_bytes().to_vec())
        }
    }

    /// Reports whether two headers retain the same immutable allocation.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (None, None) => true,
            (Some(left), Some(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// Converts invalid UTF-8 using Go's `encoding/json`/`range` rule: each
    /// invalid byte consumes one byte and emits one U+FFFD. Rust's ordinary
    /// lossy conversion instead collapses some malformed subsequences.
    #[must_use]
    pub fn to_utf8_lossy_go(&self) -> String {
        let bytes = self.as_bytes();
        let mut offset = 0;
        let mut output = String::with_capacity(bytes.len());
        while offset < bytes.len() {
            match std::str::from_utf8(&bytes[offset..]) {
                Ok(valid) => {
                    output.push_str(valid);
                    break;
                }
                Err(error) => {
                    let valid_len = error.valid_up_to();
                    let valid = std::str::from_utf8(&bytes[offset..offset + valid_len])
                        .expect("Utf8Error valid prefix is UTF-8");
                    output.push_str(valid);
                    output.push('\u{fffd}');
                    // Go utf8.DecodeRuneInString reports size 1 for every
                    // invalid encoding, including truncated multibyte input.
                    offset += valid_len + 1;
                }
            }
        }
        output
    }

    /// Returns the exact JSON string token emitted by Go 1.25
    /// `encoding/json` with HTML escaping enabled.
    #[must_use]
    pub fn to_go_json_literal(&self) -> String {
        let bytes = self.as_bytes();
        let mut output = Vec::with_capacity(bytes.len() + 2);
        output.push(b'"');
        let mut index = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            if byte < 0x80 {
                append_go_json_ascii(&mut output, byte);
                index += 1;
                continue;
            }

            let rest = &bytes[index..];
            match std::str::from_utf8(rest) {
                Ok(valid) => {
                    append_go_json_valid_utf8(&mut output, valid);
                    break;
                }
                Err(error) if error.valid_up_to() != 0 => {
                    let valid_len = error.valid_up_to();
                    let valid = std::str::from_utf8(&rest[..valid_len])
                        .expect("Utf8Error valid prefix is UTF-8");
                    append_go_json_valid_utf8(&mut output, valid);
                    index += valid_len;
                }
                Err(_) => {
                    output.extend_from_slice(b"\\ufffd");
                    index += 1;
                }
            }
        }
        output.push(b'"');
        String::from_utf8(output).expect("Go JSON string tokens are UTF-8")
    }
}

fn append_go_json_ascii(output: &mut Vec<u8>, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    match byte {
        b'"' | b'\\' => output.extend_from_slice(&[b'\\', byte]),
        b'\x08' => output.extend_from_slice(b"\\b"),
        b'\x0c' => output.extend_from_slice(b"\\f"),
        b'\n' => output.extend_from_slice(b"\\n"),
        b'\r' => output.extend_from_slice(b"\\r"),
        b'\t' => output.extend_from_slice(b"\\t"),
        byte if byte < 0x20 || matches!(byte, b'<' | b'>' | b'&') => {
            output.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[usize::from(byte >> 4)],
                HEX[usize::from(byte & 0x0f)],
            ]);
        }
        _ => output.push(byte),
    }
}

fn append_go_json_valid_utf8(output: &mut Vec<u8>, valid: &str) {
    let mut encoded = [0_u8; 4];
    for character in valid.chars() {
        if character.is_ascii() {
            append_go_json_ascii(output, character as u8);
        } else {
            match character {
                '\u{2028}' => output.extend_from_slice(b"\\u2028"),
                '\u{2029}' => output.extend_from_slice(b"\\u2029"),
                _ => output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes()),
            }
        }
    }
}

impl Clone for GoString {
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.clone(),
            start: self.start,
            len: self.len,
        }
    }
}

impl PartialEq for GoString {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for GoString {}

impl PartialEq<str> for GoString {
    fn eq(&self, other: &str) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<&str> for GoString {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl PartialEq<String> for GoString {
    fn eq(&self, other: &String) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<GoString> for str {
    fn eq(&self, other: &GoString) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<GoString> for &str {
    fn eq(&self, other: &GoString) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<GoString> for String {
    fn eq(&self, other: &GoString) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialOrd for GoString {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GoString {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl Hash for GoString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl fmt::Debug for GoString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("GoString")
            .field(&self.as_bytes())
            .finish()
    }
}

impl fmt::Display for GoString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_utf8_lossy_go())
    }
}

impl AsRef<[u8]> for GoString {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for GoString {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_bytes(bytes)
    }
}

impl From<&[u8]> for GoString {
    fn from(bytes: &[u8]) -> Self {
        Self::from_bytes(bytes.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for GoString {
    fn from(bytes: &[u8; N]) -> Self {
        Self::from_bytes(bytes.to_vec())
    }
}

impl<const N: usize> From<[u8; N]> for GoString {
    fn from(bytes: [u8; N]) -> Self {
        Self::from_bytes(bytes.to_vec())
    }
}

impl From<String> for GoString {
    fn from(text: String) -> Self {
        Self::from_bytes(text.into_bytes())
    }
}

impl From<&String> for GoString {
    fn from(text: &String) -> Self {
        Self::from(text.as_str())
    }
}

impl From<&str> for GoString {
    fn from(text: &str) -> Self {
        Self::from_bytes(text.as_bytes().to_vec())
    }
}

impl From<&GoString> for GoString {
    fn from(source: &GoString) -> Self {
        source.clone()
    }
}

impl GoStringSource for GoString {
    fn as_go_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    fn to_go_string(&self) -> GoString {
        self.clone()
    }
}

impl GoStringSource for str {
    fn as_go_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    fn to_go_string(&self) -> GoString {
        GoString::from(self)
    }
}

impl GoStringSource for String {
    fn as_go_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    fn to_go_string(&self) -> GoString {
        GoString::from(self.as_str())
    }
}

impl GoStringSource for [u8] {
    fn as_go_bytes(&self) -> &[u8] {
        self
    }

    fn to_go_string(&self) -> GoString {
        GoString::from(self)
    }
}

impl GoStringSource for Vec<u8> {
    fn as_go_bytes(&self) -> &[u8] {
        self
    }

    fn to_go_string(&self) -> GoString {
        GoString::from(self.as_slice())
    }
}

impl<const N: usize> GoStringSource for [u8; N] {
    fn as_go_bytes(&self) -> &[u8] {
        self
    }

    fn to_go_string(&self) -> GoString {
        GoString::from(self)
    }
}

impl<T> GoStringSource for &T
where
    T: GoStringSource + ?Sized,
{
    fn as_go_bytes(&self) -> &[u8] {
        (*self).as_go_bytes()
    }

    fn to_go_string(&self) -> GoString {
        (*self).to_go_string()
    }
}

impl Serialize for GoString {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_json::value::RawValue::from_string(self.to_go_json_literal())
            .map_err(serde::ser::Error::custom)?
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for GoString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::from)
    }
}

#[cfg(test)]
mod tests {
    use super::GoString;

    #[test]
    fn structural_clone_slice_and_explicit_copy_preserve_source_backing_rules() {
        let source = GoString::from_bytes(vec![b'a', 0xff, b'b']);
        let clone = source.clone();
        assert!(source.backing_ptr_eq(&clone));
        assert_eq!(clone.as_bytes(), [b'a', 0xff, b'b']);

        let suffix = source.slice(1..);
        assert!(source.backing_ptr_eq(&suffix));
        assert_eq!(suffix.as_bytes(), [0xff, b'b']);

        let copied = source.deep_copy();
        assert!(!source.backing_ptr_eq(&copied));
        assert_eq!(source, copied);
    }

    #[test]
    fn go_lossy_conversion_replaces_each_invalid_byte() {
        let truncated = GoString::from_bytes(vec![0xe2, 0x82]);
        assert_eq!(truncated.to_utf8_lossy_go(), "\u{fffd}\u{fffd}");
        assert_eq!(
            serde_json::to_vec(&truncated).unwrap(),
            br#""\ufffd\ufffd""#
        );

        let escaped = GoString::from("<&>\u{2028}\u{2029}");
        assert_eq!(
            serde_json::to_string(&escaped).unwrap(),
            r#""\u003c\u0026\u003e\u2028\u2029""#
        );
    }
}
