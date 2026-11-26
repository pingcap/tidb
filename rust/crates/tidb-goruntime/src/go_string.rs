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

use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::{Bound, Range, RangeBounds};
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tidb_hack::MapValueLayout;

/// A Go string header retaining arbitrary bytes and immutable backing identity.
#[derive(Clone, Default)]
pub struct GoString(GoStringBacking);

#[derive(Clone, Default)]
enum GoStringBacking {
    #[default]
    Empty,
    // A copied slice needs only one allocation. Its range stays compact so
    // the fat Arc fits in the same space as a shared vector and its range.
    Copy(Arc<[u8]>, Range<u32>),
    Vec(Arc<Vec<u8>>, Range<usize>),
    Bytes(Arc<bytes::Bytes>, Range<usize>),
    Utf8(Arc<String>, Range<usize>),
    Inline { bytes: [u8; INLINE_SNAPSHOT_BYTES], len: u8 },
}

// Fit within the allocation-backed variants on both pointer widths.
const INLINE_SNAPSHOT_BYTES: usize = 4 * size_of::<usize>() - 2;

impl MapValueLayout for GoString {
    const SOURCE_SIZE: usize = 16;
    const SOURCE_ALIGN: usize = 8;
}

/// Byte view plus source-shaped interface-copy conversion used by APIs that
/// accept Go strings. A [`GoString`] implementation copies only its header;
/// UTF-8 and byte-container conveniences copy their bytes into the result.
pub trait GoStringSource {
    /// Returns the unchanged Go string bytes.
    fn as_go_bytes(&self) -> &[u8];

    /// Copies the source string header/value into the native representation.
    fn to_go_string(&self) -> GoString;
}

const MIN_SIMD_UTF8_LEN: usize = 64;

/// Iterator over a Go string's Unicode code points.
#[derive(Clone)]
pub struct GoChars<'a> {
    remaining: &'a [u8],
}

impl Iterator for GoChars<'_> {
    type Item = char;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let (&first, tail) = self.remaining.split_first()?;
        if first.is_ascii() {
            self.remaining = tail;
            return Some(char::from(first));
        }
        if !(0xc2..=0xf4).contains(&first) {
            self.remaining = tail;
            return Some(char::REPLACEMENT_CHARACTER);
        }
        match decode_multibyte(first, tail) {
            Some((ch, tail_width)) => {
                self.remaining = &tail[tail_width..];
                Some(ch)
            }
            None => {
                self.remaining = tail;
                Some(char::REPLACEMENT_CHARACTER)
            }
        }
    }

    #[inline]
    fn count(self) -> usize {
        if self.remaining.len() <= 1 || self.remaining.is_ascii() {
            return self.remaining.len();
        }
        if self.remaining.len() >= MIN_SIMD_UTF8_LEN {
            if let Some((valid, incomplete)) = utf8_with_incomplete_tail(self.remaining) {
                return valid.chars().count() + incomplete;
            }
        }
        let mut count = 0;
        for _ in self {
            count += 1;
        }
        count
    }

    #[inline]
    fn fold<B, F>(self, mut accumulator: B, mut fold: F) -> B
    where
        F: FnMut(B, char) -> B,
    {
        if self.remaining.is_ascii() {
            return self
                .remaining
                .iter()
                .fold(accumulator, |acc, &byte| fold(acc, char::from(byte)));
        }
        if self.remaining.len() >= MIN_SIMD_UTF8_LEN {
            if let Some((valid, incomplete)) = utf8_with_incomplete_tail(self.remaining) {
                accumulator = valid.chars().fold(accumulator, &mut fold);
                for _ in 0..incomplete {
                    accumulator = fold(accumulator, char::REPLACEMENT_CHARACTER);
                }
                return accumulator;
            }
        }
        for ch in self {
            accumulator = fold(accumulator, ch);
        }
        accumulator
    }
}

// The caller has checked the lead byte is in 0xc2..=0xf4. The width counts
// the continuation bytes in tail.
#[inline(always)]
fn decode_multibyte(first: u8, tail: &[u8]) -> Option<(char, usize)> {
    let (value, tail_width) = if first < 0xe0 {
        let &[second, ..] = tail else {
            return None;
        };
        if second & 0xc0 != 0x80 {
            return None;
        }
        ((u32::from(first & 0x1f) << 6) | u32::from(second & 0x3f), 1)
    } else if first < 0xf0 {
        let &[second, third, ..] = tail else {
            return None;
        };
        if ((second ^ 0x80) | (third ^ 0x80)) & 0xc0 != 0 || (first == 0xe0 && second < 0xa0) {
            return None;
        }
        (
            (u32::from(first & 0x0f) << 12)
                | (u32::from(second & 0x3f) << 6)
                | u32::from(third & 0x3f),
            2,
        )
    } else {
        let &[second, third, fourth, ..] = tail else {
            return None;
        };
        if ((second ^ 0x80) | (third ^ 0x80) | (fourth ^ 0x80)) & 0xc0 != 0
            || (first == 0xf0 && second < 0x90)
        {
            return None;
        }
        (
            (u32::from(first & 0x07) << 18)
                | (u32::from(second & 0x3f) << 12)
                | (u32::from(third & 0x3f) << 6)
                | u32::from(fourth & 0x3f),
            3,
        )
    };
    // Reject surrogate values and code points above U+10FFFF.
    char::from_u32(value).map(|ch| (ch, tail_width))
}

/// Iterates a Go string, replacing each invalid byte with one U+FFFD.
#[must_use]
#[inline]
pub fn go_chars<T>(input: &T) -> GoChars<'_>
where
    T: GoStringSource + ?Sized,
{
    GoChars {
        remaining: input.as_go_bytes(),
    }
}

/// Decodes a Go string as UTF-8, replacing each invalid byte with one U+FFFD.
///
/// Go's string iteration advances by one byte after an invalid encoding. This
/// differs from [`String::from_utf8_lossy`], which may replace a malformed
/// multi-byte subsequence with a single replacement character.
#[must_use]
pub fn decode_go_utf8_lossy(input: impl GoStringSource) -> String {
    let bytes = input.as_go_bytes();
    // A replacement takes three bytes. Reserve for the worst case on short
    // inputs so malformed text does not grow the buffer while being decoded.
    let capacity = if bytes.len() <= 8 {
        bytes.len() * char::REPLACEMENT_CHARACTER.len_utf8()
    } else {
        bytes.len()
    };
    let mut output = String::with_capacity(capacity);
    for chunk in bytes.utf8_chunks() {
        output.push_str(chunk.valid());
        for _ in chunk.invalid() {
            output.push(char::REPLACEMENT_CHARACTER);
        }
    }
    output
}

impl GoString {
    /// Copies a short, potentially mutable source into the value itself.
    /// Returns `None` when the bytes do not fit in the inline storage.
    #[doc(hidden)]
    #[inline]
    pub fn try_inline_snapshot(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > INLINE_SNAPSHOT_BYTES {
            return None;
        }
        Some(Self(Self::inline_backing(bytes)))
    }

    #[inline]
    fn inline_backing(bytes: &[u8]) -> GoStringBacking {
        if bytes.is_empty() {
            return GoStringBacking::Empty;
        }
        let mut buffer = [0; INLINE_SNAPSHOT_BYTES];
        buffer[..bytes.len()].copy_from_slice(bytes);
        GoStringBacking::Inline { bytes: buffer, len: bytes.len() as u8 }
    }

    /// Constructs a string from an owned byte allocation.
    #[must_use]
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        let mut bytes = bytes.into();
        let len = bytes.len();
        if len == 0 {
            return Self::default();
        }
        bytes.shrink_to_fit();
        Self::from_shared_vec(Arc::new(bytes), 0..len)
    }

    /// Retains an immutable view of an existing vector allocation.
    #[inline]
    #[must_use]
    pub fn from_shared_vec(bytes: Arc<Vec<u8>>, range: Range<usize>) -> Self {
        assert!(range.start <= range.end && range.end <= bytes.len());
        Self(GoStringBacking::Vec(bytes, range))
    }

    /// Retains a string view over a subslice of an immutable vector.
    ///
    /// Panics if the slice is outside the backing vector.
    #[inline]
    #[must_use]
    pub fn from_shared_subslice(owner: &Arc<Vec<u8>>, bytes: &[u8]) -> Self {
        let start = bytes
            .as_ptr()
            .addr()
            .checked_sub(owner.as_ptr().addr())
            .expect("slice is outside its backing buffer");
        assert!(start <= owner.len() && bytes.len() <= owner.len() - start);
        Self::from_shared_vec(Arc::clone(owner), start..start + bytes.len())
    }

    /// Retains an immutable string backed by an existing byte buffer.
    #[must_use]
    pub fn from_shared_bytes(bytes: bytes::Bytes) -> Self {
        let len = bytes.len();
        if len == 0 {
            return Self::default();
        }
        Self(GoStringBacking::Bytes(Arc::new(bytes), 0..len))
    }

    /// Retains a range of an already shared immutable buffer.
    #[inline]
    #[must_use]
    pub fn from_shared_bytes_range(bytes: Arc<bytes::Bytes>, range: Range<usize>) -> Self {
        assert!(range.start <= range.end && range.end <= bytes.len());
        Self(GoStringBacking::Bytes(bytes, range))
    }

    /// Returns the unchanged source bytes.
    #[inline]
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match &self.0 {
            GoStringBacking::Empty => &[],
            GoStringBacking::Inline { bytes, len } => &bytes[..usize::from(*len)],
            GoStringBacking::Copy(bytes, range) => &bytes[range.start as usize..range.end as usize],
            GoStringBacking::Vec(bytes, range) => &bytes[range.clone()],
            GoStringBacking::Bytes(bytes, range) => &bytes[range.clone()],
            GoStringBacking::Utf8(text, range) => &text.as_bytes()[range.clone()],
        }
    }

    /// Returns the source byte length.
    #[must_use]
    pub const fn len(&self) -> usize {
        match &self.0 {
            GoStringBacking::Empty => 0,
            GoStringBacking::Inline { len, .. } => *len as usize,
            GoStringBacking::Copy(_, range) => (range.end - range.start) as usize,
            GoStringBacking::Vec(_, range)
            | GoStringBacking::Bytes(_, range)
            | GoStringBacking::Utf8(_, range) => range.end - range.start,
        }
    }

    /// Returns whether the source byte length is zero.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Decodes this byte string as UTF-8 without replacement.
    pub fn as_utf8(&self) -> Result<&str, std::str::Utf8Error> {
        if let Some(text) = self.known_utf8() {
            Ok(text)
        } else {
            std::str::from_utf8(self.as_bytes())
        }
    }

    /// Returns a view when the backing and both byte boundaries prove UTF-8.
    /// An arbitrary byte-backed value may need validation even if it is valid.
    #[inline]
    pub fn known_utf8(&self) -> Option<&str> {
        match &self.0 {
            GoStringBacking::Empty => Some(""),
            GoStringBacking::Utf8(text, range) => text.get(range.clone()),
            _ => None,
        }
    }

    /// Returns a substring, sharing its immutable allocation when available.
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
            Bound::Unbounded => self.len(),
        };
        assert!(start <= end, "Go string slice starts after its end");
        assert!(end <= self.len(), "Go string slice index out of bounds");
        Self(match &self.0 {
            GoStringBacking::Empty => GoStringBacking::Empty,
            GoStringBacking::Inline { bytes, .. } => Self::inline_backing(&bytes[start..end]),
            GoStringBacking::Copy(bytes, range) => GoStringBacking::Copy(
                Arc::clone(bytes),
                range.start + start as u32..range.start + end as u32,
            ),
            GoStringBacking::Vec(bytes, range) => GoStringBacking::Vec(
                Arc::clone(bytes),
                range.start + start..range.start + end,
            ),
            GoStringBacking::Bytes(bytes, range) => GoStringBacking::Bytes(
                Arc::clone(bytes),
                range.start + start..range.start + end,
            ),
            GoStringBacking::Utf8(text, range) => GoStringBacking::Utf8(
                Arc::clone(text),
                range.start + start..range.start + end,
            ),
        })
    }

    /// Allocates a fresh copy of non-empty bytes, matching
    /// `stringutil.Copy`. Go returns the canonical empty string without an
    /// allocation when the input is empty.
    #[must_use]
    pub fn deep_copy(&self) -> Self {
        Self::from(self.as_bytes())
    }

    /// Reports whether two headers retain the same immutable allocation.
    #[must_use]
    pub fn backing_ptr_eq(&self, other: &Self) -> bool {
        use GoStringBacking::{Bytes, Copy, Empty, Utf8, Vec};
        match (&self.0, &other.0) {
            (Empty, Empty) => true,
            (Copy(left, _), Copy(right, _)) => Arc::ptr_eq(left, right),
            (Vec(left, _), Vec(right, _)) => Arc::ptr_eq(left, right),
            (Bytes(left, _), Bytes(right, _)) => Arc::ptr_eq(left, right),
            (Utf8(left, _), Utf8(right, _)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }

    /// Converts invalid UTF-8 using Go's `encoding/json`/`range` rule: each
    /// invalid byte consumes one byte and emits one U+FFFD. Rust's ordinary
    /// lossy conversion instead collapses some malformed subsequences.
    #[must_use]
    pub fn to_utf8_lossy_go(&self) -> String {
        decode_go_utf8_lossy(self)
    }

    /// Returns the exact JSON string token emitted by Go 1.25
    /// `encoding/json` with HTML escaping enabled.
    #[must_use]
    pub fn to_go_json_literal(&self) -> String {
        let bytes = self.as_bytes();
        // The first few words distinguish long plain spans from dense
        // escapes. The scalar writer is cheaper for dense escapes.
        let has_plain_span = bytes.chunks_exact(8).take(4).any(|word| {
            let word = u64::from_le_bytes(word.try_into().unwrap());
            let stops = json_word_stops(word);
            // One separator candidate still leaves room to copy ordinary
            // Unicode in spans without decoding every character.
            stops == 0 || (stops & word != 0 && stops & stops.wrapping_sub(1) == 0)
        });
        if has_plain_span {
            if let Some((valid, incomplete)) = utf8_with_incomplete_tail(bytes) {
                let mut output = String::with_capacity(bytes.len() + 2);
                output.push('"');
                append_go_json_valid_utf8(&mut output, valid);
                for _ in 0..incomplete {
                    output.push_str("\\ufffd");
                }
                output.push('"');
                return output;
            }
        }
        encode_go_json_scalar(bytes)
    }
}

fn utf8_with_incomplete_tail(bytes: &[u8]) -> Option<(&str, usize)> {
    match simdutf8::compat::from_utf8(bytes) {
        Ok(valid) => Some((valid, 0)),
        Err(error) if error.error_len().is_none() => {
            // Go consumes each byte of an incomplete final character separately.
            let (prefix, incomplete) = bytes.split_at(error.valid_up_to());
            simdutf8::basic::from_utf8(prefix)
                .ok()
                .map(|valid| (valid, incomplete.len()))
        }
        Err(_) => None,
    }
}

fn encode_go_json_scalar(bytes: &[u8]) -> String {
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
                append_go_json_scalar_utf8(&mut output, valid);
                break;
            }
            Err(error) if error.valid_up_to() != 0 => {
                let valid_len = error.valid_up_to();
                let valid = std::str::from_utf8(&rest[..valid_len])
                    .expect("Utf8Error valid prefix is UTF-8");
                append_go_json_scalar_utf8(&mut output, valid);
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

fn append_go_json_scalar_utf8(output: &mut Vec<u8>, valid: &str) {
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

const JSON_CONTROL_ESCAPES: [&str; 32] = [
    "\\u0000", "\\u0001", "\\u0002", "\\u0003", "\\u0004", "\\u0005", "\\u0006", "\\u0007",
    "\\b", "\\t", "\\n", "\\u000b", "\\f", "\\r", "\\u000e", "\\u000f",
    "\\u0010", "\\u0011", "\\u0012", "\\u0013", "\\u0014", "\\u0015", "\\u0016", "\\u0017",
    "\\u0018", "\\u0019", "\\u001a", "\\u001b", "\\u001c", "\\u001d", "\\u001e", "\\u001f",
];

fn append_go_json_valid_utf8(output: &mut String, valid: &str) {
    let bytes = valid.as_bytes();
    let mut start = 0;
    let chunks = bytes.chunks_exact(8);
    let tail = chunks.remainder();
    for (block, chunk) in chunks.enumerate() {
        let stops = json_word_stops(u64::from_le_bytes(chunk.try_into().unwrap()));
        if stops != 0 {
            append_go_json_escapes(output, valid, &mut start, block * 8, stops);
        }
    }
    if !tail.is_empty() {
        let mut padded = [b' '; 8];
        padded[..tail.len()].copy_from_slice(tail);
        let stops = json_word_stops(u64::from_le_bytes(padded));
        append_go_json_escapes(output, valid, &mut start, bytes.len() - tail.len(), stops);
    }
    output.push_str(&valid[start..]);
}

// A stop is ASCII or a UTF-8 lead byte, so it lies on a character boundary.
#[inline]
fn append_go_json_escapes(
    output: &mut String,
    valid: &str,
    start: &mut usize,
    offset: usize,
    mut stops: u64,
) {
    let bytes = valid.as_bytes();
    while stops != 0 {
        let index = offset + stops.trailing_zeros() as usize / 8;
        stops &= stops - 1;
        let (escape, width) = if bytes[index] == 0xe2 {
            match bytes.get(index + 1..index + 3) {
                Some([0x80, 0xa8]) => ("\\u2028", 3),
                Some([0x80, 0xa9]) => ("\\u2029", 3),
                _ => continue,
            }
        } else {
            let Some(escape) = json_ascii_escape(bytes[index]) else {
                continue;
            };
            (escape, 1)
        };
        if *start != index {
            output.push_str(&valid[*start..index]);
        }
        output.push_str(escape);
        *start = index + width;
    }
}

#[inline]
fn json_ascii_escape(byte: u8) -> Option<&'static str> {
    Some(match byte {
        0..=31 => JSON_CONTROL_ESCAPES[byte as usize],
        b'"' => "\\\"",
        b'\\' => "\\\\",
        b'<' => "\\u003c",
        b'>' => "\\u003e",
        b'&' => "\\u0026",
        _ => return None,
    })
}

#[inline]
fn json_word_stops(word: u64) -> u64 {
    const HIGH: u64 = 0x8080_8080_8080_8080;
    const LOW: u64 = 0x0101_0101_0101_0101;
    const COMPLETE_SEPARATOR: u64 = 0x0000_ffff_ffff_ffff;
    // Set each byte's high bit before subtracting to isolate the borrow.
    let at_least_space = (word | HIGH).wrapping_sub(0x2020_2020_2020_2020);
    let controls = !(at_least_space | word) & HIGH;
    // A borrow can also flag a neighboring byte. The writer checks each
    // candidate before emitting an escape.
    let zero_bytes = |difference: u64| difference.wrapping_sub(LOW) & !difference & HIGH;
    let ascii_stops = controls
        | zero_bytes((word & !(LOW * 4)) ^ (LOW * u64::from(b'"')))
        | zero_bytes((word & !(LOW * 2)) ^ (LOW * u64::from(b'<')))
        | zero_bytes(word ^ (LOW * u64::from(b'\\')));
    // U+2028 and U+2029 differ only in the last bit. A lead byte in the
    // final two positions may complete the match in the next word.
    let separator_tail = ((word >> 8) ^ (LOW * 0x80))
        | (((word >> 16) & !LOW) ^ (LOW * 0xa8));
    let separators = zero_bytes((word ^ (LOW * 0xe2)) | (separator_tail & COMPLETE_SEPARATOR));
    ascii_stops | separators
}

impl PartialEq for GoString {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        let left = self.as_bytes();
        let right = other.as_bytes();
        if left.len() != right.len() {
            return false;
        }
        left.as_ptr() == right.as_ptr()
            || ((left.len() < 64 || left.last_chunk::<8>() == right.last_chunk::<8>())
                && left == right)
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
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GoString {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        fn same_start<T: ?Sized, Index: Ord>(
            a: &Arc<T>,
            a_range: &Range<Index>,
            b: &Arc<T>,
            b_range: &Range<Index>,
        ) -> Option<Ordering> {
            (Arc::ptr_eq(a, b) && a_range.start == b_range.start)
                .then(|| a_range.end.cmp(&b_range.end))
        }
        use GoStringBacking::{Bytes, Copy, Utf8, Vec};
        let shared = match (&self.0, &other.0) {
            (Vec(a, ar), Vec(b, br)) => same_start(a, ar, b, br),
            (Bytes(a, ar), Bytes(b, br)) => same_start(a, ar, b, br),
            (Utf8(a, ar), Utf8(b, br)) => same_start(a, ar, b, br),
            (Copy(a, ar), Copy(b, br)) => same_start(a, ar, b, br),
            _ => None,
        };
        if let Some(order) = shared {
            return order;
        }
        let left = self.as_bytes();
        let right = other.as_bytes();
        if left.as_ptr() == right.as_ptr() {
            return left.len().cmp(&right.len());
        }
        left.cmp(right)
    }
}

impl Hash for GoString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_bytes().hash(state);
    }
}

impl Borrow<[u8]> for GoString {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
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
    #[inline]
    fn from(bytes: &[u8]) -> Self {
        if bytes.is_empty() {
            Self::default()
        } else if let Ok(len) = u32::try_from(bytes.len()) {
            Self(GoStringBacking::Copy(Arc::from(bytes), 0..len))
        } else {
            Self::from_bytes(bytes.to_vec())
        }
    }
}

impl<const N: usize> From<&[u8; N]> for GoString {
    fn from(bytes: &[u8; N]) -> Self {
        Self::from(bytes.as_slice())
    }
}

impl<const N: usize> From<[u8; N]> for GoString {
    fn from(bytes: [u8; N]) -> Self {
        Self::from(bytes.as_slice())
    }
}

impl From<String> for GoString {
    fn from(mut text: String) -> Self {
        if text.is_empty() {
            return Self::default();
        }
        text.shrink_to_fit();
        let end = text.len();
        Self(GoStringBacking::Utf8(Arc::new(text), 0..end))
    }
}

impl From<&String> for GoString {
    fn from(text: &String) -> Self {
        Self::from(text.as_str())
    }
}

impl From<&str> for GoString {
    fn from(text: &str) -> Self {
        Self::from(text.as_bytes())
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

    #[inline]
    fn to_go_string(&self) -> GoString {
        self.as_bytes().to_go_string()
    }
}

impl GoStringSource for String {
    fn as_go_bytes(&self) -> &[u8] {
        self.as_bytes()
    }

    #[inline]
    fn to_go_string(&self) -> GoString {
        self.as_bytes().to_go_string()
    }
}

impl GoStringSource for [u8] {
    fn as_go_bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    fn to_go_string(&self) -> GoString {
        GoString::try_inline_snapshot(self).unwrap_or_else(|| GoString::from(self))
    }
}

impl GoStringSource for Vec<u8> {
    fn as_go_bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    fn to_go_string(&self) -> GoString {
        self.as_slice().to_go_string()
    }
}

impl<const N: usize> GoStringSource for [u8; N] {
    fn as_go_bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    fn to_go_string(&self) -> GoString {
        self.as_slice().to_go_string()
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
    use super::{GoString, INLINE_SNAPSHOT_BYTES, go_chars};
    use std::{ops::Range, sync::Arc};

    #[test]
    fn string_equality_respects_shared_ranges_and_independent_buffers() {
        let text = GoString::from("abcdef".repeat(64));
        assert_eq!(text, text.clone());
        assert_eq!(text, text.deep_copy());
        assert_ne!(text.slice(..10), text.slice(..11));
        assert_ne!(text.slice(..10), text.slice(1..11));
        assert_eq!(text.slice(..10), text.slice(6..16));
    }

    #[test]
    fn string_order_compares_shared_prefix_lengths_and_shifted_bytes() {
        let text = GoString::from_bytes([0xff, 0x80, 0, 0x7f]);
        assert!(text.slice(..2) < text);
        assert!(text > text.slice(..0));
        assert!(text.slice(..1) > text.slice(1..));
        assert!(text.slice(1..) < text);
        assert_eq!(text.cmp(&text.deep_copy()), std::cmp::Ordering::Equal);
    }

    #[test]
    fn count_and_fold_use_the_remaining_go_runes() {
        let ascii = "a".repeat(64);
        let unicode = "é汉🐹".repeat(10);
        let cases = [
            (ascii.as_bytes().to_vec(), ascii),
            (unicode.as_bytes().to_vec(), unicode.clone()),
            (
                [unicode.as_bytes(), b"\xf0\x9f"].concat(),
                format!("{unicode}\u{fffd}\u{fffd}"),
            ),
            (
                b"a\xed\xa0\x80\xc0\xaf\xf4\x90\x80\x80z".to_vec(),
                format!("a{}z", "\u{fffd}".repeat(9)),
            ),
        ];
        for (bytes, expected) in cases {
            let mut chars = go_chars(bytes.as_slice());
            let mut expected = expected.chars();
            assert_eq!(chars.next(), expected.next());
            assert_eq!(chars.clone().count(), expected.clone().count());
            let folded = chars.fold(String::new(), |mut output, ch| {
                output.push(ch);
                output
            });
            assert_eq!(folded, expected.as_str());
        }
    }

    #[test]
    fn json_span_scan_finds_escape_bytes_at_every_word_offset() {
        for offset in 0..8 {
            for byte in 0..=u8::MAX {
                let mut word = [b'x'; 8];
                word[offset] = byte;
                let must_stop = byte < b' '
                    || matches!(byte, b'"' | b'\\' | b'<' | b'>' | b'&')
                    || (byte == 0xe2 && offset >= 6);
                assert_eq!(
                    super::json_word_stops(u64::from_le_bytes(word)),
                    if must_stop { 0x80 << (offset * 8) } else { 0 }
                );
            }
        }
        for offset in 0..8 {
            let prefix = "x".repeat(32 + offset);
            let token = GoString::from(format!("{prefix}—\u{2028}→\u{2029}"));
            assert_eq!(
                token.to_go_json_literal(),
                format!(r#""{prefix}—\u2028→\u2029""#)
            );
        }
        let token = GoString::from("01234567\"#<= >?");
        assert_eq!(
            token.to_go_json_literal(),
            r##""01234567\"#\u003c= \u003e?""##
        );
        let token = GoString::from_bytes(b"leading plain text\xe2\x82".to_vec());
        assert_eq!(token.to_go_json_literal(), r#""leading plain text\ufffd\ufffd""#);
    }

    #[test]
    fn structural_clone_slice_and_explicit_copy_preserve_source_backing_rules() {
        for source in [
            GoString::from(b"a\xffb"),
            GoString::from_bytes(vec![b'a', 0xff, b'b']),
            GoString::from_shared_bytes(bytes::Bytes::from_static(b"a\xffb")),
        ] {
            let clone = source.clone();
            assert!(source.backing_ptr_eq(&clone));
            assert_eq!(clone.as_bytes(), [b'a', 0xff, b'b']);

            let suffix = source.slice(1..);
            assert!(source.backing_ptr_eq(&suffix));
            assert_eq!(suffix.as_bytes(), [0xff, b'b']);
            let tail = suffix.slice(1..=1);
            assert!(source.backing_ptr_eq(&tail));
            assert_eq!(tail.as_bytes(), b"b");

            let copied = suffix.deep_copy();
            assert!(!suffix.backing_ptr_eq(&copied));
            assert_eq!(suffix, copied);
        }
    }

    #[test]
    fn source_conversion_copies_short_bytes_and_retains_existing_strings() {
        use super::{GoStringBacking, GoStringSource};

        let mut bytes = [0xff, b'a'];
        let copied = bytes.to_go_string();
        bytes[0] = b'b';
        assert!(matches!(copied.0, GoStringBacking::Inline { .. }));
        assert_eq!(copied.as_bytes(), &[0xff, b'a']);
        assert_eq!(bytes.to_go_string().as_bytes(), b"ba");

        let retained = GoString::from(&b"label"[..]);
        assert!(retained.backing_ptr_eq(&retained.to_go_string()));
    }

    #[test]
    fn inline_snapshot_preserves_arbitrary_bytes_without_growing_the_string() {
        let mut bytes = [0xff; INLINE_SNAPSHOT_BYTES];
        let snapshot = GoString::try_inline_snapshot(&bytes).unwrap();
        bytes.fill(0);
        assert_eq!(snapshot.len(), INLINE_SNAPSHOT_BYTES);
        assert_eq!(snapshot.as_bytes(), [0xff; INLINE_SNAPSHOT_BYTES]);
        assert_eq!(snapshot.slice(1..3).clone().as_bytes(), [0xff, 0xff]);
        assert!(snapshot.slice(..0).is_empty());
        assert!(GoString::try_inline_snapshot(&[0; INLINE_SNAPSHOT_BYTES + 1]).is_none());
        let allocated = size_of::<(Arc<Vec<u8>>, Range<usize>)>()
            .max(size_of::<(Arc<[u8]>, Range<u32>)>());
        assert!(size_of::<GoString>() <= allocated + align_of::<usize>());
    }

    #[test]
    fn owned_text_proves_utf8_only_at_valid_byte_boundaries() {
        let text = GoString::from(String::from("a界b"));
        assert_eq!(text.known_utf8(), Some("a界b"));
        assert_eq!(text.slice(1..4).known_utf8(), Some("界"));

        let split = text.slice(2..4);
        assert!(split.backing_ptr_eq(&text));
        assert_eq!(split.as_bytes(), &"界".as_bytes()[1..]);
        assert!(split.known_utf8().is_none());
        assert!(split.as_utf8().is_err());
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
