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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Byte-key and half-open range primitives translated from `pkg/kv/key.go`.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;

const CARRY_WORD: [u8; 8] = [u8::MAX; 8];

/// An owned TiDB KV key ordered lexicographically by unsigned bytes.
///
/// This is the Rust ownership form of Go's `type Key []byte`. Deriving
/// [`Clone`] performs the source `Key.Clone` deep copy because the bytes are
/// owned by a [`Vec`].
#[derive(Debug, Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Key(Vec<u8>);

impl std::borrow::Borrow<[u8]> for Key {
    fn borrow(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Key {
    /// Creates a key by taking ownership of, or copying into, a byte vector.
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
    }

    /// Returns the raw key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Returns the raw key bytes using the standard slice accessor name.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Returns the owned allocation capacity.
    ///
    /// Go's package contract exposes capacity indirectly through
    /// `KeyRangeSliceMemUsage`, so the Rust owner must retain it too.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Consumes the key and returns its owned bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Returns the next key in byte order by appending one zero byte.
    ///
    /// This is `pkg/kv/key.go`'s `Key.Next`, not the next distinct prefix.
    #[must_use]
    #[inline]
    pub fn next(&self) -> Self {
        if self.0.is_empty() {
            return Self(vec![0]);
        }
        let mut bytes = Vec::with_capacity(self.0.len() + 1);
        bytes.extend_from_slice(&self.0);
        bytes.push(0);
        Self(bytes)
    }

    /// Returns the first key after every key having this key as a prefix.
    ///
    /// The rightmost byte that is not `0xff` is incremented and each overflowed
    /// suffix byte becomes zero. If the key is empty or every byte is `0xff`,
    /// one zero byte is appended to the original key, exactly as Go
    /// `Key.PrefixNext` does.
    #[must_use]
    #[inline]
    pub fn prefix_next(&self) -> Self {
        let Some(&last) = self.0.last() else {
            return Self(vec![0]);
        };
        if last != u8::MAX {
            let mut bytes = self.0.clone();
            *bytes.last_mut().expect("nonempty key") = last + 1;
            return Self(bytes);
        }
        if let Ok(word) = <[u8; 8]>::try_from(self.0.as_slice()) {
            return match u64::from_be_bytes(word).checked_add(1) {
                Some(next) => Self(next.to_be_bytes().to_vec()),
                None => self.next(),
            };
        }
        if self.0.len() >= 24 && self.0.ends_with(&CARRY_WORD) {
            return self.prefix_next_long_carry();
        }
        let mut bytes = self.0.clone();
        for byte in bytes.iter_mut().rev() {
            *byte = byte.wrapping_add(1);
            if *byte != 0 {
                return Self(bytes);
            }
        }

        self.next()
    }

    fn prefix_next_long_carry(&self) -> Self {
        let (_, words) = self.0.as_rchunks::<8>();
        let carry_words = words
            .iter()
            .rev()
            .take_while(|&&word| word == CARRY_WORD)
            .count();
        let prefix = &self.0[..self.0.len() - 8 * carry_words];
        let Some(increment) = prefix.iter().rposition(|&byte| byte != u8::MAX) else {
            return self.next();
        };
        let mut bytes = Vec::with_capacity(self.0.len());
        bytes.extend_from_slice(&prefix[..increment]);
        bytes.push(prefix[increment] + 1);
        bytes.resize(self.0.len(), 0);
        Self(bytes)
    }

    /// Compares two keys using unsigned lexicographic byte order.
    pub fn compare(&self, another: &Self) -> Ordering {
        self.cmp(another)
    }

    /// Returns whether this key begins with `prefix`.
    pub fn has_prefix(&self, prefix: &Self) -> bool {
        self.0.starts_with(&prefix.0)
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl Deref for Key {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for Key {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<Key> for Vec<u8> {
    fn from(key: Key) -> Self {
        key.into_bytes()
    }
}

impl From<&[u8]> for Key {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl PartialEq<Vec<u8>> for Key {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_bytes() == other.as_slice()
    }
}

impl PartialEq<&[u8]> for Key {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_bytes() == *other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for Key {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_bytes() == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for Key {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_bytes() == other.as_slice()
    }
}

impl fmt::Display for Key {
    /// Formats the key as lowercase hexadecimal, matching Go `Key.String`.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            return Ok(());
        }
        if self.0.len() <= 16 {
            return write_short_key_hex(&self.0, formatter);
        }
        write_key_hex(&self.0, formatter)
    }
}

#[inline(never)]
fn write_short_key_hex(key: &[u8], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut hex = [0; 32];
    let len = tidb_goruntime::go_hex::encode(&mut hex, key);
    formatter.write_str(std::str::from_utf8(&hex[..len]).expect("hex is ASCII"))
}

// Keep the formatting buffer off the stack for empty keys.
#[inline(never)]
fn write_key_hex(key: &[u8], formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
    let mut hex = [0; 256];
    for bytes in key.chunks(hex.len() / 2) {
        let len = tidb_goruntime::go_hex::encode(&mut hex, bytes);
        formatter.write_str(std::str::from_utf8(&hex[..len]).expect("hex is ASCII"))?;
    }
    Ok(())
}

/// A half-open KV range where `start_key <= key < end_key`.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct KeyRange {
    /// Inclusive range boundary.
    pub start_key: Key,
    /// Exclusive range boundary.
    pub end_key: Key,
}

impl KeyRange {
    /// Creates a half-open key range.
    pub fn new(start_key: Key, end_key: Key) -> Self {
        Self { start_key, end_key }
    }

    /// Returns whether this range identifies exactly one point.
    ///
    /// This is the allocation-free branch structure from Go
    /// `KeyRange.IsPoint`: unequal-length bounds use `Key.Next`; equal-length
    /// bounds use `Key.PrefixNext` unless the start is all `0xff`.
    #[inline]
    pub fn is_point(&self) -> bool {
        let start = self.start_key.as_bytes();
        let end = self.end_key.as_bytes();

        if start.len() != end.len() {
            if start.is_empty() {
                return end == [0];
            }
            return start.len().checked_add(1) == Some(end.len())
                && end.get(start.len()) == Some(&0)
                && equal_key_prefix(start, &end[..start.len()]);
        }

        if let Ok(start) = <[u8; 8]>::try_from(start) {
            let end = u64::from_be_bytes(end.try_into().expect("equally sized point bounds"));
            return u64::from_be_bytes(start).checked_add(1) == Some(end);
        }
        if let Ok(start) = <[u8; 16]>::try_from(start) {
            let end = u128::from_be_bytes(end.try_into().expect("equally sized point bounds"));
            return u128::from_be_bytes(start).checked_add(1) == Some(end);
        }

        let Some((&last, start)) = start.split_last() else {
            return false;
        };
        let (&end_last, end) = end.split_last().expect("end has the same length as start");
        if last != u8::MAX {
            return last + 1 == end_last && equal_key_prefix(start, end);
        }
        end_last == 0 && point_after_carry(start, end)
    }
}

#[inline]
fn point_after_carry(start: &[u8], end: &[u8]) -> bool {
    let mut remaining = start.len();
    if remaining >= 16 {
        let (_, start_words) = start.as_rchunks::<8>();
        let (_, end_words) = end.as_rchunks::<8>();
        let carry_words = start_words
            .iter()
            .zip(end_words)
            .rev()
            .take_while(|&(left, right)| *left == CARRY_WORD && *right == [0; 8])
            .count();
        remaining -= 8 * carry_words;
    }
    for index in (0..remaining).rev() {
        if start[index] != u8::MAX {
            return start[index] + 1 == end[index]
                && equal_key_prefix(&start[..index], &end[..index]);
        }
        if end[index] != 0 {
            return false;
        }
    }
    false
}

#[inline]
fn equal_key_prefix(left: &[u8], right: &[u8]) -> bool {
    debug_assert_eq!(left.len(), right.len());
    if left.len() < 128 {
        return left == right;
    }
    let (left_blocks, left_tail) = left.as_chunks::<128>();
    let (right_blocks, right_tail) = right.as_chunks::<128>();
    for (left, right) in left_blocks.iter().zip(right_blocks) {
        let (left, _) = left.as_chunks::<8>();
        let (right, _) = right.as_chunks::<8>();
        let difference = left.iter().zip(right).fold(0, |difference, (&a, &b)| {
            difference | (u64::from_ne_bytes(a) ^ u64::from_ne_bytes(b))
        });
        if difference != 0 {
            return false;
        }
    }
    left_tail == right_tail
}

/// One key/value entry.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Entry {
    /// Entry key.
    pub key: Key,
    /// Entry value.
    pub value: Vec<u8>,
}

impl Entry {
    /// Creates one owned key/value entry.
    #[must_use]
    pub fn new(key: Key, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}

/// Returns the source-shaped memory usage of a key-range slice.
///
/// On the supported 64-bit TiDB targets a Go `KeyRange` is two slice headers,
/// or 48 bytes. Rust's `Vec<KeyRange>` owns the same number of initialized
/// elements but does not expose spare elements, so callers pass the slice
/// capacity explicitly when it differs from its length.
#[must_use]
pub fn key_range_slice_mem_usage(ranges: &[KeyRange], capacity: usize) -> i64 {
    const GO_KEY_RANGE_SIZE: usize = 48;
    let allocation = GO_KEY_RANGE_SIZE.saturating_mul(capacity);
    let key_bytes = ranges.iter().fold(0_usize, |total, range| {
        total
            .saturating_add(range.start_key.capacity())
            .saturating_add(range.end_key.capacity())
    });
    i64::try_from(allocation.saturating_add(key_bytes)).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{Key, KeyRange};
    use std::cmp::Ordering;

    #[test]
    fn key_operations_follow_source_byte_semantics() {
        let key = Key::from_bytes(b"rowkey1".as_slice());
        assert_eq!(key.next().as_bytes(), b"rowkey1\0");
        assert_eq!(key.prefix_next().as_bytes(), b"rowkey2");
        assert_eq!(key.as_bytes(), b"rowkey1");

        let child = Key::from_bytes(b"rowkey1_column".as_slice());
        assert!(child.has_prefix(&key));
        assert_eq!(key.compare(&child), Ordering::Less);
        assert_eq!(key.cmp(&child), Ordering::Less);
    }

    #[test]
    fn prefix_next_preserves_source_overflow_cases() {
        assert_eq!(
            Key::from_bytes([123, 123, 255, 255].as_slice())
                .prefix_next()
                .as_bytes(),
            [123, 124, 0, 0]
        );
        assert_eq!(
            Key::from_bytes([255].as_slice()).prefix_next().as_bytes(),
            [255, 0]
        );
        assert_eq!(Key::from_bytes(Vec::new()).prefix_next().as_bytes(), [0]);
    }

    #[test]
    fn clone_and_display_are_owned_and_byte_exact() {
        let original = Key::from_bytes([0, 15, 16, 255].as_slice());
        let mut cloned_bytes = original.clone().into_bytes();
        cloned_bytes[0] = 1;

        assert_eq!(original.as_bytes(), [0, 15, 16, 255]);
        assert_eq!(original.to_string(), "000f10ff");
    }

    #[test]
    fn key_range_uses_half_open_successors() {
        assert!(KeyRange::new(
            Key::from_bytes(b"rowkey1".as_slice()),
            Key::from_bytes(b"rowkey2".as_slice()),
        )
        .is_point());
        assert!(
            KeyRange::new(Key::from_bytes(Vec::new()), Key::from_bytes([0].as_slice()),).is_point()
        );
    }

    #[test]
    fn range_successors_preserve_prefixes_across_carry_boundaries() {
        let small = [(8, 0), (8, 7), (16, 0), (16, 8), (16, 15)];
        let wide = [0, 7, 8, 9, 63, 64, 65, 127, 128, 129].map(|carry| (270, carry));
        for (len, carry) in small.into_iter().chain(wide) {
            let increment = len - carry - 1;
            let mut start = vec![15; len];
            start[increment + 1..].fill(u8::MAX);
            let mut end = vec![15; len];
            end[increment] = 16;
            end[increment + 1..].fill(0);
            let start = Key::from_bytes(start);
            assert_eq!(start.prefix_next().as_bytes(), end);
            assert!(KeyRange::new(start.clone(), Key::from_bytes(end.clone())).is_point());
            for mismatch in [0, 127, 128, 255, 256, increment, len - 1]
                .into_iter()
                .filter(|&index| index < len)
            {
                end[mismatch] ^= 1;
                assert!(!KeyRange::new(start.clone(), Key::from_bytes(end.clone())).is_point());
                end[mismatch] ^= 1;
            }
        }

        for len in [8, 16, 257] {
            let overflow = Key::from_bytes(vec![u8::MAX; len]);
            let successor = overflow.prefix_next();
            assert_eq!(successor, overflow.next());
            assert!(KeyRange::new(overflow.clone(), successor).is_point());
            assert!(!KeyRange::new(overflow, Key::from_bytes(vec![0; len])).is_point());
        }
    }
}
