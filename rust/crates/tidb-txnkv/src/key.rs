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

/// An owned TiDB KV key ordered lexicographically by unsigned bytes.
///
/// This is the Rust ownership form of Go's `type Key []byte`. Deriving
/// [`Clone`] performs the source `Key.Clone` deep copy because the bytes are
/// owned by a [`Vec`].
#[derive(Debug, Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Key(Vec<u8>);

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
    pub fn next(&self) -> Self {
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
    pub fn prefix_next(&self) -> Self {
        let mut bytes = self.0.clone();
        for byte in bytes.iter_mut().rev() {
            *byte = byte.wrapping_add(1);
            if *byte != 0 {
                return Self(bytes);
            }
        }

        self.next()
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
        for byte in &self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
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
    pub fn is_point(&self) -> bool {
        let start = self.start_key.as_bytes();
        let end = self.end_key.as_bytes();

        if start.len() != end.len() {
            return start.len().checked_add(1) == Some(end.len())
                && end.get(start.len()) == Some(&0)
                && start == &end[..start.len()];
        }

        for index in (0..start.len()).rev() {
            if start[index] != u8::MAX {
                return start[index] + 1 == end[index] && start[..index] == end[..index];
            }
            if end[index] != 0 {
                return false;
            }
        }

        false
    }
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
}
