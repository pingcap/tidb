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

//! Allocation-bounded coprocessor key ranges translated from
//! `pkg/store/copr/key_ranges.go`.
//!
//! Go keeps a borrowed middle slice and at most one extra range on each side.
//! Rust represents the same shape with a shared middle allocation plus a
//! half-open span into it.  Consequently [`KeyRanges::slice`] and the
//! non-boundary parts of [`KeyRanges::split`] do not copy the potentially
//! large middle range list.  The source's unsafe layout cast to kvproto is
//! deliberately replaced by explicit, safe protobuf construction. Go also
//! exposes mutable pointers into borrowed storage; Rust deliberately makes
//! those references immutable and owns or shares every represented range.

use std::fmt;
use std::ops::{Index, Range};
use std::sync::Arc;

use prost::bytes::Bytes;
use tidb_proto::CoprocessorKeyRange;

use tidb_txnkv::Key;
use tidb_txnkv::KeyRange;
use tidb_txnkv::PartitionedKeyRanges;

#[derive(Clone, Debug)]
enum RangeStorage {
    Owned(Arc<[KeyRange]>),
    Request(Arc<PartitionedKeyRanges>),
}

impl RangeStorage {
    fn as_slice(&self) -> &[KeyRange] {
        match self {
            Self::Owned(ranges) => ranges,
            Self::Request(ranges) => ranges.first_partition_ranges(),
        }
    }
}

/// A key-range sequence with optional split ranges before and after a shared
/// middle span.
#[derive(Clone, Debug)]
pub struct KeyRanges {
    first: Option<KeyRange>,
    mid: RangeStorage,
    mid_start: usize,
    mid_end: usize,
    last: Option<KeyRange>,
}

impl KeyRanges {
    /// Constructs a range sequence from its contiguous middle ranges.
    pub fn new(ranges: Vec<KeyRange>) -> Self {
        let mid_end = ranges.len();
        Self {
            first: None,
            mid: RangeStorage::Owned(ranges.into()),
            mid_start: 0,
            mid_end,
            last: None,
        }
    }

    pub(crate) fn from_request(
        ranges: Arc<PartitionedKeyRanges>,
        span: Range<usize>,
        first: Option<KeyRange>,
        last: Option<KeyRange>,
    ) -> Self {
        Self {
            first,
            mid: RangeStorage::Request(ranges),
            mid_start: span.start,
            mid_end: span.end,
            last,
        }
    }

    fn middle(&self) -> &[KeyRange] {
        &self.mid.as_slice()[self.mid_start..self.mid_end]
    }

    /// Returns the number of ranges in the sequence.
    pub fn len(&self) -> usize {
        usize::from(self.first.is_some())
            + (self.mid_end - self.mid_start)
            + usize::from(self.last.is_some())
    }

    /// Returns whether the sequence contains no ranges.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the range at `index`, or `None` when the index is outside the
    /// logical sequence.
    ///
    /// Go's pointer-returning `RefAt` can yield `nil` for some out-of-range
    /// representations and panic for others. Rust exposes one safe policy via
    /// this method; [`Self::ref_at`] is the explicit panicking counterpart.
    pub fn get(&self, mut index: usize) -> Option<&KeyRange> {
        if let Some(first) = self.first.as_ref() {
            if index == 0 {
                return Some(first);
            }
            index = index.checked_sub(1)?;
        }

        let mid_len = self.mid_end - self.mid_start;
        if index < mid_len {
            return Some(&self.mid.as_slice()[self.mid_start + index]);
        }
        if index == mid_len {
            return self.last.as_ref();
        }
        None
    }

    /// Returns a reference to the range at `index` without copying it.
    ///
    /// This panics when `index` is outside the sequence, matching Go's indexed
    /// access contract while eliminating Go's representation-dependent `nil`
    /// result.
    pub fn ref_at(&self, index: usize) -> &KeyRange {
        self.get(index).expect("key range index out of bounds")
    }

    /// Returns an owned copy of the range at `index`.
    pub fn at(&self, index: usize) -> KeyRange {
        self.ref_at(index).clone()
    }

    /// Returns the half-open subsequence `[from, to)` without copying its
    /// middle range list.
    pub fn slice(&self, mut from: usize, mut to: usize) -> Self {
        assert!(from <= to, "key range slice starts after its end");
        assert!(to <= self.len(), "key range slice end out of bounds");

        let first = self.first.as_ref().and_then(|first| {
            let include_first = from == 0 && to > 0;
            from = from.saturating_sub(1);
            to = to.saturating_sub(1);
            include_first.then(|| first.clone())
        });

        let mid_len = self.mid_end - self.mid_start;
        let (mid, mid_start, mid_end) = if from <= mid_len {
            (
                self.mid.clone(),
                self.mid_start + from,
                self.mid_start + to.min(mid_len),
            )
        } else {
            (RangeStorage::Owned(Arc::default()), 0, 0)
        };
        let last = if to > mid_len && from < to {
            self.last.clone()
        } else {
            None
        };
        Self {
            first,
            mid,
            mid_start,
            mid_end,
            last,
        }
    }

    /// Applies `function` to every range in first/middle/last order.
    pub fn for_each(&self, mut function: impl FnMut(&KeyRange)) {
        if let Some(first) = self.first.as_ref() {
            function(first);
        }
        for range in self.middle() {
            function(range);
        }
        if let Some(last) = self.last.as_ref() {
            function(last);
        }
    }

    /// Splits the sequence into ranges left and right of `key`.
    ///
    /// If `key` is strictly inside one range, that range is split into two
    /// boundary ranges.  Otherwise the existing range boundaries are kept.
    pub fn split(&self, key: &Key) -> (Self, Self) {
        let key = key.as_bytes();
        let (index, range) = self.split_position(key);
        (self.split_left(index, range, key), self.split_right(index, range, key))
    }

    pub(crate) fn split_for_paging(&self, key: &[u8], descending: bool) -> Self {
        let (index, range) = self.split_position(key);
        if descending {
            self.split_left(index, range, key)
        } else {
            self.split_right(index, range, key)
        }
    }

    #[inline]
    fn split_position(&self, key: &[u8]) -> (usize, Option<&KeyRange>) {
        let ends_before = |range: &KeyRange| {
            let end = range.end_key.as_bytes();
            !end.is_empty() && end <= key
        };
        let index = if self.first.is_none() && self.last.is_none() {
            self.middle().partition_point(ends_before)
        } else {
            self.split_with_boundaries(ends_before)
        };
        let range = self.get(index).filter(|range| key > range.start_key.as_bytes());
        (index, range)
    }

    fn split_with_boundaries(&self, ends_before: impl Fn(&KeyRange) -> bool) -> usize {
        let mut left_index = 0;
        let mut right_index = self.len();
        while left_index < right_index {
            let middle = left_index + (right_index - left_index) / 2;
            if ends_before(self.ref_at(middle)) {
                left_index = middle + 1;
            } else {
                right_index = middle;
            }
        }
        left_index
    }

    fn split_left(&self, index: usize, range: Option<&KeyRange>, key: &[u8]) -> Self {
        let mut left = self.slice(0, index);
        if let Some(range) = range {
            left.last = Some(KeyRange::new(range.start_key.clone(), Key::from(key)));
        }
        left
    }

    fn split_right(&self, index: usize, range: Option<&KeyRange>, key: &[u8]) -> Self {
        let mut right = self.slice(index + usize::from(range.is_some()), self.len());
        if let Some(range) = range {
            right.first = Some(KeyRange::new(Key::from(key), range.end_key.clone()));
        }
        right
    }

    /// Copies the represented sequence into a contiguous range vector.
    pub fn to_ranges(&self) -> Vec<KeyRange> {
        let mut ranges = Vec::with_capacity(self.len());
        self.for_each(|range| ranges.push(range.clone()));
        ranges
    }

    /// Replaces the sequence with the provided contiguous ranges.
    pub fn reset(&mut self, ranges: Vec<KeyRange>) {
        *self = Self::new(ranges);
    }

    /// Converts every range to the generated coprocessor protobuf type.
    ///
    /// Native boundaries share packed storage when more than one is present.
    pub fn to_pb_ranges(&self) -> Vec<CoprocessorKeyRange> {
        if self.first.is_none() && self.last.is_none() {
            key_ranges_to_proto(self.middle().iter().map(range_boundaries))
        } else {
            key_ranges_to_proto(self.iter().map(range_boundaries))
        }
    }

    pub(crate) fn to_api_v2_pb_ranges(
        &self,
        codec: &tikv_client::request::ApiV2Codec,
    ) -> Vec<CoprocessorKeyRange> {
        if self.first.is_none() && self.last.is_none() {
            codec.encode_cop_range_bounds(self.middle().iter().map(range_boundaries))
        } else {
            codec.encode_cop_range_bounds(self.iter().map(range_boundaries))
        }
    }

    /// Iterates over the logical ranges in order.
    pub fn iter(&self) -> impl Iterator<Item = &KeyRange> + Clone {
        self.first
            .iter()
            .chain(self.middle())
            .chain(self.last.iter())
    }
}

fn range_boundaries(range: &KeyRange) -> (&[u8], &[u8]) {
    (range.start_key.as_slice(), range.end_key.as_slice())
}

/// Projects ordered ranges, sharing the byte allocation across a batch.
pub(crate) fn key_ranges_to_proto<'a>(
    mut ranges: impl Iterator<Item = (&'a [u8], &'a [u8])> + Clone,
) -> Vec<CoprocessorKeyRange> {
    if ranges.size_hint() == (1, Some(1)) {
        let (start, end) = ranges.next().expect("one range");
        let range = if start.is_empty() || end.is_empty() {
            CoprocessorKeyRange {
                start: Bytes::copy_from_slice(start),
                end: Bytes::copy_from_slice(end),
            }
        } else {
            let mut packed = Vec::with_capacity(start.len() + end.len());
            packed.extend_from_slice(start);
            packed.extend_from_slice(end);
            // Pre-share the owner so cloning the outgoing request does not
            // promote each boundary into a separate ref-counted allocation.
            let mut packed = Bytes::from_owner(packed);
            CoprocessorKeyRange {
                start: packed.split_to(start.len()),
                end: packed,
            }
        };
        return vec![range];
    }
    let byte_len = ranges
        .clone()
        .map(|(start, end)| start.len() + end.len())
        .sum();
    let mut packed = Vec::with_capacity(byte_len);
    for (start, end) in ranges.clone() {
        packed.extend_from_slice(start);
        packed.extend_from_slice(end);
    }
    // The packed bytes will back many slices. Establish their shared owner
    // before slicing instead of promoting an exclusive Bytes on its first clone.
    let packed = if packed.is_empty() {
        Bytes::new()
    } else {
        Bytes::from_owner(packed)
    };
    let mut offset = 0;
    ranges
        .map(|(start, end)| {
            let middle = offset + start.len();
            let limit = middle + end.len();
            let range = CoprocessorKeyRange {
                start: packed.slice(offset..middle),
                end: packed.slice(middle..limit),
            };
            offset = limit;
            range
        })
        .collect()
}

impl Default for KeyRanges {
    fn default() -> Self {
        Self {
            first: None,
            mid: RangeStorage::Owned(Arc::default()),
            mid_start: 0,
            mid_end: 0,
            last: None,
        }
    }
}

impl From<Vec<KeyRange>> for KeyRanges {
    fn from(ranges: Vec<KeyRange>) -> Self {
        Self::new(ranges)
    }
}

impl Index<usize> for KeyRanges {
    type Output = KeyRange;

    fn index(&self, index: usize) -> &Self::Output {
        self.ref_at(index)
    }
}

impl PartialEq for KeyRanges {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter())
    }
}

impl Eq for KeyRanges {}

impl fmt::Display for KeyRanges {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for index in 0..self.len() {
            let range = self.ref_at(index);
            write!(
                formatter,
                "[{}, {}]",
                GoQuotedBytes(range.start_key.as_bytes()),
                GoQuotedBytes(range.end_key.as_bytes())
            )?;
        }
        Ok(())
    }
}

/// Formats byte keys in the quoted form used by Go's `%q` string verb.
struct GoQuotedBytes<'a>(&'a [u8]);

impl fmt::Display for GoQuotedBytes<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("\"")?;
        let mut bytes = self.0;
        while !bytes.is_empty() {
            let valid_length = match std::str::from_utf8(bytes) {
                Ok(valid) => valid.len(),
                Err(error) => error.valid_up_to(),
            };
            if valid_length > 0 {
                // `valid_length` is guaranteed to end on a UTF-8 boundary.
                let valid = std::str::from_utf8(&bytes[..valid_length])
                    .expect("validated UTF-8 prefix must decode");
                for character in valid.chars() {
                    write_go_quoted_character(formatter, character)?;
                }
                bytes = &bytes[valid_length..];
                continue;
            }

            // Go's strconv quoting emits an invalid UTF-8 byte as `\xNN` and
            // then resumes decoding at the following byte.
            write!(formatter, "\\x{:02x}", bytes[0])?;
            bytes = &bytes[1..];
        }
        formatter.write_str("\"")
    }
}

fn write_go_quoted_character(formatter: &mut fmt::Formatter<'_>, character: char) -> fmt::Result {
    match character {
        '\x07' => formatter.write_str("\\a"),
        '\x08' => formatter.write_str("\\b"),
        '\x0c' => formatter.write_str("\\f"),
        '\n' => formatter.write_str("\\n"),
        '\r' => formatter.write_str("\\r"),
        '\t' => formatter.write_str("\\t"),
        '\x0b' => formatter.write_str("\\v"),
        '\\' => formatter.write_str("\\\\"),
        '\"' => formatter.write_str("\\\""),
        ' '..='~' => write!(formatter, "{character}"),
        character if tidb_go_strconv::is_print(character) => {
            write!(formatter, "{character}")
        }
        character if character < ' ' || character == '\u{007f}' => {
            write!(formatter, "\\x{:02x}", u32::from(character))
        }
        character if u32::from(character) <= 0xffff => {
            write!(formatter, "\\u{:04x}", u32::from(character))
        }
        character => write!(formatter, "\\U{:08x}", u32::from(character)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slices_share_the_middle_allocation() {
        let ranges = KeyRanges::new(vec![
            KeyRange::new(Key::from_bytes(b"a"), Key::from_bytes(b"b")),
            KeyRange::new(Key::from_bytes(b"c"), Key::from_bytes(b"d")),
        ]);
        let slice = ranges.slice(1, 2);

        assert!(std::ptr::eq(ranges.ref_at(1), slice.ref_at(0)));
        assert_eq!((slice.mid_start, slice.mid_end), (1, 2));
    }

    #[test]
    fn cloned_native_singleton_shares_both_boundary_bytes() {
        let native = KeyRanges::new(vec![KeyRange::new(
            b"a\0".as_slice().into(),
            b"a\xff".as_slice().into(),
        )])
        .to_pb_ranges();
        let range = &native[0];
        assert_eq!(
            range.start.as_ptr().addr() + range.start.len(),
            range.end.as_ptr().addr(),
        );
        let cloned = range.clone();
        assert_eq!(cloned.start.as_ptr(), range.start.as_ptr());
        assert_eq!(cloned.end.as_ptr(), range.end.as_ptr());
        drop(native);
        assert_eq!(cloned.start.as_ref(), b"a\0");
        assert_eq!(cloned.end.as_ref(), b"a\xff");
    }

    #[test]
    fn native_projection_keeps_split_edges_alive_in_order() {
        let native = {
            let ranges = KeyRanges::new(vec![
                KeyRange::new(b"a".as_slice().into(), b"d".as_slice().into()),
                KeyRange::new(b"f".as_slice().into(), b"k".as_slice().into()),
                KeyRange::new(b"p".as_slice().into(), b"z".as_slice().into()),
            ]);
            let (_, tail) = ranges.split(&b"b".as_slice().into());
            let (bounded, _) = tail.split(&b"w".as_slice().into());
            bounded.to_pb_ranges()
        };
        let actual: Vec<_> = native
            .iter()
            .map(|range| (range.start.as_ref(), range.end.as_ref()))
            .collect();
        assert_eq!(
            actual,
            [
                (b"b".as_slice(), b"d".as_slice()),
                (b"f".as_slice(), b"k".as_slice()),
                (b"p".as_slice(), b"w".as_slice()),
            ],
        );
    }
}
