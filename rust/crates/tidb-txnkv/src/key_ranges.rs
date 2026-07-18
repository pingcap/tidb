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
use std::sync::Arc;

use tidb_proto::CoprocessorKeyRange;

use crate::go_is_print;
use crate::{Key, KeyRange};

/// A key-range sequence with optional split ranges before and after a shared
/// middle span.
#[derive(Clone, Debug)]
pub struct KeyRanges {
    first: Option<KeyRange>,
    mid: Arc<[KeyRange]>,
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
            mid: ranges.into(),
            mid_start: 0,
            mid_end,
            last: None,
        }
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
            return Some(&self.mid[self.mid_start + index]);
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

        let mut ranges = Self::default();
        if let Some(first) = self.first.as_ref() {
            if from == 0 && to > 0 {
                ranges.first = Some(first.clone());
            }
            from = from.saturating_sub(1);
            to = to.saturating_sub(1);
        }

        let mid_len = self.mid_end - self.mid_start;
        if to <= mid_len {
            ranges.mid = Arc::clone(&self.mid);
            ranges.mid_start = self.mid_start + from;
            ranges.mid_end = self.mid_start + to;
        } else {
            if from <= mid_len {
                ranges.mid = Arc::clone(&self.mid);
                ranges.mid_start = self.mid_start + from;
                ranges.mid_end = self.mid_end;
            }
            if from < to {
                ranges.last = self.last.clone();
            }
        }
        ranges
    }

    /// Applies `function` to every range in first/middle/last order.
    pub fn for_each(&self, mut function: impl FnMut(&KeyRange)) {
        if let Some(first) = self.first.as_ref() {
            function(first);
        }
        for range in &self.mid[self.mid_start..self.mid_end] {
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
        let mut left_index = 0;
        let mut right_index = self.len();
        while left_index < right_index {
            let middle = left_index + (right_index - left_index) / 2;
            let range = self.ref_at(middle);
            if range.end_key.as_bytes().is_empty() || range.end_key > *key {
                right_index = middle;
            } else {
                left_index = middle + 1;
            }
        }
        let split_index = left_index;

        if split_index < self.len() {
            let range = self.ref_at(split_index);
            if key > &range.start_key {
                let mut left = self.slice(0, split_index);
                left.last = Some(KeyRange::new(range.start_key.clone(), key.clone()));

                let mut right = self.slice(split_index + 1, self.len());
                right.first = Some(KeyRange::new(key.clone(), range.end_key.clone()));
                return (left, right);
            }
        }

        (
            self.slice(0, split_index),
            self.slice(split_index, self.len()),
        )
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
    /// Go aliases the identically laid-out structs through `unsafe.Pointer`.
    /// Rust keeps the same values while explicitly cloning both byte fields,
    /// so no layout or lifetime assumption crosses the protobuf boundary.
    pub fn to_pb_ranges(&self) -> Vec<CoprocessorKeyRange> {
        let mut ranges = Vec::with_capacity(self.len());
        self.for_each(|range| {
            ranges.push(CoprocessorKeyRange {
                start: range.start_key.as_bytes().to_vec(),
                end: range.end_key.as_bytes().to_vec(),
            });
        });
        ranges
    }
}

impl Default for KeyRanges {
    fn default() -> Self {
        Self::new(Vec::new())
    }
}

impl PartialEq for KeyRanges {
    fn eq(&self, other: &Self) -> bool {
        self.len() == other.len()
            && (0..self.len()).all(|index| self.ref_at(index) == other.ref_at(index))
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
        character if go_is_print::is_print(character) => {
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

        assert!(Arc::ptr_eq(&ranges.mid, &slice.mid));
        assert_eq!((slice.mid_start, slice.mid_end), (1, 2));
    }
}
