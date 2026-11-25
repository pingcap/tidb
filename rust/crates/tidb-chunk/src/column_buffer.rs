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

use std::fmt;
use std::ops::{Deref, DerefMut, Range};

use bytes::{Buf, Bytes};

use crate::go_append::{go_grow, go_slices_grow, GoAppend};

/// Column metadata with a movable start, so the incremental decoder can
/// advance past consumed offsets and bitmap bytes without copying the tail.
#[derive(Default)]
pub(crate) struct ColumnBuffer<T> {
    values: Vec<T>,
    start: usize,
}

impl<T> ColumnBuffer<T> {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity).into()
    }

    pub(crate) fn capacity(&self) -> usize {
        self.values.capacity()
    }

    pub(crate) fn advance(&mut self, count: usize) {
        assert!(count <= self.len(), "column metadata prefix exceeds length");
        self.start += count;
    }

    pub(crate) fn clear(&mut self) {
        self.values.clear();
        self.start = 0;
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        if len < self.len() {
            self.values.truncate(self.start + len);
        }
    }

    #[inline]
    pub(crate) fn set_prefix_end(&mut self, len: usize, value: T) {
        let end = self.start + len;
        self.values[end - 1] = value;
        self.values.truncate(end);
    }

    /// Compact only when an operation needs to grow the visible portion.
    fn compact(&mut self) -> &mut Vec<T> {
        if self.start != 0 {
            self.values.drain(..self.start);
            self.start = 0;
        }
        &mut self.values
    }

    pub(crate) fn push(&mut self, value: T) {
        self.compact().push(value);
    }

    pub(crate) fn reserve(&mut self, count: usize) {
        go_slices_grow(self.compact(), count);
    }

    pub(crate) fn extend_from_slice(&mut self, values: &[T])
    where
        T: Clone,
    {
        self.compact().extend_from_slice(values);
    }

    pub(crate) fn resize(&mut self, len: usize, value: T)
    where
        T: Clone,
    {
        self.compact().resize(len, value);
    }

    /// Install decoded metadata with an allocation sized to this frame.
    pub(crate) fn replace(&mut self, values: impl ExactSizeIterator<Item = T>) {
        if self.values.capacity() == values.len() {
            self.clear();
            self.values.extend(values);
        } else {
            *self = values.collect::<Vec<_>>().into();
        }
    }
}

impl<T> From<Vec<T>> for ColumnBuffer<T> {
    fn from(values: Vec<T>) -> Self {
        Self { values, start: 0 }
    }
}

impl ColumnBuffer<u8> {
    #[inline]
    pub(crate) fn bit_is_set(&self, bit: usize) -> bool {
        self.values[self.start + bit / 8] & (1 << (bit & 7)) != 0
    }

    #[inline]
    pub(crate) fn append_set_bit(&mut self, row: usize) {
        let offset = row >> 3;
        let bit = 1 << (row & 7);
        if let Some(byte) = self.values.get_mut(self.start + offset) {
            *byte |= bit;
        } else {
            self.append_set_bit_slow(offset, bit);
        }
    }

    #[cold]
    fn append_set_bit_slow(&mut self, offset: usize, bit: u8) {
        if offset == self.len() {
            self.go_push(bit);
        } else {
            self.go_push(0);
            self[offset] |= bit;
        }
    }
}

impl<T> Deref for ColumnBuffer<T> {
    type Target = [T];

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.values[self.start..]
    }
}

impl<T> DerefMut for ColumnBuffer<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.values[self.start..]
    }
}

impl<T> AsRef<[T]> for ColumnBuffer<T> {
    fn as_ref(&self) -> &[T] {
        self
    }
}

impl<T: Clone> Clone for ColumnBuffer<T> {
    fn clone(&self) -> Self {
        self.to_vec().into()
    }
}

impl<T: fmt::Debug> fmt::Debug for ColumnBuffer<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_ref().fmt(formatter)
    }
}

impl<T: PartialEq, U: AsRef<[T]>> PartialEq<U> for ColumnBuffer<T> {
    fn eq(&self, other: &U) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl<T: Eq> Eq for ColumnBuffer<T> {}

impl<T> GoAppend<T> for ColumnBuffer<T> {
    fn go_push(&mut self, value: T) {
        self.compact().go_push(value);
    }

    fn go_extend_from_slice(&mut self, values: &[T])
    where
        T: Clone,
    {
        self.compact().go_extend_from_slice(values);
    }
}

/// Variable-column offsets are either writable words or a view of the
/// response bytes. Wire words need no alignment to be read as byte arrays.
pub(crate) enum ColumnOffsets {
    Owned(ColumnBuffer<i64>),
    Wire { bytes: Bytes, capacity: usize },
    // Sparse reconstruction can defer allocating the original capacity.
    Inline { words: [i64; 2], len: usize, capacity: usize },
}

#[derive(Clone, Copy)]
pub(crate) enum OffsetSlice<'a> {
    Words(&'a [i64]),
    Wire(&'a [[u8; 8]]),
}

impl<'a> OffsetSlice<'a> {
    #[inline]
    pub(crate) fn len(self) -> usize {
        match self {
            Self::Words(words) => words.len(),
            Self::Wire(words) => words.len(),
        }
    }

    #[inline]
    pub(crate) fn at(self, index: usize) -> i64 {
        match self {
            Self::Words(words) => words[index],
            Self::Wire(words) => i64::from_ne_bytes(words[index]),
        }
    }

    #[inline]
    pub(crate) fn cell_range(self, row: usize) -> Range<usize> {
        if self.len() == 0 {
            return 0..0;
        }
        self.get_cell_range(row).expect("cell offsets")
    }

    #[inline]
    pub(crate) fn get_cell_range(self, row: usize) -> Option<Range<usize>> {
        match self {
            Self::Words(offsets) => {
                let [start, end] = offsets.get(row..)?.first_chunk::<2>()?;
                Some(*start as usize..*end as usize)
            }
            Self::Wire(words) => {
                let [start, end] = words.get(row..)?.first_chunk::<2>()?;
                Some(i64::from_ne_bytes(*start) as usize..i64::from_ne_bytes(*end) as usize)
            }
        }
    }

    pub(crate) fn iter(self) -> impl ExactSizeIterator<Item = i64> + 'a {
        (0..self.len()).map(move |index| self.at(index))
    }

    pub(crate) fn to_vec(self) -> Vec<i64> {
        match self {
            Self::Words(words) => words.to_vec(),
            Self::Wire(words) => words.iter().copied().map(i64::from_ne_bytes).collect(),
        }
    }

    #[inline]
    pub(crate) fn encode(self, out: &mut Vec<u8>) {
        match self {
            Self::Wire(words) => out.extend_from_slice(words.as_flattened()),
            Self::Words(words) => {
                let start = out.len();
                out.resize(start + std::mem::size_of_val(words), 0);
                for (output, offset) in out[start..].as_chunks_mut::<8>().0.iter_mut().zip(words) {
                    *output = offset.to_ne_bytes();
                }
            }
        }
    }
}

impl ColumnOffsets {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self::Owned(ColumnBuffer::with_capacity(capacity))
    }

    pub(crate) fn capacity(&self) -> usize {
        match self {
            Self::Owned(offsets) => offsets.capacity(),
            Self::Wire { capacity, .. } | Self::Inline { capacity, .. } => *capacity,
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Owned(offsets) => offsets.len(),
            Self::Wire { bytes, .. } => bytes.len() / 8,
            Self::Inline { len, .. } => *len,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub(crate) fn as_slice(&self) -> OffsetSlice<'_> {
        match self {
            Self::Owned(offsets) => OffsetSlice::Words(offsets),
            Self::Wire { bytes, .. } => OffsetSlice::Wire(bytes.as_chunks::<8>().0),
            Self::Inline { words, len, .. } => OffsetSlice::Words(&words[..*len]),
        }
    }

    #[inline]
    pub(crate) fn slice(&self, range: Range<usize>) -> OffsetSlice<'_> {
        match self {
            Self::Owned(offsets) => OffsetSlice::Words(&offsets[range]),
            Self::Wire { bytes, .. } => OffsetSlice::Wire(&bytes.as_chunks::<8>().0[range]),
            Self::Inline { words, len, .. } => OffsetSlice::Words(&words[..*len][range]),
        }
    }

    #[inline]
    pub(crate) fn at(&self, index: usize) -> i64 {
        match self {
            Self::Owned(offsets) => offsets[index],
            Self::Wire { bytes, .. } => i64::from_ne_bytes(bytes.as_chunks::<8>().0[index]),
            Self::Inline { words, len, .. } => words[..*len][index],
        }
    }

    #[inline]
    pub(crate) fn cell_range(&self, row: usize) -> Range<usize> {
        // An uninitialized variable column exposes a zero-length cell.
        match self {
            Self::Owned(offsets) if offsets.is_empty() => 0..0,
            Self::Wire { bytes, .. } if bytes.is_empty() => 0..0,
            Self::Owned(offsets) => {
                let [start, end] = offsets[row..].first_chunk::<2>().expect("cell offsets");
                *start as usize..*end as usize
            }
            Self::Wire { bytes, .. } => {
                let [start, end] = bytes.as_chunks::<8>().0[row..]
                    .first_chunk::<2>()
                    .expect("wire cell offsets");
                i64::from_ne_bytes(*start) as usize..i64::from_ne_bytes(*end) as usize
            }
            Self::Inline { len: 0, .. } => 0..0,
            Self::Inline { words, len, .. } => {
                let [start, end] = words[..*len][row..].first_chunk::<2>().expect("cell offsets");
                *start as usize..*end as usize
            }
        }
    }

    pub(crate) fn get(&self, index: usize) -> Option<i64> {
        (index < self.len()).then(|| self.at(index))
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = i64> + '_ {
        self.as_slice().iter()
    }

    pub(crate) fn replace_owned(&mut self, words: &[[u8; 8]]) {
        let decoded = words.iter().copied().map(i64::from_ne_bytes);
        match self {
            Self::Owned(offsets) => offsets.replace(decoded),
            Self::Wire { .. } => *self = decoded.collect::<Vec<_>>().into(),
            Self::Inline { .. } => self.make_owned().replace(decoded),
        }
    }

    /// Tests for the same validated input range and the same capacity the
    /// decoder would install. Wire offsets are immutable and retain their
    /// order through slicing.
    #[inline]
    pub(crate) fn matches_wire(&self, input: &[u8]) -> bool {
        matches!(self, Self::Wire { bytes, capacity }
            if *capacity == input.len() / 8
                && std::ptr::eq(bytes.as_ref(), input))
    }

    /// Stores a wire offset table after the caller validates its order.
    pub(crate) fn replace_wire(&mut self, bytes: Bytes) {
        let capacity = bytes.len() / 8;
        *self = Self::Wire { bytes, capacity };
    }

    #[inline]
    pub(crate) fn advance(&mut self, count: usize) {
        match self {
            Self::Owned(offsets) => offsets.advance(count),
            Self::Wire { bytes, capacity } => {
                bytes.advance(count * 8);
                *capacity -= count;
            }
            Self::Inline { words, len, capacity } => {
                words.copy_within(count..*len, 0);
                *len -= count;
                *capacity -= count;
            }
        }
    }

    pub(crate) fn truncate(&mut self, len: usize) {
        match self {
            Self::Owned(offsets) => offsets.truncate(len),
            Self::Wire { bytes, .. } => bytes.truncate(len * 8),
            Self::Inline { len: visible, .. } => *visible = (*visible).min(len),
        }
    }

    pub(crate) fn clear(&mut self) {
        match self {
            Self::Owned(offsets) => offsets.clear(),
            Self::Wire { capacity, .. } => *self = Self::with_capacity(*capacity),
            Self::Inline { len, .. } => *len = 0,
        }
    }

    #[inline]
    pub(crate) fn make_owned(&mut self) -> &mut ColumnBuffer<i64> {
        match self {
            Self::Owned(offsets) => offsets,
            Self::Wire { .. } | Self::Inline { .. } => self.materialize(),
        }
    }

    #[cold]
    fn materialize(&mut self) -> &mut ColumnBuffer<i64> {
        if let Self::Wire { bytes, capacity } = self {
            let mut words = Vec::with_capacity(*capacity);
            words.extend(
                bytes
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .copied()
                    .map(i64::from_ne_bytes),
            );
            *self = words.into();
        } else if let Self::Inline { words, len, capacity } = self {
            let mut owned = Vec::with_capacity(*capacity);
            owned.extend_from_slice(&words[..*len]);
            *self = owned.into();
        }
        match self {
            Self::Owned(offsets) => offsets,
            _ => unreachable!("offsets were materialized"),
        }
    }

    #[inline]
    pub(crate) fn reconstruct_one(&mut self, end: i64) {
        match self {
            Self::Owned(offsets) => offsets.set_prefix_end(2, end),
            Self::Wire { bytes, .. } if bytes.get(8..16) == Some(end.to_ne_bytes().as_slice()) => {
                bytes.truncate(16);
            }
            _ => {
                *self = Self::Inline {
                    words: [self.at(0), end],
                    len: 2,
                    capacity: self.capacity(),
                };
            }
        }
    }

    /// Rebuilds the offsets of a wire selection containing only NULLs.
    pub(crate) fn reconstruct_nulls(&mut self, rows: usize) {
        static ZERO_OFFSETS: [u8; 32 * 8] = [0; 32 * 8];
        let len = rows + 1;
        let start = self.at(0);
        let capacity = self.capacity();
        if start == 0 && len <= ZERO_OFFSETS.len() / 8 {
            *self = Self::Wire {
                bytes: Bytes::from_static(&ZERO_OFFSETS[..len * 8]),
                capacity,
            };
        } else {
            let mut offsets = Vec::with_capacity(capacity);
            offsets.push(start);
            offsets.resize(len, 0);
            *self = offsets.into();
        }
    }

    pub(crate) fn set(&mut self, index: usize, value: i64) {
        self.make_owned()[index] = value;
    }

    pub(crate) fn reserve(&mut self, count: usize) {
        self.make_owned().reserve(count);
    }

    pub(crate) fn push(&mut self, value: i64) {
        self.make_owned().push(value);
    }

    #[inline]
    pub(crate) fn go_push(&mut self, value: i64) {
        self.make_owned().compact().go_push(value);
    }

    #[inline]
    pub(crate) fn append_with_delta(&mut self, offsets: OffsetSlice<'_>, delta: i64) {
        let out = self.make_owned().compact();
        match offsets {
            OffsetSlice::Words(words) => {
                let start = out.len();
                out.go_extend_from_slice(words);
                if delta != 0 {
                    for value in &mut out[start..] {
                        *value += delta;
                    }
                }
            }
            OffsetSlice::Wire(words) => {
                go_grow(out, words.len());
                out.extend(words.iter().map(|word| i64::from_ne_bytes(*word) + delta));
            }
        }
    }

    /// Appends row offsets, growing at the same boundaries as Go's per-row append.
    pub(crate) fn append_row_offsets(&mut self, offsets: OffsetSlice<'_>, delta: i64) {
        if offsets.len() <= self.capacity() - self.len() {
            self.append_with_delta(offsets, delta);
        } else {
            let out = self.make_owned().compact();
            for offset in offsets.iter() {
                out.go_push(offset + delta);
            }
        }
    }

    pub(crate) fn append_repeated_offset(&mut self, value: i64, count: usize) {
        let out = self.make_owned().compact();
        if count <= out.capacity() - out.len() {
            out.resize(out.len() + count, value);
        } else {
            // Preserve Go's growth steps if the repeated appends do not fit.
            for _ in 0..count {
                out.go_push(value);
            }
        }
    }

    pub(crate) fn append_row_offsets_within(&mut self, range: Range<usize>, delta: i64) {
        let out = self.make_owned().compact();
        let start = out.len();
        if range.len() <= out.capacity() - start {
            out.extend_from_within(range);
            if delta != 0 {
                for offset in &mut out[start..] {
                    *offset += delta;
                }
            }
        } else {
            for row in range {
                out.go_push(out[row] + delta);
            }
        }
    }
}

impl Default for ColumnOffsets {
    fn default() -> Self {
        Self::Owned(ColumnBuffer::default())
    }
}

impl From<Vec<i64>> for ColumnOffsets {
    fn from(values: Vec<i64>) -> Self {
        Self::Owned(values.into())
    }
}

impl Clone for ColumnOffsets {
    fn clone(&self) -> Self {
        self.as_slice().to_vec().into()
    }
}

impl fmt::Debug for ColumnOffsets {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_list().entries(self.iter()).finish()
    }
}

impl PartialEq for ColumnOffsets {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for ColumnOffsets {}

#[cfg(test)]
impl PartialEq<Vec<i64>> for ColumnOffsets {
    fn eq(&self, other: &Vec<i64>) -> bool {
        self.iter().eq(other.iter().copied())
    }
}
