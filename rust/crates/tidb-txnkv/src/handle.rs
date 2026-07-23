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

//! Safe, closed Handle representations translated from `pkg/kv/key.go`.

use crate::Key;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt;
use tidb_codec::{cut_one, decode_one, encode_int, CodecError};
use tidb_datatype::Datum;

/// A complete TiDB row-handle value without open trait objects or layout aliases.
#[derive(Debug, Clone)]
pub enum Handle {
    /// A signed integer row handle.
    Int(IntHandle),
    /// A codec-delimited multi-column row handle.
    Common(CommonHandle),
    /// A physical partition id paired with an underlying row handle.
    Partition(PartitionHandle),
}

/// TiDB's signed integer row handle.
#[derive(Debug, Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IntHandle(i64);

/// TiDB's encoded multi-column row handle.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CommonHandle {
    encoded: Key,
    column_ends: Vec<u16>,
}

/// A physical partition id paired with an underlying Handle.
#[derive(Debug, Clone)]
pub struct PartitionHandle {
    partition_id: i64,
    handle: Box<Handle>,
}

/// A typed replacement for Go's cross-kind `Handle.Compare` panics.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum HandleCompareError {
    /// Integer and common handles do not share a comparison domain.
    KindMismatch,
    /// Partition ordering requires partition handles on both sides.
    PartitionMismatch,
}

impl fmt::Display for HandleCompareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KindMismatch => {
                formatter.write_str("integer and common handles cannot be compared")
            }
            Self::PartitionMismatch => {
                formatter.write_str("partition handle requires another partition handle")
            }
        }
    }
}

impl std::error::Error for HandleCompareError {}

impl IntHandle {
    /// Creates an integer handle.
    pub const fn new(value: i64) -> Self {
        Self(value)
    }

    /// Returns the signed handle value.
    pub const fn value(self) -> i64 {
        self.0
    }

    /// Integer handles always occupy the integer Handle domain.
    pub const fn is_int(self) -> bool {
        true
    }

    /// Returns the source successor, including two's-complement overflow behavior.
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    /// Returns the fixed encoded width.
    pub const fn len(self) -> usize {
        8
    }

    /// Integer handles are never empty.
    pub const fn is_empty(self) -> bool {
        false
    }

    /// Returns TiDB's eight-byte mem-comparable encoding.
    pub fn encoded(self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(8);
        encode_int(&mut encoded, self.0);
        encoded
    }

    /// Returns this handle as its one-column datum representation.
    pub fn data(self) -> Vec<Datum> {
        vec![Datum::new_int(self.0)]
    }

    /// Compares source Handle equality, delegating through a partition wrapper.
    pub fn equal(self, other: &Handle) -> bool {
        other.int_value() == Some(self.0)
    }

    /// Compares another integer-domain handle.
    pub fn compare(self, other: &Handle) -> Result<Ordering, HandleCompareError> {
        other
            .int_value()
            .map(|value| self.0.cmp(&value))
            .ok_or(HandleCompareError::KindMismatch)
    }
}

impl fmt::Display for IntHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl CommonHandle {
    /// Parses a complete comparable datum key and records exact column bounds.
    ///
    /// Inputs shorter than nine bytes are zero-padded only after parsing, as in
    /// Go. A padded handle can be parsed again because a leading zero is the
    /// padding/NULL terminator in this specific source contract.
    pub fn new(encoded: impl Into<Vec<u8>>) -> Result<Self, CodecError> {
        let original = encoded.into();
        let mut remain = original.as_slice();
        let mut column_ends = Vec::new();
        let mut end = 0_u16;
        while !remain.is_empty() {
            if remain[0] == 0 {
                break;
            }
            let (column, next) = cut_one(remain)?;
            end = end.wrapping_add(column.len() as u16);
            column_ends.push(end);
            remain = next;
        }
        let encoded = if original.len() < 9 {
            let mut padded = original;
            padded.resize(9, 0);
            padded
        } else {
            original
        };
        Ok(Self {
            encoded: Key::from_bytes(encoded),
            column_ends,
        })
    }

    /// Common handles never occupy the integer Handle domain.
    pub const fn is_int(&self) -> bool {
        false
    }

    /// Returns the stored, possibly padded encoding.
    pub fn encoded(&self) -> &[u8] {
        self.encoded.as_bytes()
    }

    /// Returns the stored encoded length.
    pub fn len(&self) -> usize {
        self.encoded.as_bytes().len()
    }

    /// Returns whether the stored encoding is empty.
    pub fn is_empty(&self) -> bool {
        self.encoded.as_bytes().is_empty()
    }

    /// Returns the number of parsed columns.
    pub fn num_columns(&self) -> usize {
        self.column_ends.len()
    }

    /// Returns one exact encoded column, or `None` for an invalid index.
    pub fn encoded_column(&self, index: usize) -> Option<&[u8]> {
        let end = usize::from(*self.column_ends.get(index)?);
        let start = index
            .checked_sub(1)
            .map_or(0, |previous| usize::from(self.column_ends[previous]));
        self.encoded.as_bytes().get(start..end)
    }

    /// Decodes every parsed column through the production datum codec.
    pub fn data(&self) -> Result<Vec<Datum>, CodecError> {
        (0..self.num_columns())
            .map(|index| {
                let encoded = self
                    .encoded_column(index)
                    .expect("index comes from the recorded column count");
                let (remain, datum) = decode_one(encoded)?;
                if !remain.is_empty() {
                    return Err(CodecError::InvalidEncoding("column decoder left bytes"));
                }
                Ok(datum)
            })
            .collect()
    }

    /// Returns the first byte key after the stored prefix.
    ///
    /// As in Go, the successor retains offsets but is not guaranteed to remain
    /// a decodable datum sequence.
    pub fn next(&self) -> Self {
        Self {
            encoded: self.encoded.prefix_next(),
            column_ends: self.column_ends.clone(),
        }
    }

    /// Compares common handles by their stored unsigned bytes.
    pub fn compare(&self, other: &Self) -> Ordering {
        self.encoded.compare(&other.encoded)
    }

    /// Compares source Handle equality, delegating through a partition wrapper.
    pub fn equal(&self, other: &Handle) -> bool {
        !other.is_int() && self.encoded() == other.encoded()
    }

    /// Compares another common-domain handle.
    pub fn compare_handle(&self, other: &Handle) -> Result<Ordering, HandleCompareError> {
        if other.is_int() {
            Err(HandleCompareError::KindMismatch)
        } else {
            Ok(self.encoded().cmp(other.encoded().as_slice()))
        }
    }

    /// Returns Go's source-shaped total memory usage.
    #[must_use]
    pub fn mem_usage(&self) -> u64 {
        48_u64.saturating_add(self.extra_mem_size())
    }

    /// Returns allocation bytes behind the two source slices.
    #[must_use]
    pub fn extra_mem_size(&self) -> u64 {
        let encoded = u64::try_from(self.encoded.capacity()).unwrap_or(u64::MAX);
        let offsets = u64::try_from(self.column_ends.capacity())
            .unwrap_or(u64::MAX)
            .saturating_mul(2);
        encoded.saturating_add(offsets)
    }
}

impl fmt::Display for CommonHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data = match self.data() {
            Ok(data) => data,
            Err(error) => return error.fmt(formatter),
        };
        formatter.write_str("{")?;
        for (index, datum) in data.iter().enumerate() {
            if index != 0 {
                formatter.write_str(", ")?;
            }
            match datum.sql_string() {
                Ok(value) => formatter.write_str(&value)?,
                Err(error) => return error.fmt(formatter),
            }
        }
        formatter.write_str("}")
    }
}

impl PartitionHandle {
    /// Creates a partition handle without changing the underlying handle.
    pub fn new(partition_id: i64, handle: impl Into<Handle>) -> Self {
        Self {
            partition_id,
            handle: Box::new(handle.into()),
        }
    }

    /// Returns the physical partition id.
    pub const fn partition_id(&self) -> i64 {
        self.partition_id
    }

    /// Borrows the underlying logical handle.
    pub fn inner(&self) -> &Handle {
        &self.handle
    }

    /// Returns whether the underlying logical handle is an integer.
    pub fn is_int(&self) -> bool {
        self.handle.is_int()
    }

    /// Returns the delegated integer value.
    pub fn int_value(&self) -> Option<i64> {
        self.handle.int_value()
    }

    /// Returns the delegated encoded bytes.
    pub fn encoded(&self) -> Vec<u8> {
        self.handle.encoded()
    }

    /// Returns the delegated encoded length.
    pub fn len(&self) -> usize {
        self.handle.len()
    }

    /// Returns whether the delegated encoding is empty.
    pub fn is_empty(&self) -> bool {
        self.handle.is_empty()
    }

    /// Returns the delegated common-handle column count.
    pub fn num_columns(&self) -> Option<usize> {
        self.handle.num_columns()
    }

    /// Returns a delegated encoded common-handle column.
    pub fn encoded_column(&self, index: usize) -> Option<&[u8]> {
        self.handle.encoded_column(index)
    }

    /// Decodes the delegated logical handle data.
    pub fn data(&self) -> Result<Vec<Datum>, CodecError> {
        self.handle.data()
    }

    /// Delegates source `Next`, which intentionally drops the partition wrapper.
    pub fn next(&self) -> Handle {
        self.handle.next()
    }

    /// Compares partition id first, then the underlying handle.
    pub fn compare(&self, other: &Self) -> Result<Ordering, HandleCompareError> {
        match self.partition_id.cmp(&other.partition_id) {
            Ordering::Equal => self.handle.compare(&other.handle),
            ordering => Ok(ordering),
        }
    }

    /// Returns source equality, including partition ids only when both sides
    /// are partition handles.
    pub fn equal(&self, other: &Handle) -> bool {
        match other {
            Handle::Partition(right) => {
                self.partition_id == right.partition_id && self.handle.equal(&right.handle)
            }
            handle => self.handle.equal(handle),
        }
    }

    /// Returns Go's source-shaped total memory usage.
    #[must_use]
    pub fn mem_usage(&self) -> u64 {
        self.handle.mem_usage().saturating_add(24)
    }

    /// Returns allocation bytes owned by the underlying handle.
    #[must_use]
    pub fn extra_mem_size(&self) -> u64 {
        self.handle.extra_mem_size()
    }
}

impl fmt::Display for PartitionHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.handle.fmt(formatter)
    }
}

impl Handle {
    /// Returns whether the underlying logical handle is an integer.
    pub fn is_int(&self) -> bool {
        match self {
            Self::Int(_) => true,
            Self::Common(_) => false,
            Self::Partition(partition) => partition.handle.is_int(),
        }
    }

    /// Returns the delegated integer value.
    pub fn int_value(&self) -> Option<i64> {
        match self {
            Self::Int(handle) => Some(handle.value()),
            Self::Common(_) => None,
            Self::Partition(partition) => partition.handle.int_value(),
        }
    }

    /// Returns the delegated encoded handle bytes.
    pub fn encoded(&self) -> Vec<u8> {
        match self {
            Self::Int(handle) => handle.encoded(),
            Self::Common(handle) => handle.encoded().to_vec(),
            Self::Partition(partition) => partition.handle.encoded(),
        }
    }

    /// Returns the delegated encoded length.
    pub fn len(&self) -> usize {
        match self {
            Self::Int(_) => 8,
            Self::Common(handle) => handle.len(),
            Self::Partition(partition) => partition.handle.len(),
        }
    }

    /// Returns whether the delegated encoding is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a successor. Partition embedding follows Go and delegates to
    /// the inner handle, so the returned value is not partition-wrapped.
    pub fn next(&self) -> Self {
        match self {
            Self::Int(handle) => Self::Int(handle.next()),
            Self::Common(handle) => Self::Common(handle.next()),
            Self::Partition(partition) => partition.handle.next(),
        }
    }

    /// Returns source Handle equality, including partition/non-partition delegation.
    pub fn equal(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Partition(left), Self::Partition(right)) => {
                left.partition_id == right.partition_id && left.handle.equal(&right.handle)
            }
            (Self::Partition(left), right) => left.handle.equal(right),
            (left, Self::Partition(right)) => left.equal(&right.handle),
            (Self::Int(left), Self::Int(right)) => left == right,
            (Self::Common(left), Self::Common(right)) => left.encoded() == right.encoded(),
            _ => false,
        }
    }

    /// Returns source ordering with typed errors in place of Go panics.
    pub fn compare(&self, other: &Self) -> Result<Ordering, HandleCompareError> {
        match self {
            Self::Partition(left) => match other {
                Self::Partition(right) => left.compare(right),
                _ => Err(HandleCompareError::PartitionMismatch),
            },
            Self::Int(left) => other
                .int_value()
                .map(|right| left.value().cmp(&right))
                .ok_or(HandleCompareError::KindMismatch),
            Self::Common(left) => {
                if other.is_int() {
                    Err(HandleCompareError::KindMismatch)
                } else {
                    Ok(left.encoded().cmp(other.encoded().as_slice()))
                }
            }
        }
    }

    /// Returns the delegated typed data columns.
    pub fn data(&self) -> Result<Vec<Datum>, CodecError> {
        match self {
            Self::Int(handle) => Ok(handle.data()),
            Self::Common(handle) => handle.data(),
            Self::Partition(partition) => partition.handle.data(),
        }
    }

    /// Returns the common-handle column count; integer handles return `None`.
    pub fn num_columns(&self) -> Option<usize> {
        match self {
            Self::Int(_) => None,
            Self::Common(handle) => Some(handle.num_columns()),
            Self::Partition(partition) => partition.handle.num_columns(),
        }
    }

    /// Returns a common-handle encoded column; integer handles return `None`.
    pub fn encoded_column(&self, index: usize) -> Option<&[u8]> {
        match self {
            Self::Int(_) => None,
            Self::Common(handle) => handle.encoded_column(index),
            Self::Partition(partition) => partition.handle.encoded_column(index),
        }
    }

    /// Returns Go's source-shaped total memory usage.
    #[must_use]
    pub fn mem_usage(&self) -> u64 {
        match self {
            Self::Int(_) => 8,
            Self::Common(handle) => handle.mem_usage(),
            Self::Partition(handle) => handle.mem_usage(),
        }
    }

    /// Returns source allocation bytes outside the handle value itself.
    #[must_use]
    pub fn extra_mem_size(&self) -> u64 {
        match self {
            Self::Int(_) => 0,
            Self::Common(handle) => handle.extra_mem_size(),
            Self::Partition(handle) => handle.extra_mem_size(),
        }
    }
}

impl fmt::Display for Handle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(handle) => handle.fmt(formatter),
            Self::Common(handle) => handle.fmt(formatter),
            Self::Partition(partition) => partition.handle.fmt(formatter),
        }
    }
}

impl From<IntHandle> for Handle {
    fn from(handle: IntHandle) -> Self {
        Self::Int(handle)
    }
}

impl From<CommonHandle> for Handle {
    fn from(handle: CommonHandle) -> Self {
        Self::Common(handle)
    }
}

impl From<PartitionHandle> for Handle {
    fn from(handle: PartitionHandle) -> Self {
        Self::Partition(handle)
    }
}

#[derive(Debug, Clone, Eq, Hash, PartialEq)]
enum MapKey {
    Int(i64),
    Common(Vec<u8>),
    PartitionInt(i64, i64),
    PartitionCommon(i64, Vec<u8>),
}

impl MapKey {
    fn from_handle(handle: &Handle) -> Self {
        match handle {
            Handle::Partition(partition) if partition.handle.is_int() => Self::PartitionInt(
                partition.partition_id,
                partition
                    .handle
                    .int_value()
                    .expect("integer kind guarantees an integer value"),
            ),
            Handle::Partition(partition) => {
                Self::PartitionCommon(partition.partition_id, partition.handle.encoded())
            }
            Handle::Int(handle) => Self::Int(handle.value()),
            Handle::Common(handle) => Self::Common(handle.encoded().to_vec()),
        }
    }
}

/// A generic map keyed by source Handle identity.
///
/// One closed key enum eliminates Go's four-map branching while preserving
/// partition separation and encoded common-handle identity.
#[derive(Debug, Clone)]
pub struct HandleMap<V> {
    entries: HashMap<MapKey, (Handle, V)>,
}

impl<V> Default for HandleMap<V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

impl<V> HandleMap<V> {
    /// Creates an empty map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the value having the same source Handle identity.
    pub fn get(&self, handle: &Handle) -> Option<&V> {
        self.entries
            .get(&MapKey::from_handle(handle))
            .map(|(_, value)| value)
    }

    /// Inserts or overwrites a Handle value and returns the previous value.
    pub fn set(&mut self, handle: impl Into<Handle>, value: V) -> Option<V> {
        let handle = handle.into();
        self.entries
            .insert(MapKey::from_handle(&handle), (handle, value))
            .map(|(_, value)| value)
    }

    /// Deletes a Handle value and returns it when present.
    pub fn delete(&mut self, handle: &Handle) -> Option<V> {
        self.entries
            .remove(&MapKey::from_handle(handle))
            .map(|(_, value)| value)
    }

    /// Returns the number of logical Handle entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no Handle entries exist.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates entries until `visit` returns false.
    pub fn range(&self, mut visit: impl FnMut(&Handle, &V) -> bool) {
        for (handle, value) in self.entries.values() {
            if !visit(handle, value) {
                return;
            }
        }
    }

    /// Returns the source Go map's shallow memory-accounting value.
    #[must_use]
    pub fn mem_usage(&self) -> i64 {
        const SIZEOF_HANDLE_MAP: usize = 32;
        const SIZEOF_INT64: usize = 8;
        const SIZEOF_INTERFACE: usize = 16;
        const SIZEOF_STRING: usize = 16;
        const SIZEOF_STR_HANDLE_VALUE: usize = 32;
        const SIZEOF_MAP: usize = 8;

        let mut bytes = SIZEOF_HANDLE_MAP;
        let mut partition_ints = HashSet::new();
        let mut partition_strings = HashSet::new();
        for key in self.entries.keys() {
            match key {
                MapKey::Int(_) => {
                    bytes = bytes.saturating_add(SIZEOF_INT64 + SIZEOF_INTERFACE);
                }
                MapKey::Common(encoded) => {
                    bytes = bytes
                        .saturating_add(SIZEOF_STRING)
                        .saturating_add(encoded.len())
                        .saturating_add(SIZEOF_STR_HANDLE_VALUE);
                }
                MapKey::PartitionInt(partition_id, _) => {
                    partition_ints.insert(*partition_id);
                    bytes = bytes.saturating_add(SIZEOF_INT64 + SIZEOF_INTERFACE);
                }
                MapKey::PartitionCommon(partition_id, encoded) => {
                    partition_strings.insert(*partition_id);
                    bytes = bytes
                        .saturating_add(SIZEOF_STRING)
                        .saturating_add(encoded.len())
                        .saturating_add(SIZEOF_STR_HANDLE_VALUE);
                }
            }
        }
        bytes = bytes
            .saturating_add(
                partition_ints
                    .len()
                    .saturating_mul(SIZEOF_INT64 + SIZEOF_MAP),
            )
            .saturating_add(
                partition_strings
                    .len()
                    .saturating_mul(SIZEOF_INT64 + SIZEOF_MAP),
            );
        i64::try_from(bytes).unwrap_or(i64::MAX)
    }
}

/// Handle map with insertion-time shallow memory deltas.
///
/// Like Go's `MemAwareHandleMap`, values and heap objects reachable through
/// values are deliberately excluded from accounting.
#[derive(Debug, Clone, Default)]
pub struct MemAwareHandleMap<V> {
    entries: HandleMap<V>,
    accounted_bytes: i64,
}

impl<V> MemAwareHandleMap<V> {
    /// Creates an empty map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: HandleMap::new(),
            accounted_bytes: 0,
        }
    }

    /// Returns a value by source handle identity.
    #[must_use]
    pub fn get(&self, handle: &Handle) -> Option<&V> {
        self.entries.get(handle)
    }

    /// Inserts or overwrites a value and returns the shallow accounting delta.
    pub fn set(&mut self, handle: impl Into<Handle>, value: V) -> i64 {
        let before = self.entries.mem_usage();
        self.entries.set(handle, value);
        let after = self.entries.mem_usage();
        let delta = after.saturating_sub(before);
        self.accounted_bytes = self.accounted_bytes.saturating_add(delta);
        delta
    }

    /// Iterates entries until `visit` returns false.
    pub fn range(&self, visit: impl FnMut(&Handle, &V) -> bool) {
        self.entries.range(visit);
    }

    /// Returns the sum of insertion deltas.
    #[must_use]
    pub const fn accounted_bytes(&self) -> i64 {
        self.accounted_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_codec::encode_key;

    #[test]
    fn handle_semantics_are_source_complete_without_panics() {
        let int = Handle::from(IntHandle::new(100));
        assert!(int.is_int());
        assert_eq!(int.int_value(), Some(100));
        assert_eq!(int.next().int_value(), Some(101));
        assert_eq!(int.to_string(), "100");

        let encoded = encode_key(&[Datum::new_int(100), Datum::new_string("abc")]).unwrap();
        let common = CommonHandle::new(encoded).unwrap();
        assert_eq!(common.num_columns(), 2);
        assert_eq!(common.to_string(), "{100, abc}");
        assert_eq!(common.compare(&common.next()), Ordering::Less);

        let partition_int = Handle::from(PartitionHandle::new(2, int.clone()));
        assert!(partition_int.equal(&int));
        assert!(int.equal(&partition_int));
        let common_handle = Handle::from(common.clone());
        let partition_common = Handle::from(PartitionHandle::new(1, common_handle.clone()));
        assert!(partition_common.equal(&common_handle));
        assert!(common_handle.equal(&partition_common));
        assert_eq!(
            int.compare(&common_handle),
            Err(HandleCompareError::KindMismatch)
        );
    }

    #[test]
    fn short_decimal_handle_padding_reparses() {
        let encoded =
            encode_key(&[Datum::new_decimal(tidb_datatype::Decimal::from_int(1))]).unwrap();
        assert!(encoded.len() < 9);
        let handle = CommonHandle::new(encoded.clone()).unwrap();
        assert_eq!(handle.len(), 9);
        assert_eq!(handle.encoded_column(0), Some(encoded.as_slice()));
        let reparsed = CommonHandle::new(handle.encoded().to_vec()).unwrap();
        assert_eq!(reparsed.encoded_column(0), handle.encoded_column(0));
    }
}
