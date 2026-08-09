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

//! Private Go-slice header and backing-array representation for chunk buffers.
//!
//! This module is deliberately unused by `Column` until its direct semantics
//! are proven. The owned variant keeps the common path lock-free. Sharing a
//! header promotes its backing once; sibling headers then retain independent
//! `(start, len, cap)` state while mutations within capacity remain visible.

#![allow(dead_code)]

use std::mem::{replace, size_of};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use tidb_datatype::go_runtime::{go_64_next_slice_capacity_for_element, GoSliceElementLayout};

enum GoSliceBacking<T> {
    Nil,
    Owned(Box<OwnedBacking<T>>),
    Shared(Arc<RwLock<Vec<T>>>),
}

struct OwnedBacking<T> {
    values: Vec<T>,
}

/// A Go slice header whose initialized backing covers the complete logical
/// capacity. `Owned` is the no-lock fast path; sharing is always explicit.
pub(crate) struct GoSlice<T> {
    backing: GoSliceBacking<T>,
    start: usize,
    len: usize,
    capacity: usize,
}

pub(crate) enum GoSliceRead<'a, T> {
    Owned(&'a [T]),
    Shared {
        backing: RwLockReadGuard<'a, Vec<T>>,
        start: usize,
        len: usize,
    },
}

impl<T> Deref for GoSliceRead<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(values) => values,
            Self::Shared {
                backing,
                start,
                len,
            } => &backing[*start..*start + *len],
        }
    }
}

pub(crate) enum GoSliceWrite<'a, T> {
    Owned(&'a mut [T]),
    Shared {
        backing: RwLockWriteGuard<'a, Vec<T>>,
        start: usize,
        len: usize,
    },
}

impl<T> Deref for GoSliceWrite<'_, T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Owned(values) => values,
            Self::Shared {
                backing,
                start,
                len,
            } => &backing[*start..*start + *len],
        }
    }
}

impl<T> DerefMut for GoSliceWrite<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Owned(values) => values,
            Self::Shared {
                backing,
                start,
                len,
            } => &mut backing[*start..*start + *len],
        }
    }
}

impl<T> GoSlice<T> {
    /// Constructs an allocated header exposing exactly the supplied values.
    /// Rust spare capacity stays unobservable because Go capacity must be
    /// fully initialized and safely resliceable.
    pub(crate) fn from_vec(values: Vec<T>) -> Self {
        let len = values.len();
        Self {
            backing: GoSliceBacking::Owned(Box::new(OwnedBacking { values })),
            start: 0,
            len,
            capacity: len,
        }
    }

    /// Constructs an allocated header with fully initialized logical capacity.
    pub(crate) fn from_vec_with_capacity(mut values: Vec<T>, capacity: usize) -> Self
    where
        T: Default,
    {
        assert!(capacity >= values.len(), "Go slice cap is smaller than len");
        let len = values.len();
        values.resize_with(capacity, T::default);
        Self {
            backing: GoSliceBacking::Owned(Box::new(OwnedBacking { values })),
            start: 0,
            len,
            capacity,
        }
    }

    pub(crate) fn with_capacity(capacity: usize) -> Self
    where
        T: Default,
    {
        Self::from_vec_with_capacity(Vec::new(), capacity)
    }

    pub(crate) const fn is_allocated(&self) -> bool {
        !matches!(&self.backing, GoSliceBacking::Nil)
    }

    pub(crate) const fn is_shared(&self) -> bool {
        matches!(&self.backing, GoSliceBacking::Shared(_))
    }

    pub(crate) const fn start(&self) -> usize {
        self.start
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Exact bytes charged by this logical header, independent of the Rust
    /// allocator's spare capacity and without deduplicating shared backings.
    pub(crate) fn capacity_bytes(&self) -> usize {
        self.capacity
            .checked_mul(size_of::<T>())
            .expect("Go slice capacity accounting overflow")
    }

    /// Reports Go backing-array identity. Two nil headers compare equal;
    /// allocated-empty headers retain distinct identities.
    pub(crate) fn backing_ptr_eq(&self, other: &Self) -> bool {
        match (&self.backing, &other.backing) {
            (GoSliceBacking::Nil, GoSliceBacking::Nil) => true,
            (GoSliceBacking::Owned(left), GoSliceBacking::Owned(right)) => {
                std::ptr::eq(left.as_ref(), right.as_ref())
            }
            (GoSliceBacking::Shared(left), GoSliceBacking::Shared(right)) => {
                Arc::ptr_eq(left, right)
            }
            _ => false,
        }
    }

    /// Returns a guard-backed visible slice. Shared references cannot escape
    /// after the read lock is dropped.
    pub(crate) fn read_visible(&self) -> GoSliceRead<'_, T> {
        match &self.backing {
            GoSliceBacking::Nil => GoSliceRead::Owned(&[]),
            GoSliceBacking::Owned(values) => {
                GoSliceRead::Owned(&values.values[self.start..self.start + self.len])
            }
            GoSliceBacking::Shared(backing) => GoSliceRead::Shared {
                backing: backing
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                start: self.start,
                len: self.len,
            },
        }
    }

    /// Returns exclusive visible access, locking only a promoted backing.
    pub(crate) fn write_visible(&mut self) -> GoSliceWrite<'_, T> {
        match &mut self.backing {
            GoSliceBacking::Nil => GoSliceWrite::Owned(&mut []),
            GoSliceBacking::Owned(values) => {
                GoSliceWrite::Owned(&mut values.values[self.start..self.start + self.len])
            }
            GoSliceBacking::Shared(backing) => GoSliceWrite::Shared {
                backing: backing
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                start: self.start,
                len: self.len,
            },
        }
    }

    pub(crate) fn with_visible<R>(&self, read: impl FnOnce(&[T]) -> R) -> R {
        read(&self.read_visible())
    }

    pub(crate) fn with_visible_mut<R>(&mut self, write: impl FnOnce(&mut [T]) -> R) -> R {
        write(&mut self.write_visible())
    }

    pub(crate) fn snapshot(&self) -> Vec<T>
    where
        T: Clone,
    {
        self.read_visible().to_vec()
    }

    /// Promotes an owned backing once and returns a sibling with an identical
    /// header. Header changes thereafter remain independent.
    pub(crate) fn share(&mut self) -> Self {
        let sibling_backing = match replace(&mut self.backing, GoSliceBacking::Nil) {
            GoSliceBacking::Nil => GoSliceBacking::Nil,
            GoSliceBacking::Owned(values) => {
                let OwnedBacking { values } = *values;
                let shared = Arc::new(RwLock::new(values));
                self.backing = GoSliceBacking::Shared(Arc::clone(&shared));
                GoSliceBacking::Shared(shared)
            }
            GoSliceBacking::Shared(shared) => {
                self.backing = GoSliceBacking::Shared(Arc::clone(&shared));
                GoSliceBacking::Shared(shared)
            }
        };
        Self {
            backing: sibling_backing,
            start: self.start,
            len: self.len,
            capacity: self.capacity,
        }
    }

    /// Go two-index slicing: `header[low:high]` retains `cap(header)-low`.
    pub(crate) fn share_two_index(&mut self, low: usize, high: usize) -> Self {
        assert!(low <= high, "invalid Go slice range");
        assert!(high <= self.capacity, "Go slice bound exceeds capacity");
        let mut sibling = self.share();
        sibling.start += low;
        sibling.len = high - low;
        sibling.capacity -= low;
        sibling
    }

    /// Go full slicing: `header[low:high:max]` clips capacity to `max-low`.
    pub(crate) fn share_full(&mut self, low: usize, high: usize, max: usize) -> Self {
        assert!(low <= high, "invalid Go slice range");
        assert!(high <= max, "Go slice high bound exceeds max");
        assert!(max <= self.capacity, "Go slice max exceeds capacity");
        let mut sibling = self.share();
        sibling.start += low;
        sibling.len = high - low;
        sibling.capacity = max - low;
        sibling
    }

    /// Advances this header like repeated decoding from `header[count:]`.
    pub(crate) fn advance(&mut self, count: usize) {
        assert!(count <= self.len, "Go slice advance exceeds length");
        self.start += count;
        self.len -= count;
        self.capacity -= count;
    }

    /// Changes only this header's visible length, up to its capacity.
    pub(crate) fn reslice(&mut self, new_len: usize) {
        assert!(new_len <= self.capacity, "Go reslice exceeds capacity");
        self.len = new_len;
    }

    /// Applies Go `header = header[:0]`; unlike Go's `clear` built-in, this
    /// changes the header length and does not zero backing elements.
    pub(crate) fn reset_len(&mut self) {
        self.len = 0;
    }

    pub(crate) fn truncate(&mut self, new_len: usize) {
        assert!(new_len <= self.len, "Go truncate grows the slice");
        self.len = new_len;
    }

    /// Appends with Go 1.25's 64-bit capacity rule. Within-capacity writes
    /// stay shared; growth copies the visible range and detaches this header.
    pub(crate) fn append_owned_go(
        &mut self,
        values: Vec<T>,
        element_size: usize,
        layout: GoSliceElementLayout,
    ) where
        T: Clone + Default,
    {
        assert_eq!(
            element_size,
            size_of::<T>(),
            "Go element size differs from the backing element"
        );
        assert!(
            element_size > 0,
            "zero-width Go slices are not chunk buffers"
        );
        if values.is_empty() {
            return;
        }
        let old_len = self.len;
        let new_len = old_len
            .checked_add(values.len())
            .expect("Go slice length overflow");
        if new_len <= self.capacity {
            self.len = new_len;
            for (destination, value) in self.write_visible()[old_len..new_len]
                .iter_mut()
                .zip(values)
            {
                *destination = value;
            }
            return;
        }

        let new_capacity =
            go_64_next_slice_capacity_for_element(new_len, self.capacity, element_size, layout);
        let mut detached = self.snapshot();
        detached.extend(values);
        detached.resize_with(new_capacity, T::default);
        self.backing = GoSliceBacking::Owned(Box::new(OwnedBacking { values: detached }));
        self.start = 0;
        self.len = new_len;
        self.capacity = new_capacity;
    }

    /// Appends a sibling header's visible values without holding its read lock
    /// while this header acquires a write lock. This is the only header-to-
    /// header append surface, so same-backing appends cannot self-deadlock.
    pub(crate) fn append_from_go(
        &mut self,
        source: &Self,
        element_size: usize,
        layout: GoSliceElementLayout,
    ) where
        T: Clone + Default,
    {
        let source = source.snapshot();
        self.append_owned_go(source, element_size, layout);
    }

    /// Go `copy(dst[dst_start:], src)`. Snapshotting first makes overlapping
    /// copies and shared-backing copies safe without recursive lock acquisition.
    pub(crate) fn copy_from(&mut self, dst_start: usize, source: &Self) -> usize
    where
        T: Clone,
    {
        assert!(dst_start <= self.len, "Go copy destination out of bounds");
        let source = source.snapshot();
        let copied = source.len().min(self.len - dst_start);
        self.write_visible()[dst_start..dst_start + copied].clone_from_slice(&source[..copied]);
        copied
    }
}

impl GoSlice<u8> {
    /// Reads fixed-width native-endian storage into an aligned byte array.
    pub(crate) fn read_ne_bytes<const N: usize>(&self, offset: usize) -> [u8; N] {
        let end = offset.checked_add(N).expect("native-endian read overflow");
        let visible = self.read_visible();
        visible[offset..end]
            .try_into()
            .expect("native-endian read exceeds Go slice length")
    }

    /// Writes fixed-width native-endian storage from an aligned byte array.
    pub(crate) fn write_ne_bytes<const N: usize>(&mut self, offset: usize, value: [u8; N]) {
        let end = offset.checked_add(N).expect("native-endian write overflow");
        self.write_visible()[offset..end].copy_from_slice(&value);
    }
}

impl<T> Default for GoSlice<T> {
    fn default() -> Self {
        Self {
            backing: GoSliceBacking::Nil,
            start: 0,
            len: 0,
            capacity: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NO_POINTERS: GoSliceElementLayout = GoSliceElementLayout::NoPointers;

    #[test]
    fn nil_and_allocated_empty_headers_remain_distinct() {
        let nil = GoSlice::<u8>::default();
        let nil_peer = GoSlice::<u8>::default();
        let mut nil_for_subheader = GoSlice::<u8>::default();
        let nil_subheader = nil_for_subheader.share_two_index(0, 0);
        let mut empty = GoSlice::from_vec(Vec::<u8>::new());
        let empty_peer = empty.share_two_index(0, 0);

        assert!(!nil.is_allocated());
        assert!(empty.is_allocated());
        assert_eq!((nil.len(), nil.capacity()), (0, 0));
        assert_eq!((empty.len(), empty.capacity()), (0, 0));
        assert!(nil.backing_ptr_eq(&nil_peer));
        assert!(!nil_subheader.is_allocated());
        assert!(!nil.backing_ptr_eq(&empty));
        assert!(empty_peer.is_allocated());
        assert!(empty.backing_ptr_eq(&empty_peer));
    }

    #[test]
    fn subheaders_share_backing_but_keep_independent_geometry() {
        let mut source = GoSlice::from_vec_with_capacity(vec![10_i64, 20, 30], 6);
        assert!(!source.is_shared());

        let mut two_index = source.share_two_index(1, 3);
        let mut full = source.share_full(1, 2, 4);
        let mut chained = two_index.share_full(1, 2, 4);
        assert!(source.is_shared());
        assert!(source.backing_ptr_eq(&two_index));
        assert!(source.backing_ptr_eq(&full));
        assert!(source.backing_ptr_eq(&chained));
        assert_eq!((two_index.len(), two_index.capacity()), (2, 5));
        assert_eq!((full.len(), full.capacity()), (1, 3));
        assert_eq!(
            (chained.start(), chained.len(), chained.capacity()),
            (2, 1, 3)
        );
        assert_eq!((two_index.start(), full.start()), (1, 1));
        assert_eq!(two_index.capacity_bytes(), 5 * size_of::<i64>());

        two_index.write_visible()[0] = 21;
        chained.write_visible()[0] = 31;
        two_index.reslice(4);
        full.advance(1);
        full.reslice(2);

        assert_eq!(source.snapshot(), vec![10, 21, 31]);
        assert_eq!(two_index.snapshot(), vec![21, 31, 0, 0]);
        assert_eq!(full.snapshot(), vec![31, 0]);
        assert_eq!(full.start(), 2);
        assert_eq!((source.len(), source.capacity()), (3, 6));
    }

    #[test]
    fn reset_reappend_shares_within_capacity_and_growth_detaches_one_header() {
        let mut backing = GoSlice::from_vec_with_capacity(vec![0_u8, 1, 2, 3], 8);
        let mut source = backing.share_full(2, 4, 5);
        let peer = source.share();

        source.reset_len();
        source.append_owned_go(vec![7, 8, 9], 1, NO_POINTERS);
        assert!(source.backing_ptr_eq(&peer));
        assert!(source.backing_ptr_eq(&backing));
        assert_eq!(peer.snapshot(), vec![7, 8]);
        assert_eq!(backing.snapshot(), vec![0, 1, 7, 8]);

        source.append_owned_go(vec![10], 1, NO_POINTERS);
        assert!(!source.backing_ptr_eq(&peer));
        assert!(!source.backing_ptr_eq(&backing));
        assert!(!source.is_shared());
        assert_eq!((source.start(), source.len()), (0, 4));
        assert_eq!(source.snapshot(), vec![7, 8, 9, 10]);
        assert_eq!(peer.snapshot(), vec![7, 8]);
        assert_eq!(backing.snapshot(), vec![0, 1, 7, 8]);
        assert_eq!(
            source.capacity(),
            go_64_next_slice_capacity_for_element(4, 3, 1, NO_POINTERS)
        );
    }

    #[test]
    fn guarded_access_copy_truncate_and_native_endian_helpers_are_safe() {
        let value = 0x0102_0304_0506_0708_i64;
        let mut bytes = GoSlice::from_vec_with_capacity(value.to_ne_bytes().to_vec(), 16);
        assert!(!bytes.is_shared());
        bytes.write_ne_bytes(0, (-value).to_ne_bytes());
        assert_eq!(i64::from_ne_bytes(bytes.read_ne_bytes::<8>(0)), -value);
        assert!(!bytes.is_shared());

        let peer = bytes.share();
        bytes.write_ne_bytes(0, value.to_ne_bytes());
        assert_eq!(i64::from_ne_bytes(peer.read_ne_bytes::<8>(0)), value);

        bytes.reslice(16);
        assert_eq!(bytes.copy_from(8, &peer), 8);
        assert_eq!(i64::from_ne_bytes(bytes.read_ne_bytes::<8>(8)), value);
        bytes.truncate(8);
        bytes.with_visible_mut(|visible| visible[0] = 0x11);
        assert_eq!(bytes.with_visible(|visible| visible[0]), 0x11);
        assert_eq!(peer.read_visible()[0], 0x11);
    }

    #[test]
    fn append_from_same_backing_releases_the_read_guard_before_writing() {
        let mut destination = GoSlice::from_vec_with_capacity(vec![1_u8, 2], 4);
        let source = destination.share();

        destination.append_from_go(&source, 1, NO_POINTERS);

        assert!(destination.backing_ptr_eq(&source));
        assert_eq!(destination.snapshot(), vec![1, 2, 1, 2]);
        assert_eq!(source.snapshot(), vec![1, 2]);
    }

    #[test]
    fn go_slice_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<GoSlice<u8>>();

        let slice = GoSlice::with_capacity(4);
        let returned = std::thread::spawn(move || slice).join().expect("worker");
        assert!(returned.is_allocated());
        assert_eq!((returned.len(), returned.capacity()), (0, 4));
    }
}
