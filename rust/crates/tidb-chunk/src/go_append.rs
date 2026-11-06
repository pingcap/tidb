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

//! Go `append`'s growth (`runtime.growslice`) for a column's buffers.
//!
//! A column's capacities are what `Chunk.MemoryUsage` reports, so they grow
//! the way Go's slices do: an append that does not fit doubles a small
//! slice, grows a large one by a quarter, and rounds the allocation up to an
//! allocator size class. Each Go `append` is one growth step; `slices.Grow`
//! reserves past the current capacity.

use tidb_goruntime::go_runtime::{go_64_next_slice_capacity_for_element, GoSliceElementLayout};

/// The capacity Go's `append` of `additional` elements to a slice of `len`
/// and `capacity` leaves.
fn next_capacity<T>(len: usize, capacity: usize, additional: usize) -> usize {
    let needed = len.checked_add(additional).expect("Go slice length overflow");
    if needed <= capacity {
        return capacity;
    }
    go_64_next_slice_capacity_for_element(
        needed,
        capacity,
        std::mem::size_of::<T>().max(1),
        GoSliceElementLayout::NoPointers,
    )
}

/// Grows `vec` as Go's `append(s, x...)` of `additional` elements does.
pub(crate) fn go_grow<T>(vec: &mut Vec<T>, additional: usize) {
    let capacity = next_capacity::<T>(vec.len(), vec.capacity(), additional);
    if capacity > vec.capacity() {
        vec.reserve_exact(capacity - vec.len());
    }
}

/// Go `slices.Grow(s, n)`: room for `n` more elements, grown -- when there
/// is not -- as `append(s[:cap(s)], make([]T, n-(cap(s)-len(s)))...)` grows
/// it, to a length of `len(s)+n` from the capacity.
pub(crate) fn go_slices_grow<T>(vec: &mut Vec<T>, n: usize) {
    if vec.capacity() - vec.len() >= n {
        return;
    }
    let capacity = next_capacity::<T>(vec.len(), vec.capacity(), n);
    vec.reserve_exact(capacity - vec.len());
}

/// Go `append(make([]T, 0), src...)`: a new slice holding `src` with the
/// capacity that one append from empty leaves (`CopyConstruct`).
pub(crate) fn go_appended<T: Clone>(src: &[T]) -> Vec<T> {
    let mut vec = Vec::new();
    go_grow(&mut vec, src.len());
    vec.extend_from_slice(src);
    vec
}

/// [`Vec`] appends that grow like Go's.
pub(crate) trait GoAppend<T> {
    /// Go `s = append(s, value)`.
    fn go_push(&mut self, value: T);

    /// Go `s = append(s, src...)`.
    fn go_extend_from_slice(&mut self, src: &[T])
    where
        T: Clone;
}

impl<T> GoAppend<T> for Vec<T> {
    #[inline]
    fn go_push(&mut self, value: T) {
        if self.len() == self.capacity() {
            go_grow(self, 1);
        }
        self.push(value);
    }

    #[inline]
    fn go_extend_from_slice(&mut self, src: &[T])
    where
        T: Clone,
    {
        go_grow(self, src.len());
        self.extend_from_slice(src);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Go program on this machine (go1.26, 64-bit): appending one byte to a
    /// full 32-byte slice leaves 64, 800 bytes to an empty 256-capacity one
    /// 896, `append(nil, 7 bytes...)` 8, and one int64 to a full 33-element
    /// slice 72 (66 elements round up to the 576-byte size class).
    #[test]
    fn growth_follows_go_growslice() {
        let mut bytes: Vec<u8> = Vec::with_capacity(32);
        bytes.extend_from_slice(&[0; 32]);
        bytes.go_push(1);
        assert_eq!(bytes.capacity(), 64);

        let mut bytes: Vec<u8> = Vec::with_capacity(256);
        go_grow(&mut bytes, 800);
        assert_eq!(bytes.capacity(), 896);

        assert_eq!(go_appended(&[1u8; 7]).capacity(), 8);

        let mut offsets: Vec<i64> = Vec::with_capacity(33);
        offsets.extend_from_slice(&[0; 33]);
        offsets.go_push(1);
        assert_eq!(offsets.capacity(), 72);
    }

    /// Go `slices.Grow` (same program): 8 more in an 8-of-16 slice stays 16, 9
    /// more grows it to 32, and 800 more in an empty 256-capacity one 896 --
    /// a length of 800 grown from the capacity, not 1056.
    #[test]
    fn slices_grow_grows_from_the_capacity() {
        let mut bytes: Vec<u8> = Vec::with_capacity(16);
        bytes.extend_from_slice(&[0; 8]);
        go_slices_grow(&mut bytes, 8);
        assert_eq!(bytes.capacity(), 16);
        go_slices_grow(&mut bytes, 9);
        assert_eq!(bytes.capacity(), 32);

        let mut bytes: Vec<u8> = Vec::with_capacity(256);
        go_slices_grow(&mut bytes, 800);
        assert_eq!(bytes.capacity(), 896);
    }
}
