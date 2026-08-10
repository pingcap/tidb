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

//! Safe Rust landing for Go `pkg/util/arena` (`arena.go`).
//!
//! A pre-allocating byte allocator that reduces allocation cost. Go's
//! `SimpleAllocator.Alloc` hands back `arena[off:off:off+cap]` — a length-0,
//! capacity-`cap` slice into a shared backing array — advancing `off` while it
//! fits, else a fresh `make`. This API keeps the source len, capacity, panic,
//! and offset contracts, but returns owned [`Vec<u8>`] buffers. It therefore
//! does not claim Go's shared mutable backing or stale-byte reuse after
//! `Reset`: preserving those with the same Vec API would violate Rust
//! ownership, and this workspace forbids unsafe code.

/// Pre-allocates memory to reduce memory allocation cost. It is not
/// thread-safe.
pub trait Allocator {
    /// Allocates a buffer with length 0 and capacity `capacity`.
    fn alloc(&mut self, capacity: usize) -> Vec<u8>;

    /// Allocates a buffer with `length` and `capacity`.
    fn alloc_with_len(&mut self, length: usize, capacity: usize) -> Vec<u8>;

    /// Resets the arena offset. All previously allocated memory must no longer
    /// be in use.
    fn reset(&mut self);
}

/// A simple implementation of [`Allocator`].
pub struct SimpleAllocator {
    arena: Vec<u8>,
    off: usize,
}

impl SimpleAllocator {
    /// Creates a `SimpleAllocator` with a specified capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            arena: Vec::with_capacity(capacity),
            off: 0,
        }
    }
}

impl Allocator for SimpleAllocator {
    fn alloc(&mut self, capacity: usize) -> Vec<u8> {
        // Go advances `off` and returns a slice into the arena only while the
        // request fits (strict `<`, matching the source); otherwise it falls
        // back to a fresh allocation and leaves `off` untouched.
        if self.off + capacity < self.arena.capacity() {
            self.off += capacity;
        }
        Vec::with_capacity(capacity)
    }

    fn alloc_with_len(&mut self, length: usize, capacity: usize) -> Vec<u8> {
        let mut slice = self.alloc(capacity);
        assert!(
            length <= capacity,
            "allocation length {length} exceeds capacity {capacity}"
        );
        slice.resize(length, 0);
        slice
    }

    fn reset(&mut self) {
        self.off = 0;
    }
}

/// Implements [`Allocator`] without pre-allocating memory. Go exposes the
/// singleton `StdAllocator`; this zero-sized type is its Rust equivalent.
pub struct StdAllocator;

impl Allocator for StdAllocator {
    fn alloc(&mut self, capacity: usize) -> Vec<u8> {
        Vec::with_capacity(capacity)
    }

    fn alloc_with_len(&mut self, length: usize, capacity: usize) -> Vec<u8> {
        let mut slice = self.alloc(capacity);
        assert!(
            length <= capacity,
            "allocation length {length} exceeds capacity {capacity}"
        );
        slice.resize(length, 0);
        slice
    }

    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {
    use std::panic::{catch_unwind, AssertUnwindSafe};

    use super::{Allocator, SimpleAllocator, StdAllocator};

    const ARENA_CAP: usize = 1000;
    const ALLOC_CAP_SMALL: usize = 10;
    const ALLOC_CAP_MEDIUM: usize = 20;
    const ALLOC_CAP_OUT: usize = 1024;

    // Go `TestSimpleArenaAllocator`.
    #[test]
    fn simple_arena_allocator() {
        let mut arena = SimpleAllocator::new(ARENA_CAP);
        let slice = arena.alloc(ALLOC_CAP_SMALL);
        assert_eq!(arena.off, ALLOC_CAP_SMALL);
        assert_eq!(slice.len(), 0);
        assert_eq!(slice.capacity(), ALLOC_CAP_SMALL);

        let slice = arena.alloc(ALLOC_CAP_MEDIUM);
        assert_eq!(arena.off, ALLOC_CAP_SMALL + ALLOC_CAP_MEDIUM);
        assert_eq!(slice.len(), 0);
        assert_eq!(slice.capacity(), ALLOC_CAP_MEDIUM);

        // Does not fit the arena: `off` is unchanged and the allocation falls
        // back to a fresh buffer of the requested capacity.
        let slice = arena.alloc(ALLOC_CAP_OUT);
        assert_eq!(arena.off, ALLOC_CAP_SMALL + ALLOC_CAP_MEDIUM);
        assert_eq!(slice.len(), 0);
        assert_eq!(slice.capacity(), ALLOC_CAP_OUT);

        let slice = arena.alloc_with_len(2, ALLOC_CAP_SMALL);
        assert_eq!(
            arena.off,
            ALLOC_CAP_SMALL + ALLOC_CAP_MEDIUM + ALLOC_CAP_SMALL
        );
        assert_eq!(slice.len(), 2);
        assert_eq!(slice.capacity(), ALLOC_CAP_SMALL);

        arena.reset();
        assert_eq!(arena.off, 0);
        assert_eq!(arena.arena.capacity(), ARENA_CAP);
    }

    // Go `TestStdAllocator`.
    #[test]
    fn std_allocator() {
        let mut allocator = StdAllocator;
        let slice = allocator.alloc(ALLOC_CAP_MEDIUM);
        assert_eq!(slice.len(), 0);
        assert_eq!(slice.capacity(), ALLOC_CAP_MEDIUM);

        let slice = allocator.alloc_with_len(ALLOC_CAP_SMALL, ALLOC_CAP_MEDIUM);
        assert_eq!(slice.len(), ALLOC_CAP_SMALL);
        assert_eq!(slice.capacity(), ALLOC_CAP_MEDIUM);

        allocator.reset();
        let slice = allocator.alloc(ALLOC_CAP_SMALL);
        assert_eq!(slice.len(), 0);
        assert_eq!(slice.capacity(), ALLOC_CAP_SMALL);
    }

    #[test]
    fn source_simple_length_over_capacity_panics_after_allocating_capacity() {
        let mut allocator = SimpleAllocator::new(8);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = allocator.alloc_with_len(3, 2);
        }));

        assert!(result.is_err(), "Go rejects a reslice beyond capacity");
        assert_eq!(allocator.off, 2, "Go allocates capacity before panicking");
    }

    #[test]
    fn source_std_length_over_capacity_panics() {
        let mut allocator = StdAllocator;
        let result = catch_unwind(AssertUnwindSafe(|| {
            let _ = allocator.alloc_with_len(3, 2);
        }));

        assert!(
            result.is_err(),
            "Go make rejects length greater than capacity"
        );
    }

    #[test]
    fn source_strict_exact_fit_and_reset_contracts() {
        let mut allocator = SimpleAllocator::new(4);
        let exact = allocator.alloc(4);
        assert_eq!(exact.len(), 0);
        assert_eq!(exact.capacity(), 4);
        assert_eq!(allocator.off, 0, "Go uses strict less-than for arena fit");

        let within = allocator.alloc(3);
        assert_eq!(within.capacity(), 3);
        assert_eq!(allocator.off, 3);

        allocator.reset();
        assert_eq!(allocator.off, 0);
        assert_eq!(allocator.arena.capacity(), 4);
    }

    #[test]
    fn safe_rust_owned_buffers_are_zeroed_after_reset() {
        let mut allocator = SimpleAllocator::new(4);
        let mut first = allocator.alloc(2);
        first.push(9);
        drop(first);

        allocator.reset();
        let reused = allocator.alloc_with_len(1, 2);
        assert_eq!(reused, [0]);
    }
}
