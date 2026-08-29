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

//! Safe Rust implementation of Go `pkg/util/arena` (`arena.go`).

use std::cell::Cell;
use std::rc::Rc;

/// A byte-slice descriptor over shared backing storage.
///
/// Cloning copies the descriptor while retaining the same backing allocation,
/// matching assignment of a Go byte slice.
#[derive(Clone)]
pub struct ArenaBytes {
    storage: Rc<Vec<Cell<u8>>>,
    start: usize,
    len: usize,
    capacity: usize,
}

impl ArenaBytes {
    fn fresh(length: usize, capacity: usize) -> Self {
        let storage = Rc::new((0..capacity).map(|_| Cell::new(0)).collect());
        let mut bytes = Self {
            storage,
            start: 0,
            len: 0,
            capacity,
        };
        bytes.set_len(length);
        bytes
    }

    fn shared(storage: Rc<Vec<Cell<u8>>>, start: usize, capacity: usize) -> Self {
        Self {
            storage,
            start,
            len: 0,
            capacity,
        }
    }

    /// Reslices to `length` without clearing bytes newly brought into view.
    pub fn set_len(&mut self, length: usize) {
        assert!(
            length <= self.capacity,
            "allocation length {length} exceeds capacity {}",
            self.capacity
        );
        self.len = length;
    }

    /// Returns the logical slice length.
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Reports whether the logical slice is empty.
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the slice capacity.
    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    /// Reads a byte within the logical slice.
    pub fn get(&self, index: usize) -> Option<u8> {
        (index < self.len).then(|| self.storage[self.start + index].get())
    }

    /// Replaces a byte within the logical slice.
    pub fn set(&self, index: usize, value: u8) {
        assert!(
            index < self.len,
            "index {index} exceeds length {}",
            self.len
        );
        self.storage[self.start + index].set(value);
    }

    /// Copies the logical slice bytes into an owned vector.
    pub fn to_vec(&self) -> Vec<u8> {
        (0..self.len)
            .map(|index| self.storage[self.start + index].get())
            .collect()
    }
}

/// Pre-allocates memory to reduce memory allocation cost. It is not
/// thread-safe.
pub trait Allocator {
    /// Allocates a buffer with length 0 and capacity `capacity`.
    fn alloc(&mut self, capacity: usize) -> ArenaBytes;

    /// Allocates a buffer with `length` and `capacity`.
    fn alloc_with_len(&mut self, length: usize, capacity: usize) -> ArenaBytes;

    /// Resets the arena offset. All previously allocated memory must no longer
    /// be in use.
    fn reset(&mut self);
}

/// A simple implementation of [`Allocator`].
pub struct SimpleAllocator {
    arena: Rc<Vec<Cell<u8>>>,
    off: usize,
}

impl SimpleAllocator {
    /// Creates a `SimpleAllocator` with a specified capacity.
    pub fn new(capacity: usize) -> Self {
        Self {
            arena: Rc::new((0..capacity).map(|_| Cell::new(0)).collect()),
            off: 0,
        }
    }
}

impl Allocator for SimpleAllocator {
    fn alloc(&mut self, capacity: usize) -> ArenaBytes {
        let end = self
            .off
            .checked_add(capacity)
            .expect("arena allocation offset overflow");
        if end < self.arena.len() {
            let bytes = ArenaBytes::shared(Rc::clone(&self.arena), self.off, capacity);
            self.off = end;
            return bytes;
        }
        ArenaBytes::fresh(0, capacity)
    }

    fn alloc_with_len(&mut self, length: usize, capacity: usize) -> ArenaBytes {
        let mut bytes = self.alloc(capacity);
        bytes.set_len(length);
        bytes
    }

    fn reset(&mut self) {
        self.off = 0;
    }
}

/// Implements [`Allocator`] without pre-allocating memory. Go exposes the
/// singleton `StdAllocator`; this zero-sized type is its Rust equivalent.
pub struct StdAllocator;

impl Allocator for StdAllocator {
    fn alloc(&mut self, capacity: usize) -> ArenaBytes {
        ArenaBytes::fresh(0, capacity)
    }

    fn alloc_with_len(&mut self, length: usize, capacity: usize) -> ArenaBytes {
        ArenaBytes::fresh(length, capacity)
    }

    fn reset(&mut self) {}
}

#[cfg(test)]
mod tests {
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
        assert_eq!(arena.arena.len(), ARENA_CAP);
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
    }

    #[test]
    fn reset_reuses_the_go_backing_storage() {
        let mut allocator = SimpleAllocator::new(4);
        let first = allocator.alloc_with_len(1, 2);
        let alias = first.clone();
        first.set(0, 9);
        assert_eq!(alias.get(0), Some(9));
        drop(first);
        drop(alias);

        allocator.reset();
        let reused = allocator.alloc_with_len(1, 2);
        assert_eq!(reused.to_vec(), [9]);
    }
}
