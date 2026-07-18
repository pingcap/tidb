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

//! Source-shaped parser allocation primitives.
//!
//! Go's generic `Alloc[T]` deliberately falls back to `new(T)`. Its optimized
//! path is a typed slab: batches retain the runtime type so the garbage
//! collector can trace pointer fields. Rust already traces ownership through
//! types, so [`Slab`] keeps one reference-counted typed batch. A [`SlabHandle`]
//! keeps its batch alive across [`Slab::reset`], exactly matching the Go
//! lifetime contract without exposing raw pointers or a byte arena.

use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

/// Number of values in one source typed-slab batch.
pub const SLAB_SIZE: usize = 64;

/// Go's retained historical block-size constant.
pub const DEFAULT_BLOCK_SIZE: usize = 8 * 1024;

/// Generic allocation context.
///
/// The current Go generic path is intentionally stateless: hot types own
/// dedicated typed slabs, while all other types use `new(T)`. Rust mirrors
/// that split through [`Slab`] and [`alloc`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Arena;

impl Arena {
    /// Creates an empty generic allocation context.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Resets generic allocation state.
    ///
    /// The source generic fallback has no retained state; dedicated
    /// [`Slab`] owners reset their batches directly.
    pub const fn reset(&mut self) {}
}

/// Allocates a zero/default-initialized value through the source generic
/// fallback.
#[must_use]
pub fn alloc<T: Default>(_arena: &Arena) -> Box<T> {
    Box::default()
}

/// Allocates `len` default-initialized values, or `None` for a zero length.
///
/// `None` preserves the source's observable nil-slice result instead of
/// silently collapsing it into an empty, non-null collection.
#[must_use]
pub fn alloc_slice<T: Default>(_: &Arena, len: usize) -> Option<Vec<T>> {
    (len != 0).then(|| std::iter::repeat_with(T::default).take(len).collect())
}

/// Stable handle to one value in a typed slab batch.
///
/// Cloning a handle shares the same value. The batch remains alive until the
/// last handle is dropped, including after the owning slab is reset.
#[derive(Debug)]
pub struct SlabHandle<T> {
    batch: Rc<[RefCell<T>]>,
    index: usize,
}

impl<T> Clone for SlabHandle<T> {
    fn clone(&self) -> Self {
        Self {
            batch: Rc::clone(&self.batch),
            index: self.index,
        }
    }
}

impl<T> SlabHandle<T> {
    /// Borrows the allocated value.
    pub fn borrow(&self) -> Ref<'_, T> {
        self.batch[self.index].borrow()
    }

    /// Mutably borrows the allocated value.
    pub fn borrow_mut(&self) -> RefMut<'_, T> {
        self.batch[self.index].borrow_mut()
    }

    /// Returns a stable identity pointer for diagnostics and source tests.
    #[must_use]
    pub fn as_ptr(&self) -> *mut T {
        self.batch[self.index].as_ptr()
    }
}

/// GC-safe typed batch allocator corresponding to Go's `slab[T]`.
#[derive(Debug)]
pub struct Slab<T> {
    current: Option<Rc<[RefCell<T>]>>,
    next: usize,
}

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self {
            current: None,
            next: SLAB_SIZE,
        }
    }
}

impl<T: Default> Slab<T> {
    /// Allocates one default-initialized value, growing by one typed batch
    /// when the current batch is exhausted.
    pub fn alloc(&mut self) -> SlabHandle<T> {
        if self.next >= SLAB_SIZE {
            let batch: Rc<[RefCell<T>]> = std::iter::repeat_with(|| RefCell::new(T::default()))
                .take(SLAB_SIZE)
                .collect::<Vec<_>>()
                .into();
            self.current = Some(batch);
            self.next = 0;
        }
        let index = self.next;
        self.next += 1;
        SlabHandle {
            batch: Rc::clone(self.current.as_ref().expect("slab batch exists")),
            index,
        }
    }

    /// Releases retained batches and restarts allocation at the first slot.
    /// Existing handles keep their old batches alive.
    pub fn reset(&mut self) {
        self.current = None;
        self.next = SLAB_SIZE;
    }
}
