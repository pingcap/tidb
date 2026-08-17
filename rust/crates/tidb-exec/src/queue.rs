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

//! Circular-buffer queue, transcreated from Go package `pkg/util/queue`
//! (pkg/util/queue/queue.go).
//!
//! Upstream is a generic circular-buffer queue used by executor-tier code. The
//! whole package is one file and one exported type; this module carries every
//! production symbol of it.
//!
//! Go symbol -> Rust symbol:
//!   * `queue.Queue[T]` -> [`Queue`]
//!   * `queue.NewQueue[T](capacity)` -> [`Queue::with_capacity`]
//!   * `(*Queue[T]).Push` -> [`Queue::push`]
//!   * `(*Queue[T]).Pop` -> [`Queue::pop`]
//!   * `(*Queue[T]).Len` -> [`Queue::len`]
//!   * `(*Queue[T]).IsEmpty` -> [`Queue::is_empty`]
//!   * `(*Queue[T]).Clear` -> [`Queue::clear`]
//!   * `(*Queue[T]).ClearAndExpandIfNeed` -> [`Queue::clear_and_expand_if_need`]
//!   * `(*Queue[T]).Cap` -> [`Queue::cap`]
//!
//! Observable behaviors deliberately preserved rather than "fixed":
//!   * `Pop` on an empty queue panics (Go: `panic("Queue is empty")`). It is not
//!     an `Option`-returning API upstream, and callers rely on the panic being a
//!     programming-error signal, so the panic is reproduced with the same message.
//!   * The Go zero value `queue.Queue[T]{}` has a *nil* `elements` slice, and
//!     `Push` special-cases nil by allocating a 1-element backing array. A queue
//!     built by `NewQueue(0)` instead holds a non-nil *empty* slice, for which
//!     that special case does not fire and `Push` panics on the out-of-range
//!     index write. Rust `Vec` has no nil/empty distinction, so `Queue::default`
//!     records the nil state explicitly in `elements_is_nil` to keep both
//!     upstream behaviors distinguishable.
//!   * Growth is exactly "double the backing array when full", and the
//!     reallocation re-bases the ring at head = 0, so `Cap` after growth is
//!     `2 * old_cap`.

use std::fmt;

/// A circular-buffer queue.
///
/// Transcreation of Go `queue.Queue[T]` (pkg/util/queue/queue.go:18-23).
pub struct Queue<T> {
    /// Backing ring. `None` slots stand for Go's zero-valued slice elements:
    /// Go never clears a popped slot, but the slot's contents are unreachable
    /// through the API, so taking the value out is observationally identical.
    elements: Vec<Option<T>>,
    /// Tracks whether `elements` corresponds to Go's *nil* slice rather than a
    /// non-nil empty one. Only the Go zero value has a nil slice.
    elements_is_nil: bool,
    head: usize,
    tail: usize,
    size: usize,
}

impl<T> Queue<T> {
    /// Creates a new queue with the given capacity.
    ///
    /// Transcreation of Go `NewQueue[T]` (pkg/util/queue/queue.go:26-30).
    ///
    /// Note that `with_capacity(0)` reproduces Go's `NewQueue[T](0)`: the
    /// backing slice is non-nil but empty, so the very first `push` panics.
    /// Use `Queue::default` for the Go zero value, which grows on first push.
    pub fn with_capacity(capacity: usize) -> Self {
        let mut elements = Vec::with_capacity(capacity);
        elements.resize_with(capacity, || None);
        Self {
            elements,
            elements_is_nil: false,
            head: 0,
            tail: 0,
            size: 0,
        }
    }

    /// Pushes an element to the queue.
    ///
    /// Transcreation of Go `(*Queue[T]).Push` (pkg/util/queue/queue.go:33-51).
    ///
    /// # Panics
    ///
    /// Panics when the backing ring has zero capacity and cannot grow, matching
    /// Go's index-out-of-range panic on `r.elements[r.tail] = element`. That is
    /// only reachable via `Queue::with_capacity(0)`.
    pub fn push(&mut self, element: T) {
        if self.elements_is_nil {
            self.elements = vec_of_none(1);
            self.elements_is_nil = false;
        }

        if self.size == self.elements.len() {
            // Double capacity when full.
            let mut new_elements = vec_of_none(self.elements.len() * 2);
            let old_len = self.elements.len();
            for (i, slot) in new_elements.iter_mut().enumerate().take(self.size) {
                // `old_len` is nonzero whenever this loop body runs, because
                // `self.size == old_len` and `size > 0` here.
                *slot = self.elements[(self.head + i) % old_len].take();
            }
            self.elements = new_elements;
            self.head = 0;
            self.tail = self.size;
        }

        assert!(
            self.tail < self.elements.len(),
            "index out of range [{}] with length {}",
            self.tail,
            self.elements.len()
        );
        self.elements[self.tail] = Some(element);
        self.tail = (self.tail + 1) % self.elements.len();
        self.size += 1;
    }

    /// Pops an element from the queue.
    ///
    /// Transcreation of Go `(*Queue[T]).Pop` (pkg/util/queue/queue.go:54-62).
    ///
    /// # Panics
    ///
    /// Panics with `"Queue is empty"` when the queue is empty, exactly as Go does.
    pub fn pop(&mut self) -> T {
        assert!(self.size != 0, "Queue is empty");
        let element = self.elements[self.head]
            .take()
            .expect("occupied ring slot must hold a value");
        self.head = (self.head + 1) % self.elements.len();
        self.size -= 1;
        element
    }

    /// Returns the number of elements in the queue.
    ///
    /// Transcreation of Go `(*Queue[T]).Len` (pkg/util/queue/queue.go:65-67).
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the queue is empty.
    ///
    /// Transcreation of Go `(*Queue[T]).IsEmpty` (pkg/util/queue/queue.go:70-72).
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Clears the queue.
    ///
    /// Transcreation of Go `(*Queue[T]).Clear` (pkg/util/queue/queue.go:75-79).
    ///
    /// Like Go, this only resets the ring indices; the backing array keeps its
    /// capacity. Unlike Go it also drops the elements still held in the ring,
    /// because leaving them alive would leak in Rust's ownership model whereas
    /// Go's GC reclaims them once the slice slot is overwritten.
    pub fn clear(&mut self) {
        for slot in &mut self.elements {
            *slot = None;
        }
        self.head = 0;
        self.tail = 0;
        self.size = 0;
    }

    /// Clears the queue and tries to expand the backing array.
    ///
    /// Transcreation of Go `(*Queue[T]).ClearAndExpandIfNeed`
    /// (pkg/util/queue/queue.go:82-88). Go reallocates a *fresh* slice of the
    /// requested size when the current one is smaller, so the capacity becomes
    /// exactly `size` rather than at-least-`size`.
    pub fn clear_and_expand_if_need(&mut self, size: usize) {
        self.clear();

        if self.elements.len() < size {
            self.elements = vec_of_none(size);
            self.elements_is_nil = false;
        }
    }

    /// Returns the capacity of the queue.
    ///
    /// Transcreation of Go `(*Queue[T]).Cap` (pkg/util/queue/queue.go:91-93),
    /// which reports `len(r.elements)` — the ring size, not Go's `cap`.
    pub fn cap(&self) -> usize {
        self.elements.len()
    }

    /// Index of the ring slot holding the front element.
    ///
    /// Not an upstream export: Go's `queue_test.go` asserts directly on the
    /// unexported `q.head` field, which an out-of-module Rust test cannot reach.
    /// Exposed read-only so the transcreated circular-buffer test can pin the
    /// same wrap-around invariant.
    pub fn head_index(&self) -> usize {
        self.head
    }

    /// Index of the ring slot that the next push will write.
    ///
    /// Not an upstream export; see [`Queue::head_index`].
    pub fn tail_index(&self) -> usize {
        self.tail
    }
}

/// Go's zero value `queue.Queue[T]{}`: a nil backing slice that `Push` grows to
/// one element on first use.
impl<T> Default for Queue<T> {
    fn default() -> Self {
        Self {
            elements: Vec::new(),
            elements_is_nil: true,
            head: 0,
            tail: 0,
            size: 0,
        }
    }
}

impl<T> fmt::Debug for Queue<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Queue")
            .field("len", &self.size)
            .field("cap", &self.elements.len())
            .field("head", &self.head)
            .field("tail", &self.tail)
            .finish()
    }
}

fn vec_of_none<T>(len: usize) -> Vec<Option<T>> {
    let mut v = Vec::with_capacity(len);
    v.resize_with(len, || None);
    v
}
