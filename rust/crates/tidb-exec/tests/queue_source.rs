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

//! Source-backed tests for the circular-buffer queue.
//!
//! Upstream coverage: pkg/util/queue/queue_test.go:22-86 (`TestQueue`), whose
//! four `t.Run` subtests are transcreated one-for-one below. The two tests
//! marked WRITTEN cover production symbols that upstream leaves untested
//! (`ClearAndExpandIfNeed`) and the zero-value/`NewQueue(0)` split that Go gets
//! from nil-vs-empty slices; they are labeled so they are not mistaken for
//! upstream-pinned behavior.

use tidb_exec::queue::Queue;

/// TRANSCREATED from queue_test.go:23-47 (`TestQueue/basic operations`).
#[test]
fn queue_basic_operations() {
    let mut q: Queue<i32> = Queue::with_capacity(2);

    // Initial state.
    assert!(q.is_empty(), "new queue should be empty");
    assert_eq!(q.len(), 0, "new queue should have length 0");
    assert_eq!(q.cap(), 2, "new queue should have capacity 2");

    // Push.
    q.push(1);
    q.push(2);
    assert_eq!(q.len(), 2, "queue length should be 2 after pushing 2 elements");
    assert!(
        !q.is_empty(),
        "queue should not be empty after pushing elements"
    );

    // Automatic capacity increase.
    q.push(3);
    assert_eq!(q.cap(), 4, "queue capacity should double when full");

    // Pop.
    assert_eq!(q.pop(), 1, "first pop should return 1");
    assert_eq!(q.pop(), 2, "second pop should return 2");
    assert_eq!(q.pop(), 3, "third pop should return 3");

    assert!(
        q.is_empty(),
        "queue should be empty after popping all elements"
    );
}

/// TRANSCREATED from queue_test.go:49-58 (`TestQueue/clear operation`).
#[test]
fn queue_clear_operation() {
    let mut q: Queue<String> = Queue::with_capacity(4);
    q.push("a".to_owned());
    q.push("b".to_owned());
    q.push("c".to_owned());

    q.clear();
    assert!(q.is_empty(), "queue should be empty after clear");
    assert_eq!(q.len(), 0, "queue length should be 0 after clear");
}

/// TRANSCREATED from queue_test.go:60-68 (`TestQueue/panic on empty pop`).
#[test]
#[should_panic(expected = "Queue is empty")]
fn queue_pop_on_empty_panics() {
    let mut q: Queue<i32> = Queue::with_capacity(1);
    q.pop();
}

/// TRANSCREATED from queue_test.go:70-85 (`TestQueue/circular buffer behavior`).
///
/// Upstream reads the unexported `q.head`/`q.tail` fields directly; the Rust
/// equivalents are the read-only `head_index`/`tail_index` accessors.
#[test]
fn queue_circular_buffer_behavior() {
    let mut q: Queue<i32> = Queue::with_capacity(3);
    q.push(1);
    q.push(2);
    q.pop(); // Remove 1.
    q.push(3);
    q.push(4); // This should wrap around.

    assert_eq!(q.head_index(), 1, "queue.head should be 1");
    assert_eq!(q.tail_index(), 1, "queue.tail should be 1");

    assert_eq!(q.pop(), 2, "expected 2");
    assert_eq!(q.pop(), 3, "expected 3");
    assert_eq!(q.pop(), 4, "expected 4");

    // The ring never grew: three pushes into a capacity-3 buffer.
    assert_eq!(q.cap(), 3);
}

/// WRITTEN: `ClearAndExpandIfNeed` (pkg/util/queue/queue.go:82-88) has no
/// upstream test. Pins that a smaller request leaves the ring untouched while a
/// larger one reallocates to exactly the requested size, and that both reset
/// the queue to empty.
#[test]
fn queue_clear_and_expand_if_need() {
    let mut q: Queue<i32> = Queue::with_capacity(4);
    q.push(1);
    q.push(2);

    // Requesting no more than the current ring keeps the existing allocation.
    q.clear_and_expand_if_need(3);
    assert!(q.is_empty());
    assert_eq!(q.cap(), 4);
    assert_eq!(q.head_index(), 0);
    assert_eq!(q.tail_index(), 0);

    // Requesting more reallocates to exactly the requested size, not a doubling.
    q.clear_and_expand_if_need(7);
    assert!(q.is_empty());
    assert_eq!(q.cap(), 7);

    q.push(10);
    assert_eq!(q.pop(), 10);
}

/// WRITTEN: no upstream test constructs the zero value. Pins that the Go zero
/// value `queue.Queue[T]{}` (nil backing slice) grows to a one-element ring on
/// its first push and then doubles, which `NewQueue(0)` deliberately does not do.
#[test]
fn queue_zero_value_grows_from_nil_backing_array() {
    let mut q: Queue<i32> = Queue::default();
    assert_eq!(q.cap(), 0);
    assert!(q.is_empty());

    q.push(1);
    assert_eq!(q.cap(), 1);
    q.push(2);
    assert_eq!(q.cap(), 2);
    q.push(3);
    assert_eq!(q.cap(), 4);

    assert_eq!(q.pop(), 1);
    assert_eq!(q.pop(), 2);
    assert_eq!(q.pop(), 3);
    assert!(q.is_empty());
}

/// WRITTEN: pins the counterpart of the case above — a zero-capacity ring built
/// by `NewQueue(0)` is non-nil in Go, so `Push` skips the nil fast path,
/// doubles zero to zero, and panics on the out-of-range slot write.
#[test]
#[should_panic(expected = "index out of range")]
fn queue_zero_capacity_push_panics() {
    let mut q: Queue<i32> = Queue::with_capacity(0);
    q.push(1);
}
