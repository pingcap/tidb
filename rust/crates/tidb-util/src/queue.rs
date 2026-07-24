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

//! Complete transcreation of Go `pkg/util/queue` (`queue.go`).
//!
//! A generic circular-buffer queue. Go pre-allocates the backing array with
//! `make([]T, capacity)` (zero-valued slots); Rust cannot zero-fill an
//! arbitrary `T`, so the backing store is `Vec<Option<T>>` with unused slots
//! held as `None`. `Cap` reports the slot count, exactly as `len(elements)`
//! does in Go.

/// A circular-buffer implementation of a queue.
pub struct Queue<T> {
    elements: Vec<Option<T>>,
    head: usize,
    tail: usize,
    size: usize,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Queue {
            elements: Vec::new(),
            head: 0,
            tail: 0,
            size: 0,
        }
    }
}

impl<T> Queue<T> {
    /// Creates a new queue with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Queue {
            elements: none_vec(capacity),
            ..Default::default()
        }
    }

    /// Pushes an element onto the queue.
    pub fn push(&mut self, element: T) {
        // Go allocates a single slot when the backing store has never been
        // allocated (`elements == nil`). Rust cannot distinguish nil from an
        // empty slice, so an empty backing store always grows to one slot here;
        // this also removes Go's `make([]T, 0)*2 == 0` degenerate case where a
        // `NewQueue(0)` panics on the first push.
        if self.elements.is_empty() {
            self.elements = none_vec(1);
        }

        if self.size == self.elements.len() {
            // Double capacity when full.
            let old_len = self.elements.len();
            let mut new_elements = none_vec(old_len * 2);
            for (i, slot) in new_elements.iter_mut().enumerate().take(self.size) {
                *slot = self.elements[(self.head + i) % old_len].take();
            }
            self.elements = new_elements;
            self.head = 0;
            self.tail = self.size;
        }

        let len = self.elements.len();
        self.elements[self.tail] = Some(element);
        self.tail = (self.tail + 1) % len;
        self.size += 1;
    }

    /// Pops an element from the queue.
    ///
    /// # Panics
    ///
    /// Panics if the queue is empty.
    pub fn pop(&mut self) -> T {
        assert!(self.size != 0, "Queue is empty");
        let element = self.elements[self.head]
            .take()
            .expect("occupied slot holds a value");
        self.head = (self.head + 1) % self.elements.len();
        self.size -= 1;
        element
    }

    /// Returns the number of elements in the queue.
    #[must_use]
    pub fn len(&self) -> usize {
        self.size
    }

    /// Returns true if the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Clears the queue.
    pub fn clear(&mut self) {
        self.head = 0;
        self.tail = 0;
        self.size = 0;
    }

    /// Clears the queue and expands the backing store if needed.
    pub fn clear_and_expand_if_need(&mut self, size: usize) {
        self.clear();
        if self.elements.len() < size {
            self.elements = none_vec(size);
        }
    }

    /// Returns the capacity of the queue.
    #[must_use]
    pub fn cap(&self) -> usize {
        self.elements.len()
    }
}

fn none_vec<T>(n: usize) -> Vec<Option<T>> {
    let mut v = Vec::with_capacity(n);
    v.resize_with(n, || None);
    v
}

#[cfg(test)]
mod tests {
    use super::Queue;

    // Go `TestQueue` / "basic operations".
    #[test]
    fn basic_operations() {
        let mut q: Queue<i32> = Queue::new(2);

        // Initial state.
        assert!(q.is_empty(), "new queue should be empty");
        assert_eq!(q.len(), 0, "new queue should have length 0");
        assert_eq!(q.cap(), 2, "new queue should have capacity 2");

        // Push.
        q.push(1);
        q.push(2);
        assert_eq!(
            q.len(),
            2,
            "queue length should be 2 after pushing 2 elements"
        );
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

    // Go `TestQueue` / "clear operation".
    #[test]
    fn clear_operation() {
        let mut q: Queue<String> = Queue::new(4);
        q.push("a".to_string());
        q.push("b".to_string());
        q.push("c".to_string());

        q.clear();
        assert!(q.is_empty(), "queue should be empty after clear");
        assert_eq!(q.len(), 0, "queue length should be 0 after clear");
    }

    // Go `TestQueue` / "panic on empty pop".
    #[test]
    #[should_panic(expected = "Queue is empty")]
    fn panic_on_empty_pop() {
        let mut q: Queue<i32> = Queue::new(1);
        q.pop();
    }

    // Go `TestQueue` / "circular buffer behavior".
    #[test]
    fn circular_buffer_behavior() {
        let mut q: Queue<i32> = Queue::new(3);
        q.push(1);
        q.push(2);
        q.pop(); // Remove 1.
        q.push(3);
        q.push(4); // This should wrap around.

        assert_eq!(q.head, 1, "queue head should be 1");
        assert_eq!(q.tail, 1, "queue tail should be 1");

        assert_eq!(q.pop(), 2, "expected 2");
        assert_eq!(q.pop(), 3, "expected 3");
        assert_eq!(q.pop(), 4, "expected 4");
    }
}
