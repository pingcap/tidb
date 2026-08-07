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
//! held as `None`. `backing_allocated` preserves Go's observable distinction
//! between a nil zero-value slice and the allocated empty slice created by
//! `NewQueue(0)`. `Cap` reports the slot count, exactly as `len(elements)` does
//! in Go.

/// A circular-buffer implementation of a queue.
pub struct Queue<T> {
    elements: Vec<Option<T>>,
    head: usize,
    tail: usize,
    size: usize,
    backing_allocated: bool,
}

impl<T> Default for Queue<T> {
    fn default() -> Self {
        Queue {
            elements: Vec::new(),
            head: 0,
            tail: 0,
            size: 0,
            backing_allocated: false,
        }
    }
}

impl<T> Queue<T> {
    /// Creates a new queue with the given capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Queue {
            elements: none_vec(capacity),
            backing_allocated: true,
            ..Default::default()
        }
    }

    /// Pushes an element onto the queue.
    pub fn push(&mut self, element: T) {
        if !self.backing_allocated {
            self.elements = none_vec(1);
            self.backing_allocated = true;
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
            self.backing_allocated = true;
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
    use sha2::{Digest, Sha256};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    use std::{
        collections::{BTreeMap, BTreeSet},
        fs,
        path::PathBuf,
    };

    use super::Queue;

    const ARTIFACTS: &str = include_str!("queue.artifacts.tsv");
    const INVENTORY: &str = include_str!("queue.inventory.tsv");

    fn data_rows(contents: &'static str) -> Vec<Vec<&'static str>> {
        contents
            .lines()
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .skip(1)
            .map(|line| line.split('\t').collect())
            .collect()
    }

    fn repository_root() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
    }

    fn sha256(bytes: impl AsRef<[u8]>) -> String {
        format!("{:x}", Sha256::digest(bytes.as_ref()))
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn queue_lockdown_inventory_and_symbols() {
        let artifact_rows = data_rows(ARTIFACTS);
        assert_eq!(artifact_rows.len(), 3);
        assert!(artifact_rows.iter().all(|row| row.len() == 3));
        let root = repository_root();
        for row in artifact_rows {
            assert_eq!(
                sha256(fs::read(root.join(row[0])).expect("read queue artifact")),
                row[2],
                "owned artifact drifted: {}",
                row[0]
            );
        }

        let rows = data_rows(INVENTORY);
        assert_eq!(rows.len(), 50);
        assert!(rows.iter().all(|row| row.len() == 10));
        let allowed_symbols = [
            "Queue",
            "Queue::cap",
            "Queue::clear",
            "Queue::clear_and_expand_if_need",
            "Queue::is_empty",
            "Queue::len",
            "Queue::new",
            "Queue::pop",
            "Queue::push",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let allowed_evidence = [
            "basic_operations",
            "circular_buffer_behavior",
            "clear_operation",
            "panic_on_empty_pop",
            "source_clear_and_expand_contracts",
            "source_clear_retains_slots_until_overwrite_or_expand",
            "source_default_and_zero_capacity_constructor_are_distinct",
            "source_wrapped_growth_preserves_fifo_order",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let mut ids = BTreeSet::new();
        let mut categories = BTreeMap::new();
        let mut statuses = BTreeMap::new();
        for row in rows {
            assert!(ids.insert(row[0]), "duplicate obligation id: {}", row[0]);
            assert_eq!(row[6], "PORTED", "unexpected queue disposition: {row:?}");
            assert!(
                allowed_symbols.contains(row[7]),
                "unanchored owner: {row:?}"
            );
            let evidence = row[8]
                .strip_prefix("rust-test:")
                .expect("queue evidence test prefix");
            assert!(
                allowed_evidence.contains(evidence),
                "unanchored evidence: {row:?}"
            );
            assert!(!row[9].is_empty());
            *categories.entry(row[1]).or_insert(0usize) += 1;
            *statuses.entry(row[6]).or_insert(0usize) += 1;
        }
        assert_eq!(statuses, BTreeMap::from([("PORTED", 50)]));
        assert_eq!(
            categories,
            BTreeMap::from([
                ("branch", 8),
                ("declaration", 1),
                ("field", 4),
                ("function", 8),
                ("loop", 2),
                ("test", 1),
                ("test_assertion", 21),
                ("test_helper_closure", 5),
            ])
        );

        let _: fn(usize) -> Queue<i32> = Queue::new;
        let _: fn(&mut Queue<i32>, i32) = Queue::push;
        let _: fn(&mut Queue<i32>) -> i32 = Queue::pop;
        let _: fn(&Queue<i32>) -> usize = Queue::len;
        let _: fn(&Queue<i32>) -> bool = Queue::is_empty;
        let _: fn(&mut Queue<i32>) = Queue::clear;
        let _: fn(&mut Queue<i32>, usize) = Queue::clear_and_expand_if_need;
        let _: fn(&Queue<i32>) -> usize = Queue::cap;
        let _: Queue<i32> = Queue::default();

        let _: fn() = basic_operations;
        let _: fn() = circular_buffer_behavior;
        let _: fn() = clear_operation;
        let _: fn() = panic_on_empty_pop;
        let _: fn() = source_clear_and_expand_contracts;
        let _: fn() = source_clear_retains_slots_until_overwrite_or_expand;
        let _: fn() = source_default_and_zero_capacity_constructor_are_distinct;
        let _: fn() = source_wrapped_growth_preserves_fifo_order;
    }

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

    #[test]
    fn source_default_and_zero_capacity_constructor_are_distinct() {
        let mut zero_value = Queue::default();
        zero_value.push(7);
        assert_eq!(zero_value.cap(), 1);
        assert_eq!(zero_value.pop(), 7);

        let new_zero_push = std::panic::catch_unwind(|| {
            let mut allocated_empty = Queue::new(0);
            allocated_empty.push(7);
        });
        assert!(
            new_zero_push.is_err(),
            "Go NewQueue(0) panics on its first push"
        );
    }

    #[test]
    fn source_wrapped_growth_preserves_fifo_order() {
        let mut q = Queue::new(3);
        q.push(0);
        q.push(1);
        q.push(2);
        assert_eq!(q.pop(), 0);
        assert_eq!(q.pop(), 1);
        q.push(3);
        q.push(4);

        q.push(5);
        assert_eq!(q.cap(), 6);
        assert_eq!(q.head, 0);
        assert_eq!(q.tail, 4);
        assert_eq!([q.pop(), q.pop(), q.pop(), q.pop()], [2, 3, 4, 5]);
        assert!(q.is_empty());
    }

    #[test]
    fn source_clear_and_expand_contracts() {
        let mut q = Queue::new(3);
        q.push(1);
        q.push(2);
        assert_eq!(q.pop(), 1);

        q.clear_and_expand_if_need(2);
        assert_eq!(q.cap(), 3);
        assert_eq!(q.len(), 0);
        assert!(q.is_empty());
        q.push(3);
        assert_eq!(q.pop(), 3);

        q.push(4);
        q.clear_and_expand_if_need(5);
        assert_eq!(q.cap(), 5);
        assert_eq!(q.len(), 0);
        q.push(5);
        assert_eq!(q.pop(), 5);

        let mut zero_value = Queue::default();
        zero_value.clear_and_expand_if_need(0);
        zero_value.push(6);
        assert_eq!(zero_value.cap(), 1);
        assert_eq!(zero_value.pop(), 6);

        let mut expanded_zero_value = Queue::default();
        expanded_zero_value.clear_and_expand_if_need(2);
        expanded_zero_value.push(7);
        assert_eq!(expanded_zero_value.cap(), 2);
        assert_eq!(expanded_zero_value.pop(), 7);
    }

    #[test]
    fn source_clear_retains_slots_until_overwrite_or_expand() {
        let drops = Arc::new(AtomicUsize::new(0));
        let mut q = Queue::new(2);
        q.push(DropProbe(Arc::clone(&drops)));
        q.push(DropProbe(Arc::clone(&drops)));

        q.clear();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        q.clear_and_expand_if_need(2);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        q.push(DropProbe(Arc::clone(&drops)));
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        q.clear_and_expand_if_need(3);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert_eq!(q.cap(), 3);
        assert!(q.is_empty());
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
