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

//! Go master `pkg/mvservice/utils.go`: the scheduler's wake-up primitive
//! and its priority queue.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};

use crossbeam_channel::{bounded, Receiver, Sender};

/// Go `Notifier`: a multi-producer, single-consumer wake-up primitive.
pub struct Notifier {
    tx: Sender<()>,
    /// Go `C`.
    pub c: Receiver<()>,
    awake: AtomicI32,
}

impl Default for Notifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Notifier {
    /// Go `NewNotifier`.
    #[must_use]
    pub fn new() -> Self {
        let (tx, c) = bounded(1);
        Self { tx, c, awake: AtomicI32::new(0) }
    }

    /// Go `clear`: resets the awake flag and returns the previous state.
    pub(crate) fn clear(&self) -> bool {
        self.awake.swap(0, Ordering::SeqCst) != 0
    }

    /// Go `Wait`: blocks until a wake signal is received.
    pub fn wait(&self) {
        let _ = self.c.recv();
        self.clear();
    }

    /// Go `Wake`: notifies the consumer.
    pub fn wake(&self) {
        // 1 -> 1: do nothing
        // 0 -> 1: send signal
        if self.awake.swap(1, Ordering::SeqCst) == 0 {
            let _ = self.tx.send(());
        }
    }

    pub(crate) fn is_awake(&self) -> bool {
        self.awake.load(Ordering::SeqCst) != 0
    }

    /// Go `WeakWake`: wakes the consumer only when it is currently not
    /// awake. It may lose signals under concurrent races.
    pub fn weak_wake(&self) {
        if self.is_awake() {
            return;
        }
        self.wake();
    }
}

/// Go `Lessable`: values ordered by `Less`.
pub trait Lessable {
    /// Go `Less`.
    fn less(&self, other: &Self) -> bool;
}

/// Go `Item[V]`: the handle of one priority-queue element. Go's handle is
/// the element's pointer, whose `index` the heap keeps current; here the
/// queue maps the handle to that index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Item(u64);

/// Go `PriorityQueue[V]`: a min-heap on `Less`, run by Go
/// `container/heap`'s algorithm so equal elements order as Go's do.
pub struct PriorityQueue<V> {
    heap: Vec<(Item, V)>,
    index: HashMap<Item, usize>,
    next: u64,
}

impl<V> Default for PriorityQueue<V> {
    fn default() -> Self {
        Self { heap: Vec::new(), index: HashMap::new(), next: 0 }
    }
}

impl<V: Lessable> PriorityQueue<V> {
    /// Go `Len`.
    #[must_use]
    pub fn len(&self) -> usize {
        self.heap.len()
    }

    /// Whether the queue is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Go `Front`: the smallest element without removing it.
    #[must_use]
    pub fn front(&self) -> Option<(Item, &V)> {
        self.heap.first().map(|(item, value)| (*item, value))
    }

    /// Go `item.Value` of a live handle.
    #[must_use]
    pub fn get(&self, item: Item) -> Option<&V> {
        self.index.get(&item).map(|&position| &self.heap[position].1)
    }

    /// Go `Push`.
    pub fn push(&mut self, value: V) -> Item {
        let item = Item(self.next);
        self.next += 1;
        self.index.insert(item, self.heap.len());
        self.heap.push((item, value));
        let last = self.heap.len() - 1;
        self.up(last);
        item
    }

    /// Go `Pop`: removes and returns the smallest element.
    pub fn pop(&mut self) -> Option<(Item, V)> {
        if self.heap.is_empty() {
            return None;
        }
        let n = self.heap.len() - 1;
        self.swap(0, n);
        self.down(0, n);
        self.pop_last()
    }

    /// Go `Update`: sets the element's value and fixes the heap. A removed
    /// handle is ignored.
    pub fn update(&mut self, item: Item, value: V) {
        let Some(&position) = self.index.get(&item) else {
            return;
        };
        self.heap[position].1 = value;
        self.fix(position);
    }

    /// Go `heap.Fix` after the element's ordering key changed in place.
    pub fn fix_item(&mut self, item: Item) {
        if let Some(&position) = self.index.get(&item) {
            self.fix(position);
        }
    }

    /// Go `Remove`: removes the element and returns its value.
    pub fn remove(&mut self, item: Item) -> Option<V> {
        let position = *self.index.get(&item)?;
        let n = self.heap.len() - 1;
        if n != position {
            self.swap(position, n);
            if !self.down(position, n) {
                self.up(position);
            }
        }
        self.pop_last().map(|(_, value)| value)
    }

    fn pop_last(&mut self) -> Option<(Item, V)> {
        let (item, value) = self.heap.pop()?;
        self.index.remove(&item);
        Some((item, value))
    }

    fn less(&self, i: usize, j: usize) -> bool {
        self.heap[i].1.less(&self.heap[j].1)
    }

    fn swap(&mut self, i: usize, j: usize) {
        self.heap.swap(i, j);
        self.index.insert(self.heap[i].0, i);
        self.index.insert(self.heap[j].0, j);
    }

    /// Go `heap.Fix`.
    fn fix(&mut self, position: usize) {
        let n = self.heap.len();
        if !self.down(position, n) {
            self.up(position);
        }
    }

    /// Go `heap.up`.
    fn up(&mut self, mut j: usize) {
        while j > 0 {
            let i = (j - 1) / 2;
            if !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            j = i;
        }
    }

    /// Go `heap.down`.
    fn down(&mut self, i0: usize, n: usize) -> bool {
        let mut i = i0;
        loop {
            let j1 = 2 * i + 1;
            if j1 >= n {
                break;
            }
            let mut j = j1;
            let j2 = j1 + 1;
            if j2 < n && self.less(j2, j1) {
                j = j2;
            }
            if !self.less(j, i) {
                break;
            }
            self.swap(i, j);
            i = j;
        }
        i > i0
    }
}
