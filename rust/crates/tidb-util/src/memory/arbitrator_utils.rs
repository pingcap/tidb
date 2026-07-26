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

//! Transcreation of Go `pkg/util/memory/utils.go`: the arbitrator's support
//! machinery — the recycling FIFO list, the producer/consumer notifier, the
//! FNV-style hashes, and the ratio helpers.
//!
//! Faithful adaptations:
//! - Go's `wrapList` recycles `container/list` elements by moving removed
//!   nodes past an `end` sentinel. The Rust [`WrapList`] keeps the same
//!   contract (O(1) push/pop/remove/move-to-front, element identity via a
//!   handle, node recycling) over an index-linked slab; `base_len` mirrors
//!   Go's `base.Len()` (live + cached nodes + sentinel) for the tests.
//! - `Notifer` (Go: capacity-1 channel + atomic flag) keeps the same
//!   multi-producer/single-consumer semantics over a `Mutex`+`Condvar`:
//!   `wake` queues at most one pending signal; `wait` blocks for it and
//!   clears the awake flag.
//! - `SampleRuntimeMemStats`/`IntoRuntimeMemStats` read the Go runtime's
//!   heap metrics; they have no Rust counterpart and are not ported (the
//!   arbitrator receives stats through `HandleRuntimeStats`).

use std::sync::atomic::{AtomicI32, Ordering::SeqCst};

pub(crate) const BYTE_SIZE_KB: i64 = 1 << 10;
pub(crate) const BYTE_SIZE_MB: i64 = 1 << 20;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const BYTE_SIZE_GB: i64 = 1 << 30;
pub(crate) const KILO: i64 = 1000;

pub(crate) const PRIME64: u64 = 1099511628211;
pub(crate) const INIT_HASH_KEY: u64 = 14695981039346656037;

/// Handle to a [`WrapList`] node (Go `wrapListElement`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct WrapListElement {
    idx: usize,
}

pub(crate) const INVALID_ELEMENT: WrapListElement = WrapListElement { idx: usize::MAX };

impl WrapListElement {
    pub(crate) fn valid(&self) -> bool {
        self.idx != usize::MAX
    }
    pub(crate) fn reset(&mut self) {
        self.idx = usize::MAX;
    }
}

impl Default for WrapListElement {
    fn default() -> Self {
        INVALID_ELEMENT
    }
}

struct WrapNode<V> {
    value: Option<V>,
    prev: usize,
    next: usize,
}

/// List with node recycling (Go `wrapList[V]`): removed nodes are cached
/// past the sentinel and reused by `push_back`.
pub(crate) struct WrapList<V> {
    // Slab of nodes; index 0 is the sentinel once initialized. The ring is
    // circular through the sentinel: sentinel.next = head, sentinel.prev =
    // tail of the cached ("dead") region. Live nodes sit between the head
    // and the `end` marker exactly as in Go.
    nodes: Vec<WrapNode<V>>,
    end: usize, // Go's `end` sentinel element (an allocated node with nil value)
    num: i64,
}

impl<V: Clone> Default for WrapList<V> {
    fn default() -> Self {
        WrapList {
            nodes: Vec::new(),
            end: usize::MAX,
            num: 0,
        }
    }
}

impl<V: Clone> WrapList<V> {
    /// Go `init`: allocate the `end` sentinel.
    pub(crate) fn init(&mut self) {
        self.nodes.clear();
        // Node 0: ring sentinel (list head/tail anchor, not an element).
        self.nodes.push(WrapNode {
            value: None,
            prev: 1,
            next: 1,
        });
        // Node 1: Go's `end` element.
        self.nodes.push(WrapNode {
            value: None,
            prev: 0,
            next: 0,
        });
        self.end = 1;
        self.num = 0;
    }

    fn unlink(&mut self, i: usize) {
        let (p, n) = (self.nodes[i].prev, self.nodes[i].next);
        self.nodes[p].next = n;
        self.nodes[n].prev = p;
    }

    fn insert_before(&mut self, i: usize, at: usize) {
        let p = self.nodes[at].prev;
        self.nodes[i].prev = p;
        self.nodes[i].next = at;
        self.nodes[p].next = i;
        self.nodes[at].prev = i;
    }

    /// Go `moveToFront`.
    pub(crate) fn move_to_front(&mut self, e: WrapListElement) {
        let head = self.nodes[0].next;
        if head != e.idx {
            self.unlink(e.idx);
            self.insert_before(e.idx, head);
        }
    }

    /// Go `remove`: clear the value, recycle the node past `end`.
    pub(crate) fn remove(&mut self, e: WrapListElement) {
        self.nodes[e.idx].value = None;
        // MoveToBack: after the end sentinel, at the ring's tail.
        self.unlink(e.idx);
        self.insert_before(e.idx, 0); // before ring sentinel == list back
        self.num -= 1;
    }

    /// Go `front`.
    pub(crate) fn front(&self) -> Option<V> {
        if self.empty() {
            return None;
        }
        self.nodes[self.nodes[0].next].value.clone()
    }

    /// Go `popFront`.
    pub(crate) fn pop_front(&mut self) -> Option<V> {
        if self.empty() {
            return None;
        }
        let head = self.nodes[0].next;
        let res = self.nodes[head].value.clone();
        self.remove(WrapListElement { idx: head });
        res
    }

    /// Go `size`.
    pub(crate) fn size(&self) -> i64 {
        self.num
    }

    /// Go `empty`.
    pub(crate) fn empty(&self) -> bool {
        self.size() == 0
    }

    /// Go `approxSize` (`//go:norace`).
    pub(crate) fn approx_size(&self) -> i64 {
        self.size()
    }

    /// Go `approxEmpty` (`//go:norace`).
    pub(crate) fn approx_empty(&self) -> bool {
        self.empty()
    }

    /// Go `base.Len()`: live nodes + `end` sentinel + cached nodes (the
    /// test surface for recycling behavior).
    pub(crate) fn base_len(&self) -> usize {
        self.nodes.len().saturating_sub(1)
    }

    /// Iterate live elements front-to-back (test surface mirroring Go's
    /// `base.Front()`/`Next()` walks).
    #[cfg(test)]
    pub(crate) fn iter_live(&self) -> Vec<(WrapListElement, V)> {
        let mut res = Vec::new();
        if self.nodes.is_empty() {
            return res;
        }
        let mut i = self.nodes[0].next;
        while i != self.end {
            if let Some(v) = &self.nodes[i].value {
                res.push((WrapListElement { idx: i }, v.clone()));
            }
            i = self.nodes[i].next;
        }
        res
    }

    /// Go `pushBack`: reuse a cached node when one exists, else allocate.
    pub(crate) fn push_back(&mut self, v: V) -> WrapListElement {
        let idx = if self.num + 1 == self.base_len() as i64 {
            // No cached node: allocate and insert before `end`.
            let idx = self.nodes.len();
            self.nodes.push(WrapNode {
                value: Some(v),
                prev: 0,
                next: 0,
            });
            self.insert_before(idx, self.end);
            idx
        } else {
            // Reuse the ring's back node (a recycled one).
            let idx = self.nodes[0].prev;
            self.unlink(idx);
            self.insert_before(idx, self.end);
            self.nodes[idx].value = Some(v);
            idx
        };
        self.num += 1;
        WrapListElement { idx }
    }
}

/// Multiple-producer single-consumer notifier (Go `Notifer`): a
/// capacity-1 channel plus an atomic awake flag, exactly as in the source.
pub(crate) struct Notifer {
    tx: crossbeam_channel::Sender<()>,
    pub(crate) rx: crossbeam_channel::Receiver<()>,
    awake: AtomicI32,
}

impl Notifer {
    /// Go `NewNotifer`.
    pub(crate) fn new() -> Notifer {
        let (tx, rx) = crossbeam_channel::bounded(1);
        Notifer {
            tx,
            rx,
            awake: AtomicI32::new(0),
        }
    }

    /// Go `clear`: returns the previous awake status.
    pub(crate) fn clear(&self) -> bool {
        self.awake.swap(0, SeqCst) != 0
    }

    /// Go `Wait`: block for the signal, then clear the awake flag.
    #[cfg_attr(not(test), allow(dead_code))] // consumed via `rx` select in the runner; direct waits are test surface
    pub(crate) fn wait(&self) {
        let _ = self.rx.recv();
        self.clear();
    }

    /// Go `Wake`.
    pub(crate) fn wake(&self) {
        // 1 -> 1: do nothing; 0 -> 1: send signal.
        if self.awake.swap(1, SeqCst) == 0 {
            let _ = self.tx.send(());
        }
    }

    /// Go `isAwake`.
    pub(crate) fn is_awake(&self) -> bool {
        self.awake.load(SeqCst) != 0
    }

    /// Go `WeakWake`: may lose signals under concurrency, by design.
    pub(crate) fn weak_wake(&self) {
        if self.is_awake() {
            return;
        }
        self.wake();
    }
}

/// Hashes a string (Go `HashStr`; iterates Unicode code points).
pub fn hash_str(key: &str) -> u64 {
    let mut hash_key = INIT_HASH_KEY;
    for c in key.chars() {
        hash_key = hash_key.wrapping_mul(PRIME64);
        hash_key ^= c as u64;
    }
    hash_key
}

/// Hashes a uint64 even number (Go `HashEvenNum`).
pub fn hash_even_num(key: u64) -> u64 {
    const STEP: u32 = 8;
    const STEP_MASK: u64 = (1u64 << STEP) - 1;

    let mut hash_key = INIT_HASH_KEY;
    // handle significant last 8 bits
    hash_key ^= key & STEP_MASK;
    hash_key = hash_key.wrapping_mul(PRIME64);
    let key = key >> STEP;

    hash_key ^= key;
    hash_key = hash_key.wrapping_mul(PRIME64);

    hash_key
}

pub(crate) fn shard_index_by_uid(key: u64, shards_mask: u64) -> u64 {
    hash_even_num(key) & shards_mask
}

pub(crate) const BASE_QUOTA_UNIT: i64 = 4 * BYTE_SIZE_KB;

pub(crate) fn get_quota_shard(quota: i64, max_quota_shard: usize) -> usize {
    let p = (quota as u64) / (BASE_QUOTA_UNIT as u64);
    let pos = (64 - p.leading_zeros()) as usize; // bits.Len64
    pos.min(max_quota_shard - 1)
}

pub(crate) fn now_unix_milli() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn next_pow2(n: u64) -> u64 {
    if n == 0 {
        return 1;
    }
    let mut n = n - 1;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    n + 1
}

/// Go `calcRatio`: x/y in per-mille.
pub(crate) fn calc_ratio(x: i64, y: i64) -> i64 {
    x * KILO / y
}

/// Go `multiRatio`: x * (per-mille ratio).
pub(crate) fn multi_ratio(x: i64, y_milli: i64) -> i64 {
    x * y_milli / KILO
}

/// Go `intoRatio`: float ratio to per-mille.
pub(crate) fn into_ratio(x: f64) -> i64 {
    (x * KILO as f64) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ported from the utils portions of Go `TestBasicUtils`.
    #[test]
    fn basic_utils() {
        {
            const CNT: u64 = 1 << 8;
            let bg_id: u64 = 4068484684;
            let mut odd = 0;
            for i in 0..CNT {
                let n = shard_index_by_uid(bg_id + i * 2, CNT - 1);
                if n & 1 != 0 {
                    odd += 1;
                }
            }
            assert_eq!(odd, CNT / 2);
        }

        const DEF_POOL_QUOTA_SHARDS: usize = 27;
        assert_eq!(
            BASE_QUOTA_UNIT * (1 << (DEF_POOL_QUOTA_SHARDS - 2)),
            128 * BYTE_SIZE_GB
        );
        assert_eq!(get_quota_shard(0, DEF_POOL_QUOTA_SHARDS), 0);
        assert_eq!(
            get_quota_shard(BASE_QUOTA_UNIT - 1, DEF_POOL_QUOTA_SHARDS),
            0
        );
        assert_eq!(get_quota_shard(BASE_QUOTA_UNIT, DEF_POOL_QUOTA_SHARDS), 1);
        assert_eq!(
            get_quota_shard(BASE_QUOTA_UNIT * 2 - 1, DEF_POOL_QUOTA_SHARDS),
            1
        );
        assert_eq!(
            get_quota_shard(BASE_QUOTA_UNIT * 2, DEF_POOL_QUOTA_SHARDS),
            2
        );
        assert_eq!(
            get_quota_shard(BASE_QUOTA_UNIT * 4 - 1, DEF_POOL_QUOTA_SHARDS),
            2
        );
        assert_eq!(
            get_quota_shard(BASE_QUOTA_UNIT * 4, DEF_POOL_QUOTA_SHARDS),
            3
        );
        assert_eq!(
            get_quota_shard(
                BASE_QUOTA_UNIT * (1 << (DEF_POOL_QUOTA_SHARDS - 2)) - 1,
                DEF_POOL_QUOTA_SHARDS
            ),
            DEF_POOL_QUOTA_SHARDS - 2
        );
        assert_eq!(
            get_quota_shard(
                BASE_QUOTA_UNIT * (1 << (DEF_POOL_QUOTA_SHARDS - 2)),
                DEF_POOL_QUOTA_SHARDS
            ),
            DEF_POOL_QUOTA_SHARDS - 1
        );
        assert_eq!(
            get_quota_shard(i64::MAX, DEF_POOL_QUOTA_SHARDS),
            DEF_POOL_QUOTA_SHARDS - 1
        );

        {
            let n = std::sync::Arc::new(Notifer::new());
            assert!(!n.is_awake());
            let n2 = std::sync::Arc::clone(&n);
            let h = std::thread::spawn(move || {
                n2.wake();
                assert!(n2.is_awake());
                n2.wake();
                n2.weak_wake();
                assert!(n2.is_awake());
            });
            h.join().unwrap();
            assert!(n.is_awake());
            n.wait();
            assert!(!n.is_awake());
        }

        {
            // wrapList recycling behavior; `base_len` mirrors Go's
            // `base.Len()` minus nothing (sentinel + cached nodes counted).
            let mut data: WrapList<i32> = WrapList::default();
            assert_eq!(data.size(), 0);
            data.init();
            assert_eq!(data.size(), 0);
            assert_eq!(data.base_len(), 1);

            assert!(data.pop_front().is_none());
            assert!(data.front().is_none());
            let (p1, p2, p3) = (1, 2, 3);

            data.push_back(p1);
            assert_eq!(data.size(), 1);
            assert_eq!(data.base_len(), 2);
            assert_eq!(data.front(), Some(p1));

            let ep2 = data.push_back(p2);
            assert_eq!(data.size(), 2);
            assert_eq!(data.base_len(), 3);
            assert_eq!(data.front(), Some(p1));

            data.remove(ep2);
            assert_eq!(data.size(), 1);
            assert_eq!(data.base_len(), 3);
            assert_eq!(data.front(), Some(p1));

            data.push_back(p3);
            assert_eq!(data.size(), 2);
            assert_eq!(data.base_len(), 3);
            assert_eq!(data.front(), Some(p1));

            assert_eq!(data.pop_front(), Some(p1));
            assert_eq!(data.size(), 1);
            assert_eq!(data.base_len(), 3);
            assert_eq!(data.front(), Some(p3));

            let ep1 = data.push_back(p1);
            assert_eq!(data.front(), Some(p3));
            data.move_to_front(ep1);
            assert_eq!(data.size(), 2);
            assert_eq!(data.base_len(), 3);
            assert_eq!(data.front(), Some(p1));

            data.push_back(p2);
            assert_eq!(data.size(), 3);
        }

        {
            assert_eq!(next_pow2(0), 1);
            for n in 1u64..63 {
                let x = 1u64 << n;
                assert_eq!(next_pow2(x), x);
                if x > 2 {
                    assert_eq!(next_pow2(x - 1), x);
                }
            }
        }
    }
}
