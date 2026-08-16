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

//! Go `pkg/infoschema`, covering `sieve.go` — one file of a much larger
//! package. It carries the SIEVE eviction algorithm that backs the infoschema
//! v2 table cache, and nothing else of `pkg/infoschema`: no schema
//! construction, no `infoschema_v2.go`, no `internal` package beyond the one
//! size helper described below.
//!
//! SIEVE is a "turn-key" web-cache eviction policy — see the paper *SIEVE is
//! simpler than LRU* and <https://cachemon.github.io/SIEVE-website/>. Entries
//! are pushed onto the front of a list and never move again. A `hand` walks
//! from the tail toward the head: an entry it lands on that has been visited
//! since the hand last passed gets its `visited` bit cleared and survives;
//! the first unvisited entry is evicted, and the hand parks on that entry's
//! predecessor (toward the head) so the next sweep resumes where this one
//! stopped. That "no promotion on hit" property is what separates SIEVE from
//! LRU: a run of one-hit wonders inserted at the head cannot push a popular
//! object out, because the popular object simply clears its bit when the hand
//! reaches it.
//!
//! # Narrowings and boundaries
//!
//! - **`pkg/infoschema/internal.Sizeof`** is not ported. That helper is a
//!   `reflect`-based deep walk over arbitrary Go values, and this file uses it
//!   for exactly one thing: the byte size of an `entry[K, V]`. Rust has no
//!   equivalent reflection, so the [`GoSized`] trait states the Go layout of a
//!   key/value type (its `unsafe.Sizeof`, its alignment, and any bytes it
//!   references indirectly), and [`entry_size`] reassembles the Go struct
//!   layout of `entry[K, V]` from it. The arithmetic reproduces `Sizeof`'s
//!   struct rule — sum of per-field sizes plus the type's padding — so
//!   `entry[int, int]` is 40 bytes on a 64-bit target, byte-exact with Go.
//! - **`entry.element` is always sized as a nil pointer (8 bytes).** Go's
//!   `entry.Size()` memoizes on first call, and the only call sites reach it
//!   before `ll.PushFront` assigns `element`, so the pointer is nil for size
//!   purposes in Go too.
//! - **`context.Context`/`context.CancelFunc`.** The Go struct stores a
//!   context and its cancel function, but no method in `sieve.go` ever reads
//!   `s.ctx`; `Close` only cancels it. With no consumer, the pair is dropped
//!   and [`Sieve::close`] is [`Sieve::purge`].
//! - **`failpoint.Inject("skipGet", ...)`.** The test-only early return at the
//!   top of `Get` is dropped; this workspace has no failpoint runtime.
//! - **`container/list`.** Go's intrusive doubly linked list becomes a slab of
//!   nodes addressed by generational handles, and the list node and the cache
//!   entry are merged into one [`Node`] (they are strictly 1:1 in Go).
//! - **Stale `hand` after `purge`.** Go's `Purge` empties the map and
//!   reinitializes the list but leaves `hand` pointing at a removed element;
//!   a later `evict` would then read a detached element and panic (or, worse,
//!   resolve a reinserted key). Here the generational handle simply fails to
//!   resolve and the sweep restarts at the tail, which is what `hand == nil`
//!   already means.
//! - **`sieveStatusHook`.** The trait is ported as [`SieveStatusHook`]; its
//!   real implementation, `sieveStatusHookImpl` in `pkg/infoschema/metrics.go`
//!   (Prometheus counters), belongs to a different file and is out of scope.
//!   Only `emptySieveStatusHook` comes along, as [`EmptySieveStatusHook`].
//! - **`K: Clone`.** Go's list element stores the key alongside the map key;
//!   the same duplication here needs one clone per insertion.
//! - **`V: Clone` on [`Sieve::get`]/[`Sieve::peek`]**, because Go returns the
//!   value by copy.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

/// The Go layout of a type used as a `Sieve` key or value.
///
/// This stands in for the `reflect` walk in `pkg/infoschema/internal.Sizeof`:
/// the size accounting must describe the *Go* type TiDB would have stored, not
/// the Rust type storing it, so the numbers stay comparable with the Go cache's
/// capacity settings.
pub trait GoSized {
    /// `unsafe.Sizeof` of the Go type: the width it occupies inside a struct.
    const GO_SIZE: u64;
    /// `unsafe.Alignof` of the Go type.
    const GO_ALIGN: u64;

    /// Bytes this value references beyond its in-struct width, as `Sizeof`
    /// counts them — a string's contents, for example. Zero for scalars.
    fn go_indirect_size(&self) -> u64 {
        0
    }
}

macro_rules! impl_go_sized_scalar {
    ($($ty:ty => ($size:expr, $align:expr)),* $(,)?) => {
        $(
            impl GoSized for $ty {
                const GO_SIZE: u64 = $size;
                const GO_ALIGN: u64 = $align;
            }
        )*
    };
}

// Go's `int`/`uint`/pointer widths are the target word size; TiDB only builds
// on 64-bit targets, which is also what the Go constants below assume.
impl_go_sized_scalar! {
    bool => (1, 1),
    i8 => (1, 1),
    u8 => (1, 1),
    i16 => (2, 2),
    u16 => (2, 2),
    i32 => (4, 4),
    u32 => (4, 4),
    i64 => (8, 8),
    u64 => (8, 8),
    isize => (8, 8),
    usize => (8, 8),
}

impl GoSized for String {
    // A Go string header: data pointer plus length.
    const GO_SIZE: u64 = 16;
    const GO_ALIGN: u64 = 8;

    fn go_indirect_size(&self) -> u64 {
        self.len() as u64
    }
}

const fn align_up(offset: u64, align: u64) -> u64 {
    offset.div_ceil(align) * align
}

/// Go layout size of `entry[K, V]`, excluding anything the key or value
/// references indirectly.
///
/// The fields are, in declaration order, `key K`, `value V`, `visited bool`,
/// `element *list.Element`, and `size uint64`.
const fn entry_layout_size(key: (u64, u64), value: (u64, u64)) -> u64 {
    let fields = [key, value, (1, 1), (8, 8), (8, 8)];
    let mut offset = 0;
    let mut max_align = 1;
    let mut i = 0;
    while i < fields.len() {
        let (size, align) = fields[i];
        offset = align_up(offset, align) + size;
        if align > max_align {
            max_align = align;
        }
        i += 1;
    }
    align_up(offset, max_align)
}

/// Go's `entry[K, V].Size()`: the struct's own bytes plus whatever the key and
/// value reference indirectly.
///
/// This is the unit the cache's capacity is denominated in, and the value a
/// Go test reads out of a zero `entry[K, V]`.
pub fn entry_size<K: GoSized, V: GoSized>(key: &K, value: &V) -> u64 {
    entry_layout_size((K::GO_SIZE, K::GO_ALIGN), (V::GO_SIZE, V::GO_ALIGN))
        + key.go_indirect_size()
        + value.go_indirect_size()
}

/// Size of a zero `entry[K, V]`, the per-entry cost of keys and values that
/// reference nothing indirectly.
pub fn zero_entry_size<K: GoSized, V: GoSized>() -> u64 {
    entry_layout_size((K::GO_SIZE, K::GO_ALIGN), (V::GO_SIZE, V::GO_ALIGN))
}

/// Observer for cache activity, invoked while the cache lock is held.
pub trait SieveStatusHook: Send + Sync {
    /// A [`Sieve::get`] found its key.
    fn on_hit(&self) {}
    /// A [`Sieve::get`] missed.
    fn on_miss(&self) {}
    /// One entry was evicted by the SIEVE hand.
    fn on_evict(&self) {}
    /// The cache's total size and entry count changed.
    fn on_update(&self, _size: u64, _count: u64) {}
    /// The capacity limit changed.
    fn on_update_limit(&self, _limit: u64) {}
}

/// The hook a `Sieve` starts with: it records nothing.
#[derive(Debug, Default, Clone, Copy)]
pub struct EmptySieveStatusHook;

impl SieveStatusHook for EmptySieveStatusHook {}

/// A slab slot's identity: index plus the generation it was allocated in, so a
/// handle to a freed entry cannot be mistaken for its replacement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Handle {
    index: usize,
    generation: u64,
}

/// One cache entry, doubling as the list element that holds its key.
///
/// `prev` points toward the head of the list (Go's `Element.Prev`), `next`
/// toward the tail.
#[derive(Debug)]
struct Node<K, V> {
    key: K,
    value: V,
    visited: bool,
    size: u64,
    prev: Option<usize>,
    next: Option<usize>,
}

#[derive(Debug)]
struct Slot<K, V> {
    generation: u64,
    node: Option<Node<K, V>>,
}

struct Inner<K, V> {
    count: u64,
    size: u64,
    /// May be zero, which disables the cache entirely (infoschema v2 off).
    capacity: u64,
    items: HashMap<K, usize>,
    slots: Vec<Slot<K, V>>,
    free: Vec<usize>,
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
    hand: Option<Handle>,
    hook: Arc<dyn SieveStatusHook>,
}

impl<K: Eq + Hash + Clone, V> Inner<K, V> {
    fn handle(&self, index: usize) -> Handle {
        Handle {
            index,
            generation: self.slots[index].generation,
        }
    }

    fn resolve(&self, handle: Handle) -> Option<usize> {
        let slot = self.slots.get(handle.index)?;
        if slot.generation == handle.generation && slot.node.is_some() {
            Some(handle.index)
        } else {
            None
        }
    }

    fn node(&self, index: usize) -> &Node<K, V> {
        self.slots[index]
            .node
            .as_ref()
            .expect("sieve: referencing a freed element")
    }

    fn node_mut(&mut self, index: usize) -> &mut Node<K, V> {
        self.slots[index]
            .node
            .as_mut()
            .expect("sieve: referencing a freed element")
    }

    /// Go's `ll.PushFront(key)`, allocating the entry with it.
    fn push_front(&mut self, node: Node<K, V>) -> usize {
        let index = match self.free.pop() {
            Some(index) => {
                self.slots[index].node = Some(node);
                index
            }
            None => {
                self.slots.push(Slot {
                    generation: 0,
                    node: Some(node),
                });
                self.slots.len() - 1
            }
        };
        let old_head = self.head.replace(index);
        {
            let node = self.node_mut(index);
            node.prev = None;
            node.next = old_head;
        }
        match old_head {
            Some(old) => self.node_mut(old).prev = Some(index),
            None => self.tail = Some(index),
        }
        self.len += 1;
        index
    }

    /// Go's `ll.Remove(e)`, freeing the entry with it.
    fn unlink(&mut self, index: usize) -> Node<K, V> {
        let node = self.slots[index]
            .node
            .take()
            .expect("sieve: removing a freed element");
        match node.prev {
            Some(prev) => self.node_mut(prev).next = node.next,
            None => self.head = node.next,
        }
        match node.next {
            Some(next) => self.node_mut(next).prev = node.prev,
            None => self.tail = node.prev,
        }
        self.slots[index].generation += 1;
        self.free.push(index);
        self.len -= 1;
        node
    }

    /// Go's `removeEntry`.
    fn remove_entry(&mut self, index: usize) {
        let node = self.unlink(index);
        self.items.remove(&node.key);
        self.size -= node.size;
        self.count -= 1;
        self.hook.on_update(self.size, self.count);
    }

    /// Go's `evict`: advance the hand from its parked position toward the head,
    /// clearing `visited` bits, and drop the first entry that has none.
    fn evict(&mut self) {
        let mut current = self
            .hand
            .and_then(|handle| self.resolve(handle))
            // If the hand is unset, start at the tail element in the list.
            .or(self.tail)
            .expect("sieve: evicting from an empty list");

        while self.node(current).visited {
            self.node_mut(current).visited = false;
            current = self
                .node(current)
                .prev
                .or(self.tail)
                .expect("sieve: evicting from an empty list");
        }

        self.hand = self.node(current).prev.map(|prev| self.handle(prev));
        self.remove_entry(current);
        self.hook.on_evict();
    }

    /// Go's bounded eviction loop, shared by `Set` and
    /// `SetCapacityAndWaitEvict`.
    fn evict_until_within_capacity(&mut self) {
        let mut i = 0;
        while self.size > self.capacity && i < 10 {
            self.evict();
            i += 1;
        }
    }
}

/// An efficient turn-key eviction algorithm for web caches.
///
/// See the blog post <https://cachemon.github.io/SIEVE-website/blog/2023/12/17/sieve-is-simpler-than-lru/>
/// and also the academic paper "SIEVE is simpler than LRU".
pub struct Sieve<K, V> {
    inner: Mutex<Inner<K, V>>,
}

impl<K: Eq + Hash + Clone + GoSized, V: GoSized> Sieve<K, V> {
    /// Go's `newSieve`. A `capacity` of zero disables the cache.
    pub fn new(capacity: u64) -> Self {
        Self {
            inner: Mutex::new(Inner {
                count: 0,
                size: 0,
                capacity,
                items: HashMap::new(),
                slots: Vec::new(),
                free: Vec::new(),
                head: None,
                tail: None,
                len: 0,
                hand: None,
                hook: Arc::new(EmptySieveStatusHook),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<K, V>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Replaces the activity observer.
    pub fn set_status_hook(&self, hook: Arc<dyn SieveStatusHook>) {
        self.lock().hook = hook;
    }

    /// Sets the capacity without waiting for the cache to shrink to it.
    pub fn set_capacity(&self, capacity: u64) {
        let mut inner = self.lock();
        inner.capacity = capacity;
        inner.hook.on_update_limit(capacity);
    }

    /// Sets the capacity and evicts until the cache fits inside it.
    pub fn set_capacity_and_wait_evict(&self, capacity: u64) {
        self.set_capacity(capacity);
        loop {
            let mut inner = self.lock();
            if inner.size <= inner.capacity {
                break;
            }
            inner.evict_until_within_capacity();
        }
    }

    /// The current capacity.
    pub fn capacity(&self) -> u64 {
        self.lock().capacity
    }

    /// Inserts or updates `key`. An update refreshes the value and marks the
    /// entry visited, but never moves it in the list — that is the SIEVE
    /// property.
    pub fn set(&self, key: K, value: V) {
        let mut inner = self.lock();

        if let Some(&index) = inner.items.get(&key) {
            let node = inner.node_mut(index);
            node.value = value;
            node.visited = true;
            return;
        }

        inner.evict_until_within_capacity();

        // Calculate the size first without putting to the list.
        let size = entry_size(&key, &value);
        inner.size += size;
        inner.count += 1;
        let (total, count) = (inner.size, inner.count);
        inner.hook.on_update(total, count);

        let index = inner.push_front(Node {
            key: key.clone(),
            value,
            visited: false,
            size,
            prev: None,
            next: None,
        });
        inner.items.insert(key, index);
    }

    /// Looks `key` up, marking it visited on a hit.
    pub fn get(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let mut inner = self.lock();
        match inner.items.get(key).copied() {
            Some(index) => {
                let node = inner.node_mut(index);
                node.visited = true;
                let value = node.value.clone();
                inner.hook.on_hit();
                Some(value)
            }
            None => {
                inner.hook.on_miss();
                None
            }
        }
    }

    /// Removes `key`, reporting whether it was present.
    pub fn remove(&self, key: &K) -> bool {
        let mut inner = self.lock();

        let Some(index) = inner.items.get(key).copied() else {
            return false;
        };

        // If the element to be removed is the hand, then move the hand to the
        // previous one.
        if inner.hand.and_then(|handle| inner.resolve(handle)) == Some(index) {
            inner.hand = inner.node(index).prev.map(|prev| inner.handle(prev));
        }

        inner.remove_entry(index);
        true
    }

    /// Whether `key` is cached, without marking it visited.
    pub fn contains(&self, key: &K) -> bool {
        self.lock().items.contains_key(key)
    }

    /// Reads `key` without marking it visited.
    pub fn peek(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let inner = self.lock();
        let index = inner.items.get(key).copied()?;
        Some(inner.node(index).value.clone())
    }

    /// Total accounted size of the cached entries in bytes.
    pub fn size(&self) -> u64 {
        self.lock().size
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.lock().len
    }

    /// Whether the cache holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops every entry.
    pub fn purge(&self) {
        let mut inner = self.lock();
        let indexes: Vec<usize> = inner.items.values().copied().collect();
        for index in indexes {
            inner.remove_entry(index);
        }
        // Go also reinitializes the list here; `remove_entry` already emptied it.
    }

    /// Go's `Close`: purges the cache. Nothing else survives the purge, since
    /// the Go context this also cancelled has no reader.
    pub fn close(&self) {
        self.purge();
    }
}

impl<K: Eq + Hash + Clone + GoSized, V: GoSized> std::fmt::Debug for Sieve<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.lock();
        f.debug_struct("Sieve")
            .field("count", &inner.count)
            .field("size", &inner.size)
            .field("capacity", &inner.capacity)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::size;

    #[test]
    fn test_get_and_set() {
        let items: Vec<i64> = (1..=10).collect();
        let cache = Sieve::<i64, i64>::new(10 * size::MB);

        for &v in &items {
            cache.set(v, v * 10);
        }

        for &v in &items {
            let val = cache.get(&v);
            assert!(val.is_some());
            assert_eq!(v * 10, val.unwrap());
        }

        cache.close();
    }

    #[test]
    fn test_remove() {
        let cache = Sieve::<i64, i64>::new(10 * size::MB);
        cache.set(1, 10);

        let val = cache.get(&1);
        assert!(val.is_some());
        assert_eq!(10, val.unwrap());

        // After removing the key, it should not be found.
        assert!(cache.remove(&1));
        assert!(cache.get(&1).is_none());

        // This should not panic.
        assert!(!cache.remove(&-1));

        cache.close();
    }

    #[test]
    fn test_sieve_policy() {
        let cache = Sieve::<i64, i64>::new(10 * zero_entry_size::<i64, i64>());
        let one_hit_wonders: Vec<i64> = vec![1, 2, 3, 4, 5];
        let popular_objects: Vec<i64> = vec![6, 7, 8, 9, 10];

        // Add objects to the cache.
        for &v in &one_hit_wonders {
            cache.set(v, v);
        }
        for &v in &popular_objects {
            cache.set(v, v);
        }

        // Hit popular objects.
        for &v in &popular_objects {
            assert!(cache.get(&v).is_some());
        }

        // Add another objects to the cache.
        for &v in &one_hit_wonders {
            cache.set(v * 10, v * 10);
        }

        // Check popular objects are not evicted.
        for &v in &popular_objects {
            assert!(cache.get(&v).is_some(), "popular object {v} was evicted");
        }

        cache.close();
    }

    #[test]
    fn test_contains() {
        let cache = Sieve::<String, String>::new(10 * size::MB);
        assert!(!cache.contains(&"hello".to_owned()));

        cache.set("hello".to_owned(), "world".to_owned());
        assert!(cache.contains(&"hello".to_owned()));

        cache.close();
    }

    #[test]
    fn test_cache_size() {
        let sz = zero_entry_size::<i64, i64>();
        // Go's `entry[int, int]` is 40 bytes on a 64-bit target.
        assert_eq!(40, sz);

        let cache = Sieve::<i64, i64>::new(10 * size::MB);
        assert_eq!(0, cache.size());

        cache.set(1, 1);
        assert_eq!(sz, cache.size());

        // Duplicated keys only update the recent-ness of the key and value.
        cache.set(1, 1);
        assert_eq!(sz, cache.size());

        cache.set(2, 2);
        assert_eq!(2 * sz, cache.size());

        cache.close();
    }

    #[test]
    fn test_purge() {
        let cache = Sieve::<i64, i64>::new(10 * size::MB);
        cache.set(1, 1);
        cache.set(2, 2);
        assert_eq!(2, cache.len());

        cache.purge();
        assert_eq!(0, cache.len());

        cache.close();
    }
}
