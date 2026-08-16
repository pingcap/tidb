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

//! Go `lockstore.go`: the skiplist itself.
//!
//! Node layout inside an arena block, byte for byte as Go's `node` struct is
//! laid out by the compiler on a 64-bit target:
//!
//! ```text
//! 0..8    arenaAddr of this node       (nodeHeader.addr)
//! 8..10   height                       (nodeHeader.height, u16)
//! 10..12  key length                   (nodeHeader.keyLen, u16)
//! 12..16  value length                 (nodeHeader.valLen, u32)
//! 16..16+height*8   next addresses, one per level (nextsBase and beyond)
//! then the key bytes, then the value bytes
//! ```
//!
//! Keeping that layout is not cosmetic: node size is what decides when a block
//! overflows, and Go's own tests assert on the resulting block counts.

use std::cmp::Ordering;

use crate::arena::{Arena, ArenaAddr, NULL_ARENA_ADDR};

/// Go `maxHeight`.
pub const MAX_HEIGHT: usize = 16;

/// Go `nodeHeaderSize`: `unsafe.Sizeof(nodeHeader{})` on a 64-bit target.
pub const NODE_HEADER_SIZE: usize = 16;

/// Reads a node's height.
fn node_height(a: &Arena, addr: ArenaAddr) -> usize {
    let d = a.get(addr, NODE_HEADER_SIZE);
    usize::from(u16::from_le_bytes([d[8], d[9]]))
}

/// Reads a node's key length.
fn node_key_len(a: &Arena, addr: ArenaAddr) -> usize {
    let d = a.get(addr, NODE_HEADER_SIZE);
    usize::from(u16::from_le_bytes([d[10], d[11]]))
}

/// Reads a node's value length.
fn node_val_len(a: &Arena, addr: ArenaAddr) -> usize {
    let d = a.get(addr, NODE_HEADER_SIZE);
    u32::from_le_bytes([d[12], d[13], d[14], d[15]]) as usize
}

/// Go `(*node).nodeLen`: the header plus the next-pointer array.
fn node_len(a: &Arena, addr: ArenaAddr) -> usize {
    node_height(a, addr) * 8 + NODE_HEADER_SIZE
}

/// Go `(*node).getKey`.
fn node_key(a: &Arena, addr: ArenaAddr) -> &[u8] {
    let node_len = node_len(a, addr);
    let key_len = node_key_len(a, addr);
    &a.get(addr, node_len + key_len)[node_len..]
}

/// Go `(*node).getValue`.
fn node_value(a: &Arena, addr: ArenaAddr) -> &[u8] {
    let prefix = node_len(a, addr) + node_key_len(a, addr);
    &a.get(addr, prefix + node_val_len(a, addr))[prefix..]
}

/// Go `(*node).getNextAddr`.
///
/// narrowing: Go reads this with `atomic.LoadUint64` so that a lock-free
/// reader sees a torn-free link while the single writer publishes one. Rust
/// gets the same guarantee from `&`/`&mut` exclusivity — a reader holding `&`
/// proves no writer is running — so the load is a plain read.
fn node_next(a: &Arena, addr: ArenaAddr, level: usize) -> ArenaAddr {
    let off = NODE_HEADER_SIZE + level * 8;
    let d = a.get(addr, off + 8);
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&d[off..off + 8]);
    ArenaAddr(u64::from_le_bytes(buf))
}

/// Go `(*node).setNextAddr` / `(*node).setNexts`; see [`node_next`] for why
/// this is not an atomic store.
fn node_set_next(a: &mut Arena, addr: ArenaAddr, level: usize, next: ArenaAddr) {
    let off = NODE_HEADER_SIZE + level * 8;
    let d = a.get_mut(addr, off + 8);
    d[off..off + 8].copy_from_slice(&next.0.to_le_bytes());
}

/// Go's `ls.rand`, a `math/rand.Source64` seeded from `time.Now().Unix()`.
///
/// narrowing: no random-number crate is present in this offline workspace, so
/// this is splitmix64 rather than Go's lagged-Fibonacci source. Only the
/// distribution matters: [`MemStore::random_height`] promotes a node while the
/// draw falls below `u64::MAX / 4`, so heights are geometric with p = 1/4
/// either way. Individual node heights are not observable — no API exposes
/// them, and Go's own tests only assert that the resulting block count varies
/// by less than one percent between runs.
#[derive(Debug)]
struct Rand {
    state: u64,
}

impl Rand {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn uint64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Go `Hint`: the splice a caller carries between successive writes so that a
/// sequential workload does not re-search from the top of the list every time.
#[derive(Debug, Clone)]
pub struct Hint {
    height: usize,
    prev: [ArenaAddr; MAX_HEIGHT + 1],
    next: [ArenaAddr; MAX_HEIGHT + 1],
}

impl Default for Hint {
    fn default() -> Self {
        Self::new()
    }
}

impl Hint {
    /// Go's `new(Hint)`: a zero hint, which forces a full recompute on first
    /// use.
    #[must_use]
    pub fn new() -> Self {
        Self {
            height: 0,
            prev: [NULL_ARENA_ADDR; MAX_HEIGHT + 1],
            next: [NULL_ARENA_ADDR; MAX_HEIGHT + 1],
        }
    }
}

/// Go `MemStore`: a skiplist variant used to store locks.
///
/// Compared to a normal skiplist it only supports single-threaded writes, but
/// it can reuse memory so that memory usage does not keep growing.
///
/// narrowing: Go's readers walk the list with no lock at all, relying on
/// atomic link loads plus [`crate::arena::REUSE_SAFE_DURATION`] to bound the
/// window in which a freed node might still be read. This workspace forbids
/// `unsafe`, so the same single-writer/many-reader discipline is expressed as
/// `&mut self` for writes and `&self` for reads, and callers that want it
/// across threads wrap the store the way Go's own `TestMemStoreConcurrent`
/// already does — in an `RwLock`. Everything observable is unchanged: key
/// ordering, get/put/delete/replace results, iterator positioning, arena block
/// accounting, and the dump format. What is deliberately *not* reproduced is
/// Go's memory layout in the C sense — there are no raw pointers, no node
/// structs overlaid on memory, and no lock-free reader.
#[derive(Debug)]
pub struct MemStore {
    /// Current height, `1 <= height <= MAX_HEIGHT`.
    height: usize,
    head: ArenaAddr,
    arena: Arena,
    // We only consume 2 bits for a random height call.
    rand: Rand,
    length: usize,
}

impl MemStore {
    /// Go `NewMemStore`.
    #[must_use]
    pub fn new(arena_block_size: usize) -> Self {
        Self::with_seed(arena_block_size, default_seed())
    }

    /// [`MemStore::new`] with an explicit height-generator seed.
    ///
    /// Go has no such constructor: it always seeds from
    /// `time.Now().Unix()`. Naming the seed lets a test reproduce one exact
    /// shape of the list, which is the only way to pin block counts — the
    /// assertion that made Go's own `TestIterator` unstable enough to be
    /// skipped.
    #[must_use]
    pub fn with_seed(arena_block_size: usize, seed: u64) -> Self {
        let mut ls = Self {
            height: 1,
            head: NULL_ARENA_ADDR,
            arena: Arena::new(arena_block_size),
            rand: Rand::new(seed),
            length: 0,
        };
        ls.set_head_node();
        ls
    }

    /// Go `setHeadNode`.
    fn set_head_node(&mut self) {
        let n = self.new_node(&[], &[], MAX_HEIGHT);
        for i in 0..MAX_HEIGHT {
            node_set_next(&mut self.arena, n, i, NULL_ARENA_ADDR);
        }
        self.head = n;
    }

    /// The arena backing this store, as Go's `getArena` returns it.
    #[must_use]
    pub fn arena(&self) -> &Arena {
        &self.arena
    }

    /// Go `Get`: looks `key` up, refilling `buf` on a hit.
    ///
    /// `buf` is Go's caller-owned reusable output buffer. On a hit it is
    /// truncated and refilled with the value; on a miss it is left untouched
    /// and `None` returns, matching Go's `nil` result.
    pub fn get<'b>(&self, key: &[u8], buf: &'b mut Vec<u8>) -> Option<&'b [u8]> {
        let (e, matched) = self.find_greater(key, true);
        if !matched {
            return None;
        }
        buf.clear();
        buf.extend_from_slice(node_value(&self.arena, e));
        Some(buf.as_slice())
    }

    /// Go `getNext`.
    pub(crate) fn get_next(&self, n: ArenaAddr, level: usize) -> ArenaAddr {
        node_next(&self.arena, n, level)
    }

    /// The head node, which no lookup ever returns.
    pub(crate) fn head(&self) -> ArenaAddr {
        self.head
    }

    /// The key of a node this store returned.
    pub(crate) fn key_of(&self, addr: ArenaAddr) -> &[u8] {
        node_key(&self.arena, addr)
    }

    /// The value of a node this store returned.
    pub(crate) fn value_of(&self, addr: ArenaAddr) -> &[u8] {
        node_value(&self.arena, addr)
    }

    /// Go `findGreater`: the first entry `> key`, or `>= key` when
    /// `allow_equal`. The bool reports an exact match.
    pub(crate) fn find_greater(&self, key: &[u8], allow_equal: bool) -> (ArenaAddr, bool) {
        let mut prev = self.head;
        let mut level = self.height - 1;
        loop {
            let next = node_next(&self.arena, prev, level);
            if !next.is_null() {
                let cmp = node_key(&self.arena, next).cmp(key);
                if cmp == Ordering::Less {
                    // next key is still smaller, keep moving.
                    prev = next;
                    continue;
                }
                if cmp == Ordering::Equal {
                    // prev.key < key == next.key.
                    if allow_equal {
                        return (next, true);
                    }
                    level = 0;
                    prev = next;
                    continue;
                }
            }
            // next is greater than key or next is null. go to the lower level.
            if level > 0 {
                level -= 1;
                continue;
            }
            return (next, false);
        }
    }

    /// Go `findLess`: the last entry `< key`, or `<= key` when `allow_equal`.
    /// The bool reports an exact match.
    pub(crate) fn find_less(&self, key: &[u8], allow_equal: bool) -> (ArenaAddr, bool) {
        let mut prev = self.head;
        let mut level = self.height - 1;
        loop {
            let next = node_next(&self.arena, prev, level);
            if !next.is_null() {
                let cmp = key.cmp(node_key(&self.arena, next));
                if cmp == Ordering::Greater {
                    // prev.key < next.key < key. We can continue to move right.
                    prev = next;
                    continue;
                }
                if cmp == Ordering::Equal && allow_equal {
                    // prev.key < key == next.key.
                    return (next, true);
                }
            }
            // get closer to the key in the lower level.
            if level > 0 {
                level -= 1;
                continue;
            }
            break;
        }
        // We are not going to return head.
        if prev == self.head {
            return (NULL_ARENA_ADDR, false);
        }
        (prev, false)
    }

    /// Go `findSpliceForLevel`: returns `(before, after)` with
    /// `before.key < key <= after.key`, starting the walk at `before`. The
    /// bool reports that `after.key == key`.
    fn find_splice_for_level(
        &self,
        key: &[u8],
        mut before: ArenaAddr,
        level: usize,
    ) -> (ArenaAddr, ArenaAddr, bool) {
        loop {
            // Assume before.key < key.
            let next_addr = node_next(&self.arena, before, level);
            if next_addr.is_null() {
                return (before, NULL_ARENA_ADDR, false);
            }
            let cmp = node_key(&self.arena, next_addr).cmp(key);
            if cmp != Ordering::Less {
                // before.key < key < next.key. We are done for this level.
                return (before, next_addr, cmp == Ordering::Equal);
            }
            before = next_addr; // Keep moving right on this level.
        }
    }

    /// Go `findLast`: the last element, or null for an empty store. Like every
    /// other find, it never returns the head.
    pub(crate) fn find_last(&self) -> ArenaAddr {
        let mut e = self.head;
        let mut level = self.height - 1;
        loop {
            let next = node_next(&self.arena, e, level);
            if !next.is_null() {
                e = next;
                continue;
            }
            if level == 0 {
                if e == self.head {
                    return NULL_ARENA_ADDR;
                }
                return e;
            }
            level -= 1;
        }
    }

    /// Go `Put`: inserts or replaces, returning whether the key was new.
    pub fn put(&mut self, key: &[u8], v: &[u8]) -> bool {
        let mut hint = Hint::new();
        self.put_with_hint(key, v, &mut hint)
    }

    /// Go `PutWithHint`: [`MemStore::put`] reusing a caller-owned splice.
    pub fn put_with_hint(&mut self, key: &[u8], v: &[u8], hint: &mut Hint) -> bool {
        let ls_height = self.height;
        let recompute_height = self.calculate_recompute_height(key, hint, ls_height);
        let mut old = NULL_ARENA_ADDR;
        if recompute_height > 0 {
            for i in (0..recompute_height).rev() {
                // Use higher level to speed up for current level.
                let (prev, next, exists) = self.find_splice_for_level(key, hint.prev[i + 1], i);
                hint.prev[i] = prev;
                hint.next[i] = next;
                if exists {
                    old = next;
                }
            }
        } else if !hint.next[0].is_null() && node_key(&self.arena, hint.next[0]) == key {
            old = hint.next[0];
        }

        if !old.is_null() {
            self.replace(key, v, hint, old);
            return false;
        }
        let height = self.random_height();
        let x = self.new_node(key, v, height);
        if height > ls_height {
            self.height = height;
        }

        // We always insert from the base level and up. After you add a node in
        // the base level, we cannot create a node in the level above because it
        // would have discovered the node in the base level.
        for i in 0..height {
            node_set_next(&mut self.arena, x, i, hint.next[i]);
            if hint.prev[i].is_null() {
                hint.prev[i] = self.head;
            }
            node_set_next(&mut self.arena, hint.prev[i], i, x);
            hint.prev[i] = x;
        }
        self.length += 1;
        true
    }

    /// Go `replace`: writes a fresh node with the same height over `old`'s
    /// place in every level, then frees `old`.
    fn replace(&mut self, key: &[u8], v: &[u8], hint: &mut Hint, old: ArenaAddr) {
        let old_height = node_height(&self.arena, old);
        let x = self.new_node(key, v, old_height);
        for i in 0..old_height {
            let next_addr = node_next(&self.arena, old, i);
            node_set_next(&mut self.arena, x, i, next_addr);
            hint.next[i] = next_addr;
            node_set_next(&mut self.arena, hint.prev[i], i, x);
            hint.prev[i] = x;
        }
        self.arena.free(old);
    }

    /// Go `MaxEntrySize`: any entry larger than this will likely fail.
    #[must_use]
    pub fn max_entry_size(&self) -> usize {
        self.arena.block_size - NODE_HEADER_SIZE - self.height * 8
    }

    /// Go `newNode`.
    fn new_node(&mut self, key: &[u8], v: &[u8], height: usize) -> ArenaAddr {
        // The base level is already allocated in the node struct.
        let node_size = NODE_HEADER_SIZE + height * 8 + key.len() + v.len();
        let mut addr = self.arena.alloc(node_size);
        if addr.is_null() {
            self.arena.grow();
            // The new arena block must have enough memory to alloc.
            addr = self.arena.alloc(node_size);
        }
        let node_len = NODE_HEADER_SIZE + height * 8;
        let data = self.arena.get_mut(addr, node_size);
        data[0..8].copy_from_slice(&addr.0.to_le_bytes());
        data[8..10].copy_from_slice(&(height as u16).to_le_bytes());
        data[10..12].copy_from_slice(&(key.len() as u16).to_le_bytes());
        data[12..16].copy_from_slice(&(v.len() as u32).to_le_bytes());
        data[node_len..node_len + key.len()].copy_from_slice(key);
        data[node_len + key.len()..].copy_from_slice(v);
        addr
    }

    /// Go `randomHeight`: geometric with p = 1/4, capped at [`MAX_HEIGHT`].
    fn random_height(&mut self) -> usize {
        let mut h = 1;
        while h < MAX_HEIGHT && self.rand.uint64() < u64::MAX / 4 {
            h += 1;
        }
        h
    }

    /// Go `calculateRecomputeHeight`: how much of the hint's splice is stale
    /// for `key` and must be searched again.
    fn calculate_recompute_height(&self, key: &[u8], hint: &mut Hint, list_height: usize) -> usize {
        let mut recompute_height = 0;
        if hint.height < list_height {
            // Either splice is never used or list height has grown, we
            // recompute all.
            hint.prev[list_height] = self.head;
            hint.next[list_height] = NULL_ARENA_ADDR;
            hint.height = list_height;
            recompute_height = list_height;
        } else {
            while recompute_height < list_height {
                let prev_node = hint.prev[recompute_height];
                let next_node = hint.next[recompute_height];
                let prev_next = if prev_node.is_null() {
                    NULL_ARENA_ADDR
                } else {
                    node_next(&self.arena, prev_node, recompute_height)
                };
                if prev_next != next_node {
                    recompute_height += 1;
                    continue;
                }
                let key_before_prev = prev_node != self.head
                    && !prev_node.is_null()
                    && key <= node_key(&self.arena, prev_node);
                if key_before_prev {
                    while prev_node == hint.prev[recompute_height] {
                        recompute_height += 1;
                    }
                    continue;
                }
                let key_after_next = !next_node.is_null() && key > node_key(&self.arena, next_node);
                if key_after_next {
                    while next_node == hint.next[recompute_height] {
                        recompute_height += 1;
                    }
                    continue;
                }
                break;
            }
        }
        recompute_height
    }

    /// Go `DeleteWithHint`: removes `key`, returning whether it was present.
    pub fn delete_with_hint(&mut self, key: &[u8], hint: &mut Hint) -> bool {
        let list_height = self.height;
        let recompute_height = self.calculate_recompute_height(key, hint, list_height);
        let mut key_node = NULL_ARENA_ADDR;
        if recompute_height > 0 {
            for i in (0..recompute_height).rev() {
                // Use higher level to speed up for current level.
                let (prev, next, matched) = self.find_splice_for_level(key, hint.prev[i + 1], i);
                hint.prev[i] = prev;
                hint.next[i] = next;
                if matched {
                    key_node = next;
                }
            }
        } else if !hint.next[0].is_null() && node_key(&self.arena, hint.next[0]) == key {
            key_node = hint.next[0];
        }
        if key_node.is_null() {
            return false;
        }
        for i in (0..node_height(&self.arena, key_node)).rev() {
            // Change the nexts from higher to lower, so the data is consistent
            // at any point.
            let addr = node_next(&self.arena, key_node, i);
            hint.next[i] = addr;
            node_set_next(&mut self.arena, hint.prev[i], i, addr);
        }
        self.arena.free(key_node);
        self.length -= 1;
        true
    }

    /// Go `Delete`.
    pub fn delete(&mut self, key: &[u8]) -> bool {
        let mut hint = Hint::new();
        self.delete_with_hint(key, &mut hint)
    }

    /// Go `Len`: the number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Whether the store holds no entry. Go has no such method; Rust's API
    /// guidelines pair it with [`MemStore::len`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }
}

/// Go's `rand.NewSource(time.Now().Unix())` seed.
fn default_seed() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, RwLock};
    use std::time::{Duration, Instant};

    use super::*;
    use crate::arena::REUSE_SAFE_DURATION;
    use crate::testutil::{num_to_key, perm, TestRand, KEY_PREFIX};

    fn insert_mem_store(ls: &mut MemStore, prefix: &str, val_prefix: &str, n: usize) {
        let mut rng = TestRand::new();
        let perms = perm(&mut rng, n);
        let mut hint = Hint::new();
        for v in perms {
            let key_str = format!("{prefix}{v:020}");
            let val = format!("{val_prefix}{key_str}");
            ls.put_with_hint(key_str.as_bytes(), val.as_bytes(), &mut hint);
        }
    }

    fn check_mem_store(ls: &MemStore, prefix: &str, val_prefix: &str, n: usize) {
        let mut rng = TestRand::new();
        let perms = perm(&mut rng, n);
        let mut buf = Vec::new();
        for v in perms {
            let key = format!("{prefix}{v:020}");
            let val = ls.get(key.as_bytes(), &mut buf).expect("key must exist");
            assert_eq!(&val[..val_prefix.len()], val_prefix.as_bytes());
            assert_eq!(&val[val_prefix.len()..], key.as_bytes());
        }
    }

    fn delete_mem_store(ls: &mut MemStore, prefix: &str, n: usize) {
        let mut rng = TestRand::new();
        let perms = perm(&mut rng, n);
        for v in perms {
            let key = format!("{prefix}{v:020}");
            assert!(ls.delete(key.as_bytes()));
        }
    }

    /// Go `TestMemStore`.
    #[test]
    fn test_mem_store() {
        let prefix = KEY_PREFIX;
        let n = 30000;
        let mut ls = MemStore::new(1 << 10);
        let mut buf = Vec::new();
        assert!(ls.get(b"a", &mut buf).is_none());
        insert_mem_store(&mut ls, prefix, "", n);
        let num_blocks = ls.arena().blocks.len();
        check_mem_store(&ls, prefix, "", n);
        delete_mem_store(&mut ls, prefix, n);
        assert_eq!(ls.arena().blocks.len(), num_blocks);
        std::thread::sleep(REUSE_SAFE_DURATION);
        insert_mem_store(&mut ls, prefix, "", n);
        // Because the height is random, we insert again, the block number may
        // be different.
        let diff = ls.arena().blocks.len() - num_blocks;
        assert!(diff < num_blocks / 100, "diff {diff}, blocks {num_blocks}");
        assert!(ls.get(&num_to_key(n), &mut buf).is_none());
        assert!(ls.get(b"abc", &mut buf).is_none());
    }

    /// Go `TestReplace`.
    #[test]
    fn test_replace() {
        let prefix = KEY_PREFIX;
        let n = 30000;
        let mut ls = MemStore::new(1 << 10);
        insert_mem_store(&mut ls, prefix, "old", n);
        check_mem_store(&ls, prefix, "old", n);
        insert_mem_store(&mut ls, prefix, "new", n);
        check_mem_store(&ls, prefix, "new", n);
    }

    /// Go `TestMemStoreConcurrent`.
    ///
    /// Adaptations: Go's writer runs for 10 seconds; this runs for 2, which is
    /// enough to interleave millions of reads with the write stream while
    /// keeping the suite fast. Go guards the store with an explicit
    /// `sync.RWMutex`; Rust makes that guard the way the store is shared at
    /// all, which is the same discipline expressed in the type system. The
    /// assertion is Go's: a reader must never observe a value that does not
    /// match its key, i.e. never a half-published or recycled node.
    #[test]
    fn test_mem_store_concurrent() {
        const KEY_RANGE: usize = 10;
        let concurrent_keys: Vec<Vec<u8>> = (0..KEY_RANGE).map(num_to_key).collect();

        let ls = Arc::new(RwLock::new(MemStore::new(1 << 20)));
        let close = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut readers = Vec::new();
        for i in 0..KEY_RANGE {
            let ls = Arc::clone(&ls);
            let close = Arc::clone(&close);
            readers.push(std::thread::spawn(move || {
                let key = num_to_key(i);
                let mut buf = Vec::with_capacity(100);
                let mut n = 0u64;
                loop {
                    n += 1;
                    if n.is_multiple_of(128) && close.load(std::sync::atomic::Ordering::Relaxed) {
                        return n;
                    }
                    let guard = ls.read().expect("lock poisoned");
                    if let Some(result) = guard.get(&key, &mut buf) {
                        assert_eq!(result, key.as_slice(), "data corruption");
                    }
                }
            }));
        }

        let mut rng = TestRand::new();
        let start = Instant::now();
        let (mut total_insert, mut total_delete) = (0u64, 0u64);
        let mut hint = Hint::new();
        loop {
            if total_insert.is_multiple_of(128) && start.elapsed() > Duration::from_secs(2) {
                break;
            }
            let key = &concurrent_keys[rng.below(KEY_RANGE)];
            if ls
                .write()
                .expect("lock poisoned")
                .put_with_hint(key, key, &mut hint)
            {
                total_insert += 1;
            }
            let key = &concurrent_keys[rng.below(KEY_RANGE)];
            if ls
                .write()
                .expect("lock poisoned")
                .delete_with_hint(key, &mut hint)
            {
                total_delete += 1;
            }
        }
        close.store(true, std::sync::atomic::Ordering::Relaxed);
        for r in readers {
            r.join().expect("reader panicked");
        }
        assert!(total_insert > 0 && total_delete > 0);
    }

    /// Not a Go test: Go's `Put`/`Delete` without a hint are only exercised
    /// through the big randomized tests, so the small ordered surface is
    /// pinned directly.
    #[test]
    fn test_put_delete_len() {
        let mut ls = MemStore::new(1 << 10);
        assert!(ls.is_empty());
        assert!(ls.put(b"b", b"2"));
        assert!(ls.put(b"a", b"1"));
        assert!(ls.put(b"c", b"3"));
        assert_eq!(ls.len(), 3);
        // A second put of the same key replaces rather than inserts.
        assert!(!ls.put(b"b", b"22"));
        assert_eq!(ls.len(), 3);
        let mut buf = Vec::new();
        assert_eq!(ls.get(b"b", &mut buf), Some(b"22".as_slice()));
        assert!(ls.delete(b"b"));
        assert!(!ls.delete(b"b"));
        assert_eq!(ls.len(), 2);
        assert!(ls.get(b"b", &mut buf).is_none());
        assert!(ls.max_entry_size() < 1 << 10);
    }
}
