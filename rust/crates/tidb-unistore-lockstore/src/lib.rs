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
//!
//! # Concurrency without `unsafe`
//!
//! Go implements this with raw pointer arithmetic: a node is a variable-length
//! struct overlaid on arena bytes, and readers walk the whole list with no
//! lock, observing links through `atomic.LoadUint64` while the single writer
//! publishes them with `atomic.StoreUint64`. This workspace forbids `unsafe`,
//! so:
//!
//! - **Addresses replace pointers.** Every link is an [`arena::ArenaAddr`] —
//!   Go's own `blockIdx+1 << 32 | blockOffset` packing — and a block's bytes
//!   are little-endian atomic words in Go's byte layout. A link is one word,
//!   stored with release and loaded with acquire ordering, Go's atomics.
//! - **Readers take no lock.** [`MemStore::get`] and [`Iterator`] read while
//!   the writer runs. A grown arena is a new block list the store publishes
//!   whole (Go's `arenaPtr` swap); a reader whose view predates a block it
//!   reaches reloads the list.
//! - **One writer.** Go's store "only supports single thread write", which
//!   its callers honour by funnelling writes through `writeLockWorker`. The
//!   writer's state sits behind a lock that makes the contract checkable.
//!
//! [`arena::REUSE_SAFE_DURATION`] is Go's: a block emptied by deletes is
//! rewritten only after the delay, so a reader still on a deleted node reads
//! its old bytes.
//!
//! # Other narrowings
//!
//! - [`lockstore::MemStore::with_seed`] is not a Go constructor. Go always
//!   seeds node heights from `time.Now().Unix()` with `math/rand`; no
//!   random-number crate is reachable offline, so heights come from splitmix64
//!   with the same p = 1/4 geometric distribution, and the extra constructor
//!   lets a test fix the list's shape. Individual node heights are not
//!   observable through any API.
//! - `Get` takes the caller's reusable buffer as `&mut Vec<u8>` and returns
//!   `Option<&[u8]>`, where Go appends into a `[]byte` and returns `nil` on a
//!   miss.
//! - Go's `LoadFromFile` swallows its own errors through a `defer` that
//!   overwrites the named return; see [`lockstore::MemStore::load_from_file`].
//!
//! # Not ported
//!
//! `lockstore_test.go`'s four benchmarks — `BenchmarkMemStoreDeleteInsertGet`,
//! `BenchmarkMemStoreIterate`, `BenchmarkPutWithHint`, and `BenchmarkPut` —
//! are skipped. They are timing harnesses, not tests: each measures the same
//! `Put`/`Delete`/`Get`/iterate calls the ported tests already assert on, and
//! `cargo bench` on stable has no counterpart to `testing.B`.
//!
//! `main_test.go` is likewise skipped: it is `goleak` plus TiDB's global
//! `testsetup` for the Go test binary, neither of which has an analogue here.

pub mod arena;
pub mod iterator;
pub mod load_dump;
#[cfg(test)]
mod testutil;

pub use arena::{Arena, ArenaAddr, NULL_ARENA_ADDR, REUSE_SAFE_DURATION};
pub use iterator::Iterator;

use std::cmp::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex, PoisonError};

use arc_swap::ArcSwap;

use crate::arena::{ArenaBlock, Blocks};

/// Go `maxHeight`.
pub const MAX_HEIGHT: usize = 16;

/// Go `nodeHeaderSize`: `unsafe.Sizeof(nodeHeader{})` on a 64-bit target.
pub const NODE_HEADER_SIZE: usize = 16;

/// Go's `nodeHeader` after `addr`: `height`, `keyLen`, `valLen`, the node's
/// second word.
#[derive(Clone, Copy)]
struct NodeHeader {
    height: usize,
    key_len: usize,
    val_len: usize,
}

impl NodeHeader {
    fn decode(word: u64) -> Self {
        Self {
            height: (word & 0xffff) as usize,
            key_len: ((word >> 16) & 0xffff) as usize,
            val_len: (word >> 32) as usize,
        }
    }

    /// Go `(*node).nodeLen`: the header plus the next-pointer array.
    fn node_len(self) -> usize {
        self.height * 8 + NODE_HEADER_SIZE
    }
}

/// The node operations over one block list: Go's `(*node)` methods reached
/// through `ls.getArena()`.
pub(crate) struct Nodes<'a>(&'a [Arc<ArenaBlock>]);

impl Nodes<'_> {
    fn block(&self, addr: ArenaAddr) -> (&ArenaBlock, usize) {
        assert!(
            addr.block_idx() < self.0.len(),
            "arena.get out of range. len(blocks)={}, addr.blockIdx()={}, addr.blockOffset()={}",
            self.0.len(),
            addr.block_idx(),
            addr.block_offset(),
        );
        (&self.0[addr.block_idx()], addr.block_offset() as usize)
    }

    fn header(&self, addr: ArenaAddr) -> NodeHeader {
        let (block, offset) = self.block(addr);
        NodeHeader::decode(block.load_word(offset + 8))
    }

    /// Go `(*node).getNextAddr`: `atomic.LoadUint64`.
    fn next(&self, addr: ArenaAddr, level: usize) -> ArenaAddr {
        let (block, offset) = self.block(addr);
        ArenaAddr(block.load_word(offset + NODE_HEADER_SIZE + level * 8))
    }

    /// Go `(*node).setNextAddr`: `atomic.StoreUint64`, which publishes the
    /// node a link points to.
    fn set_next(&self, addr: ArenaAddr, level: usize, next: ArenaAddr) {
        let (block, offset) = self.block(addr);
        block.store_word(offset + NODE_HEADER_SIZE + level * 8, next.0);
    }

    /// `bytes.Compare(node.getKey(), key)`.
    fn cmp_key(&self, addr: ArenaAddr, key: &[u8]) -> Ordering {
        let header = self.header(addr);
        let (block, offset) = self.block(addr);
        block.cmp_bytes(offset + header.node_len(), header.key_len, key)
    }

    /// Go `(*node).getKey`, copied out.
    pub(crate) fn key(&self, addr: ArenaAddr, out: &mut Vec<u8>) {
        let header = self.header(addr);
        let (block, offset) = self.block(addr);
        out.clear();
        block.read_bytes(offset + header.node_len(), header.key_len, out);
    }

    /// Go `(*node).getValue`, copied out.
    pub(crate) fn value(&self, addr: ArenaAddr, out: &mut Vec<u8>) {
        let header = self.header(addr);
        let (block, offset) = self.block(addr);
        out.clear();
        block.read_bytes(offset + header.node_len() + header.key_len, header.val_len, out);
    }
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

/// The single writer's state: Go's arena allocation fields and `ls.rand`.
/// Go's store "only supports single thread write" (its callers funnel writes
/// through `writeLockWorker`); the lock is that contract made checkable.
#[derive(Debug)]
struct Writer {
    arena: Arena,
    // We only consume 2 bits for a random height call.
    rand: Rand,
}

/// Go `MemStore`: a skiplist variant used to store locks.
///
/// Compared to a normal skiplist it only supports single-threaded writes, but
/// it can reuse memory so that memory usage does not keep growing. Readers
/// take no lock: links are loaded and stored atomically, a grown arena is
/// published whole (Go's `arenaPtr`), and an emptied block is reused only
/// after [`crate::arena::REUSE_SAFE_DURATION`].
pub struct MemStore {
    /// Current height, `1 <= height <= MAX_HEIGHT` (Go `atomic` height).
    height: AtomicUsize,
    pub(crate) head: ArenaAddr,
    /// Go `arenaPtr`: the block list readers walk.
    blocks: ArcSwap<Vec<Arc<ArenaBlock>>>,
    /// The arena's [`arena::Grace`]: readers pin it.
    grace: Arc<arena::Grace>,
    writer: Mutex<Writer>,
    length: AtomicUsize,
}

impl std::fmt::Debug for MemStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemStore")
            .field("height", &self.height)
            .field("length", &self.length)
            .finish_non_exhaustive()
    }
}

/// A reader's view of the blocks (Go reads `ls.getArena()` at every access),
/// reloaded when a link names a block grown after the view was taken, and
/// its pin on the grace period that keeps the nodes it reads from being
/// rewritten. The guard is `arc_swap`'s: a reader takes no shared reference
/// count.
pub(crate) struct ReadView<'a> {
    ls: &'a MemStore,
    blocks: arc_swap::Guard<Blocks>,
    _pin: arena::GracePin<'a>,
}

impl ReadView<'_> {
    pub(crate) fn nodes(&mut self, addr: ArenaAddr) -> Nodes<'_> {
        if !addr.is_null() && addr.block_idx() >= self.blocks.len() {
            self.blocks = self.ls.blocks.load();
        }
        Nodes(&self.blocks)
    }
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
        let mut writer = Writer {
            arena: Arena::new(arena_block_size),
            rand: Rand::new(seed),
        };
        let head = new_node(&mut writer.arena, &[], &[], MAX_HEIGHT);
        let ls = Self {
            height: AtomicUsize::new(1),
            head,
            blocks: ArcSwap::new(Arc::clone(&writer.arena.blocks)),
            grace: Arc::clone(&writer.arena.grace),
            writer: Mutex::new(writer),
            length: AtomicUsize::new(0),
        };
        // Go `setHeadNode`.
        {
            let writer = ls.lock_writer();
            let nodes = Nodes(&writer.arena.blocks);
            for i in 0..MAX_HEIGHT {
                nodes.set_next(head, i, NULL_ARENA_ADDR);
            }
        }
        ls
    }

    fn lock_writer(&self) -> std::sync::MutexGuard<'_, Writer> {
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The single writer, held across a batch of writes (Go's
    /// `writeLockWorker` applies a batch group as the one writer).
    #[must_use]
    pub fn writer(&self) -> MemStoreWriter<'_> {
        MemStoreWriter {
            ls: self,
            writer: self.lock_writer(),
        }
    }

    pub(crate) fn read_view(&self) -> ReadView<'_> {
        // Pin first: the blocks loaded after it hold every node the read can
        // reach.
        let pin = self.grace.pin();
        ReadView {
            ls: self,
            blocks: self.blocks.load(),
            _pin: pin,
        }
    }

    /// Go `len(ls.getArena().blocks)`.
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.lock_writer().arena.block_count()
    }

    /// Go `len(ls.getArena().pendingBlocks)`.
    #[must_use]
    pub fn pending_block_count(&self) -> usize {
        self.lock_writer().arena.pending_block_count()
    }

    fn get_height(&self) -> usize {
        self.height.load(AtomicOrdering::Acquire)
    }

    /// Go `Get`: looks `key` up, refilling `buf` on a hit.
    ///
    /// `buf` is Go's caller-owned reusable output buffer. On a hit it is
    /// truncated and refilled with the value; on a miss it is left untouched
    /// and `None` returns, matching Go's `nil` result.
    pub fn get<'b>(&self, key: &[u8], buf: &'b mut Vec<u8>) -> Option<&'b [u8]> {
        let mut view = self.read_view();
        let (e, matched) = self.find_greater(&mut view, key, true);
        if !matched {
            return None;
        }
        view.nodes(e).value(e, buf);
        Some(buf.as_slice())
    }

    /// Go `getNext`.
    pub(crate) fn get_next(&self, view: &mut ReadView<'_>, n: ArenaAddr, level: usize) -> ArenaAddr {
        view.nodes(n).next(n, level)
    }

    /// Go `findGreater`: the first entry `> key`, or `>= key` when
    /// `allow_equal`. The bool reports an exact match.
    pub(crate) fn find_greater(&self, view: &mut ReadView<'_>, key: &[u8], allow_equal: bool) -> (ArenaAddr, bool) {
        let mut prev = self.head;
        let mut level = self.get_height() - 1;
        loop {
            let next = view.nodes(prev).next(prev, level);
            if !next.is_null() {
                let cmp = view.nodes(next).cmp_key(next, key);
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
    pub(crate) fn find_less(&self, view: &mut ReadView<'_>, key: &[u8], allow_equal: bool) -> (ArenaAddr, bool) {
        let mut prev = self.head;
        let mut level = self.get_height() - 1;
        loop {
            let next = view.nodes(prev).next(prev, level);
            if !next.is_null() {
                let cmp = view.nodes(next).cmp_key(next, key).reverse();
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

    /// Go `findLast`: the last element, or null for an empty store. Like every
    /// other find, it never returns the head.
    pub(crate) fn find_last(&self, view: &mut ReadView<'_>) -> ArenaAddr {
        let mut e = self.head;
        let mut level = self.get_height() - 1;
        loop {
            let next = view.nodes(e).next(e, level);
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
    pub fn put(&self, key: &[u8], v: &[u8]) -> bool {
        let mut hint = Hint::new();
        self.put_with_hint(key, v, &mut hint)
    }

    /// Go `PutWithHint`: [`MemStore::put`] reusing a caller-owned splice.
    pub fn put_with_hint(&self, key: &[u8], v: &[u8], hint: &mut Hint) -> bool {
        self.writer().put_with_hint(key, v, hint)
    }

    fn put_with_hint_locked(&self, writer: &mut Writer, key: &[u8], v: &[u8], hint: &mut Hint) -> bool {
        let ls_height = self.get_height();
        let recompute_height = {
            let nodes = Nodes(&writer.arena.blocks);
            self.calculate_recompute_height(&nodes, key, hint, ls_height)
        };
        let mut old = NULL_ARENA_ADDR;
        {
            let nodes = Nodes(&writer.arena.blocks);
            if recompute_height > 0 {
                for i in (0..recompute_height).rev() {
                    // Use higher level to speed up for current level.
                    let (prev, next, exists) = find_splice_for_level(&nodes, key, hint.prev[i + 1], i);
                    hint.prev[i] = prev;
                    hint.next[i] = next;
                    if exists {
                        old = next;
                    }
                }
            } else if !hint.next[0].is_null() && nodes.cmp_key(hint.next[0], key) == Ordering::Equal {
                old = hint.next[0];
            }
        }

        if !old.is_null() {
            self.replace(writer, key, v, hint, old);
            return false;
        }
        let height = random_height(&mut writer.rand);
        let x = self.new_node(writer, key, v, height);
        if height > ls_height {
            self.height.store(height, AtomicOrdering::Release);
        }

        // We always insert from the base level and up. After you add a node in
        // the base level, we cannot create a node in the level above because it
        // would have discovered the node in the base level.
        let nodes = Nodes(&writer.arena.blocks);
        for i in 0..height {
            nodes.set_next(x, i, hint.next[i]);
            if hint.prev[i].is_null() {
                hint.prev[i] = self.head;
            }
            nodes.set_next(hint.prev[i], i, x);
            hint.prev[i] = x;
        }
        self.length.fetch_add(1, AtomicOrdering::Relaxed);
        true
    }

    /// Go `replace`: writes a fresh node with the same height over `old`'s
    /// place in every level, then frees `old`.
    fn replace(&self, writer: &mut Writer, key: &[u8], v: &[u8], hint: &mut Hint, old: ArenaAddr) {
        let old_height = Nodes(&writer.arena.blocks).header(old).height;
        let x = self.new_node(writer, key, v, old_height);
        let nodes = Nodes(&writer.arena.blocks);
        for i in 0..old_height {
            let next_addr = nodes.next(old, i);
            nodes.set_next(x, i, next_addr);
            hint.next[i] = next_addr;
            // Go indexes the nil prev and panics here; stale hints past the
            // recomputed height are unreachable with valid hints.
            assert!(
                !hint.prev[i].is_null(),
                "lockstore replace: null prev at level {i}"
            );
            nodes.set_next(hint.prev[i], i, x);
            hint.prev[i] = x;
        }
        writer.arena.free(old);
    }

    /// Go `MaxEntrySize`: any entry larger than this will likely fail.
    #[must_use]
    pub fn max_entry_size(&self) -> usize {
        self.lock_writer().arena.block_size - NODE_HEADER_SIZE - self.get_height() * 8
    }

    /// Go `newNode`, with Go's `grow` publishing the grown arena.
    fn new_node(&self, writer: &mut Writer, key: &[u8], v: &[u8], height: usize) -> ArenaAddr {
        let grown_before = Arc::as_ptr(&writer.arena.blocks);
        let addr = new_node(&mut writer.arena, key, v, height);
        if Arc::as_ptr(&writer.arena.blocks) != grown_before {
            // Go `ls.setArena(arena.grow())`.
            self.blocks.store(Arc::clone(&writer.arena.blocks));
        }
        addr
    }

    /// Go `calculateRecomputeHeight`: how much of the hint's splice is stale
    /// for `key` and must be searched again.
    fn calculate_recompute_height(&self, nodes: &Nodes<'_>, key: &[u8], hint: &mut Hint, list_height: usize) -> usize {
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
                    nodes.next(prev_node, recompute_height)
                };
                if prev_next != next_node {
                    recompute_height += 1;
                    continue;
                }
                let key_before_prev = prev_node != self.head
                    && !prev_node.is_null()
                    && nodes.cmp_key(prev_node, key) != Ordering::Less;
                if key_before_prev {
                    while prev_node == hint.prev[recompute_height] {
                        recompute_height += 1;
                    }
                    continue;
                }
                let key_after_next = !next_node.is_null() && nodes.cmp_key(next_node, key) == Ordering::Less;
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
    pub fn delete_with_hint(&self, key: &[u8], hint: &mut Hint) -> bool {
        self.writer().delete_with_hint(key, hint)
    }

    fn delete_with_hint_locked(&self, writer: &mut Writer, key: &[u8], hint: &mut Hint) -> bool {
        let list_height = self.get_height();
        let nodes = Nodes(&writer.arena.blocks);
        let recompute_height = self.calculate_recompute_height(&nodes, key, hint, list_height);
        let mut key_node = NULL_ARENA_ADDR;
        if recompute_height > 0 {
            for i in (0..recompute_height).rev() {
                // Use higher level to speed up for current level.
                let (prev, next, matched) = find_splice_for_level(&nodes, key, hint.prev[i + 1], i);
                hint.prev[i] = prev;
                hint.next[i] = next;
                if matched {
                    key_node = next;
                }
            }
        } else if !hint.next[0].is_null() && nodes.cmp_key(hint.next[0], key) == Ordering::Equal {
            key_node = hint.next[0];
        }
        if key_node.is_null() {
            return false;
        }
        for i in (0..nodes.header(key_node).height).rev() {
            // Change the nexts from higher to lower, so the data is consistent
            // at any point.
            let addr = nodes.next(key_node, i);
            hint.next[i] = addr;
            // Go indexes the nil prev and panics; same unreachable-with-
            // valid-hints guard as replace.
            assert!(
                !hint.prev[i].is_null(),
                "lockstore delete: null prev at level {i}"
            );
            nodes.set_next(hint.prev[i], i, addr);
        }
        writer.arena.free(key_node);
        self.length.fetch_sub(1, AtomicOrdering::Relaxed);
        true
    }

    /// Go `Delete`.
    pub fn delete(&self, key: &[u8]) -> bool {
        let mut hint = Hint::new();
        self.delete_with_hint(key, &mut hint)
    }

    /// Go `Len`: the number of live entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.length.load(AtomicOrdering::Relaxed)
    }

    /// Whether the store holds no entry. Go has no such method; Rust's API
    /// guidelines pair it with [`MemStore::len`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// [`MemStore::writer`]: writes applied under one hold of the writer.
pub struct MemStoreWriter<'a> {
    ls: &'a MemStore,
    writer: std::sync::MutexGuard<'a, Writer>,
}

impl MemStoreWriter<'_> {
    /// Go `PutWithHint`.
    pub fn put_with_hint(&mut self, key: &[u8], v: &[u8], hint: &mut Hint) -> bool {
        self.ls.put_with_hint_locked(&mut self.writer, key, v, hint)
    }

    /// Go `DeleteWithHint`.
    pub fn delete_with_hint(&mut self, key: &[u8], hint: &mut Hint) -> bool {
        self.ls.delete_with_hint_locked(&mut self.writer, key, hint)
    }
}

/// Go `findSpliceForLevel`: returns `(before, after)` with
/// `before.key < key <= after.key`, starting the walk at `before`. The bool
/// reports that `after.key == key`.
fn find_splice_for_level(
    nodes: &Nodes<'_>,
    key: &[u8],
    mut before: ArenaAddr,
    level: usize,
) -> (ArenaAddr, ArenaAddr, bool) {
    loop {
        // Assume before.key < key.
        let next_addr = nodes.next(before, level);
        if next_addr.is_null() {
            return (before, NULL_ARENA_ADDR, false);
        }
        let cmp = nodes.cmp_key(next_addr, key);
        if cmp != Ordering::Less {
            // before.key < key < next.key. We are done for this level.
            return (before, next_addr, cmp == Ordering::Equal);
        }
        before = next_addr; // Keep moving right on this level.
    }
}

/// Go `newNode`: allocates (growing the arena when no block has room) and
/// writes the node's header, key and value; its links are set by the caller,
/// whose last `setNextAddr` publishes it.
fn new_node(arena: &mut Arena, key: &[u8], v: &[u8], height: usize) -> ArenaAddr {
    // The base level is already allocated in the node struct.
    let node_size = NODE_HEADER_SIZE + height * 8 + key.len() + v.len();
    let mut addr = arena.alloc(node_size);
    if addr.is_null() {
        arena.grow();
        // The new arena block must have enough memory to alloc.
        addr = arena.alloc(node_size);
    }
    let node_len = NODE_HEADER_SIZE + height * 8;
    let block = arena.block(addr.block_idx());
    let offset = addr.block_offset() as usize;
    block.write_bytes(offset, &addr.0.to_le_bytes());
    let header = height as u64 | ((key.len() as u64) << 16) | ((v.len() as u64) << 32);
    block.write_bytes(offset + 8, &header.to_le_bytes());
    block.write_bytes(offset + node_len, key);
    block.write_bytes(offset + node_len + key.len(), v);
    addr
}

/// Go `randomHeight`: geometric with p = 1/4, capped at [`MAX_HEIGHT`].
fn random_height(rand: &mut Rand) -> usize {
    let mut h = 1;
    while h < MAX_HEIGHT && rand.uint64() < u64::MAX / 4 {
        h += 1;
    }
    h
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
        let num_blocks = ls.block_count();
        check_mem_store(&ls, prefix, "", n);
        delete_mem_store(&mut ls, prefix, n);
        assert_eq!(ls.block_count(), num_blocks);
        std::thread::sleep(REUSE_SAFE_DURATION);
        insert_mem_store(&mut ls, prefix, "", n);
        // Because the height is random, we insert again, the block number may
        // be different.
        let diff = ls.block_count() - num_blocks;
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

    /// [`test_mem_store_concurrent`] with the readers Go's MVCC store has:
    /// no lock at all (`store.lockStore.Get` beside `writeLockWorker`).
    #[test]
    fn test_mem_store_lock_free_readers() {
        const KEY_RANGE: usize = 10;
        let concurrent_keys: Vec<Vec<u8>> = (0..KEY_RANGE).map(num_to_key).collect();
        // Small blocks, so the writer grows and reuses blocks under the
        // readers.
        let ls = Arc::new(MemStore::new(1 << 10));
        let close = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut readers = Vec::new();
        for i in 0..KEY_RANGE {
            let ls = Arc::clone(&ls);
            let close = Arc::clone(&close);
            readers.push(std::thread::spawn(move || {
                let key = num_to_key(i);
                let mut buf = Vec::with_capacity(100);
                while !close.load(std::sync::atomic::Ordering::Relaxed) {
                    if let Some(result) = ls.get(&key, &mut buf) {
                        assert_eq!(result, key.as_slice(), "data corruption");
                    }
                    let mut it = ls.new_iterator();
                    it.seek_to_first();
                    while it.valid() {
                        assert_eq!(it.key(), it.value(), "data corruption");
                        it.next();
                    }
                }
            }));
        }
        let mut rng = TestRand::new();
        let start = Instant::now();
        let mut hint = Hint::new();
        while start.elapsed() < Duration::from_secs(2) {
            let key = &concurrent_keys[rng.below(KEY_RANGE)];
            ls.put_with_hint(key, key, &mut hint);
            let key = &concurrent_keys[rng.below(KEY_RANGE)];
            ls.delete_with_hint(key, &mut hint);
        }
        close.store(true, std::sync::atomic::Ordering::Relaxed);
        for r in readers {
            r.join().expect("reader panicked");
        }
        assert!(ls.block_count() > 1, "the writer grew the arena under the readers");
    }

    /// Not a Go test: Go's `Put`/`Delete` without a hint are only exercised
    /// through the big randomized tests, so the small ordered surface is
    /// pinned directly.
    #[test]
    fn test_put_delete_len() {
        let ls = MemStore::new(1 << 10);
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

