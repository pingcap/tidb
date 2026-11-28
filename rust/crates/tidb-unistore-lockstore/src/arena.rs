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

//! Go `arena.go`: the block allocator the skiplist carves its nodes out of.
//!
//! Every byte of accounting here is Go's: the same 8-byte alignment, the same
//! `blockIdx+1 << 32 | blockOffset` address packing, the same per-block
//! reference count, and the same delay before a drained block is handed back
//! to the writable queue. A block's bytes are atomic 8-byte words, so a reader
//! walks nodes while the single writer links new ones, as Go's readers do over
//! raw memory with `atomic.LoadUint64`.

use std::collections::VecDeque;
use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Go `arenaAddr`: a `blockIdx`/`blockOffset` pair packed into one word.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct ArenaAddr(pub u64);

/// Go `alignMask`: 29 bits of 1 and 3 bits of 0.
const ALIGN_MASK: usize = (1 << 32) - 8;
/// Go `nullBlockOffset`.
const NULL_BLOCK_OFFSET: u32 = u32::MAX;
/// Go `nullArenaAddr`.
pub const NULL_ARENA_ADDR: ArenaAddr = ArenaAddr(0);

/// Go `reuseSafeDuration`: the time waited before an emptied block is reused.
///
/// Go needs the delay because its readers walk nodes without a lock, so
/// data corruption can happen under this sequence:
///  1. a reader reads a node;
///  2. a writer deletes the node, frees the block, puts it into the writable
///     queue, and that block becomes the first writable block;
///  3. the writer inserts another node, overwriting the block just freed;
///  4. the reader reads the key/value of that deleted node.
///
/// The window between 1 and 4 is very short, so it is very unlikely — but it
/// can happen, hence the wait. Here it cannot happen at all: past the wait, a
/// drained block becomes writable only after a [`Grace`] period, once every
/// reader that could still hold one of its nodes has finished.
pub const REUSE_SAFE_DURATION: Duration = Duration::from_millis(100);

impl ArenaAddr {
    /// Go `(arenaAddr).blockIdx`.
    #[must_use]
    pub fn block_idx(self) -> usize {
        (self.0 >> 32) as usize - 1
    }

    /// Go `(arenaAddr).blockOffset`.
    #[must_use]
    pub fn block_offset(self) -> u32 {
        self.0 as u32
    }

    /// Whether this is Go's `nullArenaAddr`.
    #[must_use]
    pub fn is_null(self) -> bool {
        self == NULL_ARENA_ADDR
    }
}

/// Go `newArenaAddr`.
#[must_use]
pub fn new_arena_addr(block_idx: usize, block_offset: u32) -> ArenaAddr {
    ArenaAddr(((block_idx as u64 + 1) << 32) | u64::from(block_offset))
}

/// Go `pendingBlock`.
#[derive(Debug)]
struct PendingBlock {
    block_idx: usize,
    reusable_time: Instant,
}

/// Go `arena.blocks`: the block list a reader walks, replaced whole when the
/// arena grows (Go `grow` returns a new `arena` the store publishes).
pub type Blocks = Arc<Vec<Arc<ArenaBlock>>>;

/// The stripes of [`Grace`]'s reader counts, each on its own cache line.
const GRACE_STRIPES: usize = 64;

#[repr(align(128))]
#[derive(Debug, Default)]
struct Stripe(AtomicUsize);

/// The readers' grace period: what makes a drained block safe to rewrite.
/// A reader pins the current epoch while it reads; before the writer
/// rewrites a drained block it flips the epoch and waits until no reader of
/// the old epoch remains. A reader that pins after the flip starts after the
/// block drained, so it cannot reach the block's nodes.
#[derive(Debug)]
pub struct Grace {
    epoch: AtomicUsize,
    readers: [[Stripe; GRACE_STRIPES]; 2],
}

impl Default for Grace {
    fn default() -> Self {
        Self {
            epoch: AtomicUsize::new(0),
            readers: std::array::from_fn(|_| std::array::from_fn(|_| Stripe::default())),
        }
    }
}

/// A reader's pin on [`Grace`], released on drop.
#[derive(Debug)]
pub struct GracePin<'a> {
    grace: &'a Grace,
    parity: usize,
    stripe: usize,
}

impl Grace {
    /// Pins the current epoch for a read.
    pub fn pin(&self) -> GracePin<'_> {
        thread_local! {
            static STRIPE: usize = {
                static NEXT: AtomicUsize = AtomicUsize::new(0);
                NEXT.fetch_add(1, Ordering::Relaxed) % GRACE_STRIPES
            };
        }
        let stripe = STRIPE.with(|stripe| *stripe);
        loop {
            let epoch = self.epoch.load(Ordering::SeqCst);
            let parity = epoch & 1;
            self.readers[parity][stripe].0.fetch_add(1, Ordering::SeqCst);
            // A flip between the load and the count means the writer may not
            // see this reader; it pins the new epoch instead.
            if self.epoch.load(Ordering::SeqCst) == epoch {
                return GracePin {
                    grace: self,
                    parity,
                    stripe,
                };
            }
            self.readers[parity][stripe].0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Waits until every reader pinned before this call has finished.
    pub fn synchronize(&self) {
        let parity = self.epoch.fetch_add(1, Ordering::SeqCst) & 1;
        while self.readers[parity]
            .iter()
            .any(|stripe| stripe.0.load(Ordering::SeqCst) != 0)
        {
            std::thread::yield_now();
        }
    }
}

impl Drop for GracePin<'_> {
    fn drop(&mut self) {
        self.grace.readers[self.parity][self.stripe]
            .0
            .fetch_sub(1, Ordering::SeqCst);
    }
}

/// Go `arena`: the blocks plus the single writer's allocation state.
#[derive(Debug)]
pub struct Arena {
    /// The readers' grace period this arena's block reuse waits on.
    pub(crate) grace: Arc<Grace>,
    pub(crate) block_size: usize,
    pub(crate) blocks: Blocks,
    pub(crate) writable_queue: Vec<usize>,
    pending_blocks: VecDeque<PendingBlock>,
}

impl Arena {
    /// Go `newArenaLocator`.
    #[must_use]
    pub fn new(block_size: usize) -> Self {
        Self {
            grace: Arc::new(Grace::default()),
            block_size,
            blocks: Arc::new(vec![Arc::new(ArenaBlock::new(block_size))]),
            writable_queue: vec![0],
            pending_blocks: VecDeque::new(),
        }
    }

    /// Go `(*arena).alloc`: returns [`NULL_ARENA_ADDR`] when no writable block
    /// has room, which tells the caller to grow.
    pub fn alloc(&mut self, size: usize) -> ArenaAddr {
        loop {
            if self.writable_queue.is_empty() {
                let now = Instant::now();
                if let Some(pending) = self
                    .pending_blocks
                    .pop_front_if(|pending| now > pending.reusable_time)
                {
                    // Go makes the drained block writable again; first every
                    // reader that might still hold one of its nodes finishes.
                    self.grace.synchronize();
                    self.writable_queue.push(pending.block_idx);
                    continue;
                }
                return NULL_ARENA_ADDR;
            }
            let avail_idx = *self.writable_queue.last().expect("queue is not empty");
            let block_offset = self.block(avail_idx).alloc(size);
            if block_offset != NULL_BLOCK_OFFSET {
                return new_arena_addr(avail_idx, block_offset);
            }
            self.writable_queue.pop();
        }
    }

    /// Go `(*arena).free`: drops one reference to the block behind `addr` and
    /// makes it reusable once nothing references it.
    ///
    /// We do not know whether a concurrent reader still references the deleted
    /// entry, so the old data must not be overwritten for a while; the block
    /// only becomes writable again after [`REUSE_SAFE_DURATION`].
    pub fn free(&mut self, addr: ArenaAddr) {
        let block_idx = addr.block_idx();
        let block = self.block(block_idx);
        let ref_count = block.ref_count.fetch_sub(1, Ordering::Relaxed) - 1;
        // No reference, the arena block can be reused.
        if ref_count == 0 && block.length.load(Ordering::Relaxed) > block.size {
            block.length.store(0, Ordering::Relaxed);
            self.pending_blocks.push_back(PendingBlock {
                block_idx,
                reusable_time: Instant::now() + REUSE_SAFE_DURATION,
            });
        }
    }

    /// Go `(*arena).grow`: a new block list holding the old blocks and one
    /// more, which the store publishes for its readers; the new block is the
    /// writable one and the pending list carries over. (Go's copy also drops
    /// the old writable queue, always empty here: `alloc` only fails after
    /// emptying it.)
    pub fn grow(&mut self) {
        let mut blocks = Vec::with_capacity(self.blocks.len() + 1);
        blocks.extend(self.blocks.iter().cloned());
        let avail_idx = blocks.len();
        blocks.push(Arc::new(ArenaBlock::new(self.block_size)));
        self.blocks = Arc::new(blocks);
        self.writable_queue.push(avail_idx);
    }

    /// The block at `idx`.
    pub(crate) fn block(&self, idx: usize) -> &ArenaBlock {
        &self.blocks[idx]
    }

    /// Go `len(arena.blocks)`.
    #[must_use]
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    /// The number of blocks queued for delayed reuse. Go's test reads
    /// `len(arena.pendingBlocks)` directly.
    #[must_use]
    pub fn pending_block_count(&self) -> usize {
        self.pending_blocks.len()
    }
}

/// Go `arenaBlock`: `buf` as little-endian 8-byte words, so a node's links
/// are single atomic words and its other bytes are read without a lock.
#[derive(Debug)]
pub struct ArenaBlock {
    buf: Box<[AtomicU64]>,
    /// Go `len(buf)`.
    size: usize,
    ref_count: AtomicU64,
    /// Go's `length` deliberately runs past `len(buf)` on the allocation that
    /// overflows the block; `free` uses that overflow as the "this block is
    /// finished" marker.
    length: AtomicUsize,
}

impl ArenaBlock {
    /// Go `newArenaBlock`.
    #[must_use]
    pub fn new(block_size: usize) -> Self {
        Self {
            buf: (0..block_size.div_ceil(8)).map(|_| AtomicU64::new(0)).collect(),
            size: block_size,
            ref_count: AtomicU64::new(0),
            length: AtomicUsize::new(0),
        }
    }

    /// Go `(*arenaBlock).alloc`: the returned offset is 8-byte aligned.
    pub fn alloc(&self, size: usize) -> u32 {
        let offset = (self.length.load(Ordering::Relaxed) + 7) & ALIGN_MASK;
        self.length.store(offset + size, Ordering::Relaxed);
        if offset + size > self.size {
            return NULL_BLOCK_OFFSET;
        }
        self.ref_count.fetch_add(1, Ordering::Relaxed);
        offset as u32
    }

    /// The aligned word at byte `offset`, Go's `atomic.LoadUint64`.
    pub(crate) fn load_word(&self, offset: usize) -> u64 {
        self.buf[offset / 8].load(Ordering::Acquire)
    }

    /// Go's `atomic.StoreUint64` of the aligned word at byte `offset`: what
    /// publishes a node to readers.
    pub(crate) fn store_word(&self, offset: usize, value: u64) {
        self.buf[offset / 8].store(value, Ordering::Release);
    }

    /// Copies `len` bytes from byte `offset` into `out`.
    pub(crate) fn read_bytes(&self, offset: usize, len: usize, out: &mut Vec<u8>) {
        let mut pos = 0;
        while pos < len {
            let at = offset + pos;
            let word = self.buf[at / 8].load(Ordering::Relaxed).to_le_bytes();
            let start = at % 8;
            let take = (8 - start).min(len - pos);
            out.extend_from_slice(&word[start..start + take]);
            pos += take;
        }
    }

    /// `bytes.Compare(buf[offset:offset+len], other)`.
    pub(crate) fn cmp_bytes(&self, offset: usize, len: usize, other: &[u8]) -> CmpOrdering {
        let shared = len.min(other.len());
        let mut pos = 0;
        // Whole words from an aligned start compare as big-endian integers:
        // lexicographic order on eight bytes.
        if offset % 8 == 0 {
            while pos + 8 <= shared {
                let word = self.buf[(offset + pos) / 8].load(Ordering::Relaxed).swap_bytes();
                let theirs = u64::from_be_bytes(other[pos..pos + 8].try_into().expect("eight bytes"));
                match word.cmp(&theirs) {
                    CmpOrdering::Equal => pos += 8,
                    unequal => return unequal,
                }
            }
        }
        while pos < shared {
            let at = offset + pos;
            let word = self.buf[at / 8].load(Ordering::Relaxed).to_le_bytes();
            let start = at % 8;
            let take = (8 - start).min(shared - pos);
            match word[start..start + take].cmp(&other[pos..pos + take]) {
                CmpOrdering::Equal => pos += take,
                unequal => return unequal,
            }
        }
        len.cmp(&other.len())
    }

    /// Writes `bytes` at byte `offset` of a node no reader can reach yet.
    pub(crate) fn write_bytes(&self, offset: usize, bytes: &[u8]) {
        let mut pos = 0;
        while pos < bytes.len() {
            let at = offset + pos;
            let cell = &self.buf[at / 8];
            let mut word = cell.load(Ordering::Relaxed).to_le_bytes();
            let start = at % 8;
            let take = (8 - start).min(bytes.len() - pos);
            word[start..start + take].copy_from_slice(&bytes[pos..pos + take]);
            cell.store(u64::from_le_bytes(word), Ordering::Relaxed);
            pos += take;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Not a Go test: Go's arena has no direct unit test, but the address
    /// packing and the alignment are what every node offset depends on.
    #[test]
    fn test_addr_packing_and_alignment() {
        let addr = new_arena_addr(3, 4096);
        assert_eq!(addr.block_idx(), 3);
        assert_eq!(addr.block_offset(), 4096);
        assert!(!addr.is_null());
        assert!(NULL_ARENA_ADDR.is_null());

        let block = ArenaBlock::new(64);
        assert_eq!(block.alloc(5), 0);
        // 5 rounds up to the next multiple of 8.
        assert_eq!(block.alloc(5), 8);
        assert_eq!(block.alloc(8), 16);
        // Overflowing the block yields the null offset and marks it finished.
        assert_eq!(block.alloc(64), NULL_BLOCK_OFFSET);
        assert!(block.length.load(Ordering::Relaxed) > block.size);
    }

    /// Not a Go test: pins the writable-queue/pending-block cycle that Go's
    /// `TestMemStore` only observes indirectly through the block count.
    #[test]
    fn test_alloc_grow_and_delayed_reuse() {
        let mut arena = Arena::new(64);
        let a = arena.alloc(32);
        assert_eq!(a.block_idx(), 0);
        // The next allocation overflows block 0 and empties the writable queue.
        assert!(arena.alloc(64).is_null());
        arena.grow();
        let b = arena.alloc(32);
        assert_eq!(b.block_idx(), 1);

        // Block 0 still holds one live allocation, so freeing it queues it for
        // delayed reuse rather than making it writable at once.
        arena.free(a);
        assert_eq!(arena.pending_block_count(), 1);
        assert!(arena.alloc(64).is_null());
        std::thread::sleep(REUSE_SAFE_DURATION + Duration::from_millis(20));
        let c = arena.alloc(32);
        assert_eq!(c.block_idx(), 0);
        assert_eq!(arena.pending_block_count(), 0);
    }
}
