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
//! to the writable queue. What changes is only that the addresses index into
//! `Vec<u8>` blocks rather than into raw memory.

use std::collections::VecDeque;
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
/// Go needs the delay because its readers walk raw pointers without a lock, so
/// data corruption can happen under this sequence:
///  1. a reader reads a node;
///  2. a writer deletes the node, frees the block, puts it into the writable
///     queue, and that block becomes the first writable block;
///  3. the writer inserts another node, overwriting the block just freed;
///  4. the reader reads the key/value of that deleted node.
///
/// The window between 1 and 4 is very short, so it is very unlikely — but it
/// can happen, hence the wait. Rust's borrow checker already excludes the race
/// (see [`crate::MemStore`]), but the delay is kept because it is observable:
/// it decides when block reuse begins, and therefore how many blocks a
/// workload allocates.
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

/// Go `arena`.
#[derive(Debug)]
pub struct Arena {
    pub(crate) block_size: usize,
    pub(crate) blocks: Vec<ArenaBlock>,
    pub(crate) writable_queue: Vec<usize>,
    pending_blocks: VecDeque<PendingBlock>,
}

impl Arena {
    /// Go `newArenaLocator`.
    #[must_use]
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            blocks: vec![ArenaBlock::new(block_size)],
            writable_queue: vec![0],
            pending_blocks: VecDeque::new(),
        }
    }

    /// Go `(*arena).get`.
    ///
    /// # Panics
    ///
    /// Panics when `addr` names a block past the end, mirroring Go's
    /// `log.S().Fatalf` on the same condition.
    #[must_use]
    pub fn get(&self, addr: ArenaAddr, size: usize) -> &[u8] {
        assert!(
            addr.block_idx() < self.blocks.len(),
            "arena.get out of range. len(blocks)={}, addr.blockIdx()={}, addr.blockOffset()={}, size={size}",
            self.blocks.len(),
            addr.block_idx(),
            addr.block_offset(),
        );
        self.blocks[addr.block_idx()].get(addr.block_offset(), size)
    }

    /// The mutable counterpart of [`Arena::get`].
    ///
    /// Go has no such method: it hands out one raw pointer and writes through
    /// it. Rust needs the write path spelled out separately, which is what
    /// makes the exclusivity of a write checkable at compile time.
    pub fn get_mut(&mut self, addr: ArenaAddr, size: usize) -> &mut [u8] {
        let block_idx = addr.block_idx();
        assert!(
            block_idx < self.blocks.len(),
            "arena.get out of range. len(blocks)={}, addr.blockIdx()={block_idx}, addr.blockOffset()={}, size={size}",
            self.blocks.len(),
            addr.block_offset(),
        );
        self.blocks[block_idx].get_mut(addr.block_offset(), size)
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
                    self.writable_queue.push(pending.block_idx);
                    continue;
                }
                return NULL_ARENA_ADDR;
            }
            let avail_idx = *self.writable_queue.last().expect("queue is not empty");
            let block_offset = self.blocks[avail_idx].alloc(size);
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
        let block = &mut self.blocks[block_idx];
        block.ref_count -= 1;
        // No reference, the arena block can be reused.
        if block.ref_count == 0 && block.length > block.buf.len() {
            block.length = 0;
            self.pending_blocks.push_back(PendingBlock {
                block_idx,
                reusable_time: Instant::now() + REUSE_SAFE_DURATION,
            });
        }
    }

    /// Go `(*arena).grow`.
    ///
    /// narrowing: Go builds a whole new `arena` value and swaps the store's
    /// `arenaPtr` to it, because a lock-free reader may still be walking the
    /// old block slice. Rust reaches the arena through `&`/`&mut`, so no
    /// reader can be mid-walk while this runs and the new block is appended in
    /// place. The result is identical: the same blocks in the same order, the
    /// new index pushed onto the writable queue, and the pending list carried
    /// over. Go's copy also drops the old writable queue, which is always
    /// empty here — `alloc` only fails after emptying it.
    pub fn grow(&mut self) {
        let avail_idx = self.blocks.len();
        self.blocks.push(ArenaBlock::new(self.block_size));
        self.writable_queue.push(avail_idx);
    }

    /// The number of blocks queued for delayed reuse. Go's test reads
    /// `len(arena.pendingBlocks)` directly.
    #[must_use]
    pub fn pending_block_count(&self) -> usize {
        self.pending_blocks.len()
    }
}

/// Go `arenaBlock`.
#[derive(Debug)]
pub struct ArenaBlock {
    buf: Vec<u8>,
    ref_count: u64,
    /// Go's `length` deliberately runs past `len(buf)` on the allocation that
    /// overflows the block; `free` uses that overflow as the "this block is
    /// finished" marker.
    length: usize,
}

impl ArenaBlock {
    /// Go `newArenaBlock`.
    #[must_use]
    pub fn new(block_size: usize) -> Self {
        Self {
            buf: vec![0; block_size],
            ref_count: 0,
            length: 0,
        }
    }

    /// Go `(*arenaBlock).get`.
    #[must_use]
    pub fn get(&self, offset: u32, size: usize) -> &[u8] {
        let offset = offset as usize;
        &self.buf[offset..offset + size]
    }

    /// The mutable counterpart of [`ArenaBlock::get`].
    pub fn get_mut(&mut self, offset: u32, size: usize) -> &mut [u8] {
        let offset = offset as usize;
        &mut self.buf[offset..offset + size]
    }

    /// Go `(*arenaBlock).alloc`: the returned offset is 8-byte aligned.
    pub fn alloc(&mut self, size: usize) -> u32 {
        let offset = (self.length + 7) & ALIGN_MASK;
        self.length = offset + size;
        if self.length > self.buf.len() {
            return NULL_BLOCK_OFFSET;
        }
        self.ref_count += 1;
        offset as u32
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

        let mut block = ArenaBlock::new(64);
        assert_eq!(block.alloc(5), 0);
        // 5 rounds up to the next multiple of 8.
        assert_eq!(block.alloc(5), 8);
        assert_eq!(block.alloc(8), 16);
        // Overflowing the block yields the null offset and marks it finished.
        assert_eq!(block.alloc(64), NULL_BLOCK_OFFSET);
        assert!(block.length > block.buf.len());
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
