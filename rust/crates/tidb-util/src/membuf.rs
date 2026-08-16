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

//! Complete transcreation of Go `pkg/lightning/membuf` (`buffer.go`,
//! `limiter.go`): a block-based arena over a reusable block pool, plus the
//! quota limiter that bounds it.
//!
//! Go hands out `[]byte` slices that alias the arena's blocks, which Rust's
//! borrow checker will not express — a second allocation invalidates the
//! first borrow. The package already carries the answer: [`SliceLocation`],
//! the compact handle it introduced so callers could stop retaining slices.
//! Allocation therefore returns a location, and [`Buffer::slice`] /
//! [`Buffer::slice_mut`] read it back. The bytes, block growth, limits, and
//! accounting are identical; only the alias is expressed as a handle.
//!
//! Go's block cache is a buffered channel used with non-blocking send and
//! receive — a bounded free list, modeled here as a capped queue. Go's
//! limiter parks each waiter on its own channel and closes them in FIFO
//! order; here each waiter parks on its own condition variable, woken in the
//! same order, so a large waiter at the head still blocks the small ones
//! behind it rather than being starved.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

/// Go `defaultPoolSize`: how many blocks the pool caches.
pub const DEFAULT_POOL_SIZE: usize = 1024;
/// Go `defaultBlockSize`: 1 MiB.
pub const DEFAULT_BLOCK_SIZE: usize = 1 << 20;

/// Go's `smallObjOverheadBatch`: the granularity at which per-object
/// bookkeeping is charged to the limiter.
const SMALL_OBJ_OVERHEAD_BATCH: usize = 256 * 1024;

/// Go's `sizeOfSlice`, the cost charged for one handed-out `[]byte` header.
const SIZE_OF_SLICE: usize = 24;
/// Go's `sizeOfSliceLocation`, the cost charged for one `SliceLocation`.
const SIZE_OF_SLICE_LOCATION: usize = 12;

/// Go `ErrCannotAcquireMemory`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CannotAcquireMemory;

impl std::fmt::Display for CannotAcquireMemory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("cannot acquire memory from membuf limiter")
    }
}

impl std::error::Error for CannotAcquireMemory {}

/// Go `GetAlignedSize`: `size` rounded up to a whole number of blocks.
#[must_use]
pub const fn get_aligned_size(size: u64, block_size: u64) -> u64 {
    get_block_cnt(size, block_size) * block_size
}

/// Go's private `getBlockCnt`: `ceil(size / block_size)`.
#[must_use]
pub const fn get_block_cnt(size: u64, block_size: u64) -> u64 {
    size.div_ceil(block_size)
}

/// Go `Allocator`.
pub trait Allocator: Send + Sync {
    /// Go `Alloc`.
    fn alloc(&self, n: usize) -> Vec<u8>;
    /// Go `Free`.
    fn free(&self, block: Vec<u8>);
}

/// Go's private `stdAllocator`.
#[derive(Debug, Default)]
pub struct StdAllocator;

impl Allocator for StdAllocator {
    fn alloc(&self, n: usize) -> Vec<u8> {
        vec![0; n]
    }

    fn free(&self, _block: Vec<u8>) {}
}

struct LimiterInner {
    limit: i64,
    waiters: VecDeque<(i64, Arc<Waiter>)>,
}

#[derive(Default)]
struct Waiter {
    granted: Mutex<bool>,
    signal: Condvar,
}

/// Go `Limiter`: blocks an acquire once outstanding tokens reach the limit.
pub struct Limiter {
    init_limit: i64,
    inner: Mutex<LimiterInner>,
}

impl std::fmt::Debug for Limiter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Limiter")
            .field("init_limit", &self.init_limit)
            .field("limit", &self.limit())
            .finish()
    }
}

impl Limiter {
    /// Go `NewLimiter`.
    #[must_use]
    pub fn new(limit: i64) -> Self {
        Self {
            init_limit: limit,
            inner: Mutex::new(LimiterInner {
                limit,
                waiters: VecDeque::new(),
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LimiterInner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The tokens currently available.
    #[must_use]
    pub fn limit(&self) -> i64 {
        self.lock().limit
    }

    /// Go `Acquire`: takes `n` tokens, blocking until they are free.
    ///
    /// A waiter joins a queue rather than retrying, so acquires are granted in
    /// arrival order.
    pub fn acquire(&self, n: usize) {
        let n = n as i64;
        let waiter = {
            let mut inner = self.lock();
            if inner.limit >= n {
                inner.limit -= n;
                return;
            }
            let waiter = Arc::new(Waiter::default());
            inner.waiters.push_back((n, Arc::clone(&waiter)));
            waiter
        };

        let mut granted = waiter
            .granted
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while !*granted {
            granted = waiter
                .signal
                .wait(granted)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Go `TryAcquire`: takes `n` tokens or reports failure.
    ///
    /// An existing waiter blocks this even when the tokens would fit, so a
    /// non-blocking caller cannot jump the queue.
    pub fn try_acquire(&self, n: usize) -> bool {
        let mut inner = self.lock();
        if !inner.waiters.is_empty() || inner.limit < n as i64 {
            return false;
        }
        inner.limit -= n as i64;
        true
    }

    /// Go `Release`: returns `n` tokens and wakes whatever waiters now fit,
    /// in order, stopping at the first that does not.
    pub fn release(&self, n: usize) {
        let mut inner = self.lock();
        inner.limit += n as i64;
        if inner.limit > self.init_limit {
            tracing::error!(
                limit = inner.limit,
                init_limit = self.init_limit,
                "limit overflow"
            );
        }

        let mut woken = Vec::new();
        while let Some((needed, _)) = inner.waiters.front() {
            if inner.limit < *needed {
                break;
            }
            let (needed, waiter) = inner.waiters.pop_front().expect("front just checked");
            inner.limit -= needed;
            woken.push(waiter);
        }
        drop(inner);

        for waiter in woken {
            let mut granted = waiter
                .granted
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            *granted = true;
            waiter.signal.notify_all();
        }
    }
}

/// How a [`Pool`] is configured (Go's `Option` functions).
pub struct PoolConfig {
    /// Go `WithAllocator`.
    pub allocator: Arc<dyn Allocator>,
    /// Go `WithBlockSize`.
    pub block_size: usize,
    /// Go `WithBlockNum`: how many blocks the pool caches.
    pub block_num: usize,
    /// Go `WithPoolMemoryLimiter`.
    pub limiter: Option<Arc<Limiter>>,
}

impl std::fmt::Debug for PoolConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PoolConfig")
            .field("block_size", &self.block_size)
            .field("block_num", &self.block_num)
            .field("has_limiter", &self.limiter.is_some())
            .finish()
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            allocator: Arc::new(StdAllocator),
            block_size: DEFAULT_BLOCK_SIZE,
            block_num: DEFAULT_POOL_SIZE,
            limiter: None,
        }
    }
}

/// Go `Pool`: a fixed-size free list of equally sized blocks.
pub struct Pool {
    allocator: Arc<dyn Allocator>,
    block_size: usize,
    block_num: usize,
    block_cache: Mutex<VecDeque<Vec<u8>>>,
    limiter: Option<Arc<Limiter>>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Pool")
            .field("block_size", &self.block_size)
            .field("cached_blocks", &self.cached_block_count())
            .finish()
    }
}

impl Pool {
    /// Go `NewPool`.
    #[must_use]
    pub fn new(config: PoolConfig) -> Self {
        Self {
            allocator: config.allocator,
            block_size: config.block_size,
            block_num: config.block_num,
            block_cache: Mutex::new(VecDeque::new()),
            limiter: config.limiter,
        }
    }

    /// A pool with every default (Go `NewPool()` with no options).
    #[must_use]
    pub fn with_defaults() -> Self {
        Self::new(PoolConfig::default())
    }

    /// The pool's block size.
    #[must_use]
    pub const fn block_size(&self) -> usize {
        self.block_size
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, VecDeque<Vec<u8>>> {
        self.block_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// How many blocks are currently cached.
    #[must_use]
    pub fn cached_block_count(&self) -> usize {
        self.cache().len()
    }

    /// Go `TotalSize`: the memory held by the pool itself, excluding buffers.
    #[must_use]
    pub fn total_size(&self) -> i64 {
        (self.cached_block_count() * self.block_size) as i64
    }

    /// Go's private `acquire`: charge the limiter, then take a block.
    fn acquire_block(&self) -> Vec<u8> {
        if let Some(limiter) = &self.limiter {
            limiter.acquire(self.block_size);
        }
        self.take_block()
    }

    /// Go's private `takeBlock`: reuse a cached block, else allocate.
    fn take_block(&self) -> Vec<u8> {
        if let Some(block) = self.cache().pop_front() {
            return block;
        }
        self.allocator.alloc(self.block_size)
    }

    /// Go's private `release`: cache the block if there is room, else free it,
    /// then return its quota.
    fn release_block(&self, block: Vec<u8>) {
        {
            let mut cache = self.cache();
            if cache.len() < self.block_num {
                cache.push_back(block);
            } else {
                drop(cache);
                self.allocator.free(block);
            }
        }
        if let Some(limiter) = &self.limiter {
            limiter.release(self.block_size);
        }
    }

    /// Go `Destroy`: frees every cached block.
    pub fn destroy(&self) {
        let blocks: Vec<Vec<u8>> = self.cache().drain(..).collect();
        for block in blocks {
            self.allocator.free(block);
        }
    }
}

/// Go `SliceLocation`: where an allocation lives inside its buffer.
///
/// Go keeps this smaller than a slice and free of pointers so a large
/// population of them stays cheap; the same holds here.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SliceLocation {
    buf_idx: i32,
    offset: i32,
    /// Go `Length`.
    pub length: i32,
}

/// What one allocation produced.
///
/// Go's `AllocBytes` answers a slice that is either arena-backed or, when the
/// request exceeds a block, a standalone heap slice outside the arena and
/// outside the limiter. That split is explicit here.
#[derive(Debug, PartialEq, Eq)]
pub enum Allocation {
    /// Arena-backed; read it through [`Buffer::slice`].
    Pooled(SliceLocation),
    /// Larger than one block, so allocated on its own.
    Standalone(Vec<u8>),
}

/// Go `Buffer`: an arena that draws fixed-size blocks from a [`Pool`].
pub struct Buffer {
    pool: Arc<Pool>,
    blocks: Vec<Vec<u8>>,
    /// Go `blockCntLimit`, with `None` for Go's -1 (unlimited).
    block_cnt_limit: Option<usize>,
    /// Go `curBlockIdx`, with `None` for Go's -1 (no block yet).
    cur_block_idx: Option<usize>,
    cur_idx: usize,
    small_obj_overhead: usize,
    small_obj_overhead_cache: usize,
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Buffer")
            .field("blocks", &self.blocks.len())
            .field("block_cnt_limit", &self.block_cnt_limit)
            .field("cur_block_idx", &self.cur_block_idx)
            .field("cur_idx", &self.cur_idx)
            .finish()
    }
}

impl Buffer {
    /// Go `Pool.NewBuffer` with no options.
    #[must_use]
    pub fn new(pool: Arc<Pool>) -> Self {
        Self {
            pool,
            blocks: Vec::new(),
            block_cnt_limit: None,
            cur_block_idx: None,
            cur_idx: 0,
            small_obj_overhead: 0,
            small_obj_overhead_cache: 0,
        }
    }

    /// Go `Pool.NewBuffer(WithBufferMemoryLimit(limit))`.
    ///
    /// The limit is approximate: memory comes in blocks, so the effective cap
    /// is `block_size * ceil(limit / block_size)`.
    #[must_use]
    pub fn with_memory_limit(pool: Arc<Pool>, limit: u64) -> Self {
        let block_cnt_limit = get_block_cnt(limit, pool.block_size as u64) as usize;
        let mut buffer = Self::new(pool);
        buffer.block_cnt_limit = Some(block_cnt_limit);
        buffer.blocks = Vec::with_capacity(block_cnt_limit);
        buffer
    }

    /// Go `TotalSize`.
    #[must_use]
    pub fn total_size(&self) -> i64 {
        (self.blocks.len() * self.pool.block_size) as i64
    }

    fn cur_block_len(&self) -> usize {
        self.cur_block_idx
            .map_or(0, |index| self.blocks[index].len())
    }

    /// Go's private `recordSmallObjOverhead`.
    fn record_small_obj_overhead(&mut self, n: usize) {
        let Some(limiter) = self.pool.limiter.as_ref() else {
            return;
        };
        if n > self.small_obj_overhead_cache {
            limiter.acquire(SMALL_OBJ_OVERHEAD_BATCH);
            self.small_obj_overhead_cache += SMALL_OBJ_OVERHEAD_BATCH;
            self.small_obj_overhead += SMALL_OBJ_OVERHEAD_BATCH;
        }
        self.small_obj_overhead_cache -= n;
    }

    /// Go's private `releaseSmallObjOverhead`.
    fn release_small_obj_overhead(&mut self) {
        if let Some(limiter) = self.pool.limiter.as_ref() {
            limiter.release(self.small_obj_overhead);
        }
        self.small_obj_overhead = 0;
        self.small_obj_overhead_cache = 0;
    }

    /// Go `Reset`: keeps the blocks but starts filling from the first again.
    pub fn reset(&mut self) {
        if self.pool.limiter.is_some() {
            self.release_small_obj_overhead();
        }
        if !self.blocks.is_empty() {
            self.cur_block_idx = Some(0);
            self.cur_idx = 0;
        }
    }

    /// Go `Destroy`: returns every block to the pool.
    pub fn destroy(&mut self) {
        if self.pool.limiter.is_some() {
            self.release_small_obj_overhead();
        }
        for block in std::mem::take(&mut self.blocks) {
            self.pool.release_block(block);
        }
        self.cur_block_idx = None;
        self.cur_idx = 0;
    }

    /// Go's private `switchToNextBlock`: reuse an already-held block.
    fn switch_to_next_block(&mut self) -> bool {
        let next = match self.cur_block_idx {
            Some(index) => index + 1,
            None => 0,
        };
        if next < self.blocks.len() {
            self.cur_block_idx = Some(next);
            self.cur_idx = 0;
            return true;
        }
        false
    }

    fn append_block(&mut self, block: Vec<u8>) {
        self.blocks.push(block);
        self.cur_block_idx = Some(self.blocks.len() - 1);
        self.cur_idx = 0;
    }

    /// Go's private `addBlock`.
    fn add_block(&mut self) {
        if self.switch_to_next_block() {
            return;
        }
        let block = self.pool.acquire_block();
        self.append_block(block);
    }

    /// Go's private `addBlockWithReservedLimiterQuota`.
    fn add_block_with_reserved_quota(&mut self) {
        if self.switch_to_next_block() {
            return;
        }
        let block = self.pool.take_block();
        self.append_block(block);
    }

    /// Whether the block-count limit forbids starting another block.
    fn block_limit_reached(&self) -> bool {
        let Some(limit) = self.block_cnt_limit else {
            return false;
        };
        let next_index = match self.cur_block_idx {
            Some(index) => index + 1,
            None => 0,
        };
        next_index >= limit
    }

    /// Go's private `allocBytesWithSliceLocation`.
    fn alloc_location(&mut self, n: usize) -> Option<SliceLocation> {
        if n > self.pool.block_size {
            return None;
        }
        if self.cur_idx + n > self.cur_block_len() {
            if self.block_limit_reached() {
                return None;
            }
            self.add_block();
        }
        let location = SliceLocation {
            buf_idx: self.cur_block_idx.expect("a block was just ensured") as i32,
            offset: self.cur_idx as i32,
            length: n as i32,
        };
        self.cur_idx += n;
        Some(location)
    }

    /// Go `AllocBytes`. A request larger than one block is served on its own,
    /// outside the arena and outside the limiter.
    pub fn alloc_bytes(&mut self, n: usize) -> Option<Allocation> {
        if n > self.pool.block_size {
            return Some(Allocation::Standalone(vec![0; n]));
        }
        let location = self.alloc_location(n)?;
        if self.pool.limiter.is_some() {
            self.record_small_obj_overhead(SIZE_OF_SLICE);
        }
        Some(Allocation::Pooled(location))
    }

    /// Go `AllocBytesWithSliceLocation`: always arena-backed, so an
    /// over-block request simply fails.
    pub fn alloc_bytes_with_slice_location(&mut self, n: usize) -> Option<SliceLocation> {
        let location = self.alloc_location(n)?;
        if self.pool.limiter.is_some() {
            self.record_small_obj_overhead(SIZE_OF_SLICE_LOCATION);
        }
        Some(location)
    }

    /// Go `TryAllocBytes`: never blocks on the limiter.
    ///
    /// On failure the buffer is unchanged, which is why the quota for both a
    /// new block and the bookkeeping batch is reserved in one attempt before
    /// anything is mutated.
    pub fn try_alloc_bytes(&mut self, n: usize) -> Result<Option<Allocation>, CannotAcquireMemory> {
        if n > self.pool.block_size {
            return Ok(Some(Allocation::Standalone(vec![0; n])));
        }

        let need_block = self.cur_idx + n > self.cur_block_len();
        if need_block && self.block_limit_reached() {
            return Ok(None);
        }

        if let Some(limiter) = self.pool.limiter.clone() {
            let mut need_bytes = 0;
            let holds_spare_block = self
                .cur_block_idx
                .is_some_and(|index| index < self.blocks.len().saturating_sub(1));
            if need_block && !holds_spare_block {
                need_bytes += self.pool.block_size;
            }
            let needs_overhead_batch = SIZE_OF_SLICE > self.small_obj_overhead_cache;
            if needs_overhead_batch {
                need_bytes += SMALL_OBJ_OVERHEAD_BATCH;
            }
            if need_bytes > 0 && !limiter.try_acquire(need_bytes) {
                return Err(CannotAcquireMemory);
            }

            if need_block {
                self.add_block_with_reserved_quota();
            }
            if needs_overhead_batch {
                self.small_obj_overhead_cache += SMALL_OBJ_OVERHEAD_BATCH;
                self.small_obj_overhead += SMALL_OBJ_OVERHEAD_BATCH;
            }
            self.small_obj_overhead_cache -= SIZE_OF_SLICE;
        } else if need_block {
            self.add_block();
        }

        let location = SliceLocation {
            buf_idx: self.cur_block_idx.expect("a block was just ensured") as i32,
            offset: self.cur_idx as i32,
            length: n as i32,
        };
        self.cur_idx += n;
        Ok(Some(Allocation::Pooled(location)))
    }

    /// Go `GetSlice`.
    #[must_use]
    pub fn slice(&self, location: &SliceLocation) -> &[u8] {
        let start = location.offset as usize;
        let end = start + location.length as usize;
        &self.blocks[location.buf_idx as usize][start..end]
    }

    /// The mutable form of [`Buffer::slice`], which is how a caller fills an
    /// allocation Go would have handed back directly.
    pub fn slice_mut(&mut self, location: &SliceLocation) -> &mut [u8] {
        let start = location.offset as usize;
        let end = start + location.length as usize;
        &mut self.blocks[location.buf_idx as usize][start..end]
    }

    /// Go `AddBytes`.
    pub fn add_bytes(&mut self, bytes: &[u8]) -> Option<Allocation> {
        match self.alloc_bytes(bytes.len())? {
            Allocation::Pooled(location) => {
                self.slice_mut(&location).copy_from_slice(bytes);
                Some(Allocation::Pooled(location))
            }
            Allocation::Standalone(mut owned) => {
                owned.copy_from_slice(bytes);
                Some(Allocation::Standalone(owned))
            }
        }
    }

    /// Go `TryAddBytes`.
    pub fn try_add_bytes(
        &mut self,
        bytes: &[u8],
    ) -> Result<Option<Allocation>, CannotAcquireMemory> {
        match self.try_alloc_bytes(bytes.len())? {
            None => Ok(None),
            Some(Allocation::Pooled(location)) => {
                self.slice_mut(&location).copy_from_slice(bytes);
                Ok(Some(Allocation::Pooled(location)))
            }
            Some(Allocation::Standalone(mut owned)) => {
                owned.copy_from_slice(bytes);
                Ok(Some(Allocation::Standalone(owned)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};

    fn pooled(allocation: Option<Allocation>) -> SliceLocation {
        match allocation {
            Some(Allocation::Pooled(location)) => location,
            other => panic!("expected a pooled allocation, got {other:?}"),
        }
    }

    // Go `TestGetAlignedSizeGetBlockCnt`.
    #[test]
    fn aligned_size_and_block_count() {
        assert_eq!(get_block_cnt(10, 16), 1);
        assert_eq!(get_block_cnt(17, 16), 2);
        assert_eq!(get_aligned_size(10, 16), 16);
        assert_eq!(get_aligned_size(17, 16), 32);
    }

    // Go `TestLimiter`: concurrent acquire/release never exceeds the limit,
    // and every token comes back.
    #[test]
    fn limiter_never_exceeds_its_limit() {
        let limit = 20;
        let limiter = Arc::new(Limiter::new(limit));
        let outstanding = Arc::new(AtomicI64::new(0));

        std::thread::scope(|scope| {
            for _ in 0..100 {
                let limiter = Arc::clone(&limiter);
                let outstanding = Arc::clone(&outstanding);
                scope.spawn(move || {
                    limiter.acquire(1);
                    let held = outstanding.fetch_add(1, Ordering::SeqCst) + 1;
                    assert!(held <= limit, "{held} outstanding exceeds {limit}");
                    outstanding.fetch_sub(1, Ordering::SeqCst);
                    limiter.release(1);
                });
            }
        });

        assert_eq!(limiter.limit(), limit);
    }

    // Go `TestWaitUpMultipleCaller`: one release wakes every waiter that now
    // fits, and the limit reflects all of them.
    #[test]
    fn releasing_wakes_multiple_waiters() {
        let limit = 20;
        let limiter = Arc::new(Limiter::new(limit));
        limiter.acquire(18);

        let finished = Arc::new(AtomicI64::new(0));
        std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..3 {
                let limiter = Arc::clone(&limiter);
                let finished = Arc::clone(&finished);
                handles.push(scope.spawn(move || {
                    limiter.acquire(3);
                    finished.fetch_add(1, Ordering::SeqCst);
                }));
            }

            // None can proceed: only 2 tokens are free and each wants 3.
            std::thread::sleep(std::time::Duration::from_millis(50));
            assert_eq!(finished.load(Ordering::SeqCst), 0);

            limiter.release(18);
            for handle in handles {
                handle.join().expect("waiter finished");
            }
        });

        assert_eq!(limiter.limit(), limit - 3 * 3);
    }

    // A queued waiter blocks a later non-blocking caller even when the tokens
    // would fit, so try_acquire cannot jump the queue.
    #[test]
    fn try_acquire_yields_to_waiters() {
        let limiter = Limiter::new(10);
        assert!(limiter.try_acquire(10));
        assert!(!limiter.try_acquire(1));

        // With a waiter queued, even a fitting request is refused.
        let limiter = Arc::new(Limiter::new(10));
        limiter.acquire(10);
        let waiting = Arc::clone(&limiter);
        std::thread::scope(|scope| {
            let handle = scope.spawn(move || waiting.acquire(6));
            std::thread::sleep(std::time::Duration::from_millis(50));
            limiter.release(4);
            assert!(!limiter.try_acquire(1));
            limiter.release(6);
            handle.join().expect("waiter finished");
        });
    }

    // Go `TestBufferPool`: blocks come back to the pool and are reused.
    #[test]
    fn buffers_return_their_blocks_to_the_pool() {
        let pool = Arc::new(Pool::new(PoolConfig {
            block_size: 1024,
            block_num: 2,
            ..PoolConfig::default()
        }));
        assert_eq!(pool.total_size(), 0);

        let mut buffer = Buffer::new(Arc::clone(&pool));
        let first = pooled(buffer.alloc_bytes(16));
        buffer.slice_mut(&first).copy_from_slice(&[7; 16]);
        assert_eq!(buffer.slice(&first), &[7; 16]);
        assert_eq!(buffer.total_size(), 1024);

        buffer.destroy();
        // The block is now cached rather than freed.
        assert_eq!(pool.cached_block_count(), 1);
        assert_eq!(pool.total_size(), 1024);

        // A fresh buffer reuses it instead of allocating.
        let mut buffer = Buffer::new(Arc::clone(&pool));
        let _ = buffer.alloc_bytes(16);
        assert_eq!(pool.cached_block_count(), 0);
        buffer.destroy();

        pool.destroy();
        assert_eq!(pool.cached_block_count(), 0);
    }

    // Go `TestBufferIsolation`: separate allocations never overlap.
    #[test]
    fn allocations_are_isolated_from_each_other() {
        let pool = Arc::new(Pool::new(PoolConfig {
            block_size: 1024,
            ..PoolConfig::default()
        }));
        let mut buffer = Buffer::new(pool);

        let first = pooled(buffer.add_bytes(&[1; 10]));
        let second = pooled(buffer.add_bytes(&[2; 10]));
        let third = pooled(buffer.add_bytes(&[3; 10]));

        assert_eq!(buffer.slice(&first), &[1; 10]);
        assert_eq!(buffer.slice(&second), &[2; 10]);
        assert_eq!(buffer.slice(&third), &[3; 10]);

        // Writing through one does not disturb its neighbours.
        buffer.slice_mut(&second).copy_from_slice(&[9; 10]);
        assert_eq!(buffer.slice(&first), &[1; 10]);
        assert_eq!(buffer.slice(&second), &[9; 10]);
        assert_eq!(buffer.slice(&third), &[3; 10]);
    }

    // Reset rewinds to the first block and keeps the blocks for reuse.
    #[test]
    fn reset_rewinds_without_returning_blocks() {
        let pool = Arc::new(Pool::new(PoolConfig {
            block_size: 16,
            ..PoolConfig::default()
        }));
        let mut buffer = Buffer::new(Arc::clone(&pool));

        let first = pooled(buffer.add_bytes(&[1; 16]));
        // A second allocation needs a second block.
        let _ = buffer.add_bytes(&[2; 16]);
        assert_eq!(buffer.total_size(), 32);

        buffer.reset();
        assert_eq!(buffer.total_size(), 32);
        let after = pooled(buffer.alloc_bytes(16));
        // Reset returns to the very start of the first block.
        assert_eq!(after, first);
    }

    // Go `TestBufferMemLimit`: the per-buffer limit caps blocks, and an
    // allocation past it fails rather than growing.
    #[test]
    fn a_buffer_memory_limit_caps_its_blocks() {
        let pool = Arc::new(Pool::new(PoolConfig {
            block_size: 16,
            ..PoolConfig::default()
        }));
        // Two blocks' worth: ceil(20/16) == 2.
        let mut buffer = Buffer::with_memory_limit(Arc::clone(&pool), 20);

        assert!(buffer.alloc_bytes(16).is_some());
        assert!(buffer.alloc_bytes(16).is_some());
        assert_eq!(buffer.total_size(), 32);
        // The third block is refused.
        assert!(buffer.alloc_bytes(16).is_none());

        // An over-block request bypasses the arena entirely.
        match buffer.alloc_bytes(64) {
            Some(Allocation::Standalone(owned)) => assert_eq!(owned.len(), 64),
            other => panic!("expected a standalone allocation, got {other:?}"),
        }
    }

    // Go `TestPoolMemLimit`: the pool's limiter bounds block acquisition, and
    // try_alloc_bytes reports exhaustion instead of blocking.
    #[test]
    fn a_pool_limiter_bounds_block_acquisition() {
        let block_size = SMALL_OBJ_OVERHEAD_BATCH;
        // Room for exactly one block plus one bookkeeping batch.
        let limiter = Arc::new(Limiter::new((block_size + SMALL_OBJ_OVERHEAD_BATCH) as i64));
        let pool = Arc::new(Pool::new(PoolConfig {
            block_size,
            limiter: Some(Arc::clone(&limiter)),
            ..PoolConfig::default()
        }));
        let mut buffer = Buffer::new(Arc::clone(&pool));

        // The first allocation takes one block and one overhead batch.
        assert!(buffer
            .try_alloc_bytes(8)
            .expect("quota available")
            .is_some());
        assert_eq!(limiter.limit(), 0);

        // A second block cannot be funded.
        let filling = block_size;
        assert_eq!(buffer.try_alloc_bytes(filling), Err(CannotAcquireMemory));

        // Destroying the buffer returns both the block and the overhead.
        buffer.destroy();
        assert_eq!(
            limiter.limit(),
            (block_size + SMALL_OBJ_OVERHEAD_BATCH) as i64
        );
    }

    // Destroy returns the buffer to a usable, empty state.
    #[test]
    fn destroy_empties_the_buffer() {
        let pool = Arc::new(Pool::new(PoolConfig {
            block_size: 32,
            ..PoolConfig::default()
        }));
        let mut buffer = Buffer::new(Arc::clone(&pool));
        let _ = buffer.add_bytes(&[1; 8]);
        assert_eq!(buffer.total_size(), 32);

        buffer.destroy();
        assert_eq!(buffer.total_size(), 0);

        // It can be filled again afterwards.
        let again = pooled(buffer.add_bytes(&[2; 8]));
        assert_eq!(buffer.slice(&again), &[2; 8]);
    }

    // A slice location survives further allocations, which is the whole point
    // of handing one out instead of a slice.
    #[test]
    fn slice_locations_stay_valid_across_allocations() {
        let pool = Arc::new(Pool::new(PoolConfig {
            block_size: 64,
            ..PoolConfig::default()
        }));
        let mut buffer = Buffer::new(pool);

        let first = buffer
            .alloc_bytes_with_slice_location(4)
            .expect("fits in a block");
        buffer.slice_mut(&first).copy_from_slice(&[1, 2, 3, 4]);

        for _ in 0..50 {
            let _ = buffer.add_bytes(&[9; 8]);
        }

        assert_eq!(buffer.slice(&first), &[1, 2, 3, 4]);
        assert_eq!(first.length, 4);
    }
}
