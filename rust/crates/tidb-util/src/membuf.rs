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

//! Block-based arena and quota limiter from Go `pkg/lightning/membuf`.

use std::backtrace::Backtrace;
use std::collections::VecDeque;
use std::ops::{Deref, DerefMut, Range};
use std::sync::{Arc, Condvar, Mutex, RwLock};

/// Go `defaultPoolSize`: how many blocks the pool caches.
const DEFAULT_POOL_SIZE: isize = 1024;
/// Go `defaultBlockSize`: 1 MiB.
const DEFAULT_BLOCK_SIZE: isize = 1 << 20;

/// Go's `smallObjOverheadBatch`: the granularity at which per-object
/// bookkeeping is charged to the limiter.
const SMALL_OBJ_OVERHEAD_BATCH: isize = 256 * 1024;

/// Go's `sizeOfSlice`, the cost charged for one handed-out `[]byte` header.
const SIZE_OF_SLICE: isize = 3 * std::mem::size_of::<usize>() as isize;
/// Go's `sizeOfSliceLocation`, the cost charged for one `SliceLocation`.
const SIZE_OF_SLICE_LOCATION: isize = std::mem::size_of::<SliceLocation>() as isize;

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
pub fn get_aligned_size(size: u64, block_size: u64) -> u64 {
    get_block_cnt(size, block_size).wrapping_mul(block_size)
}

/// Go's private `getBlockCnt`: `ceil(size / block_size)`.
const fn get_block_cnt(size: u64, block_size: u64) -> u64 {
    size.wrapping_add(block_size).wrapping_sub(1) / block_size
}

/// Go `Allocator`.
pub trait Allocator: Send + Sync {
    /// Go `Alloc`.
    fn alloc(&self, n: isize) -> Block;
    /// Go `Free`.
    fn free(&self, block: Block);
}

/// Native owner for a block returned by a Go-style [`Allocator`].
pub struct Block {
    bytes: Option<Vec<u8>>,
    capacity: usize,
    release_on_drop: bool,
}

impl Block {
    /// Wraps storage whose allocator permits automatic release.
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        let capacity = bytes.capacity();
        Self {
            bytes: Some(bytes),
            capacity,
            release_on_drop: true,
        }
    }

    /// Go `cap(block)` for the full block returned by an allocator.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub(crate) fn manually_managed(bytes: Vec<u8>) -> Self {
        let capacity = bytes.len();
        Self {
            bytes: Some(bytes),
            capacity,
            release_on_drop: false,
        }
    }

    pub(crate) fn release(mut self) {
        self.release_on_drop = true;
    }
}

impl Deref for Block {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.bytes.as_deref().expect("block storage is present")
    }
}

impl DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.bytes.as_deref_mut().expect("block storage is present")
    }
}

impl Drop for Block {
    fn drop(&mut self) {
        if !self.release_on_drop {
            std::mem::forget(self.bytes.take());
        }
    }
}

/// Go's private `stdAllocator`.
#[derive(Default)]
struct StdAllocator;

impl Allocator for StdAllocator {
    fn alloc(&self, n: isize) -> Block {
        Block::from_vec(vec![
            0;
            usize::try_from(n).expect("negative allocation size")
        ])
    }

    fn free(&self, _block: Block) {}
}

struct LimiterInner {
    limit: isize,
    waiters: VecDeque<(isize, Arc<Waiter>)>,
}

#[derive(Default)]
struct Waiter {
    granted: Mutex<bool>,
    signal: Condvar,
}

/// Go `Limiter`: blocks an acquire once outstanding tokens reach the limit.
pub struct Limiter {
    init_limit: isize,
    inner: Mutex<LimiterInner>,
}

impl Limiter {
    /// Go `NewLimiter`.
    pub fn new(limit: isize) -> Self {
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

    #[cfg(test)]
    fn limit(&self) -> isize {
        self.lock().limit
    }

    /// Go `Acquire`: takes `n` tokens, blocking until they are free.
    ///
    /// A waiter joins a queue rather than retrying, so acquires are granted in
    /// arrival order.
    pub fn acquire(&self, n: isize) {
        let waiter = {
            let mut inner = self.lock();
            if inner.limit >= n {
                inner.limit = inner.limit.wrapping_sub(n);
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
    pub fn try_acquire(&self, n: isize) -> bool {
        let mut inner = self.lock();
        if !inner.waiters.is_empty() || inner.limit < n {
            return false;
        }
        inner.limit = inner.limit.wrapping_sub(n);
        true
    }

    /// Go `Release`: returns `n` tokens and wakes whatever waiters now fit,
    /// in order, stopping at the first that does not.
    pub fn release(&self, n: isize) {
        let mut inner = self.lock();
        inner.limit = inner.limit.wrapping_add(n);
        if inner.limit > self.init_limit {
            tracing::error!(
                limit = inner.limit,
                init_limit = self.init_limit,
                stack = %Backtrace::force_capture(),
                "limit overflow"
            );
        }

        let mut woken = Vec::new();
        while let Some((needed, _)) = inner.waiters.front() {
            if inner.limit < *needed {
                break;
            }
            let (needed, waiter) = inner.waiters.pop_front().expect("front just checked");
            inner.limit = inner.limit.wrapping_sub(needed);
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

enum PoolOptionKind {
    BlockNum(isize),
    BlockSize(isize),
    Allocator(Option<Arc<dyn Allocator>>),
    Limiter(Option<Arc<Limiter>>),
}

/// Native opaque representation of Go `Option`.
pub struct PoolOption(PoolOptionKind);

/// Go `WithBlockNum`.
pub fn with_block_num(num: isize) -> PoolOption {
    PoolOption(PoolOptionKind::BlockNum(num))
}

/// Go `WithBlockSize`.
pub fn with_block_size(bytes: isize) -> PoolOption {
    PoolOption(PoolOptionKind::BlockSize(bytes))
}

/// Go `WithAllocator`; `None` represents a nil interface.
pub fn with_allocator(allocator: Option<Arc<dyn Allocator>>) -> PoolOption {
    PoolOption(PoolOptionKind::Allocator(allocator))
}

/// Go `WithPoolMemoryLimiter`; `None` represents a nil pointer.
pub fn with_pool_memory_limiter(limiter: Option<Arc<Limiter>>) -> PoolOption {
    PoolOption(PoolOptionKind::Limiter(limiter))
}

/// Go `Pool`: a fixed-size free list of equally sized blocks.
pub struct Pool {
    allocator: Option<Arc<dyn Allocator>>,
    block_size: isize,
    block_cache: Mutex<PoolState>,
    limiter: Option<Arc<Limiter>>,
}

struct PoolState {
    blocks: VecDeque<Block>,
    capacity: usize,
    closed: bool,
}

impl Pool {
    /// Go `NewPool`.
    pub fn new(options: impl IntoIterator<Item = PoolOption>) -> Self {
        let mut allocator: Option<Arc<dyn Allocator>> = Some(Arc::new(StdAllocator));
        let mut block_size = DEFAULT_BLOCK_SIZE;
        let mut capacity = DEFAULT_POOL_SIZE as usize;
        let mut limiter = None;
        for option in options {
            match option.0 {
                PoolOptionKind::BlockNum(value) => {
                    capacity = usize::try_from(value).expect("negative block count");
                }
                PoolOptionKind::BlockSize(value) => block_size = value,
                PoolOptionKind::Allocator(value) => allocator = value,
                PoolOptionKind::Limiter(value) => limiter = value,
            }
        }
        Self {
            allocator,
            block_size,
            block_cache: Mutex::new(PoolState {
                blocks: VecDeque::new(),
                capacity,
                closed: false,
            }),
            limiter,
        }
    }

    fn cache(&self) -> std::sync::MutexGuard<'_, PoolState> {
        self.block_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn cached_block_count(&self) -> usize {
        self.cache().blocks.len()
    }

    /// Go `TotalSize`: the memory held by the pool itself, excluding buffers.
    pub fn total_size(&self) -> i64 {
        (self.cached_block_count() as isize).wrapping_mul(self.block_size) as i64
    }

    /// Go's private `acquire`: charge the limiter, then take a block.
    fn acquire_block(&self) -> Block {
        if let Some(limiter) = &self.limiter {
            limiter.acquire(self.block_size);
        }
        self.take_block()
    }

    /// Go's private `takeBlock`: reuse a cached block, else allocate.
    fn take_block(&self) -> Block {
        {
            let mut state = self.cache();
            if state.closed {
                return Block::from_vec(Vec::new());
            }
            if let Some(block) = state.blocks.pop_front() {
                return block;
            }
        }
        self.allocator
            .as_ref()
            .expect("nil membuf allocator")
            .alloc(self.block_size)
    }

    /// Go's private `release`: cache the block if there is room, else free it,
    /// then return its quota.
    fn release_block(&self, block: Block) {
        {
            let mut state = self.cache();
            assert!(!state.closed, "send on closed membuf block cache");
            if state.blocks.len() < state.capacity {
                state.blocks.push_back(block);
            } else {
                drop(state);
                self.allocator
                    .as_ref()
                    .expect("nil membuf allocator")
                    .free(block);
            }
        }
        if let Some(limiter) = &self.limiter {
            limiter.release(self.block_size);
        }
    }

    /// Go `Destroy`: frees every cached block.
    pub fn destroy(&self) {
        let blocks: Vec<Block> = {
            let mut state = self.cache();
            assert!(!state.closed, "close of closed membuf block cache");
            state.closed = true;
            state.blocks.drain(..).collect()
        };
        for block in blocks {
            self.allocator
                .as_ref()
                .expect("nil membuf allocator")
                .free(block);
        }
    }

    /// Go `Pool.NewBuffer`.
    pub fn new_buffer(self: &Arc<Self>, options: impl IntoIterator<Item = BufferOption>) -> Buffer {
        Buffer::new(Arc::clone(self), options)
    }
}

/// Native opaque representation of Go `BufferOption`.
pub struct BufferOption {
    limit: u64,
}

/// Go `WithBufferMemoryLimit`.
pub fn with_buffer_memory_limit(limit: u64) -> BufferOption {
    BufferOption { limit }
}

/// Go `SliceLocation`: where an allocation lives inside its buffer.
///
/// Go keeps this smaller than a slice and free of pointers so a large
/// population of them stays cheap; the same holds here.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct SliceLocation {
    buf_idx: i32,
    offset: i32,
    /// Go `Length`.
    pub length: i32,
}

#[derive(Clone)]
enum BytesStorage {
    Pooled {
        block: Arc<RwLock<Block>>,
        offset: usize,
        length: usize,
    },
    Standalone(Arc<RwLock<Block>>),
}

/// Native slice header for the `[]byte` values returned by Go.
///
/// Pooled values alias their buffer and therefore have the same contract as
/// Go: all aliases must be released before the buffer is reset or destroyed.
#[derive(Clone)]
pub struct Bytes {
    storage: BytesStorage,
}

impl Bytes {
    fn pooled(block: Arc<RwLock<Block>>, offset: usize, length: usize) -> Self {
        Self {
            storage: BytesStorage::Pooled {
                block,
                offset,
                length,
            },
        }
    }

    fn standalone(length: usize) -> Self {
        Self {
            storage: BytesStorage::Standalone(Arc::new(RwLock::new(Block::from_vec(vec![
                0;
                length
            ])))),
        }
    }

    /// Go `len(bytes)`.
    pub fn len(&self) -> usize {
        match &self.storage {
            BytesStorage::Pooled { length, .. } => *length,
            BytesStorage::Standalone(bytes) => bytes
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
        }
    }

    /// Go `cap(bytes)`; pooled allocations use a full slice expression.
    pub fn capacity(&self) -> usize {
        self.len()
    }

    /// Borrows the slice's bytes.
    pub fn as_slice(&self) -> BytesRef<'_> {
        match &self.storage {
            BytesStorage::Pooled {
                block,
                offset,
                length,
            } => BytesRef {
                guard: block
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                range: *offset..*offset + *length,
            },
            BytesStorage::Standalone(bytes) => {
                let guard = bytes
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let length = guard.len();
                BytesRef {
                    guard,
                    range: 0..length,
                }
            }
        }
    }

    /// Mutably borrows the slice's bytes.
    pub fn as_mut_slice(&self) -> BytesMut<'_> {
        match &self.storage {
            BytesStorage::Pooled {
                block,
                offset,
                length,
            } => BytesMut {
                guard: block
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                range: *offset..*offset + *length,
            },
            BytesStorage::Standalone(bytes) => {
                let guard = bytes
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let length = guard.len();
                BytesMut {
                    guard,
                    range: 0..length,
                }
            }
        }
    }
}

/// Read guard for a native Go-style byte slice.
pub struct BytesRef<'a> {
    guard: std::sync::RwLockReadGuard<'a, Block>,
    range: Range<usize>,
}

impl Deref for BytesRef<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.guard[self.range.clone()]
    }
}

/// Write guard for a native Go-style byte slice.
pub struct BytesMut<'a> {
    guard: std::sync::RwLockWriteGuard<'a, Block>,
    range: Range<usize>,
}

impl Deref for BytesMut<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.guard[self.range.clone()]
    }
}

impl DerefMut for BytesMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard[self.range.clone()]
    }
}

/// Go `Buffer`: an arena that draws fixed-size blocks from a [`Pool`].
pub struct Buffer {
    pool: Arc<Pool>,
    blocks: Vec<Arc<RwLock<Block>>>,
    block_cnt_limit: isize,
    cur_block_idx: isize,
    cur_idx: isize,
    small_obj_overhead: isize,
    small_obj_overhead_cache: isize,
}

impl Buffer {
    fn new(pool: Arc<Pool>, options: impl IntoIterator<Item = BufferOption>) -> Self {
        let mut buffer = Self {
            pool,
            blocks: Vec::with_capacity(128),
            block_cnt_limit: -1,
            cur_block_idx: -1,
            cur_idx: 0,
            small_obj_overhead: 0,
            small_obj_overhead_cache: 0,
        };
        for option in options {
            let block_cnt_limit =
                get_block_cnt(option.limit, buffer.pool.block_size as u64) as isize;
            buffer.block_cnt_limit = block_cnt_limit;
            buffer.blocks = Vec::with_capacity(
                usize::try_from(block_cnt_limit).expect("negative buffer block limit"),
            );
        }
        buffer
    }

    /// Go `TotalSize`.
    pub fn total_size(&self) -> i64 {
        (self.blocks.len() as isize).wrapping_mul(self.pool.block_size) as i64
    }

    fn cur_block_len(&self) -> isize {
        if self.cur_block_idx < 0 {
            return 0;
        }
        self.blocks[self.cur_block_idx as usize]
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len() as isize
    }

    /// Go's private `recordSmallObjOverhead`.
    fn record_small_obj_overhead(&mut self, n: isize) {
        let Some(limiter) = self.pool.limiter.as_ref() else {
            return;
        };
        if n > self.small_obj_overhead_cache {
            limiter.acquire(SMALL_OBJ_OVERHEAD_BATCH);
            self.small_obj_overhead_cache = self
                .small_obj_overhead_cache
                .wrapping_add(SMALL_OBJ_OVERHEAD_BATCH);
            self.small_obj_overhead = self
                .small_obj_overhead
                .wrapping_add(SMALL_OBJ_OVERHEAD_BATCH);
        }
        self.small_obj_overhead_cache = self.small_obj_overhead_cache.wrapping_sub(n);
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
            self.cur_block_idx = 0;
            self.cur_idx = 0;
        }
    }

    /// Go `Destroy`: returns every block to the pool.
    pub fn destroy(&mut self) {
        if self.pool.limiter.is_some() {
            self.release_small_obj_overhead();
        }
        for block in std::mem::take(&mut self.blocks) {
            let block = Arc::try_unwrap(block)
                .unwrap_or_else(|_| panic!("membuf aliases must be released before Destroy"))
                .into_inner()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.pool.release_block(block);
        }
        self.cur_block_idx = -1;
        self.cur_idx = 0;
    }

    /// Go's private `switchToNextBlock`: reuse an already-held block.
    fn switch_to_next_block(&mut self) -> bool {
        let next = self.cur_block_idx.wrapping_add(1);
        if next < self.blocks.len() as isize {
            self.cur_block_idx = next;
            self.cur_idx = 0;
            return true;
        }
        false
    }

    fn append_block(&mut self, block: Block) {
        self.blocks.push(Arc::new(RwLock::new(block)));
        self.cur_block_idx = self.blocks.len() as isize - 1;
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
        self.block_cnt_limit >= 0 && self.cur_block_idx.wrapping_add(1) >= self.block_cnt_limit
    }

    /// Go's private `allocBytesWithSliceLocation`.
    fn alloc_bytes_with_location(&mut self, n: isize) -> (Option<Bytes>, SliceLocation) {
        if n > self.pool.block_size {
            return (None, SliceLocation::default());
        }
        if self.cur_idx.wrapping_add(n) > self.cur_block_len() {
            if self.block_limit_reached() {
                return (None, SliceLocation::default());
            }
            self.add_block();
        }
        let location = SliceLocation {
            buf_idx: self.cur_block_idx as i32,
            offset: self.cur_idx as i32,
            length: n as i32,
        };
        self.cur_idx = self.cur_idx.wrapping_add(n);
        let bytes = if self.cur_block_idx < 0 {
            if n == 0 {
                None
            } else {
                panic!("slice bounds out of range")
            }
        } else {
            Some(self.get_slice(&location))
        };
        (bytes, location)
    }

    /// Go `AllocBytes`. A request larger than one block is served on its own,
    /// outside the arena and outside the limiter.
    pub fn alloc_bytes(&mut self, n: isize) -> Option<Bytes> {
        if n > self.pool.block_size {
            return Some(Bytes::standalone(
                usize::try_from(n).expect("negative allocation size"),
            ));
        }
        let (bytes, _) = self.alloc_bytes_with_location(n);
        if bytes.is_some() && self.pool.limiter.is_some() {
            self.record_small_obj_overhead(SIZE_OF_SLICE);
        }
        bytes
    }

    /// Go `AllocBytesWithSliceLocation`: always arena-backed, so an
    /// over-block request simply fails.
    pub fn alloc_bytes_with_slice_location(&mut self, n: isize) -> (Option<Bytes>, SliceLocation) {
        let (bytes, location) = self.alloc_bytes_with_location(n);
        if bytes.is_some() && self.pool.limiter.is_some() {
            self.record_small_obj_overhead(SIZE_OF_SLICE_LOCATION);
        }
        (bytes, location)
    }

    /// Go `TryAllocBytes`: never blocks on the limiter.
    ///
    /// On failure the buffer is unchanged, which is why the quota for both a
    /// new block and the bookkeeping batch is reserved in one attempt before
    /// anything is mutated.
    pub fn try_alloc_bytes(&mut self, n: isize) -> Result<Option<Bytes>, CannotAcquireMemory> {
        if n > self.pool.block_size {
            return Ok(Some(Bytes::standalone(
                usize::try_from(n).expect("negative allocation size"),
            )));
        }

        let need_block = self.cur_idx.wrapping_add(n) > self.cur_block_len();
        if need_block && self.block_limit_reached() {
            return Ok(None);
        }

        if let Some(limiter) = self.pool.limiter.clone() {
            let mut need_bytes: isize = 0;
            if need_block && self.cur_block_idx >= self.blocks.len() as isize - 1 {
                need_bytes = need_bytes.wrapping_add(self.pool.block_size);
            }
            let needs_overhead_batch = SIZE_OF_SLICE > self.small_obj_overhead_cache;
            if needs_overhead_batch {
                need_bytes = need_bytes.wrapping_add(SMALL_OBJ_OVERHEAD_BATCH);
            }
            if need_bytes > 0 && !limiter.try_acquire(need_bytes) {
                return Err(CannotAcquireMemory);
            }

            if need_block {
                self.add_block_with_reserved_quota();
            }
            if needs_overhead_batch {
                self.small_obj_overhead_cache = self
                    .small_obj_overhead_cache
                    .wrapping_add(SMALL_OBJ_OVERHEAD_BATCH);
                self.small_obj_overhead = self
                    .small_obj_overhead
                    .wrapping_add(SMALL_OBJ_OVERHEAD_BATCH);
            }
            self.small_obj_overhead_cache =
                self.small_obj_overhead_cache.wrapping_sub(SIZE_OF_SLICE);
        } else if need_block {
            self.add_block();
        }

        let location = SliceLocation {
            buf_idx: self.cur_block_idx as i32,
            offset: self.cur_idx as i32,
            length: n as i32,
        };
        self.cur_idx = self.cur_idx.wrapping_add(n);
        if self.cur_block_idx < 0 {
            if n == 0 {
                Ok(None)
            } else {
                panic!("slice bounds out of range")
            }
        } else {
            Ok(Some(self.get_slice(&location)))
        }
    }

    /// Go `GetSlice`.
    pub fn get_slice(&self, location: &SliceLocation) -> Bytes {
        let start = usize::try_from(location.offset).expect("negative slice offset");
        let end = usize::try_from(location.offset.wrapping_add(location.length))
            .expect("negative slice end");
        let block = Arc::clone(&self.blocks[location.buf_idx as usize]);
        let length = block
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())[start..end]
            .len();
        Bytes::pooled(block, start, length)
    }

    /// Go `AddBytes`.
    pub fn add_bytes(&mut self, bytes: &[u8]) -> Option<Bytes> {
        let output = self.alloc_bytes(bytes.len() as isize)?;
        output.as_mut_slice().copy_from_slice(bytes);
        Some(output)
    }

    /// Go `TryAddBytes`.
    pub fn try_add_bytes(&mut self, bytes: &[u8]) -> Result<Option<Bytes>, CannotAcquireMemory> {
        let Some(output) = self.try_alloc_bytes(bytes.len() as isize)? else {
            return Ok(None);
        };
        output.as_mut_slice().copy_from_slice(bytes);
        Ok(Some(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    #[derive(Default)]
    struct TestAllocator {
        allocs: AtomicUsize,
        frees: AtomicUsize,
    }

    impl Allocator for TestAllocator {
        fn alloc(&self, n: isize) -> Block {
            self.allocs.fetch_add(1, Ordering::SeqCst);
            Block::from_vec(vec![
                0;
                usize::try_from(n).expect("negative allocation size")
            ])
        }

        fn free(&self, _block: Block) {
            self.frees.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn bytes(bytes: &Bytes) -> BytesRef<'_> {
        bytes.as_slice()
    }

    fn bytes_mut(bytes: &Bytes) -> BytesMut<'_> {
        bytes.as_mut_slice()
    }

    #[test]
    fn buffer_pool() {
        let allocator = Arc::new(TestAllocator::default());
        let pool = Arc::new(Pool::new([
            with_block_num(2),
            with_allocator(Some(allocator.clone())),
            with_block_size(1024),
        ]));

        let mut buffer = pool.new_buffer([]);
        assert_eq!(buffer.alloc_bytes(256).unwrap().len(), 256);
        assert_eq!(allocator.allocs.load(Ordering::SeqCst), 1);
        assert_eq!(buffer.alloc_bytes(512).unwrap().len(), 512);
        assert_eq!(allocator.allocs.load(Ordering::SeqCst), 1);
        assert_eq!(buffer.alloc_bytes(257).unwrap().len(), 257);
        assert_eq!(allocator.allocs.load(Ordering::SeqCst), 2);
        assert_eq!(buffer.alloc_bytes(767).unwrap().len(), 767);
        assert_eq!(allocator.allocs.load(Ordering::SeqCst), 2);

        assert_eq!(buffer.alloc_bytes(1025).unwrap().len(), 1025);
        assert_eq!(allocator.allocs.load(Ordering::SeqCst), 2);

        assert_eq!(allocator.frees.load(Ordering::SeqCst), 0);
        buffer.destroy();
        assert_eq!(allocator.frees.load(Ordering::SeqCst), 0);

        let mut buffer = pool.new_buffer([]);
        for _ in 0..6 {
            let _ = buffer.alloc_bytes(512);
        }
        buffer.destroy();
        assert_eq!(allocator.allocs.load(Ordering::SeqCst), 3);
        assert_eq!(allocator.frees.load(Ordering::SeqCst), 1);
        pool.destroy();
    }

    #[test]
    fn pool_mem_limit() {
        let limiter = Arc::new(Limiter::new(
            (2 * 1024 * 1024 + 2 * SMALL_OBJ_OVERHEAD_BATCH) as isize,
        ));
        let pool = Arc::new(Pool::new([
            with_block_size(2 * 1024 * 1024),
            with_pool_memory_limiter(Some(limiter)),
        ]));
        let mut buffer = pool.new_buffer([]);
        let _ = buffer.alloc_bytes(1024 * 1024);
        let _ = buffer.alloc_bytes(1024 * 1024);

        let (done_tx, done_rx) = mpsc::sync_channel(1);
        let thread_pool = Arc::clone(&pool);
        let waiter = std::thread::spawn(move || {
            let mut second = thread_pool.new_buffer([]);
            let _ = second.alloc_bytes(1024 * 1024);
            second.destroy();
            done_tx.send(()).unwrap();
        });

        std::thread::sleep(Duration::from_millis(50));
        assert!(done_rx.try_recv().is_err());
        buffer.reset();
        let _ = buffer.alloc_bytes(1024 * 1024);
        let _ = buffer.alloc_bytes(1024 * 1024);
        assert!(done_rx.try_recv().is_err());
        buffer.destroy();
        done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        waiter.join().unwrap();

        let _ = buffer.alloc_bytes(2 * 1024 * 1024);
        buffer.destroy();
        pool.destroy();

        let limiter = Arc::new(Limiter::new((1024 + SMALL_OBJ_OVERHEAD_BATCH) as isize));
        let pool = Arc::new(Pool::new([
            with_block_num(0),
            with_block_size(1024),
            with_pool_memory_limiter(Some(limiter)),
        ]));
        let mut first = pool.new_buffer([]);
        assert!(first.try_add_bytes(b"a").unwrap().is_some());
        let mut second = pool.new_buffer([]);
        assert!(matches!(
            second.try_add_bytes(b"b"),
            Err(CannotAcquireMemory)
        ));
        first.destroy();
        assert!(second.try_add_bytes(b"b").unwrap().is_some());
        second.destroy();
        pool.destroy();

        let limiter = Arc::new(Limiter::new(1024));
        let pool = Arc::new(Pool::new([
            with_block_num(0),
            with_block_size(1024),
            with_pool_memory_limiter(Some(Arc::clone(&limiter))),
        ]));
        let mut buffer = pool.new_buffer([]);
        assert!(matches!(
            buffer.try_alloc_bytes(1),
            Err(CannotAcquireMemory)
        ));
        assert_eq!(limiter.limit(), 1024);
        assert!(buffer.blocks.is_empty());
        assert_eq!(buffer.cur_block_idx, -1);
        assert_eq!(buffer.cur_idx, 0);
        assert_eq!(buffer.small_obj_overhead, 0);
        assert_eq!(buffer.small_obj_overhead_cache, 0);
        pool.destroy();
    }

    #[test]
    fn buffer_isolation() {
        let pool = Arc::new(Pool::new([with_block_size(1024)]));
        let mut buffer = pool.new_buffer([]);

        let first = buffer.alloc_bytes(16).unwrap();
        let second = buffer.alloc_bytes(16).unwrap();
        assert_eq!(first.len(), first.capacity());
        assert_eq!(second.len(), second.capacity());

        getrandom::fill(&mut bytes_mut(&second)).unwrap();
        let original_second = bytes(&second).to_vec();
        let mut appended_first = bytes(&first).to_vec();
        appended_first.extend_from_slice(&[0, 1, 2, 3]);
        assert_eq!(original_second.as_slice(), &*bytes(&second));
        assert_ne!(&*bytes(&second), appended_first.as_slice());
        drop(first);
        drop(second);
        buffer.destroy();
        pool.destroy();
    }

    #[test]
    fn buffer_mem_limit() {
        let pool = Arc::new(Pool::new([with_block_size(10)]));
        let mut buffer = pool.new_buffer([with_buffer_memory_limit(5)]);

        assert!(buffer.alloc_bytes_with_slice_location(9).0.is_some());
        assert!(buffer.alloc_bytes_with_slice_location(3).0.is_none());

        buffer.destroy();
        assert!(buffer.alloc_bytes_with_slice_location(3).0.is_some());

        let mut buffer = pool.new_buffer([with_buffer_memory_limit(20)]);
        assert!(buffer.alloc_bytes_with_slice_location(9).0.is_some());
        assert!(buffer.alloc_bytes_with_slice_location(9).0.is_some());
        assert!(buffer.alloc_bytes_with_slice_location(2).0.is_none());

        buffer.reset();
        assert!(buffer.alloc_bytes_with_slice_location(9).0.is_some());
        assert!(buffer.alloc_bytes_with_slice_location(9).0.is_some());
        assert!(buffer.alloc_bytes_with_slice_location(2).0.is_none());
        buffer.destroy();
        pool.destroy();
    }

    #[test]
    fn get_aligned_size_get_block_count() {
        assert_eq!(get_block_cnt(10, 16), 1);
        assert_eq!(get_block_cnt(17, 16), 2);
        assert_eq!(get_aligned_size(10, 16), 16);
        assert_eq!(get_aligned_size(17, 16), 32);
    }

    #[test]
    fn limiter() {
        let limit = 20;
        let current = Arc::new(AtomicIsize::new(0));
        let limiter = Arc::new(Limiter::new(limit));

        std::thread::scope(|scope| {
            for _ in 0..100 {
                let current = Arc::clone(&current);
                let limiter = Arc::clone(&limiter);
                scope.spawn(move || {
                    limiter.acquire(1);
                    let value = current.fetch_add(1, Ordering::SeqCst) + 1;
                    assert!(value <= limit);
                    current.fetch_sub(1, Ordering::SeqCst);
                    limiter.release(1);
                });
            }
        });
        assert_eq!(limiter.limit(), limit);
    }

    #[test]
    fn wait_up_multiple_caller() {
        let limit = 20;
        let limiter = Arc::new(Limiter::new(limit));
        limiter.acquire(18);

        let (started_tx, started_rx) = mpsc::sync_channel(3);
        let (finished_tx, finished_rx) = mpsc::sync_channel(3);
        std::thread::scope(|scope| {
            for _ in 0..3 {
                let limiter = Arc::clone(&limiter);
                let started_tx = started_tx.clone();
                let finished_tx = finished_tx.clone();
                scope.spawn(move || {
                    started_tx.send(()).unwrap();
                    limiter.acquire(3);
                    finished_tx.send(()).unwrap();
                });
            }
            for _ in 0..3 {
                started_rx.recv().unwrap();
            }
            assert!(finished_rx.try_recv().is_err());
            limiter.release(18);
            for _ in 0..3 {
                finished_rx.recv().unwrap();
            }
        });
        assert_eq!(limiter.limit(), limit - 3 * 3);
    }
}
