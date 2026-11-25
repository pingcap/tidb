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

//! `pkg/util/chunk/alloc.go`: bounded chunk and column reuse.
//!
//! Go can retain a raw pointer to every allocated chunk and later reclaim its
//! fields from `Allocator.Reset`. Rust expresses the same ownership boundary
//! without a second mutable owner: [`AllocatedChunk`] owns the chunk, queues
//! only the allocation-time registry entries admitted by the source bounds,
//! and [`Allocator::reset`] moves those objects into the free lists. Callers
//! therefore end a chunk's lease before reset; live chunks cannot be
//! invalidated behind a safe Rust reference.

use crate::chunk::Chunk;
use crate::column::{get_fixed_len, Column, VAR_ELEM_LEN};
use crate::column_slot::ColumnSlot;
use parking_lot::Mutex as StateMutex;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once, PoisonError, Weak};
use tidb_datatype::FieldType;

const DEFAULT_MAX_FREE_CHUNKS: usize = 64;
const DEFAULT_MAX_FREE_COLUMNS_PER_TYPE: usize = 256;

static MAX_FREE_CHUNKS: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FREE_CHUNKS);
static MAX_FREE_COLUMNS_PER_TYPE: AtomicUsize = AtomicUsize::new(DEFAULT_MAX_FREE_COLUMNS_PER_TYPE);

/// Go `MaxCachedLen`: maximum variable-column data capacity admitted to the
/// allocator cache.
pub static MAX_CACHED_LEN: AtomicUsize = AtomicUsize::new(16 * 1024);

/// Go `InitChunkAllocSize`.
///
/// Values are clamped to `math.MaxInt32` before a newly-created allocator
/// snapshots them, matching the source's `uint32 -> int` policy.
pub fn init_chunk_alloc_size(max_free_chunks: u32, max_free_columns: u32) {
    let clamp = |value: u32| usize::try_from(value.min(i32::MAX as u32)).expect("u32 fits usize");
    MAX_FREE_CHUNKS.store(clamp(max_free_chunks), Ordering::Relaxed);
    MAX_FREE_COLUMNS_PER_TYPE.store(clamp(max_free_columns), Ordering::Relaxed);
}

/// Go `ColumnAllocator`.
pub trait ColumnAllocator {
    /// Go `ColumnAllocator.NewColumn`.
    fn new_column(&mut self, field_type: &FieldType, count: usize) -> Column;
}

/// Go `DefaultColumnAllocator`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultColumnAllocator;

impl ColumnAllocator for DefaultColumnAllocator {
    fn new_column(&mut self, field_type: &FieldType, count: usize) -> Column {
        Column::new_column(field_type, count)
    }
}

#[derive(Debug)]
struct RecycledColumn {
    column: Box<Column>,
    recycler: Weak<StateMutex<AllocatorState>>,
}

#[derive(Debug, Default)]
struct ColumnList {
    free: Vec<Option<RecycledColumn>>,
    /// Indexed by registration order. Returning a column fills its slot.
    pending: Vec<Option<RecycledColumn>>,
}

impl ColumnList {
    fn take_recycled(&mut self, count: usize) -> Option<(RecycledColumn, usize)> {
        let cached = self.free.pop().flatten()?;
        // Go compares against count, not count * type_size.
        let capacity = cached.column.get_data_cap();
        (capacity >= count).then_some((cached, capacity))
    }

    fn reserve_registration(
        &mut self,
        data_capacity: usize,
        limit: usize,
        max_cached_len: usize,
    ) -> Option<usize> {
        // Go tests the capacity when registering. Rejected columns leave room
        // for smaller allocations later in the same generation.
        if self.pending.len() >= limit || data_capacity >= max_cached_len {
            return None;
        }
        let sequence = self.pending.len();
        self.pending.push(None);
        Some(sequence)
    }

    fn reset(&mut self, type_size: i64, limit: usize, max_cached_len: usize) {
        if self.pending.is_empty() {
            return;
        }

        let mut remaining = limit.saturating_sub(self.free.len());
        self.pending.retain_mut(|slot| {
            let Some(cached) = slot else {
                return false;
            };
            if remaining == 0 || !check_column_type(type_size, &cached.column, max_cached_len) {
                return false;
            }
            cached.column.reset();
            remaining -= 1;
            true
        });
        if self.free.is_empty() {
            std::mem::swap(&mut self.free, &mut self.pending);
        } else {
            self.free.append(&mut self.pending);
        }
    }
}

const COLUMN_WIDTHS: [i64; 5] = [VAR_ELEM_LEN, 4, 8, 16, 40];

fn column_bucket(type_size: i64) -> usize {
    match type_size {
        VAR_ELEM_LEN => 0,
        4 => 1,
        8 => 2,
        16 => 3,
        40 => 4,
        _ => unreachable!("allocator columns have a source physical width"),
    }
}

#[derive(Debug)]
struct PoolColumnAllocator {
    pool: [ColumnList; COLUMN_WIDTHS.len()],
    free_columns_per_type: usize,
}

impl PoolColumnAllocator {
    fn new(free_columns_per_type: usize) -> Self {
        Self {
            pool: std::array::from_fn(|_| ColumnList::default()),
            free_columns_per_type,
        }
    }

    fn reset(&mut self) {
        let max_cached_len = MAX_CACHED_LEN.load(Ordering::Relaxed);
        for (list, type_size) in self.pool.iter_mut().zip(COLUMN_WIDTHS) {
            list.reset(type_size, self.free_columns_per_type, max_cached_len);
        }
    }

    #[cfg(test)]
    fn cached(&self, type_size: i64) -> usize {
        self.pool[column_bucket(type_size)].free.len()
    }
}

impl ColumnAllocator for PoolColumnAllocator {
    fn new_column(&mut self, field_type: &FieldType, count: usize) -> Column {
        let type_size = get_fixed_len(field_type);
        self.pool[column_bucket(type_size)].take_recycled(count).map_or_else(
            || Column::new_column_with_type_size(type_size, count),
            |(cached, _)| *cached.column,
        )
    }
}

fn check_column_type(expected_type_size: i64, column: &Column, max_cached_len: usize) -> bool {
    if column.avoid_reusing {
        return false;
    }
    if expected_type_size == VAR_ELEM_LEN {
        return !column.is_fixed() && column.get_data_cap() <= max_cached_len;
    }
    column.is_fixed() && expected_type_size == column.elem_buffer_capacity() as i64
}

#[derive(Debug)]
struct AllocatorState {
    pending_chunks: Vec<Chunk>,
    free_chunks: Vec<Chunk>,
    columns: PoolColumnAllocator,
    free_chunk_limit: usize,
    registered_chunks: usize,
    generation: u64,
}

/// Recycling provenance carried by one allocation-time admitted column owner.
/// It moves with lazy whole-column aliases and enqueues the raw column exactly
/// once when that owner finally dies.
pub(crate) struct ColumnRecycleRegistration {
    bucket: usize,
    /// Go's `allocColumns` index: this column's registration order in its
    /// type's list, which `Reset` walks to rebuild the free list.
    sequence: usize,
    generation: u64,
    recycler: Weak<StateMutex<AllocatorState>>,
}

impl ColumnRecycleRegistration {
    fn new(
        bucket: usize,
        sequence: usize,
        generation: u64,
        recycler: Weak<StateMutex<AllocatorState>>,
    ) -> Self {
        Self {
            bucket,
            sequence,
            generation,
            recycler,
        }
    }

    pub(crate) fn recycle(self, column: Box<Column>) {
        let Some(recycler) = self.recycler.upgrade() else {
            return;
        };
        self.recycle_into(column, &mut recycler.lock());
    }

    fn recycle_into(self, column: Box<Column>, state: &mut AllocatorState) {
        if self.generation == state.generation {
            let slot = &mut state.columns.pool[self.bucket].pending[self.sequence];
            debug_assert!(slot.is_none());
            *slot = Some(RecycledColumn {
                column,
                recycler: self.recycler,
            });
        }
    }
}

impl std::fmt::Debug for ColumnRecycleRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ColumnRecycleRegistration")
            .field("expected_type_size", &COLUMN_WIDTHS[self.bucket])
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl AllocatorState {
    fn recycle_owned_columns(
        &mut self,
        columns: &mut [ColumnSlot],
        recycler: &Arc<StateMutex<AllocatorState>>,
    ) -> bool {
        let owner = Arc::as_ptr(recycler);
        let mut drained = true;
        for slot in columns {
            if let Some((column, registration)) = slot.take_owned_registration(|registration| {
                std::ptr::eq(registration.recycler.as_ptr(), owner)
                    && registration.generation == self.generation
            }) {
                registration.recycle_into(column, self);
            } else {
                drained = false;
            }
        }
        drained
    }

    fn alloc_into(
        &mut self,
        chunk: &mut Chunk,
        fields: &[FieldType],
        capacity: usize,
        max_chunk_size: usize,
        recycler: &Arc<StateMutex<AllocatorState>>,
    ) -> (bool, u64) {
        let capacity = capacity.min(max_chunk_size);
        let cache_chunk = self.registered_chunks < self.free_chunk_limit;
        if cache_chunk {
            self.registered_chunks += 1;
        }
        chunk.prepare_allocator_shell(capacity, max_chunk_size);
        chunk.columns.reserve(fields.len());
        let column_limit = self.columns.free_columns_per_type;
        let max_cached_len = MAX_CACHED_LEN.load(Ordering::Relaxed);
        for field in fields {
            let type_size = get_fixed_len(field);
            let bucket = column_bucket(type_size);
            let list = &mut self.columns.pool[bucket];
            if let Some((RecycledColumn { column, recycler }, data_cap)) =
                list.take_recycled(capacity)
            {
                if let Some(sequence) =
                    list.reserve_registration(data_cap, column_limit, max_cached_len)
                {
                    let registration = ColumnRecycleRegistration::new(
                        bucket,
                        sequence,
                        self.generation,
                        recycler,
                    );
                    chunk.columns.push(ColumnSlot::with_registration(column, registration));
                } else {
                    chunk.columns.push(ColumnSlot::new(*column));
                }
            } else {
                let column = Column::new_column_with_type_size(type_size, capacity);
                if let Some(sequence) =
                    list.reserve_registration(column.get_data_cap(), column_limit, max_cached_len)
                {
                    let registration = ColumnRecycleRegistration::new(
                        bucket,
                        sequence,
                        self.generation,
                        Arc::downgrade(recycler),
                    );
                    chunk
                        .columns
                        .push(ColumnSlot::with_registration(Box::new(column), registration));
                } else {
                    chunk.columns.push(ColumnSlot::new(column));
                }
            }
        }
        (cache_chunk, self.generation)
    }

    fn reset(&mut self) {
        self.columns.reset();
        if self.free_chunks.is_empty() {
            std::mem::swap(&mut self.free_chunks, &mut self.pending_chunks);
        } else {
            self.free_chunks.append(&mut self.pending_chunks);
        }
        self.registered_chunks = 0;
        self.generation = self.generation.wrapping_add(1);
    }
}

/// Ordinary transferred chunks can still return their unique columns in one
/// operation. Other owners recycle individually after the lock is released.
pub(crate) fn recycle_registered_columns(columns: &mut [ColumnSlot]) {
    let [first, _, ..] = columns else {
        return;
    };
    let Some(registration) = first.recycle_registration() else {
        return;
    };
    let Some(recycler) = registration.recycler.upgrade() else {
        return;
    };
    recycler.lock().recycle_owned_columns(columns, &recycler);
}

/// An ownership-safe lease returned by [`Allocator::alloc`].
///
/// It dereferences to [`Chunk`]. Dropping it queues any allocation-time
/// registered shell/columns for the next allocator reset, which is the Rust
/// equivalent of ending all Go uses before calling `Allocator.Reset`.
pub struct AllocatedChunk {
    chunk: Option<Chunk>,
    cache_chunk: bool,
    generation: u64,
    recycler: Option<Arc<StateMutex<AllocatorState>>>,
}

impl AllocatedChunk {
    fn unpooled(chunk: Chunk) -> Self {
        Self {
            chunk: Some(chunk),
            cache_chunk: false,
            generation: 0,
            recycler: None,
        }
    }

    /// Go `AllocPool.Alloc`'s chunk as an executor holds it: an ordinary
    /// chunk whose columns stay the allocator's, recycled (until the next
    /// [`Allocator::reset`]) when the chunk drops, as Go's `Reset` reclaims
    /// every column it handed out. The chunk shell itself is not cached.
    #[must_use]
    pub fn into_pooled_chunk(mut self) -> Chunk {
        self.recycler = None;
        self.chunk.take().expect("allocated chunk is present")
    }

    /// Consume the lease without recycling it.
    #[must_use]
    pub fn into_chunk(mut self) -> Chunk {
        self.chunk
            .as_mut()
            .expect("allocated chunk is present")
            .detach_allocator_registrations();
        self.recycler = None;
        self.chunk.take().expect("allocated chunk is present")
    }
}

impl Deref for AllocatedChunk {
    type Target = Chunk;

    fn deref(&self) -> &Self::Target {
        self.chunk.as_ref().expect("allocated chunk is present")
    }
}

impl DerefMut for AllocatedChunk {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.chunk.as_mut().expect("allocated chunk is present")
    }
}

impl Drop for AllocatedChunk {
    fn drop(&mut self) {
        let (Some(recycler), Some(chunk)) = (&self.recycler, &mut self.chunk) else {
            return;
        };
        let mut state = recycler.lock();
        // A lease surviving Reset cannot be invalidated as Go's raw pointer
        // can. Discard it when it eventually drops so stale generations never
        // defeat the source cache bounds.
        if self.generation != state.generation {
            return;
        }
        // Unique columns belonging to this allocator can queue under the same
        // lock. Leave shared owners and registrations from other allocators
        // for the normal drop path after releasing it.
        let drained = state.recycle_owned_columns(&mut chunk.columns, recycler);
        if drained {
            // Only empty slots remain. Clearing them cannot recycle another
            // owner under the state lock.
            if self.cache_chunk {
                chunk.clear_columns_for_allocator();
                state
                    .pending_chunks
                    .push(self.chunk.take().expect("allocated chunk is present"));
            }
            return;
        }
        drop(state);
        // Owner drops can enqueue into `recycler`, so they MUST happen without
        // its state mutex held.
        chunk.clear_columns_for_allocator();
        if self.cache_chunk {
            let mut state = recycler.lock();
            if self.generation == state.generation {
                state
                    .pending_chunks
                    .push(self.chunk.take().expect("allocated chunk is present"));
            }
        }
    }
}

/// Go `chunk.Allocator` with an ownership-returning Rust allocation lease.
pub trait Allocator: Send + Sync {
    /// Go `Allocator.Alloc`.
    fn alloc(&self, fields: &[FieldType], capacity: usize, max_chunk_size: usize)
        -> AllocatedChunk;

    /// Go `Allocator.CheckReuseAllocSize`.
    fn check_reuse_alloc_size(&self) -> bool;

    /// Go `Allocator.Reset`.
    fn reset(&self);
}

/// Go `allocator`: bounded reusable chunk shells and columns.
#[derive(Clone, Debug)]
pub struct ChunkAllocator {
    state: Arc<StateMutex<AllocatorState>>,
    reuse_enabled: bool,
}

impl ChunkAllocator {
    /// Go `NewAllocator`.
    pub fn new() -> Self {
        let free_chunk_limit = MAX_FREE_CHUNKS.load(Ordering::Relaxed);
        let free_columns_per_type = MAX_FREE_COLUMNS_PER_TYPE.load(Ordering::Relaxed);
        Self {
            reuse_enabled: free_chunk_limit > 0 || free_columns_per_type > 0,
            state: Arc::new(StateMutex::new(AllocatorState {
                pending_chunks: Vec::new(),
                free_chunks: Vec::new(),
                columns: PoolColumnAllocator::new(free_columns_per_type),
                free_chunk_limit,
                registered_chunks: 0,
                generation: 0,
            })),
        }
    }

    #[cfg(test)]
    fn cached_chunks(&self) -> usize {
        self.state.lock().free_chunks.len()
    }

    #[cfg(test)]
    fn cached_columns(&self, type_size: i64) -> usize {
        self.state.lock().columns.cached(type_size)
    }

    #[cfg(test)]
    fn pending_objects(&self) -> usize {
        let state = self.state.lock();
        state.pending_chunks.len()
            + state
                .columns
                .pool
                .iter()
                .map(|list| list.pending.iter().flatten().count())
                .sum::<usize>()
    }
}

impl Default for ChunkAllocator {
    fn default() -> Self {
        Self::new()
    }
}

impl Allocator for ChunkAllocator {
    fn alloc(
        &self,
        fields: &[FieldType],
        capacity: usize,
        max_chunk_size: usize,
    ) -> AllocatedChunk {
        // The caller owns the chunk outside the lock, so an unwind releases
        // the state lock before dropping any already registered columns.
        let mut chunk;
        let (cache_chunk, generation) = {
            let mut state = self.state.lock();
            chunk = state.free_chunks.pop().unwrap_or_default();
            state.alloc_into(&mut chunk, fields, capacity, max_chunk_size, &self.state)
        };
        AllocatedChunk {
            chunk: Some(chunk),
            cache_chunk,
            generation,
            recycler: Some(Arc::clone(&self.state)),
        }
    }

    fn check_reuse_alloc_size(&self) -> bool {
        self.reuse_enabled
    }

    fn reset(&self) {
        self.state.lock().reset();
    }
}

/// Go `NewAllocator`.
pub fn new_allocator() -> ChunkAllocator {
    ChunkAllocator::new()
}

/// Go `syncAllocator`.
pub struct SyncAllocator {
    allocator: Mutex<Box<dyn Allocator>>,
}

impl SyncAllocator {
    /// Go `NewSyncAllocator`.
    pub fn new(allocator: impl Allocator + 'static) -> Self {
        Self {
            allocator: Mutex::new(Box::new(allocator)),
        }
    }
}

impl Allocator for SyncAllocator {
    fn alloc(
        &self,
        fields: &[FieldType],
        capacity: usize,
        max_chunk_size: usize,
    ) -> AllocatedChunk {
        self.allocator
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .alloc(fields, capacity, max_chunk_size)
    }

    fn check_reuse_alloc_size(&self) -> bool {
        self.allocator
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .check_reuse_alloc_size()
    }

    fn reset(&self) {
        self.allocator
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reset();
    }
}

/// Go `NewSyncAllocator`.
pub fn new_sync_allocator(allocator: impl Allocator + 'static) -> SyncAllocator {
    SyncAllocator::new(allocator)
}

/// Go `reuseHookAllocator`.
pub struct ReuseHookAllocator {
    hook_called: Once,
    hook: Box<dyn Fn() + Send + Sync>,
    allocator: Box<dyn Allocator>,
}

impl ReuseHookAllocator {
    /// Go `NewReuseHookAllocator`.
    pub fn new(
        allocator: impl Allocator + 'static,
        hook: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            hook_called: Once::new(),
            hook: Box::new(hook),
            allocator: Box::new(allocator),
        }
    }
}

impl Allocator for ReuseHookAllocator {
    fn alloc(
        &self,
        fields: &[FieldType],
        capacity: usize,
        max_chunk_size: usize,
    ) -> AllocatedChunk {
        if self.allocator.check_reuse_alloc_size() {
            // Go's sync.Once treats a panicking hook as completed. A later
            // caller clears the poison without running the hook again.
            self.hook_called.call_once_force(|state| {
                if !state.is_poisoned() {
                    (self.hook)();
                }
            });
        }
        self.allocator.alloc(fields, capacity, max_chunk_size)
    }

    fn check_reuse_alloc_size(&self) -> bool {
        self.allocator.check_reuse_alloc_size()
    }

    fn reset(&self) {
        self.allocator.reset();
    }
}

/// Go `NewReuseHookAllocator`.
pub fn new_reuse_hook_allocator(
    allocator: impl Allocator + 'static,
    hook: impl Fn() + Send + Sync + 'static,
) -> ReuseHookAllocator {
    ReuseHookAllocator::new(allocator, hook)
}

/// Go `emptyAllocator`: always constructs fresh chunks.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyAllocator;

impl Allocator for EmptyAllocator {
    fn alloc(
        &self,
        fields: &[FieldType],
        capacity: usize,
        max_chunk_size: usize,
    ) -> AllocatedChunk {
        AllocatedChunk::unpooled(Chunk::new(fields, capacity, max_chunk_size))
    }

    fn check_reuse_alloc_size(&self) -> bool {
        false
    }

    fn reset(&self) {}
}

/// Go `NewEmptyAllocator`.
pub const fn new_empty_allocator() -> EmptyAllocator {
    EmptyAllocator
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `allocator.Reset` rebuilds a type's free list in `allocColumns`
    /// order, so the column registered last is the first a later `Alloc`
    /// takes, whatever order its chunks were dropped in. Go probe
    /// (`chunk.NewAllocator`, one BIGINT column): A at capacity 1, B at 2, 100
    /// rows into A, `Reset`, three allocations at capacity 0 -- 137, 1032 and
    /// 120 bytes (B's column, A's, then a fresh one).
    #[test]
    fn reset_reuses_columns_in_go_registration_order() {
        let alloc = new_allocator();
        let fields = [FieldType::new(FieldTypeCode::LongLong)];
        let mut a = alloc.alloc(&fields, 1, 1024).into_pooled_chunk();
        let b = alloc.alloc(&fields, 2, 1024).into_pooled_chunk();
        for i in 0..100 {
            a.append_int64(0, i);
        }
        assert_eq!((a.memory_usage(), b.memory_usage()), (1032, 137));
        drop(b);
        drop(a);
        alloc.reset();
        let c = alloc.alloc(&fields, 0, 1024).into_pooled_chunk();
        let d = alloc.alloc(&fields, 0, 1024).into_pooled_chunk();
        let e = alloc.alloc(&fields, 0, 1024).into_pooled_chunk();
        assert_eq!((c.memory_usage(), d.memory_usage(), e.memory_usage()), (137, 1032, 120));
    }
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;
    use tidb_datatype::{EvalType, FieldTypeCode};

    static CONFIG_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn fields() -> Vec<FieldType> {
        vec![
            FieldType::new(FieldTypeCode::Varchar),
            FieldType::new(FieldTypeCode::Json),
            FieldType::new(FieldTypeCode::Float),
            FieldType::new(FieldTypeCode::NewDecimal),
            FieldType::new(FieldTypeCode::Double),
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::Timestamp),
            FieldType::new(FieldTypeCode::Datetime),
        ]
    }

    fn restore_defaults() {
        init_chunk_alloc_size(
            DEFAULT_MAX_FREE_CHUNKS as u32,
            DEFAULT_MAX_FREE_COLUMNS_PER_TYPE as u32,
        );
        MAX_CACHED_LEN.store(16 * 1024, Ordering::Relaxed);
    }

    #[test]
    fn allocator_configuration_does_not_eagerly_reserve_cache_limit() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(4096, 4096);

        let allocator = new_allocator();
        let (pending_capacity, free_capacity, chunk_limit, column_limit) = {
            let state = allocator.state.lock();
            (
                state.pending_chunks.capacity(),
                state.free_chunks.capacity(),
                state.free_chunk_limit,
                state.columns.free_columns_per_type,
            )
        };
        assert_eq!(chunk_limit, 4096);
        assert_eq!(column_limit, 4096);

        init_chunk_alloc_size(u32::MAX, u32::MAX);
        let clamped = new_allocator();
        let (clamped_chunk_limit, clamped_column_limit) = {
            let state = clamped.state.lock();
            (state.free_chunk_limit, state.columns.free_columns_per_type)
        };
        restore_defaults();

        assert_eq!(pending_capacity, 0);
        assert_eq!(free_capacity, 0);
        assert_eq!(clamped_chunk_limit, i32::MAX as usize);
        assert_eq!(clamped_column_limit, i32::MAX as usize);
    }

    /// Go `TestAllocator` and `TestColumnAllocator`.
    #[test]
    fn allocator_recycles_source_shapes_at_reset() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        restore_defaults();
        let mut default = DefaultColumnAllocator;
        for field in &fields() {
            assert_eq!(Column::new_column(field, 5), default.new_column(field, 5));
        }
        let allocator = new_allocator();
        {
            let chunk = allocator.alloc(&fields(), 5, 100);
            assert_eq!(chunk.capacity(), 5);
            assert_eq!(chunk.required_rows(), 100);
            assert_eq!(chunk.column(0).type_size(), VAR_ELEM_LEN);
            assert_eq!(chunk.column(2).type_size(), 4);
            assert_eq!(chunk.column(3).type_size(), 40);
            assert_eq!(chunk.column(4).type_size(), 8);
            assert_eq!(chunk.column(2).get_data_cap(), 20);
            assert_eq!(chunk.column(3).get_data_cap(), 200);
        }
        allocator.reset();
        assert_eq!(allocator.cached_chunks(), 1);
        assert_eq!(allocator.cached_columns(VAR_ELEM_LEN), 2);
        assert_eq!(allocator.cached_columns(4), 1);
        assert_eq!(allocator.cached_columns(8), 4);
        assert_eq!(allocator.cached_columns(40), 1);

        let reused = allocator.alloc(&fields(), 5, 100);
        assert_eq!(reused.num_cols(), fields().len());
        assert_eq!(reused.num_rows(), 0);
        restore_defaults();
    }

    /// Go cache bounds and the `InitChunkAllocSize` zero case.
    #[test]
    fn allocator_enforces_chunk_column_and_large_value_bounds() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(5, 3);
        MAX_CACHED_LEN.store(64, Ordering::Relaxed);
        let allocator = new_allocator();
        let varchar = vec![FieldType::new(FieldTypeCode::Varchar)];
        for index in 0..12 {
            let mut chunk = allocator.alloc(&varchar, 1, 1);
            if index == 0 {
                chunk.append_string(0, "x".repeat(128));
            }
        }
        allocator.reset();
        assert_eq!(allocator.cached_chunks(), 5);
        // The first three columns consume the allocation registry slots. The
        // first one is later rejected for being over-sized, exactly as Go's
        // two-stage `put`/`Reset` admission leaves two cached columns.
        assert_eq!(allocator.cached_columns(VAR_ELEM_LEN), 2);

        init_chunk_alloc_size(0, 0);
        let disabled = new_allocator();
        assert!(!disabled.check_reuse_alloc_size());
        restore_defaults();
    }

    #[test]
    fn allocator_bounds_pending_ownership_before_reset() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        let varchar = vec![FieldType::new(FieldTypeCode::Varchar)];

        init_chunk_alloc_size(2, 3);
        let bounded = new_allocator();
        for _ in 0..100 {
            drop(bounded.alloc(&varchar, 1, 1));
        }
        assert_eq!(bounded.pending_objects(), 5);
        bounded.reset();
        assert_eq!(bounded.cached_chunks(), 2);
        assert_eq!(bounded.cached_columns(VAR_ELEM_LEN), 3);

        init_chunk_alloc_size(0, 1);
        MAX_CACHED_LEN.store(64, Ordering::Relaxed);
        let admission = new_allocator();
        // An initially over-sized column is rejected by Go's allocation-time
        // `put` and therefore leaves the one slot available to this small one.
        drop(admission.alloc(&varchar, 8, 8));
        drop(admission.alloc(&varchar, 1, 1));
        assert_eq!(admission.pending_objects(), 1);
        admission.reset();
        assert_eq!(admission.cached_columns(VAR_ELEM_LEN), 1);

        init_chunk_alloc_size(0, 0);
        let disabled = new_allocator();
        for _ in 0..100 {
            drop(disabled.alloc(&varchar, 1, 1));
        }
        assert_eq!(disabled.pending_objects(), 0);
        restore_defaults();
    }

    /// Go `TestColumnAllocatorCheck` (`pkg/util/chunk/alloc_test.go:276`):
    /// with cache bounds (10, 20), four allocations of a (Float, Datetime)
    /// chunk and one reset leave exactly four cached columns per fixed type
    /// size -- Float's 4-byte pool and Datetime's 8-byte pool each hold the
    /// full count, proving per-type accounting rather than one shared pool.
    #[test]
    fn go_test_column_allocator_check() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(10, 20);
        let field_types = vec![
            FieldType::new(FieldTypeCode::Float),
            FieldType::new(FieldTypeCode::Datetime),
        ];
        let allocator = new_allocator();
        for _ in 0..4 {
            drop(allocator.alloc(&field_types, 5, 10));
        }
        allocator.reset();
        assert_eq!(allocator.cached_columns(4), 4); // Go getFixedLen(TypeFloat)
        assert_eq!(allocator.cached_columns(8), 4); // Go getFixedLen(TypeDatetime)
        restore_defaults();
    }

    /// Go issue #31981 and `TestColumnAllocatorCheck`.
    #[test]
    fn allocator_rejects_borrowed_and_retyped_columns() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(10, 20);
        let allocator = new_allocator();

        let float = vec![FieldType::new(FieldTypeCode::Float)];
        let mut borrowed = allocator.alloc(&float, 5, 10);
        borrowed.column_mut(0).avoid_reusing = true;
        drop(borrowed);

        let mut retyped = allocator.alloc(&float, 5, 10);
        retyped
            .column_mut(0)
            .reset_for_eval_type(EvalType::Datetime);
        drop(retyped);
        allocator.reset();
        assert_eq!(allocator.cached_columns(4), 0);
        restore_defaults();
    }

    /// Go `TestNoDuplicateColumnReuse` plus the displaced-owner failure class:
    /// aliasing one slot must neither enqueue one identity twice nor lose the
    /// owner that the destination slot previously held.
    #[test]
    fn allocator_aliases_recycle_each_original_owner_once() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(8, 32);
        let allocator = new_allocator();
        {
            let mut chunk = allocator.alloc(&fields(), 5, 10);
            chunk.make_ref(1, 3);
            assert!(chunk.columns_share_identity(1, &chunk, 3));
        }
        allocator.reset();

        // Every allocation-time owner is present exactly once. In particular,
        // index 3's displaced 40-byte decimal owner was not lost, and index 1's
        // aliased variable owner was not registered twice.
        assert_eq!(allocator.cached_columns(VAR_ELEM_LEN), 2);
        assert_eq!(allocator.cached_columns(4), 1);
        assert_eq!(allocator.cached_columns(8), 4);
        assert_eq!(allocator.cached_columns(40), 1);

        let reused = allocator.alloc(&fields(), 5, 10);
        for left in 0..reused.num_cols() {
            for right in left + 1..reused.num_cols() {
                assert!(
                    !reused.columns_share_identity(left, &reused, right),
                    "allocator published one owner in slots {left} and {right}"
                );
            }
        }
        drop(reused);
        restore_defaults();
    }

    #[test]
    fn allocator_recycles_a_displaced_original_while_lease_remains_live() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(0, 2);
        let allocator = new_allocator();
        let int_fields = vec![
            FieldType::new(FieldTypeCode::LongLong),
            FieldType::new(FieldTypeCode::LongLong),
        ];
        let mut chunk = allocator.alloc(&int_fields, 1, 1);
        chunk.make_ref(0, 1);

        // Replacing slot 1 drops its independently registered owner. Its
        // provenance queues it even though the lease and aliased owner live.
        allocator.reset();
        assert_eq!(allocator.cached_columns(8), 1);
        assert!(chunk.columns_share_identity(0, &chunk, 1));
        drop(chunk);
        restore_defaults();
    }

    #[test]
    fn allocator_never_resets_a_live_cross_chunk_alias() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(1, 1);
        let allocator = new_allocator();
        let fields = vec![FieldType::new(FieldTypeCode::LongLong)];
        let mut source = allocator.alloc(&fields, 1, 1);
        source.append_int64(0, 17);
        let mut destination = Chunk::new_chunk_with_capacity(&fields, 1);
        destination
            .make_ref_to(0, &mut source, 0)
            .expect("neither chunk has a selection");

        drop(source);
        allocator.reset();
        assert_eq!(allocator.cached_columns(8), 0);
        assert_eq!(destination.column(0).get_int64(0), 17);
        destination
            .column_mut(0)
            .with_int64s_mut(|values| values[0] = 23);
        assert_eq!(destination.column(0).get_int64(0), 23);

        // The registration belongs to the pre-reset generation, so final owner
        // drop discards it instead of polluting the new generation's cache.
        drop(destination);
        allocator.reset();
        assert_eq!(allocator.cached_columns(8), 0);
        restore_defaults();
    }

    #[test]
    fn allocator_returns_swapped_columns_to_their_original_owners() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        restore_defaults();
        let fields = [FieldType::new(FieldTypeCode::LongLong)];
        let first = new_allocator();
        let second = new_allocator();
        let mut left = first.alloc(&fields, 4, 32);
        let mut right = second.alloc(&fields, 8, 32);
        left.swap_columns(&mut right);
        drop(left);
        drop(right);

        first.reset();
        second.reset();
        assert_eq!(first.cached_columns(8), 1);
        assert_eq!(second.cached_columns(8), 1);
        assert_eq!(first.alloc(&fields, 1, 32).column(0).get_data_cap(), 32);
        assert_eq!(second.alloc(&fields, 1, 32).column(0).get_data_cap(), 64);
    }

    #[test]
    fn transferred_chunk_returns_local_and_foreign_columns_but_keeps_live_aliases() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        restore_defaults();
        let field = FieldType::new(FieldTypeCode::LongLong);
        let fields = vec![field.clone(); 3];
        let first = new_allocator();
        let second = new_allocator();
        let mut left = first.alloc(&fields, 4, 32).into_pooled_chunk();
        let mut right = second.alloc(&fields, 8, 32).into_pooled_chunk();
        left.swap_column_with(1, &mut right, 1).unwrap();
        left.append_int64(2, 17);
        let mut alias = Chunk::new_chunk_with_capacity(&[field], 1);
        alias.make_ref_to(0, &mut left, 2).unwrap();
        drop(left);

        first.reset();
        second.reset();
        assert_eq!(first.cached_columns(8), 1);
        assert_eq!(second.cached_columns(8), 1);
        assert_eq!(alias.column(0).get_int64(0), 17);

        drop(right);
        drop(alias);
        first.reset();
        second.reset();
        assert_eq!(first.cached_columns(8), 1);
        assert_eq!(second.cached_columns(8), 1);
    }

    /// Go `TestReuseHookAllocator`.
    #[test]
    fn reuse_hook_runs_once_only_when_reuse_is_configured() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        let calls = Arc::new(AtomicUsize::new(0));
        init_chunk_alloc_size(0, 0);
        let disabled_calls = Arc::clone(&calls);
        let disabled = new_reuse_hook_allocator(new_allocator(), move || {
            disabled_calls.fetch_add(1, Ordering::Relaxed);
        });
        drop(disabled.alloc(&fields(), 5, 100));
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        init_chunk_alloc_size(10, 20);
        let enabled_calls = Arc::clone(&calls);
        let enabled = new_reuse_hook_allocator(new_allocator(), move || {
            enabled_calls.fetch_add(1, Ordering::Relaxed);
        });
        drop(enabled.alloc(&fields(), 5, 100));
        drop(enabled.alloc(&fields(), 5, 100));
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        restore_defaults();
    }

    #[test]
    fn reuse_hook_panic_is_marked_done_like_go_sync_once() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        init_chunk_alloc_size(1, 1);
        let allocator = Arc::new(new_reuse_hook_allocator(new_allocator(), || {
            panic!("hook panic");
        }));
        let first = Arc::clone(&allocator);
        assert!(
            std::thread::spawn(move || drop(first.alloc(&fields(), 1, 1)))
                .join()
                .is_err()
        );

        // Go's sync.Once is done after the panic. The next allocation reaches
        // the wrapped allocator instead of re-running/re-panicking the hook.
        let chunk = allocator.alloc(&fields(), 1, 1);
        assert_eq!(chunk.num_cols(), fields().len());
        restore_defaults();
    }

    #[derive(Clone)]
    struct ProbeAllocator {
        allocated: mpsc::Sender<()>,
    }

    impl Allocator for ProbeAllocator {
        fn alloc(
            &self,
            fields: &[FieldType],
            capacity: usize,
            max_chunk_size: usize,
        ) -> AllocatedChunk {
            self.allocated.send(()).expect("allocation observer");
            AllocatedChunk::unpooled(Chunk::new(fields, capacity, max_chunk_size))
        }

        fn check_reuse_alloc_size(&self) -> bool {
            true
        }

        fn reset(&self) {}
    }

    #[test]
    fn reuse_hook_blocks_concurrent_alloc_until_hook_returns() {
        let (hook_entered_tx, hook_entered_rx) = mpsc::channel();
        let release_hook = Arc::new(std::sync::Barrier::new(2));
        let hook_release = Arc::clone(&release_hook);
        let (allocated_tx, allocated_rx) = mpsc::channel();
        let allocator = Arc::new(new_reuse_hook_allocator(
            ProbeAllocator {
                allocated: allocated_tx,
            },
            move || {
                hook_entered_tx.send(()).expect("hook observer");
                hook_release.wait();
            },
        ));

        let first = Arc::clone(&allocator);
        let first_thread = std::thread::spawn(move || drop(first.alloc(&fields(), 1, 1)));
        hook_entered_rx.recv().expect("hook entered");

        let (second_started_tx, second_started_rx) = mpsc::channel();
        let second = Arc::clone(&allocator);
        let second_thread = std::thread::spawn(move || {
            second_started_tx.send(()).expect("second started");
            drop(second.alloc(&fields(), 1, 1));
        });
        second_started_rx.recv().expect("second started");
        let passed_wrapped_allocator_early =
            allocated_rx.recv_timeout(Duration::from_millis(50)).is_ok();

        release_hook.wait();
        let remaining_allocations = if passed_wrapped_allocator_early { 1 } else { 2 };
        for _ in 0..remaining_allocations {
            allocated_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("wrapped allocation after hook release");
        }
        first_thread.join().expect("first allocation thread");
        second_thread.join().expect("second allocation thread");
        assert!(!passed_wrapped_allocator_early);
    }

    /// Go `TestSyncAllocator` with enough contention to exercise every lock
    /// boundary without making the unit test needlessly expensive.
    #[test]
    fn sync_allocator_is_thread_safe() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        restore_defaults();
        let allocator = Arc::new(new_sync_allocator(new_allocator()));
        let fields = Arc::new(fields());
        let workers: Vec<_> = (0..32)
            .map(|_| {
                let allocator = Arc::clone(&allocator);
                let fields = Arc::clone(&fields);
                std::thread::spawn(move || {
                    for _ in 0..64 {
                        let chunk = allocator.alloc(&fields, 5, 100);
                        assert_eq!(chunk.num_cols(), fields.len());
                        drop(chunk);
                        allocator.reset();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().expect("allocator worker");
        }
        restore_defaults();
    }

    #[test]
    fn empty_allocator_never_reuses() {
        let allocator = new_empty_allocator();
        assert!(!allocator.check_reuse_alloc_size());
        let chunk = allocator.alloc(&fields(), 9, 17);
        assert_eq!(chunk.capacity(), 9);
        assert_eq!(chunk.required_rows(), 17);
        allocator.reset();
    }
    /// Go `TestAvoidColumnReuse` (`pkg/util/chunk/alloc_test.go`, issue
    /// #31981): columns flagged `avoidReusing` never enter the pool, and the
    /// decoder flags every column it hands back.
    #[test]
    fn go_test_avoid_column_reuse() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        restore_defaults();
        let allocator = new_allocator();
        for _ in 0..(DEFAULT_MAX_FREE_CHUNKS + 10) {
            let mut chunk = allocator.alloc(&fields(), 5, 10);
            for index in 0..chunk.num_cols() {
                chunk.column_mut(index).avoid_reusing = true;
            }
        }
        allocator.reset();

        // No column entered the pool.
        for type_size in [VAR_ELEM_LEN, 4, 8, 40] {
            assert_eq!(allocator.cached_columns(type_size), 0);
        }

        // The decoder sets the avoid-reusing flag on the columns it fills.
        let mut chk = allocator.alloc(&fields(), 5, 1024);
        for _ in 0..10 {
            for index in 0..chk.num_cols() {
                chk.append_null(index);
            }
        }
        let codec = crate::codec::Codec::new(fields());
        let buf = codec.encode(&chk);

        let mut decoder =
            crate::codec::Decoder::new(Chunk::new_chunk_with_capacity(&fields(), 0), fields());
        decoder.reset(&buf);
        decoder.reuse_interm_chk(&mut chk);
        for index in 0..chk.num_cols() {
            assert!(chk.column(index).avoid_reusing);
        }
        restore_defaults();
    }

    /// Go `TestColumnAllocatorLimit`: `InitChunkAllocSize` raises, lowers and
    /// disables both free lists; over-sized variable columns are not cached.
    #[test]
    fn go_test_column_allocator_limit() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        let varchar_fields = || vec![FieldType::new(FieldTypeCode::Varchar)];
        let field_types = fields();

        init_chunk_alloc_size(10, 20);
        let mut allocator = new_allocator();
        assert!(allocator.check_reuse_alloc_size());
        for _ in 0..(DEFAULT_MAX_FREE_CHUNKS + 10) {
            drop(allocator.alloc(&field_types, 5, 10));
        }
        allocator.reset();
        assert_eq!(allocator.cached_chunks(), 10);
        for type_size in [VAR_ELEM_LEN, 4, 8, 40] {
            assert!(allocator.cached_columns(type_size) <= 20);
        }

        // Reduce capacity.
        init_chunk_alloc_size(5, 10);
        allocator = new_allocator();
        for _ in 0..(DEFAULT_MAX_FREE_CHUNKS + 10) {
            drop(allocator.alloc(&field_types, 5, 10));
        }
        allocator.reset();
        assert_eq!(allocator.cached_chunks(), 5);
        for type_size in [VAR_ELEM_LEN, 4, 8, 40] {
            assert!(allocator.cached_columns(type_size) <= 10);
        }

        // Increase capacity.
        init_chunk_alloc_size(50, 100);
        allocator = new_allocator();
        for _ in 0..(DEFAULT_MAX_FREE_CHUNKS + 10) {
            drop(allocator.alloc(&field_types, 5, 10));
        }
        allocator.reset();
        assert_eq!(allocator.cached_chunks(), 50);
        for type_size in [VAR_ELEM_LEN, 4, 8, 40] {
            assert!(allocator.cached_columns(type_size) <= 100);
        }

        // Long characters are not cached: every COLUMN pool ends up empty.
        // Go keeps the chunk SHELL in `alloc.free` regardless -- only
        // `columnAlloc.put` refuses over-sized variable columns.
        allocator = new_allocator();
        // Go observes the column entering the allocator's registry at
        // allocation time; this port admits it when the lease drops.
        let mut rs = allocator.alloc(&varchar_fields(), 1024, 1024);
        rs.column_mut(0)
            .data
            .extend_from_slice(&vec![b'a'; 20_480][..]);
        drop(rs);
        allocator.reset();
        for type_size in [VAR_ELEM_LEN, 4, 8, 40] {
            assert_eq!(
                allocator.cached_columns(type_size),
                0,
                "type size {type_size}"
            );
        }
        assert_eq!(allocator.cached_chunks(), 1);

        init_chunk_alloc_size(0, 0);
        let disabled = new_allocator();
        assert!(!disabled.check_reuse_alloc_size());
        restore_defaults();
    }

    /// Go `TestAllocator` (`pkg/util/chunk/alloc_test.go:27`): the pooled
    /// allocator hands back chunks whose fixed-length columns carry an
    /// `elemBuf` and `initCap`-sized data capacity while variable-length ones
    /// carry neither; recycling keeps that shape, and the CHUNK free list is
    /// capped at `maxFreeChunks` no matter how many chunks were dropped.
    #[test]
    fn go_test_allocator() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        restore_defaults();
        let allocator = new_allocator();

        let field_types = fields();
        let init_cap = 5;
        let max_chunk_size = 100;

        let check = |chunk: &Chunk| {
            assert_eq!(field_types.len(), chunk.num_cols());
            // Variable-length columns (varchar, json) have no elemBuf.
            assert!(chunk.column(0).elem_buf.is_none());
            assert!(chunk.column(1).elem_buf.is_none());
            for index in 2..8 {
                let elem_len = chunk.column(index).elem_buf.as_ref().map_or(0, Vec::len);
                assert_eq!(
                    chunk.column(index).type_size() as usize,
                    elem_len,
                    "fixed-length column {index} stores its elem size"
                );
            }
            for index in 2..8 {
                assert_eq!(
                    chunk.column(index).get_data_cap(),
                    init_cap * chunk.column(index).type_size() as usize
                );
            }
        };

        let chunk = allocator.alloc(&field_types, init_cap, max_chunk_size);
        check(&chunk);

        // Call Reset and alloc again, check the result.
        allocator.reset();
        let chunk = allocator.alloc(&field_types, init_cap, max_chunk_size);
        check(&chunk);

        // Check the chunk free-list cap: dropping far more chunks than the
        // limit still caches only `maxFreeChunks` of them.
        drop(chunk);
        for _ in 0..(DEFAULT_MAX_FREE_CHUNKS + 10) {
            drop(allocator.alloc(&field_types, init_cap, max_chunk_size));
        }
        allocator.reset();
        assert_eq!(allocator.cached_chunks(), DEFAULT_MAX_FREE_CHUNKS);
        restore_defaults();
    }

    /// Go `TestNoDuplicateColumnReuse` (`pkg/util/chunk/alloc_test.go:122`,
    /// issue #29554): a chunk whose columns ALIAS each other (`MakeRef`) must
    /// not enqueue the same owner twice on reset -- otherwise two pool slots
    /// would hand out columns sharing one set of buffers.
    #[test]
    fn go_test_no_duplicate_column_reuse() {
        let _guard = CONFIG_TEST_LOCK.lock().expect("config test lock");
        restore_defaults();
        let allocator = new_allocator();
        for _ in 0..(DEFAULT_MAX_FREE_CHUNKS + 10) {
            let mut chunk = allocator.alloc(&fields(), 5, 10);
            chunk.make_ref(1, 3);
        }
        allocator.reset();

        // Drain every per-type free list and make sure no two pooled columns
        // designate the same buffer storage. Go compares `*Column` pointers;
        // this port compares the shared-bytes identity of each owner.
        {
            let state = allocator.state.lock();
            for list in &state.columns.pool {
                let identities: Vec<*const u8> = list
                    .free
                    .iter()
                    .map(|cached| {
                        let column = &cached.as_ref().expect("free slots are populated").column;
                        // The deref through `SharedBytesRead` yields the
                        // owner's live buffer, so its address identifies the
                        // storage exactly as Go's `*Column` does.
                        let bytes: &[u8] = &column.data.read();
                        bytes.as_ptr()
                    })
                    .collect();
                let unique: std::collections::HashSet<*const u8> =
                    identities.iter().copied().collect();
                assert_eq!(
                    identities.len(),
                    unique.len(),
                    "the same column owner entered one free list twice"
                );
            }
        }
        restore_defaults();
    }
}
