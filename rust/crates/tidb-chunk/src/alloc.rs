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
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
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

#[derive(Debug, Default)]
struct ColumnList {
    free: Vec<Column>,
}

impl ColumnList {
    fn pop(&mut self) -> Option<Column> {
        self.free.pop()
    }

    fn push(&mut self, column: Column, limit: usize) {
        if self.free.len() < limit {
            self.free.push(column);
        }
    }
}

#[derive(Debug)]
struct PoolColumnAllocator {
    pool: HashMap<i64, ColumnList>,
    free_columns_per_type: usize,
}

impl PoolColumnAllocator {
    fn new(free_columns_per_type: usize) -> Self {
        Self {
            pool: HashMap::new(),
            free_columns_per_type,
        }
    }

    fn new_size_column(&mut self, type_size: i64, count: usize) -> Column {
        if let Some(column) = self.pool.get_mut(&type_size).and_then(ColumnList::pop) {
            // This intentionally mirrors Go's comparison against `count`, not
            // `count*typeSize`.
            if column.data_capacity() >= count {
                return column;
            }
        }
        Column::new_column_with_type_size(type_size, count)
    }

    fn put_if_eligible(&mut self, expected_type_size: i64, mut column: Column) {
        if !check_column_type(expected_type_size, &column) {
            return;
        }
        if column.data_capacity() >= MAX_CACHED_LEN.load(Ordering::Relaxed) {
            // `poolColumnAllocator.put` uses `< MaxCachedLen`; `Reset` also
            // rejects variable columns with `> MaxCachedLen`. The stricter
            // insertion boundary wins at equality in the source.
            return;
        }
        column.reset();
        self.pool
            .entry(expected_type_size)
            .or_default()
            .push(column, self.free_columns_per_type);
    }

    #[cfg(test)]
    fn cached(&self, type_size: i64) -> usize {
        self.pool.get(&type_size).map_or(0, |list| list.free.len())
    }
}

impl ColumnAllocator for PoolColumnAllocator {
    fn new_column(&mut self, field_type: &FieldType, count: usize) -> Column {
        self.new_size_column(get_fixed_len(field_type), count)
    }
}

fn check_column_type(expected_type_size: i64, column: &Column) -> bool {
    if column.avoid_reusing {
        return false;
    }
    if expected_type_size == VAR_ELEM_LEN {
        return !column.is_fixed()
            && column.data_capacity() <= MAX_CACHED_LEN.load(Ordering::Relaxed);
    }
    column.is_fixed() && expected_type_size == column.elem_buffer_capacity() as i64
}

fn can_register_allocated_column(expected_type_size: i64, column: &Column) -> bool {
    !column.avoid_reusing
        && (expected_type_size > 0 || expected_type_size == VAR_ELEM_LEN)
        && column.data_capacity() < MAX_CACHED_LEN.load(Ordering::Relaxed)
}

#[derive(Debug)]
struct AllocatorState {
    pending_chunks: Vec<Chunk>,
    pending_columns: HashMap<i64, Vec<Column>>,
    free_chunks: Vec<Chunk>,
    columns: PoolColumnAllocator,
    free_chunk_limit: usize,
    registered_chunks: usize,
    registered_columns: HashMap<i64, usize>,
    generation: u64,
}

/// Recycling provenance carried by one allocation-time admitted column owner.
/// It moves with lazy whole-column aliases and enqueues the raw column exactly
/// once when that owner finally dies.
pub(crate) struct ColumnRecycleRegistration {
    expected_type_size: i64,
    generation: u64,
    recycler: Weak<Mutex<AllocatorState>>,
}

impl ColumnRecycleRegistration {
    fn new(
        expected_type_size: i64,
        generation: u64,
        recycler: Weak<Mutex<AllocatorState>>,
    ) -> Self {
        Self {
            expected_type_size,
            generation,
            recycler,
        }
    }

    pub(crate) fn recycle(self, column: Column) {
        let Some(recycler) = self.recycler.upgrade() else {
            return;
        };
        let mut state = lock_state(&recycler);
        if self.generation == state.generation {
            state
                .pending_columns
                .entry(self.expected_type_size)
                .or_default()
                .push(column);
        }
    }
}

impl std::fmt::Debug for ColumnRecycleRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ColumnRecycleRegistration")
            .field("expected_type_size", &self.expected_type_size)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}

impl AllocatorState {
    fn alloc(
        &mut self,
        fields: &[FieldType],
        capacity: usize,
        max_chunk_size: usize,
    ) -> (Chunk, bool, Vec<(i64, bool)>, u64) {
        let capacity = capacity.min(max_chunk_size);
        let cache_chunk = self.registered_chunks < self.free_chunk_limit;
        if cache_chunk {
            self.registered_chunks += 1;
        }
        let mut registrations = Vec::with_capacity(fields.len());
        let mut columns = Vec::with_capacity(fields.len());
        let column_limit = self.columns.free_columns_per_type;
        for field in fields {
            let type_size = get_fixed_len(field);
            let column = self.columns.new_column(field, capacity);
            let registered = self.registered_columns.entry(type_size).or_default();
            // Go registers a column at allocation time only when `put` admits
            // it. An over-sized column must not consume one of the bounded
            // registry slots that a later small column can use.
            let cache_column =
                *registered < column_limit && can_register_allocated_column(type_size, &column);
            if cache_column {
                *registered += 1;
            }
            registrations.push((type_size, cache_column));
            columns.push(column);
        }
        let chunk = if let Some(mut chunk) = self.free_chunks.pop() {
            chunk.restore_reusable_columns(columns, capacity, max_chunk_size);
            chunk
        } else {
            Chunk::from_reusable_columns(columns, capacity, max_chunk_size)
        };
        (chunk, cache_chunk, registrations, self.generation)
    }

    fn reset(&mut self) {
        for (expected_type_size, columns) in std::mem::take(&mut self.pending_columns) {
            for column in columns {
                self.columns.put_if_eligible(expected_type_size, column);
            }
        }
        self.free_chunks
            .extend(std::mem::take(&mut self.pending_chunks));
        self.registered_chunks = 0;
        self.registered_columns.clear();
        self.generation = self.generation.wrapping_add(1);
    }
}

fn lock_state(state: &Mutex<AllocatorState>) -> MutexGuard<'_, AllocatorState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
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
    recycler: Option<Arc<Mutex<AllocatorState>>>,
}

impl AllocatedChunk {
    fn pooled(
        mut chunk: Chunk,
        cache_chunk: bool,
        column_registrations: Vec<(i64, bool)>,
        generation: u64,
        recycler: Arc<Mutex<AllocatorState>>,
    ) -> Self {
        let weak_recycler = Arc::downgrade(&recycler);
        chunk.attach_allocator_registrations(column_registrations.into_iter().map(
            |(expected_type_size, cache)| {
                cache.then(|| {
                    ColumnRecycleRegistration::new(
                        expected_type_size,
                        generation,
                        Weak::clone(&weak_recycler),
                    )
                })
            },
        ));
        Self {
            chunk: Some(chunk),
            cache_chunk,
            generation,
            recycler: Some(recycler),
        }
    }

    fn unpooled(chunk: Chunk) -> Self {
        Self {
            chunk: Some(chunk),
            cache_chunk: false,
            generation: 0,
            recycler: None,
        }
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
        let (Some(recycler), Some(mut chunk)) = (self.recycler.take(), self.chunk.take()) else {
            return;
        };
        let state = lock_state(&recycler);
        // A lease surviving Reset cannot be invalidated as Go's raw pointer
        // can. Discard it when it eventually drops so stale generations never
        // defeat the source cache bounds.
        if self.generation != state.generation {
            return;
        }
        drop(state);
        // Owner drops can enqueue into `recycler`, so they MUST happen without
        // its state mutex held.
        chunk.clear_columns_for_allocator();
        if self.cache_chunk {
            let mut state = lock_state(&recycler);
            if self.generation == state.generation {
                state.pending_chunks.push(chunk);
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
    state: Arc<Mutex<AllocatorState>>,
}

impl ChunkAllocator {
    /// Go `NewAllocator`.
    #[must_use]
    pub fn new() -> Self {
        let free_chunk_limit = MAX_FREE_CHUNKS.load(Ordering::Relaxed);
        let free_columns_per_type = MAX_FREE_COLUMNS_PER_TYPE.load(Ordering::Relaxed);
        Self {
            state: Arc::new(Mutex::new(AllocatorState {
                pending_chunks: Vec::new(),
                pending_columns: HashMap::new(),
                free_chunks: Vec::new(),
                columns: PoolColumnAllocator::new(free_columns_per_type),
                free_chunk_limit,
                registered_chunks: 0,
                registered_columns: HashMap::new(),
                generation: 0,
            })),
        }
    }

    #[cfg(test)]
    fn cached_chunks(&self) -> usize {
        lock_state(&self.state).free_chunks.len()
    }

    #[cfg(test)]
    fn cached_columns(&self, type_size: i64) -> usize {
        lock_state(&self.state).columns.cached(type_size)
    }

    #[cfg(test)]
    fn pending_objects(&self) -> usize {
        let state = lock_state(&self.state);
        state.pending_chunks.len() + state.pending_columns.values().map(Vec::len).sum::<usize>()
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
        let (chunk, cache_chunk, column_registrations, generation) =
            lock_state(&self.state).alloc(fields, capacity, max_chunk_size);
        AllocatedChunk::pooled(
            chunk,
            cache_chunk,
            column_registrations,
            generation,
            Arc::clone(&self.state),
        )
    }

    fn check_reuse_alloc_size(&self) -> bool {
        let state = lock_state(&self.state);
        state.free_chunk_limit > 0 || state.columns.free_columns_per_type > 0
    }

    fn reset(&self) {
        lock_state(&self.state).reset();
    }
}

/// Go `NewAllocator`.
#[must_use]
pub fn new_allocator() -> ChunkAllocator {
    ChunkAllocator::new()
}

/// Go `syncAllocator`.
pub struct SyncAllocator {
    allocator: Mutex<Box<dyn Allocator>>,
}

impl SyncAllocator {
    /// Go `NewSyncAllocator`.
    #[must_use]
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
#[must_use]
pub fn new_sync_allocator(allocator: impl Allocator + 'static) -> SyncAllocator {
    SyncAllocator::new(allocator)
}

/// Go `reuseHookAllocator`.
pub struct ReuseHookAllocator {
    hook_called: Mutex<bool>,
    hook: Box<dyn Fn() + Send + Sync>,
    allocator: Box<dyn Allocator>,
}

impl ReuseHookAllocator {
    /// Go `NewReuseHookAllocator`.
    #[must_use]
    pub fn new(
        allocator: impl Allocator + 'static,
        hook: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            hook_called: Mutex::new(false),
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
            // Go `sync.Once` serializes all callers through the hook, marks it
            // done even when the hook panics, and lets recovered later calls
            // proceed. Hold a poison-recovering mutex across the invocation to
            // preserve all three properties.
            {
                let mut hook_called = self
                    .hook_called
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if !*hook_called {
                    *hook_called = true;
                    (self.hook)();
                }
            }
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
#[must_use]
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
#[must_use]
pub const fn new_empty_allocator() -> EmptyAllocator {
    EmptyAllocator
}

#[cfg(test)]
mod tests {
    use super::*;
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
            let state = lock_state(&allocator.state);
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
            let state = lock_state(&clamped.state);
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
        let allocator = new_allocator();
        {
            let chunk = allocator.alloc(&fields(), 5, 100);
            assert_eq!(chunk.capacity(), 5);
            assert_eq!(chunk.required_rows(), 100);
            assert_eq!(chunk.column(0).type_size(), VAR_ELEM_LEN);
            assert_eq!(chunk.column(2).type_size(), 4);
            assert_eq!(chunk.column(3).type_size(), 40);
            assert_eq!(chunk.column(4).type_size(), 8);
            assert_eq!(chunk.column(2).data_capacity(), 20);
            assert_eq!(chunk.column(3).data_capacity(), 200);
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
        let mut destination = Chunk::new_with_capacity(&fields, 1);
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
                        drop(allocator.alloc(&fields, 5, 100));
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

        let mut decoder = crate::codec::Decoder::new(
            Chunk::new_with_capacity(&fields(), 0),
            fields(),
        );
        decoder.reset(&buf);
        decoder.reuse_intermediate_chunk(&mut chk);
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
}
