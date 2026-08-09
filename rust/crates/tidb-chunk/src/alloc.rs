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
//! it when dropped, and [`Allocator::reset`] moves queued objects into the
//! bounded free lists. Callers therefore end a chunk's lease before reset;
//! live chunks cannot be invalidated behind a safe Rust reference.

use crate::chunk::Chunk;
use crate::column::{get_fixed_len, Column, VAR_ELEM_LEN};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError};
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

#[derive(Debug)]
struct PendingChunk {
    chunk: Chunk,
    expected_type_sizes: Vec<i64>,
}

#[derive(Debug)]
struct AllocatorState {
    pending: Vec<PendingChunk>,
    free_chunks: Vec<Chunk>,
    columns: PoolColumnAllocator,
    free_chunk_limit: usize,
}

impl AllocatorState {
    fn alloc(&mut self, fields: &[FieldType], capacity: usize, max_chunk_size: usize) -> Chunk {
        let capacity = capacity.min(max_chunk_size);
        let columns: Vec<_> = fields
            .iter()
            .map(|field| self.columns.new_column(field, capacity))
            .collect();
        if let Some(mut chunk) = self.free_chunks.pop() {
            chunk.restore_reusable_columns(columns, capacity, max_chunk_size);
            chunk
        } else {
            Chunk::from_reusable_columns(columns, capacity, max_chunk_size)
        }
    }

    fn reset(&mut self) {
        for mut pending in self.pending.drain(..) {
            let columns = pending.chunk.drain_columns_for_allocator();
            for (expected_type_size, column) in pending.expected_type_sizes.into_iter().zip(columns)
            {
                self.columns.put_if_eligible(expected_type_size, column);
            }
            if self.free_chunks.len() < self.free_chunk_limit {
                self.free_chunks.push(pending.chunk);
            }
        }
    }
}

fn lock_state(state: &Mutex<AllocatorState>) -> MutexGuard<'_, AllocatorState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// An ownership-safe lease returned by [`Allocator::alloc`].
///
/// It dereferences to [`Chunk`]. Dropping it queues the chunk for the next
/// allocator reset, which is the Rust equivalent of ending all Go uses before
/// calling `Allocator.Reset`.
pub struct AllocatedChunk {
    chunk: Option<Chunk>,
    expected_type_sizes: Vec<i64>,
    recycler: Option<Arc<Mutex<AllocatorState>>>,
}

impl AllocatedChunk {
    fn pooled(chunk: Chunk, fields: &[FieldType], recycler: Arc<Mutex<AllocatorState>>) -> Self {
        Self {
            chunk: Some(chunk),
            expected_type_sizes: fields.iter().map(get_fixed_len).collect(),
            recycler: Some(recycler),
        }
    }

    fn unpooled(chunk: Chunk) -> Self {
        Self {
            chunk: Some(chunk),
            expected_type_sizes: Vec::new(),
            recycler: None,
        }
    }

    /// Consume the lease without recycling it.
    #[must_use]
    pub fn into_chunk(mut self) -> Chunk {
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
        let (Some(recycler), Some(chunk)) = (self.recycler.take(), self.chunk.take()) else {
            return;
        };
        lock_state(&recycler).pending.push(PendingChunk {
            chunk,
            expected_type_sizes: std::mem::take(&mut self.expected_type_sizes),
        });
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
                pending: Vec::new(),
                free_chunks: Vec::with_capacity(free_chunk_limit),
                columns: PoolColumnAllocator::new(free_columns_per_type),
                free_chunk_limit,
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
        let chunk = lock_state(&self.state).alloc(fields, capacity, max_chunk_size);
        AllocatedChunk::pooled(chunk, fields, Arc::clone(&self.state))
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
    once: Once,
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
            once: Once::new(),
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
            self.once.call_once(|| (self.hook)());
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
                chunk.append_string(0, &"x".repeat(128));
            }
        }
        allocator.reset();
        assert_eq!(allocator.cached_chunks(), 5);
        assert_eq!(allocator.cached_columns(VAR_ELEM_LEN), 3);

        init_chunk_alloc_size(0, 0);
        let disabled = new_allocator();
        assert!(!disabled.check_reuse_alloc_size());
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
}
