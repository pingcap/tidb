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

//! `pkg/executor/internal/exec`: the [`Executor`] trait and the [`ExecutorMeta`]
//! shared base state.

pub mod fast_bytes_map;
pub mod ruv2;
pub mod table_access;
pub mod index_usage_reporter;
pub mod predicate_pushdown;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::FieldType;
use tidb_expr::schema::Schema;

use tidb_driver_error::ExecError;

/// Go `exec.Executor`: a pull-based operator in the execution tree.
///
/// Callers drive it with `open()`, repeated `next(&mut chunk)` (an empty result
/// chunk signals EOF, as in Go), then `close()`.
///
/// Go's `RuntimeStats` is applied by physical-plan construction when the
/// statement has an enabled runtime-stat source. The remaining observability/
/// control methods (`HandleSQLKillerSignal`,
/// `RegisterSQLAndPlanInExecForTopProfiling`, `Detach`) and the
/// `context.Context` argument are still outside this trait. Physical-plan
/// construction applies Go's before/after SQL-killer behavior at each built
/// operator boundary and converts Open/Next/Close panics to executor errors.
pub trait Executor: Send {
    /// Go `Open`: prepare the operator (and, by convention, its children).
    fn open(&mut self) -> Result<(), ExecError>;

    /// Go `Next`: produce the next batch of rows into `req` (which is reset
    /// first). An empty `req` on return means the operator is exhausted.
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError>;

    /// Go `Close`: release resources.
    fn close(&mut self) -> Result<(), ExecError>;

    /// Go `Schema`: the operator's output schema.
    fn schema(&self) -> &Schema;

    /// Go `RetFieldTypes`: the output column types.
    fn ret_field_types(&self) -> &[FieldType];

    /// Go `InitCap`: the initial per-chunk row capacity.
    fn init_cap(&self) -> usize;

    /// Go `MaxChunkSize`: the maximum per-chunk row count.
    fn max_chunk_size(&self) -> usize;

    /// Go `NewChunk`: allocate a result chunk sized for this operator's output.
    fn new_chunk(&self) -> Chunk;

    /// Go `exec.NewFirstChunk(e)`: `chunk.New(e.RetFieldTypes(), e.InitCap(),
    /// e.MaxChunkSize())`, never from the executor's `AllocPool`.
    fn new_first_chunk(&self) -> Chunk {
        Chunk::new(self.ret_field_types(), self.init_cap(), self.max_chunk_size())
    }

    /// This operator as a negotiable base-table source, when it is one.
    ///
    /// The default `None` is the fail-closed shape the whole negotiation is
    /// built on: an operator is offered no pushed filter, no row cap and no
    /// column prune unless it opts in by returning itself, so it cannot
    /// inherit a promise it never made. See [`crate::table_access`] for the
    /// contract an implementor takes on -- in particular the staged-row
    /// promise, which is written down exactly once, there.
    fn table_access(&mut self) -> Option<&mut dyn crate::table_access::TableAccess> {
        None
    }

    /// Go `UnionScanExec.open`'s `reader.(type)` switch: this operator as
    /// the reader a union scan reads the transaction's buffer beside, seen
    /// through the wrappers the builder puts around it (and a
    /// `SelectionExec`, which Go looks through). `None` for any other
    /// operator.
    fn union_scan_reader(&mut self) -> Option<&mut dyn std::any::Any> {
        None
    }

    /// Go `aggExecutorTreeInputEmpty` (`pkg/executor/join/hash_join_v1.go`):
    /// whether this already-executed operator tree is an aggregation that saw
    /// NO input row, and therefore owes its single output row entirely to the
    /// aggregates' default values.
    ///
    /// Only `tidb_executor::apply::ApplyExec` asks, and only of its outer child --
    /// see the rule quoted there. Go's walk recurses through a SINGLE-child
    /// operator, stops at a join (`false`) and needs every branch of a union
    /// to be empty; the default here is Go's leaf answer, so an operator that
    /// is not an aggregation and does not forward reports `false`.
    fn agg_tree_input_empty(&self) -> bool {
        false
    }

    /// Go `Executor.Detach` with `executor.Detach`'s walk of its children:
    /// whether this executor and every child of it can keep running after
    /// the session context moves on to another statement. Go's detached copy
    /// carries static contexts; an executor here owns its statement state
    /// already, so a detachable tree is handed over as it is. The default is
    /// Go's `BaseExecutorV2.Detach`: `false`.
    fn detach(&mut self) -> bool {
        false
    }
}


/// A row counter owned by one executor and observed by its statement.
/// Moving the executor to a worker does not move or invalidate the observer.
/// Counts carry no data-publication ordering; chunk/worker handoffs do that.
#[derive(Clone, Debug, Default)]
pub struct RowCount(Arc<AtomicU64>);

impl RowCount {
    /// Read the current count without synchronizing executor data.
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Replace the count from its single owning executor.
    pub fn set(&self, rows: u64) {
        self.0.store(rows, Ordering::Relaxed);
    }
}

/// Go `exec.executorMeta`: the schema/id/children/result-type base state shared
/// by every operator (Go embeds it via `BaseExecutorV2`).
///
/// The chunk-sizing fields (`init_cap`, `max_chunk_size`) and the
/// `AllocPool` come from Go's `executorChunkAllocator`; runtime stats attach
/// at the physical-builder boundary, while killer and RU-tracking helpers
/// remain deferred.
#[derive(Clone)]
pub struct ExecutorMeta {
    schema: Schema,
    ret_field_types: Vec<FieldType>,
    id: i64,
    init_cap: usize,
    max_chunk_size: usize,
    /// Go `executorChunkAllocator.AllocPool`: the session's reusable chunk
    /// allocator when the executor was built, else plain `chunk.New` (Go's
    /// `NewEmptyAllocator`).
    alloc_pool: Option<Arc<dyn tidb_chunk::alloc::Allocator>>,
}

impl std::fmt::Debug for ExecutorMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutorMeta")
            .field("schema", &self.schema)
            .field("ret_field_types", &self.ret_field_types)
            .field("id", &self.id)
            .field("init_cap", &self.init_cap)
            .field("max_chunk_size", &self.max_chunk_size)
            .field("alloc_pool", &self.alloc_pool.is_some())
            .finish()
    }
}

impl ExecutorMeta {
    /// Go `newExecutorMeta` (+ the chunk-allocator sizing): derives the result
    /// field types from the schema's columns.
    ///
    /// # Panics
    /// If a schema column lacks a result type -- Go dereferences `RetType`
    /// unconditionally, so a nil there is already a bug at this point.
    #[must_use]
    pub fn new(schema: Schema, id: i64, init_cap: usize, max_chunk_size: usize) -> Self {
        let ret_field_types = schema
            .columns
            .iter()
            .map(|c| {
                c.ret_type
                    .clone()
                    .expect("executor schema column must have a result type")
            })
            .collect();
        ExecutorMeta {
            schema,
            ret_field_types,
            id,
            init_cap,
            max_chunk_size,
            alloc_pool: None,
        }
    }

    /// Go `newExecutorChunkAllocator`'s `AllocPool: vars.GetChunkAllocator()`.
    #[must_use]
    pub fn with_alloc_pool(mut self, alloc_pool: Option<Arc<dyn tidb_chunk::alloc::Allocator>>) -> Self {
        self.alloc_pool = alloc_pool;
        self
    }

    /// Go `AllocPool`, for a writer that sizes its own child chunk
    /// (`newDMLChildChunk`).
    #[must_use]
    pub fn alloc_pool(&self) -> Option<&Arc<dyn tidb_chunk::alloc::Allocator>> {
        self.alloc_pool.as_ref()
    }

    /// Go `Schema`.
    #[must_use]
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Go `RetFieldTypes`.
    #[must_use]
    pub fn ret_field_types(&self) -> &[FieldType] {
        &self.ret_field_types
    }

    /// Go `ID`.
    #[must_use]
    pub fn id(&self) -> i64 {
        self.id
    }

    /// Go `InitCap`.
    #[must_use]
    pub fn init_cap(&self) -> usize {
        self.init_cap
    }

    /// Go `MaxChunkSize`.
    #[must_use]
    pub fn max_chunk_size(&self) -> usize {
        self.max_chunk_size
    }

    /// Go `NewChunk`: a result chunk for this operator's output types.
    #[must_use]
    pub fn new_chunk(&self) -> Chunk {
        self.new_chunk_with_capacity(&self.ret_field_types, self.init_cap, self.max_chunk_size)
    }

    /// Go `exec.NewFirstChunk` over this executor: `chunk.New` with its own
    /// types and sizes, never from the `AllocPool`.
    #[must_use]
    pub fn new_first_chunk(&self) -> Chunk {
        Chunk::new(&self.ret_field_types, self.init_cap, self.max_chunk_size)
    }

    /// Go `NewChunkWithCapacity`: `AllocPool.Alloc(fields, capacity,
    /// maxChunkSize)`.
    #[must_use]
    pub fn new_chunk_with_capacity(&self, fields: &[FieldType], capacity: usize, max_chunk_size: usize) -> Chunk {
        new_chunk_with_alloc_pool(self.alloc_pool.as_ref(), fields, capacity, max_chunk_size)
    }
}

/// Go `exec.NewFirstChunk(e)`: `chunk.New(e.RetFieldTypes(), e.InitCap(),
/// e.MaxChunkSize())`, never from the executor's `AllocPool` (unlike
/// `NewChunk` / `TryNewCacheChunk`).
#[must_use]
pub fn new_first_chunk(e: &dyn Executor) -> Chunk {
    e.new_first_chunk()
}

/// Go `AllocPool.Alloc(fields, capacity, maxChunkSize)`, an absent pool being
/// Go's `emptyAllocator` (`chunk.New`).
#[must_use]
pub fn new_chunk_with_alloc_pool(
    alloc_pool: Option<&Arc<dyn tidb_chunk::alloc::Allocator>>,
    fields: &[FieldType],
    capacity: usize,
    max_chunk_size: usize,
) -> Chunk {
    match alloc_pool {
        Some(pool) => pool.alloc(fields, capacity, max_chunk_size).into_pooled_chunk(),
        None => Chunk::new(fields, capacity, max_chunk_size),
    }
}
