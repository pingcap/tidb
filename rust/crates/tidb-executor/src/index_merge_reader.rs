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

//! `pkg/executor/index_merge_reader.go`: `IndexMergeReaderExecutor` -- the
//! reader that runs several index (or table) access paths over one table and
//! combines their HANDLES by union or intersection before looking the rows up.
//!
//! # What this file owns
//!
//! Index merge is `IndexLookUpReader` with N probes instead of one. The three
//! stages Go names in its own doc comment are all here:
//!
//! 1. the partial workers, which fetch handle batches per access path
//!    (`partialIndexWorker.fetchHandles` :1777, `partialTableWorker.fetchHandles`
//!    :699, both ending in `buildTableTask` :1902/:805) -- modelled as the
//!    [`PartialHandleSource`] trait, because *how* a path produces handles is
//!    already the business of [`crate::access_path`] and
//!    [`crate::index_range`], not of this file;
//! 2. the process worker, which is the actual index-merge algebra
//!    (`fetchLoopUnion` :1245, `fetchLoopUnionWithOrderBy` :1111 with
//!    `handleHeap` :1016, `fetchLoopIntersection` :1577 with
//!    `intersectionProcessWorker.doIntersectionPerPartition` :1433 and
//!    `intersectionCollectWorker.doIntersectionLimitAndDispatch` :1360);
//! 3. the table scan worker, which turns a handle batch into rows
//!    (`indexMergeTableScanWorker.executeTask` :1988 via
//!    `buildFinalTableReader` :854).
//!
//! Stage 2's handle algebra lives here. Stages 1 and 3 reuse the retained
//! physical executor tree. Worker concurrency and handle-state memory
//! accounting remain separate package-parity obligations.
//!
//! # Reuse rather than restatement
//!
//! * [`crate::kv_table::TableHandle`] identifies local row handles. Union
//!   deduplicates through the shared [`tidb_txnkv::HandleMap`]; intersection uses the
//!   shared [`tidb_txnkv::MemAwareHandleMap`] with Go's checkpoint accounting.
//! * [`crate::access_path::HandleSourceExec`] is Go's `buildFinalTableReader`
//!   result: a reader over an already-known handle list. Stage 3 builds one
//!   per task and drains it, exactly as `executeTask` does.
//! * `tidb_expr::compare_datums_with_collation` is Go
//!   `chunk.GetCompareFunc(keyType)`, already used by [`crate::sort`]; the
//!   order-by heap calls it rather than growing a second comparator.
//! * Range building and key encoding are NOT here at all -- a partial path
//!   arrives as a [`PartialHandleSource`] that has already done that work
//!   through [`crate::index_range`] / [`crate::kv_table`].
//!
//! # Sequential here, worker-parallel there
//!
//! Go's topology per statement is: one `partialIndexWorker` or
//! `partialTableWorker` goroutine per access path, all writing one `fetchCh`;
//! one `indexMergeProcessWorker` reading it (which, for intersection, fans out
//! again into `IndexMergeIntersectionConcurrency()` `intersectionProcessWorker`
//! goroutines plus an optional `intersectionCollectWorker`); then
//! `LookupTableTaskChannelSize`-buffered `workCh`/`resultCh` feeding several
//! `indexMergeTableScanWorker` goroutines, with the main `Next` goroutine
//! draining `resultCh` in order.
//!
//! This port runs all of that on one thread. What that costs, stage by stage:
//!
//! * **Partial workers.** Go interleaves the paths' batches into `fetchCh` in
//!   whatever order they arrive; this port drains path 0 fully, then path 1,
//!   and so on. Nothing downstream depends on the interleaving: the union
//!   dedup set, the intersection counter map, and the order-by heap are all
//!   order-insensitive over their whole input.
//! * **Intersection fan-out.** Go shards by `task.parTblIdx % workerCnt`, so
//!   each worker owns a disjoint set of partitions and its own handle map --
//!   there is no shared mutable state at all. It also collapses to ONE worker
//!   whenever `hasGlobalIndex`, precisely because a global index would break
//!   that disjointness. A single sequential map over `(partition, handle)`
//!   computes the identical multiset.
//! * **Table scan workers.** Unordered union produces each task on demand,
//!   retaining its dedup and LIMIT state across calls, so an outer LIMIT can
//!   close the pipeline without first draining every partial. Each table
//!   task completes its reader before exposing rows and owns the result chunks;
//!   tasks are handed to `resultCh` in the order the process worker created
//!   them, and `getResultTask` consumes `resultCh` in that order. So the
//!   *task* order is already deterministic in Go, and rows within a task come
//!   out in the task's handle order. Running the readers one at a time changes
//!   latency, not row order.
//!
//! Sequential execution does not establish parity for latency, speculative
//! reads, concurrent failure timing or memory use. For result ordering it
//! chooses one of the interleavings Go leaves unspecified:
//!
//! * **Union output order.** Go's first-writer-wins dedup means which of two
//!   duplicate handles' batch position survives depends on goroutine
//!   scheduling; with a pushed limit that decides which handles survive at
//!   all. Go makes no order promise for a union index merge without
//!   `ORDER BY` (the planner puts a `Sort` above when one is needed), so the
//!   path-0-first drain is one legal schedule. See
//!   [`IndexMergeReaderExec::pushed_limit`] for the one case where this is
//!   visible.
//! * **Intersection output order.** Go iterates `kv.MemAwareHandleMap.Range`,
//!   which promises no handle order. The shared Rust handle map has the same
//!   unordered traversal contract; a LIMIT without ORDER BY may select any
//!   corresponding prefix of that traversal.
//!
//! # Narrowings, all named
//!
//! * `keepOrder` re-sorting inside `executeTask` (:2030) is a no-op here.
//!   Go sorts each task's handles into KEY order before the coprocessor
//!   request (`buildTableReaderFromHandles`, `canReorderHandles = true`) and
//!   then puts the rows back into `indexOrder`;
//!   [`crate::access_path::HandleSourceExec`] reads handles in the order given
//!   and so is already in index order. Same rows, same order, one less sort.
//! * Partitioning is modelled as an opaque `partition_index` on a batch, not
//!   as Go's `kv.PartitionHandle` wrapper. The dedup/count key is
//!   `(partition_index, handle)`, which is what wrapping achieves. Global
//!   indexes remap their encoded physical IDs in the storage cursor; the
//!   partial worker maps its pruned ordinals to the reader's partition map.
//!   Final row lookup retains that physical identity.
//! * `IndexMergeRuntimeStat` (:2060), the remaining ordered/task accounting, the
//!   `failpoint` injections, `handleWorkerPanic` (:939), `syncErr` (:1734),
//!   correlated-column range rebuilding (`rebuildRangeForCorCol` :201) have
//!   no counterpart in this tier. Index-usage reporting stays on each ordinary
//!   partial scan executor: closing the partials records the same per-plan
//!   coprocessor summaries that Go's root reporter reads in `Close` (:978).
//! * `fetchLoopIntersectionWithOrderBy` (:1569) is an empty `// todo` in Go
//!   itself. It is refused here for the same reason.

use std::collections::{BTreeMap, HashMap, VecDeque};
#[cfg(test)]
use std::collections::BTreeSet;

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Collation, Datum, FieldType};
use tidb_expr::schema::Schema;

use crate::access_path::{HandleOutputColumn, HandleSourceExec};
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{KvTable, RowDecodeContext, TableHandle};

/// Rebuild the retained coprocessor tree around one task's handle scan.
pub(crate) type TableTaskBuilder =
    Box<dyn Fn(Box<dyn Executor>) -> Result<Box<dyn Executor>, ExecError> + Send>;

/// Go `physicalop.PushedDownLimit`: the `LIMIT count OFFSET offset` the
/// planner pushed into the index merge, counted in HANDLES rather than rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushedDownLimit {
    /// Go `Offset`.
    pub offset: u64,
    /// Go `Count`.
    pub count: u64,
}

/// Go `pushedLimitCountingDown` (:1226): consume `handles` against `limit`,
/// mutating it in place.
///
/// Returns `true` when the whole batch fell inside the offset and the caller
/// should skip to the next one (Go's `next` return); otherwise the retained
/// prefix is returned.
fn pushed_limit_counting_down(
    limit: &mut PushedDownLimit,
    mut handles: Vec<HandleRef>,
) -> (bool, Vec<HandleRef>) {
    let len = handles.len() as u64;
    if len <= limit.offset {
        limit.offset -= len;
        return (true, Vec::new());
    }
    handles.drain(..limit.offset as usize);
    limit.offset = 0;

    let len = handles.len() as u64;
    if len > limit.count {
        handles.truncate(limit.count as usize);
    }
    limit.count -= limit.count.min(len);
    (false, handles)
}

/// A handle together with the physical partition it was read from.
///
/// Go achieves this by WRAPPING the handle in `kv.PartitionHandle` inside the
/// process worker (`fetchLoopUnion` :1284, `doIntersectionPerPartition` :1451)
/// so that the wrapped value is what the handle map keys on. Carrying the
/// partition beside the handle keys on the same pair without needing a third
/// `TableHandle` variant that only index merge would ever build.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HandleRef {
    /// Go `indexMergeTableTask.parTblIdx`: an index into the pruned partition
    /// list, or `0` for a non-partitioned table.
    pub partition_index: usize,
    /// Go `kv.Handle`.
    pub handle: TableHandle,
}

impl HandleRef {
    /// A handle on a non-partitioned table.
    #[must_use]
    pub fn new(handle: TableHandle) -> Self {
        HandleRef {
            partition_index: 0,
            handle,
        }
    }
}

/// Storage sources have already resolved physical partition identity. Share
/// the KV package's integer/common identity within each resolved partition.
fn merge_handle_key(handle: TableHandle) -> Result<tidb_txnkv::Handle, ExecError> {
    match handle {
        TableHandle::Int(value) => Ok(tidb_txnkv::IntHandle::new(value).into()),
        TableHandle::Common(encoded) => tidb_txnkv::CommonHandle::new(encoded)
            .map(Into::into)
            .map_err(|error| ExecError::internal(format!("invalid merge common handle: {error}"))),
    }
}

/// Go's union HandleMap, with resolved partition ordinals outside the key.
/// Membership never determines output order: the first incoming handle wins.
#[derive(Default)]
struct MergeHandleSet {
    partitions: HashMap<usize, tidb_txnkv::HandleMap<()>>,
}

impl MergeHandleSet {
    fn insert(&mut self, handle: &HandleRef) -> Result<bool, ExecError> {
        self.insert_with_usage(handle).map(|(inserted, _)| inserted)
    }

    fn insert_with_usage(&mut self, handle: &HandleRef) -> Result<(bool, u64), ExecError> {
        let key = merge_handle_key(handle.handle.clone())?;
        let usage = key.mem_usage();
        let map = self.partitions.entry(handle.partition_index).or_default();
        if map.get(&key).is_some() {
            return Ok((false, usage));
        }
        map.set(key, ());
        Ok((true, usage))
    }
}

/// Typed ordering rows retained across partial, partition and process stages.
/// Materialized rows support dependency-closed sources without inventing field
/// types; production executor sources always preserve their declared types.
#[derive(Clone, Debug)]
pub enum MergeSortKeys {
    /// Rows supplied by materialized handle sources.
    Materialized(Vec<Vec<Datum>>),
    /// Columns with their original declared SQL types.
    Typed {
        /// Owned key columns.
        chunk: Chunk,
        /// Types used to decode each key column.
        fields: Vec<FieldType>,
    },
}

impl Default for MergeSortKeys {
    fn default() -> Self {
        Self::Materialized(Vec::new())
    }
}

impl From<Vec<Vec<Datum>>> for MergeSortKeys {
    fn from(rows: Vec<Vec<Datum>>) -> Self {
        Self::Materialized(rows)
    }
}

impl MergeSortKeys {
    fn typed(fields: Vec<FieldType>, capacity: usize) -> Self {
        Self::Typed {
            chunk: Chunk::new_with_capacity(&fields, capacity),
            fields,
        }
    }

    /// Number of ordering rows.
    pub fn len(&self) -> usize {
        match self {
            Self::Materialized(rows) => rows.len(),
            Self::Typed { chunk, .. } => chunk.num_rows(),
        }
    }

    /// Whether no ordering rows are retained.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn truncate(&mut self, maximum: usize) {
        match self {
            Self::Materialized(rows) => rows.truncate(maximum),
            Self::Typed { chunk, .. } if chunk.num_rows() > maximum => chunk.truncate_to(maximum),
            _ => {}
        }
    }

    fn datum(&self, row: usize, column: usize) -> Option<std::borrow::Cow<'_, Datum>> {
        match self {
            Self::Materialized(rows) => rows.get(row)?.get(column).map(std::borrow::Cow::Borrowed),
            Self::Typed { chunk, fields } => fields
                .get(column)
                .map(|field| std::borrow::Cow::Owned(chunk.get_row(row).get_datum(column, field))),
        }
    }

    fn empty_like(&self, capacity: usize) -> Self {
        match self {
            Self::Materialized(_) => Self::Materialized(Vec::with_capacity(capacity)),
            Self::Typed { fields, .. } => Self::typed(fields.clone(), capacity),
        }
    }

    fn append_from(&mut self, source: &Self, row: usize) -> Result<(), ExecError> {
        match (self, source) {
            (
                Self::Typed { chunk, fields },
                Self::Typed {
                    chunk: source,
                    fields: source_fields,
                },
            ) if fields == source_fields => chunk.append_row(source.get_row(row)),
            (Self::Materialized(rows), Self::Materialized(source)) => {
                rows.push(source[row].clone())
            }
            _ => return Err(ExecError::internal("ordered partition key layouts differ")),
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn values(&self, row: usize) -> Vec<Datum> {
        match self {
            Self::Materialized(rows) => rows[row].clone(),
            Self::Typed { fields, .. } => (0..fields.len())
                .map(|column| self.datum(row, column).unwrap().into_owned())
                .collect(),
        }
    }
}

/// One `indexMergeTableTask` as produced by a partial worker's
/// `buildTableTask` (:1902 / :805).
#[derive(Clone, Debug, Default)]
pub struct PartialHandleBatch {
    /// Go `lookupTableTask.handles`.
    pub handles: Vec<HandleRef>,
    /// Go `lookupTableTask.idxRows`, already reduced to the by-item key
    /// columns -- that reduction is `pruneTableWorkerTaskIdxRows` (:1094),
    /// which for an index path is a no-op because the by-item columns are
    /// already leading.
    ///
    /// Empty unless the index merge has `byItems`; otherwise parallel to
    /// `handles`.
    pub sort_keys: MergeSortKeys,
}

/// Go's partial workers (`partialIndexWorker` :1717, `partialTableWorker`
/// :663) reduced to what the process worker actually consumes: a stream of
/// handle batches.
///
/// The batch-size growth Go performs in `extractTaskHandles` (:1830, doubling
/// `w.batchSize` up to `maxBatchSize`) and the per-path `pushedLimit`
/// short-circuit inside it belong to the implementor, because they are
/// properties of the scan, not of the merge.
pub trait PartialHandleSource: Send {
    /// Go `partial*Worker.fetchHandles` start-up (the `startPartial*Worker`
    /// wrappers, :380 / :518).
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    /// The next batch, or `None` at end of path. An empty batch is treated as
    /// end of path, matching `extractTaskHandles` returning no handles.
    fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError>;

    /// Remaining raw handles allowed by a union worker's pushed LIMIT.
    /// Production sources use this before fetching, like Go's RequiredRows;
    /// materialized sources need only retain the permitted prefix.
    fn next_batch_with_max_handles(
        &mut self,
        maximum: usize,
    ) -> Result<Option<PartialHandleBatch>, ExecError> {
        if maximum == 0 {
            return Ok(None);
        }
        let mut batch = self.next_batch()?;
        if let Some(batch) = &mut batch {
            batch.handles.truncate(maximum);
            batch.sort_keys.truncate(maximum);
        }
        Ok(batch)
    }

    /// Go `partial*Worker` teardown.
    fn close(&mut self) -> Result<(), ExecError> {
        Ok(())
    }
}

/// A partial path whose handles are already materialized.
///
/// This is the dependency-closed source: Go's coverage of index merge runs
/// through `testkit` end to end, so the merge algebra itself is exercised
/// here by feeding it batches directly.
pub struct MaterializedHandleSource {
    batches: VecDeque<PartialHandleBatch>,
}

impl MaterializedHandleSource {
    /// A path that emits `batches` in order.
    #[must_use]
    pub fn new(batches: Vec<PartialHandleBatch>) -> Self {
        MaterializedHandleSource {
            batches: batches.into(),
        }
    }

    /// A path that emits one batch of int handles with no sort keys.
    #[must_use]
    pub fn from_int_handles(handles: &[i64]) -> Self {
        Self::new(vec![PartialHandleBatch {
            handles: handles
                .iter()
                .map(|h| HandleRef::new(TableHandle::Int(*h)))
                .collect(),
            sort_keys: MergeSortKeys::default(),
        }])
    }
}

impl PartialHandleSource for MaterializedHandleSource {
    fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError> {
        Ok(self.batches.pop_front())
    }
}

/// The physical columns a partial index-merge plan emits for its row handle.
///
/// Go's partial workers receive `HandleCols` from the retained reader plan.
/// Rust derives the same slots from the partial plan schema and table metadata
/// while building the executor, then keeps that physical decision here.
pub(crate) enum PartialHandleColumns {
    /// One integer handle column.
    Int(usize),
    /// Every common-handle component, in primary-key order.
    Common(Vec<usize>),
}

/// Several partition results belong to ONE partial access path. Keeping that
/// boundary matters for intersection's per-path membership counting.
pub(crate) struct PartitionedHandleSource {
    sources: Vec<Box<dyn PartialHandleSource>>,
    current: usize,
    opened: Vec<bool>,
    rows: crate::executor::RowCount,
    by_items: Vec<MergeByItem>,
    key_comparators: MergeKeyComparators,
    batches: Vec<Option<(PartialHandleBatch, usize)>>,
    heap: Vec<usize>,
    initialized: bool,
    batch_size: usize,
}

impl PartitionedHandleSource {
    pub(crate) fn new(sources: Vec<Box<dyn PartialHandleSource>>) -> Self {
        let count = sources.len();
        Self {
            sources,
            current: 0,
            opened: vec![false; count],
            rows: crate::executor::RowCount::default(),
            by_items: Vec::new(),
            key_comparators: MergeKeyComparators::default(),
            batches: Vec::new(),
            heap: Vec::new(),
            initialized: false,
            batch_size: 1024,
        }
    }

    pub(crate) fn with_order(mut self, by_items: Vec<MergeByItem>, batch_size: usize) -> Self {
        self.by_items = by_items;
        self.batch_size = batch_size.max(1);
        if !self.by_items.is_empty() {
            self.batches.resize_with(self.sources.len(), || None);
        }
        self
    }

    pub(crate) fn produced_rows(&self) -> crate::executor::RowCount {
        self.rows.clone()
    }

    fn refill(&mut self, index: usize, maximum: usize) -> Result<bool, ExecError> {
        if !self.opened[index] {
            self.opened[index] = true;
            self.sources[index].open()?;
        }
        if let Some(batch) = self.sources[index].next_batch_with_max_handles(maximum)? {
            if batch.handles.len() != batch.sort_keys.len() {
                return Err(ExecError::internal(
                    "ordered partition needs one sort key per handle",
                ));
            }
            self.batches[index] = (!batch.handles.is_empty()).then_some((batch, 0));
        }
        if self.batches[index].is_none() {
            self.opened[index] = false;
            self.sources[index].close()?;
            return Ok(false);
        }
        Ok(true)
    }

    fn less_partition(&self, left: usize, right: usize) -> Result<bool, ExecError> {
        let (left_batch, left_row) = self.batches[left].as_ref().expect("heap head");
        let (right_batch, right_row) = self.batches[right].as_ref().expect("heap head");
        compare_merge_keys(
            &self.key_comparators,
            &self.by_items,
            (&left_batch.sort_keys, *left_row),
            (&right_batch.sort_keys, *right_row),
        )
        .map(|order| order.is_lt())
    }

    fn sift_down(&mut self, mut parent: usize) -> Result<(), ExecError> {
        loop {
            let mut child = parent * 2 + 1;
            if child >= self.heap.len() {
                return Ok(());
            }
            if child + 1 < self.heap.len()
                && self.less_partition(self.heap[child + 1], self.heap[child])?
            {
                child += 1;
            }
            if !self.less_partition(self.heap[child], self.heap[parent])? {
                return Ok(());
            }
            self.heap.swap(parent, child);
            parent = child;
        }
    }

    /// Go sortedSelectResults: retain one batch per partition and a heap of
    /// their first rows. A partial path must be globally sorted before the
    /// top-N worker can safely stop reading it early.
    fn next_ordered_batch(
        &mut self,
        maximum: usize,
    ) -> Result<Option<PartialHandleBatch>, ExecError> {
        if !self.initialized {
            self.initialized = true;
            for index in 0..self.sources.len() {
                if self.refill(index, maximum)? {
                    self.heap.push(index);
                }
            }
            for parent in (0..self.heap.len() / 2).rev() {
                self.sift_down(parent)?;
            }
        }
        let mut result = PartialHandleBatch::default();
        while result.handles.len() < self.batch_size.min(maximum) {
            let Some(&index) = self.heap.first() else {
                break;
            };
            if self.batches[index].is_none() {
                if !self.refill(index, maximum - result.handles.len())? {
                    self.heap.swap_remove(0);
                }
                self.sift_down(0)?;
                continue;
            }
            let (batch, row) = self.batches[index].as_mut().expect("heap head");
            if result.handles.is_empty() {
                result.sort_keys = batch.sort_keys.empty_like(self.batch_size);
            }
            result.sort_keys.append_from(&batch.sort_keys, *row)?;
            result.handles.push(batch.handles[*row].clone());
            *row += 1;
            if *row == batch.handles.len() {
                self.batches[index] = None;
            }
            // Keep an empty head pending: do not read the next partition
            // batch after the caller's last requested handle.
            if self.batches[index].is_some() {
                self.sift_down(0)?;
            }
        }
        self.rows.set(self.rows.get() + result.handles.len() as u64);
        Ok((!result.handles.is_empty()).then_some(result))
    }
}

impl PartialHandleSource for PartitionedHandleSource {
    fn open(&mut self) -> Result<(), ExecError> {
        self.current = 0;
        self.opened.fill(false);
        self.initialized = false;
        self.key_comparators = MergeKeyComparators::default();
        self.heap.clear();
        self.batches.iter_mut().for_each(|batch| *batch = None);
        self.rows.set(0);
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError> {
        self.next_batch_with_max_handles(usize::MAX)
    }

    fn next_batch_with_max_handles(
        &mut self,
        maximum: usize,
    ) -> Result<Option<PartialHandleBatch>, ExecError> {
        if maximum == 0 {
            return Ok(None);
        }
        if !self.by_items.is_empty() {
            return self.next_ordered_batch(maximum);
        }
        while let Some(source) = self.sources.get_mut(self.current) {
            if !self.opened[self.current] {
                self.opened[self.current] = true;
                source.open()?;
            }
            if let Some(batch) = source.next_batch_with_max_handles(maximum)? {
                self.rows.set(self.rows.get() + batch.handles.len() as u64);
                return Ok(Some(batch));
            }
            self.opened[self.current] = false;
            source.close()?;
            self.current += 1;
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), ExecError> {
        let mut error = None;
        for (source, opened) in self.sources.iter_mut().zip(&mut self.opened) {
            if std::mem::take(opened) {
                if let Err(err) = source.close() {
                    error.get_or_insert(err);
                }
            }
        }
        self.heap.clear();
        self.batches.iter_mut().for_each(|batch| *batch = None);
        error.map_or(Ok(()), Err)
    }
}

/// A retained physical partial plan reduced to the handle batches consumed by
/// Go's index-merge process worker.
pub(crate) struct ExecutorPartialHandleSource {
    executor: Box<dyn Executor>,
    table: KvTable,
    decode_context: RowDecodeContext,
    field_types: Vec<FieldType>,
    handle_columns: PartialHandleColumns,
    sort_key_columns: Vec<usize>,
    retained_columns: Vec<usize>,
    memory_label: i64,
    memory: Option<MergeProcessMemory>,
    batch_size: usize,
    initial_batch_size: usize,
    max_batch_size: usize,
    partition_index: usize,
}

impl ExecutorPartialHandleSource {
    /// Wraps one fully-built partial physical tree. Column slots have already
    /// been resolved against the retained plan schema by the physical builder.
    #[must_use]
    pub(crate) fn new(
        executor: Box<dyn Executor>,
        table: KvTable,
        decode_context: RowDecodeContext,
        handle_columns: PartialHandleColumns,
        sort_key_columns: Vec<usize>,
    ) -> Self {
        let field_types = executor.ret_field_types().to_vec();
        let batch_size = executor.max_chunk_size().max(1);
        Self {
            executor,
            table,
            decode_context,
            field_types,
            handle_columns,
            retained_columns: sort_key_columns.clone(),
            memory_label: 0,
            memory: None,
            sort_key_columns,
            batch_size,
            initial_batch_size: batch_size,
            max_batch_size: batch_size,
            partition_index: 0,
        }
    }

    pub(crate) fn with_memory_label(mut self, label: i64) -> Self {
        self.memory_label = label;
        self
    }

    /// Go getRetTpsForIndexScan: ordering keys, then every handle component,
    /// then the physical ID when the retained request includes it. Do not
    /// deduplicate overlapping slots: those are separate output columns in Go.
    pub(crate) fn with_index_row_layout(mut self, physical_id_column: Option<usize>) -> Self {
        self.retained_columns = self.sort_key_columns.clone();
        match &self.handle_columns {
            PartialHandleColumns::Int(column) => self.retained_columns.push(*column),
            PartialHandleColumns::Common(columns) => self.retained_columns.extend(columns),
        }
        self.retained_columns.extend(physical_id_column);
        self
    }

    /// Go's partial worker grows complete handle tasks independently of the
    /// executor chunk size, capped by IndexLookupSize.
    pub(crate) fn with_batch_sizes(mut self, initial: usize, maximum: usize) -> Self {
        self.max_batch_size = maximum.max(1);
        self.initial_batch_size = initial.clamp(1, self.max_batch_size);
        self.batch_size = self.initial_batch_size;
        self
    }

    pub(crate) fn with_partition_index(mut self, partition_index: usize) -> Self {
        self.partition_index = partition_index;
        self
    }

    fn handle_of(&self, row: tidb_chunk::row::Row<'_>) -> Result<TableHandle, ExecError> {
        match &self.handle_columns {
            PartialHandleColumns::Int(column) => {
                match row.get_datum(*column, &self.field_types[*column]) {
                    Datum::Int(handle) => Ok(TableHandle::Int(handle)),
                    Datum::UInt(handle) => Ok(TableHandle::Int(handle as i64)),
                    other => Err(ExecError::unsupported(format!(
                        "an index-merge partial plan emitted a non-integer handle: {other:?}"
                    ))),
                }
            }
            PartialHandleColumns::Common(columns) => {
                let values = columns
                    .iter()
                    .map(|column| row.get_datum(*column, &self.field_types[*column]))
                    .collect::<Vec<_>>();
                self.table
                    .common_handle_from_values(&values, self.decode_context.zone())
                    .map_err(|error| {
                        ExecError::unsupported(format!(
                            "an index-merge common handle failed to encode: {error:?}"
                        ))
                    })
            }
        }
    }
}

impl PartialHandleSource for ExecutorPartialHandleSource {
    fn open(&mut self) -> Result<(), ExecError> {
        self.batch_size = self.initial_batch_size;
        self.memory = Some(MergeProcessMemory::new(
            &self.decode_context.expression().statement_memory(),
            self.memory_label,
        ));
        if let Err(error) = self.executor.open() {
            self.memory = None;
            return Err(error);
        }
        Ok(())
    }

    fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError> {
        self.next_batch_with_max_handles(usize::MAX)
    }

    fn next_batch_with_max_handles(
        &mut self,
        maximum: usize,
    ) -> Result<Option<PartialHandleBatch>, ExecError> {
        let target = self.batch_size.min(maximum);
        if target == 0 {
            return Ok(None);
        }
        let mut scratch = MergeScratchMemory {
            memory: self.memory.as_ref().expect("partial source is open"),
            bytes: 0,
        };
        let mut chunk = self.executor.new_chunk();
        let mut handles = Vec::with_capacity(target);
        let mut sort_keys = if self.sort_key_columns.is_empty() {
            MergeSortKeys::default()
        } else {
            MergeSortKeys::typed(
                self.retained_columns.iter().map(|column| self.field_types[*column].clone()).collect(),
                self.batch_size,
            )
        };
        while handles.len() < target {
            chunk.set_required_rows(
                (target - handles.len()) as isize,
                self.executor.max_chunk_size(),
            );
            self.executor.next(&mut chunk)?;
            if chunk.num_rows() == 0 {
                return Ok(
                    (!handles.is_empty()).then_some(PartialHandleBatch { handles, sort_keys })
                );
            }
            scratch.consume(chunk.memory_usage())?;
            for row_index in 0..chunk.num_rows().min(maximum - handles.len()) {
                let row = chunk.get_row(row_index);
                handles.push(HandleRef {
                    handle: self.handle_of(row)?,
                    partition_index: self.partition_index,
                });
                if let MergeSortKeys::Typed { chunk: keys, .. } = &mut sort_keys {
                    keys.append_row_by_col_idxs(row, Some(&self.retained_columns));
                }
            }
        }
        if handles.len() >= self.batch_size {
            self.batch_size = self.batch_size.saturating_mul(2).min(self.max_batch_size);
        }
        Ok(Some(PartialHandleBatch { handles, sort_keys }))
    }

    fn close(&mut self) -> Result<(), ExecError> {
        let result = self.executor.close();
        self.memory = None;
        result
    }
}

/// One `plannerutil.ByItems` entry, reduced to what `handleHeap.Less` (:1031)
/// needs: the comparison Go picks with `chunk.GetCompareFunc(keyType)` and the
/// `Desc` flag that negates it.
#[derive(Clone, Copy, Debug)]
pub struct MergeByItem {
    /// The key column's derived collation, as [`crate::sort`] uses.
    pub collation: Collation,
    /// Go `ByItems.Desc`.
    pub desc: bool,
}

/// Compile Go's typed key comparators once for a process/partition worker.
/// Retain the originating types so heterogeneous fixture rows can safely use
/// the existing Datum comparator instead of interpreting a different layout.
#[derive(Default)]
struct MergeKeyComparators {
    compiled: std::sync::OnceLock<CompiledMergeComparators>,
}

struct CompiledMergeComparators {
    fields: Vec<FieldType>,
    functions: Vec<Option<tidb_chunk::compare::ColumnCompareFunc>>,
}

fn compare_merge_keys(
    comparators: &MergeKeyComparators,
    by_items: &[MergeByItem],
    a: (&MergeSortKeys, usize),
    b: (&MergeSortKeys, usize),
) -> Result<std::cmp::Ordering, ExecError> {
    let typed = match (a.0, b.0) {
        (
            MergeSortKeys::Typed {
                chunk: left,
                fields: left_fields,
            },
            MergeSortKeys::Typed {
                chunk: right,
                fields: right_fields,
            },
        ) if left_fields.len() >= by_items.len() && right_fields.len() >= by_items.len() => {
            let compiled = comparators
                .compiled
                .get_or_init(|| CompiledMergeComparators {
                    fields: left_fields[..by_items.len()].to_vec(),
                    functions: left_fields
                        .iter()
                        .zip(by_items)
                        .map(|(field, item)| {
                            let mut field = field.clone();
                            field.set_collation(item.collation);
                            tidb_chunk::compare::get_column_compare_func(&field)
                        })
                        .collect(),
                });
            (left_fields[..by_items.len()] == compiled.fields
                && right_fields[..by_items.len()] == compiled.fields)
                .then_some((left, right, &compiled.functions))
        }
        _ => None,
    };
    for (i, item) in by_items.iter().enumerate() {
        let mut order = if let Some((left, right, functions)) = typed {
            if let Some(compare) = &functions[i] {
                compare(&left.column(i), a.1, &right.column(i), b.1)
            } else {
                let left = a.0.datum(a.1, i).expect("typed key column");
                let right = b.0.datum(b.1, i).expect("typed key column");
                tidb_expr::compare_datums_with_collation(&left, &right, item.collation)?
            }
        } else {
            let (Some(left), Some(right)) = (a.0.datum(a.1, i), b.0.datum(b.1, i)) else {
                return Err(ExecError::internal(
                    "index merge by-item key is shorter than the by-item list",
                ));
            };
            tidb_expr::compare_datums_with_collation(&left, &right, item.collation)?
        };
        if item.desc {
            order = order.reverse();
        }
        if !order.is_eq() {
            return Ok(order);
        }
    }
    Ok(std::cmp::Ordering::Equal)
}

/// Go rowIdx. A global task ordinal replaces the pair of partial/task ordinals
/// because this owner receives each task once and never reorders its storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct MergeRowIndex {
    task: usize,
    row: usize,
}

/// Go `handleHeap` (:1016).
///
/// Go keeps a `container/heap` whose `Less` is INVERTED for an ascending
/// by-item, so `heap.Pop` yields the currently *largest* key and popping while
/// `Len() > requiredCnt` retains the smallest `requiredCnt` keys. This keeps
/// the same binary-heap invariant as Go, with logarithmic insertion and
/// eviction instead of scanning the entire retained set for each row.
struct HandleHeap {
    /// Go `requiredCnt`; `0` means "keep everything".
    required_cnt: usize,
    by_items: Vec<MergeByItem>,
    key_comparators: MergeKeyComparators,
    /// Go taskMap: even duplicate and evicted rows remain owned until drain.
    tasks: Vec<PartialHandleBatch>,
    /// Heap entries borrow rows by stable indices, not Rust references, so
    /// growing task storage cannot invalidate them.
    entries: Vec<MergeRowIndex>,
    // Drop retained storage before detaching the process tracker.
    memory: Option<MergeProcessMemory>,
}

impl HandleHeap {
    fn new(by_items: Vec<MergeByItem>, pushed_limit: Option<PushedDownLimit>) -> Self {
        // Go separates the logical retention bound from its initial
        // allocation (issue 70910). Large LIMITs must retain every requested
        // candidate without preallocating their entire, possibly huge bound.
        let required_cnt =
            pushed_limit.map_or(0, |limit| limit.count.saturating_add(limit.offset) as usize);
        HandleHeap {
            required_cnt,
            by_items,
            key_comparators: MergeKeyComparators::default(),
            tasks: Vec::new(),
            entries: Vec::with_capacity(required_cnt.min(1024)),
            memory: None,
        }
    }

    fn with_memory(mut self, memory: &crate::StatementMemory, label: i64) -> Self {
        self.memory = Some(MergeProcessMemory::new(memory, label));
        self
    }

    fn consume(&self, bytes: i64) -> Result<(), ExecError> {
        self.memory
            .as_ref()
            .map_or(Ok(()), |memory| memory.consume(bytes))
    }

    fn charge_task(&self, task: usize) -> Result<(), ExecError> {
        // Materialized algebra fixtures have no producer-owned chunk. Real
        // ordered partial sources always provide the typed retained layout.
        if let MergeSortKeys::Typed { chunk, .. } = &self.tasks[task].sort_keys {
            self.consume(chunk.memory_usage())?;
        }
        Ok(())
    }

    /// Go `handleHeap.Less`, with the sign convention spelled out: the raw
    /// comparison is negated for an ASCENDING item, so "less" means "sorts
    /// later".
    fn less(&self, a: MergeRowIndex, b: MergeRowIndex) -> Result<bool, ExecError> {
        compare_merge_keys(
            &self.key_comparators,
            &self.by_items,
            (&self.tasks[a.task].sort_keys, a.row),
            (&self.tasks[b.task].sort_keys, b.row),
        )
        .map(|order| order.is_gt())
    }

    fn retain_task(&mut self, task: PartialHandleBatch) -> usize {
        let index = self.tasks.len();
        self.tasks.push(task);
        index
    }

    /// Go `heap.Push` followed by the `Len() > requiredCnt` eviction (:1150).
    ///
    /// Returns `true` when the value just pushed is the one evicted -- Go's
    /// `top == the row just pushed` test, which is what lets it mark a partial
    /// path `useless`. The eviction itself is what matters; the `uselessMap`
    /// early-exit is a scan-shortening optimization over an already-sorted
    /// path and cannot change the retained set.
    fn push(&mut self, index: MergeRowIndex) -> Result<bool, ExecError> {
        self.entries.push(index);
        // Go charges unsafe.Sizeof(h.idx), a three-word slice header, for
        // each Push/Pop. This is its accounting contract, not Rust slot size.
        self.consume(24)?;
        let mut position = self.entries.len() - 1;
        while position > 0 {
            let parent = (position - 1) / 2;
            if !self.less(self.entries[position], self.entries[parent])? {
                break;
            }
            self.entries.swap(position, parent);
            position = parent;
        }
        if self.required_cnt == 0 || self.entries.len() <= self.required_cnt {
            return Ok(false);
        }
        let pushed = position == 0;
        self.pop()?;
        Ok(pushed)
    }

    /// Go heap.Pop: remove the largest ordering key and restore the heap.
    fn pop(&mut self) -> Result<MergeRowIndex, ExecError> {
        let result = self.entries.swap_remove(0);
        let mut parent = 0;
        while parent * 2 + 1 < self.entries.len() {
            let mut child = parent * 2 + 1;
            if child + 1 < self.entries.len()
                && self.less(self.entries[child + 1], self.entries[child])?
            {
                child += 1;
            }
            if !self.less(self.entries[child], self.entries[parent])? {
                break;
            }
            self.entries.swap(parent, child);
            parent = child;
        }
        self.consume(-24)?;
        Ok(result)
    }

    /// Go's final drain (:1172): pop `needCount` times into `fhs` back to
    /// front, which leaves the survivors in by-item order with the smallest
    /// `Offset` of them dropped.
    fn drain_sorted_handles(
        &mut self,
        pushed_limit: Option<PushedDownLimit>,
    ) -> Result<Vec<HandleRef>, ExecError> {
        let len = self.entries.len();
        let need = match pushed_limit {
            Some(limit) => len.saturating_sub(limit.offset as usize),
            None => len,
        };
        if need == 0 {
            return Ok(Vec::new());
        }
        let mut handles = Vec::with_capacity(need);
        for _ in 0..need {
            let index = self.pop()?;
            handles.push(self.tasks[index.task].handles[index.row].clone());
        }
        // Go fills the output backward while popping the largest keys.
        handles.reverse();
        Ok(handles)
    }
}

/// A merge worker's tracker and canonical statement cancellation authority.
/// Rust ownership mirrors Go's deferred Detach, including early error exits.
struct MergeProcessMemory {
    tracker: std::sync::Arc<tidb_util::memory::Tracker>,
    memory: crate::StatementMemory,
}

impl MergeProcessMemory {
    fn new(memory: &crate::StatementMemory, label: i64) -> Self {
        Self {
            tracker: memory.operator_tracker(label),
            memory: memory.clone(),
        }
    }

    fn consume(&self, bytes: i64) -> Result<(), ExecError> {
        self.tracker.consume(bytes);
        self.memory.check()
    }
}

impl Drop for MergeProcessMemory {
    fn drop(&mut self) {
        self.tracker.detach();
    }
}

/// Go extractTaskHandles defers releasing the sum of fetched chunk charges,
/// while the worker tracker itself survives for later extraction calls.
struct MergeScratchMemory<'a> {
    memory: &'a MergeProcessMemory,
    bytes: i64,
}

impl MergeScratchMemory<'_> {
    fn consume(&mut self, bytes: i64) -> Result<(), ExecError> {
        self.bytes = self.bytes.saturating_add(bytes);
        self.memory.consume(bytes)
    }
}

impl Drop for MergeScratchMemory<'_> {
    fn drop(&mut self) {
        // Cleanup must release the charge even when cancellation is latched.
        self.memory.tracker.consume(-self.bytes);
    }
}

/// Go intersectionProcessWorker's batched map/counter accounting. Detach on
/// every exit, including quota errors, after the owned maps have been dropped.
struct IntersectionProcessMemory {
    tracker: MergeProcessMemory,
    rows: usize,
    delta: i64,
    batch_size: usize,
}

impl IntersectionProcessMemory {
    fn new(memory: &crate::StatementMemory, label: i64, batch_size: usize) -> Self {
        Self {
            tracker: MergeProcessMemory::new(memory, label),
            rows: 0,
            delta: 0,
            batch_size,
        }
    }

    fn insert(&mut self, map_delta: i64, extra: u64) {
        self.rows += 1;
        self.delta = self
            .delta
            .saturating_add(map_delta)
            .saturating_add(i64::try_from(extra).unwrap_or(i64::MAX));
    }

    fn finish_batch(&mut self) -> Result<(), ExecError> {
        if self.rows >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), ExecError> {
        let counters = i64::try_from(self.rows)
            .unwrap_or(i64::MAX)
            .saturating_mul(8);
        let bytes = self.delta.saturating_add(counters);
        self.rows = 0;
        self.delta = 0;
        self.tracker.consume(bytes)
    }
}

/// Persistent state of Go's union process worker between table tasks.
struct UnionProcess {
    partial: usize,
    seen: MergeHandleSet,
    pushed_limit: Option<PushedDownLimit>,
    initial_partial_budget: usize,
    remaining: usize,
    memory: MergeProcessMemory,
}

impl UnionProcess {
    fn new(limit: Option<PushedDownLimit>, memory: &crate::StatementMemory, label: i64) -> Self {
        let budget = limit.map_or(usize::MAX, |limit| {
            limit.offset.saturating_add(limit.count) as usize
        });
        Self {
            partial: 0,
            seen: MergeHandleSet::default(),
            pushed_limit: limit,
            initial_partial_budget: budget,
            remaining: budget,
            memory: MergeProcessMemory::new(memory, label),
        }
    }
}

/// Completed table-worker result. Stable row indices refer into owned chunks;
/// all backing storage and charges survive until replacement or reader Close.
struct MergeTableTask {
    handles: Vec<HandleRef>,
    chunks: Vec<Chunk>,
    rows: Vec<(usize, usize)>,
    cursor: usize,
    memory: MergeProcessMemory,
}

/// Go `IndexMergeReaderExecutor` (:89).
///
/// The partial paths are the union or intersection operands; the table is the
/// one every path indexes, and every surviving handle is read from it.
pub struct IndexMergeReaderExec {
    meta: ExecutorMeta,
    /// Go `table`.
    table: KvTable,
    /// The statement-class row decoding [`HandleSourceExec`] needs; this tier
    /// carries it where Go reads `sessionctx`.
    decode_context: RowDecodeContext,
    /// Go `partialPlans`, reduced to their handle output.
    partials: Vec<Box<dyn PartialHandleSource>>,
    /// Prefix whose Open was attempted and still needs teardown.
    opened_partials: usize,
    /// Go `isIntersection`.
    is_intersection: bool,
    /// Go `byItems`. Empty selects `fetchLoopUnion`; non-empty selects
    /// `fetchLoopUnionWithOrderBy`.
    by_items: Vec<MergeByItem>,
    /// Go `pushedLimit`.
    ///
    /// This is the one place the sequential drain is observable: with a union
    /// and a pushed limit but no `byItems`, WHICH handles survive depends on
    /// the order batches reach the process worker, and Go's order is the
    /// goroutine schedule. Go does not promise a particular set here either --
    /// a `LIMIT` without `ORDER BY` has no promised membership in MySQL or in
    /// TiDB -- so draining path 0 first is a legal outcome, not a divergence.
    pushed_limit: Option<PushedDownLimit>,
    /// The retained table reader's exact output mapping. Go builds the final
    /// table reader from `TablePlan`; the mapping is its pruned schema.
    output_columns: Option<Vec<HandleOutputColumn>>,
    /// Scan input and retained operator tree for Go's per-task table request.
    table_source_schema: Option<Schema>,
    table_task_builder: Option<TableTaskBuilder>,
    /// Go len(tblPlans) == 1: no table-side operator can remove rows.
    table_plan_is_scan: bool,
    /// Go `sessionVars.IndexLookupSize`: the handle count per table task.
    batch_size: usize,
    /// Go `workerStarted`.
    started: bool,
    /// Go `workCh`/`resultCh` contents, in creation order.
    tasks: VecDeque<Vec<HandleRef>>,
    /// Unordered union emits one task on demand instead of queuing its full input.
    union_process: Option<UnionProcess>,
    /// Go resultCurr: completed rows retained until replacement or Close.
    current: Option<MergeTableTask>,
}

impl IndexMergeReaderExec {
    /// Builds the reader. `partials` are the access paths in plan order --
    /// intersection requires a handle to appear on ALL of them, so the count
    /// is load-bearing (Go: `*val == len(w.indexMerge.partialPlans)`, :1483).
    #[must_use]
    pub fn new(
        meta: ExecutorMeta,
        table: KvTable,
        decode_context: RowDecodeContext,
        partials: Vec<Box<dyn PartialHandleSource>>,
        is_intersection: bool,
    ) -> Self {
        IndexMergeReaderExec {
            meta,
            table,
            decode_context,
            partials,
            opened_partials: 0,
            is_intersection,
            by_items: Vec::new(),
            pushed_limit: None,
            output_columns: None,
            table_source_schema: None,
            table_task_builder: None,
            table_plan_is_scan: true,
            batch_size: 20_000,
            started: false,
            tasks: VecDeque::new(),
            union_process: None,
            current: None,
        }
    }

    /// Go `pushedLimit`.
    #[must_use]
    pub fn with_pushed_limit(mut self, limit: PushedDownLimit) -> Self {
        self.pushed_limit = Some(limit);
        self
    }

    /// Applies the retained final table plan's output projection.
    #[must_use]
    pub(crate) fn with_output_columns(mut self, output_columns: Vec<HandleOutputColumn>) -> Self {
        self.output_columns = Some(output_columns);
        self
    }

    /// Preserve the table request tree, including its operator order, for
    /// every handle batch. Each invocation gets fresh task-local state.
    pub(crate) fn with_table_plan(
        mut self,
        schema: Schema,
        builder: TableTaskBuilder,
        table_plan_is_scan: bool,
    ) -> Self {
        self.table_source_schema = Some(schema);
        self.table_task_builder = Some(builder);
        self.table_plan_is_scan = table_plan_is_scan;
        self
    }

    /// Go `byItems`; only meaningful for a union, because Go's
    /// `fetchLoopIntersectionWithOrderBy` (:1569) is an unimplemented `todo`.
    #[must_use]
    pub fn with_by_items(mut self, by_items: Vec<MergeByItem>) -> Self {
        self.by_items = by_items;
        self
    }

    /// Go `sessionVars.IndexLookupSize`.
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Start partial sources. Unordered union streams tasks; ordered union
    /// and intersection finish their collection phase before probing rows.
    fn start_workers(&mut self) -> Result<(), ExecError> {
        self.started = true;
        let memory = self.decode_context.expression().statement_memory();
        let result = (|| {
            for partial in &mut self.partials {
                memory.check()?;
                self.opened_partials += 1;
                partial.open()?;
            }
            if self.is_intersection {
                if !self.by_items.is_empty() {
                    return Err(ExecError::unsupported(
                        "index merge intersection with an order-by is not implemented (Go: fetchLoopIntersectionWithOrderBy)",
                    ));
                }
                self.fetch_loop_intersection().map(Some)
            } else if self.by_items.is_empty() {
                self.union_process = Some(UnionProcess::new(
                    self.pushed_limit,
                    &self.decode_context.expression().statement_memory(),
                    self.meta.id(),
                ));
                Ok(None)
            } else {
                self.fetch_loop_union_with_order_by().map(Some)
            }
        })();
        // Streaming union owns the open sources until exhaustion, error or
        // Close. Collection modes and failed startup release them here.
        if matches!(result, Ok(None)) {
            return Ok(());
        }
        let closed = self.close_partials();
        let handles = result?;
        closed?;
        self.tasks.extend(handles.into_iter().flatten());
        Ok(())
    }

    fn next_table_task(&mut self) -> Result<Option<Vec<HandleRef>>, ExecError> {
        if self.union_process.is_none() {
            return Ok(self.tasks.pop_front());
        }
        let result = self.next_union_task();
        if matches!(result, Ok(Some(_))) {
            return result;
        }
        self.union_process = None;
        let closed = self.close_partials();
        let task = result?;
        closed?;
        Ok(task)
    }

    fn close_partials(&mut self) -> Result<(), ExecError> {
        let mut error = None;
        let opened = std::mem::take(&mut self.opened_partials);
        for partial in &mut self.partials[..opened] {
            if let Err(err) = partial.close() {
                error.get_or_insert(err);
            }
        }
        error.map_or(Ok(()), Err)
    }

    /// Go `indexMergeProcessWorker.fetchLoopUnion` (:1245).
    ///
    /// First writer wins: a handle already in `hMap` is dropped, so a handle
    /// on several paths is read once. The pushed limit is applied to each
    /// batch AFTER dedup, which is why an entirely-duplicate batch does not
    /// consume any of the offset (Go `continue`s on `len(fhs) == 0` before
    /// touching `pushedLimit`).
    fn next_union_task(&mut self) -> Result<Option<Vec<HandleRef>>, ExecError> {
        let memory = self.decode_context.expression().statement_memory();
        let process = self.union_process.as_mut().expect("active union process");
        loop {
            memory.check()?;
            if process.pushed_limit.is_some_and(|limit| limit.count == 0) {
                return Ok(None);
            }
            let Some(partial) = self.partials.get_mut(process.partial) else {
                return Ok(None);
            };
            let batch = partial.next_batch_with_max_handles(process.remaining)?;
            memory.check()?;
            let Some(batch) = batch.filter(|batch| !batch.handles.is_empty()) else {
                process.partial += 1;
                process.remaining = process.initial_partial_budget;
                continue;
            };
            // Go charges raw task capacity before dedup or output LIMIT.
            // Keep the charge until the process exits, including skipped
            // duplicate-only batches and capacity beyond the populated rows.
            let bytes = i64::try_from(batch.handles.capacity())
                .unwrap_or(i64::MAX)
                .saturating_mul(8);
            process.memory.consume(bytes)?;
            process.remaining = process.remaining.saturating_sub(batch.handles.len());
            let mut fresh = Vec::with_capacity(batch.handles.len());
            for handle in batch.handles {
                if process.seen.insert(&handle)? {
                    fresh.push(handle);
                }
            }
            if fresh.is_empty() {
                continue;
            }
            if let Some(limit) = process.pushed_limit.as_mut() {
                let (skip, kept) = pushed_limit_counting_down(limit, fresh);
                if skip {
                    continue;
                }
                fresh = kept;
            }
            return Ok(Some(fresh));
        }
    }

    /// Collect the same state transitions for dependency-closed algebra tests.
    #[cfg(test)]
    fn fetch_loop_union(&mut self) -> Result<Vec<Vec<HandleRef>>, ExecError> {
        self.union_process = Some(UnionProcess::new(
            self.pushed_limit,
            &self.decode_context.expression().statement_memory(),
            self.meta.id(),
        ));
        let mut tasks = Vec::new();
        while let Some(task) = self.next_union_task()? {
            tasks.push(task);
        }
        self.union_process = None;
        Ok(tasks)
    }

    /// Go `indexMergeProcessWorker.fetchLoopUnionWithOrderBy` (:1111).
    ///
    /// Every distinct handle across all paths enters the heap keyed by its
    /// by-item values; the heap keeps at most `requiredCnt` of them, and the
    /// survivors leave in by-item order, batched into `IndexLookupSize` tasks
    /// (Go additionally records `indexOrder` so the table worker can restore
    /// that order after the coprocessor reorders the batch -- see the module
    /// header for why this port needs no such restoration).
    fn fetch_loop_union_with_order_by(&mut self) -> Result<Vec<Vec<HandleRef>>, ExecError> {
        let memory = self.decode_context.expression().statement_memory();
        let mut heap = HandleHeap::new(self.by_items.clone(), self.pushed_limit)
            .with_memory(&memory, self.meta.id());
        let partition_handle_bytes = if self.table.partition().is_some() {
            24
        } else {
            0
        };
        let mut distinct = MergeHandleSet::default();
        for partial in &mut self.partials {
            let mut remaining = self.pushed_limit.map_or(usize::MAX, |limit| {
                limit.offset.saturating_add(limit.count) as usize
            });
            'path: loop {
                memory.check()?;
                let batch = partial.next_batch_with_max_handles(remaining)?;
                memory.check()?;
                let Some(batch) = batch else {
                    break;
                };
                if batch.handles.is_empty() {
                    break;
                }
                remaining = remaining.saturating_sub(batch.handles.len());
                if batch.sort_keys.len() != batch.handles.len() {
                    return Err(ExecError::internal(
                        "index merge order-by needs one sort key per handle",
                    ));
                }
                let task = heap.retain_task(batch);
                let mut useless = false;
                for row in 0..heap.tasks[task].handles.len() {
                    let (inserted, usage) =
                        distinct.insert_with_usage(&heap.tasks[task].handles[row])?;
                    if inserted && heap.push(MergeRowIndex { task, row })? {
                        // Go breaks before charging the just-evicted handle,
                        // but still charges the whole retained chunk below.
                        useless = true;
                        break;
                    }
                    heap.consume(
                        i64::try_from(usage)
                            .unwrap_or(i64::MAX)
                            .saturating_add(partition_handle_bytes),
                    )?;
                }
                heap.charge_task(task)?;
                if useless {
                    break 'path;
                }
            }
        }
        let handles = heap.drain_sorted_handles(self.pushed_limit)?;
        Ok(handles
            .chunks(self.batch_size)
            .map(<[HandleRef]>::to_vec)
            .collect())
    }

    /// Go `indexMergeProcessWorker.fetchLoopIntersection` (:1577) together
    /// with `intersectionProcessWorker.doIntersectionPerPartition` (:1433) and
    /// `intersectionCollectWorker.doIntersectionLimitAndDispatch` (:1360).
    ///
    /// A handle survives when its occurrence count equals the number of
    /// partial paths. Note what this counts: `doIntersectionPerPartition`
    /// increments once per OCCURRENCE, not once per distinct path, so a path
    /// that yields the same handle twice contributes twice. That is Go's
    /// behavior and it is safe only because each path's handles are already
    /// distinct (an index scan visits each entry once); reproduced literally
    /// rather than de-duplicated per path.
    fn fetch_loop_intersection(&mut self) -> Result<Vec<Vec<HandleRef>>, ExecError> {
        let memory = self.decode_context.expression().statement_memory();
        let path_count = self.partials.len();
        let mut accounting =
            IntersectionProcessMemory::new(&memory, self.meta.id(), self.batch_size);
        let mut counts: BTreeMap<
            usize,
            tidb_txnkv::MemAwareHandleMap<Box<std::cell::Cell<usize>>>,
        > = BTreeMap::new();
        for partial in &mut self.partials {
            loop {
                memory.check()?;
                let batch = partial.next_batch()?;
                memory.check()?;
                let Some(batch) = batch else {
                    break;
                };
                if batch.handles.is_empty() {
                    break;
                }
                for handle in batch.handles {
                    // Physical partition identity is already resolved by the
                    // partial source, including global-index handles.
                    let map = counts.entry(handle.partition_index).or_default();
                    let key = merge_handle_key(handle.handle)?;
                    if let Some(count) = map.get(&key) {
                        count.set(count.get() + 1);
                    } else {
                        let extra = key.extra_mem_size();
                        let delta = map.set(key, Box::new(std::cell::Cell::new(1)));
                        accounting.insert(delta, extra);
                    }
                }
                accounting.finish_batch()?;
            }
        }
        accounting.flush()?;
        // Like Go, do not charge the small intersection result separately.
        // Each partition owns an independent handle identity/count domain.
        let mut per_partition = Vec::with_capacity(counts.len());
        for (partition_index, map) in &counts {
            let mut survivors = Vec::new();
            map.range(|handle, count| {
                if count.get() == path_count {
                    let handle = match handle.int_value() {
                        Some(value) => TableHandle::Int(value),
                        None => TableHandle::Common(handle.encoded()),
                    };
                    survivors.push(HandleRef {
                        partition_index: *partition_index,
                        handle,
                    });
                }
                true
            });
            per_partition.push((*partition_index, survivors));
        }
        let mut pushed_limit = self.pushed_limit;
        let mut tasks = Vec::new();
        for (_, group) in per_partition {
            for chunk in group.chunks(self.batch_size) {
                memory.check()?;
                let mut handles = chunk.to_vec();
                if let Some(limit) = pushed_limit.as_mut() {
                    if limit.count == 0 {
                        return Ok(tasks);
                    }
                    let (next, kept) = pushed_limit_counting_down(limit, handles);
                    if next {
                        continue;
                    }
                    handles = kept;
                }
                tasks.push(handles);
            }
        }
        Ok(tasks)
    }

    fn execute_table_task(&self, handles: Vec<HandleRef>) -> Result<MergeTableTask, ExecError> {
        let mut reader = self.build_final_table_reader(&handles)?;
        let memory = self.decode_context.expression().statement_memory();
        let mut task = MergeTableTask {
            rows: Vec::with_capacity(handles.len()),
            handles,
            chunks: Vec::new(),
            cursor: 0,
            memory: MergeProcessMemory::new(&memory, self.meta.id()),
        };
        let result: Result<(), ExecError> = (|| {
            reader.open()?;
            task.memory.consume(
                i64::try_from(task.handles.capacity())
                    .unwrap_or(i64::MAX)
                    .saturating_mul(8),
            )?;
            loop {
                memory.check()?;
                let mut chunk = reader.new_chunk();
                reader.next(&mut chunk)?;
                memory.check()?;
                if chunk.num_rows() == 0 {
                    break;
                }
                task.memory.consume(chunk.memory_usage())?;
                let index = task.chunks.len();
                task.rows
                    .extend((0..chunk.num_rows()).map(|row| (index, row)));
                task.chunks.push(chunk);
            }
            // Go chunk.Row is a chunk pointer and row index on this target.
            task.memory.consume(
                i64::try_from(task.rows.capacity())
                    .unwrap_or(i64::MAX)
                    .saturating_mul(16),
            )?;
            if self.table_plan_is_scan && task.handles.len() != task.rows.len() {
                return Err(ExecError::internal(format!(
                    "handle count {} isn't equal to value count {}",
                    task.handles.len(), task.rows.len(),
                )));
            }
            Ok(())
        })();
        // Go defers Close and logs its error; a cleanup error must not replace
        // the task's execution error or turn its successful rows into failure.
        if let Err(error) = reader.close() {
            tracing::warn!(?error, "index merge table reader close failed");
        }
        result?;
        Ok(task)
    }

    /// Go `buildFinalTableReader` (:854) + `executeTask` (:1988): the reader
    /// over one task's handles.
    fn build_final_table_reader(
        &self,
        handles: &[HandleRef],
    ) -> Result<Box<dyn Executor>, ExecError> {
        let meta = ExecutorMeta::new(
            self.table_source_schema
                .as_ref()
                .unwrap_or(self.meta.schema())
                .clone(),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        let physical_ids = self.table.record_physical_ids();
        let partition_ids = handles
            .iter()
            .map(|handle| {
                physical_ids
                    .get(handle.partition_index)
                    .copied()
                    .ok_or_else(|| {
                        ExecError::internal("index-merge task references an unknown partition")
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let handles = handles.iter().map(|h| h.handle.clone()).collect();
        let source = match &self.output_columns {
            Some(columns) => HandleSourceExec::new_mapped_with_context(
                meta.clone(),
                self.table.clone(),
                handles,
                columns.clone(),
                self.decode_context.clone(),
            ),
            None => HandleSourceExec::new_with_context(
                meta.clone(),
                self.table.clone(),
                handles,
                self.decode_context.clone(),
            ),
        }
        .with_partition_ids(Some(partition_ids));
        match &self.table_task_builder {
            Some(build) => build(Box::new(source)),
            None => Ok(Box::new(source)),
        }
    }
}

impl Executor for IndexMergeReaderExec {
    /// Go `Open` (:174). The process worker is not started here: Go starts its
    /// goroutines lazily on the first `Next` (`if !e.workerStarted`), and so
    /// does this port.
    fn open(&mut self) -> Result<(), ExecError> {
        self.started = false;
        self.tasks.clear();
        self.union_process = None;
        self.current = None;
        Ok(())
    }

    /// Go `Next` (:880) + `getResultTask` (:907).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        let result = (|| {
            let memory = self.decode_context.expression().statement_memory();
            memory.check()?;
            if !self.started {
                self.start_workers()?;
            }
            req.reset();
            loop {
                memory.check()?;
                if let Some(task) = self.current.as_mut() {
                    let count = (task.rows.len() - task.cursor)
                        .min(self.meta.max_chunk_size() - req.num_rows());
                    for &(chunk, row) in &task.rows[task.cursor..task.cursor + count] {
                        req.append_row(task.chunks[chunk].get_row(row));
                    }
                    task.cursor += count;
                    if req.num_rows() >= self.meta.max_chunk_size() {
                        return Ok(());
                    }
                }
                let Some(handles) = self.next_table_task()? else {
                    return Ok(());
                };
                // Keep the exhausted previous task until the replacement is
                // complete, as Go getResultTask does while waiting on doneCh.
                self.current = Some(self.execute_table_task(handles)?);
            }
        })();
        if result.is_err() {
            // A table-task failure must stop the still-live union sources.
            // Preserve the execution error if teardown also fails.
            let _ = self.close();
        }
        result
    }

    fn close(&mut self) -> Result<(), ExecError> {
        self.current = None;
        self.tasks.clear();
        self.union_process = None;
        self.close_partials()
    }

    fn schema(&self) -> &Schema {
        self.meta.schema()
    }

    fn ret_field_types(&self) -> &[FieldType] {
        self.meta.ret_field_types()
    }

    fn init_cap(&self) -> usize {
        self.meta.init_cap()
    }

    fn max_chunk_size(&self) -> usize {
        self.meta.max_chunk_size()
    }

    fn new_chunk(&self) -> Chunk {
        self.meta.new_chunk()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// WRITTEN test (Go's own coverage of index merge runs through
    /// `testkit`/`tests/integrationtest`, which is not dependency-closed
    /// here): the handle algebra of the process worker, driven directly.
    fn int_handles(values: &[i64]) -> Vec<HandleRef> {
        values
            .iter()
            .map(|v| HandleRef::new(TableHandle::Int(*v)))
            .collect()
    }

    fn source(values: &[i64]) -> Box<dyn PartialHandleSource> {
        Box::new(MaterializedHandleSource::from_int_handles(values))
    }

    fn batched_source(batches: &[&[i64]]) -> Box<dyn PartialHandleSource> {
        Box::new(MaterializedHandleSource::new(
            batches
                .iter()
                .map(|b| PartialHandleBatch {
                    handles: int_handles(b),
                    sort_keys: MergeSortKeys::default(),
                })
                .collect(),
        ))
    }

    fn ordered_source(rows: &[(i64, i64)]) -> Box<dyn PartialHandleSource> {
        Box::new(MaterializedHandleSource::new(vec![PartialHandleBatch {
            handles: rows
                .iter()
                .map(|(h, _)| HandleRef::new(TableHandle::Int(*h)))
                .collect(),
            sort_keys: rows.iter().map(|(_, k)| vec![Datum::Int(*k)]).collect::<Vec<_>>().into(),
        }]))
    }

    /// A reader with no table behind it; only the process-worker stage runs.
    fn merge(
        partials: Vec<Box<dyn PartialHandleSource>>,
        intersection: bool,
    ) -> IndexMergeReaderExec {
        IndexMergeReaderExec::new(
            ExecutorMeta::new(Schema::new(Vec::new()), 1, 32, 1024),
            KvTable::new(1, Vec::new()),
            RowDecodeContext::for_test_query_utc(),
            partials,
            intersection,
        )
    }

    fn flatten(tasks: Vec<Vec<HandleRef>>) -> Vec<i64> {
        tasks
            .into_iter()
            .flatten()
            .map(|h| h.handle.int_value().expect("int handle"))
            .collect()
    }

    #[test]
    fn union_emits_every_handle_once_across_paths() {
        let mut exec = merge(vec![source(&[1, 3, 5]), source(&[3, 4, 5, 6])], false);
        let tasks = exec.fetch_loop_union().expect("union");
        // Path 0 first, then path 1's handles that path 0 did not already
        // claim: 3 and 5 are dropped as duplicates.
        assert_eq!(flatten(tasks), vec![1, 3, 5, 4, 6]);
    }

    #[test]
    fn union_drops_a_batch_that_is_entirely_duplicate() {
        let mut exec = merge(
            vec![source(&[1, 2]), batched_source(&[&[1, 2], &[7]])],
            false,
        );
        let tasks = exec.fetch_loop_union().expect("union");
        // The all-duplicate batch produces no task at all (Go `continue`s on
        // `len(fhs) == 0`), so the emitted tasks are [1,2] and [7].
        assert_eq!(tasks.len(), 2);
        assert_eq!(flatten(tasks), vec![1, 2, 7]);
    }

    #[test]
    fn ordered_merge_tracks_retained_chunks_handles_and_heap_until_exit() {
        for quota in [1, 40, 100, -1] {
            let memory = crate::StatementMemory::new(quota, crate::OomAction::Cancel, 7);
            let context = crate::StmtContext::for_query_with_memory(memory.clone());
            let fields = vec![FieldType::new(tidb_datatype::FieldTypeCode::LongLong)];
            let mut chunk = Chunk::new_with_capacity(&fields, 8);
            chunk.append_datum(0, &Datum::Int(1));
            chunk.append_datum(0, &Datum::Int(2));
            let chunk_bytes = chunk.memory_usage();
            let partial = || -> Box<dyn PartialHandleSource> {
                // Build producer chunks at their declared capacity. Chunk's
                // deep Clone may compact backing vectors to populated length.
                let mut chunk = Chunk::new_with_capacity(&fields, 8);
                chunk.append_datum(0, &Datum::Int(1));
                chunk.append_datum(0, &Datum::Int(2));
                Box::new(MaterializedHandleSource::new(vec![PartialHandleBatch {
                    handles: int_handles(&[1, 2]),
                    sort_keys: MergeSortKeys::Typed {
                        chunk,
                        fields: fields.clone(),
                    },
                }]))
            };
            let mut exec = merge(vec![partial(), partial()], false);
            exec.decode_context = RowDecodeContext::for_query(&context);
            exec.by_items = vec![MergeByItem {
                collation: Collation::Binary,
                desc: false,
            }];
            let result = exec.start_workers();
            if quota > 0 {
                assert!(
                    matches!(result, Err(ExecError::MemoryExceedForQuery { conn_id: 7 })),
                    "{result:?}"
                );
                assert_eq!(
                    memory.stmt_tracker().max_consumed(),
                    match quota { 1 => 24, 40 => 56, _ => 64 + chunk_bytes }
                );
            } else {
                result.unwrap();
                assert_eq!(flatten(exec.tasks.drain(..).collect()), vec![1, 2]);
                // Two unique heap entries, four incoming integer handles
                // (including duplicates), and both retained key chunks.
                assert_eq!(
                    memory.stmt_tracker().max_consumed(),
                    2 * 24 + 4 * 8 + 2 * chunk_bytes
                );
            }
            assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
            exec.close().unwrap();
        }
    }

    #[test]
    fn ordered_merge_eviction_still_charges_retained_chunk() {
        let memory = crate::StatementMemory::default();
        let context = crate::StmtContext::for_query_with_memory(memory.clone());
        let fields = vec![FieldType::new(tidb_datatype::FieldTypeCode::LongLong)];
        let bytes = Chunk::new_with_capacity(&fields, 8).memory_usage();
        let partial = |id: i64| -> Box<dyn PartialHandleSource> {
            let mut chunk = Chunk::new_with_capacity(&fields, 8);
            chunk.append_datum(0, &Datum::Int(id));
            Box::new(MaterializedHandleSource::new(vec![PartialHandleBatch {
                handles: int_handles(&[id]),
                sort_keys: MergeSortKeys::Typed {
                    chunk,
                    fields: fields.clone(),
                },
            }]))
        };
        let mut exec =
            merge(vec![partial(1), partial(3)], false).with_pushed_limit(PushedDownLimit {
                offset: 0,
                count: 1,
            });
        exec.decode_context = RowDecodeContext::for_query(&context);
        exec.by_items = vec![MergeByItem {
            collation: Collation::Binary,
            desc: false,
        }];
        exec.start_workers().unwrap();
        assert_eq!(flatten(exec.tasks.drain(..).collect()), vec![1]);
        // The second handle was pushed and popped before its own charge.
        // Its chunk remains retained, despite the sorted-path early exit.
        assert_eq!(memory.stmt_tracker().max_consumed(), 24 + 8 + 2 * bytes);
        assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
        exec.close().unwrap();
    }

    #[test]
    fn typed_merge_comparison_preserves_null_collation_and_direction() {
        let fields = vec![FieldType::new(tidb_datatype::FieldTypeCode::VarString)];
        let values = vec![
            Datum::Null,
            Datum::new_string("A"),
            Datum::new_string("a"),
            Datum::new_string("a "),
            Datum::new_string("é"),
            Datum::new_string("z".repeat(512)),
        ];
        let mut chunk = Chunk::new_with_capacity(&fields, values.len());
        for value in &values {
            chunk.append_datum(0, value);
        }
        let keys = MergeSortKeys::Typed { chunk, fields };
        for collation in [Collation::Binary, Collation::Utf8Mb4GeneralCi] {
            for desc in [false, true] {
                let compiled = MergeKeyComparators::default();
                let items = vec![MergeByItem { collation, desc }];
                for left in 0..values.len() {
                    for right in 0..values.len() {
                        let mut expected = tidb_expr::compare_datums_with_collation(
                            &values[left],
                            &values[right],
                            collation,
                        )
                        .unwrap();
                        if desc {
                            expected = expected.reverse();
                        }
                        assert_eq!(
                            compare_merge_keys(&compiled, &items, (&keys, left), (&keys, right))
                                .unwrap(),
                            expected
                        );
                    }
                }
                assert!(compiled.compiled.get().unwrap().functions[0].is_some());
            }
        }
    }

    #[test]
    fn ordered_partition_merge_preserves_typed_key_chunks() {
        let field = FieldType::new(tidb_datatype::FieldTypeCode::VarString);
        let source = |ids: &[i64], values: &[Datum]| -> Box<dyn PartialHandleSource> {
            let mut chunk = Chunk::new_with_capacity(std::slice::from_ref(&field), 8);
            for value in values {
                chunk.append_datum(0, value);
            }
            Box::new(MaterializedHandleSource::new(vec![PartialHandleBatch {
                handles: int_handles(ids),
                sort_keys: MergeSortKeys::Typed {
                    chunk,
                    fields: vec![field.clone()],
                },
            }]))
        };
        let mut partial = PartitionedHandleSource::new(vec![
            source(&[1, 3], &[Datum::Null, Datum::new_string("b")]),
            source(&[2, 4], &[Datum::new_string("a"), Datum::new_string("z")]),
        ])
        .with_order(
            vec![MergeByItem {
                collation: Collation::Binary,
                desc: false,
            }],
            2,
        );
        partial.open().unwrap();
        let mut handles = Vec::new();
        let mut values = Vec::new();
        while let Some(batch) = partial.next_batch().unwrap() {
            let MergeSortKeys::Typed { chunk, fields } = &batch.sort_keys else {
                panic!("partition merge discarded typed key storage");
            };
            assert_eq!(fields, std::slice::from_ref(&field));
            assert_eq!(chunk.capacity(), 2);
            assert_eq!(chunk.num_rows(), batch.handles.len());
            values
                .extend((0..batch.handles.len()).map(|row| batch.sort_keys.values(row)[0].clone()));
            handles.extend(batch.handles);
        }
        partial.close().unwrap();
        assert_eq!(handles, int_handles(&[1, 2, 3, 4]));
        assert_eq!(
            values,
            vec![
                Datum::Null,
                Datum::new_string("a"),
                Datum::new_string("b"),
                Datum::new_string("z")
            ]
        );
    }

    #[test]
    fn union_handle_identity_is_shared_across_modes_and_partitions() {
        let common = tidb_codec::encode_key(&[Datum::Int(7)]).unwrap();
        let handles = [0, 1]
            .into_iter()
            .flat_map(|partition_index| {
                [TableHandle::Int(7), TableHandle::Common(common.clone())]
                    .into_iter()
                    .map(move |handle| HandleRef {
                        partition_index,
                        handle,
                    })
            })
            .collect::<Vec<_>>();
        for ordered in [false, true] {
            let partial = || -> Box<dyn PartialHandleSource> {
                Box::new(MaterializedHandleSource::new(vec![PartialHandleBatch {
                    handles: handles.clone(),
                    sort_keys: (0..handles.len())
                        .map(|index| vec![Datum::Int(index as i64)])
                        .collect::<Vec<_>>().into(),
                }]))
            };
            let mut exec = merge(vec![partial(), partial()], false);
            let tasks = if ordered {
                exec.by_items = vec![MergeByItem {
                    desc: false,
                    collation: Collation::Binary,
                }];
                exec.fetch_loop_union_with_order_by().unwrap()
            } else {
                exec.fetch_loop_union().unwrap()
            };
            assert_eq!(tasks.into_iter().flatten().collect::<Vec<_>>(), handles);
        }
    }

    #[test]
    fn union_process_memory_lives_until_eof_error_or_close() {
        for quota in [20, -1] {
            let memory = crate::StatementMemory::new(quota, crate::OomAction::Cancel, 7);
            let context = crate::StmtContext::for_query_with_memory(memory.clone());
            let mut exec = merge(vec![batched_source(&[&[1, 2], &[1, 2], &[3]])], false);
            exec.decode_context = RowDecodeContext::for_query(&context);
            exec.start_workers().unwrap();
            assert_eq!(
                flatten(vec![exec.next_table_task().unwrap().unwrap()]),
                vec![1, 2]
            );
            assert_eq!(memory.stmt_tracker().bytes_consumed(), 16);
            let next = exec.next_table_task();
            if quota == 20 {
                assert!(
                    matches!(next, Err(ExecError::MemoryExceedForQuery { conn_id: 7 })),
                    "{next:?}"
                );
                assert_eq!(memory.stmt_tracker().max_consumed(), 32);
            } else {
                assert_eq!(flatten(vec![next.unwrap().unwrap()]), vec![3]);
                assert_eq!(memory.stmt_tracker().bytes_consumed(), 40);
                assert!(exec.next_table_task().unwrap().is_none());
            }
            assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
            exec.close().unwrap();
        }
        for finish in ["close", "drop", "cancel"] {
            let memory = crate::StatementMemory::default();
            let context = crate::StmtContext::for_query_with_memory(memory.clone());
            let mut handles = Vec::with_capacity(4);
            handles.push(HandleRef::new(TableHandle::Int(1)));
            let mut exec = merge(
                vec![Box::new(MaterializedHandleSource::new(vec![
                    PartialHandleBatch {
                        handles,
                        sort_keys: MergeSortKeys::default(),
                    },
                ]))],
                false,
            );
            exec.decode_context = RowDecodeContext::for_query(&context);
            exec.start_workers().unwrap();
            exec.next_table_task().unwrap().unwrap();
            // Go accounts input capacity, including duplicate batches and slack.
            assert_eq!(memory.stmt_tracker().bytes_consumed(), 32);
            match finish {
                "close" => exec.close().unwrap(),
                "drop" => drop(exec),
                _ => {
                    memory
                        .sql_killer()
                        .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
                    assert!(matches!(exec.next_table_task(), Err(ExecError::Killed(_))));
                    exec.close().unwrap();
                }
            }
            assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
        }
    }

    #[test]
    fn intersection_handle_memory_obeys_quota_and_releases_on_exit() {
        for batch_size in [1, 20_000] {
            for quota in [1, -1] {
                let memory = crate::StatementMemory::new(quota, crate::OomAction::Cancel, 7);
                let context = crate::StmtContext::for_query_with_memory(memory.clone());
                let handles = (0..32).collect::<Vec<_>>();
                let mut exec = merge(vec![source(&handles), source(&handles)], true)
                    .with_batch_size(batch_size);
                exec.decode_context = RowDecodeContext::for_query(&context);
                let result = exec.start_workers();
                if quota == 1 {
                    assert!(
                        matches!(result, Err(ExecError::MemoryExceedForQuery { conn_id: 7 })),
                        "{result:?}"
                    );
                    assert!(exec.tasks.is_empty());
                } else {
                    result.unwrap();
                    assert_eq!(exec.tasks.iter().map(Vec::len).sum::<usize>(), 32);
                }
                // Go charges 703 bytes for the two map checkpoints and 32
                // pointed-to int counters, then detaches the process tracker.
                assert_eq!(memory.stmt_tracker().max_consumed(), 703 + 32 * 8);
                assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
                exec.close().unwrap();
                assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
            }
        }
    }

    #[test]
    fn intersection_accounts_common_payloads_in_separate_partition_maps() {
        let memory = crate::StatementMemory::new(-1, crate::OomAction::Cancel, 7);
        let context = crate::StmtContext::for_query_with_memory(memory.clone());
        let mut expected = 2 * (694 + 1071) + 64 * 8;
        let mut handles = Vec::new();
        for partition_index in 0..2 {
            for index in 0..32 {
                let encoded = tidb_codec::encode_key(&[
                    Datum::Int(index),
                    Datum::new_string("x".repeat(100)),
                ])
                .unwrap();
                let common = tidb_txnkv::CommonHandle::new(encoded.clone()).unwrap();
                expected += common.extra_mem_size() as i64;
                handles.push(HandleRef {
                    partition_index,
                    handle: TableHandle::Common(encoded),
                });
            }
        }
        let partial = || {
            Box::new(MaterializedHandleSource::new(vec![PartialHandleBatch {
                handles: handles.clone(),
                sort_keys: MergeSortKeys::default(),
            }])) as Box<dyn PartialHandleSource>
        };
        let mut exec = merge(vec![partial(), partial()], true);
        exec.decode_context = RowDecodeContext::for_query(&context);
        exec.start_workers().unwrap();
        let actual = exec
            .tasks
            .iter()
            .flatten()
            .cloned()
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, handles.into_iter().collect());
        assert_eq!(memory.stmt_tracker().max_consumed(), expected);
        assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
        exec.close().unwrap();
    }

    #[test]
    fn intersection_keeps_only_handles_on_every_path() {
        let mut exec = merge(
            vec![
                source(&[1, 2, 3, 4]),
                source(&[2, 3, 5]),
                source(&[3, 2, 9]),
            ],
            true,
        );
        let tasks = exec.fetch_loop_intersection().expect("intersection");
        // Go's handle map does not promise an order within the intersection.
        let mut handles = flatten(tasks);
        handles.sort_unstable();
        assert_eq!(handles, vec![2, 3]);
    }

    #[test]
    fn intersection_of_one_path_is_that_path() {
        let mut exec = merge(vec![source(&[4, 1, 4])], true);
        let tasks = exec.fetch_loop_intersection().expect("intersection");
        // The repeated 4 counts twice, so it does NOT equal `path_count == 1`
        // and is dropped -- Go's occurrence counting, reproduced.
        assert_eq!(flatten(tasks), vec![1]);
    }

    #[test]
    fn intersection_separates_partitions() {
        let partition = |values: &[(usize, i64)]| -> Box<dyn PartialHandleSource> {
            Box::new(MaterializedHandleSource::new(vec![PartialHandleBatch {
                handles: values
                    .iter()
                    .map(|(p, h)| HandleRef {
                        partition_index: *p,
                        handle: TableHandle::Int(*h),
                    })
                    .collect(),
                sort_keys: MergeSortKeys::default(),
            }]))
        };
        let mut exec = merge(
            vec![
                partition(&[(0, 1), (1, 1), (1, 2)]),
                partition(&[(1, 1), (0, 2)]),
            ],
            true,
        );
        let tasks = exec.fetch_loop_intersection().expect("intersection");
        // Handle 1 is on both paths only in partition 1; the (0,1)/(0,2) and
        // (1,2) entries each appear once.
        let survivors: Vec<(usize, i64)> = tasks
            .into_iter()
            .flatten()
            .map(|h| (h.partition_index, h.handle.int_value().expect("int")))
            .collect();
        assert_eq!(survivors, vec![(1, 1)]);
    }

    #[test]
    fn intersection_splits_survivors_into_batch_sized_tasks() {
        let all: Vec<i64> = (1..=7).collect();
        let mut exec = merge(vec![source(&all), source(&all)], true).with_batch_size(3);
        let tasks = exec.fetch_loop_intersection().expect("intersection");
        assert_eq!(
            tasks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![3, 3, 1]
        );
    }

    #[test]
    fn a_pushed_limit_counts_deduplicated_union_handles() {
        let mut exec = merge(vec![source(&[1, 2, 3]), source(&[2, 4, 5])], false)
            .with_pushed_limit(PushedDownLimit {
                offset: 1,
                count: 3,
            });
        let tasks = exec.fetch_loop_union().expect("union");
        // First batch [1,2,3] loses 1 to the offset, leaving [2,3] and count
        // 1; the second batch dedups to [4,5] and is truncated to [4].
        assert_eq!(flatten(tasks), vec![2, 3, 4]);
    }

    #[test]
    fn a_pushed_offset_can_swallow_a_whole_batch() {
        let mut exec = merge(vec![batched_source(&[&[1, 2], &[3, 4]])], false).with_pushed_limit(
            PushedDownLimit {
                offset: 2,
                count: 1,
            },
        );
        let tasks = exec.fetch_loop_union().expect("union");
        assert_eq!(flatten(tasks), vec![3]);
    }

    #[test]
    fn a_pushed_limit_of_zero_emits_nothing() {
        let mut exec = merge(vec![source(&[1, 2, 3])], false).with_pushed_limit(PushedDownLimit {
            offset: 0,
            count: 0,
        });
        assert_eq!(
            flatten(exec.fetch_loop_union().expect("union")),
            Vec::<i64>::new()
        );
    }

    #[test]
    fn an_intersection_pushed_limit_truncates_the_survivors() {
        let all: Vec<i64> = (1..=6).collect();
        let mut exec = merge(vec![source(&all), source(&all)], true)
            .with_batch_size(2)
            .with_pushed_limit(PushedDownLimit {
                offset: 1,
                count: 3,
            });
        let tasks = exec.fetch_loop_intersection().expect("intersection");
        let handles = flatten(tasks);
        assert_eq!(handles.len(), 3);
        assert!(handles.iter().all(|handle| (1..=6).contains(handle)));
        assert_eq!(handles.iter().collect::<BTreeSet<_>>().len(), 3);
    }

    fn asc() -> Vec<MergeByItem> {
        vec![MergeByItem {
            collation: Collation::Binary,
            desc: false,
        }]
    }

    #[test]
    fn union_with_order_by_merges_the_paths_in_key_order() {
        let mut exec = merge(
            vec![
                ordered_source(&[(10, 1), (20, 5)]),
                ordered_source(&[(30, 3), (40, 7)]),
            ],
            false,
        )
        .with_by_items(asc());
        let tasks = exec.fetch_loop_union_with_order_by().expect("order by");
        assert_eq!(flatten(tasks), vec![10, 30, 20, 40]);
    }

    #[test]
    fn union_with_order_by_desc_reverses_the_key_order() {
        let mut exec = merge(
            vec![
                ordered_source(&[(10, 1), (20, 5)]),
                ordered_source(&[(30, 3), (40, 7)]),
            ],
            false,
        )
        .with_by_items(vec![MergeByItem {
            collation: Collation::Binary,
            desc: true,
        }]);
        let tasks = exec.fetch_loop_union_with_order_by().expect("order by");
        assert_eq!(flatten(tasks), vec![40, 20, 30, 10]);
    }

    #[test]
    fn union_with_order_by_deduplicates_before_ordering() {
        let mut exec = merge(
            vec![
                ordered_source(&[(10, 1), (20, 5)]),
                ordered_source(&[(10, 1), (15, 2)]),
            ],
            false,
        )
        .with_by_items(asc());
        let tasks = exec.fetch_loop_union_with_order_by().expect("order by");
        assert_eq!(flatten(tasks), vec![10, 15, 20]);
    }

    #[test]
    fn bare_table_task_rejects_missing_handles() {
        let mut exec = merge(vec![source(&[1])], false);
        let memory = exec.decode_context.expression().statement_memory();
        exec.open().unwrap();
        let mut chunk = exec.new_chunk();
        let error = exec.next(&mut chunk).expect_err("missing row must fail bare lookup");
        assert!(format!("{error:?}").contains("handle count 1 isn't equal to value count 0"));
        assert_eq!(chunk.num_rows(), 0);
        assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
        exec.close().unwrap();
    }

    #[test]
    fn table_task_error_is_reported_before_any_partial_rows() {
        struct LateError {
            meta: ExecutorMeta,
            first: bool,
            fail: bool,
        }
        impl Executor for LateError {
            fn open(&mut self) -> Result<(), ExecError> {
                self.first = true;
                Ok(())
            }
            fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
                req.reset();
                if std::mem::take(&mut self.first) {
                    req.set_num_virtual_rows(1);
                    Ok(())
                } else if self.fail {
                    Err(ExecError::internal("late table task failure"))
                } else {
                    Ok(())
                }
            }
            fn close(&mut self) -> Result<(), ExecError> {
                Err(ExecError::internal("deferred table close failure"))
            }
            fn schema(&self) -> &Schema {
                self.meta.schema()
            }
            fn ret_field_types(&self) -> &[FieldType] {
                self.meta.ret_field_types()
            }
            fn init_cap(&self) -> usize {
                1
            }
            fn max_chunk_size(&self) -> usize {
                1
            }
            fn new_chunk(&self) -> Chunk {
                self.meta.new_chunk()
            }
        }
        for (fail, quota) in [(true, -1), (false, -1), (false, 20)] {
            let memory = crate::StatementMemory::new(quota, crate::OomAction::Cancel, 7);
            let context = crate::StmtContext::for_query_with_memory(memory.clone());
            let mut exec = merge(vec![source(&[1])], false).with_table_plan(
                Schema::new(Vec::new()),
                Box::new(move |_| {
                    Ok(Box::new(LateError {
                        meta: ExecutorMeta::new(Schema::new(Vec::new()), 2, 1, 1),
                        first: true,
                        fail,
                    }))
                }),
                false,
            );
            exec.decode_context = RowDecodeContext::for_query(&context);
            exec.open().unwrap();
            let mut chunk = exec.new_chunk();
            let result = exec.next(&mut chunk);
            if fail {
                assert!(format!("{:?}", result.unwrap_err()).contains("late table task failure"));
                assert_eq!(chunk.num_rows(), 0);
                assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
            } else if quota > 0 {
                assert!(matches!(
                    result,
                    Err(ExecError::MemoryExceedForQuery { conn_id: 7 })
                ));
                assert_eq!(chunk.num_rows(), 0);
                assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
            } else {
                result.unwrap();
                assert_eq!(chunk.num_rows(), 1);
                // getResultTask retains the final task at EOF until Close:
                // one handle slot (8) and one Go row reference (16).
                assert_eq!(memory.stmt_tracker().bytes_consumed(), 24);
            }
            exec.close().unwrap();
            assert_eq!(memory.stmt_tracker().bytes_consumed(), 0);
        }
    }

    #[test]
    fn table_task_failure_closes_live_union_sources() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        struct Source(Arc<AtomicUsize>);
        impl PartialHandleSource for Source {
            fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError> {
                Ok(Some(PartialHandleBatch {
                    handles: int_handles(&[1]),
                    sort_keys: MergeSortKeys::default(),
                }))
            }
            fn close(&mut self) -> Result<(), ExecError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Err(ExecError::internal("close failure"))
            }
        }
        let closes = Arc::new(AtomicUsize::new(0));
        let mut exec = merge(vec![Box::new(Source(Arc::clone(&closes)))], false).with_table_plan(
            Schema::new(Vec::new()),
            Box::new(|_| Err(ExecError::internal("table failure"))),
            false,
        );
        exec.open().unwrap();
        let mut chunk = exec.new_chunk();
        assert!(format!("{:?}", exec.next(&mut chunk).unwrap_err()).contains("table failure"));
        assert_eq!(closes.load(Ordering::SeqCst), 1);
        assert!(exec.union_process.is_none());
        exec.close().unwrap();
        assert_eq!(closes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn cancellation_during_partial_fetch_stops_merge_and_closes_sources() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        struct CancellingSource {
            memory: crate::StatementMemory,
            reads: Arc<AtomicUsize>,
            closes: Arc<AtomicUsize>,
        }
        impl PartialHandleSource for CancellingSource {
            fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError> {
                let read = self.reads.fetch_add(1, Ordering::SeqCst);
                self.memory
                    .sql_killer()
                    .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
                Ok((read == 0).then(|| PartialHandleBatch {
                    handles: int_handles(&[1]),
                    sort_keys: vec![vec![Datum::Int(1)]].into(),
                }))
            }
            fn close(&mut self) -> Result<(), ExecError> {
                self.closes.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        for (intersection, ordered) in [(false, false), (false, true), (true, false)] {
            for before_open in [false, true] {
                let context = crate::StmtContext::for_query();
                let reads = Arc::new(AtomicUsize::new(0));
                let closes = Arc::new(AtomicUsize::new(0));
                let partials = (0..2)
                    .map(|_| {
                        Box::new(CancellingSource {
                            memory: context.statement_memory(),
                            reads: Arc::clone(&reads),
                            closes: Arc::clone(&closes),
                        }) as Box<dyn PartialHandleSource>
                    })
                    .collect();
                let mut exec = merge(partials, intersection);
                exec.decode_context = RowDecodeContext::for_query(&context);
                if ordered {
                    exec = exec.with_by_items(asc());
                }
                if before_open {
                    context
                        .statement_memory()
                        .sql_killer()
                        .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
                }
                let result = exec
                    .start_workers()
                    .and_then(|_| exec.next_table_task().map(|_| ()));
                assert!(matches!(result, Err(ExecError::Killed(_))), "{result:?}");
                assert_eq!(reads.load(Ordering::SeqCst), usize::from(!before_open));
                assert_eq!(
                    closes.load(Ordering::SeqCst),
                    if before_open { 0 } else { 2 }
                );
                assert!(exec.tasks.is_empty());
                exec.close().unwrap();
                assert_eq!(
                    closes.load(Ordering::SeqCst),
                    if before_open { 0 } else { 2 }
                );
            }
        }
    }

    #[test]
    fn partitioned_partial_budget_spans_partitions_and_ordered_refills() {
        for ordered in [false, true] {
            let mut source = PartitionedHandleSource::new(vec![
                ordered_source(&[]),
                ordered_source(&[(1, 1)]),
                ordered_source(&[(2, 2), (3, 3), (4, 4), (5, 5)]),
            ])
            .with_order(if ordered { asc() } else { Vec::new() }, 2);
            source.open().unwrap();
            let mut remaining = 3;
            let mut handles = Vec::new();
            while let Some(batch) = source.next_batch_with_max_handles(remaining).unwrap() {
                remaining -= batch.handles.len();
                handles.extend(
                    batch
                        .handles
                        .into_iter()
                        .map(|handle| handle.handle.int_value().unwrap()),
                );
            }
            assert_eq!(remaining, 0);
            assert_eq!(handles, vec![1, 2, 3]);
            assert_eq!(source.produced_rows().get(), 3);
            source.close().unwrap();
        }
    }

    #[test]
    fn ordered_handle_heap_matches_sorted_retention_with_bounded_allocation() {
        let huge = HandleHeap::new(
            Vec::new(),
            Some(PushedDownLimit {
                offset: 0,
                count: u64::MAX,
            }),
        );
        assert_eq!(huge.required_cnt, usize::MAX);
        assert_eq!(huge.entries.capacity(), 1024);
        for desc in [false, true] {
            for (offset, count) in [(0, 1), (7, 19), (1100, 1300), (0, 5000)] {
                let limit = PushedDownLimit { offset, count };
                let mut heap = HandleHeap::new(
                    vec![MergeByItem {
                        collation: Collation::Binary,
                        desc,
                    }],
                    Some(limit),
                );
                // A permutation exercises both sift directions, unlike one
                // already-sorted partial path with an early-exit opportunity.
                for index in 0..4096 {
                    let key = (index * 997 % 4096) as i64;
                    let task = heap.retain_task(PartialHandleBatch {
                        handles: vec![HandleRef::new(TableHandle::Int(key))],
                        sort_keys: vec![vec![Datum::Int(key)]].into(),
                    });
                    heap.push(MergeRowIndex { task, row: 0 }).unwrap();
                }
                // Go retains all input tasks even when the heap bound has
                // evicted most rows. Verify that entries only reference that
                // storage and that eviction did not shrink its lifetime.
                assert_eq!(heap.tasks.len(), 4096);
                assert_eq!(
                    heap.tasks.iter().map(|task| task.sort_keys.len()).sum::<usize>(),
                    4096
                );
                let actual = heap
                    .drain_sorted_handles(Some(limit))
                    .unwrap()
                    .into_iter()
                    .map(|handle| handle.handle.int_value().unwrap())
                    .collect::<Vec<_>>();
                let mut expected = (0..4096).collect::<Vec<i64>>();
                if desc {
                    expected.reverse();
                }
                let expected = expected
                    .into_iter()
                    .skip(offset as usize)
                    .take(count as usize)
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    fn union_with_order_by_and_limit_keeps_the_smallest_keys() {
        // Go startPartialIndexWorker merges sorted partition results before
        // applying the process worker's per-path early termination.
        for desc in [false, true] {
            for offset in [0, 1] {
                let order = vec![MergeByItem {
                    collation: Collation::Binary,
                    desc,
                }];
                let partition_rows = if desc {
                    vec![vec![(3, 3), (2, 2), (1, 1)], vec![(30, 30), (20, 20)]]
                } else {
                    vec![vec![(10, 10), (20, 20), (30, 30)], vec![(1, 1), (2, 2)]]
                };
                // One-row input batches force repeated partition refills;
                // an empty partition must not terminate the logical path.
                let sources = std::iter::once(ordered_source(&[]))
                    .chain(partition_rows.iter().map(|rows| {
                        Box::new(MaterializedHandleSource::new(
                            rows.iter()
                                .map(|(handle, key)| PartialHandleBatch {
                                    handles: int_handles(&[*handle]),
                                    sort_keys: vec![vec![Datum::Int(*key)]].into(),
                                })
                                .collect(),
                        )) as Box<dyn PartialHandleSource>
                    }))
                    .collect();
                let partitioned =
                    PartitionedHandleSource::new(sources).with_order(order.clone(), 1);
                let mut partitioned_exec = merge(vec![Box::new(partitioned)], false)
                    .with_by_items(order)
                    .with_pushed_limit(PushedDownLimit { offset, count: 2 });
                partitioned_exec.start_workers().unwrap();
                let tasks = partitioned_exec.tasks.drain(..).collect();
                let expected = match (desc, offset) {
                    (false, 0) => vec![1, 2],
                    (false, _) => vec![2, 10],
                    (true, 0) => vec![30, 20],
                    (true, _) => vec![20, 3],
                };
                assert_eq!(flatten(tasks), expected);
                partitioned_exec.close().unwrap();
            }
        }

        let mut exec = merge(
            vec![
                ordered_source(&[(20, 1), (10, 9)]),
                ordered_source(&[(40, 3), (30, 5)]),
            ],
            false,
        )
        .with_by_items(asc())
        .with_pushed_limit(PushedDownLimit {
            offset: 0,
            count: 2,
        });
        let tasks = exec.fetch_loop_union_with_order_by().expect("order by");
        // Keys 1 and 3 are the two smallest.
        assert_eq!(flatten(tasks), vec![20, 40]);
    }

    #[test]
    fn union_with_order_by_offset_drops_the_smallest_keys() {
        let mut exec = merge(
            vec![ordered_source(&[(20, 1), (40, 3), (30, 5), (10, 9)])],
            false,
        )
        .with_by_items(asc())
        .with_pushed_limit(PushedDownLimit {
            offset: 2,
            count: 1,
        });
        let tasks = exec.fetch_loop_union_with_order_by().expect("order by");
        // The heap keeps `offset + count == 3` candidates -- keys 1, 3, 5 --
        // and the final drain drops the smallest `offset` of them.
        assert_eq!(flatten(tasks), vec![30]);
    }

    #[test]
    fn union_with_order_by_batches_at_the_lookup_size() {
        let rows: Vec<(i64, i64)> = (1..=5).map(|v| (v, v)).collect();
        let mut exec = merge(vec![ordered_source(&rows)], false)
            .with_by_items(asc())
            .with_batch_size(2);
        let tasks = exec.fetch_loop_union_with_order_by().expect("order by");
        assert_eq!(
            tasks.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![2, 2, 1]
        );
    }

    #[test]
    fn intersection_with_an_order_by_is_refused_as_go_leaves_it_unimplemented() {
        let mut exec = merge(vec![source(&[1]), source(&[1])], true).with_by_items(asc());
        let err = exec.start_workers().expect_err("refused");
        assert!(matches!(err, ExecError::Unsupported(_)));
    }

    /// Go TestIndexMergeError / TestIndexMergeCoprGoroutinesLeak: every opened
    /// partial is torn down even when another partial fails.
    #[test]
    fn index_merge_error_closes_all_started_partials() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        };
        struct Source {
            opened: Arc<AtomicUsize>,
            closed: Arc<AtomicUsize>,
            failure: &'static str,
        }
        impl PartialHandleSource for Source {
            fn open(&mut self) -> Result<(), ExecError> {
                self.opened.fetch_add(1, Ordering::Relaxed);
                if self.failure == "open" {
                    return Err(ExecError::internal("open failure"));
                }
                Ok(())
            }
            fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError> {
                if self.failure == "read" {
                    return Err(ExecError::internal("read failure"));
                }
                Ok(None)
            }
            fn close(&mut self) -> Result<(), ExecError> {
                self.closed.fetch_add(1, Ordering::Relaxed);
                if self.failure == "close" {
                    return Err(ExecError::internal("close failure"));
                }
                Ok(())
            }
        }
        for failures in [
            ["", "", ""],
            ["close", "", ""],
            ["close", "read", ""],
            ["close", "open", ""],
        ] {
            let counts: Vec<_> = (0..3)
                .map(|_| (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))))
                .collect();
            let partials = counts
                .iter()
                .zip(failures)
                .map(|((opened, closed), failure)| {
                    Box::new(Source {
                        opened: opened.clone(),
                        closed: closed.clone(),
                        failure,
                    }) as Box<dyn PartialHandleSource>
                })
                .collect();
            let mut exec = merge(partials, false);
            exec.open().unwrap();
            let result = exec
                .start_workers()
                .and_then(|_| exec.next_table_task().map(|_| ()));
            match failures[1] {
                "open" => assert!(format!("{:?}", result.unwrap_err()).contains("open failure")),
                "read" => assert!(format!("{:?}", result.unwrap_err()).contains("read failure")),
                _ if failures[0] == "close" => {
                    assert!(format!("{:?}", result.unwrap_err()).contains("close failure"))
                }
                _ => result.unwrap(),
            }
            // Shutdown errors must not strand a later path or cause a
            // second Close of an already drained remote result.
            let _ = exec.close();
            let _ = exec.close();
            for (index, (opened, closed)) in counts.iter().enumerate() {
                let expected = usize::from(failures[1] != "open" || index < 2);
                assert_eq!(
                    (
                        opened.load(Ordering::Relaxed),
                        closed.load(Ordering::Relaxed)
                    ),
                    (expected, expected),
                    "{failures:?}, source {index}"
                );
            }
        }
    }

    #[test]
    fn counting_down_a_limit_reports_a_wholly_skipped_batch() {
        let mut limit = PushedDownLimit {
            offset: 5,
            count: 2,
        };
        let (next, kept) = pushed_limit_counting_down(&mut limit, int_handles(&[1, 2]));
        assert!(next);
        assert!(kept.is_empty());
        assert_eq!(limit.offset, 3);
        assert_eq!(limit.count, 2);
    }

    #[test]
    fn counting_down_a_limit_truncates_and_exhausts_the_count() {
        let mut limit = PushedDownLimit {
            offset: 1,
            count: 2,
        };
        let (next, kept) = pushed_limit_counting_down(&mut limit, int_handles(&[1, 2, 3, 4]));
        assert!(!next);
        assert_eq!(
            kept.iter()
                .map(|h| h.handle.int_value().expect("int"))
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(limit.offset, 0);
        assert_eq!(limit.count, 0);
    }
}
