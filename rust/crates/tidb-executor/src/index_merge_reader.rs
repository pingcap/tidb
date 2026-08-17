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
//! Stage 2 is the part with no Rust counterpart anywhere and it is ported in
//! full. Stages 1 and 3 are boundaries onto code that already exists.
//!
//! # Reuse rather than restatement
//!
//! * [`crate::kv_table::TableHandle`] is Go `kv.Handle`; it is already `Ord` +
//!   `Eq`, which is what makes Go's `kv.HandleMap`/`kv.MemAwareHandleMap` a
//!   plain [`BTreeMap`]/[`BTreeSet`] here.
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
//! * **Table scan workers.** Each owns its task and builds its own reader; the
//!   tasks are handed to `resultCh` in the order the process worker created
//!   them, and `getResultTask` consumes `resultCh` in that order. So the
//!   *task* order is already deterministic in Go, and rows within a task come
//!   out in the task's handle order. Running the readers one at a time changes
//!   latency, not row order.
//!
//! What is genuinely lost: nothing observable in *which rows* come out.
//! Two orderings that Go leaves unspecified and this port therefore simply
//! picks one of:
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
//!   which is a Go map iteration -- randomized per run. This port emits in
//!   ascending [`TableHandle`] order, which is deterministic and one of the
//!   orders Go permits.
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
//!   indexes (`hasGlobalIndex`, which remap a handle's partition through
//!   `partitionIDMap`) are NOT ported: a partial source must already report
//!   the partition its handles belong to.
//! * `IndexMergeRuntimeStat` (:2060), the `memory.Tracker` accounting, the
//!   `failpoint` injections, `handleWorkerPanic` (:939), `syncErr` (:1734),
//!   correlated-column range rebuilding (`rebuildRangeForCorCol` :201) and
//!   `IndexUsageReporter` have no counterpart in this tier.
//! * `fetchLoopIntersectionWithOrderBy` (:1569) is an empty `// todo` in Go
//!   itself. It is refused here for the same reason.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Collation, Datum, FieldType};
use tidb_expr::schema::Schema;

use crate::access_path::HandleSourceExec;
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{KvTable, RowDecodeContext, TableHandle};

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
    pub sort_keys: Vec<Vec<Datum>>,
}

/// Go's partial workers (`partialIndexWorker` :1717, `partialTableWorker`
/// :663) reduced to what the process worker actually consumes: a stream of
/// handle batches.
///
/// The batch-size growth Go performs in `extractTaskHandles` (:1830, doubling
/// `w.batchSize` up to `maxBatchSize`) and the per-path `pushedLimit`
/// short-circuit inside it belong to the implementor, because they are
/// properties of the scan, not of the merge.
pub trait PartialHandleSource {
    /// Go `partial*Worker.fetchHandles` start-up (the `startPartial*Worker`
    /// wrappers, :380 / :518).
    fn open(&mut self) -> Result<(), ExecError> {
        Ok(())
    }

    /// The next batch, or `None` at end of path. An empty batch is treated as
    /// end of path, matching `extractTaskHandles` returning no handles.
    fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError>;

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
            sort_keys: Vec::new(),
        }])
    }
}

impl PartialHandleSource for MaterializedHandleSource {
    fn next_batch(&mut self) -> Result<Option<PartialHandleBatch>, ExecError> {
        Ok(self.batches.pop_front())
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

/// Go `handleHeap` (:1016).
///
/// Go keeps a `container/heap` whose `Less` is INVERTED for an ascending
/// by-item, so `heap.Pop` yields the currently *largest* key and popping while
/// `Len() > requiredCnt` retains the smallest `requiredCnt` keys. This keeps
/// the same retained SET with a linear eviction scan instead of a sift, which
/// is indistinguishable downstream because the survivors are fully sorted
/// before they are emitted.
struct HandleHeap {
    /// Go `requiredCnt`; `0` means "keep everything".
    required_cnt: usize,
    by_items: Vec<MergeByItem>,
    /// `(sort key, handle)` for every distinct handle still retained.
    entries: Vec<(Vec<Datum>, HandleRef)>,
}

impl HandleHeap {
    fn new(by_items: Vec<MergeByItem>, pushed_limit: Option<PushedDownLimit>) -> Self {
        // Go: `requiredCnt = min(1024, Count+Offset)` (:1082). The 1024 is a
        // real cap on the heap, not just a pre-allocation hint, so an index
        // merge with `ORDER BY ... LIMIT n` for `n > 1024` keeps only 1024
        // candidates in Go too. Reproduced rather than corrected.
        let required_cnt = pushed_limit.map_or(0, |limit| {
            1024usize.min(limit.count.saturating_add(limit.offset) as usize)
        });
        HandleHeap {
            required_cnt,
            by_items,
            entries: Vec::new(),
        }
    }

    /// Go `handleHeap.Less`, with the sign convention spelled out: the raw
    /// comparison is negated for an ASCENDING item, so "less" means "sorts
    /// later".
    fn less(&self, a: &[Datum], b: &[Datum]) -> Result<bool, ExecError> {
        for (i, item) in self.by_items.iter().enumerate() {
            let (Some(left), Some(right)) = (a.get(i), b.get(i)) else {
                return Err(ExecError::internal(
                    "index merge by-item key is shorter than the by-item list",
                ));
            };
            let mut cmp = tidb_expr::compare_datums_with_collation(left, right, item.collation)?;
            if !item.desc {
                cmp = cmp.reverse();
            }
            match cmp {
                std::cmp::Ordering::Less => return Ok(true),
                std::cmp::Ordering::Greater => return Ok(false),
                std::cmp::Ordering::Equal => {}
            }
        }
        Ok(false)
    }

    /// Go `heap.Push` followed by the `Len() > requiredCnt` eviction (:1150).
    ///
    /// Returns `true` when the value just pushed is the one evicted -- Go's
    /// `top == the row just pushed` test, which is what lets it mark a partial
    /// path `useless`. The eviction itself is what matters; the `uselessMap`
    /// early-exit is a scan-shortening optimization over an already-sorted
    /// path and cannot change the retained set, so it is not ported.
    fn push(&mut self, key: Vec<Datum>, handle: HandleRef) -> Result<bool, ExecError> {
        self.entries.push((key, handle));
        if self.required_cnt == 0 || self.entries.len() <= self.required_cnt {
            return Ok(false);
        }
        // The "minimum under Less" is the maximum by value for an ascending
        // item; that is what `heap.Pop` would hand back.
        let mut victim = 0usize;
        for i in 1..self.entries.len() {
            if self.less(&self.entries[i].0, &self.entries[victim].0)? {
                victim = i;
            }
        }
        let pushed = victim + 1 == self.entries.len();
        self.entries.remove(victim);
        Ok(pushed)
    }

    /// Go's final drain (:1172): pop `needCount` times into `fhs` back to
    /// front, which leaves the survivors in by-item order with the smallest
    /// `Offset` of them dropped.
    fn into_sorted_handles(
        mut self,
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
        let mut error = None;
        let mut entries = std::mem::take(&mut self.entries);
        entries.sort_by(|a, b| {
            // `less(a, b)` is "a sorts later"; the emitted order is the
            // reverse of that, i.e. by-item order.
            match self.less(&a.0, &b.0) {
                Ok(true) => std::cmp::Ordering::Greater,
                Ok(false) => match self.less(&b.0, &a.0) {
                    Ok(true) => std::cmp::Ordering::Less,
                    Ok(false) => std::cmp::Ordering::Equal,
                    Err(err) => {
                        error.get_or_insert(err);
                        std::cmp::Ordering::Equal
                    }
                },
                Err(err) => {
                    error.get_or_insert(err);
                    std::cmp::Ordering::Equal
                }
            }
        });
        if let Some(err) = error {
            return Err(err);
        }
        // Go pops the LARGEST `need` entries, so the smallest `len - need`
        // (the offset) are the ones dropped.
        Ok(entries
            .drain(len - need..)
            .map(|(_, handle)| handle)
            .collect())
    }
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
    /// Go `sessionVars.IndexLookupSize`: the handle count per table task.
    batch_size: usize,
    /// Go `workerStarted`.
    started: bool,
    /// Go `workCh`/`resultCh` contents, in creation order.
    tasks: VecDeque<Vec<HandleRef>>,
    /// Go's current `indexMergeTableScanWorker` reader over one task.
    current: Option<HandleSourceExec>,
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
            is_intersection,
            by_items: Vec::new(),
            pushed_limit: None,
            batch_size: 20_000,
            started: false,
            tasks: VecDeque::new(),
            current: None,
        }
    }

    /// Go `pushedLimit`.
    #[must_use]
    pub fn with_pushed_limit(mut self, limit: PushedDownLimit) -> Self {
        self.pushed_limit = Some(limit);
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

    /// Go `startWorkers` (:317) plus the process worker running to completion.
    fn start_workers(&mut self) -> Result<(), ExecError> {
        for partial in &mut self.partials {
            partial.open()?;
        }
        let handles = if self.is_intersection {
            if !self.by_items.is_empty() {
                // Go `fetchLoopIntersectionWithOrderBy` (:1569) is an empty
                // body with a `todo`, so no plan reaches it. Refusing is the
                // same behavior stated instead of silently ignoring the order.
                return Err(ExecError::unsupported(
                    "index merge intersection with an order-by is not implemented (Go: fetchLoopIntersectionWithOrderBy)",
                ));
            }
            self.fetch_loop_intersection()?
        } else if self.by_items.is_empty() {
            self.fetch_loop_union()?
        } else {
            self.fetch_loop_union_with_order_by()?
        };
        for batch in handles {
            self.tasks.push_back(batch);
        }
        for partial in &mut self.partials {
            partial.close()?;
        }
        self.started = true;
        Ok(())
    }

    /// Go `indexMergeProcessWorker.fetchLoopUnion` (:1245).
    ///
    /// First writer wins: a handle already in `hMap` is dropped, so a handle
    /// on several paths is read once. The pushed limit is applied to each
    /// batch AFTER dedup, which is why an entirely-duplicate batch does not
    /// consume any of the offset (Go `continue`s on `len(fhs) == 0` before
    /// touching `pushedLimit`).
    fn fetch_loop_union(&mut self) -> Result<Vec<Vec<HandleRef>>, ExecError> {
        let mut pushed_limit = self.pushed_limit;
        let mut seen: BTreeSet<HandleRef> = BTreeSet::new();
        let mut tasks = Vec::new();
        for partial in &mut self.partials {
            loop {
                if pushed_limit.is_some_and(|limit| limit.count == 0) {
                    return Ok(tasks);
                }
                let Some(batch) = partial.next_batch()? else {
                    break;
                };
                if batch.handles.is_empty() {
                    break;
                }
                let mut fresh = Vec::with_capacity(batch.handles.len());
                for handle in batch.handles {
                    if seen.insert(handle.clone()) {
                        fresh.push(handle);
                    }
                }
                if fresh.is_empty() {
                    continue;
                }
                if let Some(limit) = pushed_limit.as_mut() {
                    let (next, kept) = pushed_limit_counting_down(limit, fresh);
                    if next {
                        continue;
                    }
                    fresh = kept;
                }
                tasks.push(fresh);
            }
        }
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
        let mut heap = HandleHeap::new(self.by_items.clone(), self.pushed_limit);
        let mut distinct: BTreeSet<HandleRef> = BTreeSet::new();
        for partial in &mut self.partials {
            'path: while let Some(batch) = partial.next_batch()? {
                if batch.handles.is_empty() {
                    break;
                }
                if batch.sort_keys.len() != batch.handles.len() {
                    return Err(ExecError::internal(
                        "index merge order-by needs one sort key per handle",
                    ));
                }
                for (handle, key) in batch.handles.into_iter().zip(batch.sort_keys) {
                    if !distinct.insert(handle.clone()) {
                        continue;
                    }
                    if heap.push(key, handle)? {
                        // Go marks this path `useless` and stops reading it:
                        // the path is sorted, so everything after this handle
                        // would also be evicted.
                        break 'path;
                    }
                }
            }
        }
        let handles = heap.into_sorted_handles(self.pushed_limit)?;
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
        let path_count = self.partials.len();
        let mut counts: BTreeMap<HandleRef, usize> = BTreeMap::new();
        for partial in &mut self.partials {
            while let Some(batch) = partial.next_batch()? {
                if batch.handles.is_empty() {
                    break;
                }
                for handle in batch.handles {
                    *counts.entry(handle).or_insert(0) += 1;
                }
            }
        }
        // Go groups by partition first (one handle map per `parTblIdx`) and
        // splits each partition's survivors into `batchSize` tasks. The
        // `BTreeMap` is already ordered by `(partition_index, handle)`, so
        // grouping is a scan.
        let mut per_partition: Vec<(usize, Vec<HandleRef>)> = Vec::new();
        for (handle, count) in counts {
            if count != path_count {
                continue;
            }
            match per_partition.last_mut() {
                Some((partition, group)) if *partition == handle.partition_index => {
                    group.push(handle);
                }
                _ => per_partition.push((handle.partition_index, vec![handle])),
            }
        }
        let mut pushed_limit = self.pushed_limit;
        let mut tasks = Vec::new();
        for (_, group) in per_partition {
            for chunk in group.chunks(self.batch_size) {
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

    /// Go `buildFinalTableReader` (:854) + `executeTask` (:1988): the reader
    /// over one task's handles.
    fn build_final_table_reader(&self, handles: Vec<HandleRef>) -> HandleSourceExec {
        let meta = ExecutorMeta::new(
            self.meta.schema().clone(),
            self.meta.id(),
            self.meta.init_cap(),
            self.meta.max_chunk_size(),
        );
        HandleSourceExec::new_with_context(
            meta,
            self.table.clone(),
            handles.into_iter().map(|h| h.handle).collect(),
            self.decode_context.clone(),
        )
    }
}

impl Executor for IndexMergeReaderExec {
    /// Go `Open` (:174). The process worker is not started here: Go starts its
    /// goroutines lazily on the first `Next` (`if !e.workerStarted`), and so
    /// does this port.
    fn open(&mut self) -> Result<(), ExecError> {
        self.started = false;
        self.tasks.clear();
        self.current = None;
        Ok(())
    }

    /// Go `Next` (:880) + `getResultTask` (:907).
    fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
        if !self.started {
            self.start_workers()?;
        }
        req.reset();
        loop {
            if self.current.is_none() {
                let Some(task) = self.tasks.pop_front() else {
                    return Ok(());
                };
                let mut reader = self.build_final_table_reader(task);
                reader.open()?;
                self.current = Some(reader);
            }
            let reader = self.current.as_mut().expect("reader was just installed");
            reader.next(req)?;
            if req.num_rows() > 0 {
                return Ok(());
            }
            // Go's table worker finishes a task and the main loop takes the
            // next one; an exhausted reader is not EOF for the operator.
            let mut reader = self.current.take().expect("reader was just installed");
            reader.close()?;
        }
    }

    fn close(&mut self) -> Result<(), ExecError> {
        if let Some(mut reader) = self.current.take() {
            reader.close()?;
        }
        self.tasks.clear();
        for partial in &mut self.partials {
            partial.close()?;
        }
        Ok(())
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
                    sort_keys: Vec::new(),
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
            sort_keys: rows.iter().map(|(_, k)| vec![Datum::Int(*k)]).collect(),
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
        // 2 and 3 are the only handles on all three paths; emitted in
        // ascending handle order.
        assert_eq!(flatten(tasks), vec![2, 3]);
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
                sort_keys: Vec::new(),
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
        assert_eq!(flatten(tasks), vec![2, 3, 4]);
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
    fn union_with_order_by_and_limit_keeps_the_smallest_keys() {
        let mut exec = merge(
            vec![
                ordered_source(&[(10, 9), (20, 1)]),
                ordered_source(&[(30, 5), (40, 3)]),
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
            vec![ordered_source(&[(10, 9), (20, 1), (30, 5), (40, 3)])],
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
