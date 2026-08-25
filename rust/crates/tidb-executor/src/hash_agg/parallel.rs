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

//! The PARALLEL execution model of `pkg/executor/aggregate`'s `HashAggExec`
//! (Go `agg_hash_executor.go`'s `parallelExec`, `agg_hash_partial_worker.go`,
//! `agg_hash_final_worker.go`), transcreated onto Rust threads and channels.
//!
//! # Worker topology (mirrors Go's ASCII diagram)
//!
//! ```text
//!                          +-------------+
//!                          | main thread |   consumes finalOutputCh / emits
//!                          +------+------+
//!                                 ^
//!                                 |  final_output_ch (mpsc, one merged map)
//!                    +------------+------------+
//!                    | final worker 0 .. M-1   |   merges N sub-maps each
//!                    +------------^------------+
//!                                 |  shuffle ch f, cap = N (partialOutputChs)
//!              +------------------+------------------+
//!              | partial worker 0 .. N-1           |   M sub-maps each
//!              +-----------------^-----------------+
//!                                |  input lane i, cap = 1 (partialInputChs)
//!                          +-----+-----+
//!                          | fetcher = |   reads the child executor
//!                          |main thread|
//!                          +-----------+
//! ```
//!
//! # Go channel -> Rust channel mapping
//!
//! * `partialInputChs[i]` (`chan *chunk.Chunk`, capacity 1) becomes one
//!   `std::sync::mpsc::sync_channel(1)` per partial worker: Go's backpressure
//!   contract -- the fetcher blocks only when a lane falls a full chunk
//!   behind.
//! * `inputCh`/`giveBackCh` chunk recycling is dropped (named divergence):
//!   the fetcher allocates a fresh request chunk per dispatch, exactly like
//!   the bounded integer fast path beside which this pipeline lives.
//! * `partialOutputChs[f]` (`chan AggPartialResultMapper`, capacity
//!   `partialConcurrency`) becomes one `sync_channel(N)` per final worker --
//!   every partial worker sends exactly one sub-map there.
//! * `finalOutputCh` (`chan *AfFinalResult`) becomes an unbounded
//!   `mpsc::channel` of [`FinalMsg`]. Go streams result chunks across `Next`
//!   calls; here the whole aggregation completes inside one `execute()` call
//!   and the main thread then emits groups in first-seen order.
//! * `finishCh` becomes [`PipelineAbort`] plus channel disconnects. Every
//!   worker DRAINS its inputs even after an error (Go's
//!   `finalizeWorkerProcess`), so no sender or receiver can block forever --
//!   the same liveness Go's `select` on `finishCh` provides.
//!
//! Each partial worker owns `M` sub-maps (Go's
//! `HashAggPartialWorker.partialResultsMap[finalConcurrency]`); a group key
//! routes to final worker `bucket(key) % M`, so one group's partial pieces
//! all land on one final worker. DIVERGENCE (unobservable): Go partitions
//! with `murmur3.Sum32`, this uses FNV-1a 32-bit -- only partition
//! ASSIGNMENT differs, never results. Every group records the global row
//! sequence of its first contributing row; merges keep the minimum, and the
//! final emission sorts by it, so output order is FIRST-SEEN order -- the
//! serial path's exact contract, stricter than Go's random map iteration.
//!
//! # What stays serial, and why
//!
//! * DISTINCT / aggregate ORDER BY -> serial: Go's `IsUnparallelExec`
//!   (`pkg/executor/builder.go:2058`).
//! * `partial == 1 && final == 1` (or either `<= 0`) -> serial: Go's
//!   builder.go workaround rule.
//! * Order-sensitive or float-domain aggregates -- `GROUP_CONCAT`,
//!   `JSON_ARRAYAGG`, `JSON_OBJECTAGG`, `APPROX_PERCENTILE`, the variance
//!   family, and `SUM`/`AVG` over REAL arguments -> serial. Merging them
//!   across workers cannot reproduce this port's line-for-line equality with
//!   `unparallelExec`; integer/decimal SUM/AVG fold in the exact decimal
//!   domain and merge exactly.
//! * Statement quotas below 256 MiB stay on the spill-capable serial path.
//! * Context shareability: Go passes `sessionctx.Context` to every worker;
//!   the Rust evaluation context must be shareable too, which
//!   [`HashAggContext`] declares. Production `StmtContext` now qualifies:
//!   its session handles are `Arc` + lock/atomic shared state, so both it and
//!   `NoColumns` drive the pipeline.
//!
//! # Spill interaction
//!
//! Go's parallel spill (`parallelHashAggSpillHelper`, agg_spill.go) is NOT
//! ported yet -- explicitly unfinished, not silently skipped. A pipeline-mode
//! Open registers NO soft-limit spill action; the fetcher instead calls
//! `StatementMemory::check()` between chunks, so a quota overrun surfaces as
//! Go's 8175 cancellation rather than unbounded growth or silent truncation.

use super::*;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc::{channel, sync_channel};
use std::sync::{Arc, Mutex};

/// Marks an evaluation context that [`HashAggExec`] accepts.
///
/// Implementors state whether worker threads may evaluate expressions through
/// a shared `&Self` ([`Self::PARALLEL_WORKERS_MAY_EVAL`]); the bridge method
/// carries the `Self: Sync` proof into the pipeline without imposing that
/// bound on contexts that cannot honor it.
pub trait HashAggContext: Columns {
    /// Whether `&Self` may be dereferenced concurrently by the hash-aggregate
    /// worker threads. Go shares its session context with every worker
    /// freely; a Rust context may do the same only when it is `Sync`.
    const PARALLEL_WORKERS_MAY_EVAL: bool;

    /// Bridges into the `Self: Sync`-gated pipeline. Returns `None` when the
    /// context cannot share evaluation across threads; the executor then
    /// never enters pipeline mode.
    fn run_parallel_pipeline_bridge(exec: &mut HashAggExec<Self>) -> Option<Result<(), ExecError>>
    where
        Self: Sized,
    {
        let _ = exec;
        None
    }
}

impl HashAggContext for tidb_expr::NoColumns {
    const PARALLEL_WORKERS_MAY_EVAL: bool = true;

    fn run_parallel_pipeline_bridge(exec: &mut HashAggExec<Self>) -> Option<Result<(), ExecError>> {
        Some(exec.execute_parallel_pipeline())
    }
}

impl HashAggContext for crate::StmtContext {
    /// The production statement context shares every interior-mutable handle
    /// through `Arc` + `Mutex`/atomics (the transcreation of Go sharing one
    /// `sessionctx.Context` with every worker goroutine), so worker threads
    /// may evaluate expressions through `&StmtContext`.
    const PARALLEL_WORKERS_MAY_EVAL: bool = true;

    fn run_parallel_pipeline_bridge(exec: &mut HashAggExec<Self>) -> Option<Result<(), ExecError>> {
        Some(exec.execute_parallel_pipeline())
    }
}

// The bridge's `Self: Sync` proof is checked at every call site; this
// assertion keeps it true even when no pipeline-eligible aggregation is
// compiled anywhere in the workspace.
const _: () = {
    const fn assert_sync<T: Sync>() {}
    assert_sync::<crate::StmtContext>();
};

/// Diagnostics shared with the pipeline's workers, mirroring in spirit Go's
/// per-worker `AggWorkerStat` registration.
#[derive(Default)]
pub(super) struct PipelineStats {
    /// Resolved worker counts (Go's session concurrency variables).
    pub(super) partial_concurrency: usize,
    pub(super) final_concurrency: usize,
    /// Chunks successfully dispatched by the fetcher.
    pub(super) dispatched_chunks: AtomicUsize,
    /// Ids of the partial-worker threads that actually ran.
    pub(super) partial_worker_threads: Mutex<Vec<std::thread::ThreadId>>,
}

impl PipelineStats {
    pub(super) fn new(partial_concurrency: usize, final_concurrency: usize) -> Self {
        PipelineStats {
            partial_concurrency,
            final_concurrency,
            dispatched_chunks: AtomicUsize::new(0),
            partial_worker_threads: Mutex::new(Vec::new()),
        }
    }

    fn record_partial_worker(&self) {
        self.partial_worker_threads
            .lock()
            .expect("pipeline stats lock")
            .push(std::thread::current().id());
    }
}

enum FinalMsg {
    /// One final worker's merged map.
    Maps(PipelineMap),
    /// A worker surfaced an error (Go's `AfFinalResult{err}`).
    Err(ExecError),
}

/// Shared liveness flag: any worker error raises this so the fetcher stops
/// feeding lanes (Go's early-termination role for `finishCh`).
#[derive(Clone, Default)]
struct PipelineAbort(Arc<AtomicBool>);

impl PipelineAbort {
    fn raised(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn raise(&self) {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

/// The `'static` snapshot the persistent pool's workers read: everything
/// `fold_chunk` needs, cloned once per aggregation instead of borrowed from
/// the executor (the pool cannot hold borrows; see `worker_pool`).
struct PipelinePlan<C: Columns + Send + Sync + Clone + 'static> {
    ctx: C,
    group_by: Vec<Expression>,
    group_collations: Vec<tidb_datatype::Collation>,
    integer_columns: Option<Vec<(usize, bool)>>,
    agg_funcs: Vec<AggFunc>,
}

/// A pipeline group-map key. The single-signed-integer shape (q18's
/// `group by l_orderkey`, 1.5M groups) keys by the raw `Option<i64>` so no
/// per-group key allocation happens; every other shape keeps the encoded
/// `Vec<u8>` key.
#[derive(Clone, PartialEq, Eq, Hash)]
enum PipelineMapKey {
    Int(Option<i64>),
    Bytes(Vec<u8>),
}

impl PipelineMapKey {
    /// The byte length `new_group_bytes` was charging under the encoded
    /// representation, kept for tracker continuity.
    fn charge_len(&self) -> usize {
        match self {
            // 8-byte varint body + flag + separator.
            PipelineMapKey::Int(_) => 10,
            PipelineMapKey::Bytes(bytes) => bytes.len(),
        }
    }
}

type PipelineMap = HashMap<PipelineMapKey, PipelineGroup, BuildHasherDefault<super::ParallelIntHasher>>;

/// One group inside a worker's map: the global row sequence of the first
/// contributing row plus the group's aggregate states.
struct PipelineGroup {
    first_seq: u64,
    states: Vec<AggState>,
}

impl PipelineGroup {
    /// Creates the group; the CALLER batches the tracker consume (one
    /// round-trip per chunk, not per group — 1.5M-group shapes showed the
    /// lock in profiles).
    fn new(funcs: &[AggFunc], seq: u64, key_len: usize) -> (Self, i64) {
        let bytes = new_group_bytes(key_len, funcs.len());
        (
            PipelineGroup {
                first_seq: seq,
                states: funcs.iter().map(AggState::new).collect(),
            },
            bytes,
        )
    }
}

/// FNV-1a 32-bit over the encoded key: plays Go's
/// `murmur3.Sum32(key) % finalConcurrency` partition role. See the module
/// docs for why a different hash here is unobservable.
fn key_bucket(key: &[u8], bucket_count: usize) -> usize {
    let mut hash: u32 = 0x811c_9dc5;
    for &byte in key {
        hash ^= u32::from(byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    (hash as usize) % bucket_count
}

/// Reads one concurrency system variable with Go's resolution order: the
/// session value first (a context answers `None` when unset), then the
/// global-scope snapshot.
fn resolved_concurrency<C: Columns>(ctx: &C, name: &str) -> Option<usize> {
    let read = |scope| match ctx.sysvar(scope, name) {
        Some(Datum::Int(value)) if value > 0 => usize::try_from(value).ok(),
        Some(Datum::UInt(value)) => usize::try_from(value).ok(),
        Some(Datum::Bytes(raw)) => String::from_utf8_lossy(&raw)
            .trim()
            .parse::<i64>()
            .ok()
            .filter(|value| *value > 0)
            .and_then(|value| usize::try_from(value).ok()),
        _ => None,
    };
    read(None).or_else(|| read(Some(tidb_ast::SysVarScope::Global)))
}

fn executor_concurrency<C: Columns>(ctx: &C) -> usize {
    resolved_concurrency(ctx, "tidb_executor_concurrency")
        .unwrap_or(tidb_vardef::defaults::DEF_EXECUTOR_CONCURRENCY as usize)
}

impl<C: HashAggContext> HashAggExec<C> {
    /// Test/diagnostic override for the resolved worker counts, standing in
    /// for a session that has SET the concurrency variables.
    #[cfg(test)]
    pub(crate) fn with_pipeline_concurrency_override(
        mut self,
        partial: usize,
        final_: usize,
    ) -> Self {
        self.pipeline_concurrency_override = Some((partial, final_));
        self
    }

    /// `(partial, final, dispatched_chunks, partial_worker_threads)` for the
    /// last Open's pipeline run; `None` when the aggregation ran serially.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn pipeline_run_info(&self) -> Option<(usize, usize, usize, usize)> {
        let stats = self.pipeline_stats.as_ref()?;
        Some((
            stats.partial_concurrency,
            stats.final_concurrency,
            stats
                .dispatched_chunks
                .load(std::sync::atomic::Ordering::SeqCst),
            stats
                .partial_worker_threads
                .lock()
                .expect("pipeline stats lock")
                .len(),
        ))
    }

    /// Resolves the two worker counts the way Go's `initForParallelExec`
    /// reads `sessionVars.HashAggPartialConcurrency()` /
    /// `HashAggFinalConcurrency()`: the variable if set (> 0), else
    /// `tidb_executor_concurrency`, else the process default.
    pub(super) fn resolved_pipeline_concurrency(&self) -> (usize, usize) {
        if let Some((partial, final_)) = self.pipeline_concurrency_override {
            return (partial, final_);
        }
        let fallback = executor_concurrency(&self.ctx);
        let resolve = |name: &str| resolved_concurrency(&self.ctx, name).unwrap_or(fallback);
        (
            resolve("tidb_hashagg_partial_concurrency"),
            resolve("tidb_hashagg_final_concurrency"),
        )
    }

    /// Decides whether THIS aggregation may run the parallel pipeline,
    /// returning the resolved `(partial, final)` worker counts when it may.
    /// Every refusal routes back to the complete serial implementation.
    ///
    /// Requires `C: HashAggContext` so the context-capability constant
    /// participates in the decision at `Open` time.
    pub(super) fn pipeline_eligibility(&self) -> Option<(usize, usize)> {
        // The binary-string Web3Bench shape has a columnar serial fold that
        // is cheaper than materializing per-worker expression rows. Keep it
        // on that path even when the session falls back to the default
        // worker concurrency; this also makes the single-concurrency result
        // independent of whether the two hash-agg variables were set.
        if self.direct_string_group_column().is_some()
            && self.direct_string_aggregate_specs().is_some()
        {
            return None;
        }
        // The Datum-flattened output buffer cannot carry zero-width virtual
        // rows; GROUP BY without aggregates stays serial.
        if self.agg_funcs.is_empty() {
            return None;
        }
        // Go `builder.go:2058`: DISTINCT / ORDER BY aggregates force
        // `IsUnparallelExec`.
        for func in &self.agg_funcs {
            if func.distinct || !func.order_by.is_empty() {
                return None;
            }
            match &func.kind {
                AggKind::Count | AggKind::FinalCount | AggKind::FirstRow => {}
                AggKind::Min | AggKind::Max | AggKind::Bit(_) => {
                    func.arg.as_ref()?;
                }
                AggKind::Sum => {
                    // Serial SUM switches to the float domain on the first
                    // REAL datum; keep the pipeline on arguments whose static
                    // type folds exactly (integer/decimal).
                    let arg = func.arg.as_ref()?;
                    if !matches!(
                        arg.static_type()?.eval_type(),
                        tidb_datatype::EvalType::Int | tidb_datatype::EvalType::Decimal
                    ) {
                        return None;
                    }
                }
                AggKind::Avg => {
                    // The pushed-down final form (partial count argument +
                    // one partial sum extra) folds exactly; plain AVG must
                    // avoid the float-domain switch like SUM does.
                    if func.extra_args.len() > 1 {
                        return None;
                    }
                    if func.extra_args.is_empty() {
                        let arg = func.arg.as_ref()?;
                        if !matches!(
                            arg.static_type()?.eval_type(),
                            tidb_datatype::EvalType::Int | tidb_datatype::EvalType::Decimal
                        ) {
                            return None;
                        }
                    }
                }
                // Order-sensitive or float-domain families cannot merge
                // without diverging from the serial path.
                _ => return None,
            }
        }
        // Low quotas belong on the spill-capable serial path.
        if self.memory.quota() > 0 && self.memory.quota() < 256 * 1024 * 1024 {
            return None;
        }
        // Go `builder.go:2062`: both concurrencies at 1 (or non-positive)
        // means "run serially".
        let (partial, final_concurrency) = self.resolved_pipeline_concurrency();
        if partial == 0 || final_concurrency == 0 || (partial == 1 && final_concurrency == 1) {
            return None;
        }
        Some((partial, final_concurrency))
    }
}

impl<C: Columns + Send + Sync + Clone + 'static + HashAggContext> HashAggExec<C> {
    /// Go `prepare4ParallelExec` fused with `parallelExec`'s consumption:
    /// the main thread fetches child chunks and round-robin-dispatches them
    /// to the partial-worker lanes; partial workers fold rows into their own
    /// maps and shuffle per-bucket sub-maps to the final workers; the final
    /// workers merge their buckets and hand ONE merged map each back; the
    /// main thread then finishes values in first-seen order.
    pub(super) fn execute_parallel_pipeline(&mut self) -> Result<(), ExecError> {
        let stats = Arc::clone(
            self.pipeline_stats
                .as_ref()
                .expect("pipeline stats installed"),
        );
        let partial_concurrency = stats.partial_concurrency;
        let final_concurrency = stats.final_concurrency;

        let abort = PipelineAbort::default();
        let (final_tx, final_rx) = channel::<FinalMsg>();

        // Input lanes (Go `partialInputChs`, capacity 1).
        let mut lane_txs = Vec::with_capacity(partial_concurrency);
        let mut lane_rxs = Vec::with_capacity(partial_concurrency);
        for _ in 0..partial_concurrency {
            let (tx, rx) = sync_channel::<(Chunk, u64)>(1);
            lane_txs.push(tx);
            lane_rxs.push(rx);
        }
        // Shuffle channels (Go `partialOutputChs`, capacity = partial count).
        let mut shuffle_txs = Vec::with_capacity(final_concurrency);
        let mut shuffle_rxs = Vec::with_capacity(final_concurrency);
        for _ in 0..final_concurrency {
            let (tx, rx) =
                sync_channel::<PipelineMap>(partial_concurrency.max(1));
            shuffle_txs.push(tx);
            shuffle_rxs.push(rx);
        }

        // The persistent pool's tasks are `'static`: clone the shared plan
        // pieces once into an Arc instead of borrowing them from the
        // executor. The clone cost is one pass over the small plan vectors
        // per aggregation; the saved thread spawns were a top profiling
        // cost on every grouped query.
        let plan = Arc::new(PipelinePlan {
            ctx: self.ctx.clone(),
            group_by: self.group_by.clone(),
            group_collations: self.group_collations.clone(),
            integer_columns: self.integer_group_columns.clone(),
            agg_funcs: self.agg_funcs.clone(),
        });

        // Split the borrows: the fetcher keeps the mutable executor state.
        let HashAggExec {
            child,
            child_chunk,
            child_returned_empty,
            truncated,
            memory,
            parallel_output,
            ..
        } = self;
        let memory: &StatementMemory = memory;
        let tracker: &Arc<Tracker> = &self.tracker;

        let mut base_seq = 0u64;
        let mut next_lane = 0usize;
        let mut fetch_error: Option<ExecError> = None;
        let mut child_drained = false;

        {
            // ---- Partial workers (Go `HashAggPartialWorker.run`). ----
            let mut partial_handles = Vec::with_capacity(partial_concurrency);
            for _ in 0..partial_concurrency {
                let lane_rx = lane_rxs.pop().expect("one receiver per partial worker");
                let shuffle_txs = shuffle_txs.clone();
                let final_tx = final_tx.clone();
                let abort = abort.clone();
                let stats_ref = Arc::clone(&stats);
                let tracker = Arc::clone(tracker);
                let plan = Arc::clone(&plan);
                partial_handles.push(crate::worker_pool::spawn(move || {
                    stats_ref.record_partial_worker();
                    let mut maps: Vec<PipelineMap> =
                        (0..shuffle_txs.len()).map(|_| PipelineMap::default()).collect();
                    let mut error: Option<ExecError> = None;
                    while let Ok((chunk, base)) = lane_rx.recv() {
                        if error.is_none() {
                            let fold = fold_chunk(
                                FoldInputs {
                                    ctx: &plan.ctx,
                                    group_by: &plan.group_by,
                                    group_collations: &plan.group_collations,
                                    integer_columns: plan.integer_columns.as_deref(),
                                    agg_funcs: &plan.agg_funcs,
                                },
                                &mut maps,
                                shuffle_txs.len(),
                                &tracker,
                                &chunk,
                                base,
                            );
                            if let Err(fold_error) = fold {
                                error = Some(fold_error);
                                abort.raise();
                            }
                        }
                        // Keep draining: the fetcher must never block forever
                        // on a lane whose worker stopped folding (Go drains
                        // `inputCh` in `finalizeWorkerProcess`).
                    }
                    match error {
                        None => {
                            // Go `shuffleIntermData`: one sub-map per bucket.
                            for (bucket, map) in maps.into_iter().enumerate() {
                                if shuffle_txs[bucket].send(map).is_err() {
                                    break;
                                }
                            }
                        }
                        Some(error) => {
                            let _ = final_tx.send(FinalMsg::Err(error));
                        }
                    }
                }));
            }

            // ---- Final workers (Go `HashAggFinalWorker.run`). ----
            let mut final_handles = Vec::with_capacity(final_concurrency);
            for _ in 0..final_concurrency {
                let shuffle_rx = shuffle_rxs.pop().expect("one receiver per final worker");
                let final_tx = final_tx.clone();
                final_handles.push(crate::worker_pool::spawn(move || {
                    let mut acc: Option<PipelineMap> = None;
                    while let Ok(map) = shuffle_rx.recv() {
                        // Go `mergeInputIntoResultMap`: the FIRST map becomes
                        // the accumulator directly; later maps merge in.
                        let merged = acc.get_or_insert_with(PipelineMap::default);
                        if let Err(error) = merge_map(merged, map) {
                            let _ = final_tx.send(FinalMsg::Err(error));
                        }
                    }
                    let _ = final_tx.send(FinalMsg::Maps(acc.unwrap_or_default()));
                }));
            }

            // ---- Fetcher: the MAIN thread (Go `fetchChildData`). ----
            loop {
                if abort.raised() {
                    break;
                }
                let before = child_chunk.memory_usage();
                if let Err(error) = child.next(child_chunk) {
                    fetch_error = Some(error);
                    break;
                }
                let rows = child_chunk.num_rows();
                if rows == 0 {
                    child_drained = true;
                    break;
                }
                *child_returned_empty = false;
                tracker.consume(child_chunk.memory_usage() - before);
                // The parallel spill helper is not ported yet (module docs):
                // the quota check between chunks is what bounds memory here.
                if let Err(error) = memory.check() {
                    fetch_error = Some(error);
                    break;
                }
                if abort.raised() {
                    break;
                }
                let replacement = child.new_chunk();
                let chunk = std::mem::replace(child_chunk, replacement);
                if lane_txs[next_lane].send((chunk, base_seq)).is_err() {
                    // The lane errored out and disconnected.
                    break;
                }
                stats
                    .dispatched_chunks
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                base_seq += rows as u64;
                next_lane = (next_lane + 1) % partial_concurrency;
            }
            // Close the lanes so every partial worker sees EOF and exits
            // (Go closes `partialInputChs` once its fetcher returns), then
            // wait for the partial workers so every shuffled map is sent
            // before the shuffle senders disappear, and for the final
            // workers to hand their merged maps back.
            drop(lane_txs);
            for handle in partial_handles {
                let _ = handle.recv();
            }
            drop(shuffle_txs);
            for handle in final_handles {
                let _ = handle.recv();
            }
        }

        // Release the final-channel sender held by THIS frame, then drain
        // until the workers' senders are gone (they have joined above).
        drop(final_tx);
        let mut merged_maps = Vec::with_capacity(final_concurrency);
        let mut first_error = fetch_error;
        while let Ok(message) = final_rx.recv() {
            match message {
                FinalMsg::Maps(map) => merged_maps.push(map),
                FinalMsg::Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        // Merge the final workers' bucket maps into one global map, keeping
        // each group's minimum first-seen sequence.
        let mut global = PipelineMap::default();
        for map in merged_maps {
            merge_map(&mut global, map)?;
        }
        let mut groups: Vec<PipelineGroup> = global.into_values().collect();
        groups.sort_unstable_by_key(|group| group.first_seq);

        // Finish values in first-seen order (the serial path's contract).
        let ret_types = self.meta.ret_field_types().to_vec();
        let width = plan.agg_funcs.len();
        parallel_output.clear();
        if groups.is_empty() && plan.group_by.is_empty() {
            // Go: no group-by and no data yields ONE empty group, so a
            // global COUNT is 0 rather than an empty result set.
            let mut states: Vec<AggState> = plan.agg_funcs.iter().map(AggState::new).collect();
            for (c, state) in states.iter_mut().enumerate() {
                let value = finish_agg_value(
                    state,
                    &plan.agg_funcs[c],
                    &ret_types[c],
                    &plan.ctx,
                    &mut truncated[c],
                )?;
                parallel_output.push(value);
            }
        } else {
            for group in &mut groups {
                for (c, state) in group.states.iter_mut().enumerate() {
                    let value = finish_agg_value(
                        state,
                        &plan.agg_funcs[c],
                        &ret_types[c],
                        &plan.ctx,
                        &mut truncated[c],
                    )?;
                    parallel_output.push(value);
                }
            }
        }
        self.parallel_output_width = width;
        self.parallel_output_cursor = 0;
        self.parallel_output_active = true;
        self.executed = true;
        if child_drained {
            self.is_child_drained = true;
        }
        Ok(())
    }
}

/// The plan pieces a partial worker folds rows with.
struct FoldInputs<'a, C> {
    ctx: &'a C,
    group_by: &'a [Expression],
    group_collations: &'a [tidb_datatype::Collation],
    integer_columns: Option<&'a [(usize, bool)]>,
    agg_funcs: &'a [AggFunc],
}

/// Go `HashAggPartialWorker.updatePartialResult`: encode every row's group
/// key (identically to the serial `fold_chunk`), route it to a bucket, open
/// the group on first sight (charging the tracker), and update its states.
fn fold_chunk<C: Columns>(
    inputs: FoldInputs<'_, C>,
    maps: &mut [PipelineMap],
    bucket_count: usize,
    tracker: &Arc<Tracker>,
    chunk: &Chunk,
    base_seq: u64,
) -> Result<(), ExecError> {
    let FoldInputs {
        ctx,
        group_by,
        group_collations,
        integer_columns,
        agg_funcs,
    } = inputs;
    let mut new_group_bytes_total = 0i64;
    for row_index in 0..chunk.num_rows() {
        let row = chunk.get_row(row_index);
        let seq = base_seq.saturating_add(row_index as u64);
        let (key, key_len): (PipelineMapKey, usize) = match integer_columns {
            Some([(index, false)]) => {
                let index = *index;
                let value = if row.is_null(index) {
                    None
                } else {
                    Some(row.get_int64(index))
                };
                let key = PipelineMapKey::Int(value);
                let len = key.charge_len();
                (key, len)
            }
            _ => {
                let mut key = Vec::new();
                match integer_columns {
                    Some(columns) => {
                        for &(index, unsigned) in columns {
                            append_integer_group_key_part(row, index, unsigned, &mut key);
                            key.push(0xff);
                        }
                    }
                    None => {
                        for (expr, collation) in group_by.iter().zip(group_collations) {
                            let datum = expr.eval(ctx, row)?;
                            append_group_key_part(collation, &datum, &mut key);
                            key.push(0xff); // separator: key parts are length-coded
                        }
                    }
                }
                let len = key.len();
                (PipelineMapKey::Bytes(key), len)
            }
        };
        let bucket = match &key {
            PipelineMapKey::Int(value) => key_bucket(
                &match value {
                    Some(v) => v.to_le_bytes().to_vec(),
                    None => vec![NIL_FLAG],
                },
                bucket_count,
            ),
            PipelineMapKey::Bytes(bytes) => key_bucket(bytes, bucket_count),
        };
        let entry = match maps[bucket].entry(key) {
            std::collections::hash_map::Entry::Occupied(occupied) => occupied.into_mut(),
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let (group, bytes) = PipelineGroup::new(agg_funcs, seq, key_len);
                new_group_bytes_total += bytes;
                vacant.insert(group)
            }
        };
        update_group(entry, agg_funcs, ctx, row, tracker)?;
    }
    if new_group_bytes_total > 0 {
        tracker.consume(new_group_bytes_total);
    }
    Ok(())
}

/// Go `UpdatePartialResult` per function for one row of an already-opened
/// group. Mirrors the serial `update_group`'s semantics (COUNT's NULL skip,
/// FIRST_ROW's once-only capture) minus the typed-column fast paths, whose
/// results are identical by construction.
fn update_group<C: Columns>(
    group: &mut PipelineGroup,
    agg_funcs: &[AggFunc],
    ctx: &C,
    row: tidb_chunk::row::Row<'_>,
    tracker: &Arc<Tracker>,
) -> Result<(), ExecError> {
    let mut delta = 0i64;
    for (c, func) in agg_funcs.iter().enumerate() {
        let state = &mut group.states[c];
        if matches!(func.kind, AggKind::Count)
            && !func.distinct
            && func.extra_args.is_empty()
            && func.order_by.is_empty()
        {
            let input_is_non_null = match func.arg.as_ref() {
                None => Some(true),
                Some(expr) => expr.as_column().and_then(|column| {
                    usize::try_from(column.index)
                        .ok()
                        .map(|index| !row.is_null(index))
                }),
            };
            if input_is_non_null.is_some_and(|present| state.update_count_fast(present)) {
                continue;
            }
        }
        if matches!(func.kind, AggKind::FirstRow) && state.has_first_row() {
            continue;
        }
        let mut extra_values = Vec::new();
        let input = eval_agg_input(func, ctx, row, &mut extra_values)?;
        if let Some((coefficient, scale)) = input.decimal_coefficient {
            if state.partial_update_with_coefficient(coefficient, scale) {
                continue;
            }
            // The fast fold refused (overflow): replay via the complete
            // path with the materialized datum.
            let value = tidb_datatype::Datum::Decimal(tidb_datatype::Decimal::from_scaled_i128(
                coefficient,
                scale,
            ));
            delta += state.update(Some(value), &extra_values, Vec::new(), input.distinct_key)?;
            continue;
        }
        delta += state.update(input.value, &extra_values, Vec::new(), input.distinct_key)?;
    }
    tracker.consume(delta);
    Ok(())
}

/// Merges one shuffled sub-map into an accumulator (Go
/// `mergeInputIntoResultMap`: a fresh accumulator adopts the first map
/// as-is).
fn merge_map(
    global: &mut PipelineMap,
    incoming: PipelineMap,
) -> Result<(), ExecError> {
    for (key, group) in incoming {
        match global.entry(key) {
            Entry::Vacant(slot) => {
                slot.insert(group);
            }
            Entry::Occupied(mut slot) => merge_groups(slot.get_mut(), group)?,
        }
    }
    Ok(())
}

/// Merges two copies of one group, keeping the earliest first-seen sequence
/// so downstream ordering stays deterministic.
fn merge_groups(dst: &mut PipelineGroup, src: PipelineGroup) -> Result<(), ExecError> {
    let src_is_first = src.first_seq < dst.first_seq;
    if src_is_first {
        dst.first_seq = src.first_seq;
    }
    for (c, state) in src.states.into_iter().enumerate() {
        merge_state(&mut dst.states[c], state, src_is_first)?;
    }
    Ok(())
}

/// Go `MergePartialResult` for exactly the aggregate kinds eligibility lets
/// through: every arm folds EXACTLY (integer/decimal domain or order-free
/// comparison), so a merged result equals the serial accumulation bit for
/// bit. Any other pair is an eligibility-gate bug, not a value.
fn merge_state(dst: &mut AggState, mut src: AggState, src_is_first: bool) -> Result<(), ExecError> {
    // Fixed-scale AVG accumulators over the same column share one scale; a
    // representation or scale mismatch materializes both sides into full
    // decimals so the merge stays exact.
    let scales_match = matches!(
        (&dst.partial, &src.partial),
        (
            Partial::AvgDecimalFast { scale: a, .. },
            Partial::AvgDecimalFast { scale: b, .. }
        ) if a == b
    );
    if !scales_match
        && (matches!(dst.partial, Partial::AvgDecimalFast { .. })
            || matches!(src.partial, Partial::AvgDecimalFast { .. }))
    {
        dst.partial.materialize_avg_fast();
        src.partial.materialize_avg_fast();
    }
    match (&mut dst.partial, src.partial) {
        (Partial::Count(a), Partial::Count(b)) => *a = a.wrapping_add(b),
        (Partial::FinalCount(a), Partial::FinalCount(b)) => *a = a.wrapping_add(b),
        (Partial::SumDecimal(a), Partial::SumDecimal(b)) => {
            if let Some(sum) = b {
                *a = Some(match a.take() {
                    Some(current) => current.add(&sum),
                    None => sum,
                });
            }
        }
        (Partial::FirstRow(slot), Partial::FirstRow(value)) => {
            if slot.is_none() || (src_is_first && value.is_some()) {
                *slot = value;
            }
        }
        (
            Partial::MaxMin {
                value: dst_value,
                is_max,
            },
            Partial::MaxMin {
                value: src_value, ..
            },
        ) => match (dst_value.as_mut(), src_value) {
            (_, None) => {}
            (None, Some(value)) => *dst_value = Some(value),
            (Some(current), Some(value)) => {
                let ordering =
                    tidb_expr::compare_datums_with_collation(&value, current, dst.collation)?;
                if (*is_max && ordering == Ordering::Greater)
                    || (!*is_max && ordering == Ordering::Less)
                {
                    *current = value;
                }
            }
        },
        (
            Partial::AvgDecimal {
                sum: dst_sum,
                count: dst_count,
            },
            Partial::AvgDecimal {
                sum: src_sum,
                count: src_count,
            },
        ) => {
            *dst_sum = dst_sum.add(&src_sum);
            *dst_count = dst_count.wrapping_add(src_count);
        }
        (
            Partial::AvgDecimalFast {
                sum: dst_sum,
                count: dst_count,
                ..
            },
            Partial::AvgDecimalFast {
                sum: src_sum,
                count: src_count,
                ..
            },
        ) => {
            *dst_sum = dst_sum.wrapping_add(src_sum);
            *dst_count = dst_count.wrapping_add(src_count);
        }
        (
            Partial::SumDecimalFast {
                sum: dst_sum,
                scale: dst_scale,
            },
            Partial::SumDecimalFast {
                sum: src_sum,
                scale: src_scale,
            },
        ) if dst_scale == &src_scale => {
            *dst_sum = dst_sum.wrapping_add(src_sum);
        }
        (state @ Partial::SumDecimalFast { .. }, Partial::SumDecimal(None)) => {
            // An empty partial contributes nothing to a Fast accumulator.
            let _ = state;
        }
        // A Fast state adopting an empty partial, or vice versa: the empty
        // side contributes nothing.
        (Partial::SumDecimal(None), Partial::SumDecimalFast { .. }) => {
            // dst keeps its own accumulator; nothing to add.
        }
        (state @ Partial::SumDecimalFast { .. }, Partial::SumDecimal(None)) => {
            let _ = state;
            // dst's materialized Decimal already holds the total.
        }
        // Mixed Fast/materialized states arise only after an overflow
        // replay materialized BOTH sides into SumDecimal(Some); they take
        // the exact merge arm below. A lone mismatch is unreachable.
        (Partial::Bit { acc: dst_acc, op }, Partial::Bit { acc: src_acc, .. }) => match op {
            BitOp::And => *dst_acc &= src_acc,
            BitOp::Or => *dst_acc |= src_acc,
            BitOp::Xor => *dst_acc ^= src_acc,
        },
        _ => {
            return Err(ExecError::unsupported(
                "aggregate kind reached the parallel merge gate unfiltered",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::NoColumns;
    use tidb_expr::column::Column;

    fn long() -> FieldType {
        FieldType::new(FieldTypeCode::LongLong)
    }

    fn decimal() -> FieldType {
        FieldType::new(FieldTypeCode::NewDecimal)
    }

    fn col(index: i64) -> Expression {
        let mut c = Column::new(index + 1, long());
        c.index = index;
        Expression::Column(c)
    }

    fn decimal_col(index: i64) -> Expression {
        let mut c = Column::new(index + 1, decimal());
        c.index = index;
        Expression::Column(c)
    }

    /// A source emitting `rows` in fixed-size chunks, so the pipeline sees
    /// more input chunks than lanes.
    struct MultiChunkSource {
        meta: ExecutorMeta,
        fields: Vec<FieldType>,
        data: Chunk,
        offset: usize,
        chunk_size: usize,
    }
    impl MultiChunkSource {
        fn new(rows: &[(i64, i64)], chunk_size: usize) -> Box<dyn Executor> {
            let fields = vec![long(), long()];
            let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
            for (g, v) in rows {
                data.append_int64(0, *g);
                data.append_int64(1, *v);
            }
            let mut cols = Vec::new();
            for i in 0..2 {
                let mut c = Column::new(i + 1, long());
                c.index = i;
                cols.push(c);
            }
            Box::new(MultiChunkSource {
                meta: ExecutorMeta::new(Schema::new(cols), 0, chunk_size, chunk_size),
                fields,
                data,
                offset: 0,
                chunk_size,
            })
        }
    }
    impl Executor for MultiChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.offset = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.offset + self.chunk_size).min(self.data.num_rows());
            while self.offset < end {
                req.append_row(self.data.get_row(self.offset));
                self.offset += 1;
            }
            Ok(())
        }
        fn close(&mut self) -> Result<(), ExecError> {
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
            Chunk::new_with_capacity(&self.fields, self.chunk_size)
        }
    }

    fn out_meta(types: &[FieldType]) -> ExecutorMeta {
        let mut cols = Vec::new();
        for (i, t) in types.iter().enumerate() {
            let mut c = Column::new((i + 1) as i64, t.clone());
            c.index = i as i64;
            cols.push(c);
        }
        // Output chunk cap 4 forces several next() round trips.
        ExecutorMeta::new(Schema::new(cols), 9, 4, 1024)
    }

    /// Drains every row of an OPENED aggregation without closing it.
    fn drain_rows(exec: &mut HashAggExec<NoColumns>) -> Vec<Vec<Datum>> {
        let types: Vec<FieldType> = exec.ret_field_types().to_vec();
        let mut req = exec.new_chunk();
        let mut out = Vec::new();
        loop {
            exec.next(&mut req).unwrap();
            if req.num_rows() == 0 {
                break;
            }
            for r in 0..req.num_rows() {
                let row = req.get_row(r);
                out.push(
                    (0..req.num_cols())
                        .map(|c| row.get_datum(c, &types[c]))
                        .collect(),
                );
            }
        }
        out
    }

    /// Drives one aggregation to completion and collects every row.
    fn run(exec: &mut HashAggExec<NoColumns>) -> Vec<Vec<Datum>> {
        exec.open().unwrap();
        let rows = drain_rows(exec);
        exec.close().unwrap();
        rows
    }

    fn build(
        group_by: Vec<Expression>,
        funcs: Vec<AggFunc>,
        child: Box<dyn Executor>,
        types: &[FieldType],
    ) -> HashAggExec<NoColumns> {
        HashAggExec::new(
            out_meta(types),
            group_by,
            funcs,
            child,
            NoColumns,
            StatementMemory::default(),
        )
    }

    const GROUPS: i64 = 97;
    const ROWS_PER_GROUP: usize = 400;
    const CHUNK_SIZE: usize = 100;

    fn dataset() -> Vec<(i64, i64)> {
        // Deterministic spread: group g gets values that stress COUNT/SUM/
        // MIN/MAX/FIRST_ROW, including NULL-adjacent extremes and negatives.
        (0..GROUPS as usize * ROWS_PER_GROUP)
            .map(|i| {
                let g = (i / ROWS_PER_GROUP) as i64;
                let v = match i % 7 {
                    0 => -3 - (i as i64 % 50),
                    1 => 1_000_000 + g,
                    2 => -(g * 31),
                    _ => ((i as i64) * 37 % 997) - 300,
                };
                (g, v)
            })
            .collect()
    }

    fn count_sum_min_max_first_funcs() -> Vec<AggFunc> {
        vec![
            AggFunc::new(AggKind::Count, Some(col(1))),
            AggFunc::new(AggKind::Count, None),
            AggFunc::new(AggKind::Sum, Some(col(1))),
            AggFunc::new(AggKind::Min, Some(col(1))),
            AggFunc::new(AggKind::Max, Some(col(1))),
            AggFunc::new(AggKind::FirstRow, Some(col(0))),
        ]
    }

    fn wide_out_types() -> Vec<FieldType> {
        vec![
            long(),
            long(),
            decimal(), // integer SUM lands in DECIMAL
            long(),
            long(),
            long(),
        ]
    }

    /// FAIL-BEFORE/PASS-AFTER regression: the pipeline must engage (worker
    /// threads ran, every chunk dispatched) and produce EXACTLY the serial
    /// path's rows in the serial path's first-seen order.
    #[test]
    fn pipeline_matches_serial_path_and_uses_multiple_workers() {
        let data = dataset();

        // Serial reference: both concurrencies at 1 (Go's IsUnparallelExec
        // workaround rule keeps this shape on `unparallelExec`).
        let mut serial_exec = build(
            vec![col(0)],
            count_sum_min_max_first_funcs(),
            MultiChunkSource::new(&data, CHUNK_SIZE),
            &wide_out_types(),
        );
        let serial_rows = run(&mut serial_exec);
        assert_eq!(serial_rows.len(), GROUPS as usize, "one row per group");

        // Pipeline under test: default concurrency resolves to >1 workers
        // for NoColumns.
        let mut parallel_exec = build(
            vec![col(0)],
            count_sum_min_max_first_funcs(),
            MultiChunkSource::new(&data, CHUNK_SIZE),
            &wide_out_types(),
        );
        assert!(
            parallel_exec.pipeline_eligibility().is_some(),
            "this aggregate shape must be pipeline-eligible"
        );
        let (partial, final_) = parallel_exec.resolved_pipeline_concurrency();
        assert!(partial > 1 && final_ > 1, "defaults must exceed 1 worker");
        parallel_exec.open().unwrap();
        let parallel_rows = drain_rows(&mut parallel_exec);
        // Diagnostics must be read while the Open is still live: `close`
        // releases the pipeline stats.
        let info = parallel_exec.pipeline_run_info().expect("pipeline ran");
        parallel_exec.close().unwrap();

        assert_eq!(
            parallel_rows, serial_rows,
            "pipeline output must equal the serial path line for line"
        );

        // Concurrency evidence: every lane received a share of the chunks
        // (round-robin over {chunks} >= lanes), and more than ONE partial-
        // worker thread executed. Both are impossible on the serial path.
        let (_p, _f, dispatched, threads) = info;
        let expected_chunks = data.len().div_ceil(CHUNK_SIZE);
        assert_eq!(dispatched, expected_chunks, "every chunk was folded");
        assert!(threads > 1, "multiple partial-worker threads ran");
    }

    /// The Go builder's workaround rule: concurrency 1/1 stays serial even
    /// when the context could support the pipeline.
    #[test]
    fn concurrency_one_keeps_serial_path() {
        let exec = build(
            vec![col(0)],
            count_sum_min_max_first_funcs(),
            MultiChunkSource::new(&[(1, 5)], 1),
            &wide_out_types(),
        )
        .with_pipeline_concurrency_override(1, 1);
        assert!(exec.pipeline_eligibility().is_none());
    }

    /// DISTINCT aggregates keep Go's `IsUnparallelExec` fallback.
    #[test]
    fn distinct_aggregate_stays_serial() {
        let mut func = AggFunc::new(AggKind::Count, Some(col(1)));
        func.distinct = true;
        let exec = build(
            vec![col(0)],
            vec![func],
            MultiChunkSource::new(&[(1, 5)], 1),
            &[long()],
        );
        assert!(exec.pipeline_eligibility().is_none());
    }

    /// REAL-domain SUM is excluded from the exactness gate.
    #[test]
    fn real_sum_is_not_pipeline_eligible() {
        let real_type = FieldType::new(FieldTypeCode::Double);
        let mut column = Column::new(2, real_type);
        column.index = 1;
        let func = AggFunc::new(AggKind::Sum, Some(Expression::Column(column)));
        let exec = build(
            vec![col(0)],
            vec![func],
            MultiChunkSource::new(&[(1, 5)], 1),
            &[long()],
        );
        assert!(exec.pipeline_eligibility().is_none());
    }

    /// Empty input with no group-by emits exactly one defaults row through
    /// the pipeline, like `unparallelExec` does.
    #[test]
    fn empty_input_global_aggregate_emits_defaults_row() {
        let funcs = count_sum_min_max_first_funcs();
        let expected = {
            let mut exec = build(
                vec![],
                funcs.clone(),
                MultiChunkSource::new(&[], CHUNK_SIZE),
                &wide_out_types(),
            )
            .with_pipeline_concurrency_override(1, 1);
            run(&mut exec)
        };
        let mut exec = build(
            vec![],
            funcs,
            MultiChunkSource::new(&[], CHUNK_SIZE),
            &wide_out_types(),
        );
        assert!(exec.pipeline_eligibility().is_some());
        assert_eq!(run(&mut exec), expected);
        assert_eq!(expected.len(), 1);
    }

    /// AVG over an integer argument folds in the exact decimal domain and
    /// matches the serial path bit for bit across a multi-lane fold.
    #[test]
    fn avg_decimal_matches_serial() {
        let data: Vec<(i64, i64)> = (0..5000)
            .map(|i| (i64::from(i % 13), (i as i64) * 7 - 900))
            .collect();
        let funcs = || vec![AggFunc::new(AggKind::Avg, Some(col(1)))];
        let types = [decimal()];

        let mut serial_exec = build(
            vec![col(0)],
            funcs(),
            MultiChunkSource::new(&data, 128),
            &types,
        )
        .with_pipeline_concurrency_override(1, 1);
        let expected = run(&mut serial_exec);

        let mut exec = build(
            vec![col(0)],
            funcs(),
            MultiChunkSource::new(&data, 128),
            &types,
        );
        assert!(exec.pipeline_eligibility().is_some());
        assert_eq!(run(&mut exec), expected);
    }

    /// A test context answering session-variable reads from a map, to prove
    /// the concurrency settings actually steer worker counts.
    #[derive(Clone)]
    struct VarCtx(HashMap<String, String>);
    impl Columns for VarCtx {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }
        fn sysvar(&self, scope: Option<tidb_ast::SysVarScope>, name: &str) -> Option<Datum> {
            if scope.is_none() {
                self.0
                    .get(name)
                    .map(|value| Datum::Bytes(value.clone().into_bytes()))
            } else {
                None
            }
        }
    }
    impl HashAggContext for VarCtx {
        const PARALLEL_WORKERS_MAY_EVAL: bool = true;

        fn run_parallel_pipeline_bridge(
            exec: &mut HashAggExec<Self>,
        ) -> Option<Result<(), ExecError>> {
            Some(exec.execute_parallel_pipeline())
        }
    }

    #[test]
    fn session_variables_resolve_worker_counts() {
        let vars = |partial: &str, final_: &str| {
            HashMap::from([
                (
                    "tidb_hashagg_partial_concurrency".to_owned(),
                    partial.to_owned(),
                ),
                (
                    "tidb_hashagg_final_concurrency".to_owned(),
                    final_.to_owned(),
                ),
            ])
        };
        let make = |map: HashMap<String, String>| {
            HashAggExec::new(
                out_meta(&[long()]),
                vec![col(0)],
                vec![AggFunc::new(AggKind::Count, Some(col(1)))],
                MultiChunkSource::new(&[(1, 1)], 1),
                VarCtx(map),
                StatementMemory::default(),
            )
        };
        let exec = make(vars("7", "9"));
        assert_eq!(exec.resolved_pipeline_concurrency(), (7, 9));
        assert_eq!(exec.pipeline_eligibility(), Some((7, 9)));

        // Unset falls back to tidb_executor_concurrency.
        let exec = make(HashMap::from([(
            "tidb_executor_concurrency".to_owned(),
            "6".to_owned(),
        )]));
        assert_eq!(exec.resolved_pipeline_concurrency(), (6, 6));

        // 1/1 refuses the pipeline (Go builder.go).
        let exec = make(vars("1", "1"));
        assert_eq!(exec.pipeline_eligibility(), None);
    }

    /// FAIL-BEFORE/PASS-AFTER: the PRODUCTION statement context — the one
    /// `Session` builds, whose session handles are `Arc` + lock/atomic shared
    /// state — must drive the worker pipeline itself, not only `NoColumns`
    /// and test contexts. Before the context became shareable,
    /// `PARALLEL_WORKERS_MAY_EVAL` was false for it: `open` never installed
    /// pipeline stats, so `pipeline_run_info()` returned `None` and this test
    /// failed at the `expect` below.
    #[test]
    fn production_stmt_context_drives_the_pipeline() {
        fn build_with_ctx(
            group_by: Vec<Expression>,
            funcs: Vec<AggFunc>,
            child: Box<dyn Executor>,
            types: &[FieldType],
            ctx: crate::StmtContext,
        ) -> HashAggExec<crate::StmtContext> {
            HashAggExec::new(
                out_meta(types),
                group_by,
                funcs,
                child,
                ctx,
                StatementMemory::default(),
            )
        }

        fn drain(exec: &mut HashAggExec<crate::StmtContext>) -> Vec<Vec<Datum>> {
            let types: Vec<FieldType> = exec.ret_field_types().to_vec();
            let mut req = exec.new_chunk();
            let mut out = Vec::new();
            loop {
                exec.next(&mut req).unwrap();
                if req.num_rows() == 0 {
                    break;
                }
                for r in 0..req.num_rows() {
                    let row = req.get_row(r);
                    out.push(
                        (0..req.num_cols())
                            .map(|c| row.get_datum(c, &types[c]))
                            .collect(),
                    );
                }
            }
            out
        }

        let data = dataset();

        // Serial reference: both concurrencies forced to 1.
        let mut serial_exec = build_with_ctx(
            vec![col(0)],
            count_sum_min_max_first_funcs(),
            MultiChunkSource::new(&data, CHUNK_SIZE),
            &wide_out_types(),
            crate::StmtContext::for_query(),
        )
        .with_pipeline_concurrency_override(1, 1);
        serial_exec.open().unwrap();
        let serial_rows = drain(&mut serial_exec);
        serial_exec.close().unwrap();

        // The same aggregate under a real `StmtContext`, default concurrency:
        // the pipeline must engage AND reproduce the serial output exactly.
        let mut exec = build_with_ctx(
            vec![col(0)],
            count_sum_min_max_first_funcs(),
            MultiChunkSource::new(&data, CHUNK_SIZE),
            &wide_out_types(),
            crate::StmtContext::for_query(),
        );
        assert!(
            exec.pipeline_eligibility().is_some(),
            "the aggregate shape is pipeline-eligible"
        );
        exec.open().unwrap();
        let rows = drain(&mut exec);
        let info = exec
            .pipeline_run_info()
            .expect("production StmtContext selected the parallel pipeline");
        exec.close().unwrap();

        assert_eq!(
            rows, serial_rows,
            "pipeline output must equal the serial path line for line"
        );
        let (_partial, _final_, dispatched, threads) = info;
        assert!(dispatched > 0, "every chunk was dispatched to workers");
        assert!(threads > 1, "multiple partial-worker threads ran");
    }
}
