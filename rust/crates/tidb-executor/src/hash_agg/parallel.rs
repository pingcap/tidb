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
//! `agg_hash_final_worker.go`), using reusable chunk tasks and channels.
//!
//! # Worker topology (mirrors Go's ASCII diagram)
//!
//! ```text
//!                          +-------------+
//!                          | main thread |   consumes finalOutputCh / emits
//!                          +------+------+
//!                                 ^
//!                                 |  final_output_ch (bounded chunks)
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
//! * `partialInputChs[i]` is a worker-local command queue. Worker-bound
//!   resource tokens limit it to one pending chunk; each runnable chunk is
//!   submitted to the shared executor pool, retaining Go's backpressure.
//! * `inputCh`/`giveBackCh` is a bounded queue of worker-bound reusable
//!   chunks. Workers swap columns and return their previous buffer before
//!   folding, so there are exactly two chunk owners per partial worker.
//! * Rust transfers owned mapper vectors through partial-worker barrier
//!   replies. A spill serializes them directly; otherwise one merge task per
//!   final bucket preserves the N-to-M partitioning.
//! * `finalOutputCh` streams chunks across `Next` calls. Each final worker
//!   resumes when its result holder is returned before producing more rows.
//!   The holder owns its continuation, not a blocked native worker thread.
//! * `finishCh` becomes [`PipelineAbort`] plus channel disconnects. Every
//!   worker DRAINS its inputs even after an error (Go's
//!   `finalizeWorkerProcess`), so no sender or receiver can block forever --
//!   the same liveness Go's `select` on `finishCh` provides.
//!
//! Each partial worker owns `M` sub-maps (Go's
//! `HashAggPartialWorker.partialResultsMap[finalConcurrency]`); a group key
//! routes to final worker `bucket(key) % M`, so one group's partial pieces
//! all land on one final worker. Partitioning uses Go's
//! `murmur3.Sum32(groupKey) % finalConcurrency`. Like Go's parallel HashAgg,
//! no global first-seen sequence or Rust-only result sort is maintained.
//!
//! # What stays serial, and why
//!
//! * Aggregate ORDER BY -> serial: Go's `IsUnparallelExec`
//!   (`pkg/executor/builder.go:2162`). DISTINCT is not such a gate: Go keeps
//!   worker-local value sets and unions them in `MergePartialResult`, which
//!   [`merge_state`] reproduces.
//! * `partial == 1 && final == 1` (or either `<= 0`) -> serial: Go's
//!   builder.go workaround rule.
//! * Every aggregate without its own ORDER BY uses the partial/final worker
//!   path, including REAL, variance, JSON, approximate and DISTINCT families,
//!   matching Go's builder admission.
//! * Context shareability: Go passes `sessionctx.Context` to every worker;
//!   the Rust evaluation context must be shareable too, which
//!   [`HashAggContext`] declares. Production `StmtContext` now qualifies:
//!   its session handles are `Arc` + lock/atomic shared state, so both it and
//!   `NoColumns` drive the pipeline.
//!
//! # Spill interaction
//!
//! When aggregate memory tracking, temporary storage, and
//! `tidb_enable_parallel_hashagg_spill` are enabled, the soft-limit action
//! drains the in-flight chunks and writes every partial-worker map across
//! 256 Murmur3 partitions. Final workers restore one partition at a time and
//! merge its serialized partial states. DISTINCT partial states carry their
//! retained value inputs through the spill record so final workers can union
//! worker-local sets exactly as Go does.

use super::spill::parallel_new_group_bytes;
use super::*;
use hashbrown::hash_map::{Entry, EntryRef};
use hashbrown::HashMap as SwissMap;
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
use std::sync::Mutex;
use tidb_codec::JoinKeyColumn;
use tidb_vardef::tidb_vars::{
    TIDB_ENABLE_PARALLEL_HASHAGG_SPILL, TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
};

/// Marks an evaluation context that [`HashAggExec`] accepts.
///
/// Implementors state whether worker threads may evaluate expressions through
/// a shared `&Self` ([`Self::PARALLEL_WORKERS_MAY_EVAL`]); the bridge method
/// carries the `Self: Sync` proof into the pipeline without imposing that
/// bound on contexts that cannot honor it.
pub trait HashAggContext: Columns + Send {
    /// Whether `&Self` may be dereferenced concurrently by the hash-aggregate
    /// worker threads. Go shares its session context with every worker
    /// freely; a Rust context may do the same only when it is `Sync`.
    const PARALLEL_WORKERS_MAY_EVAL: bool;

    /// Resolved worker counts carried by Go's typed `SessionVars` fields.
    /// Contexts without a SQL session leave this absent and use the generic
    /// variable/default fallback below.
    fn hashagg_concurrency(&self) -> Option<(usize, usize)> {
        None
    }

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

    fn hashagg_concurrency(&self) -> Option<(usize, usize)> {
        Some(crate::StmtContext::hashagg_concurrency(self))
    }

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

/// Unit-test observations kept out of production HashAgg execution.
#[cfg(test)]
#[derive(Default)]
pub(super) struct PipelineStats {
    /// Resolved worker counts (Go's session concurrency variables).
    pub(super) partial_concurrency: usize,
    pub(super) final_concurrency: usize,
    /// Chunks successfully dispatched by the fetcher.
    pub(super) dispatched_chunks: AtomicUsize,
    /// Input and work chunks allocated for the configured partial workers.
    pub(super) input_chunks: AtomicUsize,
    final_output_chunks: AtomicUsize,
    finalized_chunks: AtomicUsize,
    final_workers_finished: AtomicUsize,
    /// Ids of the partial-worker threads that actually ran.
    pub(super) partial_worker_threads: Mutex<Vec<std::thread::ThreadId>>,
}

#[cfg(test)]
impl PipelineStats {
    pub(super) fn new(partial_concurrency: usize, final_concurrency: usize) -> Self {
        PipelineStats {
            partial_concurrency,
            final_concurrency,
            dispatched_chunks: AtomicUsize::new(0),
            input_chunks: AtomicUsize::new(0),
            final_output_chunks: AtomicUsize::new(0),
            finalized_chunks: AtomicUsize::new(0),
            final_workers_finished: AtomicUsize::new(0),
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
    integer_columns: Option<Vec<usize>>,
    agg_funcs: Vec<AggFunc>,
    input_modes: Vec<AggInputMode>,
    /// Each aggregate's argument collation, derived once for the plan as Go
    /// builds each `AggFunc`'s collator once (`aggfuncs/builder.go`), not
    /// once per group.
    collations: Vec<tidb_datatype::Collation>,
}

/// A pipeline group-map key. A single integer group item keys by its chunk
/// lane directly so the native map does not allocate one byte vector per
/// group; every other shape keeps Go's encoded group key.
#[derive(Clone, PartialEq, Eq)]
enum PipelineMapKey {
    Int(Option<i64>),
    Bytes(Vec<u8>),
}

/// The integer lane hashes its eight value bytes (a NULL its flag byte)
/// and the encoded lane its key bytes, as Go hashes the encoded group key
/// itself; the derived form would also feed both discriminants through the
/// byte hash for every row.
impl std::hash::Hash for PipelineMapKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            PipelineMapKey::Int(Some(value)) => state.write_u64(*value as u64),
            PipelineMapKey::Int(None) => state.write_u8(NIL_FLAG),
            PipelineMapKey::Bytes(bytes) => state.write(bytes),
        }
    }
}

impl PipelineMapKey {
    fn as_ref(&self) -> PipelineMapKeyRef<'_> {
        match self {
            PipelineMapKey::Int(value) => PipelineMapKeyRef::Int(*value),
            PipelineMapKey::Bytes(bytes) => PipelineMapKeyRef::Bytes(bytes),
        }
    }
}

/// A row's group key as the fold builds it, before the map holds it: Go's
/// `updatePartialResult` reuses the row's `groupKey[i][:0]` buffer and copies
/// the bytes into the map (`string(groupKey[i])`) only when it opens a group,
/// so a row of an existing group costs no allocation. Hashes and compares
/// exactly as the owned [`PipelineMapKey`] it looks up.
#[derive(Clone, Copy)]
enum PipelineMapKeyRef<'a> {
    Int(Option<i64>),
    Bytes(&'a [u8]),
}

impl std::hash::Hash for PipelineMapKeyRef<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            PipelineMapKeyRef::Int(Some(value)) => state.write_u64(*value as u64),
            PipelineMapKeyRef::Int(None) => state.write_u8(NIL_FLAG),
            PipelineMapKeyRef::Bytes(bytes) => state.write(bytes),
        }
    }
}

impl hashbrown::Equivalent<PipelineMapKey> for PipelineMapKeyRef<'_> {
    fn equivalent(&self, key: &PipelineMapKey) -> bool {
        match (self, key) {
            (PipelineMapKeyRef::Int(value), PipelineMapKey::Int(other)) => value == other,
            (PipelineMapKeyRef::Bytes(bytes), PipelineMapKey::Bytes(other)) => {
                *bytes == other.as_slice()
            }
            _ => false,
        }
    }
}

/// The owned key a vacant slot stores, copying the encoded bytes once.
impl From<&PipelineMapKeyRef<'_>> for PipelineMapKey {
    fn from(key: &PipelineMapKeyRef<'_>) -> Self {
        match key {
            PipelineMapKeyRef::Int(value) => PipelineMapKey::Int(*value),
            PipelineMapKeyRef::Bytes(bytes) => PipelineMapKey::Bytes(bytes.to_vec()),
        }
    }
}

impl PipelineMapKeyRef<'_> {
    /// The byte length `new_group_bytes` was charging under the encoded
    /// representation, kept for tracker continuity.
    fn charge_len(self) -> usize {
        match self {
            // Go preallocates ten bytes per group item in `GetGroupKey`.
            PipelineMapKeyRef::Int(_) => 10,
            PipelineMapKeyRef::Bytes(bytes) => bytes.len(),
        }
    }
}

/// Go's mapper stores references to stable partial results. Native indices
/// keep those references valid when the Swiss table or state vector grows,
/// without a lock or pointer to an entry that rehashing could move.
/// Use the native map's per-map seeded hasher, as Go uses its runtime map;
/// Murmur3 worker/spill routing remains independent of this bucket hash.
#[derive(Default)]
struct PipelineMap {
    index: SwissMap<PipelineMapKey, usize>,
    groups: Vec<PipelineGroup>,
}

impl PipelineMap {
    fn resolve(
        &mut self,
        key: PipelineMapKeyRef<'_>,
        funcs: &[AggFunc],
        collations: &[tidb_datatype::Collation],
    ) -> (usize, i64) {
        match self.index.entry_ref(&key) {
            EntryRef::Occupied(slot) => (*slot.get(), 0),
            EntryRef::Vacant(slot) => {
                let (group, bytes) = PipelineGroup::new(funcs, collations, key.charge_len());
                let index = self.groups.len();
                self.groups.push(group);
                slot.insert(index);
                (index, bytes)
            }
        }
    }

    fn merge(&mut self, key: PipelineMapKey, group: PipelineGroup) -> Result<(), ExecError> {
        match self.index.entry(key) {
            Entry::Vacant(slot) => {
                let index = self.groups.len();
                self.groups.push(group);
                slot.insert(index);
                Ok(())
            }
            Entry::Occupied(slot) => merge_groups(&mut self.groups[*slot.get()], group),
        }
    }

    fn into_entries(self) -> impl Iterator<Item = (PipelineMapKey, PipelineGroup)> {
        let mut groups = self.groups;
        self.index.into_iter().map(move |(key, index)| {
            // Each unique map entry owns exactly one group. Taking it leaves
            // an empty Vec, without allocating or changing map iteration order.
            (key, std::mem::take(&mut groups[index]))
        })
    }

    fn into_values(self) -> PipelineGroups {
        PipelineGroups {
            index: self.index.into_iter(),
            groups: self.groups,
        }
    }
}

/// Retains map iteration and group ownership across output chunk handoffs.
struct PipelineGroups {
    index: hashbrown::hash_map::IntoIter<PipelineMapKey, usize>,
    groups: Vec<PipelineGroup>,
}

impl Iterator for PipelineGroups {
    type Item = PipelineGroup;

    fn next(&mut self) -> Option<Self::Item> {
        self.index
            .next()
            .map(|(_, index)| std::mem::take(&mut self.groups[index]))
    }
}

/// One group inside a worker's map: its aggregate partial states.
#[derive(Default)]
struct PipelineGroup {
    states: Vec<AggState>,
}

impl PipelineGroup {
    /// Creates the group; the CALLER batches the tracker consume (one
    /// round-trip per chunk, not per group — 1.5M-group shapes showed the
    /// lock in profiles).
    fn new(
        funcs: &[AggFunc],
        collations: &[tidb_datatype::Collation],
        key_len: usize,
    ) -> (Self, i64) {
        let bytes = parallel_new_group_bytes(key_len, funcs);
        (
            PipelineGroup {
                states: funcs
                    .iter()
                    .zip(collations)
                    .map(|(func, collation)| AggState::new_parallel_with(func, *collation))
                    .collect(),
            },
            bytes,
        )
    }
}

const SPILLED_PARTITION_NUM: usize = 256;
const SPILL_CHUNK_SIZE: usize = 1024;
const SPILL_FORMAT_VERSION: u8 = 2;

struct SpillWriter(Vec<u8>);

impl SpillWriter {
    fn new() -> Self {
        Self(vec![SPILL_FORMAT_VERSION])
    }

    fn u8(&mut self, value: u8) {
        self.0.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn i128(&mut self, value: i128) {
        self.0.extend_from_slice(&value.to_le_bytes());
    }

    fn f64(&mut self, value: f64) {
        self.u64(value.to_bits());
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), ExecError> {
        let len = u32::try_from(value.len())
            .map_err(|_| ExecError::SpillFailed("HashAgg spill value is too large".to_owned()))?;
        self.u32(len);
        self.0.extend_from_slice(value);
        Ok(())
    }

    fn datum(&mut self, value: &Datum) -> Result<(), ExecError> {
        let encoded = value
            .marshal_json()
            .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
        self.bytes(&encoded)
    }

    fn optional_datum(&mut self, value: Option<&Datum>) -> Result<(), ExecError> {
        self.u8(u8::from(value.is_some()));
        if let Some(value) = value {
            self.datum(value)?;
        }
        Ok(())
    }
}

struct SpillReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SpillReader<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, ExecError> {
        if bytes.first().copied() != Some(SPILL_FORMAT_VERSION) {
            return Err(ExecError::SpillFailed(
                "invalid HashAgg spill format version".to_owned(),
            ));
        }
        Ok(Self { bytes, offset: 1 })
    }

    fn fixed<const N: usize>(&mut self) -> Result<[u8; N], ExecError> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| ExecError::SpillFailed("invalid HashAgg spill length".to_owned()))?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| ExecError::SpillFailed("truncated HashAgg spill record".to_owned()))?;
        self.offset = end;
        Ok(slice.try_into().expect("fixed slice length"))
    }

    fn u8(&mut self) -> Result<u8, ExecError> {
        Ok(self.fixed::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, ExecError> {
        Ok(u32::from_le_bytes(self.fixed()?))
    }

    fn u64(&mut self) -> Result<u64, ExecError> {
        Ok(u64::from_le_bytes(self.fixed()?))
    }

    fn i64(&mut self) -> Result<i64, ExecError> {
        Ok(i64::from_le_bytes(self.fixed()?))
    }

    fn i128(&mut self) -> Result<i128, ExecError> {
        Ok(i128::from_le_bytes(self.fixed()?))
    }

    fn f64(&mut self) -> Result<f64, ExecError> {
        Ok(f64::from_bits(self.u64()?))
    }

    fn bytes(&mut self) -> Result<&'a [u8], ExecError> {
        let len = usize::try_from(self.u32()?).expect("u32 always fits usize");
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| ExecError::SpillFailed("invalid HashAgg spill length".to_owned()))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| ExecError::SpillFailed("truncated HashAgg spill record".to_owned()))?;
        self.offset = end;
        Ok(value)
    }

    fn datum(&mut self) -> Result<Datum, ExecError> {
        Datum::unmarshal_json(self.bytes()?)
            .map_err(|error| ExecError::SpillFailed(error.to_string()))
    }

    fn optional_datum(&mut self) -> Result<Option<Datum>, ExecError> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.datum().map(Some),
            _ => Err(ExecError::SpillFailed(
                "invalid HashAgg optional datum flag".to_owned(),
            )),
        }
    }

    fn finish(self) -> Result<(), ExecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ExecError::SpillFailed(
                "trailing bytes in HashAgg spill record".to_owned(),
            ))
        }
    }
}

fn write_partial(writer: &mut SpillWriter, partial: &Partial) -> Result<(), ExecError> {
    match partial {
        Partial::Count(value) => {
            writer.u8(0);
            writer.i64(*value);
        }
        Partial::FinalCount(value) => {
            writer.u8(1);
            writer.i64(*value);
        }
        Partial::SumDecimal(value) => {
            writer.u8(2);
            let datum = value.as_ref().map(|value| Datum::Decimal(value.clone()));
            writer.optional_datum(datum.as_ref())?;
        }
        Partial::SumDecimalFast { sum, scale } => {
            writer.u8(3);
            writer.i128(*sum);
            writer.u32(*scale);
        }
        Partial::SumReal(value) => {
            writer.u8(4);
            writer.u8(u8::from(value.is_some()));
            if let Some(value) = value {
                writer.f64(*value);
            }
        }
        Partial::FirstRow(value) => {
            writer.u8(5);
            writer.optional_datum(value.as_ref())?;
        }
        Partial::MaxMin { value, .. } => {
            writer.u8(6);
            writer.optional_datum(value.as_ref())?;
        }
        Partial::MaxMinDecimalFast { value, scale, .. } => {
            writer.u8(18);
            writer.i128(*value);
            writer.u32(*scale);
        }
        Partial::CountDistinctInt(set) => {
            writer.u8(19);
            writer.u32(u32::try_from(set.len()).map_err(|_| {
                ExecError::SpillFailed("too many HashAgg DISTINCT values".to_owned())
            })?);
            for value in set.iter() {
                writer.i64(*value);
            }
        }
        Partial::MaxMinCount { value, count, .. } => {
            writer.u8(17);
            writer.optional_datum(value.as_ref())?;
            writer.i64(*count);
        }
        Partial::AvgDecimal { sum, count } => {
            writer.u8(7);
            writer.datum(&Datum::Decimal(sum.clone()))?;
            writer.i64(*count);
        }
        Partial::AvgDecimalFast { sum, scale, count } => {
            writer.u8(8);
            writer.i128(*sum);
            writer.u32(*scale);
            writer.i64(*count);
        }
        Partial::AvgReal { sum, count } => {
            writer.u8(9);
            writer.f64(*sum);
            writer.i64(*count);
        }
        Partial::GroupConcat { values, .. } => {
            writer.u8(10);
            writer.u32(u32::try_from(values.len()).map_err(|_| {
                ExecError::SpillFailed("too many GROUP_CONCAT spill values".to_owned())
            })?);
            for (value, sort_key) in values {
                writer.bytes(value)?;
                writer.u32(u32::try_from(sort_key.len()).map_err(|_| {
                    ExecError::SpillFailed("too many GROUP_CONCAT sort values".to_owned())
                })?);
                for datum in sort_key {
                    writer.datum(datum)?;
                }
            }
        }
        Partial::Bit { acc, .. } => {
            writer.u8(11);
            writer.u64(*acc);
        }
        Partial::Variance {
            count,
            sum,
            variance,
            ..
        } => {
            writer.u8(12);
            writer.i64(*count);
            writer.f64(*sum);
            writer.f64(*variance);
        }
        Partial::JsonArrayAgg(values, _) => {
            writer.u8(13);
            writer.u32(u32::try_from(values.len()).map_err(|_| {
                ExecError::SpillFailed("too many JSON_ARRAYAGG spill values".to_owned())
            })?);
            for value in values {
                writer.datum(&Datum::Json(value.clone()))?;
            }
        }
        Partial::JsonObjectAgg(values, _, _) => {
            writer.u8(14);
            writer.u32(u32::try_from(values.len()).map_err(|_| {
                ExecError::SpillFailed("too many JSON_OBJECTAGG spill values".to_owned())
            })?);
            for (key, value) in values {
                writer.bytes(key.as_bytes())?;
                writer.datum(&Datum::Json(value.clone()))?;
            }
        }
        Partial::ApproxCountDistinct(sketch) => {
            writer.u8(15);
            let (skip_degree, has_zero, hashes) = sketch.spill_state();
            writer.u8(skip_degree);
            writer.u8(u8::from(has_zero));
            writer.u32(u32::try_from(hashes.len()).map_err(|_| {
                ExecError::SpillFailed("too many approximate-count spill hashes".to_owned())
            })?);
            for hash in hashes {
                writer.u32(hash);
            }
        }
        Partial::ApproxPercentile { values, .. } => {
            writer.u8(16);
            writer.u32(u32::try_from(values.len()).map_err(|_| {
                ExecError::SpillFailed("too many approximate-percentile values".to_owned())
            })?);
            for value in values {
                writer.datum(value)?;
            }
        }
    }
    Ok(())
}

fn expect_decimal(value: Datum) -> Result<Decimal, ExecError> {
    match value {
        Datum::Decimal(value) => Ok(value),
        _ => Err(ExecError::SpillFailed(
            "invalid decimal HashAgg spill state".to_owned(),
        )),
    }
}

fn expect_json(value: Datum) -> Result<BinaryJSON, ExecError> {
    match value {
        Datum::Json(value) => Ok(value),
        _ => Err(ExecError::SpillFailed(
            "invalid JSON HashAgg spill state".to_owned(),
        )),
    }
}

fn read_partial(reader: &mut SpillReader<'_>, func: &AggFunc) -> Result<Partial, ExecError> {
    let tag = reader.u8()?;
    let invalid = || ExecError::SpillFailed("aggregate kind mismatch in spill state".to_owned());
    Ok(match (&func.kind, tag) {
        (AggKind::Count, 0) => Partial::Count(reader.i64()?),
        (AggKind::FinalCount, 1) => Partial::FinalCount(reader.i64()?),
        (AggKind::Sum, 2) => {
            Partial::SumDecimal(reader.optional_datum()?.map(expect_decimal).transpose()?)
        }
        (AggKind::Sum, 3) => Partial::SumDecimalFast {
            sum: reader.i128()?,
            scale: reader.u32()?,
        },
        (AggKind::Sum, 4) => {
            let value = match reader.u8()? {
                0 => None,
                1 => Some(reader.f64()?),
                _ => return Err(invalid()),
            };
            Partial::SumReal(value)
        }
        (AggKind::FirstRow, 5) => Partial::FirstRow(reader.optional_datum()?),
        (AggKind::Min, 6) => Partial::MaxMin {
            value: reader.optional_datum()?,
            is_max: false,
        },
        (AggKind::Max, 6) => Partial::MaxMin {
            value: reader.optional_datum()?,
            is_max: true,
        },
        (AggKind::Count, 19) => {
            let count = reader.u32()? as usize;
            let mut set = Int64SetWithMemoryUsage::new(std::iter::empty::<i64>()).0;
            for _ in 0..count {
                set.insert(reader.i64()?);
            }
            Partial::CountDistinctInt(set)
        }
        (AggKind::Min | AggKind::Max, 18) => Partial::MaxMinDecimalFast {
            value: reader.i128()?,
            scale: reader.u32()?,
            is_max: matches!(func.kind, AggKind::Max),
        },
        (AggKind::MinCount, 17) => Partial::MaxMinCount {
            value: reader.optional_datum()?,
            count: reader.i64()?,
            is_max: false,
        },
        (AggKind::MaxCount, 17) => Partial::MaxMinCount {
            value: reader.optional_datum()?,
            count: reader.i64()?,
            is_max: true,
        },
        (AggKind::Avg, 7) => Partial::AvgDecimal {
            sum: expect_decimal(reader.datum()?)?,
            count: reader.i64()?,
        },
        (AggKind::Avg, 8) => Partial::AvgDecimalFast {
            sum: reader.i128()?,
            scale: reader.u32()?,
            count: reader.i64()?,
        },
        (AggKind::Avg, 9) => Partial::AvgReal {
            sum: reader.f64()?,
            count: reader.i64()?,
        },
        (AggKind::GroupConcat { separator }, 10) => {
            let mut values = Vec::with_capacity(reader.u32()? as usize);
            for _ in 0..values.capacity() {
                let value = reader.bytes()?.to_vec();
                let mut sort_key = Vec::with_capacity(reader.u32()? as usize);
                for _ in 0..sort_key.capacity() {
                    sort_key.push(reader.datum()?);
                }
                values.push((value, sort_key));
            }
            Partial::GroupConcat {
                values,
                separator: separator.clone(),
            }
        }
        (AggKind::Bit(op), 11) => Partial::Bit {
            acc: reader.u64()?,
            op: *op,
        },
        (AggKind::Variance { sample, sqrt }, 12) => Partial::Variance {
            count: reader.i64()?,
            sum: reader.f64()?,
            variance: reader.f64()?,
            sample: *sample,
            sqrt: *sqrt,
        },
        (AggKind::JsonArrayAgg { value_type }, 13) => {
            let mut values = Vec::with_capacity(reader.u32()? as usize);
            for _ in 0..values.capacity() {
                values.push(expect_json(reader.datum()?)?);
            }
            Partial::JsonArrayAgg(values, Box::new(value_type.clone()))
        }
        (
            AggKind::JsonObjectAgg {
                value_type,
                key_is_binary,
            },
            14,
        ) => {
            let count = reader.u32()?;
            let mut values = BTreeMap::new();
            for _ in 0..count {
                let key = String::from_utf8(reader.bytes()?.to_vec()).map_err(|_| {
                    ExecError::SpillFailed("invalid JSON_OBJECTAGG spill key".to_owned())
                })?;
                values.insert(key, expect_json(reader.datum()?)?);
            }
            Partial::JsonObjectAgg(values, Box::new(value_type.clone()), *key_is_binary)
        }
        (AggKind::ApproxCountDistinct, 15) => {
            let skip_degree = reader.u8()?;
            let has_zero = match reader.u8()? {
                0 => false,
                1 => true,
                _ => return Err(invalid()),
            };
            let mut hashes = Vec::with_capacity(reader.u32()? as usize);
            for _ in 0..hashes.capacity() {
                hashes.push(reader.u32()?);
            }
            Partial::ApproxCountDistinct(
                ApproxCountDistinctSketch::from_spill_state(skip_degree, has_zero, &hashes)
                    .map_err(ExecError::SpillFailed)?,
            )
        }
        (AggKind::ApproxPercentile(percent), 16) => {
            let mut values = Vec::with_capacity(reader.u32()? as usize);
            for _ in 0..values.capacity() {
                values.push(reader.datum()?);
            }
            Partial::ApproxPercentile {
                values,
                percent: *percent,
            }
        }
        _ => return Err(invalid()),
    })
}

fn write_datum_vec(writer: &mut SpillWriter, values: &[Datum]) -> Result<(), ExecError> {
    writer.u32(
        u32::try_from(values.len())
            .map_err(|_| ExecError::SpillFailed("too many HashAgg spill datums".to_owned()))?,
    );
    for value in values {
        writer.datum(value)?;
    }
    Ok(())
}

fn read_datum_vec(reader: &mut SpillReader<'_>) -> Result<Vec<Datum>, ExecError> {
    let count = reader.u32()? as usize;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(reader.datum()?);
    }
    Ok(values)
}

/// Serializes one original DISTINCT input. Go's distinct partial result
/// stores the value set itself rather than only the folded scalar, because a
/// final worker must deduplicate values that appeared in multiple partial
/// workers before applying COUNT/SUM/AVG/etc.
fn write_distinct_input(writer: &mut SpillWriter, input: &DistinctInput) -> Result<(), ExecError> {
    writer.bytes(&input.key)?;
    writer.optional_datum(input.value.as_ref())?;
    write_datum_vec(writer, &input.extra)?;
    write_datum_vec(writer, &input.sort_key)?;
    Ok(())
}

fn read_distinct_input(reader: &mut SpillReader<'_>) -> Result<DistinctInput, ExecError> {
    Ok(DistinctInput {
        key: reader.bytes()?.to_vec(),
        value: reader.optional_datum()?,
        extra: read_datum_vec(reader)?,
        sort_key: read_datum_vec(reader)?,
    })
}

fn write_state(
    writer: &mut SpillWriter,
    state: &AggState,
    func: &AggFunc,
) -> Result<(), ExecError> {
    if func.distinct && !super::count_distinct_int(func) {
        let inputs = state.distinct_inputs.as_ref().ok_or_else(|| {
            ExecError::SpillFailed(
                "parallel DISTINCT state did not retain its partial inputs".to_owned(),
            )
        })?;
        writer.u32(u32::try_from(inputs.len()).map_err(|_| {
            ExecError::SpillFailed("too many HashAgg DISTINCT spill inputs".to_owned())
        })?);
        for input in inputs {
            write_distinct_input(writer, input)?;
        }
    }
    write_partial(writer, &state.partial)
}

fn read_state(reader: &mut SpillReader<'_>, func: &AggFunc) -> Result<AggState, ExecError> {
    let mut state = AggState::new_parallel(func);
    if func.distinct && !super::count_distinct_int(func) {
        let count = reader.u32()? as usize;
        let mut inputs = Vec::with_capacity(count);
        let mut seen = StringSetWithMemoryUsage::new([]).0;
        for _ in 0..count {
            let input = read_distinct_input(reader)?;
            seen.insert(GoString::from_bytes(input.key.clone()));
            inputs.push(input);
        }
        state.seen = Some(Box::new(seen));
        state.distinct_inputs = Some(inputs);
    }
    state.partial = read_partial(reader, func)?;
    Ok(state)
}

fn encode_spill_entry(
    key: &PipelineMapKey,
    group: &PipelineGroup,
    funcs: &[AggFunc],
) -> Result<Vec<u8>, ExecError> {
    let mut writer = SpillWriter::new();
    match key {
        PipelineMapKey::Int(value) => {
            writer.u8(0);
            writer.u8(u8::from(value.is_some()));
            if let Some(value) = value {
                writer.i64(*value);
            }
        }
        PipelineMapKey::Bytes(value) => {
            writer.u8(1);
            writer.bytes(value)?;
        }
    }
    writer.u32(
        u32::try_from(group.states.len())
            .map_err(|_| ExecError::SpillFailed("too many HashAgg spill states".to_owned()))?,
    );
    for (state, func) in group.states.iter().zip(funcs) {
        write_state(&mut writer, state, func)?;
    }
    Ok(writer.0)
}

fn decode_spill_entry(
    bytes: &[u8],
    funcs: &[AggFunc],
) -> Result<(PipelineMapKey, PipelineGroup), ExecError> {
    let mut reader = SpillReader::new(bytes)?;
    let key = match reader.u8()? {
        0 => match reader.u8()? {
            0 => PipelineMapKey::Int(None),
            1 => PipelineMapKey::Int(Some(reader.i64()?)),
            _ => {
                return Err(ExecError::SpillFailed(
                    "invalid integer HashAgg spill key".to_owned(),
                ));
            }
        },
        1 => PipelineMapKey::Bytes(reader.bytes()?.to_vec()),
        _ => {
            return Err(ExecError::SpillFailed(
                "invalid HashAgg spill key kind".to_owned(),
            ));
        }
    };
    if reader.u32()? as usize != funcs.len() {
        return Err(ExecError::SpillFailed(
            "aggregate count mismatch in spill state".to_owned(),
        ));
    }
    let mut states = Vec::with_capacity(funcs.len());
    for func in funcs {
        states.push(read_state(&mut reader, func)?);
    }
    reader.finish()?;
    Ok((key, PipelineGroup { states }))
}

type SpillFile = Arc<Mutex<DataInDiskByChunks>>;

pub(super) struct ParallelSpillPartitions {
    field_types: Vec<FieldType>,
    chunks: Vec<Chunk>,
    files: Vec<Option<SpillFile>>,
    storage: Arc<tidb_util::spill_storage::SpillStorage>,
    disk_tracker: Arc<disk::Tracker>,
    has_data: bool,
}

impl ParallelSpillPartitions {
    fn new(memory: &StatementMemory, disk_tracker: &Arc<disk::Tracker>) -> Self {
        Self {
            field_types: vec![FieldType::new(FieldTypeCode::LongBlob)],
            chunks: Vec::new(),
            files: Vec::new(),
            storage: memory.spill_storage(),
            disk_tracker: Arc::clone(disk_tracker),
            has_data: false,
        }
    }

    /// Go `HashAggPartialWorker.prepareForSpill`: allocate the 256 temporary
    /// chunks only after the memory action actually requests a spill.
    fn prepare(&mut self) {
        if !self.chunks.is_empty() {
            return;
        }
        self.chunks = (0..SPILLED_PARTITION_NUM)
            .map(|_| Chunk::new_with_capacity(&self.field_types, SPILL_CHUNK_SIZE))
            .collect();
        self.files = (0..SPILLED_PARTITION_NUM).map(|_| None).collect();
    }

    fn bucket(key: &PipelineMapKey) -> usize {
        map_key_bucket(key.as_ref(), SPILLED_PARTITION_NUM)
    }

    fn flush(&mut self, partition: usize) -> Result<(), ExecError> {
        if self.chunks[partition].num_rows() == 0 {
            return Ok(());
        }
        let file = self.files[partition].get_or_insert_with(|| {
            let file = DataInDiskByChunks::new(
                self.field_types.clone(),
                "hashagg-parallel-",
                Arc::clone(&self.storage),
            );
            file.disk_tracker().attach_to(&self.disk_tracker);
            Arc::new(Mutex::new(file))
        });
        file.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .add(&self.chunks[partition])
            .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
        self.chunks[partition].reset();
        self.has_data = true;
        Ok(())
    }

    fn spill_maps(
        &mut self,
        maps: impl IntoIterator<Item = PipelineMap>,
        funcs: &[AggFunc],
    ) -> Result<(), ExecError> {
        self.prepare();
        for map in maps {
            for (key, group) in map.into_entries() {
                let partition = Self::bucket(&key);
                let encoded = encode_spill_entry(&key, &group, funcs)?;
                self.chunks[partition].append_bytes(0, &encoded);
                if self.chunks[partition].num_rows() >= SPILL_CHUNK_SIZE {
                    self.flush(partition)?;
                }
            }
        }
        for partition in 0..SPILLED_PARTITION_NUM {
            self.flush(partition)?;
        }
        Ok(())
    }
}

/// Go `murmur3.Sum32(key) % finalConcurrency`.
fn key_bucket(key: &[u8], bucket_count: usize) -> usize {
    crate::shuffle::murmur3_sum32(key) as usize % bucket_count
}

fn map_key_bucket(key: PipelineMapKeyRef<'_>, bucket_count: usize) -> usize {
    match key {
        PipelineMapKeyRef::Int(value) => {
            // Go hashes the group key bytes it already holds; the integer
            // lane re-encodes them on the stack (a NULL flag, or the varint
            // flag and at most ten varint bytes) rather than in a heap
            // vector per row.
            let mut encoded = [0u8; 11];
            let len = match value {
                Some(value) => {
                    encoded[0] = VARINT_FLAG;
                    1 + encode_varint_into(&mut encoded[1..], value)
                }
                None => {
                    encoded[0] = NIL_FLAG;
                    1
                }
            };
            key_bucket(&encoded[..len], bucket_count)
        }
        PipelineMapKeyRef::Bytes(bytes) => key_bucket(bytes, bucket_count),
    }
}

/// `tidb_codec::encode_varint` (Go `binary.PutVarint`) into a stack buffer;
/// returns the bytes written (at most ten).
fn encode_varint_into(buffer: &mut [u8], value: i64) -> usize {
    let mut unsigned = (value as u64) << 1;
    if value < 0 {
        unsigned = !unsigned;
    }
    let mut written = 0;
    while unsigned >= 0x80 {
        buffer[written] = (unsigned as u8) | 0x80;
        unsigned >>= 7;
        written += 1;
    }
    buffer[written] = unsigned as u8;
    written + 1
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

fn resolved_bool<C: Columns>(ctx: &C, name: &str, default: bool) -> bool {
    let read = |scope| {
        ctx.sysvar(scope, name).and_then(|value| match value {
            Datum::Int(value) => Some(value != 0),
            Datum::UInt(value) => Some(value != 0),
            Datum::Bytes(raw) => {
                let value = String::from_utf8_lossy(&raw);
                let value = value.trim();
                if value.eq_ignore_ascii_case("ON") || value == "1" {
                    Some(true)
                } else if value.eq_ignore_ascii_case("OFF") || value == "0" {
                    Some(false)
                } else {
                    None
                }
            }
            _ => None,
        })
    };
    read(None)
        .or_else(|| read(Some(tidb_ast::SysVarScope::Global)))
        .unwrap_or(default)
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

    /// Reusable input/work chunks allocated during the last Open.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn pipeline_input_chunks(&self) -> Option<usize> {
        self.pipeline_stats
            .as_ref()
            .map(|stats| stats.input_chunks.load(std::sync::atomic::Ordering::SeqCst))
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
        #[cfg(test)]
        if let Some((partial, final_)) = self.pipeline_concurrency_override {
            return (partial, final_);
        }
        if let Some(concurrency) = self.ctx.hashagg_concurrency() {
            return concurrency;
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
        // The Datum-flattened output buffer cannot carry zero-width virtual
        // rows; GROUP BY without aggregates stays serial.
        if self.agg_funcs.is_empty() {
            return None;
        }
        // A cop partial aggregation emits its group-by columns after the
        // aggregate columns; the pipeline stages only the aggregate values, so
        // such an aggregation stays serial.
        if self.output_group_keys {
            return None;
        }
        // Go `builder.go:2162`: only an aggregate-local ORDER BY forces
        // `IsUnparallelExec`. `HasDistinct` is recorded independently for
        // spill support and does not disable the partial/final workers.
        for func in &self.agg_funcs {
            if !func.order_by.is_empty() {
                return None;
            }
        }
        // Go `builder.go:2062`: both concurrencies at 1 (or non-positive)
        // means "run serially".
        let (partial, final_concurrency) = self.resolved_pipeline_concurrency();
        if partial == 0 || final_concurrency == 0 || (partial == 1 && final_concurrency == 1) {
            return None;
        }
        Some((partial, final_concurrency))
    }

    /// Go `initForParallelExec`'s complete spill gate.
    pub(super) fn parallel_spill_enabled(&self) -> bool {
        self.memory.tmp_storage_on_oom()
            && resolved_bool(
                &self.ctx,
                TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
                tidb_vardef::defaults::DEF_TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
            )
            && resolved_bool(
                &self.ctx,
                TIDB_ENABLE_PARALLEL_HASHAGG_SPILL,
                tidb_vardef::defaults::DEF_TIDB_ENABLE_PARALLEL_HASHAGG_SPILL,
            )
    }
}

struct PipelineEpoch {
    maps: Vec<Vec<PipelineMap>>,
    retained_bytes: i64,
    child_drained: bool,
}

/// Go HashAggInput: a reusable chunk bound to one partial worker.
/// Its allocation stays charged while queued, filled, or retained by a worker.
struct PipelineInput {
    lane: usize,
    chunk: Chunk,
    charged: i64,
    tracker: Arc<Tracker>,
    input_bytes: Arc<AtomicI64>,
}

impl PipelineInput {
    fn new(
        lane: usize,
        chunk: Chunk,
        tracker: &Arc<Tracker>,
        input_bytes: &Arc<AtomicI64>,
    ) -> Self {
        let charged = chunk.memory_usage();
        tracker.consume(charged);
        input_bytes.fetch_add(charged, std::sync::atomic::Ordering::Relaxed);
        Self {
            lane,
            chunk,
            charged,
            tracker: Arc::clone(tracker),
            input_bytes: Arc::clone(input_bytes),
        }
    }

    fn account(&mut self) {
        let bytes = self.chunk.memory_usage();
        let delta = bytes - self.charged;
        if delta != 0 {
            self.tracker.consume(delta);
            self.input_bytes
                .fetch_add(delta, std::sync::atomic::Ordering::Relaxed);
        }
        self.charged = bytes;
    }

    fn swap_columns(&mut self, other: &mut Self) {
        self.chunk.swap_columns(&mut other.chunk);
        // MemoryUsage counts the column allocations that just moved.
        std::mem::swap(&mut self.charged, &mut other.charged);
    }
}

impl Drop for PipelineInput {
    fn drop(&mut self) {
        self.tracker.consume(-self.charged);
        self.input_bytes
            .fetch_sub(self.charged, std::sync::atomic::Ordering::Relaxed);
    }
}

struct PartialOutput {
    maps: Vec<PipelineMap>,
    key_bytes: i64,
}

enum PartialInput {
    Chunk(PipelineInput),
    // FIFO with input chunks: receiving every reply is Go's in-flight
    // chunk barrier, without terminating the workers for a spill.
    TakeMaps(std::sync::mpsc::SyncSender<Result<PartialOutput, ExecError>>),
}

struct PartialWorker<C: Columns + Send + Sync + Clone + 'static> {
    resources: std::sync::mpsc::SyncSender<PipelineInput>,
    current: PipelineInput,
    abort: PipelineAbort,
    plan: Arc<PipelinePlan<C>>,
    tracker: Arc<Tracker>,
    memory: StatementMemory,
    maps: Vec<PipelineMap>,
    keys: PipelineKeyBuffer,
    error: Option<ExecError>,
    #[cfg(test)]
    stats: Arc<PipelineStats>,
    #[cfg(test)]
    started: bool,
}

impl<C: Columns + Send + Sync + Clone + 'static> PartialWorker<C> {
    fn take_maps(&mut self) -> Result<PartialOutput, ExecError> {
        match self.error.take() {
            Some(error) => Err(error),
            None => Ok(PartialOutput {
                maps: self.maps.iter_mut().map(std::mem::take).collect(),
                key_bytes: self.keys.charged,
            }),
        }
    }

    fn process(&mut self, command: PartialInput) {
        #[cfg(test)]
        if !self.started {
            self.stats.record_partial_worker();
            self.started = true;
        }
        let mut input = match command {
            PartialInput::Chunk(input) => input,
            PartialInput::TakeMaps(reply) => {
                let _ = reply.send(self.take_maps());
                return;
            }
        };
        self.current.swap_columns(&mut input);
        // Go returns its previous work buffer before folding the new
        // chunk. Exactly one resource per lane exists, so this bounded
        // channel cannot fill beyond its configured worker count.
        let _ = self.resources.send(input);
        if self.error.is_none() && !self.abort.raised() {
            let fold = crate::sort_util::recover_worker_panic(|| {
                let final_concurrency = self.maps.len();
                fold_chunk(
                    FoldInputs {
                        ctx: &self.plan.ctx,
                        memory: &self.memory,
                        group_by: &self.plan.group_by,
                        integer_columns: self.plan.integer_columns.as_deref(),
                        agg_funcs: &self.plan.agg_funcs,
                        input_modes: &self.plan.input_modes,
                        collations: &self.plan.collations,
                    },
                    &mut self.maps,
                    final_concurrency,
                    &self.tracker,
                    &self.current.chunk,
                    &mut self.keys,
                )
            });
            if let Err(fold_error) = fold {
                self.error = Some(fold_error);
                self.abort.raise();
            }
        }
    }
}

struct PartialLaneState<C: Columns + Send + Sync + Clone + 'static> {
    // None means a runnable task owns the worker. Queue mutations never
    // hold a lock while folding a chunk or waiting for another worker.
    worker: Option<PartialWorker<C>>,
    pending: std::collections::VecDeque<PartialInput>,
    closed: bool,
    done: Option<std::sync::mpsc::SyncSender<()>>,
}

struct PartialLane<C: Columns + Send + Sync + Clone + 'static>(Arc<Mutex<PartialLaneState<C>>>);

/// Go's partial worker stays in its receive loop while work is available.
/// Keep a small run on one compute-pool dispatch before yielding so the Rust
/// queue does not pay one boxed closure and wakeup for every input chunk.
const PARTIAL_LANE_BATCH: usize = 8;

impl<C: Columns + Send + Sync + Clone + 'static> PartialLane<C> {
    fn send(&self, command: PartialInput) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed {
            return false;
        }
        // An idle lane has no in-flight chunks. Go's spill barrier permits
        // its maps to be detached immediately; no native task is needed.
        if let (Some(worker), PartialInput::TakeMaps(reply)) = (state.worker.as_mut(), &command) {
            let result = worker.take_maps();
            drop(state);
            let _ = reply.send(result);
            return true;
        }
        // The resource token bounds each lane to one queued input chunk.
        // A barrier can follow that chunk, but no unbounded input queue exists.
        state.pending.push_back(command);
        let worker = state.worker.take();
        drop(state);
        if let Some(worker) = worker {
            Self::schedule(Arc::clone(&self.0), worker);
        }
        true
    }

    fn schedule(queue: Arc<Mutex<PartialLaneState<C>>>, mut worker: PartialWorker<C>) {
        crate::worker_pool::enqueue_public(Box::new(move || {
            // Drain a bounded prefix outside the queue lock. New commands
            // can arrive while this batch folds; yielding after the prefix
            // keeps concurrent queries from being held behind one hot lane.
            let mut commands = Vec::with_capacity(PARTIAL_LANE_BATCH);
            {
                let mut state = queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                for _ in 0..PARTIAL_LANE_BATCH {
                    let Some(command) = state.pending.pop_front() else {
                        break;
                    };
                    commands.push(command);
                }
            }
            if commands.is_empty() {
                // A sender may have raced with the previous task's final
                // state check. Reschedule rather than dropping the worker.
                let mut state = queue
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.pending.is_empty() {
                    if state.closed {
                        let done = state.done.take();
                        drop(state);
                        drop(worker);
                        if let Some(done) = done {
                            let _ = done.send(());
                        }
                    } else {
                        state.worker = Some(worker);
                    }
                } else {
                    drop(state);
                    Self::schedule(queue, worker);
                }
                return;
            }
            for command in commands {
                if let Err(error) = crate::sort_util::recover_worker_panic(|| {
                    worker.process(command);
                    Ok(())
                }) {
                    worker.error = Some(error);
                    worker.abort.raise();
                }
            }
            let mut state = queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !state.pending.is_empty() {
                drop(state);
                // Yield between bounded batches so concurrent queries share
                // the pool, like Go's worker goroutine at a scheduler point.
                Self::schedule(queue, worker);
            } else if state.closed {
                let done = state.done.take();
                drop(state);
                drop(worker);
                if let Some(done) = done {
                    let _ = done.send(());
                }
            } else {
                state.worker = Some(worker);
            }
        }));
    }

    fn close(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        if let Some(worker) = state.worker.take() {
            let done = state.done.take();
            drop(state);
            drop(worker);
            if let Some(done) = done {
                let _ = done.send(());
            }
        }
    }
}

/// Query-owned partial workers, input resources and key scratch. Only their
/// aggregate maps are surrendered at a spill barrier, as in Go's spill().
struct PipelineWorkers<C: Columns + Send + Sync + Clone + 'static> {
    abort: PipelineAbort,
    resources: Option<std::sync::mpsc::Receiver<PipelineInput>>,
    lanes: Vec<PartialLane<C>>,
    handles: Vec<std::sync::mpsc::Receiver<()>>,
    input_bytes: Arc<AtomicI64>,
}

impl<C: Columns + Send + Sync + Clone + 'static> PipelineWorkers<C> {
    #[allow(clippy::too_many_arguments)]
    fn new(
        child: &dyn Executor,
        plan: &Arc<PipelinePlan<C>>,
        partial_concurrency: usize,
        final_concurrency: usize,
        #[cfg(test)] stats: &Arc<PipelineStats>,
        memory: &StatementMemory,
        tracker: &Arc<Tracker>,
    ) -> Self {
        let (resources_tx, resources_rx) = sync_channel(partial_concurrency);
        let mut workers = Self {
            abort: PipelineAbort::default(),
            resources: Some(resources_rx),
            lanes: Vec::with_capacity(partial_concurrency),
            handles: Vec::with_capacity(partial_concurrency),
            input_bytes: Arc::new(AtomicI64::new(0)),
        };
        for lane in 0..partial_concurrency {
            let new_input = || {
                PipelineInput::new(
                    lane,
                    Chunk::new(child.ret_field_types(), 0, child.max_chunk_size()),
                    tracker,
                    &workers.input_bytes,
                )
            };
            // Go initPartialWorkers owns one work chunk and one input resource
            // per lane. The resource identifies the lane to dispatch back to.
            resources_tx
                .send(new_input())
                .expect("resource receiver is live");
            let (done, handle) = sync_channel(1);
            let worker = PartialWorker {
                resources: resources_tx.clone(),
                current: new_input(),
                abort: workers.abort.clone(),
                plan: Arc::clone(plan),
                maps: (0..final_concurrency)
                    .map(|_| PipelineMap::default())
                    .collect(),
                tracker: Arc::clone(tracker),
                memory: memory.clone(),
                keys: PipelineKeyBuffer::new(Arc::clone(tracker)),
                error: None,
                #[cfg(test)]
                stats: Arc::clone(stats),
                #[cfg(test)]
                started: false,
            };
            workers
                .lanes
                .push(PartialLane(Arc::new(Mutex::new(PartialLaneState {
                    worker: Some(worker),
                    pending: std::collections::VecDeque::new(),
                    closed: false,
                    done: Some(done),
                }))));
            workers.handles.push(handle);
            #[cfg(test)]
            stats
                .input_chunks
                .fetch_add(2, std::sync::atomic::Ordering::SeqCst);
        }
        drop(resources_tx);
        workers
    }

    fn run_epoch(
        &mut self,
        child: &mut dyn Executor,
        child_returned_empty: &mut bool,
        #[cfg(test)] stats: &Arc<PipelineStats>,
        memory: &StatementMemory,
        spill_requested: &Arc<AtomicBool>,
    ) -> Result<PipelineEpoch, ExecError> {
        let mut fetch_error = None;
        let mut child_drained = false;
        loop {
            if self.abort.raised() {
                break;
            }
            if let Err(error) = memory.check() {
                fetch_error = Some(error);
                break;
            }
            let Ok(mut input) = self.resources.as_ref().expect("live workers").recv() else {
                break;
            };
            if self.abort.raised() {
                break;
            }
            let result = crate::sort_util::recover_worker_panic(|| child.next(&mut input.chunk));
            input.account();
            if let Err(error) = result {
                fetch_error = Some(error);
                break;
            }
            if let Err(error) = memory.check() {
                fetch_error = Some(error);
                break;
            }
            if input.chunk.num_rows() == 0 {
                child_drained = true;
                break;
            }
            *child_returned_empty = false;
            let lane = input.lane;
            if !self.lanes[lane].send(PartialInput::Chunk(input)) {
                break;
            }
            #[cfg(test)]
            stats
                .dispatched_chunks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Go checks spill after dispatch. Even if initial chunk accounting
            // requested a spill, every epoch makes input progress before draining.
            if spill_requested.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }

        // Every command follows the lane's pending chunks. Wait for all replies
        // before touching maps or resetting their memory charge; workers and
        // their reusable buffers remain alive across the barrier.
        let mut replies = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            let (tx, rx) = sync_channel(1);
            let _ = lane.send(PartialInput::TakeMaps(tx));
            replies.push(rx);
        }
        let mut partial_maps = Vec::with_capacity(replies.len());
        let mut key_bytes = 0;
        let mut first_error = fetch_error;
        for handle in replies {
            match handle.recv() {
                Ok(Ok(output)) => {
                    key_bytes += output.key_bytes;
                    partial_maps.push(output.maps);
                }
                Ok(Err(error)) => {
                    first_error.get_or_insert(error);
                }
                Err(_) => {
                    first_error.get_or_insert_with(|| {
                        ExecError::unsupported("parallel HashAgg partial worker terminated")
                    });
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        Ok(PipelineEpoch {
            maps: partial_maps,
            retained_bytes: key_bytes + self.input_bytes.load(std::sync::atomic::Ordering::Relaxed),
            child_drained,
        })
    }
}

impl<C: Columns + Send + Sync + Clone + 'static> Drop for PipelineWorkers<C> {
    fn drop(&mut self) {
        self.abort.raise();
        for lane in &self.lanes {
            lane.close();
        }
        // Disconnect return sends before joining, including unwind/error
        // paths with chunks still queued for a worker.
        self.resources.take();
        for handle in self.handles.drain(..) {
            let _ = handle.recv();
        }
    }
}

fn partition_partial_maps(
    partial_maps: Vec<Vec<PipelineMap>>,
    final_concurrency: usize,
) -> Vec<Vec<PipelineMap>> {
    let mut bucket_inputs: Vec<Vec<PipelineMap>> = (0..final_concurrency)
        .map(|_| Vec::with_capacity(partial_maps.len()))
        .collect();
    for maps in partial_maps {
        for (bucket, map) in maps.into_iter().enumerate() {
            bucket_inputs[bucket].push(map);
        }
    }
    bucket_inputs
}

impl<C: Columns + Send + Sync + Clone + 'static + HashAggContext> HashAggExec<C> {
    /// Go `prepare4ParallelExec` fused with `parallelExec`'s consumption:
    /// the main thread fetches into reusable worker-bound chunks and dispatches them
    /// to the partial-worker lanes; partial workers fold rows into their own
    /// final-bucket maps and return those maps after the partial-worker barrier;
    /// final workers stream reusable result chunks across subsequent Next
    /// calls, restoring separate spill partitions concurrently when needed.
    pub(super) fn execute_parallel_pipeline(&mut self) -> Result<(), ExecError> {
        #[cfg(test)]
        let stats = Arc::clone(
            self.pipeline_stats
                .as_ref()
                .expect("pipeline stats installed"),
        );
        let plan = Arc::new(PipelinePlan {
            ctx: self.ctx.clone(),
            group_by: self.group_by.clone(),
            integer_columns: self
                .group_by
                .iter()
                .map(|expr| {
                    let column = expr.as_column()?;
                    let index = usize::try_from(column.index).ok()?;
                    let field_type = column.get_static_type()?;
                    (field_type.eval_type() == EvalType::Int
                        && tidb_chunk::column::get_fixed_len(field_type) == 8
                        && !field_type.is_unsigned())
                    .then_some(index)
                })
                .collect(),
            agg_funcs: self.agg_funcs.clone(),
            input_modes: self.input_modes.clone(),
            collations: self.state_collations.clone(),
        });
        let spill_requested = Arc::clone(&self.parallel_spill_requested);
        let mut spilled = self
            .parallel_spill_action
            .as_ref()
            .map(|_| ParallelSpillPartitions::new(&self.memory, &self.disk_tracker));
        let mut in_memory_maps = None;
        let child_drained;
        let mut workers = PipelineWorkers::new(
            self.child.as_ref(),
            &plan,
            self.pipeline_partial_concurrency,
            self.pipeline_final_concurrency,
            #[cfg(test)]
            &stats,
            &self.memory,
            &self.tracker,
        );

        loop {
            let epoch = workers.run_epoch(
                self.child.as_mut(),
                &mut self.child_returned_empty,
                #[cfg(test)]
                &stats,
                &self.memory,
                &spill_requested,
            )?;
            let requested = spill_requested.swap(false, std::sync::atomic::Ordering::SeqCst);
            let has_spilled_data = spilled.as_ref().is_some_and(|spill| spill.has_data);
            if requested || has_spilled_data {
                let spilled = spilled.as_mut().ok_or_else(|| {
                    ExecError::unsupported("parallel HashAgg spill requested without spill action")
                })?;
                // Go spills worker-local partial states; final merging only
                // happens on restore, never just before serializing them.
                spilled.spill_maps(epoch.maps.into_iter().flatten(), &plan.agg_funcs)?;
                self.tracker.replace_bytes_used(epoch.retained_bytes);
                if epoch.child_drained {
                    child_drained = true;
                    break;
                }
                continue;
            }
            child_drained = epoch.child_drained;
            in_memory_maps = Some(epoch.maps);
            break;
        }
        drop(workers);

        // Go keeps the parallel spill files on disk until `Close` removes
        // them (`HashAggExec.dataInDisk`); dropping the helper here would
        // delete them before the caller can observe the round that spilled.
        self.parallel_spilled = spilled;
        let ret_types = self.meta.ret_field_types().to_vec();
        let max_chunk_size = self.meta.max_chunk_size();
        // Go `groupConcat.truncated`: one sentinel per function, shared by
        // every final worker, so two groups truncating on two workers still
        // warn once.
        let truncated: Arc<Vec<AtomicBool>> = Arc::new(
            self.truncated
                .iter()
                .map(|flag| AtomicBool::new(*flag))
                .collect(),
        );
        let sources = if let Some(spilled) = self.parallel_spilled.as_ref().filter(|s| s.has_data) {
            // Each final worker claims one partition at a time, as Go's
            // restoreOnePartition does. The executor retains every file's
            // ownership through Close; finishing a restore cannot delete it.
            let partitions = Arc::new(Mutex::new(
                spilled.files.iter().filter_map(Clone::clone).collect(),
            ));
            (0..self.pipeline_final_concurrency)
                .map(|_| FinalInput::Spilled(Arc::clone(&partitions)))
                .collect()
        } else {
            let mut maps = partition_partial_maps(
                in_memory_maps.unwrap_or_default(),
                self.pipeline_final_concurrency,
            );
            if self.child_returned_empty && self.emit_default_row && plan.group_by.is_empty() {
                // The empty global group goes through the same final-result
                // lifecycle as every other group; COUNT still emits zero.
                let mut default = PipelineMap::default();
                default.merge(
                    PipelineMapKey::Bytes(Vec::new()),
                    PipelineGroup {
                        states: plan.agg_funcs.iter().map(AggState::new).collect(),
                    },
                )?;
                maps[0].push(default);
            }
            maps.into_iter()
                // All partial inputs are complete here. Empty buckets have
                // no final results to evaluate, so they need no runnable task
                // or result holder. Keep the configured hash partitioning and
                // the default global group above; spilled inputs stay lazy.
                .filter(|maps| maps.iter().any(|map| !map.index.is_empty()))
                .map(|map| FinalInput::Memory(Some(map)))
                .collect()
        };
        self.parallel_output = Some(FinalOutput::start(
            sources,
            plan,
            ret_types,
            max_chunk_size,
            self.pipeline_partial_concurrency,
            truncated,
            self.memory.clone(),
            #[cfg(test)]
            stats,
        ));
        self.executed = true;
        if child_drained {
            self.is_child_drained = true;
        }
        Ok(())
    }
}

enum FinalInput {
    Memory(Option<Vec<PipelineMap>>),
    Spilled(Arc<Mutex<Vec<SpillFile>>>),
}

impl FinalInput {
    fn next_map(
        &mut self,
        funcs: &[AggFunc],
        memory: &StatementMemory,
        abort: &PipelineAbort,
    ) -> Result<Option<PipelineMap>, ExecError> {
        match self {
            Self::Memory(maps) => {
                let Some(maps) = maps.take() else {
                    return Ok(None);
                };
                // Go consumeIntermData and generateResultAndSend run on the
                // same final worker. No intermediate pool handoff is needed.
                let mut merged = PipelineMap::default();
                for map in maps {
                    if abort.raised() {
                        return Ok(None);
                    }
                    memory.check()?;
                    merge_map(&mut merged, map)?;
                }
                Ok(Some(merged))
            }
            Self::Spilled(partitions) => {
                let Some(file) = partitions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop()
                else {
                    return Ok(None);
                };
                // Only the queue claim is shared. Different workers read
                // different files concurrently, never under the queue lock.
                let mut file = file
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut restored = PipelineMap::default();
                for chunk_index in 0..file.num_chunks() {
                    if abort.raised() {
                        return Ok(None);
                    }
                    memory.check()?;
                    let chunk = file
                        .get_chunk(chunk_index)
                        .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
                    for row_index in 0..chunk.num_rows() {
                        let encoded = chunk.get_row(row_index).get_bytes(0);
                        let (key, group) = decode_spill_entry(&encoded, funcs)?;
                        restored.merge(key, group)?;
                    }
                }
                Ok(Some(restored))
            }
        }
    }
}

struct FinalChunk {
    chunk: Chunk,
    resume: Option<Box<dyn FnOnce(Chunk) + Send>>,
}

/// Go finalOutputCh with one reusable result holder per final worker.
pub(super) struct FinalOutput {
    results: Option<std::sync::mpsc::Receiver<Result<FinalChunk, ExecError>>>,
    handles: Vec<std::sync::mpsc::Receiver<()>>,
    current: Option<FinalChunk>,
    offset: usize,
    abort: PipelineAbort,
    truncated: Arc<Vec<AtomicBool>>,
}

impl FinalOutput {
    #[allow(clippy::too_many_arguments)]
    fn start<C: Columns + Send + Sync + Clone + 'static>(
        sources: Vec<FinalInput>,
        plan: Arc<PipelinePlan<C>>,
        ret_types: Vec<FieldType>,
        max_chunk_size: usize,
        partial_concurrency: usize,
        truncated: Arc<Vec<AtomicBool>>,
        memory: StatementMemory,
        #[cfg(test)] stats: Arc<PipelineStats>,
    ) -> Self {
        let (tx, rx) = sync_channel(partial_concurrency + sources.len() + 1);
        let mut output = Self {
            results: Some(rx),
            handles: Vec::with_capacity(sources.len()),
            current: None,
            offset: 0,
            abort: PipelineAbort::default(),
            truncated,
        };
        for source in sources {
            let (done, handle) = sync_channel(1);
            let worker = FinalWorker {
                source,
                groups: None,
                output: tx.clone(),
                plan: Arc::clone(&plan),
                ret_types: ret_types.clone(),
                memory: memory.clone(),
                abort: output.abort.clone(),
                truncated: Arc::clone(&output.truncated),
                #[cfg(test)]
                stats: Arc::clone(&stats),
                _completion: FinalWorkerCompletion {
                    done,
                    #[cfg(test)]
                    stats: Arc::clone(&stats),
                },
            };
            let chunk = Chunk::new(&ret_types, 0, max_chunk_size);
            output.handles.push(handle);
            worker.schedule(chunk);
            #[cfg(test)]
            stats
                .final_output_chunks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        // EOF is channel disconnection after every final worker finishes.
        drop(tx);
        output
    }

    pub(super) fn next(
        &mut self,
        req: &mut Chunk,
        memory: &StatementMemory,
    ) -> Result<bool, ExecError> {
        loop {
            memory.check()?;
            if self.current.is_none() {
                match self.results.as_ref().expect("live final output").recv() {
                    Ok(result) => self.current = Some(result?),
                    Err(_) => {
                        self.join_workers();
                        return Ok(req.num_rows() > 0);
                    }
                }
            }
            let current = self.current.as_mut().expect("received final chunk");
            let remaining = current.chunk.num_rows() - self.offset;
            let wanted = req.required_rows() - req.num_rows();
            if req.num_rows() == 0 && self.offset == 0 && remaining <= wanted {
                req.swap_columns(&mut current.chunk);
                self.return_current();
                return Ok(true);
            }
            // Keep the same holder until a smaller RequiredRows request has
            // consumed the whole chunk. Never hand its columns back early.
            let take = remaining.min(wanted);
            req.append_range_from(&current.chunk, self.offset, self.offset + take);
            self.offset += take;
            if self.offset == current.chunk.num_rows() {
                self.return_current();
            }
            if req.is_full() {
                return Ok(true);
            }
        }
    }

    fn return_current(&mut self) {
        let mut current = self.current.take().expect("current final chunk");
        current.chunk.reset();
        // Returning Go's result holder makes the worker runnable again.
        // No pool thread is occupied while the consumer owns the chunk.
        if let Some(resume) = current.resume.take() {
            resume(current.chunk);
        }
        self.offset = 0;
    }

    fn stop(&mut self) {
        self.abort.raise();
        self.results.take();
        self.current = None;
        self.join_workers();
    }

    fn join_workers(&mut self) {
        for handle in self.handles.drain(..) {
            let _ = handle.recv();
        }
    }

    pub(super) fn close(&mut self, truncated: &mut [bool]) {
        self.stop();
        for (flag, shared) in truncated.iter_mut().zip(self.truncated.iter()) {
            *flag |= shared.load(std::sync::atomic::Ordering::Acquire);
        }
    }
}

impl Drop for FinalOutput {
    fn drop(&mut self) {
        self.stop();
    }
}

struct FinalWorker<C: Columns + Send + Sync + Clone + 'static> {
    source: FinalInput,
    groups: Option<PipelineGroups>,
    output: std::sync::mpsc::SyncSender<Result<FinalChunk, ExecError>>,
    plan: Arc<PipelinePlan<C>>,
    ret_types: Vec<FieldType>,
    memory: StatementMemory,
    abort: PipelineAbort,
    truncated: Arc<Vec<AtomicBool>>,
    #[cfg(test)]
    stats: Arc<PipelineStats>,
    // Last field: release maps, input ownership and senders before Close's
    // completion barrier permits statement accounting to be reset.
    _completion: FinalWorkerCompletion,
}

struct FinalWorkerCompletion {
    done: std::sync::mpsc::SyncSender<()>,
    #[cfg(test)]
    stats: Arc<PipelineStats>,
}

impl Drop for FinalWorkerCompletion {
    fn drop(&mut self) {
        #[cfg(test)]
        self.stats
            .final_workers_finished
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _ = self.done.send(());
    }
}

impl<C: Columns + Send + Sync + Clone + 'static> FinalWorker<C> {
    fn schedule(mut self, mut chunk: Chunk) {
        crate::worker_pool::enqueue_public(Box::new(move || {
            let result = crate::sort_util::recover_worker_panic(|| self.fill(&mut chunk));
            match result {
                Err(error) => {
                    let _ = self.output.send(Err(error));
                }
                Ok(more) if !self.abort.raised() && chunk.num_rows() > 0 => {
                    #[cfg(test)]
                    self.stats
                        .finalized_chunks
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let output = self.output.clone();
                    let resume = if more {
                        Some(Box::new(move |chunk| self.schedule(chunk))
                            as Box<dyn FnOnce(Chunk) + Send>)
                    } else {
                        None
                    };
                    // One outstanding chunk per logical worker bounds this
                    // queue below its capacity; publication cannot park a CPU worker.
                    let _ = output.send(Ok(FinalChunk { chunk, resume }));
                }
                Ok(_) => {}
            }
        }));
    }

    fn fill(&mut self, current: &mut Chunk) -> Result<bool, ExecError> {
        self.memory.check()?;
        while !self.abort.raised() {
            let group = self.groups.as_mut().and_then(Iterator::next);
            if let Some(mut group) = group {
                for (column, state) in group.states.iter_mut().enumerate() {
                    append_finished_agg_value_claiming(
                        state,
                        &self.plan.agg_funcs[column],
                        &self.ret_types[column],
                        &self.plan.ctx,
                        &mut || {
                            self.truncated[column]
                                .compare_exchange(
                                    false,
                                    true,
                                    std::sync::atomic::Ordering::AcqRel,
                                    std::sync::atomic::Ordering::Acquire,
                                )
                                .is_ok()
                        },
                        true,
                        current,
                        column,
                    )?;
                }
                if self.plan.agg_funcs.is_empty() {
                    current.set_num_virtual_rows(current.num_rows() + 1);
                }
                if current.is_full() {
                    return Ok(true);
                }
            } else {
                self.groups = None;
                let Some(map) =
                    self.source
                        .next_map(&self.plan.agg_funcs, &self.memory, &self.abort)?
                else {
                    return Ok(false);
                };
                self.groups = Some(map.into_values());
            }
        }
        Ok(false)
    }
}

struct FoldInputs<'a, C> {
    ctx: &'a C,
    memory: &'a StatementMemory,
    group_by: &'a [Expression],
    integer_columns: Option<&'a [usize]>,
    agg_funcs: &'a [AggFunc],
    input_modes: &'a [AggInputMode],
    collations: &'a [tidb_datatype::Collation],
}

/// Go partial worker's groupKeyBuf: retain row buffers across input chunks.
/// Computed expressions finish a whole column before encoding; direct columns
/// can be read in place because encoding does not mutate the input chunk.
struct PipelineKeyBuffer {
    group_keys: GroupKeyBuffer,
    integers: Vec<(Option<i64>, usize)>,
    partial_results: Vec<(usize, usize)>,
    tracker: Arc<Tracker>,
    charged: i64,
}

impl PipelineKeyBuffer {
    fn new(tracker: Arc<Tracker>) -> Self {
        Self {
            group_keys: GroupKeyBuffer::default(),
            integers: Vec::new(),
            partial_results: Vec::new(),
            tracker,
            charged: 0,
        }
    }

    fn account(&mut self) {
        let bytes = (self.group_keys.memory_usage()
            + self.integers.capacity() * std::mem::size_of::<(Option<i64>, usize)>()
            + self.partial_results.capacity() * std::mem::size_of::<(usize, usize)>())
            as i64;
        self.tracker.consume(bytes - self.charged);
        self.charged = bytes;
    }

    fn prepare<C: Columns>(
        &mut self,
        inputs: &FoldInputs<'_, C>,
        chunk: &Chunk,
        bucket_count: usize,
    ) -> Result<(), ExecError> {
        let rows = chunk.num_rows();
        let result: Result<(), ExecError> = (|| {
            self.integers.clear();
            self.partial_results.clear();
            if let Some([index]) = inputs.integer_columns {
                let column = chunk.column(*index);
                column.with_raw(|raw| {
                    if let Some(selection) = chunk.sel() {
                        debug_assert_eq!(selection.len(), rows);
                        self.integers
                            .extend(selection.iter().take(rows).map(|&physical| {
                                let value = (!column.is_null(physical)).then(|| {
                                    i64::from_ne_bytes(
                                        raw.row(physical)
                                            .try_into()
                                            .expect("integer group key cell is 8 bytes"),
                                    )
                                });
                                (
                                    value,
                                    map_key_bucket(PipelineMapKeyRef::Int(value), bucket_count),
                                )
                            }));
                    } else {
                        self.integers.extend((0..rows).map(|physical| {
                            let value = (!column.is_null(physical)).then(|| {
                                i64::from_ne_bytes(
                                    raw.row(physical)
                                        .try_into()
                                        .expect("integer group key cell is 8 bytes"),
                                )
                            });
                            (
                                value,
                                map_key_bucket(PipelineMapKeyRef::Int(value), bucket_count),
                            )
                        }));
                    }
                });
                return Ok(());
            }
            self.group_keys
                .prepare(inputs.ctx, chunk, inputs.group_by, false)
        })();
        // Include retained capacity on both successful and failed evaluation;
        // Drop releases it when this partial worker ends.
        self.account();
        result?;
        inputs.memory.check()
    }

    fn key(&self, row: usize) -> PipelineMapKeyRef<'_> {
        if self.integers.is_empty() {
            PipelineMapKeyRef::Bytes(&self.group_keys.encoded[row])
        } else {
            PipelineMapKeyRef::Int(self.integers[row].0)
        }
    }
}

impl Drop for PipelineKeyBuffer {
    fn drop(&mut self) {
        self.tracker.consume(-self.charged);
    }
}

/// Go updatePartialResult: construct all keys, resolve all partial results,
/// then update aggregates in row order without probing the maps again.
fn fold_chunk<C: Columns>(
    inputs: FoldInputs<'_, C>,
    maps: &mut [PipelineMap],
    bucket_count: usize,
    tracker: &Arc<Tracker>,
    chunk: &Chunk,
    keys: &mut PipelineKeyBuffer,
) -> Result<(), ExecError> {
    keys.prepare(&inputs, chunk, bucket_count)?;
    let FoldInputs {
        ctx,
        memory,
        agg_funcs,
        input_modes,
        collations,
        ..
    } = inputs;
    let mut new_group_bytes_total = 0i64;
    let mut state_memory_delta = 0i64;
    let num_rows = chunk.num_rows();
    if keys.integers.is_empty() {
        for key_bytes in keys.group_keys.encoded.iter().take(num_rows) {
            let key = PipelineMapKeyRef::Bytes(key_bytes);
            let bucket = map_key_bucket(key, bucket_count);
            let (index, bytes) = maps[bucket].resolve(key, agg_funcs, collations);
            new_group_bytes_total += bytes;
            keys.partial_results.push((bucket, index));
        }
    } else {
        for &(value, bucket) in keys.integers.iter().take(num_rows) {
            let key = PipelineMapKeyRef::Int(value);
            let (index, bytes) = maps[bucket].resolve(key, agg_funcs, collations);
            new_group_bytes_total += bytes;
            keys.partial_results.push((bucket, index));
        }
    }
    if new_group_bytes_total > 0 {
        tracker.consume(new_group_bytes_total);
    }
    keys.account();
    memory.check()?;
    let inputs = input::bind_inputs(input_modes, chunk);
    let decimal_cache = input::prepare_decimal_cache(input_modes, chunk);
    let integer_cache = input::prepare_integer_cache(input_modes, chunk);
    if let Some(selection) = chunk.sel() {
        debug_assert_eq!(selection.len(), num_rows);
        for (row_index, &(bucket, index)) in keys.partial_results.iter().enumerate() {
            state_memory_delta += input::update_row_with_decimal_cache(
                &inputs,
                agg_funcs,
                ctx,
                &mut maps[bucket].groups[index].states,
                chunk.physical_row(selection[row_index]),
                &decimal_cache,
                &integer_cache,
            )?;
        }
    } else {
        for (row_index, &(bucket, index)) in keys.partial_results.iter().enumerate() {
            state_memory_delta += input::update_row_with_decimal_cache(
                &inputs,
                agg_funcs,
                ctx,
                &mut maps[bucket].groups[index].states,
                chunk.physical_row(row_index),
                &decimal_cache,
                &integer_cache,
            )?;
        }
    }
    // Go updatePartialResult accounts aggregate-state growth once per chunk.
    tracker.consume(state_memory_delta);
    // Go's Consume raises OOM on this worker. Rust records a kill signal;
    // return it here even when the fetcher has already observed EOF.
    memory.check()
}

/// Merges one shuffled sub-map into an accumulator (Go
/// `mergeInputIntoResultMap`: a fresh accumulator adopts the first map
/// as-is).
fn merge_map(global: &mut PipelineMap, incoming: PipelineMap) -> Result<(), ExecError> {
    if global.index.is_empty() {
        *global = incoming;
        return Ok(());
    }
    // Final workers merge complete partial maps. Reserve once for the incoming
    // batch so hashbrown does not repeatedly rehash while Go's map merge walks
    // the same batch of entries.
    global.index.reserve(incoming.index.len());
    global.groups.reserve(incoming.groups.len());
    for (key, group) in incoming.into_entries() {
        global.merge(key, group)?;
    }
    Ok(())
}

/// Merges two copies of one group in final-worker arrival order.
fn merge_groups(dst: &mut PipelineGroup, mut src: PipelineGroup) -> Result<(), ExecError> {
    // Go passes source and destination partial-result pointers. Borrow the
    // states in place; only values actually adopted by the destination move.
    for (c, state) in src.states.iter_mut().enumerate() {
        merge_state(&mut dst.states[c], state)?;
    }
    Ok(())
}

/// Go `MergePartialResult` for exactly the aggregate kinds eligibility lets
/// through: every arm folds EXACTLY (integer/decimal domain or order-free
/// comparison), so a merged result equals the serial accumulation bit for
/// bit. Any other pair is an eligibility-gate bug, not a value.
fn merge_state(dst: &mut AggState, src: &mut AggState) -> Result<(), ExecError> {
    // Go's distinct partial implementations merge their retained value sets;
    // adding worker-local COUNT/SUM/AVG scalars would double-count a value
    // present in two workers. Replay only keys newly admitted to `dst`.
    if dst.seen.is_some() || src.seen.is_some() {
        let Some(inputs) = src.distinct_inputs.take() else {
            return Err(ExecError::unsupported(
                "parallel DISTINCT state did not retain its partial inputs",
            ));
        };
        for input in inputs {
            dst.update(input.value, &input.extra, input.sort_key, Some(input.key))?;
        }
        return Ok(());
    }

    // Fast decimal representations are an execution detail. Materialize a
    // mismatched pair before dispatching so every exact combination has the
    // same merge rule as Go's decimal partial result.
    let sum_fast_matches = matches!(
        (&dst.partial, &src.partial),
        (
            Partial::SumDecimalFast { scale: a, .. },
            Partial::SumDecimalFast { scale: b, .. }
        ) if a == b
    );
    if !sum_fast_matches
        && (matches!(dst.partial, Partial::SumDecimalFast { .. })
            || matches!(src.partial, Partial::SumDecimalFast { .. }))
    {
        dst.partial.materialize_sum_fast();
        src.partial.materialize_sum_fast();
    }
    let max_min_fast_matches = matches!(
        (&dst.partial, &src.partial),
        (
            Partial::MaxMinDecimalFast { scale: a, .. },
            Partial::MaxMinDecimalFast { scale: b, .. }
        ) if a == b
    );
    if !max_min_fast_matches
        && (matches!(dst.partial, Partial::MaxMinDecimalFast { .. })
            || matches!(src.partial, Partial::MaxMinDecimalFast { .. }))
    {
        dst.partial.materialize_max_min_fast();
        src.partial.materialize_max_min_fast();
    }
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
    match (&mut dst.partial, &mut src.partial) {
        (Partial::Count(a), Partial::Count(b)) => *a = a.wrapping_add(*b),
        // Go `countPartialWithDistinct4Int.MergePartialResult`: the union of
        // the two value sets.
        (Partial::CountDistinctInt(dst_set), Partial::CountDistinctInt(src_set)) => {
            for value in src_set.iter() {
                dst_set.insert(*value);
            }
        }
        (Partial::FinalCount(a), Partial::FinalCount(b)) => *a = a.wrapping_add(*b),
        (Partial::SumDecimal(a), Partial::SumDecimal(b)) => {
            if let Some(sum) = b.take() {
                *a = Some(match a.take() {
                    Some(current) => current.add(&sum),
                    None => sum,
                });
            }
        }
        (Partial::SumReal(a), Partial::SumReal(b)) => {
            if let Some(value) = b {
                *a = Some(a.unwrap_or(0.0) + *value);
            }
        }
        (Partial::FirstRow(slot), Partial::FirstRow(value)) => {
            if slot.is_none() {
                *slot = value.take();
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
        ) => match (dst_value.as_mut(), src_value.take()) {
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
            Partial::MaxMinCount {
                value: dst_value,
                count: dst_count,
                is_max,
            },
            Partial::MaxMinCount {
                value: src_value,
                count: src_count,
                ..
            },
        ) => match (dst_value.as_mut(), src_value.take()) {
            (_, None) => {}
            (None, Some(value)) => {
                *dst_value = Some(value);
                *dst_count = *src_count;
            }
            (Some(current), Some(value)) => {
                let ordering =
                    tidb_expr::compare_datums_with_collation(&value, current, dst.collation)?;
                if (*is_max && ordering == Ordering::Greater)
                    || (!*is_max && ordering == Ordering::Less)
                {
                    *current = value;
                    *dst_count = *src_count;
                } else if ordering == Ordering::Equal {
                    *dst_count = dst_count.wrapping_add(*src_count);
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
            *dst_sum = dst_sum.add(src_sum);
            *dst_count = dst_count.wrapping_add(*src_count);
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
            *dst_sum = dst_sum.wrapping_add(*src_sum);
            *dst_count = dst_count.wrapping_add(*src_count);
        }
        (
            Partial::AvgReal {
                sum: dst_sum,
                count: dst_count,
            },
            Partial::AvgReal {
                sum: src_sum,
                count: src_count,
            },
        ) => {
            *dst_sum += *src_sum;
            *dst_count = dst_count.wrapping_add(*src_count);
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
        ) if dst_scale == src_scale => {
            *dst_sum = dst_sum.wrapping_add(*src_sum);
        }
        (state @ Partial::SumDecimalFast { .. }, Partial::SumDecimal(None)) => {
            // An empty partial contributes nothing to a Fast accumulator.
            let _ = state;
        }
        // Go `maxMin4Decimal.MergePartialResult`: compare the two extrema
        // and keep the better one.
        (
            Partial::MaxMinDecimalFast {
                value: dst_value,
                scale: dst_scale,
                is_max,
            },
            Partial::MaxMinDecimalFast {
                value: src_value,
                scale: src_scale,
                ..
            },
        ) if dst_scale == src_scale => {
            if (*is_max && *src_value > *dst_value) || (!*is_max && *src_value < *dst_value) {
                *dst_value = *src_value;
            }
        }
        // A Fast state adopting an empty partial, or vice versa: the empty
        // side contributes nothing.
        (Partial::SumDecimal(None), Partial::SumDecimalFast { .. }) => {
            // dst keeps its own accumulator; nothing to add.
        }
        // Mixed Fast/materialized states arise only after an overflow
        // replay materialized BOTH sides into SumDecimal(Some); they take
        // the exact merge arm below. A lone mismatch is unreachable.
        (Partial::Bit { acc: dst_acc, op }, Partial::Bit { acc: src_acc, .. }) => match op {
            BitOp::And => *dst_acc &= *src_acc,
            BitOp::Or => *dst_acc |= *src_acc,
            BitOp::Xor => *dst_acc ^= *src_acc,
        },
        (
            Partial::Variance {
                count: dst_count,
                sum: dst_sum,
                variance: dst_variance,
                ..
            },
            Partial::Variance {
                count: src_count,
                sum: src_sum,
                variance: src_variance,
                ..
            },
        ) => {
            if *src_count != 0 {
                if *dst_count == 0 {
                    *dst_count = *src_count;
                    *dst_sum = *src_sum;
                    *dst_variance = *src_variance;
                } else {
                    // Go `calculateMerge` (`func_varpop.go`).
                    let src_count_f = *src_count as f64;
                    let dst_count_f = *dst_count as f64;
                    let t = (src_count_f / dst_count_f) * *dst_sum - *src_sum;
                    *dst_variance += *src_variance
                        + ((dst_count_f / src_count_f) / (dst_count_f + src_count_f)) * t * t;
                    *dst_count = dst_count.wrapping_add(*src_count);
                    *dst_sum += *src_sum;
                }
            }
        }
        (
            Partial::GroupConcat {
                values: dst_values, ..
            },
            Partial::GroupConcat {
                values: src_values, ..
            },
        ) => dst_values.append(src_values),
        (Partial::JsonArrayAgg(dst_values, _), Partial::JsonArrayAgg(src_values, _)) => {
            dst_values.append(src_values);
        }
        (Partial::JsonObjectAgg(dst_values, _, _), Partial::JsonObjectAgg(src_values, _, _)) => {
            // Go's merge overwrites duplicate keys with the incoming map.
            dst_values.append(src_values);
        }
        (Partial::ApproxCountDistinct(dst_sketch), Partial::ApproxCountDistinct(src_sketch)) => {
            dst_sketch.merge(src_sketch);
        }
        (
            Partial::ApproxPercentile {
                values: dst_values, ..
            },
            Partial::ApproxPercentile {
                values: src_values, ..
            },
        ) => dst_values.append(src_values),
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
    /// The stack varint the bucket hash reads is byte-for-byte the codec's
    /// (Go `binary.PutVarint`), so an integer group lands in the same final
    /// bucket as its encoded key would.
    #[test]
    fn stack_varint_matches_the_codec() {
        for value in [
            0i64,
            1,
            -1,
            63,
            64,
            -64,
            -65,
            127,
            128,
            1 << 20,
            -(1 << 40),
            i64::MAX,
            i64::MIN,
        ] {
            let mut expected = Vec::new();
            encode_varint(&mut expected, value);
            let mut buffer = [0u8; 10];
            let len = super::encode_varint_into(&mut buffer, value);
            assert_eq!(&buffer[..len], expected.as_slice(), "{value}");
        }
    }

    use super::*;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::NoColumns;

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

    #[test]
    fn group_key_expressions_precede_aggregate_inputs() {
        // Go GetGroupKey evaluates one whole expression column at a time;
        // updatePartialResult evaluates aggregate arguments only afterward.
        #[derive(Default)]
        struct Reads(std::cell::RefCell<Vec<String>>);
        impl HashAggContext for Reads {
            const PARALLEL_WORKERS_MAY_EVAL: bool = false;
        }
        impl Columns for Reads {
            fn get(&self, _: &[String]) -> Option<Datum> {
                None
            }
            fn get_uservar(&self, name: &str) -> Option<Datum> {
                self.0.borrow_mut().push(name.to_owned());
                Some(Datum::Int(1))
            }
        }
        let variable = |name: &str| {
            Expression::ScalarFunction(tidb_expr::expression::ScalarFunction::new(
                tidb_ast::CiString::new("getvar_int"),
                long(),
                vec![Expression::Constant(tidb_expr::constant::Constant::new(
                    Datum::Bytes(name.as_bytes().to_vec()),
                    FieldType::new(FieldTypeCode::VarString),
                ))],
            ))
        };
        let ctx = Reads::default();
        let memory = StatementMemory::default();
        let tracker = memory.operator_tracker(0);
        let mut chunk = Chunk::new_with_capacity(&[long()], 3);
        for value in 0..3 {
            chunk.append_int64(0, value);
        }
        let groups = [variable("a"), variable("b")];
        let funcs = [AggFunc::new(AggKind::Sum, Some(variable("c")))];
        let mut maps = vec![PipelineMap::default()];
        let mut keys = PipelineKeyBuffer::new(Arc::clone(&tracker));
        fold_chunk(
            FoldInputs {
                ctx: &ctx,
                memory: &memory,
                group_by: &groups,
                integer_columns: None,
                agg_funcs: &funcs,
                input_modes: &funcs.iter().map(AggInputMode::new).collect::<Vec<_>>(),
                collations: &[tidb_datatype::Collation::DEFAULT],
            },
            &mut maps,
            1,
            &tracker,
            &chunk,
            &mut keys,
        )
        .unwrap();
        assert_eq!(
            *ctx.0.borrow(),
            ["a", "a", "a", "b", "b", "b", "c", "c", "c"]
        );
        ctx.0.borrow_mut().clear();
        let mut serial = HashAggExec::new(
            ExecutorMeta::new(Schema::new(vec![Column::new(0, long())]), 0, 3, 3),
            groups.to_vec(),
            funcs.to_vec(),
            MultiChunkSource::new(&[(0, 0)], 3),
            ctx,
            memory,
        );
        serial.fold_chunk(&chunk, chunk.num_rows()).unwrap();
        assert_eq!(
            *serial.ctx.0.borrow(),
            ["a", "a", "a", "b", "b", "b", "c", "c", "c"]
        );
    }

    #[test]
    fn chunk_group_keys_match_go_codec_with_selection_and_reuse() {
        // Go TestHashGroupKeyCollation and GetGroupKey's retained row buffers,
        // with Column.VecEval* applying the chunk's logical row selection.
        for collation in ["binary", "utf8_general_ci", "utf8_unicode_ci"] {
            let string = FieldType::new(FieldTypeCode::VarString)
                .with_collation(tidb_datatype::Collation::from_name(collation).unwrap());
            let fields = [
                long(),
                long().with_unsigned(true),
                string,
                decimal().with_flen(10).with_decimal(2),
            ];
            let expressions: Vec<_> = fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let mut column = Column::new(index as i64 + 1, field.clone());
                    column.index = index as i64;
                    Expression::Column(column)
                })
                .collect();
            let mut chunk = Chunk::new_with_capacity(&fields, 4);
            for (signed, unsigned, text) in [(-7, u64::MAX, "Á "), (0, 0, "a"), (7, 7, "")] {
                chunk.append_int64(0, signed);
                chunk.append_uint64(1, unsigned);
                chunk.append_string(2, text);
                chunk.append_datum(
                    3,
                    &Datum::Decimal(Decimal::from_scaled_i128(signed as i128, 2)),
                );
            }
            for column in 0..fields.len() {
                chunk.append_null(column);
            }
            let memory = StatementMemory::default();
            let tracker = memory.operator_tracker(0);
            let mut keys = PipelineKeyBuffer::new(Arc::clone(&tracker));
            for selection in [None, Some(vec![3, 0]), Some(vec![2, 0, 3, 1])] {
                chunk.set_sel(selection);
                keys.prepare(
                    &FoldInputs {
                        ctx: &NoColumns,
                        memory: &memory,
                        group_by: &expressions,
                        integer_columns: None,
                        agg_funcs: &[],
                        input_modes: &[],
                        collations: &[],
                    },
                    &chunk,
                    4,
                )
                .unwrap();
                for logical in 0..chunk.num_rows() {
                    let row = chunk.get_row(logical);
                    let mut expected = Vec::new();
                    for (index, field) in fields.iter().enumerate() {
                        expected.extend(
                            tidb_codec::hash_group_key(&[row.get_datum(index, field)], field)
                                .unwrap()
                                .pop()
                                .unwrap(),
                        );
                    }
                    assert!(
                        matches!(keys.key(logical), PipelineMapKeyRef::Bytes(bytes) if bytes == expected)
                    );
                }
                // Serial cop partials retain grouping datums as output.
                // Encoding must remain identical, without re-evaluation.
                let mut partial_keys = GroupKeyBuffer::default();
                partial_keys
                    .prepare(&NoColumns, &chunk, &expressions, true)
                    .unwrap();
                for logical in 0..chunk.num_rows() {
                    assert_eq!(
                        partial_keys.encoded[logical],
                        keys.group_keys.encoded[logical]
                    );
                    let mut values = Vec::new();
                    partial_keys.append_values(logical, &mut values);
                    let expected: Vec<_> = fields
                        .iter()
                        .enumerate()
                        .map(|(index, field)| chunk.get_row(logical).get_datum(index, field))
                        .collect();
                    assert_eq!(values, expected);
                }
            }
            assert!(tracker.bytes_consumed() > 0);
            drop(keys);
            assert_eq!(tracker.bytes_consumed(), 0);
        }
    }

    /// A source emitting `rows` in fixed-size chunks, so the pipeline sees
    /// more input chunks than lanes.
    struct MultiChunkSource {
        meta: ExecutorMeta,
        fields: Vec<FieldType>,
        data: Chunk,
        offset: usize,
        chunk_size: usize,
        selected: bool,
    }
    impl MultiChunkSource {
        fn new(rows: &[(i64, i64)], chunk_size: usize) -> Box<dyn Executor> {
            Self::with_selection(rows, chunk_size, false)
        }

        fn with_selection(
            rows: &[(i64, i64)],
            chunk_size: usize,
            selected: bool,
        ) -> Box<dyn Executor> {
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
                selected,
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
            if self.selected {
                req.set_sel(Some(
                    (0..req.num_rows())
                        .rev()
                        .filter(|row| row % 3 != 1)
                        .collect(),
                ));
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
            if let Err(error) = exec.next(&mut req) {
                panic!(
                    "HashAgg next failed after {} spill requests: {error:?}",
                    exec.spill_times()
                );
            }
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

    /// FAIL-BEFORE/PASS-AFTER: Go allocates its 256 spill chunks only from
    /// `HashAggPartialWorker.prepareForSpill`. Rust used to allocate all 256
    /// for every parallel aggregation, including DISTINCT where spill is
    /// deliberately disabled.
    #[test]
    fn spill_partitions_allocate_chunks_only_when_spill_starts() {
        let memory = StatementMemory::default();
        let disk_tracker = tidb_util::disk::Tracker::new(1, -1);
        let spill = ParallelSpillPartitions::new(&memory, &disk_tracker);

        assert!(spill.chunks.is_empty());
        assert!(spill.files.is_empty());
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

    fn sort_rows(rows: &mut [Vec<Datum>]) {
        rows.sort_by(|left, right| {
            for (left, right) in left.iter().zip(right) {
                let ordering = compare_datums(left, right).unwrap_or(Ordering::Equal);
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            left.len().cmp(&right.len())
        });
    }

    /// FAIL-BEFORE/PASS-AFTER regression: the pipeline must engage (worker
    /// threads ran, every chunk dispatched) and produce EXACTLY the serial
    /// path's result set. Go's parallel HashAgg does not promise serial
    /// first-seen output order.
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
        )
        .with_pipeline_concurrency_override(1, 1);
        let mut serial_rows = run(&mut serial_exec);
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
        let mut parallel_rows = drain_rows(&mut parallel_exec);
        // Diagnostics must be read while the Open is still live: `close`
        // releases the pipeline stats.
        let info = parallel_exec.pipeline_run_info().expect("pipeline ran");
        parallel_exec.close().unwrap();

        sort_rows(&mut serial_rows);
        sort_rows(&mut parallel_rows);
        assert_eq!(parallel_rows, serial_rows);

        // Configured workers consume the bounded resource queue; readiness
        // determines dispatch, as in Go, not a fetcher-side fallback map.
        let (_p, _f, dispatched, threads) = info;
        let expected_chunks = data.len().div_ceil(CHUNK_SIZE);
        assert_eq!(dispatched, expected_chunks, "every chunk was folded");
        assert!(threads > 1, "multiple partial-worker threads ran");
    }

    #[test]
    fn single_chunk_pipeline_uses_configured_workers_and_reusable_inputs() {
        let data: Vec<(i64, i64)> = (0..CHUNK_SIZE)
            .map(|row| ((row % 7) as i64, row as i64))
            .collect();
        let mut exec = build(
            vec![col(0)],
            count_sum_min_max_first_funcs(),
            MultiChunkSource::new(&data, CHUNK_SIZE),
            &wide_out_types(),
        );
        assert!(
            exec.pipeline_eligibility().is_some(),
            "one chunk still uses Go's configured parallel HashAgg shape"
        );

        exec.open().unwrap();
        let rows = drain_rows(&mut exec);
        let info = exec.pipeline_run_info().expect("pipeline ran");
        let allocated = exec.pipeline_input_chunks().expect("pipeline ran");
        exec.close().unwrap();

        assert_eq!(rows.len(), 7);
        let (partial, _final_, dispatched, threads) = info;
        assert_eq!(dispatched, 1);
        assert_eq!(
            threads, 1,
            "only the lane with input needs a runnable native task"
        );
        assert_eq!(allocated, 2 * partial);

        // Go GetGroupKey -> Column.VecEvalInt reconstructs input.Sel().
        // Both the key and its aggregate inputs must see those logical rows.
        let selected = || {
            build(
                vec![col(0)],
                count_sum_min_max_first_funcs(),
                MultiChunkSource::with_selection(&data, CHUNK_SIZE, true),
                &wide_out_types(),
            )
        };
        let mut reference = selected().with_pipeline_concurrency_override(1, 1);
        let mut candidate = selected();
        let mut expected = run(&mut reference);
        let mut actual = run(&mut candidate);
        sort_rows(&mut expected);
        sort_rows(&mut actual);
        assert_eq!(
            actual, expected,
            "selected keys and values must stay paired"
        );
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

    /// FAIL-BEFORE/PASS-AFTER: Go does not set `IsUnparallelExec` for
    /// DISTINCT. Worker-local sets are unioned before the final COUNT.
    #[test]
    fn distinct_aggregate_uses_parallel_set_merge() {
        let data = vec![(1, 5), (1, 5), (1, 8), (1, 8), (1, 13), (2, 7), (2, 7)];
        let funcs = || {
            let mut func = AggFunc::new(AggKind::Count, Some(col(1)));
            func.distinct = true;
            vec![func]
        };
        let mut serial = build(
            vec![col(0)],
            funcs(),
            MultiChunkSource::new(&data, 1),
            &[long()],
        )
        .with_pipeline_concurrency_override(1, 1);
        let mut expected = run(&mut serial);

        let mut parallel = build(
            vec![col(0)],
            funcs(),
            MultiChunkSource::new(&data, 1),
            &[long()],
        );
        assert!(parallel.pipeline_eligibility().is_some());
        let mut actual = run(&mut parallel);
        sort_rows(&mut expected);
        sort_rows(&mut actual);
        assert_eq!(actual, expected);
    }

    /// Two long columns whose second holds `None` as NULL, `chunk_rows` per
    /// chunk.
    struct IntNullSource {
        meta: ExecutorMeta,
        fields: Vec<FieldType>,
        data: Chunk,
        offset: usize,
        chunk_size: usize,
    }
    impl IntNullSource {
        fn new(rows: &[(i64, Option<i64>)], chunk_size: usize) -> Box<dyn Executor> {
            let fields = vec![long(), long()];
            let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
            for (group, value) in rows {
                data.append_int64(0, *group);
                match value {
                    Some(value) => data.append_int64(1, *value),
                    None => data.append_null(1),
                }
            }
            let mut group = Column::new(1, long());
            group.index = 0;
            let mut value = Column::new(2, long());
            value.index = 1;
            Box::new(IntNullSource {
                meta: ExecutorMeta::new(Schema::new(vec![group, value]), 0, chunk_size, chunk_size),
                fields,
                data,
                offset: 0,
                chunk_size,
            })
        }
    }
    impl Executor for IntNullSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.offset = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.offset + self.chunk_size).min(self.data.num_rows());
            for row in self.offset..end {
                req.append_row(self.data.get_row(row));
            }
            self.offset = end;
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

    fn integer_count_distinct_dataset(
        groups: i64,
        rows_per_group: i64,
    ) -> (Vec<(i64, Option<i64>)>, Vec<Vec<Datum>>) {
        let mut data = Vec::new();
        let mut expected = Vec::new();
        for group in 0..groups {
            let mut distinct = std::collections::HashSet::new();
            for i in 0..rows_per_group {
                let value = if i % 9 == 4 {
                    None
                } else {
                    Some((i * 31 + group) % 37 - 18)
                };
                if let Some(value) = value {
                    distinct.insert(value);
                }
                data.push((group, value));
            }
            expected.push(vec![Datum::Int(distinct.len() as i64)]);
        }
        data.push((groups, None));
        expected.push(vec![Datum::Int(0)]);
        data.sort_by_key(|(group, value)| (value.map_or(-1, |v| v.rem_euclid(5)), *group));
        (data, expected)
    }

    fn integer_count_distinct_funcs() -> Vec<AggFunc> {
        let mut func = AggFunc::new(AggKind::Count, Some(col(1)));
        func.distinct = true;
        vec![func]
    }

    /// Go's `countOriginalWithDistinct4Int` keeps the values in an `Int64Set`
    /// and skips NULL; the typed state answers the distinct count of a
    /// NULL-bearing, duplicate-heavy input serially and through the
    /// pipeline's set-union merge.
    #[test]
    fn integer_count_distinct_counts_its_value_set() {
        let (data, mut expected) = integer_count_distinct_dataset(40, 250);
        let mut serial = build(
            vec![col(0)],
            integer_count_distinct_funcs(),
            IntNullSource::new(&data, 128),
            &[long()],
        )
        .with_pipeline_concurrency_override(1, 1);
        let mut serial_rows = run(&mut serial);
        let mut parallel = build(
            vec![col(0)],
            integer_count_distinct_funcs(),
            IntNullSource::new(&data, 128),
            &[long()],
        );
        assert!(parallel.pipeline_eligibility().is_some());
        let mut parallel_rows = run(&mut parallel);
        sort_rows(&mut expected);
        sort_rows(&mut serial_rows);
        sort_rows(&mut parallel_rows);
        assert_eq!(serial_rows, expected);
        assert_eq!(parallel_rows, expected);
    }

    /// The typed value set round-trips the spill format.
    #[test]
    fn integer_count_distinct_survives_a_spill() {
        let (data, mut expected) = integer_count_distinct_dataset(20_000, 3);
        let mut spilled = HashAggExec::new(
            out_meta(&[long()]),
            vec![col(0)],
            integer_count_distinct_funcs(),
            IntNullSource::new(&data, 128),
            NoColumns,
            StatementMemory::new(2 * 1024 * 1024, crate::mem_quota::OomAction::Cancel, 42)
                .with_tmp_storage_on_oom(true),
        );
        spilled.open().unwrap();
        let mut spilled_rows = drain_rows(&mut spilled);
        assert!(spilled.spill_times() > 0, "the quota must force a spill");
        spilled.close().unwrap();
        sort_rows(&mut expected);
        sort_rows(&mut spilled_rows);
        assert_eq!(spilled_rows, expected);
    }

    /// Go admits REAL-domain SUM to the partial/final worker pipeline.
    #[test]
    fn real_sum_is_pipeline_eligible() {
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
        assert!(exec.pipeline_eligibility().is_some());
    }

    /// Go's admission does not depend on an arbitrary memory-quota cutoff.
    #[test]
    fn low_quota_does_not_change_parallel_admission() {
        let exec = HashAggExec::new(
            out_meta(&[long()]),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            MultiChunkSource::new(&[(1, 5)], 1),
            NoColumns,
            StatementMemory::new(1 << 20, crate::mem_quota::OomAction::Cancel, 42),
        );
        assert!(exec.pipeline_eligibility().is_some());
    }

    /// FAIL-BEFORE/PASS-AFTER: Go keeps the partial/final worker topology
    /// under pressure and spills serialized partial results by partition.
    /// It does not cancel merely because this is the parallel HashAgg path.
    #[test]
    fn parallel_hashagg_spills_partial_results_and_finishes() {
        // Repeat the keys after a full pass so restore must merge partial
        // states across spills, not just deserialize disjoint groups.
        let data = (0..20_000)
            .cycle()
            .take(40_000)
            .map(|value| (value, 1))
            .collect::<Vec<_>>();
        let mut exec = HashAggExec::new(
            out_meta(&[long()]),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            MultiChunkSource::new(&data, 128),
            NoColumns,
            StatementMemory::new(512 * 1024, crate::mem_quota::OomAction::Cancel, 42)
                .with_tmp_storage_on_oom(true),
        );

        exec.open().unwrap();
        let mut first = exec.new_chunk();
        first.set_required_rows(1, exec.max_chunk_size());
        exec.next(&mut first).unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(first.get_row(0).get_int64(0), 2);
        let stats = Arc::clone(exec.pipeline_stats.as_ref().unwrap());
        let final_workers = stats.final_concurrency;
        assert!(
            stats
                .finalized_chunks
                .load(std::sync::atomic::Ordering::SeqCst)
                <= final_workers + 1,
            "bounded result holders must prevent eager finalization"
        );
        let mut rows = vec![vec![Datum::new_int(2)]];
        rows.extend(drain_rows(&mut exec));
        assert_eq!(rows.len(), 20_000);
        assert!(rows.iter().all(|row| row == &[Datum::new_int(2)]));
        assert!(exec.spill_times() > 1);
        let (partial, _, _, threads) = exec.pipeline_run_info().expect("pipeline ran");
        assert_eq!(threads, partial, "workers survive every spill barrier");
        assert_eq!(exec.pipeline_input_chunks(), Some(2 * partial));
        assert_eq!(
            stats
                .final_output_chunks
                .load(std::sync::atomic::Ordering::SeqCst),
            final_workers
        );
        assert!(
            stats
                .finalized_chunks
                .load(std::sync::atomic::Ordering::SeqCst)
                > final_workers
        );
        assert_eq!(
            stats
                .final_workers_finished
                .load(std::sync::atomic::Ordering::SeqCst),
            final_workers
        );
        assert_eq!(
            exec.tracker.bytes_consumed(),
            0,
            "input and key buffers released"
        );
        assert!(exec.bytes_in_disk() > 0, "spill files survive until Close");
        exec.close().unwrap();
        assert_eq!(exec.bytes_in_disk(), 0);

        // Reopen, then interrupt while workers are waiting for their result
        // holders. Error cleanup must release every worker and spill file.
        exec.open().unwrap();
        exec.next(&mut first).unwrap();
        let stats = Arc::clone(exec.pipeline_stats.as_ref().unwrap());
        exec.memory
            .sql_killer()
            .send_kill_signal(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
        assert!(exec.next(&mut first).is_err());
        exec.close().unwrap();
        assert_eq!(
            stats
                .final_workers_finished
                .load(std::sync::atomic::Ordering::SeqCst),
            final_workers
        );
        assert_eq!(exec.bytes_in_disk(), 0);
    }

    /// FAIL-BEFORE/PASS-AFTER: Go's parallel DISTINCT aggregate spill path
    /// serializes each worker's retained value set, then final workers union
    /// those sets before applying COUNT. Rust used to disable the parallel
    /// spill action for every DISTINCT function, so a pressured query fell
    /// through to cancellation instead of producing the same result as the
    /// unspilled pipeline.
    #[test]
    fn parallel_hashagg_distinct_spill_preserves_value_sets() {
        let data = (0..20_000_i64)
            .flat_map(|group| {
                [
                    (group, group % 17),
                    (group, group % 17),
                    (group, group % 19),
                ]
            })
            .collect::<Vec<_>>();
        let make_func = || {
            let mut func = AggFunc::new(AggKind::Count, Some(col(1)));
            func.distinct = true;
            let mut sum = AggFunc::new(AggKind::Sum, Some(col(1)));
            sum.distinct = true;
            vec![func, sum]
        };

        let mut expected_exec = HashAggExec::new(
            out_meta(&[long(), decimal()]),
            vec![col(0)],
            make_func(),
            MultiChunkSource::new(&data, 128),
            NoColumns,
            StatementMemory::default(),
        );
        let mut expected = run(&mut expected_exec);
        sort_rows(&mut expected);

        let dir = crate::test_temp_storage::scratch_dir("hashagg-parallel-distinct");
        let mut exec = HashAggExec::new(
            out_meta(&[long(), decimal()]),
            vec![col(0)],
            make_func(),
            MultiChunkSource::new(&data, 128),
            NoColumns,
            StatementMemory::new(512 * 1024, crate::mem_quota::OomAction::Cancel, 42)
                .with_tmp_storage_on_oom(true)
                .with_spill_storage(crate::test_temp_storage::storage(&dir)),
        );
        exec.open().unwrap();
        let mut actual = drain_rows(&mut exec);
        assert!(exec.spill_times() > 0, "DISTINCT spill never triggered");
        sort_rows(&mut actual);
        exec.close().unwrap();
        assert_eq!(actual, expected);
        let _ = std::fs::remove_dir_all(&dir);
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
        exec.open().unwrap();
        assert_eq!(drain_rows(&mut exec), expected);
        let stats = Arc::clone(exec.pipeline_stats.as_ref().unwrap());
        let (_, _, dispatched, workers) = exec.pipeline_run_info().unwrap();
        assert_eq!(dispatched, 0);
        assert_eq!(workers, 0, "idle partial lanes require no pool task");
        assert_eq!(
            stats
                .final_output_chunks
                .load(std::sync::atomic::Ordering::SeqCst),
            1,
            "only the default global group needs a final result holder"
        );
        exec.close().unwrap();
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
        let mut expected = run(&mut serial_exec);

        let mut exec = build(
            vec![col(0)],
            funcs(),
            MultiChunkSource::new(&data, 128),
            &types,
        );
        assert!(exec.pipeline_eligibility().is_some());
        let mut actual = run(&mut exec);
        sort_rows(&mut expected);
        sort_rows(&mut actual);
        assert_eq!(actual, expected);
    }

    /// A two-column source whose second column holds DECIMAL(15,2) cells
    /// given as `(coefficient, scale)` (`None` is NULL), so an aggregate
    /// over it takes the fixed-scale coefficient path exactly as a TPC-H
    /// column does; a cell stored at another scale exercises the
    /// materialized fallback.
    struct DecimalChunkSource {
        meta: ExecutorMeta,
        fields: Vec<FieldType>,
        data: Chunk,
        offset: usize,
        chunk_size: usize,
    }
    impl DecimalChunkSource {
        fn field_type() -> FieldType {
            let mut decimal = decimal();
            decimal.set_flen(15);
            decimal.set_decimal(2);
            decimal
        }

        fn column(index: i64) -> Expression {
            let mut c = Column::new(index + 1, Self::field_type());
            c.index = index;
            Expression::Column(c)
        }

        fn new(rows: &[(i64, Option<(i64, u32)>)], chunk_size: usize) -> Box<dyn Executor> {
            let fields = vec![long(), Self::field_type()];
            let mut data = Chunk::new_with_capacity(&fields, rows.len().max(1));
            for (group, cell) in rows {
                data.append_int64(0, *group);
                match cell {
                    Some((coefficient, scale)) => data.append_my_decimal(
                        1,
                        &tidb_datatype::MyDecimal::from_scaled_i128(
                            i128::from(*coefficient),
                            *scale,
                            *scale,
                        )
                        .expect("a short fraction fits"),
                    ),
                    None => data.append_null(1),
                }
            }
            let mut group = Column::new(1, long());
            group.index = 0;
            let mut value = Column::new(2, Self::field_type());
            value.index = 1;
            Box::new(DecimalChunkSource {
                meta: ExecutorMeta::new(Schema::new(vec![group, value]), 0, chunk_size, chunk_size),
                fields,
                data,
                offset: 0,
                chunk_size,
            })
        }
    }
    impl Executor for DecimalChunkSource {
        fn open(&mut self) -> Result<(), ExecError> {
            self.offset = 0;
            Ok(())
        }
        fn next(&mut self, req: &mut Chunk) -> Result<(), ExecError> {
            req.reset();
            let end = (self.offset + self.chunk_size).min(self.data.num_rows());
            for row in self.offset..end {
                req.append_row(self.data.get_row(row));
            }
            self.offset = end;
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

    /// Group `g` holds values spread over both signs, most as DECIMAL(15,2)
    /// cents and every seventeenth as a three-digit-fraction cell that no
    /// two-digit coefficient can represent (the fast state must materialize
    /// and the datum path take over); group 0 holds only NULLs. The expected
    /// extrema are computed from the integers directly, in mills,
    /// independent of either aggregate representation.
    fn decimal_min_max_dataset(
        groups: i64,
        rows_per_group: i64,
    ) -> (Vec<(i64, Option<(i64, u32)>)>, Vec<Vec<Datum>>) {
        let mut data = Vec::new();
        let mut expected = vec![vec![Datum::Null, Datum::Null]];
        for group in 1..=groups {
            let mut mills = Vec::new();
            for i in 0..rows_per_group {
                let value = (i * 7919 + group * 31) % 20_011 - 10_005;
                if i % 17 == 3 {
                    mills.push(value * 10 + 5);
                    data.push((group, Some((value * 10 + 5, 3))));
                } else {
                    mills.push(value * 10);
                    data.push((group, Some((value, 2))));
                }
                if i % 11 == 0 {
                    data.push((group, None));
                }
            }
            // Both extrema of every group are three-digit-fraction cells
            // outside the two-digit range, so a refused fold that dropped or
            // failed its row would change the answer.
            for extreme in [-1_000_055, 1_000_055] {
                mills.push(extreme);
                data.push((group, Some((extreme, 3))));
            }
            let decimal = |mills: i64| {
                Datum::Decimal(tidb_datatype::Decimal::from_scaled_i128(mills.into(), 3))
            };
            expected.push(vec![
                decimal(*mills.iter().min().unwrap()),
                decimal(*mills.iter().max().unwrap()),
            ]);
        }
        for _ in 0..5 {
            data.push((0, None));
        }
        // Interleave the groups so every worker and every chunk sees most of them.
        data.sort_by_key(|(group, cell)| (cell.map_or(0, |(c, _)| c.rem_euclid(13)), *group));
        (data, expected)
    }

    fn decimal_min_max_funcs() -> Vec<AggFunc> {
        vec![
            AggFunc::new(AggKind::Min, Some(DecimalChunkSource::column(1))),
            AggFunc::new(AggKind::Max, Some(DecimalChunkSource::column(1))),
        ]
    }

    /// Go `maxMin4Decimal` keeps the extremum as a MyDecimal value in place;
    /// the fixed-scale coefficient state must answer exactly what the datum
    /// path answers, serial and parallel, NULL-only groups included.
    #[test]
    fn decimal_min_max_fold_by_coefficient_matches_the_expected_extrema() {
        let (data, mut expected) = decimal_min_max_dataset(37, 97);
        let types = [decimal(), decimal()];
        let mut serial_exec = build(
            vec![col(0)],
            decimal_min_max_funcs(),
            DecimalChunkSource::new(&data, 128),
            &types,
        )
        .with_pipeline_concurrency_override(1, 1);
        let mut serial = run(&mut serial_exec);
        let mut exec = build(
            vec![col(0)],
            decimal_min_max_funcs(),
            DecimalChunkSource::new(&data, 128),
            &types,
        );
        assert!(exec.pipeline_eligibility().is_some());
        let mut parallel = run(&mut exec);
        sort_rows(&mut expected);
        sort_rows(&mut serial);
        sort_rows(&mut parallel);
        assert_eq!(serial, expected);
        assert_eq!(parallel, expected);
    }

    /// The fixed-scale MIN/MAX state round-trips through the spill format
    /// and merges back into the same extrema.
    #[test]
    fn decimal_min_max_states_survive_a_spill() {
        let (data, mut expected) = decimal_min_max_dataset(20_000, 1);
        let mut exec = HashAggExec::new(
            out_meta(&[decimal(), decimal()]),
            vec![col(0)],
            decimal_min_max_funcs(),
            DecimalChunkSource::new(&data, 128),
            NoColumns,
            StatementMemory::new(2 * 1024 * 1024, crate::mem_quota::OomAction::Cancel, 42)
                .with_tmp_storage_on_oom(true),
        );
        exec.open().unwrap();
        let mut rows = drain_rows(&mut exec);
        assert!(exec.spill_times() > 0, "the quota must force a spill");
        exec.close().unwrap();
        sort_rows(&mut expected);
        sort_rows(&mut rows);
        assert_eq!(rows, expected);
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
        let mut serial_rows = drain(&mut serial_exec);
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
        let mut rows = drain(&mut exec);
        let info = exec
            .pipeline_run_info()
            .expect("production StmtContext selected the parallel pipeline");
        exec.close().unwrap();

        sort_rows(&mut serial_rows);
        sort_rows(&mut rows);
        assert_eq!(rows, serial_rows);
        let (_partial, _final_, dispatched, threads) = info;
        assert!(dispatched > 0, "every chunk was dispatched to workers");
        assert!(threads > 1, "multiple partial-worker threads ran");
    }

    /// FAIL-BEFORE/PASS-AFTER: Go's executor builder reads the resolved
    /// HashAgg worker counts from the statement session. Rust previously
    /// dropped these typed values and searched the expression builtin's
    /// deliberately narrow sysvar view, so even a 1/1 statement entered the
    /// default 5/5 pipeline.
    #[test]
    fn production_stmt_context_hashagg_concurrency_controls_admission() {
        let exec = HashAggExec::new(
            out_meta(&[long()]),
            vec![col(0)],
            vec![AggFunc::new(AggKind::Count, Some(col(1)))],
            MultiChunkSource::new(&[(1, 1)], 1),
            crate::StmtContext::for_query().with_hashagg_concurrency(1, 1),
            StatementMemory::default(),
        );

        assert_eq!(exec.pipeline_eligibility(), None);
    }
}

#[cfg(test)]
mod bucket_spread_tests {
    use super::*;

    /// The integer bucket must spread groups across the final workers.
    ///
    /// It chooses which final worker owns a group, so a hash that clustered
    /// would quietly serialize the final stage while still producing the right
    /// answer -- a performance bug no correctness test would catch. TPC-H's
    /// `l_partkey` is a dense run from 1, the shape most likely to cluster, so
    /// pin it on exactly that, at the final-worker count and at the spill
    /// partition count.
    #[test]
    fn the_integer_bucket_spreads_a_dense_key_range() {
        for bucket_count in [5_usize, SPILLED_PARTITION_NUM] {
            let keys = 200_000_i64;
            let mut counts = vec![0usize; bucket_count];
            for key in 1..=keys {
                counts[map_key_bucket(PipelineMapKeyRef::Int(Some(key)), bucket_count)] += 1;
            }
            let ideal = (keys as usize / bucket_count) as f64;
            // Four standard deviations of the binomial an even hash produces,
            // so the bound tightens with the share rather than becoming a
            // flaky assertion about ordinary spread: about 2% at five buckets,
            // about 14% at 256, where an even share is only 781 keys.
            let tolerance = 4.0 * ideal.sqrt() / ideal;
            for (bucket, count) in counts.iter().enumerate() {
                let drift = (*count as f64 - ideal).abs() / ideal;
                assert!(
                    drift < tolerance,
                    "bucket {bucket} of {bucket_count} holds {count} of {keys} keys \
                     ({:.1}% off an even share, tolerance {:.1}%); a clustering \
                     bucket serializes the final stage",
                    drift * 100.0,
                    tolerance * 100.0
                );
            }
        }
    }
}
