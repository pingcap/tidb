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
//!   the fetcher allocates a fresh request chunk per dispatch.
//! * Go waits for every partial worker before any final worker consumes its
//!   mapper. Rust transfers those owned mapper vectors through the partial
//!   task receipts, then submits one merge task per final bucket. This keeps
//!   the same N-to-M partitioning without constructing N*M shuffle messages.
//! * Go streams `finalOutputCh` chunks across `Next` calls. Rust final tasks
//!   return one owned map per worker; the whole aggregation completes inside
//!   one `execute()` call and `Next` then emits groups from those maps.
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
//! merge its serialized partial states. DISTINCT follows Go's spill gate and
//! stays in memory because its value sets are not spillable there either.

use super::spill::parallel_new_group_bytes;
use super::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::sync_channel;
use std::sync::Arc;
#[cfg(test)]
use std::sync::Mutex;
use tidb_vardef::tidb_vars::{
    TIDB_ENABLE_PARALLEL_HASHAGG_SPILL, TIDB_TRACK_AGGREGATE_MEMORY_USAGE,
};

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
}

/// A pipeline group-map key. A single integer group item keys by its chunk
/// lane directly so the native map does not allocate one byte vector per
/// group; every other shape keeps Go's encoded group key.
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
            // Go preallocates ten bytes per group item in `GetGroupKey`.
            PipelineMapKey::Int(_) => 10,
            PipelineMapKey::Bytes(bytes) => bytes.len(),
        }
    }
}

type PipelineMap = HashMap<PipelineMapKey, PipelineGroup, BuildHasherDefault<super::HashAggHasher>>;

/// One group inside a worker's map: its aggregate partial states.
struct PipelineGroup {
    states: Vec<AggState>,
}

impl PipelineGroup {
    /// Creates the group; the CALLER batches the tracker consume (one
    /// round-trip per chunk, not per group — 1.5M-group shapes showed the
    /// lock in profiles).
    fn new(funcs: &[AggFunc], key_len: usize) -> (Self, i64) {
        let bytes = parallel_new_group_bytes(key_len, funcs);
        (
            PipelineGroup {
                states: funcs.iter().map(AggState::new_parallel).collect(),
            },
            bytes,
        )
    }
}

const SPILLED_PARTITION_NUM: usize = 256;
const SPILL_CHUNK_SIZE: usize = 1024;
const SPILL_FORMAT_VERSION: u8 = 1;

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
            Partial::JsonArrayAgg(values, value_type.clone())
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
            Partial::JsonObjectAgg(values, value_type.clone(), *key_is_binary)
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

fn encode_spill_entry(key: &PipelineMapKey, group: &PipelineGroup) -> Result<Vec<u8>, ExecError> {
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
    for state in &group.states {
        write_partial(&mut writer, &state.partial)?;
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
        let mut state = AggState::new_parallel(func);
        state.partial = read_partial(&mut reader, func)?;
        states.push(state);
    }
    reader.finish()?;
    Ok((key, PipelineGroup { states }))
}

struct ParallelSpillPartitions {
    field_types: Vec<FieldType>,
    chunks: Vec<Chunk>,
    files: Vec<Option<DataInDiskByChunks>>,
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
        map_key_bucket(key, SPILLED_PARTITION_NUM)
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
            file
        });
        file.add(&self.chunks[partition])
            .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
        self.chunks[partition].reset();
        self.has_data = true;
        Ok(())
    }

    fn spill_maps(&mut self, maps: Vec<PipelineMap>) -> Result<(), ExecError> {
        self.prepare();
        for map in maps {
            for (key, group) in map {
                let partition = Self::bucket(&key);
                let encoded = encode_spill_entry(&key, &group)?;
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

    fn restore_partition(
        &mut self,
        partition: usize,
        funcs: &[AggFunc],
    ) -> Result<PipelineMap, ExecError> {
        let Some(file) = self.files[partition].as_mut() else {
            return Ok(PipelineMap::default());
        };
        let mut restored = PipelineMap::default();
        for chunk_index in 0..file.num_chunks() {
            let chunk = file
                .get_chunk(chunk_index)
                .map_err(|error| ExecError::SpillFailed(error.to_string()))?;
            for row_index in 0..chunk.num_rows() {
                let encoded = chunk.get_row(row_index).get_bytes(0);
                let (key, group) = decode_spill_entry(&encoded, funcs)?;
                match restored.entry(key) {
                    Entry::Vacant(slot) => {
                        slot.insert(group);
                    }
                    Entry::Occupied(mut slot) => merge_groups(slot.get_mut(), group)?,
                }
            }
        }
        file.close();
        Ok(restored)
    }
}

/// Go `murmur3.Sum32(key) % finalConcurrency`.
fn key_bucket(key: &[u8], bucket_count: usize) -> usize {
    crate::shuffle::murmur3_sum32(key) as usize % bucket_count
}

fn map_key_bucket(key: &PipelineMapKey, bucket_count: usize) -> usize {
    match key {
        PipelineMapKey::Int(value) => {
            let mut encoded = Vec::with_capacity(10);
            match value {
                Some(value) => {
                    encoded.push(VARINT_FLAG);
                    encode_varint(&mut encoded, *value);
                }
                None => encoded.push(NIL_FLAG),
            }
            key_bucket(&encoded, bucket_count)
        }
        PipelineMapKey::Bytes(bytes) => key_bucket(bytes, bucket_count),
    }
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
            && !self.agg_funcs.iter().any(|function| function.distinct)
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
    maps: Vec<PipelineMap>,
    child_drained: bool,
}

#[allow(clippy::too_many_arguments)]
fn spawn_partial_lane<C: Columns + Send + Sync + Clone + 'static>(
    lane_rx: std::sync::mpsc::Receiver<(Chunk, i64)>,
    abort: PipelineAbort,
    plan: Arc<PipelinePlan<C>>,
    final_concurrency: usize,
    tracker: Arc<Tracker>,
    #[cfg(test)] stats: Arc<PipelineStats>,
) -> std::sync::mpsc::Receiver<Result<Vec<PipelineMap>, ExecError>> {
    crate::worker_pool::spawn(move || {
        #[cfg(test)]
        stats.record_partial_worker();
        let mut maps: Vec<PipelineMap> = (0..final_concurrency)
            .map(|_| PipelineMap::default())
            .collect();
        let mut error: Option<ExecError> = None;
        while let Ok((chunk, chunk_charge)) = lane_rx.recv() {
            if error.is_none() {
                let fold = fold_chunk(
                    FoldInputs {
                        ctx: &plan.ctx,
                        group_by: &plan.group_by,
                        integer_columns: plan.integer_columns.as_deref(),
                        agg_funcs: &plan.agg_funcs,
                    },
                    &mut maps,
                    final_concurrency,
                    &tracker,
                    &chunk,
                );
                if let Err(fold_error) = fold {
                    error = Some(fold_error);
                    abort.raise();
                }
            }
            // Go returns the consumed input chunk to the fetcher's pool; this
            // releases the exact growth charged while filling it.
            tracker.consume(-chunk_charge);
        }
        error.map_or(Ok(maps), Err)
    })
}

#[allow(clippy::too_many_arguments)]
fn run_pipeline_epoch<C: Columns + Send + Sync + Clone + 'static>(
    child: &mut dyn Executor,
    child_chunk: &mut Chunk,
    child_returned_empty: &mut bool,
    plan: &Arc<PipelinePlan<C>>,
    partial_concurrency: usize,
    final_concurrency: usize,
    #[cfg(test)] stats: &Arc<PipelineStats>,
    memory: &StatementMemory,
    tracker: &Arc<Tracker>,
    spill_requested: &Arc<AtomicBool>,
) -> Result<PipelineEpoch, ExecError> {
    let abort = PipelineAbort::default();

    // Go can park an idle goroutine for each configured lane almost for free.
    // A Rust pool task occupies a worker while it waits on the lane channel,
    // so admit the same lane only when the fetcher has a chunk for it. This is
    // work-driven admission, not a row-count/concurrency policy: multi-chunk
    // input still activates every configured lane through round-robin dispatch.
    let mut lane_txs: Vec<Option<std::sync::mpsc::SyncSender<(Chunk, i64)>>> =
        (0..partial_concurrency).map(|_| None).collect();
    let mut partial_handles: Vec<
        Option<std::sync::mpsc::Receiver<Result<Vec<PipelineMap>, ExecError>>>,
    > = (0..partial_concurrency).map(|_| None).collect();

    let mut next_lane = 0usize;
    let mut fetch_error: Option<ExecError> = None;
    let mut child_drained = false;
    loop {
        if abort.raised() || spill_requested.load(std::sync::atomic::Ordering::SeqCst) {
            break;
        }
        memory.check()?;
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
        let chunk_charge = child_chunk.memory_usage() - before;
        tracker.consume(chunk_charge);
        let replacement = child.new_chunk();
        let chunk = std::mem::replace(child_chunk, replacement);
        if lane_txs[next_lane].is_none() {
            let (lane_tx, lane_rx) = sync_channel::<(Chunk, i64)>(1);
            partial_handles[next_lane] = Some(spawn_partial_lane(
                lane_rx,
                abort.clone(),
                Arc::clone(plan),
                final_concurrency,
                Arc::clone(tracker),
                #[cfg(test)]
                Arc::clone(stats),
            ));
            lane_txs[next_lane] = Some(lane_tx);
        }
        if lane_txs[next_lane]
            .as_ref()
            .expect("a dispatched lane has a sender")
            .send((chunk, chunk_charge))
            .is_err()
        {
            break;
        }
        #[cfg(test)]
        stats
            .dispatched_chunks
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        next_lane = (next_lane + 1) % partial_concurrency;
    }

    drop(lane_txs);
    let mut partial_maps = Vec::with_capacity(partial_concurrency);
    let mut first_error = fetch_error;
    for handle in partial_handles.into_iter().flatten() {
        match handle.recv() {
            Ok(Ok(maps)) => partial_maps.push(maps),
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

    // Go's final worker adopts the first partial map before merging later
    // inputs. Zero or one active partial lane therefore has no merge work to
    // submit to the pool; its maps already are the exact final-worker inputs.
    let maps = if partial_maps.is_empty() {
        (0..final_concurrency)
            .map(|_| PipelineMap::default())
            .collect()
    } else if partial_maps.len() == 1 {
        partial_maps
            .pop()
            .expect("one partial worker returned maps")
    } else {
        let mut bucket_inputs: Vec<Vec<PipelineMap>> = (0..final_concurrency)
            .map(|_| Vec::with_capacity(partial_maps.len()))
            .collect();
        for maps in partial_maps {
            for (bucket, map) in maps.into_iter().enumerate() {
                bucket_inputs[bucket].push(map);
            }
        }
        crate::worker_pool::map(
            bucket_inputs.into_iter().map(|inputs| {
                move || {
                    let mut inputs = inputs.into_iter();
                    let mut acc = inputs.next().unwrap_or_default();
                    for map in inputs {
                        merge_map(&mut acc, map)?;
                    }
                    Ok::<_, ExecError>(acc)
                }
            }),
            final_concurrency,
        )
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
    };
    Ok(PipelineEpoch {
        maps,
        child_drained,
    })
}

impl<C: Columns + Send + Sync + Clone + 'static + HashAggContext> HashAggExec<C> {
    /// Go `prepare4ParallelExec` fused with `parallelExec`'s consumption:
    /// the main thread fetches child chunks and round-robin-dispatches them
    /// to the partial-worker lanes; partial workers fold rows into their own
    /// final-bucket maps and return those maps after the partial-worker barrier;
    /// one final task merges each bucket and hands one map back; the main
    /// thread then finishes values in first-seen order.
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
            integer_columns: self.integer_group_columns.clone(),
            agg_funcs: self.agg_funcs.clone(),
        });
        let spill_requested = Arc::clone(&self.parallel_spill_requested);
        let mut spilled = self
            .parallel_spill_action
            .as_ref()
            .map(|_| ParallelSpillPartitions::new(&self.memory, &self.disk_tracker));
        let mut in_memory_maps = None;
        let child_drained;

        loop {
            let epoch = run_pipeline_epoch(
                self.child.as_mut(),
                &mut self.child_chunk,
                &mut self.child_returned_empty,
                &plan,
                self.pipeline_partial_concurrency,
                self.pipeline_final_concurrency,
                #[cfg(test)]
                &stats,
                &self.memory,
                &self.tracker,
                &spill_requested,
            )?;
            let requested = spill_requested.swap(false, std::sync::atomic::Ordering::SeqCst);
            let has_spilled_data = spilled.as_ref().is_some_and(|spill| spill.has_data);
            if requested || has_spilled_data {
                let spilled = spilled.as_mut().ok_or_else(|| {
                    ExecError::unsupported("parallel HashAgg spill requested without spill action")
                })?;
                spilled.spill_maps(epoch.maps)?;
                self.tracker.replace_bytes_used(0);
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

        let mut groups = Vec::new();
        if spilled.as_ref().is_some_and(|spill| spill.has_data) {
            let spilled = spilled.as_mut().expect("spilled data owns its partitions");
            // Go restores one of the 256 partitions at a time and merges all
            // partial-result files for that partition before moving on.
            for partition in (0..SPILLED_PARTITION_NUM).rev() {
                let restored = spilled.restore_partition(partition, &plan.agg_funcs)?;
                groups.extend(restored.into_values());
            }
        } else {
            for map in in_memory_maps.unwrap_or_default() {
                groups.extend(map.into_values());
            }
        }

        let ret_types = self.meta.ret_field_types().to_vec();
        let width = plan.agg_funcs.len();
        self.parallel_output.clear();
        if groups.is_empty() && plan.group_by.is_empty() {
            let mut states: Vec<AggState> = plan.agg_funcs.iter().map(AggState::new).collect();
            for (column, state) in states.iter_mut().enumerate() {
                let value = finish_agg_value(
                    state,
                    &plan.agg_funcs[column],
                    &ret_types[column],
                    &plan.ctx,
                    &mut self.truncated[column],
                )?;
                self.parallel_output.push(value);
            }
        } else {
            for group in &mut groups {
                for (column, state) in group.states.iter_mut().enumerate() {
                    let value = finish_agg_value(
                        state,
                        &plan.agg_funcs[column],
                        &ret_types[column],
                        &plan.ctx,
                        &mut self.truncated[column],
                    )?;
                    self.parallel_output.push(value);
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
    integer_columns: Option<&'a [usize]>,
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
) -> Result<(), ExecError> {
    let FoldInputs {
        ctx,
        group_by,
        integer_columns,
        agg_funcs,
    } = inputs;
    let mut new_group_bytes_total = 0i64;
    for row_index in 0..chunk.num_rows() {
        let row = chunk.get_row(row_index);
        let (key, key_len): (PipelineMapKey, usize) = match integer_columns {
            Some([index]) => {
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
                        for &index in columns {
                            append_integer_group_key_part(row, index, &mut key);
                        }
                    }
                    None => {
                        for expr in group_by {
                            let datum = expr.eval(ctx, row)?;
                            append_hash_agg_group_key_part(ctx, expr, &datum, &mut key)?;
                        }
                    }
                }
                let len = key.len();
                (PipelineMapKey::Bytes(key), len)
            }
        };
        let bucket = map_key_bucket(&key, bucket_count);
        let entry = match maps[bucket].entry(key) {
            std::collections::hash_map::Entry::Occupied(occupied) => occupied.into_mut(),
            std::collections::hash_map::Entry::Vacant(vacant) => {
                let (group, bytes) = PipelineGroup::new(agg_funcs, key_len);
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
fn merge_map(global: &mut PipelineMap, incoming: PipelineMap) -> Result<(), ExecError> {
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

/// Merges two copies of one group in final-worker arrival order.
fn merge_groups(dst: &mut PipelineGroup, src: PipelineGroup) -> Result<(), ExecError> {
    for (c, state) in src.states.into_iter().enumerate() {
        merge_state(&mut dst.states[c], state)?;
    }
    Ok(())
}

/// Go `MergePartialResult` for exactly the aggregate kinds eligibility lets
/// through: every arm folds EXACTLY (integer/decimal domain or order-free
/// comparison), so a merged result equals the serial accumulation bit for
/// bit. Any other pair is an eligibility-gate bug, not a value.
fn merge_state(dst: &mut AggState, mut src: AggState) -> Result<(), ExecError> {
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
        (Partial::SumReal(a), Partial::SumReal(b)) => {
            if let Some(value) = b {
                *a = Some(a.unwrap_or(0.0) + value);
            }
        }
        (Partial::FirstRow(slot), Partial::FirstRow(value)) => {
            if slot.is_none() {
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
            Partial::AvgReal {
                sum: dst_sum,
                count: dst_count,
            },
            Partial::AvgReal {
                sum: src_sum,
                count: src_count,
            },
        ) => {
            *dst_sum += src_sum;
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
        // Mixed Fast/materialized states arise only after an overflow
        // replay materialized BOTH sides into SumDecimal(Some); they take
        // the exact merge arm below. A lone mismatch is unreachable.
        (Partial::Bit { acc: dst_acc, op }, Partial::Bit { acc: src_acc, .. }) => match op {
            BitOp::And => *dst_acc &= src_acc,
            BitOp::Or => *dst_acc |= src_acc,
            BitOp::Xor => *dst_acc ^= src_acc,
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
            if src_count != 0 {
                if *dst_count == 0 {
                    *dst_count = src_count;
                    *dst_sum = src_sum;
                    *dst_variance = src_variance;
                } else {
                    // Go `calculateMerge` (`func_varpop.go`).
                    let src_count_f = src_count as f64;
                    let dst_count_f = *dst_count as f64;
                    let t = (src_count_f / dst_count_f) * *dst_sum - src_sum;
                    *dst_variance += src_variance
                        + ((dst_count_f / src_count_f) / (dst_count_f + src_count_f)) * t * t;
                    *dst_count = dst_count.wrapping_add(src_count);
                    *dst_sum += src_sum;
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
        ) => dst_values.extend(src_values),
        (Partial::JsonArrayAgg(dst_values, _), Partial::JsonArrayAgg(src_values, _)) => {
            dst_values.extend(src_values);
        }
        (Partial::JsonObjectAgg(dst_values, _, _), Partial::JsonObjectAgg(src_values, _, _)) => {
            // Go's merge overwrites duplicate keys with the incoming map.
            dst_values.extend(src_values);
        }
        (Partial::ApproxCountDistinct(dst_sketch), Partial::ApproxCountDistinct(src_sketch)) => {
            dst_sketch.merge(&src_sketch);
        }
        (
            Partial::ApproxPercentile {
                values: dst_values, ..
            },
            Partial::ApproxPercentile {
                values: src_values, ..
            },
        ) => dst_values.extend(src_values),
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
        );
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

        // Concurrency evidence: every lane received a share of the chunks
        // (round-robin over {chunks} >= lanes), and more than ONE partial-
        // worker thread executed. Both are impossible on the serial path.
        let (_p, _f, dispatched, threads) = info;
        let expected_chunks = data.len().div_ceil(CHUNK_SIZE);
        assert_eq!(dispatched, expected_chunks, "every chunk was folded");
        assert!(threads > 1, "multiple partial-worker threads ran");
    }

    #[test]
    fn single_chunk_pipeline_submits_only_one_partial_worker() {
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
        exec.close().unwrap();

        assert_eq!(rows.len(), 7);
        let (_partial, _final_, dispatched, threads) = info;
        assert_eq!(dispatched, 1);
        assert_eq!(
            threads, 1,
            "an idle lane must not consume a persistent worker-pool task"
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
        let data = (0..20_000).map(|value| (value, 1)).collect::<Vec<_>>();
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
        let rows = drain_rows(&mut exec);
        assert_eq!(rows.len(), data.len());
        assert!(exec.spill_times() > 0);
        exec.close().unwrap();
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
