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

//! Choosing which of a table's rows `ANALYZE TABLE` builds its histograms
//! from, and counting the facts that are true of *every* row while doing it.
//! From `pkg/statistics/row_sampler.go`.
//!
//! # Two samplers, and which one runs
//!
//! Go's `NewRowSampleCollector` picks between them by which knob the
//! statement set (`row_sampler.go:135`):
//!
//! * `WITH n SAMPLES` -> the **reservoir** collector, A-Res over a min-heap
//!   keyed on a per-row random weight, keeping the `n` largest;
//! * otherwise -> the **Bernoulli** collector, which keeps each row
//!   independently with probability `sample_rate`.
//!
//! Analyze v2's default is the Bernoulli one, because `AnalyzeOptionDefault`
//! sets `AnalyzeOptNumSamples` to `0` and lets the rate be derived from the
//! table's row count ([`adjusted_sample_rate`]). Both are ported, because
//! `WITH n SAMPLES` is a statement a user writes.
//!
//! # What is counted over every row, not every sample
//!
//! `count`, `null_count`, `total_sizes` and the FM sketches are accumulated
//! in `collectColumns`, which runs *before* the sampling decision
//! (`row_sampler.go:206`). That is the difference between an NDV that
//! describes the table and one that describes the sample, and getting it
//! backwards would understate every column's distinct-value count by exactly
//! the sample rate.
//!
//! # The memory bound
//!
//! Neither sampler bounds its kept rows by memory on its own: the reservoir
//! is bounded by `WITH n SAMPLES`, and the Bernoulli one only by
//! `sample_rate * table_rows`, so `WITH 1.0 SAMPLERATE` on a large table
//! materialises every row. Go bounds it the same way for both, from the
//! outside: the analyze executors consume every sample item into
//! `GlobalAnalyzeMemoryTracker`, whose limit is `tidb_mem_quota_analyze`
//! (`pkg/executor/select.go:141`). Exceeding it aborts the statement --
//! `globalPanicOnExceed` panics and `getAnalyzePanicErr` turns that into
//! `errAnalyzeOOM` -- rather than quietly sampling less, because a silently
//! smaller sample is a wrong histogram. [`SampleMemoryQuota`] is that bound,
//! and Go's default (`-1`) is no bound at all.
//!
//! # Slots, and why they are not just columns
//!
//! Go's collector counts one slot per analyzed column *plus* one per
//! multi-column "column group" -- which is how a composite index gets an NDV
//! over its whole key rather than over each column separately
//! (`collectColumnGroups`). A single-column group is deliberately skipped and
//! its facts copied from the column afterwards, because they are the same
//! facts. This type owns the slots; deciding what a slot means is the
//! caller's, exactly as it is `analyze_col_sampling.go`'s.

use tidb_datatype::Datum;
use tidb_util::fastrand;

use crate::cmsketch::hash_bytes;
use crate::fmsketch::{FmSketch, MAX_SKETCH_SIZE};
use crate::{fm_sketch_from_proto, fm_sketch_to_proto, FmSketchProto};

/// Go `config.DefRowsForSampleRate`: roughly how many rows an `ANALYZE` aims
/// to look at.
pub const DEF_ROWS_FOR_SAMPLE_RATE: f64 = 110_000.0;

/// One scanned row's contribution to one collector slot.
pub struct SlotValue<'a> {
    /// The `codec.EncodeValue` bytes the FM sketch hashes: one value's for a
    /// column slot, the whole group's concatenation for a group slot.
    pub encoded_value: &'a [u8],
    /// What this row adds to the slot's `tot_col_size`. Go stores the payload
    /// without its flag byte, so a caller passes `len(encoded) - 1` summed
    /// over the non-NULL members of the slot.
    pub size: i64,
    /// Whether this slot's value is NULL for this row. A NULL is counted and
    /// nothing else: no size, no sketch entry. Group slots are never NULL --
    /// Go hashes a group's datums whatever they are, and does not maintain a
    /// null count for one.
    pub is_null: bool,
}

/// One scanned row, as the collector consumes it.
pub struct ScannedRow<'a> {
    /// The analyzed columns' values, in the caller's column order. Kept whole
    /// when the row is sampled, because a column histogram and every index
    /// histogram are built from the same sampled rows.
    pub columns: &'a [Datum],
    /// One entry per collector slot, in slot order.
    pub slots: &'a [SlotValue<'a>],
}

/// Which rows to keep, Go's `NewRowSampleCollector` choice.
#[derive(Clone, Copy, Debug)]
pub enum SamplePolicy {
    /// `WITH n SAMPLES`: keep the `n` rows with the largest random weights.
    Reservoir {
        /// Go `MaxSampleSize`.
        max_sample_size: usize,
    },
    /// Keep each row independently with this probability.
    Bernoulli {
        /// Go `SampleRate`, in `[0, 1]`.
        sample_rate: f64,
    },
}

/// The only randomness Go's row samplers consume.
///
/// `RowSampleBuilder.Rng` is caller-owned, so deterministic/distributed
/// callers must be able to provide the same `math/rand.Rand.Int63` stream.
/// The default [`float64`](Self::float64) is Go's exact `Rand.Float64`
/// construction, including its retry when the conversion rounds to `1.0`.
pub trait RowSampleRng {
    /// Returns one non-negative 63-bit word, as Go `Rand.Int63` does.
    fn int63(&mut self) -> i64;

    /// Returns Go `Rand.Float64` from this source's `Int63` stream.
    fn float64(&mut self) -> f64 {
        loop {
            let value = self.int63() as f64 / (1_u64 << 63) as f64;
            if value != 1.0 {
                return value;
            }
        }
    }
}

struct GlobalRowSampleRng;

impl RowSampleRng for GlobalRowSampleRng {
    fn int63(&mut self) -> i64 {
        fastrand::uint64_n(1 << 63) as i64
    }
}

impl SamplePolicy {
    /// Go `NewRowSampleCollector`: a positive `WITH n SAMPLES` wins, a
    /// positive rate is the fallback, and neither is Go's `nil` collector.
    #[must_use]
    pub fn choose(max_sample_size: usize, sample_rate: f64) -> Option<Self> {
        if max_sample_size > 0 {
            return Some(Self::Reservoir { max_sample_size });
        }
        if sample_rate > 0.0 {
            return Some(Self::Bernoulli { sample_rate });
        }
        None
    }
}

/// Go `getAdjustedSampleRate`, reduced to the inputs this node can answer
/// for.
///
/// `realtime_count` is `mysql.stats_meta.count` for the table, `None` when it
/// has no row there at all; `approximate_count` is PD's region-derived
/// estimate, `None` when it was not asked for. The branches are Go's, in Go's
/// order, and the one divergence is stated: Go's `statsTbl == nil && !hasPD`
/// branch returns `0.001`, but that branch describes a table whose
/// `mysql.stats_meta` row a Go DDL always creates. A table this node finds
/// with no row there is one nothing has ever counted, so it takes Go's
/// *empty-table* answer -- read all of it -- rather than sampling one row in
/// a thousand of a table that may hold three.
#[must_use]
pub fn adjusted_sample_rate(realtime_count: Option<i64>, approximate_count: Option<f64>) -> f64 {
    let realtime = realtime_count.unwrap_or(0);
    if realtime == 0 {
        return 1.0;
    }
    // Go's workaround for issue 29216: a `stats_meta` count far below what PD
    // sees is a stale count, and sampling by it would read a fraction of the
    // rows it believed it was reading.
    if let Some(approximate) = approximate_count {
        if (realtime as f64) * 5.0 < approximate {
            return 1.0_f64.min(150_000.0 / approximate);
        }
    }
    1.0_f64.min(DEF_ROWS_FOR_SAMPLE_RATE / realtime as f64)
}

/// Go's analyze memory quota, as one `ANALYZE` reads it.
///
/// `tidb_mem_quota_analyze` is a byte count whose default is `-1`
/// (`vardef.DefTiDBMemQuotaAnalyze`), which Go's `memory.Tracker` reads as
/// unlimited. Any value `<= 0` means the same here.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SampleMemoryQuota(Option<u64>);

impl SampleMemoryQuota {
    /// Go's default: no bound.
    #[must_use]
    pub const fn unlimited() -> Self {
        Self(None)
    }

    /// The quota as `tidb_mem_quota_analyze` states it: bytes, or `<= 0` for
    /// unlimited.
    #[must_use]
    pub const fn from_setting(bytes: i64) -> Self {
        if bytes <= 0 {
            return Self(None);
        }
        Self(Some(bytes.unsigned_abs()))
    }

    /// The byte limit, when there is one.
    #[must_use]
    pub const fn bytes(self) -> Option<u64> {
        self.0
    }
}

/// The sample outgrew [`SampleMemoryQuota`].
///
/// Go reports this as `errAnalyzeOOM` (`pkg/executor/analyze_utils.go:88`),
/// and its text is reproduced exactly: it is what a user sees, and it names
/// the knob that fixes it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SampleMemoryExceeded;

impl std::fmt::Display for SampleMemoryExceeded {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "analyze panic due to memory quota exceeds, please try with smaller \
             samplerate(refer to {}/count)",
            DEF_ROWS_FOR_SAMPLE_RATE as i64
        )
    }
}

impl std::error::Error for SampleMemoryExceeded {}

/// What one collector slot counted over the whole scan.
#[derive(Clone, Debug, Default)]
pub struct SlotStats {
    /// Rows whose value was NULL.
    pub null_count: i64,
    /// Summed encoded payload size of the non-NULL values.
    pub total_size: i64,
    /// The FM sketch's distinct-value estimate.
    pub ndv: i64,
}

/// One row the sampler kept.
#[derive(Clone, Debug)]
pub struct SampledRow {
    /// The analyzed columns' values, in the caller's column order.
    pub columns: Vec<Datum>,
    /// The row's position after the executor sorts samples by KV handle.
    pub ordinal: isize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowSampleProto {
    pub row: Vec<Vec<u8>>,
    pub weight: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowSampleCollectorProto {
    pub samples: Vec<RowSampleProto>,
    pub null_counts: Vec<i64>,
    pub count: i64,
    /// Go protobuf uses `[]*tipb.FMSketch`, so an individual repeated entry
    /// can be nil even though generated wire decoding normally creates it.
    pub fm_sketches: Vec<Option<FmSketchProto>>,
    pub total_sizes: Vec<i64>,
}

/// Go's `baseCollector` plus whichever of its two sampling policies is in
/// force.
///
/// The caller drives the scan and hands over one row at a time; nothing here
/// reads storage.
pub struct RowSampleCollector {
    policy: SamplePolicy,
    /// Rows scanned, sampled or not.
    count: i64,
    // These are deliberately independent vectors. Go `FromProto` assigns
    // each repeated protobuf field without requiring the lengths to match.
    null_counts: Vec<i64>,
    total_sizes: Vec<i64>,
    sketches: Vec<Option<FmSketch>>,
    /// The reservoir's min-heap on weight. The Bernoulli policy leaves every
    /// weight at zero and never evicts, exactly as Go's does.
    samples: Vec<(i64, Vec<Datum>)>,
    /// The bound the kept rows are held to, and what they have consumed of
    /// it. Go tracks the same quantity -- the sample items' datums and their
    /// payloads -- through `GlobalAnalyzeMemoryTracker`.
    quota: SampleMemoryQuota,
    consumed_bytes: u64,
    /// Go `baseCollector.MemSize`, maintained by protobuf restoration and
    /// collector merge independently of the external quota tracker.
    go_mem_size: i64,
}

impl RowSampleCollector {
    /// Creates a collector with `slot_count` slots under `policy`.
    #[must_use]
    pub fn new(slot_count: usize, policy: SamplePolicy) -> Self {
        Self::with_memory_quota(slot_count, policy, SampleMemoryQuota::unlimited())
    }

    /// Creates a collector whose kept rows are bounded by `quota`.
    #[must_use]
    pub fn with_memory_quota(
        slot_count: usize,
        policy: SamplePolicy,
        quota: SampleMemoryQuota,
    ) -> Self {
        Self::with_memory_quota_and_fm_sketch_size(slot_count, policy, quota, MAX_SKETCH_SIZE)
    }

    /// Creates a collector with Go `RowSampleBuilder.MaxFMSketchSize`.
    #[must_use]
    pub fn with_memory_quota_and_fm_sketch_size(
        slot_count: usize,
        policy: SamplePolicy,
        quota: SampleMemoryQuota,
        max_fm_sketch_size: usize,
    ) -> Self {
        Self {
            policy,
            count: 0,
            null_counts: vec![0; slot_count],
            total_sizes: vec![0; slot_count],
            sketches: (0..slot_count)
                .map(|_| Some(FmSketch::new(max_fm_sketch_size)))
                .collect(),
            samples: Vec::new(),
            quota,
            consumed_bytes: 0,
            go_mem_size: 0,
        }
    }

    /// Rows scanned so far.
    #[must_use]
    pub const fn count(&self) -> i64 {
        self.count
    }

    /// The configured Go `MaxFMSketchSize` for each present slot sketch.
    #[must_use]
    pub fn fm_sketch_max_sizes(&self) -> Vec<Option<usize>> {
        self.sketches
            .iter()
            .map(|sketch| sketch.as_ref().map(FmSketch::max_size))
            .collect()
    }

    /// Offers one scanned row.
    ///
    /// The whole-table facts (`count`, `null_count`, the sizes and the FM
    /// sketches) are recorded before the sampling decision and cost nothing
    /// per row, so only the *kept* rows are charged against the quota -- the
    /// same items Go's tracker consumes. An error means the statement is
    /// over `tidb_mem_quota_analyze` and must stop: continuing without the
    /// rows it could not keep would answer with a histogram built from a
    /// sample smaller than the one the rate describes.
    ///
    /// # Panics
    ///
    /// Panics when the row does not carry one value per slot: a short row
    /// would silently stop counting the slots it omits, which is a wrong
    /// answer rather than an error.
    pub fn collect(&mut self, row: &ScannedRow<'_>) -> Result<(), SampleMemoryExceeded> {
        let mut rng = GlobalRowSampleRng;
        self.collect_with_rng(row, &mut rng)
    }

    /// Offers one row using the caller-owned Go-compatible random stream.
    pub fn collect_with_rng(
        &mut self,
        row: &ScannedRow<'_>,
        rng: &mut impl RowSampleRng,
    ) -> Result<(), SampleMemoryExceeded> {
        assert_eq!(
            row.slots.len(),
            self.null_counts.len(),
            "a scanned row must carry one value per collector slot"
        );
        self.count = self.count.wrapping_add(1);
        for (position, value) in row.slots.iter().enumerate() {
            if value.is_null {
                self.null_counts[position] = self.null_counts[position].wrapping_add(1);
                continue;
            }
            self.total_sizes[position] = self.total_sizes[position].wrapping_add(value.size);
            // Go's `FMSketch.InsertValue` hashes `codec.EncodeValue` with
            // murmur3's 64-bit sum, which is the first lane of its 128-bit
            // one.
            self.sketches[position]
                .as_mut()
                .expect("RowSampleBuilder installs one FM sketch per slot")
                .insert_hash(hash_bytes(value.encoded_value).h1);
        }
        self.sample_row(row.columns, rng)
    }

    /// Go `MergeCollector` for both reservoir and Bernoulli collectors.
    /// Whole-scan facts merge independently of which rows survive sampling.
    pub fn merge(&mut self, mut other: Self) {
        self.count = self.count.wrapping_add(other.count);
        for (position, other_sketch) in other.sketches.iter().enumerate() {
            // Go indexes the destination with every source position: shorter
            // source arrays are valid prefixes; a longer source panics.
            if let (Some(sketch), Some(other_sketch)) = (&mut self.sketches[position], other_sketch)
            {
                sketch.merge(other_sketch);
            }
        }
        for (position, other_null_count) in other.null_counts.iter().enumerate() {
            self.null_counts[position] = self.null_counts[position].wrapping_add(*other_null_count);
        }
        for (position, other_total_size) in other.total_sizes.iter().enumerate() {
            self.total_sizes[position] = self.total_sizes[position].wrapping_add(*other_total_size);
        }

        // The dynamic Go interface dispatches on the destination receiver.
        // The source collector's policy/rate/capacity is never inspected.
        match self.policy {
            SamplePolicy::Reservoir { max_sample_size } => {
                let old_sample_count = self.samples.len() as i64;
                let source_sample_count = other.samples.len() as i64;
                for (weight, columns) in other.samples.drain(..) {
                    if self.samples.len() < max_sample_size {
                        self.samples.push((weight, columns));
                        if self.samples.len() == max_sample_size {
                            self.heapify();
                        }
                    } else if self.samples[0].0 < weight {
                        self.samples[0] = (weight, columns);
                        self.sift_down(0);
                    }
                }
                let total_sample_count = old_sample_count.wrapping_add(source_sample_count);
                self.go_mem_size = if total_sample_count == 0 {
                    0
                } else {
                    self.go_mem_size
                        .wrapping_add(other.go_mem_size)
                        .wrapping_mul(self.samples.len() as i64)
                        / total_sample_count
                };
            }
            SamplePolicy::Bernoulli { .. } => {
                for (weight, columns) in other.samples.drain(..) {
                    self.samples.push((weight, columns));
                }
                self.go_mem_size = self.go_mem_size.wrapping_add(other.go_mem_size);
            }
        }
        self.consumed_bytes = u64::try_from(self.go_mem_size).unwrap_or_default();
    }

    /// Go `DestroyAndPutToPool`: release only the FM-sketch slice.
    ///
    /// Despite its name, the source deliberately leaves rows, counts, sizes,
    /// memory accounting, and sampling configuration untouched.
    pub fn destroy(&mut self) {
        self.sketches.clear();
    }

    /// Go `baseCollector.ToProto` and `RowSamplesToProto`.
    #[must_use]
    pub fn to_proto(&self) -> RowSampleCollectorProto {
        RowSampleCollectorProto {
            samples: self
                .samples
                .iter()
                .map(|(weight, columns)| RowSampleProto {
                    row: columns.iter().map(sample_column_bytes).collect(),
                    weight: *weight,
                })
                .collect(),
            null_counts: self.null_counts.clone(),
            count: self.count,
            fm_sketches: self
                .sketches
                .iter()
                // `FMSketchToProto(nil)` returns a non-nil empty message.
                .map(|sketch| Some(fm_sketch_to_proto(sketch.as_ref())))
                .collect(),
            total_sizes: self.total_sizes.clone(),
        }
    }

    /// Go `baseCollector.FromProto`, including independent repeated fields.
    pub fn from_proto(
        proto: &RowSampleCollectorProto,
        policy: SamplePolicy,
        quota: SampleMemoryQuota,
    ) -> Result<Self, SampleMemoryExceeded> {
        let sketches: Vec<_> = proto
            .fm_sketches
            .iter()
            .map(|sketch| fm_sketch_from_proto(sketch.as_ref()))
            .collect();
        let samples = proto
            .samples
            .iter()
            .map(|sample| {
                (
                    sample.weight,
                    sample
                        .row
                        .iter()
                        .map(|column| Datum::Bytes(column.clone()))
                        .collect(),
                )
            })
            .collect();
        let memory_size = proto_memory_usage(proto);
        if quota
            .bytes()
            .is_some_and(|limit| memory_size as u64 > limit)
        {
            return Err(SampleMemoryExceeded);
        }
        Ok(Self {
            policy,
            count: proto.count,
            null_counts: proto.null_counts.clone(),
            total_sizes: proto.total_sizes.clone(),
            sketches,
            samples,
            quota,
            consumed_bytes: memory_size as u64,
            go_mem_size: memory_size,
        })
    }

    #[must_use]
    pub const fn consumed_memory_bytes(&self) -> u64 {
        self.consumed_bytes
    }

    /// Go `baseCollector.MemSize` after protobuf restoration/merge.
    #[must_use]
    pub const fn go_mem_size(&self) -> i64 {
        self.go_mem_size
    }

    /// The bytes one kept row costs.
    ///
    /// Go's `SampleItem` accounting (`pkg/statistics/row_sampler.go:77`) is
    /// the datum structs plus the bytes they point at, which is what a row
    /// held in the sample actually occupies.
    fn row_bytes(columns: &[Datum]) -> u64 {
        let structs = columns.len() as u64 * std::mem::size_of::<Datum>() as u64;
        let payload: u64 = columns
            .iter()
            .map(|column| match column {
                Datum::Bytes(bytes) => bytes.len() as u64,
                Datum::String(string) => string.bytes().len() as u64,
                _ => 0,
            })
            .sum();
        structs + payload
    }

    /// Charges one kept row, or reports the quota exhausted.
    fn charge(&mut self, columns: &[Datum]) -> Result<(), SampleMemoryExceeded> {
        let Some(limit) = self.quota.bytes() else {
            return Ok(());
        };
        self.consumed_bytes = self.consumed_bytes.saturating_add(Self::row_bytes(columns));
        if self.consumed_bytes > limit {
            return Err(SampleMemoryExceeded);
        }
        Ok(())
    }

    fn sample_row(
        &mut self,
        columns: &[Datum],
        rng: &mut impl RowSampleRng,
    ) -> Result<(), SampleMemoryExceeded> {
        match self.policy {
            SamplePolicy::Bernoulli { sample_rate } => {
                if rng.float64() > sample_rate {
                    return Ok(());
                }
                self.charge(columns)?;
                self.samples.push((0, columns.to_vec()));
            }
            SamplePolicy::Reservoir { max_sample_size } => {
                // Go draws the weight before checking capacity. A direct
                // zero-capacity collector consequently indexes Samples[0]
                // below and panics; `NewRowSampleCollector` normally makes
                // that state unreachable by selecting no reservoir policy.
                let weight = rng.int63();
                if self.samples.len() < max_sample_size {
                    self.charge(columns)?;
                    self.samples.push((weight, columns.to_vec()));
                    if self.samples.len() == max_sample_size {
                        self.heapify();
                    }
                    return Ok(());
                }
                // Keeping the `max_sample_size` largest of uniformly drawn
                // weights is a uniform sample of the rows. An eviction leaves
                // the sample's size where it was, so it is not charged again.
                if self.samples[0].0 < weight {
                    self.samples[0] = (weight, columns.to_vec());
                    self.sift_down(0);
                }
            }
        }
        Ok(())
    }

    /// Answers the scan totals after reproducing the executor-owned handle
    /// phase: build every sampled row's KV handle, sort by `Handle.Compare`,
    /// then assign `SampleItem.Ordinal = j`.
    ///
    /// Handle construction can fail and belongs to the schema-aware caller;
    /// keeping it as a supplied typed key avoids pretending scan/heap order is
    /// handle order after distributed collectors merge.
    pub fn into_parts<H, E>(
        mut self,
        mut build_handle: impl FnMut(&[Datum]) -> Result<H, E>,
        mut compare_handle: impl FnMut(&H, &H) -> std::cmp::Ordering,
    ) -> Result<(i64, Vec<SlotStats>, Vec<SampledRow>), E> {
        assert_eq!(
            self.null_counts.len(),
            self.total_sizes.len(),
            "building stats requires one null/size entry per slot"
        );
        assert_eq!(
            self.null_counts.len(),
            self.sketches.len(),
            "building stats requires one FM entry per slot"
        );
        let slots = self
            .null_counts
            .into_iter()
            .zip(self.total_sizes)
            .zip(self.sketches)
            .map(|((null_count, total_size), sketch)| SlotStats {
                null_count,
                total_size,
                ndv: sketch.as_ref().map_or(0, FmSketch::ndv),
            })
            .collect();
        let mut rows_with_handles = Vec::with_capacity(self.samples.len());
        for (_, columns) in self.samples.drain(..) {
            let handle = build_handle(&columns)?;
            rows_with_handles.push((columns, handle));
        }
        rows_with_handles.sort_unstable_by(|left, right| compare_handle(&left.1, &right.1));
        let rows = rows_with_handles
            .into_iter()
            .enumerate()
            .map(|(position, (columns, _))| SampledRow {
                columns,
                ordinal: position as isize,
            })
            .collect();
        Ok((self.count, slots, rows))
    }

    fn heapify(&mut self) {
        for index in (0..self.samples.len() / 2).rev() {
            self.sift_down(index);
        }
    }

    fn sift_down(&mut self, mut index: usize) {
        loop {
            let left = index * 2 + 1;
            if left >= self.samples.len() {
                break;
            }
            let right = left + 1;
            let child =
                if right < self.samples.len() && self.samples[right].0 < self.samples[left].0 {
                    right
                } else {
                    left
                };
            if self.samples[child].0 >= self.samples[index].0 {
                break;
            }
            self.samples.swap(index, child);
            index = child;
        }
    }
}

fn sample_column_bytes(column: &Datum) -> Vec<u8> {
    match column {
        Datum::Null => vec![tidb_codec::NIL_FLAG],
        Datum::Json(value) => value.value().to_vec(),
        Datum::VectorFloat32(value) => value.serialize(),
        value => value.go_bytes().to_vec(),
    }
}

fn proto_memory_usage(proto: &RowSampleCollectorProto) -> i64 {
    const EMPTY_DATUM_SIZE: i64 = 72;
    const EMPTY_RESERVOIR_SAMPLE_ITEM_SIZE: i64 = 48;
    const REFERENCE_SIZE: i64 = 8;
    if proto.samples.is_empty() {
        return 0;
    }
    let row_len = proto.samples[0].row.len() as i64;
    let mandatory = (proto.samples.len() as i64).wrapping_mul(
        row_len
            .wrapping_mul(EMPTY_DATUM_SIZE)
            .wrapping_add(EMPTY_RESERVOIR_SAMPLE_ITEM_SIZE)
            .wrapping_add(REFERENCE_SIZE),
    );
    proto.samples.iter().fold(mandatory, |total, sample| {
        sample.row.iter().fold(total, |total, column| {
            total.wrapping_add(column.len() as i64)
        })
    })
}
