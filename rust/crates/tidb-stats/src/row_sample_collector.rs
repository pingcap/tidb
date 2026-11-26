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
//! table's row count ([`get_adjusted_sample_rate`]). Both are ported, because
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
//! smaller sample is a wrong histogram. [`RowSampleCollector::from_proto`]
//! consumes into that tracker as Go's `FromProto` does; the collector itself
//! holds no bound, and Go's default (`-1`) is no bound at all.
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

use std::borrow::Cow;

use tidb_datatype::Datum;
use tidb_txnkv::Handle;
use tidb_fastrand as fastrand;

use crate::cmsketch::hash_bytes;
use crate::fmsketch::{FmSketch, MAX_SKETCH_SIZE};
use crate::{fm_sketch_from_proto, fm_sketch_to_proto, FmSketchProto};

/// Go `config.DefRowsForSampleRate`: roughly how many rows an `ANALYZE` aims
/// to look at.
pub const DEF_ROWS_FOR_SAMPLE_RATE: f64 = 110_000.0;

/// One scanned row's contribution to one collector slot.
#[derive(Clone, Copy)]
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

/// A slot contribution whose encoded bytes have already been hashed.
#[derive(Clone, Copy)]
pub struct HashedSlotValue {
    /// The first Murmur3 lane of the slot's compact-encoded values.
    pub hash: u64,
    /// The original payload size in bytes, as in [`SlotValue::size`].
    pub size: i64,
    /// Whether this slot contributes only to its null count.
    pub is_null: bool,
}

impl SlotValue<'_> {
    pub fn into_hashed(self) -> HashedSlotValue {
        // Go's `FMSketch.InsertValue` takes the first lane of the Murmur3 sum.
        HashedSlotValue {
            hash: if self.is_null {
                0
            } else {
                hash_bytes(self.encoded_value).h1
            },
            size: self.size,
            is_null: self.is_null,
        }
    }
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
    pub fn new_row_sample_collector(max_sample_size: usize, sample_rate: f64) -> Option<Self> {
        if max_sample_size > 0 {
            return Some(Self::Reservoir { max_sample_size });
        }
        if sample_rate > 0.0 {
            return Some(Self::Bernoulli { sample_rate });
        }
        None
    }
}

/// Go `getAdjustedSampleRate`, reduced to its two count inputs.
///
/// `realtime_count` is `mysql.stats_meta.count` for the table, `None` when it
/// has no row there at all; `approximate_count` is PD's region-derived
/// estimate, `None` when PD/storage could not provide one. The branches are
/// Go's, in Go's order.
pub fn get_adjusted_sample_rate(realtime_count: Option<i64>, approximate_count: Option<f64>) -> f64 {
    if realtime_count.is_none() && approximate_count.is_none() {
        return 0.001;
    }
    let realtime = realtime_count.unwrap_or(0);
    if realtime == 0 && approximate_count.is_none() {
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
    if realtime == 0 {
        return 1.0;
    }
    1.0_f64.min(DEF_ROWS_FOR_SAMPLE_RATE / realtime as f64)
}

#[cfg(test)]
mod adjusted_sample_rate_tests {
    use super::get_adjusted_sample_rate;

    #[test]
    fn follows_go_row_count_fallbacks_and_stale_stats_rule() {
        assert_eq!(get_adjusted_sample_rate(None, None), 0.001);
        assert_eq!(get_adjusted_sample_rate(Some(0), None), 1.0);
        assert_eq!(get_adjusted_sample_rate(Some(10_000), Some(1_000_000.0)), 0.15);
        assert_eq!(get_adjusted_sample_rate(Some(10_000), Some(40_000.0)), 1.0);
    }
}


/// What one collector slot counted over the whole scan.
#[derive(Clone, Debug, Default)]
pub struct SlotStats {
    /// Rows whose value was NULL.
    pub null_count: i64,
    /// Summed encoded payload size of the non-NULL values.
    pub total_size: i64,
    /// The FM sketch's distinct-value estimate.
    pub ndv: i64,
    /// The full FM sketch retained for partition statistics/global merging.
    pub fm_sketch: Option<FmSketch>,
}

/// One row the sampler kept.
#[derive(Clone, Debug)]
pub struct SampledRow {
    /// The analyzed columns' values, in the caller's column order.
    pub columns: Vec<Datum>,
    /// The executor-built row handle used to restore physical row order.
    pub handle: Handle,
    /// The row's position after the executor sorts samples by KV handle.
    pub ordinal: isize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowSampleProto<B = Vec<u8>> {
    pub row: Vec<B>,
    pub weight: i64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RowSampleCollectorProto<B = Vec<u8>> {
    pub samples: Vec<RowSampleProto<B>>,
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
    /// Go `baseCollector.MemSize`, maintained by protobuf restoration and
    /// collector merge; the analyze executor consumes it into its tracker.
    go_mem_size: i64,
}

impl RowSampleCollector {
    /// Go's sampling builder replaces prefix/virtual index NDV and NULL
    /// facts with their independent index scan, retaining the shared samples.
    pub fn replace_special_index_stats(&mut self, slot: usize, null_count: i64, sketch: FmSketch) {
        self.null_counts[slot] = null_count;
        self.sketches[slot] = Some(sketch);
    }

    /// Creates a collector with `slot_count` slots under `policy`.
    #[must_use]
    pub fn new(slot_count: usize, policy: SamplePolicy) -> Self {
        Self::with_fm_sketch_size(slot_count, policy, MAX_SKETCH_SIZE)
    }

    /// Creates a collector with Go `RowSampleBuilder.MaxFMSketchSize`.
    #[must_use]
    pub fn with_fm_sketch_size(slot_count: usize, policy: SamplePolicy, max_fm_sketch_size: usize) -> Self {
        Self {
            policy,
            count: 0,
            null_counts: vec![0; slot_count],
            total_sizes: vec![0; slot_count],
            sketches: (0..slot_count)
                .map(|_| Some(FmSketch::new(max_fm_sketch_size)))
                .collect(),
            samples: Vec::new(),
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
    /// sketches) are recorded before the sampling decision. Go's TiKV-side
    /// collector tracks no memory; the TiDB side consumes what it restores.
    ///
    /// # Panics
    ///
    /// Panics when the row does not carry one value per slot: a short row
    /// would silently stop counting the slots it omits, which is a wrong
    /// answer rather than an error.
    pub fn collect(&mut self, row: &ScannedRow<'_>) {
        let mut rng = GlobalRowSampleRng;
        self.collect_with_rng(row, &mut rng)
    }

    /// Offers one row using the caller-owned Go-compatible random stream.
    pub fn collect_with_rng(
        &mut self,
        row: &ScannedRow<'_>,
        rng: &mut impl RowSampleRng,
    ) {
        self.collect_row(row.slots, || row.columns.to_vec(), rng);
    }

    /// Offers an owned row with the caller's Go-compatible random stream.
    pub fn collect_owned_with_rng(
        &mut self,
        columns: Vec<Datum>,
        slots: &[SlotValue<'_>],
        rng: &mut impl RowSampleRng,
    ) {
        self.collect_row(slots, || columns, rng);
    }

    fn collect_row(
        &mut self,
        slots: &[SlotValue<'_>],
        into_columns: impl FnOnce() -> Vec<Datum>,
        rng: &mut impl RowSampleRng,
    ) {
        let slots = slots.iter().copied().map(SlotValue::into_hashed);
        self.collect_hashed_with_rng(slots, into_columns, rng);
    }

    /// Records prehashed slot contributions and builds the row only when retained.
    pub fn collect_hashed(
        &mut self,
        slots: &[HashedSlotValue],
        into_columns: impl FnOnce() -> Vec<Datum>,
    ) {
        self.collect_hashed_with_rng(slots.iter().copied(), into_columns, &mut GlobalRowSampleRng);
    }

    fn collect_hashed_with_rng(
        &mut self,
        slots: impl ExactSizeIterator<Item = HashedSlotValue>,
        into_columns: impl FnOnce() -> Vec<Datum>,
        rng: &mut impl RowSampleRng,
    ) {
        assert_eq!(
            slots.len(),
            self.null_counts.len(),
            "a scanned row must carry one value per collector slot"
        );
        self.count = self.count.wrapping_add(1);
        for (position, value) in slots.enumerate() {
            if value.is_null {
                self.null_counts[position] = self.null_counts[position].wrapping_add(1);
                continue;
            }
            self.total_sizes[position] = self.total_sizes[position].wrapping_add(value.size);
            self.sketches[position]
                .as_mut()
                .expect("RowSampleBuilder installs one FM sketch per slot")
                .insert_hash(value.hash);
        }
        self.sample_row(into_columns, rng);
    }

    /// Go `MergeCollector` for both reservoir and Bernoulli collectors.
    /// Whole-scan facts merge independently of which rows survive sampling.
    pub fn merge(&mut self, mut other: Self) {
        self.merge_facts(
            other.count,
            &other.sketches,
            &other.null_counts,
            &other.total_sizes,
        );

        // The dynamic Go interface dispatches on the destination receiver.
        // The source collector's policy/rate/capacity is never inspected.
        match self.policy {
            SamplePolicy::Reservoir { max_sample_size } => {
                let old_sample_count = self.samples.len() as i64;
                let source_sample_count = other.samples.len() as i64;
                let room = max_sample_size.saturating_sub(self.samples.len());
                if other.samples.len() <= room {
                    let fills_reservoir = room != 0 && other.samples.len() == room;
                    self.append_samples(&mut other.samples);
                    if fills_reservoir {
                        self.heapify();
                    }
                } else {
                    let mut incoming = other.samples.into_iter();
                    if room != 0 {
                        // Build the heap at the same point as Go's per-row
                        // merge, before testing any weight against its root.
                        self.samples.extend(incoming.by_ref().take(room));
                        self.heapify();
                    }
                    self.merge_reservoir_samples(incoming, std::convert::identity);
                }
                self.merge_reservoir_memory(old_sample_count, source_sample_count, other.go_mem_size);
            }
            SamplePolicy::Bernoulli { .. } => {
                self.append_samples(&mut other.samples);
                self.go_mem_size = self.go_mem_size.wrapping_add(other.go_mem_size);
            }
        }
    }

    fn merge_facts(
        &mut self,
        count: i64,
        sketches: &[Option<FmSketch>],
        null_counts: &[i64],
        total_sizes: &[i64],
    ) {
        self.count = self.count.wrapping_add(count);
        for (position, other_sketch) in sketches.iter().enumerate() {
            // Go indexes each destination field at every source position.
            if let (Some(sketch), Some(other_sketch)) = (&mut self.sketches[position], other_sketch)
            {
                sketch.merge(other_sketch);
            }
        }
        for (position, count) in null_counts.iter().enumerate() {
            self.null_counts[position] = self.null_counts[position].wrapping_add(*count);
        }
        for (position, size) in total_sizes.iter().enumerate() {
            self.total_sizes[position] = self.total_sizes[position].wrapping_add(*size);
        }
    }

    fn merge_reservoir_samples<R>(
        &mut self,
        incoming: impl Iterator<Item = (i64, R)>,
        into_columns: impl Fn(R) -> Vec<Datum>,
    ) {
        for (weight, row) in incoming {
            if self.samples[0].0 < weight {
                self.samples[0] = (weight, into_columns(row));
                self.sift_down(0);
            }
        }
    }

    fn merge_reservoir_memory(&mut self, old_count: i64, source_count: i64, source_size: i64) {
        let total = old_count.wrapping_add(source_count);
        self.go_mem_size = if total == 0 {
            0
        } else {
            self.go_mem_size
                .wrapping_add(source_size)
                .wrapping_mul(self.samples.len() as i64)
                / total
        };
    }

    fn append_samples(&mut self, incoming: &mut Vec<(i64, Vec<Datum>)>) {
        if self.samples.is_empty() {
            std::mem::swap(&mut self.samples, incoming);
        } else {
            self.samples.append(incoming);
        }
    }

    /// Go `DestroyAndPutToPool`: release only the FM-sketch slice.
    ///
    /// Despite its name, the source deliberately leaves rows, counts, sizes,
    /// memory accounting, and sampling configuration untouched.
    pub fn destroy(&mut self) {
        self.sketches.clear();
    }

    /// Go `baseCollector.ToProto` and `RowSamplesToProto`.
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
            fm_sketches: encode_sketches(&self.sketches),
            total_sizes: self.total_sizes.clone(),
        }
    }

    /// Serializes a finished collector, transferring its encoded sample bytes.
    pub fn into_proto(self) -> RowSampleCollectorProto {
        RowSampleCollectorProto {
            samples: self
                .samples
                .into_iter()
                .map(|(weight, columns)| RowSampleProto {
                    row: columns
                        .into_iter()
                        .map(|column| match column {
                            Datum::Bytes(bytes) => bytes,
                            other => sample_column_bytes(&other),
                        })
                        .collect(),
                    weight,
                })
                .collect(),
            null_counts: self.null_counts,
            count: self.count,
            fm_sketches: encode_sketches(&self.sketches),
            total_sizes: self.total_sizes,
        }
    }

    /// Go `baseCollector.FromProto`, including independent repeated fields:
    /// the sample items' mandatory memory is consumed into `mem_tracker`
    /// first (a fast failure when it alone exceeds the analyze quota), then
    /// each item's payload.
    pub fn from_proto(
        proto: &RowSampleCollectorProto,
        policy: SamplePolicy,
        mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
    ) -> Self {
        Self::restore_proto(Cow::Borrowed(proto), policy, mem_tracker)
    }

    /// Restores an owned protobuf collector, transferring its sample buffers.
    pub fn from_owned_proto(
        proto: RowSampleCollectorProto,
        policy: SamplePolicy,
        mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
    ) -> Self {
        Self::restore_proto(Cow::Owned(proto), policy, mem_tracker)
    }

    /// Whether every incoming sample must compete with the current minimum.
    #[must_use]
    pub fn reservoir_is_full(&self) -> bool {
        matches!(self.policy, SamplePolicy::Reservoir { max_sample_size }
            if max_sample_size != 0 && self.samples.len() >= max_sample_size)
    }

    /// Restores and merges one response while materializing only admitted rows
    /// when the receiver is a full reservoir.
    pub fn merge_owned_proto<B>(
        &mut self,
        proto: RowSampleCollectorProto<B>,
        mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
    )
    where
        B: AsRef<[u8]> + Into<Vec<u8>>,
    {
        let (sketches, source_size) = restore_proto_facts(&proto, mem_tracker);
        let original_size = self.go_mem_size;
        let into_columns = |row: Vec<B>| {
            row.into_iter()
                .map(|value| Datum::Bytes(value.into()))
                .collect()
        };
        let source_count = proto.samples.len() as i64;
        let incoming = proto
            .samples
            .into_iter()
            .map(|sample| (sample.weight, sample.row));
        if self.reservoir_is_full() {
            let old_count = self.samples.len() as i64;
            self.merge_facts(
                proto.count,
                &sketches,
                &proto.null_counts,
                &proto.total_sizes,
            );
            self.merge_reservoir_samples(incoming, into_columns);
            self.merge_reservoir_memory(old_count, source_count, source_size);
        } else {
            self.merge(Self {
                policy: self.policy,
                count: proto.count,
                null_counts: proto.null_counts,
                total_sizes: proto.total_sizes,
                sketches,
                samples: incoming.map(|(weight, row)| (weight, into_columns(row))).collect(),
                go_mem_size: source_size,
            });
        }
        mem_tracker.consume(self.go_mem_size - original_size - source_size);
    }

    fn restore_proto(
        proto: Cow<'_, RowSampleCollectorProto>,
        policy: SamplePolicy,
        mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
    ) -> Self {
        let (sketches, go_mem_size) = restore_proto_facts(&proto, mem_tracker);
        let count = proto.count;
        let (samples, null_counts, total_sizes) = match proto {
            Cow::Borrowed(proto) => {
                let samples = proto
                    .samples
                    .iter()
                    .map(|sample| {
                        (
                            sample.weight,
                            sample.row.iter().cloned().map(Datum::Bytes).collect(),
                        )
                    })
                    .collect();
                (samples, proto.null_counts.clone(), proto.total_sizes.clone())
            }
            Cow::Owned(proto) => {
                let samples = proto
                    .samples
                    .into_iter()
                    .map(|sample| (sample.weight, sample.row.into_iter().map(Datum::Bytes).collect()))
                    .collect();
                (samples, proto.null_counts, proto.total_sizes)
            }
        };
        Self {
            policy,
            count,
            null_counts,
            total_sizes,
            sketches,
            samples,
            go_mem_size,
        }
    }

    /// Go `baseCollector.MemSize` after protobuf restoration/merge.
    #[must_use]
    pub const fn go_mem_size(&self) -> i64 {
        self.go_mem_size
    }

    fn sample_row(&mut self, into_columns: impl FnOnce() -> Vec<Datum>, rng: &mut impl RowSampleRng) {
        match self.policy {
            SamplePolicy::Bernoulli { sample_rate } => {
                if rng.float64() > sample_rate {
                    return;
                }
                self.samples.push((0, into_columns()));
            }
            SamplePolicy::Reservoir { max_sample_size } => {
                // Go draws the weight before checking capacity. A direct
                // zero-capacity collector consequently indexes Samples[0]
                // below and panics; `NewRowSampleCollector` normally makes
                // that state unreachable by selecting no reservoir policy.
                let weight = rng.int63();
                if self.samples.len() < max_sample_size {
                    self.samples.push((weight, into_columns()));
                    if self.samples.len() == max_sample_size {
                        self.heapify();
                    }
                    return;
                }
                // Keeping the `max_sample_size` largest of uniformly drawn
                // weights is a uniform sample of the rows. An eviction leaves
                // the sample's size where it was.
                if self.samples[0].0 < weight {
                    self.samples[0] = (weight, into_columns());
                    self.sift_down(0);
                }
            }
        }
    }

    /// Answers the scan totals after reproducing the executor-owned handle
    /// phase: build every sampled row's KV handle, sort by `Handle.Compare`,
    /// then assign `SampleItem.Ordinal = j`.
    ///
    /// Handle construction can fail and belongs to the schema-aware caller;
    /// keeping it as a supplied typed key avoids pretending scan/heap order is
    /// handle order after distributed collectors merge.
    pub fn into_parts<E>(
        self,
        mut build_handle: impl FnMut(&[Datum]) -> Result<Handle, E>,
    ) -> Result<(i64, Vec<SlotStats>, Vec<SampledRow>), E> {
        self.into_decoded_parts(|columns| build_handle(columns))
    }

    /// Decodes merged wire samples and builds their handles before sorting.
    /// Counts, sketches and reservoir weights have already been merged; this
    /// phase must not feed the sampled rows through the sampler again.
    pub fn into_decoded_parts<E>(
        mut self,
        mut decode_and_build_handle: impl FnMut(&mut Vec<Datum>) -> Result<Handle, E>,
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
            .map(|((null_count, total_size), sketch)| {
                let ndv = sketch.as_ref().map_or(0, FmSketch::ndv);
                SlotStats {
                    null_count,
                    total_size,
                    ndv,
                    fm_sketch: sketch,
                }
            })
            .collect();
        let mut rows_with_handles = Vec::with_capacity(self.samples.len());
        for (_, mut columns) in self.samples.drain(..) {
            let handle = decode_and_build_handle(&mut columns)?;
            rows_with_handles.push((columns, handle));
        }
        rows_with_handles.sort_unstable_by(|left, right| {
            left.1.compare(&right.1).unwrap_or_else(|error| {
                panic!("Go Handle.Compare rejected mixed handle domains: {error}")
            })
        });
        let rows = rows_with_handles
            .into_iter()
            .enumerate()
            .map(|(position, (columns, handle))| SampledRow {
                columns,
                handle,
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

fn encode_sketches(sketches: &[Option<FmSketch>]) -> Vec<Option<FmSketchProto>> {
    sketches
        .iter()
        // `FMSketchToProto(nil)` returns a non-nil empty message.
        .map(|sketch| Some(fm_sketch_to_proto(sketch.as_ref())))
        .collect()
}

/// Go `FromProto`'s `initMemSize`: every empty sample item, its reference
/// and its empty datums, sized from the first row's width.
fn proto_mandatory_memory<B>(proto: &RowSampleCollectorProto<B>) -> i64 {
    const EMPTY_RESERVOIR_SAMPLE_ITEM_SIZE: i64 = 48;
    const REFERENCE_SIZE: i64 = 8;
    let Some(first) = proto.samples.first() else {
        return 0;
    };
    (proto.samples.len() as i64).wrapping_mul(
        (first.row.len() as i64)
            .wrapping_mul(tidb_datatype::EMPTY_DATUM_SIZE)
            .wrapping_add(EMPTY_RESERVOIR_SAMPLE_ITEM_SIZE)
            .wrapping_add(REFERENCE_SIZE),
    )
}

fn proto_row_payload<B: AsRef<[u8]>>(row: &[B]) -> i64 {
    // NewBytesDatum sets binary collation and Go's Unmarshal allocates
    // each byte slice with capacity equal to its wire length.
    let collation_bytes = tidb_charset::Collation::Binary.name().len() as i64;
    row.iter().fold(
        (row.len() as i64).wrapping_mul(collation_bytes),
        |sum, column| sum.wrapping_add(column.as_ref().len() as i64),
    )
}

fn restore_proto_facts<B: AsRef<[u8]>>(
    proto: &RowSampleCollectorProto<B>,
    mem_tracker: &std::sync::Arc<tidb_memory::Tracker>,
) -> (Vec<Option<FmSketch>>, i64) {
    let sketches = proto
        .fm_sketches
        .iter()
        .map(|sketch| fm_sketch_from_proto(sketch.as_ref()))
        .collect();
    let mut size = proto_mandatory_memory(proto);
    let mut buffered = 0;
    if !proto.samples.is_empty() {
        mem_tracker.consume(size);
    }
    for sample in &proto.samples {
        let payload = proto_row_payload(&sample.row);
        mem_tracker.buffered_consume(&mut buffered, payload);
        size = size.wrapping_add(payload);
    }
    mem_tracker.consume(buffered);
    (sketches, size)
}

#[cfg(test)]
mod special_index_tests {
    use super::*;
    #[test]
    fn special_index_overrides_only_ndv_and_nulls() {
        let mut collector = RowSampleCollector::new(1, SamplePolicy::new_row_sample_collector(10, 0.0).unwrap());
        let mut sketch = FmSketch::new(1000);
        sketch.insert_hash(17);
        collector.replace_special_index_stats(0, 2, sketch);
        let proto = collector.to_proto();
        assert_eq!(proto.null_counts, [2]);
        assert_eq!(proto.total_sizes, [0]);
        assert_eq!(proto.fm_sketches[0].as_ref().unwrap().hashset, [17]);
        assert_eq!(proto.count, 0);
    }
}
