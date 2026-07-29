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
    /// The row's position among the kept rows, in physical scan order.
    ///
    /// Go's `SampleItem.Ordinal` is the row's index in the collector's own
    /// sample slice, which is what `calcCorrelation` needs: it correlates
    /// against `0..sample_num`, not against the table's row IDs.
    pub ordinal: i64,
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
    slots: Vec<SlotStats>,
    sketches: Vec<FmSketch>,
    /// The reservoir's min-heap on weight. The Bernoulli policy leaves every
    /// weight at zero and never evicts, exactly as Go's does.
    samples: Vec<(i64, Vec<Datum>, i64)>,
    scanned_ordinal: i64,
    /// The bound the kept rows are held to, and what they have consumed of
    /// it. Go tracks the same quantity -- the sample items' datums and their
    /// payloads -- through `GlobalAnalyzeMemoryTracker`.
    quota: SampleMemoryQuota,
    consumed_bytes: u64,
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
        Self {
            policy,
            count: 0,
            slots: vec![SlotStats::default(); slot_count],
            sketches: (0..slot_count)
                .map(|_| FmSketch::new(MAX_SKETCH_SIZE))
                .collect(),
            samples: Vec::new(),
            scanned_ordinal: 0,
            quota,
            consumed_bytes: 0,
        }
    }

    /// Rows scanned so far.
    #[must_use]
    pub const fn count(&self) -> i64 {
        self.count
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
        assert_eq!(
            row.slots.len(),
            self.slots.len(),
            "a scanned row must carry one value per collector slot"
        );
        self.count += 1;
        for (position, value) in row.slots.iter().enumerate() {
            if value.is_null {
                self.slots[position].null_count += 1;
                continue;
            }
            self.slots[position].total_size += value.size;
            // Go's `FMSketch.InsertValue` hashes `codec.EncodeValue` with
            // murmur3's 64-bit sum, which is the first lane of its 128-bit
            // one.
            self.sketches[position].insert_hash(hash_bytes(value.encoded_value).h1);
        }
        let ordinal = self.scanned_ordinal;
        self.scanned_ordinal += 1;
        self.sample_row(row.columns, ordinal)
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

    fn sample_row(&mut self, columns: &[Datum], ordinal: i64) -> Result<(), SampleMemoryExceeded> {
        match self.policy {
            SamplePolicy::Bernoulli { sample_rate } => {
                if random_float64() > sample_rate {
                    return Ok(());
                }
                self.charge(columns)?;
                self.samples.push((0, columns.to_vec(), ordinal));
            }
            SamplePolicy::Reservoir { max_sample_size } => {
                if max_sample_size == 0 {
                    return Ok(());
                }
                let weight = random_int63();
                if self.samples.len() < max_sample_size {
                    self.charge(columns)?;
                    self.samples.push((weight, columns.to_vec(), ordinal));
                    if self.samples.len() == max_sample_size {
                        self.heapify();
                    }
                    return Ok(());
                }
                // Keeping the `max_sample_size` largest of uniformly drawn
                // weights is a uniform sample of the rows. An eviction leaves
                // the sample's size where it was, so it is not charged again.
                if self.samples[0].0 < weight {
                    self.samples[0] = (weight, columns.to_vec(), ordinal);
                    self.sift_down(0);
                }
            }
        }
        Ok(())
    }

    /// Answers the scan's totals: rows scanned, per-slot facts, and the kept
    /// rows in physical scan order with their ordinals renumbered from zero.
    ///
    /// The renumbering is what makes the correlation meaningful: Go's
    /// `calcCorrelation` compares a sample's position in the sample against
    /// its position in the sorted sample, and both run `0..sample_num`.
    #[must_use]
    pub fn into_parts(mut self) -> (i64, Vec<SlotStats>, Vec<SampledRow>) {
        for (slot, sketch) in self.slots.iter_mut().zip(&self.sketches) {
            slot.ndv = sketch.ndv();
        }
        self.samples.sort_by_key(|(_, _, ordinal)| *ordinal);
        let rows = self
            .samples
            .into_iter()
            .enumerate()
            .map(|(position, (_, columns, _))| SampledRow {
                columns,
                ordinal: position as i64,
            })
            .collect();
        (self.count, self.slots, rows)
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

/// Go `rand.Float64()`: `Int63n(1<<53) / (1<<53)`, the one construction that
/// cannot round up to exactly 1.0.
fn random_float64() -> f64 {
    (fastrand::uint64_n(1 << 53) as f64) / ((1_u64 << 53) as f64)
}

/// Go `rand.Int63()`.
fn random_int63() -> i64 {
    fastrand::uint64_n(1 << 63) as i64
}
