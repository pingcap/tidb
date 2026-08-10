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

//! Turning one column's or one index's samples into a histogram and a TopN,
//! from `pkg/statistics/builder.go`'s `BuildHistAndTopN`, `buildHist`,
//! `pruneTopNItem` and `processTopNValue`.
//!
//! This is the WRITE half of [`crate::histogram`]: that module reads a
//! histogram somebody else built and estimates row counts from it, this one
//! builds the histogram `ANALYZE TABLE` stores.
//!
//! # Why the exact entrypoint owns a codec callback
//!
//! Go's builder holds a `getComparedBytes` closure that is `codec.EncodeKey`
//! for a column and the identity for an index, because an index sample's
//! value already *is* an index key. [`try_build_hist_and_topn_in_place`] keeps
//! that boundary caller-owned and fallible; [`build_hist_and_topn`] is only a
//! convenience adapter for samples that already carry their encoding.
//!
//! # What this owns, and what it does not
//!
//! Sampling is [`crate::row_sample_collector`]'s. Persistence -- the
//! per-bucket count *delta* `mysql.stats_buckets` stores, the blob a bound is
//! converted to -- belongs to the writer, because it is a property of the
//! table, not of the histogram. [`crate::histogram::Bucket::count`] below is
//! cumulative, exactly as Go's in-memory `Bucket.Count` is.

use std::{cmp::Ordering, sync::Arc};

use tidb_datatype::{Collation, Datum, DatumValueError};
use tidb_util::generic::BoundedMinHeap;
use tidb_util::memory::Tracker;

use crate::cmsketch::TopN;
use crate::correlation::calc_correlation;
use crate::go_stable_sort::go_stable_sort_by;
use crate::histogram::{Bucket, Histogram};

/// Go `topNPruningThreshold`: a singleton value is dropped once the heap
/// already holds a tenth of the TopN it was asked for.
const TOPN_PRUNING_THRESHOLD: usize = 10;

/// Go `bucketNDVDivisor`.
const BUCKET_NDV_DIVISOR: i64 = 2;

/// One sampled value, in both the form the builder compares and the form the
/// histogram stores.
///
/// `ordinal` is Go's `SampleItem.Ordinal`: the row's position in physical
/// scan order, which is the whole input to the column/handle correlation.
#[derive(Clone, Debug)]
pub struct SampleItem {
    /// The order-preserving encoding the builder compares and groups on:
    /// `codec.EncodeKey` for a column, the index key itself for an index.
    pub encoded: Vec<u8>,
    /// The value a bucket bound is stored from.
    pub value: Datum,
    /// The sample's position in physical scan order.
    pub ordinal: isize,
}

/// One column's or one index's samples, plus the whole-table facts the
/// builder scales them by -- Go's `statistics.SampleCollector` as
/// `BuildHistAndTopN` consumes it.
#[derive(Clone, Debug, Default)]
pub struct SampleCollector {
    /// Samples, in any order; the builder sorts them.
    pub samples: Vec<SampleItem>,
    /// Rows whose value was NULL. Counted over every scanned row, not just
    /// the sampled ones.
    pub null_count: i64,
    /// Non-NULL rows scanned.
    pub count: i64,
    /// The estimated distinct-value count, over every scanned row.
    pub ndv: i64,
    /// Go `SampleCollector.TotalSize`: the summed encoded size of every
    /// scanned non-NULL value, minus its flag byte.
    pub total_size: i64,
}

/// What one call to [`build_hist_and_topn`] produced.
#[derive(Clone, Debug)]
pub struct HistogramAndTopN {
    /// The histogram, with cumulative bucket counts.
    pub histogram: Histogram,
    /// The TopN, `None` when the builder collected none.
    pub topn: Option<TopN>,
}

/// Errors returned by Go-shaped fallible histogram entrypoints.
#[derive(Debug)]
pub enum HistogramBuildError<E> {
    /// `Datum.Compare` failed while sorting samples or extending a bucket.
    Compare(DatumValueError),
    /// The caller-owned column key encoder failed.
    Encode(E),
}

impl<E: std::fmt::Display> std::fmt::Display for HistogramBuildError<E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Compare(error) => std::fmt::Display::fmt(error, formatter),
            Self::Encode(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}

impl<E> std::error::Error for HistogramBuildError<E> where E: std::error::Error + 'static {}

/// Go's two-result `getComparedBytes`: an encoder can return allocated bytes
/// together with an error, and the builder accounts that allocation before
/// returning the error.
#[derive(Debug)]
pub struct ComparedBytesResult<E> {
    /// The encoded comparison key, including any partial result on failure.
    pub encoded: Vec<u8>,
    /// The codec or statement-context error, if one survived `HandleError`.
    pub error: Option<E>,
}

impl<E> ComparedBytesResult<E> {
    /// A successful encoding.
    #[must_use]
    pub fn success(encoded: Vec<u8>) -> Self {
        Self {
            encoded,
            error: None,
        }
    }

    /// A failed encoding that preserves Go's simultaneously returned bytes.
    #[must_use]
    pub fn failure(encoded: Vec<u8>, error: E) -> Self {
        Self {
            encoded,
            error: Some(error),
        }
    }
}

/// The two independent buffers used by `BuildHistAndTopN` and `buildHist`.
///
/// Keeping this as a public native primitive makes the Go threshold contract
/// testable without allocating a 100 MiB vector just to manufacture a
/// capacity. Dropping or explicitly flushing it performs Go's deferred final
/// `Consume(bufferedMemSize)` followed by `Release(bufferedReleaseSize)`.
pub struct BuilderMemoryBuffer {
    tracker: Option<Arc<Tracker>>,
    buffered_mem_size: i64,
    buffered_release_size: i64,
}

impl BuilderMemoryBuffer {
    /// Starts one Go-shaped pair of temporary-memory buffers.
    #[must_use]
    pub fn new(tracker: Option<Arc<Tracker>>) -> Self {
        Self {
            tracker,
            buffered_mem_size: 0,
            buffered_release_size: 0,
        }
    }

    /// Accounts one temporary allocation in both buffers.
    pub fn account_temporary(&mut self, bytes: i64) {
        if let Some(tracker) = &self.tracker {
            tracker.buffered_consume(&mut self.buffered_mem_size, bytes);
            tracker.buffered_release(&mut self.buffered_release_size, bytes);
        }
    }

    /// Bytes waiting for the next consume flush.
    #[must_use]
    pub const fn pending_consume(&self) -> i64 {
        self.buffered_mem_size
    }

    /// Bytes waiting for the next release flush.
    #[must_use]
    pub const fn pending_release(&self) -> i64 {
        self.buffered_release_size
    }

    /// Runs the deferred final flush now.
    pub fn flush(&mut self) {
        if let Some(tracker) = &self.tracker {
            tracker.consume(self.buffered_mem_size);
            self.buffered_mem_size = 0;
            tracker.release(self.buffered_release_size);
            self.buffered_release_size = 0;
        }
    }
}

impl Drop for BuilderMemoryBuffer {
    fn drop(&mut self) {
        self.flush();
    }
}

/// The knobs `ANALYZE ... WITH n BUCKETS, m TOPN` sets, plus the session
/// defaults they are compared against.
///
/// Go asks `isAnalyzeDefaultValue(numTopN, vardef.AnalyzeDefaultNumTopN)`
/// before it prunes anything: a user who named a TopN size meant it, and the
/// heuristics that would silently return fewer entries are switched off. The
/// defaults therefore have to travel with the values rather than be assumed.
#[derive(Clone, Copy, Debug)]
pub struct BuildOptions {
    /// `WITH n BUCKETS`, or the session default.
    pub num_buckets: isize,
    /// `WITH m TOPN`, or the session default.
    pub num_topn: isize,
    /// `tidb_analyze_default_num_buckets`.
    pub default_num_buckets: u64,
    /// `tidb_analyze_default_num_topn`.
    pub default_num_topn: u64,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            num_buckets: crate::constants::DEFAULT_HISTOGRAM_BUCKETS as isize,
            num_topn: crate::constants::DEFAULT_TOP_N_VALUE as isize,
            default_num_buckets: crate::constants::DEFAULT_HISTOGRAM_BUCKETS as u64,
            default_num_topn: crate::constants::DEFAULT_TOP_N_VALUE as u64,
        }
    }
}

/// Go `TopNWithRange`: a TopN candidate plus the run of the sorted sample
/// array it occupies, so the histogram pass can skip exactly those rows
/// without searching for them.
#[derive(Clone, Debug)]
struct TopNWithRange {
    encoded: Vec<u8>,
    count: u64,
    start_idx: i64,
    end_idx: i64,
}

/// Go `SequentialRangeChecker`.
///
/// The histogram pass walks sample indices in order, so the ranges it must
/// skip can be walked in order too; this is that one shared cursor rather
/// than a per-index search.
pub struct SequentialRangeChecker {
    ranges: Vec<(i64, i64)>,
    current: usize,
}

impl SequentialRangeChecker {
    /// Creates the source checker from inclusive `(start, end)` ranges and
    /// sorts an unsorted input by its start index.
    #[must_use]
    pub fn from_ranges(ranges: &[(i64, i64)]) -> Self {
        let mut ranges = ranges.to_vec();
        Self::from_ranges_in_place(&mut ranges)
    }

    /// Go `NewSequentialRangeChecker` sorts the caller's slice in place.
    #[must_use]
    pub fn from_ranges_in_place(ranges: &mut [(i64, i64)]) -> Self {
        ranges.sort_by_key(|(start, _)| *start);
        Self {
            ranges: ranges.to_vec(),
            current: 0,
        }
    }

    fn new(ranges: &[TopNWithRange]) -> Self {
        let ranges: Vec<(i64, i64)> = ranges
            .iter()
            .map(|item| (item.start_idx, item.end_idx))
            .collect();
        Self::from_ranges(&ranges)
    }

    /// Go `IsIndexInTopNRange`. Calls are intentionally stateful and assume
    /// sequential indices, so querying an earlier completed range stays false.
    pub fn is_index_in_topn_range(&mut self, idx: i64) -> bool {
        self.contains(idx)
    }

    fn contains(&mut self, idx: i64) -> bool {
        while self.current < self.ranges.len() && idx > self.ranges[self.current].1 {
            self.current += 1;
        }
        match self.ranges.get(self.current) {
            Some((start, end)) => idx >= *start && idx <= *end,
            None => false,
        }
    }
}

/// Go `isAnalyzeDefaultValue`.
const fn is_analyze_default_value(value: isize, default_value: u64) -> bool {
    value >= 0 && value as u64 == default_value
}

/// What `processTopNValue` weighs a candidate against: the size asked for,
/// whether the user asking for it switched the heuristics off, and how much
/// of the table one sampled row stands for.
struct TopNPolicy {
    num_topn: isize,
    allow_pruning: bool,
    sample_factor: f64,
}

type TopNHeap = BoundedMinHeap<TopNWithRange, fn(&TopNWithRange, &TopNWithRange) -> Ordering>;

fn compare_topn(left: &TopNWithRange, right: &TopNWithRange) -> Ordering {
    left.count.cmp(&right.count)
}

/// Go `processTopNValue`.
fn process_topn_value(
    heap: &mut TopNHeap,
    candidate: TopNWithRange,
    policy: &TopNPolicy,
    last_value: bool,
) {
    // A value seen exactly once is not evidence of skew: while sampling it is
    // as likely to be a common value the sample nearly missed as a rare one.
    // Go drops it once the heap holds a tenth of its entries, or whenever the
    // sample is only a fraction of the table.
    if !last_value
        && candidate.count == 1
        && policy.allow_pruning
        && (heap.len() as isize >= policy.num_topn / TOPN_PRUNING_THRESHOLD as isize
            || policy.sample_factor > 1.0)
    {
        return;
    }
    heap.add(candidate);
}

/// Go `pruneTopNItem`.
///
/// Walks the TopN from its least common entry and keeps it only when its
/// sampled count beats what a value *outside* the list would be expected to
/// show -- a continuity-corrected Wald bound over the hypergeometric variance
/// of sampling without replacement. An entry that does not beat it carries no
/// information the histogram does not already carry, and storing it would
/// only make the estimator trust a number the sample cannot support.
fn prune_topn_item(
    mut topns: Vec<TopNWithRange>,
    ndv: i64,
    null_count: i64,
    sample_rows: i64,
    total_rows: i64,
) -> Vec<TopNWithRange> {
    if total_rows <= 1 || topns.len() as i64 >= ndv || topns.len() <= 1 {
        return topns;
    }
    let mut sum_count: u64 = topns[..topns.len() - 1].iter().map(|item| item.count).sum();
    let mut topn_num = topns.len();
    while topn_num > 0 {
        let mut selectivity =
            1.0 - sum_count as f64 / sample_rows as f64 - null_count as f64 / total_rows as f64;
        selectivity = selectivity.clamp(0.0, 1.0);
        let other_ndv = ndv as f64 - (topn_num as f64 - 1.0);
        if other_ndv > 1.0 {
            selectivity /= other_ndv;
        }
        let total_rows_n = total_rows as f64;
        let sample_n = sample_rows as f64;
        let expected = total_rows_n * topns[topn_num - 1].count as f64 / sample_n;
        let variance = sample_n * expected * (total_rows_n - expected) * (total_rows_n - sample_n)
            / (total_rows_n * total_rows_n * (total_rows_n - 1.0));
        let stddev = variance.sqrt();
        if topns[topn_num - 1].count as f64 > selectivity * sample_n + 2.0 * stddev + 0.5 {
            break;
        }
        topn_num -= 1;
        if topn_num == 0 {
            break;
        }
        sum_count -= topns[topn_num - 1].count;
    }
    topns.truncate(topn_num);
    topns
}

/// Builds one column's or one index's histogram and TopN, Go
/// `BuildHistAndTopN`.
///
/// `is_column` is Go's `isColumn`: here it decides only whether the
/// physical/logical correlation is computed, because the value encoding the
/// caller already performed is the other half of that flag.
pub fn build_hist_and_topn(
    id: i64,
    collector: &SampleCollector,
    options: BuildOptions,
    is_column: bool,
) -> HistogramAndTopN {
    try_build_hist_and_topn(id, collector, options, is_column, |sample, _| {
        Ok::<_, std::convert::Infallible>(sample.encoded.clone())
    })
    .expect("pre-encoded samples must remain comparable")
}

/// Value-oriented convenience around [`try_build_hist_and_topn_in_place`].
///
/// Go sorts `collector.Samples` itself. This adapter deliberately clones for
/// callers that only need the value; use the in-place entrypoint when the Go
/// receiver mutation is observable.
pub fn try_build_hist_and_topn<E>(
    id: i64,
    collector: &SampleCollector,
    options: BuildOptions,
    is_column: bool,
    mut compared_bytes: impl FnMut(&SampleItem, bool) -> Result<Vec<u8>, E>,
) -> Result<HistogramAndTopN, HistogramBuildError<E>> {
    let mut collector = collector.clone();
    try_build_hist_and_topn_in_place(
        id,
        &mut collector,
        options,
        is_column,
        move |sample, is_column| match compared_bytes(sample, is_column) {
            Ok(encoded) => ComparedBytesResult::success(encoded),
            Err(error) => ComparedBytesResult::failure(Vec::new(), error),
        },
    )
}

/// Go `BuildHistAndTopN`, including its in-place stable sort, fallible datum
/// comparisons, fallible codec boundary, and optional temporary-memory
/// tracker.
pub fn try_build_hist_and_topn_in_place<E>(
    id: i64,
    collector: &mut SampleCollector,
    options: BuildOptions,
    is_column: bool,
    compared_bytes: impl FnMut(&SampleItem, bool) -> ComparedBytesResult<E>,
) -> Result<HistogramAndTopN, HistogramBuildError<E>> {
    try_build_hist_and_topn_tracked(id, collector, options, is_column, None, compared_bytes)
}

/// Tracker-bearing form of [`try_build_hist_and_topn_in_place`].
pub fn try_build_hist_and_topn_tracked<E>(
    id: i64,
    collector: &mut SampleCollector,
    options: BuildOptions,
    is_column: bool,
    tracker: Option<Arc<Tracker>>,
    mut compared_bytes: impl FnMut(&SampleItem, bool) -> ComparedBytesResult<E>,
) -> Result<HistogramAndTopN, HistogramBuildError<E>> {
    let mut outer_memory = BuilderMemoryBuffer::new(tracker.clone());
    let count = collector.count;
    let null_count = collector.null_count;
    let ndv = collector.ndv.min(count);
    let mut histogram = Histogram {
        id,
        ndv,
        null_count,
        tot_col_size: collector.total_size,
        ..Histogram::default()
    };
    if count == 0 || collector.samples.is_empty() || ndv == 0 {
        return Ok(HistogramAndTopN {
            histogram,
            topn: None,
        });
    }

    // Go sorts before `NewHistogram`; a comparison error therefore wins over
    // the negative-capacity panic and the caller sees the mutation. The
    // constructor allocates both the bounds chunk and `Buckets` with
    // `numBuckets`, so a negative value cannot reach Go's later `<= 0`
    // return. Only zero reaches that branch.
    sort_builder_samples(&mut collector.samples).map_err(HistogramBuildError::Compare)?;
    assert!(
        options.num_buckets >= 0,
        "histogram bucket count cannot be negative"
    );
    let samples = &collector.samples;

    let sample_num = samples.len() as i64;
    let sample_factor = count as f64 / sample_num as f64;
    let num_topn = options.num_topn;
    let allow_pruning = is_analyze_default_value(num_topn, options.default_num_topn);
    let policy = TopNPolicy {
        num_topn,
        allow_pruning,
        sample_factor,
    };

    // Step 1: the TopN candidates, and the sorted-sample run each occupies.
    assert!(num_topn >= 0, "maxSize cannot be negative");
    let mut heap = BoundedMinHeap::new(num_topn as usize, compare_topn as _);
    let first_encoding = compared_bytes(&samples[0], is_column);
    if is_column {
        outer_memory.account_temporary(
            i64::try_from(first_encoding.encoded.capacity()).unwrap_or(i64::MAX),
        );
    }
    if let Some(error) = first_encoding.error {
        return Err(HistogramBuildError::Encode(error));
    }
    let mut cur = first_encoding.encoded;
    let mut cur_cnt = 0_f64;
    let mut cur_start_idx = 0_i64;
    let mut sample_ndv = 1_i64;
    let mut corr_xy_sum = 0_f64;

    for i in 0..sample_num {
        let sample = &samples[i as usize];
        if is_column {
            corr_xy_sum += i as f64 * sample.ordinal as f64;
        }
        if num_topn == 0 {
            continue;
        }
        let sample_encoding = compared_bytes(sample, is_column);
        if is_column {
            outer_memory.account_temporary(
                i64::try_from(sample_encoding.encoded.capacity()).unwrap_or(i64::MAX),
            );
        }
        if let Some(error) = sample_encoding.error {
            return Err(HistogramBuildError::Encode(error));
        }
        let sample_bytes = sample_encoding.encoded;
        if cur == sample_bytes {
            cur_cnt += 1.0;
            continue;
        }
        sample_ndv += 1;
        process_topn_value(
            &mut heap,
            TopNWithRange {
                encoded: cur.clone(),
                count: cur_cnt as u64,
                start_idx: cur_start_idx,
                end_idx: i - 1,
            },
            &policy,
            false,
        );
        cur = sample_bytes;
        cur_cnt = 1.0;
        cur_start_idx = i;
    }

    if is_column {
        histogram.correlation = calc_correlation(sample_num, corr_xy_sum);
    }

    if num_topn != 0 && (!allow_pruning || (sample_factor <= 1.0 || cur_cnt > 1.0)) {
        process_topn_value(
            &mut heap,
            TopNWithRange {
                encoded: cur.clone(),
                count: cur_cnt as u64,
                start_idx: cur_start_idx,
                end_idx: sample_num - 1,
            },
            &policy,
            true,
        );
    }

    let mut pruned = heap.to_sorted_slice();
    if allow_pruning {
        pruned = prune_topn_item(pruned, ndv, null_count, sample_num, count);
        // A TopN that swallowed the whole sample leaves no buckets at all,
        // and its length is then a claim about the column's NDV that the
        // sample cannot support. Trimming it is what lets the histogram
        // describe the values the sample never saw.
        if sample_ndv > 1
            && sample_factor > 1.0
            && ndv > sample_ndv
            && pruned.len() as i64 >= sample_ndv
        {
            let keep = usize::try_from((sample_ndv - 1).max(1)).unwrap_or(usize::MAX);
            pruned.truncate(keep);
        }
    }

    let len_topn = pruned.len() as i64;
    let have_all_ndv = sample_ndv == len_topn && len_topn > 0;

    // Step 2: the collected counts are sample counts; the stored ones are
    // table counts.
    let mut topn_total_count = 0_u64;
    let mut topn_sample_count = 0_i64;
    let mut topn = TopN::new(pruned.len());
    for item in &pruned {
        topn_sample_count += item.count as i64;
        let scaled = (item.count as f64 * sample_factor) as u64;
        topn_total_count += scaled;
        topn.append(&item.encoded, scaled);
    }

    if have_all_ndv || options.num_buckets == 0 {
        return Ok(HistogramAndTopN {
            histogram,
            topn: Some(topn),
        });
    }

    // Step 3: the histogram over what the TopN left behind.
    let samples_excluding_topn = sample_num - topn_sample_count;
    if samples_excluding_topn > 0 {
        let remaining_ndv = ndv - len_topn;
        let mut num_buckets = options.num_buckets as i64;
        // Nothing was pruned away, so there is no skew left for the buckets
        // to describe, and asking for 256 of them would only split a smooth
        // distribution into noise.
        if len_topn < num_topn as i64
            && is_analyze_default_value(options.num_buckets, options.default_num_buckets)
        {
            num_buckets = (remaining_ndv / BUCKET_NDV_DIVISOR).max(1).min(num_buckets);
        }
        let mut checker = SequentialRangeChecker::new(&pruned);
        build_hist(
            &mut histogram,
            samples,
            count - topn_total_count as i64,
            remaining_ndv,
            num_buckets,
            samples_excluding_topn,
            &mut checker,
            tracker,
        )
        .map_err(HistogramBuildError::Compare)?;
    }

    Ok(HistogramAndTopN {
        histogram,
        topn: Some(topn),
    })
}

/// Go `BuildColumnHist`, using the caller-supplied whole-table count, NDV,
/// and NULL count rather than the collector's FM-sketch summary.
pub fn build_column_histogram(
    id: i64,
    collector: &SampleCollector,
    num_buckets: i64,
    count: i64,
    ndv: i64,
    null_count: i64,
) -> Histogram {
    try_build_column_histogram(id, collector, num_buckets, count, ndv, null_count)
        .expect("pre-encoded samples must remain comparable")
}

/// Fallible Go `BuildColumnHist`, preserving both stable-sort and bucket
/// comparison errors.
pub fn try_build_column_histogram(
    id: i64,
    collector: &SampleCollector,
    num_buckets: i64,
    count: i64,
    ndv: i64,
    null_count: i64,
) -> Result<Histogram, DatumValueError> {
    let mut collector = collector.clone();
    try_build_column_histogram_in_place(id, &mut collector, num_buckets, count, ndv, null_count)
}

/// Exact Go `BuildColumnHist` receiver-mutation form: the collector's sample
/// slice is stably sorted in place before histogram allocation.
pub fn try_build_column_histogram_in_place(
    id: i64,
    collector: &mut SampleCollector,
    num_buckets: i64,
    count: i64,
    ndv: i64,
    null_count: i64,
) -> Result<Histogram, DatumValueError> {
    let ndv = ndv.min(count);
    let mut histogram = Histogram {
        id,
        ndv,
        null_count,
        tot_col_size: collector.total_size,
        ..Histogram::default()
    };
    if count == 0 || collector.samples.is_empty() {
        return Ok(histogram);
    }

    // This is the source allocation point: `NewHistogram` follows the stable
    // sort and panics for a negative bucket capacity, while zero remains a
    // valid capacity and lets `buildHist` form one unbounded bucket.
    sort_builder_samples(&mut collector.samples)?;
    assert!(
        num_buckets >= 0,
        "histogram bucket count cannot be negative"
    );
    let samples = &collector.samples;
    let mut checker = SequentialRangeChecker::from_ranges(&[]);
    build_hist(
        &mut histogram,
        samples,
        count,
        ndv,
        num_buckets,
        samples.len() as i64,
        &mut checker,
        None,
    )?;
    let corr_xy_sum = samples
        .iter()
        .enumerate()
        .map(|(position, sample)| position as f64 * sample.ordinal as f64)
        .sum();
    histogram.correlation = calc_correlation(samples.len() as i64, corr_xy_sum);
    Ok(histogram)
}

/// Go `BuildColumn`, forwarding the collector's whole-scan summary.
#[must_use]
pub fn build_column(id: i64, collector: &SampleCollector, num_buckets: i64) -> Histogram {
    build_column_histogram(
        id,
        collector,
        num_buckets,
        collector.count,
        collector.ndv,
        collector.null_count,
    )
}

/// Go `buildHist`.
///
/// Every count written here is cumulative, and every one is a sample count
/// scaled up by `sample_factor` -- the histogram describes the table, not the
/// sample.
#[allow(clippy::too_many_arguments)] // Retain Go buildHist's source-shaped argument boundary.
fn build_hist(
    histogram: &mut Histogram,
    samples: &[SampleItem],
    count: i64,
    ndv: i64,
    num_buckets: i64,
    sample_count_exclude_topn: i64,
    checker: &mut SequentialRangeChecker,
    tracker: Option<Arc<Tracker>>,
) -> Result<(), DatumValueError> {
    let sample_num = samples.len() as i64;
    let sample_factor = count as f64 / sample_count_exclude_topn as f64;
    // A value sampled once may well appear only once in the table, so the
    // first repeat recorded for a bucket is the conservative per-distinct-
    // value average rather than a whole sample's worth of rows.
    let ndv_factor = (count as f64 / ndv as f64).min(sample_factor);
    // The `+ sample_factor` is Go's: a bucket's count only ever grows in
    // whole `sample_factor` steps, so a limit that is not a multiple of one
    // would cut every bucket a step early and build too many.
    let values_per_bucket = count as f64 / num_buckets as f64 + sample_factor;

    let mut first_sample_idx = -1_i64;
    for i in 0..sample_num {
        if !checker.contains(i) {
            first_sample_idx = i;
            break;
        }
    }
    if first_sample_idx == -1 {
        return Ok(());
    }

    let first = &samples[first_sample_idx as usize];
    histogram.buckets.push(Bucket {
        count: sample_factor as i64,
        repeat: ndv_factor as i64,
        // Go's sampling path calls `AppendBucket`, which is
        // `AppendBucketWithNDV(.., ndv = 0)`; only the incremental
        // `SortedBuilder` records a per-bucket NDV.
        ndv: 0,
        lower_bound: first.value.clone(),
        upper_bound: first.value.clone(),
    });
    let mut memory = BuilderMemoryBuffer::new(tracker);
    let mut bucket_idx = 0_usize;
    let mut last_count = 0_i64;
    let mut processed_count = 1_i64;

    for i in (first_sample_idx + 1)..sample_num {
        if checker.contains(i) {
            continue;
        }
        processed_count += 1;
        let sample = &samples[i as usize];
        memory.account_temporary(
            i64::try_from(
                histogram.buckets[bucket_idx]
                    .upper_bound
                    .estimated_mem_usage(),
            )
            .unwrap_or(i64::MAX),
        );
        let total_count = processed_count as f64 * sample_factor;
        if histogram.buckets[bucket_idx]
            .upper_bound
            .compare(&sample.value, Collation::Binary)?
            == std::cmp::Ordering::Equal
        {
            // One value never spans two buckets, whatever that does to the
            // bucket's size: an estimator that found half a value's rows in
            // one bucket and half in the next would double-count the
            // boundary.
            histogram.buckets[bucket_idx].count = total_count as i64;
            if histogram.buckets[bucket_idx].repeat == ndv_factor as i64 {
                // The value has now been seen exactly twice.
                histogram.buckets[bucket_idx].repeat = (2.0 * sample_factor) as i64;
            } else {
                histogram.buckets[bucket_idx].repeat += sample_factor as i64;
            }
        } else if total_count - last_count as f64 <= values_per_bucket {
            let bucket = &mut histogram.buckets[bucket_idx];
            bucket.upper_bound = sample.value.clone();
            bucket.count = total_count as i64;
            bucket.repeat = ndv_factor as i64;
        } else {
            last_count = histogram.buckets[bucket_idx].count;
            bucket_idx += 1;
            histogram.buckets.push(Bucket {
                count: total_count as i64,
                repeat: ndv_factor as i64,
                ndv: 0,
                lower_bound: sample.value.clone(),
                upper_bound: sample.value.clone(),
            });
        }
    }
    Ok(())
}

fn sort_builder_samples(samples: &mut [SampleItem]) -> Result<(), DatumValueError> {
    let mut error = None;
    go_stable_sort_by(samples, |left, right| {
        match left.value.compare(&right.value, Collation::Binary) {
            Ok(ordering) => {
                error = None;
                ordering
            }
            Err(found) => {
                error = Some(found);
                std::cmp::Ordering::Less
            }
        }
    });
    error.map_or(Ok(()), Err)
}
