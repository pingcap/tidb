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
//! # Why the caller hands over encoded bytes
//!
//! Go's builder holds a `getComparedBytes` closure that is `codec.EncodeKey`
//! for a column and the identity for an index, because an index sample's
//! value already *is* an index key. Both branches need the datum codec, which
//! this crate deliberately does not depend on, so [`SampleItem`] carries the
//! encoded form next to the value and the builder never encodes anything.
//!
//! That also removes Go's one remaining comparison subtlety: the builder
//! compares bucket bounds with `Datum.Compare` under the binary collator and
//! sorts the samples with the same comparison. `codec.EncodeKey` is
//! order-preserving, so comparing the encoded bytes is that same order --
//! one comparison rule for sorting, TopN grouping and bucket extension alike,
//! rather than three chances to disagree.
//!
//! # What this owns, and what it does not
//!
//! Sampling is [`crate::row_sample_collector`]'s. Persistence -- the
//! per-bucket count *delta* `mysql.stats_buckets` stores, the blob a bound is
//! converted to -- belongs to the writer, because it is a property of the
//! table, not of the histogram. [`crate::histogram::Bucket::count`] below is
//! cumulative, exactly as Go's in-memory `Bucket.Count` is.

use tidb_datatype::Datum;

use crate::bounded_min_heap::BoundedMinHeap;
use crate::cmsketch::TopN;
use crate::correlation::calc_correlation;
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
    pub ordinal: i64,
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
    pub num_buckets: usize,
    /// `WITH m TOPN`, or the session default.
    pub num_topn: usize,
    /// `tidb_analyze_default_num_buckets`.
    pub default_num_buckets: usize,
    /// `tidb_analyze_default_num_topn`.
    pub default_num_topn: usize,
}

impl Default for BuildOptions {
    fn default() -> Self {
        Self {
            num_buckets: crate::constants::DEFAULT_HISTOGRAM_BUCKETS,
            num_topn: crate::constants::DEFAULT_TOP_N_VALUE,
            default_num_buckets: crate::constants::DEFAULT_HISTOGRAM_BUCKETS,
            default_num_topn: crate::constants::DEFAULT_TOP_N_VALUE,
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
struct SequentialRangeChecker {
    ranges: Vec<(i64, i64)>,
    current: usize,
}

impl SequentialRangeChecker {
    fn new(ranges: &[TopNWithRange]) -> Self {
        let mut ranges: Vec<(i64, i64)> = ranges
            .iter()
            .map(|item| (item.start_idx, item.end_idx))
            .collect();
        ranges.sort_by_key(|(start, _)| *start);
        Self { ranges, current: 0 }
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
const fn is_analyze_default_value(value: usize, default_value: usize) -> bool {
    value == default_value
}

/// What `processTopNValue` weighs a candidate against: the size asked for,
/// whether the user asking for it switched the heuristics off, and how much
/// of the table one sampled row stands for.
struct TopNPolicy {
    num_topn: usize,
    allow_pruning: bool,
    sample_factor: f64,
}

/// Go `processTopNValue`.
fn process_topn_value(
    heap: &mut BoundedMinHeap<TopNWithRange>,
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
        && (heap.len() >= policy.num_topn / TOPN_PRUNING_THRESHOLD || policy.sample_factor > 1.0)
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
#[must_use]
pub fn build_hist_and_topn(
    id: i64,
    collector: &SampleCollector,
    options: BuildOptions,
    is_column: bool,
) -> HistogramAndTopN {
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
        return HistogramAndTopN {
            histogram,
            topn: None,
        };
    }

    let mut samples = collector.samples.clone();
    // Go sorts by `Datum.Compare`; the encoding is order-preserving, so this
    // is the same order without a second comparison rule.
    samples.sort_by(|left, right| left.encoded.cmp(&right.encoded));

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
    let mut heap: BoundedMinHeap<TopNWithRange> = BoundedMinHeap::new(
        isize::try_from(num_topn).unwrap_or(isize::MAX),
        Some(
            |left: &TopNWithRange, right: &TopNWithRange| match left.count.cmp(&right.count) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            },
        ),
    );
    let mut cur = samples[0].encoded.clone();
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
        if cur == sample.encoded {
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
        cur.clone_from(&sample.encoded);
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

    let mut pruned = heap.to_sorted_slice().unwrap_or_default();
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
        return HistogramAndTopN {
            histogram,
            topn: Some(topn),
        };
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
            &samples,
            count - topn_total_count as i64,
            remaining_ndv,
            num_buckets,
            samples_excluding_topn,
            &mut checker,
        );
    }

    HistogramAndTopN {
        histogram,
        topn: Some(topn),
    }
}

/// Go `buildHist`.
///
/// Every count written here is cumulative, and every one is a sample count
/// scaled up by `sample_factor` -- the histogram describes the table, not the
/// sample.
fn build_hist(
    histogram: &mut Histogram,
    samples: &[SampleItem],
    count: i64,
    ndv: i64,
    num_buckets: i64,
    sample_count_exclude_topn: i64,
    checker: &mut SequentialRangeChecker,
) {
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
        return;
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
    // The bucket's upper bound in the comparison domain, kept alongside the
    // stored `Datum` so the "same value" test below is the same byte
    // comparison the sort and the TopN grouping used.
    let mut upper_encoded = first.encoded.clone();

    let mut bucket_idx = 0_usize;
    let mut last_count = 0_i64;
    let mut processed_count = 1_i64;

    for i in (first_sample_idx + 1)..sample_num {
        if checker.contains(i) {
            continue;
        }
        processed_count += 1;
        let sample = &samples[i as usize];
        let total_count = processed_count as f64 * sample_factor;
        if upper_encoded == sample.encoded {
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
            upper_encoded.clone_from(&sample.encoded);
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
            upper_encoded.clone_from(&sample.encoded);
        }
    }
}
