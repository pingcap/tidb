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

//! `pkg/statistics/builder.go`'s arithmetic, over samples chosen so the
//! answer can be read off the source rather than off a previous run of this
//! code.
//!
//! Every case here samples the *whole* table (`sample_factor == 1`), because
//! that is the case Go's own analyze v2 takes for any table under
//! `DefRowsForSampleRate` rows and the one where the numbers are checkable by
//! hand: a bucket count is a row count, a repeat is an occurrence count, and
//! a TopN count is what it says.

use tidb_datatype::Datum;
use tidb_stats::builder::{build_hist_and_topn, BuildOptions, SampleCollector, SampleItem};

/// An integer sample, encoded the way an order-preserving key encoder would:
/// sign-flipped big-endian, so byte order is numeric order.
fn int_of(value: &Datum) -> i64 {
    match value {
        Datum::Int(value) => *value,
        other => panic!("this fixture stores only integers, not {other:?}"),
    }
}

fn item(value: i64, ordinal: i64) -> SampleItem {
    let bits = (value as u64) ^ (1 << 63);
    SampleItem {
        encoded: bits.to_be_bytes().to_vec(),
        value: Datum::Int(value),
        ordinal,
    }
}

fn collector(values: &[i64]) -> SampleCollector {
    let mut distinct: Vec<i64> = values.to_vec();
    distinct.sort_unstable();
    distinct.dedup();
    SampleCollector {
        samples: values
            .iter()
            .enumerate()
            .map(|(ordinal, value)| item(*value, ordinal as i64))
            .collect(),
        null_count: 0,
        count: values.len() as i64,
        ndv: distinct.len() as i64,
        total_size: values.len() as i64 * 8,
    }
}

/// The plainest statement the builder makes: over a fully sampled column with
/// no repeated value and TopN switched off, every row lands in a bucket and
/// the last bucket's cumulative count is the table's row count.
#[test]
fn a_full_sample_puts_every_row_in_the_histogram() {
    let values: Vec<i64> = (1..=20).collect();
    let built = build_hist_and_topn(
        1,
        &collector(&values),
        BuildOptions {
            num_buckets: 4,
            num_topn: 0,
            ..BuildOptions::default()
        },
        true,
    );
    let buckets = &built.histogram.buckets;
    assert_eq!(built.histogram.ndv, 20);
    assert_eq!(
        buckets.last().expect("a bucket").count,
        20,
        "the last bucket's cumulative count is the row count"
    );
    // `valuesPerBucket = count/numBuckets + sampleFactor = 20/4 + 1 = 6`, so
    // the buckets fill six rows at a time and four of them hold twenty rows.
    assert_eq!(buckets.len(), 4);
    for bucket in buckets {
        assert!(
            int_of(&bucket.lower_bound) <= int_of(&bucket.upper_bound),
            "a bucket's bounds are in order"
        );
    }
    for pair in buckets.windows(2) {
        assert!(
            pair[0].count < pair[1].count,
            "cumulative counts are strictly increasing"
        );
        assert!(
            int_of(&pair[0].upper_bound) < int_of(&pair[1].lower_bound),
            "buckets do not overlap"
        );
    }
}

/// A small fully-sampled column ends up entirely in the TopN, with no buckets
/// at all -- and that is Go's answer, not an accident.
///
/// Fourteen rows over seven distinct values, against the default TopN of 100.
/// `pruneTopNItem` returns untouched because `len(topns) >= ndv`: there is no
/// "rest of the distribution" for a kept entry to be compared against. And
/// `processTopNValue` drops a singleton only once the heap holds
/// `numTopN/topNPruningThreshold` entries, which seven never reaches. Every
/// distinct value is therefore a TopN entry, `haveAllNDV` holds, and
/// `BuildHistAndTopN` returns before building a single bucket: an exact
/// per-value count is strictly better than a histogram over the same values.
#[test]
fn a_small_full_sample_is_described_entirely_by_the_topn() {
    let mut values = vec![7_i64; 8];
    values.extend([1, 2, 3, 4, 5, 6]);
    let built = build_hist_and_topn(1, &collector(&values), BuildOptions::default(), true);
    let topn = built.topn.expect("a TopN");
    assert_eq!(topn.num(), 7, "every distinct value is a TopN entry");
    assert!(
        built.histogram.buckets.is_empty(),
        "a TopN that covers every sampled value leaves no buckets to build"
    );
    let skewed = topn
        .entries()
        .iter()
        .find(|entry| entry.encoded == item(7, 0).encoded)
        .expect("the repeated value is in the TopN");
    assert_eq!(
        skewed.count, 8,
        "a fully sampled TopN count is the occurrence count"
    );
    assert_eq!(
        topn.total_count(),
        values.len() as u64,
        "the TopN alone accounts for every row"
    );
}

/// More distinct values than the TopN can hold is what makes buckets exist:
/// the commonest values are stored exactly, and the histogram describes the
/// rest.
#[test]
fn a_column_wider_than_the_topn_gets_buckets_for_the_remainder() {
    let values: Vec<i64> = (1..=300).collect();
    let built = build_hist_and_topn(1, &collector(&values), BuildOptions::default(), true);
    let topn = built.topn.expect("a TopN");
    assert!(
        topn.num() <= 100,
        "the TopN never exceeds the requested size, got {}",
        topn.num()
    );
    let histogram_rows = built
        .histogram
        .buckets
        .last()
        .map(|bucket| bucket.count)
        .unwrap_or_default();
    assert_eq!(
        histogram_rows + topn.total_count() as i64,
        300,
        "the buckets hold exactly the rows the TopN did not"
    );
    for pair in built.histogram.buckets.windows(2) {
        assert!(
            int_of(&pair[0].upper_bound) < int_of(&pair[1].lower_bound),
            "buckets do not overlap"
        );
    }
}

/// The counts a histogram and its TopN carry must add up to the column's
/// non-NULL row count. A planner that estimated over both would otherwise
/// either lose rows or invent them.
#[test]
fn the_topn_and_the_histogram_together_account_for_every_row() {
    let values = vec![1, 1, 1, 1, 2, 2, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10];
    let built = build_hist_and_topn(1, &collector(&values), BuildOptions::default(), true);
    let topn_total: u64 = built
        .topn
        .as_ref()
        .map(|topn| topn.total_count())
        .unwrap_or_default();
    let histogram_total = built
        .histogram
        .buckets
        .last()
        .map(|bucket| bucket.count)
        .unwrap_or_default();
    assert_eq!(
        topn_total as i64 + histogram_total,
        values.len() as i64,
        "TopN rows plus histogram rows are the column's rows"
    );
}

/// One value never spans two buckets: Go extends the current bucket instead
/// of starting a new one, whatever that does to the bucket's size, because an
/// estimator that found half a value's rows either side of a boundary would
/// double-count it.
#[test]
fn a_repeated_value_stays_inside_one_bucket() {
    // Six copies of `5` against a bucket budget that would otherwise split
    // them, with TopN off so they must live in the histogram.
    let values = vec![1, 2, 5, 5, 5, 5, 5, 5, 8, 9];
    let built = build_hist_and_topn(
        1,
        &collector(&values),
        BuildOptions {
            num_buckets: 5,
            num_topn: 0,
            ..BuildOptions::default()
        },
        true,
    );
    let holding: Vec<_> = built
        .histogram
        .buckets
        .iter()
        .filter(|bucket| {
            int_of(&bucket.lower_bound) <= 5 && 5 <= int_of(&bucket.upper_bound)
        })
        .collect();
    assert_eq!(holding.len(), 1, "`5` lives in exactly one bucket");
    assert_eq!(
        holding[0].repeat, 6,
        "the bucket records how many times its upper bound repeats"
    );
}

/// A column with no rows produces a histogram with no buckets rather than one
/// empty bucket, and no TopN at all -- which is what tells the loader the
/// difference between "analyzed and empty" and "never analyzed".
#[test]
fn an_empty_column_produces_no_buckets() {
    let built = build_hist_and_topn(1, &collector(&[]), BuildOptions::default(), true);
    assert!(built.histogram.buckets.is_empty());
    assert!(built.topn.is_none());
    assert_eq!(built.histogram.ndv, 0);
}

/// A column whose physical order is its sorted order correlates at 1, and one
/// stored backwards at -1. This is the statistic the planner uses to decide
/// whether an index range scan will read rows in table order.
#[test]
fn correlation_reads_the_physical_order() {
    let ascending: Vec<SampleItem> = (0..10).map(|value| item(value, value)).collect();
    let descending: Vec<SampleItem> = (0..10).map(|value| item(value, 9 - value)).collect();
    for (samples, expected) in [(ascending, 1.0_f64), (descending, -1.0_f64)] {
        let built = build_hist_and_topn(
            1,
            &SampleCollector {
                samples,
                null_count: 0,
                count: 10,
                ndv: 10,
                total_size: 80,
            },
            BuildOptions::default(),
            true,
        );
        assert!(
            (built.histogram.correlation - expected).abs() < 1e-9,
            "expected correlation {expected}, got {}",
            built.histogram.correlation
        );
    }
}

/// An index histogram carries no correlation: Go computes it only when
/// `isColumn`, because an index key's physical order is the index's own.
#[test]
fn an_index_histogram_has_no_correlation() {
    let values: Vec<i64> = (1..=10).collect();
    let built = build_hist_and_topn(1, &collector(&values), BuildOptions::default(), false);
    assert_eq!(built.histogram.correlation, 0.0);
}

/// NULLs are counted beside the histogram, never inside it: `count` is the
/// non-NULL row count and `null_count` carries the rest.
#[test]
fn nulls_are_counted_outside_the_histogram() {
    let values: Vec<i64> = (1..=10).collect();
    let mut collected = collector(&values);
    collected.null_count = 5;
    let built = build_hist_and_topn(1, &collected, BuildOptions::default(), true);
    assert_eq!(built.histogram.null_count, 5);
    let stored: i64 = built
        .histogram
        .buckets
        .last()
        .map(|bucket| bucket.count)
        .unwrap_or_default()
        + built
            .topn
            .as_ref()
            .map(|topn| topn.total_count() as i64)
            .unwrap_or_default();
    assert_eq!(stored, 10, "the histogram describes the non-NULL rows only");
}

/// A user-chosen TopN size switches Go's pruning heuristics off
/// (`isAnalyzeDefaultValue`), so `WITH 1 TOPN` keeps the one entry it asked
/// for even when the default path would have pruned it away.
#[test]
fn a_user_chosen_topn_size_is_honoured() {
    let values: Vec<i64> = (1..=20).collect();
    let built = build_hist_and_topn(
        1,
        &collector(&values),
        BuildOptions {
            num_topn: 1,
            ..BuildOptions::default()
        },
        true,
    );
    let topn = built.topn.expect("a TopN");
    assert_eq!(
        topn.num(),
        1,
        "an explicitly requested TopN entry survives, though every value \
         occurs exactly once"
    );
}
