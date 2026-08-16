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

//! Source-backed tests for global TopN merging, ported from
//! `pkg/statistics/handle/globalstats/topn_test.go` plus the two
//! `globalstats` merge cases in `global_stats_test.go`.
//!
//! Skipped source artifacts, named:
//! - `topn_bench_test.go`: `BenchmarkMergePartTopN2GlobalTopNWithHists` and
//!   `BenchmarkMergeGlobalStatsTopNByConcurrencyWithHists` (plus their
//!   `prepareTopNsAndHists` helper) are throughput benchmarks with no
//!   assertions; their code path is instead covered by
//!   `source_merge_by_concurrency_matches_sequential_merge` below.
//! - `global_stats_test.go::TestMergeGlobalStatsForCMSketch` drives
//!   `ANALYZE`/`EXPLAIN` through `testkit` against a mock store and asserts on
//!   a physical plan; nothing in it reaches `topn.go`/`merge_worker.go`
//!   without a running server, planner, and statistics handle, so it is not
//!   ported here.
//! - `global_stats_test.go::TestEmptyHists` likewise needs `testkit` and
//!   `MergePartitionStats2GlobalStatsByTableID`; its dependency-closed core,
//!   the `CheckEmptyTopNs` early return taken with both the sync and the
//!   async/concurrent merge setting, is ported as
//!   `source_empty_topns_short_circuit_both_merge_paths`.

use chrono::Utc;
use tidb_codec::encode_key;
use tidb_datatype::{Collation, Datum};
use tidb_stats::{
    merge_global_stats_topn, merge_global_stats_topn_by_concurrency, merge_part_topn_2_global_topn,
    Bucket, Histogram, StatsWrapper, TopN, TopNMergeOptions,
};
use tidb_util::sqlkiller::SqlKiller;

/// Go `mysql.TypeTiny`, the element type of the source test histograms.
const TYPE_TINY: u8 = 1;

fn key(values: &[i64]) -> Vec<u8> {
    let datums: Vec<Datum> = values.iter().copied().map(Datum::Int).collect();
    encode_key(&datums).expect("encode int key")
}

fn options(version: i64, n: u32) -> TopNMergeOptions {
    TopNMergeOptions {
        version,
        n,
        is_index: false,
        value_type: TYPE_TINY,
        collation: Collation::Binary,
    }
}

/// Go `TestMergePartTopN2GlobalTopNWithoutHists`.
#[test]
fn source_merge_part_topn_to_global_topn_without_hists() {
    // Construct TopN, should be key(1, 1) -> 2, key(1, 2) -> 2, key(1, 3) -> 3.
    let topns: Vec<TopN> = (0..10)
        .map(|_| {
            let mut topn = TopN::new(3);
            topn.append(&key(&[1, 1]), 2);
            topn.append(&key(&[1, 2]), 2);
            topn.append(&key(&[1, 3]), 3);
            topn
        })
        .collect();

    // Test merge 2 topN with nil hists.
    let merged = merge_part_topn_2_global_topn(
        None::<&Utc>,
        &topns,
        &mut [],
        &options(1, 2),
        &SqlKiller::default(),
    )
    .expect("merge succeeds")
    .expect("non-empty counter");

    assert_eq!(merged.top_n.entries().len(), 2, "should only have 2 topN");
    assert_eq!(merged.top_n.total_count(), 50, "should have 50 rows");
    assert_eq!(merged.remainder.len(), 1, "should have 1 left topN");
}

/// Builds the source histogram of `TestMergePartTopN2GlobalTopNWithHists`.
///
/// The Go test appends four `Bounds` rows (1, 2, 3, 4) and four `Buckets`, so
/// its `Bounds` chunk only covers buckets 0 and 1 while `Len()` reports 4.
/// `LocateBucket` and `BinarySearchRemoveVal` never read past row 3 for the
/// value this test looks up, so the port keeps the observable shape — four
/// buckets, ascending bounds, the same per-bucket `Repeat`/`Count`, and the
/// same `NotNullCount()` of 40 — with bounds continued to 8 so that every
/// bucket has the (lower, upper) pair the ported `Histogram` requires.
fn source_hist() -> Histogram {
    let mut hist = Histogram::new(1, 10, 0, 0, 4, 0);
    for (index, count) in [20_i64, 30, 30, 40].into_iter().enumerate() {
        hist.buckets.push(Bucket {
            count,
            repeat: 10,
            ndv: 0,
            lower_bound: Datum::Int(index as i64 * 2 + 1),
            upper_bound: Datum::Int(index as i64 * 2 + 2),
        });
    }
    hist
}

/// The partition TopN of Go `TestMergePartTopN2GlobalTopNWithHists`:
/// key1 -> 2, key2 -> 2, and key3 -> 3 only on even partitions.
fn source_topns_with_hists() -> Vec<TopN> {
    (0..10)
        .map(|i| {
            let mut topn = TopN::new(3);
            topn.append(&key(&[1]), 2);
            topn.append(&key(&[2]), 2);
            if i % 2 == 0 {
                topn.append(&key(&[3]), 3);
            }
            topn
        })
        .collect()
}

/// Go `TestMergePartTopN2GlobalTopNWithHists`.
#[test]
fn source_merge_part_topn_to_global_topn_with_hists() {
    let topns = source_topns_with_hists();
    let mut hists: Vec<Histogram> = (0..10).map(|_| source_hist()).collect();

    // Test merge 2 topN.
    let merged = merge_part_topn_2_global_topn(
        None::<&Utc>,
        &topns,
        &mut hists,
        &options(1, 2),
        &SqlKiller::default(),
    )
    .expect("merge succeeds")
    .expect("non-empty counter");

    assert_eq!(merged.top_n.entries().len(), 2, "should only have 2 topN");
    assert_eq!(merged.top_n.total_count(), 55, "should have 55");
    assert_eq!(merged.remainder.len(), 1, "should have 1 left topN");

    // The odd partitions lack key3, so each of their histograms gives up the
    // 40/10 rows the merge folds into the global TopN and removes them.
    assert_eq!(hists[0].buckets[3].count, 40);
    assert_eq!(hists[1].buckets[1].count, 26);
    assert_eq!(hists[1].buckets[3].count, 36);
}

/// The `merge_worker.go` pipeline, the path Go exercises only through
/// `BenchmarkMergeGlobalStatsTopNByConcurrencyWithHists`. The concurrent merge
/// must reach the same global TopN as the sequential one.
#[test]
fn source_merge_by_concurrency_matches_sequential_merge() {
    let mut wrapper = StatsWrapper::new(
        (0..10).map(|_| source_hist()).collect(),
        source_topns_with_hists(),
    );

    let merged = merge_global_stats_topn_by_concurrency(
        4,
        3,
        &mut wrapper,
        None::<&Utc>,
        &options(1, 2),
        &SqlKiller::default(),
    )
    .expect("merge succeeds")
    .expect("non-empty counter");

    assert_eq!(merged.top_n.entries().len(), 2);
    assert_eq!(merged.top_n.total_count(), 55);
    assert_eq!(merged.remainder.len(), 1);
}

/// The `mergeGlobalStatsTopN` dispatcher picks the sequential path below a
/// concurrency of 2 and the worker pipeline at or above it; both agree.
#[test]
fn source_merge_dispatcher_agrees_across_concurrency() {
    for merge_concurrency in [1_usize, 2, 4, 16] {
        let mut wrapper = StatsWrapper::new(
            (0..10).map(|_| source_hist()).collect(),
            source_topns_with_hists(),
        );
        let merged = merge_global_stats_topn(
            merge_concurrency,
            &mut wrapper,
            None::<&Utc>,
            &options(1, 2),
            &SqlKiller::default(),
        )
        .expect("merge succeeds")
        .expect("non-empty counter");
        assert_eq!(merged.top_n.total_count(), 55, "{merge_concurrency} workers");
        assert_eq!(merged.remainder.len(), 1, "{merge_concurrency} workers");
    }
}

/// Dependency-closed core of Go `TestEmptyHists`: a table whose partitions
/// carry no TopN short-circuits before any histogram work, with the merge
/// concurrency setting the Go test toggles making no difference.
#[test]
fn source_empty_topns_short_circuit_both_merge_paths() {
    for merge_concurrency in [1_usize, 4] {
        let mut wrapper = StatsWrapper::new(
            (0..12).map(|_| Histogram::default()).collect(),
            (0..12).map(|_| TopN::new(0)).collect(),
        );
        let merged = merge_global_stats_topn(
            merge_concurrency,
            &mut wrapper,
            None::<&Utc>,
            &options(2, 2),
            &SqlKiller::default(),
        )
        .expect("merge succeeds");
        assert!(merged.is_none(), "{merge_concurrency} workers");
    }
}

/// With `version >= 2` a partition skips its own histogram, so the value is
/// not double counted from the partition that already listed it in its TopN.
#[test]
fn source_version_two_skips_the_owning_partition_histogram() {
    let topns = source_topns_with_hists();
    let mut hists: Vec<Histogram> = (0..10).map(|_| source_hist()).collect();
    let merged = merge_part_topn_2_global_topn(
        None::<&Utc>,
        &topns,
        &mut hists,
        &options(2, 3),
        &SqlKiller::default(),
    )
    .expect("merge succeeds")
    .expect("non-empty counter");

    // key1 and key2 are in every partition TopN, so version has no effect on
    // them; key3 still collects 4 rows from each of the five odd partitions.
    assert_eq!(merged.top_n.total_count(), 75);
    assert!(merged.remainder.is_empty());
}

/// A killed query aborts the merge, Go's `killer.HandleSignal()` check.
#[test]
fn source_kill_signal_aborts_the_merge() {
    use tidb_util::sqlkiller::KillSignal;

    let killer = SqlKiller::default();
    killer.send_kill_signal(KillSignal::QueryInterrupted);
    let topns = source_topns_with_hists();
    let mut hists: Vec<Histogram> = (0..10).map(|_| source_hist()).collect();
    let error = merge_part_topn_2_global_topn(
        None::<&Utc>,
        &topns,
        &mut hists,
        &options(1, 2),
        &killer,
    )
    .expect_err("killed merge fails");
    assert!(!error.to_string().is_empty());
}
