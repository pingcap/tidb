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

//! Receipts for pinned `pkg/statistics/handle/globalstats/topn_test.go`.

use chrono::Utc;
use tidb_codec::encode_key;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_stats::histogram::{Bucket, Histogram};
use tidb_stats::{
    merge_partition_stats_item, merge_partition_topn, merge_partition_topn_concurrently,
    CmsSketch, FmSketch, GlobalStatsMergeError, GlobalStatsMergeMode, PartitionStatsItem, TopN,
    MAX_PARTITION_MERGE_BATCH_SIZE, MAX_SKETCH_SIZE,
};
use tidb_util::sqlkiller::{KillSignal, SqlKiller};

fn encoded(value: i64) -> Vec<u8> {
    encode_key(&[Datum::Int(value)]).expect("integer key encodes")
}

#[test]
fn source_max_partition_merge_batch_size() {
    assert_eq!(MAX_PARTITION_MERGE_BATCH_SIZE, 256);
}

#[test]
fn source_merge_partition_topn_without_histograms() {
    let killer = SqlKiller::default();
    let topns = (0..10)
        .map(|_| {
            let mut topn = TopN::new(3);
            topn.append(&encoded(1), 2);
            topn.append(&encoded(2), 2);
            topn.append(&encoded(3), 3);
            topn
        })
        .collect::<Vec<_>>();
    let refs = topns.iter().map(Some).collect::<Vec<_>>();
    let (global, remainder, _) = merge_partition_topn(
        Some(&Utc),
        1,
        &refs,
        2,
        Vec::new(),
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        &killer,
    )
    .expect("TopN merge succeeds");
    assert_eq!(global.expect("non-empty TopN").total_count(), 50);
    assert_eq!(remainder.len(), 1);
}

#[test]
fn source_merge_partition_topn_counts_and_removes_histogram_values() {
    let killer = SqlKiller::default();
    let topns = (0..10)
        .map(|partition| {
            let mut topn = TopN::new(3);
            topn.append(&encoded(1), 2);
            topn.append(&encoded(2), 2);
            if partition % 2 == 0 {
                topn.append(&encoded(3), 3);
            }
            topn
        })
        .collect::<Vec<_>>();
    let refs = topns.iter().map(Some).collect::<Vec<_>>();
    let histograms = (0..10)
        .map(|_| Histogram {
            id: 1,
            ndv: 10,
            null_count: 0,
            last_update_version: 0,
            tot_col_size: 0,
            correlation: 0.0,
            buckets: vec![Bucket {
                count: 40,
                repeat: 10,
                ndv: 0,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(4),
            }],
        })
        .collect();
    let (global, remainder, histograms) = merge_partition_topn(
        Some(&Utc),
        1,
        &refs,
        2,
        histograms,
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        &killer,
    )
    .expect("TopN merge succeeds");
    assert_eq!(global.expect("non-empty TopN").total_count(), 55);
    assert_eq!(remainder.len(), 1);
    assert_eq!(histograms[1].buckets[0].count, 36);
}

#[test]
fn source_concurrent_topn_merge_matches_the_blocking_worker_result() {
    let killer = SqlKiller::default();
    let partitions = (0..10)
        .map(|partition| {
            let mut item = partition_item([partition, partition + 100]);
            let mut topn = TopN::new(3);
            topn.append(&encoded(1), 2);
            topn.append(&encoded(2), 2);
            if partition % 2 == 0 {
                topn.append(&encoded(3), 3);
            }
            item.topn = Some(topn);
            item.histogram.buckets[0].count = 40;
            item.histogram.buckets[0].repeat = 10;
            item
        })
        .collect::<Vec<_>>();
    let merge = |concurrency| {
        merge_partition_stats_item(
            Some(&Utc),
            2,
            2,
            256,
            400,
            &FieldType::new(FieldTypeCode::Tiny),
            false,
            GlobalStatsMergeMode::Blocking,
            concurrency,
            partitions.clone(),
            &killer,
        )
        .expect("global item merge succeeds")
    };
    let sequential = merge(1);
    let concurrent = merge(2);
    let topn_receipt = |topn: Option<TopN>| {
        topn.expect("global TopN exists")
            .entries()
            .iter()
            .map(|entry| (entry.encoded.clone(), entry.count))
            .collect::<Vec<_>>()
    };

    assert_eq!(topn_receipt(sequential.topn), topn_receipt(concurrent.topn));
    let sequential = sequential.histogram.expect("global histogram exists");
    let concurrent = concurrent.histogram.expect("global histogram exists");
    assert_eq!(sequential.ndv, concurrent.ndv);
    assert_eq!(sequential.total_row_count(), concurrent.total_row_count());
    assert_eq!(sequential.buckets.len(), concurrent.buckets.len());
    for (sequential, concurrent) in sequential.buckets.iter().zip(&concurrent.buckets) {
        assert_eq!(sequential.count, concurrent.count);
        assert_eq!(sequential.repeat, concurrent.repeat);
        assert_eq!(sequential.lower_bound, concurrent.lower_bound);
        assert_eq!(sequential.upper_bound, concurrent.upper_bound);
    }
}

fn partition_item(fm_hashes: impl IntoIterator<Item = u64>) -> PartitionStatsItem {
    PartitionStatsItem {
        histogram: Histogram {
            id: 1,
            ndv: 2,
            null_count: 0,
            last_update_version: 7,
            tot_col_size: 10,
            correlation: 0.0,
            buckets: vec![Bucket {
                count: 10,
                repeat: 1,
                ndv: 2,
                lower_bound: Datum::Int(1),
                upper_bound: Datum::Int(4),
            }],
        },
        cmsketch: None,
        topn: None,
        fm_sketch: Some(FmSketch::from_raw_parts(0, MAX_SKETCH_SIZE, fm_hashes)),
    }
}

#[test]
fn source_merge_item_uses_fm_ndv_and_clears_bucket_ndv() {
    let killer = SqlKiller::default();
    let merged = merge_partition_stats_item(
        Some(&Utc),
        2,
        100,
        256,
        20,
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        GlobalStatsMergeMode::Async,
        1,
        vec![partition_item([1, 2]), partition_item([2, 3])],
        &killer,
    )
    .expect("global item merge succeeds");
    let histogram = merged.histogram.expect("histogram is produced");
    assert_eq!(histogram.ndv, 3);
    assert!(histogram.buckets.iter().all(|bucket| bucket.ndv == 0));
}

#[test]
fn source_merge_item_without_fm_uses_go_s_nil_ndv() {
    let killer = SqlKiller::default();
    let mut item = partition_item([]);
    item.fm_sketch = None;
    let merged = merge_partition_stats_item(
        Some(&Utc),
        2,
        100,
        256,
        10,
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        GlobalStatsMergeMode::Async,
        1,
        vec![item],
        &killer,
    )
    .expect("Go's nil FM sketch receiver reports NDV zero");
    assert_eq!(merged.histogram.expect("histogram is produced").ndv, 0);
}

#[test]
fn source_async_and_blocking_cms_nil_order_matches_go_workers() {
    let killer = SqlKiller::default();
    let first = partition_item([1]);
    let mut second = partition_item([2]);
    let mut cms = CmsSketch::new(2, 8);
    cms.insert_bytes_by_count(b"x", 5);
    second.cmsketch = Some(cms);
    let merge = |mode| {
        merge_partition_stats_item(
            Some(&Utc),
            2,
            100,
            256,
            20,
            &FieldType::new(FieldTypeCode::Tiny),
            false,
            mode,
            1,
            vec![first.clone(), second.clone()],
            &killer,
        )
        .expect("global item merge succeeds")
    };
    assert!(merge(GlobalStatsMergeMode::Blocking).cmsketch.is_none());
    assert_eq!(
        merge(GlobalStatsMergeMode::Async)
            .cmsketch
            .expect("async worker adopts the later sketch")
            .total_count(),
        5
    );
}

#[test]
fn source_sequential_topn_merge_preserves_the_kill_error() {
    let killer = SqlKiller::default();
    killer.send_kill_signal(KillSignal::QueryInterrupted);
    let mut topn = TopN::new(1);
    topn.append(&encoded(1), 1);
    let error = merge_partition_topn(
        Some(&Utc),
        2,
        &[Some(&topn)],
        1,
        vec![partition_item([]).histogram],
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        &killer,
    )
    .expect_err("the sequential worker checks the statement killer");
    assert!(matches!(
        error,
        GlobalStatsMergeError::Killed(ref error) if error.code == 1317
    ));
}

#[test]
fn source_concurrent_topn_merge_joins_worker_kill_errors_like_go() {
    let killer = SqlKiller::default();
    killer.send_kill_signal(KillSignal::QueryInterrupted);
    let mut first = partition_item([1]);
    let mut first_topn = TopN::new(1);
    first_topn.append(&encoded(1), 1);
    first.topn = Some(first_topn);
    let mut second = partition_item([2]);
    let mut second_topn = TopN::new(1);
    second_topn.append(&encoded(2), 1);
    second.topn = Some(second_topn);
    let error = merge_partition_stats_item(
        Some(&Utc),
        2,
        1,
        256,
        20,
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        GlobalStatsMergeMode::Blocking,
        2,
        vec![first, second],
        &killer,
    )
    .expect_err("the concurrent coordinator returns its joined worker errors");
    assert!(matches!(error, GlobalStatsMergeError::Concurrent(_)));
}

#[test]
fn exported_topn_workers_poll_killer_before_skipping_an_empty_topn() {
    let killer = SqlKiller::default();
    killer.send_kill_signal(KillSignal::QueryInterrupted);
    let empty = TopN::new(0);
    let field_type = FieldType::new(FieldTypeCode::Tiny);
    let histogram = partition_item([]).histogram;

    assert!(matches!(
        merge_partition_topn(
            Some(&Utc),
            2,
            &[Some(&empty)],
            1,
            vec![histogram.clone()],
            &field_type,
            false,
            &killer,
        ),
        Err(GlobalStatsMergeError::Killed(_))
    ));
    assert!(matches!(
        merge_partition_topn_concurrently(
            Some(&Utc),
            2,
            &[Some(&empty)],
            1,
            vec![histogram],
            &field_type,
            false,
            1,
            1,
            &killer,
        ),
        Err(GlobalStatsMergeError::Concurrent(_))
    ));
}

#[test]
fn global_topn_selector_skips_empty_topns_before_polling_killer() {
    let killer = SqlKiller::default();
    killer.send_kill_signal(KillSignal::QueryInterrupted);
    let merged = merge_partition_stats_item(
        Some(&Utc),
        2,
        1,
        256,
        10,
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        GlobalStatsMergeMode::Blocking,
        2,
        vec![partition_item([1])],
        &killer,
    )
    .expect("the Go selector returns before calling either exported TopN worker");
    assert!(merged.topn.is_none());
    assert!(merged.histogram.is_some());
}
