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
    merge_partition_stats_item, merge_partition_topn, CmsSketch, FmSketch, GlobalStatsMergeMode,
    PartitionStatsItem, TopN, MAX_SKETCH_SIZE,
};

fn encoded(value: i64) -> Vec<u8> {
    encode_key(&[Datum::Int(value)]).expect("integer key encodes")
}

#[test]
fn source_merge_partition_topn_without_histograms() {
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
    )
    .expect("TopN merge succeeds");
    assert_eq!(global.expect("non-empty TopN").total_count(), 50);
    assert_eq!(remainder.len(), 1);
}

#[test]
fn source_merge_partition_topn_counts_and_removes_histogram_values() {
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
    )
    .expect("TopN merge succeeds");
    assert_eq!(global.expect("non-empty TopN").total_count(), 55);
    assert_eq!(remainder.len(), 1);
    assert_eq!(histograms[1].buckets[0].count, 36);
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
    let merged = merge_partition_stats_item(
        Some(&Utc),
        2,
        100,
        256,
        20,
        &FieldType::new(FieldTypeCode::Tiny),
        false,
        GlobalStatsMergeMode::Async,
        vec![partition_item([1, 2]), partition_item([2, 3])],
    )
    .expect("global item merge succeeds");
    let histogram = merged.histogram.expect("histogram is produced");
    assert_eq!(histogram.ndv, 3);
    assert!(histogram.buckets.iter().all(|bucket| bucket.ndv == 0));
}

#[test]
fn source_async_and_blocking_cms_nil_order_matches_go_workers() {
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
            vec![first.clone(), second.clone()],
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
