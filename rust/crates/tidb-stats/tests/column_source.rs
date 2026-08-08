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

use tidb_datatype::Datum;
use tidb_stats::{
    column_is_all_evicted, column_stats_validity, copy_column, empty_column, Bucket, CmsSketch,
    Column, ColumnInfo, ColumnValidityContext, FmSketch, Histogram, StatsLoadedStatus, TopN,
    ALL_EVICTED, ALL_LOADED,
};

fn top_n(count: u64) -> TopN {
    let mut top_n = TopN::new(1);
    top_n.append(&[1], count);
    top_n
}

fn populated_column(stats_version: i64) -> Column {
    Column {
        cmsketch: Some(CmsSketch::new(2, 4)),
        top_n: Some(top_n(3)),
        fm_sketch: Some(FmSketch::new(8)),
        info: Some(ColumnInfo {
            id: 7,
            name: "a".to_owned(),
            primary_key: true,
        }),
        histogram: Histogram {
            id: 7,
            ndv: 2,
            null_count: 2,
            buckets: vec![Bucket {
                count: 5,
                repeat: 1,
                ndv: 2,
                lower_bound: Datum::new_int(1),
                upper_bound: Datum::new_int(2),
            }],
            ..Histogram::default()
        },
        stats_loaded_status: StatsLoadedStatus::full_load(),
        physical_id: 11,
        stats_version,
        is_handle: true,
        histogram_memory_usage: 17,
    }
}

#[test]
fn source_copy_is_nil_preserving_and_deep() {
    assert_eq!(copy_column(None), None);
    let source = populated_column(2);
    let mut copied = copy_column(Some(&source)).unwrap();
    copied.histogram.buckets[0].count = 99;
    copied.cmsketch.as_mut().unwrap().insert_bytes(b"x");
    copied.info.as_mut().unwrap().name = "changed".to_owned();
    assert_eq!(source.histogram.buckets[0].count, 5);
    assert_eq!(source.cmsketch.as_ref().unwrap().total_count(), 0);
    assert_eq!(source.info.as_ref().unwrap().name, "a");
}

#[test]
fn source_v1_and_v2_counts_and_increase_factor_match() {
    let v1 = populated_column(1);
    assert_eq!(v1.not_null_count(), 5.0);
    assert_eq!(v1.total_row_count(), 7.0);
    assert_eq!(v1.increase_factor(14), 2.0);

    let v2 = populated_column(2);
    assert_eq!(v2.not_null_count(), 8.0);
    assert_eq!(v2.total_row_count(), 10.0);
    assert_eq!(v2.increase_factor(15), 1.5);

    let empty = empty_column(9, false, ColumnInfo::default());
    assert_eq!(empty.increase_factor(200), 1.0);
}

#[test]
#[should_panic(expected = "v2 column has no TopN")]
fn source_v2_count_keeps_the_topn_precondition() {
    let mut column = populated_column(2);
    column.top_n = None;
    let _ = column.total_row_count();
}

#[test]
fn source_memory_usage_composes_every_optional_payload() {
    let column = populated_column(1);
    let usage = column.memory_usage();
    assert_eq!(usage.column_id, 7);
    assert_eq!(usage.histogram_mem_usage, 17);
    assert_eq!(usage.cmsketch_mem_usage, 32);
    assert_eq!(usage.topn_mem_usage, 65);
    assert_eq!(usage.fmsketch_mem_usage, 16);
    assert_eq!(usage.total_mem_usage, 130);

    let mut minimal = column;
    minimal.cmsketch = None;
    minimal.top_n = None;
    minimal.fm_sketch = None;
    assert_eq!(minimal.memory_usage().total_mem_usage, 17);
}

#[test]
fn source_drop_preserves_v2_cms_but_not_v1_cms() {
    for version in [1, 2] {
        let mut column = populated_column(version);
        column.drop_unnecessary_data();
        assert_eq!(column.cmsketch.is_some(), version >= 2);
        assert!(column.top_n.is_none());
        assert!(column.histogram.buckets.is_empty());
        assert_eq!(column.evicted_status(), ALL_EVICTED);
        assert!(column.is_all_evicted());
    }
    assert!(column_is_all_evicted(None));

    let mut uninitialized = populated_column(1);
    uninitialized.stats_loaded_status = StatsLoadedStatus::default();
    uninitialized.drop_unnecessary_data();
    assert!(!uninitialized.is_stats_initialized());
    assert!(!uninitialized.is_all_evicted());
    assert_eq!(uninitialized.evicted_status(), ALL_EVICTED);
}

#[test]
fn source_invalidity_truth_table_and_load_effect_match() {
    let base = ColumnValidityContext {
        has_plan_context: true,
        has_statement_context: true,
        physical_id: 12,
        ..ColumnValidityContext::default()
    };
    let missing = column_stats_validity(None, base, 7);
    assert!(missing.invalid);
    assert_eq!(missing.load_request.unwrap().table_id, 12);

    let mut column = populated_column(1);
    let valid = column_stats_validity(Some(&column), base, 7);
    assert!(!valid.invalid);
    assert!(valid.load_request.is_none());

    column.stats_loaded_status = StatsLoadedStatus::new(true, ALL_EVICTED);
    let evicted = column_stats_validity(Some(&column), base, 7);
    assert!(evicted.invalid);
    assert!(evicted.load_request.is_some());

    column.histogram.ndv = 0;
    assert!(!column_stats_validity(Some(&column), base, 7).invalid);

    for context in [
        ColumnValidityContext {
            restricted_sql: true,
            ..base
        },
        ColumnValidityContext {
            cannot_trigger_load: true,
            ..base
        },
        ColumnValidityContext {
            has_statement_context: false,
            ..base
        },
    ] {
        assert!(column_stats_validity(None, context, 7)
            .load_request
            .is_none());
    }
    assert!(column_stats_validity(None, base, -1).load_request.is_none());
    assert!(
        column_stats_validity(
            Some(&populated_column(1)),
            ColumnValidityContext {
                pseudo: true,
                ..base
            },
            7,
        )
        .invalid
    );
}

#[test]
fn source_status_availability_and_empty_column_boundaries_match() {
    let mut column = populated_column(0);
    assert!(!column.is_analyzed());
    assert!(column.stats_available());
    column.histogram.ndv = 0;
    column.histogram.null_count = 0;
    assert!(!column.stats_available());
    column.stats_version = 1;
    assert!(column.stats_available());
    assert!(column.is_analyzed());
    assert_eq!(column.item_id(), 7);
    assert_eq!(column.stats_version(), 1);
    assert_eq!(column.histogram().id, 7);
    assert_eq!(column.top_n().unwrap().total_count(), 3);
    assert!(column.is_cms_exist());
    assert_eq!(column.stats_loaded_status.status_to_string(), "allLoaded");
    assert_eq!(column.evicted_status(), ALL_LOADED);

    let empty = empty_column(
        -1,
        true,
        ColumnInfo {
            id: 5,
            name: "pk".to_owned(),
            primary_key: true,
        },
    );
    assert_eq!(empty.physical_id, -1);
    assert_eq!(empty.histogram.id, 5);
    assert!(empty.histogram.buckets.is_empty());
    assert!(empty.is_handle);
}
