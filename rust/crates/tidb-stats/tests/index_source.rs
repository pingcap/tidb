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
    copy_index, index_is_all_evicted, index_stats_validity, Bucket, CmsSketch, FmSketch, Histogram,
    Index, IndexInfo, IndexValidityContext, StatsLoadedStatus, TopN, ALL_EVICTED,
};

fn populated_index(version: i64) -> Index {
    let mut top_n = TopN::new(1);
    top_n.append(b"top", 3);
    let mut cmsketch = CmsSketch::new(2, 4);
    cmsketch.insert_bytes_by_count(b"cms", 4);
    Index {
        cmsketch: Some(cmsketch),
        top_n: Some(top_n),
        fm_sketch: Some(FmSketch::new(8)),
        info: Some(IndexInfo {
            id: 9,
            name: "idx_a".to_owned(),
            columns: vec!["a".to_owned()],
        }),
        histogram: Histogram {
            id: 9,
            null_count: 1,
            buckets: vec![Bucket {
                count: 6,
                repeat: 1,
                ndv: 2,
                lower_bound: Datum::Bytes(b"a".to_vec()),
                upper_bound: Datum::Bytes(b"z".to_vec()),
            }],
            ..Histogram::default()
        },
        stats_loaded_status: StatsLoadedStatus::full_load(),
        stats_version: version,
        physical_id: 12,
        histogram_memory_usage: 11,
    }
}

#[test]
fn source_copy_nil_and_deep_boundaries_match() {
    assert_eq!(copy_index(None), None);
    let source = populated_index(2);
    let mut copied = copy_index(Some(&source)).unwrap();
    copied.histogram.buckets[0].count = 100;
    copied.info.as_mut().unwrap().columns.push("b".to_owned());
    copied.cmsketch.as_mut().unwrap().insert_bytes(b"new");
    assert_eq!(source.histogram.buckets[0].count, 6);
    assert_eq!(source.info.as_ref().unwrap().columns, ["a"]);
    assert_eq!(source.cmsketch.as_ref().unwrap().total_count(), 4);
}

#[test]
fn source_count_factor_query_and_accessors_match() {
    let v1 = populated_index(1);
    assert_eq!(v1.total_row_count(), 7.0);
    assert_eq!(v1.increase_factor(14), 2.0);
    assert_eq!(v1.query_bytes(b"top", 8), 3);
    assert_eq!(v1.query_bytes(b"cms", 8), 4);
    let mut histogram_only = v1.clone();
    histogram_only.cmsketch = None;
    assert_eq!(histogram_only.query_bytes(b"other", 8), 8);
    assert_eq!(v1.item_id(), 9);
    assert_eq!(v1.stats_version(), 1);
    assert!(v1.is_analyzed());
    assert!(v1.is_cms_exist());
    assert_eq!(v1.histogram().id, 9);
    assert_eq!(v1.top_n().unwrap().total_count(), 3);

    let mut v2 = populated_index(2);
    assert_eq!(v2.total_row_count(), 10.0);
    v2.histogram.buckets.clear();
    v2.histogram.null_count = 0;
    v2.top_n = Some(TopN::default());
    assert_eq!(v2.increase_factor(100), 1.0);
}

#[test]
#[should_panic(expected = "v2 index has no TopN")]
fn source_v2_count_keeps_the_topn_precondition() {
    let mut index = populated_index(2);
    index.top_n = None;
    let _ = index.total_row_count();
}

#[test]
fn source_drop_and_test_only_evict_match_status_boundaries() {
    for version in [1, 2] {
        let mut index = populated_index(version);
        index.drop_unnecessary_data();
        assert_eq!(index.cmsketch.is_some(), version >= 2);
        assert!(index.top_n.is_none());
        assert!(index.histogram.buckets.is_empty());
        assert!(index.is_all_evicted());
        assert!(index.is_evicted());
    }
    assert!(index_is_all_evicted(None));

    let mut index = populated_index(2);
    index.evict_all_stats();
    assert!(index.cmsketch.is_none());
    assert!(index.top_n.is_none());
    assert_eq!(index.evicted_status(), ALL_EVICTED);
}

#[test]
fn source_memory_excludes_fm_sketch() {
    let index = populated_index(1);
    let usage = index.memory_usage();
    assert_eq!(usage.index_id, 9);
    assert_eq!(usage.histogram_mem_usage, 11);
    assert_eq!(usage.cmsketch_mem_usage, 32);
    assert_eq!(usage.topn_mem_usage, 67);
    assert_eq!(usage.total_mem_usage, 110);
}

#[test]
fn source_invalidity_queues_load_without_short_circuiting() {
    let base = IndexValidityContext {
        physical_id: 12,
        ..IndexValidityContext::default()
    };
    let missing = index_stats_validity(None, base, 9);
    assert!(missing.invalid);
    assert!(missing.load_request.is_some());

    let mut index = populated_index(1);
    let valid = index_stats_validity(Some(&index), base, 9);
    assert!(!valid.invalid);
    assert!(valid.load_request.is_none());

    index.stats_loaded_status = StatsLoadedStatus::all_evicted();
    let partial = index_stats_validity(Some(&index), base, 9);
    assert!(!partial.invalid);
    assert!(partial.load_request.is_some());

    for context in [
        IndexValidityContext {
            restricted_sql: true,
            ..base
        },
        IndexValidityContext {
            cannot_trigger_load: true,
            ..base
        },
    ] {
        assert!(index_stats_validity(None, context, 9)
            .load_request
            .is_none());
    }
    assert!(
        index_stats_validity(
            Some(&populated_index(1)),
            IndexValidityContext {
                pseudo: true,
                ..base
            },
            9,
        )
        .invalid
    );
}
