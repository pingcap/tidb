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
    legacy_sample_collector_from_proto, legacy_sample_collector_to_proto, sort_legacy_sample_items,
    CmsSketch, FmSketch, LegacySampleCollector, LegacySampleItem, MAX_SAMPLE_VALUE_LENGTH,
};

fn item(value: i64) -> LegacySampleItem {
    LegacySampleItem {
        value: Datum::Int(value),
        encoded: vec![value as u8],
        ordinal: value,
    }
}

#[test]
fn source_sort_is_stable_and_total_size_uses_full_encoded_length() {
    let mut last = item(2);
    last.ordinal = 20;
    let mut items = vec![item(2), item(1), last];
    sort_legacy_sample_items(&mut items).unwrap();
    assert_eq!(
        items.iter().map(|item| item.ordinal).collect::<Vec<_>>(),
        [1, 2, 20]
    );
    let mut collector = LegacySampleCollector {
        samples: items,
        ..LegacySampleCollector::default()
    };
    collector.calculate_total_size();
    assert_eq!(collector.total_size, 3);
}

#[test]
fn source_collect_null_nonnull_and_destroy_boundaries_match() {
    let mut collector = LegacySampleCollector {
        fm_sketch: Some(FmSketch::new(8)),
        cmsketch: Some(CmsSketch::new(2, 4)),
        max_sample_size: 4,
        ..LegacySampleCollector::default()
    };
    collector.collect(Datum::Null, vec![]);
    collector.collect(Datum::Int(1), vec![8, 1]);
    assert_eq!(collector.null_count, 1);
    assert_eq!(collector.count, 1);
    assert_eq!(collector.total_size, 1);
    assert_eq!(collector.seen_values, 1);
    assert_eq!(collector.samples.len(), 1);
    collector.destroy();
    assert!(collector.fm_sketch.is_none());
    assert!(collector.cmsketch.is_none());
    assert!(collector.samples.is_empty());
    assert_eq!(collector.count, 0);
    assert!(!collector.is_merger);
}

#[test]
fn source_merge_combines_counts_sketches_and_reservoir_input() {
    let make = |value| {
        let mut collector = LegacySampleCollector {
            fm_sketch: Some(FmSketch::new(8)),
            cmsketch: Some(CmsSketch::new(2, 4)),
            max_sample_size: 8,
            is_merger: true,
            ..LegacySampleCollector::default()
        };
        collector.samples.push(item(value));
        collector.null_count = value;
        collector.count = value;
        collector.total_size = value;
        collector
    };
    let mut left = make(1);
    left.merge(&make(2));
    assert_eq!(left.null_count, 3);
    assert_eq!(left.count, 3);
    assert_eq!(left.total_size, 3);
    assert_eq!(left.samples.len(), 2);
}

#[test]
fn source_proto_round_trip_filters_only_oversized_samples() {
    let mut collector = LegacySampleCollector {
        fm_sketch: Some(FmSketch::new(8)),
        cmsketch: Some(CmsSketch::new(2, 4)),
        null_count: 3,
        count: 5,
        total_size: 9,
        ..LegacySampleCollector::default()
    };
    collector.samples.push(LegacySampleItem {
        value: Datum::Bytes(vec![1]),
        encoded: vec![1],
        ordinal: 0,
    });
    collector.samples.push(LegacySampleItem {
        value: Datum::Bytes(vec![0; MAX_SAMPLE_VALUE_LENGTH + 1]),
        encoded: vec![0; MAX_SAMPLE_VALUE_LENGTH + 1],
        ordinal: 1,
    });
    let proto = legacy_sample_collector_to_proto(&collector).unwrap();
    let decoded = legacy_sample_collector_from_proto(&proto).unwrap();
    assert_eq!(decoded.null_count, 3);
    assert_eq!(decoded.count, 5);
    assert_eq!(decoded.total_size, 9);
    assert_eq!(decoded.samples.len(), 1);
}

#[test]
fn source_extract_topn_zero_and_frequency_boundaries_match() {
    let mut collector = LegacySampleCollector {
        cmsketch: Some(CmsSketch::new(2, 32)),
        ..LegacySampleCollector::default()
    };
    for value in [1_u8, 1, 1, 2, 2, 3] {
        collector.cmsketch.as_mut().unwrap().insert_bytes(&[value]);
        collector.samples.push(LegacySampleItem {
            value: Datum::Bytes(vec![value]),
            encoded: vec![value],
            ordinal: 0,
        });
    }
    collector
        .extract_topn(0, |bytes| Ok::<_, ()>(bytes.to_vec()))
        .unwrap();
    assert!(collector.top_n.is_none());
    collector
        .extract_topn(2, |bytes| Ok::<_, ()>(bytes.to_vec()))
        .unwrap();
    let top_n = collector.top_n.as_ref().unwrap();
    assert_eq!(top_n.num(), 2);
    assert_eq!(top_n.query_bytes(&[1]), Some(3));
    assert_eq!(top_n.query_bytes(&[2]), Some(2));
}
