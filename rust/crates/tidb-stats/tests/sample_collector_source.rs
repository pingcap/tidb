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

use tidb_datatype::{Datum, VectorFloat32};
use tidb_stats::{
    hash_bytes, legacy_row_to_datums, legacy_sample_collector_from_proto,
    legacy_sample_collector_to_proto, sort_legacy_sample_items, CmsSketch, FmSketch,
    LegacyRecordChunk, LegacySampleBuilder, LegacySampleBuilderError, LegacySampleCollector,
    LegacySampleCollectorProto, LegacySampleItem, LegacySampleRng, SortedHistogramBuilder,
    EMPTY_SAMPLE_ITEM_SIZE, MAX_SAMPLE_VALUE_LENGTH,
};

fn item(value: i64) -> LegacySampleItem {
    LegacySampleItem {
        value: Datum::Int(value),
        handle: None,
        ordinal: value as isize,
    }
}

#[test]
fn source_sample_constants_match_go_64_bit_layout_and_length_gate() {
    assert_eq!(EMPTY_SAMPLE_ITEM_SIZE, 72 + 16 + 8);
    assert_eq!(MAX_SAMPLE_VALUE_LENGTH, 65_535 / 2);
}

#[test]
fn source_sort_is_stable_and_total_size_reads_datum_get_bytes() {
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
    assert_eq!(collector.total_size, 0, "integer Datum.GetBytes is empty");
}

#[test]
fn source_sort_returns_the_last_comparator_error_on_go_schedule() {
    let vector = || Datum::new_vector_float32(VectorFloat32::must_create(vec![1.0]));
    let mut three = vec![
        LegacySampleItem {
            value: vector(),
            handle: None,
            ordinal: 0,
        },
        item(1),
        item(2),
    ];
    tidb_stats::sample_collector::sort_legacy_sample_items(&mut three).unwrap();
    assert_eq!(
        three
            .iter()
            .map(|sample| sample.ordinal)
            .collect::<Vec<_>>(),
        [1, 2, 0],
        "later successful insertion comparison clears the earlier vector error"
    );

    let mut across_blocks = Vec::with_capacity(21);
    across_blocks.push(LegacySampleItem {
        value: vector(),
        handle: None,
        ordinal: 0,
    });
    across_blocks.extend((1..20).map(item));
    across_blocks.push(LegacySampleItem {
        value: Datum::Null,
        handle: None,
        ordinal: 20,
    });
    tidb_stats::sample_collector::sort_legacy_sample_items(&mut across_blocks).unwrap();
    let mut expected = vec![20];
    expected.extend(1..20);
    expected.push(0);
    assert_eq!(
        across_blocks
            .iter()
            .map(|sample| sample.ordinal)
            .collect::<Vec<_>>(),
        expected,
        "the 20-item insertion block and final SymMerge follow Go's permutation"
    );
}

#[test]
fn source_collect_null_nonnull_and_destroy_boundaries_match() {
    let mut collector = LegacySampleCollector {
        fm_sketch: Some(FmSketch::new(8)),
        cmsketch: Some(CmsSketch::new(2, 4)),
        max_sample_size: 4,
        ..LegacySampleCollector::default()
    };
    collector
        .collect(Datum::Null, |_| Err::<Vec<u8>, _>("must not encode NULL"))
        .unwrap();
    collector
        .collect(Datum::Int(1), |_| Ok::<_, ()>(vec![8, 2]))
        .unwrap();
    assert_eq!(collector.null_count, 1);
    assert_eq!(collector.count, 1);
    assert_eq!(collector.total_size, -1);
    assert_eq!(collector.seen_values, 1);
    assert_eq!(collector.samples.len(), 1);
    assert!(collector.samples[0].value.go_bytes().is_empty());
    assert!(collector
        .fm_sketch
        .as_ref()
        .unwrap()
        .contains(hash_bytes(&[8, 2]).h1));
    assert_eq!(collector.cmsketch.as_ref().unwrap().query_bytes(&[]), 1);
    assert_eq!(
        legacy_sample_collector_to_proto(&collector)
            .unwrap()
            .samples,
        [Vec::<u8>::new()]
    );
    assert!(collector.samples.capacity() > 0);
    collector.destroy();
    assert!(collector.fm_sketch.is_none());
    assert!(collector.cmsketch.is_none());
    assert!(collector.samples.is_empty());
    assert_eq!(collector.samples.capacity(), 0, "Go assigns Samples=nil");
    assert_eq!(collector.count, 0);
    assert!(!collector.is_merger);
}

#[test]
fn source_collect_null_wraps_and_returns_before_every_other_mutation() {
    let mut collector = LegacySampleCollector {
        null_count: i64::MAX,
        count: 7,
        seen_values: 11,
        total_size: 13,
        max_sample_size: 1,
        ..LegacySampleCollector::default()
    };
    collector
        .collect(Datum::Null, |_| Err::<Vec<u8>, _>("must not encode NULL"))
        .unwrap();
    assert_eq!(collector.null_count, i64::MIN);
    assert_eq!(collector.count, 7);
    assert_eq!(collector.seen_values, 11);
    assert_eq!(collector.total_size, 13);
    assert!(collector.samples.is_empty());
}

#[test]
fn source_collect_error_and_nil_fm_expose_go_partial_receiver_mutation() {
    let mut encoder_error = LegacySampleCollector {
        fm_sketch: Some(FmSketch::new(8)),
        cmsketch: Some(CmsSketch::new(2, 4)),
        max_sample_size: 1,
        ..LegacySampleCollector::default()
    };
    let result = encoder_error.collect(Datum::Int(1), |_| Err("encode failed"));
    assert_eq!(result, Err("encode failed"));
    assert_eq!(encoder_error.count, 1);
    assert_eq!(encoder_error.seen_values, 0);
    assert_eq!(encoder_error.total_size, 0);
    assert_eq!(encoder_error.cmsketch.as_ref().unwrap().total_count(), 0);
    assert!(encoder_error.samples.is_empty());

    let mut nil_fm = LegacySampleCollector {
        max_sample_size: 1,
        ..LegacySampleCollector::default()
    };
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = nil_fm.collect(Datum::Int(1), |_| Ok::<_, ()>(vec![8, 2]));
    }));
    assert!(panic.is_err());
    assert_eq!(
        nil_fm.count, 1,
        "Count increments before nil FM dereference"
    );
    assert_eq!(nil_fm.seen_values, 0);
}

#[derive(Default)]
struct LegacyWords {
    uint64_calls: usize,
    uint32_calls: usize,
}

impl LegacySampleRng for LegacyWords {
    fn uint64_n(&mut self, _upper: u64) -> u64 {
        self.uint64_calls += 1;
        0
    }

    fn uint32_n(&mut self, _upper: u32) -> u32 {
        self.uint32_calls += 1;
        0
    }
}

#[test]
fn source_nonpositive_legacy_reservoir_still_draws_uint64() {
    for max_sample_size in [0, -1] {
        let mut collector = LegacySampleCollector {
            fm_sketch: Some(FmSketch::new(8)),
            max_sample_size,
            ..LegacySampleCollector::default()
        };
        let mut rng = LegacyWords::default();
        collector
            .collect_with_rng(
                Datum::Bytes(vec![1]),
                |_| Ok::<_, ()>(vec![tidb_codec::BYTES_FLAG, 1]),
                &mut rng,
            )
            .unwrap();
        assert_eq!(rng.uint64_calls, 1);
        assert_eq!(rng.uint32_calls, 0);
        assert!(collector.samples.is_empty());
    }
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

    let mut nil_cms_destination = make(1);
    nil_cms_destination.cmsketch = None;
    nil_cms_destination.merge(&make(2));
    assert!(nil_cms_destination.cmsketch.is_none());
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
        handle: None,
        ordinal: 0,
    });
    collector.samples.push(LegacySampleItem {
        value: Datum::Bytes(vec![0; MAX_SAMPLE_VALUE_LENGTH + 1]),
        handle: None,
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
fn source_proto_accepts_nil_fm_but_to_proto_normalizes_it_to_empty_message() {
    let decoded = legacy_sample_collector_from_proto(&LegacySampleCollectorProto {
        fm_sketch: None,
        ..LegacySampleCollectorProto::default()
    })
    .unwrap();
    assert!(decoded.fm_sketch.is_none());
    assert_eq!(
        legacy_sample_collector_to_proto(&decoded)
            .unwrap()
            .fm_sketch,
        Some(Default::default())
    );
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
            handle: None,
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

#[test]
fn source_extract_topn_zero_returns_before_reading_or_mutating_the_collector() {
    let mut collector = LegacySampleCollector {
        top_n: Some(tidb_stats::TopN::new(3)),
        count: 7,
        ..LegacySampleCollector::default()
    };
    collector
        .extract_topn(0, |_| Err::<Vec<u8>, _>("normalizer must not run"))
        .unwrap();
    assert_eq!(collector.count, 7);
    assert_eq!(collector.top_n.as_ref().unwrap().num(), 0);
    assert!(collector.cmsketch.is_none());
}

#[test]
fn source_calc_total_size_empty_resets_the_accumulator() {
    let mut collector = LegacySampleCollector {
        total_size: i64::MIN,
        ..LegacySampleCollector::default()
    };
    collector.calculate_total_size();
    assert_eq!(collector.total_size, 0);
}

#[test]
fn source_extract_topn_keeps_the_two_thirds_candidate_tail() {
    let mut collector = LegacySampleCollector {
        cmsketch: Some(CmsSketch::new(5, 2_048)),
        ..LegacySampleCollector::default()
    };
    for value in [b'a', b'a', b'a', b'b', b'b', b'b', b'c', b'c', b'd', b'd'] {
        collector.cmsketch.as_mut().unwrap().insert_bytes(&[value]);
        collector.samples.push(LegacySampleItem {
            value: Datum::Bytes(vec![value]),
            handle: None,
            ordinal: 0,
        });
    }
    let mut normalized_in_order = Vec::new();
    tidb_stats::sample_collector::LegacySampleCollector::extract_topn_with_tie_stabilization(
        &mut collector,
        2,
        true,
        |bytes| {
            normalized_in_order.push(bytes.to_vec());
            Ok::<_, ()>(bytes.to_vec())
        },
    )
    .unwrap();
    assert_eq!(normalized_in_order, [b"a", b"b", b"c", b"d"]);
    assert_eq!(
        collector.top_n.as_ref().unwrap().num(),
        4,
        "Go can retain up to twice the requested TopN at the two-thirds cutoff"
    );
}

#[test]
fn source_extract_topn_error_exposes_completed_prefix_mutation() {
    let mut collector = LegacySampleCollector {
        cmsketch: Some(CmsSketch::new(5, 2_048)),
        ..LegacySampleCollector::default()
    };
    for value in [b'a', b'a', b'a', b'b', b'b'] {
        collector.cmsketch.as_mut().unwrap().insert_bytes(&[value]);
        collector.samples.push(LegacySampleItem {
            value: Datum::Bytes(vec![value]),
            handle: None,
            ordinal: 0,
        });
    }
    let result =
        tidb_stats::sample_collector::LegacySampleCollector::extract_topn_with_tie_stabilization(
            &mut collector,
            2,
            true,
            |bytes| {
                if bytes == b"b" {
                    Err("second candidate failed")
                } else {
                    Ok(bytes.to_vec())
                }
            },
        );
    assert_eq!(result, Err("second candidate failed"));
    let top_n = collector.top_n.as_ref().expect("Go initializes TopN first");
    assert_eq!(top_n.num(), 1);
    assert_eq!(top_n.entries()[0].encoded, b"a");
    assert_eq!(top_n.entries()[0].count, 3);
    assert_eq!(
        collector.cmsketch.as_ref().unwrap().total_count(),
        2,
        "candidate one was subtracted before candidate two failed"
    );
}

#[test]
fn source_sample_builder_collects_pk_columns_and_stops_on_empty_chunk() {
    let builder = LegacySampleBuilder {
        column_count: 2,
        max_sample_size: 8,
        max_fm_sketch_size: 16,
        cmsketch_depth: 2,
        cmsketch_width: 8,
        collated_columns: vec![false, true],
    };
    let mut primary_key = SortedHistogramBuilder::new(4, 1, 2);
    let chunks = vec![
        LegacyRecordChunk {
            field_count: 3,
            rows: vec![
                vec![Datum::Int(1), Datum::Null, Datum::Bytes(vec![9, 1])],
                vec![
                    Datum::Int(2),
                    Datum::Bytes(vec![8, 2]),
                    Datum::Bytes(vec![9, 3]),
                ],
            ],
        },
        LegacyRecordChunk {
            field_count: 3,
            rows: Vec::new(),
        },
        LegacyRecordChunk {
            field_count: 3,
            rows: vec![vec![
                Datum::Int(99),
                Datum::Bytes(vec![8, 99]),
                Datum::Bytes(vec![9, 99]),
            ]],
        },
    ];
    let collectors = builder
        .collect_column_stats(
            chunks,
            Some(&mut primary_key),
            |_, datum, collated| {
                let value_bytes = match &datum {
                    Datum::Bytes(bytes) => {
                        if collated {
                            vec![bytes[0], bytes[1].wrapping_add(10)]
                        } else {
                            bytes.clone()
                        }
                    }
                    _ => unreachable!(),
                };
                Ok::<_, ()>(Datum::Bytes(value_bytes))
            },
            |datum| Ok::<_, ()>(tidb_codec::encode_value(std::slice::from_ref(datum)).unwrap()),
        )
        .unwrap();
    assert_eq!(primary_key.count(), 2);
    assert_eq!(collectors[0].null_count, 1);
    assert_eq!(collectors[0].count, 1);
    assert_eq!(collectors[1].count, 2);
    assert_eq!(collectors[1].samples[0].value.go_bytes(), [9, 11]);
    assert_eq!(collectors[1].samples[1].value.go_bytes(), [9, 13]);
    assert!(collectors
        .iter()
        .all(|collector| collector.cmsketch.is_some()));
}

#[test]
fn source_sample_builder_checks_zero_fields_only_after_nonempty_chunk() {
    let builder = LegacySampleBuilder {
        column_count: 0,
        max_sample_size: 0,
        max_fm_sketch_size: 1,
        cmsketch_depth: 0,
        cmsketch_width: 1,
        collated_columns: Vec::new(),
    };
    let empty_first = builder
        .collect_column_stats(
            [LegacyRecordChunk {
                field_count: 0,
                rows: Vec::new(),
            }],
            None,
            |_, datum, _| Ok::<_, ()>(datum),
            |_| Ok::<_, ()>(Vec::new()),
        )
        .unwrap();
    assert!(empty_first.is_empty());

    let nonempty = builder.collect_column_stats(
        [LegacyRecordChunk {
            field_count: 0,
            rows: vec![Vec::new()],
        }],
        None,
        |_, datum, _| Ok::<_, ()>(datum),
        |_| Ok::<_, ()>(Vec::new()),
    );
    assert!(matches!(
        nonempty,
        Err(LegacySampleBuilderError::ZeroFields)
    ));
}

#[test]
fn source_sample_builder_skips_prepare_and_fm_encoding_for_null() {
    let builder = LegacySampleBuilder {
        column_count: 1,
        max_sample_size: 1,
        max_fm_sketch_size: 8,
        cmsketch_depth: 0,
        cmsketch_width: 0,
        collated_columns: vec![true],
    };
    let callbacks = std::cell::Cell::new(0);
    let collectors = builder
        .collect_column_stats(
            [LegacyRecordChunk {
                field_count: 1,
                rows: vec![vec![Datum::Null]],
            }],
            None,
            |_, _, _| {
                callbacks.set(callbacks.get() + 1);
                Err::<Datum, _>("prepare must not run")
            },
            |_| {
                callbacks.set(callbacks.get() + 1);
                Err::<Vec<u8>, _>("FM encode must not run")
            },
        )
        .unwrap();
    assert_eq!(callbacks.get(), 0);
    assert_eq!(collectors[0].null_count, 1);
}

#[test]
fn source_sample_builder_collator_gate_and_index_order_match() {
    let uncollated = LegacySampleBuilder {
        column_count: 1,
        max_sample_size: 1,
        max_fm_sketch_size: 8,
        cmsketch_depth: 0,
        cmsketch_width: 0,
        collated_columns: vec![false],
    };
    let prepare_calls = std::cell::Cell::new(0);
    let collectors = uncollated
        .collect_column_stats(
            [LegacyRecordChunk {
                field_count: 1,
                rows: vec![vec![Datum::Bytes(vec![1])]],
            }],
            None,
            |_, _, _| {
                prepare_calls.set(prepare_calls.get() + 1);
                Err::<Datum, _>("uncollated values must not be prepared")
            },
            |datum| Ok::<_, &str>(tidb_codec::encode_value(std::slice::from_ref(datum)).unwrap()),
        )
        .unwrap();
    assert_eq!(prepare_calls.get(), 0);
    assert_eq!(collectors[0].count, 1);

    let collated = LegacySampleBuilder {
        collated_columns: vec![true],
        ..uncollated.clone()
    };
    let prepare_calls = std::cell::Cell::new(0);
    let encode_calls = std::cell::Cell::new(0);
    let collectors = collated
        .collect_column_stats(
            [LegacyRecordChunk {
                field_count: 1,
                rows: vec![vec![Datum::Null], vec![Datum::Bytes(vec![2])]],
            }],
            None,
            |_, datum, _| {
                prepare_calls.set(prepare_calls.get() + 1);
                Ok::<_, ()>(datum)
            },
            |_| {
                encode_calls.set(encode_calls.get() + 1);
                Ok::<_, ()>(Vec::new())
            },
        )
        .unwrap();
    assert_eq!(prepare_calls.get(), 1);
    assert_eq!(encode_calls.get(), 1);
    assert_eq!(collectors[0].null_count, 1);
    assert_eq!(collectors[0].count, 1);

    let missing_collator = LegacySampleBuilder {
        collated_columns: Vec::new(),
        ..uncollated
    };
    let panic = std::panic::catch_unwind(|| {
        let _ = missing_collator.collect_column_stats(
            [LegacyRecordChunk {
                field_count: 1,
                rows: vec![vec![Datum::Null]],
            }],
            None,
            |_, datum, _| Ok::<_, ()>(datum),
            |_| Ok::<_, ()>(Vec::new()),
        );
    });
    assert!(
        panic.is_err(),
        "Go indexes Collators[i] before testing whether the datum is NULL"
    );
}

#[test]
fn source_sample_builder_cms_dimensions_require_two_positive_operands() {
    for (depth, width, expected) in [(0, 8, false), (8, 0, false), (-1, 8, false), (8, 4, true)] {
        let builder = LegacySampleBuilder {
            column_count: 1,
            max_sample_size: 0,
            max_fm_sketch_size: 1,
            cmsketch_depth: depth,
            cmsketch_width: width,
            collated_columns: vec![false],
        };
        let collectors = builder
            .collect_column_stats(
                [LegacyRecordChunk {
                    field_count: 1,
                    rows: Vec::new(),
                }],
                None,
                |_, datum, _| Ok::<_, ()>(datum),
                |_| Ok::<_, ()>(Vec::new()),
            )
            .unwrap();
        assert_eq!(collectors[0].cmsketch.is_some(), expected);
    }
}

#[test]
fn source_row_to_datums_uses_field_count_not_physical_width() {
    assert_eq!(
        legacy_row_to_datums(&[Datum::Int(1), Datum::Int(2)], 1),
        [Datum::Int(1)]
    );
}

#[test]
#[should_panic]
fn source_row_to_datums_panics_when_declared_field_is_absent() {
    let _ = legacy_row_to_datums(&[Datum::Int(1)], 2);
}
