// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! `tidb_mem_quota_analyze` bounds what one `ANALYZE` keeps.
//!
//! The Bernoulli sampler has no cap of its own: its kept-row count is
//! `sample_rate * table_rows`, and the rate comes from the client
//! (`ANALYZE ... WITH 1.0 SAMPLERATE`), so on a large table it materialises
//! every row. Go bounds that with the analyze memory quota and *aborts* the
//! statement when it is passed, rather than keeping fewer rows -- a silently
//! smaller sample than the rate describes is a wrong histogram.
//!
//! Captured from a real TiDB (`set global tidb_mem_quota_analyze = 1`, then
//! `ANALYZE TABLE zzm` over 4000 rows):
//!
//! ```text
//! ERROR: analyze panic due to memory quota exceeds, please try with smaller
//!        samplerate(refer to 110000/count)
//! ```
//!
//! and with the default `-1`, no bound applies at all.

use tidb_datatype::{BinaryJSON, Datum, VectorFloat32};
use tidb_stats::row_sample_collector::{
    RowSampleCollector, RowSampleCollectorProto, RowSampleProto, RowSampleRng,
    SamplePolicy, SampledRow, ScannedRow, SlotStats, SlotValue,
};
use tidb_stats::FmSketchProto;
use tidb_txnkv::{Handle, IntHandle};

#[derive(Default)]
struct WordRng {
    words: std::collections::VecDeque<i64>,
    consumed: usize,
}

impl WordRng {
    fn from(words: impl IntoIterator<Item = i64>) -> Self {
        Self {
            words: words.into_iter().collect(),
            consumed: 0,
        }
    }
}

impl RowSampleRng for WordRng {
    fn int63(&mut self) -> i64 {
        self.consumed += 1;
        self.words.pop_front().expect("scripted Int63 exhausted")
    }
}

fn source_handle(columns: &[Datum]) -> Result<Handle, ()> {
    Ok(IntHandle::new(match columns.first() {
        Some(Datum::Int(value)) => *value,
        Some(value) => value.go_bytes().first().copied().unwrap_or_default() as i64,
        None => 0,
    })
    .into())
}

fn finish(collector: RowSampleCollector) -> (i64, Vec<SlotStats>, Vec<SampledRow>) {
    collector
        .into_parts(source_handle)
        .expect("test handle construction is infallible")
}

fn offer(collector: &mut RowSampleCollector, value: i64) {
    let encoded = value.to_be_bytes().to_vec();
    let columns = [Datum::Int(value)];
    let slots = [SlotValue {
        encoded_value: &encoded,
        size: encoded.len() as i64,
        is_null: false,
    }];
    collector
        .collect(&ScannedRow {
            columns: &columns,
            slots: &slots,
        });
}

/// A free-standing ANALYZE tracker with no limit (Go `memory.NewTracker(id, -1)`).
fn tracker() -> std::sync::Arc<tidb_memory::Tracker> {
    tidb_memory::Tracker::new(tidb_memory::LABEL_FOR_ANALYZE_MEMORY, -1)
}

#[test]
fn source_bernoulli_merge_combines_scan_facts_and_samples() {
    let policy = SamplePolicy::Bernoulli { sample_rate: 1.0 };
    let mut left = RowSampleCollector::new(1, policy);
    let mut right = RowSampleCollector::new(1, policy);
    offer(&mut left, 1);
    offer(&mut right, 2);
    left.merge(right);
    let (count, slots, samples) = finish(left);
    assert_eq!(count, 2);
    assert_eq!(slots[0].total_size, 16);
    assert_eq!(slots[0].ndv, 2);
    assert_eq!(samples.len(), 2);
}

#[test]
fn source_destroy_releases_only_the_fm_sketch_slice() {
    let mut collector = RowSampleCollector::new(1, SamplePolicy::Bernoulli { sample_rate: 1.0 });
    offer(&mut collector, 1);
    let before = collector.to_proto();
    collector.destroy();
    let after = collector.to_proto();
    assert_eq!(after.count, before.count);
    assert_eq!(after.null_counts, before.null_counts);
    assert_eq!(after.total_sizes, before.total_sizes);
    assert_eq!(after.samples, before.samples);
    assert_eq!(before.fm_sketches.len(), 1);
    assert!(after.fm_sketches.is_empty());
}

#[test]
fn source_row_sample_proto_restores_weights_bytes_and_memory_accounting() {
    let proto = RowSampleCollectorProto {
        samples: vec![
            RowSampleProto {
                row: vec![vec![0], vec![1, 2]],
                weight: 9,
            },
            RowSampleProto {
                row: vec![vec![3, 4, 5], vec![0]],
                weight: 7,
            },
        ],
        null_counts: vec![1, 0],
        count: 8,
        fm_sketches: vec![
            Some(FmSketchProto::default()),
            Some(FmSketchProto::default()),
        ],
        total_sizes: vec![10, 20],
    };
    let mem_tracker = tracker();
    let collector = RowSampleCollector::from_proto(
        &proto,
        SamplePolicy::Reservoir { max_sample_size: 2 },
        &mem_tracker,
    );
    // Go accounts for 400 mandatory bytes, 7 payload bytes, and 4 binary
    // collation names of 6 bytes each.
    assert_eq!(collector.go_mem_size(), 431);
    assert_eq!(mem_tracker.bytes_consumed(), 431);
    assert_eq!(collector.to_proto(), proto);

    let bytes = proto.samples[0].row[0].as_ptr();
    let mem_tracker = tracker();
    let owned = RowSampleCollector::from_owned_proto(
        proto,
        SamplePolicy::Reservoir { max_sample_size: 2 },
        &mem_tracker,
    );
    assert_eq!(owned.go_mem_size(), 431);
    assert_eq!(mem_tracker.bytes_consumed(), 431);
    let roundtrip = owned.into_proto();
    assert_eq!(roundtrip, collector.to_proto());
    assert_eq!(roundtrip.samples[0].row[0].as_ptr(), bytes);
}

#[test]
fn source_row_samples_to_proto_encodes_null_as_the_nil_flag() {
    let mut collector = RowSampleCollector::new(1, SamplePolicy::Bernoulli { sample_rate: 1.0 });
    let columns = [Datum::Null];
    let slots = [SlotValue {
        encoded_value: &[0],
        size: 1,
        is_null: true,
    }];
    collector
        .collect(&ScannedRow {
            columns: &columns,
            slots: &slots,
        });
    assert_eq!(
        collector.to_proto().samples[0].row[0],
        [tidb_codec::NIL_FLAG]
    );
}

#[test]
fn source_row_samples_to_proto_uses_get_bytes_for_non_byte_datums() {
    let mut collector = RowSampleCollector::new(1, SamplePolicy::Bernoulli { sample_rate: 1.0 });
    let columns = [Datum::Int(1)];
    let slots = [SlotValue {
        encoded_value: &[tidb_codec::INT_FLAG, 2],
        size: -1,
        is_null: false,
    }];
    collector
        .collect(&ScannedRow {
            columns: &columns,
            slots: &slots,
        });
    assert_eq!(collector.to_proto().samples[0].row[0], Vec::<u8>::new());
}

#[test]
fn source_row_samples_to_proto_reads_json_and_vector_backing_bytes() {
    let json = BinaryJSON::parse(r#"{"answer":42}"#).unwrap();
    let vector = VectorFloat32::must_create(vec![1.0]);
    let columns = [
        Datum::Json(json.clone()),
        Datum::VectorFloat32(vector.clone()),
    ];
    let slots: [SlotValue<'_>; 0] = [];
    let mut collector = RowSampleCollector::new(0, SamplePolicy::Bernoulli { sample_rate: 1.0 });
    collector
        .collect(&ScannedRow {
            columns: &columns,
            slots: &slots,
        });
    assert_eq!(
        collector.to_proto().samples[0].row,
        [json.value().to_vec(), vector.serialize()]
    );
}

#[test]
fn source_from_proto_accepts_independent_parallel_field_lengths_and_nil_fm() {
    let collector = RowSampleCollector::from_proto(
        &RowSampleCollectorProto {
            null_counts: vec![7],
            total_sizes: vec![],
            fm_sketches: vec![None, Some(FmSketchProto::default())],
            ..RowSampleCollectorProto::default()
        },
        SamplePolicy::Bernoulli { sample_rate: 1.0 },
        &tracker(),
    );
    let restored = collector.to_proto();
    assert_eq!(restored.null_counts, [7]);
    assert!(restored.total_sizes.is_empty());
    assert_eq!(restored.fm_sketches.len(), 2);
    assert_eq!(restored.fm_sketches[0], Some(FmSketchProto::default()));
    assert_eq!(restored.fm_sketches[1], Some(FmSketchProto::default()));
}

#[test]
fn source_row_scan_int64_counters_wrap_at_boundaries() {
    let proto = RowSampleCollectorProto {
        samples: Vec::new(),
        null_counts: vec![0, i64::MAX],
        count: i64::MAX,
        fm_sketches: vec![
            Some(FmSketchProto::default()),
            Some(FmSketchProto::default()),
        ],
        total_sizes: vec![i64::MAX, 0],
    };
    let mut collector = RowSampleCollector::from_proto(
        &proto,
        SamplePolicy::Bernoulli { sample_rate: 0.0 },
        &tracker(),
    );
    let columns = [Datum::Bytes(vec![1]), Datum::Null];
    let slots = [
        SlotValue {
            encoded_value: &[1],
            size: 1,
            is_null: false,
        },
        SlotValue {
            encoded_value: &[0],
            size: 0,
            is_null: true,
        },
    ];
    let mut rng = WordRng::from([1]);
    collector
        .collect_with_rng(
            &ScannedRow {
                columns: &columns,
                slots: &slots,
            },
            &mut rng,
        );
    let wrapped = collector.to_proto();
    assert_eq!(wrapped.count, i64::MIN);
    assert_eq!(wrapped.total_sizes[0], i64::MIN);
    assert_eq!(wrapped.null_counts[1], i64::MIN);
}

#[test]
fn source_destination_policy_controls_cross_policy_merge() {
    let source_proto = RowSampleCollectorProto {
        samples: vec![RowSampleProto {
            row: vec![vec![9]],
            weight: 9,
        }],
        null_counts: vec![3],
        count: 4,
        fm_sketches: vec![Some(FmSketchProto::default())],
        total_sizes: vec![5],
    };
    let source = RowSampleCollector::from_proto(
        &source_proto,
        SamplePolicy::Reservoir {
            max_sample_size: 99,
        },
        &tracker(),
    );
    let mut bernoulli = RowSampleCollector::new(2, SamplePolicy::Bernoulli { sample_rate: 0.01 });
    bernoulli.merge(source);
    let merged = bernoulli.to_proto();
    assert_eq!(
        merged.samples[0].weight, 9,
        "Bernoulli merge preserves source weight"
    );
    assert_eq!(
        merged.null_counts,
        [3, 0],
        "short source arrays merge as prefixes"
    );
    assert_eq!(merged.total_sizes, [5, 0]);

    let source = RowSampleCollector::from_proto(
        &RowSampleCollectorProto {
            samples: vec![
                RowSampleProto {
                    row: vec![vec![1]],
                    weight: 1,
                },
                RowSampleProto {
                    row: vec![vec![7]],
                    weight: 7,
                },
            ],
            ..RowSampleCollectorProto::default()
        },
        SamplePolicy::Bernoulli { sample_rate: 1.0 },
        &tracker(),
    );
    let mut reservoir = RowSampleCollector::new(0, SamplePolicy::Reservoir { max_sample_size: 1 });
    reservoir.merge(source);
    assert_eq!(reservoir.to_proto().samples[0].weight, 7);
}

#[test]
fn reservoir_merge_fills_before_rejecting_ties() {
    let policy = SamplePolicy::Reservoir { max_sample_size: 4 };
    let restore = |rows: &[(u8, i64)]| {
        RowSampleCollector::from_owned_proto(
            RowSampleCollectorProto {
                samples: rows
                    .iter()
                    .map(|&(value, weight)| RowSampleProto {
                        row: vec![vec![value]],
                        weight,
                    })
                    .collect(),
                ..RowSampleCollectorProto::default()
            },
            policy,
            &tracker(),
        )
    };

    let mut collector = restore(&[(b'a', 5)]);
    collector.merge(restore(&[(b'b', 2), (b'c', 5), (b'd', 2)]));
    assert_eq!(
        collector
            .to_proto()
            .samples
            .iter()
            .map(|sample| sample.row[0][0])
            .collect::<Vec<_>>(),
        b"bdca",
    );

    let incoming = [(b'e', 2), (b'f', 6), (b'g', 2)];
    collector.merge(restore(&incoming));
    let samples = collector.into_proto().samples;
    assert_eq!(
        samples
            .iter()
            .map(|sample| (sample.row[0][0], sample.weight))
            .collect::<Vec<_>>(),
        [(b'd', 2), (b'a', 5), (b'c', 5), (b'f', 6)],
    );

    let mut overflow = restore(&[(b'a', 5)]);
    let input = [
        (b'b', 2),
        (b'c', 5),
        (b'd', 2),
        incoming[0],
        incoming[1],
        incoming[2],
    ];
    overflow.merge(restore(&input));
    assert_eq!(overflow.into_proto().samples, samples);
}

#[test]
fn full_reservoir_materializes_only_admitted_proto_rows() {
    struct CellBytes<'a> {
        bytes: Vec<u8>,
        conversions: &'a std::cell::Cell<usize>,
    }

    impl AsRef<[u8]> for CellBytes<'_> {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl From<CellBytes<'_>> for Vec<u8> {
        fn from(cell: CellBytes<'_>) -> Self {
            cell.conversions.set(cell.conversions.get() + 1);
            cell.bytes
        }
    }

    let policy = SamplePolicy::Reservoir { max_sample_size: 1 };
    let initial = RowSampleCollectorProto {
        samples: vec![RowSampleProto {
            row: vec![vec![5]],
            weight: 5,
        }],
        ..Default::default()
    };
    let incoming = RowSampleCollectorProto {
        count: 3,
        samples: vec![
            RowSampleProto {
                row: vec![vec![5; 16 * 1024]],
                weight: 5,
            },
            RowSampleProto {
                row: vec![vec![6]],
                weight: 6,
            },
            RowSampleProto {
                row: vec![vec![7; 16 * 1024]],
                weight: 6,
            },
        ],
        ..Default::default()
    };

    let eager_tracker = tracker();
    let mut expected = RowSampleCollector::from_proto(&initial, policy, &eager_tracker);
    let fragment = RowSampleCollector::from_proto(&incoming, policy, &eager_tracker);
    let previous = expected.go_mem_size();
    let fragment_size = fragment.go_mem_size();
    expected.merge(fragment);
    eager_tracker.consume(expected.go_mem_size() - previous - fragment_size);

    let conversions = std::cell::Cell::new(0);
    let lazy = RowSampleCollectorProto {
        count: incoming.count,
        null_counts: incoming.null_counts,
        total_sizes: incoming.total_sizes,
        fm_sketches: incoming.fm_sketches,
        samples: incoming
            .samples
            .into_iter()
            .map(|sample| RowSampleProto {
                weight: sample.weight,
                row: sample
                    .row
                    .into_iter()
                    .map(|bytes| CellBytes {
                        bytes,
                        conversions: &conversions,
                    })
                    .collect(),
            })
            .collect(),
    };
    let lazy_tracker = tracker();
    let mut actual = RowSampleCollector::from_owned_proto(initial, policy, &lazy_tracker);
    actual.merge_owned_proto(lazy, &lazy_tracker);

    assert_eq!(conversions.get(), 1);
    assert_eq!(actual.go_mem_size(), expected.go_mem_size());
    assert_eq!(lazy_tracker.bytes_consumed(), eager_tracker.bytes_consumed());
    assert_eq!(actual.into_proto(), expected.into_proto());
}

#[test]
fn source_merge_updates_base_collector_mem_size_by_destination_policy() {
    let make = |weight, payload: usize, policy| {
        RowSampleCollector::from_proto(
            &RowSampleCollectorProto {
                samples: vec![RowSampleProto {
                    row: vec![vec![1; payload]],
                    weight,
                }],
                ..RowSampleCollectorProto::default()
            },
            policy,
            &tracker(),
        )
    };

    let policy = SamplePolicy::Reservoir { max_sample_size: 1 };
    let mut reservoir = make(1, 1, policy);
    let source = make(9, 101, SamplePolicy::Bernoulli { sample_rate: 0.5 });
    let expected = reservoir
        .go_mem_size()
        .wrapping_add(source.go_mem_size())
        .wrapping_mul(1)
        / 2;
    reservoir.merge(source);
    assert_eq!(reservoir.go_mem_size(), expected);

    let mut bernoulli = make(1, 1, SamplePolicy::Bernoulli { sample_rate: 0.1 });
    let source = make(2, 5, SamplePolicy::Reservoir { max_sample_size: 7 });
    let expected = bernoulli.go_mem_size().wrapping_add(source.go_mem_size());
    bernoulli.merge(source);
    assert_eq!(bernoulli.go_mem_size(), expected);
}

#[test]
fn source_go_float64_consumes_int63_without_low_bit_truncation() {
    let columns = [Datum::Int(1)];
    let slots: [SlotValue<'_>; 0] = [];
    let row = ScannedRow {
        columns: &columns,
        slots: &slots,
    };
    let mut collector = RowSampleCollector::new(0, SamplePolicy::Bernoulli { sample_rate: 0.25 });
    let mut half = WordRng::from([1_i64 << 62]);
    collector.collect_with_rng(&row, &mut half);
    assert!(collector.to_proto().samples.is_empty());
    assert_eq!(half.consumed, 1);

    let mut collector = RowSampleCollector::new(0, SamplePolicy::Bernoulli { sample_rate: 0.25 });
    let mut rounded_one_then_zero = WordRng::from([i64::MAX, 0]);
    collector
        .collect_with_rng(&row, &mut rounded_one_then_zero);
    assert_eq!(collector.to_proto().samples.len(), 1);
    assert_eq!(rounded_one_then_zero.consumed, 2);
}

#[test]
fn source_reservoir_consumes_one_int63_and_handle_order_sets_ordinals() {
    let mut collector = RowSampleCollector::new(0, SamplePolicy::Reservoir { max_sample_size: 2 });
    let slots: [SlotValue<'_>; 0] = [];
    let mut rng = WordRng::from([11, 12]);
    for value in [2, 1] {
        let columns = [Datum::Int(value)];
        collector
            .collect_with_rng(
                &ScannedRow {
                    columns: &columns,
                    slots: &slots,
                },
                &mut rng,
            );
    }
    assert_eq!(rng.consumed, 2);
    let (_, _, rows) = finish(collector);
    assert_eq!(rows[0].columns[0], Datum::Int(1));
    assert_eq!(rows[0].ordinal, 0);
    assert_eq!(rows[1].columns[0], Datum::Int(2));
    assert_eq!(rows[1].ordinal, 1);
}

#[test]
fn owned_reservoir_transfers_selected_bytes_and_counts_rejected_rows() {
    let mut collector = RowSampleCollector::new(1, SamplePolicy::Reservoir { max_sample_size: 1 });
    let mut rng = WordRng::from([5, 5, 6]);
    collector.collect_owned_with_rng(
        vec![Datum::Bytes(vec![1, 2])],
        &[SlotValue {
            encoded_value: &[1, 2],
            size: 1,
            is_null: false,
        }],
        &mut rng,
    );
    collector.collect_owned_with_rng(
        vec![Datum::Null],
        &[SlotValue {
            encoded_value: &[0],
            size: 0,
            is_null: true,
        }],
        &mut rng,
    );
    let payload = vec![7; 128];
    let original = payload.as_ptr();
    collector.collect_owned_with_rng(
        vec![Datum::Bytes(payload)],
        &[SlotValue {
            encoded_value: &[3, 4],
            size: 127,
            is_null: false,
        }],
        &mut rng,
    );
    let proto = collector.into_proto();
    assert_eq!(rng.consumed, 3);
    assert_eq!(proto.count, 3);
    assert_eq!(proto.null_counts, [1]);
    assert_eq!(proto.total_sizes, [128]);
    assert_eq!(proto.fm_sketches[0].as_ref().unwrap().hashset.len(), 2);
    assert_eq!(proto.samples.len(), 1);
    assert_eq!(proto.samples[0].weight, 6);
    assert_eq!(proto.samples[0].row[0].as_ptr(), original);
    assert_eq!(proto.samples[0].row[0], [7; 128]);
}

#[test]
fn source_max_fm_sketch_size_is_applied_to_every_slot() {
    let collector = RowSampleCollector::with_fm_sketch_size(
        3,
        SamplePolicy::Bernoulli { sample_rate: 1.0 },
        2,
    );
    assert_eq!(collector.fm_sketch_max_sizes(), [Some(2), Some(2), Some(2)]);
}
