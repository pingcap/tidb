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

use tidb_datatype::Datum;
use tidb_stats::row_sample_collector::{
    RowSampleCollector, RowSampleCollectorProto, RowSampleProto, SampleMemoryQuota, SamplePolicy,
    ScannedRow, SlotValue,
};
use tidb_stats::FmSketchProto;

fn offer(collector: &mut RowSampleCollector, value: i64) -> Result<(), String> {
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
        })
        .map_err(|exceeded| exceeded.to_string())
}

#[test]
fn a_bernoulli_sample_past_the_quota_fails_the_statement() {
    // One row's worth of budget: the second kept row is over it.
    let mut collector = RowSampleCollector::with_memory_quota(
        1,
        SamplePolicy::Bernoulli { sample_rate: 1.0 },
        SampleMemoryQuota::from_setting(std::mem::size_of::<Datum>() as i64),
    );
    offer(&mut collector, 1).expect("the first kept row fits the quota");
    let error = (2..=10_000)
        .find_map(|value| offer(&mut collector, value).err())
        .expect("a full-rate Bernoulli sample of 10000 rows outgrows a one-row quota");
    assert_eq!(
        error,
        "analyze panic due to memory quota exceeds, please try with smaller \
         samplerate(refer to 110000/count)"
    );
}

#[test]
fn a_reservoir_sample_past_the_quota_fails_the_statement_too() {
    let mut collector = RowSampleCollector::with_memory_quota(
        1,
        SamplePolicy::Reservoir {
            max_sample_size: 10_000,
        },
        SampleMemoryQuota::from_setting(std::mem::size_of::<Datum>() as i64),
    );
    offer(&mut collector, 1).expect("the first kept row fits the quota");
    assert!(offer(&mut collector, 2).is_err());
}

#[test]
fn gos_default_quota_is_no_bound_at_all() {
    // `vardef.DefTiDBMemQuotaAnalyze = -1`, which the tracker reads as
    // unlimited -- so this node's default must keep every row a full-rate
    // sample selects, exactly as Go's does.
    assert_eq!(SampleMemoryQuota::from_setting(-1).bytes(), None);
    assert_eq!(SampleMemoryQuota::from_setting(0).bytes(), None);
    assert_eq!(SampleMemoryQuota::unlimited().bytes(), None);
    assert_eq!(SampleMemoryQuota::from_setting(4096).bytes(), Some(4096));

    let mut collector = RowSampleCollector::new(1, SamplePolicy::Bernoulli { sample_rate: 1.0 });
    for value in 0..5_000 {
        offer(&mut collector, value).expect("the default quota bounds nothing");
    }
    let (scanned, _, sampled) = collector.into_parts();
    assert_eq!(scanned, 5_000);
    assert_eq!(sampled.len(), 5_000);
}

#[test]
fn source_bernoulli_merge_combines_scan_facts_and_samples() {
    let policy = SamplePolicy::Bernoulli { sample_rate: 1.0 };
    let mut left = RowSampleCollector::new(1, policy);
    let mut right = RowSampleCollector::new(1, policy);
    offer(&mut left, 1).unwrap();
    offer(&mut right, 2).unwrap();
    left.merge(right).unwrap();
    let (count, slots, samples) = left.into_parts();
    assert_eq!(count, 2);
    assert_eq!(slots[0].total_size, 16);
    assert_eq!(slots[0].ndv, 2);
    assert_eq!(samples.len(), 2);
}

#[test]
fn source_destroy_resets_rows_slots_and_sketches() {
    let mut collector = RowSampleCollector::new(1, SamplePolicy::Bernoulli { sample_rate: 1.0 });
    offer(&mut collector, 1).unwrap();
    collector.destroy();
    let (count, slots, samples) = collector.into_parts();
    assert_eq!(count, 0);
    assert_eq!(slots[0].null_count, 0);
    assert_eq!(slots[0].total_size, 0);
    assert_eq!(slots[0].ndv, 0);
    assert!(samples.is_empty());
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
        fm_sketches: vec![FmSketchProto::default(), FmSketchProto::default()],
        total_sizes: vec![10, 20],
    };
    let collector = RowSampleCollector::from_proto(
        &proto,
        SamplePolicy::Reservoir { max_sample_size: 2 },
        SampleMemoryQuota::unlimited(),
    )
    .unwrap();
    // 2 * (2 Datum structs * 72 + empty item 48 + reference 8) + 7 payload bytes.
    assert_eq!(collector.consumed_memory_bytes(), 407);
    assert_eq!(collector.to_proto(), proto);

    assert!(RowSampleCollector::from_proto(
        &proto,
        SamplePolicy::Reservoir { max_sample_size: 2 },
        SampleMemoryQuota::from_setting(406),
    )
    .is_err());
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
        })
        .unwrap();
    assert_eq!(
        collector.to_proto().samples[0].row[0],
        [tidb_codec::NIL_FLAG]
    );
}

#[test]
#[should_panic(expected = "row-sample protobuf slot counts differ")]
fn source_malformed_row_sample_proto_slot_lengths_are_rejected() {
    let _ = RowSampleCollector::from_proto(
        &RowSampleCollectorProto {
            null_counts: vec![0],
            ..RowSampleCollectorProto::default()
        },
        SamplePolicy::Bernoulli { sample_rate: 1.0 },
        SampleMemoryQuota::unlimited(),
    );
}
