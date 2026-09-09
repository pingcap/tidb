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

//! Byte-literal and numeric anchors that upgrade the structural ports of
//! `pkg/distsql/request_builder_test.go`.
//!
//! The sibling `request_builder_source.rs` derives its expectations with the
//! same codec primitives as the implementation, exactly like Go's own
//! `getExpectedRanges` helper does. This module adds the complementary
//! anchors that cannot be derived locally: the checked-in byte arrays every
//! Go `require.Equal(t, expect[i], actual[i])` compares against (copied
//! verbatim from `origin/master`'s `TestTableRangesToKVRanges` /
//! `TestIndexRangesToKVRanges`), plus the numeric `kv.Request` field values
//! those tests pin through their struct literals.

use tidb_distsql::{
    index_ranges_to_kv_ranges, table_ranges_to_kv_ranges, DatumRange, DistSqlContext,
    IsolationLevel, KvPriority, MIN_ALLOWED_MAX_PAGING_SIZE, MIN_PAGING_SIZE, ReplicaReadType,
    RequestBuilder, RequestEnvelope, RequestType,
};

fn datum(value: i64) -> tidb_datatype::Datum {
    tidb_datatype::Datum::Int(value)
}

fn source_ranges() -> Vec<DatumRange> {
    vec![
        DatumRange::inclusive(vec![datum(1)], vec![datum(2)]),
        DatumRange {
            low: vec![datum(2)],
            high: vec![datum(4)],
            low_exclude: true,
            high_exclude: true,
        },
        DatumRange {
            low: vec![datum(4)],
            high: vec![datum(19)],
            low_exclude: false,
            high_exclude: true,
        },
        DatumRange {
            low: vec![datum(19)],
            high: vec![datum(32)],
            low_exclude: true,
            high_exclude: false,
        },
        DatumRange {
            low: vec![datum(34)],
            high: vec![datum(34)],
            low_exclude: true,
            high_exclude: false,
        },
    ]
}

/// Record-key header shared by every expectation: `t` + complemented-int64
/// table id 13 + `_r`, before the encoded boundary.
fn record_prefix() -> Vec<u8> {
    vec![
        0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0d, 0x5f, 0x72,
    ]
}

fn master_table_range(low_tail: u8, high_tail: u8) -> (Vec<u8>, Vec<u8>) {
    let mut start = record_prefix();
    start.extend_from_slice(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, low_tail]);
    let mut end = record_prefix();
    end.extend_from_slice(&[0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, high_tail]);
    (start, end)
}

/// Source: `pkg/distsql/request_builder_test.go::TestTableRangesToKVRanges`
/// (`pkg/distsql/request_builder.go:707 TableRangesToKVRanges`).
///
/// Five signed ranger ranges over table id 13 must land in these exact bytes;
/// the exclusion adjustments and PrefixNext expansion are anchored by
/// `origin/master` literals rather than re-derived encodings.
#[test]
fn table_ranges_to_kv_ranges_matches_master_byte_literals() {
    let actual = table_ranges_to_kv_ranges(13, &source_ranges()).expect("table ranges");
    let expected = [
        master_table_range(0x01, 0x03),
        master_table_range(0x03, 0x04),
        master_table_range(0x04, 0x13),
        master_table_range(0x14, 0x21),
        master_table_range(0x23, 0x23),
    ];
    assert_eq!(actual.len(), expected.len());
    for (range, (start_key, end_key)) in actual.iter().zip(expected) {
        assert_eq!(range.start_key.as_slice(), start_key.as_slice());
        assert_eq!(range.end_key.as_slice(), end_key.as_slice());
    }
}

/// Index-seek-key header: `t` + table id 12 + `_i` + complemented-int64 index
/// id 15, before the encoded boundary.
fn index_seek_prefix() -> Vec<u8> {
    vec![
        0x74, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x5f, 0x69, 0x80, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x0f,
    ]
}

fn master_index_range(low_tail: u8, high_tail: u8) -> (Vec<u8>, Vec<u8>) {
    let mut start = index_seek_prefix();
    start.extend_from_slice(&[0x03, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, low_tail]);
    let mut end = index_seek_prefix();
    end.extend_from_slice(&[0x03, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, high_tail]);
    (start, end)
}

/// Source: `pkg/distsql/request_builder_test.go::TestIndexRangesToKVRanges`
/// (`pkg/distsql/request_builder.go:969 IndexRangesToKVRanges`).
///
/// Same five ranges against table id 12, index id 15. The 28-byte literals
/// pin the full seek-key layout, including the mem-comparable int-value
/// prefix `0x03` the Go source keeps inside every encoded boundary.
#[test]
fn index_ranges_to_kv_ranges_matches_master_byte_literals() {
    let actual = index_ranges_to_kv_ranges(&[12], 15, &source_ranges()).expect("index ranges");
    let expected = [
        master_index_range(0x01, 0x03),
        master_index_range(0x03, 0x04),
        master_index_range(0x04, 0x13),
        master_index_range(0x14, 0x21),
        master_index_range(0x23, 0x23),
    ];
    assert_eq!(actual.len(), 1);
    assert_eq!(actual[0].len(), expected.len());
    for (range, (start_key, end_key)) in actual[0].iter().zip(expected) {
        assert_eq!(range.start_key.as_slice(), start_key.as_slice());
        assert_eq!(range.end_key.as_slice(), end_key.as_slice());
    }
}

/// Source: request-builder default fields across
/// `TestRequestBuilder1..8` (`pkg/distsql/request_builder_test.go:249-798`).
///
/// Every Go expectation lists `Tp`, `IsolationLevel`, `Priority`,
/// `ReplicaRead`, `ResourceGroupName`, and paging bounds as concrete values.
/// The sibling port checks these only partially through field-by-field
/// comparisons that mirror several expectations at once; this pins the
/// complete default projection Go states in all eight literals.
#[test]
fn builder_session_projection_pins_the_go_request_field_defaults() {
    // DAG/Analyze/Checksum wire types from the expected structs.
    assert_eq!(RequestType::Dag.raw(), 103);
    assert_eq!(RequestType::Analyze.raw(), 104);
    assert_eq!(RequestType::Checksum.raw(), 105);

    // TestRequestBuilder5/6 set their own options; builders 1-4 and 7-8 run
    // SetFromSessionVars over a default session context, expecting exactly
    // Concurrency=DefDistSQLScanConcurrency, IsolationLevel=SI, Priority=0,
    // NotFillCache=false, ReplicaRead=leader, scope=global, name=default.
    let mut builder = RequestBuilder::new();
    builder
        .set_non_partitioned_key_ranges(Vec::new())
        .set_dag_request(RequestEnvelope::new(Vec::new()), vec![])
        .set_desc(false)
        .set_keep_order(false)
        .set_from_context(&DistSqlContext::new());
    let request = builder.build().expect("session-projected request");

    assert_eq!(request.request_type, RequestType::Dag);
    assert_eq!(request.isolation_level, IsolationLevel::Snapshot);
    assert_eq!(request.priority, KvPriority::Normal);
    assert_eq!(request.replica_read, ReplicaReadType::Leader);
    assert!(!request.not_fill_cache);
    assert_eq!(request.resource_group_name, "default");
    assert_eq!(request.start_ts, 0);
    assert_eq!(
        request.paging.min_size,
        u64::try_from(MIN_PAGING_SIZE).unwrap_or(request.paging.min_size)
    );
    assert_eq!(
        request.paging.max_size,
        u64::try_from(MIN_ALLOWED_MAX_PAGING_SIZE).unwrap_or(request.paging.max_size)
    );

    // `expect.Paging.MinPagingSize = paging.MinPagingSize` /
    // `MaxPagingSize = paging.MinAllowedMaxPagingSize` keep the Go constants.
    assert_eq!(MIN_PAGING_SIZE, 128);
    assert_eq!(MIN_ALLOWED_MAX_PAGING_SIZE, 50_000);
}
