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

//! Direct translations of the table-handle range tests in
//! `pkg/distsql/request_builder_test.go`.

use tidb_codec::{encode_int, encode_key, encode_row_key};
use tidb_datatype::Datum;
use tidb_distsql::{table_handles_to_kv_ranges, KvRequestBuilder};
use tidb_txnkv::{CommonHandle, Handle, IntHandle, PartitionHandle};

fn int(value: i64) -> Handle {
    Handle::Int(IntHandle::new(value))
}

fn partition(partition_id: i64, value: i64) -> Handle {
    Handle::Partition(PartitionHandle::new(partition_id, IntHandle::new(value)))
}

fn common(value: i64) -> (CommonHandle, Vec<u8>) {
    let encoded = encode_key(&[Datum::new_int(value)]).expect("encode common-handle datum");
    let handle = CommonHandle::new(encoded.clone()).expect("valid common handle");
    (handle, encoded)
}

fn expected_range(table_id: i64, start: i64, end: i64) -> (Vec<u8>, Vec<u8>) {
    let mut low = Vec::new();
    encode_int(&mut low, start);
    let mut high = Vec::new();
    encode_int(&mut high, end);
    let high = tidb_txnkv::Key::from_bytes(high).prefix_next();
    (
        encode_row_key(table_id, &low),
        encode_row_key(table_id, high.as_bytes()),
    )
}

/// Source: `request_builder_test.go::TestTableHandlesToKVRanges`.
#[test]
fn test_table_handles_to_kv_ranges() {
    let handles = [
        int(0),
        int(2),
        int(3),
        int(4),
        int(5),
        int(10),
        int(11),
        int(100),
        int(i64::MAX - 1),
        int(i64::MAX),
    ];
    let expected = [
        (0, 0),
        (2, 5),
        (10, 11),
        (100, 100),
        (i64::MAX - 1, i64::MAX),
    ];

    let (ranges, hints) = table_handles_to_kv_ranges(1, &handles);
    assert_eq!(hints, [1, 4, 2, 1, 2]);
    assert_eq!(ranges.len(), expected.len());
    for (range, (start, end)) in ranges.iter().zip(expected) {
        let (start_key, end_key) = expected_range(1, start, end);
        assert_eq!(range.start_key, start_key);
        assert_eq!(range.end_key, end_key);
    }

    let mut builder = KvRequestBuilder::new();
    builder.set_table_handles(1, &handles);
    let request = builder.build().expect("table-handle request build");
    let attached = request.key_ranges.expect("ranges attached");
    assert!(attached.is_non_partitioned());
    assert_eq!(attached.partitions, vec![ranges]);
    assert_eq!(attached.row_count_hints, vec![hints]);
}

/// Source: `request_builder_test.go::TestTablePartitionHandlesToKVRanges`.
#[test]
fn test_table_partition_handles_to_kv_ranges() {
    let handles = [
        partition(1, 0),
        partition(2, 2),
        partition(2, 3),
        partition(2, 4),
        partition(3, 5),
        partition(1, 10),
        partition(2, 11),
        partition(3, 100),
        partition(1, i64::MAX - 1),
        partition(1, i64::MAX),
    ];
    let expected = [
        (1, 0, 0),
        (2, 2, 4),
        (3, 5, 5),
        (1, 10, 10),
        (2, 11, 11),
        (3, 100, 100),
        (1, i64::MAX - 1, i64::MAX),
    ];

    let (ranges, hints) = table_handles_to_kv_ranges(0, &handles);
    assert_eq!(hints, [1, 3, 1, 1, 1, 1, 2]);
    assert_eq!(ranges.len(), expected.len());
    for (range, (table_id, start, end)) in ranges.iter().zip(expected) {
        let (start_key, end_key) = expected_range(table_id, start, end);
        assert_eq!(range.start_key, start_key);
        assert_eq!(range.end_key, end_key);
    }
}

/// The source keeps common handles as point ranges even when their encoded
/// bytes happen to be adjacent; logical adjacency is defined only for ints.
#[test]
fn common_handles_remain_canonical_point_ranges() {
    let (common, stored) = common(7);
    let handles = [Handle::Common(common)];

    let (ranges, hints) = table_handles_to_kv_ranges(9, &handles);
    assert_eq!(hints, [1]);
    assert_eq!(ranges[0].start_key, encode_row_key(9, &stored));
    let end = tidb_txnkv::Key::from_bytes(stored).next();
    assert_eq!(ranges[0].end_key, encode_row_key(9, end.as_bytes()));
}

#[test]
fn partition_wrapped_common_handle_uses_physical_table_id() {
    let (common, stored) = common(8);
    let handles = [Handle::Partition(PartitionHandle::new(17, common))];

    let (ranges, hints) = table_handles_to_kv_ranges(9, &handles);
    assert_eq!(hints, [1]);
    assert_eq!(ranges[0].start_key, encode_row_key(17, &stored));
    let end = tidb_txnkv::Key::from_bytes(stored).next();
    assert_eq!(ranges[0].end_key, encode_row_key(17, end.as_bytes()));
}

#[test]
fn empty_handles_preserve_non_partitioned_builder_shape() {
    let (ranges, hints) = table_handles_to_kv_ranges(1, &[]);
    assert!(ranges.is_empty());
    assert!(hints.is_empty());

    let mut builder = KvRequestBuilder::new();
    builder.set_table_handles(1, &[]);
    let request = builder.build().expect("empty table-handle request build");
    let attached = request.key_ranges.expect("empty ranges attached");
    assert!(attached.is_non_partitioned());
    assert_eq!(attached.partitions, vec![vec![]]);
    assert_eq!(attached.row_count_hints, vec![vec![]]);
}

#[test]
#[should_panic(expected = "sorted integer-handle run")]
fn mixed_handle_domains_preserve_source_precondition() {
    let (common, _) = common(2);
    let handles = [int(1), Handle::Common(common)];
    let _ = table_handles_to_kv_ranges(1, &handles);
}
