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

//! Source-shaped coverage for signed integer handle range encoding from
//! `pkg/distsql/request_builder.go` and `request_builder_test.go`.

use tidb_codec::{encode_int, encode_row_key};
use tidb_distsql::{signed_handle_ranges_to_kv_ranges, KvRequestBuildError, SignedHandleRange};
use tidb_txnkv::Key;

fn row_key(table_id: i64, value: i64, prefix_next: bool) -> Vec<u8> {
    let mut encoded = Vec::new();
    encode_int(&mut encoded, value);
    if prefix_next {
        encoded = Key::from_bytes(encoded).prefix_next().into_bytes();
    }
    encode_row_key(table_id, &encoded)
}

#[test]
fn inclusive_point_and_open_boundaries_match_encode_handle_key() {
    let table_id = 42;
    let ranges = [
        SignedHandleRange::inclusive(-7, -7).unwrap(),
        SignedHandleRange::new(0, 9, true, true).unwrap(),
    ];
    let encoded = signed_handle_ranges_to_kv_ranges(table_id, &ranges);

    assert_eq!(encoded[0].start_key, row_key(table_id, -7, false));
    assert_eq!(encoded[0].end_key, row_key(table_id, -7, true));
    assert_eq!(encoded[1].start_key, row_key(table_id, 0, true));
    assert_eq!(encoded[1].end_key, row_key(table_id, 9, false));
}

#[test]
fn signed_extremes_and_not_equal_keep_two_ordered_ranges() {
    let ranges = [
        SignedHandleRange::new(i64::MIN, 0, false, true).unwrap(),
        SignedHandleRange::new(0, i64::MAX, true, false).unwrap(),
    ];
    let encoded = signed_handle_ranges_to_kv_ranges(7, &ranges);

    assert_eq!(encoded.len(), 2);
    assert_eq!(encoded[0].start_key, row_key(7, i64::MIN, false));
    assert_eq!(encoded[0].end_key, row_key(7, 0, false));
    assert_eq!(encoded[1].start_key, row_key(7, 0, true));
    assert_eq!(encoded[1].end_key, row_key(7, i64::MAX, true));
    assert!(encoded[0].end_key < encoded[1].start_key);
}

#[test]
fn contradiction_is_an_empty_range_list_not_an_invalid_key() {
    assert!(signed_handle_ranges_to_kv_ranges(1, &[]).is_empty());
    assert_eq!(
        SignedHandleRange::new(5, 4, false, false),
        Err(KvRequestBuildError::RangeEncoding)
    );
    assert_eq!(
        SignedHandleRange::new(5, 5, true, false),
        Err(KvRequestBuildError::RangeEncoding)
    );
    assert_eq!(
        SignedHandleRange::new(5, 5, false, true),
        Err(KvRequestBuildError::RangeEncoding)
    );
}
