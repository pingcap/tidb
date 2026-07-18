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

//! Focused source vectors for `pkg/tablecodec/tablecodec.go` row-key encoding.

use tidb_codec::{encode_int, encode_row_key, gen_table_record_prefix};

fn expected_prefix(table_id: i64) -> Vec<u8> {
    let mut expected = vec![b't'];
    encode_int(&mut expected, table_id);
    expected.extend_from_slice(b"_r");
    expected
}

#[test]
fn row_key_prefix_and_opaque_handle_bytes_match_source() {
    for table_id in [i64::MIN, -1, 0, 1, 55, i64::MAX] {
        let prefix = expected_prefix(table_id);
        assert_eq!(gen_table_record_prefix(table_id), prefix);

        let handle = [0x00, 0x7f, 0x80, 0xff];
        let mut expected = prefix;
        expected.extend_from_slice(&handle);
        assert_eq!(encode_row_key(table_id, &handle), expected);
    }
}

#[test]
fn empty_and_variable_width_handles_are_not_reinterpreted() {
    let prefix = gen_table_record_prefix(42);
    assert_eq!(encode_row_key(42, &[]), prefix);

    let common_handle = [1, 2, 3, 0, 4, 5, 6, 7, 8, 9, 10];
    assert_eq!(
        &encode_row_key(42, &common_handle)[prefix.len()..],
        common_handle
    );
}
