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

//! Exact Go-generated transition vectors for `EncodeComparableVarint`.

use tidb_codec::{decode_comparable_varint, encode_comparable_varint};

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/number_boundaries.hex");

#[test]
fn negative_comparable_varint_transitions_are_byte_exact() {
    let transitions = [
        ("negative_1byte_last", -((1_i64 << 8) - 1)),
        ("negative_2byte_first", -(1_i64 << 8)),
        ("negative_2byte_last", -((1_i64 << 16) - 1)),
        ("negative_3byte_first", -(1_i64 << 16)),
        ("negative_3byte_last", -((1_i64 << 24) - 1)),
        ("negative_4byte_first", -(1_i64 << 24)),
        ("negative_4byte_last", -((1_i64 << 32) - 1)),
        ("negative_5byte_first", -(1_i64 << 32)),
        ("negative_5byte_last", -((1_i64 << 40) - 1)),
        ("negative_6byte_first", -(1_i64 << 40)),
        ("negative_6byte_last", -((1_i64 << 48) - 1)),
        ("negative_7byte_first", -(1_i64 << 48)),
        ("negative_7byte_last", -((1_i64 << 56) - 1)),
        ("negative_8byte_first", -(1_i64 << 56)),
        ("negative_min", i64::MIN),
    ];

    for (name, value) in transitions {
        let mut encoded = Vec::new();
        encode_comparable_varint(&mut encoded, value);
        assert_eq!(encoded, fixture(name), "Go transition {name}");
        let (remain, decoded) = decode_comparable_varint(&encoded).unwrap();
        assert!(remain.is_empty());
        assert_eq!(decoded, value);
    }
}

fn fixture(name: &str) -> Vec<u8> {
    let prefix = format!("{name}=");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("non-hex fixture byte {byte}"),
    }
}
