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

//! Direct translations of `pkg/kv/key_test.go:121-323` Handle assertions.

use std::cmp::Ordering;

use tidb_codec::{decode_int, decode_one, encode_key};
use tidb_datatype::{Datum, Decimal};
use tidb_txnkv::{CommonHandle, Handle, HandleMap, IntHandle, PartitionHandle};

const FIXTURE: &str = include_str!("../fixtures/handles.hex");

fn fixture(name: &str) -> Vec<u8> {
    let prefix = format!("{name}=");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    assert_eq!(hex.len() % 2, 0);
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

fn common_100_abc() -> CommonHandle {
    CommonHandle::new(fixture("handle_100_abc")).unwrap()
}

/// Complete translation of `TestHandle`, with typed comparison errors replacing
/// the source interface's out-of-domain panics.
#[test]
fn test_handle() {
    let int = IntHandle::new(100);
    let int_handle = Handle::from(int);
    assert!(int_handle.is_int());

    let encoded = int.encoded();
    let (remain, decoded) = decode_int(&encoded).unwrap();
    assert!(remain.is_empty());
    assert_eq!(decoded, int.value());

    let next = int_handle.next();
    assert_eq!(next.int_value(), Some(101));
    assert!(!int_handle.equal(&next));
    assert_eq!(int_handle.compare(&next), Ok(Ordering::Less));
    assert_eq!(int_handle.to_string(), "100");

    let encoded = encode_key(&[Datum::new_int(100), Datum::new_string("abc")]).unwrap();
    assert_eq!(encoded, fixture("handle_100_abc"));
    let common = common_100_abc();
    let common_handle = Handle::from(common.clone());
    assert!(!common_handle.is_int());

    let common_next = common_handle.next();
    assert!(!common_handle.equal(&common_next));
    assert_eq!(common_handle.compare(&common_next), Ok(Ordering::Less));
    assert_eq!(common_next.len(), common_handle.len());
    assert_eq!(common.num_columns(), 2);

    let (remain, first) = decode_one(common.encoded_column(0).unwrap()).unwrap();
    assert!(remain.is_empty());
    assert_eq!(first.as_int(), Some(100));
    let (remain, second) = decode_one(common.encoded_column(1).unwrap()).unwrap();
    assert!(remain.is_empty());
    assert_eq!(second.as_raw_bytes(), Some(&b"abc"[..]));
    assert_eq!(common.to_string(), "{100, abc}");

    let partition_int = Handle::from(PartitionHandle::new(2, int_handle.clone()));
    assert!(partition_int.equal(&int_handle));
    assert!(int_handle.equal(&partition_int));

    let partition_common = Handle::from(PartitionHandle::new(1, common_next.clone()));
    assert!(partition_common.equal(&common_next));
    assert!(common_next.equal(&partition_common));
}

/// Complete translation of `TestPaddingHandle` against exact Go decimal bytes.
#[test]
fn test_padding_handle() {
    let encoded = encode_key(&[Datum::new_decimal(Decimal::from_int(1))]).unwrap();
    assert_eq!(encoded, fixture("decimal_1"));
    assert!(encoded.len() < 9);

    let handle = CommonHandle::new(encoded.clone()).unwrap();
    assert_eq!(handle.encoded().len(), 9);
    assert_eq!(handle.encoded_column(0), Some(encoded.as_slice()));

    let reparsed = CommonHandle::new(handle.encoded().to_vec()).unwrap();
    assert_eq!(handle.encoded_column(0), reparsed.encoded_column(0));
}

#[derive(Debug, Eq, PartialEq)]
enum Value {
    Integer(i32),
    Text(&'static str),
}

/// Translation of every portable `TestHandleMap` assertion. Go object-layout
/// memory constants are deliberately excluded from this semantic map API.
#[test]
fn test_handle_map() {
    let mut map = HandleMap::new();
    let int = Handle::from(IntHandle::new(1));
    assert_eq!(map.set(int.clone(), Value::Integer(1)), None);
    assert_eq!(map.get(&int), Some(&Value::Integer(1)));
    assert_eq!(map.delete(&int), Some(Value::Integer(1)));
    assert_eq!(map.get(&int), None);

    let common = Handle::from(common_100_abc());
    map.set(common.clone(), Value::Text("a"));
    assert_eq!(map.get(&common), Some(&Value::Text("a")));
    assert_eq!(map.delete(&common), Some(Value::Text("a")));
    assert_eq!(map.get(&common), None);

    map.set(common.clone(), Value::Text("a"));
    let common_two = Handle::from(CommonHandle::new(fixture("handle_101_abc")).unwrap());
    map.set(common_two.clone(), Value::Text("b"));
    let common_three = Handle::from(CommonHandle::new(fixture("handle_99_def")).unwrap());
    map.set(common_three, Value::Text("c"));
    assert_eq!(map.len(), 3);

    let mut count = 0;
    map.range(|handle, value| {
        count += 1;
        if handle.equal(&common) {
            assert_eq!(value, &Value::Text("a"));
        } else if handle.equal(&common_two) {
            assert_eq!(value, &Value::Text("b"));
        } else {
            assert_eq!(value, &Value::Text("c"));
        }
        count != 2
    });
    assert_eq!(count, 2);
}

/// Complete seven-row translation of `TestCommonHandlesFitIntHandleRange`.
#[test]
fn test_common_handles_fit_int_handle_range() {
    let min = IntHandle::new(i64::MIN).encoded();
    let max = IntHandle::new(i64::MAX).encoded();
    assert_eq!(min, fixture("int_handle_min"));
    assert_eq!(max, fixture("int_handle_max"));

    let production_cases = [
        (
            "range_int_string",
            vec![Datum::new_int(101), Datum::new_string("abc")],
        ),
        (
            "range_string_int",
            vec![Datum::new_string("abc"), Datum::new_int(101)],
        ),
        (
            "range_negative_int_string",
            vec![Datum::new_int(-101), Datum::new_string("abc")],
        ),
        (
            "range_min_max_int",
            vec![Datum::new_int(i64::MIN), Datum::new_int(i64::MAX)],
        ),
        ("range_bytes_ff", vec![Datum::new_bytes(vec![0xff, 0xff])]),
        ("range_bytes_00", vec![Datum::new_bytes(vec![0x00, 0x00])]),
    ];

    for (name, values) in production_cases {
        let encoded = encode_key(&values).unwrap();
        assert_eq!(encoded, fixture(name), "Go codec oracle {name}");
        assert_common_between_int_bounds(&min, &max, encoded);
    }

    // Go BinaryLiteral is normalized by EncodeKey to UInt(65535). The Rust
    // datatype owner has no BinaryLiteral variant yet, so validate the exact Go
    // output through the real normalized UInt encoder and decoder/handle parser
    // rather than inventing an opaque codec-only datum kind.
    let binary = fixture("range_binary_ff");
    let normalized = encode_key(&[Datum::new_uint(65_535)]).unwrap();
    assert_eq!(normalized, binary);
    let (remain, decoded) = decode_one(&binary).unwrap();
    assert!(remain.is_empty());
    assert_eq!(decoded.as_uint(), Some(65_535));
    assert_common_between_int_bounds(&min, &max, binary);
}

fn assert_common_between_int_bounds(min: &[u8], max: &[u8], encoded: Vec<u8>) {
    let common = CommonHandle::new(encoded).unwrap();
    assert!(min < common.encoded());
    assert!(max > common.encoded());
}

/// Complete translation of `TestHandleMapWithPartialHandle`.
#[test]
fn test_handle_map_with_partition_handle() {
    let partition_one = Handle::from(PartitionHandle::new(1, IntHandle::new(1)));
    let partition_two = Handle::from(PartitionHandle::new(2, IntHandle::new(1)));
    let partition_three = Handle::from(PartitionHandle::new(1, IntHandle::new(3)));
    let int = Handle::from(IntHandle::new(1));
    let common = Handle::from(CommonHandle::new(fixture("decimal_1")).unwrap());

    let mut map = HandleMap::new();
    map.set(partition_one.clone(), 1);
    map.set(partition_two.clone(), 2);
    map.set(partition_three.clone(), 5);
    map.set(int.clone(), 3);
    map.set(common.clone(), 4);

    assert_eq!(map.get(&partition_one), Some(&1));
    assert_eq!(map.get(&partition_two), Some(&2));
    assert_eq!(map.get(&partition_three), Some(&5));
    assert_eq!(map.get(&int), Some(&3));
    assert_eq!(map.get(&common), Some(&4));
    assert_eq!(map.len(), 5);

    assert_eq!(map.delete(&partition_one), Some(1));
    assert_eq!(map.get(&partition_one), None);
    assert_eq!(map.len(), 4);

    let missing = Handle::from(PartitionHandle::new(3, IntHandle::new(1)));
    assert_eq!(map.delete(&missing), None);
    assert_eq!(map.len(), 4);
}
