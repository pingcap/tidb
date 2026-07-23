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

//! Executable equivalents of every benchmark in Go `pkg/tablecodec`.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::{BinaryLiteral, Datum, Decimal};
use tidb_tablecodec::table_key::{decode_row_key, encode_row_key_with_handle, RecordHandle};
use tidb_tablecodec::{
    decode_handle_in_index_value, encode_handle_in_unique_index_value, encode_table_value,
    is_record_key, COMMON_HANDLE_FLAG, INDEX_VERSION_FLAG,
};
use tidb_txnkv::IntHandle;

const ITERATIONS: usize = 10_000;

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn prefix_next(mut key: Vec<u8>) -> Vec<u8> {
    for index in (0..key.len()).rev() {
        if key[index] != u8::MAX {
            key[index] += 1;
            key.truncate(index + 1);
            return key;
        }
    }
    key.push(0);
    key
}

fn main() {
    // Source: `bench_test.go::BenchmarkEncodeRowKeyWithHandle`.
    measure("BenchmarkEncodeRowKeyWithHandle", || {
        black_box(encode_row_key_with_handle(100, &RecordHandle::Int(100)));
    });

    // Source: `bench_test.go::BenchmarkEncodeEndKey`.
    measure("BenchmarkEncodeEndKey", || {
        black_box(encode_row_key_with_handle(100, &RecordHandle::Int(100)));
        black_box(encode_row_key_with_handle(100, &RecordHandle::Int(101)));
    });

    // Source: `bench_test.go::BenchmarkEncodeRowKeyWithPrefixNex`.
    measure("BenchmarkEncodeRowKeyWithPrefixNex", || {
        black_box(prefix_next(encode_row_key_with_handle(
            100,
            &RecordHandle::Int(100),
        )));
    });

    // Source: `bench_test.go::BenchmarkDecodeRowKey`.
    let row_key = encode_row_key_with_handle(100, &RecordHandle::Int(100));
    measure("BenchmarkDecodeRowKey", || {
        black_box(decode_row_key(&row_key).unwrap());
    });

    // Source: `bench_test.go::BenchmarkDecodeIndexKeyIntHandle`.
    let int_index_value = encode_handle_in_unique_index_value(&IntHandle::new(256).into(), false);
    measure("BenchmarkDecodeIndexKeyIntHandle", || {
        black_box(decode_handle_in_index_value(&int_index_value).unwrap());
    });

    // Source: `bench_test.go::BenchmarkDecodeIndexKeyCommonHandle`.
    let common_encoded = tidb_codec::encode_key(&[Datum::Int(1), Datum::Int(2)]).unwrap();
    let mut common_index_value = vec![0, INDEX_VERSION_FLAG, 1, COMMON_HANDLE_FLAG];
    common_index_value
        .extend_from_slice(&u16::try_from(common_encoded.len()).unwrap().to_be_bytes());
    common_index_value.extend_from_slice(&common_encoded);
    measure("BenchmarkDecodeIndexKeyCommonHandle", || {
        black_box(decode_handle_in_index_value(&common_index_value).unwrap());
    });

    // Source: `tablecodec_test.go::BenchmarkHasTablePrefix`.
    measure("BenchmarkHasTablePrefix", || {
        black_box(b"foobar".first() == Some(&b't'));
    });

    // Source: `tablecodec_test.go::BenchmarkHasTablePrefixBuiltin`.
    measure("BenchmarkHasTablePrefixBuiltin", || {
        black_box(b"foobar".starts_with(b"t"));
    });

    // Source: `tablecodec_test.go::BenchmarkEncodeValue`.
    let values = [
        Datum::Int(100),
        Datum::new_bytes(b"abc"),
        Datum::Decimal(Decimal::from_literal("1")),
        Datum::Int(0),
        Datum::UInt(0),
        Datum::BinaryLiteral(BinaryLiteral::from(vec![100])),
        Datum::Float32(1.5),
    ];
    measure("BenchmarkEncodeValue", || {
        for value in &values {
            black_box(encode_table_value(None, value).unwrap());
        }
    });

    assert!(is_record_key(&row_key));
}
