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

//! Executable equivalents of every benchmark in `pkg/util/codec/bench_test.go`.

use std::hint::black_box;
use std::time::Instant;

use tidb_codec::{
    decode, decode_decimal, decode_one_typed, encode_bytes, encode_decimal_fixed, encode_int,
    encode_value, BYTES_FLAG,
};
use tidb_datatype::{Datum, Decimal, FieldType, FieldTypeCode};

const ITERATIONS: usize = 10_000;
const VALUE_COUNT: usize = 100;

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn compose_encoded_data() -> Vec<u8> {
    let values = (0..VALUE_COUNT)
        .map(|value| Datum::Int(value as i64))
        .collect::<Vec<_>>();
    encode_value(&values).unwrap()
}

fn benchmark_decode_with_size(encoded: &[u8]) {
    black_box(decode(black_box(encoded), VALUE_COUNT).unwrap());
}

fn benchmark_decode_with_out_size(encoded: &[u8]) {
    black_box(decode(black_box(encoded), 1).unwrap());
}

fn benchmark_encode_int_with_size() {
    let mut output = Vec::with_capacity(8);
    encode_int(&mut output, 10);
    black_box(output);
}

fn benchmark_encode_int_with_out_size() {
    let mut output = Vec::new();
    encode_int(&mut output, 10);
    black_box(output);
}

fn benchmark_decode_decimal(encoded: &[u8]) {
    black_box(decode_decimal(black_box(encoded)).unwrap());
}

fn benchmark_decode_one_to_chunk(encoded: &[u8], field_type: &FieldType) {
    black_box(decode_one_typed(black_box(encoded), field_type).unwrap());
}

fn main() {
    let encoded = compose_encoded_data();
    measure("BenchmarkDecodeWithSize", || {
        benchmark_decode_with_size(&encoded);
    });
    measure("BenchmarkDecodeWithOutSize", || {
        benchmark_decode_with_out_size(&encoded);
    });
    measure("BenchmarkEncodeIntWithSize", benchmark_encode_int_with_size);
    measure(
        "BenchmarkEncodeIntWithOutSize",
        benchmark_encode_int_with_out_size,
    );

    let decimal = Decimal::from_signed_literal("1211.1211113");
    let mut decimal_bytes = Vec::new();
    encode_decimal_fixed(&mut decimal_bytes, &decimal, 0, 0).unwrap();
    measure("BenchmarkDecodeDecimal", || {
        benchmark_decode_decimal(&decimal_bytes);
    });

    let mut raw = vec![BYTES_FLAG];
    encode_bytes(&mut raw, b"a");
    let integer = FieldType::new(FieldTypeCode::LongLong);
    measure("BenchmarkDecodeOneToChunk", || {
        benchmark_decode_one_to_chunk(&raw, &integer);
    });
}
