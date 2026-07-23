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

//! Executable equivalents of the datum and default-type benchmarks in
//! `pkg/types`.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::{
    datums_to_string, default_field_type_for_value, is_printable, parse_datetime, Collation, Datum,
    Decimal, FieldTypeValue,
};

const ITERATIONS: usize = 10_000;

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn main() {
    if cfg!(test) {
        return;
    }

    let timestamp = parse_datetime("2018-03-08 16:01:00.315313", &chrono_tz::UTC, true, false)
        .unwrap()
        .time;
    let values = vec![
        Datum::new_int(1),
        Datum::new_real(1.23),
        Datum::new_string("abcde"),
        Datum::new_decimal(Decimal::from_signed_literal("1.2345")),
        Datum::new_time(timestamp),
    ];
    let equal_values = values.clone();
    measure("BenchmarkCompareDatum", || {
        for (left, right) in values.iter().zip(&equal_values) {
            black_box(left.compare(right, Collation::Binary).unwrap());
        }
    });
    measure("BenchmarkCompareDatumByReflect", || {
        black_box(values == equal_values);
    });

    let printable_values = vec![
        Datum::new_int(1),
        Datum::new_uint(2),
        Datum::new_float32_from_f64(-3.111_111_1),
        Datum::new_real(4.123),
        Datum::new_decimal(Decimal::from_signed_literal("6.66666")),
        Datum::new_string("dklsfjkaslnfwoiewlkfjaslkfjljs"),
        Datum::new_bytes(b"xxxxxxxxxxxxxxxxxxxxxxx"),
        Datum::MinNotNull,
        Datum::MaxValue,
    ];
    measure("BenchmarkDatumsToString", || {
        black_box(datums_to_string(&printable_values, true, false).unwrap());
    });
    let string = [Datum::new_string("1".repeat(512))];
    measure("BenchmarkDatumsToStringStr", || {
        black_box(datums_to_string(&string, true, false).unwrap());
    });
    let long_string = [Datum::new_string("1".repeat(10 * 1024))];
    measure("BenchmarkDatumsToStringLongStr", || {
        black_box(datums_to_string(&long_string, true, false).unwrap());
    });
    let truncated = Datum::new_string("1".repeat(128));
    let integer = Datum::new_int(2);
    measure("BenchmarkDatumTruncatedStringify", || {
        black_box(truncated.truncated_stringify().unwrap());
        black_box(integer.truncated_stringify().unwrap());
    });

    let printable_inputs: Vec<Vec<u8>> = vec![
        b"abc".to_vec(),
        "abcé".as_bytes().to_vec(),
        vec![b'a', 0, b'b', b'c'],
        vec![b'a', b'b', b'c', 0xc3, 0xa9],
        "abc".repeat(1_000).into_bytes(),
    ];
    measure("BenchmarkIsPrintable", || {
        for input in &printable_inputs {
            black_box(is_printable(input));
        }
    });

    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let full: Vec<u64> = (0..1_000_000)
        .map(|_| {
            state ^= state << 7;
            state ^= state >> 9;
            state
        })
        .collect();
    for (name, modulus) in [
        ("LenOfUint64_input full range", None),
        ("LenOfUint64_input 0 to 64K", Some(64_000)),
        ("LenOfUint64_input 0 to 512", Some(512)),
    ] {
        let mut index = 0;
        measure(&format!("BenchmarkDefaultTypeForValue/{name}"), || {
            let value = full[index % full.len()];
            index += 1;
            let value = modulus.map_or(value, |modulus| value % modulus);
            black_box(default_field_type_for_value(
                FieldTypeValue::Unsigned(value),
                "utf8mb4",
                "utf8mb4_bin",
            ));
        });
    }
}
