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

//! Executable equivalents of every benchmark in `mydecimal_benchmark_test.go`.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::Decimal;

const ITERATIONS: usize = 10_000;

fn measure(name: &str, mut operation: impl FnMut()) {
    let started = Instant::now();
    for _ in 0..ITERATIONS {
        operation();
    }
    println!("{name}: {:?}", started.elapsed());
}

fn decimal(input: &str) -> Decimal {
    let expanded = tidb_datatype::convert_scientific_notation(input).unwrap();
    Decimal::from_signed_literal(&expanded)
}

fn main() {
    if cfg!(test) {
        return;
    }

    let rounding = [
        ("123456789.987654321", 1),
        ("15.1", 0),
        ("15.5", 0),
        ("15.9", 0),
        ("-15.1", 0),
        ("-15.5", 0),
        ("-15.9", 0),
        ("15.1", 1),
        ("-15.1", 1),
        ("15.17", 1),
        ("15.4", -1),
        ("-15.4", -1),
        ("5.4", -1),
        (".999", 0),
        ("999999999", -9),
    ]
    .map(|(input, scale)| (decimal(input), scale));
    measure("BenchmarkRound", || {
        for (value, scale) in &rounding {
            black_box(value.round_to_scale(*scale));
            black_box(value.truncate_to_scale(*scale));
            black_box(value.round_ceiling_to_scale(*scale));
        }
    });

    let values: Vec<Decimal> = (1..=1_000)
        .map(|value| decimal(&format!("{}.{:06}", value, value * 977 % 1_000_000)))
        .collect();
    measure("BenchmarkToFloat64New", || {
        for value in &values {
            black_box(value.to_f64());
        }
    });
    measure("BenchmarkToFloat64Old", || {
        for value in &values {
            black_box(value.to_string().parse::<f64>().unwrap());
        }
    });

    let codec_values: Vec<Decimal> = [
        "1.000000000000",
        "3",
        "12.000000000",
        "120",
        "120000",
        "100000000000.00000",
        "0.000000001200000000",
        "98765.4321",
        "-123.456000000000000000",
        "0",
        "0000000000",
        "0.00000000000",
    ]
    .into_iter()
    .map(decimal)
    .collect();
    measure("BenchmarkMyDecimalToBin", || {
        for value in &codec_values {
            let (precision, frac) = value.precision_and_frac();
            black_box(value.to_bin(precision, frac).unwrap());
        }
    });
    measure("BenchmarkMyDecimalToHashKey", || {
        for value in &codec_values {
            black_box(value.to_hash_key().unwrap());
        }
    });
}
