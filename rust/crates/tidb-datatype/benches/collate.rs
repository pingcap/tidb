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

//! Stable custom harness for every benchmark in
//! `pkg/util/collate/collate_bench_test.go`.

use std::hint::black_box;
use std::time::Instant;

use tidb_datatype::Collation;

const SHORT: usize = 2 << 4;
const MIDDLE: usize = 2 << 10;
const LONG: usize = 2 << 20;

const COLLATIONS: [(&str, Collation); 5] = [
    ("Utf8mb4Bin", Collation::Utf8Mb4Bin),
    ("Utf8mb4GeneralCI", Collation::Utf8Mb4GeneralCi),
    ("Utf8mb4UnicodeCI", Collation::Utf8Mb4UnicodeCi),
    ("Utf8mb40900AICI", Collation::Utf8Mb40900AiCi),
    ("Utf8mb40900Bin", Collation::Utf8Mb40900Bin),
];

const LENGTHS: [(&str, usize); 3] = [("Short", SHORT), ("Mid", MIDDLE), ("Long", LONG)];

fn generate_data(length: usize, offset: usize) -> Vec<u8> {
    const RUNES: [&str; 3] = ["ß", "s", "s"];
    (0..length)
        .flat_map(|index| RUNES[(index + offset) % RUNES.len()].bytes())
        .collect()
}

fn measure(name: &str, operation: impl FnOnce()) {
    let started = Instant::now();
    operation();
    println!("{name}: {:?}", started.elapsed());
}

fn main() {
    // `cargo test --all-targets` compiles this source with `cfg(test)`;
    // benchmark work runs only under an explicit `cargo bench`.
    if cfg!(test) {
        return;
    }

    for (length_name, length) in LENGTHS {
        let left = generate_data(length, 0);
        let right = generate_data(length, 1);
        for (collation_name, collation) in COLLATIONS {
            measure(
                &format!("Benchmark{collation_name}_Compare{length_name}"),
                || {
                    black_box(collation.compare(black_box(&left), black_box(&right)));
                },
            );
        }
    }

    for (length_name, length) in LENGTHS {
        let value = generate_data(length, 0);
        for (collation_name, collation) in COLLATIONS {
            measure(
                &format!("Benchmark{collation_name}_Key{length_name}"),
                || {
                    black_box(collation.key(black_box(&value)));
                },
            );
            measure(
                &format!("Benchmark{collation_name}_ImmutableKey{length_name}"),
                || {
                    // Rust owns returned keys, so the source ImmutableKey
                    // operation has the same executable implementation.
                    black_box(collation.key(black_box(&value)));
                },
            );
        }
    }
}
