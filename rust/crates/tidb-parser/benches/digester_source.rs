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

//! Stable workloads for the benchmarks in `pkg/parser/digester_test.go`.

use std::hint::black_box;

use sha2::{Digest as _, Sha256};
use tidb_parser::Digest;

#[test]
fn benchmark_digest_hex_encode() {
    let bytes = Sha256::digest(b"abc").to_vec();
    for _ in 0..10_000 {
        black_box(Digest::new(bytes.clone()).to_string());
    }
}

#[test]
fn benchmark_digest_sprintf() {
    let digest = Digest::new(Sha256::digest(b"abc").to_vec());
    for _ in 0..10_000 {
        black_box(format!("{digest}"));
    }
}
