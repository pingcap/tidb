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

//! Ports of `pkg/util/fastrand` unit tests from Go (`random_test.go`).
//!
//! The Go package contains a single `TestRand` plus four parallel benchmarks.
//! The Go-only `TestMain` (`main_test.go`) installs TiDB global test setup and
//! a goleak goroutine check; this Rust module starts no background workers, so
//! there is nothing to translate for it.

use crate::fastrand::{buf, uint32_n, uint64_n};

/// Go: pkg/util/fastrand/random_test.go TestRand
#[test]
fn rand() {
    let x = uint32_n(1024);
    assert!(x < 1024);
    let y = uint64_n(1_u64 << 63);
    assert!(y < 1_u64 << 63);

    let _ = buf(20);
    let mut arr = [false; 256];
    for _ in 0..1024 {
        let idx = uint32_n(256);
        arr[idx as usize] = true;
    }
    let sum = arr.iter().filter(|seen| !**seen).count();
    assert!(sum < 24);
}

/// Go: pkg/util/fastrand/random_test.go BenchmarkFastRandBuf
///
// go-parity-gap: Go benchmark, not a unit test; kept as an opt-in smoke check
/// so the exercised call path stays compiled and runnable manually.
#[test]
#[ignore]
fn benchmark_fast_rand_buf() {
    for _ in 0..10_000 {
        let _ = buf(20);
    }
}

/// Go: pkg/util/fastrand/random_test.go BenchmarkFastRandUint32N
///
// go-parity-gap: Go benchmark, not a unit test; kept as an opt-in smoke check
/// so the exercised call path stays compiled and runnable manually.
#[test]
#[ignore]
fn benchmark_fast_rand_uint32_n() {
    for _ in 0..10_000 {
        let _ = uint32_n(127);
    }
}

/// Go: pkg/util/fastrand/random_test.go BenchmarkFastRand
///
// go-parity-gap: Go benchmark over the runtime-linked `Uint32`; kept as an
/// opt-in smoke check runnable manually.
#[test]
#[ignore]
fn benchmark_fast_rand() {
    for _ in 0..10_000 {
        let _ = crate::fastrand::uint32();
    }
}

/// Go: pkg/util/fastrand/random_test.go BenchmarkGlobalRand (uses math/rand).
///
// go-parity-gap: benchmarks Go's global `math/rand` against fastrand; that
/// comparison is meaningless outside Go's runtime, only the fastrand side is
/// smoked here.
#[test]
#[ignore]
fn benchmark_global_rand() {
    for _ in 0..10_000 {
        let _ = crate::fastrand::uint32();
    }
}
