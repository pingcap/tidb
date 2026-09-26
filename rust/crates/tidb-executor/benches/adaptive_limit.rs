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

//! Microbenchmarks for the two controller hot paths measured by Go's
//! `BenchmarkAdaptiveLimitControllerObserveJoinProgress` and
//! `BenchmarkAdaptiveLimitControllerReservationRoundTrip`.
//!
//! Run with `cargo bench -p tidb-executor --bench adaptive_limit`.

use std::hint::black_box;
use std::time::{Duration, Instant};

#[allow(dead_code, unused_imports)]
#[path = "../src/adaptive_limit.rs"]
mod adaptive_limit;

use adaptive_limit::{AdaptiveLimitConfig, AdaptiveLimitController};

const MEASURE_FOR: Duration = Duration::from_secs(1);

fn controller(outer_window: u64) -> std::sync::Arc<AdaptiveLimitController> {
    AdaptiveLimitController::for_index_join(AdaptiveLimitConfig {
        demand_rows: u64::MAX,
        initial_outer_window: outer_window,
        max_outer_window: outer_window,
        initial_lookup_window: 1,
        max_lookup_window: 1,
        initial_lookup_batch_size: 1,
        max_lookup_batch_size: 1,
    })
}

fn report(name: &str, operations: u64, elapsed: Duration) {
    println!("{name} operations {operations}");
    println!(
        "{name} ns_per_op {:.2}",
        elapsed.as_nanos() as f64 / operations as f64
    );
}

fn bench_observe_join_progress() {
    let controller = controller(usize::MAX as u64);
    let reserved = controller
        .try_reserve_outer(usize::MAX)
        .expect("initial outer window admits the seed reservation");
    controller.commit_outer(reserved, reserved);

    let start = Instant::now();
    let mut operations = 0_u64;
    while start.elapsed() < MEASURE_FOR {
        controller.observe_join_progress(black_box(1), black_box(1));
        operations += 1;
    }
    report(
        "adaptive_observe_join_progress",
        operations,
        start.elapsed(),
    );
}

fn bench_reservation_round_trip() {
    let controller = controller(1);
    let start = Instant::now();
    let mut operations = 0_u64;
    while start.elapsed() < MEASURE_FOR {
        let reserved = controller
            .try_reserve_outer(black_box(1))
            .expect("one-row reservation remains available");
        controller.commit_outer(reserved, reserved);
        controller.observe_join_progress(reserved, reserved);
        operations += 1;
    }
    report(
        "adaptive_reservation_round_trip",
        operations,
        start.elapsed(),
    );
}

fn main() {
    bench_observe_join_progress();
    bench_reservation_round_trip();
}
