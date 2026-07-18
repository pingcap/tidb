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

//! Direct concurrent read-byte EMA obligations from TiDB's Go tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use tidb_distsql::ReadBytesEma;

const BASE: Duration = Duration::from_secs(1_000);

fn assert_within(actual: u64, expected: u64, delta: u64) {
    assert!(
        actual.abs_diff(expected) <= delta,
        "actual={actual}, expected={expected}, delta={delta}"
    );
}

#[test]
fn test_ru_ema_seeded_prediction_and_first_sample_replaces_seed() {
    const PAGE_SIZE_BYTES: u64 = 4 * 1024 * 1024;
    let ema = ReadBytesEma::new(PAGE_SIZE_BYTES);
    assert_eq!(ema.predict(), PAGE_SIZE_BYTES);

    ema.observe(1_000_000, BASE);
    assert_eq!(ema.predict(), 1_000_000);
}

#[test]
fn source_zero_time_observation_has_zero_weight_and_does_not_advance_time() {
    let ema = ReadBytesEma::new(4_000_000);
    ema.observe(1_000_000, Duration::ZERO);
    assert_eq!(ema.predict(), 4_000_000);

    ema.observe(1_000_000, BASE);
    assert_eq!(ema.predict(), 1_000_000);
}

#[test]
fn test_ru_ema_unseeded_first_observation_and_steady_input() {
    let ema = ReadBytesEma::new(0);
    assert_eq!(ema.predict(), 0);

    ema.observe(1_000_000, BASE);
    assert_eq!(ema.predict(), 1_000_000);
    ema.observe(1_000_000, BASE + Duration::from_millis(100));
    assert_within(ema.predict(), 1_000_000, 1);
}

#[test]
fn test_ru_ema_tracks_shift() {
    let ema = ReadBytesEma::new(0);
    for index in 0..5 {
        ema.observe(100_000, BASE + Duration::from_millis(index * 100));
    }
    assert_within(ema.predict(), 100_000, 1);

    for index in 5..20 {
        ema.observe(500_000, BASE + Duration::from_millis(index * 100));
    }
    assert!(ema.predict() > 400_000);
    assert!(ema.predict() <= 500_000);
}

#[test]
fn test_ru_ema_large_gap_collapses_weight() {
    let ema = ReadBytesEma::new(0);
    ema.observe(100_000, BASE);
    ema.observe(1_000_000, BASE + Duration::from_secs(10));
    assert_within(ema.predict(), 1_000_000, 1_000);
}

#[test]
fn test_ru_ema_concurrent_observe_and_predict() {
    const WRITERS: u64 = 8;
    const ITERATIONS: u64 = 200;
    let ema = Arc::new(ReadBytesEma::new(0));
    let running = Arc::new(AtomicBool::new(true));
    let start = Arc::new(Barrier::new(WRITERS as usize + 2));

    let reader_ema = Arc::clone(&ema);
    let reader_running = Arc::clone(&running);
    let reader_start = Arc::clone(&start);
    let reader = thread::spawn(move || {
        reader_start.wait();
        while reader_running.load(Ordering::Acquire) {
            let _ = reader_ema.predict();
            thread::yield_now();
        }
    });

    let mut writers = Vec::new();
    for writer in 0..WRITERS {
        let writer_ema = Arc::clone(&ema);
        let writer_start = Arc::clone(&start);
        writers.push(thread::spawn(move || {
            writer_start.wait();
            for index in 0..ITERATIONS {
                writer_ema.observe(
                    100_000 + writer * 1_000 + index,
                    BASE + Duration::from_millis(index),
                );
            }
        }));
    }
    start.wait();
    for writer in writers {
        writer.join().expect("writer must finish");
    }
    running.store(false, Ordering::Release);
    reader.join().expect("reader must finish");
    assert!(ema.predict() > 0);
}

#[test]
fn test_ru_ema_non_monotonic_time() {
    let ema = ReadBytesEma::new(0);
    ema.observe(100_000, BASE);
    ema.observe(500_000, BASE - Duration::from_secs(1));
    assert_within(ema.predict(), 100_000, 1);
}
