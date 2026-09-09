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

//! Executable translation of `BenchmarkRecordGoroutineProfile`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

fn run_case(label: &str, thread_count: usize) {
    let ready = Arc::new(Barrier::new(thread_count + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let workers = (0..thread_count)
        .map(|worker| {
            let ready = Arc::clone(&ready);
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name(format!("memory-alarm-{worker}"))
                .spawn(move || {
                    ready.wait();
                    while !stop.load(Ordering::Acquire) {
                        std::thread::park_timeout(Duration::from_millis(10));
                    }
                })
                .unwrap()
        })
        .collect::<Vec<_>>();
    ready.wait();

    let record_dir = tempfile::tempdir().unwrap();
    let started = Instant::now();
    tidb_util::memoryusagealarm::record_goroutine_profile_for_benchmark(record_dir.path()).unwrap();
    println!(
        "BenchmarkRecordGoroutineProfile/{label}: {:?}",
        started.elapsed()
    );

    stop.store(true, Ordering::Release);
    for worker in &workers {
        worker.thread().unpark();
    }
    for worker in workers {
        worker.join().unwrap();
    }
}

fn main() {
    run_case("WithBackgroundGoroutine/10", 10);
    run_case("WithBackgroundGoroutine/100", 100);
    run_case("WithBackgroundGoroutine/1000", 1000);
    // The pinned Go source labels this case 10000 but passes 1000.
    run_case("WithBackgroundGoroutine/10000", 1000);
}
