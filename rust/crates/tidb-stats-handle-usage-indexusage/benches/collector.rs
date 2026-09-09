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

//! Go `BenchmarkIndexCollector`.

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use tidb_stats_handle_usage_indexusage::{new_sample, Collector};

fn benchmark_index_collector(name: &str, report_per_operation: usize) {
    const OPERATIONS: usize = 100_000;
    let operations: Vec<_> = (0..OPERATIONS)
        .map(|index| {
            (
                (index % 10) as i64,
                ((index / 10) % 10) as i64,
                new_sample(1, 1, (index % 100) as u64, 100),
            )
        })
        .collect();
    let collector = Arc::new(Collector::new());
    collector.start_worker();
    let started = Instant::now();
    let operation_index = AtomicUsize::new(0);
    let iteration = AtomicUsize::new(0);
    let workers = thread::available_parallelism().map_or(1, usize::from);
    thread::scope(|scope| {
        for _ in 0..workers {
            let collector = Arc::clone(&collector);
            let operations = &operations;
            let operation_index = &operation_index;
            let iteration = &iteration;
            scope.spawn(move || {
                let mut session = collector.spawn_session_collector();
                let mut local_counter = 0;
                while iteration.fetch_add(1, Ordering::Relaxed) < OPERATIONS {
                    let index = operation_index.load(Ordering::Relaxed);
                    let (table_id, index_id, sample) = operations[index].clone();
                    session.update(table_id, index_id, sample);
                    if local_counter % report_per_operation == 0 {
                        session.report();
                    }
                    local_counter += 1;
                    operation_index.fetch_add(1, Ordering::Relaxed);
                }
                session.flush();
            });
        }
    });
    collector.close();
    println!("{name}: {:?}", black_box(started.elapsed()));
}

fn main() {
    benchmark_index_collector("Report per 1 op", 1);
    benchmark_index_collector("Report per 4 ops", 4);
    benchmark_index_collector("Report per 8 ops", 8);
}
