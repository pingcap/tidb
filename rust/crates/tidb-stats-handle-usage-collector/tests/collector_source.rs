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

//! Go `collector_test.go`.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;

use tidb_stats_handle_usage_collector::GlobalCollector;

#[test]
fn session_send_delta() {
    let merged = Arc::new(AtomicI64::new(0));
    let merged_by_worker = Arc::clone(&merged);
    let collector = GlobalCollector::new(move |delta| {
        merged_by_worker.fetch_add(delta, Ordering::Relaxed);
    });
    collector.start_worker();
    let session = collector.spawn_session();
    let mut expected = 0;
    for _ in 0..256 {
        if session.send_delta(1) {
            expected += 1;
        }
    }
    collector.close();
    assert_eq!(merged.load(Ordering::Relaxed), expected);
}

#[test]
fn session_parallel_send_delta() {
    let merged = Arc::new(AtomicI64::new(0));
    let merged_by_worker = Arc::clone(&merged);
    let collector = Arc::new(GlobalCollector::new(move |delta| {
        merged_by_worker.fetch_add(delta, Ordering::Relaxed);
    }));
    collector.start_worker();
    let expected = Arc::new(AtomicI64::new(0));
    let mut workers = Vec::new();
    for _ in 0..256 {
        let session = collector.spawn_session();
        let expected = Arc::clone(&expected);
        workers.push(thread::spawn(move || {
            for _ in 0..256 {
                if session.send_delta(1) {
                    expected.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for worker in workers {
        worker.join().expect("sender panicked");
    }
    collector.close();
    assert_eq!(
        merged.load(Ordering::Relaxed),
        expected.load(Ordering::Relaxed)
    );
}

#[test]
fn session_parallel_send_delta_sync() {
    let merged = Arc::new(AtomicI64::new(0));
    let merged_by_worker = Arc::clone(&merged);
    let collector = Arc::new(GlobalCollector::new(move |delta| {
        merged_by_worker.fetch_add(delta, Ordering::Relaxed);
    }));
    collector.start_worker();
    let mut workers = Vec::new();
    for _ in 0..256 {
        let session = collector.spawn_session();
        workers.push(thread::spawn(move || {
            for _ in 0..256 {
                session.send_delta_sync(1);
            }
        }));
    }
    for worker in workers {
        worker.join().expect("sender panicked");
    }
    collector.close();
    assert_eq!(merged.load(Ordering::Relaxed), 256 * 256);
}
