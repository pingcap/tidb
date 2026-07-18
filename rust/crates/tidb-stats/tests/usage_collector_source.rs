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

//! Source-backed tests for the statistics usage collector boundary.

use std::sync::{Arc, Mutex};
use std::thread;

use tidb_stats::{GlobalCollector, DEFAULT_CHANNEL_SIZE};

#[test]
fn source_session_send_delta_drains_accepted_normal_updates() {
    let merged = Arc::new(Mutex::new(0_u64));
    let merged_for_worker = Arc::clone(&merged);
    let collector = GlobalCollector::new(move |delta: u64| {
        *merged_for_worker.lock().expect("merge lock poisoned") += delta;
    });
    collector.start_worker();
    let session = collector.spawn_session();
    let mut expected = 0_u64;
    for _ in 0..256 {
        if session.send_delta(1) {
            expected += 1;
        }
    }
    collector.close();
    assert_eq!(*merged.lock().expect("merge lock poisoned"), expected);
}

#[test]
fn source_parallel_send_delta_keeps_serial_merge_count() {
    let merged = Arc::new(Mutex::new(0_u64));
    let merged_for_worker = Arc::clone(&merged);
    let collector = Arc::new(GlobalCollector::new(move |delta: u64| {
        *merged_for_worker.lock().expect("merge lock poisoned") += delta;
    }));
    collector.start_worker();

    let mut handles = Vec::new();
    let expected = Arc::new(Mutex::new(0_u64));
    for _ in 0..256 {
        let session = collector.spawn_session();
        let expected = Arc::clone(&expected);
        handles.push(thread::spawn(move || {
            for _ in 0..256 {
                if session.send_delta(1) {
                    *expected.lock().expect("expected lock poisoned") += 1;
                }
            }
        }));
    }
    for handle in handles {
        handle.join().expect("sender panicked");
    }
    collector.close();
    assert_eq!(
        *merged.lock().expect("merge lock poisoned"),
        *expected.lock().expect("expected lock poisoned")
    );
}

#[test]
fn source_parallel_send_delta_sync_accepts_every_update() {
    let merged = Arc::new(Mutex::new(0_u64));
    let merged_for_worker = Arc::clone(&merged);
    let collector = Arc::new(GlobalCollector::new(move |delta: u64| {
        *merged_for_worker.lock().expect("merge lock poisoned") += delta;
    }));
    collector.start_worker();

    let mut handles = Vec::new();
    for _ in 0..256 {
        let session = collector.spawn_session();
        handles.push(thread::spawn(move || {
            for _ in 0..256 {
                assert!(session.send_delta_sync(1));
            }
        }));
    }
    for handle in handles {
        handle.join().expect("sender panicked");
    }
    collector.close();
    assert_eq!(*merged.lock().expect("merge lock poisoned"), 256 * 256);
}

#[test]
fn source_queue_capacity_matches_go_defaults() {
    assert_eq!(DEFAULT_CHANNEL_SIZE, 10);
}
