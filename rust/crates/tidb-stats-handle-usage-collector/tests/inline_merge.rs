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

//! `GlobalCollector::with_inline_merge`: a delta the sending thread can
//! merge never reaches the worker; one it hands back takes the channel.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tidb_stats_handle_usage_collector::GlobalCollector;

#[test]
fn inline_merge_skips_the_worker() {
    let merged = Arc::new(AtomicI64::new(0));
    let merged_by_worker = Arc::new(AtomicI64::new(0));
    let (worker_target, inline_target) = (Arc::clone(&merged), Arc::clone(&merged));
    let by_worker = Arc::clone(&merged_by_worker);
    let collector = GlobalCollector::with_inline_merge(
        move |delta: i64| {
            worker_target.fetch_add(delta, Ordering::SeqCst);
            by_worker.fetch_add(delta, Ordering::SeqCst);
        },
        move |delta: i64| {
            inline_target.fetch_add(delta, Ordering::SeqCst);
            Ok(())
        },
    );
    // No worker: the channel would fill after DEFAULT_CHANNEL_SIZE sends,
    // but every delta merges inline instead.
    let session = collector.spawn_session();
    for delta in 1..=50 {
        assert!(session.send_delta(delta));
    }
    assert_eq!(merged.load(Ordering::SeqCst), (1..=50).sum::<i64>());
    assert_eq!(merged_by_worker.load(Ordering::SeqCst), 0);
    collector.close();
}

#[test]
fn declined_inline_merge_takes_the_channel() {
    let merged = Arc::new(AtomicI64::new(0));
    let merged_by_worker = Arc::new(AtomicI64::new(0));
    let busy = Arc::new(AtomicBool::new(true));
    let (worker_target, inline_target) = (Arc::clone(&merged), Arc::clone(&merged));
    let by_worker = Arc::clone(&merged_by_worker);
    let busy_for_inline = Arc::clone(&busy);
    let collector = GlobalCollector::with_inline_merge(
        move |delta: i64| {
            worker_target.fetch_add(delta, Ordering::SeqCst);
            by_worker.fetch_add(delta, Ordering::SeqCst);
        },
        move |delta: i64| {
            if busy_for_inline.load(Ordering::SeqCst) {
                Err(delta)
            } else {
                inline_target.fetch_add(delta, Ordering::SeqCst);
                Ok(())
            }
        },
    );
    collector.start_worker();
    let session = collector.spawn_session();
    assert!(session.send_delta(7));
    let deadline = Instant::now() + Duration::from_secs(10);
    while merged_by_worker.load(Ordering::SeqCst) != 7 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(merged_by_worker.load(Ordering::SeqCst), 7);

    busy.store(false, Ordering::SeqCst);
    assert!(session.send_delta(5));
    assert_eq!(merged.load(Ordering::SeqCst), 12);
    assert_eq!(merged_by_worker.load(Ordering::SeqCst), 7);
    collector.close();
}
