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

//! Semantic boundary tests for accepted Go package `pkg/util/systimemon`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, SystemTime};

use tidb_util::systimemon::{SystemTimeMonitor, MONITOR_INTERVAL};

#[test]
fn system_time_monitor_reports_the_source_backward_jump() {
    assert_eq!(MONITOR_INTERVAL, Duration::from_millis(100));

    let calls = Arc::new(AtomicUsize::new(0));
    let now_calls = Arc::clone(&calls);
    let (reported_tx, reported_rx) = mpsc::channel();
    let monitor = SystemTimeMonitor::start(
        move || {
            if now_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                SystemTime::UNIX_EPOCH + Duration::from_secs(2)
            } else {
                SystemTime::UNIX_EPOCH + Duration::from_secs(1)
            }
        },
        move || {
            let _ = reported_tx.send(());
        },
    );

    reported_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("the source backward jump must invoke its callback");
    drop(monitor);
}
