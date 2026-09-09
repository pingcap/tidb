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

//! Source test for Go `pkg/util/systimemon`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use tidb_util::systimemon::start_monitor;

/// Go `TestSystimeMonitor`.
#[test]
fn test_systime_monitor() {
    let err_triggered = Arc::new(AtomicBool::new(false));
    let callback_flag = Arc::clone(&err_triggered);
    let now_triggered = Arc::new(AtomicBool::new(false));
    let clock_flag = Arc::clone(&now_triggered);

    thread::spawn(move || {
        start_monitor(
            move || {
                if !clock_flag.swap(true, Ordering::SeqCst) {
                    return SystemTime::now();
                }
                SystemTime::now() - Duration::from_secs(2)
            },
            move || callback_flag.store(true, Ordering::SeqCst),
        );
    });

    let deadline = Instant::now() + Duration::from_secs(1);
    while !err_triggered.load(Ordering::SeqCst) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(err_triggered.load(Ordering::SeqCst));
}
