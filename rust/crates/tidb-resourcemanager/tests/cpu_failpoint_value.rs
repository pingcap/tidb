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

//! Source `TestFailpointCPUValue`.

#![cfg(feature = "failpoints")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tidb_resourcemanager::scheduler::{Command, CpuScheduler, Scheduler};
use tidb_resourcemanager::util::{Component, MockGPool};

#[test]
fn test_failpoint_cpu_value() {
    fail::cfg("GetCgroupCPUErr", "return").unwrap();
    let observer = tidb_util::cpu::Observer::new();
    let exit = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for _ in 0..10 {
        let exit = Arc::clone(&exit);
        workers.push(std::thread::spawn(move || {
            while !exit.load(Ordering::Relaxed) {
                std::thread::yield_now();
            }
        }));
    }
    observer.start();
    for _ in 0..10 {
        std::thread::sleep(Duration::from_millis(200));
        let (value, unsupported) = tidb_util::cpu::get_cpu_usage();
        assert!(unsupported);
        assert_eq!(0.0, value);
    }
    let scheduler = CpuScheduler::new();
    let pool = MockGPool::new("test", 10);
    assert_eq!(Command::Hold, scheduler.tune(Component::Unknown, &pool));
    fail::remove("GetCgroupCPUErr");
    exit.store(true, Ordering::Relaxed);
    for worker in workers {
        worker.join().unwrap();
    }
}
