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

use tidb_util::logutil::{init_logger, new_log_config, FileLogConfig, DEFAULT_LOG_FORMAT};
use tidb_util::systimemon::{SystemTimeMonitor, MONITOR_INTERVAL};

struct RestoreStdoutLogger;

impl Drop for RestoreStdoutLogger {
    fn drop(&mut self) {
        let config = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            FileLogConfig::default(),
            false,
        );
        let _ = init_logger(&config);
    }
}

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

#[test]
fn system_time_monitor_logs_the_source_lifecycle_messages() {
    let directory = tempfile::tempdir().expect("log directory");
    let filename = directory.path().join("systimemon.log");
    let config = new_log_config(
        "info",
        DEFAULT_LOG_FORMAT,
        "",
        "",
        FileLogConfig {
            filename: filename.to_string_lossy().into_owned(),
            max_size: 4096,
            ..FileLogConfig::default()
        },
        true,
    );
    init_logger(&config).expect("install the logger used by systimemon");
    let _restore = RestoreStdoutLogger;

    let calls = Arc::new(AtomicUsize::new(0));
    let now_calls = Arc::clone(&calls);
    let (reported_tx, reported_rx) = mpsc::channel();
    let monitor = SystemTimeMonitor::start(
        move || {
            if now_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                SystemTime::UNIX_EPOCH + Duration::from_nanos(2)
            } else {
                SystemTime::UNIX_EPOCH + Duration::from_nanos(1)
            }
        },
        move || {
            let _ = reported_tx.send(());
        },
    );

    reported_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("backward jump must invoke the handler");
    drop(monitor);

    let output = std::fs::read_to_string(filename).expect("read system-time monitor logs");
    assert!(output.contains("start system time monitor"), "{output}");
    assert!(output.contains("system time jump backward"), "{output}");
    assert!(output.contains("last=2"), "{output}");
}
