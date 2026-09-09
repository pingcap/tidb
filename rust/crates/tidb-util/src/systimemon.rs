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

//! System-wall-clock regression monitoring from `pkg/util/systimemon`.

use std::time::{Duration, SystemTime};

use crossbeam_channel::tick;
use tidb_log::{Field, Value};

const MONITOR_INTERVAL: Duration = Duration::from_millis(100);

/// Calls `systime_err_handler` whenever system time jumps backward.
///
/// This function runs for the process lifetime. Its caller owns launching the
/// background thread, as Go's caller owns launching the goroutine.
pub fn start_monitor<N, H>(mut now: N, mut systime_err_handler: H)
where
    N: FnMut() -> SystemTime,
    H: FnMut(),
{
    crate::logutil::bg_logger().info("start system time monitor", &[]);
    let ticker = tick(MONITOR_INTERVAL);
    loop {
        let last = now();
        ticker.recv().expect("system time ticker cannot disconnect");
        if now() < last {
            crate::logutil::bg_logger().error(
                "system time jump backward",
                &[Field::new("last", Value::I64(unix_nanos(last)))],
            );
            systime_err_handler();
        }
    }
}

fn unix_nanos(time: SystemTime) -> i64 {
    let nanos = match time.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(duration) => duration.as_nanos() as i128,
        Err(error) => -(error.duration().as_nanos() as i128),
    };
    nanos as i64
}
