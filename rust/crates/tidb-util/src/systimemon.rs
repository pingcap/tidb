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

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime};

use tidb_log::{Field, Value};

/// Source monitoring cadence.
pub const MONITOR_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Default)]
struct StopState {
    stopped: Mutex<bool>,
    wake: Condvar,
}

impl StopState {
    fn lock(&self) -> MutexGuard<'_, bool> {
        self.stopped
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

/// Owns one background wall-clock monitor.
///
/// The Go server launches `StartMonitor` as a process-lifetime goroutine. The
/// Rust server owns this guard for the same lifetime, while `Drop` gives tests
/// and bounded server runs deterministic cleanup.
#[derive(Debug)]
pub struct SystemTimeMonitor {
    state: Arc<StopState>,
    worker: Option<JoinHandle<()>>,
}

impl SystemTimeMonitor {
    /// Starts monitoring `now` and invokes `on_backward` after every observed
    /// wall-clock regression.
    #[must_use]
    pub fn start<N, H>(now: N, on_backward: H) -> Self
    where
        N: FnMut() -> SystemTime + Send + 'static,
        H: FnMut() + Send + 'static,
    {
        Self::start_with_interval(MONITOR_INTERVAL, now, on_backward)
    }

    fn start_with_interval<N, H>(interval: Duration, mut now: N, mut on_backward: H) -> Self
    where
        N: FnMut() -> SystemTime + Send + 'static,
        H: FnMut() + Send + 'static,
    {
        tidb_log::info("start system time monitor", &[]);
        let state = Arc::new(StopState::default());
        let worker_state = Arc::clone(&state);
        let worker = thread::Builder::new()
            .name("tidb-system-time-monitor".to_owned())
            .spawn(move || loop {
                let previous = now();
                let stopped = worker_state.lock();
                let (stopped, _) = worker_state
                    .wake
                    .wait_timeout_while(stopped, interval, |stopped| !*stopped)
                    .unwrap_or_else(|error| error.into_inner());
                if *stopped {
                    break;
                }
                drop(stopped);

                if now() < previous {
                    tidb_log::error(
                        "system time jump backward",
                        &[Field::new("last", Value::I64(unix_nanos(previous)))],
                    );
                    on_backward();
                }
            })
            .expect("system-time monitor thread must start");

        Self {
            state,
            worker: Some(worker),
        }
    }

    fn stop_and_join(&mut self) {
        {
            let mut stopped = self.state.lock();
            *stopped = true;
        }
        self.state.wake.notify_all();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
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

impl Drop for SystemTimeMonitor {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;

    use super::*;

    #[test]
    fn reports_a_backward_jump_and_stops_cleanly() {
        let calls = Arc::new(AtomicUsize::new(0));
        let now_calls = Arc::clone(&calls);
        let (reported_tx, reported_rx) = mpsc::channel();
        let monitor = SystemTimeMonitor::start_with_interval(
            Duration::from_millis(1),
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
            .expect("backward jump must invoke the handler");
        drop(monitor);
    }
}
