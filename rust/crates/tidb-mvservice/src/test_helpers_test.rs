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

//! Go master `pkg/mvservice/test_helpers_test.go`: the scheduler-side
//! helpers. The package clock is process-wide, so every test that reads it
//! holds [`serial`] (Go's tests run one package at a time per process).

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crossbeam_channel::Receiver;
use tidb_timer::go_time::GoTime;

use crate::executor::TaskExecutor;
use crate::time_proxy::{install_mock_time_module_for_test, mvs_after, MockTimeGuard, MockTimeModule};

/// Go `testEventuallyWait`.
pub(crate) const TEST_EVENTUALLY_WAIT: Duration = Duration::from_secs(1);
/// Go `testEventuallyTick`.
pub(crate) const TEST_EVENTUALLY_TICK: Duration = Duration::from_millis(1);

static SERIAL: Mutex<()> = Mutex::new(());

/// One test at a time over the process-wide clock.
pub(crate) fn serial() -> MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go `installMockTimeForTest`: a mock clock for the rest of the test.
pub(crate) fn install_mock_time_for_test() -> (Arc<MockTimeModule>, MockTimeGuard) {
    let module = MockTimeModule::new(GoTime::from_unix(1_700_000_000, 0));
    let guard = install_mock_time_module_for_test(Arc::clone(&module));
    (module, guard)
}

/// Go `require.Eventually` on wall time.
pub(crate) fn eventually(mut cond: impl FnMut() -> bool, wait: Duration, tick: Duration) {
    let deadline = Instant::now() + wait;
    loop {
        if cond() {
            return;
        }
        assert!(Instant::now() < deadline, "condition never satisfied");
        std::thread::sleep(tick);
    }
}

/// Go `waitForCount`.
pub(crate) fn wait_for_count(get: impl Fn() -> i64, want: i64) {
    eventually(|| get() == want, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
}

/// Go `waitForSignal` (the package clock's timeout).
pub(crate) fn wait_for_signal(ch: &Receiver<()>, timeout: i64) {
    crossbeam_channel::select! {
        recv(ch) -> _ => {}
        recv(mvs_after(timeout)) -> _ => panic!("timeout waiting for signal"),
    }
}

/// Go `waitForStart`.
pub(crate) fn wait_for_start(ch: &Receiver<String>, timeout: i64) -> String {
    crossbeam_channel::select! {
        recv(ch) -> got => got.unwrap(),
        recv(mvs_after(timeout)) -> _ => panic!("timeout waiting for start"),
    }
}

/// Go `waitForNamedStart`.
pub(crate) fn wait_for_named_start(ch: &Receiver<String>, want: &str, timeout: i64) {
    let deadline = mvs_after(timeout);
    loop {
        crossbeam_channel::select! {
            recv(ch) -> got => if got.unwrap() == want { return; },
            recv(deadline) -> _ => panic!("timeout waiting for {want} to start"),
        }
    }
}

/// Go `assertTaskExecutorMetrics`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn assert_task_executor_metrics(
    exec: &TaskExecutor,
    submitted: i64,
    finished: i64,
    failed: i64,
    timeout: i64,
    rejected: i64,
    running: i64,
    waiting: i64,
    timed_out_running: i64,
) {
    use std::sync::atomic::Ordering::SeqCst;
    let c = &exec.metrics.counters;
    let g = &exec.metrics.gauges;
    assert_eq!(
        [
            c.submitted_count.load(SeqCst),
            c.finished_count.load(SeqCst),
            c.failed_count.load(SeqCst),
            c.timeout_count.load(SeqCst),
            c.rejected_count.load(SeqCst),
            g.running_count.load(SeqCst),
            g.waiting_count.load(SeqCst),
            g.timed_out_running_count.load(SeqCst),
        ],
        [submitted, finished, failed, timeout, rejected, running, waiting, timed_out_running]
    );
}
