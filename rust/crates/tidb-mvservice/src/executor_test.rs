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

//! Go master `pkg/mvservice/executor_test.go`.

use std::sync::atomic::{AtomicBool, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use tidb_timer::go_time::{HOUR, SECOND};

use crate::backpressure::{CpuMemBackpressureController, TaskBackpressureController, DEFAULT_TASK_BACKPRESSURE_DELAY};
use crate::executor::TaskExecutor;
use crate::test_helpers_test::*;

const MS: i64 = SECOND / 1000;

fn blocking_task(started: Sender<String>, name: &str, block: Receiver<()>) -> crate::executor::TaskFn {
    let name = name.to_owned();
    Box::new(move || {
        started.send(name).unwrap();
        let _ = block.recv();
        Ok(())
    })
}

#[test]
fn test_task_executor_max_concurrency() {
    let _serial = serial();
    let _clock = install_mock_time_for_test();
    let exec = TaskExecutor::new(1, 0);
    assert!(exec.run());
    assert!(!exec.run()); // idempotent

    let (started_tx, started) = bounded(2);
    let (block_tx, block) = bounded::<()>(0);
    exec.submit("t1", blocking_task(started_tx.clone(), "t1", block.clone()));
    exec.submit("t2", blocking_task(started_tx, "t2", block));

    let first = wait_for_start(&started, HOUR);
    assert!(first == "t1" || first == "t2");
    assert!(started.try_recv().is_err(), "unexpected concurrent start");
    assert_task_executor_metrics(&exec, 2, 0, 0, 0, 0, 1, 1, 0);

    drop(block_tx);
    let second = wait_for_start(&started, HOUR);
    assert_ne!(first, second);
    wait_for_count(|| exec.metrics.counters.finished_count.load(SeqCst), 2);
    assert_task_executor_metrics(&exec, 2, 2, 0, 0, 0, 0, 0, 0);
    exec.close();
}

#[test]
fn test_task_executor_update_max_concurrency() {
    let _serial = serial();
    let _clock = install_mock_time_for_test();
    let exec = TaskExecutor::new(1, 0);
    exec.run();

    let (started_tx, started) = bounded::<()>(2);
    let (block_tx, block) = bounded::<()>(0);
    for name in ["t1", "t2"] {
        let (started_tx, block) = (started_tx.clone(), block.clone());
        exec.submit(
            name,
            Box::new(move || {
                started_tx.send(()).unwrap();
                let _ = block.recv();
                Ok(())
            }),
        );
    }
    wait_for_signal(&started, HOUR);
    assert!(started.try_recv().is_err(), "unexpected concurrent start");
    assert_task_executor_metrics(&exec, 2, 0, 0, 0, 0, 1, 1, 0);

    exec.set_max_concurrency(2);
    wait_for_signal(&started, HOUR);
    assert_task_executor_metrics(&exec, 2, 0, 0, 0, 0, 2, 0, 0);

    drop(block_tx);
    wait_for_count(|| exec.metrics.counters.finished_count.load(SeqCst), 2);
    assert_task_executor_metrics(&exec, 2, 2, 0, 0, 0, 0, 0, 0);
    exec.close();
}

#[test]
fn test_task_executor_timeout_releases_slot() {
    let _serial = serial();
    let (module, _clock) = install_mock_time_for_test();
    let exec = TaskExecutor::new(1, 50 * MS);
    exec.run();

    let (started_tx, started) = unbounded();
    let (block_tx, block) = bounded::<()>(0);
    let (long_started_tx, long_started) = bounded::<()>(1);
    let long_started_tx = Mutex::new(Some(long_started_tx));
    let started_long = started_tx.clone();
    exec.submit(
        "long",
        Box::new(move || {
            started_long.send("long".to_owned()).unwrap();
            long_started_tx.lock().unwrap().take();
            let _ = block.recv();
            Ok(())
        }),
    );
    let _ = long_started.recv();
    assert_task_executor_metrics(&exec, 1, 0, 0, 0, 0, 1, 0, 0);

    module.advance(500 * MS);
    wait_for_count(|| exec.metrics.counters.timeout_count.load(SeqCst), 1);
    wait_for_count(|| exec.metrics.gauges.running_count.load(SeqCst), 0);
    wait_for_count(|| exec.metrics.gauges.timed_out_running_count.load(SeqCst), 1);

    exec.submit(
        "short",
        Box::new(move || {
            started_tx.send("short".to_owned()).unwrap();
            Ok(())
        }),
    );
    wait_for_named_start(&started, "short", HOUR);

    drop(block_tx);
    wait_for_count(|| exec.metrics.counters.finished_count.load(SeqCst), 2);
    assert_task_executor_metrics(&exec, 2, 2, 0, 1, 0, 0, 0, 0);
    exec.close();
}

#[test]
fn test_task_executor_update_timeout() {
    let _serial = serial();
    let (module, _clock) = install_mock_time_for_test();
    let exec = TaskExecutor::new(1, 0);
    exec.run();
    exec.set_timeout(40 * MS);

    let (started_tx, started) = unbounded();
    let (block_tx, block) = bounded::<()>(0);
    let (long_started_tx, long_started) = bounded::<()>(1);
    let long_started_tx = Mutex::new(Some(long_started_tx));
    let started_long = started_tx.clone();
    exec.submit(
        "long",
        Box::new(move || {
            started_long.send("long".to_owned()).unwrap();
            long_started_tx.lock().unwrap().take();
            let _ = block.recv();
            Ok(())
        }),
    );
    let _ = long_started.recv();
    assert_task_executor_metrics(&exec, 1, 0, 0, 0, 0, 1, 0, 0);

    exec.submit(
        "short",
        Box::new(move || {
            started_tx.send("short".to_owned()).unwrap();
            Ok(())
        }),
    );
    module.advance(40 * MS);
    wait_for_named_start(&started, "short", 300 * MS);
    wait_for_count(|| exec.metrics.gauges.timed_out_running_count.load(SeqCst), 1);

    drop(block_tx);
    wait_for_count(|| exec.metrics.counters.timeout_count.load(SeqCst), 1);
    wait_for_count(|| exec.metrics.counters.finished_count.load(SeqCst), 2);
    assert_task_executor_metrics(&exec, 2, 2, 0, 1, 0, 0, 0, 0);
    exec.close();
}

#[test]
fn test_task_executor_reject_after_close() {
    let _serial = serial();
    let exec = TaskExecutor::new(1, 0);
    assert!(exec.close());
    assert!(!exec.close());
    exec.submit("rejected", Box::new(|| Ok(())));
    assert_task_executor_metrics(&exec, 0, 0, 0, 0, 1, 0, 0, 0);
}

struct ToggleBackpressureController {
    blocked: AtomicBool,
    delay: i64,
}

impl TaskBackpressureController for ToggleBackpressureController {
    fn should_backpressure(&self) -> (bool, i64) {
        if self.blocked.load(SeqCst) {
            return (true, self.delay);
        }
        (false, 0)
    }
}

struct BlockingBackpressureController {
    entered: Sender<()>,
    release: Receiver<()>,
}

impl TaskBackpressureController for BlockingBackpressureController {
    fn should_backpressure(&self) -> (bool, i64) {
        let _ = self.entered.try_send(());
        let _ = self.release.recv();
        (false, 0)
    }
}

struct SignalBackpressureController {
    entered: Sender<()>,
    delay: i64,
}

impl TaskBackpressureController for SignalBackpressureController {
    fn should_backpressure(&self) -> (bool, i64) {
        let _ = self.entered.try_send(());
        (true, self.delay)
    }
}

#[test]
fn test_task_executor_backpressure() {
    let _serial = serial();
    let (module, _clock) = install_mock_time_for_test();
    let exec = TaskExecutor::new(1, 0);
    exec.run();
    let controller = Arc::new(ToggleBackpressureController { blocked: AtomicBool::new(true), delay: SECOND });
    exec.set_backpressure_controller(Some(controller.clone()));

    let (started_tx, started) = bounded::<()>(1);
    let (done_tx, done) = bounded::<()>(1);
    exec.submit(
        "blocked",
        Box::new(move || {
            started_tx.send(()).unwrap();
            done_tx.send(()).unwrap();
            Ok(())
        }),
    );
    assert!(started.try_recv().is_err(), "task should not start under backpressure");

    module.advance(5 * SECOND);
    assert!(started.try_recv().is_err(), "task should still be blocked");
    eventually(|| exec.metrics.counters.backpressure_count.load(SeqCst) > 0, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    assert_eq!(exec.metrics.counters.backpressure_count.load(SeqCst), 1);

    controller.blocked.store(false, SeqCst);
    module.advance(SECOND);
    wait_for_signal(&done, HOUR);
    eventually(|| exec.metrics.gauges.backpressure_blocked.load(SeqCst) == 0, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    exec.close();
}

#[test]
fn test_task_executor_backpressure_check_does_not_block_submit() {
    let _serial = serial();
    let exec = TaskExecutor::new(1, 0);
    exec.run();
    let (entered_tx, entered) = bounded(1);
    let (release_tx, release) = bounded::<()>(0);
    exec.set_backpressure_controller(Some(Arc::new(BlockingBackpressureController { entered: entered_tx, release })));

    let (started_tx, started) = unbounded();
    let s1 = started_tx.clone();
    exec.submit("t1", Box::new(move || s1.send("t1".to_owned()).map_err(|e| e.to_string())));
    entered.recv_timeout(Duration::from_secs(1)).expect("worker did not reach backpressure controller");

    let (submit_done_tx, submit_done) = bounded::<()>(0);
    let exec2 = Arc::clone(&exec);
    std::thread::spawn(move || {
        exec2.submit("t2", Box::new(move || started_tx.send("t2".to_owned()).map_err(|e| e.to_string())));
        drop(submit_done_tx);
    });
    assert!(
        matches!(submit_done.recv_timeout(Duration::from_secs(1)), Err(crossbeam_channel::RecvTimeoutError::Disconnected)),
        "submit should not be blocked by backpressure check"
    );

    drop(release_tx);
    let wait_started = |want: &str| loop {
        match started.recv_timeout(Duration::from_secs(1)) {
            Ok(got) if got == want => return,
            Ok(_) => {}
            Err(_) => panic!("timeout waiting for {want} to start"),
        }
    };
    wait_started("t1");
    wait_started("t2");
    eventually(|| exec.metrics.counters.finished_count.load(SeqCst) == 2, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    exec.close();
}

#[test]
fn test_task_executor_close_interrupts_backpressure_wait() {
    let _serial = serial();
    let exec = TaskExecutor::new(1, 0);
    exec.run();
    let (entered_tx, entered) = bounded(1);
    exec.set_backpressure_controller(Some(Arc::new(SignalBackpressureController { entered: entered_tx, delay: HOUR })));
    exec.submit("blocked", Box::new(|| panic!("task should not run under persistent backpressure")));
    entered.recv_timeout(Duration::from_secs(1)).expect("worker did not reach backpressure check");

    let (closed_tx, closed) = bounded::<()>(0);
    let exec2 = Arc::clone(&exec);
    std::thread::spawn(move || {
        exec2.close();
        drop(closed_tx);
    });
    assert!(
        matches!(closed.recv_timeout(Duration::from_secs(1)), Err(crossbeam_channel::RecvTimeoutError::Disconnected)),
        "close should interrupt backpressure wait"
    );
    eventually(|| exec.metrics.gauges.backpressure_blocked.load(SeqCst) == 0, TEST_EVENTUALLY_WAIT, TEST_EVENTUALLY_TICK);
    assert!(exec.metrics.counters.backpressure_count.load(SeqCst) > 0);
}

#[test]
fn test_cpu_mem_backpressure_controller() {
    let mut controller = CpuMemBackpressureController::new(0.7, 0.8, 0);
    controller.get_cpu_usage = Box::new(|| (0.9, false));
    controller.get_mem_total = Box::new(|| 100);
    controller.get_mem_used = Box::new(|| Ok(20));
    assert_eq!(controller.should_backpressure(), (true, DEFAULT_TASK_BACKPRESSURE_DELAY));

    controller.get_cpu_usage = Box::new(|| (0.1, false));
    controller.get_mem_used = Box::new(|| Ok(90));
    controller.delay = 200 * MS;
    assert_eq!(controller.should_backpressure(), (true, 200 * MS));

    controller.get_mem_used = Box::new(|| Ok(10));
    assert_eq!(controller.should_backpressure(), (false, 0));
}

/// Go `TestWaitForCountDoesNotAdvanceMockTime`.
#[test]
fn test_wait_for_count_does_not_advance_mock_time() {
    let _serial = serial();
    let (module, _clock) = install_mock_time_for_test();
    let start = module.now();
    let polls = std::sync::atomic::AtomicI64::new(0);
    wait_for_count(|| polls.fetch_add(1, SeqCst) + 1, 2);
    assert_eq!(start, module.now());
}

fn mk_req(i: i32) -> crate::executor::TaskRequest {
    crate::executor::TaskRequest { name: format!("t{i}"), task: Box::new(|| Ok(())) }
}

/// Go `TestTaskQueueRingBufferFIFO`.
#[test]
fn test_task_queue_ring_buffer_fifo() {
    let mut q = crate::executor::TaskQueue::default();
    for i in 1..=4 {
        q.push(mk_req(i));
    }
    for i in 1..=2 {
        let req = q.pop().expect("a request");
        assert_eq!(format!("t{i}"), req.name);
    }
    for i in 5..=12 {
        q.push(mk_req(i));
    }
    assert_eq!(10, q.length());
    for i in 3..=12 {
        let req = q.pop().expect("a request");
        assert_eq!(format!("t{i}"), req.name);
    }
    assert_eq!(0, q.length());
    assert!(q.pop().is_none());
}

/// Go `TestTaskQueueClearsReferences`.
#[test]
fn test_task_queue_clears_references() {
    let mut q = crate::executor::TaskQueue::default();
    q.push(mk_req(0));
    assert!(q.pop().is_some());
    assert_eq!(0, q.length());
    assert!(!q.buf.is_empty());
    assert!(q.buf[0].is_none(), "the popped slot holds no name or task");

    q.push(mk_req(1));
    q.push(mk_req(2));
    let pending = q.clear();
    assert_eq!(2, pending);
    assert_eq!(0, q.length());
    assert!(q.buf.is_empty());
    assert_eq!(0, q.head);
}
