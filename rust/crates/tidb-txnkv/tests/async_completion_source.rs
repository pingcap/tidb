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

#![allow(missing_docs)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tidb_txnkv::rpc::{
    completion_pair, CompletionCallback, CompletionCancellation, CompletionError,
    CompletionRunLoop, CompletionRunLoopState,
};

fn wait_for_state(run_loop: &CompletionRunLoop, expected: CompletionRunLoopState) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while run_loop.state() != expected && Instant::now() < deadline {
        thread::yield_now();
    }
    assert_eq!(run_loop.state(), expected);
}

// client-go/util/async/core_test.go:39 TestInjectOrder.
#[test]
fn injected_transforms_run_in_reverse_order() {
    let run_loop = CompletionRunLoop::new();
    let terminal = Arc::new(Mutex::new(Vec::new()));
    let terminal_result = Arc::clone(&terminal);
    let callback = CompletionCallback::new(run_loop, move |result: Result<Vec<i32>, ()>| {
        *terminal_result.lock().unwrap() = result.unwrap();
    });

    for value in [3, 2, 1] {
        callback
            .inject(move |result| {
                let mut values = result?;
                values.push(value);
                Ok(values)
            })
            .unwrap();
    }
    callback.invoke(Ok(Vec::new())).unwrap();

    assert_eq!(*terminal.lock().unwrap(), vec![1, 2, 3]);
}

// client-go/util/async/core_test.go:51 TestFulfillOnce, including all four
// Invoke/Schedule orderings.
#[test]
fn invoke_and_schedule_fulfill_exactly_once_in_every_order() {
    let invoke_loop = CompletionRunLoop::new();
    let invoked = Arc::new(Mutex::new(Vec::new()));
    let invoked_result = Arc::clone(&invoked);
    let callback = CompletionCallback::new(invoke_loop.clone(), move |result: Result<i32, ()>| {
        invoked_result.lock().unwrap().push(result.unwrap());
    });
    assert_eq!(callback.invoke(Ok(1)), Ok(()));
    assert_eq!(
        callback.invoke(Ok(2)),
        Err(CompletionError::AlreadyCompleted)
    );
    assert_eq!(*invoked.lock().unwrap(), vec![1]);
    assert_eq!(invoke_loop.num_runnable(), 0);

    let schedule_loop = CompletionRunLoop::new();
    let scheduled = Arc::new(Mutex::new(Vec::new()));
    let scheduled_result = Arc::clone(&scheduled);
    let callback =
        CompletionCallback::new(schedule_loop.clone(), move |result: Result<i32, ()>| {
            scheduled_result.lock().unwrap().push(result.unwrap());
        });
    assert_eq!(callback.schedule(Ok(1)), Ok(()));
    assert_eq!(
        callback.schedule(Ok(2)),
        Err(CompletionError::AlreadyCompleted)
    );
    assert_eq!(schedule_loop.num_runnable(), 1);
    assert!(scheduled.lock().unwrap().is_empty());
    assert_eq!(schedule_loop.execute_ready().executed(), 1);
    assert_eq!(*scheduled.lock().unwrap(), vec![1]);

    let invoke_schedule_loop = CompletionRunLoop::new();
    let invoke_schedule = Arc::new(Mutex::new(Vec::new()));
    let invoke_schedule_result = Arc::clone(&invoke_schedule);
    let callback = CompletionCallback::new(
        invoke_schedule_loop.clone(),
        move |result: Result<i32, ()>| {
            invoke_schedule_result.lock().unwrap().push(result.unwrap());
        },
    );
    assert_eq!(callback.invoke(Ok(1)), Ok(()));
    assert_eq!(
        callback.schedule(Ok(2)),
        Err(CompletionError::AlreadyCompleted)
    );
    assert_eq!(invoke_schedule_loop.num_runnable(), 0);
    assert_eq!(*invoke_schedule.lock().unwrap(), vec![1]);

    let schedule_invoke_loop = CompletionRunLoop::new();
    let schedule_invoke = Arc::new(Mutex::new(Vec::new()));
    let schedule_invoke_result = Arc::clone(&schedule_invoke);
    let callback = CompletionCallback::new(
        schedule_invoke_loop.clone(),
        move |result: Result<i32, ()>| {
            schedule_invoke_result.lock().unwrap().push(result.unwrap());
        },
    );
    assert_eq!(callback.schedule(Ok(1)), Ok(()));
    assert_eq!(
        callback.invoke(Ok(2)),
        Err(CompletionError::AlreadyCompleted)
    );
    assert_eq!(schedule_invoke_loop.num_runnable(), 1);
    assert!(schedule_invoke.lock().unwrap().is_empty());
    assert_eq!(schedule_invoke_loop.execute_ready().executed(), 1);
    assert_eq!(*schedule_invoke.lock().unwrap(), vec![1]);
}

// A nested terminal attempt must observe the once claim made before transforms
// or the final callback execute.
#[test]
fn nested_invoke_from_transform_cannot_fulfill_twice() {
    type Callback = CompletionCallback<i32, ()>;

    let slot = Arc::new(Mutex::new(None::<Callback>));
    let nested = Arc::new(Mutex::new(None));
    let terminal = Arc::new(Mutex::new(Vec::new()));
    let terminal_result = Arc::clone(&terminal);
    let callback = Callback::new(CompletionRunLoop::new(), move |result| {
        terminal_result.lock().unwrap().push(result.unwrap());
    });

    let transform_slot = Arc::clone(&slot);
    let transform_nested = Arc::clone(&nested);
    callback
        .inject(move |result| {
            let attempt = transform_slot
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .invoke(Ok(99));
            *transform_nested.lock().unwrap() = Some(attempt);
            result
        })
        .unwrap();
    *slot.lock().unwrap() = Some(callback.clone());

    assert_eq!(callback.invoke(Ok(1)), Ok(()));
    assert_eq!(
        *nested.lock().unwrap(),
        Some(Err(CompletionError::AlreadyCompleted))
    );
    assert_eq!(*terminal.lock().unwrap(), vec![1]);
}

#[test]
fn nested_schedule_from_terminal_cannot_fulfill_twice() {
    type Callback = CompletionCallback<i32, ()>;

    let slot = Arc::new(Mutex::new(None::<Callback>));
    let nested = Arc::new(Mutex::new(None));
    let terminal = Arc::new(Mutex::new(Vec::new()));
    let terminal_slot = Arc::clone(&slot);
    let terminal_nested = Arc::clone(&nested);
    let terminal_result = Arc::clone(&terminal);
    let callback = Callback::new(CompletionRunLoop::new(), move |result| {
        terminal_result.lock().unwrap().push(result.unwrap());
        let attempt = terminal_slot
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .schedule(Ok(99));
        *terminal_nested.lock().unwrap() = Some(attempt);
    });
    *slot.lock().unwrap() = Some(callback.clone());

    assert_eq!(callback.invoke(Ok(1)), Ok(()));
    assert_eq!(
        *nested.lock().unwrap(),
        Some(Err(CompletionError::AlreadyCompleted))
    );
    assert_eq!(*terminal.lock().unwrap(), vec![1]);
    assert_eq!(callback.run_loop().num_runnable(), 0);
}

#[test]
fn concurrent_nested_invoke_schedule_race_has_one_winner() {
    const RACERS: usize = 16;

    let run_loop = CompletionRunLoop::new();
    let terminals = Arc::new(Mutex::new(Vec::new()));
    let terminal_results = Arc::clone(&terminals);
    let callback = CompletionCallback::new(run_loop.clone(), move |result: Result<usize, ()>| {
        terminal_results.lock().unwrap().push(result.unwrap());
    });
    let barrier = Arc::new(Barrier::new(RACERS + 1));
    let mut racers = Vec::new();
    for value in 0..RACERS {
        let callback = callback.clone();
        let barrier = Arc::clone(&barrier);
        racers.push(thread::spawn(move || {
            barrier.wait();
            if value % 2 == 0 {
                callback.invoke(Ok(value))
            } else {
                callback.schedule(Ok(value))
            }
        }));
    }
    barrier.wait();

    let results: Vec<_> = racers
        .into_iter()
        .map(|racer| racer.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| **result == Err(CompletionError::AlreadyCompleted))
            .count(),
        RACERS - 1
    );
    let outcome = run_loop.execute_ready();
    assert_eq!(outcome.error(), None);
    assert!(outcome.executed() <= 1);
    assert_eq!(terminals.lock().unwrap().len(), 1);
}

// client-go/util/async/runloop_test.go:27 TestGo. In the Rust pull model,
// producer work is appended and remains dormant until the caller drives it;
// the completion layer itself starts no runtime or worker thread.
#[test]
fn producer_work_uses_the_caller_driven_queue() {
    let run_loop = CompletionRunLoop::new();
    let value = Arc::new(AtomicUsize::new(0));
    let task_value = Arc::clone(&value);
    run_loop.append(move || task_value.store(1, Ordering::Release));

    assert_eq!(value.load(Ordering::Acquire), 0);
    assert_eq!(run_loop.num_runnable(), 1);
    let outcome = run_loop.execute_ready();
    assert_eq!(outcome.executed(), 1);
    assert_eq!(outcome.error(), None);
    assert_eq!(value.load(Ordering::Acquire), 1);
}

// client-go/util/async/runloop_test.go:47 TestExecWait.
#[test]
fn append_notifies_a_waiting_driver() {
    let run_loop = CompletionRunLoop::new();
    let cancellation = CompletionCancellation::new();
    let values = Arc::new(Mutex::new(Vec::new()));
    let (done_tx, done_rx) = mpsc::channel();
    let driver_loop = run_loop.clone();
    let driver_cancellation = cancellation.clone();
    let driver = thread::spawn(move || {
        done_tx
            .send(driver_loop.execute(&driver_cancellation))
            .unwrap();
    });
    wait_for_state(&run_loop, CompletionRunLoopState::Waiting);

    let task_values = Arc::clone(&values);
    run_loop.append(move || task_values.lock().unwrap().push(1));
    let outcome = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    driver.join().unwrap();

    assert_eq!(outcome.executed(), 1);
    assert_eq!(outcome.error(), None);
    assert_eq!(run_loop.state(), CompletionRunLoopState::Idle);
    assert_eq!(run_loop.num_runnable(), 0);
    assert_eq!(*values.lock().unwrap(), vec![1]);
}

// client-go/util/async/runloop_test.go:63 TestExecOnce.
#[test]
fn one_drive_drains_tasks_appended_by_running_tasks() {
    let run_loop = CompletionRunLoop::new();
    let values = Arc::new(Mutex::new(Vec::new()));
    let outer_loop = run_loop.clone();
    let outer_values = Arc::clone(&values);
    run_loop.append(move || {
        let inner_values = Arc::clone(&outer_values);
        outer_loop.append(move || inner_values.lock().unwrap().push(2));
        outer_values.lock().unwrap().push(1);
    });

    let outcome = run_loop.execute_ready();
    assert_eq!(outcome.executed(), 2);
    assert_eq!(outcome.error(), None);
    assert_eq!(run_loop.state(), CompletionRunLoopState::Idle);
    assert_eq!(run_loop.num_runnable(), 0);
    assert_eq!(*values.lock().unwrap(), vec![1, 2]);
}

// client-go/util/async/runloop_test.go:81 TestExecTwice.
#[test]
fn work_appended_after_a_completed_drive_waits_for_the_next_drive() {
    let run_loop = CompletionRunLoop::new();
    let values = Arc::new(Mutex::new(Vec::new()));
    let first_values = Arc::clone(&values);
    run_loop.append(move || first_values.lock().unwrap().push(1));

    let first = run_loop.execute_ready();
    assert_eq!(first.executed(), 1);
    assert_eq!(first.error(), None);

    let second_values = Arc::clone(&values);
    run_loop.append(move || second_values.lock().unwrap().push(2));
    assert_eq!(*values.lock().unwrap(), vec![1]);
    let second = run_loop.execute_ready();
    assert_eq!(second.executed(), 1);
    assert_eq!(second.error(), None);
    assert_eq!(*values.lock().unwrap(), vec![1, 2]);
}

// client-go/util/async/runloop_test.go:108 TestExecCancelWhileRunning.
#[test]
fn cancellation_preserves_running_then_newly_appended_task_order() {
    let run_loop = CompletionRunLoop::new();
    let cancellation = CompletionCancellation::new();
    let values = Arc::new(Mutex::new(Vec::new()));

    let first_loop = run_loop.clone();
    let first_cancellation = cancellation.clone();
    let first_values = Arc::clone(&values);
    run_loop.append(move || {
        let appended_values = Arc::clone(&first_values);
        first_loop.append(move || appended_values.lock().unwrap().push(3));
        first_values.lock().unwrap().push(1);
        first_cancellation.cancel();
    });
    let second_values = Arc::clone(&values);
    run_loop.append(move || second_values.lock().unwrap().push(2));

    let cancelled = run_loop.execute(&cancellation);
    assert_eq!(cancelled.executed(), 1);
    assert_eq!(cancelled.error(), Some(CompletionError::Cancelled));
    assert_eq!(run_loop.state(), CompletionRunLoopState::Idle);
    assert_eq!(run_loop.num_runnable(), 2);
    assert_eq!(*values.lock().unwrap(), vec![1]);

    let resumed = run_loop.execute_ready();
    assert_eq!(resumed.executed(), 2);
    assert_eq!(resumed.error(), None);
    assert_eq!(*values.lock().unwrap(), vec![1, 2, 3]);
}

// client-go/util/async/runloop_test.go:131 TestExecCancelWhileWaiting.
#[test]
fn cancellation_notifies_a_waiting_driver() {
    let run_loop = CompletionRunLoop::new();
    let cancellation = CompletionCancellation::new();
    let (done_tx, done_rx) = mpsc::channel();
    let driver_loop = run_loop.clone();
    let driver_cancellation = cancellation.clone();
    let driver = thread::spawn(move || {
        done_tx
            .send(driver_loop.execute(&driver_cancellation))
            .unwrap();
    });
    wait_for_state(&run_loop, CompletionRunLoopState::Waiting);

    cancellation.cancel();
    let outcome = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    driver.join().unwrap();
    assert_eq!(outcome.executed(), 0);
    assert_eq!(outcome.error(), Some(CompletionError::Cancelled));
    assert_eq!(run_loop.state(), CompletionRunLoopState::Idle);
    assert_eq!(run_loop.num_runnable(), 0);
}

// client-go/util/async/runloop_test.go:144 TestExecConcurrent.
#[test]
fn a_second_driver_is_rejected_while_the_first_waits() {
    let run_loop = CompletionRunLoop::new();
    let cancellation = CompletionCancellation::new();
    let (done_tx, done_rx) = mpsc::channel();
    let first_loop = run_loop.clone();
    let first_cancellation = cancellation.clone();
    let first = thread::spawn(move || {
        done_tx
            .send(first_loop.execute(&first_cancellation))
            .unwrap();
    });
    wait_for_state(&run_loop, CompletionRunLoopState::Waiting);

    let second = run_loop.execute_ready();
    assert_eq!(second.executed(), 0);
    assert_eq!(second.error(), Some(CompletionError::ConcurrentDriver));

    cancellation.cancel();
    let first_outcome = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    first.join().unwrap();
    assert_eq!(first_outcome.error(), Some(CompletionError::Cancelled));
    assert_eq!(run_loop.state(), CompletionRunLoopState::Idle);
}

#[test]
fn pull_completion_drives_ready_delivery_and_cancellation_suppresses_it() {
    let run_loop = CompletionRunLoop::new();
    let (callback, mut pending) = completion_pair::<i32, ()>(run_loop.clone());
    assert_eq!(pending.try_complete(), Ok(None));
    callback.schedule(Ok(7)).unwrap();
    assert_eq!(pending.try_complete(), Ok(Some(Ok(7))));
    assert_eq!(pending.try_complete(), Ok(None));

    let (cancelled_callback, mut cancelled_pending) = completion_pair::<i32, ()>(run_loop);
    cancelled_callback.schedule(Ok(9)).unwrap();
    cancelled_pending.cancel();
    assert!(cancelled_pending.is_cancelled());
    assert_eq!(cancelled_pending.try_complete(), Ok(None));
}
