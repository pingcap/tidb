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
    completion_pair, CompletionCallback, CompletionCancellation, CompletionCancellationReason,
    CompletionError, CompletionRunLoop, CompletionRunLoopState, CompletionSpawner,
    UnaryCallContext,
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
    callback.invoke(Ok(Vec::new()));

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
    callback.invoke(Ok(1));
    callback.invoke(Ok(2));
    assert_eq!(*invoked.lock().unwrap(), vec![1]);
    assert_eq!(invoke_loop.num_runnable(), 0);

    let schedule_loop = CompletionRunLoop::new();
    let scheduled = Arc::new(Mutex::new(Vec::new()));
    let scheduled_result = Arc::clone(&scheduled);
    let callback =
        CompletionCallback::new(schedule_loop.clone(), move |result: Result<i32, ()>| {
            scheduled_result.lock().unwrap().push(result.unwrap());
        });
    callback.schedule(Ok(1));
    callback.schedule(Ok(2));
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
    callback.invoke(Ok(1));
    callback.schedule(Ok(2));
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
    callback.schedule(Ok(1));
    callback.invoke(Ok(2));
    assert_eq!(schedule_invoke_loop.num_runnable(), 1);
    assert!(schedule_invoke.lock().unwrap().is_empty());
    assert_eq!(schedule_invoke_loop.execute_ready().executed(), 1);
    assert_eq!(*schedule_invoke.lock().unwrap(), vec![1]);
}

#[test]
fn concurrent_duplicate_waits_for_the_winning_once_body() {
    let run_loop = CompletionRunLoop::new();
    let terminal = Arc::new(Mutex::new(Vec::new()));
    let terminal_result = Arc::clone(&terminal);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let callback = CompletionCallback::new(run_loop.clone(), move |result: Result<i32, ()>| {
        entered_tx.send(()).unwrap();
        release_rx.recv().unwrap();
        terminal_result.lock().unwrap().push(result.unwrap());
    });

    let winner_callback = callback.clone();
    let winner = thread::spawn(move || winner_callback.invoke(Ok(1)));
    entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

    let (loser_started_tx, loser_started_rx) = mpsc::channel();
    let (loser_done_tx, loser_done_rx) = mpsc::channel();
    let loser_callback = callback.clone();
    let loser = thread::spawn(move || {
        loser_started_tx.send(()).unwrap();
        loser_callback.schedule(Ok(2));
        loser_done_tx.send(()).unwrap();
    });
    loser_started_rx
        .recv_timeout(Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        loser_done_rx.recv_timeout(Duration::from_millis(50)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );

    release_tx.send(()).unwrap();
    loser_done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    // The losing Schedule returns only after the winning Invoke body. It must
    // not leave a second terminal task for an immediate drive.
    let outcome = run_loop.execute_ready();
    assert_eq!(outcome.executed(), 0);
    assert_eq!(outcome.error(), None);
    winner.join().unwrap();
    loser.join().unwrap();
    assert_eq!(*terminal.lock().unwrap(), vec![1]);
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

    for racer in racers {
        racer.join().unwrap();
    }
    let outcome = run_loop.execute_ready();
    assert_eq!(outcome.error(), None);
    assert!(outcome.executed() <= 1);
    assert_eq!(terminals.lock().unwrap().len(), 1);
}

#[derive(Default)]
struct TestSpawner {
    tasks: Mutex<Vec<Box<dyn FnOnce() + Send + 'static>>>,
}

impl CompletionSpawner for TestSpawner {
    fn go(&self, task: Box<dyn FnOnce() + Send + 'static>) {
        self.tasks.lock().unwrap().push(task);
    }
}

impl TestSpawner {
    fn take(&self) -> Box<dyn FnOnce() + Send + 'static> {
        self.tasks.lock().unwrap().pop().unwrap()
    }
}

// client-go/util/async/runloop_test.go:27 TestGo.
#[test]
fn go_starts_a_thread_or_delegates_to_the_configured_pool() {
    let run_loop = CompletionRunLoop::new();
    let (done_tx, done_rx) = mpsc::channel();
    run_loop.go(move || done_tx.send(1).unwrap());
    assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)), Ok(1));

    let spawner = Arc::new(TestSpawner::default());
    let pooled_loop = CompletionRunLoop::with_spawner(spawner.clone());
    let value = Arc::new(AtomicUsize::new(0));
    let task_value = Arc::clone(&value);
    pooled_loop.go(move || task_value.store(1, Ordering::Release));
    assert_eq!(value.load(Ordering::Acquire), 0);
    assert_eq!(spawner.tasks.lock().unwrap().len(), 1);
    spawner.take()();
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
    let cancellation = CompletionCancellation::new();
    let values = Arc::new(Mutex::new(Vec::new()));
    let (release_tx, release_rx) = mpsc::channel();
    let delayed_loop = run_loop.clone();
    let delayed_values = Arc::clone(&values);
    let first_values = Arc::clone(&values);
    let producer_loop = run_loop.clone();
    run_loop.append(move || {
        first_values.lock().unwrap().push(1);
        producer_loop.go(move || {
            release_rx.recv().unwrap();
            delayed_loop.append(move || delayed_values.lock().unwrap().push(2));
        });
    });

    let first = run_loop.execute_ready();
    assert_eq!(first.executed(), 1);
    assert_eq!(first.error(), None);

    let (done_tx, done_rx) = mpsc::channel();
    let driver_loop = run_loop.clone();
    let driver_cancellation = cancellation.clone();
    let driver = thread::spawn(move || {
        done_tx
            .send(driver_loop.execute(&driver_cancellation))
            .unwrap();
    });
    wait_for_state(&run_loop, CompletionRunLoopState::Waiting);
    release_tx.send(()).unwrap();

    let second = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    driver.join().unwrap();
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
        first_cancellation.cancel_with(CompletionCancellationReason::DeadlineExceeded);
    });
    let second_values = Arc::clone(&values);
    run_loop.append(move || second_values.lock().unwrap().push(2));

    let cancelled = run_loop.execute(&cancellation);
    assert_eq!(cancelled.executed(), 1);
    assert_eq!(cancelled.error(), Some(CompletionError::DeadlineExceeded));
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

#[test]
fn one_deadline_cancellation_safely_wakes_multiple_run_loops() {
    let first_loop = CompletionRunLoop::new();
    let second_loop = CompletionRunLoop::new();
    let cancellation = CompletionCancellation::new();
    let (done_tx, done_rx) = mpsc::channel();

    let first_driver_loop = first_loop.clone();
    let first_cancellation = cancellation.clone();
    let first_done = done_tx.clone();
    let first = thread::spawn(move || {
        first_done
            .send(first_driver_loop.execute(&first_cancellation))
            .unwrap();
    });
    let second_driver_loop = second_loop.clone();
    let second_cancellation = cancellation.clone();
    let second = thread::spawn(move || {
        done_tx
            .send(second_driver_loop.execute(&second_cancellation))
            .unwrap();
    });
    wait_for_state(&first_loop, CompletionRunLoopState::Waiting);
    wait_for_state(&second_loop, CompletionRunLoopState::Waiting);

    cancellation.cancel_with(CompletionCancellationReason::DeadlineExceeded);
    cancellation.cancel();
    let first_outcome = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    let second_outcome = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    first.join().unwrap();
    second.join().unwrap();

    assert_eq!(
        cancellation.reason(),
        Some(CompletionCancellationReason::DeadlineExceeded)
    );
    assert_eq!(
        first_outcome.error(),
        Some(CompletionError::DeadlineExceeded)
    );
    assert_eq!(
        second_outcome.error(),
        Some(CompletionError::DeadlineExceeded)
    );
    assert_eq!(first_loop.state(), CompletionRunLoopState::Idle);
    assert_eq!(second_loop.state(), CompletionRunLoopState::Idle);
}

// client-go/util/async/runloop_test.go:144 TestExecConcurrent.
#[test]
fn a_second_driver_is_rejected_while_the_first_runs() {
    let run_loop = CompletionRunLoop::new();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    run_loop.append(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    });
    let (done_tx, done_rx) = mpsc::channel();
    let first_loop = run_loop.clone();
    let first = thread::spawn(move || {
        done_tx.send(first_loop.execute_ready()).unwrap();
    });
    started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(run_loop.state(), CompletionRunLoopState::Running);

    let second = run_loop.execute_ready();
    assert_eq!(second.executed(), 0);
    assert_eq!(second.error(), Some(CompletionError::ConcurrentDriver));

    release_tx.send(()).unwrap();
    let first_outcome = done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
    first.join().unwrap();
    assert_eq!(first_outcome.executed(), 1);
    assert_eq!(first_outcome.error(), None);
    assert_eq!(run_loop.state(), CompletionRunLoopState::Idle);
}

#[test]
fn pull_completion_drives_ready_delivery_and_cancellation_suppresses_it() {
    let run_loop = CompletionRunLoop::new();
    let (request, mut pending) = completion_pair::<i32, (), _>(run_loop.clone(), || {});
    assert_eq!(pending.try_complete(), Ok(None));
    request.schedule(Ok(7));
    assert_eq!(pending.try_complete(), Ok(Some(Ok(7))));
    assert_eq!(pending.try_complete(), Ok(None));

    let cancel_calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = Arc::clone(&cancel_calls);
    let (cancelled_request, mut cancelled_pending) =
        completion_pair::<i32, (), _>(run_loop, move || {
            hook_calls.fetch_add(1, Ordering::AcqRel);
        });
    let cancellation_observer = cancelled_request.clone();
    assert!(!cancelled_request.is_cancelled());
    cancelled_pending.cancel();
    cancelled_pending.cancel();
    assert!(cancelled_pending.is_cancelled());
    assert!(cancelled_request.is_cancelled());
    assert!(cancellation_observer.is_cancelled());
    assert_eq!(cancel_calls.load(Ordering::Acquire), 1);
    cancelled_request.schedule(Ok(9));
    assert_eq!(cancelled_pending.try_complete(), Ok(None));
}

#[test]
fn completion_request_schedules_one_typed_failure() {
    let run_loop = CompletionRunLoop::new();
    let (request, mut pending) = completion_pair::<i32, &'static str, _>(run_loop.clone(), || {});
    let scheduler_request = request.clone();

    scheduler_request.schedule_error("batch connection closed");
    request.schedule(Ok(7));

    assert_eq!(run_loop.num_runnable(), 1);
    assert_eq!(
        pending.try_complete(),
        Ok(Some(Err("batch connection closed")))
    );
    assert_eq!(pending.try_complete(), Ok(None));
}

#[test]
fn pull_backed_invoke_before_wait_uses_the_wakeable_publication_queue() {
    let run_loop = CompletionRunLoop::new();
    let (request, mut pending) = completion_pair::<i32, (), _>(run_loop.clone(), || {});
    let call = UnaryCallContext::with_timeout(Duration::from_secs(1));

    request.invoke(Ok(17));

    assert_eq!(run_loop.num_runnable(), 1);
    assert_eq!(pending.complete(&call), Ok(Ok(17)));
    assert_eq!(run_loop.num_runnable(), 0);
}

#[test]
fn pull_backed_invoke_racing_a_blocked_wait_wakes_the_same_run_loop() {
    let run_loop = CompletionRunLoop::new();
    let (request, mut pending) = completion_pair::<i32, (), _>(run_loop.clone(), || {});
    let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
    let (done_tx, done_rx) = mpsc::channel();
    let waiter = thread::spawn(move || done_tx.send(pending.complete(&call)).unwrap());
    wait_for_state(&run_loop, CompletionRunLoopState::Waiting);

    request.invoke(Ok(23));

    assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)), Ok(Ok(Ok(23))));
    waiter.join().unwrap();
    assert_eq!(run_loop.state(), CompletionRunLoopState::Idle);
}
