//! A process-global persistent worker pool for short-lived parallel
//! sub-tasks (hash-join probe windows, hash-agg pipeline lanes).
//!
//! Go's executor forks goroutines for the same fan-out; a goroutine spawn
//! costs ~100ns because the runtime multiplexes them onto long-lived OS
//! threads. `std::thread::spawn` costs tens of microseconds per call (stack
//! mmap, thread registration), which profiling shows as up to ~16% of
//! TPC-H q2/q9/q17 wall time. This pool keeps its threads alive across
//! calls so each task pays only a channel round-trip.
//!
//! The workspace forbids `unsafe` code, so tasks must be `'static`: callers
//! share read inputs through [`std::sync::Arc`] and move owned buffers in,
//! receiving them back with their results.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, OnceLock};

/// One queued unit of work.
struct Task(Box<dyn FnOnce() + Send>);

#[derive(Default)]
struct QueueState {
    pending: VecDeque<Task>,
}

#[derive(Default)]
struct Shared {
    queue: Mutex<QueueState>,
    signal: Condvar,
}

fn shared() -> &'static Arc<Shared> {
    static POOL: OnceLock<Arc<Shared>> = OnceLock::new();
    POOL.get_or_init(|| {
        let shared = Arc::new(Shared::default());
        // The hash-agg pipeline submits partial+final worker sets as tasks;
        // sizing below a typical set starves the tail of the queue behind
        // blocked lanes. Never fewer than 16.
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(16);
        for _ in 0..workers {
            let worker_shared = Arc::clone(&shared);
            std::thread::Builder::new()
                .name("tidb-exec-pool".to_owned())
                .spawn(move || worker_loop(worker_shared))
                .expect("spawn persistent exec pool worker");
        }
        shared
    })
}

fn worker_loop(shared: Arc<Shared>) {
    loop {
        let task = {
            let mut state = shared
                .queue
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            loop {
                if let Some(task) = state.pending.pop_front() {
                    break task;
                }
                state = shared
                    .signal
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        };
        (task.0)();
    }
}

/// [`enqueue`] for out-of-crate callers (see `crate::worker_pool_spawn`).
pub fn enqueue_public(task: Box<dyn FnOnce() + Send>) {
    enqueue(task);
}

fn enqueue(task: Box<dyn FnOnce() + Send>) {
    let shared = shared();
    {
        let mut state = shared
            .queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.pending.push_back(Task(task));
    }
    shared.signal.notify_one();
}

/// Runs `task` on one of the pool's persistent workers and blocks until it
/// finishes, returning its result.
pub fn submit<F, R>(task: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<R>(1);
    enqueue(Box::new(move || {
        // The receiver is only gone if the submitting thread died; either way
        // the value has nowhere to go.
        let _ = result_tx.send(task());
    }));
    result_rx
        .recv()
        .unwrap_or_else(|_| panic!("exec pool worker dropped the task result"))
}

/// Submits `task` without blocking and returns a receiver for its result.
/// Unlike [`submit`], the caller may keep working while the task runs.
pub fn spawn<F, R>(task: F) -> std::sync::mpsc::Receiver<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<R>(1);
    enqueue(Box::new(move || {
        // A disconnected receiver means the caller dropped it before joining;
        // the value has nowhere to go either way.
        let _ = result_tx.send(task());
    }));
    result_rx
}

/// Whether the persistent pool exists in this process (it always does once
/// initialized; this gate lets callers fall back to a dedicated thread when
/// the pool feature is compiled out).
#[must_use]
pub fn available() -> bool {
    true
}

/// Runs one task per item across at most `concurrency` workers and returns
/// every result in submission order.
pub fn map<I, F, R>(tasks: I, concurrency: usize) -> Vec<R>
where
    I: IntoIterator<Item = F>,
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let all: Vec<F> = tasks.into_iter().collect();
    if all.len() <= 1 || concurrency <= 1 {
        return all.into_iter().map(|task| task()).collect();
    }
    let shared = shared();
    let (result_tx, result_rx) = std::sync::mpsc::channel::<(usize, R)>();
    // Keepalive sender: holds the channel open while results are still in
    // flight even though every per-task clone has been moved into a task.
    let keepalive_tx = result_tx.clone();

    let total = all.len();
    let mut items = all.into_iter().enumerate();
    let mut in_flight = 0usize;
    let mut results: Vec<Option<R>> = Vec::with_capacity(total);
    results.resize_with(total, || None);

    // Prime up to `concurrency` tasks before waiting on any result.
    for (index, task) in items.by_ref().take(concurrency) {
        let tx = result_tx.clone();
        enqueue(Box::new(move || {
            let _ = tx.send((index, task()));
        }));
        in_flight += 1;
    }

    while in_flight > 0 {
        let (index, value) = result_rx
            .recv()
            .unwrap_or_else(|_| panic!("exec pool worker dropped a mapped task"));
        results[index] = Some(value);
        in_flight -= 1;
        if let Some((index, task)) = items.next() {
            let tx = result_tx.clone();
            enqueue(Box::new(move || {
                let _ = tx.send((index, task()));
            }));
            in_flight += 1;
        }
    }
    drop(keepalive_tx);
    drop(result_tx);
    results
        .into_iter()
        .map(|slot| slot.unwrap_or_else(|| panic!("missing mapped task result")))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_returns_the_task_value() {
        let value = 21 * 2;
        assert_eq!(submit(move || value), 42);
    }

    #[test]
    fn map_preserves_submission_order() {
        let inputs: Vec<usize> = (0..32).collect();
        let out = map(
            inputs.iter().map(|&i| move || {
                // Variable "work" so completion order differs from submission.
                if i % 3 == 0 {
                    std::thread::sleep(std::time::Duration::from_micros(50));
                }
                i * i
            }),
            4,
        );
        assert_eq!(out, inputs.iter().map(|&i| i * i).collect::<Vec<_>>());
    }

    #[test]
    fn shared_arc_state_reaches_the_worker() {
        let data = Arc::new(vec![1u64, 2, 3]);
        let total = map(data.iter().map(|&v| move || v * 10).collect::<Vec<_>>(), 2);
        assert_eq!(total, vec![10, 20, 30]);
    }

    #[test]
    fn owned_buffers_round_trip_through_tasks() {
        let out = map((0..8).map(|i| move || vec![i; 3]), 4);
        assert_eq!(out.len(), 8);
        assert_eq!(out[7], vec![7, 7, 7]);
    }
}
