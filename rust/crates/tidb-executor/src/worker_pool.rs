//! Persistent workers for ready, owned executor batches.
//! Go multiplexes executor goroutines onto reusable runtime threads. This
//! queue likewise keeps CPU workers alive, parking them when no work is ready.
//! Blocking input pipelines run on separate lanes; jobs move their buffers
//! and return ownership at a completion barrier.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

type LaneTask = Box<dyn FnOnce() + Send + 'static>;

/// One reusable process-wide lane set. The width is part of the registry key
/// so changing the session concurrency still gets a matching lane set.
struct SharedLanePool {
    sender: std::sync::mpsc::SyncSender<LaneTask>,
    in_use: AtomicBool,
}

#[derive(Hash, PartialEq, Eq)]
struct LanePoolKey {
    name: &'static str,
    workers: usize,
}

fn lane_registry() -> &'static Mutex<HashMap<LanePoolKey, Vec<Arc<SharedLanePool>>>> {
    static REGISTRY: OnceLock<Mutex<HashMap<LanePoolKey, Vec<Arc<SharedLanePool>>>>> =
        OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn shared_lane_pool(name: &'static str, workers: usize) -> Arc<SharedLanePool> {
    let mut registry = lane_registry()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let pools = registry.entry(LanePoolKey { name, workers }).or_default();
    if let Some(pool) = pools.iter().find(|pool| {
        pool.in_use
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }) {
        return Arc::clone(pool);
    }
    let (sender, receiver) = std::sync::mpsc::sync_channel::<LaneTask>(workers);
    let receiver = Arc::new(Mutex::new(receiver));
    for _ in 0..workers {
        let receiver = Arc::clone(&receiver);
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || loop {
                let task = receiver
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .recv();
                let Ok(task) = task else { break };
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task));
            })
            .expect("spawn persistent exec lane");
    }
    let pool = Arc::new(SharedLanePool {
        sender,
        in_use: AtomicBool::new(true),
    });
    pools.push(Arc::clone(&pool));
    pool
}

struct LaneActivity {
    state: Mutex<LaneActivityState>,
    done: Condvar,
}

struct LaneActivityState {
    in_flight: usize,
    closed: bool,
}

/// A per-executor view of a process-wide registry of reusable lanes for work
/// that may block on external I/O. Go's index-join workers are goroutines
/// reused for every task; idle sets are returned to the registry after Close,
/// while overlapping executors receive separate sets to preserve concurrency.
pub struct LanePool {
    shared: Arc<SharedLanePool>,
    activity: Arc<LaneActivity>,
    width: usize,
}

impl LanePool {
    /// Gets `concurrency` persistent lanes and bounds this executor's
    /// in-flight work to the same count, matching Go's buffered worker
    /// channel. Native workers are created once per concurrently active set
    /// and reused by later executors.
    pub fn new(name: &'static str, concurrency: usize) -> Self {
        let workers = concurrency.max(1);
        let shared = shared_lane_pool(name, workers);
        Self {
            shared,
            activity: Arc::new(LaneActivity {
                state: Mutex::new(LaneActivityState {
                    in_flight: 0,
                    closed: false,
                }),
                done: Condvar::new(),
            }),
            width: workers,
        }
    }

    /// Queues one task for a reusable lane. Admission is bounded per
    /// executor, while idle lane sets are reused across executors.
    pub fn submit<F>(&self, task: F) -> Result<(), ()>
    where
        F: FnOnce() + Send + 'static,
    {
        {
            let mut state = self
                .activity
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while state.in_flight >= self.width && !state.closed {
                state = self
                    .activity
                    .done
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            if state.closed {
                return Err(());
            }
            state.in_flight += 1;
        }
        let activity = Arc::clone(&self.activity);
        let result = self.shared.sender.send(Box::new(move || {
            let _guard = LaneActivityGuard { activity };
            task();
        }));
        if result.is_err() {
            let mut state = self
                .activity
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.in_flight = state.in_flight.saturating_sub(1);
            self.activity.done.notify_all();
        }
        result.map_err(|_| ())
    }

    /// Waits for every task currently admitted through this executor view.
    /// Unlike `Drop`, this keeps the reusable lane set open for a later
    /// build/restore round.
    pub fn wait(&self) {
        let mut state = self
            .activity
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.in_flight != 0 {
            state = self
                .activity
                .done
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct LaneActivityGuard {
    activity: Arc<LaneActivity>,
}

impl Drop for LaneActivityGuard {
    fn drop(&mut self) {
        let mut state = self
            .activity
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight = state.in_flight.saturating_sub(1);
        self.activity.done.notify_all();
    }
}
impl Drop for LanePool {
    fn drop(&mut self) {
        let mut state = self
            .activity
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.closed = true;
        self.activity.done.notify_all();
        while state.in_flight != 0 {
            state = self
                .activity
                .done
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        self.shared.in_use.store(false, Ordering::Release);
    }
}

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
        // Go's GOMAXPROCS: one worker per core. Every task on this pool is
        // compute that runs to completion; a lane that blocks for a query's
        // lifetime (an aggregate partial lane, an index-join task draining
        // TiKV, a lookup batch) is a goroutine in Go and gets its own thread
        // here ([`spawn_lane`]), so a blocked lane never holds a core's
        // worker and the pool never starves behind one.
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .max(2);
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

/// Runs a long-lived lane on a thread of its own and returns a receiver for
/// its result: Go's goroutine for a worker that blocks on a channel or on
/// TiKV for a query's lifetime. Such a lane must not occupy one of the
/// pool's per-core workers, where it would starve the short compute tasks
/// (and deadlock a core-sized pool when the tasks it waits for are queued
/// behind it). A thread costs tens of microseconds, once per lane per query.
pub fn spawn_lane<F, R>(name: &'static str, task: F) -> std::sync::mpsc::Receiver<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<R>(1);
    spawn_lane_detached(name, move || {
        // A disconnected receiver means the caller dropped it before joining;
        // the value has nowhere to go either way.
        let _ = result_tx.send(task());
    });
    result_rx
}

/// [`spawn_lane`] for a lane that reports through its own channel.
pub fn spawn_lane_detached<F>(name: &'static str, task: F)
where
    F: FnOnce() + Send + 'static,
{
    // Go's runtime aborts the process when it cannot create a thread
    // (`newosproc`); a lane that cannot start has no other home either.
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(task)
        .expect("spawn exec lane thread");
}

/// Submits `task` without blocking and returns a receiver for its result.
/// Unlike [`submit`], the caller may keep working while the task runs.
pub fn spawn<F, R>(task: F) -> std::sync::mpsc::Receiver<R>
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    #[cfg(test)]
    SPAWNED.with(|count| count.set(count.get() + 1));
    let (result_tx, result_rx) = std::sync::mpsc::sync_channel::<R>(1);
    enqueue(Box::new(move || {
        // A disconnected receiver means the caller dropped it before joining;
        // the value has nowhere to go either way.
        let _ = result_tx.send(task());
    }));
    result_rx
}

#[cfg(test)]
thread_local! {
    /// Tasks this thread handed to [`spawn`]; lets an executor test prove that
    /// a small input never reached a pool lane. Per thread, so tests running
    /// in parallel cannot disturb each other's count.
    static SPAWNED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// [`spawn`] calls made by the calling thread so far (test builds only).
#[cfg(test)]
pub(crate) fn spawned_so_far() -> usize {
    SPAWNED.with(std::cell::Cell::get)
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
    let total = all.len();
    let tasks = Arc::new(Mutex::new(all.into_iter().enumerate()));
    let (result_tx, result_rx) = std::sync::mpsc::channel();
    let mut results: Vec<Option<R>> = Vec::with_capacity(total);
    results.resize_with(total, || None);

    // All input is ready. Each worker claims a whole batch, then computes
    // outside the lock; no worker waits for a producer or result consumer.
    for _ in 0..concurrency.min(total) {
        let tasks = Arc::clone(&tasks);
        let result_tx = result_tx.clone();
        enqueue(Box::new(move || loop {
            let next = tasks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .next();
            let Some((index, task)) = next else { break };
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(task));
            let _ = result_tx.send((index, result));
        }));
    }
    drop(result_tx);

    // Channel EOF joins every admitted worker, including on task panic.
    // Propagate the first panic only after all owned input has been released.
    let mut panic = None;
    for (index, result) in result_rx {
        match result {
            Ok(value) => results[index] = Some(value),
            Err(error) if panic.is_none() => panic = Some(error),
            Err(_) => {}
        }
    }
    if let Some(error) = panic {
        std::panic::resume_unwind(error);
    }
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
    fn blocking_lane_workers_are_reused_across_handles() {
        // Width one makes the worker identity deterministic. A second handle
        // with the same Go-style lane shape must attach to the existing
        // process worker instead of starting another native thread.
        let (first_tx, first_rx) = std::sync::mpsc::sync_channel(1);
        let first = LanePool::new("tidb-worker-pool-reuse-test", 1);
        first
            .submit(move || first_tx.send(std::thread::current().id()).unwrap())
            .unwrap();
        drop(first);
        let first_id = first_rx.recv().unwrap();

        let (second_tx, second_rx) = std::sync::mpsc::sync_channel(1);
        let second = LanePool::new("tidb-worker-pool-reuse-test", 1);
        second
            .submit(move || second_tx.send(std::thread::current().id()).unwrap())
            .unwrap();
        drop(second);
        assert_eq!(first_id, second_rx.recv().unwrap());
    }

    #[test]
    fn blocking_lane_drop_waits_for_in_flight_work() {
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        let pool = LanePool::new("tidb-worker-pool-close-test", 1);
        pool.submit(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        started_rx.recv().unwrap();
        let releaser = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(10));
            release_tx.send(()).unwrap();
        });
        drop(pool);
        releaser.join().unwrap();
    }

    /// The pool wakes a worker only when one is parked, so a lost wakeup
    /// would strand a task forever. Enqueue from several threads, both while
    /// the pool is idle and while it is saturated, and require every task to
    /// run.
    #[test]
    fn every_task_runs_whether_the_pool_is_idle_or_busy() {
        const PRODUCERS: usize = 4;
        const PER_PRODUCER: usize = 64;
        let (tx, rx) = std::sync::mpsc::channel::<usize>();
        // A first wave while the pool is idle (every worker parked).
        for index in 0..PER_PRODUCER {
            let tx = tx.clone();
            enqueue(Box::new(move || {
                let _ = tx.send(index);
            }));
        }
        // Then waves from several threads at once, on a pool that is still
        // draining the first.
        let threads: Vec<_> = (0..PRODUCERS)
            .map(|producer| {
                let tx = tx.clone();
                std::thread::spawn(move || {
                    for index in 0..PER_PRODUCER {
                        let tx = tx.clone();
                        enqueue(Box::new(move || {
                            let _ = tx.send(producer * PER_PRODUCER + index);
                        }));
                    }
                })
            })
            .collect();
        for thread in threads {
            thread.join().expect("producer");
        }
        drop(tx);
        let mut seen = 0;
        while rx.recv_timeout(std::time::Duration::from_secs(30)).is_ok() {
            seen += 1;
        }
        assert_eq!(seen, PER_PRODUCER * (PRODUCERS + 1), "every task ran");
    }

    #[test]
    fn map_preserves_submission_order() {
        let inputs: Vec<usize> = (0..32).collect();
        let out = map(
            inputs.iter().map(|&i| {
                move || {
                    // Variable "work" so completion order differs from submission.
                    if i % 3 == 0 {
                        std::thread::sleep(std::time::Duration::from_micros(50));
                    }
                    i * i
                }
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
