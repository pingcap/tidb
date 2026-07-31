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

//! A pool of threads for work that must *own* a thread for its whole life.
//!
//! This is not a task executor. A job submitted here keeps its worker thread to
//! itself from the moment it starts until it returns -- nothing else is
//! interleaved onto that thread. That is the property the caller needs when the
//! thing the job builds is not `Send`: a `!Send` value created inside the job
//! can be created, used and dropped there, and only `Send` handles (channels,
//! numbers) cross back out.
//!
//! What the pool changes against `thread::spawn` is who pays for the thread. A
//! finished job parks its worker instead of ending it, so the next job of the
//! same shape starts on an already-running thread: one channel send rather than
//! a `pthread_create` plus a `join`. On the cluster statement path -- one
//! read-only transaction per statement, each pinned to a thread because the
//! TiKV transport is worker-local -- that is the difference between roughly 28
//! and 4 microseconds per statement, charged to *every* statement including
//! ones that read no table at all.
//!
//! The pool never queues. A submission takes an idle worker if there is one and
//! spawns a new thread if there is not, so a job can never wait behind another
//! job's transaction: two statements that overlap get two threads, exactly as
//! they did when every statement spawned. Only the idle set is bounded; a
//! worker that finds the park full ends instead, which is what keeps a burst of
//! connections from leaving its peak thread count parked forever.

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, OnceLock};

/// One unit of work that owns its thread until it returns.
type Job = Box<dyn FnOnce() + Send + 'static>;

/// How many idle workers the process keeps parked between jobs.
///
/// Parked threads cost a stack and nothing else, and the number only has to
/// cover the statements that are in flight at the same instant -- one per busy
/// connection. Past that a returning worker ends rather than parks.
const PARKED_WORKER_LIMIT: usize = 64;

/// The process-wide park of idle workers.
pub struct PinnedThreadPool {
    idle: Mutex<Vec<Sender<Job>>>,
    limit: usize,
}

impl PinnedThreadPool {
    /// A pool that parks at most `limit` idle workers.
    #[must_use]
    pub const fn with_limit(limit: usize) -> Self {
        Self {
            idle: Mutex::new(Vec::new()),
            limit,
        }
    }

    /// The pool every pinned transaction of this process runs on.
    pub fn shared() -> &'static Self {
        static SHARED: OnceLock<PinnedThreadPool> = OnceLock::new();
        SHARED.get_or_init(|| Self::with_limit(PARKED_WORKER_LIMIT))
    }

    /// Runs `job` on a thread it owns until it returns.
    ///
    /// Takes a parked worker when one is free and starts a new one otherwise,
    /// so submission never blocks on another job. `name` names the thread only
    /// when a new one has to be started; a reused worker keeps the name it was
    /// born with, since renaming a live thread is not something the platform
    /// offers and the name is a debugging aid rather than an identity.
    ///
    /// # Errors
    ///
    /// Returns the reason the platform refused a new thread, and only in the
    /// case where a new thread was actually needed.
    pub fn run(&'static self, name: &str, job: Job) -> Result<(), String> {
        let mut job = job;
        // A parked sender can still be stale: its worker may have ended between
        // being parked and being taken (it cannot, today, but the pool must not
        // depend on that to stay correct). Send failure just means try again.
        while let Some(worker) = self.take_idle() {
            match worker.send(job) {
                Ok(()) => return Ok(()),
                Err(returned) => job = returned.0,
            }
        }
        self.start_worker(name, job)
    }

    fn take_idle(&self) -> Option<Sender<Job>> {
        self.idle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .pop()
    }

    /// Parks `worker`, or reports that the park is full and the worker should
    /// end.
    fn park(&self, worker: Sender<Job>) -> bool {
        let mut idle = self
            .idle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if idle.len() >= self.limit {
            return false;
        }
        idle.push(worker);
        true
    }

    fn start_worker(&'static self, name: &str, first: Job) -> Result<(), String> {
        let (jobs, incoming) = mpsc::channel::<Job>();
        let own_handle = jobs.clone();
        std::thread::Builder::new()
            .name(name.to_owned())
            .spawn(move || self.serve(&incoming, own_handle))
            .map_err(|error| error.to_string())?;
        jobs.send(first)
            .map_err(|_| "the pinned worker ended before its first job".to_owned())
    }

    /// Runs job after job on this one thread, parking between them.
    fn serve(&'static self, incoming: &Receiver<Job>, own_handle: Sender<Job>) {
        while let Ok(job) = incoming.recv() {
            job();
            if !self.park(own_handle.clone()) {
                return;
            }
        }
    }

    /// How many workers are parked right now. Test and profile use only.
    #[must_use]
    pub fn parked(&self) -> usize {
        self.idle
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread::ThreadId;

    /// A test pool of its own, so a test's parked workers never mix with the
    /// shared one's.
    fn test_pool(limit: usize) -> &'static PinnedThreadPool {
        Box::leak(Box::new(PinnedThreadPool::with_limit(limit)))
    }

    /// Blocks until at least `count` workers are parked.
    ///
    /// A worker parks a moment AFTER its job returned, so a test that submits
    /// jobs back to back can outrun the park and get a fresh thread. That race
    /// is harmless in production -- the next statement is far away -- but a test
    /// about reuse has to wait for the state it is testing.
    fn wait_for_park(pool: &'static PinnedThreadPool, count: usize) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while pool.parked() < count {
            assert!(
                std::time::Instant::now() < deadline,
                "expected {count} parked worker(s), found {}",
                pool.parked()
            );
            std::thread::yield_now();
        }
    }

    /// Runs `job` and waits for it, which is what every transaction handle does
    /// on its opening handshake.
    fn run_and_wait<T: Send + 'static>(
        pool: &'static PinnedThreadPool,
        job: impl FnOnce() -> T + Send + 'static,
    ) -> T {
        let (done, answer) = mpsc::channel();
        pool.run(
            "pinned-pool-test",
            Box::new(move || {
                let _ = done.send(job());
            }),
        )
        .unwrap();
        answer.recv().unwrap()
    }

    #[test]
    fn a_finished_job_leaves_its_thread_for_the_next_one() {
        let pool = test_pool(4);
        let first = run_and_wait(pool, std::thread::current);
        wait_for_park(pool, 1);
        let second = run_and_wait(pool, std::thread::current);
        assert_eq!(
            first.id(),
            second.id(),
            "the second job should reuse the parked worker"
        );
    }

    #[test]
    fn overlapping_jobs_never_wait_on_each_other() {
        let pool = test_pool(4);
        // The first job does not return until the second has run, so a pool
        // that queued instead of spawning would deadlock here rather than fail.
        let (release, wait) = mpsc::channel::<()>();
        let (first_started, first_running) = mpsc::channel::<ThreadId>();
        pool.run(
            "pinned-pool-test",
            Box::new(move || {
                first_started.send(std::thread::current().id()).unwrap();
                wait.recv().unwrap();
            }),
        )
        .unwrap();
        let held = first_running.recv().unwrap();
        let concurrent = run_and_wait(pool, || std::thread::current().id());
        assert_ne!(held, concurrent, "an overlapping job needs its own thread");
        release.send(()).unwrap();
    }

    #[test]
    fn the_park_is_bounded() {
        let pool = test_pool(1);
        // Two overlapping jobs mean two workers; only one of them may park.
        let (release, wait) = mpsc::channel::<()>();
        let (started, running) = mpsc::channel::<()>();
        pool.run(
            "pinned-pool-test",
            Box::new(move || {
                started.send(()).unwrap();
                wait.recv().unwrap();
            }),
        )
        .unwrap();
        running.recv().unwrap();
        run_and_wait(pool, || ());
        release.send(()).unwrap();
        // The held worker parks (or ends) once released; either way the park
        // never exceeds its limit.
        for _ in 0..100 {
            assert!(pool.parked() <= 1);
            std::thread::yield_now();
        }
    }

    /// The hazard reusing a thread introduces, and the reason it is safe here.
    ///
    /// A transaction reaches its worker as a value the job builds, uses and
    /// drops; nothing about it is cached on the thread. If any of it ever were
    /// -- a session, a lease, a probe -- the second statement to land on a
    /// parked worker would silently inherit the first statement's state, which
    /// is the one way this change could alter what a statement reads.
    ///
    /// The pool does NOT clear thread-local state, and this pins that it does
    /// not, so the rule is stated where it can be read: a value that must not
    /// outlive one transaction has to be owned by the job, never parked on the
    /// thread. Today the transaction path holds to that -- the session, the
    /// region-cache lease and the transport are all built inside the job by
    /// `open_session` and dropped when it returns.
    #[test]
    fn thread_local_state_survives_a_reused_worker() {
        thread_local! {
            static LEFTOVER: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
        }
        let pool = test_pool(1);
        let first = run_and_wait(pool, || {
            let seen = LEFTOVER.with(std::cell::Cell::get);
            LEFTOVER.with(|slot| slot.set(99));
            (std::thread::current().id(), seen)
        });
        wait_for_park(pool, 1);
        let second = run_and_wait(pool, || {
            (
                std::thread::current().id(),
                LEFTOVER.with(std::cell::Cell::get),
            )
        });
        assert_eq!(first.0, second.0, "the test needs the worker to be reused");
        assert_eq!(first.1, 0, "the first job starts blank");
        assert_eq!(
            second.1, 99,
            "thread state DOES survive a reused worker -- which is exactly why \
             nothing on the transaction path may be kept there"
        );
    }

    /// A job's `!Send` values are destroyed before its worker takes another
    /// job.
    ///
    /// This is the region-cache lease and the transport handle: the previous
    /// shape released them by ending the thread, and a parked worker cannot
    /// rely on that. What replaces it is that the job owns them and the job has
    /// returned.
    #[test]
    fn a_jobs_values_are_dropped_before_its_worker_is_reused() {
        struct Tracked(Arc<AtomicUsize>);
        impl Drop for Tracked {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let pool = test_pool(1);
        let dropped = Arc::new(AtomicUsize::new(0));
        let owned = Tracked(Arc::clone(&dropped));
        let first = run_and_wait(pool, move || {
            let _owned = owned;
            std::thread::current().id()
        });
        wait_for_park(pool, 1);
        let second = run_and_wait(pool, {
            let dropped = Arc::clone(&dropped);
            move || (std::thread::current().id(), dropped.load(Ordering::SeqCst))
        });
        assert_eq!(first, second.0, "the test needs the worker to be reused");
        assert_eq!(
            second.1, 1,
            "the previous job's values must be gone before the next job starts"
        );
    }

    /// The whole point, as a number rather than as a claim.
    ///
    /// The transaction handshake is: submit a job, wait for it to report a
    /// timestamp, send one request, close it. Timed against the same handshake
    /// on a freshly spawned thread, the pool has to be decisively cheaper --
    /// that difference is the per-statement cost this exists to remove, and a
    /// pool that quietly stopped reusing threads would land back on the spawn
    /// number.
    #[test]
    fn the_pool_is_decisively_cheaper_than_spawning_per_job() {
        const ROUNDS: usize = 200;
        let pool = test_pool(4);

        let mut pooled = Vec::with_capacity(ROUNDS);
        let mut spawned = Vec::with_capacity(ROUNDS);
        for round in 0..ROUNDS + 20 {
            let start = std::time::Instant::now();
            handshake_on_pool(pool);
            let pool_elapsed = start.elapsed();
            let start = std::time::Instant::now();
            handshake_on_new_thread();
            let spawn_elapsed = start.elapsed();
            // The first rounds warm the park; a cold pool spawns like the
            // baseline does, which is the honest behaviour, not the one under
            // test.
            if round >= 20 {
                pooled.push(pool_elapsed.as_nanos());
                spawned.push(spawn_elapsed.as_nanos());
            }
            // Measure the steady state -- a parked worker waiting for the next
            // statement -- rather than a round that outran the park.
            wait_for_park(pool, 1);
        }
        pooled.sort_unstable();
        spawned.sort_unstable();
        let pooled_median = pooled[pooled.len() / 2];
        let spawned_median = spawned[spawned.len() / 2];
        // Measured at roughly 10us against 28us in a release profile. The bar
        // is set well inside that so an ordinarily loaded machine does not fail
        // it, while a pool that spawned every time could not pass it.
        assert!(
            pooled_median * 3 < spawned_median * 2,
            "the pooled handshake ({pooled_median}ns) should be far cheaper \
             than spawning one ({spawned_median}ns)"
        );
    }

    /// One transaction-shaped handshake against the pool: open, report, serve
    /// one request, close.
    fn handshake_on_pool(pool: &'static PinnedThreadPool) {
        let (requests, incoming) = mpsc::channel::<mpsc::Sender<u64>>();
        let (opened, opened_reply) = mpsc::channel::<u64>();
        pool.run(
            "pinned-pool-test",
            Box::new(move || {
                if opened.send(7).is_err() {
                    return;
                }
                while let Ok(reply) = incoming.recv() {
                    let _ = reply.send(0);
                }
            }),
        )
        .unwrap();
        opened_reply.recv().unwrap();
        let (reply, answer) = mpsc::channel();
        requests.send(reply).unwrap();
        answer.recv().unwrap();
        drop(requests);
    }

    /// The same handshake the way the statement path used to get it.
    fn handshake_on_new_thread() {
        let (requests, incoming) = mpsc::channel::<mpsc::Sender<u64>>();
        let (opened, opened_reply) = mpsc::channel::<u64>();
        let worker = std::thread::Builder::new()
            .name("pinned-pool-test".to_owned())
            .spawn(move || {
                if opened.send(7).is_err() {
                    return;
                }
                while let Ok(reply) = incoming.recv() {
                    let _ = reply.send(0);
                }
            })
            .unwrap();
        opened_reply.recv().unwrap();
        let (reply, answer) = mpsc::channel();
        requests.send(reply).unwrap();
        answer.recv().unwrap();
        drop(requests);
        worker.join().unwrap();
    }

    #[test]
    fn every_job_runs_exactly_once() {
        let pool = test_pool(8);
        let ran = Arc::new(AtomicUsize::new(0));
        let mut answers = Vec::new();
        for _ in 0..64 {
            let ran = Arc::clone(&ran);
            let (done, answer) = mpsc::channel();
            pool.run(
                "pinned-pool-test",
                Box::new(move || {
                    ran.fetch_add(1, Ordering::SeqCst);
                    let _ = done.send(());
                }),
            )
            .unwrap();
            answers.push(answer);
        }
        for answer in answers {
            answer.recv().unwrap();
        }
        assert_eq!(ran.load(Ordering::SeqCst), 64);
    }
}
