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
