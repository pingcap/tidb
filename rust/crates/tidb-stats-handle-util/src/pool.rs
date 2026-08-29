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

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use tidb_syssession::{Pool as SessionPool, SessionContext};

const MAX_IDLE_WORKERS: usize = i16::MAX as usize;
const IDLE_RECYCLE: Duration = Duration::from_secs(60);

type Job = Box<dyn FnOnce() + Send + 'static>;

enum WorkerMessage {
    Run(Job),
    Close,
}

struct IdleWorker {
    id: u64,
    sender: mpsc::SyncSender<WorkerMessage>,
}

struct WorkerPoolInner {
    idle: Mutex<Vec<IdleWorker>>,
    closed: AtomicBool,
    next_id: AtomicU64,
}

/// Native equivalent of the `gp.Pool` constructed by Go `NewPool`.
#[derive(Clone)]
pub struct StatsWorkerPool {
    inner: Arc<WorkerPoolInner>,
}

impl Default for StatsWorkerPool {
    fn default() -> Self {
        Self {
            inner: Arc::new(WorkerPoolInner {
                idle: Mutex::new(Vec::new()),
                closed: AtomicBool::new(false),
                next_id: AtomicU64::new(1),
            }),
        }
    }
}

impl StatsWorkerPool {
    fn remove_idle(inner: &WorkerPoolInner, id: u64) {
        let mut idle = inner
            .idle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = idle.iter().position(|worker| worker.id == id) {
            idle.swap_remove(index);
        }
    }

    fn worker(inner: Arc<WorkerPoolInner>, id: u64, first: Job) {
        let mut next = Some(first);
        loop {
            if let Some(job) = next.take() {
                job();
            }
            if inner.closed.load(Ordering::SeqCst) {
                return;
            }

            let (sender, receiver) = mpsc::sync_channel(1);
            {
                let mut idle = inner
                    .idle
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if idle.len() >= MAX_IDLE_WORKERS || inner.closed.load(Ordering::SeqCst) {
                    return;
                }
                idle.push(IdleWorker { id, sender });
            }

            match receiver.recv_timeout(IDLE_RECYCLE) {
                Ok(WorkerMessage::Run(job)) => next = Some(job),
                Ok(WorkerMessage::Close) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    Self::remove_idle(&inner, id);
                    return;
                }
            }
        }
    }

    /// Go `(*gp.Pool).Go`. A task submitted after close is ignored; a task
    /// accepted before close runs to completion.
    pub fn go(&self, job: impl FnOnce() + Send + 'static) {
        if self.inner.closed.load(Ordering::SeqCst) {
            return;
        }
        let mut message = WorkerMessage::Run(Box::new(job));
        loop {
            let worker = self
                .inner
                .idle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .pop();
            let Some(worker) = worker else {
                break;
            };
            if self.inner.closed.load(Ordering::SeqCst) {
                return;
            }
            match worker.sender.try_send(message) {
                Ok(()) => return,
                Err(mpsc::TrySendError::Full(returned))
                | Err(mpsc::TrySendError::Disconnected(returned)) => message = returned,
            }
        }

        if self.inner.closed.load(Ordering::SeqCst) {
            return;
        }
        let WorkerMessage::Run(job) = message else {
            unreachable!("only tasks are submitted")
        };
        let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::clone(&self.inner);
        std::thread::Builder::new()
            .name("stats-worker".to_owned())
            .spawn(move || Self::worker(inner, id, job))
            .expect("create statistics worker");
    }

    /// Go `(*gp.Pool).Close`. Calling it twice panics like closing the Go
    /// channel twice.
    pub fn close(&self) {
        assert!(
            self.inner
                .closed
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "close of closed statistics worker pool"
        );
        let idle = std::mem::take(
            &mut *self
                .inner
                .idle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for worker in idle {
            let _ = worker.sender.try_send(WorkerMessage::Close);
        }
    }
}

/// Go `Pool`.
pub trait Pool<C: SessionContext + ?Sized + 'static>: Send + Sync {
    /// Go `GPool`.
    fn gpool(&self) -> &StatsWorkerPool;
    /// Go `SPool`.
    fn spool(&self) -> &dyn SessionPool<C>;
    /// Go `Close`; only the goroutine pool is closed.
    fn close(&self);
}

/// Go's private `pool` implementation.
pub struct StatsPool<C: SessionContext + ?Sized + 'static> {
    gpool: StatsWorkerPool,
    spool: Arc<dyn SessionPool<C>>,
}

impl<C: SessionContext + ?Sized + 'static> StatsPool<C> {
    /// Go `NewPool`.
    #[must_use]
    pub fn new(spool: Arc<dyn SessionPool<C>>) -> Self {
        Self {
            gpool: StatsWorkerPool::default(),
            spool,
        }
    }
}

impl<C: SessionContext + ?Sized + 'static> Pool<C> for StatsPool<C> {
    fn gpool(&self) -> &StatsWorkerPool {
        &self.gpool
    }

    fn spool(&self) -> &dyn SessionPool<C> {
        self.spool.as_ref()
    }

    fn close(&self) {
        self.gpool.close();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::time::Duration;

    use tidb_syssession::{Pool as SessionPool, Session, SessionContext};

    use super::{Pool, StatsPool, StatsWorkerPool};

    struct FakeSessionPool {
        close_count: AtomicUsize,
    }

    impl SessionPool<dyn SessionContext> for FakeSessionPool {
        fn get(&self) -> tidb_syssession::Result<Session> {
            panic!("unused")
        }

        fn put(&self, _session: &Session) {
            panic!("unused")
        }

        fn with_session(
            &self,
            _callback: &mut dyn FnMut(&Session) -> tidb_sqlexec::Result<()>,
        ) -> tidb_sqlexec::Result<()> {
            panic!("unused")
        }

        fn with_force_block_gc_session(
            &self,
            _cancelled: &dyn Fn() -> bool,
            _callback: &mut dyn FnMut(&Session) -> tidb_sqlexec::Result<()>,
        ) -> tidb_sqlexec::Result<()> {
            panic!("unused")
        }

        fn close(&self) {
            self.close_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn worker_pool_runs_accepted_tasks_and_ignores_post_close_tasks() {
        let pool = StatsWorkerPool::default();
        let (sender, receiver) = mpsc::channel();
        pool.go(move || sender.send(1).unwrap());
        assert_eq!(receiver.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        pool.close();

        let (sender, receiver) = mpsc::channel();
        pool.go(move || sender.send(2).unwrap());
        assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
    }

    #[test]
    #[should_panic(expected = "close of closed statistics worker pool")]
    fn worker_pool_double_close_panics() {
        let pool = StatsWorkerPool::default();
        pool.close();
        pool.close();
    }

    #[test]
    fn stats_pool_exposes_both_pools_and_only_closes_worker_pool() {
        let concrete = Arc::new(FakeSessionPool {
            close_count: AtomicUsize::new(0),
        });
        let session_pool: Arc<dyn SessionPool<dyn SessionContext>> = concrete.clone();
        let pool = StatsPool::new(Arc::clone(&session_pool));

        assert!(std::ptr::eq(pool.spool(), session_pool.as_ref()));
        pool.close();

        let (sender, receiver) = mpsc::channel();
        pool.gpool().go(move || sender.send(()).unwrap());
        assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
        assert_eq!(concrete.close_count.load(Ordering::SeqCst), 0);
    }
}
