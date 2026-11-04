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

//! Go `pkg/ttl/ttlworker/worker.go`: the lifecycle every TTL worker shares.

use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;
use tidb_ttl_client::context::{CancelFunc, Context, CtxError};

use crate::Result;

/// Go `workerStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    /// Go `workerStatusCreated`.
    Created,
    /// Go `workerStatusRunning`.
    Running,
    /// Go `workerStatusStopping`.
    Stopping,
    /// Go `workerStatusStopped`.
    Stopped,
}

/// Go `worker`.
pub trait Worker: Send + Sync {
    /// Go `Start`.
    fn start(&self);
    /// Go `Stop`.
    fn stop(&self);
    /// Go `Status`.
    fn status(&self) -> WorkerStatus;
    /// Go `Error`.
    fn error(&self) -> Option<String>;
    /// Go `WaitStopped`.
    fn wait_stopped(&self, ctx: &Context, timeout: Duration) -> std::result::Result<(), CtxError>;
}

/// The state Go's `baseWorker` guards with its embedded mutex, plus the
/// worker's own fields that share that mutex (Go's `w.Lock()` in a worker
/// guards both).
pub struct BaseState<S> {
    status: WorkerStatus,
    err: Option<String>,
    /// The concrete worker's fields under the same lock.
    pub extra: S,
}

impl<S> BaseState<S> {
    /// Whether the worker is `workerStatusRunning`.
    pub fn status_is_running(&self) -> bool {
        self.status == WorkerStatus::Running
    }
}

type LoopFn = Box<dyn FnOnce() -> Result<()> + Send>;

/// Go `baseWorker`. `M` is the type sent on its channel, `S` the concrete
/// worker's fields guarded by its mutex.
pub struct BaseWorker<M, S> {
    state: Mutex<BaseState<S>>,
    /// Go `ctx`.
    pub ctx: Context,
    cancel: CancelFunc,
    /// Go `ch`: the unbuffered message channel.
    pub ch: (Sender<M>, Receiver<M>),
    loop_fn: Mutex<Option<LoopFn>>,
    /// Go `wg`: set once the loop goroutine returns.
    finished: (Mutex<bool>, Condvar),
}

impl<M: Send + 'static, S: Send + 'static> BaseWorker<M, S> {
    /// Go `init(loop)`. The loop is installed by [`Self::set_loop`], since a
    /// Rust loop closure needs the worker it belongs to.
    pub fn new(extra: S) -> Arc<Self> {
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        Arc::new(Self {
            state: Mutex::new(BaseState {
                status: WorkerStatus::Created,
                err: None,
                extra,
            }),
            ctx,
            cancel,
            ch: crossbeam_channel::bounded(0),
            loop_fn: Mutex::new(None),
            finished: (Mutex::new(false), Condvar::new()),
        })
    }

    /// Go `init`'s `w.loopFunc = loop`.
    pub fn set_loop(&self, loop_fn: LoopFn) {
        *self.loop_fn.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(loop_fn);
    }

    /// Go's `w.Lock()`.
    pub fn lock(&self) -> MutexGuard<'_, BaseState<S>> {
        self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `Start`.
    pub fn start(self: &Arc<Self>) {
        let mut state = self.lock();
        if state.status != WorkerStatus::Created {
            return;
        }
        let worker = Arc::clone(self);
        std::thread::Builder::new()
            .name("ttl-worker".to_owned())
            .spawn(move || worker.run_loop())
            .expect("ttl worker thread spawns");
        state.status = WorkerStatus::Running;
    }

    /// Go `Stop`.
    pub fn stop(&self) {
        let mut state = self.lock();
        match state.status {
            WorkerStatus::Created => {
                self.cancel.cancel();
                Self::to_stopped(&mut state, None);
                drop(state);
                self.mark_finished();
            }
            WorkerStatus::Running => {
                self.cancel.cancel();
                state.status = WorkerStatus::Stopping;
            }
            WorkerStatus::Stopping | WorkerStatus::Stopped => {}
        }
    }

    /// Go `Status`.
    pub fn status(&self) -> WorkerStatus {
        self.lock().status
    }

    /// Go `Error`.
    pub fn error(&self) -> Option<String> {
        self.lock().err.clone()
    }

    /// Go `WaitStopped`: waits for the loop to return, at most `timeout`
    /// and no longer than `ctx` lives.
    pub fn wait_stopped(&self, ctx: &Context, timeout: Duration) -> std::result::Result<(), CtxError> {
        // consider the situation when the worker has stopped, but the context
        // has also stopped. We should return without error
        if self.status() == WorkerStatus::Stopped {
            return Ok(());
        }
        let deadline = std::time::Instant::now() + timeout;
        let (finished, changed) = &self.finished;
        let mut is_finished = finished.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut ctx_err = None;
        while !*is_finished {
            if let Some(err) = ctx.err() {
                ctx_err = Some(err);
                break;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                ctx_err = Some(CtxError::DeadlineExceeded);
                break;
            }
            is_finished = changed
                .wait_timeout(is_finished, (deadline - now).min(Duration::from_millis(10)))
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        drop(is_finished);
        if self.status() != WorkerStatus::Stopped {
            return Err(ctx_err.unwrap_or(CtxError::DeadlineExceeded));
        }
        Ok(())
    }

    /// Go `Send`.
    pub fn send(&self) -> Sender<M> {
        self.ch.0.clone()
    }

    /// Go `loop`: runs the loop function, recovering a panic, then stops.
    fn run_loop(&self) {
        let loop_fn = self.loop_fn.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            loop_fn.map_or(Ok(()), |loop_fn| loop_fn())
        }));
        let err = match result {
            Ok(result) => result.err(),
            Err(panic) => {
                let message = panic
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                bg_logger().info("ttl worker panic", &[Field::new("recover", Value::Str(message))]);
                debug_assert!(false, "ttl worker panic");
                None
            }
        };
        let mut state = self.lock();
        Self::to_stopped(&mut state, err);
        drop(state);
        self.mark_finished();
    }

    /// Go `toStopped`.
    fn to_stopped(state: &mut BaseState<S>, err: Option<String>) {
        state.status = WorkerStatus::Stopped;
        state.err = err;
    }

    fn mark_finished(&self) {
        let (finished, changed) = &self.finished;
        *finished.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_worker_runs_its_loop_until_stopped() {
        let worker = BaseWorker::<(), ()>::new(());
        let loop_worker = Arc::downgrade(&worker);
        let ctx = worker.ctx.clone();
        worker.set_loop(Box::new(move || {
            let _ = ctx.done().recv();
            drop(loop_worker);
            Err("loop error".to_owned())
        }));
        assert_eq!(worker.status(), WorkerStatus::Created);
        worker.start();
        assert_eq!(worker.status(), WorkerStatus::Running);
        assert!(worker
            .wait_stopped(&Context::background(), Duration::from_millis(20))
            .is_err());
        worker.stop();
        worker
            .wait_stopped(&Context::background(), Duration::from_secs(10))
            .unwrap();
        assert_eq!(worker.status(), WorkerStatus::Stopped);
        assert_eq!(worker.error().as_deref(), Some("loop error"));

        let idle = BaseWorker::<(), ()>::new(());
        idle.stop();
        assert_eq!(idle.status(), WorkerStatus::Stopped);
    }
}
