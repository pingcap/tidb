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

//! Shared native scheduling with connection-scoped task lifetimes.

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Wake, Waker};

use tokio::runtime::{Handle, Runtime};
use tokio::task::{AbortHandle, JoinSet};

/// Workers driving the transport runtime; see [`execution_runtime`].
const TRANSPORT_WORKER_THREADS: usize = 1;

/// The transport runtime: tonic/h2 framing, the batch stream loops, PD and
/// TTL keep-alive. Connection scopes and transport joins own their lifetime,
/// not this runtime.
///
/// client-go serializes that work per store connection (`batchSendLoop` and
/// `batchRecvLoop` are one goroutine each), and it costs a few microseconds
/// per request, so one worker keeps up. Sizing this pool to the core count
/// instead made every TiKV response wake a second, idle worker that stole
/// nothing and parked again (Tokio's `notify_parked_local`): measured under
/// sysbench on a 4-core node, that was ~2.5 extra context switches per
/// statement and 11% of the node's CPU. Work that scales with the query --
/// the coprocessor response workers -- runs on [`query_worker_runtime`], the
/// way Go's `copIteratorWorker` goroutines run across every P while the
/// transport stays one loop per store. `block_in_place` callers still hand
/// their core to a fresh thread, so a blocking section never stalls the
/// transport.
pub fn execution_runtime() -> Result<&'static Runtime, String> {
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(TRANSPORT_WORKER_THREADS)
                .thread_name("tikv-execution")
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// Go `runtime.GOMAXPROCS(0)`: the parallelism the process may use, which
/// `copr.Store.numcpu` and the query worker pool below are sized by.
#[must_use]
pub fn go_max_procs() -> usize {
    std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
}

/// The runtime for query-scaled work: one worker per available core, the
/// way Go's `copIteratorWorker` goroutines spread over `GOMAXPROCS`. It is
/// distinct from [`execution_runtime`] so a TiKV response completing on the
/// transport worker hands the coprocessor task over once (a remote wake of
/// one parked worker) instead of racing an idle sibling for it, while the
/// response decoding of every session no longer serialises on the single
/// transport thread.
pub fn query_worker_runtime() -> Result<&'static Runtime, String> {
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(go_max_procs())
                .thread_name("tikv-query-worker")
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// One physical channel generation, including tonic/h2 background work.
/// Only the channel-pool owner closes and joins this scope; clones can spawn.
#[derive(Clone)]
pub(in crate::rpc) struct ConnectionTasks {
    runtime: Handle,
    tasks: Arc<Mutex<Option<JoinSet<()>>>>,
}

impl ConnectionTasks {
    pub(in crate::rpc) fn new(runtime: &Handle) -> Self {
        Self {
            runtime: runtime.clone(),
            tasks: Arc::new(Mutex::new(Some(JoinSet::new()))),
        }
    }

    pub(in crate::rpc) fn spawn<F>(&self, task: F) -> Option<AbortHandle>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut guard = self.tasks.lock().unwrap_or_else(|error| error.into_inner());
        let Some(tasks) = guard.as_mut() else {
            // Future destruction may release endpoint executor clones.
            drop(guard);
            return None;
        };
        while tasks.try_join_next().is_some() {}
        Some(tasks.spawn_on(task, &self.runtime))
    }

    /// Awaited by the transport owner, never from a task in this scope.
    pub(in crate::rpc) async fn close(&self) {
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(mut tasks) = tasks {
            // Do not hold the scope lock while aborted futures are dropped:
            // tonic may try to spawn another task during connection teardown.
            tasks.shutdown().await;
        }
    }

    /// Fallback for an unwinding owner. Dropping JoinSet aborts its tasks.
    pub(in crate::rpc) fn abort(&self) {
        let tasks = self
            .tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        drop(tasks);
    }
}

/// Bridge only synchronous public APIs. Async producers and the command owner
/// never use this path. Returning a Tokio worker before parking also permits
/// callers hosted on that scheduler without starving their own RPC tasks.
pub(in crate::rpc) fn wait<F: Future>(future: F) -> F::Output {
    blocking(|| futures::executor::block_on(future))
}

struct ThreadWake(std::thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Synchronous response bridge with the caller's original absolute deadline.
/// The native park timeout supplies the clock; no second timer task or thread
/// is registered merely to wake a thread that is already waiting on the reply.
pub(in crate::rpc) fn wait_with_call<F: Future>(
    future: F,
    call: &super::UnaryCallContext,
) -> Result<F::Output, super::CompletionError> {
    thread_local! {
        static WAKE: Waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    }
    blocking(|| {
        let mut future = std::pin::pin!(future);
        let mut cancelled = std::pin::pin!(call.cancellation().cancelled());
        WAKE.with(|waker| {
            let mut cx = Context::from_waker(waker);
            loop {
                if cancelled.as_mut().poll(&mut cx).is_ready() {
                    return Err(super::CompletionError::Cancelled);
                }
                if call.timeout().is_zero() {
                    return Err(super::CompletionError::DeadlineExceeded);
                }
                if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
                    return if call.cancellation().is_cancelled() {
                        Err(super::CompletionError::Cancelled)
                    } else if call.timeout().is_zero() {
                        Err(super::CompletionError::DeadlineExceeded)
                    } else {
                        Ok(result)
                    };
                }
                // unpark retains a permit if publication/cancellation raced
                // the poll. Spurious wakeups recheck both state and deadline.
                if call.deadline().is_some() {
                    std::thread::park_timeout(call.timeout());
                } else {
                    std::thread::park();
                }
            }
        })
    })
}

fn blocking<T>(work: impl FnOnce() -> T) -> T {
    if Handle::try_current()
        .is_ok_and(|handle| handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

impl<F> hyper::rt::Executor<F> for ConnectionTasks
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, task: F) {
        self.spawn(task);
    }
}
