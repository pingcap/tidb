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

//! Native scheduling, with transport-owned I/O and connection task lifetimes.

use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Wake, Waker};

use tokio::runtime::{Handle, Runtime};
use tokio::task::{AbortHandle, JoinSet};

/// Native scheduler shared by independent coprocessor workers.
/// Transport command and I/O tasks have an independently joined lifetime.
pub fn execution_runtime() -> Result<&'static Runtime, String> {
    static RUNTIME: OnceLock<Result<Runtime, String>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .thread_name("tikv-execution")
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(Clone::clone)
}

/// The transport owner's command loop and connection I/O share one driver.
/// Publication and stream wakeups stay local instead of crossing a runtime for
/// every packet. Connection count controls sockets, not native driver threads.
/// SQL, cop workers and blocking recovery remain outside this event loop.
pub(in crate::rpc) struct TransportIo {
    pub(in crate::rpc) handle: Handle,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TransportIo {
    pub(in crate::rpc) fn new() -> Result<Self, String> {
        let (ready, receiver) = std::sync::mpsc::sync_channel(1);
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let thread = std::thread::Builder::new()
            .name("tikv-transport".to_owned())
            .spawn(move || {
                match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => {
                        if ready.send(Ok(runtime.handle().clone())).is_ok() {
                            runtime.block_on(async {
                                let _ = stopped.await;
                            });
                        }
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error.to_string()));
                    }
                }
            })
            .map_err(|error| error.to_string())?;
        match receiver
            .recv()
            .map_err(|error| error.to_string())
            .and_then(|result| result)
        {
            Ok(handle) => Ok(Self {
                handle,
                stop: Some(stop),
                thread: Some(thread),
            }),
            Err(error) => {
                let _ = thread.join();
                Err(error)
            }
        }
    }

    pub(in crate::rpc) fn shutdown(&mut self) -> std::thread::Result<()> {
        self.stop.take();
        self.thread.take().map_or(Ok(()), |thread| thread.join())
    }
}

impl Drop for TransportIo {
    fn drop(&mut self) {
        // The transport joins all connection task scopes before stopping I/O.
        // Dropping the runtime on its own thread also works for async callers.
        let _ = self.shutdown();
    }
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
                std::thread::park_timeout(call.timeout());
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
