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

//! Bounded detached cleanup for read-side locks.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use crate::UnaryCancellation;

/// TiDB client-go's process-wide `AsyncResolveLockSemaphoreLimit`.
const GLOBAL_ASYNC_RESOLVE_LIMIT: usize = 10_000;
const ASYNC_RESOLVE_BLOCKING_WORKERS: usize = 512;

static GLOBAL_ASYNC_RESOLVE_TASKS: AtomicUsize = AtomicUsize::new(0);
static ASYNC_RESOLVE_RUNTIME: OnceLock<Option<tokio::runtime::Runtime>> = OnceLock::new();

/// Owned input for one detached transaction cleanup.
#[derive(Clone, Debug)]
pub(crate) struct AsyncLockResolveTask {
    pub(crate) txn_id: u64,
    pub(crate) commit_version: u64,
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) request_source: String,
    /// Whether ResolveLock should carry `keys` or scan the routed region.
    pub(crate) include_keys: bool,
    /// Whether Go's `resultRequired=false` enables TiKV-side async resolve.
    pub(crate) server_side_async: bool,
    /// Whether this transaction-level task should schedule its region groups
    /// independently, matching `batchLiteResolveLocks`' nested read tasks.
    pub(crate) schedule_regions: bool,
    /// Whether Go's `resolveLock` counter belongs to this task.
    pub(crate) count_resolve_locks: bool,
    /// Whether each region request is Go's lite ResolveLock path.
    pub(crate) count_resolve_lock_lite: bool,
}

type AsyncResolveHandler = dyn Fn(AsyncLockResolveTask, UnaryCancellation) + Send + Sync + 'static;

#[derive(Default)]
struct PoolState {
    closed: bool,
    in_flight: usize,
}

/// Per-read-authority task lifecycle over a process-wide bounded admission
/// count, matching client-go's `asyncResolveTaskPool` and global semaphore.
pub(crate) struct AsyncResolvePool {
    state: Mutex<PoolState>,
    drained: Condvar,
    cancellation: UnaryCancellation,
    handler: Arc<AsyncResolveHandler>,
}

impl AsyncResolvePool {
    pub(crate) fn new(
        handler: impl Fn(AsyncLockResolveTask, UnaryCancellation) + Send + Sync + 'static,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(PoolState::default()),
            drained: Condvar::new(),
            cancellation: UnaryCancellation::new(),
            handler: Arc::new(handler),
        })
    }

    /// Attempts detached scheduling. `false` means the caller must resolve in
    /// place, as client-go does when the global semaphore or local pool is full.
    pub(crate) fn try_spawn(self: &Arc<Self>, task: AsyncLockResolveTask) -> bool {
        let Some(runtime) = async_resolve_runtime() else {
            return false;
        };
        if !acquire_global_permit() {
            return false;
        }
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                release_global_permit();
                return false;
            }
            state.in_flight += 1;
        }

        let permit = InFlightPermit {
            pool: Arc::clone(self),
        };
        let handler = Arc::clone(&self.handler);
        let cancellation = self.cancellation.clone();
        runtime.spawn_blocking(move || {
            let _permit = permit;
            let _gauge = AsyncResolveGaugeGuard::new();
            handler(task, cancellation);
        });
        true
    }

    /// Cancels and drains this resolver's detached work before its shared
    /// region-cache authority is shut down.
    pub(crate) fn close_and_wait(&self) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.closed = true;
        }
        self.cancellation.cancel();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while state.in_flight != 0 {
            state = self
                .drained
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }
}

struct AsyncResolveGaugeGuard(Option<prometheus::Gauge>);

impl AsyncResolveGaugeGuard {
    fn new() -> Self {
        let gauge = crate::client_go_metrics::lock_resolver_read_async_gauge();
        if let Some(gauge) = &gauge {
            gauge.inc();
        }
        Self(gauge)
    }
}

impl Drop for AsyncResolveGaugeGuard {
    fn drop(&mut self) {
        if let Some(gauge) = &self.0 {
            gauge.dec();
        }
    }
}

struct InFlightPermit {
    pool: Arc<AsyncResolvePool>,
}

impl Drop for InFlightPermit {
    fn drop(&mut self) {
        release_global_permit();
        let mut state = self
            .pool
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.in_flight -= 1;
        if state.in_flight == 0 {
            self.pool.drained.notify_all();
        }
    }
}

fn acquire_global_permit() -> bool {
    let mut active = GLOBAL_ASYNC_RESOLVE_TASKS.load(Ordering::Acquire);
    loop {
        if active >= GLOBAL_ASYNC_RESOLVE_LIMIT {
            return false;
        }
        match GLOBAL_ASYNC_RESOLVE_TASKS.compare_exchange_weak(
            active,
            active + 1,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(current) => active = current,
        }
    }
}

fn release_global_permit() {
    let previous = GLOBAL_ASYNC_RESOLVE_TASKS.fetch_sub(1, Ordering::AcqRel);
    debug_assert!(previous > 0);
}

fn async_resolve_runtime() -> Option<&'static tokio::runtime::Runtime> {
    ASYNC_RESOLVE_RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .max_blocking_threads(ASYNC_RESOLVE_BLOCKING_WORKERS)
                .enable_all()
                .build()
                .ok()
        })
        .as_ref()
}
