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

//! TiDB's `context.Context` for storage calls: a cancellation carrier and an
//! optional deadline, with the statement's KV-exec counter.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

/// Cloneable caller-owned cancellation state for one or more storage calls
/// (Go's `ctx.Done()`). Cancellation is monotonic and can be triggered from
/// any thread while the synchronous caller waits.
#[derive(Clone)]
pub struct UnaryCancellation {
    inner: Arc<CancellationState>,
}

struct CancellationState {
    cancelled: AtomicBool,
    // Like Go cancelCtx.mu, this coordinates first cancellation with the
    // waits. The flag can be read without taking this lock.
    waiters: Mutex<()>,
    blocking_wait: OnceLock<Condvar>,
    async_wait: OnceLock<tokio::sync::Notify>,
}

impl fmt::Debug for UnaryCancellation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnaryCancellation")
            .field("cancelled", &self.is_cancelled())
            .finish_non_exhaustive()
    }
}

impl Default for UnaryCancellation {
    fn default() -> Self {
        Self::new()
    }
}

impl UnaryCancellation {
    /// Creates an active, not-yet-cancelled carrier.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CancellationState {
                cancelled: AtomicBool::new(false),
                waiters: Mutex::new(()),
                blocking_wait: OnceLock::new(),
                async_wait: OnceLock::new(),
            }),
        }
    }

    /// Makes cancellation visible to current and future calls.
    pub fn cancel(&self) {
        if self.is_cancelled() {
            return;
        }
        {
            let _guard = self
                .inner
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if self.is_cancelled() {
                return;
            }
            self.inner.cancelled.store(true, Ordering::Release);
        }
        // New waiters now observe cancellation instead of registering. Do not
        // initialize notification machinery when nobody ever waited on it.
        if let Some(changed) = self.inner.blocking_wait.get() {
            changed.notify_all();
        }
        if let Some(changed) = self.inner.async_wait.get() {
            changed.notify_waiters();
        }
    }

    /// Whether the caller has already cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    /// Whether both carriers observe and update the same cancellation state.
    #[must_use]
    pub fn shares_state_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Waits until this carrier is cancelled or `timeout` elapses.
    ///
    /// Returns `true` only for caller cancellation. A `false` result means the
    /// full timeout elapsed. The condition variable is paired with the same
    /// cancellation flag, so synchronous waiting neither polls nor creates
    /// another cancellation authority.
    #[must_use]
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let guard = self
            .inner
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_cancelled() {
            return true;
        }
        let changed = self.inner.blocking_wait.get_or_init(Condvar::new);
        let _ = changed
            .wait_timeout_while(guard, timeout, |_| !self.is_cancelled())
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.is_cancelled()
    }

    /// Resolves once the carrier is cancelled (Go `<-ctx.Done()`).
    pub async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = {
            let _guard = self
                .inner
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if self.is_cancelled() {
                return;
            }
            // Notify::notified observes notify_waiters from creation, even
            // before its first poll. Construct under the cancellation lock so
            // cancellation cannot fall between the check and registration.
            self.inner
                .async_wait
                .get_or_init(tokio::sync::Notify::new)
                .notified()
        };
        notified.await;
    }
}

/// Transport-neutral per-call deadline and cancellation authority.
#[derive(Clone, Debug)]
pub struct UnaryCallContext {
    deadline: Option<Instant>,
    cancellation: UnaryCancellation,
    kv_exec_counter: Option<tidb_util::topsql_stmtstats::KvExecCounterHandle>,
}

impl UnaryCallContext {
    /// Binds an exact timeout and caller-owned cancellation carrier.
    #[must_use]
    pub fn new(timeout: Duration, cancellation: UnaryCancellation) -> Self {
        Self::with_deadline(Instant::now() + timeout, cancellation)
    }

    /// Binds an existing absolute deadline and caller-owned cancellation.
    #[must_use]
    pub const fn with_deadline(deadline: Instant, cancellation: UnaryCancellation) -> Self {
        Self {
            deadline: Some(deadline),
            cancellation,
            kv_exec_counter: None,
        }
    }

    /// A cancellation scope with an optional execution deadline. Individual
    /// RPCs derive a bounded child context when they are dispatched.
    #[must_use]
    pub const fn with_optional_deadline(
        deadline: Option<Instant>,
        cancellation: UnaryCancellation,
    ) -> Self {
        Self {
            deadline,
            cancellation,
            kv_exec_counter: None,
        }
    }

    /// Constructs a non-cancelled call context for compatibility callers.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self::new(timeout, UnaryCancellation::new())
    }

    /// Binds the statement observer without changing deadline or cancellation.
    #[must_use]
    pub fn with_kv_exec_counter(
        mut self,
        counter: Option<tidb_util::topsql_stmtstats::KvExecCounterHandle>,
    ) -> Self {
        self.kv_exec_counter = counter;
        self
    }

    /// The observer belongs to this call, never the shared connection fleet.
    pub fn kv_exec_counter(&self) -> Option<&tidb_util::topsql_stmtstats::KvExecCounterHandle> {
        self.kv_exec_counter.as_ref()
    }

    /// Remaining budget, or `Duration::MAX` for a cancellation-only scope.
    /// Network callers must derive a finite RPC child from an unbounded scope.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.deadline.map_or(Duration::MAX, |deadline| {
            deadline.saturating_duration_since(Instant::now())
        })
    }

    /// Returns the absolute deadline, absent for a cancellation-only scope.
    #[must_use]
    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Shared cancellation carrier for this call.
    #[must_use]
    pub const fn cancellation(&self) -> &UnaryCancellation {
        &self.cancellation
    }
}

/// Shared cancellation token retained by detached execution.
#[derive(Debug, Default)]
pub struct CancelHandle {
    cancellation: UnaryCancellation,
    children: Mutex<Vec<Weak<Self>>>,
}

impl CancelHandle {
    /// Marks this scope and every live child request as cancelled.
    pub fn cancel(&self) {
        self.cancellation.cancel();
        let mut children = self
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        children.retain(|child| {
            let Some(child) = child.upgrade() else {
                return false;
            };
            child.cancel();
            true
        });
    }

    /// Creates one request-local cancellation authority linked to this scope.
    ///
    /// Registration and the parent cancellation check share one mutex. A
    /// concurrent parent cancellation therefore either observes this child
    /// in the registry or completes before the child checks the parent state.
    #[must_use]
    pub fn request_child(self: &Arc<Self>) -> Arc<Self> {
        let child = Arc::new(Self::default());
        let mut children = self
            .children
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        children.retain(|child| child.strong_count() > 0);
        if self.is_cancelled() {
            child.cancel();
        } else {
            children.push(Arc::downgrade(&child));
        }
        child
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    /// Returns the canonical transport-neutral cancellation carrier.
    ///
    /// Every returned carrier shares the same monotonic cancellation state,
    /// including when cancellation happened before carrier acquisition.
    #[must_use]
    pub fn unary_cancellation(&self) -> UnaryCancellation {
        self.cancellation.clone()
    }
}
