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

//! Shared execution handles and detach-time owned state.

use std::sync::{
    atomic::{AtomicU32, AtomicU64, Ordering},
    Arc, Mutex, Weak,
};
pub use tidb_txnkv::KvVariables;
use tidb_txnkv::UnaryCancellation;

/// Shared kill signal corresponding to Go's `*sqlkiller.SQLKiller` identity.
#[derive(Debug, Default)]
pub struct KillHandle {
    signal: Arc<AtomicU32>,
}

impl KillHandle {
    /// Returns the currently published kill signal (`0` means not killed).
    #[must_use]
    pub fn signal(&self) -> u32 {
        self.signal.load(Ordering::Acquire)
    }

    /// Publishes a kill signal once, preserving the first reason.
    ///
    /// No transport or error mapping is performed here; those belong to a
    /// future session/protocol owner.
    pub fn request_kill(&self, signal: u32) -> bool {
        self.signal
            .compare_exchange(0, signal, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
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

/// Owned CPU-usage samples copied by `DistSqlContext::detach`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CpuUsage {
    samples: Vec<u64>,
}

impl CpuUsage {
    /// Creates a snapshot from source-ordered samples.
    #[must_use]
    pub fn from_samples(samples: Vec<u64>) -> Self {
        Self { samples }
    }

    /// Returns the samples in this owned snapshot.
    #[must_use]
    pub fn samples(&self) -> &[u64] {
        &self.samples
    }

    /// Appends one sample to this owned snapshot.
    pub fn push_sample(&mut self, sample: u64) {
        self.samples.push(sample);
    }
}

/// Execution state carried beside a request context.
pub struct ExecutionState {
    /// Shared kill handle; detach preserves this identity.
    pub killer: Arc<KillHandle>,
    /// Shared cancellation handle; detach preserves this identity.
    pub cancel: Arc<CancelHandle>,
    /// Owned CPU usage snapshot; detach clones its contents.
    pub cpu_usage: CpuUsage,
    /// Copied KV variables with a shared `killed` handle.
    pub kv_vars: KvVariables,
    /// Statement-wide max-keys-read accumulator, if enabled.
    pub max_keys_read_counter: Option<Arc<AtomicU64>>,
    /// Whether this state belongs to a detached execution.
    pub detached: bool,
}

impl std::fmt::Debug for ExecutionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Native Variables contains a callback that need not implement Debug.
        f.debug_struct("ExecutionState")
            .field("killer", &self.killer)
            .field("cancel", &self.cancel)
            .field("cpu_usage", &self.cpu_usage)
            .field("backoff_lock_fast", &self.kv_vars.backoff_lock_fast)
            .field("backoff_weight", &self.kv_vars.backoff_weight)
            .field("disable_txn_file", &self.kv_vars.disable_txn_file)
            .field(
                "txn_file_min_mutation_size",
                &self.kv_vars.txn_file_min_mutation_size,
            )
            .field(
                "has_kill_signal_handler",
                &self.kv_vars.kill_signal_handler.is_some(),
            )
            .field("max_keys_read_counter", &self.max_keys_read_counter)
            .field("detached", &self.detached)
            .finish()
    }
}

impl ExecutionState {
    /// Creates execution state with fresh kill and cancellation handles.
    #[must_use]
    pub fn new() -> Self {
        let killer = Arc::new(KillHandle::default());
        Self::with_handles(killer, Arc::new(CancelHandle::default()))
    }

    /// Creates execution state with explicit shared handles.
    #[must_use]
    pub fn with_handles(killer: Arc<KillHandle>, cancel: Arc<CancelHandle>) -> Self {
        Self {
            kv_vars: KvVariables::new(Arc::clone(&killer.signal)),
            killer,
            cancel,
            cpu_usage: CpuUsage::default(),
            max_keys_read_counter: None,
            detached: false,
        }
    }

    /// Returns a detached copy with Go-compatible ownership and identity.
    #[must_use]
    pub fn detach(&self) -> Self {
        let mut kv_vars = self.kv_vars.clone();
        kv_vars.killed = Arc::clone(&self.killer.signal);
        Self {
            killer: Arc::clone(&self.killer),
            cancel: Arc::clone(&self.cancel),
            cpu_usage: self.cpu_usage.clone(),
            kv_vars,
            // Go allocates a fresh zeroed atomic when the source context had a
            // statement-wide accumulator; it does not carry the old count.
            max_keys_read_counter: self
                .max_keys_read_counter
                .as_ref()
                .map(|_| Arc::new(AtomicU64::new(0))),
            detached: true,
        }
    }

    /// Returns the current max-keys-read count, if the accumulator exists.
    #[must_use]
    pub fn max_keys_read_count(&self) -> Option<u64> {
        self.max_keys_read_counter
            .as_ref()
            .map(|counter| counter.load(Ordering::Acquire))
    }

    /// Increments the max-keys-read accumulator when it is enabled.
    pub fn add_keys_read(&self, amount: u64) {
        if let Some(counter) = &self.max_keys_read_counter {
            counter.fetch_add(amount, Ordering::AcqRel);
        }
    }
}

impl Default for ExecutionState {
    fn default() -> Self {
        Self::new()
    }
}
