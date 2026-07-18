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
    Arc,
};
use tidb_txnkv::UnaryCancellation;

/// Shared kill signal corresponding to Go's `*sqlkiller.SQLKiller` identity.
#[derive(Debug, Default)]
pub struct KillHandle {
    signal: AtomicU32,
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
}

impl CancelHandle {
    /// Marks this request as cancelled.
    pub fn cancel(&self) {
        self.cancellation.cancel();
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

/// The dependency-closed subset of TiKV client variables used by the context.
///
/// Go copies the variables struct during detach and rewires its `Killed`
/// pointer to the shared SQL killer. The same shape is represented here by
/// cloning scalar fields while retaining the killer handle identity.
#[derive(Clone, Debug)]
pub struct KvVariables {
    /// Source `BackoffLockFast` setting.
    pub backoff_lock_fast: u64,
    /// Source `BackOffWeight` setting.
    pub backoff_weight: u64,
    /// Shared kill signal observed by KV operations.
    pub killed: Arc<KillHandle>,
}

impl KvVariables {
    /// Creates variables attached to a particular shared kill handle.
    #[must_use]
    pub fn with_killer(killer: Arc<KillHandle>) -> Self {
        Self {
            backoff_lock_fast: 0,
            backoff_weight: 0,
            killed: killer,
        }
    }

    /// Returns whether this variable set observes the supplied killer.
    #[must_use]
    pub fn shares_killer_with(&self, killer: &Arc<KillHandle>) -> bool {
        Arc::ptr_eq(&self.killed, killer)
    }
}

impl Default for KvVariables {
    fn default() -> Self {
        Self::with_killer(Arc::new(KillHandle::default()))
    }
}

/// Execution state carried beside a request context.
#[derive(Debug)]
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
            kv_vars: KvVariables::with_killer(Arc::clone(&killer)),
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
        Self {
            killer: Arc::clone(&self.killer),
            cancel: Arc::clone(&self.cancel),
            cpu_usage: self.cpu_usage.clone(),
            kv_vars: KvVariables {
                backoff_lock_fast: self.kv_vars.backoff_lock_fast,
                backoff_weight: self.kv_vars.backoff_weight,
                killed: Arc::clone(&self.killer),
            },
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
