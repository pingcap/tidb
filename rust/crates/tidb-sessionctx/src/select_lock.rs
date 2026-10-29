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

//! The lock hand-off a statement attempt shares between its locking
//! operators and its owner (Go `SelectLockExec`'s `doLockKeys` inputs).

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use tidb_txnkv::transaction::LockWaitTime;


/// Go `LazyTxn.LockKeys`'s `updateState(TxnLockAcquiring)` / back to
/// `TxnRunning`: told `true` before a lock request and `false` after it, so
/// `information_schema.TIDB_TRX` shows the transaction waiting on a lock.
#[derive(Clone)]
pub struct LockStateObserver(Arc<dyn Fn(bool) + Send + Sync>);

impl LockStateObserver {
    /// Wraps the session's transaction-state publisher.
    pub fn new(observe: impl Fn(bool) + Send + Sync + 'static) -> Self {
        Self(Arc::new(observe))
    }

    /// Publishes whether a lock request is in flight.
    pub fn set(&self, acquiring: bool) {
        (self.0)(acquiring);
    }
}

impl std::fmt::Debug for LockStateObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LockStateObserver")
    }
}

/// Keys selected by all locking operators in one statement attempt.
/// The owner must drain this set at every attempt boundary, including errors.
#[derive(Clone, Debug)]
pub struct SelectedLockKeys {
    state: Arc<Mutex<SelectedLocks>>,
    default_wait: LockWaitTime,
    shared_lock_promotion: bool,
    /// Go `IsPessimisticReadConsistency`: a read-committed transaction's
    /// locking point reads read first and lock only the rows they found.
    read_committed: bool,
    /// See [`LockStateObserver`].
    lock_state: Option<LockStateObserver>,
    /// Whether the statement's storage takes pessimistic locks while the plan
    /// runs, so [`SelectLockExec`] locks inside `Next` as Go's does (`doLockKeys`)
    /// instead of publishing its keys for the owner's lock step.
    lock_in_execution: bool,
}

#[derive(Debug, Default)]
struct SelectedLocks {
    dml: BTreeSet<Vec<u8>>,
    reads: Vec<(LockWaitTime, Vec<Vec<u8>>)>,
}

/// What one statement attempt left for its owner's lock step.
#[derive(Debug, Default)]
pub struct TakenLocks {
    /// Locking reads' keys with their wait policy, in EOF order. Empty when
    /// the operators locked inside execution.
    pub reads: Vec<(LockWaitTime, Vec<Vec<u8>>)>,
    /// Go `TxnCtx.UnchangedKeysForLock`: matched rows an UPDATE left as they
    /// were, which Go's `handlePessimisticDML` locks together with the keys
    /// the statement wrote.
    pub unchanged: Vec<Vec<u8>>,
}

impl Default for SelectedLockKeys {
    fn default() -> Self {
        Self::new(LockWaitTime::default(), false)
    }
}

impl SelectedLockKeys {
    /// Captures the session policy shared by this attempt's locking operators.
    pub fn new(default_wait: LockWaitTime, shared_lock_promotion: bool) -> Self {
        Self {
            state: Arc::default(),
            default_wait,
            shared_lock_promotion,
            read_committed: false,
            lock_state: None,
            lock_in_execution: false,
        }
    }

    /// Declares that the statement's storage locks inside execution (Go
    /// `doLockKeys` from `SelectLockExec.Next`): the cluster node's
    /// statements, whose lock failures carry the retry the owner acts on.
    #[must_use]
    pub fn with_lock_in_execution(mut self, lock_in_execution: bool) -> Self {
        self.lock_in_execution = lock_in_execution;
        self
    }

    /// Whether [`SelectLockExec`] locks inside `Next`.
    pub fn lock_in_execution(&self) -> bool {
        self.lock_in_execution
    }

    /// See [`LockStateObserver`].
    pub fn lock_state(&self) -> Option<LockStateObserver> {
        self.lock_state.clone()
    }

    /// Publishes lock acquisition to the session's transaction state.
    #[must_use]
    pub fn with_lock_state_observer(mut self, observer: Option<LockStateObserver>) -> Self {
        self.lock_state = observer;
        self
    }

    /// Records the transaction's read-committed isolation.
    #[must_use]
    pub fn with_read_committed(mut self, read_committed: bool) -> Self {
        self.read_committed = read_committed;
        self
    }

    /// Go `IsPessimisticReadConsistency` for this attempt's transaction.
    pub fn read_committed(&self) -> bool {
        self.read_committed
    }

    /// Whether FOR SHARE is promoted to a real lock instead of Go's no-op.
    pub fn shared_lock_promotion(&self) -> bool {
        self.shared_lock_promotion
    }

    /// This statement's SET-able innodb_lock_wait_timeout.
    pub fn default_wait(&self) -> LockWaitTime {
        self.default_wait
    }

    /// Adds a matched DML row that needs a lock even without a mutation.
    pub fn insert(&self, key: Vec<u8>) {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dml
            .insert(key);
    }

    /// Takes the attempt's unique keys in encoded-key order.
    pub fn take(&self) -> Vec<Vec<u8>> {
        self.take_requests()
            .into_iter()
            .flat_map(|(_, keys)| keys)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Drains operators in EOF order, preserving each operator's wait policy.
    /// Matched DML rows follow locking reads, as in Go's statement lock step.
    pub fn take_requests(&self) -> Vec<(LockWaitTime, Vec<Vec<u8>>)> {
        let mut taken = self.take_all();
        if !taken.unchanged.is_empty() {
            taken.reads.push((self.default_wait, taken.unchanged));
        }
        taken.reads
    }

    /// Drains the attempt's published keys, keeping the matched-but-unchanged
    /// rows apart from the locking reads' keys: Go locks the former together
    /// with the keys the statement wrote, in one `LockKeys`.
    pub fn take_all(&self) -> TakenLocks {
        let state = std::mem::take(&mut *self.state.lock().unwrap_or_else(|p| p.into_inner()));
        TakenLocks {
            reads: state.reads,
            unchanged: state.dml.into_iter().collect(),
        }
    }

    pub fn extend(&self, keys: BTreeSet<Vec<u8>>, wait: LockWaitTime) {
        if !keys.is_empty() {
            self.state
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .reads
                .push((wait, keys.into_iter().collect()));
        }
    }
}

