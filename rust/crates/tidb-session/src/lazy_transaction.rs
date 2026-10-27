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

//! Go's pending transaction (`LazyTxn` in the pending state) for the
//! cluster node.
//!
//! `session.PrepareTxnCtx` (`pkg/session/session.go:4908`) enters a new
//! transaction with `EnterNewTxnBeforeStmt` for any statement that finds no
//! valid or pending one, and `baseTxnContextProvider.OnInitialize`
//! (`pkg/sessiontxn/isolation/base.go:81`) leaves it unactivated
//! (`activeNow = false`): no timestamp is taken and `TxnCtx.StartTS` stays 0.
//! The transaction becomes valid at the first caller of `ActivateTxn`
//! (`base.go:283`) -- a table read, a point get, a DML builder, `SAVEPOINT`
//! -- which waits the timestamp future and publishes `StartTS`. `BEGIN`
//! (`EnterNewTxnWithBeginStmt`) activates at once and does not come here.
//!
//! [`LazyClusterTransaction`] is that state over the store transaction a
//! `begin` call would have opened eagerly: every method that needs the store
//! transaction opens it first, and the few that Go answers without one (the
//! pessimistic flag, the statement count, a pending resource group) are
//! answered from what the wrapper holds. A snapshot handed out before the
//! activation opens the transaction at its first read.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use tidb_cluster::cluster_table_storage::LockKeysOutcome;
use tidb_cluster::session_commit_protocol::SessionTransactionOptions;
use tidb_executor::cluster_storage::{
    ClusterSnapshot, DuplicateKeyHint, MutationBuffer, SnapshotPairs,
};
use tidb_executor::storage::StorageError;
use tidb_txnkv::transaction::{LockWaitTime, SchemaLeaseChecker};
use tidb_txnkv::Key;

use crate::cluster_transactions::{CommittedTxnInfo, OpenClusterTransaction};
use tidb_cluster::sql_query_error::SqlQueryError;

/// Opens the store transaction from the options in force at the activation
/// (Go `SetOptionsOnTxnActive`), and publishes its start timestamp.
pub type TransactionOpener = Box<
    dyn FnOnce(SessionTransactionOptions) -> Result<Box<dyn OpenClusterTransaction>, String> + Send,
>;

struct Pending {
    opener: Option<TransactionOpener>,
    options: SessionTransactionOptions,
    resource_group: Option<String>,
    assertion_level: Option<tidb_proto::KvrpcAssertionLevel>,
    buffer: Option<MutationBuffer>,
    checker: Option<Arc<dyn SchemaLeaseChecker>>,
    /// The activation's own failure, reported by every later call that needs
    /// the transaction.
    failed: Option<String>,
    open: Option<Box<dyn OpenClusterTransaction>>,
}

struct Shared {
    pessimistic: bool,
    created_at: Instant,
    statements: std::sync::atomic::AtomicU64,
    state: Mutex<Pending>,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, Pending> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    /// Go `ActivateTxn`: opens the store transaction the first time it is
    /// needed and replays what the session set while it was pending.
    fn activate_in(state: &mut Pending) -> Result<(), String> {
        if state.open.is_some() {
            return Ok(());
        }
        if let Some(error) = &state.failed {
            return Err(error.clone());
        }
        let Some(opener) = state.opener.take() else {
            return Err("the pending transaction was already consumed".to_owned());
        };
        match opener(state.options.clone()) {
            Ok(transaction) => {
                if let Some(buffer) = &state.buffer {
                    transaction.bind_mutation_buffer(buffer);
                }
                if let Some(checker) = &state.checker {
                    let mut transaction = transaction;
                    transaction.set_schema_lease_checker(Arc::clone(checker));
                    Self::replay_statement_settings(state, transaction.as_ref())?;
                    state.open = Some(transaction);
                    return Ok(());
                }
                Self::replay_statement_settings(state, transaction.as_ref())?;
                state.open = Some(transaction);
                Ok(())
            }
            Err(error) => {
                state.failed = Some(error.clone());
                Err(error)
            }
        }
    }

    fn replay_statement_settings(
        state: &Pending,
        transaction: &dyn OpenClusterTransaction,
    ) -> Result<(), String> {
        if let Some(group) = &state.resource_group {
            transaction.set_resource_group_name(group)?;
        }
        if let Some(level) = state.assertion_level {
            transaction.set_statement_assertion_level(level)?;
        }
        Ok(())
    }

    /// Runs `use_open` against the activated transaction.
    fn with_open<T>(
        &self,
        use_open: impl FnOnce(&dyn OpenClusterTransaction) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut state = self.state();
        Self::activate_in(&mut state)?;
        use_open(state.open.as_deref().expect("just activated"))
    }
}

/// A transaction that has entered but not yet activated; see the module
/// documentation.
pub struct LazyClusterTransaction {
    shared: Arc<Shared>,
}

impl LazyClusterTransaction {
    pub fn new(
        pessimistic: bool,
        options: SessionTransactionOptions,
        buffer: MutationBuffer,
        opener: TransactionOpener,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                pessimistic,
                created_at: Instant::now(),
                statements: std::sync::atomic::AtomicU64::new(0),
                state: Mutex::new(Pending {
                    opener: Some(opener),
                    options,
                    resource_group: None,
                    assertion_level: None,
                    buffer: Some(buffer),
                    checker: None,
                    failed: None,
                    open: None,
                }),
            }),
        }
    }

    /// A snapshot that opens the transaction at its first read and is then
    /// the snapshot `make` builds over it.
    fn lazy_snapshot(
        &self,
        make: impl FnOnce(&dyn OpenClusterTransaction) -> Result<Box<dyn ClusterSnapshot>, String>
            + Send
            + 'static,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        let already_open = self.shared.state().open.is_some();
        if already_open {
            return self.shared.with_open(|transaction| make(transaction));
        }
        Ok(Box::new(LazySnapshot {
            inner: Mutex::new(LazySnapshotState {
                shared: Arc::clone(&self.shared),
                make: Some(Box::new(make)),
                opened: None,
                read_options: None,
                kv_context: None,
            }),
        }))
    }
}

impl fmt::Debug for LazyClusterTransaction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LazyClusterTransaction")
            .field("pessimistic", &self.shared.pessimistic)
            .field("active", &self.shared.state().open.is_some())
            .finish()
    }
}

impl OpenClusterTransaction for LazyClusterTransaction {
    fn bind_mutation_buffer(&self, buffer: &MutationBuffer) {
        let mut state = self.shared.state();
        match state.open.as_ref() {
            Some(transaction) => transaction.bind_mutation_buffer(buffer),
            None => state.buffer = Some(buffer.clone()),
        }
    }

    fn start_ts(&self) -> u64 {
        // `Txn(true)`: reading the start timestamp activates, and an
        // activation that failed is reported by the call that needs a
        // transaction, so the zero here is never used to read or commit.
        self.shared
            .with_open(|transaction| Ok(transaction.start_ts()))
            .unwrap_or(0)
    }

    fn start_ts_if_active(&self) -> Option<u64> {
        self.shared
            .state()
            .open
            .as_ref()
            .map(|transaction| transaction.start_ts())
    }

    fn activate(&self) -> Result<(), String> {
        self.shared.with_open(|_| Ok(()))
    }

    fn set_activation_options(&self, options: SessionTransactionOptions) {
        let mut state = self.shared.state();
        if state.open.is_none() {
            state.options = options;
        }
    }

    fn opened_at(&self) -> Instant {
        self.shared.created_at
    }

    fn statement_count(&self) -> u64 {
        self.shared
            .statements
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    fn note_statement(&self) {
        self.shared
            .statements
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn set_resource_group_name(&self, name: &str) -> Result<(), String> {
        let mut state = self.shared.state();
        match state.open.as_ref() {
            Some(transaction) => transaction.set_resource_group_name(name),
            None => {
                state.resource_group = Some(name.to_owned());
                Ok(())
            }
        }
    }

    fn set_statement_assertion_level(
        &self,
        level: tidb_proto::KvrpcAssertionLevel,
    ) -> Result<(), String> {
        let mut state = self.shared.state();
        match state.open.as_ref() {
            Some(transaction) => transaction.set_statement_assertion_level(level),
            None => {
                state.assertion_level = Some(level);
                Ok(())
            }
        }
    }

    fn set_schema_lease_checker(&mut self, checker: Arc<dyn SchemaLeaseChecker>) {
        let mut state = self.shared.state();
        match state.open.as_mut() {
            Some(transaction) => transaction.set_schema_lease_checker(checker),
            None => state.checker = Some(checker),
        }
    }

    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.lazy_snapshot(|transaction| transaction.snapshot())
    }

    fn snapshot_for(&self, locking: bool) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.lazy_snapshot(move |transaction| transaction.snapshot_for(locking))
    }

    fn commit(
        self: Box<Self>,
        buffer: &tidb_executor::cluster_storage::MutationBuffer,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError> {
        let activated = self.shared.state().open.is_some();
        // Go commits a transaction that never became valid by doing
        // nothing (`doCommitWithRetry` returns for `!txn.Valid()`). A buffer
        // that holds writes means a statement staged them without
        // activating, which the statement builders prevent; open one then so
        // they are published rather than dropped.
        if !activated && buffer.is_empty() {
            return Ok(None);
        }
        let transaction = {
            let mut state = self.shared.state();
            Shared::activate_in(&mut state).map_err(SqlQueryError::unknown)?;
            state.open.take().expect("just activated")
        };
        transaction.commit(buffer)
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
        let transaction = self.shared.state().open.take();
        match transaction {
            Some(transaction) => transaction.rollback(),
            None => Ok(()),
        }
    }

    fn is_pessimistic(&self) -> bool {
        self.shared.pessimistic
    }

    fn for_update_ts(&self) -> u64 {
        self.shared
            .state()
            .open
            .as_ref()
            .map_or(0, |transaction| transaction.for_update_ts())
    }

    fn fresh_locking_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.lazy_snapshot(|transaction| transaction.fresh_locking_snapshot())
    }

    fn fresh_statement_snapshot(&self, locking: bool) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.lazy_snapshot(move |transaction| transaction.fresh_statement_snapshot(locking))
    }

    fn deferred_statement_snapshot(
        &self,
        locking: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.lazy_snapshot(move |transaction| transaction.deferred_statement_snapshot(locking))
    }

    fn take_statement_lock_failure(
        &self,
    ) -> Option<tidb_cluster::cluster_table_storage::StatementLockFailure> {
        self.shared
            .state()
            .open
            .as_ref()
            .and_then(|transaction| transaction.take_statement_lock_failure())
    }

    fn snapshot_at(&self, read_ts: u64) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.lazy_snapshot(move |transaction| transaction.snapshot_at(read_ts))
    }

    fn snapshot_at_for(
        &self,
        read_ts: u64,
        locking: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.lazy_snapshot(move |transaction| transaction.snapshot_at_for(read_ts, locking))
    }

    fn lock_staged_keys(&self, keys: Vec<Vec<u8>>) -> Result<LockKeysOutcome, String> {
        self.shared
            .with_open(|transaction| transaction.lock_staged_keys(keys))
    }

    fn lock_staged_keys_with_assertions(
        &self,
        keys: Vec<Vec<u8>>,
        presume_not_exists: std::collections::BTreeSet<Vec<u8>>,
        duplicate_hints: std::collections::BTreeMap<Vec<u8>, DuplicateKeyHint>,
        wait: LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        self.shared.with_open(|transaction| {
            transaction.lock_staged_keys_with_assertions(
                keys,
                presume_not_exists,
                duplicate_hints,
                wait,
            )
        })
    }

    fn lock_staged_keys_with_values(
        &self,
        keys: Vec<Vec<u8>>,
        wait: LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        self.shared
            .with_open(|transaction| transaction.lock_staged_keys_with_values(keys, wait))
    }

    fn lock_staged_keys_with_values_if_exist(
        &self,
        keys: Vec<Vec<u8>>,
        wait: LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        self.shared
            .with_open(|transaction| transaction.lock_staged_keys_with_values_if_exist(keys, wait))
    }

    fn finish_pessimistic_statement(&self, successful: bool) -> Result<(), String> {
        match self.shared.state().open.as_ref() {
            Some(transaction) => transaction.finish_pessimistic_statement(successful),
            None => Ok(()),
        }
    }
}

type SnapshotMaker =
    Box<dyn FnOnce(&dyn OpenClusterTransaction) -> Result<Box<dyn ClusterSnapshot>, String> + Send>;

struct LazySnapshotState {
    shared: Arc<Shared>,
    make: Option<SnapshotMaker>,
    opened: Option<Box<dyn ClusterSnapshot>>,
    read_options: Option<tidb_txnkv::SnapshotReadOptions>,
    kv_context: Option<tidb_txnkv::StatementKvContextCell>,
}

impl LazySnapshotState {
    /// Activates the transaction and builds the statement's snapshot over it.
    fn open(&mut self) -> Result<&mut dyn ClusterSnapshot, StorageError> {
        if self.opened.is_none() {
            let make = self.make.take().ok_or_else(|| {
                StorageError::Backend("the snapshot was already consumed".to_owned())
            })?;
            let mut snapshot = self
                .shared
                .with_open(|transaction| make(transaction))
                .map_err(StorageError::Backend)?;
            if let Some(options) = &self.read_options {
                snapshot.set_snapshot_read_options(options.clone());
            }
            if let Some(context) = &self.kv_context {
                snapshot.set_statement_kv_context(context);
            }
            self.opened = Some(snapshot);
        }
        Ok(self.opened.as_deref_mut().expect("just opened"))
    }
}

/// One statement's read handle over a transaction that may still be pending:
/// the first act that reads opens the transaction.
struct LazySnapshot {
    inner: Mutex<LazySnapshotState>,
}

impl LazySnapshot {
    fn state(&self) -> MutexGuard<'_, LazySnapshotState> {
        self.inner
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

impl fmt::Debug for LazySnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LazySnapshot")
            .field("opened", &self.state().opened.is_some())
            .finish()
    }
}

impl ClusterSnapshot for LazySnapshot {
    fn set_snapshot_read_options(&mut self, options: tidb_txnkv::SnapshotReadOptions) {
        let mut state = self.state();
        if let Some(opened) = state.opened.as_mut() {
            opened.set_snapshot_read_options(options.clone());
        }
        state.read_options = Some(options);
    }

    fn set_statement_kv_context(&mut self, context: &tidb_txnkv::StatementKvContextCell) {
        let mut state = self.state();
        if let Some(opened) = state.opened.as_mut() {
            opened.set_statement_kv_context(context);
        }
        state.kv_context = Some(context.clone());
    }

    fn point_rpc_counts(&mut self) -> (u64, u64) {
        self.state()
            .opened
            .as_mut()
            .map_or((0, 0), |opened| opened.point_rpc_counts())
    }

    fn lock_keys_for_read(
        &mut self,
        keys: &[Key],
        wait: LockWaitTime,
        only_if_exists: bool,
    ) -> Result<(), StorageError> {
        self.state()
            .open()?
            .lock_keys_for_read(keys, wait, only_if_exists)
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.state().open()?.get(key)
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.state().open()?.batch_get(keys)
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.state().open()?.scan(start, end, limit)
    }

    fn scan_committed(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.state().open()?.scan_committed(start, end, limit)
    }

    fn get_committed(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.state().open()?.get_committed(key)
    }

    fn batch_get_committed(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.state().open()?.batch_get_committed(keys)
    }

    fn start_ts(&self) -> u64 {
        self.state().open().map_or(0, |opened| opened.start_ts())
    }
}
