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

//! TiDB transaction driver over the vendored client-rust transaction engine.
//!
//! This is the Rust counterpart of Go's `pkg/store/driver/txn.tikvTxn`: TiDB
//! keeps its typed transaction-buffer contract — staging scopes for statement
//! commit/rollback, key flags, assertions, tombstones, buffer iteration —
//! while two-phase commit, lock resolution, and snapshot reads are delegated
//! to `tikv_client`, the transcreated client-go engine.
//!
//! Staging goes into the transaction's own authoritative buffer
//! (`get_mem_buffer`, client-go's `KVTxn.GetMemBuffer`), so what TiDB stages
//! *is* what the engine commits: there is no second buffer and no
//! reconciliation step. Flags therefore reach prewrite the way client-go's
//! `initKeysAndMutations` reads them off the memdb — a presume-key-not-exists
//! flag makes the mutation an insert, assertions and prewrite constraint
//! checks ride along as per-key state.
//!
//! The driver's API is synchronous, like client-go's `KVTxn` and like the
//! TiDB transaction consumers it stands in for; the vendored crate's
//! `SyncTransaction` supplies the runtime and its nested-runtime guard.

use std::sync::Arc;

use tikv_client::pd::PdClient;
use tikv_client::transaction::{MutationAssertion, MutationOptions, SyncTransaction, Transaction};
use tikv_client::TimestampExt;

use crate::batch_getter::GetOptions;
use crate::driver::mem_buffer::{MemBufferDriver, StagingHandle};
use crate::driver::tikv_mem_buffer::{TikvMemBufferBackend, TikvMemBufferError};
use crate::key_flags::AssertionState;
use crate::{AssertionOp, FlagsOp, Key, KeyFlags, MemBufferBackend};

/// Error surface of the transaction driver: a buffer-level failure or a
/// client-engine failure, kept distinct so callers can classify retries the
/// way Go separates driver errors from client-go errors.
#[derive(Debug)]
pub enum TikvTransactionError {
    /// The staged transaction buffer failed.
    Buffer(TikvMemBufferError),
    /// The client transaction engine failed (2PC, snapshot read, rollback).
    Client(tikv_client::Error),
}

impl std::fmt::Display for TikvTransactionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Buffer(error) => write!(formatter, "transaction buffer error: {error}"),
            Self::Client(error) => write!(formatter, "tikv client error: {error}"),
        }
    }
}

impl std::error::Error for TikvTransactionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Buffer(error) => Some(error),
            Self::Client(error) => Some(error),
        }
    }
}

impl From<TikvMemBufferError> for TikvTransactionError {
    fn from(error: TikvMemBufferError) -> Self {
        Self::Buffer(error)
    }
}

impl From<tikv_client::Error> for TikvTransactionError {
    fn from(error: tikv_client::Error) -> Self {
        Self::Client(error)
    }
}

/// Per-mutation controls carried by one staged key, for callers that set them
/// through the engine's mutation surface rather than through buffer flags.
#[must_use]
pub fn mutation_options_from_flags(flags: KeyFlags) -> MutationOptions {
    let assertion = match flags.assertion() {
        AssertionState::Unset => MutationAssertion::None,
        AssertionState::Exists => MutationAssertion::Exist,
        AssertionState::NotExists => MutationAssertion::NotExist,
        AssertionState::Unknown => MutationAssertion::Unknown,
    };
    MutationOptions::default()
        .assertion(assertion)
        .need_constraint_check_in_prewrite(flags.has_need_constraint_check_in_prewrite())
}

/// TiDB-facing transaction over the client-rust engine.
pub struct TikvTransactionDriver<PdC: PdClient> {
    transaction: SyncTransaction<PdC>,
    start_ts: u64,
    pipelined_dml: bool,
    read_only: bool,
}

impl<PdC: PdClient> TikvTransactionDriver<PdC> {
    /// Wraps one begun writable client transaction.
    #[must_use]
    pub fn new(transaction: Transaction<PdC>, runtime: Arc<tokio::runtime::Runtime>) -> Self {
        Self::with_mode(transaction, runtime, false)
    }

    /// Wraps one begun read-only client transaction.
    ///
    /// The mode has to be supplied by whoever built the transaction options:
    /// the engine's own `is_read_only` answers the *dynamic* question client-go
    /// `KVTxn.IsReadOnly` answers — "has this written anything yet" — which is
    /// true of every freshly opened writable transaction too.
    #[must_use]
    pub fn new_read_only(
        transaction: Transaction<PdC>,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self::with_mode(transaction, runtime, true)
    }

    fn with_mode(
        transaction: Transaction<PdC>,
        runtime: Arc<tokio::runtime::Runtime>,
        read_only: bool,
    ) -> Self {
        // The start timestamp is captured here because the source exposes it
        // on the async transaction, while every later call goes through the
        // blocking wrapper.
        let start_ts = transaction.start_timestamp().version();
        let pipelined_dml = transaction.is_pipelined();
        Self {
            transaction: SyncTransaction::new(transaction, runtime),
            start_ts,
            pipelined_dml,
            read_only,
        }
    }

    /// The transaction's staged buffer under TiDB's typed contract: staging
    /// scopes, flags, assertions, tombstones, and ordered iteration.
    ///
    /// This borrows the *same* buffer the engine commits out of, so a caller
    /// holding this view is staging directly into the transaction, exactly
    /// like Go's `txn.GetMemBuffer()`.
    pub fn mem_buffer(&mut self) -> MemBufferDriver<TikvMemBufferBackend<'_>> {
        MemBufferDriver::new(
            TikvMemBufferBackend::new(self.transaction.get_mem_buffer()),
            self.pipelined_dml,
        )
    }

    /// The wrapped client transaction, for engine options TiDB sets directly
    /// (priorities, resource groups, schema hooks).
    pub fn transaction_mut(&mut self) -> &mut SyncTransaction<PdC> {
        &mut self.transaction
    }

    /// The transaction's start timestamp, as Go `KVTxn.StartTS`.
    #[must_use]
    pub const fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Number of keys staged in the transaction buffer.
    pub fn len(&mut self) -> usize {
        self.transaction.get_mem_buffer().len()
    }

    /// Whether the transaction has staged no keys.
    pub fn is_empty(&mut self) -> bool {
        self.len() == 0
    }

    /// Approximate staged size in bytes, as Go `KVTxn.Size`.
    pub fn size(&mut self) -> usize {
        self.transaction.get_mem_buffer().size()
    }

    /// Union read: the staged buffer overlays the transaction snapshot, and a
    /// buffered tombstone hides a committed value, like Go's union store.
    ///
    /// The engine's own `get` already consults the staged buffer first, since
    /// that buffer is authoritative; the tombstone mapping is this driver's,
    /// because TiDB reads an empty buffered value as "deleted", not as an
    /// empty value.
    pub fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, TikvTransactionError> {
        match self
            .mem_buffer()
            .backend_mut()
            .get(key, GetOptions::default())
        {
            Ok(entry) => {
                if entry.is_value_empty() {
                    return Ok(None);
                }
                return Ok(Some(entry.value));
            }
            Err(TikvMemBufferError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(self.transaction.get(key.as_bytes().to_vec())?)
    }

    /// Snapshot range read, as Go `Snapshot.Iter`: committed data only, with
    /// the staged buffer overlaid by the caller's own buffer scan.
    pub fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: u32,
    ) -> Result<Vec<(Key, Vec<u8>)>, TikvTransactionError> {
        let range = start.as_bytes().to_vec()..end.as_bytes().to_vec();
        Ok(self
            .transaction
            .scan(range, limit)?
            .map(|pair| (Key::from(Vec::<u8>::from(pair.0)), pair.1))
            .collect())
    }

    /// Stages one value into the transaction buffer.
    pub fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), TikvTransactionError> {
        self.mem_buffer().backend_mut().set(key, value)?;
        Ok(())
    }

    /// Stages one value and applies typed flags to its key.
    pub fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        operations: &[FlagsOp],
    ) -> Result<(), TikvTransactionError> {
        self.mem_buffer()
            .backend_mut()
            .set_with_flags(key, value, operations)?;
        Ok(())
    }

    /// Stages one deletion tombstone.
    pub fn delete(&mut self, key: Key) -> Result<(), TikvTransactionError> {
        self.mem_buffer().backend_mut().delete(key)?;
        Ok(())
    }

    /// Applies one assertion to a staged key, as Go's mutation assertions.
    pub fn update_assertion_flags(&mut self, key: &Key, operation: AssertionOp) {
        self.mem_buffer()
            .backend_mut()
            .update_assertion_flags(key, operation);
    }

    /// Returns the typed flags currently carried by one staged key.
    pub fn get_flags(&mut self, key: &Key) -> Result<KeyFlags, TikvTransactionError> {
        Ok(self.mem_buffer().backend_mut().get_flags(key)?)
    }

    /// Starts a statement staging scope (Go `StmtCommit`/`StmtRollback`).
    pub fn staging(&mut self) -> StagingHandle {
        self.mem_buffer().staging()
    }

    /// Rolls one staging scope back.
    pub fn cleanup(&mut self, handle: StagingHandle) {
        self.mem_buffer().cleanup(handle);
    }

    /// Commits one staging scope into its parent.
    pub fn release(&mut self, handle: StagingHandle) {
        self.mem_buffer().release(handle);
    }

    /// Acquires pessimistic locks at statement time, as Go `KVTxn.LockKeys`
    /// does for `SELECT ... FOR UPDATE` and for pessimistic DML.
    pub fn lock_keys(&mut self, keys: &[Key]) -> Result<(), TikvTransactionError> {
        self.transaction
            .lock_keys(keys.iter().map(|key| key.as_bytes().to_vec()))?;
        Ok(())
    }

    /// Runs client-go two-phase commit over the staged buffer. Returns the
    /// commit timestamp for a transaction that wrote anything.
    pub fn commit(&mut self) -> Result<Option<u64>, TikvTransactionError> {
        Ok(self
            .transaction
            .commit()?
            .map(|timestamp| timestamp.version()))
    }

    /// Rolls the transaction back; staged entries never reach the store.
    ///
    /// Only a writable transaction can be rolled back: a read-only one never
    /// took a lock or prewrote anything, so the engine rejects the call the
    /// way client-go does. Callers that do not know which they hold should use
    /// [`Self::finish_without_writes`].
    pub fn rollback(&mut self) -> Result<(), TikvTransactionError> {
        self.transaction.rollback()?;
        Ok(())
    }

    /// Whether this transaction was opened read-only and so can never write.
    ///
    /// Distinct from the engine's `is_read_only`, which answers client-go
    /// `KVTxn.IsReadOnly`'s dynamic question ("nothing staged yet") and is
    /// therefore true of a fresh writable transaction as well.
    #[must_use]
    pub const fn is_read_only_mode(&self) -> bool {
        self.read_only
    }

    /// Whether nothing has been staged yet, as Go `KVTxn.IsReadOnly`.
    pub fn has_no_writes(&mut self) -> bool {
        self.is_empty()
    }

    /// Ends a transaction that wrote nothing.
    ///
    /// A writable transaction still has to be rolled back so its locks and
    /// staged state are released; a read-only one has neither, and the engine
    /// treats rolling it back as an invalid transition. This is the one call a
    /// statement path can make without tracking which kind it opened — and the
    /// engine's drop check requires making it, since dropping a still-active
    /// transaction is a bug it refuses to ignore.
    pub fn finish_without_writes(&mut self) -> Result<(), TikvTransactionError> {
        if self.read_only {
            return Ok(());
        }
        self.rollback()
    }
}
