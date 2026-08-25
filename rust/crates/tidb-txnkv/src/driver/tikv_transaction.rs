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
//! keeps its full typed transaction-buffer contract — staging scopes for
//! statement commit/rollback, key flags, assertions, tombstones, buffer
//! iteration — while two-phase commit, lock resolution, and snapshot reads
//! are delegated to `tikv_client::transaction::Transaction`, the transcreated
//! client-go engine.
//!
//! One deliberate difference from Go, pending upstream unification: client-go
//! commits directly out of the memdb that TiDB staged into, while client-rust
//! `Transaction` still owns a separate mutation buffer. This driver therefore
//! stages into [`MemBufferDriver`]`<`[`TikvMemBufferBackend`]`>` (itself the
//! transcreated client-go memdb) and drains the surviving entries — values,
//! tombstones, and their flags — into the transaction at commit, mapping
//! flags exactly as client-go's `initKeysAndMutations` does: a
//! presume-key-not-exists flag becomes an insert (`Op_Insert`), assertions
//! and prewrite constraint checks become per-mutation options. When upstream
//! merges its transaction buffer with its memdb, the drain step collapses
//! away without changing this driver's surface.
//!
//! The driver is optimistic-transaction scoped: pessimistic lock acquisition
//! has its own driver seam because locks must be taken at statement time, not
//! commit time.

use tikv_client::pd::PdClient;
use tikv_client::transaction::{MutationAssertion, MutationOptions, Transaction};
use tikv_client::Timestamp;

use crate::batch_getter::GetOptions;
use crate::driver::mem_buffer::{MemBufferDriver, StagingHandle};
use crate::driver::tikv_mem_buffer::{TikvMemBufferBackend, TikvMemBufferError};
use crate::key_flags::AssertionState;
use crate::{FlagsOp, Key, KeyFlags, KvIterator, MemBufferBackend};

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

fn mutation_options_from_flags(flags: KeyFlags) -> MutationOptions {
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

/// TiDB-facing optimistic transaction over the client-rust engine.
pub struct TikvTransactionDriver<PdC: PdClient> {
    buffer: MemBufferDriver<TikvMemBufferBackend>,
    transaction: Transaction<PdC>,
}

impl<PdC: PdClient> TikvTransactionDriver<PdC> {
    /// Wraps one begun client transaction with an empty staged buffer.
    #[must_use]
    pub fn new(transaction: Transaction<PdC>) -> Self {
        Self {
            buffer: MemBufferDriver::new(TikvMemBufferBackend::new(), false),
            transaction,
        }
    }

    /// The staged transaction buffer: staging scopes, flags, tombstones, and
    /// ordered iteration, exactly the Go `MemBuffer` contract.
    pub fn mem_buffer(&mut self) -> &mut MemBufferDriver<TikvMemBufferBackend> {
        &mut self.buffer
    }

    /// Read-only view of the staged buffer.
    #[must_use]
    pub fn mem_buffer_ref(&self) -> &MemBufferDriver<TikvMemBufferBackend> {
        &self.buffer
    }

    /// The wrapped client transaction, for engine options TiDB sets directly
    /// (priorities, resource groups, schema hooks).
    pub fn transaction_mut(&mut self) -> &mut Transaction<PdC> {
        &mut self.transaction
    }

    /// The transaction's start timestamp.
    #[must_use]
    pub fn start_timestamp(&self) -> Timestamp {
        self.transaction.start_timestamp()
    }

    /// Union read: the staged buffer overlays the transaction snapshot, and a
    /// buffered tombstone hides a committed value, like Go's union store.
    pub async fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, TikvTransactionError> {
        match self.buffer.backend_mut().get(key, GetOptions::default()) {
            Ok(entry) => {
                if entry.is_value_empty() {
                    return Ok(None);
                }
                return Ok(Some(entry.value));
            }
            Err(TikvMemBufferError::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        Ok(self.transaction.get(key.as_bytes().to_vec()).await?)
    }

    /// Stages one value into the transaction buffer.
    pub fn set(&mut self, key: Key, value: Vec<u8>) -> Result<(), TikvTransactionError> {
        self.buffer.backend_mut().set(key, value)?;
        Ok(())
    }

    /// Stages one value with typed flags.
    pub fn set_with_flags(
        &mut self,
        key: Key,
        value: Vec<u8>,
        operations: &[FlagsOp],
    ) -> Result<(), TikvTransactionError> {
        self.buffer.set_with_flags(key, value, operations)?;
        Ok(())
    }

    /// Stages one deletion tombstone.
    pub fn delete(&mut self, key: Key) -> Result<(), TikvTransactionError> {
        self.buffer.backend_mut().delete(key)?;
        Ok(())
    }

    /// Starts a statement staging scope (Go `StmtCommit`/`StmtRollback`).
    pub fn staging(&mut self) -> StagingHandle {
        self.buffer.staging()
    }

    /// Rolls one staging scope back.
    pub fn cleanup(&mut self, handle: StagingHandle) {
        self.buffer.cleanup(handle);
    }

    /// Commits one staging scope into its parent.
    pub fn release(&mut self, handle: StagingHandle) {
        self.buffer.release(handle);
    }

    /// Drains the surviving buffer entries into the client transaction and
    /// runs client-go two-phase commit. Returns the commit timestamp for a
    /// non-empty transaction.
    pub async fn commit(&mut self) -> Result<Option<Timestamp>, TikvTransactionError> {
        let entries = self.drain_entries()?;
        for (key, value, flags) in entries {
            let raw_key = key.as_bytes().to_vec();
            let options = mutation_options_from_flags(flags);
            if value.is_empty() {
                // A tombstone commits as a delete; assertions still apply.
                self.transaction
                    .delete_with_options(raw_key, options)
                    .await?;
            } else if flags.has_presume_key_not_exists() {
                // Client-go's initKeysAndMutations turns the
                // presume-key-not-exists flag into Op_Insert.
                self.transaction
                    .insert_with_options(raw_key, value, options)
                    .await?;
            } else {
                self.transaction
                    .put_with_options(raw_key, value, options)
                    .await?;
            }
        }
        Ok(self.transaction.commit().await?)
    }

    /// Rolls the transaction back; staged entries never reach the store.
    pub async fn rollback(&mut self) -> Result<(), TikvTransactionError> {
        self.transaction.rollback().await?;
        Ok(())
    }

    fn drain_entries(&mut self) -> Result<Vec<(Key, Vec<u8>, KeyFlags)>, TikvTransactionError> {
        let backend = self.buffer.backend_mut();
        let mut entries = Vec::with_capacity(backend.len());
        let start = Key::from(Vec::new());
        let mut iterator = backend.iter(&start, None)?;
        while iterator.valid() {
            entries.push((iterator.key().clone(), iterator.value().to_vec()));
            iterator.next()?;
        }
        drop(iterator);
        Ok(entries
            .into_iter()
            .map(|(key, value)| {
                let flags = backend.get_flags(&key).unwrap_or_default();
                (key, value, flags)
            })
            .collect())
    }
}
