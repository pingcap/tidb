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

/// Commit-outcome parity with the coordinator facade.
///
/// This is the seam consumers branch on: every caller of the previous
/// coordinator matched on [`OptimisticCommitOutcome`], so the engine's simpler
/// result is mapped back onto exactly those four terminal states.
impl<PdC: PdClient> TikvTransactionDriver<PdC> {
    /// Stages one source mutation set and runs two-phase commit, reporting the
    /// terminal outcome in the coordinator facade's vocabulary.
    ///
    /// The kind-to-op/assertion mapping is the source's own
    /// (`OptimisticMutation::to_proto`), expressed here as buffer state because
    /// the engine reads its mutations off the staged buffer the way client-go's
    /// `initKeysAndMutations` does: a presume-key-not-exists flag makes the
    /// mutation an `Op_Insert`, and the assertion rides along per key.
    pub fn commit_mutations(
        &mut self,
        mutations: Vec<crate::transaction::OptimisticMutation>,
    ) -> Result<crate::transaction::OptimisticCommitOutcome, TikvTransactionError> {
        use crate::transaction::OptimisticMutationKind as Kind;

        let mutation_count = mutations.len();
        let primary_key = mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .min()
            .unwrap_or_default();

        for mutation in mutations {
            let key = Key::from(mutation.key().to_vec());
            let value = mutation.value().to_vec();
            match mutation.kind() {
                Kind::Insert | Kind::UniqueIndexInsert => {
                    self.set_with_flags(key.clone(), value, &[FlagsOp::SetPresumeKeyNotExists])?;
                    self.update_assertion_flags(&key, AssertionOp::AssertNotExist);
                }
                Kind::PutExisting => {
                    self.set(key.clone(), value)?;
                    self.update_assertion_flags(&key, AssertionOp::AssertExist);
                }
                Kind::Delete => {
                    self.delete(key.clone())?;
                    self.update_assertion_flags(&key, AssertionOp::AssertExist);
                }
                Kind::IndexPut | Kind::MetaPut => self.set(key, value)?,
                Kind::IndexDelete | Kind::MetaDelete => self.delete(key)?,
                Kind::LockOnly => {
                    // A key this transaction locked but never wrote still has
                    // to be prewritten, so the primary lock exists after
                    // prewrite. The engine emits `Op_Lock` for a staged key
                    // carrying the locked flag and no value change.
                    self.mem_buffer()
                        .backend_mut()
                        .update_flags(&key, &[FlagsOp::SetNeedLocked]);
                }
            }
        }

        Ok(self.finish_commit(primary_key, mutation_count))
    }

    fn finish_commit(
        &mut self,
        primary_key: Vec<u8>,
        mutation_count: usize,
    ) -> crate::transaction::OptimisticCommitOutcome {
        use crate::transaction::{
            CommittedTransaction, OptimisticCommitOutcome, OptimisticTransactionReceipt,
            RolledBackTransaction, TransactionCause, UndeterminedTransaction,
        };

        // The physical publication telemetry the previous receipt carried
        // (per-batch publications, observed region epochs) is now the engine's
        // own internal accounting and is not exposed; consumers read the
        // commit timestamp and the terminal state, which are.
        let mut receipt =
            OptimisticTransactionReceipt::new(0, self.start_ts, primary_key, mutation_count);

        match self.commit() {
            Ok(commit_ts) => {
                receipt.commit_ts = commit_ts.unwrap_or_default();
                OptimisticCommitOutcome::Committed(CommittedTransaction {
                    receipt,
                    secondary_failures: Vec::new(),
                })
            }
            Err(error) => {
                if is_undetermined(&error) {
                    // The primary was published and its result is unknown:
                    // retrying an ambiguously committed write is a correctness
                    // bug, so this stays its own terminal state.
                    return OptimisticCommitOutcome::Undetermined(UndeterminedTransaction {
                        receipt,
                        cause: TransactionCause::Transport {
                            detail: error.to_string(),
                        },
                    });
                }
                // Every other failure is definitive: the engine rolled its own
                // prewrites back before returning.
                OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
                    receipt,
                    cause: classify_cause(&error),
                })
            }
        }
    }
}

/// Whether the engine reported an ambiguous commit result.
fn is_undetermined(error: &TikvTransactionError) -> bool {
    matches!(
        error,
        TikvTransactionError::Client(tikv_client::Error::UndeterminedError(_))
    )
}

/// Maps one engine failure onto the facade's cause vocabulary.
fn classify_cause(error: &TikvTransactionError) -> crate::transaction::TransactionCause {
    use crate::transaction::TransactionCause;
    use tikv_client::Error as ClientError;

    let TikvTransactionError::Client(client_error) = error else {
        return TransactionCause::Transport {
            detail: error.to_string(),
        };
    };
    match client_error {
        ClientError::KeyExists(exists) => TransactionCause::AlreadyExists {
            key: exists.already_exist.key.clone(),
            detail: client_error.to_string(),
        },
        ClientError::AssertionFailed(failed) => TransactionCause::AssertionFailed {
            key: failed.assertion_failed.key.clone(),
            detail: client_error.to_string(),
        },
        ClientError::WriteConflict(_) | ClientError::WriteConflictInLatch(_) => {
            TransactionCause::WriteConflict {
                detail: client_error.to_string(),
            }
        }
        ClientError::ResolveLockError(locks) => TransactionCause::Lock {
            key: locks
                .first()
                .map(|lock| lock.key.clone())
                .unwrap_or_default(),
            detail: client_error.to_string(),
        },
        _ => TransactionCause::Transport {
            detail: client_error.to_string(),
        },
    }
}

/// Pessimistic-path surface, as Go `KVTxn`'s statement-time locking.
///
/// A pessimistic transaction takes its locks while statements run and then
/// finishes through the same two-phase commit; these are the calls the
/// statement layer makes between those two points.
impl<PdC: PdClient> TikvTransactionDriver<PdC> {
    /// Reads one key under a statement-time pessimistic lock, as Go
    /// `KVTxn.LockKeys` followed by the read for `SELECT ... FOR UPDATE`.
    pub fn get_for_update(&mut self, key: &Key) -> Result<Option<Vec<u8>>, TikvTransactionError> {
        Ok(self.transaction.get_for_update(key.as_bytes().to_vec())?)
    }

    /// Reads several keys under statement-time pessimistic locks.
    pub fn batch_get_for_update(
        &mut self,
        keys: &[Key],
    ) -> Result<Vec<(Key, Vec<u8>)>, TikvTransactionError> {
        Ok(self
            .transaction
            .batch_get_for_update(keys.iter().map(|key| key.as_bytes().to_vec()))?
            .into_iter()
            .map(|pair| (Key::from(Vec::<u8>::from(pair.0)), pair.1))
            .collect())
    }

    /// The keys this transaction currently holds pessimistic locks on.
    ///
    /// Lock state is the *engine's* own: client-go records it as the
    /// `HasLocked` bit on its buffer entry, and TiDB's typed `KeyFlags`
    /// deliberately does not model that bit — `pkg/kv/keyflags.go` exposes
    /// `HasNeedLocked` ("this key must be locked"), which is a different fact.
    /// So this reads the engine's flags directly rather than TiDB's typed
    /// projection of them.
    pub fn locked_keys(&mut self) -> Vec<Key> {
        let memdb = self.transaction.get_mem_buffer();
        let mut keys = Vec::new();
        let mut iterator = memdb.iter_with_flags(None, None);
        while iterator.valid() {
            keys.push(iterator.key().to_vec());
            if iterator.next().is_err() {
                break;
            }
        }
        drop(iterator);
        keys.into_iter()
            .filter(|key| {
                memdb
                    .get_flags_readonly(key)
                    .is_ok_and(|flags| flags.has_locked() || flags.has_locked_in_share_mode())
            })
            .map(Key::from)
            .collect()
    }
}

/// Statement-scoped pessimistic locking, TiDB's fair locking.
///
/// `@@tidb_pessimistic_txn_fair_locking` is client-go's *aggressive locking*:
/// a statement's locks are taken inside a scope that can be retried at a fresh
/// `for_update_ts` (the statement re-runs against newer data) or cancelled
/// (the statement rolls back, releasing only its own locks) without ending the
/// transaction. These are the calls the previous facade spelled
/// `advance_for_update_ts`, `release_statement_locks`, and
/// `pessimistic_rollback`.
impl<PdC: PdClient> TikvTransactionDriver<PdC> {
    /// Opens a statement lock scope.
    pub fn start_statement_locking(&mut self) {
        self.transaction.inner_mut().start_aggressive_locking();
    }

    /// Whether a statement lock scope is open.
    pub fn is_statement_locking(&mut self) -> bool {
        self.transaction.inner_mut().is_in_aggressive_locking_mode()
    }

    /// Retries the statement at a fresh `for_update_ts`, keeping the locks it
    /// already holds that are still wanted. This is the facade's
    /// `advance_for_update_ts`.
    pub fn retry_statement_locking(&mut self) -> Result<(), TikvTransactionError> {
        self.transaction
            .block_on(|transaction| transaction.retry_aggressive_locking())?;
        Ok(())
    }

    /// Rolls the statement back, releasing the locks it took and nothing else.
    /// This is the facade's `release_statement_locks`/`pessimistic_rollback`.
    pub fn cancel_statement_locking(&mut self) -> Result<(), TikvTransactionError> {
        self.transaction
            .block_on(|transaction| transaction.cancel_aggressive_locking())?;
        Ok(())
    }

    /// Closes the statement scope, keeping its locks for the transaction.
    pub fn done_statement_locking(&mut self) -> Result<(), TikvTransactionError> {
        self.transaction
            .block_on(|transaction| transaction.done_aggressive_locking())?;
        Ok(())
    }
}
