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

//! One connection's open `BEGIN` ... `COMMIT` transaction over real TiKV.
//!
//! This is the executor-side owner of everything a multi-statement transaction
//! does between its two control statements. The connection holds exactly one of
//! these while a transaction is open; every statement in between runs *through*
//! it rather than opening a transaction of its own.
//!
//! Three contracts live here, all of them Go's:
//!
//! **Read-your-own-writes.** A statement inside the transaction observes the
//! transaction's own uncommitted writes. Go does this in `pkg/store/driver/txn`:
//! `tikvTxn.Get` consults the `MemBuffer` first and only falls back to the
//! snapshot at `start_ts` when the key is unstaged. [`Self::staged_row`] is that
//! membuffer lookup, keyed by clustered handle, and [`Self::read_at_snapshot`]
//! is the same overlay in the shape the write planner consumes — so an `UPDATE`
//! of a row the transaction already wrote computes its new row from the value it
//! is replacing, not from the stale snapshot. This is exactly what makes
//! [`TransactionMutationBuffer`]'s repeated-update coalescing correct.
//!
//! **Statement scope versus transaction scope.** A pessimistic lock failure ends
//! the *statement*: the transaction stays open and usable, matching
//! `pkg/executor/adapter.go`, which retries or reports a locking statement
//! without touching the transaction. A write conflict is the asymmetry between
//! the two modes: pessimistic detects it while locking, so it is statement-
//! scoped and retried under a fresh `for_update_ts`; optimistic detects it at
//! Prewrite, so it can only surface at `COMMIT` and it ends the transaction.
//!
//! **Nothing is published before `COMMIT`.** Buffered mutations reach TiKV only
//! in the final two-phase commit, so a concurrent reader sees the pre-transaction
//! value until then. A pessimistic transaction does take locks earlier, but a
//! TiKV pessimistic lock blocks writers, not readers.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use tidb_codec::table_key::{decode_record_key, encode_row_key_with_handle, RecordHandle};
use tidb_datatype::{Datum, SessionTimeZone};
use tidb_executor::deadlock_history::record_deadlock;
use tidb_pd_client::PdClient;
use tidb_planner::physical_selection::{ComparisonOp, ComparisonOperand, PhysicalSelectionPlan};
use tidb_planner::prepared_dml::ConfiguredPreparedWrite;
use tidb_planner::read_only_scan::{
    ConfiguredColumnKind, ConfiguredTable, ReadLockWait, ReadOnlyScanPlan, ResolvedProjectionColumn,
};
use tidb_planner::signed_bigint_ranger::SignedBigIntRange;
use tidb_planner::tikv_scan_spec::ScanColumnInfo;
use tidb_planner::txn_mode::SessionTxnMode;
use tidb_tablecodec::decode_table_row_to_map;
use tidb_txnkv::pd_capability::CapabilityTimestampSource;
use tidb_txnkv::rpc::TonicCoprocessorClient;
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    CommitProtocol, LockKeepAlive, LockWaitTime, OptimisticCommitOutcome,
    OptimisticCoordinatorError, OptimisticMutationKind, PessimisticLockFailure,
    RealOptimisticTransaction, RealOptimisticTransactionOpener, RealPessimisticTransaction,
    StorePdCapability, StoreWriteClient, StoreWriteLoader, TransactionMutationBuffer,
    MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::PdRegionLoader;

use crate::pessimistic_lock_error::{
    commit_outcome_to_sql_error, is_retryable_statement_failure, lock_failure_to_sql_error,
    locked_with_conflict_error, transaction_cause_to_sql_error, LockSqlError,
};
use crate::real_tikv_dml::{
    plan_configured_write, ConfiguredWriteError, ConfiguredWritePlan, ConfiguredWriteReport,
    WritePlanningSnapshot,
};

/// A transaction's own uncommitted rows for one read, by clustered handle.
///
/// `None` is a row the transaction deleted; `Some(row)` is its value in the
/// read's own projection. A reader applies these over the snapshot's rows.
pub type StagedRowOverlay = Vec<(i64, Option<Vec<Datum>>)>;

/// How many times one locking statement re-acquires under a fresh
/// `for_update_ts` after a write conflict.
///
/// Go retries a pessimistic statement until `pessimistic-txn.max-retry-count`;
/// this bounded node keeps a small fixed budget so a statement that keeps losing
/// reports 9007 to the client instead of spinning against PD forever.
const MAX_LOCK_RETRIES: usize = 8;

/// The failure of one statement inside an open transaction.
///
/// The distinction is the whole point of the type: [`Self::Statement`] leaves
/// the transaction open, so the connection reports the error and keeps serving
/// the same transaction; [`Self::Transaction`] means the transaction is over.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionStatementError {
    /// The statement failed; the transaction survives and stays usable.
    Statement(LockSqlError),
    /// The transaction itself failed and must be abandoned. The connection
    /// closes it out and returns to autocommit.
    Transaction(LockSqlError),
}

impl TransactionStatementError {
    /// The client-visible error either way.
    #[must_use]
    pub const fn sql_error(&self) -> &LockSqlError {
        match self {
            Self::Statement(error) | Self::Transaction(error) => error,
        }
    }

    /// Whether the transaction stays open after this failure.
    #[must_use]
    pub const fn keeps_transaction_open(&self) -> bool {
        matches!(self, Self::Statement(_))
    }

    /// A failure of the bounded write path itself (a shape this node does not
    /// admit, an overflow, an encoding refusal). It aborts the statement and
    /// nothing else: no mutation was staged and no lock state changed.
    fn write(error: &ConfiguredWriteError) -> Self {
        Self::Statement(LockSqlError {
            code: 1105,
            state: *b"HY000",
            message: error.to_string(),
        })
    }

    /// A refusal this bounded transaction issues itself, before touching TiKV.
    fn refused(message: impl Into<String>) -> Self {
        Self::Statement(LockSqlError {
            code: 1105,
            state: *b"HY000",
            message: message.into(),
        })
    }

    fn from_lock_failure(failure: &PessimisticLockFailure) -> Self {
        let error = lock_failure_to_sql_error(failure);
        if failure.is_statement_scoped() {
            Self::Statement(error)
        } else {
            Self::Transaction(error)
        }
    }
}

fn record_lock_failure(failure: &PessimisticLockFailure) {
    if let PessimisticLockFailure::Deadlock(detail) = failure {
        record_deadlock(detail);
    }
}

impl std::fmt::Display for TransactionStatementError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.sql_error().message)
    }
}

impl std::error::Error for TransactionStatementError {}

/// How one transaction ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionEnd {
    /// `COMMIT` published every buffered mutation, or the transaction had none.
    Committed,
    /// `ROLLBACK` discarded them, releasing anything the transaction held.
    RolledBack,
}

/// The two concrete transactions this node can hold open.
///
/// They are one type here because every statement path treats them alike except
/// where the mode genuinely differs: only the pessimistic one can lock, and only
/// the optimistic one can first learn of a conflict at `COMMIT`.
enum OpenTransaction<C, L, P>
where
    P: StorePdCapability,
{
    Optimistic(Box<RealOptimisticTransaction<C, L, CapabilityTimestampSource<P>>>),
    Pessimistic(Box<RealPessimisticTransaction<C, L, CapabilityTimestampSource<P>>>),
}

impl<C, L, P> OpenTransaction<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn start_ts(&self) -> u64 {
        match self {
            Self::Optimistic(transaction) => transaction.start_ts(),
            Self::Pessimistic(transaction) => transaction.start_ts(),
        }
    }
}

/// Budget for the RPCs that end a transaction, matching client-go's
/// `cleanupMaxBackoff = 20000` — see [`MultiStatementTransaction::transaction_end_call`].
///
/// Every path that ends a transaction shares it, including the served
/// `--cluster-session` one in [`crate::cluster_table_storage`].
pub const TRANSACTION_END_TIMEOUT: Duration = Duration::from_secs(20);

/// One open explicit transaction, owned by exactly one connection.
pub struct MultiStatementTransaction<C = TonicCoprocessorClient, L = PdRegionLoader, P = PdClient>
where
    P: StorePdCapability,
{
    open: OpenTransaction<C, L, P>,
    mode: SessionTxnMode,
    /// The one table this node serves, needed to turn a clustered handle into
    /// the record key the buffer is keyed by.
    table: ConfiguredTable,
    buffer: TransactionMutationBuffer,
    /// Per-statement RPC budget. Stored as a duration, never as an already
    /// minted [`UnaryCallContext`]: that type carries an absolute deadline, so
    /// one minted at `BEGIN` would hand every later statement — and the commit
    /// — whatever was left of the first statement's budget, which for a
    /// transaction a client held open is nothing.
    timeout: Duration,
    /// Refreshes the primary lock's TTL for as long as a pessimistic
    /// transaction holds it; `None` until the first lock is taken.
    keep_alive: Option<LockKeepAlive>,
    /// Row values TiKV returned WITH a pessimistic lock, keyed by encoded key.
    ///
    /// This is Go's `TxnCtx.SetPessimisticLockCache`
    /// (`pkg/executor/point_get.go:620`, filled from `lockCtx.IterateValuesNotLocked`):
    /// a DML whose row it is about to modify asks its PessimisticLock request to carry
    /// the row back (`InitReturnValues`, `pkg/executor/point_get.go:614`), and every later
    /// read of that key answers from this cache instead of storage. The entries live as
    /// long as the locks do — the transaction holds them until COMMIT — so a value answered
    /// here cannot go stale behind the transaction's back; [`Self::read_at_snapshot`] reads
    /// buffer-then-cache-then-snapshot exactly like Go's `PointGetExecutor.get`
    /// (`pkg/executor/point_get.go:656`: memBuffer, lock cache, store).
    lock_values: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    opener: RealOptimisticTransactionOpener<C, L, P>,
}

impl<C, L, P> MultiStatementTransaction<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    /// Opens one explicit transaction in `mode` over the shared authorities.
    ///
    /// The publication budget is the transaction-size limit itself, because a
    /// multi-statement transaction cannot know its mutation set when `BEGIN`
    /// runs — unlike a single-statement write, whose exact bounds are computed
    /// before it spends a timestamp. The commit still enforces the same limits.
    pub fn begin(
        opener: &RealOptimisticTransactionOpener<C, L, P>,
        mode: SessionTxnMode,
        fair_locking: bool,
        commit_protocol: CommitProtocol,
        table: ConfiguredTable,
        timeout: Duration,
    ) -> Result<Self, OptimisticCoordinatorError> {
        let open = match mode {
            SessionTxnMode::Optimistic => {
                let mut transaction =
                    opener.begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)?;
                // `@@tidb_enable_async_commit` / `@@tidb_enable_1pc` reaching the
                // transaction; the commit-time eligibility check still decides.
                transaction.set_commit_protocol(commit_protocol);
                OpenTransaction::Optimistic(Box::new(transaction))
            }
            SessionTxnMode::Pessimistic => {
                let mut transaction = opener.begin_pessimistic(
                    MAX_OPTIMISTIC_MUTATIONS,
                    MAX_OPTIMISTIC_TRANSACTION_BYTES,
                )?;
                // `@@tidb_pessimistic_txn_fair_locking`. Only a pessimistic
                // transaction locks, so only it can lock fairly.
                transaction.set_fair_locking(fair_locking);
                transaction.set_commit_protocol(commit_protocol);
                OpenTransaction::Pessimistic(Box::new(transaction))
            }
        };
        Ok(Self {
            open,
            mode,
            table,
            buffer: TransactionMutationBuffer::new(),
            timeout,
            keep_alive: None,
            lock_values: BTreeMap::new(),
            opener: opener.clone(),
        })
    }

    /// Go `session.checkTxnAborted`: refuses every statement of a transaction
    /// whose keep-alive has given up on the lifetime bound.
    ///
    /// The keep-alive raises `LockExpired` once the transaction outlives
    /// client-go's `MaxTxnTTL` (`2pc.go`: "the pessimistic locks may expire if
    /// the ttl manager has timed out, set `LockExpired` flag so that this
    /// transaction could only commit or rollback with no more statement
    /// executions"). Past that point TiKV may let another transaction resolve
    /// the locks this one believes it holds, so a statement that read through
    /// them would be reading rows it no longer owns -- the lost-update shape.
    /// `COMMIT` and `ROLLBACK` are the two statements Go still admits, and
    /// they do not come through here.
    ///
    /// The failure is statement-scoped, so the transaction stays open for
    /// exactly those two.
    pub fn check_lock_expired(&self) -> Result<(), TransactionStatementError> {
        if self
            .keep_alive
            .as_ref()
            .is_some_and(LockKeepAlive::lock_expired)
        {
            return Err(TransactionStatementError::Statement(LockSqlError {
                code: tidb_error::tidb::errcode::ErrLockExpire,
                state: *b"HY000",
                message: tidb_error::tidb::errname::ErrLockExpire.raw.to_owned(),
            }));
        }
        Ok(())
    }

    /// A fresh RPC budget for one statement, starting now.
    fn statement_call(&self) -> UnaryCallContext {
        UnaryCallContext::with_timeout(self.timeout)
    }

    /// A fresh RPC budget for the commit, rollback and cleanup that end the
    /// transaction, deliberately unrelated to any statement's remaining time.
    ///
    /// This is client-go's rule. `twoPhaseCommitter.cleanup` builds its context
    /// from the store, not from the `ctx` the failing statement handed it:
    ///
    /// ```text
    /// cleanupKeysCtx := context.WithValue(c.store.Ctx(), retry.TxnStartKey, ctx.Value(retry.TxnStartKey))
    /// ...
    /// err = c.cleanupMutations(retry.NewBackofferWithVars(cleanupKeysCtx, cleanupMaxBackoff, c.txn.vars), c.mutations)
    /// ```
    ///
    /// with `cleanupMaxBackoff = 20000`, and `doActionOnGroupMutations` takes
    /// the same decision for the secondary commits:
    ///
    /// ```text
    /// secondaryBo := retry.NewBackofferWithVars(c.store.Ctx(), CommitSecondaryMaxBackoff, c.txn.vars)
    /// ```
    ///
    /// The statement's `ctx` survives only as the log/trace value carried onto
    /// the new context; the deadline comes from the store's lifetime. Ending a
    /// transaction is the store's work, not the last statement's, so a
    /// transaction a client held open for minutes must still be able to publish
    /// or roll back.
    fn transaction_end_call() -> UnaryCallContext {
        UnaryCallContext::with_timeout(TRANSACTION_END_TIMEOUT)
    }

    /// The snapshot every read in this transaction runs at.
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        self.open.start_ts()
    }

    /// The mode this transaction runs in.
    #[must_use]
    pub const fn mode(&self) -> SessionTxnMode {
        self.mode
    }

    /// Whether the transaction has buffered any write yet.
    #[must_use]
    pub fn has_staged_writes(&self) -> bool {
        !self.buffer.is_empty()
    }

    /// The row image this transaction has staged for `handle`.
    ///
    /// `None` means the transaction has not written that row, so a read must
    /// take whatever the snapshot at `start_ts` holds. `Some(None)` means the
    /// transaction deleted it, and `Some(Some(row))` means the transaction's own
    /// replacement row — the read-your-own-writes overlay a reader applies over
    /// the snapshot's rows.
    #[must_use]
    pub fn staged_row(&self, handle: i64) -> Option<Option<&[u8]>> {
        let key = encode_row_key_with_handle(self.table.table_id(), &RecordHandle::Int(handle));
        let staged = self.buffer.staged(&key)?;
        match staged.kind() {
            OptimisticMutationKind::Delete => Some(None),
            OptimisticMutationKind::Insert | OptimisticMutationKind::PutExisting => {
                Some(Some(staged.value()))
            }
            // Index keys are never record keys, and a meta key lives in the `m`
            // namespace, so neither can carry one.
            OptimisticMutationKind::IndexPut
            | OptimisticMutationKind::UniqueIndexInsert
            | OptimisticMutationKind::IndexDelete
            | OptimisticMutationKind::MetaPut
            | OptimisticMutationKind::MetaDelete => None,
            // An `Op_Lock` changes no value, so it overlays nothing. It also
            // cannot reach this buffer: only the commit coordinator adds one,
            // for a pinned primary the buffer never staged.
            OptimisticMutationKind::LockOnly => None,
        }
    }

    /// The transaction's own uncommitted rows that fall inside `ranges`,
    /// decoded into the projection a read is about to return.
    ///
    /// This is the union-scan half of read-your-own-writes: the reader applies
    /// these over the rows the snapshot at `start_ts` produces, so a row this
    /// transaction rewrote comes back with its new value, a row it deleted does
    /// not come back, and a row it inserted comes back even though the snapshot
    /// has never seen it. Go builds the same overlay in `UnionScanExec` from the
    /// `MemBuffer` entries within the scanned key range.
    ///
    /// An entry is `None` when the transaction deleted the row.
    pub fn read_overlay(
        &self,
        projection: &[ResolvedProjectionColumn],
        ranges: &[SignedBigIntRange],
    ) -> Result<StagedRowOverlay, TransactionStatementError> {
        let mut overlay = Vec::new();
        for staged in self.buffer.staged_entries() {
            // Only record keys carry rows; a secondary-index entry changes no
            // row the projection can return.
            let Ok((table_id, RecordHandle::Int(handle))) = decode_record_key(staged.key()) else {
                continue;
            };
            if table_id != self.table.table_id()
                || !ranges
                    .iter()
                    .any(|range| range.start() <= handle && handle <= range.end())
            {
                continue;
            }
            let row = match staged.kind() {
                OptimisticMutationKind::Delete => None,
                OptimisticMutationKind::Insert | OptimisticMutationKind::PutExisting => Some(
                    decode_staged_projection(projection, handle, staged.value())
                        .map_err(|error| TransactionStatementError::write(&error))?,
                ),
                OptimisticMutationKind::IndexPut
                | OptimisticMutationKind::UniqueIndexInsert
                | OptimisticMutationKind::IndexDelete
                | OptimisticMutationKind::MetaPut
                | OptimisticMutationKind::MetaDelete
                // An `Op_Lock` changes no value, so it contributes no row.
                | OptimisticMutationKind::LockOnly => continue,
            };
            overlay.push((handle, row));
        }
        Ok(overlay)
    }

    /// The transaction's staged read overlay for one lowered scan, including
    /// the residual Selection the snapshot reader applies at TiKV.
    ///
    /// Go's `UnionScanExec` evaluates its conditions over both the snapshot
    /// reader and the `MemBuffer` rows it merges in. The real-TiKV reader has
    /// already applied this Selection to the snapshot half, so only staged
    /// rows need the local pass here.
    pub fn read_overlay_for_plan(
        &self,
        plan: &ReadOnlyScanPlan,
    ) -> Result<StagedRowOverlay, TransactionStatementError> {
        let selection = plan.selection();
        let scan_columns = &plan.table_scan().pushdown().columns;
        let mut overlay = Vec::new();
        for staged in self.buffer.staged_entries() {
            let Ok((table_id, RecordHandle::Int(handle))) = decode_record_key(staged.key()) else {
                continue;
            };
            if table_id != self.table.table_id()
                || !plan
                    .handle_ranges()
                    .iter()
                    .any(|range| range.start() <= handle && handle <= range.end())
            {
                continue;
            }
            let row = match staged.kind() {
                OptimisticMutationKind::Delete => None,
                OptimisticMutationKind::Insert | OptimisticMutationKind::PutExisting => {
                    if let Some(selection) = selection {
                        let scan_row = decode_staged_scan_columns(
                            &self.table,
                            scan_columns,
                            handle,
                            staged.value(),
                        )
                        .map_err(|error| TransactionStatementError::write(&error))?;
                        if !selection_matches_staged_row(selection, &scan_row)
                            .map_err(|error| TransactionStatementError::write(&error))?
                        {
                            continue;
                        }
                    }
                    Some(
                        decode_staged_projection(plan.projected_columns(), handle, staged.value())
                            .map_err(|error| TransactionStatementError::write(&error))?,
                    )
                }
                OptimisticMutationKind::IndexPut
                | OptimisticMutationKind::UniqueIndexInsert
                | OptimisticMutationKind::IndexDelete
                | OptimisticMutationKind::MetaPut
                | OptimisticMutationKind::MetaDelete
                | OptimisticMutationKind::LockOnly => continue,
            };
            overlay.push((handle, row));
        }
        Ok(overlay)
    }

    /// Plans one bound write against this transaction and buffers its mutations.
    ///
    /// Nothing reaches TiKV here. The write is planned over the read-your-own-
    /// writes overlay, so a row this transaction already wrote is re-read from
    /// the buffer, and the resulting mutations are staged for the commit.
    ///
    /// In pessimistic mode the rows the statement touches are locked first, as
    /// Go's `UPDATE`/`DELETE` executors do through `SelectLockExec`, so the
    /// conflict is detected now rather than at commit.
    pub fn execute_write(
        &mut self,
        write: &ConfiguredPreparedWrite,
        session_tz: &SessionTimeZone,
    ) -> Result<ConfiguredWriteReport, TransactionStatementError> {
        let plan = if self.mode.is_pessimistic() {
            match write {
                ConfiguredPreparedWrite::ReplaceRows { .. }
                | ConfiguredPreparedWrite::InsertOnDuplicateRows { .. } => {
                    self.plan_pessimistic_conflict_write(write, session_tz)?
                }
                _ => {
                    let handles = written_handles(write);
                    if !handles.is_empty() {
                        // A write blocks for the lock rather than failing fast:
                        // NOWAIT is a locking-read clause, never a DML one.
                        self.lock_handles(&handles, ReadLockWait::Blocking)?;
                    }
                    plan_configured_write(self, write, &self.statement_call(), session_tz)
                        .map_err(|error| TransactionStatementError::write(&error))?
                }
            }
        } else {
            plan_configured_write(self, write, &self.statement_call(), session_tz)
                .map_err(|error| TransactionStatementError::write(&error))?
        };
        match plan {
            ConfiguredWritePlan::Write {
                mutations,
                affected_rows,
            } => {
                for mutation in mutations {
                    self.buffer.stage(mutation).map_err(|error| {
                        TransactionStatementError::refused(format!(
                            "configured write staging: {error}"
                        ))
                    })?;
                }
                Ok(ConfiguredWriteReport {
                    affected_rows,
                    no_write: None,
                    warnings: Vec::new(),
                })
            }
            ConfiguredWritePlan::NoWrite {
                reason,
                affected_rows,
            } => Ok(ConfiguredWriteReport {
                affected_rows,
                no_write: Some(reason),
                warnings: Vec::new(),
            }),
            ConfiguredWritePlan::Ignore {
                mutations,
                affected_rows,
                warnings,
            } => {
                for mutation in mutations {
                    self.buffer.stage(mutation).map_err(|error| {
                        TransactionStatementError::refused(format!(
                            "configured write staging: {error}"
                        ))
                    })?;
                }
                Ok(ConfiguredWriteReport {
                    affected_rows,
                    no_write: None,
                    warnings,
                })
            }
        }
    }

    /// Plans a pessimistic conflict-resolving write until every key it might
    /// change is locked.
    ///
    /// Go runs the replace executor, locks its resulting MemBuffer write set,
    /// then retries the statement after a lock conflict. A primary or unique
    /// conflict is discovered only by reading storage, so locking just the
    /// incoming handle (as point UPDATE does) misses the old record and index
    /// entries that REPLACE deletes. Every re-plan runs at the current
    /// `for_update_ts`; once all emitted mutation keys are held, no concurrent
    /// writer can add another conflicting version to the candidate key set.
    fn plan_pessimistic_conflict_write(
        &mut self,
        write: &ConfiguredPreparedWrite,
        session_tz: &SessionTimeZone,
    ) -> Result<ConfiguredWritePlan, TransactionStatementError> {
        loop {
            let plan = plan_configured_write(self, write, &self.statement_call(), session_tz)
                .map_err(|error| TransactionStatementError::write(&error))?;
            let held = match &self.open {
                OpenTransaction::Pessimistic(transaction) => transaction
                    .locked_keys()
                    .into_iter()
                    .collect::<BTreeSet<_>>(),
                OpenTransaction::Optimistic(_) => unreachable!(
                    "conflict-write planning is called only for a pessimistic transaction"
                ),
            };
            let missing = planned_mutation_keys(&plan)
                .into_iter()
                .filter(|key| !held.contains(key))
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return Ok(plan);
            }
            self.lock_keys(&missing, ReadLockWait::Blocking)?;
        }
    }

    /// Acquires the exclusive row locks a `SELECT ... FOR UPDATE` demands.
    ///
    /// A write conflict here costs only this statement: the locks are retried
    /// under a fresh `for_update_ts`, which is precisely what pessimistic mode
    /// buys. A statement that fails after locking part of its key set releases
    /// exactly the keys it added, so the next statement never blocks on this
    /// transaction's own abandoned locks.
    pub fn lock_handles(
        &mut self,
        handles: &[i64],
        wait: ReadLockWait,
    ) -> Result<(), TransactionStatementError> {
        self.lock_handles_impl(handles, wait, false)
    }

    /// [`Self::lock_handles`], asking TiKV to return each locked row with its
    /// lock and caching what comes back.
    ///
    /// This is Go's point-write fold (`pkg/executor/point_get.go:612-624`,
    /// `PointGetExecutor.getAndLock`): `lockCtx.InitReturnValues(1)` marks the
    /// PessimisticLock request, the response lands in
    /// `TxnCtx.SetPessimisticLockCache`, and the statement's one row read then
    /// answers from that cache — ONE round trip for lock plus read. The planner's
    /// snapshot read sees the cache through [`Self::read_at_snapshot`], so a point
    /// `UPDATE`/`DELETE` no longer pays a separate kv_get before rewriting its row.
    pub fn lock_handles_returning_values(
        &mut self,
        handles: &[i64],
        wait: ReadLockWait,
    ) -> Result<(), TransactionStatementError> {
        self.lock_handles_impl(handles, wait, true)
    }

    fn lock_handles_impl(
        &mut self,
        handles: &[i64],
        wait: ReadLockWait,
        return_values: bool,
    ) -> Result<(), TransactionStatementError> {
        if handles.is_empty() {
            return Ok(());
        }
        if !self.mode.is_pessimistic() {
            return Err(TransactionStatementError::refused(
                "a locking read requires a pessimistic transaction; optimistic transactions \
                 detect conflicts at COMMIT",
            ));
        }
        let keys = handles
            .iter()
            .map(|handle| {
                encode_row_key_with_handle(self.table.table_id(), &RecordHandle::Int(*handle))
            })
            .collect::<Vec<_>>();
        self.lock_keys_with_values(&keys, wait, return_values)
    }

    /// Acquires exclusive pessimistic locks on already-encoded DML keys.
    ///
    /// Point `UPDATE`/`DELETE` use [`Self::lock_handles`], while REPLACE must
    /// also protect unique-index and displaced-row keys discovered from its
    /// own tentative mutation set.
    fn lock_keys(
        &mut self,
        keys: &[Vec<u8>],
        wait: ReadLockWait,
    ) -> Result<(), TransactionStatementError> {
        self.lock_keys_with_values(keys, wait, false)
    }

    fn lock_keys_with_values(
        &mut self,
        keys: &[Vec<u8>],
        wait: ReadLockWait,
        return_values: bool,
    ) -> Result<(), TransactionStatementError> {
        let wait = match wait {
            // Go maps a plain `FOR UPDATE` to `@@innodb_lock_wait_timeout`,
            // not to "wait forever": `AlwaysWait` here could only end at the
            // statement's own control-plane call deadline, five seconds, and
            // it would end as a transport failure that destroys the whole
            // transaction instead of the statement-scoped 1205 MySQL promises.
            ReadLockWait::Blocking => LockWaitTime::session_lock_wait_timeout(),
            ReadLockWait::NoWait => LockWaitTime::NoWait,
            ReadLockWait::Seconds(seconds) => LockWaitTime::Timeout(Duration::from_secs(seconds)),
        };
        let call = self.statement_call();
        let OpenTransaction::Pessimistic(transaction) = &mut self.open else {
            unreachable!("the pessimistic mode check above admits only a pessimistic transaction");
        };
        let held: BTreeSet<Vec<u8>> = transaction.locked_keys().into_iter().collect();
        let mut attempt = 0;
        let acquired = loop {
            // This shared lock path deliberately carries no absence
            // presumption. Locking reads target existing rows, while REPLACE
            // must be allowed to observe and delete a duplicate rather than
            // fail at lock time; ordinary INSERT retains its NotExist
            // assertion at Prewrite.
            //
            // With `return_values`, TiKV answers each locked key's current row
            // IN the PessimisticLock response — Go's `KeyReturningValue`
            // request flag, set from `InitReturnValues` when an executor needs
            // the row it is about to modify (`pkg/executor/point_get.go:614`).
            let retry_reason = match if return_values {
                transaction
                    .acquire_locks_returning_values(keys, &BTreeSet::new(), wait, &call)
            } else {
                transaction.acquire_locks(keys, &BTreeSet::new(), wait, &call)
            } {
                Ok(acquired) => {
                    // Cache whatever rows rode back BEFORE deciding what the
                    // acquisition means: both exits below KEEP these locks (a
                    // clean break, or fair locking's grant-despite-conflict),
                    // so the values stay valid either way. Conflict-granted
                    // keys answer no value — Go recomputes such a statement
                    // from a newer snapshot, and so does this one.
                    self.lock_values.extend(
                        acquired
                            .values
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone())),
                    );
                    if acquired.locked_with_conflict.is_empty() {
                        break acquired;
                    }
                    // Fair locking: TiKV granted the locks despite a newer
                    // committed version. The locks stay — that is the whole point,
                    // the retry needs no second PessimisticLock — but the statement
                    // must be recomputed at a timestamp that can see that version.
                    let (key, conflict_commit_ts) = acquired
                        .locked_with_conflict
                        .iter()
                        .max_by_key(|(_, conflict_ts)| *conflict_ts)
                        .expect("the non-empty branch above admits at least one conflict");
                    locked_with_conflict_error(transaction.start_ts(), *conflict_commit_ts, key)
                }
                Err(failure) => {
                    // Go's lock-context callback records a deadlock as soon as
                    // TiKV reports it, before statement-lock cleanup. Build the
                    // terminal SQL error now so a rollback failure cannot erase
                    // the diagnostic event.
                    record_lock_failure(&failure);
                    let terminal_error = (!is_retryable_statement_failure(&failure))
                        .then(|| TransactionStatementError::from_lock_failure(&failure));
                    // Release only what this statement added; the transaction's
                    // earlier locks must survive its own failed statement.
                    let added = keys
                        .iter()
                        .filter(|key| !held.contains(*key))
                        .cloned()
                        .collect::<Vec<_>>();
                    if let Err(cause) = transaction.pessimistic_rollback(&added, &call) {
                        return Err(TransactionStatementError::Transaction(
                            transaction_cause_to_sql_error(&cause),
                        ));
                    }
                    if let Some(error) = terminal_error {
                        return Err(error);
                    }
                    if matches!(
                        &failure,
                        PessimisticLockFailure::Deadlock(detail) if detail.is_retryable
                    ) {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    lock_failure_to_sql_error(&failure)
                }
            };
            if attempt >= MAX_LOCK_RETRIES {
                return Err(TransactionStatementError::Statement(retry_reason));
            }
            // A newer statement timestamp is what makes the retry see the
            // committed version that beat this one. Go takes it from PD rather
            // than adopting the conflicting commit timestamp, which would let a
            // later commit exceed PD's maximum allocated timestamp.
            transaction
                .advance_for_update_ts()
                .map_err(|error| TransactionStatementError::from_lock_failure(&error))?;
            attempt += 1;
        };
        // The primary lock now has to survive every later statement, so its TTL
        // is refreshed from the moment it exists.
        if self.keep_alive.is_none() {
            let keep_alive = self
                .opener
                .start_lock_keep_alive(acquired.primary_key.clone(), transaction.start_ts())
                .map_err(|error| {
                    TransactionStatementError::Transaction(LockSqlError {
                        code: 1105,
                        state: *b"HY000",
                        message: format!(
                            "cannot keep the transaction's primary lock alive: {error}"
                        ),
                    })
                })?;
            self.keep_alive = Some(keep_alive);
        }
        Ok(())
    }

    /// Publishes every buffered mutation in one two-phase commit.
    ///
    /// A transaction that staged nothing commits trivially — there is nothing to
    /// publish and no commit timestamp to take — after releasing any lock it
    /// took. An optimistic transaction learns here, and only here, that another
    /// transaction beat it to a key: that is the 9007 the client sees.
    pub fn commit(self) -> Result<TransactionEnd, TransactionStatementError> {
        self.finish(true)
    }

    /// Discards every buffered mutation and releases every held lock.
    ///
    /// Nothing buffered was ever published, so a rollback publishes nothing
    /// either; the only physical work is returning the pessimistic locks.
    pub fn rollback(self) -> Result<TransactionEnd, TransactionStatementError> {
        self.finish(false)
    }

    fn finish(mut self, publish: bool) -> Result<TransactionEnd, TransactionStatementError> {
        let call = Self::transaction_end_call();
        let mutations = if publish {
            std::mem::take(&mut self.buffer).into_mutations()
        } else {
            Vec::new()
        };
        // The keep-alive thread must stop before the locks it refreshes are
        // resolved, so a heartbeat can never revive a lock the commit released.
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.close();
        }
        let end = if publish {
            TransactionEnd::Committed
        } else {
            TransactionEnd::RolledBack
        };
        match self.open {
            OpenTransaction::Optimistic(transaction) => {
                if mutations.is_empty() {
                    transaction
                        .finish_without_writes()
                        .map_err(coordinator_error)?;
                    return Ok(end);
                }
                let outcome = transaction
                    .commit(mutations, &call)
                    .map_err(coordinator_error)?;
                classify_commit_outcome(&outcome).map(|()| end)
            }
            OpenTransaction::Pessimistic(mut transaction) => {
                if mutations.is_empty() {
                    let held = transaction.locked_keys();
                    transaction
                        .pessimistic_rollback(&held, &call)
                        .map_err(|cause| {
                            TransactionStatementError::Transaction(transaction_cause_to_sql_error(
                                &cause,
                            ))
                        })?;
                    transaction
                        .into_two_pc()
                        .finish_without_writes()
                        .map_err(coordinator_error)?;
                    return Ok(end);
                }
                let outcome = transaction
                    .commit(mutations, &call)
                    .map_err(coordinator_error)?;
                classify_commit_outcome(&outcome).map(|()| end)
            }
        }
    }
}

/// Keys a tentative write would modify, in source mutation order.
///
/// `RealPessimisticTransaction::acquire_locks` deduplicates and orders them
/// before it reaches TiKV. Keeping this extraction order-preserving makes the
/// write planner and the eventual transaction buffer describe the same set.
fn planned_mutation_keys(plan: &ConfiguredWritePlan) -> Vec<Vec<u8>> {
    match plan {
        ConfiguredWritePlan::Write { mutations, .. } => mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect(),
        ConfiguredWritePlan::NoWrite { .. } => Vec::new(),
        ConfiguredWritePlan::Ignore { mutations, .. } => mutations
            .iter()
            .map(|mutation| mutation.key().to_vec())
            .collect(),
    }
}

/// Read-your-own-writes for the write planner.
///
/// The transaction's own staged entry for `key` wins outright; only an unstaged
/// key falls through to the snapshot at `start_ts`. That is `tikvTxn.Get`'s
/// order, and it is what lets a second `UPDATE` of one row compute its new value
/// from the first one's.
impl<C, L, P> WritePlanningSnapshot for MultiStatementTransaction<C, L, P>
where
    C: StoreWriteClient,
    L: StoreWriteLoader,
    P: StorePdCapability,
{
    fn read_at_snapshot(
        &mut self,
        key: &[u8],
        call: &UnaryCallContext,
    ) -> Result<Option<Vec<u8>>, ConfiguredWriteError> {
        // An `Op_Lock` stages no value, so it must not shadow the snapshot;
        // every other staged kind decides the read outright.
        if let Some(staged) = self
            .buffer
            .staged(key)
            .filter(|staged| staged.kind() != OptimisticMutationKind::LockOnly)
        {
            return Ok(match staged.kind() {
                OptimisticMutationKind::Delete
                | OptimisticMutationKind::IndexDelete
                | OptimisticMutationKind::MetaDelete => None,
                OptimisticMutationKind::Insert
                | OptimisticMutationKind::PutExisting
                | OptimisticMutationKind::IndexPut
                | OptimisticMutationKind::UniqueIndexInsert
                | OptimisticMutationKind::MetaPut => Some(staged.value().to_vec()),
                OptimisticMutationKind::LockOnly => unreachable!("filtered above"),
            });
        }
        // Go `PointGetExecutor.get`'s order (`pkg/executor/point_get.go:656-680`):
        // the transaction's own staged write decides first; then a row TiKV
        // answered WITH a pessimistic lock — the transaction holds that lock, so
        // nobody else can have changed it; only an unstaged, never-locked key
        // falls through to storage. A pessimistic statement's retry reads its
        // current for-update timestamp, not the transaction's original start
        // snapshot: the latter remains the Prewrite start version, but using it
        // here after a lock conflict would recompute the same stale mutation.
        if self.mode.is_pessimistic() {
            if let Some(cached) = self.lock_values.get(key) {
                return Ok(cached.clone());
            }
        }
        let value = match &mut self.open {
            OpenTransaction::Optimistic(transaction) => transaction.snapshot_get(key, call)?.value,
            // A pessimistic statement's retry reads its current for-update
            // timestamp, not the transaction's original start snapshot. The
            // latter remains the Prewrite start version, but using it here
            // after a lock conflict would recompute the same stale mutation.
            OpenTransaction::Pessimistic(transaction) => transaction.for_update_get(key, call)?,
        };
        Ok(value)
    }
}

/// Decodes one staged row into exactly the columns a read projects.
///
/// The clustered primary key is not stored in the row value — it *is* the record
/// key's handle — so it is filled from the handle. Every other projected column
/// decodes through the same `Datum`-based row codec
/// (`tidb_tablecodec::decode_table_row_to_map`) the real-TiKV read path and the
/// write path's own row rewrite both use, so a staged row (one this
/// transaction itself just wrote) admits every type the write path can now
/// produce — including an explicit SQL `NULL` in a nullable column, which
/// decodes to [`Datum::Null`] rather than a decode failure.
fn decode_staged_projection(
    projection: &[ResolvedProjectionColumn],
    handle: i64,
    row: &[u8],
) -> Result<Vec<Datum>, ConfiguredWriteError> {
    let field_types: std::collections::BTreeMap<i64, tidb_datatype::FieldType> = projection
        .iter()
        .filter(|column| column.kind() != ConfiguredColumnKind::ClusteredPrimaryKey)
        .map(|column| {
            (
                column.scan_column().column_id,
                column.scalar_type().chunk_field_type(),
            )
        })
        .collect();
    let decoded = decode_table_row_to_map(row, &field_types, None)
        .map_err(|error| ConfiguredWriteError::RowRead(error.to_string()))?;
    projection
        .iter()
        .map(|column| match column.kind() {
            ConfiguredColumnKind::ClusteredPrimaryKey => Ok(Datum::new_int(handle)),
            _ => decoded
                .get(&column.scan_column().column_id)
                .cloned()
                .ok_or_else(|| {
                    ConfiguredWriteError::RowRead(format!(
                        "configured row is missing column ID {}",
                        column.scan_column().column_id
                    ))
                }),
        })
        .collect()
}

/// Decodes exactly the source columns a residual physical Selection reads.
/// The scan's column order is the Selection's input-offset authority; the
/// ordinary projection can omit all of them.
fn decode_staged_scan_columns(
    table: &ConfiguredTable,
    scan_columns: &[ScanColumnInfo],
    handle: i64,
    row: &[u8],
) -> Result<Vec<Datum>, ConfiguredWriteError> {
    let mut field_types = BTreeMap::new();
    for scan_column in scan_columns {
        let column = table
            .columns()
            .iter()
            .find(|column| column.id() == scan_column.column_id)
            .ok_or_else(|| {
                ConfiguredWriteError::RowRead(format!(
                    "scan requested unknown configured column ID {}",
                    scan_column.column_id
                ))
            })?;
        if column.kind() != ConfiguredColumnKind::ClusteredPrimaryKey {
            field_types.insert(
                scan_column.column_id,
                column.scalar_type().chunk_field_type(),
            );
        }
    }
    let decoded = decode_table_row_to_map(row, &field_types, None)
        .map_err(|error| ConfiguredWriteError::RowRead(error.to_string()))?;
    scan_columns
        .iter()
        .map(|scan_column| {
            let column = table
                .columns()
                .iter()
                .find(|column| column.id() == scan_column.column_id)
                .expect("column identity was checked before staged-row decode");
            match column.kind() {
                ConfiguredColumnKind::ClusteredPrimaryKey => Ok(Datum::new_int(handle)),
                ConfiguredColumnKind::Stored => {
                    decoded.get(&scan_column.column_id).cloned().ok_or_else(|| {
                        ConfiguredWriteError::RowRead(format!(
                            "configured row is missing column ID {}",
                            scan_column.column_id
                        ))
                    })
                }
            }
        })
        .collect()
}

/// Applies the exact lowered residual comparison conjunction to one staged
/// scan row. A NULL comparison is not true and therefore cannot pass a SQL
/// `WHERE`, matching `expression.EvalBool` in Go's UnionScan.
fn selection_matches_staged_row(
    selection: &PhysicalSelectionPlan,
    row: &[Datum],
) -> Result<bool, ConfiguredWriteError> {
    for condition in selection.conditions() {
        let operand = |operand: ComparisonOperand| match operand {
            ComparisonOperand::Int(value) => Ok(Datum::new_int(value)),
            ComparisonOperand::InputOffset(offset) => {
                row.get(offset as usize).cloned().ok_or_else(|| {
                    ConfiguredWriteError::RowRead(format!(
                        "selection reads scan offset {offset}, but staged row has only {} columns",
                        row.len()
                    ))
                })
            }
        };
        let left = operand(condition.lhs())?;
        let right = operand(condition.rhs())?;
        let op = match condition.op() {
            ComparisonOp::Lt => tidb_ast::BinaryOp::Lt,
            ComparisonOp::Le => tidb_ast::BinaryOp::Le,
            ComparisonOp::Gt => tidb_ast::BinaryOp::Gt,
            ComparisonOp::Ge => tidb_ast::BinaryOp::Ge,
            ComparisonOp::Eq => tidb_ast::BinaryOp::Eq,
            ComparisonOp::Ne => tidb_ast::BinaryOp::Ne,
        };
        if !matches!(
            tidb_expr::apply_binary(op, left, right)
                .map_err(|error| ConfiguredWriteError::RowRead(format!("{error:?}")))?,
            Datum::Int(1)
        ) {
            return Ok(false);
        }
    }
    Ok(true)
}

/// The clustered handles one bound write touches, which are the rows a
/// pessimistic transaction locks before it plans the statement.
fn written_handles(write: &ConfiguredPreparedWrite) -> Vec<i64> {
    match write {
        ConfiguredPreparedWrite::InsertRows { .. }
        | ConfiguredPreparedWrite::InsertIgnoreRows { .. }
        | ConfiguredPreparedWrite::ReplaceRows { .. }
        | ConfiguredPreparedWrite::InsertOnDuplicateRows { .. } => {
            // An INSERT's row is new, so there is no existing row to lock; TiKV
            // enforces its absence through the Insert operation's assertion.
            Vec::new()
        }
        ConfiguredPreparedWrite::UpdatePoint { handle, .. }
        | ConfiguredPreparedWrite::DeletePoint { handle, .. } => vec![*handle],
    }
}

/// Maps a terminal commit outcome onto this transaction's error shape.
///
/// The classification itself is shared with every other commit path in
/// [`commit_outcome_to_sql_error`]; what is local here is that a failed commit
/// ends the transaction rather than one statement.
fn classify_commit_outcome(
    outcome: &OptimisticCommitOutcome,
) -> Result<(), TransactionStatementError> {
    commit_outcome_to_sql_error(outcome).map_err(TransactionStatementError::Transaction)
}

fn coordinator_error(error: OptimisticCoordinatorError) -> TransactionStatementError {
    TransactionStatementError::Transaction(LockSqlError {
        code: 1105,
        state: *b"HY000",
        message: format!("[kv:1105]transaction failed: {error}"),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::{
        classify_commit_outcome, planned_mutation_keys, record_lock_failure,
        selection_matches_staged_row, written_handles, ConfiguredWritePlan, Duration,
        MultiStatementTransaction, TransactionStatementError, UnaryCallContext,
        TRANSACTION_END_TIMEOUT,
    };
    use crate::pessimistic_lock_error::{transaction_cause_to_sql_error, ERR_WRITE_CONFLICT};
    use tidb_planner::physical_selection::{
        BigIntComparison, ComparisonOp, ComparisonOperand, PhysicalSelectionPlan,
    };
    use tidb_planner::prepared_dml::{
        ConfiguredAssignment, ConfiguredPreparedWrite, PreparedBindValue,
    };
    use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
    use tidb_txnkv::transaction::{
        DeadlockDetail, DeadlockWaitChainItem, OptimisticCommitOutcome, OptimisticMutation,
        OptimisticTransactionReceipt, PessimisticLockFailure, RolledBackTransaction,
        TransactionCause,
    };

    use tidb_datatype::Datum;
    use tidb_executor::deadlock_history::{
        configure_global_deadlock_history, global_deadlock_history,
    };

    static DEADLOCK_HISTORY_TEST: Mutex<()> = Mutex::new(());

    struct ResetDeadlockHistory;

    impl Drop for ResetDeadlockHistory {
        fn drop(&mut self) {
            global_deadlock_history().clear();
            configure_global_deadlock_history(0, false);
        }
    }

    fn table() -> ConfiguredTable {
        ConfiguredTable::new(
            "campaign",
            "accounts",
            42,
            [
                ConfiguredColumn::clustered_primary_key("id", 1),
                ConfiguredColumn::stored_not_null("balance", 2),
            ],
        )
    }

    fn rolled_back(cause: TransactionCause) -> OptimisticCommitOutcome {
        OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
            cause,
        })
    }

    #[test]
    fn an_optimistic_commit_that_lost_the_race_reports_9007_and_ends_the_transaction() {
        // The asymmetry this whole type exists for: an optimistic transaction
        // cannot learn of the conflict before COMMIT, and the conflict costs the
        // transaction rather than one statement.
        let outcome = rolled_back(TransactionCause::WriteConflict {
            detail: "conflictStartTS=7".to_owned(),
        });
        let error = classify_commit_outcome(&outcome).expect_err("a lost race cannot report OK");
        assert_eq!(error.sql_error().code, ERR_WRITE_CONFLICT);
        assert!(error.sql_error().message.contains("conflictStartTS=7"));
        assert!(
            !error.keeps_transaction_open(),
            "a failed commit ends the transaction"
        );
    }

    #[test]
    fn a_successful_commit_is_the_only_outcome_that_answers_ok() {
        for (cause, code) in [
            (
                TransactionCause::Region {
                    detail: "epoch not match".to_owned(),
                },
                1105,
            ),
            (
                TransactionCause::Transport {
                    detail: "connection reset".to_owned(),
                },
                1105,
            ),
        ] {
            let error = classify_commit_outcome(&rolled_back(cause))
                .expect_err("a non-commit never reports durable rows");
            assert_eq!(
                error.sql_error().code,
                code,
                "untyped region and transport failures stay generic"
            );
            assert!(!error.keeps_transaction_open());
        }
        assert!(classify_commit_outcome(&OptimisticCommitOutcome::Committed(
            tidb_txnkv::transaction::CommittedTransaction {
                receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
                secondary_failures: Vec::new(),
            }
        ))
        .is_ok());
    }

    #[test]
    fn a_write_conflict_is_the_only_cause_with_a_code_of_its_own() {
        assert_eq!(
            transaction_cause_to_sql_error(&TransactionCause::WriteConflict {
                detail: "d".to_owned()
            })
            .code,
            ERR_WRITE_CONFLICT
        );
        let other = transaction_cause_to_sql_error(&TransactionCause::AlreadyExists {
            key: b"k".to_vec(),
            detail: "duplicate".to_owned(),
        });
        assert_eq!(other.code, 1105);
        assert!(
            other.message.contains("duplicate"),
            "the exact cause survives instead of being flattened"
        );
    }

    #[test]
    fn only_statement_scoped_failures_keep_the_transaction_open() {
        let statement = TransactionStatementError::refused("bounded refusal");
        assert!(statement.keeps_transaction_open());
        let ended = TransactionStatementError::Transaction(statement.sql_error().clone());
        assert!(!ended.keeps_transaction_open());
    }

    #[test]
    fn a_live_deadlock_failure_is_recorded_before_it_reaches_sql() {
        let _serial = DEADLOCK_HISTORY_TEST.lock().unwrap();
        let _reset = ResetDeadlockHistory;
        configure_global_deadlock_history(2, false);
        global_deadlock_history().clear();
        let failure = PessimisticLockFailure::Deadlock(DeadlockDetail {
            lock_ts: 7,
            lock_key: b"blocked".to_vec(),
            deadlock_key_hash: 9,
            deadlock_key: b"held".to_vec(),
            is_retryable: false,
            wait_chain: vec![DeadlockWaitChainItem {
                txn: 7,
                wait_for_txn: 8,
                key: b"row-key".to_vec(),
                resource_group_tag: Vec::new(),
            }],
        });

        record_lock_failure(&failure);
        let error = TransactionStatementError::from_lock_failure(&failure);
        assert_eq!(error.sql_error().code, 1213);
        let records = global_deadlock_history().get_all();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, 1);
        assert_eq!(records[0].wait_chain[0].try_lock_txn, 7);
        assert_eq!(records[0].wait_chain[0].txn_holding_lock, 8);
        assert_eq!(records[0].wait_chain[0].key, b"row-key");
    }

    #[test]
    fn retryable_deadlock_history_obeys_the_process_policy() {
        let _serial = DEADLOCK_HISTORY_TEST.lock().unwrap();
        let _reset = ResetDeadlockHistory;
        let failure = PessimisticLockFailure::Deadlock(DeadlockDetail {
            lock_ts: 7,
            lock_key: b"blocked".to_vec(),
            deadlock_key_hash: 9,
            deadlock_key: b"held".to_vec(),
            is_retryable: true,
            wait_chain: Vec::new(),
        });

        configure_global_deadlock_history(2, false);
        record_lock_failure(&failure);
        assert!(global_deadlock_history().get_all().is_empty());

        configure_global_deadlock_history(2, true);
        record_lock_failure(&failure);
        let records = global_deadlock_history().get_all();
        assert_eq!(records.len(), 1);
        assert!(records[0].is_retryable);
    }

    #[test]
    fn a_pessimistic_statement_locks_the_rows_it_rewrites_and_not_the_rows_it_creates() {
        // An UPDATE/DELETE must lock the existing row before planning it; an
        // INSERT has no existing row, and TiKV asserts its absence at Prewrite.
        let table = table();
        assert_eq!(
            written_handles(&ConfiguredPreparedWrite::UpdatePoint {
                table: table.clone(),
                handle: 9,
                column_index: 1,
                assignment: ConfiguredAssignment::Set(PreparedBindValue::Int(4)),
            }),
            vec![9]
        );
        assert_eq!(
            written_handles(&ConfiguredPreparedWrite::DeletePoint {
                table: table.clone(),
                handle: -3,
            }),
            vec![-3]
        );
        assert!(written_handles(&ConfiguredPreparedWrite::InsertRows {
            table,
            rows: Vec::new(),
        })
        .is_empty());
    }

    #[test]
    fn a_replace_lock_set_covers_every_tentative_record_and_index_mutation() {
        // REPLACE can delete a row found through a unique key, then delete its
        // old index entry and add the incoming record/index entries. The
        // pessimistic loop must lock all of them, including the displaced row
        // key that was not knowable from the incoming handle alone.
        let plan = ConfiguredWritePlan::Write {
            mutations: vec![
                OptimisticMutation::delete(b"record/old".to_vec()).unwrap(),
                OptimisticMutation::index_delete(b"index/old".to_vec()).unwrap(),
                OptimisticMutation::insert(b"record/new".to_vec(), b"row".to_vec()).unwrap(),
                OptimisticMutation::unique_index_insert(b"index/new".to_vec(), b"handle".to_vec())
                    .unwrap(),
            ],
            affected_rows: 2,
        };
        assert_eq!(
            planned_mutation_keys(&plan),
            vec![
                b"record/old".to_vec(),
                b"index/old".to_vec(),
                b"record/new".to_vec(),
                b"index/new".to_vec(),
            ]
        );
    }

    #[test]
    fn the_call_that_ends_a_transaction_does_not_spend_the_statement_budget() {
        // Regression for the held-transaction commit failure: a `BEGIN` used to
        // mint one `UnaryCallContext` and every later RPC inherited its absolute
        // deadline, so a transaction held for longer than one statement's budget
        // reached Prewrite with `timed out after 0ms`. Both budgets must be
        // minted where they are spent, and the transaction-end budget comes from
        // the store's lifetime rather than any statement's.
        let statement = Duration::from_millis(50);
        let opened = UnaryCallContext::with_timeout(statement);
        std::thread::sleep(Duration::from_millis(120));
        assert!(
            opened.timeout().is_zero(),
            "a context minted at BEGIN is exhausted once the transaction is held"
        );
        assert!(
            UnaryCallContext::with_timeout(statement).timeout() > Duration::ZERO,
            "a statement must start its own budget"
        );
        assert_eq!(TRANSACTION_END_TIMEOUT, Duration::from_secs(20));
        assert!(
            <MultiStatementTransaction>::transaction_end_call().timeout() > Duration::from_secs(19),
            "commit, rollback and cleanup take the store's budget, not the statement's"
        );
    }

    #[test]
    fn a_staged_row_uses_the_snapshot_selections_sql_comparison_semantics() {
        let selection = PhysicalSelectionPlan::from_bigint_conditions(vec![BigIntComparison::new(
            ComparisonOp::Gt,
            ComparisonOperand::InputOffset(1),
            ComparisonOperand::Int(10),
        )
        .unwrap()])
        .unwrap();

        assert!(
            selection_matches_staged_row(&selection, &[Datum::Int(7), Datum::Int(11)]).unwrap()
        );
        assert!(
            !selection_matches_staged_row(&selection, &[Datum::Int(7), Datum::Int(10)]).unwrap()
        );
        assert!(
            !selection_matches_staged_row(&selection, &[Datum::Int(7), Datum::Null]).unwrap(),
            "a NULL WHERE comparison is not true"
        );
    }
}
