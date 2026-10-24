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

//! The transaction seam: one fresh read snapshot per autocommit statement,
//! one publication of its staged writes, and the single transaction an
//! explicit `BEGIN` holds open. Split out of `cluster_session_node` because
//! it is one of the independent seams that accreted there; see that module's
//! doc comment for the statement lifecycle this seam is exercised by.
//!
//! # The pessimistic explicit transaction
//!
//! `BEGIN` under Go's default `tidb_txn_mode = 'pessimistic'` (and `BEGIN
//! PESSIMISTIC` outright) opens [`SessionTransaction::begin_pessimistic`]:
//! after each DML statement the session locks the statement's staged keys
//! (`OpenClusterTransaction::lock_staged_keys`, Go `handlePessimisticDML`'s
//! lock step over `KeysNeedToLock`), a conflict rolls the STATEMENT back and
//! re-executes it reading at the advanced `for_update_ts`
//! ([`OpenClusterTransaction::snapshot_at`]), and `COMMIT` prewrites with
//! pessimistic constraints on the held locks. `BEGIN OPTIMISTIC` keeps the
//! old contract -- no locks, the loser's `COMMIT` reports 9007 -- exactly as
//! Go keeps it. Both are pinned by
//! `tests::unistore_cop::racing_pessimistic_updates_both_commit_with_serial_effect`
//! and its optimistic sibling; sysbench's parallel `oltp_read_write` was the
//! workload that demanded the wiring.
//!
//! # Locking reads
//!
//! Go's `SelectLockExec` sits above the reader and pessimistically locks each
//! produced row's key at the statement's `for_update_ts`, so a second
//! transaction's `FOR UPDATE` WAITS and then reads the committed value. It
//! does so inside `Next`, in the call that finds its child drained, which for
//! a single-table UPDATE or DELETE comes after the last chunk's writes
//! (`buildUpdate` / `buildDelete` plan the lock over the source). Here
//! `SelectLockExec` takes the same step through
//! [`ClusterSnapshot::do_lock_keys`]. A locking point plan (`PointGet.Lock`)
//! locks in its own `Next` through `lock_keys_for_read`, before the read under
//! repeatable read, with the rows' values. Either lock's failure is kept by the
//! statement's snapshot ([`StatementLockFailure`]) and the statement owner acts
//! on it as `handlePessimisticLockError` does: a conflict re-runs the statement
//! at the advanced `for_update_ts`. What the plan did not lock is locked by
//! the owner after the statement, as `handlePessimisticDML` does: the keys it
//! wrote and the matched rows an UPDATE left unchanged, in one request.
//! Ordinary reads keep their transaction snapshot and cannot use the
//! lock-value cache. Read-committed point reads lock only what exists.
//!
//! `LOCK IN SHARE MODE` is Go's own documented no-op and stays one.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tidb_txnkv::transaction::StoreClient;

use tidb_exec::cluster_table_storage::{
    commit_staged_buffer, lock_pessimistic_statement_with, mutation_buffer_from_mutations,
    overlay_staged_mutations, stage_mutations, LockKeysOutcome, MaxTsSnapshot,
    PreparedStatementSnapshot, SessionTransaction, StatementSnapshot,
};
use tidb_exec::pessimistic_lock_error::LockSqlError;
use tidb_exec::real_tikv_read::RealOptimisticTransactionOpener;
use tidb_exec::session_commit_protocol::SessionTransactionOptions;
use tidb_executor::advisory_lock_state::{
    AdvisoryLockError, AdvisoryLockLease, AdvisoryLockService,
};
use tidb_executor::cluster_storage::{
    ClusterSnapshot, DuplicateKeyHint, MutationBuffer, SnapshotPairs,
};
use tidb_executor::storage::StorageError;
use tidb_txnkv::driver::transaction_error::{RedactArg, Redaction};
use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::SchemaLeaseChecker;
use tidb_txnkv::transaction::{LockWaitTime, PessimisticLockFailure, RealTransaction};
use tidb_txnkv::Key;

use tidb_exec::sql_query_error::SqlQueryError;

/// Carries a commit's own client-visible triple onto the wire, so a 9007 stays
/// a 9007 instead of collapsing into the generic 1105.
pub fn sql_error(error: LockSqlError) -> SqlQueryError {
    SqlQueryError {
        write_conflict: error.write_conflict,
        identity: error.identity,
        ..SqlQueryError::new(error.code, error.state, error.message)
    }
}

/// Go `session.tryReplaceWriteConflictError` (`pkg/session/session.go:913`):
/// "adds more information about the table in the error message" of a
/// commit's write conflict, naming the table of the conflicting key
/// (`args[3]`) and of the primary key (`args[5]`) through the transaction's
/// infoschema. `table_by_id` is Go's `is.TableByID` followed by
/// `infoschema.SchemaByTable`: `None` for no table, otherwise the schema name
/// when the table has one, and the table name.
///
/// `redaction` is the process mode (`errors.RedactLogEnabled`). Go skips the
/// rewrite when it is `ON`. Under `MARKER` the key arguments are no longer
/// strings, so no table is named, but the new error still redacts its
/// arguments again, as Go's `FastGenByArgs` does.
pub fn try_replace_write_conflict_error(
    mut error: SqlQueryError,
    redaction: Redaction,
    table_by_id: impl Fn(i64) -> Option<(Option<String>, String)>,
) -> SqlQueryError {
    let Some(args) = error.write_conflict.take() else {
        return error;
    };
    if redaction == Redaction::Enabled {
        error.write_conflict = Some(args);
        return error;
    }
    let mut args = *args;
    if let Some(field) = add_table_name_in_table_id_field(&args.key_table_id, &table_by_id) {
        args.key_table_id = RedactArg::Str(field);
    }
    if let Some(field) = add_table_name_in_table_id_field(&args.primary_table_id, &table_by_id) {
        args.primary_table_id = RedactArg::Str(field);
    }
    let args = args.fast_gen_by_args(redaction);
    error.message = args.message();
    error.write_conflict = Some(Box::new(args));
    error
}

/// Go `addTableNameInTableIDField` (`pkg/session/session.go:944`): appends
/// `, tableName=<schema>.<table>` to a `{tableID=N` field, with Go's
/// `unknown` and `unknown.<table>` fallbacks. A field that is not a string
/// (a redaction marker) or does not end in a table ID is left alone.
fn add_table_name_in_table_id_field(
    table_id_field: &RedactArg,
    table_by_id: &impl Fn(i64) -> Option<(Option<String>, String)>,
) -> Option<String> {
    let RedactArg::Str(key_table_id) = table_id_field else {
        return None;
    };
    let table_id_str = key_table_id.split('=').next_back()?;
    let table_id = table_id_str.parse::<i64>().ok()?;
    let table_name = match table_by_id(table_id) {
        None => "unknown".to_owned(),
        Some((None, table)) => format!("unknown.{table}"),
        Some((Some(schema), table)) => format!("{schema}.{table}"),
    };
    Some(format!("{key_table_id}, tableName={table_name}"))
}

/// The commit callback's `TxnInfo` (client-go `txn.go`'s `CommitHook`
/// argument): the timestamps and protocol of a transaction that committed,
/// which Go records as `@@tidb_last_txn_info`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommittedTxnInfo {
    /// The transaction's start timestamp.
    pub start_ts: u64,
    /// The timestamp it committed at.
    pub commit_ts: u64,
    /// client-go's `txn_commit_mode`: `2pc`, `async_commit` or `1pc`.
    pub commit_mode: &'static str,
}

impl CommittedTxnInfo {
    /// The info of a COMMITTED outcome; any other outcome committed nothing.
    #[must_use]
    pub fn from_outcome(
        outcome: &tidb_txnkv::transaction::OptimisticCommitOutcome,
    ) -> Option<Self> {
        let tidb_txnkv::transaction::OptimisticCommitOutcome::Committed(committed) = outcome else {
            return None;
        };
        let receipt = &committed.receipt;
        Some(Self {
            start_ts: receipt.start_ts,
            commit_ts: receipt.commit_ts,
            commit_mode: match receipt.commit_protocol {
                tidb_txnkv::transaction::CommittedProtocol::TwoPhase => "2pc",
                tidb_txnkv::transaction::CommittedProtocol::AsyncCommit => "async_commit",
                tidb_txnkv::transaction::CommittedProtocol::OnePc => "1pc",
            },
        })
    }
}

/// Everything a connection needs from the cluster's transaction tier: one
/// fresh read snapshot per autocommit statement, one publication of its staged
/// writes, and the single transaction an explicit `BEGIN` holds open.
///
/// The seam exists so the statement lifecycle -- which is the correctness core
/// of this mode -- is exercised without a cluster. The production
/// implementation is [`RealClusterTransactions`]; the tests drive the same
/// lifecycle against an in-memory committed store.
pub trait ClusterTransactions: Send + Sync {
    /// A fresh TSO from the store's oracle (Go `GetOracle().GetTimestamp`):
    /// what `NewTxnInStmt` takes for the session transaction a DDL, account or
    /// ANALYZE statement runs under.
    fn fresh_timestamp(&self) -> Result<u64, String> {
        Err("a fresh timestamp is unavailable from this storage owner".into())
    }
    fn external_timestamp(&self, _value: Option<u64>) -> Result<u64, String> {
        Err("external timestamp is unavailable from this storage owner".into())
    }
    /// PD region count and approximate storage-key count for one physical
    /// table's record range. Embedded stores have no PD HTTP authority.
    fn record_region_stats(
        &self,
        _table_id: i64,
    ) -> Result<Option<tidb_exec::pd_approximate_count::RegionCountStats>, String> {
        Ok(None)
    }

    /// Reads current pessimistic lock waits from every store. A store-local
    /// RPC failure is skipped, matching Go `tikvStore.GetLockWaits`.
    fn lock_waits(&self) -> Result<Vec<tidb_proto::KvrpcWaitForEntry>, String> {
        Ok(Vec::new())
    }

    /// Starts preparing one ordinary autocommit snapshot without waiting for
    /// its timestamp. The first read consumes the returned future.
    fn prepare_snapshot(
        &self,
        resource_group: &str,
    ) -> Result<Box<dyn PendingClusterSnapshot>, String>;

    /// Synchronously opens one autocommit statement's read snapshot at its own
    /// timestamp. This is the fail-closed fallback when no prepared future was
    /// installed; ordinary statements use [`Self::prepare_snapshot`].
    fn open_snapshot(&self, resource_group: &str) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// The caller needs both schema and data at this historical timestamp.
    fn open_snapshot_at(
        &self,
        _read_ts: u64,
        _resource_group: &str,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        Err("historical snapshots are unavailable from this storage owner".into())
    }

    /// Opens one autocommit statement's read snapshot at `u64::MAX` -- the
    /// latest committed version -- spending no PD timestamp.
    ///
    /// Reached only from a statement that DECLARED its whole read is one point
    /// get on the clustered handle; see
    /// [`ClusterSnapshot::declare_autocommit_point_get`].
    fn open_max_ts_snapshot(
        &self,
        resource_group: &str,
    ) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Opens the writable transaction owned by one autocommit UPDATE/DELETE.
    ///
    /// The statement reads and publishes through this same transaction.  The
    /// open is started before DML planning so its timestamp request can
    /// overlap the CPU work (Go's `txnFuture`), while the transaction itself
    /// remains statement owned and is handed back to the session at the
    /// statement boundary. `pessimistic` is Go `decideTxnMode`'s answer for
    /// this attempt: an autocommit DML runs optimistically unless it is the
    /// retry of a write conflict (or the non-default `pessimistic-auto-commit`
    /// config is on).
    /// `fair_locking` is Go `OnPessimisticStmtStart`'s gate
    /// (`pkg/sessiontxn/isolation/base.go:711`): the session's
    /// `@@tidb_pessimistic_txn_fair_locking`, false for an internal
    /// statement.
    fn prepare_autocommit_write(
        &self,
        resource_group: &str,
        pessimistic: bool,
        fair_locking: bool,
        options: SessionTransactionOptions,
    ) -> Result<Box<dyn PendingClusterTransaction>, String>;

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction **at `read_ts`**, then empties the buffer. An empty buffer
    /// publishes nothing.
    ///
    /// `read_ts` is the timestamp the statement's own reads were served at, or
    /// `None` if the statement read no cluster row and therefore has none. It
    /// is not an optimisation: publishing a value computed from a read at `T`
    /// under any timestamp later than `T` puts every commit in between outside
    /// TiKV's conflict check, which is silent lost-update. See
    /// [`StatementReadTs`].
    ///
    /// The error is the client-visible one, because a publication TiKV refused
    /// has a code of its own: a lost race is 9007, not a generic failure.
    fn commit(
        &self,
        buffer: &MutationBuffer,
        read_ts: Option<u64>,
        resource_group: &str,
        options: SessionTransactionOptions,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError>;

    /// [`Self::commit`] with the session's schema lease checker bound to the
    /// publication (Go `SetOptionsBeforeCommit`); a tier with no validator
    /// publishes unchecked.
    fn commit_with_schema_lease(
        &self,
        buffer: &MutationBuffer,
        read_ts: Option<u64>,
        resource_group: &str,
        _schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
        options: SessionTransactionOptions,
        observer: Option<crate::process::ProcessTransactionObserver>,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError> {
        if let (Some(observer), Some(ts)) = (observer, read_ts) {
            observer.activated(ts);
        }
        self.commit(buffer, read_ts, resource_group, options)
    }

    /// Publishes one restricted-session write plan at the snapshot timestamp
    /// that produced it.
    fn commit_optimistic_mutations(
        &self,
        mutations: Vec<tidb_txnkv::transaction::BufferMutation>,
        read_ts: u64,
        resource_group: &str,
        options: SessionTransactionOptions,
    ) -> Result<(), SqlQueryError> {
        let buffer = mutation_buffer_from_mutations(mutations).map_err(|error| match error {
            StorageError::Sql(error) => SqlQueryError::new(error.code, error.state, error.message)
                .with_identity(error.identity),
            error => SqlQueryError::unknown(error.to_string()),
        })?;
        self.commit(&buffer, Some(read_ts), resource_group, options)
            .map(|_| ())
    }

    /// Opens the one transaction an explicit `BEGIN` holds until `COMMIT` or
    /// `ROLLBACK`. `pessimistic` is the session's `tidb_txn_mode` verdict at
    /// `BEGIN` -- Go's default is pessimistic (`DefTiDBTxnMode`), and the
    /// pessimistic transaction locks per statement instead of first learning
    /// of a conflict at `COMMIT`.
    fn begin(
        &self,
        pessimistic: bool,
        fair_locking: bool,
        resource_group: &str,
        options: SessionTransactionOptions,
    ) -> Result<Box<dyn OpenClusterTransaction>, String>;

    /// Acquires the TiKV pessimistic key backing one advisory lock.
    fn acquire_advisory_lock(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError>;

    /// Checks the same physical key without retaining it.
    fn is_advisory_lock_used(&self, name: &str) -> bool;
}

/// Adapts the cluster transaction authority to the expression/session lock
/// service without creating a second lock namespace.
pub struct ClusterAdvisoryLockService {
    transactions: Arc<dyn ClusterTransactions>,
}

impl ClusterAdvisoryLockService {
    #[must_use]
    pub fn new(transactions: Arc<dyn ClusterTransactions>) -> Self {
        Self { transactions }
    }
}

impl AdvisoryLockService for ClusterAdvisoryLockService {
    fn acquire(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError> {
        self.transactions.acquire_advisory_lock(name, timeout)
    }

    fn is_used(&self, name: &str) -> bool {
        self.transactions.is_advisory_lock_used(name)
    }
}

/// One ordinary autocommit snapshot whose timestamp request is in flight.
pub trait PendingClusterSnapshot: Send {
    /// Waits for preparation and returns the snapshot every statement read
    /// will share.
    fn wait(self: Box<Self>) -> Result<Box<dyn ClusterSnapshot>, String>;
}

/// An autocommit write's transaction whose timestamp request is in flight.
///
/// Go keeps the `oracle.Future` from `AdviseWarmup` and only waits for it
/// when the statement first needs its transaction; this is that future for a
/// statement that will publish. Waiting opens the transaction on the calling
/// thread (`begin_at` is local state over the shared transport), so no thread
/// is spawned per statement to do it.
pub trait PendingClusterTransaction: Send {
    /// Waits for the timestamp and opens the statement's own transaction.
    fn wait(self: Box<Self>) -> Result<Box<dyn OpenClusterTransaction>, String>;
}

/// The transaction an explicit `BEGIN` holds open across its statements.
///
/// Every statement of the transaction reads through [`Self::snapshot`], so they
/// all share the timestamp `BEGIN` took, and [`Self::commit`] prewrites at that
/// same timestamp -- which is what makes a racing writer a write conflict
/// instead of a silent overwrite.
pub trait OpenClusterTransaction: Send {
    /// Production stores expose the client MemDB to executor staging.
    fn bind_mutation_buffer(&self, _buffer: &MutationBuffer) {}

    /// The transaction timestamp shared by every statement until it ends.
    fn start_ts(&self) -> u64;

    /// Go `baseTxnContextProvider.ActivateTxn`: waits the transaction's
    /// timestamp and makes it valid. An eagerly opened transaction is
    /// active already; a pending one (`lazy_transaction`) opens here.
    fn activate(&self) -> Result<(), String> {
        Ok(())
    }

    /// Go `TxnCtx.StartTS` as `setLastTxnInfoBeforeTxnEnd` reads it: the
    /// start timestamp, or `None` for a pending transaction that no
    /// statement activated. Asking never activates.
    fn start_ts_if_active(&self) -> Option<u64> {
        Some(self.start_ts())
    }

    /// Go `SetOptionsOnTxnActive`: the session values a pending transaction
    /// opens with are the ones in force when it activates, so the session
    /// refreshes them at every statement until then.
    fn set_activation_options(&self, _options: SessionTransactionOptions) {}

    /// The wall clock this transaction began at (Go's
    /// `tidb_session_transaction_duration_seconds` start point).
    fn opened_at(&self) -> std::time::Instant;

    /// Statements executed inside this transaction (Go's
    /// `tidb_session_transaction_statement_num` observation input).
    fn statement_count(&self) -> u64;

    /// Counts one statement against this transaction (Go's
    /// `TxnCtx.StatementCount`).
    fn note_statement(&self);

    /// Rebinds all subsequent requests to the statement's resolved resource
    /// group while retaining the transaction and its timestamp.
    fn set_resource_group_name(&self, name: &str) -> Result<(), String>;

    /// Refreshes the current statement's pessimistic existence-check setting.
    fn set_statement_assertion_level(
        &self,
        level: tidb_proto::KvrpcAssertionLevel,
    ) -> Result<(), String>;

    /// Go `SetOptionsBeforeCommit`'s `kv.SchemaChecker` option
    /// (`base.go:606-615`): the session's schema lease checker, asked at
    /// commit whether the schema this transaction was planned against is
    /// still in force. A tier with no validator commits unchecked, as Go
    /// does without the option.
    fn set_schema_lease_checker(&mut self, _checker: Arc<dyn SchemaLeaseChecker>) {}

    /// One statement's read handle. Dropping it ends the statement, never the
    /// transaction.
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// [`Self::snapshot`] told whether the statement it serves takes LOCKS --
    /// Go's `e.lock`, the gate on the pessimistic lock cache
    /// (`pkg/executor/point_get.go:677`). Defaults to the non-locking
    /// snapshot so a tier that holds no lock cache is unaffected.
    fn snapshot_for(&self, _locking: bool) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.snapshot()
    }

    /// Publishes the staged writes at the transaction's own start timestamp and
    /// empties the buffer.
    ///
    /// The error is the client-visible one: a transaction whose prewrite lost
    /// the race against a newer commit reports 9007, as Go's does.
    fn commit(
        self: Box<Self>,
        buffer: &MutationBuffer,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError>;

    /// Ends the transaction without publishing anything.
    fn rollback(self: Box<Self>) -> Result<(), String>;

    /// Whether this transaction locks per statement (Go's pessimistic mode).
    /// The default is the optimistic answer, which every mock and the
    /// optimistic transaction share.
    fn is_pessimistic(&self) -> bool {
        false
    }

    /// Go `TransactionContext.forUpdateTS`: the pessimistic transaction's
    /// current for-update timestamp. Zero (the optimistic answer) defers to
    /// the start timestamp in `GetForUpdateTS`.
    fn for_update_ts(&self) -> u64 {
        0
    }

    /// Starts a current-read statement without changing the transaction's
    /// repeatable-read snapshot.
    fn fresh_locking_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        Err("only a pessimistic transaction opens a current locking snapshot".to_owned())
    }

    /// A statement handle at a fresh statement timestamp, which also moves
    /// the for-update timestamp: Go's pessimistic READ-COMMITTED provider
    /// (`getStmtTS`) gives every statement its own.
    fn fresh_statement_snapshot(&self, _locking: bool) -> Result<Box<dyn ClusterSnapshot>, String> {
        Err("only a pessimistic transaction reads at a statement timestamp".to_owned())
    }

    /// A pessimistic statement handle whose for-update timestamp its first
    /// act decides (Go `optimizeForNotFetchingLatestTS`): a locking point
    /// read keeps the current one, any other read advances it.
    fn deferred_statement_snapshot(
        &self,
        _locking: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        Err("only a pessimistic transaction reads at a statement timestamp".to_owned())
    }

    /// The outcome of a lock the running statement took during execution
    /// (Go `PointGetExecutor.getAndLock`) when the statement failed on it.
    fn take_statement_lock_failure(
        &self,
    ) -> Option<tidb_exec::cluster_table_storage::StatementLockFailure> {
        None
    }

    /// A read handle at an explicit statement timestamp -- the pessimistic
    /// retry's advanced `for_update_ts`. Only a pessimistic transaction can
    /// serve one.
    fn snapshot_at(&self, _read_ts: u64) -> Result<Box<dyn ClusterSnapshot>, String> {
        Err("only a pessimistic transaction reads at a statement timestamp".to_owned())
    }

    /// [`Self::snapshot_at`] told whether the statement takes locks.
    fn snapshot_at_for(
        &self,
        read_ts: u64,
        _locking: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.snapshot_at(read_ts)
    }

    /// Acquires pessimistic locks on one statement's written keys -- Go
    /// `handlePessimisticDML`'s lock step -- and reports the session layer's
    /// next move.
    fn lock_staged_keys(&self, _keys: Vec<Vec<u8>>) -> Result<LockKeysOutcome, String> {
        Err("only a pessimistic transaction locks statement keys".to_owned())
    }

    /// [`Self::lock_staged_keys`] with the lazy INSERT assertions Go carries
    /// from `MemBuffer` into `KVTxn.LockKeys`.
    fn lock_staged_keys_with_assertions(
        &self,
        keys: Vec<Vec<u8>>,
        _presume_not_exists: BTreeSet<Vec<u8>>,
        _duplicate_hints: BTreeMap<Vec<u8>, DuplicateKeyHint>,
        _wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        self.lock_staged_keys(keys)
    }

    /// [`Self::lock_staged_keys`], asking TiKV to answer each newly locked
    /// key's row WITH the lock and serving later reads of those keys from the
    /// answers — Go's point-write fold (`InitReturnValues` /
    /// `TxnCtx.SetPessimisticLockCache`, `pkg/executor/point_get.go:612-624`).
    fn lock_staged_keys_with_values(
        &self,
        _keys: Vec<Vec<u8>>,
        _wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        Err("only a pessimistic transaction locks statement keys".to_owned())
    }

    /// [`Self::lock_staged_keys_with_values`] locking only the keys that exist
    /// (Go `LockCtx.LockOnlyIfExists`, read committed's `lockKeyIfExists`).
    fn lock_staged_keys_with_values_if_exist(
        &self,
        _keys: Vec<Vec<u8>>,
        _wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        Err("only a pessimistic transaction locks statement keys".to_owned())
    }

    /// Releases the locks a FAILED statement accumulated across its retry
    /// rounds -- Go `OnPessimisticStmtEnd(isSuccessful=false)` ->
    /// `CancelFairLocking`. The optimistic default holds no locks and
    /// releases nothing.
    fn finish_pessimistic_statement(&self, _successful: bool) -> Result<(), String> {
        Ok(())
    }
}

/// Rebuilds and stages one Go SQL statement inside a caller-owned pessimistic
/// transaction, leaving transaction commit or rollback to that caller.
pub fn stage_pessimistic_statement<T>(
    transaction: &dyn OpenClusterTransaction,
    staged: &MutationBuffer,
    lock_wait_timeout: Duration,
    build: impl FnMut(
        Box<dyn ClusterSnapshot>,
        u64,
    ) -> Result<(T, Vec<tidb_txnkv::transaction::BufferMutation>), String>,
) -> Result<T, String> {
    transaction.bind_mutation_buffer(staged);
    let result = lock_pessimistic_statement_with(
        transaction.start_ts(),
        staged,
        |retry_ts| {
            let snapshot = match retry_ts {
                Some(retry_ts) => transaction.snapshot_at_for(retry_ts, true),
                None => transaction.snapshot_for(true),
            }?;
            Ok(overlay_staged_mutations(snapshot, staged))
        },
        |keys, presume_not_exists, duplicate_hints| {
            transaction.lock_staged_keys_with_assertions(
                keys,
                presume_not_exists,
                duplicate_hints,
                tidb_txnkv::transaction::LockWaitTime::Timeout(lock_wait_timeout),
            )
        },
        build,
    )
    .map_err(|error| error.to_string());
    transaction.finish_pessimistic_statement(result.is_ok())?;
    let (value, mutations) = result?;
    stage_mutations(staged, mutations).map_err(|error| error.to_string())?;
    Ok(value)
}

/// The timestamp one autocommit statement is at: written when the first read
/// waits for its prepared snapshot, read back by statement publication.
///
/// The two halves cannot share the read transaction itself, because the read
/// handle is unbound and dropped before the publication is decided — that
/// ordering is what keeps a read transaction from outliving its statement. So
/// the *number* outlives the handle, and this is where it lives.
///
/// `None` after the statement means it never read a cluster row. That is a
/// real, distinct case, not a missing value: an `INSERT ... VALUES` with no
/// read has nothing a racing commit could have made stale, so its publication
/// takes a fresh timestamp and is correct doing so.
///
/// A statement that read at the max-ts shortcut records `u64::MAX`, which is
/// not a timestamp; [`RealOptimisticTransactionOpener::begin_at`] refuses it,
/// so such a statement fails closed instead of publishing anywhere.
#[derive(Clone, Debug, Default)]
pub struct StatementReadTs {
    value: Arc<Mutex<Option<u64>>>,
    current_tso: tidb_executor::CurrentTso,
    observer: Option<crate::process::ProcessTransactionObserver>,
    /// Where the open records Go's `DurationWaitTS`.
    phase_durations: Option<tidb_executor::phase_durations::SessionPhaseDurations>,
}

impl StatementReadTs {
    pub fn new(current_tso: tidb_executor::CurrentTso) -> Self {
        Self {
            value: Arc::default(),
            current_tso,
            observer: None,
            phase_durations: None,
        }
    }

    /// Records the statement's timestamp wait on the session's clocks.
    pub fn with_phase_durations(
        mut self,
        phase_durations: tidb_executor::phase_durations::SessionPhaseDurations,
    ) -> Self {
        self.phase_durations = Some(phase_durations);
        self
    }

    /// Go `LazyTxn.Wait`'s deferred `DurationWaitTS = time.Since(begin)`:
    /// the pending transaction became valid after `waited`.
    fn record_wait_ts(&self, waited: Duration) {
        if let Some(phase_durations) = &self.phase_durations {
            phase_durations.set_duration_wait_ts(waited);
        }
    }

    pub fn with_observer(
        mut self,
        observer: Option<crate::process::ProcessTransactionObserver>,
    ) -> Self {
        self.observer = observer;
        self
    }

    fn record(&self, start_ts: u64) {
        *self
            .value
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = Some(start_ts);
        self.current_tso.publish(start_ts);
        if let Some(observer) = &self.observer {
            observer.activated(start_ts);
        }
    }

    /// The timestamp the statement read at, or `None` if it never read.
    pub fn get(&self) -> Option<u64> {
        *self
            .value
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
}

/// One autocommit statement's read snapshot, prepared after planning and
/// activated at its FIRST read.
///
/// The session driver binds a statement's snapshot before the statement is
/// planned, because the slot has to be in place before the executor exists.
/// Opening synchronously at bind time both precedes the plan-shape decision and
/// serializes PD latency with executor setup. Go instead installs an oracle
/// future after `AdviseOptimizeWithPlan`: planning chooses ordinary versus
/// MaxTS first, ordinary TSO work overlaps executor construction, and `Txn()`
/// waits only if execution needs a snapshot. `prepare_open` is that future;
/// `with_open` is the wait/activation boundary.
struct DeferredSnapshot {
    transactions: Arc<dyn ClusterTransactions>,
    /// The statement-scoped group every lazy/prefetched open must retain.
    resource_group: Arc<str>,
    /// Where the open publishes the statement's timestamp, so the publication
    /// can find it after this handle is gone.
    read_ts: StatementReadTs,
    /// A same-transaction write owner, handed to the session when this
    /// statement snapshot is dropped.  The slot is shared because the storage
    /// seam intentionally exposes only `ClusterSnapshot` to the executor.
    write_handoff: Option<WriteTransactionSlot>,
    /// See [`LockRetrySlot`].
    lock_retry: Option<LockRetrySlot>,
    mutation_buffer: Option<MutationBuffer>,
    /// Behind one `Mutex` because `start_ts` takes `&self` and must answer
    /// with the timestamp of the same transaction the reads use -- and because
    /// the declaration below must be settled against the open atomically.
    state: Mutex<DeferredState>,
}

/// The statement's read transaction, and the shape it was declared with.
#[derive(Default)]
struct DeferredState {
    read_options: Option<tidb_txnkv::SnapshotReadOptions>,
    /// The session's statement ctx values, bound to whatever the first read
    /// opens.
    kv_context: Option<tidb_txnkv::StatementKvContextCell>,
    /// An ordinary timestamp request started after planning and not yet waited.
    prepared: Option<Box<dyn PendingClusterSnapshot>>,
    /// `None` until the first read waits for the prepared snapshot.
    opened: Option<Box<dyn ClusterSnapshot>>,
    /// A writable transaction being opened for an UPDATE/DELETE.  Its read
    /// handle is installed in `opened`; the owner stays here until Drop can
    /// hand it to the session for commit.
    prefetched_write: Option<Result<Box<dyn PendingClusterTransaction>, String>>,
    write_transaction: Option<Box<dyn OpenClusterTransaction>>,
    /// Whether the statement declared its whole read is one point get on the
    /// clustered handle, which is what decides WHICH transaction the first
    /// read opens.
    ///
    /// It is a property of the statement, taken once before any read, never of
    /// a read. Were it re-decided per read, one statement's two reads would
    /// land on two different latest-committed versions -- and the counters
    /// would still show a saving. Keeping it beside `opened` under one lock is
    /// what makes "declared before the first read" checkable rather than
    /// hoped for.
    max_ts: bool,
}

impl fmt::Debug for DeferredSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state();
        formatter
            .debug_struct("DeferredSnapshot")
            .field("prepared", &state.prepared.is_some())
            .field("opened", &state.opened.is_some())
            .field("max_ts", &state.max_ts)
            .field("write_transaction", &state.write_transaction.is_some())
            .finish()
    }
}

impl DeferredSnapshot {
    fn new(
        transactions: Arc<dyn ClusterTransactions>,
        read_ts: StatementReadTs,
        resource_group: Arc<str>,
    ) -> Self {
        Self {
            transactions,
            resource_group,
            read_ts,
            write_handoff: None,
            lock_retry: None,
            mutation_buffer: None,
            state: Mutex::new(DeferredState::default()),
        }
    }

    fn new_prefetched_write(
        transactions: Arc<dyn ClusterTransactions>,
        read_ts: StatementReadTs,
        write_handoff: WriteTransactionSlot,
        lock_retry: LockRetrySlot,
        mutation_buffer: MutationBuffer,
        resource_group: Arc<str>,
        pessimistic: bool,
        fair_locking: bool,
        options: SessionTransactionOptions,
    ) -> Self {
        // The timestamp request goes out now and is waited for by the first
        // read; the transaction opens on that thread. Spawning a thread per
        // statement to open it eagerly cost a thread creation and join per
        // autocommit write.
        let prefetched_write = Some(transactions.prepare_autocommit_write(
            &resource_group,
            pessimistic,
            fair_locking,
            options,
        ));
        Self {
            transactions,
            resource_group,
            read_ts,
            write_handoff: Some(write_handoff),
            lock_retry: Some(lock_retry),
            mutation_buffer: Some(mutation_buffer),
            state: Mutex::new(DeferredState {
                prefetched_write,
                ..DeferredState::default()
            }),
        }
    }

    /// A conflict the statement's lock found after its read: the statement
    /// owner re-runs it at `for_update_ts`.
    fn lock_conflict(&self, for_update_ts: u64) -> StorageError {
        if let Some(slot) = &self.lock_retry {
            *slot.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(for_update_ts);
        }
        StorageError::Backend(format!(
            "pessimistic lock conflict: the statement retries at {for_update_ts}"
        ))
    }

    fn state(&self) -> std::sync::MutexGuard<'_, DeferredState> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn prepare_open(&self) -> Result<(), StorageError> {
        let mut state = self.state();
        if state.max_ts || state.prepared.is_some() || state.opened.is_some() {
            return Ok(());
        }
        state.prepared = Some(
            self.transactions
                .prepare_snapshot(&self.resource_group)
                .map_err(StorageError::Backend)?,
        );
        Ok(())
    }

    /// Runs `use_snapshot` against the statement's read transaction, opening
    /// it first if this is the statement's first read.
    ///
    /// The open is where the declaration is spent: a declared statement opens
    /// at `u64::MAX` and pays no PD timestamp, and every later read of the
    /// same statement goes through the transaction this one opened -- so a
    /// statement never splits across two timestamps whichever branch it took.
    fn with_open<T>(
        &self,
        use_snapshot: impl FnOnce(&mut dyn ClusterSnapshot) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut guard = self.state();
        self.open_in(&mut guard, None)?;
        use_snapshot(guard.opened.as_mut().expect("just opened").as_mut())
    }

    /// Opens the statement's read transaction when this is its first act.
    /// `lock` is a locking point read's keys when that read is the first act:
    /// a pessimistic implicit transaction then locks them with returned
    /// values before any read (Go `PointGetExecutor.getAndLock`), and the
    /// statement reads from the lock responses.
    fn open_in(
        &self,
        guard: &mut DeferredState,
        lock: Option<(Vec<Vec<u8>>, tidb_txnkv::transaction::LockWaitTime, bool)>,
    ) -> Result<(), StorageError> {
        if guard.opened.is_none() {
            // Go `LazyTxn.Wait` times turning the pending transaction valid:
            // waiting for its timestamp future and creating the transaction.
            // A prefetched write records it once its transaction is valid,
            // before the point-write lock that follows.
            let begin_wait = std::time::Instant::now();
            let waits_prefetched_write = !guard.max_ts && guard.prefetched_write.is_some();
            let opened = if guard.max_ts {
                self.transactions.open_max_ts_snapshot(&self.resource_group)
            } else if let Some(prefetched_write) = guard.prefetched_write.take() {
                let transaction = prefetched_write.and_then(PendingClusterTransaction::wait);
                self.read_ts.record_wait_ts(begin_wait.elapsed());
                let transaction = transaction.map_err(StorageError::Backend)?;
                if let Some(buffer) = &self.mutation_buffer {
                    transaction.bind_mutation_buffer(buffer);
                }
                // Go enables point-write locking only for pessimistic
                // transactions; a locking plan alone does not imply it.
                let lock = lock.filter(|_| transaction.is_pessimistic());
                let snapshot = if let Some((keys, wait, only_if_exists)) = lock {
                    let outcome = if only_if_exists {
                        transaction.lock_staged_keys_with_values_if_exist(keys, wait)
                    } else {
                        transaction.lock_staged_keys_with_values(keys, wait)
                    }
                    .map_err(StorageError::Backend)?;
                    match outcome {
                        LockKeysOutcome::Locked { .. } => transaction.snapshot_for(true),
                        LockKeysOutcome::RetryStatement { for_update_ts, .. } => {
                            transaction.snapshot_at_for(for_update_ts, true)
                        }
                        LockKeysOutcome::StatementError(error)
                        | LockKeysOutcome::TransactionError(error) => {
                            // The lock ran in this transaction: the failed
                            // statement's `TxnCtx.StartTS` is its start.
                            self.read_ts.record(transaction.start_ts());
                            return Err(lock_storage_error(error));
                        }
                    }
                } else {
                    transaction.snapshot()
                }
                .map_err(StorageError::Backend)?;
                guard.write_transaction = Some(transaction);
                Ok(snapshot)
            } else if let Some(prepared) = guard.prepared.take() {
                prepared.wait().or_else(|prepared_error| {
                    self.transactions
                        .open_snapshot(&self.resource_group)
                        .map_err(|fallback_error| {
                        format!(
                            "prepared snapshot failed ({prepared_error}); replacement snapshot failed ({fallback_error})"
                        )
                    })
                })
            } else {
                self.transactions.open_snapshot(&self.resource_group)
            };
            if !waits_prefetched_write {
                self.read_ts.record_wait_ts(begin_wait.elapsed());
            }
            let mut opened = opened.map_err(StorageError::Backend)?;
            if let Some(options) = &guard.read_options {
                opened.set_snapshot_read_options(options.clone());
            }
            if let Some(context) = &guard.kv_context {
                opened.set_statement_kv_context(context);
            }
            // Recorded at the open, under the same lock, so the timestamp the
            // statement publishes at is the one its reads are served at and
            // cannot be a later transaction's.
            self.read_ts.record(opened.start_ts());
            guard.opened = Some(opened);
        }
        Ok(())
    }
}

/// A pessimistic lock's SQL error as the storage seam reports it, keeping
/// Go's code and SQLSTATE (1205, 1213, 3572, ...).
fn lock_storage_error(error: tidb_exec::pessimistic_lock_error::LockSqlError) -> StorageError {
    StorageError::Sql(
        tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
            .with_identity_opt(error.identity),
    )
}

/// The transaction owner a write statement transfers after its snapshot handle
/// is unbound.  Keeping this alias here avoids exposing the concrete TiKV
/// transaction implementation through the storage trait.
pub type WriteTransactionSlot = Arc<Mutex<Option<Box<dyn OpenClusterTransaction>>>>;

/// Where an autocommit statement's lock, taken after its read, leaves the
/// `for_update_ts` its conflict asks the statement to be re-run at (Go
/// `handlePessimisticLockError` -> `OnStmtRetry`). The statement owner takes
/// it back after the snapshot is gone.
pub type LockRetrySlot = Arc<Mutex<Option<u64>>>;

impl Drop for DeferredSnapshot {
    fn drop(&mut self) {
        let Some(handoff) = self.write_handoff.take() else {
            return;
        };
        let mut state = self.state();
        // An UPDATE/DELETE normally consumes the prefetch in its first read.
        // Receiving an unopened prefetch here also covers a no-row write, so
        // the statement still commits/finishes the same transaction rather
        // than leaking a worker.
        let transaction = state.write_transaction.take().or_else(|| {
            state
                .prefetched_write
                .take()
                .and_then(Result::ok)
                .and_then(|pending| pending.wait().ok())
        });
        drop(state);
        if let Some(transaction) = transaction {
            // Go's executor builder activates a DML's transaction before the
            // statement runs (`buildInsert` -> `updateForUpdateTS` ->
            // `GetStmtForUpdateTS`), so a write that never read -- a point
            // INSERT, failed or not -- still has `TxnCtx.StartTS`, which its
            // slow log and `tidb_last_query_info` report.
            if self.read_ts.get().is_none() {
                self.read_ts.record(transaction.start_ts());
            }
            *handoff.lock().unwrap_or_else(|poison| poison.into_inner()) = Some(transaction);
        }
    }
}

impl ClusterSnapshot for DeferredSnapshot {
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

    fn prepare(&mut self) -> Result<(), StorageError> {
        self.prepare_open()
    }

    /// Go `PointGetExecutor.getAndLock` in a pessimistic implicit
    /// transaction: as the statement's first act the lock opens the write
    /// transaction and the statement reads from the lock responses; after a
    /// read it locks through the open transaction.
    fn lock_keys_for_read(
        &mut self,
        keys: &[Key],
        wait: tidb_txnkv::transaction::LockWaitTime,
        only_if_exists: bool,
    ) -> Result<(), StorageError> {
        let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let mut guard = self.state();
        if guard.opened.is_none() {
            return self.open_in(&mut guard, Some((keys, wait, only_if_exists)));
        }
        let Some(transaction) = guard
            .write_transaction
            .as_ref()
            .filter(|transaction| transaction.is_pessimistic())
        else {
            return Ok(());
        };
        let outcome = if only_if_exists {
            transaction.lock_staged_keys_with_values_if_exist(keys, wait)
        } else {
            transaction.lock_staged_keys_with_values(keys, wait)
        };
        match outcome.map_err(StorageError::Backend)? {
            LockKeysOutcome::Locked { .. } => Ok(()),
            // The statement already read at its start timestamp, so a newer
            // committed version is a write conflict for the whole statement.
            LockKeysOutcome::RetryStatement { for_update_ts, .. } => {
                Err(self.lock_conflict(for_update_ts))
            }
            LockKeysOutcome::StatementError(error) | LockKeysOutcome::TransactionError(error) => {
                Err(lock_storage_error(error))
            }
        }
    }

    /// Go `doLockKeys` from `SelectLockExec.Next` in a pessimistic implicit
    /// transaction: the statement has read through its write transaction,
    /// which locks the keys of the rows the plan returned.
    fn do_lock_keys(
        &mut self,
        keys: &[Key],
        wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<(), StorageError> {
        let keys: Vec<Vec<u8>> = keys.iter().map(|key| key.as_bytes().to_vec()).collect();
        let mut guard = self.state();
        self.open_in(&mut guard, None)?;
        let Some(transaction) = guard
            .write_transaction
            .as_ref()
            .filter(|transaction| transaction.is_pessimistic())
        else {
            return Ok(());
        };
        let outcome = transaction
            .lock_staged_keys_with_assertions(keys, BTreeSet::new(), BTreeMap::new(), wait)
            .map_err(StorageError::Backend)?;
        match outcome {
            LockKeysOutcome::Locked { .. } => Ok(()),
            // The statement already read at its start timestamp, so a newer
            // committed version is a write conflict for the whole statement.
            LockKeysOutcome::RetryStatement { for_update_ts, .. } => {
                Err(self.lock_conflict(for_update_ts))
            }
            LockKeysOutcome::StatementError(error) | LockKeysOutcome::TransactionError(error) => {
                Err(lock_storage_error(error))
            }
        }
    }

    fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.with_open(|snapshot| snapshot.get(key))
    }

    fn batch_get(&mut self, keys: &[Key]) -> Result<SnapshotPairs, StorageError> {
        self.with_open(|snapshot| snapshot.batch_get(keys))
    }

    fn scan(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.with_open(|snapshot| snapshot.scan(start, end, limit))
    }

    fn scan_committed(
        &mut self,
        start: &Key,
        end: &Key,
        limit: Option<usize>,
    ) -> Result<SnapshotPairs, StorageError> {
        self.with_open(|snapshot| snapshot.scan_committed(start, end, limit))
    }

    fn start_ts(&self) -> u64 {
        // A remote pushdown scan names the timestamp before issuing its
        // request, so asking IS the statement's first read: opening here is
        // what keeps the coprocessor request and any local read on one
        // transaction. `0` on failure is the "no MVCC timestamp" answer the
        // trait already defines, which refuses the remote scan and leaves the
        // error to be reported by the read that follows.
        self.with_open(|snapshot| Ok(snapshot.start_ts()))
            .unwrap_or(0)
    }

    /// Takes the declaration, but only while the statement still has no read
    /// transaction.
    ///
    /// A statement that has already read has already spent a timestamp, and
    /// switching now would put its remaining reads on a second snapshot. That
    /// is Go's `p.txn != nil` refusal in `AdviseOptimizeWithPlan` -- "the
    /// startTS has already been used" -- and it is checked under the same lock
    /// the open takes, so there is no window between the two.
    fn declare_autocommit_point_get(&mut self) -> bool {
        let mut state = self.state();
        if state.opened.is_some() {
            return false;
        }
        // Go chooses the MaxTS constant future before warmup. If a caller ever
        // reverses those two operations, discard the ordinary future here.
        state.prepared.take();
        state.max_ts = true;
        true
    }
}

/// Binds one autocommit statement's snapshot without starting a timestamp.
///
/// `read_ts` is the statement's, and outlives the returned handle: whatever
/// timestamp the statement's first read waits for is written there, and the
/// statement's publication reads it back after this handle has been dropped.
pub fn deferred_snapshot(
    transactions: Arc<dyn ClusterTransactions>,
    read_ts: StatementReadTs,
    resource_group: Arc<str>,
) -> Box<dyn ClusterSnapshot> {
    Box::new(DeferredSnapshot::new(transactions, read_ts, resource_group))
}

/// [`deferred_snapshot`] with a writable transaction opened in parallel with
/// DML planning.  The statement reads and commits through that one owner.
pub fn prefetched_write_snapshot(
    transactions: Arc<dyn ClusterTransactions>,
    read_ts: StatementReadTs,
    write_handoff: WriteTransactionSlot,
    lock_retry: LockRetrySlot,
    mutation_buffer: MutationBuffer,
    resource_group: Arc<str>,
    pessimistic: bool,
    fair_locking: bool,
    options: SessionTransactionOptions,
) -> Box<dyn ClusterSnapshot> {
    Box::new(DeferredSnapshot::new_prefetched_write(
        transactions,
        read_ts,
        write_handoff,
        lock_retry,
        mutation_buffer,
        resource_group,
        pessimistic,
        fair_locking,
        options,
    ))
}

/// The production transaction tier: real read-only transactions and the
/// optimistic 2PC, both over the node's one process authority.
pub struct RealClusterTransactions<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    /// The opener stamped with each resource group already seen. Go keeps
    /// the group name on the request context; the Rust opener carries it,
    /// so every statement used to clone the whole opener for its group. A
    /// session's group rarely changes, so the stamped clone is kept and
    /// shared. Bounded by the number of distinct groups this node serves.
    group_openers: Arc<Mutex<Vec<GroupOpener<S>>>>,
    /// `mysql.advisory_locks`, located by name the first time a lock is
    /// taken (the table is never recreated, so its key shape is fixed for
    /// the life of the cluster).
    advisory_lock_table: Arc<Mutex<Option<tidb_exec::mysql_bootstrap::AdvisoryLockTable>>>,
}

/// One resource group's stamped opener; see `RealClusterTransactions::group_openers`.
type GroupOpener<S> = (Arc<str>, Arc<RealOptimisticTransactionOpener<S>>);

struct RealPendingSnapshot<S>(PreparedStatementSnapshot<S>)
where
    S: StoreClient;

impl<S> PendingClusterSnapshot for RealPendingSnapshot<S>
where
    S: StoreClient,
{
    fn wait(self: Box<Self>) -> Result<Box<dyn ClusterSnapshot>, String> {
        self.0
            .wait()
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }
}

impl<S> RealClusterTransactions<S>
where
    S: StoreClient,
{
    /// Binds the tier to an already-connected authority's write capability.
    #[must_use]
    pub fn new(opener: RealOptimisticTransactionOpener<S>, timeout: Duration) -> Self {
        Self {
            opener: Arc::new(opener),
            timeout,
            group_openers: Arc::new(Mutex::new(Vec::new())),
            advisory_lock_table: Arc::new(Mutex::new(None)),
        }
    }

    fn opener_for_resource_group(
        &self,
        resource_group: &str,
    ) -> Arc<RealOptimisticTransactionOpener<S>> {
        let mut cached = self
            .group_openers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((_, opener)) = cached
            .iter()
            .find(|(name, _)| name.as_ref() == resource_group)
        {
            return Arc::clone(opener);
        }
        let name = Arc::<str>::from(resource_group);
        let opener = Arc::new(
            self.opener
                .as_ref()
                .clone()
                .with_resource_group_name(Arc::clone(&name)),
        );
        cached.push((name, Arc::clone(&opener)));
        opener
    }

    /// Go resolves `mysql.advisory_locks` through the info schema on every
    /// `GET_LOCK`; this tier keeps the one located table, found by name in
    /// the stored schema whichever layout the bootstrap gave it.
    fn advisory_lock_table(
        &self,
    ) -> Result<tidb_exec::mysql_bootstrap::AdvisoryLockTable, AdvisoryLockError> {
        let mut cached = self
            .advisory_lock_table
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(table) = cached.as_ref() {
            return Ok(table.clone());
        }
        let mut transaction = self
            .opener
            .begin()
            .map_err(|error| AdvisoryLockError::Internal(error.to_string()))?;
        let located = {
            let mut snapshot = tidb_exec::real_tikv_catalog::TransactionMetaSnapshot::new(
                &mut transaction,
                self.timeout,
            );
            tidb_exec::mysql_bootstrap::AdvisoryLockTable::locate(&mut snapshot)
        };
        let _ = transaction.rollback(&UnaryCallContext::with_timeout(self.timeout));
        let table = located.map_err(|error| AdvisoryLockError::Internal(error.to_string()))?;
        *cached = Some(table.clone());
        Ok(table)
    }

    fn acquire_advisory_lock_lease(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<RealAdvisoryLockLease<S>, AdvisoryLockError> {
        let key = self
            .advisory_lock_table()?
            .key(name)
            .map_err(|error| AdvisoryLockError::Internal(error.to_string()))?;
        // Go's advisory lock session: a pessimistic transaction that locks and
        // never publishes, holding its lock until rollback.
        let mut transaction = self
            .opener
            .begin()
            .map_err(|error| AdvisoryLockError::Internal(error.to_string()))?;
        transaction.set_pessimistic(true);
        let wait = if timeout.is_zero() {
            LockWaitTime::NoWait
        } else {
            LockWaitTime::Timeout(timeout)
        };
        let call = UnaryCallContext::with_timeout(timeout.saturating_add(self.timeout));
        let presume_not_exists = BTreeSet::from([key.clone()]);
        match transaction.acquire_locks(
            std::slice::from_ref(&key),
            &presume_not_exists,
            wait,
            &call,
        ) {
            Ok(acquired) => acquired,
            Err(failure) => {
                finish_advisory_transaction(transaction, self.timeout);
                return Err(map_advisory_lock_failure(failure));
            }
        };
        Ok(RealAdvisoryLockLease {
            transaction: Some(transaction),
            end_timeout: self.timeout,
        })
    }
}

struct RealAdvisoryLockLease<S>
where
    S: StoreClient,
{
    transaction: Option<RealTransaction<S>>,
    end_timeout: Duration,
}

impl<S> RealAdvisoryLockLease<S>
where
    S: StoreClient,
{
    fn finish(&mut self) {
        let Some(transaction) = self.transaction.take() else {
            return;
        };
        finish_advisory_transaction(transaction, self.end_timeout);
    }
}

fn finish_advisory_transaction<S: StoreClient>(transaction: RealTransaction<S>, timeout: Duration) {
    let call = UnaryCallContext::with_timeout(timeout);
    let _ = transaction.rollback(&call);
}

impl<S> Drop for RealAdvisoryLockLease<S>
where
    S: StoreClient,
{
    fn drop(&mut self) {
        self.finish();
    }
}

impl<S> AdvisoryLockLease for RealAdvisoryLockLease<S>
where
    S: StoreClient,
{
    fn release(mut self: Box<Self>) {
        self.finish();
    }
}

fn map_advisory_lock_failure(failure: PessimisticLockFailure) -> AdvisoryLockError {
    match failure {
        PessimisticLockFailure::LockAcquireFailAndNoWaitSet { .. }
        | PessimisticLockFailure::LockWaitTimeout { .. } => AdvisoryLockError::Timeout,
        PessimisticLockFailure::Deadlock(_) => AdvisoryLockError::Deadlock,
        other => AdvisoryLockError::Internal(other.to_string()),
    }
}

/// [`PendingClusterTransaction`] over one real opener: the timestamp future
/// dispatched at statement start, opened as an optimistic or pessimistic
/// session transaction when first needed.
struct PendingSessionTransaction<S>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    start_ts: tidb_txnkv::driver::tikv_store::StoreTimestampFuture,
    timeout: Duration,
    pessimistic: bool,
    fair_locking: bool,
    options: SessionTransactionOptions,
}

impl<S> PendingClusterTransaction for PendingSessionTransaction<S>
where
    S: StoreClient + Send + Sync + 'static,
{
    fn wait(self: Box<Self>) -> Result<Box<dyn OpenClusterTransaction>, String> {
        let options = self.options;
        let start_ts = match self.start_ts.wait() {
            Ok(start_ts) => start_ts,
            Err(error) => {
                // Go `txnFuture.wait` (`pkg/session/txn.go:702-713`): a
                // failed timestamp future is logged and the transaction is
                // opened on a fresh synchronous timestamp instead of failing
                // the statement (only unistore propagates the error).
                tidb_util::logutil::bg_logger().warn(
                    "wait tso failed",
                    &[tidb_log::Field::new(
                        "error",
                        tidb_log::Value::Str(error.to_string()),
                    )],
                );
                return if self.pessimistic {
                    SessionTransaction::begin_pessimistic(self.opener, self.timeout, options)
                        .map(|mut transaction| {
                            transaction.set_fair_locking(self.fair_locking);
                            Box::new(transaction) as Box<dyn OpenClusterTransaction>
                        })
                        .map_err(|error| error.to_string())
                } else {
                    SessionTransaction::begin(self.opener, self.timeout, options)
                        .map(|transaction| Box::new(transaction) as Box<dyn OpenClusterTransaction>)
                        .map_err(|error| error.to_string())
                };
            }
        };
        if self.pessimistic {
            // Go `decideTxnMode`: the retry of a conflicted autocommit DML is
            // pessimistic, so it waits on the row instead of failing again.
            SessionTransaction::begin_pessimistic_at(self.opener, start_ts, self.timeout, options)
                .map(|mut transaction| {
                    // Go `OnPessimisticStmtStart` -> `StartFairLocking`, gated on
                    // the session's `@@tidb_pessimistic_txn_fair_locking`.
                    transaction.set_fair_locking(self.fair_locking);
                    Box::new(transaction) as Box<dyn OpenClusterTransaction>
                })
                .map_err(|error| error.to_string())
        } else {
            SessionTransaction::begin_at(self.opener, start_ts, self.timeout, options)
                .map(|transaction| Box::new(transaction) as Box<dyn OpenClusterTransaction>)
                .map_err(|error| error.to_string())
        }
    }
}

impl<S> ClusterTransactions for RealClusterTransactions<S>
where
    S: StoreClient,
{
    fn fresh_timestamp(&self) -> Result<u64, String> {
        self.opener.store().get_timestamp_async().wait()
    }

    fn external_timestamp(&self, value: Option<u64>) -> Result<u64, String> {
        self.opener.store().external_timestamp(value)
    }

    fn record_region_stats(
        &self,
        table_id: i64,
    ) -> Result<Option<tidb_exec::pd_approximate_count::RegionCountStats>, String> {
        let store = self.opener.store();
        let Some(pd_http_client) = store.pd_http_client() else {
            return Ok(None);
        };
        store
            .block_on(tidb_exec::pd_approximate_count::load_record_region_stats(
                &pd_http_client,
                table_id,
                self.timeout,
            ))
            .map(Some)
    }

    /// Go `infoschema_reader.go`'s `DATA_LOCK_WAITS` source: the store's
    /// `GetLockWaits` plus its lock resolver's `Resolving()`.
    fn lock_waits(&self) -> Result<Vec<tidb_proto::KvrpcWaitForEntry>, String> {
        let store = self.opener.store();
        let mut result = store.get_lock_waits()?;
        result.extend(store.resolving_locks().into_iter().map(|lock| {
            tidb_proto::KvrpcWaitForEntry {
                txn: lock.txn_id,
                wait_for_txn: lock.lock_txn_id,
                key: lock.key,
                ..tidb_proto::KvrpcWaitForEntry::default()
            }
        }));
        Ok(result)
    }

    fn prepare_snapshot(
        &self,
        resource_group: &str,
    ) -> Result<Box<dyn PendingClusterSnapshot>, String> {
        StatementSnapshot::prepare(self.opener_for_resource_group(resource_group), self.timeout)
            .map(|snapshot| {
                Box::new(RealPendingSnapshot(snapshot)) as Box<dyn PendingClusterSnapshot>
            })
            .map_err(|error| error.to_string())
    }

    fn open_snapshot_at(
        &self,
        read_ts: u64,
        resource_group: &str,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        // Validate against this store's timestamp authority before opening any history.
        // Native reads retain their ordinary GC visibility check.
        let current = self.open_snapshot(resource_group)?;
        let current_ts = current.start_ts();
        drop(current);
        if read_ts > current_ts {
            return Err(format!("cannot set read timestamp to a future time, readTS: {read_ts}, currentTS: {current_ts}"));
        }
        StatementSnapshot::open_at(
            self.opener_for_resource_group(resource_group),
            read_ts,
            self.timeout,
        )
        .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
        .map_err(|error| error.to_string())
    }

    fn open_snapshot(&self, resource_group: &str) -> Result<Box<dyn ClusterSnapshot>, String> {
        StatementSnapshot::open(self.opener_for_resource_group(resource_group), self.timeout)
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }

    fn open_max_ts_snapshot(
        &self,
        resource_group: &str,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        Ok(Box::new(MaxTsSnapshot::new(
            self.opener_for_resource_group(resource_group),
            self.timeout,
        )))
    }

    fn prepare_autocommit_write(
        &self,
        resource_group: &str,
        pessimistic: bool,
        fair_locking: bool,
        options: SessionTransactionOptions,
    ) -> Result<Box<dyn PendingClusterTransaction>, String> {
        let opener = self.opener_for_resource_group(resource_group);
        let start_ts = opener
            .prepare_read_only_start_ts()
            .map_err(|error| error.to_string())?;
        Ok(Box::new(PendingSessionTransaction {
            opener,
            start_ts,
            timeout: self.timeout,
            pessimistic,
            fair_locking,
            options,
        }))
    }

    fn commit(
        &self,
        buffer: &MutationBuffer,
        read_ts: Option<u64>,
        resource_group: &str,
        options: SessionTransactionOptions,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError> {
        self.commit_with_schema_lease(buffer, read_ts, resource_group, None, options, None)
    }

    fn commit_with_schema_lease(
        &self,
        buffer: &MutationBuffer,
        read_ts: Option<u64>,
        resource_group: &str,
        schema_lease_checker: Option<Arc<dyn SchemaLeaseChecker>>,
        options: SessionTransactionOptions,
        observer: Option<crate::process::ProcessTransactionObserver>,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError> {
        // Go's autocommit `finishStmt` on a statement that staged nothing
        // publishes nothing; skip the opener and protocol lookups too.
        if buffer.is_empty() {
            return Ok(None);
        }
        let opener = self.opener_for_resource_group(resource_group);
        commit_staged_buffer(
            &opener,
            buffer,
            read_ts,
            options,
            schema_lease_checker,
            |ts| {
                if let Some(observer) = observer {
                    observer.activated(ts);
                }
            },
        )
        .map(|outcome| outcome.as_ref().and_then(CommittedTxnInfo::from_outcome))
        .map_err(sql_error)
    }

    fn commit_optimistic_mutations(
        &self,
        mutations: Vec<tidb_txnkv::transaction::BufferMutation>,
        read_ts: u64,
        resource_group: &str,
        options: SessionTransactionOptions,
    ) -> Result<(), SqlQueryError> {
        if mutations.is_empty() {
            return Ok(());
        }
        let mut transaction = self
            .opener_for_resource_group(resource_group)
            .begin_at(read_ts)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        options.apply(&mut transaction);
        let outcome = transaction
            .commit(mutations, &UnaryCallContext::with_timeout(self.timeout))
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        match outcome {
            tidb_txnkv::transaction::OptimisticCommitOutcome::Committed(_) => Ok(()),
            other => Err(SqlQueryError::unknown(format!(
                "restricted transaction did not commit: {other:?}"
            ))),
        }
    }

    fn begin(
        &self,
        pessimistic: bool,
        fair_locking: bool,
        resource_group: &str,
        options: SessionTransactionOptions,
    ) -> Result<Box<dyn OpenClusterTransaction>, String> {
        let opener = self.opener_for_resource_group(resource_group);
        let transaction = if pessimistic {
            SessionTransaction::begin_pessimistic(opener, self.timeout, options).map(
                |mut transaction| {
                    // Go `OnPessimisticStmtStart` -> `KVTxn.StartFairLocking`,
                    // gated on the session's `@@tidb_pessimistic_txn_fair_locking`.
                    transaction.set_fair_locking(fair_locking);
                    transaction
                },
            )
        } else {
            SessionTransaction::begin(opener, self.timeout, options)
        };
        transaction
            .map(|transaction| Box::new(transaction) as Box<dyn OpenClusterTransaction>)
            .map_err(|error| error.to_string())
    }

    fn acquire_advisory_lock(
        &self,
        name: &str,
        timeout: Duration,
    ) -> Result<Box<dyn AdvisoryLockLease>, AdvisoryLockError> {
        self.acquire_advisory_lock_lease(name, timeout)
            .map(|lease| Box::new(lease) as Box<dyn AdvisoryLockLease>)
    }

    fn is_advisory_lock_used(&self, name: &str) -> bool {
        match self.acquire_advisory_lock_lease(name, Duration::from_secs(1)) {
            Ok(mut lease) => {
                lease.finish();
                false
            }
            Err(_) => true,
        }
    }
}

impl<S> OpenClusterTransaction for SessionTransaction<S>
where
    S: StoreClient,
{
    fn bind_mutation_buffer(&self, buffer: &MutationBuffer) {
        SessionTransaction::bind_mutation_buffer(self, buffer);
    }

    fn start_ts(&self) -> u64 {
        SessionTransaction::start_ts(self)
    }

    fn opened_at(&self) -> std::time::Instant {
        // The exec type's INHERENT method (fields take precedence in method
        // position, and the inherent method terminates the dispatch chain
        // that the trait-method path form would recurse into).
        SessionTransaction::opened_at(self)
    }

    fn statement_count(&self) -> u64 {
        SessionTransaction::statement_count(self)
    }

    fn note_statement(&self) {
        SessionTransaction::note_statement(self)
    }

    fn set_resource_group_name(&self, name: &str) -> Result<(), String> {
        SessionTransaction::set_resource_group_name(self, name).map_err(|error| error.to_string())
    }

    fn set_statement_assertion_level(
        &self,
        level: tidb_proto::KvrpcAssertionLevel,
    ) -> Result<(), String> {
        SessionTransaction::set_statement_assertion_level(self, level)
            .map_err(|error| error.to_string())
    }

    fn set_schema_lease_checker(&mut self, checker: Arc<dyn SchemaLeaseChecker>) {
        SessionTransaction::set_schema_lease_checker(self, checker);
    }

    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::snapshot(self).map_err(|error| error.to_string())
    }

    fn snapshot_for(&self, locking: bool) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::snapshot_for(self, locking).map_err(|error| error.to_string())
    }

    fn commit(
        self: Box<Self>,
        buffer: &MutationBuffer,
    ) -> Result<Option<CommittedTxnInfo>, SqlQueryError> {
        SessionTransaction::commit(*self, buffer)
            .map(|outcome| outcome.as_ref().and_then(CommittedTxnInfo::from_outcome))
            .map_err(sql_error)
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
        SessionTransaction::rollback(*self)
    }

    fn is_pessimistic(&self) -> bool {
        SessionTransaction::is_pessimistic(self)
    }

    fn for_update_ts(&self) -> u64 {
        SessionTransaction::for_update_ts(self)
    }

    fn fresh_locking_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::fresh_locking_snapshot(self).map_err(|error| error.to_string())
    }

    fn fresh_statement_snapshot(&self, locking: bool) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::fresh_statement_snapshot(self, locking)
            .map_err(|error| error.to_string())
    }

    fn deferred_statement_snapshot(
        &self,
        locking: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::deferred_statement_snapshot(self, locking)
            .map_err(|error| error.to_string())
    }

    fn take_statement_lock_failure(
        &self,
    ) -> Option<tidb_exec::cluster_table_storage::StatementLockFailure> {
        SessionTransaction::take_statement_lock_failure(self)
    }

    fn snapshot_at(&self, read_ts: u64) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::snapshot_at(self, read_ts).map_err(|error| error.to_string())
    }

    fn snapshot_at_for(
        &self,
        read_ts: u64,
        locking: bool,
    ) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::snapshot_at_for(self, read_ts, locking).map_err(|e| e.to_string())
    }

    fn lock_staged_keys(&self, keys: Vec<Vec<u8>>) -> Result<LockKeysOutcome, String> {
        SessionTransaction::lock_keys(self, keys).map_err(|error| error.to_string())
    }

    fn lock_staged_keys_with_assertions(
        &self,
        keys: Vec<Vec<u8>>,
        presume_not_exists: BTreeSet<Vec<u8>>,
        duplicate_hints: BTreeMap<Vec<u8>, DuplicateKeyHint>,
        wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        SessionTransaction::lock_keys_with_assertions(
            self,
            keys,
            presume_not_exists,
            duplicate_hints,
            false,
            wait,
        )
        .map_err(|error| error.to_string())
    }

    fn lock_staged_keys_with_values(
        &self,
        keys: Vec<Vec<u8>>,
        wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        SessionTransaction::lock_keys_with_values(self, keys, true, wait)
            .map_err(|error| error.to_string())
    }

    fn lock_staged_keys_with_values_if_exist(
        &self,
        keys: Vec<Vec<u8>>,
        wait: tidb_txnkv::transaction::LockWaitTime,
    ) -> Result<LockKeysOutcome, String> {
        SessionTransaction::lock_keys_with_values_if_exist(self, keys, wait)
            .map_err(|error| error.to_string())
    }

    fn finish_pessimistic_statement(&self, successful: bool) -> Result<(), String> {
        SessionTransaction::finish_statement(self, successful).map_err(|error| error.to_string())
    }
}

#[cfg(test)]
mod write_conflict_rewrite_tests {
    use tidb_codec::table_key::{
        encode_index_seek_key, encode_row_key_with_handle, hex, RecordHandle,
    };
    use tidb_txnkv::driver::transaction_error::{
        pretty_write_key, RedactArg, Redaction, WriteConflict, WriteConflictArgs,
    };

    use super::try_replace_write_conflict_error;
    use tidb_exec::sql_query_error::SqlQueryError;

    /// The commit error `sql_error` hands the session for one conflict.
    fn commit_error(key: &[u8], primary: &[u8], redaction: Redaction) -> SqlQueryError {
        let args = WriteConflictArgs::new(
            &WriteConflict {
                start_ts: 10,
                conflict_ts: 11,
                conflict_commit_ts: 12,
                key: key.to_vec(),
                primary: primary.to_vec(),
                reason: "Optimistic".to_owned(),
            },
            redaction,
        );
        SqlQueryError {
            write_conflict: Some(Box::new(args.clone())),
            ..SqlQueryError::new(9007, *b"HY000", args.message())
        }
    }

    /// Table 28 is `acc2.t`, table 29 has lost its schema, and nothing else
    /// exists: Go's `is.TableByID` followed by `infoschema.SchemaByTable`.
    fn tables(table_id: i64) -> Option<(Option<String>, String)> {
        match table_id {
            28 => Some((Some("acc2".to_owned()), "t".to_owned())),
            29 => Some((None, "orphan".to_owned())),
            _ => None,
        }
    }

    #[test]
    fn record_and_index_keys_gain_their_table_names() {
        let record = encode_row_key_with_handle(28, &RecordHandle::Int(20));
        // An index key of table 28 whose one value is the integer 7.
        let index = encode_index_seek_key(28, 2, &[0x03, 0x80, 0, 0, 0, 0, 0, 0, 0x07]);
        let error = try_replace_write_conflict_error(
            commit_error(&index, &record, Redaction::Disabled),
            Redaction::Disabled,
            tables,
        );
        let (_, index_rest) = pretty_write_key(&index);
        assert!(
            index_rest.starts_with(", indexID=2, indexValues={"),
            "{index_rest}"
        );
        assert_eq!(error.code, 9007);
        assert_eq!(
            error.message,
            format!(
                "Write conflict, txnStartTS=10, conflictStartTS=11, conflictCommitTS=12, \
                 key={{tableID=28, tableName=acc2.t{index_rest}, originalKey={}, \
                 primary={{tableID=28, tableName=acc2.t, handle=20}}, originalPrimaryKey={}, \
                 reason=Optimistic [try again later]",
                hex(&index),
                hex(&record)
            )
        );
        let args = error
            .write_conflict
            .expect("the arguments stay with the error");
        assert_eq!(
            args.key_table_id,
            RedactArg::Str("{tableID=28, tableName=acc2.t".to_owned())
        );
    }

    #[test]
    fn tables_the_infoschema_cannot_name_use_go_s_fallbacks() {
        let missing = encode_row_key_with_handle(404, &RecordHandle::Int(1));
        let orphan = encode_row_key_with_handle(29, &RecordHandle::Int(2));
        let error = try_replace_write_conflict_error(
            commit_error(&missing, &orphan, Redaction::Disabled),
            Redaction::Disabled,
            tables,
        );
        assert!(
            error
                .message
                .contains("key={tableID=404, tableName=unknown, handle=1}, originalKey="),
            "{}",
            error.message
        );
        assert!(
            error.message.contains(
                "primary={tableID=29, tableName=unknown.orphan, handle=2}, originalPrimaryKey="
            ),
            "{}",
            error.message
        );
    }

    #[test]
    fn keys_without_a_table_id_are_left_alone() {
        let raw = b"not a table key".to_vec();
        let before = commit_error(&raw, &raw, Redaction::Disabled);
        let after = try_replace_write_conflict_error(before.clone(), Redaction::Disabled, tables);
        assert_eq!(after, before);
    }

    /// Go returns before touching the error when redaction is `ON`.
    #[test]
    fn redaction_on_skips_the_rewrite() {
        let record = encode_row_key_with_handle(28, &RecordHandle::Int(20));
        let before = commit_error(&record, &record, Redaction::Enabled);
        let after = try_replace_write_conflict_error(before.clone(), Redaction::Enabled, tables);
        assert_eq!(after, before);
        assert_eq!(
            after.message,
            "Write conflict, txnStartTS=10, conflictStartTS=11, conflictCommitTS=12, \
             key=????, reason=Optimistic [try again later]"
        );
    }

    /// Under `MARKER` the key arguments are Go `*redactFormatter`s, not
    /// strings, so no table is named, and `FastGenByArgs` marks them again.
    #[test]
    fn redaction_marker_names_no_table_and_marks_again() {
        let record = encode_row_key_with_handle(28, &RecordHandle::Int(20));
        let original = hex(&record);
        let before = commit_error(&record, &record, Redaction::Marker);
        assert_eq!(
            before.message,
            format!(
                "Write conflict, txnStartTS=10, conflictStartTS=11, conflictCommitTS=12, \
                 key=‹{{tableID=28›‹, handle=20}}, originalKey={original}, primary=›\
                 ‹{{tableID=28›‹, handle=20}}, originalPrimaryKey={original}›, \
                 reason=Optimistic [try again later]"
            )
        );
        let after = try_replace_write_conflict_error(before, Redaction::Marker, tables);
        assert_eq!(
            after.message,
            format!(
                "Write conflict, txnStartTS=10, conflictStartTS=11, conflictCommitTS=12, \
                 key=‹‹‹{{tableID=28›››‹‹‹, handle=20}}, originalKey={original}, primary=›››\
                 ‹‹‹{{tableID=28›››‹‹‹, handle=20}}, originalPrimaryKey={original}›››, \
                 reason=Optimistic [try again later]"
            )
        );
    }

    #[test]
    fn errors_without_conflict_arguments_pass_through() {
        let error = SqlQueryError::new(9007, *b"HY000", "Write conflict");
        assert_eq!(
            try_replace_write_conflict_error(error.clone(), Redaction::Disabled, tables),
            error
        );
    }
}
