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

//! Statement-summary admission from Go `pkg/executor/adapter.go`.
//!
//! The live session completion producer in `tidb-session::observation` uses
//! this policy before publishing to `tidb-stmtsummary`. Detailed statement
//! measurements and slow-log rule evaluation remain separate obligations.

mod result_fields;
pub use result_fields::*;

/// Go `TelemetryInfo`, `PartitionTelemetryInfo` and
/// `AccountLockTelemetryInfo`, carried on the statement context.
pub use tidb_sessionctx::telemetry_info::{AccountLockTelemetryInfo, PartitionTelemetryInfo, TelemetryInfo};

/// The statement kinds `SummaryStmt` special-cases.
///
/// Go: `pkg/executor/adapter.go:2204, 2217`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SummaryStmtKind {
    /// Go: `*ast.PrepareStmt` — skipped; `EXECUTE` is recorded instead.
    Prepare,
    /// Go: `*ast.CommitStmt` — recorded, but only when a previous statement
    /// digest exists to attribute it to.
    Commit,
    /// Any other statement.
    Other,
}

/// Everything `SummaryStmt` consults before recording.
///
/// Go: `pkg/executor/adapter.go:2189-2225`.
#[derive(Clone, Debug)]
pub struct SummaryGate {
    /// Go: `sessVars.InRestrictedSQL`.
    pub in_restricted_sql: bool,
    /// Go: `sessVars.User.Username`, empty when `sessVars.User == nil`.
    pub user_name: String,
    /// Go: `sessVars.InExplainExplore`.
    pub in_explain_explore: bool,
    /// Go: `stmtsummaryv2.Enabled()`.
    pub summary_enabled: bool,
    /// Go: `stmtsummaryv2.EnabledInternal()`.
    pub summary_internal_enabled: bool,
    /// The statement's kind.
    pub stmt_kind: SummaryStmtKind,
    /// Go: `sessVars.GetPrevStmtDigest()`.
    pub prev_stmt_digest: String,
}

/// What `SummaryStmt` does for a given gate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SummaryAction {
    /// Go: summary disabled for this statement — `SetPrevStmtDigest("")` then
    /// return. Note this is the *only* skip path that clears the digest.
    ClearPrevDigestAndSkip,
    /// Go: `*ast.PrepareStmt` returns without touching the previous digest.
    SkipPrepare,
    /// Go: a `COMMIT` seen before any digest was recorded is ignored, again
    /// without touching the previous digest.
    SkipCommitWithoutPrevDigest,
    /// Go: the statement is recorded via `stmtsummaryv2.Add`, and the previous
    /// statement digest is replaced by this statement's digest.
    Record {
        /// Go's `isInternalSQL`, stored on `StmtExecInfo.IsInternal`.
        is_internal: bool,
        /// Go: the previous statement digest a `COMMIT` is attributed to;
        /// `None` for every other statement kind.
        attributed_prev_digest: Option<String>,
    },
}

/// Whether a statement counts as internal for statement-summary purposes.
///
/// Go: `pkg/executor/adapter.go:2198`. A statement with no user name counts as
/// internal just like restricted SQL, but `EXPLAIN EXPLORE` forces it back to
/// user-visible so its inner statements land in the summary.
#[must_use]
pub fn is_internal_sql(in_restricted_sql: bool, user_name: &str, in_explain_explore: bool) -> bool {
    (in_restricted_sql || user_name.is_empty()) && !in_explain_explore
}

/// Decides whether `SummaryStmt` records this statement.
///
/// Go: `ExecStmt.SummaryStmt` at `pkg/executor/adapter.go:2189`.
#[must_use]
pub fn decide_summary_stmt(gate: &SummaryGate) -> SummaryAction {
    let is_internal = is_internal_sql(
        gate.in_restricted_sql,
        &gate.user_name,
        gate.in_explain_explore,
    );
    if !gate.summary_enabled || (is_internal && !gate.summary_internal_enabled) {
        return SummaryAction::ClearPrevDigestAndSkip;
    }
    if gate.stmt_kind == SummaryStmtKind::Prepare {
        return SummaryAction::SkipPrepare;
    }
    let attributed_prev_digest = if gate.stmt_kind == SummaryStmtKind::Commit {
        if gate.prev_stmt_digest.is_empty() {
            return SummaryAction::SkipCommitWithoutPrevDigest;
        }
        Some(gate.prev_stmt_digest.clone())
    } else {
        None
    };
    SummaryAction::Record {
        is_internal,
        attributed_prev_digest,
    }
}

// boundary: the `StmtExecInfo` population (`pkg/executor/adapter.go:2258-2302`)
// reads ~30 session/stmt-context fields (`stmtCtx.SQLDigest`,
// `stmtCtx.CopTasksSummary`, `sessVars.MemTracker`, `GetPlanDigest`,
// `keyspace.GetKeyspaceNameBySettings`, `calculateStatementTotalRUV2`, ...).
// The summary sink lives in `tidb-stmtsummary`. The live `tidb-session`
// completion producer reuses `decide_summary_stmt`; detailed plan/RPC/phase
// measurements remain explicit integration obligations, not package acceptance.

#[cfg(test)]
mod tests {
    use super::*;

    fn summary_gate() -> SummaryGate {
        SummaryGate {
            in_restricted_sql: false,
            user_name: "root".to_owned(),
            in_explain_explore: false,
            summary_enabled: true,
            summary_internal_enabled: false,
            stmt_kind: SummaryStmtKind::Other,
            prev_stmt_digest: String::new(),
        }
    }

    #[test]
    fn missing_user_counts_as_internal() {
        assert!(is_internal_sql(false, "", false));
        assert!(is_internal_sql(true, "root", false));
        assert!(!is_internal_sql(false, "root", false));
    }

    #[test]
    fn explain_explore_forces_internal_sql_back_to_user_visible() {
        assert!(!is_internal_sql(true, "", true));
    }

    #[test]
    fn disabled_summary_clears_prev_digest() {
        let gate = SummaryGate {
            summary_enabled: false,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::ClearPrevDigestAndSkip
        );
    }

    #[test]
    fn internal_sql_needs_the_internal_switch() {
        let gate = SummaryGate {
            in_restricted_sql: true,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::ClearPrevDigestAndSkip
        );
        let enabled = SummaryGate {
            summary_internal_enabled: true,
            ..gate
        };
        assert_eq!(
            decide_summary_stmt(&enabled),
            SummaryAction::Record {
                is_internal: true,
                attributed_prev_digest: None,
            }
        );
    }

    #[test]
    fn prepare_is_skipped_without_clearing_the_digest() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Prepare,
            prev_stmt_digest: "abc".to_owned(),
            ..summary_gate()
        };
        assert_eq!(decide_summary_stmt(&gate), SummaryAction::SkipPrepare);
    }

    #[test]
    fn commit_without_prev_digest_is_ignored() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Commit,
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::SkipCommitWithoutPrevDigest
        );
    }

    #[test]
    fn commit_is_attributed_to_the_previous_statement() {
        let gate = SummaryGate {
            stmt_kind: SummaryStmtKind::Commit,
            prev_stmt_digest: "abc".to_owned(),
            ..summary_gate()
        };
        assert_eq!(
            decide_summary_stmt(&gate),
            SummaryAction::Record {
                is_internal: false,
                attributed_prev_digest: Some("abc".to_owned()),
            }
        );
    }
}

/// Go `handleNoDelayExecutor`'s switch on the executor (`DeleteExec`,
/// `InsertExec`, `UpdateExec`, `ReplaceExec`, `LoadDataExec`, `DDLExec`,
/// `ImportIntoExec`), which `handleNoDelay` hands an `EXPLAIN ANALYZE`'s
/// analyzed executor.
#[must_use]
pub fn is_write_executor(stmt: &tidb_ast::Stmt) -> bool {
    match stmt {
        tidb_ast::Stmt::Dml(dml) => matches!(
            dml.as_ref(),
            tidb_ast::DmlStmt::Insert(_)
                | tidb_ast::DmlStmt::Update(_)
                | tidb_ast::DmlStmt::Delete(_)
                | tidb_ast::DmlStmt::ImportInto(_)
                | tidb_ast::DmlStmt::LoadData(_)
        ),
        tidb_ast::Stmt::Ddl(_) => true,
        tidb_ast::Stmt::Admin(admin) => match admin.as_ref() {
            tidb_ast::AdminStmt::Explain(explain) if explain.analyze => {
                explain.statement().is_some_and(is_write_executor)
            }
            _ => false,
        },
        _ => false,
    }
}

/// Go `ExecStmt.isSelectForUpdate`: a SELECT whose lock the executor builder
/// keeps (`hasLock`), which `PessimisticLockEligible` allows only inside a
/// transaction or with autocommit off, and which a `FOR SHARE` the no-op
/// switch admitted never takes.
pub fn is_select_for_update(session: &impl crate::stmt_ctx_build::StmtCtxSession, stmt: &tidb_ast::Stmt) -> bool {
    let tidb_ast::Stmt::Query(query) = stmt else {
        return false;
    };
    let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
        return false;
    };
    let Some(lock) = &select.lock else {
        return false;
    };
    let takes_lock = match lock.kind {
        tidb_ast::LockKind::Update => true,
        tidb_ast::LockKind::Share => {
            crate::sessionctx::SessionContext::session_vars(session).shared_lock_promotion()
        }
    };
    takes_lock && (!session.is_autocommit() || session.in_transaction())
}

/// Go `ExecStmt`'s refusals for a session reading at `tidb_snapshot` or a
/// low-resolution TSO, once the statement has compiled: a write executor
/// (`handleNoDelay`, `adapter.go:1152`), and a SELECT whose lock is taken --
/// under `tidb_snapshot` in a pessimistic transaction only
/// (`handlePessimisticSelectForUpdate`, `adapter.go:1067`; `Exec`,
/// `adapter.go:830`).
///
/// # Errors
///
/// Go's refusal.
pub fn check_no_delay_snapshot_writes(
    session: &impl crate::stmt_ctx_build::StmtCtxSession,
    stmt: &tidb_ast::Stmt,
) -> Result<(), crate::DriverError> {
    let vars = crate::sessionctx::SessionContext::session_vars(session);
    let writes = is_write_executor(stmt);
    if vars.snapshot_ts() != 0
        && (writes || (is_select_for_update(session, stmt) && session.statement_txn_mode().is_pessimistic()))
    {
        return Err(crate::DriverError::unsupported(
            "can not execute write statement when 'tidb_snapshot' is set",
        ));
    }
    if vars.use_low_resolution_tso() {
        if writes {
            return Err(crate::DriverError::unsupported(
                "can not execute write statement when 'tidb_low_resolution_tso' is set",
            ));
        }
        if is_select_for_update(session, stmt) {
            return Err(crate::DriverError::unsupported(
                "can not execute select for update statement when 'tidb_low_resolution_tso' is set",
            ));
        }
    }
    Ok(())
}

/// Go `handlePessimisticDML`'s step after one execution of the statement.
#[derive(Debug)]
pub enum PessimisticStep {
    /// The statement stands; its keys are locked.
    Done,
    /// Re-execute the statement reading at this advanced `for_update_ts`.
    Retry {
        /// Go's `lockErr`, for the provider's decision.
        lock_error: tidb_store_driver_error::pessimistic_lock_error::LockSqlError,
    },
}

/// What Go's `handlePessimisticDML` lock step reaches through `a.Ctx`: the
/// statement's transaction, its staged writes and the session's lock state.
pub trait PessimisticDmlSession {
    /// Whether the statement's transaction (the explicit one, or a
    /// pessimistic autocommit statement's implicit one) is pessimistic;
    /// `None` without one.
    fn statement_txn_pessimistic(&self) -> Option<bool>;
    /// Whether an explicit transaction is open and optimistic.
    fn explicit_txn_optimistic(&self) -> bool;
    /// The transaction's staged writes (`txn.GetMemBuffer()`).
    fn statement_buffer(&self) -> &tidb_store_driver_txn::cluster_storage::MutationBuffer;
    /// The rows the statement's lock operators read, which the lock step
    /// takes (`StmtCtx`'s selected lock keys).
    fn take_statement_selected_locks(&mut self) -> tidb_sessionctx::select_lock::TakenLocks;
    /// Go `SessionVars.ForeignKeyCheckInSharedLock`.
    fn foreign_key_check_in_shared_lock(&self) -> bool;
    /// Go `getLockWaitTime`: the session's `innodb_lock_wait_timeout`.
    fn statement_lock_wait(&self) -> tidb_txnkv::transaction::LockWaitTime;
    /// Go `TxnCtx.TemporaryTables`.
    fn statement_temporary_tables(&self) -> &tidb_tblctx::temporary_table::TemporaryTables;
    /// Go `TxnCtx` lock-waiting state, published while a lock waits.
    fn publish_lock_waiting(&self, waiting: bool);
    /// Go `handlePessimisticLockError` / `AddAssertEntranceForLockError`.
    fn record_lock_error(&mut self, code: u16);
    /// Go `txn.LockKeys` of the statement's transaction (`shared`:
    /// `LockKeysInShareMode`); `None` without one.
    fn lock_txn_keys(
        &self,
        keys: Vec<Vec<u8>>,
        presume_not_exists: std::collections::BTreeSet<Vec<u8>>,
        duplicate_hints: std::collections::BTreeMap<Vec<u8>, tidb_kv::cluster_storage::DuplicateKeyHint>,
        wait: tidb_txnkv::transaction::LockWaitTime,
        shared: bool,
    ) -> Option<Result<tidb_store_driver_txn::cluster_table_storage::LockKeysOutcome, String>>;

    /// Go `handlePessimisticDML`'s lock step for the statement that just
    /// ran: `KeysNeedToLock` plus `CollectUnchangedKeysForXLock` in one
    /// `LockKeys`, the foreign-key checks' shared keys after them.
    ///
    /// # Errors
    ///
    /// The lock's statement or transaction error.
    fn lock_pessimistic_statement_keys(
        &mut self,
        savepoint: &tidb_store_driver_txn::cluster_storage::BufferCheckpoint,
    ) -> Result<PessimisticStep, tidb_store_driver_error::sql_query_error::SqlQueryError> {
        let Some(pessimistic) = self.statement_txn_pessimistic() else {
            return Ok(PessimisticStep::Done);
        };
        if !pessimistic {
            self.lock_optimistic_statement_keys();
            return Ok(PessimisticStep::Done);
        }
        // The rows a plan's `SelectLockExec` read were locked inside
        // execution, and a key the transaction already holds takes no second
        // request, so what is left here is what the plan did not lock: a
        // statement whose lock operator never saw its child drained, and the
        // keys it wrote.
        let taken = self.take_statement_selected_locks();
        let mut requests = taken.reads;
        let mut keys = self.statement_buffer().pessimistic_keys_since(*savepoint);
        keys.extend(taken.unchanged);
        // With `tidb_foreign_key_check_in_shared_lock` off, Go locks the
        // foreign-key checks' keys exclusively with the others; on, it takes
        // them in share mode after the exclusive locks, except a key this
        // transaction wrote (`moveWrittenSharedLockKeysToExclusive`).
        let mut shared_keys = Vec::new();
        if self.foreign_key_check_in_shared_lock() {
            let exclusive: std::collections::BTreeSet<Vec<u8>> = keys.iter().cloned().collect();
            for key in taken.unchanged_shared {
                if exclusive.contains(&key) {
                    continue;
                }
                // A key written by this transaction must not be protected
                // only by a shared lock: prewrite would put over it.
                if self.statement_buffer().get(&tidb_txnkv::Key::from(key.clone())).is_some() {
                    keys.push(key);
                } else {
                    shared_keys.push(key);
                }
            }
        } else {
            keys.extend(taken.unchanged_shared);
        }
        keys.sort();
        keys.dedup();
        let wait = self.statement_lock_wait();
        if !keys.is_empty() {
            requests.push((wait, keys));
        }
        for (wait, keys) in requests {
            match self.lock_pessimistic_keys(savepoint, keys, wait, false)? {
                PessimisticStep::Done => {}
                retry => return Ok(retry),
            }
        }
        if !shared_keys.is_empty() {
            return self.lock_pessimistic_keys(savepoint, shared_keys, wait, true);
        }
        Ok(PessimisticStep::Done)
    }

    /// Go `SelectLockExec.Next` -> `doLockKeys` in an optimistic
    /// transaction: `LockKeys` takes no lock and only flags the keys the
    /// statement's lock operators published (see
    /// `MutationBuffer::lock_keys_optimistically`).
    fn lock_optimistic_statement_keys(&mut self) {
        if !self.explicit_txn_optimistic() {
            return;
        }
        let taken = self.take_statement_selected_locks();
        let keys: Vec<Vec<u8>> = taken.reads.into_iter().flat_map(|(_, keys)| keys).collect();
        self.statement_buffer().lock_keys_optimistically(&keys);
    }

    /// Go `doLockKeys` -> `txn.LockKeys` of `keys` and what
    /// `handlePessimisticLockError` makes of the outcome.
    ///
    /// # Errors
    ///
    /// The lock's statement or transaction error; the statement's writes go.
    fn lock_pessimistic_keys(
        &mut self,
        savepoint: &tidb_store_driver_txn::cluster_storage::BufferCheckpoint,
        keys: Vec<Vec<u8>>,
        wait: tidb_txnkv::transaction::LockWaitTime,
        shared: bool,
    ) -> Result<PessimisticStep, tidb_store_driver_error::sql_query_error::SqlQueryError> {
        use tidb_store_driver_txn::cluster_table_storage::LockKeysOutcome;
        // Go `tryLockKeys`' `filterTemporaryTableKeys`: a temporary table's
        // rows are never locked in the store.
        let keys = tidb_tblctx::temporary_table::filter_temporary_table_keys(self.statement_temporary_tables(), keys);
        if self.statement_txn_pessimistic().is_none() {
            return Ok(PessimisticStep::Done);
        }
        // Table-selected native flags decide which keys need locking. A
        // live session switch cannot override an earlier key's selected policy.
        let key_set: std::collections::BTreeSet<Vec<u8>> = keys.iter().cloned().collect();
        let presume_not_exists = self
            .statement_buffer()
            .presume_not_exists_since(*savepoint)
            .into_iter()
            .filter(|key| key_set.contains(key))
            .collect::<std::collections::BTreeSet<_>>();
        // Ownership removes a repeated acquisition, not a NEW absence check.
        // Go KVTxn.LockKeys verifies NeedCheckExists even on a held key.
        if keys.is_empty() {
            return Ok(PessimisticStep::Done);
        }
        // Go `ExtractKeyExistsErrFromHandle` also decodes keys staged by earlier statements.
        let duplicate_hints = keys
            .iter()
            .filter_map(|key| {
                self.statement_buffer()
                    .duplicate_key_hint_for(key)
                    .map(|hint| (key.clone(), hint))
            })
            .collect();
        // Every error exit rolls the STATEMENT back -- Go's `StmtRollback`
        // runs on any statement error, transport failures included.
        self.publish_lock_waiting(true);
        let lock_result = self.lock_txn_keys(keys, presume_not_exists, duplicate_hints, wait, shared);
        self.publish_lock_waiting(false);
        let outcome = match lock_result {
            None => return Ok(PessimisticStep::Done),
            Some(Ok(outcome)) => outcome,
            Some(Err(error)) => {
                self.statement_buffer().restore(*savepoint);
                return Err(tidb_store_driver_error::sql_query_error::SqlQueryError::unknown(error));
            }
        };
        match outcome {
            LockKeysOutcome::Locked { .. } => Ok(PessimisticStep::Done),
            LockKeysOutcome::RetryStatement { lock_error, .. } => {
                self.record_lock_error(tidb_errno::errcode::ErrWriteConflict);
                Ok(PessimisticStep::Retry { lock_error })
            }
            LockKeysOutcome::StatementError(error) => {
                self.record_lock_error(error.code);
                // Statement-scoped, Go's 1205/1213 family: this statement's
                // writes go, the transaction stays open.
                self.statement_buffer().restore(*savepoint);
                Err(tidb_store_driver_error::sql_query_error::lock_sql_error(&error))
            }
            LockKeysOutcome::TransactionError(error) => {
                self.record_lock_error(error.code);
                // The transaction is no longer usable; later statements and
                // `ROLLBACK` report that state on their own.
                self.statement_buffer().restore(*savepoint);
                Err(tidb_store_driver_error::sql_query_error::lock_sql_error(&error))
            }
        }
    }
}
