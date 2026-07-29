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

//! One connection's explicit-transaction state.
//!
//! Models the `BEGIN`/`START TRANSACTION` ... `COMMIT`/`ROLLBACK` lifecycle a
//! session carries across statements (Go `pkg/session` `LazyTxn`/`TxnState`).
//! The state here is deliberately thin: which mode the client asked for, and —
//! once a statement actually needs it — the one real transaction every statement
//! in between runs through, which is owned by
//! [`MultiStatementTransaction`] in the executor.
//!
//! The transaction is opened **lazily**, on the first statement after `BEGIN`
//! rather than by `BEGIN` itself, exactly as TiDB defers a transaction's
//! `start_ts` to first use. A `BEGIN; COMMIT;` pair therefore spends no PD
//! timestamp at all, and every statement in a real transaction shares the one
//! `start_ts` the first of them took.

use tidb_exec::global_sysvar_initial::{
    global_system_variable_initial_value, GlobalSysvarEnvironment, ENABLE_1PC, ENABLE_ASYNC_COMMIT,
    ON, PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
};
use tidb_exec::multi_statement_transaction::{
    MultiStatementTransaction, TransactionEnd, TransactionStatementError,
};
use tidb_planner::txn_mode::{txn_mode_for_begin, SessionTxnMode, TransactionMode};
use tidb_txnkv::transaction::CommitProtocol;

/// Whether `@@tidb_pessimistic_txn_fair_locking` is on for this node.
///
/// The variable's registry value is `OFF`, but that is not what a cluster runs
/// with: Go `GlobalSystemVariableInitialValue` overrides it to `ON` when a
/// classic-kernel cluster is bootstrapped, and that is what
/// `mysql.global_variables` then holds. Captured from TiDB's mock store:
/// `SELECT @@tidb_pessimistic_txn_fair_locking, @@global....` reads `1 1`, and
/// `mysql.global_variables` holds `ON`. This node has no `SET`-able session
/// variable store, so it takes that bootstrap value directly.
#[must_use]
pub fn session_fair_locking() -> bool {
    global_system_variable_initial_value(
        PESSIMISTIC_TRANSACTION_FAIR_LOCKING,
        // The registry default this node would otherwise carry.
        "OFF",
        GlobalSysvarEnvironment {
            store_is_tikv: true,
            in_test: false,
            next_gen: false,
        },
    ) == ON
}

/// The fast-commit protocol this node's transactions may use, from
/// `@@tidb_enable_async_commit` / `@@tidb_enable_1pc`.
///
/// Like fair locking, both variables carry the registry default `OFF`, but Go
/// `GlobalSystemVariableInitialValue` overrides them to `ON` on a real-TiKV
/// cluster (`config.Store == StoreTypeTiKV`), and that is the value
/// `mysql.global_variables` then holds. This node has no `SET`-able session
/// store, so it takes that bootstrap value directly. The commit-time
/// eligibility check still decides whether any given transaction actually uses
/// a fast-commit path.
#[must_use]
pub fn session_commit_protocol() -> CommitProtocol {
    let environment = GlobalSysvarEnvironment {
        store_is_tikv: true,
        in_test: false,
        next_gen: false,
    };
    CommitProtocol {
        enable_async_commit: global_system_variable_initial_value(
            ENABLE_ASYNC_COMMIT,
            "OFF",
            environment,
        ) == ON,
        enable_1pc: global_system_variable_initial_value(ENABLE_1PC, "OFF", environment) == ON,
        ..CommitProtocol::default()
    }
}

/// The explicit-transaction state of one session.
#[derive(Default)]
pub struct SessionTransaction {
    /// The mode the open transaction was opened in. `None` while no transaction
    /// is open, so it doubles as "is a transaction open".
    mode: Option<SessionTxnMode>,
    /// The real transaction, present once a statement has needed it.
    open: Option<MultiStatementTransaction>,
}

impl SessionTransaction {
    /// A session that starts outside any explicit transaction (autocommit).
    #[must_use]
    pub const fn new() -> Self {
        Self {
            mode: None,
            open: None,
        }
    }

    /// Opens an explicit transaction for `BEGIN` / `START TRANSACTION`.
    ///
    /// Re-issuing `BEGIN` while a transaction is already open implicitly
    /// **commits** the current one and starts a fresh transaction, matching
    /// MySQL and Go `pkg/session.Session.NewTxn`. The commit is reported, so a
    /// client whose implicit commit failed is never told the new transaction
    /// started cleanly.
    ///
    /// `mode` is the `BEGIN` statement's own keyword. This node has no
    /// `SET`-able session-variable store, so a bare `BEGIN` resolves against
    /// the registry default of `@@tidb_txn_mode`, which is `pessimistic`.
    pub fn begin(&mut self, mode: TransactionMode) -> Result<(), TransactionStatementError> {
        let previous = self.open.take();
        self.mode = Some(txn_mode_for_begin(
            mode,
            tidb_planner::txn_mode::PESSIMISTIC_TXN_MODE,
        ));
        match previous {
            Some(transaction) => transaction.commit().map(|_| ()),
            None => Ok(()),
        }
    }

    /// Ends the open transaction for `COMMIT` or `ROLLBACK`.
    ///
    /// The session returns to autocommit either way, including when the
    /// transaction failed to commit: the transaction is over regardless, and
    /// leaving it "open" would make the next statement run against a coordinator
    /// that has already terminated. `COMMIT`/`ROLLBACK` outside a transaction is
    /// a no-op, which this expresses by simply clearing already-clear state.
    pub fn end(&mut self, commit: bool) -> Result<TransactionEnd, TransactionStatementError> {
        self.mode = None;
        let Some(transaction) = self.open.take() else {
            return Ok(if commit {
                TransactionEnd::Committed
            } else {
                TransactionEnd::RolledBack
            });
        };
        if commit {
            transaction.commit()
        } else {
            transaction.rollback()
        }
    }

    /// Abandons the open transaction after a failure that ended it.
    ///
    /// A [`TransactionStatementError::Transaction`] means the coordinator is
    /// finished; keeping it would let the next statement run through a dead
    /// transaction. The session returns to autocommit, exactly as it would after
    /// an explicit `ROLLBACK`.
    pub fn abandon(&mut self) {
        self.mode = None;
        self.open = None;
    }

    /// The mode the open transaction runs in, if one is open.
    #[must_use]
    pub const fn mode(&self) -> Option<SessionTxnMode> {
        self.mode
    }

    /// Whether an explicit transaction is open, i.e. the connection should
    /// advertise `SERVER_STATUS_IN_TRANS`.
    #[must_use]
    pub const fn is_active(&self) -> bool {
        self.mode.is_some()
    }

    /// The already-open real transaction, if a statement has needed one yet.
    #[must_use]
    pub const fn opened(&self) -> Option<&MultiStatementTransaction> {
        self.open.as_ref()
    }

    /// The already-open real transaction, for a statement that acts on it.
    pub const fn opened_mut(&mut self) -> Option<&mut MultiStatementTransaction> {
        self.open.as_mut()
    }

    /// The real transaction every statement in this session runs through,
    /// opening it on first use.
    ///
    /// Returns `None` outside an explicit transaction, where each statement is
    /// its own autocommit transaction and takes its own fresh snapshot exactly
    /// as before.
    pub fn opened_or_begin<E>(
        &mut self,
        begin: impl FnOnce(SessionTxnMode) -> Result<MultiStatementTransaction, E>,
    ) -> Result<Option<&mut MultiStatementTransaction>, E> {
        let Some(mode) = self.mode else {
            return Ok(None);
        };
        if self.open.is_none() {
            self.open = Some(begin(mode)?);
        }
        Ok(self.open.as_mut())
    }
}

#[cfg(test)]
mod tests {
    use super::{SessionTransaction, SessionTxnMode, TransactionMode};
    use tidb_exec::multi_statement_transaction::MultiStatementTransaction;

    /// A begin that must never run, for the paths that take no transaction.
    fn unreachable_begin(_: SessionTxnMode) -> Result<MultiStatementTransaction, &'static str> {
        panic!("no transaction may be opened outside an explicit transaction")
    }

    #[test]
    fn a_fresh_session_is_not_in_a_transaction_and_opens_none() {
        let mut txn = SessionTransaction::new();
        assert!(!txn.is_active());
        assert_eq!(txn.mode(), None);
        assert!(txn
            .opened_or_begin(unreachable_begin)
            .expect("autocommit needs no transaction")
            .is_none());
    }

    #[test]
    fn the_begin_keyword_decides_the_mode_and_ending_clears_it() {
        let mut txn = SessionTransaction::new();
        // No SET-able variable store here, so a bare BEGIN takes the registry
        // default of @@tidb_txn_mode.
        txn.begin(TransactionMode::Default).unwrap();
        assert_eq!(txn.mode(), Some(SessionTxnMode::Pessimistic));
        txn.begin(TransactionMode::Optimistic).unwrap();
        assert_eq!(txn.mode(), Some(SessionTxnMode::Optimistic));
        txn.begin(TransactionMode::Pessimistic).unwrap();
        assert_eq!(txn.mode(), Some(SessionTxnMode::Pessimistic));
        txn.end(true).unwrap();
        assert_eq!(txn.mode(), None);
        assert!(!txn.is_active());
    }

    #[test]
    fn a_transaction_that_never_ran_a_statement_costs_no_timestamp() {
        // BEGIN alone opens nothing: the coordinator (and its PD timestamp)
        // appears only when a statement needs it.
        let mut txn = SessionTransaction::new();
        txn.begin(TransactionMode::Optimistic).unwrap();
        assert!(txn.is_active());
        assert!(txn.opened().is_none());
        txn.end(true)
            .expect("committing a transaction that never opened is trivial");
        assert!(!txn.is_active());
    }

    #[test]
    fn commit_and_rollback_outside_a_transaction_are_no_ops() {
        let mut txn = SessionTransaction::new();
        assert!(txn.end(true).is_ok());
        assert!(txn.end(false).is_ok());
        assert!(!txn.is_active());
    }

    #[test]
    fn abandoning_a_failed_transaction_returns_the_session_to_autocommit() {
        let mut txn = SessionTransaction::new();
        txn.begin(TransactionMode::Pessimistic).unwrap();
        txn.abandon();
        assert!(!txn.is_active());
        assert!(txn
            .opened_or_begin(unreachable_begin)
            .expect("an abandoned transaction leaves the session in autocommit")
            .is_none());
    }
}
