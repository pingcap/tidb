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

//! Which concurrency-control mode a transaction runs in.
//!
//! Two Go functions decide this, and they do not agree, so both are ported:
//! `pkg/session/txnmanager.go`'s `newProviderWithRequest` chooses the provider
//! for an explicit `BEGIN`, and `pkg/session.decideTxnMode` chooses it for the
//! statement that implicitly opens a transaction. The difference is the whole
//! point of the autocommit rule below.

pub use tidb_ast::TransactionMode;

/// Go `vardef.PessimisticTxnMode`.
pub const PESSIMISTIC_TXN_MODE: &str = "pessimistic";
/// Go `vardef.OptimisticTxnMode`.
pub const OPTIMISTIC_TXN_MODE: &str = "optimistic";

/// The mode a transaction actually runs in, once every default is resolved.
///
/// Go carries this as a string that may also be empty; every consumer treats
/// the empty string as optimistic, so the resolved form has only two states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionTxnMode {
    /// No read-time locks; conflicts are detected by the two-phase commit.
    Optimistic,
    /// Locking statements take TiKV pessimistic locks at `for_update_ts`.
    Pessimistic,
}

impl SessionTxnMode {
    /// Whether locking statements in this mode acquire pessimistic locks.
    #[must_use]
    pub const fn is_pessimistic(self) -> bool {
        matches!(self, Self::Pessimistic)
    }

    /// Go's string spelling, as `@@tidb_txn_mode` reports it.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Optimistic => OPTIMISTIC_TXN_MODE,
            Self::Pessimistic => PESSIMISTIC_TXN_MODE,
        }
    }
}

/// Reads `@@tidb_txn_mode` the way every Go consumer reads it.
///
/// Go compares against `ast.Pessimistic` and sends everything else -- the
/// empty string included, which `SET tidb_txn_mode = ''` really can store
/// because the variable is `AllowEmptyAll` -- down the optimistic path.
#[must_use]
pub fn txn_mode_variable(value: &str) -> SessionTxnMode {
    if value == PESSIMISTIC_TXN_MODE {
        SessionTxnMode::Pessimistic
    } else {
        SessionTxnMode::Optimistic
    }
}

/// Go `newProviderWithRequest`: the mode an explicit `BEGIN` opens in.
///
/// `BEGIN OPTIMISTIC` / `BEGIN PESSIMISTIC` name the mode outright; a bare
/// `BEGIN` or `START TRANSACTION` falls back to `@@tidb_txn_mode`. Captured
/// from TiDB: with `tidb_txn_mode = 'optimistic'`, `BEGIN PESSIMISTIC` still
/// takes row locks, and vice versa.
#[must_use]
pub fn txn_mode_for_begin(mode: TransactionMode, txn_mode_var: &str) -> SessionTxnMode {
    match mode {
        TransactionMode::Optimistic => SessionTxnMode::Optimistic,
        TransactionMode::Pessimistic => SessionTxnMode::Pessimistic,
        TransactionMode::Default => txn_mode_variable(txn_mode_var),
    }
}

/// Everything Go `decideTxnMode` reads about the statement about to run.
#[derive(Clone, Copy, Debug)]
pub struct StatementTxnModeInputs<'a> {
    /// `@@tidb_txn_mode`.
    pub txn_mode_var: &'a str,
    /// `SessionVars.RetryInfo.Retrying`: this statement is a retry of one the
    /// optimistic commit path already failed.
    pub retrying: bool,
    /// `@@autocommit` with no explicit transaction open.
    pub autocommit: bool,
    /// The `pessimistic-txn.pessimistic-auto-commit` config, `false` by
    /// default, so the common autocommit DML is optimistic.
    pub pessimistic_auto_commit: bool,
    /// Whether the statement is DML (`INSERT`/`UPDATE`/`DELETE`/...), which is
    /// all `pessimistic-auto-commit` applies to.
    pub is_dml: bool,
}

/// Go `decideTxnMode`: the mode for a statement that opens its own transaction.
///
/// The surprise this encodes is that a plain autocommit `UPDATE` under the
/// default `tidb_txn_mode = 'pessimistic'` runs **optimistically**: only a
/// retry, or the non-default `pessimistic-auto-commit` config, makes it
/// pessimistic. Captured from TiDB's mock store, where an autocommit `UPDATE`
/// against a row another session holds a pessimistic lock on first fails
/// Prewrite with `9007 ... reason=Optimistic`, and only the automatic retry
/// (which is pessimistic) reports `1205`.
#[must_use]
pub fn txn_mode_for_statement(inputs: StatementTxnModeInputs<'_>) -> SessionTxnMode {
    if inputs.retrying {
        return SessionTxnMode::Pessimistic;
    }
    if txn_mode_variable(inputs.txn_mode_var) == SessionTxnMode::Optimistic {
        return SessionTxnMode::Optimistic;
    }
    if !inputs.autocommit {
        return SessionTxnMode::Pessimistic;
    }
    if inputs.pessimistic_auto_commit && inputs.is_dml {
        return SessionTxnMode::Pessimistic;
    }
    SessionTxnMode::Optimistic
}

#[cfg(test)]
mod tests {
    use super::{
        txn_mode_for_begin, txn_mode_for_statement, txn_mode_variable, SessionTxnMode,
        StatementTxnModeInputs,
    };
    use tidb_ast::TransactionMode;

    fn statement(txn_mode_var: &str) -> StatementTxnModeInputs<'_> {
        StatementTxnModeInputs {
            txn_mode_var,
            retrying: false,
            autocommit: true,
            pessimistic_auto_commit: false,
            is_dml: true,
        }
    }

    #[test]
    fn only_the_exact_pessimistic_spelling_is_pessimistic() {
        assert_eq!(
            txn_mode_variable("pessimistic"),
            SessionTxnMode::Pessimistic
        );
        assert_eq!(txn_mode_variable("optimistic"), SessionTxnMode::Optimistic);
        // `SET tidb_txn_mode = ''` stores the empty string, which Go reads as
        // optimistic rather than as the pessimistic default.
        assert_eq!(txn_mode_variable(""), SessionTxnMode::Optimistic);
    }

    #[test]
    fn an_explicit_begin_mode_overrides_the_variable() {
        assert_eq!(
            txn_mode_for_begin(TransactionMode::Pessimistic, "optimistic"),
            SessionTxnMode::Pessimistic
        );
        assert_eq!(
            txn_mode_for_begin(TransactionMode::Optimistic, "pessimistic"),
            SessionTxnMode::Optimistic
        );
        assert_eq!(
            txn_mode_for_begin(TransactionMode::Default, "pessimistic"),
            SessionTxnMode::Pessimistic
        );
        assert_eq!(
            txn_mode_for_begin(TransactionMode::Default, ""),
            SessionTxnMode::Optimistic
        );
    }

    #[test]
    fn an_autocommit_dml_is_optimistic_even_in_pessimistic_mode() {
        assert_eq!(
            txn_mode_for_statement(statement("pessimistic")),
            SessionTxnMode::Optimistic
        );
        assert_eq!(
            txn_mode_for_statement(StatementTxnModeInputs {
                pessimistic_auto_commit: true,
                ..statement("pessimistic")
            }),
            SessionTxnMode::Pessimistic
        );
        // The config only reaches DML.
        assert_eq!(
            txn_mode_for_statement(StatementTxnModeInputs {
                pessimistic_auto_commit: true,
                is_dml: false,
                ..statement("pessimistic")
            }),
            SessionTxnMode::Optimistic
        );
    }

    #[test]
    fn a_statement_inside_an_open_transaction_follows_the_variable() {
        assert_eq!(
            txn_mode_for_statement(StatementTxnModeInputs {
                autocommit: false,
                ..statement("pessimistic")
            }),
            SessionTxnMode::Pessimistic
        );
        assert_eq!(
            txn_mode_for_statement(StatementTxnModeInputs {
                autocommit: false,
                ..statement("optimistic")
            }),
            SessionTxnMode::Optimistic
        );
    }

    #[test]
    fn a_retry_is_pessimistic_whatever_the_variable_says() {
        assert_eq!(
            txn_mode_for_statement(StatementTxnModeInputs {
                retrying: true,
                ..statement("optimistic")
            }),
            SessionTxnMode::Pessimistic
        );
    }
}
