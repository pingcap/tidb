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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-owned transaction lifecycle for the in-memory executor seed.
//!
//! Go's `pkg/session/txn.go` represents the real lazy KV transaction as
//! invalid, pending, or valid. This seed has no TSO future, KV transaction,
//! write set, or commit protocol, so it must not borrow those names and imply
//! protocol parity. Its complete current state space is smaller: either no
//! rollback image exists, or one active in-memory transaction owns its initial
//! catalog image and every savepoint image. [`TransactionPhase`] makes that
//! invariant structural; savepoints cannot exist beside an idle transaction.
//!
//! The transition order follows the owning Go lifecycle and tests:
//! `pkg/session/test/txn/txn_test.go::{TestAutocommit,
//! TestTxnLazyInitialize,TestInTrans}`, `pkg/executor/simple.go`'s
//! BEGIN/COMMIT/ROLLBACK handlers, and the autocommit/one-shot isolation
//! setters in `pkg/sessionctx/variable/{sysvar.go,session.go}`. The catalog
//! images are only a temporary rollback model. They are not a
//! `tidb-txnkv` client or an MVCC snapshot.

use std::collections::BTreeMap;

use tidb_ast::{Join, JoinNode, QueryStmt, SetOprStmt, SetOprTermBody};

use crate::{ExecError, Table};

/// Whether evaluating a query reads catalog table state.
///
/// `TestTxnLazyInitialize` distinguishes a table-free `SELECT 1`, which
/// leaves an autocommit-off session idle, from `SELECT * FROM t`, which
/// establishes the transaction. Derived tables and set operations recurse to
/// the same base-table fact.
pub(crate) fn query_reads_base_table(query: &QueryStmt) -> bool {
    match query {
        QueryStmt::Select(select) => select.from.as_ref().is_some_and(join_reads_base_table),
        QueryStmt::SetOpr(setopr) => setopr_reads_base_table(setopr),
    }
}

fn setopr_reads_base_table(setopr: &SetOprStmt) -> bool {
    setopr.terms.iter().any(|term| match &term.body {
        SetOprTermBody::Select(select) => select.from.as_ref().is_some_and(join_reads_base_table),
        SetOprTermBody::Nested(setopr) => setopr_reads_base_table(setopr),
    })
}

fn join_reads_base_table(join: &Join) -> bool {
    join_node_reads_base_table(&join.left)
        || join.right.as_ref().is_some_and(join_node_reads_base_table)
}

fn join_node_reads_base_table(node: &JoinNode) -> bool {
    match node {
        JoinNode::Table(_) => true,
        JoinNode::Derived { subquery, .. } => query_reads_base_table(subquery),
        JoinNode::Join(join) => join_reads_base_table(join),
    }
}

/// Per-session transaction settings plus the seed's current lifecycle phase.
#[derive(Debug, Clone, Default)]
pub(crate) struct TransactionState {
    /// The session autocommit status. `Database::new` uses [`Self::new`] to
    /// install TiDB's real default; derived `Default` intentionally preserves
    /// the pre-extraction `Database::default()` behavior.
    autocommit: bool,
    /// Session isolation readback. Storage-level isolation is not implemented.
    session_isolation: String,
    /// Readback for `SET TRANSACTION ISOLATION LEVEL ...`.
    one_shot_isolation: String,
    phase: TransactionPhase,
}

/// The complete lifecycle state that the in-memory seed can represent.
#[derive(Debug, Clone, Default)]
enum TransactionPhase {
    #[default]
    Idle,
    Active(ActiveTransaction),
}

/// Rollback data that can only exist while a seed transaction is active.
#[derive(Debug, Clone)]
struct ActiveTransaction {
    rollback_catalog: BTreeMap<String, Table>,
    savepoints: Vec<Savepoint>,
}

#[derive(Debug, Clone)]
struct Savepoint {
    name: String,
    catalog: BTreeMap<String, Table>,
}

impl TransactionState {
    /// Creates TiDB's normal out-of-the-box session transaction settings.
    pub(crate) fn new() -> Self {
        Self {
            autocommit: true,
            session_isolation: "REPEATABLE-READ".to_string(),
            one_shot_isolation: String::new(),
            phase: TransactionPhase::Idle,
        }
    }

    pub(crate) fn autocommit(&self) -> bool {
        self.autocommit
    }

    pub(crate) fn session_isolation(&self) -> &str {
        &self.session_isolation
    }

    pub(crate) fn one_shot_isolation(&self) -> &str {
        &self.one_shot_isolation
    }

    pub(crate) fn is_active(&self) -> bool {
        matches!(self.phase, TransactionPhase::Active(_))
    }

    /// Starts a fresh explicit transaction over the current catalog.
    /// Replacing an active phase commits its live catalog changes by dropping
    /// only the old rollback image, matching a second Go BEGIN.
    pub(crate) fn begin(&mut self, tables: &BTreeMap<String, Table>) {
        self.phase = TransactionPhase::Active(ActiveTransaction {
            rollback_catalog: tables.clone(),
            savepoints: Vec::new(),
        });
    }

    /// Ends the active seed transaction without changing the live catalog.
    pub(crate) fn commit(&mut self) {
        self.phase = TransactionPhase::Idle;
    }

    /// Restores the initial catalog image, if any, and ends the transaction.
    pub(crate) fn rollback(&mut self, tables: &mut BTreeMap<String, Table>) {
        if let TransactionPhase::Active(active) = std::mem::take(&mut self.phase) {
            *tables = active.rollback_catalog;
        }
    }

    /// Lazily starts the seed transaction when autocommit is disabled and a
    /// table-backed statement or SAVEPOINT actually needs it.
    pub(crate) fn ensure_implicit(&mut self, tables: &BTreeMap<String, Table>) {
        if !self.autocommit && !self.is_active() {
            self.begin(tables);
        }
    }

    /// Applies a session autocommit change. Only a real false-to-true
    /// transition commits; a redundant true while an explicit transaction is
    /// active deliberately leaves that transaction open.
    pub(crate) fn set_autocommit(&mut self, enabled: bool) {
        if enabled && !self.autocommit {
            self.commit();
        }
        self.autocommit = enabled;
    }

    pub(crate) fn set_session_isolation(&mut self, isolation: String) {
        self.session_isolation = isolation;
    }

    pub(crate) fn set_one_shot_isolation(&mut self, isolation: String) -> Result<(), ExecError> {
        if self.is_active() {
            return Err(ExecError::Unsupported(
                "SET tx_isolation_one_shot while a transaction is in progress",
            ));
        }
        self.one_shot_isolation = isolation;
        Ok(())
    }

    /// Records a savepoint if a transaction exists. With autocommit disabled,
    /// SAVEPOINT itself lazily opens that transaction; with autocommit enabled
    /// and no explicit transaction it remains a source-compatible no-op.
    pub(crate) fn savepoint(&mut self, name: &str, tables: &BTreeMap<String, Table>) {
        self.ensure_implicit(tables);
        let TransactionPhase::Active(active) = &mut self.phase else {
            return;
        };
        active
            .savepoints
            .retain(|savepoint| !savepoint.name.eq_ignore_ascii_case(name));
        active.savepoints.push(Savepoint {
            name: name.to_string(),
            catalog: tables.clone(),
        });
    }

    pub(crate) fn rollback_to_savepoint(
        &mut self,
        name: &str,
        tables: &mut BTreeMap<String, Table>,
    ) -> Result<(), ExecError> {
        let TransactionPhase::Active(active) = &mut self.phase else {
            return Err(ExecError::UnknownSavepoint(name.to_string()));
        };
        let index = active
            .savepoints
            .iter()
            .position(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| ExecError::UnknownSavepoint(name.to_string()))?;
        *tables = active.savepoints[index].catalog.clone();
        active.savepoints.truncate(index + 1);
        Ok(())
    }

    pub(crate) fn release_savepoint(&mut self, name: &str) -> Result<(), ExecError> {
        let TransactionPhase::Active(active) = &mut self.phase else {
            return Err(ExecError::UnknownSavepoint(name.to_string()));
        };
        let index = active
            .savepoints
            .iter()
            .position(|savepoint| savepoint.name.eq_ignore_ascii_case(name))
            .ok_or_else(|| ExecError::UnknownSavepoint(name.to_string()))?;
        active.savepoints.truncate(index);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn savepoint_count(&self) -> usize {
        match &self.phase {
            TransactionPhase::Idle => 0,
            TransactionPhase::Active(active) => active.savepoints.len(),
        }
    }
}
