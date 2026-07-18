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

//! Dependency-closed admission policy for TiDB's non-transactional DML.
//!
//! `pkg/session/nontransactional.go::checkConstraint` runs before any shard
//! planning or worker starts. Its session-facing part is deliberately kept
//! separate from the transaction implementation here: callers provide a
//! snapshot of autocommit/transaction and compatibility-variable facts, and
//! this module returns a typed decision. It does not open or commit a
//! transaction, inspect a catalog, choose a shard column, run jobs, publish
//! metrics, or implement the failpoint/error aggregation path.

/// The inner DML families admitted by the source `BATCH` wrapper.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NonTransactionalDmlKind {
    /// `BATCH ... INSERT ... SELECT ...`.
    InsertSelect,
    /// `BATCH ... REPLACE ... SELECT ...` (the Go AST uses `InsertStmt` with
    /// `IsReplace`, so it follows the same admission branch).
    ReplaceSelect,
    /// `BATCH ... UPDATE ...`.
    Update,
    /// `BATCH ... DELETE ...`.
    Delete,
    /// An `INSERT`/`REPLACE` whose source is not a `SELECT`.
    InsertWithoutSelect,
    /// A statement outside the source `ShardableDMLStmt` set.
    Unsupported,
}
/// The session and compatibility-variable facts consulted before a
/// non-transactional DML statement is admitted.
///
/// The values are intentionally copied in by the session owner. In
/// particular, [`in_txn`](Self::in_txn) is an observation, not a transaction
/// lifecycle transition; `transaction.rs` remains the sole owner of the
/// in-memory transaction phase in this executor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonTransactionalSessionState {
    /// Whether the session has `@@autocommit = 1`.
    pub autocommit: bool,
    /// Whether `SessionVars.InTxn()` is currently true.
    pub in_txn: bool,
    /// Global `tidb_enable_batch_dml` gate.
    pub batch_dml_enabled: bool,
    /// Session `tidb_dml_batch_size` value.
    pub dml_batch_size: u64,
    /// Session `tidb_batch_delete` value.
    pub batch_delete: bool,
    /// Session `tidb_batch_insert` value.
    pub batch_insert: bool,
    /// Whether `tidb_read_consistency` is weak.
    pub weak_read_consistency: bool,
    /// Session `tidb_snapshot` timestamp; zero means no pinned snapshot.
    pub snapshot_ts: u64,
}

impl Default for NonTransactionalSessionState {
    fn default() -> Self {
        Self {
            autocommit: true,
            in_txn: false,
            batch_dml_enabled: false,
            dml_batch_size: 0,
            batch_delete: false,
            batch_insert: false,
            weak_read_consistency: false,
            snapshot_ts: 0,
        }
    }
}

/// Why the source admission check rejected a non-transactional statement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NonTransactionalAdmissionError {
    /// Non-transactional DML requires autocommit and no active transaction.
    NotAutocommit {
        /// Observed `@@autocommit` value.
        autocommit: bool,
        /// Observed `InTxn()` value.
        in_txn: bool,
    },
    /// The compatibility batch-DML mode is already active for insert/delete.
    BatchDmlAlreadyEnabled,
    /// Weak reads cannot be combined with non-transactional writes.
    WeakReadConsistency,
    /// A pinned `tidb_snapshot` cannot be used for non-transactional writes.
    SnapshotPinned,
    /// An insert/replace must use a SELECT source.
    InsertRequiresSelect,
    /// The wrapped statement is not one of the admitted DML families.
    UnsupportedStatement,
}

/// Checks the dependency-free part of `checkConstraint`.
pub fn admit_non_transactional_dml(
    state: NonTransactionalSessionState,
    kind: NonTransactionalDmlKind,
) -> Result<(), NonTransactionalAdmissionError> {
    if !(state.autocommit && !state.in_txn) {
        return Err(NonTransactionalAdmissionError::NotAutocommit {
            autocommit: state.autocommit,
            in_txn: state.in_txn,
        });
    }
    if state.batch_dml_enabled
        && state.dml_batch_size > 0
        && (state.batch_delete || state.batch_insert)
    {
        return Err(NonTransactionalAdmissionError::BatchDmlAlreadyEnabled);
    }
    if state.weak_read_consistency {
        return Err(NonTransactionalAdmissionError::WeakReadConsistency);
    }
    if state.snapshot_ts != 0 {
        return Err(NonTransactionalAdmissionError::SnapshotPinned);
    }

    match kind {
        NonTransactionalDmlKind::InsertSelect
        | NonTransactionalDmlKind::ReplaceSelect
        | NonTransactionalDmlKind::Update
        | NonTransactionalDmlKind::Delete => Ok(()),
        NonTransactionalDmlKind::InsertWithoutSelect => {
            Err(NonTransactionalAdmissionError::InsertRequiresSelect)
        }
        NonTransactionalDmlKind::Unsupported => {
            Err(NonTransactionalAdmissionError::UnsupportedStatement)
        }
    }
}
