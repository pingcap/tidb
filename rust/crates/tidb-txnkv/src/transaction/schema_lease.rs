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

//! The commit-time schema lease check.
//!
//! client-go `twoPhaseCommitter.checkSchemaValid` (`2pc.go:2149-2168`): a
//! transaction carries the session's `SchemaLeaseChecker` (Go
//! `kv.SchemaChecker`, set by `SetOptionsBeforeCommit`,
//! `pkg/sessiontxn/isolation/base.go:560-616`), and the committer asks it
//! whether the schema the transaction was planned against is still the one
//! in force at the timestamp the commit will carry. A transaction with no
//! checker commits unchecked -- Go: "Schema check is not mandatory since MDL
//! is introduced".
//!
//! The checker itself lives above this crate (it needs the domain's schema
//! validator); this crate only knows when to ask and what to do with the
//! answer: the three client-go call sites, in [`super::coordinator`].

use std::sync::Arc;

/// Go `domain.ErrInfoSchemaChanged` (8028) or `ErrInfoSchemaExpired` (8027),
/// already rendered the way the session reports it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaLeaseError {
    /// The MySQL error code.
    pub code: u16,
    /// The client-visible message, `[class:code]` prefix included.
    pub message: String,
}

/// Go `kv.SchemaChecker` / client-go `SchemaLeaseChecker`.
pub trait SchemaLeaseChecker: Send + Sync {
    /// Go `CheckBySchemaVer(txnTS, startInfoSchema)`: whether a transaction
    /// planned at the checker's schema version may commit at `check_ts`,
    /// having written `related_physical_table_ids`.
    ///
    /// # Errors
    ///
    /// The rendered `ErrInfoSchemaChanged` or `ErrInfoSchemaExpired`.
    fn check_by_schema_ver(
        &self,
        check_ts: u64,
        related_physical_table_ids: &[i64],
    ) -> Result<(), SchemaLeaseError>;
}

/// What one transaction carries to its commit: the session's checker and
/// the physical tables the transaction wrote (Go `TxnCtx.TableDeltaMap`'s
/// keys, `base.go:585-596`).
#[derive(Clone)]
pub struct SchemaLease {
    /// The session's checker.
    pub checker: Arc<dyn SchemaLeaseChecker>,
    /// The physical table (or partition) IDs the transaction wrote.
    pub related_physical_table_ids: Vec<i64>,
}

impl SchemaLease {
    /// Go `checkSchemaValid` for a transaction that carries a checker.
    pub(super) fn check(&self, check_ts: u64) -> Result<(), SchemaLeaseError> {
        self.checker
            .check_by_schema_ver(check_ts, &self.related_physical_table_ids)
    }
}

impl std::fmt::Debug for SchemaLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SchemaLease")
            .field(
                "related_physical_table_ids",
                &self.related_physical_table_ids,
            )
            .finish_non_exhaustive()
    }
}
