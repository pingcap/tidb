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

//! Go `executor.Compiler` and the planner-side caches and binding matching it
//! reaches through `sctx`: the prepared and non-prepared plan caches, the
//! plan-time binding match, `SET_VAR` hints and EXPLAIN. Each module is an
//! extension trait over [`CompilerSession`], the session primitives the
//! compiler reads.

use crate::{Catalog, DriverError};
use tidb_ast::Stmt;

// modules
pub mod explain;
pub mod prepared;
pub mod prepared_ast;
pub mod non_prepared_plan_cache;
pub mod plan_cacheable_checker;

// traits
pub use explain::ExplainCompile;
pub use prepared::PreparedExec;
pub use prepared_ast::PreparedAstCompile;
pub use non_prepared_plan_cache::NonPreparedPlanCacheCompile;
pub use plan_cacheable_checker::PlanCacheableChecker;

/// The session primitives Go's compiler reads through `sctx`.
pub trait CompilerSession: crate::admin::AdminExec {
/// Go `SessionVars`' non-prepared plan cache.
    fn non_prepared_plan_cache_mut(&mut self) -> &mut crate::compiler::non_prepared_plan_cache::NonPreparedPlanCache;

    /// The session's physical plan cache (Go `GetSessionPlanCache`).
    fn physical_plan_cache(&self) -> &crate::SessionPlanCache;

    /// The statement's hints, parsed without catalog metadata.
    fn parse_statement_hints_without_catalog(
        &self,
        stmt: &tidb_ast::Stmt,
        current_database: &str,
    ) -> tidb_hint::StmtHints;



    /// Sizes the session plan cache from its variables.
    fn configure_session_plan_cache(&self);

    /// Go `tidb_session_plan_cache_size`.
    fn session_plan_cache_capacity(&self) -> usize;

/// Whether the statement reads a pinned snapshot (`tidb_snapshot`).

    /// The tables the open transaction has staged writes to.
    fn staged_dirty_table_ids(&self) -> Vec<i64>;

    /// The memo of the prepared plan cache's key environment.
    fn prepared_plan_cache_environment_cache(
        &self,
    ) -> &std::cell::RefCell<Option<crate::compiler::prepared_ast::PreparedPlanCacheEnvironmentCache>>;

    /// Brings a node session's catalog up to the node's latest schema, as
    /// every statement through the node does first; a no-op elsewhere.
    fn refresh_node_catalog(&mut self) -> Result<(), DriverError>;
    /// Effective historical timestamp of the active statement or stale transaction.
    fn historical_read_ts(&self) -> Option<u64>;
    /// Session primitive.
    fn external_timestamp_read_enabled(&self) -> bool;
    /// Session primitive.
    fn collect_table_privileges(&self, stmt: &tidb_ast::Stmt, preparing: bool) -> Result<Vec<crate::table_privilege::TablePrivilegeRequest>, DriverError>;
    /// Checks one statement's `visitInfo`s as Go's optimizer does:
    /// `CheckPrivilege` against the live grants, then `CheckTableLock`.
    fn check_table_privilege_requests(&self, requests: &[crate::table_privilege::TablePrivilegeRequest]) -> Result<(), DriverError>;
/// Go `SessionVars.PreparedStmts` / `PreparedStmtNameToID`.
    fn prepared_statements_mut(&mut self) -> &mut crate::compiler::prepared::PreparedStore;

    /// Go `StmtCtx`'s pending plan-cache refusal, reported at EXECUTE.
    fn pending_plan_cache_refusal_mut(&mut self) -> &mut Option<&'static str>;

    /// Runs Go's `matchAgainstToLike` before physical planning. Prepared
    /// cache misses and ordinary statements both call this method, so the
    /// cache retains the same rewritten tree the normal executor receives.
    fn rewrite_fts_for_planning(&self, stmt: &mut tidb_ast::Stmt) -> Result<(), DriverError>;
    /// [`Self::with_catalog_retrieving`] over any way of running the
    /// statement, such as a staged catalog for a write.
    fn retrieving_memory_tables<T>(&mut self, ctx: &crate::StmtContext, run: impl FnMut(&mut Self) -> Result<T, DriverError>, ) -> Result<T, DriverError>;
    /// Session primitive.
    fn release_prepared_plans(&self, select: Option<&crate::PreparedSelectPlan>, dml: Option<&crate::PreparedDmlPlan>);
    /// Resolves system-variable scope and visibility before shared planning.
    /// User-variable AST nodes remain intact so the rewriter can observe inline
    /// assignments' declared types in source order.
    fn bind_variables(&mut self, stmt: &mut tidb_ast::Stmt) -> Result<(), DriverError>;
    /// Allocates in the namespace shared by SQL PREPARE and COM_STMT_PREPARE.
    fn allocate_prepared_statement_id(&mut self) -> u32;
/// Go `SessionVars.PreparedStmts`, read.
    fn prepared_statements_ref(&self) -> &crate::compiler::prepared::PreparedStore;

    /// Session primitive.
    fn drain_eval_warnings(&mut self, ctx: &crate::StmtContext);
    /// Runs one DML statement over a STAGE of the catalog this statement sees,
    /// so a statement that fails partway leaves the tables as it found them.
    ///
    /// This is Go's statement-level rollback. A statement opens a staging
    /// handle on the transaction's membuffer (`pkg/kv/union_store.go`:
    /// `MemBuffer.Staging()`), writes into it, and
    /// `pkg/executor/adapter.go` chooses between
    /// `pkg/session/session.go`'s `StmtCommit` -- `Release()`, folding the
    /// stage into the transaction -- and `StmtRollback` -- `Cleanup()`,
    /// dropping it. The transaction itself is untouched either way, which is
    /// why a failed statement inside `BEGIN` discards only its own writes and
    /// the statements around it survive to `COMMIT`.
    ///
    /// The stage here is an image of the catalog rather than an undo log,
    /// because this tier's tables ARE the buffer: `Catalog::clone` deep-copies
    /// the in-process bytes (`MemTableStorage::clone_box`). Restoring the
    /// image is the same observable effect as `Cleanup()`.
    ///
    /// # What the image does NOT undo
    ///
    /// The restore takes back exactly what `TableStorage::clone_box` copied by
    /// VALUE. `MemTableStorage` copies its bytes, so the rows come back. A
    /// storage whose `clone_box` clones a shared HANDLE does not: the image
    /// and the original write into the same place, so a failed statement's
    /// rows survive this restore. `tidb_executor::cluster_storage` is exactly
    /// that -- its `MutationBuffer` and snapshot are `Arc`s shared by every
    /// table of the session -- so on the cluster path the guard is NOT this
    /// function. It is the statement savepoint the convergence node takes over
    /// the buffer itself (`tidb_server`'s `ClusterServerSession::with_statement`:
    /// `MutationBuffer::staged()` before the statement, `restore()` on its
    /// error arm), which is the same `Staging()`/`Cleanup()` pair one tier
    /// down. Any future front end that drives a `Session` over cluster storage
    /// must bring such a savepoint with it; this restore alone will not roll
    /// its writes back.
    ///
    /// The `_tidb_rowid` a row without a clustered handle gets is NOT undone
    /// either, and for the same reason: Go allocates it from the very counter
    /// the AUTO_INCREMENT column uses (`autoid.NewAllocatorsFromTblInfo`
    /// builds one `RowIDAllocType` allocator for both), so a handle a failed
    /// statement consumed stays consumed.
    ///
    /// AUTO_INCREMENT deliberately survives the restore: Go allocates ids
    /// outside transaction semantics and never returns a consumed one, and
    /// `KvTable`'s `AutoIdAllocator` is a SHARED cell that a catalog copy
    /// keeps pointing at -- so the burn is retained with no exclusion rule
    /// here (captured: a failed one-row insert into an `AUTO_INCREMENT` table
    /// stores nothing and the next successful insert skips the burned id).
    ///
    /// Making this the one door every mutating statement goes through is the
    /// point: the restore lives in a guard's `Drop`, so no exit of `body` --
    /// and no DML arm added later -- can forget it.
    ///
    /// The guard rather than an error arm is what makes a PANIC take the same
    /// path as an `Err`. An `inspect_err` restore is skipped entirely when
    /// `body` unwinds, so a caught panic would leave a HALF-APPLIED statement
    /// in the in-process rows.
    fn with_staged_catalog<T>(&mut self, body: impl FnOnce(&mut Catalog) -> Result<T, DriverError>, ) -> Result<T, DriverError>;
    /// [`Self::require_statement_table_privileges`] for the statement inside
    /// an EXPLAIN, which Go plans under `StmtCtx.InExplainStmt`: every view
    /// `BuildDataSourceFromView` expands then needs SHOW VIEW, and a view
    /// expanded after the first SELECT, both refused with `ErrViewNoExplain`.
    fn require_explain_statement_privileges(&self, stmt: &tidb_ast::Stmt) -> Result<(), DriverError>;
    /// Session primitive.
    fn bind_statement_hints(&mut self, stmt: &Stmt) -> Option<Stmt>;
    /// Applies Go `optimize.go`'s `StmtHints.SetVars` after the canonical
    /// `hint.ParseStmtHints` pass: each `SET_VAR(name = value)` writes the
    /// session variable for the duration of THIS statement only, and where
    /// the same name appears twice the FIRST occurrence wins.
    ///
    /// The snapshot goes on [`Session::set_var_hint_restore`]. The next
    /// statement boundary restores it, as Go's `ResetContextOfStmt` does,
    /// including after an execution error.
    ///
    fn apply_set_var_hints(&mut self, stmt: &Stmt) -> Result<(), DriverError>;
    /// Resets Go `StmtCtx.ResourceGroupName` for one statement before its
    /// snapshot, transaction, planner, or executor is opened, and publishes
    /// it as the process list's `RESOURCE_GROUP` (Go `SetProcessInfo` reads
    /// the statement's group, hint included).
    fn activate_statement_resource_group(&mut self, stmt: &Stmt);
/// Go `StmtCtx`'s message and affected rows for the OK packet.
    fn set_statement_result(&mut self, message: String, affected_rows: u64);

}
