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

//! The convergence node: the wide-SQL session, over cluster storage, over the
//! MySQL wire.
//!
//! Every half of this already existed and was proven on its own. The session
//! driver plans and executes joins, subqueries, aggregates, window functions,
//! `SHOW` and `EXPLAIN` ([`crate::domain::pipeline_session`] serves it over TCP against
//! an in-process catalog). [`crate::domain::cluster_session`] binds that same driver's
//! catalog to real transactional storage (the `cluster-session-smoke` binary
//! proves it against a live cluster). [`crate::domain::cluster_privileges`] turns the
//! cluster's own `mysql.*` rows into the registry the handshake authenticates
//! against. This module is the one node where all three run at once:
//!
//! * the catalog and the accounts are read from the cluster at boot, and the
//!   catalog keeps following the cluster's schema version;
//! * a client authenticates as an account a Go TiDB created;
//! * each connection gets its own [`Session`] whose tables read and write
//!   through [`ClusterTableStorage`];
//! * `COMMIT` publishes the connection's staged writes through the optimistic
//!   2PC, which a Go TiDB then reads back.
//!
//! # The statement lifecycle
//!
//! The connection's tables are built once, against a [`SwappableSnapshot`] slot
//! every table shares; before a statement the connection binds a snapshot into
//! that slot and afterwards takes it back -- whether the statement succeeded or
//! failed, so a failure never leaves a lock behind.
//!
//! *Which* snapshot is the connection's transaction state. Outside `BEGIN` the
//! connection opens one [`StatementSnapshot`] per statement, at its own
//! timestamp, and publishes that statement's writes right after: Go's implicit
//! per-statement transaction. The bind itself costs nothing -- the snapshot is
//! prepared after planning and waited at the statement's FIRST read -- and one statement shape skips the
//! timestamp entirely: an autocommit point get on the clustered handle, which
//! [`ClusterServerSession::declare_read_shape`] declares to the bound snapshot
//! before the statement runs, and which then reads at `u64::MAX`. That
//! declaration is made from the STATEMENT and never from a read, because a
//! read alone cannot tell a point-get `SELECT` from an `UPDATE`'s
//! read-before-write or from one row lookup of a double read. Inside `BEGIN`
//! ... `COMMIT` the connection holds
//! one [`SessionTransaction`], every statement reads through it at the single
//! timestamp `BEGIN` took, and `COMMIT` prewrites the accumulated buffer on
//! that same transaction. That is Go's one `kv.Transaction` per session: later
//! statements do not see commits made after `BEGIN` (repeatable read), and a
//! writer that raced the transaction is rejected at prewrite as a write
//! conflict instead of being silently overwritten.
//!
//! There are FOUR doors onto that state, and the invariant is that they cannot
//! disagree: `explicit` is open exactly when the driver session says a
//! transaction is. Each door was a silently-wrong-answer bug until it was
//! routed, all with the same shape -- the driver's flag flipped, `explicit`
//! left unopened, so every statement read at a fresh timestamp and no racing
//! writer was ever detected.
//!
//! * A text `BEGIN`/`COMMIT`/`ROLLBACK`, through
//!   [`ClusterServerSession::control_transaction`].
//! * A prepared one. It used to be run as an ordinary statement instead; it is
//!   now classified at PREPARE and routed to the same place, so which protocol
//!   carried it does not enter into any of this.
//! * `SET autocommit = 0`, which carries no keyword at all: the store
//!   session's `prepare_statement_context` enters the pending transaction for
//!   the statement about to run (Go `PrepareTxnCtx`), and
//!   [`ClusterServerSession::commit_if_session_left_transaction`] is the
//!   statement that ends the transaction from the inside.
//! * `SAVEPOINT`, which under `autocommit = 0` is Go's `Txn(true)` and so is
//!   itself a transaction opening (`TxnManager::savepoint_checkpoint`).
//!
//! None of them tracks transaction state here. Each asks the driver session,
//! which owns the rules; a second copy that could drift is the bug all four
//! were.
//!
//! A PREPARE is deliberately NOT a fifth door, and that took measuring: Go's
//! `PrepareStmt` does call `PrepareTxnCtx`, but the transaction it leaves is
//! *pending* -- no timestamp, `InTxn()` still false -- so under
//! `autocommit = 0` the first statement that touches data is still what opens
//! one. This node's PREPARE probe RUNS the statement to learn its result
//! columns, so it had to be routed away from the opening
//! ([`ClusterServerSession::probe_statement`]); leaving it there pinned the
//! connection's `start_ts` at PREPARE time and every later statement of that
//! transaction read a snapshot the client never asked for.
//!
//! Writes never touch the slot: they stage into the connection's
//! [`MutationBuffer`], which outlives the statement. A failed statement is
//! rolled back to the buffer snapshot taken before it ran, so an explicit
//! transaction keeps exactly the writes of its statements that succeeded.
//!
//! # DDL: the one stored-schema change this node performs
//!
//! `CREATE TABLE`, `DROP TABLE`, `CREATE DATABASE`, `DROP DATABASE`,
//! `CREATE INDEX`, `DROP INDEX`, and their single-action `ALTER TABLE`
//! spellings are not run by the session driver against
//! its own in-memory catalog -- that copy is a *read* of the cluster's schema,
//! so changing it alone would be a silently wrong answer. They are routed to
//! the [`ClusterDdl`] seam, which publishes the meta-key mutations through the
//! same optimistic 2PC the DML path uses ([`tidb_ddl::real_tikv_ddl`]), and a
//! Go TiDB then sees the object.
//!
//! An index change publishes a second thing in that same transaction: the
//! entries the rows the table ALREADY holds owe it. They are staged by the very
//! `KvTable` call an `INSERT` maintains an index with, over this seam's own
//! storage -- see [`RealClusterDdl`]'s backfiller -- because an index that
//! exists and holds nothing loses rows from every query routed through it,
//! with no error anywhere.
//!
//! Two catalogs have to catch up afterwards, and they do it at different
//! moments for different reasons:
//!
//! * **The node's**, immediately. [`RealClusterDdl`] runs one reload pass
//!   inline on the statement's own thread rather than waiting up to `lease/2`
//!   for [`CatalogReloader`]'s tick -- this node is the one that wrote the
//!   change, so it needs no notification to know about it. Both publishers
//!   swap the whole catalog into the same slot, so they cannot interleave.
//! * **The connection's**, at its next statement. A connection's tables are
//!   built once, over the snapshot slot they share, so a table created after
//!   the connection opened has no entry at all; [`ClusterServerSession`]
//!   rebuilds that catalog -- against the same storage handles, leaving the
//!   slot and the staged writes untouched -- whenever the node's schema
//!   version has moved past the one the connection was built from. Never
//!   inside an explicit transaction: its statements keep the schema its
//!   `BEGIN` saw, exactly as they keep its snapshot.
//!
//! # Accounts: the other stored state this node writes
//!
//! `CREATE USER`, `DROP USER`, `GRANT`, `REVOKE` and `SET PASSWORD` are the
//! same shape of problem as the DDL above -- the account table is a *read* of
//! the cluster's `mysql.*` rows -- and take the same shape of answer: the
//! [`ClusterAccountWriter`] seam. What differs is that the statement's meaning
//! is not re-derived from the AST at all; the session driver runs it against a
//! scratch account table read from the cluster inside the change's own
//! transaction, and the seam writes back whatever that table now says.
//! [`crate::domain::cluster_account_seam`] states why, and what a failure leaves
//! behind.
//!
//! # What this mode refuses, and why
//!
//! * Every stored-schema change the cluster DDL path cannot express: `ALTER`,
//!   `TRUNCATE`, `RENAME`, `CREATE VIEW`/`SEQUENCE`, the index shapes whose
//!   entries this node would not go on to maintain (prefix, expression,
//!   partial, `GLOBAL`, `FULLTEXT`/`SPATIAL`/`VECTOR`), and the
//!   `CREATE TABLE` clauses [`tidb_exec::table_info_build`] refuses (foreign
//!   keys, partitions, ...). Each is refused with its own reason rather than a
//!   generic unsupported error.
//! * A table the storage tier cannot lay out (a view, a sequence, a
//!   partitioned table, one mid-DDL). [`cluster_session_catalog`] reports each
//!   by name with its exact reason, and the node prints them at boot rather
//!   than letting them vanish.
//!
//! Everything else the session driver can answer -- including `SHOW` and
//! `information_schema` over the loaded catalog -- runs, and reports the
//! driver's own error where it cannot.
//!
//! [`ClusterTableStorage`]: tidb_executor::cluster_storage::ClusterTableStorage
//! [`StatementSnapshot`]: tidb_cluster::cluster_table_storage::StatementSnapshot
//! [`SessionTransaction`]: tidb_cluster::cluster_table_storage::SessionTransaction

use crate::store_session::{sql_query_error, StoreTxnOps};
use crate::statement_lifecycle::CatalogFollowing as _;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_executor::compiler::PreparedExec;
use tidb_executor::ddl_exec::DdlExec as _;
use tidb_executor::stats_exec::StatsExec as _;
use tidb_executor::compiler::PreparedAstCompile;
use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::domain::resultset_source::ResultSetSource;
use crate::identity::CompiledExecutor;
use chrono::{Datelike, Timelike, Utc};
use tidb_ast::Stmt;
use tidb_domain::historical_stats::{
    HistoricalStatsMetrics, HistoricalStatsWorker, InfoSchemaView, SessionInfoSchema,
    StatsHandle as HistoricalStatsHandle, TableMeta,
};
use tidb_cluster::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::cluster_analyze::AnalyzeStatement;
use tidb_cluster::cluster_catalog::ClusterCatalog;
use tidb_ddl::cluster_ddl::DdlStatement;
use tidb_exec::cluster_load_stats::prepare_cluster_load_stats_parsed;
use tidb_exec::cluster_stats_load::ClusterStatsLoader;
use tidb_exec::cluster_stats_lock::prepare_cluster_stats_lock_parsed;
use tidb_exec::cluster_stats_lock::ClusterStatsLockStatement;
use tidb_exec::real_tikv_analyze::prepare_cluster_analyze_parsed;
use tidb_cluster::real_tikv_catalog::SnapshotMetaSnapshot;
use tidb_exec::real_tikv_stats_lock::ClusterStatsLockCommitError;
use tidb_cluster::schema_validator::SchemaValidator;
use tidb_exec::stats_watch::SharedStats;
use tidb_executor::access_path::StatementReadShape;
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SwappableSnapshot,
};
use tidb_planner::transaction_control::{
    classify_transaction_control, classify_transaction_control_stmt, TransactionControl,
};
use crate::privilege::PrivilegeRegistry;
use crate::process::ProcessRegistry;
use crate::{
    GlobalSysvars, Session, StmtKind, StmtOutput,
    TableStorageStatsProvider,
};
use tidb_txnkv::transaction::SchemaLeaseChecker;

use tidb_stats_handle_cache::{StatsTableRowCache, StatsTableRowSizeSource, TableHistId};

use crate::domain::cluster_account_seam::ClusterAccountWriter;
use crate::domain::cluster_analyze_seam::ClusterAnalyze;
use crate::domain::cluster_session::{
    cluster_session_catalog, cluster_session_catalog_with_templates, KvTableTemplates,
    SkippedTable, TableAutoIds,
};
use crate::domain::cluster_stats_lock_seam::ClusterStatsLock;
use crate::domain::cluster_sysvar_seam::ClusterSysvarWriter;
use crate::domain::pipeline_session::MaterializedResultSetSource;
use crate::domain::session_api::{
    ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult, SessionContext,
    SqlQueryError, WriteOutcome,
};
use tidb_executor::ddl_exec::map_error;
use crate::domain::wire_status::WireStatus;


/// The PD/TiKV control-plane deadline this node's boot and statements use, the
/// same one the bounded node applies.
pub const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Pinned Go `statistics/handle.StatsOwnerKey`.
pub const STATS_OWNER_KEY: &str = "/tidb/stats/owner";
/// Pinned Go `statistics/handle.StatsPrompt`.
pub const STATS_OWNER_PROMPT: &str = "stats";

/// Go `kv.ErrWriteConflict`, the one cause an autocommit statement is replayed
/// for.

pub(crate) use tidb_domain_core::bdr_role_store;
mod catalog_follower;
mod binding_maintenance;
pub use binding_maintenance::attach_binding_sessions;
use tidb_ddl::worker as ddl;
pub(crate) use tidb_domain_core::ddl_job_history_store;
mod ddl_notifier;
pub mod gc_worker;
use crate::statement_lifecycle::{ReadStatement, StatementLifecycle};
#[cfg(test)]
use crate::statement_lifecycle::{back_off_upper_ms, jitter_below, RETRY_BACK_OFF_CAP_MS};
#[cfg(test)]
use tidb_cluster::cluster_table_storage::LockKeysOutcome;
use crate::txn_manager::{CommitHooks, CommitTxnRegions, TxnManager};
mod non_transactional_dml;
mod record_set;
mod ru_stats_writer;
pub mod schema_lease;
pub use tidb_domain_core::schema_sync;
mod stat;
mod statistics;
mod test_session;
pub mod stats_maintenance;
use crate::cluster_transactions as transactions;

pub use ddl::{ClusterDdl, RealClusterDdl};
pub use tidb_ddl::real_tikv_ddl::ClusterDdlReport;
#[cfg(test)]
use transactions::sql_error;
pub use transactions::{ClusterTransactions, OpenClusterTransaction, RealClusterTransactions};

/// Go `Compile`'s plan kind for one statement, as the node dispatches on
/// it: [`ClusterServerSession::compile`].
enum CompiledStatement {
    /// A stored-state change, lowered (Go `core.DDL`, `core.Analyze`,
    /// `core.LoadStats` and the statistics-lock plans).
    StoredState(StatementRoute),
    /// Go `core.Set`: `SetExecutor`, whose GLOBAL assignments write through
    /// the session's domain services.
    Set,
    /// Go `core.Simple` for an account statement, which the node commits
    /// ahead of where Go's executor calls `NewTxnInStmt`
    /// ([`ClusterServerSession::run_account_executor`]).
    AccountSimple,
    /// A plan with no result schema: Go's Insert/Update/Delete and the other
    /// write plans, answered with an OK packet.
    Write,
    /// Go `core.Trace`.
    Trace,
    /// A plan with a result schema.
    Query,
}

/// The executor Go's `executorBuilder.build` picks for a compiled plan
/// ([`ClusterServerSession::route_for`]), where this node runs it rather
/// than the session's own statement path.
enum StatementRoute {
    /// Changes nothing stored outside this process.
    Ordinary,
    /// Go `DDLExec`: the implicit commit, the DDL executor's checks and the
    /// job(s) ([`ClusterServerSession::run_ddl_exec`]).
    DdlExec,
    /// DDL the session's `DDLExec` answers without a job (Go
    /// `DDLExec.Next` over a LOCAL temporary table).
    SessionDdl,
    /// One `ANALYZE TABLE`, per table it named.
    Analyze(Vec<AnalyzeStatement>),
    /// One LOAD STATS statement awaiting its client-local file bytes.
    LoadStats,
    /// One foreground dump of this node's pending statistics deltas.
    FlushStatsDelta(FlushStatsDeltaTargets),
    /// One persisted `LOCK STATS` or `UNLOCK STATS` operation.
    StatsLock(ClusterStatsLockStatement),
    /// One `DROP STATS`, over the physical ids Go's `executeDropStats` names.
    DropStats(Vec<i64>),
}

use crate::stats_statement_service::FlushStatsDeltaTargets;

/// What [`ClusterServerSession::run_statement`] left for its caller.
enum Executed {
    /// The statement answered with an OK packet's outcome.
    Done(WriteOutcome),
    /// A query compiled; its record set is the caller's to open, inside
    /// the `session.ExecuteStmt` region still held.
    Query(tidb_util::tracing::ThreadRegionScope),
    /// An `ADMIN TRACE` compiled; the wrapped statement is the caller's to run.
    Trace,
}

/// The statement an `ADMIN TRACE` plan wraps.
fn trace_of(stmt: &Stmt) -> &tidb_ast::TraceStmt {
    let Stmt::Admin(admin) = stmt else {
        unreachable!("a TRACE plan compiles from ADMIN TRACE")
    };
    let tidb_ast::AdminStmt::Trace(trace) = admin.as_ref() else {
        unreachable!("a TRACE plan compiles from ADMIN TRACE")
    };
    trace
}

mod domain;
pub use domain::*;
pub(crate) use tidb_domain_core::cluster_session::detached_storage;
pub(crate) use tidb_domain_core::session_transaction::global_lock_wait_timeout;
pub(crate) use tidb_domain_core::sys_session::{stats_session_error, system_process_trackers};
pub(crate) use tidb_domain_core::util::{is_analyze_table_sql, sql_string_literal};
mod sessions;
pub use sessions::*;
pub use login_policy::DomainLoginPolicy;
pub use settings::DomainSessionSettings;
pub use test_session::DomainTestSessions;

/// The store half of Go's `session`: the fields `session.go` keeps beside
/// `sessionVars` for a session over a store -- its domain, its `txn`
/// (`LazyTxn` under the `TxnManager`), the snapshot its tables read through,
/// the schema it follows and the connection's OK-packet state. A
/// [`Session`] over a store holds one in `Session::node`.
pub struct NodeConn {
    /// Go `domain.GetDomain(sctx)`: the node's domain this session runs in.
    domain: Arc<Domain>,
    /// Go `sessiontxn.TxnManager` with its `LazyTxn`.
    txn: TxnManager,
    /// The slot every table of the session reads through; rebound per
    /// statement.
    slot: crate::statement_lifecycle::SnapshotSlot,
    /// The catalog the session follows.
    follower: catalog_follower::CatalogFollower,
    /// Whether the running write is one whose transaction Go's executor
    /// builder activates before it runs (`updateForUpdateTS` ->
    /// `GetStmtForUpdateTS`).
    activate_statement_txn: bool,
    commit_hooks: crate::store_session::CommitHooksFactory,
    /// This connection's staged writes, published by `COMMIT` (or by the end
    /// of an autocommit statement).
    buffer: MutationBuffer,
    /// This connection's id, the registry's key.
    connection_id: u64,
    /// Go `QueryCtx.AffectedRows` / `LastInsertID` of the last statement
    /// Go `QueryCtx.ExecuteStmt` answered without a result set.
    ok_packet: WriteOutcome,
    /// Go `LoadDataVarKey` / `LoadStatsVarKey`: the statement a LOCAL load
    /// parked until the connection brings the client's file.
    parked_local_infile: Option<ParkedLocalInfile>,
    /// Go `session.store` when it is a testkit mock store: the store the
    /// session keeps alive (`None` on a server, whose store outlives every
    /// connection).
    pub(crate) store: Option<Arc<crate::domain::mock_store::MockStoreDomain>>,
}

/// One [`Session`] over a store with its store half ([`NodeConn`]) lent
/// beside it: the receiver of every step Go's `session` takes on its store.
///
/// The session's `node` is out while this lives; [`Session::node_conn`]
/// lends it and dropping the value puts it back.
pub struct ClusterServerSession<'s> {
    session: &'s mut Session,
    node: NodeLoan<'s>,
}

/// The store half a [`ClusterServerSession`] works on: taken from the
/// session (and put back on drop) or reborrowed from another loan.
enum NodeLoan<'s> {
    Taken(Option<Box<NodeConn>>),
    Borrowed(&'s mut NodeConn),
}

impl std::ops::Deref for NodeLoan<'_> {
    type Target = NodeConn;
    fn deref(&self) -> &NodeConn {
        match self {
            NodeLoan::Taken(node) => node.as_deref().expect("a lent store half"),
            NodeLoan::Borrowed(node) => node,
        }
    }
}

impl std::ops::DerefMut for NodeLoan<'_> {
    fn deref_mut(&mut self) -> &mut NodeConn {
        match self {
            NodeLoan::Taken(node) => node.as_deref_mut().expect("a lent store half"),
            NodeLoan::Borrowed(node) => node,
        }
    }
}

impl Drop for ClusterServerSession<'_> {
    fn drop(&mut self) {
        if let NodeLoan::Taken(node) = &mut self.node {
            self.session.node = node.take();
        }
    }
}

impl Session {
    /// Lends this session's store half beside it for one step on the store.
    ///
    /// # Panics
    ///
    /// On a session with no store.
    pub fn node_conn(&mut self) -> ClusterServerSession<'_> {
        let node = self.node.take().expect("a session on the store");
        ClusterServerSession {
            session: self,
            node: NodeLoan::Taken(Some(node)),
        }
    }

    /// This session's store half.
    ///
    /// # Panics
    ///
    /// On a session with no store.
    pub(crate) fn node(&self) -> &NodeConn {
        self.node.as_deref().expect("a session on the store")
    }
}

impl NodeConn {
    /// The tables this connection's catalog left out, with their reasons.
    #[must_use]
    pub fn skipped_tables(&self) -> &[SkippedTable] {
        &self.follower.skipped
    }
}

impl<'s> ClusterServerSession<'s> {
    /// The same session and store half for a shorter borrow, as a step that
    /// lends the connection to a result it returns needs.
    pub(crate) fn reborrow(&mut self) -> ClusterServerSession<'_> {
        ClusterServerSession {
            session: &mut *self.session,
            node: NodeLoan::Borrowed(&mut self.node),
        }
    }
}

/// A LOAD DATA LOCAL or LOAD STATS waiting for the client's file.
struct ParkedLocalInfile {
    sql: String,
    stmt: Stmt,
    path: String,
}

impl ClusterServerSession<'_> {
    /// Go `session.Close` on the store: run by [`Session`]'s drop.
    pub(crate) fn close(&mut self) {
        // Go session.Close rolls back before releasing session/process state.
        // The same path retires history, native locks, schema pins and buffers.
        let _ = self.discard_explicit();
        // Go `session.Close` then releases the session's table locks
        // through the DDL (`UnlockTables`), logging a failure. The release
        // runs through the cluster job, never the in-process catalog the
        // driver session would otherwise edit when it drops.
        if tidb_config::config_tree::config::table_lock_enabled() {
            if let Some(statement) = self.session.cluster_unlock_tables_statement() {
                if self
                    .node
                    .domain
                    .ddl
                    .execute(&statement, &self.session.ddl_statement_context())
                    .is_err()
                {
                    // Go `session.Close` logs the connection, not the error.
                    tidb_util::logutil::bg_logger().error(
                        "release table lock failed",
                        &[tidb_log::Field::new(
                            "conn",
                            tidb_log::Value::U64(self.session.connection_id()),
                        )],
                    );
                }
                self.session.finish_cluster_lock_job(&statement);
            }
        }
    }
}

/// Go `executor.Compile` of a transaction statement's `Simple` plan
/// (`planner.Preprocess`, `planner.Optimize`) handing over to
/// `session.runStmt`, which logs the statement's text. Nothing on an
/// untraced thread.
fn compile_simple_statement(sql: &str) {
    if tidb_util::tracing::current_span().is_none() {
        return;
    }
    tidb_util::tracing::begin_thread_region("executor.Compile", Some(sql.to_owned()));
    tidb_util::tracing::start_thread_region("planner.Preprocess").end();
    tidb_util::tracing::start_thread_region("planner.Optimize").end();
    tidb_executor::driver::begin_run_stmt_region();
}

/// Go `ExecStmt.handleNoDelayExecutor` and, under it, `*executor.SimpleExec`'s
/// `exec.Next` region; both end when the value drops.
struct SimpleExecRegions {
    // Dropped in declaration order: the executor's call ends first.
    _next: tidb_util::tracing::ThreadRegion,
    _handle: tidb_util::tracing::ThreadRegion,
}

impl SimpleExecRegions {
    fn begin() -> Self {
        let handle = tidb_util::tracing::start_thread_region_ex("executor.handleNoDelayExecutor");
        let next = tidb_util::tracing::start_thread_region_ex("*executor.SimpleExec.Next");
        Self {
            _next: next,
            _handle: handle,
        }
    }
}

/// The node's [`crate::txn_manager::SchemaPins`]: its MDL registry.
struct NodeSchemaPins(Arc<schema_sync::SchemaPinRegistry>);

impl crate::txn_manager::SchemaPins for NodeSchemaPins {
    fn hold(&self, connection_id: u64, version: i64) -> Box<dyn Send> {
        Box::new(self.0.hold(connection_id, version))
    }
}

/// What a commit reads off this node: the schema lease checker, the binding
/// cache and the catalog a write-conflict message is rewritten against.
struct NodeCommitHooks {
    bindings: Option<Arc<dyn crate::domain::cluster_binding_seam::ClusterBindings>>,
    schema_validator: Option<Arc<SchemaValidator>>,
    schema_version: i64,
    catalog: crate::SharedCatalog,
}

impl CommitHooks for NodeCommitHooks {
    /// Go `SetOptionsBeforeCommit` (`base.go:560-616`): the checker a commit
    /// carries -- the node's validator and the schema version this
    /// transaction was planned against (`GetTxnInfoSchema().SchemaMetaVersion()`:
    /// the version pinned at `BEGIN`, or the autocommit statement's own).
    /// The physical tables it wrote and the MDL switch are read at commit.
    fn schema_lease_checker(&self) -> Option<Arc<dyn SchemaLeaseChecker>> {
        self.schema_validator.as_ref().map(|validator| {
            Arc::new(schema_lease::SessionSchemaLeaseChecker::new(
                Arc::clone(validator),
                self.schema_version,
            )) as Arc<dyn SchemaLeaseChecker>
        })
    }

    fn binding_records_changed(&self, buffer: &MutationBuffer) -> bool {
        self.bindings
            .as_ref()
            .is_some_and(|bindings| bindings.has_changes(buffer))
    }

    fn refresh_committed_bindings(&self, changed: bool) {
        if changed {
            if let Some(bindings) = &self.bindings {
                if let Err(error) = bindings.reload() {
                    // The commit succeeded. Retain the last image and let the
                    // lease worker retry; never replay a committed statement.
                    eprintln!("committed binding cache reload failed: {error}");
                }
            }
        }
    }

    /// Go `doCommitWithRetry`'s `tryReplaceWriteConflictError`
    /// (`pkg/session/session.go:862`), against the transaction's infoschema
    /// (`GetTxnInfoSchema`): the connection's catalog, which is not rebuilt
    /// while a transaction is open.
    fn rewrite_write_conflict(&self, error: SqlQueryError) -> SqlQueryError {
        if error.write_conflict.is_none() {
            return error;
        }
        let catalog = self
            .catalog
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let redaction = tidb_txnkv::driver::transaction_error::Redaction::current();
        transactions::try_replace_write_conflict_error(error, redaction, |table_id| {
            catalog
                .table_names_by_id(table_id)
                .map(|(schema, table)| (Some(schema), table))
        })
    }
}

impl<'s> ClusterServerSession<'s> {

    /// What a commit reads off the server, for the schema version the session
    /// is following now.
    #[must_use]
    pub fn commit_hooks(&self) -> Box<dyn CommitHooks> {
        (self.node.commit_hooks)(&self.session, self.node.follower.schema_version())
    }

    /// The borrowed context one statement runs in.
    pub fn lifecycle(&mut self) -> StatementLifecycle<'_> {
        let hooks = self.commit_hooks();
        let node = &mut *self.node;
        StatementLifecycle {
            session: &mut self.session,
            txn: &mut node.txn,
            slot: &node.slot,
            follower: &mut node.follower,
            hooks,
            activate_statement_txn: node.activate_statement_txn,
        }
    }

    /// The `SimpleExec` transaction statements (`BEGIN`, `COMMIT`,
    /// `ROLLBACK`, `SAVEPOINT`, `RELEASE SAVEPOINT`) over this session's
    /// transaction manager: [`Session::control_transaction_stmt`] with the
    /// store half through [`StoreTxnOps`].
    pub fn control_transaction_stmt(&mut self, stmt: &Stmt) -> Result<Option<bool>, SqlQueryError> {
        let hooks = self.commit_hooks();
        let node = &mut *self.node;
        let mut ops = StoreTxnOps {
            txn: &mut node.txn,
            slot: &node.slot,
            follower: &mut node.follower,
            hooks,
        };
        self.session
            .control_transaction_stmt(stmt, &mut ops)
            .map_err(sql_query_error)
    }

    /// Go `sessiontxn.NewTxnInStmt` for the executor of the running
    /// statement: [`Session::new_txn_in_stmt`] over this session's
    /// transaction manager, when a transaction is open.
    pub fn new_txn_in_stmt(&mut self) -> Result<(), SqlQueryError> {
        if self.node.txn.explicit.is_none() && !self.session.in_transaction() {
            return Ok(());
        }
        let hooks = self.commit_hooks();
        let node = &mut *self.node;
        let mut ops = StoreTxnOps {
            txn: &mut node.txn,
            slot: &node.slot,
            follower: &mut node.follower,
            hooks,
        };
        self.session
            .new_txn_in_stmt(&mut ops)
            .map_err(sql_query_error)
    }

    /// Rebinds the session's tables to the latest catalog, unless a
    /// transaction has pinned the one it started with.
    pub fn rebuild_catalog_if_stale(&mut self) {
        let node = &mut *self.node;
        node.follower.rebuild_if_stale(&mut self.session, &node.txn);
    }

    /// Go `PrepareTxnCtx` for the statement about to run: follows the catalog,
    /// then enters the `autocommit = 0` transaction (pending: it takes a
    /// timestamp at the first statement that activates it).
    pub fn prepare_statement_context(
        &mut self,
        resource_group: &str,
        read_only: bool,
    ) -> Result<(), SqlQueryError> {
        self.rebuild_catalog_if_stale();
        if read_only && self.node.txn.explicit.is_none() && !self.session.in_transaction() {
            let vars = self.session.vars();
            if vars.snapshot_ts() != 0
                || vars
                    .get_system("tidb_read_staleness")
                    .is_ok_and(|v| v.parse::<i64>().is_ok_and(|v| v != 0))
            {
                return Ok(());
            }
        }
        let node = &mut *self.node;
        node.txn.enter_if_autocommit_off(
            &self.session,
            resource_group,
            node.follower.schema_version(),
        );
        // Go `SetOptionsOnTxnActive` reads the session values when the
        // transaction activates, so a pending one follows each statement's.
        if let Some(transaction) = &self.node.txn.explicit {
            transaction
                .set_activation_options(TxnManager::statement_transaction_options(&self.session));
        }
        Ok(())
    }

    /// Runs one client statement after refreshing the catalog and opening any
    /// implicit transaction. A pessimistic statement's locking point reads
    /// lock inside execution with returned row values (Go
    /// `PointGetExecutor.getAndLock`, `InitReturnValues` /
    /// `SetPessimisticLockCache`), through `TableStorage::lock_keys_for_read`.
    pub fn with_prelocked_statement<T>(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
        is_dml: bool,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.prepare_statement_context(
            resource_group,
            shape != StatementReadShape::AutocommitWrite,
        )?;
        self.lifecycle()
            .with_bound_statement(shape, resource_group, true, is_dml, run)
    }

    /// [`Self::with_prelocked_statement`] for work the CLIENT did not ask to
    /// run: the PREPARE probe, which executes a query only to learn its result
    /// columns.
    ///
    /// The one thing it must not do is open the transaction `autocommit = 0`
    /// implies. Go's `PrepareStmt` calls `PrepareTxnCtx` too
    /// (`pkg/session/session.go:3171`), but with a nil statement and through
    /// `EnterNewTxnBeforeStmt`, which leaves the transaction *pending*: no
    /// timestamp is spent and the first statement that really reads is what
    /// takes one. Captured: `@@tidb_current_ts` is `0` after a PREPARE under
    /// `autocommit = 0`, and a read issued after another session's commit sees
    /// the NEW value. An already-open transaction is read through as usual.
    pub fn probe_statement<T>(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.lifecycle()
            .with_bound_statement(shape, resource_group, false, false, run)
    }

    /// Whether Go's executor builder activates `stmt`'s transaction before
    /// the statement runs: `buildInsert` (INSERT and REPLACE), `buildUpdate`
    /// and `buildDelete` call `updateForUpdateTS`, whose
    /// `GetStmtForUpdateTS` activates it. A text `EXECUTE` builds the
    /// statement it names.
    #[must_use]
    pub fn activates_txn_at_build(&self, stmt: &Stmt) -> bool {
        let stmt = match stmt {
            Stmt::Session(session) => match session.as_ref() {
                tidb_ast::SessionStmt::Execute { name, .. } => {
                    match self.session.prepared_statement_node(name) {
                        Some(prepared) => prepared,
                        None => return false,
                    }
                }
                _ => return false,
            },
            stmt => stmt,
        };
        matches!(
            stmt,
            Stmt::Dml(dml) if matches!(
                dml.as_ref(),
                tidb_ast::DmlStmt::Insert(_)
                    | tidb_ast::DmlStmt::Update(_)
                    | tidb_ast::DmlStmt::Delete(_)
            )
        )
    }

    /// Runs an ordinary write statement (INSERT, UPDATE, DELETE and the like)
    /// through the statement lifecycle and answers its affected-row count and
    /// last insert id.
    pub fn run_ordinary_write(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<(u64, u64), SqlQueryError> {
        let owned = sql.to_owned();
        let resource_group = self.session.statement_resource_group(stmt).into_owned();
        // A write declares nothing. Its read-before-write reaches the snapshot
        // as the same `get` a point-get SELECT issues, which is exactly why the
        // declaration is made from the statement rather than from the read.
        //
        // Go's pessimistic point write folds its row read INTO its lock inside
        // execution (`PointGetExecutor.getAndLock`,
        // `pkg/executor/point_get.go:612-624`): the executor locks the keys its
        // plan names through `TableStorage::lock_keys_for_read`, so nothing is
        // classified or locked here before the statement compiles.
        self.node.activate_statement_txn = self.activates_txn_at_build(stmt);
        let attempt = self.with_prelocked_statement(
            StatementReadShape::Unknown,
            &resource_group,
            true,
            move |session| match session
                .exec_statement_parsed(stmt.clone(), &owned)
                .map_err(sql_query_error)?
            {
                StmtOutput::Affected(count) => Ok(count),
                StmtOutput::Done(_) => Ok(0),
                StmtOutput::Rows { .. } => Err(SqlQueryError::unknown(
                    "a write statement unexpectedly produced rows",
                )),
            },
        );
        self.node.activate_statement_txn = false;
        let affected_rows = attempt?;
        self.session.add_statement_read_keys(affected_rows);
        Ok((affected_rows, self.session.statement_insert_id()))
    }
}

impl<'s> ClusterServerSession<'s> {

    /// The statement lifecycle proper; see
    /// [`StatementLifecycle::with_bound_statement`].
    fn with_bound_statement<T>(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
        notify_executor_breakpoint: bool,
        is_dml: bool,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.lifecycle().with_bound_statement(
            shape,
            resource_group,
            notify_executor_breakpoint,
            is_dml,
            run,
        )
    }

    fn begin_read_statement(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
    ) -> Result<ReadStatement, SqlQueryError> {
        self.lifecycle().begin_read_statement(shape, resource_group)
    }

    fn end_read_statement(
        &mut self,
        statement: ReadStatement,
        success: bool,
    ) -> Result<(), SqlQueryError> {
        self.lifecycle().end_read_statement(statement, success)
    }

    /// Ends the explicit transaction by publishing its buffer; see
    /// [`StatementLifecycle::commit_explicit`].
    fn commit_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.lifecycle().commit_explicit()
    }

    /// [`StatementLifecycle::may_retry_autocommit_statement`], for the tests
    /// that pin Go's retryable-error allowlist.
    #[cfg(test)]
    fn may_retry_autocommit_statement(&mut self, error: &SqlQueryError, retried: u32) -> bool {
        self.lifecycle().may_retry_autocommit_statement(error, retried)
    }

    /// Drops the explicit transaction without publishing anything; see
    /// [`StatementLifecycle::discard_explicit`].
    fn discard_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.lifecycle().discard_explicit()
    }

    /// Publishes the transaction a statement ended from the INSIDE (`SET
    /// autocommit = 1`); see [`StatementLifecycle::commit_if_session_left_transaction`].
    fn commit_if_session_left_transaction(&mut self) -> Result<(), SqlQueryError> {
        self.lifecycle().commit_if_session_left_transaction()
    }

}

impl<'s> ClusterServerSession<'s> {
    /// Go `TiDBContext.ExecuteStmt` (`pkg/server/driver_tidb.go:376`): every
    /// error `session.ExecuteStmt` returns is appended to the statement
    /// context as an `Error`-level warning, which `SHOW WARNINGS`, `SHOW
    /// ERRORS`, `@@warning_count` and `@@error_count` then report. The
    /// session's own statement pipeline files the errors it raises; this is
    /// the door for the steps this node runs outside it (COMMIT's prewrite,
    /// the locking read's lock). A row the session already filed for the
    /// same error is not filed twice.
    fn record_statement_failure(&mut self, error: SqlQueryError) -> SqlQueryError {
        self.session
            .record_ddl_failure(error.code, error.message.clone(), error.identity);
        error
    }

    fn control_transaction_parsed_inner(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<bool>, SqlQueryError> {
        let control = match stmt {
            Stmt::Session(statement) => match statement.as_ref() {
                tidb_ast::SessionStmt::Begin(begin) if begin.as_of.is_some() => {
                    Some(TransactionControl::Begin { mode: begin.mode })
                }
                _ => classify_transaction_control_stmt(stmt),
            },
            _ => classify_transaction_control_stmt(stmt),
        };
        if control.is_some() {
            // Go `TiDBContext.checkSandBoxMode` gates every statement of a
            // session admitted with an expired password, BEGIN included.
            self
                .session
                .check_sandbox_mode(sql)
                .map_err(map_error)?;
            self.session.restore_statement_variables();
        }
        // Go `ExecStmt.handleNoDelayExecutor` -> `SimpleExec.Next`: the
        // statement's own work. The commit a COMMIT or ROLLBACK leaves to
        // `runStmt`'s `finishStmt` runs after it (`session.CommitTxn`).
        let mut simple_exec = control.is_some().then(SimpleExecRegions::begin);
        // Refused BEFORE the driver session is touched, which is the whole
        // point: `Session::control_transaction` sets `in_transaction` for any
        // BEGIN spelling, so honoring the refusal afterwards would leave the
        // session inside a transaction that this node never opened -- no
        // `self.node.txn.explicit`, so every following statement reads at a fresh
        // timestamp, its writes stay in the buffer, and its COMMIT publishes
        // without a conflict check. The read-only node already refuses here
        // (`real_tikv_node`); this one used to fall through an empty arm.
        if let Some(TransactionControl::Unsupported(feature)) = control {
            return Err(SqlQueryError::unknown(format!(
                "{feature} is not supported yet"
            )));
        }
        // Go builds a fresh `TxnCtx` at every transaction boundary, and its
        // GLOBAL temporary rows never reach a commit (`temporaryTableKVFilter`
        // strips their keys), so a COMMIT or ROLLBACK starts this session's
        // transaction-scoped state over. A BEGIN does so at its own `NewTxn`,
        // after the checks that may refuse it
        // (`Session::control_transaction_stmt`).
        if matches!(
            control,
            Some(TransactionControl::Commit | TransactionControl::Rollback { .. })
        ) {
            self.session.reset_transaction_scope();
        }
        // Go `SimpleExec.Next`: the statement itself, the store half through
        // the session's transaction manager -- `BEGIN`'s implicit commit and
        // activation, `ROLLBACK`'s `RollbackTxn`, the savepoints' membuffer
        // checkpoints.
        let state = match self.control_transaction_stmt(stmt) {
            Ok(state) => state,
            Err(error) => {
                // A failed statement that has ended the session's transaction
                // must not leave the store transaction for the next
                // statement's `commit_if_session_left_transaction` to publish.
                if self.node.txn.explicit.is_some() && !self.session.in_transaction() {
                    self.discard_explicit()?;
                }
                return Err(error);
            }
        };
        let Some(in_transaction) = state else {
            return Ok(None);
        };
        match control {
            Some(TransactionControl::Commit) => {
                // Go `executeCommit` only leaves the transaction; `finishStmt`
                // then commits it under `session.CommitTxn`, and
                // `doCommitWithRetry` opens its region only over a valid
                // transaction.
                drop(simple_exec.take());
                let _commit_txn = CommitTxnRegions::begin(self.node.txn.explicit.is_some());
                self.commit_explicit()?;
            }
            Some(TransactionControl::Rollback) => {
                // Go `executeRollback` rolled the transaction back in the
                // `SimpleExec`; `finishStmt`'s `CommitTxn` then finds no
                // valid transaction.
                drop(simple_exec.take());
                let _commit_txn = CommitTxnRegions::begin(false);
            }
            _ => {}
        }
        drop(simple_exec);
        // The control path bypasses the ordinary statement pipeline's finish
        // publish, so the process row still carried the previous statement's
        // status (`in transaction; autocommit` after a `ROLLBACK`). Refresh it
        // from the live session state, Go's OK-packet status word.
        self.session.refresh_process_status();
        Ok(Some(in_transaction))
    }

    fn execute_ordinary_write_parsed(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<WriteOutcome, SqlQueryError> {
        let (affected_rows, last_insert_id) = self.run_ordinary_write(sql, stmt)?;
        Ok(WriteOutcome {
            affected_rows,
            last_insert_id,
        })
    }


    fn observe_routed_write(
        &mut self,
        sql: &str,
        stmt: &Stmt,
        execute: impl FnOnce(&mut Self) -> Result<WriteOutcome, SqlQueryError>,
    ) -> Result<WriteOutcome, SqlQueryError> {
        self.session.begin_routed_statement_observation(sql, stmt);
        self.session.observe_precompiled_privilege_tables();
        let result = execute(self);
        self.session.finish_routed_statement_observation_with_error(
            result.is_ok(),
            result.as_ref().map_or(0, |outcome| outcome.affected_rows),
            result.as_ref().err().map(|error| error.error_string()),
        );
        result
    }

    /// Go `TraceExec`: runs the wrapped statement through its normal route
    /// (write or result set), then answers with the spans of that run.
    fn execute_trace(
        mut self,
        trace: &tidb_ast::TraceStmt,
    ) -> Result<QueryResult<'s>, SqlQueryError> {
        crate::trace::check_options(trace).map_err(SqlQueryError::unknown)?;
        let inner = (*trace.statement).clone();
        let inner_sql = crate::trace::statement_text(&inner);
        // Go `TraceExec.Next`: the statement runs under the recording
        // tracer's `trace` span, `util.ContextWithTraceExecDetails` set.
        let recorder = crate::trace::StatementTrace::start();
        let entered = recorder.enter();
        // Go `executeChild`: `ExecuteStmt`, then `drainRecordSet` pulls the
        // record set it returned under the same span.
        let mut affected = 0_u64;
        match self.run_statement(&inner_sql, &inner, false) {
            Ok(Executed::Done(outcome)) => affected = outcome.affected_rows,
            Ok(opened) => match match opened {
                Executed::Query(execute_stmt) => {
                    let opened = self.reborrow().open_query(&inner_sql, &inner);
                    drop(execute_stmt);
                    opened
                }
                Executed::Trace => self.reborrow().execute_trace(trace_of(&inner)),
                Executed::Done(_) => unreachable!("an OK outcome is matched above"),
            } {
                Ok(mut result) => {
                    let mut rows = 0_u64;
                    loop {
                        match result.source().next_batch(1024) {
                            Ok(batch) if batch.is_empty() => {
                                recorder.event(&format!(
                                    "execute done, ReturnRow: {rows}, ModifyRow: 0"
                                ));
                                break;
                            }
                            Ok(batch) => rows += batch.len() as u64,
                            Err(failure) => {
                                recorder.event(&format!(
                                    "execute with error({}): {}",
                                    failure.code, failure.message
                                ));
                                break;
                            }
                        }
                    }
                }
                Err(failure) => recorder.event(&format!(
                    "execute with error({}): {}",
                    failure.code, failure.message
                )),
            },
            Err(failure) => recorder.event(&format!(
                "execute with error({}): {}",
                failure.code, failure.message
            )),
        }
        drop(entered);
        recorder.event(&format!("execute done, modify row: {affected}"));
        let (spans, started) = recorder.finish();
        let (columns, result_rows) = crate::trace::render(&trace.format, &spans, started)
            .map_err(SqlQueryError::unknown)?;
        let columns = crate::domain::pipeline_session::select_fields(&columns);
        Ok(QueryResult::new(Box::new(
            crate::domain::pipeline_session::MaterializedResultSetSource::new(columns, result_rows),
        )))
    }

    /// The unguarded rebuild: `BEGIN` calls this directly because the session
    /// already reports in-transaction by the time the transaction opens, and
    /// Go pins the LATEST schema at transaction start
    /// (`domain.GetSnapshotInfoSchema(startTS)`) — a table committed before
    /// `BEGIN` is visible to every statement of the new transaction.
    #[cfg(test)]
    fn rebuild_catalog_now(&mut self) {
        self.node.follower.rebuild(&mut self.session);
    }

    /// Go `Compile` then `executorBuilder.build`: the plan the statement
    /// compiles to (preprocess, `PlanBuilder.Build`, `CheckPrivilege`) picks
    /// the executor this node runs it with ([`Self::route_for`]).
    fn compile_route(&mut self, stmt: &tidb_ast::Stmt) -> Result<StatementRoute, SqlQueryError> {
        self.session.restore_statement_variables();
        let executor = self
            .session
            .compile_statement(stmt)
            .map_err(map_error)?;
        self.route_for(stmt, executor)
    }

    /// The executor this node runs a compiled statement with: the job-running
    /// DDL and the statistics executors are the node's; everything else is
    /// the session's own statement path ([`StatementRoute::Ordinary`]).
    fn route_for(
        &mut self,
        stmt: &tidb_ast::Stmt,
        executor: CompiledExecutor,
    ) -> Result<StatementRoute, SqlQueryError> {
        // The BDR role lives in the cluster's meta key; the session answers
        // from the store this node installed. Without one, answering from
        // the node's own catalog would report or set a role no other node
        // sees.
        if matches!(stmt, Stmt::Admin(admin) if matches!(
            admin.as_ref(),
            tidb_ast::AdminStmt::SetBdrRole(_)
                | tidb_ast::AdminStmt::UnsetBdrRole
                | tidb_ast::AdminStmt::ShowBdrRole
        )) && !self.session.has_bdr_role_store()
        {
            return Err(SqlQueryError::unknown(
                "ADMIN SET/UNSET/SHOW BDR ROLE needs this node's TiKV meta store",
            ));
        }
        match executor {
            // Go `SimpleExec.Next` switches on the statement node.
            CompiledExecutor::Simple => {
                if let Some(targets) = self.session.flush_stats_delta_targets(stmt) {
                    return Ok(StatementRoute::FlushStatsDelta(targets));
                }
                if let Some(physical_ids) =
                    self.session.drop_stats_ids(stmt).map_err(map_error)?
                {
                    return Ok(StatementRoute::DropStats(physical_ids));
                }
                Ok(StatementRoute::Ordinary)
            }
            CompiledExecutor::LoadStats => Ok(StatementRoute::LoadStats),
            CompiledExecutor::Analyze => {
                match prepare_cluster_analyze_parsed(stmt, self.session.current_database()) {
                    Ok(Some(tables)) if !tables.is_empty() => Ok(StatementRoute::Analyze(tables)),
                    Ok(_) => Err(SqlQueryError::unknown(
                        "this node runs ANALYZE TABLE for a named table only",
                    )),
                    Err(error) => Err(crate::domain::session_api::cluster_analyze_error(
                        tidb_exec::real_tikv_analyze::ClusterAnalyzeError::Analyze(error),
                    )),
                }
            }
            CompiledExecutor::StatsLock => {
                match prepare_cluster_stats_lock_parsed(
                    stmt,
                    self.session.current_database(),
                ) {
                    Ok(Some(statement)) => Ok(StatementRoute::StatsLock(statement)),
                    Ok(None) => Err(SqlQueryError::unknown(
                        "this node could not lower the statistics lock statement",
                    )),
                    Err(refusal) => Err(crate::domain::session_api::cluster_stats_lock_error(
                        ClusterStatsLockCommitError::Plan(refusal),
                    )),
                }
            }
            // Go `DDLExec.Next`: a statement the session's `DDLExec` answers
            // returns before `NewTxnInStmt`; every other DDL commits the open
            // transaction first and then meets the DDL executor's checks
            // ([`Self::run_ddl_exec`]).
            CompiledExecutor::Ddl => match stmt {
                Stmt::Ddl(ddl) => match self.session.ddl_exec_next(ddl) {
                    crate::ddl_exec::DdlExecNext::Job
                    | crate::ddl_exec::DdlExecNext::DropTableJob { .. } => {
                        Ok(StatementRoute::DdlExec)
                    }
                    _ => Ok(StatementRoute::SessionDdl),
                },
                _ => Ok(StatementRoute::DdlExec),
            },
            CompiledExecutor::Other => Ok(StatementRoute::Ordinary),
        }
    }

    /// Go `ExecStmt.handleNoDelayExecutor` for an account statement's
    /// `Simple` plan, which `Compile` has checked
    /// ([`Self::compile`]): where Go's executor calls `NewTxnInStmt`
    /// (`GrantExec`, `RevokeExec`, `SimpleExec.autoNewTxn`) the node commits
    /// the open transaction first, then the session runs the executor over
    /// the domain's privilege tables (`Session::with_account_service`). SET
    /// PASSWORD and SET DEFAULT ROLE leave the transaction open.
    fn run_account_executor(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<WriteOutcome, SqlQueryError> {
        if Session::simple_exec_new_txn_in_stmt(stmt) {
            self.new_txn_in_stmt()?;
            self.record_new_txn_in_stmt();
        }
        self
            .session
            .exec_statement_parsed(stmt.clone(), sql)
            .map_err(map_error)?;
        // Go answers an account statement with an OK packet carrying no rows,
        // whether it changed anything or was an `IF [NOT] EXISTS` no-op.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    fn stage_stats_notifier_event(
        &self,
        event: &tidb_ddl_notifier::SchemaChangeEvent,
    ) -> Result<(), SqlQueryError> {
        use tidb_model::ActionType;

        match event.action_type() {
            ActionType::ACTION_CREATE_TABLE => {
                let table = event.create_table_info();
                let ids = stats_physical_ids(table, self.dynamic_partition_pruning()?);
                self.stage_stats_table_change(table, None, &ids, &[], false)
            }
            ActionType::ACTION_TRUNCATE_TABLE => {
                let (table, old_table) = event.truncate_table_info();
                let new_ids = stats_physical_ids(table, self.dynamic_partition_pruning()?);
                let old_ids = stats_physical_ids(old_table, self.dynamic_partition_pruning()?);
                self.stage_stats_table_change(table, None, &new_ids, &old_ids, false)
            }
            ActionType::ACTION_DROP_TABLE => {
                let table = event.drop_table_info();
                let old_ids = stats_physical_ids(table, self.dynamic_partition_pruning()?);
                self.stage_stats_table_change(table, None, &[], &old_ids, false)
            }
            ActionType::ACTION_ADD_COLUMN => {
                let (table, columns) = event.add_column_info();
                self.stage_stats_columns(table, columns)
            }
            ActionType::ACTION_MODIFY_COLUMN => {
                let (table, columns, analyzed) = event.modify_column_info();
                if analyzed {
                    Ok(())
                } else {
                    self.stage_stats_columns(table, columns)
                }
            }
            ActionType::ACTION_ADD_TABLE_PARTITION => {
                let (table, added) = event.add_partition_info();
                let ids = added
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                self.stage_stats_table_change(table, Some(table.id), &ids, &[], false)
            }
            ActionType::ACTION_TRUNCATE_TABLE_PARTITION => {
                let (table, added, dropped) = event.truncate_partition_info();
                let inserted = added
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                let retired = dropped
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                self.stage_stats_table_change(table, Some(table.id), &inserted, &retired, true)
            }
            ActionType::ACTION_DROP_TABLE_PARTITION => {
                let (table, dropped) = event.drop_partition_info();
                let retired = dropped
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                self.stage_stats_table_change(table, Some(table.id), &[], &retired, true)
            }
            ActionType::ACTION_EXCHANGE_TABLE_PARTITION => {
                let (table, partition, standalone) = event.exchange_partition_info();
                let partition_id = partition
                    .definitions
                    .snapshot()
                    .first()
                    .ok_or_else(|| {
                        SqlQueryError::unknown("exchange event has no partition definition")
                    })?
                    .id;
                self.stage_exchange_partition_stats(table.id, partition_id, standalone.id)
            }
            ActionType::ACTION_REORGANIZE_PARTITION => {
                let (table, added, dropped) = event.reorganize_partition_info();
                let inserted = added
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                let retired = dropped
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                self.stage_stats_table_change(table, Some(table.id), &inserted, &retired, false)
            }
            ActionType::ACTION_ALTER_TABLE_PARTITIONING => {
                let (old_table_id, table, added) = event.add_partitioning_info();
                let inserted = added
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                self.stage_stats_table_change(table, Some(table.id), &inserted, &[], false)?;
                self.stage_change_global_stats_id(old_table_id, table.id)
            }
            ActionType::ACTION_REMOVE_PARTITIONING => {
                let (old_table_id, table, dropped) = event.remove_partitioning_info();
                self.stage_change_global_stats_id(old_table_id, table.id)?;
                let retired = dropped
                    .definitions
                    .snapshot()
                    .into_iter()
                    .map(|definition| definition.id)
                    .collect::<Vec<_>>();
                self.stage_stats_table_change(table, Some(table.id), &[], &retired, false)
            }
            ActionType::ACTION_DROP_SCHEMA => {
                let mut ids = Vec::new();
                for table in &event.drop_schema_info().tables {
                    ids.extend(table.partitions.iter().map(|partition| partition.id));
                    ids.push(table.id);
                }
                self.stage_stats_meta_refreshes(&ids, true)
            }
            ActionType::ACTION_FLASHBACK_CLUSTER => self.stage_stats_version_refresh(),
            ActionType::ACTION_ADD_INDEX => Ok(()),
            _ => {
                // Go `subscriber.handle`'s default case.
                tidb_stats_handle_logutil::stats_logger().error(
                    "Unhandled schema change event",
                    &[tidb_log::Field::new(
                        "type",
                        tidb_log::Value::Str(event.to_string()),
                    )],
                );
                Ok(())
            }
        }
    }

    fn notifier_transaction(&self) -> Result<&dyn OpenClusterTransaction, SqlQueryError> {
        self.node
            .txn
            .explicit
            .as_deref()
            .ok_or_else(|| SqlQueryError::unknown("DDL notifier handler has no active transaction"))
    }

    fn stage_stats_table_change(
        &self,
        table: &tidb_model::table_info::TableInfo,
        logical_table_id: Option<i64>,
        inserted_ids: &[i64],
        retired_ids: &[i64],
        adjust_global: bool,
    ) -> Result<(), SqlQueryError> {
        let row_ids = Arc::clone(&self.node.domain.auto_ids);
        let transaction = self.notifier_transaction()?;
        let catalog = self.node.domain.catalog.load();
        let version = transaction.start_ts();
        let loader = ClusterStatsLoader::locate(&catalog)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        for &new_id in inserted_ids {
            for statement in
                tidb_exec::cluster_stats_write::insert_table_stats_statements(table, new_id)
            {
                let now =
                    system_time_timestamp(SystemTime::now()).map_err(SqlQueryError::unknown)?;
                transactions::stage_pessimistic_statement(
                    transaction,
                    &self.node.buffer,
                    self.session.lock_wait_timeout(),
                    |snapshot, _| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let plan =
                            tidb_exec::cluster_stats_write::plan_insert_table_stats_statement(
                                &mut snapshot,
                                &catalog,
                                &statement,
                                version,
                                now,
                                Some(row_ids.as_ref()),
                            )
                            .map_err(|error| error.to_string())?;
                        Ok(((), plan.mutations))
                    },
                )
                .map_err(SqlQueryError::unknown)?;
            }
            self.record_schema_change_history(
                transaction,
                &self.node.buffer,
                &catalog,
                new_id,
                version,
            )
            .map_err(SqlQueryError::unknown)?;
        }

        let mut count = 0i64;
        if adjust_global {
            for &old_id in retired_ids {
                let snapshot = transaction
                    .snapshot_for(false)
                    .map_err(SqlQueryError::unknown)?;
                let snapshot = tidb_cluster::cluster_table_storage::overlay_staged_mutations(
                    snapshot,
                    &self.node.buffer,
                );
                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                count = count.wrapping_add(
                    loader
                        .load_meta(&mut snapshot, old_id)
                        .map_err(|error| SqlQueryError::unknown(error.to_string()))?
                        .map_or(0, |(_, _, _, count, _)| count as i64),
                );
            }
        }
        if count != 0 {
            let logical_table_id = logical_table_id.ok_or_else(|| {
                SqlQueryError::unknown(
                    "partition statistics update is missing its logical table id",
                )
            })?;
            let snapshot = transaction
                .snapshot_for(false)
                .map_err(SqlQueryError::unknown)?;
            let snapshot = tidb_cluster::cluster_table_storage::overlay_staged_mutations(
                snapshot,
                &self.node.buffer,
            );
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            let locked = tidb_exec::cluster_stats_write::load_stats_locked_table_ids(
                &mut snapshot,
                &catalog,
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            let update = tidb_stats_handle_usage::DeltaUpdate {
                table_id: logical_table_id,
                delta: tidb_stats_handle_usage::TableDelta {
                    delta: count.wrapping_neg(),
                    count,
                    init_time: None,
                },
                is_locked: locked.contains(&logical_table_id),
            };
            for statement in tidb_exec::cluster_stats_write::stats_delta_statements(&[update]) {
                let now =
                    system_time_timestamp(SystemTime::now()).map_err(SqlQueryError::unknown)?;
                transactions::stage_pessimistic_statement(
                    transaction,
                    &self.node.buffer,
                    self.session.lock_wait_timeout(),
                    |snapshot, _| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let plan = tidb_exec::cluster_stats_write::plan_stats_delta_statement(
                            &mut snapshot,
                            &catalog,
                            &statement,
                            version,
                            now,
                            Some(row_ids.as_ref()),
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(((), plan.mutations))
                    },
                )
                .map_err(SqlQueryError::unknown)?;
            }
        }
        self.stage_stats_meta_refreshes(retired_ids, false)
    }

    fn stage_stats_meta_refreshes(
        &self,
        ids: &[i64],
        best_effort: bool,
    ) -> Result<(), SqlQueryError> {
        let row_ids = Arc::clone(&self.node.domain.auto_ids);
        let transaction = self.notifier_transaction()?;
        let catalog = self.node.domain.catalog.load();
        let version = transaction.start_ts();
        for &physical_id in ids {
            let refreshed = transactions::stage_pessimistic_statement(
                transaction,
                &self.node.buffer,
                self.session.lock_wait_timeout(),
                |snapshot, _| {
                    let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                    let plan = tidb_exec::cluster_stats_write::plan_stats_meta_version_refresh(
                        &mut snapshot,
                        &catalog,
                        physical_id,
                        version,
                        Some(row_ids.as_ref()),
                    )
                    .map_err(|error| error.to_string())?;
                    Ok(((), plan.mutations))
                },
            );
            if let Err(error) = refreshed {
                if !best_effort {
                    return Err(SqlQueryError::unknown(error));
                }
                log_stats_gc_version_failed(physical_id, error);
                continue;
            }
            if let Err(error) = self.record_schema_change_history(
                transaction,
                &self.node.buffer,
                &catalog,
                physical_id,
                version,
            ) {
                if !best_effort {
                    return Err(SqlQueryError::unknown(error));
                }
                log_stats_gc_version_failed(physical_id, error);
            }
        }
        Ok(())
    }

    fn stage_exchange_partition_stats(
        &self,
        global_table_id: i64,
        partition_id: i64,
        standalone_table_id: i64,
    ) -> Result<(), SqlQueryError> {
        let row_ids = Arc::clone(&self.node.domain.auto_ids);
        let transaction = self.notifier_transaction()?;
        let catalog = self.node.domain.catalog.load();
        let snapshot = transaction
            .snapshot_for(false)
            .map_err(SqlQueryError::unknown)?;
        let snapshot = tidb_cluster::cluster_table_storage::overlay_staged_mutations(
            snapshot,
            &self.node.buffer,
        );
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        let loader = ClusterStatsLoader::locate(&catalog)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let partition = loader
            .load_meta(&mut snapshot, partition_id)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let standalone = loader
            .load_meta(&mut snapshot, standalone_table_id)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let (partition_modify, partition_count) = partition
            .map(|(_, _, modify, count, _)| (modify, count as i64))
            .unwrap_or_default();
        let (standalone_modify, standalone_count) = standalone
            .map(|(_, _, modify, count, _)| (modify, count as i64))
            .unwrap_or_default();
        let count_delta = standalone_count.wrapping_sub(partition_count);
        let modify_count_delta = standalone_count
            .wrapping_add(partition_count)
            .wrapping_sub(partition_modify)
            .wrapping_add(standalone_modify);
        if count_delta == 0 && modify_count_delta == 0 {
            return Ok(());
        }
        let locked =
            tidb_exec::cluster_stats_write::load_stats_locked_table_ids(&mut snapshot, &catalog)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?
                .contains(&global_table_id);
        let version = transaction.start_ts();
        transactions::stage_pessimistic_statement(
            transaction,
            &self.node.buffer,
            self.session.lock_wait_timeout(),
            |snapshot, _| {
                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                let plan = tidb_exec::cluster_stats_write::plan_exchange_partition_stats_update(
                    &mut snapshot,
                    &catalog,
                    global_table_id,
                    count_delta,
                    modify_count_delta,
                    locked,
                    version,
                    system_time_timestamp(SystemTime::now())?,
                    Some(row_ids.as_ref()),
                )
                .map_err(|error| error.to_string())?;
                Ok(((), plan.mutations))
            },
        )
        .map_err(SqlQueryError::unknown)
    }

    fn stage_stats_columns(
        &self,
        table: &tidb_model::table_info::TableInfo,
        columns: &[tidb_model::ColumnInfo],
    ) -> Result<(), SqlQueryError> {
        let row_ids = Arc::clone(&self.node.domain.auto_ids);
        let transaction = self.notifier_transaction()?;
        let catalog = self.node.domain.catalog.load();
        let version = transaction.start_ts();
        let loader = ClusterStatsLoader::locate(&catalog)
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        let physical_ids = stats_physical_ids(table, self.dynamic_partition_pruning()?);
        let defaults = columns
            .iter()
            .map(|column| {
                let value = if column.is_virtual_generated() {
                    None
                } else {
                    Some(
                        tidb_cluster::system_row_write::origin_default(column, table.name.original())
                            .map_err(|error| SqlQueryError::unknown(error.to_string()))?,
                    )
                };
                Ok((column, value))
            })
            .collect::<Result<Vec<_>, SqlQueryError>>()?;
        for physical_id in physical_ids {
            let snapshot = transaction
                .snapshot_for(false)
                .map_err(SqlQueryError::unknown)?;
            let snapshot = tidb_cluster::cluster_table_storage::overlay_staged_mutations(
                snapshot,
                &self.node.buffer,
            );
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            let Some((_, _, _, count, _)) = loader
                .load_meta(&mut snapshot, physical_id)
                .map_err(|error| SqlQueryError::unknown(error.to_string()))?
            else {
                continue;
            };
            let mut inserted_any = false;
            for (column, origin_default) in &defaults {
                let inserted = transactions::stage_pessimistic_statement(
                    transaction,
                    &self.node.buffer,
                    self.session.lock_wait_timeout(),
                    |snapshot, _| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let (inserted, plan) =
                            tidb_exec::cluster_stats_write::plan_insert_column_stats(
                                &mut snapshot,
                                &catalog,
                                physical_id,
                                column.id,
                                count as i64,
                                origin_default.as_ref(),
                                version,
                                system_time_timestamp(SystemTime::now())?,
                                Some(row_ids.as_ref()),
                            )
                            .map_err(|error| error.to_string())?;
                        Ok((inserted, plan.mutations))
                    },
                )
                .map_err(SqlQueryError::unknown)?;
                inserted_any |= inserted;
                if inserted {
                    if let Some(default) = origin_default.as_ref().filter(|value| !value.is_null())
                    {
                        transactions::stage_pessimistic_statement(
                            transaction,
                            &self.node.buffer, self.session.lock_wait_timeout(),
                            |snapshot, _| {
                                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                                let plan = tidb_exec::cluster_stats_write::plan_insert_column_default_bucket(
                                    &mut snapshot,
                                    &catalog,
                                    physical_id,
                                    column.id,
                                    count as i64,
                                    default,
                                    system_time_timestamp(SystemTime::now())?,
                                    Some(row_ids.as_ref()),
                                )
                                .map_err(|error| error.to_string())?;
                                Ok(((), plan.mutations))
                            },
                        )
                        .map_err(SqlQueryError::unknown)?;
                    }
                }
            }
            if inserted_any {
                transactions::stage_pessimistic_statement(
                    transaction,
                    &self.node.buffer,
                    self.session.lock_wait_timeout(),
                    |snapshot, _| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let plan = tidb_exec::cluster_stats_write::plan_stats_meta_version_refresh(
                            &mut snapshot,
                            &catalog,
                            physical_id,
                            version,
                            Some(row_ids.as_ref()),
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(((), plan.mutations))
                    },
                )
                .map_err(SqlQueryError::unknown)?;
            }
            self.record_schema_change_history(
                transaction,
                &self.node.buffer,
                &catalog,
                physical_id,
                version,
            )
            .map_err(SqlQueryError::unknown)?;
        }
        Ok(())
    }
    fn stage_change_global_stats_id(
        &self,
        old_table_id: i64,
        new_table_id: i64,
    ) -> Result<(), SqlQueryError> {
        let transaction = self.notifier_transaction()?;
        let catalog = self.node.domain.catalog.load();
        for target in tidb_exec::cluster_stats_write::GLOBAL_STATS_ID_TABLES {
            transactions::stage_pessimistic_statement(
                transaction,
                &self.node.buffer,
                self.session.lock_wait_timeout(),
                |snapshot, _| {
                    let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                    let plan = tidb_exec::cluster_stats_write::plan_change_global_stats_table_id(
                        &mut snapshot,
                        &catalog,
                        target,
                        old_table_id,
                        new_table_id,
                    )
                    .map_err(|error| error.to_string())?;
                    Ok(((), plan.mutations))
                },
            )
            .map_err(SqlQueryError::unknown)?;
        }
        Ok(())
    }

    fn stage_stats_version_refresh(&self) -> Result<(), SqlQueryError> {
        let transaction = self.notifier_transaction()?;
        let catalog = self.node.domain.catalog.load();
        let version = transaction.start_ts();
        for target in tidb_exec::cluster_stats_write::STATS_VERSION_TABLES {
            transactions::stage_pessimistic_statement(
                transaction,
                &self.node.buffer,
                self.session.lock_wait_timeout(),
                |snapshot, _| {
                    let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                    let plan = tidb_exec::cluster_stats_write::plan_update_stats_table_version(
                        &mut snapshot,
                        &catalog,
                        target,
                        version,
                        Some(self.node.domain.auto_ids.as_ref()),
                    )
                    .map_err(|error| error.to_string())?;
                    Ok(((), plan.mutations))
                },
            )
            .map_err(SqlQueryError::unknown)?;
        }
        Ok(())
    }

    fn record_schema_change_history(
        &self,
        transaction: &dyn OpenClusterTransaction,
        staged: &MutationBuffer,
        catalog: &ClusterCatalog,
        physical_id: i64,
        version: u64,
    ) -> Result<(), String> {
        let row_ids = Arc::clone(&self.node.domain.auto_ids);
        let enabled = self
            .node
            .domain
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value));
        if !enabled
            || !self
                .node
                .domain
                .stats
                .load()
                .get(&physical_id)
                .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                .is_some_and(|stats| stats.is_initialized())
        {
            return Ok(());
        }
        let (modify_count, count) = transactions::stage_pessimistic_statement(
            transaction,
            staged,
            self.session.lock_wait_timeout(),
            |snapshot, _| {
                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                let (counts, plan) =
                    tidb_exec::cluster_stats_write::plan_historical_stats_meta_lock(
                        &mut snapshot,
                        catalog,
                        physical_id,
                        version,
                    )
                    .map_err(|error| error.to_string())?;
                Ok((counts, plan.mutations))
            },
        )?;
        let now = tidb_exec::mysql_bootstrap::local_now_datetime6();
        transactions::stage_pessimistic_statement(
            transaction,
            staged,
            self.session.lock_wait_timeout(),
            |snapshot, _| {
                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                let plan = tidb_exec::cluster_stats_write::plan_historical_stats_meta_replace(
                    &mut snapshot,
                    catalog,
                    physical_id,
                    modify_count,
                    count,
                    version,
                    "schema change",
                    now,
                    Some(row_ids.as_ref()),
                )
                .map_err(|error| error.to_string())?;
                Ok(((), plan.mutations))
            },
        )
    }

    fn dynamic_partition_pruning(&self) -> Result<bool, SqlQueryError> {
        self.node
            .domain
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .map(|mode| mode == "dynamic")
            .map_err(|error| SqlQueryError::unknown(format!("{error:?}")))
    }

    /// Runs a statement whose stored-state change takes a route of its own
    /// (Go's DDL, account, `SET GLOBAL`, ANALYZE and statistics executors), and
    /// answers `None` for an ordinary statement. Both the write door and the
    /// result-set door come through here, so what a routed statement does
    /// cannot depend on which answer shape it would otherwise have taken.
    fn run_routed_statement(
        &mut self,
        route: StatementRoute,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<WriteOutcome>, SqlQueryError> {
        let outcome = match route {
            // Go's DDL returns once this domain has loaded the new schema,
            // so the issuing session sees it at once, not at its next
            // statement.
            StatementRoute::DdlExec => self
                .observe_routed_write(sql, stmt, |node| node.run_ddl_exec(sql, stmt))
                .inspect(|_| self.rebuild_catalog_if_stale()),
            StatementRoute::SessionDdl => {
                self.observe_routed_write(sql, stmt, |node| node.run_session_ddl(sql, stmt))
            }
            StatementRoute::Analyze(tables) => {
                self.observe_routed_write(sql, stmt, |node| node.run_analyze(&tables))
            }
            // Go `LoadStatsExec` records the path and the connection reads the
            // file from the client; a session's own reader is that transfer.
            StatementRoute::LoadStats => {
                let path = prepare_cluster_load_stats_parsed(stmt)
                    .map(|statement| statement.path)
                    .unwrap_or_default();
                let data = self
                    .session
                    .read_client_local_file(&path)
                    .map_err(map_error)?;
                self.run_load_stats(&data)
            }
            StatementRoute::FlushStatsDelta(targets) => {
                self.observe_routed_write(sql, stmt, |node| node.run_flush_stats_delta(&targets))
            }
            StatementRoute::StatsLock(statement) => {
                self.observe_routed_write(sql, stmt, |node| node.run_stats_lock(&statement))
            }
            StatementRoute::DropStats(physical_ids) => {
                self.observe_routed_write(sql, stmt, |node| node.run_drop_stats(&physical_ids))
            }
            StatementRoute::Ordinary => return Ok(None),
        };
        outcome.map(Some)
    }

    /// The services [`Domain::attach_connection`] installed on
    /// this connection's session.
    fn domain_services(&self) -> &crate::domain_services::DomainServices {
        self
            .session
            .domain_services()
            .expect("attach_connection installs the domain's services")
    }

    /// Go `sessiontxn.NewTxnInStmt` (`executor/ddl.go:151`, `simple.go:153`,
    /// `grant.go:144`, `revoke.go:71`): a DDL, account or ANALYZE statement
    /// runs under a session transaction opened at a fresh TSO, and the
    /// autocommit `CommitTxn` that ends the statement records it as
    /// `@@tidb_last_txn_info` (start only: nothing was written through it).
    /// `@@tidb_current_ts` is 0 again by the next statement, as in Go.
    pub(super) fn record_new_txn_in_stmt(&mut self) {
        if let Ok(start_ts) = self.node.domain.transactions.fresh_timestamp() {
            self.session.record_started_txn_info(start_ts);
        }
    }

    /// Go `preSplitAndScatterTable` after CREATE TABLE, on a node whose
    /// `split-table` is on (Go `EnableSplitTableRegion`).
    fn pre_split_created_tables(&mut self, created: &[i64]) {
        if created.is_empty() || !tidb_config::config_tree::config::get_global_config().split_table
        {
            return;
        }
        let regions = self.session.region_store();
        let scatter_scope = self
            .session
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_SCATTER_REGION)
            .unwrap_or_default();
        let loaded = self.node.domain.catalog.load();
        for id in created {
            let found = loaded
                .databases
                .iter()
                .flat_map(|database| database.tables.iter())
                .find(|table| table.id == *id);
            if let Some(table) = found {
                tidb_exec::pre_split::pre_split_and_scatter(
                    regions.as_ref(),
                    table,
                    &scatter_scope,
                );
            }
        }
    }

}

/// Go `session.Session`'s reads of the last statement a server connection
/// ran, which the driver's `QueryCtx` answers the wire with.
impl Session {
    /// Go `session.AffectedRows`: the OK packet's count.
    pub fn affected_rows(&self) -> u64 {
        self.node().ok_packet.affected_rows
    }

    /// Go `session.LastInsertID`: the OK packet's id, not `LAST_INSERT_ID()`
    /// ([`Session::last_insert_id`]).
    pub fn statement_last_insert_id(&self) -> u64 {
        self.node().ok_packet.last_insert_id
    }

    /// Go `ctx.Value(LoadDataVarKey)` / `ctx.Value(LoadStatsVarKey)`: the
    /// client-local path the last statement parked.
    pub fn local_infile_request(&self) -> Option<String> {
        self.node()
            .parked_local_infile
            .as_ref()
            .map(|parked| parked.path.clone())
    }
}

/// Go `DDLExec` over this connection: the session's DDL state and the store
/// steps the node takes for it.
impl tidb_executor::ddl_exec::DdlExec for ClusterServerSession<'_> {
    fn begin_ddl_statement(&mut self, parsed: &Stmt) -> Result<(), tidb_executor::DriverError> {
        self.session.begin_ddl_statement(parsed)
    }

    fn cluster_cleanup_table_lock_statement(
        &self,
        tables: &[Vec<String>],
    ) -> Result<Option<DdlStatement>, tidb_executor::DriverError> {
        self.session.cluster_cleanup_table_lock_statement(tables)
    }

    fn drop_table_job(
        &self,
        ddl: &tidb_ast::DdlStmt,
    ) -> Option<(tidb_ast::DropTableStmt, Vec<Vec<String>>)> {
        match self.session.ddl_exec_next(ddl) {
            crate::ddl_exec::DdlExecNext::DropTableJob { persistent, local } => {
                Some((persistent, local))
            }
            _ => None,
        }
    }

    fn ddl_statement_context(&self) -> tidb_executor::StmtContext {
        self.session.ddl_statement_context()
    }

    fn current_database(&self) -> &str {
        self.session.current_database()
    }

    fn session_vars(&self) -> &tidb_variable::SessionVars {
        self.session.vars()
    }

    fn ddl_target_checks(&mut self, stmt: &Stmt) -> Result<(), tidb_executor::DriverError> {
        self.session.ddl_target_checks(stmt)
    }

    fn cluster_lock_tables_statement(
        &mut self,
        locks: &[tidb_ast::TableLock],
    ) -> Result<DdlStatement, tidb_executor::DriverError> {
        self.session.cluster_lock_tables_statement(locks)
    }

    fn cluster_unlock_tables_statement(&self) -> Option<DdlStatement> {
        self.session.cluster_unlock_tables_statement()
    }

    fn require_table_exists(&mut self, path: &[String]) -> Result<(), tidb_executor::DriverError> {
        self.session.require_table_exists(path)
    }

    fn begin_routed_statement_warnings(&mut self) {
        self.session.begin_routed_statement_warnings();
    }

    fn drain_context_warnings(&mut self, ctx: &tidb_executor::StmtContext) {
        self.session.drain_context_warnings(ctx);
    }

    fn drop_local_temporary_tables(
        &mut self,
        names: &[Vec<String>],
    ) -> Result<(), tidb_executor::DriverError> {
        self.session.drop_local_temporary_tables(names)
    }

    fn sql_killer(&self) -> Arc<tidb_util::sqlkiller::SqlKiller> {
        self.session.sql_killer()
    }

    fn append_warning(
        &mut self,
        level: tidb_util::context::WarningLevel,
        code: u16,
        message: String,
    ) {
        self.session.append_warning(level, code, message);
    }

    fn set_statement_message(&mut self, message: String) {
        self.session.set_statement_message(message);
    }

    fn validate_snapshot_write(&self, stmt: &Stmt) -> Result<(), tidb_executor::DriverError> {
        self.session.validate_snapshot_write(stmt)
    }

    fn ddl_preserves_transaction(&self, stmt: &Stmt) -> bool {
        self.session.ddl_preserves_transaction(stmt)
    }

    fn exec_statement_parsed(
        &mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<StmtOutput, tidb_executor::DriverError> {
        self.session.exec_statement_parsed(stmt, sql)
    }

    fn statement_insert_id(&self) -> u64 {
        self.session.statement_insert_id()
    }

    fn new_txn_in_stmt(&mut self) -> Result<(), SqlQueryError> {
        ClusterServerSession::new_txn_in_stmt(self)
    }

    fn record_new_txn_in_stmt(&mut self) {
        ClusterServerSession::record_new_txn_in_stmt(self);
    }

    fn probe_resource_group_statement(
        &mut self,
        ddl: &tidb_ast::DdlStmt,
    ) -> Result<Option<DdlStatement>, SqlQueryError> {
        let resource_group = self.session.current_resource_group().to_owned();
        self.probe_statement(StatementReadShape::Unknown, &resource_group, |session| {
            session
                .cluster_resource_group_statement(ddl)
                .map_err(map_error)
        })
    }

    fn probe_cluster_view(
        &mut self,
        stmt: &Stmt,
    ) -> Result<Option<(String, String, bool, tidb_executor::ViewDef)>, SqlQueryError> {
        let resource_group = self.session.current_resource_group().to_owned();
        self.probe_statement(StatementReadShape::Unknown, &resource_group, |session| {
            session.resolve_cluster_view_parsed(stmt).map_err(map_error)
        })
    }

    fn published_catalog(&self) -> Arc<ClusterCatalog> {
        self.node.domain.catalog.load()
    }

    fn run_ddl_statement(
        &mut self,
        sql: &str,
        parsed: &Stmt,
        statement: &DdlStatement,
    ) -> Result<Option<i64>, tidb_executor::DriverError> {
        let service = Arc::clone(&self.domain_services().ddl);
        self.session
            .run_ddl_statement(sql, parsed, statement, service.as_ref())
    }

    fn tiflash_store_count(&self) -> Result<u64, String> {
        self.domain_services().ddl.tiflash_store_count()
    }

    fn pre_split_created_tables(&mut self, created: &[i64]) {
        ClusterServerSession::pre_split_created_tables(self, created);
    }
}

/// Go `QueryCtx`'s steps that run on the store, behind the server's driver
/// for [`Session`].
impl<'s> ClusterServerSession<'s> {
    pub fn execute_stmt(
        mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<QueryResult<'s>>, SqlQueryError> {
        match self.run_statement(sql, stmt, true)? {
            Executed::Done(_) => Ok(None),
            Executed::Query(_execute_stmt) => self.open_query(sql, stmt).map(Some),
            Executed::Trace => self.execute_trace(trace_of(stmt)).map(Some),
        }
    }

    /// Go `session.ExecuteStmt` up to its record set: the statement context,
    /// a LOCAL file request parked, transaction control, `Compile`, and
    /// `runStmt` for a statement that answers no rows -- what is left for
    /// the caller is opening a query's (or a TRACE's) record set.
    /// `record_failure` is Go `TiDBContext.ExecuteStmt` appending the error
    /// to the statement context, which the wire's connection does.
    fn run_statement(
        &mut self,
        sql: &str,
        stmt: &Stmt,
        record_failure: bool,
    ) -> Result<Executed, SqlQueryError> {
        // Go builds a fresh statement context per statement: the previous
        // statement's counts and any file exchange it left unfinished end here.
        self.node.ok_packet = WriteOutcome::default();
        self.node.parked_local_infile = None;
        self.session.clear_local_infile();
        // Go `LoadDataExec` / `LoadStatsExec`: the statement runs far enough
        // to be admitted, then parks its file request on the session.
        if let Some(path) = self.local_infile_path_parsed(sql, stmt)? {
            self.accept_local_infile_parsed(sql, stmt)?;
            self.node.parked_local_infile = Some(ParkedLocalInfile {
                sql: sql.to_owned(),
                stmt: stmt.clone(),
                path,
            });
            return Ok(Executed::Done(WriteOutcome::default()));
        }
        // A text `EXECUTE` of a prepared DDL runs that DDL (Go `ExecuteExec`
        // builds the prepared plan as its body).
        if let Some((body_sql, body)) = self.session.prepared_ddl_body(stmt) {
            return self.run_statement(&body_sql, &body, record_failure);
        }
        if self.control_transaction_parsed(sql, stmt)?.is_some() {
            return Ok(Executed::Done(WriteOutcome::default()));
        }
        // Go's `session.ExecuteStmt` region covers `Compile` and `runStmt`;
        // a query's record set is opened inside it and drained outside.
        let execute_stmt = tidb_util::tracing::begin_thread_region_scope("session.ExecuteStmt");
        self.begin_statement();
        let compiled = match self.compile(sql, stmt) {
            Ok(CompiledStatement::Query) => return Ok(Executed::Query(execute_stmt)),
            // Go `TraceExec` runs the statement it wraps and reports its spans.
            Ok(CompiledStatement::Trace) => return Ok(Executed::Trace),
            compiled => compiled,
        };
        match compiled.and_then(|compiled| self.run_without_result(compiled, sql, stmt)) {
            Ok(outcome) => {
                self.node.ok_packet = outcome;
                Ok(Executed::Done(outcome))
            }
            Err(error) => {
                // Go `TiDBContext.ExecuteStmt` appends the error to the
                // statement context; the data-conversion codes a DML filed as
                // its own warning are not appended twice.
                if record_failure
                    && (matches!(stmt, Stmt::Ddl(_))
                        || !matches!(error.code, 1148 | 1210 | 1264 | 1265 | 1366 | 1406))
                {
                    self
                        .session
                        .record_ddl_failure(error.code, error.message.clone(), error.identity);
                }
                Err(error)
            }
        }
    }

    pub fn finish_local_infile(&mut self, data: &[u8]) -> Result<(), SqlQueryError> {
        let parked = self.node.parked_local_infile.take().ok_or_else(|| {
            SqlQueryError::unknown("statement did not request a client-local file")
        })?;
        self.node.ok_packet = self.execute_local_infile_parsed(&parked.sql, &parked.stmt, data)?;
        Ok(())
    }

    pub fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.session.select_database(name).map_err(map_error)
    }


    pub fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        // Transaction control carries no markers and answers with no columns,
        // so there is nothing to probe -- and probing it would RUN it, which
        // for `ROLLBACK` means publishing the buffer this connection is about
        // to discard. It is applied at EXECUTE, through
        // [`Self::control_transaction`].
        let parsed = (*self.session).parse_statement(sql).map_err(map_error)?;
        self.session.restore_statement_variables();
        if classify_transaction_control_stmt(&parsed).is_some() {
            return Ok(PreparedGeneral::new(sql.to_owned(), 0, Vec::new()));
        }
        // Go `PrepareExec` plans against the transaction's infoschema
        // (`GetTxnInfoSchema`): the latest schema outside a transaction, so a
        // table another connection created since this one's last statement
        // resolves.
        self.rebuild_catalog_if_stale();
        let prepared_ast = self
            .session
            .prepare_ast_parsed(sql, parsed)
            .map_err(map_error)?;
        self.session
            .note_prepared_plan_cache_refusal(prepared_ast.statement())
            .map_err(map_error)?;
        let parameter_count = prepared_ast.parameter_count();
        let kind = self.session.statement_kind_parsed(prepared_ast.statement());
        if kind == StmtKind::Write {
            // A prepared DDL is admitted here and executed at EXECUTE, so a
            // refusal -- an unsupported shape, an unsupported column type --
            // is reported at PREPARE, where Go reports it too. Its WARNINGS
            // (a FLUSH STATS_DELTA's missing tables, lowering notes) are
            // Go's execution-time ones: PREPARE shows only its plan-cache
            // note, and EXECUTE routes the statement again.
            let warnings_before = self.session.warnings().len();
            // Go's PREPARE preprocesses and builds the statement; its
            // privileges are EXECUTE's.
            let executor = self
                .session
                .compile_statement_for_prepare(prepared_ast.statement())
                .map_err(map_error)?;
            self.route_for(prepared_ast.statement(), executor)?;
            self.session.truncate_warnings(warnings_before);
            // Keep ordinary DML's parsed tree as well.  The MySQL binary
            // protocol prepares YCSB's INSERT/UPDATE/DELETE once and then
            // executes that handle thousands of times; throwing this tree
            // away here would send every execution back through parse and
            // SQL-text binding, defeating the prepared path's fixed-plan
            // work.  Routed schema/account/ANALYZE statements still use their
            // dedicated SQL route at EXECUTE and must not be run through the
            // ordinary statement driver.
            if !matches!(prepared_ast.statement(), tidb_ast::Stmt::Dml(_)) {
                return Ok(PreparedGeneral::new(
                    sql.to_owned(),
                    parameter_count,
                    Vec::new(),
                ));
            }
            return Ok(PreparedGeneral::with_prepared_ast(prepared_ast, Vec::new()));
        }
        // Go reports a query's result columns at PREPARE time from the
        // prepared plan schema. It does not execute the query with every
        // marker bound to NULL; doing so can make a large range scan during
        // COM_STMT_PREPARE (and can pin a transaction under autocommit=0).
        // The bound AST carries NULL markers solely for type inference. The
        // plan-only path does not open or drain an executor.
        let probe: Vec<tidb_datatype::Datum> =
            std::iter::repeat_n(tidb_datatype::Datum::Null, parameter_count).collect();
        let bound_probe = prepared_ast.bind(&probe).map_err(map_error)?;
        let result_columns = match self.session.plan_bound_prepared_columns(bound_probe) {
            Ok(columns) => crate::domain::pipeline_session::select_fields(&columns),
            // A variable error (Go's `variable` package) is the statement's
            // own failure and is reported at prepare time.
            Err(error)
                if error.clone().to_mysql_error().identity.is_some_and(|identity| {
                    identity.class() == tidb_error::terror::TerrorClass::Variable
                }) =>
            {
                return Err(map_error(error))
            }
            // A query whose metadata cannot be resolved without real values
            // reports none at prepare time, matching the existing wire
            // fallback. Such shapes remain on the ordinary execute path.
            Err(_) => Vec::new(),
        };
        Ok(PreparedGeneral::with_prepared_ast(
            prepared_ast,
            result_columns,
        ))
    }


    pub fn execute_general(
        mut self,
        statement: &PreparedGeneral,
        values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'s>, SqlQueryError> {
        self.session.restore_statement_variables();
        let process_statement = self
            .session
            .retain_process_statement_with_digest(statement.sql(), statement.digest());
        // A `BEGIN` is a `BEGIN` whichever protocol carried it. Run as an
        // ordinary statement it would flip only the driver session's own flag
        // and leave `self.node.txn.explicit` unopened -- the two pieces of transaction
        // state disagreeing, with every following statement reading at a fresh
        // timestamp and a racing writer never detected. Routed here the state
        // cannot diverge, whoever calls this.
        // A retained ordinary template cannot be transaction control:
        // `prepare_general` returns an untemplated handle for BEGIN/COMMIT/
        // ROLLBACK.  Avoid reparsing the SQL text on every EXECUTE for the
        // common DML/query case; the untemplated branch still preserves the
        // transaction-control route.
        if statement.template().is_none() && classify_transaction_control(statement.sql()).is_some()
        {
            self.session.set_binary_prepared_execution(true);
            let result = self.control_transaction(statement.sql());
            self.session.set_binary_prepared_execution(false);
            result?;
            return Ok(GeneralExecuteOutcome::Write(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        // `prepare_general` has already routed ordinary parsed DML/query
        // templates. Re-running `statement_stored_state_change` here would
        // parse the same SQL text on every EXECUTE (visible in YCSB insert
        // samples); only routed statements without a retained template need
        // the SQL route at execute time.
        if statement.template().is_none() {
            // prepare_general retains only text for dedicated routes. Parse
            // once and use the same completion/durability owner as COM_QUERY.
            let parsed = (*self.session)
                .parse_statement(statement.sql())
                .map_err(map_error)?;
            self.session.set_binary_prepared_execution(true);
            let result = self.execute_write_parsed(statement.sql(), &parsed);
            self.session.set_binary_prepared_execution(false);
            if let Some(outcome) = result? {
                return Ok(GeneralExecuteOutcome::Write(outcome));
            }
        }
        // Bind and classify against the catalog and transaction that this
        // execution will use, not the state preceding the statement refresh.
        let params = crate::domain::pipeline_session::prepared_parameters(values);
        let sql = statement.sql().to_owned();
        let prepared_ast = statement.prepared_ast().cloned();
        let retained = statement.template();
        let (effective_template, binding_sql) = retained.map_or((None, None), |template| {
            let (effective, binding_sql) =
                self.session.prepared_statement_with_binding(template);
            (Some(effective), binding_sql)
        });
        let effective = effective_template.as_deref();
        // Plan-cache validation must see the current schema before the read
        // policy is declared. Rebuilding only inside the statement lifecycle
        // would let a stale row-handle plan choose MaxTS and then fall back to
        // a newly rebuilt, potentially multi-read plan.
        self.rebuild_catalog_if_stale();
        let resource_group = match effective {
            Some(template) => self.session.statement_resource_group(template).into_owned(),
            None => self
                .session
                .statement_resource_group_sql(statement.sql())
                .map_err(map_error)?,
        };
        self.prepare_statement_context(
            &resource_group,
            effective.is_some_and(|stmt| matches!(stmt, Stmt::Query(_))),
        )?;
        let cache_allowed = effective.is_some_and(|template| {
            self.session
                .prepared_plan_cache_allowed_for_statement(template)
        });
        // Bind the retained point plan once and use that same plan's
        // `noSecondRead` classification for snapshot selection. The previous
        // path rebuilt the whole point matcher on every EXECUTE, discarded
        // it, then bound this cached plan separately.
        let cached_point_get = cache_allowed
            .then(|| statement.point_get_plan())
            .flatten()
            .and_then(|plan| {
                self.session.bind_cached_prepared_point_get_for_binding(
                    &plan,
                    &params,
                    binding_sql.as_deref(),
                )
            });
        let point_read_shape = cached_point_get
            .as_ref()
            .map(|execution| execution.plan().statement_read_shape());
        let fast = cached_point_get.is_some();
        // On a Go plan-cache miss, physical optimization first gives
        // TryFastPlan this shape; the point plan it returns is what later
        // cache hits rebuild. Do not substitute the generic SELECT tree for
        // that point plan merely because both descriptors were retained.
        let cached_select = if fast || !cache_allowed {
            None
        } else {
            statement.select_plan().and_then(|plan| {
                self.session.bind_cached_prepared_select_for_statement(
                    &plan,
                    &params,
                    effective.expect("cacheable prepared SELECT retains a statement"),
                    binding_sql.as_deref(),
                )
            })
        };
        let fast_select = cached_select.is_some();
        let cached_dml = cache_allowed
            .then(|| statement.dml_plan())
            .flatten()
            .and_then(|plan| {
                self.session.bind_cached_prepared_dml_for_statement(
                    &plan,
                    &params,
                    effective.expect("cacheable prepared DML retains a statement"),
                    binding_sql.as_deref(),
                )
            });
        let direct_dml = cached_dml.is_some();
        let direct = fast || fast_select || direct_dml;
        let bound_template = if direct && !fast_select {
            None
        } else {
            effective
                .map(|template| tidb_executor::bind_statement(template.clone(), &params))
                .transpose()
                .map_err(map_error)?
        };
        let shape = if fast {
            point_read_shape.unwrap_or(StatementReadShape::Unknown)
        } else if fast_select {
            // A retained physical SELECT still needs the locking statement's
            // fresh for-update snapshot and lock-value cache on every replay.
            // Classify this execution's values, not the unbound PREPARE
            // markers: a complete point key may use MaxTS even on cache hits.
            self.session.statement_read_shape_bound(
                bound_template.as_ref().expect("cached SELECT is bound"),
            )
        } else if direct_dml {
            StatementReadShape::AutocommitWrite
        } else {
            bound_template.as_ref().map_or_else(
                || self.session.statement_read_shape(&sql, &params),
                |bound| self.session.statement_read_shape_bound(bound),
            )
        };
        let is_write = match effective {
            Some(template) => self.session.statement_kind_parsed(template) == StmtKind::Write,
            None => self.session.statement_kind(&sql).map_err(map_error)? == StmtKind::Write,
        };
        if effective.is_some_and(|stmt| matches!(stmt, Stmt::Query(_))) {
            // Go `executePlanCacheStmt`'s `ContextWithInitializedExecDetails`.
            self.session.initialize_statement_exec_details(&sql);
            let result = self.open_statement_result(shape, &resource_group, |session| {
                session.set_binary_prepared_execution(true);
                let opened = (|| {
                    if let Some(point) = &cached_point_get {
                        if let Some(opened) = session.open_prepared_point_get(
                            point.clone(),
                            effective.expect("point plan retains its statement"),
                            &sql,
                        )? {
                            return Ok(opened);
                        }
                    }
                    if let Some(cached) = &cached_select {
                        session.open_prepared_record_set_for(
                            cached,
                            prepared_ast
                                .as_ref()
                                .expect("cached SELECT retains its definition"),
                        )
                    } else {
                        // A point-plan identity miss rebuilds the ordinary bound plan.
                        let bound = match &bound_template {
                            Some(bound) => bound.clone(),
                            None => tidb_executor::bind_statement(
                                effective
                                    .expect("prepared query retains its template")
                                    .clone(),
                                &params,
                            )?,
                        };
                        session.open_bound_record_set_for(
                            bound,
                            prepared_ast
                                .as_ref()
                                .expect("prepared query retains its definition"),
                        )
                    }
                })();
                session.set_binary_prepared_execution(false);
                opened.map_err(map_error)
            })?;
            return Ok(GeneralExecuteOutcome::Rows(match process_statement {
                Some(statement) => result.with_process_statement(statement),
                None => result,
            }));
        }
        // Session finishes its scratch execution inside the durable storage
        // attempt. Retain one observation until commit/retry has completed.
        let observed_write = is_write && effective.is_some();
        if let Some(stmt) = effective.filter(|_| is_write) {
            self.session
                .begin_prepared_routed_statement_observation(&sql, stmt);
        }
        self.session.set_binary_prepared_execution(true);
        self.node.activate_statement_txn =
            is_write && effective.is_some_and(|stmt| self.activates_txn_at_build(stmt));
        let attempt = self.with_prelocked_statement(shape, &resource_group, is_write, |session| {
            if direct_dml {
                let cached = cached_dml
                    .as_ref()
                    .expect("direct prepared DML carries its bound statement");
                let output = session
                    .execute_cached_prepared_dml_for(
                        cached,
                        prepared_ast
                            .as_ref()
                            .expect("direct prepared DML has its retained definition"),
                    )
                    .map_err(map_error)?;
                return Ok(output);
            }
            if let Some(bound) = bound_template.as_ref().cloned() {
                session
                    .run_parsed_bound_owned_for(
                        bound,
                        prepared_ast
                            .as_ref()
                            .expect("a retained template has its prepared definition"),
                    )
                    .map_err(map_error)
            } else {
                session
                    .exec_statement_with_params(&sql, &params)
                    .map_err(map_error)
            }
        });
        self.node.activate_statement_txn = false;
        self.session.set_binary_prepared_execution(false);
        if observed_write {
            let affected_rows = match &attempt {
                Ok(StmtOutput::Affected(count)) => *count,
                _ => 0,
            };
            self.session.finish_routed_statement_observation_with_error(
                attempt.is_ok(),
                affected_rows,
                attempt.as_ref().err().map(|error| error.error_string()),
            );
        }
        let output = attempt?;
        if is_write {
            let affected_rows = match &output {
                StmtOutput::Affected(count) => *count,
                StmtOutput::Rows { .. } | StmtOutput::Done(_) => 0,
            };
            self.session.add_statement_read_keys(affected_rows);
        }
        let result_authority = matches!(&output, StmtOutput::Rows { .. })
            .then(|| self.session.result_materialization_authority());
        Ok(match output {
            StmtOutput::Rows { columns, rows } => {
                let field_types = columns.iter().map(|(_, field)| field.clone()).collect();
                let result = QueryResult::new(Box::new(MaterializedResultSetSource::new(
                    crate::domain::pipeline_session::select_fields(&columns),
                    rows,
                )))
                .with_cursor_materialization(
                    field_types,
                    result_authority.expect("a row result carries materialization authority"),
                )
                .with_statement_status(
                    self.session.wire_warning_count(),
                    WireStatus::of_session(&self.session),
                )
                .with_statement_output(
                    self.session.statement_affected_rows(),
                    self.session.statement_insert_id(),
                    self.session.statement_message().as_bytes().to_vec(),
                );
                GeneralExecuteOutcome::Rows(match process_statement {
                    Some(statement) => result.with_process_statement(statement),
                    None => result,
                })
            }
            StmtOutput::Affected(count) => GeneralExecuteOutcome::Write(WriteOutcome {
                affected_rows: count,
                last_insert_id: self.session.statement_insert_id(),
            }),
            StmtOutput::Done(_) => GeneralExecuteOutcome::Write(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }),
        })
    }
}

/// The steps Go's `ExecuteStmt` takes for each statement kind, run by
/// [`ClusterServerSession::execute_stmt`] and by internal callers that know the
/// statement's kind.
impl<'s> ClusterServerSession<'s> {
    pub(crate) fn local_infile_path_parsed(
        &mut self,
        _sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<String>, SqlQueryError> {
        // Go `LoadDataExec.Open` asks the client for a LOCAL load's file.
        if let Stmt::Dml(dml) = stmt {
            if let tidb_ast::DmlStmt::LoadData(load) = dml.as_ref() {
                return Ok(load.local.then(|| load.path.clone()));
            }
        }
        let Some(statement) = prepare_cluster_load_stats_parsed(stmt) else {
            return Ok(None);
        };
        if statement.path.is_empty() {
            return Err(SqlQueryError::unknown("Load Stats: file path is empty"));
        }
        Ok(Some(statement.path))
    }

    pub(crate) fn accept_local_infile_parsed(
        &mut self,
        _sql: &str,
        stmt: &Stmt,
    ) -> Result<(), SqlQueryError> {
        // The executor is built against the current schema, as the write
        // door builds every statement.
        self.rebuild_catalog_if_stale();
        self.session.accept_local_infile(stmt).map_err(map_error)
    }

    pub(crate) fn execute_local_infile_parsed(
        &mut self,
        sql: &str,
        stmt: &Stmt,
        data: &[u8],
    ) -> Result<WriteOutcome, SqlQueryError> {
        self.session.clear_statement_message();
        if matches!(stmt, Stmt::Dml(dml) if matches!(dml.as_ref(), tidb_ast::DmlStmt::LoadData(_)))
        {
            // The load is an ordinary write over the client's bytes.
            self.session.set_local_infile_data(data.to_vec());
            let outcome = self.observe_routed_write(sql, stmt, |node| {
                node.execute_ordinary_write_parsed(sql, stmt)
            });
            self.session.clear_local_infile();
            return outcome;
        }
        if prepare_cluster_load_stats_parsed(stmt).is_none() {
            return Err(SqlQueryError::unknown(
                "statement did not request a client-local file",
            ));
        }
        self.run_load_stats(data)
    }

    /// Maps `BEGIN`/`COMMIT`/`ROLLBACK` onto the connection's buffer.
    ///
    /// The driver session owns the *state* (so `in_transaction` and the
    /// statement's OK-packet status flag agree with the in-process tier); this
    /// adds what the state means for cluster storage.
    pub(crate) fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        let stmt = (*self.session).parse_statement(sql).map_err(map_error)?;
        self.control_transaction_parsed(sql, &stmt)
    }

    pub(crate) fn control_transaction_parsed(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<bool>, SqlQueryError> {
        let observed = classify_transaction_control_stmt(stmt).is_some();
        // Go runs a transaction statement as any other: `session.ExecuteStmt`
        // compiles its `Simple` plan and `runStmt` drives the `SimpleExec`.
        let _execute_stmt =
            observed.then(|| tidb_util::tracing::begin_thread_region_scope("session.ExecuteStmt"));
        if observed {
            compile_simple_statement(sql);
            self.session.begin_routed_statement_observation(sql, stmt);
        }
        let result = self
            .control_transaction_parsed_inner(sql, stmt)
            .map_err(|error| self.record_statement_failure(error));
        if observed {
            self.session.finish_routed_statement_observation_with_error(
                result.is_ok(),
                0,
                result.as_ref().err().map(|error| error.error_string()),
            );
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn execute_write(
        &mut self,
        sql: &str,
    ) -> Result<Option<WriteOutcome>, SqlQueryError> {
        let stmt = (*self.session).parse_statement(sql).map_err(|error| {
            self.session.record_parse_failure(&error);
            map_error(error)
        })?;
        self.execute_write_parsed(sql, &stmt)
    }

    /// One parse per command (Go `session.ParseSQL`, done by the connection
    /// through [`Session::parse_at_statement_boundary`]); routing, the SET door, the
    /// kind, the resource group, the prelock keys and the run all read that
    /// node.
    /// Go `ExecuteInternal`: one statement of internal SQL through
    /// [`ClusterServerSession::execute_stmt`]. Answers the rows of a result set, none
    /// otherwise; [`Session::affected_rows`] then holds the count.
    pub(crate) fn execute_internal(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, SqlQueryError> {
        let stmt = self
            .session
            .parse_at_statement_boundary(sql)
            .map_err(map_error)?;
        let Some(mut result) = self.reborrow().execute_stmt(sql, &stmt)? else {
            return Ok(Vec::new());
        };
        let to_sql_error = |error: tidb_executor::MysqlError| SqlQueryError::from(error);
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(1024).map_err(to_sql_error)?;
            if batch.is_empty() {
                break;
            }
            rows.extend(batch);
        }
        source.finish().map_err(to_sql_error)?;
        source.close().map_err(to_sql_error)?;
        Ok(rows)
    }

    /// [`Self::run_statement`] for a caller that runs a statement for its
    /// OK outcome, opening a query itself when there is none.
    pub(crate) fn execute_write_parsed(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<WriteOutcome>, SqlQueryError> {
        Ok(match self.run_statement(sql, stmt, false)? {
            Executed::Done(outcome) => Some(outcome),
            Executed::Query(_) | Executed::Trace => None,
        })
    }

    /// Go `PrepareTxnCtx` and `ResetContextOfStmt`: once per statement,
    /// before it is compiled.
    fn begin_statement(&mut self) {
        // Go `TxnCtx.StatementCount`: every statement inside an explicit
        // transaction counts toward its COMMIT observation.
        if let Some(transaction) = self.node.txn.explicit.as_ref() {
            transaction.note_statement();
        }
        // Go builds a fresh `TxnCtx` -- membuffer and GLOBAL temporary rows --
        // for every statement no transaction continues into: neither this
        // node's explicit transaction nor one the session itself opened.
        if self.node.txn.explicit.is_none() {
            self.session.start_statement_transaction_scope();
        }
        self.session.clear_statement_message();
        // No plan the previous statement compiled outlives it.
        self.session.clear_precompiled_statement();
        // A statement without a query result leaves no result field names.
        self.session.last_result_output_names.clear();
        // Go resolves and plans against the current infoschema before it
        // chooses a statement snapshot. Keep schema refresh ahead of routing,
        // access-shape classification, and the statement lifecycle.
        self.rebuild_catalog_if_stale();
    }

    /// Go `executor.Compiler.Compile` as this node runs it: the plan kind
    /// that decides which executor runs the statement, as Go's
    /// `executorBuilder.build` switches on the plan's type. Preceded by
    /// `ExecuteStmt`'s `validateStatementReadOnlyInStaleness`
    /// (`session.go:2485`), and starts with `checkSEMStmt`, as Go's
    /// `PlanBuilder.Build` does for every statement.
    fn compile(&mut self, sql: &str, stmt: &Stmt) -> Result<CompiledStatement, SqlQueryError> {
        self
            .session
            .validate_statement_read_only_in_staleness(stmt)
            .map_err(map_error)?;
        self
            .session
            .check_sem_stmt(sql, stmt)
            .map_err(map_error)?;
        self.session.restore_statement_variables();
        // Go `buildDDL` builds a CREATE VIEW body, whose non-correlated
        // subqueries `Build` evaluates (`EvalSubqueryFirstRow`), so that one
        // DDL compiles with a statement snapshot to read through.
        let executor = if matches!(stmt, Stmt::Ddl(ddl) if matches!(**ddl, tidb_ast::DdlStmt::CreateView(_)))
        {
            let resource_group = self.session.current_resource_group().to_owned();
            self.probe_statement(StatementReadShape::Unknown, &resource_group, |session| {
                session.compile_statement(stmt).map_err(map_error)
            })?
        } else {
            self
                .session
                .compile_statement(stmt)
                .map_err(map_error)?
        };
        // An account statement's `SimpleExec` commits the user's transaction
        // after `Compile` ([`Self::run_account_executor`]).
        if executor == CompiledExecutor::Simple && Session::is_account_statement(stmt) {
            return Ok(CompiledStatement::AccountSimple);
        }
        // The node's own executors answer whatever shape the plan has.
        let route = self.route_for(stmt, executor)?;
        if !matches!(route, StatementRoute::Ordinary) {
            return Ok(CompiledStatement::StoredState(route));
        }
        if matches!(stmt, Stmt::Session(node) if matches!(node.as_ref(), tidb_ast::SessionStmt::Set(_) | tidb_ast::SessionStmt::SetPassword(_)))
        {
            return Ok(CompiledStatement::Set);
        }
        if self.session.statement_kind_parsed(stmt) == StmtKind::Write {
            return Ok(CompiledStatement::Write);
        }
        if matches!(stmt, Stmt::Admin(admin) if matches!(admin.as_ref(), tidb_ast::AdminStmt::Trace(_)))
        {
            return Ok(CompiledStatement::Trace);
        }
        Ok(CompiledStatement::Query)
    }

    /// Go's `runStmt` for a compiled statement that answers no result set.
    fn run_without_result(
        &mut self,
        compiled: CompiledStatement,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<WriteOutcome, SqlQueryError> {
        match compiled {
            CompiledStatement::StoredState(route) => Ok(self
                .run_routed_statement(route, sql, stmt)?
                .expect("a stored-state route answers an outcome")),
            // Go runs `SetExecutor` through the ordinary statement
            // lifecycle: a value's subquery reads the statement snapshot,
            // and a failure is recorded as the statement's error.
            CompiledStatement::AccountSimple => self.observe_routed_write(sql, stmt, |node| {
                node.run_account_executor(sql, stmt)
            }),
            CompiledStatement::Set => self.observe_routed_write(sql, stmt, |node| {
                let outcome = node.execute_ordinary_write_parsed(sql, stmt)?;
                // SET autocommit=1 may commit the open transaction.
                node.commit_if_session_left_transaction()?;
                Ok(outcome)
            }),
            // Go `session.ExecuteStmt`: compile and `runStmt`, the autocommit
            // commit included.
            CompiledStatement::Write => self.observe_routed_write(sql, stmt, |node| {
                node.execute_ordinary_write_parsed(sql, stmt)
            }),
            CompiledStatement::Trace | CompiledStatement::Query => {
                unreachable!("a result-set plan is opened, not run for an outcome")
            }
        }
    }

    pub(crate) fn execute(self, sql: &str) -> Result<QueryResult<'s>, SqlQueryError> {
        let stmt = (*self.session).parse_statement(sql).map_err(|error| {
            self.session.record_parse_failure(&error);
            map_error(error)
        })?;
        self.execute_parsed(sql, &stmt)
    }

    /// One parse per command (Go `session.ParseSQL`, done by the connection
    /// through [`Session::parse_at_statement_boundary`]).
    pub(crate) fn execute_parsed(
        mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<QueryResult<'s>, SqlQueryError> {
        match self.run_statement(sql, stmt, false)? {
            Executed::Done(outcome) => Ok(QueryResult::new(Box::new(
                crate::domain::pipeline_session::affected_rows_source(outcome.affected_rows),
            ))),
            Executed::Query(_execute_stmt) => self.open_query(sql, stmt),
            Executed::Trace => self.execute_trace(trace_of(stmt)),
        }
    }

    /// Go's `runStmt` for a statement that answers a result set.
    fn open_query(self, sql: &str, stmt: &Stmt) -> Result<QueryResult<'s>, SqlQueryError> {
        if let Stmt::Dml(dml) = stmt {
            if let tidb_ast::DmlStmt::Batch(batch) = dml.as_ref() {
                return self.run_non_transactional_dml(batch);
            }
        }
        // Go `handleStmt`'s `ContextWithInitializedExecDetails`, before the
        // statement below can reach storage -- and before `ExecuteStmt`
        // publishes the process info, which takes the digest computed here.
        self.session.initialize_statement_exec_details(sql);
        let process_statement = match self.session.pending_statement_digest(sql) {
            Some(digest) => self
                .session
                .retain_process_statement_with_digest(sql, &digest),
            None => self.session.retain_process_statement(sql),
        };
        let resource_group = self.session.statement_resource_group(stmt).into_owned();
        let shape = self.session.statement_read_shape_bound(stmt);
        let result = self.open_statement_result(shape, &resource_group, |session| {
            session
                .open_record_set_parsed(stmt.clone(), sql)
                .map_err(map_error)
        })?;
        Ok(match process_statement {
            Some(statement) => result.with_process_statement(statement),
            None => result,
        })
    }
}


/// The cluster's [`ClusterDdl`] as the session's [`crate::ddl_service::DdlService`]:
/// the same publish-and-refresh call, with its client error carried across the
/// crate boundary as the already-classified `(code, state, message)` triple.
/// The cluster account writer and this node's system sessions as the
/// session's [`AccountService`].
///
/// [`AccountService`]: crate::stored_state_service::AccountService
struct ClusterAccountService {
    accounts: Arc<dyn ClusterAccountWriter>,
    /// The node, whose domain pool Go's executor takes its system session
    /// from (`GetSysSession`).
    factory: Weak<Domain>,
}

impl crate::stored_state_service::AccountService for ClusterAccountService {
    fn load_accounts(&self) -> Result<PrivilegeRegistry, tidb_executor::DriverError> {
        self.accounts
            .load_accounts()
            .map_err(tidb_executor::DriverError::unsupported)
    }

    fn system_session(
        &self,
    ) -> Result<Box<dyn crate::user_table::AccountSqlSession>, tidb_executor::DriverError> {
        let factory = self.factory.upgrade().ok_or_else(|| {
            tidb_executor::DriverError::unsupported("cluster session factory is stopped")
        })?;
        let session = factory
            .open_system_session()
            .map_err(sql_query_error_to_driver)?;
        Ok(Box::new(AccountSystemSession(session)))
    }

    fn notify_update_privilege(&self) -> Result<(), tidb_executor::DriverError> {
        self.accounts
            .notify_update_privilege()
            .map_err(tidb_executor::DriverError::unsupported)
    }
}

/// A system session running an account executor's SQL.
struct AccountSystemSession(Session);

/// Go `withRestrictedSQLExecutor` without `ExecOptionUseCurSession`: each
/// statement takes an internal session of this node (`getInternalSession`)
/// and runs in that session's own autocommit transaction.
struct NodeRestrictedSql {
    factory: Weak<Domain>,
}

impl crate::restricted_sql::RestrictedSqlExecutor for NodeRestrictedSql {
    fn exec_restricted_sql(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_executor::DriverError> {
        let factory = self.factory.upgrade().ok_or_else(|| {
            tidb_executor::DriverError::unsupported("cluster session factory is stopped")
        })?;
        let session = factory
            .open_system_session()
            .map_err(sql_query_error_to_driver)?;
        crate::user_table::AccountSqlSession::execute(&mut AccountSystemSession(session), sql)
    }
}

impl crate::user_table::AccountSqlSession for AccountSystemSession {
    fn execute(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_executor::DriverError> {
        self.0
            .node_conn()
            .execute_internal(sql)
            .map_err(sql_query_error_to_driver)
    }
}

/// The cluster sysvar writer as the session's [`SysvarService`].
///
/// [`SysvarService`]: crate::stored_state_service::SysvarService
struct ClusterSysvarService(Arc<dyn ClusterSysvarWriter>);

struct ClusterPendingSysvarChange(Box<dyn crate::domain::cluster_sysvar_seam::PendingSysvarChange>);

impl crate::stored_state_service::SysvarService for ClusterSysvarService {
    fn begin(
        &self,
    ) -> Result<
        Box<dyn crate::stored_state_service::PendingSysvarChange>,
        tidb_executor::DriverError,
    > {
        let pending = self
            .0
            .begin()
            .map_err(tidb_executor::DriverError::unsupported)?;
        Ok(Box::new(ClusterPendingSysvarChange(pending)))
    }
}

impl crate::stored_state_service::PendingSysvarChange for ClusterPendingSysvarChange {
    fn table(&self) -> GlobalSysvars {
        self.0.table()
    }

    fn commit(self: Box<Self>) -> Result<Vec<String>, tidb_executor::DriverError> {
        self.0.commit().map_err(sql_query_error_to_driver)
    }
}

fn sql_query_error_to_driver(error: SqlQueryError) -> tidb_executor::DriverError {
    tidb_executor::DriverError::Mysql(
        tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
            .with_identity_opt(error.identity),
    )
}

struct ClusterDdlService {
    ddl: Arc<dyn ClusterDdl>,
    /// The node's latest schema (Go the DDL's `infoCache.GetLatest()`).
    catalog: Arc<SharedClusterCatalog>,
}

impl crate::ddl_service::DdlService for ClusterDdlService {
    fn execute(
        &self,
        statement: &DdlStatement,
        job_context: &tidb_executor::StmtContext,
    ) -> Result<tidb_ddl::real_tikv_ddl::ClusterDdlReport, tidb_executor::DriverError> {
        self.ddl.execute(statement, job_context).map_err(|error| {
            tidb_executor::DriverError::Mysql(
                tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
                    .with_identity_opt(error.identity),
            )
        })
    }

    fn check_cached_table_ddl(
        &self,
        stmt: &Stmt,
        current_db: &str,
    ) -> Result<(), tidb_executor::DriverError> {
        tidb_ddl::cluster_ddl::cached_table::check_cached_table_ddl(
            &self.catalog.load(),
            current_db,
            stmt,
        )
        .map_err(tidb_executor::DriverError::Mysql)
    }

    fn tiflash_store_count(&self) -> Result<u64, String> {
        self.ddl.tiflash_store_count().map_err(|error| error.message)
    }
}


/// Go `copr.MPPClient` over the node's `KVStore`.
#[must_use]
pub fn build_mpp_client(
    authority: &tidb_cluster::real_tikv_read::ProductionReadProcessAuthority,
    schema_version: impl Fn() -> i64 + Send + Sync + 'static,
) -> tidb_exec::mpp_client::TiFlashMppClient {
    tidb_exec::mpp_client::TiFlashMppClient::new(
        authority.tikv_store().clone(),
        schema_version,
    )
}

/// Spawns the TiFlash replica availability poller over the node's catalog
/// watch, DDL executor, and the store's PD HTTP client (Go
/// `initTiFlashReplicaManager` over `is.pdHTTPCli`). A store without a PD
/// HTTP client simply never flips availability; the read path answers
/// accordingly.
#[must_use]
pub fn build_tiflash_replica_poll(
    catalog: std::sync::Arc<tidb_cluster::catalog_watch::SharedCatalog>,
    pd_http_client: Option<tikv_client::pd_http::Client>,
    security: &tidb_pd_client::ClusterSecurity,
    ddl: Arc<dyn tidb_ddl::tiflash_replica_manager::TiFlashReplicaControl>,
) -> Option<tidb_ddl::tiflash_replica_manager::TiFlashReplicaPoller> {
    let pd_http_client = pd_http_client?;
    let result = tidb_ddl::tiflash_replica_manager::TiFlashReplicaManager::with_pd_http_client(
        catalog,
        ddl,
        pd_http_client,
        security,
    );
    match result {
        Ok(manager) => Some(manager.spawn()),
        Err(error) => {
            eprintln!("tiflash replica poll startup: {error}");
            None
        }
    }
}

#[cfg(test)]
mod tests;

mod login_policy;
mod settings;

struct ClusterExternalTimestamp(Arc<dyn ClusterTransactions>);
impl std::fmt::Debug for ClusterExternalTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterExternalTimestamp")
    }
}
impl crate::vars::ExternalTimestampProvider for ClusterExternalTimestamp {
    fn get(&self) -> Result<u64, String> {
        self.0.external_timestamp(None)
    }
    fn set(&self, ts: u64) -> Result<(), String> {
        self.0.external_timestamp(Some(ts)).map(|_| ())
    }
}
