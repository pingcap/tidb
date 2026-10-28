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
use tidb_executor::compiler::PreparedAstCompile;
use std::borrow::Cow;
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
use tidb_ddl::real_tikv_ddl::prepare_cluster_ddl_parsed;
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
    ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};
use crate::domain::wire_status::WireStatus;

fn is_analyze_table_sql(sql: &str) -> bool {
    tidb_parser::normalize_digest(sql)
        .0
        .starts_with("analyze table")
}

/// The PD/TiKV control-plane deadline this node's boot and statements use, the
/// same one the bounded node applies.
pub const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Pinned Go `statistics/handle.StatsOwnerKey`.
pub const STATS_OWNER_KEY: &str = "/tidb/stats/owner";
/// Pinned Go `statistics/handle.StatsPrompt`.
pub const STATS_OWNER_PROMPT: &str = "stats";

/// Go `kv.ErrWriteConflict`, the one cause an autocommit statement is replayed
/// for.

mod bdr_role_store;
mod catalog_follower;
mod binding_maintenance;
use tidb_ddl::worker as ddl;
mod ddl_job_history_store;
mod ddl_notifier;
pub mod gc_worker;
use crate::statement_lifecycle::{ReadStatement, StatementLifecycle};
#[cfg(test)]
use crate::statement_lifecycle::{back_off_upper_ms, jitter_below, RETRY_BACK_OFF_CAP_MS};
#[cfg(test)]
use tidb_cluster::cluster_table_storage::LockKeysOutcome;
#[cfg(test)]
#[cfg(test)]
const ERR_WRITE_CONFLICT: u16 = tidb_cluster::pessimistic_lock_error::ERR_WRITE_CONFLICT;
use crate::txn_manager::{CommitHooks, CommitTxnRegions, TxnManager};
mod non_transactional_dml;
mod record_set;
mod ru_stats_writer;
pub mod schema_lease;
pub mod schema_sync;
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

/// What [`ClusterServerSession::lower_ddl_exec`] lowered a DDL to.
enum LoweredDdl {
    /// The job(s) to run in order, stopping at the first failure (Go
    /// `AlterTable`'s table-option loop is several).
    Jobs(Vec<DdlStatement>),
    /// Go `DDLExec.Next`'s DROP that also names LOCAL temporary tables: the
    /// job drops the rest, then `dropLocalTemporaryTables` drops `local`.
    DropTableJob {
        statement: DdlStatement,
        local: Vec<Vec<String>>,
    },
    /// Go's executor functions that submit several jobs in order (the
    /// table-option loop, `AlterSchema`): each part is lowered when its turn
    /// comes, so a refusal leaves the earlier parts applied, as Go's does.
    Sequence {
        parts: Vec<Stmt>,
        context: tidb_executor::StmtContext,
    },
}

impl LoweredDdl {
    fn one(statement: DdlStatement) -> Self {
        Self::Jobs(vec![statement])
    }
}

/// The statements one Go executor function runs as several jobs in order --
/// a multi-option `ALTER TABLE ... <table options>` (Go `AlterTable`'s
/// option loop) and an `ALTER DATABASE` with several changes (Go
/// `AlterSchema`). `None` for every other statement.
fn job_sequence_parts(stmt: &Stmt) -> Option<Vec<Stmt>> {
    let Stmt::Ddl(ddl) = stmt else {
        return None;
    };
    let parts = match &**ddl {
        tidb_ast::DdlStmt::AlterTable(alter) => tidb_ddl::cluster_ddl::split_table_option_alter(alter)?
            .into_iter()
            .map(|part| tidb_ast::DdlStmt::AlterTable(Box::new(part)))
            .collect(),
        tidb_ast::DdlStmt::AlterDatabase { name, options } => {
            tidb_ddl::cluster_ddl::split_alter_database(name, options)?
        }
        _ => return None,
    };
    Some(
        parts
            .into_iter()
            .map(|part| Stmt::Ddl(tidb_ast::NodeBox::new(part)))
            .collect(),
    )
}

/// A DDL shape this node does not plan.
fn unplanned_ddl() -> SqlQueryError {
    SqlQueryError::unknown(
        "this node changes the cluster's catalog for CREATE TABLE, DROP TABLE, \
         CREATE DATABASE, DROP DATABASE, CREATE/DROP VIEW, placement policy \
         changes, index changes, and single-action ALTER INDEX changes only; \
         run this statement on a TiDB server",
    )
}

struct ClusterDataLockWaits {
    transactions: Arc<dyn ClusterTransactions>,
}

struct ClusterStatisticsItemLoader {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    stats: Arc<SharedStats>,
    global_vars: GlobalSysvars,
}

/// The domain loading resources outlive each connection's schema catalog.
struct ClusterStatisticsLoading {
    loader: Arc<ClusterStatisticsItemLoader>,
    workers: Arc<tidb_executor::driver::StatisticsLoadWorkers>,
    view: Arc<tidb_executor::driver::StatisticsView>,
}

impl ClusterStatisticsLoading {
    /// The statistics handle one session plans from: the domain's view, and
    /// the storage loader behind the synchronous-load workers.
    fn handle(&self) -> Arc<tidb_stats_handle::StatsHandle> {
        let handle = Arc::new(tidb_stats_handle::StatsHandle::default());
        handle.set_statistics_view(Arc::clone(&self.view));
        handle.set_item_loader(self.loader.clone(), Arc::clone(&self.workers));
        handle
    }
}

struct ClusterStatisticsSource(Arc<SharedStats>);

impl tidb_executor::driver::StatisticsSource for ClusterStatisticsSource {
    fn table(&self, physical_id: i64) -> Option<Arc<tidb_stats::Table>> {
        self.0
            .load()
            .get(&physical_id)
            .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
            .cloned()
    }

    fn get_physical_table_stats(
        &self,
        physical_id: i64,
        table: &tidb_executor::driver::PseudoStatsTable,
    ) -> Option<Arc<tidb_stats::Table>> {
        self.0
            .get_physical_table_stats(physical_id, table.partitioned, table.cacheable, || {
                tidb_stats::pseudo_table(&table.info, false, true)
            })
    }
}

struct ClusterColumnStatsUsageProvider {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
}

struct ClusterTableStorageStatsProvider {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    /// Go `cache.TableRowStatsCache`
    /// (`pkg/statistics/handle/cache/stats_table_row_cache.go`): the size
    /// cache the information-schema `TABLES`/`PARTITIONS` readers refresh in
    /// batch and then serve from. Go keeps one process-wide instance; this
    /// node owns one per provider.
    row_stats_cache: StatsTableRowCache,
}

impl ClusterTableStorageStatsProvider {
    /// The two restricted reads Go's `UpdateByID` performs inside
    /// `getRowCountTables`/`getColLengthTables`. Kept behind
    /// [`StatsTableRowSizeSource`] so the cache can enforce its
    /// both-reads-or-nothing copy contract.
    fn size_reads(
        &self,
        catalog: &tidb_cluster::cluster_catalog::ClusterCatalog,
        resource_group: &str,
        ids: &[i64],
        need_column_lengths: bool,
    ) -> Result<SizeReads, String> {
        let loader = ClusterStatsLoader::locate(catalog).map_err(|error| error.to_string())?;
        let row_snapshot = self.transactions.open_snapshot(resource_group)?;
        let rows = loader
            .load_table_row_counts(&mut SnapshotMetaSnapshot::new(row_snapshot), ids)
            .map_err(|error| error.to_string())?;
        let lengths = if need_column_lengths {
            let length_snapshot = self.transactions.open_snapshot(resource_group)?;
            loader
                .load_column_lengths(&mut SnapshotMetaSnapshot::new(length_snapshot), ids)
                .map_err(|error| error.to_string())?
        } else {
            // Statement-local `TableSizeStats` skips the histogram read when
            // no retained projection consumes column lengths; the cached map
            // is then left unchanged, which those projections never observe.
            Vec::new()
        };
        Ok((
            rows.into_iter()
                .map(|row| (row.table_id, row.count))
                .collect(),
            lengths
                .into_iter()
                .map(|row| {
                    (
                        (row.table_id, row.histogram_id),
                        u64::try_from(row.total_size.max(0)).expect("nonnegative i64 fits in u64"),
                    )
                })
                .collect(),
        ))
    }
}

/// The size source one refresh closes over: Go's `UpdateByID` runs both
/// reads against one session context; this adapter runs them against one
/// provider, catalog image, and per-read statement snapshots. Both reads are
/// performed once, on the first call, exactly like Go's sequential pair.
/// One refreshed pair of restricted reads: the row counts and the column
/// lengths Go's `UpdateByID` copies into the cache together.
type SizeReads = (Vec<(i64, u64)>, Vec<(TableHistId, u64)>);

struct ProviderSizeReads<'a> {
    provider: &'a ClusterTableStorageStatsProvider,
    catalog: &'a tidb_cluster::cluster_catalog::ClusterCatalog,
    resource_group: &'a str,
    need_column_lengths: bool,
    reads: RefCell<Option<Result<SizeReads, String>>>,
}

impl ProviderSizeReads<'_> {
    /// Performs both restricted reads once, on the first call, exactly like
    /// Go's sequential `getRowCountTables`/`getColLengthTables` pair.
    fn load(&self, ids: &[i64]) -> Result<SizeReads, String> {
        {
            let mut reads = self.reads.borrow_mut();
            if reads.is_none() {
                *reads = Some(self.provider.size_reads(
                    self.catalog,
                    self.resource_group,
                    ids,
                    self.need_column_lengths,
                ));
            }
        }
        self.reads
            .borrow()
            .as_ref()
            .expect("reads are computed above")
            .clone()
    }
}

impl StatsTableRowSizeSource for ProviderSizeReads<'_> {
    type Error = String;

    fn read_row_counts(&self, ids: &[i64]) -> Result<Vec<(i64, u64)>, Self::Error> {
        self.load(ids).map(|(rows, _)| rows)
    }

    fn read_column_lengths(&self, ids: &[i64]) -> Result<Vec<(TableHistId, u64)>, Self::Error> {
        self.load(ids).map(|(_, lengths)| lengths)
    }
}

impl TableStorageStatsProvider for ClusterTableStorageStatsProvider {
    fn load_table_row_stats(
        &self,
        resource_group: &str,
        need_column_lengths: bool,
        physical_ids: &[i64],
    ) -> Result<tidb_executor::table_size_stats::TableRowStatsSnapshot, String> {
        // The latest catalog only locates `mysql.stats_meta` and
        // `mysql.stats_histograms`, as Go's restricted SQL resolves them; the
        // tables sized are the statement's own (`physical_ids`).
        let catalog = self.catalog.load();

        // Go `updateStatsCacheIfNeed` + `TableRowStatsCache.UpdateByID`
        // (`executor/infoschema_reader.go:671`): refresh the node's size cache
        // for every physical ID the statement's schema holds first. A failed
        // restricted read only warns — the reader then serves the cache's
        // previous values, not zeros.
        let source = ProviderSizeReads {
            provider: self,
            catalog: &catalog,
            resource_group,
            need_column_lengths,
            reads: RefCell::new(None),
        };
        if let Err(error) = self.row_stats_cache.update_by_id(&source, physical_ids) {
            // Go `updateStatsCacheIfNeed`.
            tidb_util::logutil::bg_logger().warn(
                "cannot update stats cache for tables",
                &[crate::domain::zap_error(error)],
            );
        }

        // Go's readers answer from `cache.TableRowStatsCache` after the
        // refresh attempt, so the returned values are the cache's, whatever
        // the fresh reads did.
        let (rows, lengths) = self.row_stats_cache.snapshot();
        Ok(tidb_executor::table_size_stats::TableRowStatsSnapshot {
            table_rows: rows.into_iter().collect(),
            column_lengths: lengths.into_iter().collect(),
        })
    }
}

struct ClusterApproximateTableCountProvider {
    transactions: Arc<dyn ClusterTransactions>,
    cache: Arc<Mutex<tidb_exec::pd_approximate_count::ApproximateTableCountCache>>,
    cleanup_tx: std::sync::mpsc::SyncSender<bool>,
    cleanup_worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl ClusterApproximateTableCountProvider {
    fn new(transactions: Arc<dyn ClusterTransactions>) -> Self {
        let cache = Arc::new(Mutex::new(
            tidb_exec::pd_approximate_count::ApproximateTableCountCache::new(
                1_048_576,
                Duration::from_secs(30),
            ),
        ));
        let (cleanup_tx, cleanup_rx) = std::sync::mpsc::sync_channel(1);
        let cleanup_cache = Arc::clone(&cache);
        let cleanup_worker = std::thread::Builder::new()
            .name("pd-helper-cleanup".to_owned())
            .spawn(move || loop {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let wait = cleanup_cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .next_expiration_delay(now)
                    .max(Duration::from_micros(1));
                match cleanup_rx.recv_timeout(wait) {
                    Ok(true) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Ok(false) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default();
                        cleanup_cache
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .delete_expired(now);
                    }
                }
            })
            .expect("PD helper cleanup worker must start");
        Self {
            transactions,
            cache,
            cleanup_tx,
            cleanup_worker: Mutex::new(Some(cleanup_worker)),
        }
    }
}

impl Drop for ClusterApproximateTableCountProvider {
    fn drop(&mut self) {
        let _ = self.cleanup_tx.send(true);
        if let Some(worker) = self
            .cleanup_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = worker.join();
        }
    }
}

impl tidb_exec::real_tikv_analyze::ApproximateTableCountProvider
    for ClusterApproximateTableCountProvider
{
    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> (f64, bool) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let key = tidb_exec::pd_approximate_count::approximate_table_count_key(
            physical_id,
            database,
            table,
            partition,
        );
        if let Some(count) = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key, now)
        {
            return (count, true);
        }
        // Go's concurrent cache releases its lock before invoking PD or
        // restricted SQL, so concurrent misses may load the same key twice.
        let loaded = match self.transactions.record_region_stats(physical_id) {
            Ok(Some(stats)) if stats.count > 2 => (stats.storage_keys as f64, true),
            Ok(Some(_)) => {
                exact_physical_row_count(self.transactions.as_ref(), resource_group, physical_id)
                    .map_or((0.0, false), |count| (count as f64, true))
            }
            Ok(None) | Err(_) => (0.0, false),
        };
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, now, loaded.0);
        let _ = self.cleanup_tx.try_send(false);
        loaded
    }
}

struct ClusterAnalyzeStatusProvider {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    approximate_counts: Arc<ClusterApproximateTableCountProvider>,
}

impl crate::AnalyzeStatusProvider for ClusterAnalyzeStatusProvider {
    fn load_analyze_status(
        &self,
        resource_group: &str,
    ) -> Result<Vec<tidb_stats::AnalyzeStatusJob>, String> {
        let snapshot = self.transactions.open_snapshot(resource_group)?;
        tidb_exec::cluster_stats_write::load_analyze_status_jobs(
            &mut SnapshotMetaSnapshot::new(snapshot),
            &self.catalog.load(),
        )
        .map_err(|error| error.to_string())
    }

    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> i64 {
        tidb_exec::real_tikv_analyze::ApproximateTableCountProvider::approximate_table_count(
            self.approximate_counts.as_ref(),
            resource_group,
            physical_id,
            database,
            table,
            partition,
        )
        .0 as i64
    }
}

fn exact_physical_row_count(
    transactions: &dyn ClusterTransactions,
    resource_group: &str,
    physical_id: i64,
) -> Result<i64, String> {
    const PAGE_SIZE: usize = 1_024;
    let prefix = tidb_txnkv::Key::from_bytes(tidb_tablecodec::table_key::gen_table_record_prefix(
        physical_id,
    ));
    let end = prefix.prefix_next();
    let mut cursor = prefix;
    let mut snapshot = transactions.open_snapshot(resource_group)?;
    let mut count = 0_i64;
    loop {
        let page = snapshot
            .scan(&cursor, &end, Some(PAGE_SIZE))
            .map_err(|error| error.to_string())?;
        count = count.saturating_add(i64::try_from(page.len()).unwrap_or(i64::MAX));
        if page.len() < PAGE_SIZE {
            return Ok(count);
        }
        cursor = tidb_txnkv::Key::from_bytes(page.last().expect("full page is nonempty").0.clone())
            .next();
    }
}

impl crate::ColumnStatsUsageProvider for ClusterColumnStatsUsageProvider {
    fn load_column_stats_usage(
        &self,
        location: &tidb_datatype::SessionTimeZone,
        resource_group: &str,
    ) -> Result<
        std::collections::HashMap<
            tidb_model::TableItemID,
            (Option<tidb_datatype::Time>, Option<tidb_datatype::Time>),
        >,
        String,
    > {
        let snapshot = self.transactions.open_snapshot(resource_group)?;
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        tidb_exec::cluster_predicate_column::load_column_stats_usage(
            &mut snapshot,
            &self.catalog.load(),
            location,
        )
        .map(|usage| {
            usage
                .into_iter()
                .map(|(item, times)| (item, (times.last_used_at, times.last_analyzed_at)))
                .collect()
        })
        .map_err(|error| error.to_string())
    }
}

impl tidb_executor::driver::StatisticsItemLoader for ClusterStatisticsItemLoader {
    fn load_items(
        &self,
        items: &[tidb_model::StatsLoadItem],
        resource_group: &str,
    ) -> Result<Vec<(i64, Arc<tidb_executor::access_cost::TableStatistics>)>, String> {
        let catalog = self.catalog.load();
        let loader = ClusterStatsLoader::locate(&catalog).map_err(|error| error.to_string())?;
        let skipped_column_types = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES)
            .map(|value| crate::varsutil::parse_analyze_skip_column_types(&value))
            .unwrap_or_default();
        for requested in items {
            let item = requested.table_item_id;
            let Some(table) = catalog
                .databases
                .iter()
                .flat_map(|database| &database.tables)
                .find(|table| table.id == item.table_id)
            else {
                continue;
            };
            if item.is_index
                && !table
                    .indices
                    .iter_deref()
                    .any(|index| index.read().id == item.id)
            {
                continue;
            }
            let column_type = (!item.is_index)
                .then(|| {
                    table.cols().iter_deref().find_map(|column| {
                        let column = column.read();
                        (column.id == item.id).then(|| column.field_type.clone())
                    })
                })
                .flatten();
            if !item.is_index && column_type.is_none() {
                continue;
            }
            if column_type.as_ref().is_some_and(|field_type| {
                skipped_column_types.contains(tidb_datatype::type_to_str(
                    field_type.code(),
                    field_type.charset_name(),
                ))
            }) {
                continue;
            }
            let current = self.stats.load();
            let Some(current) = current
                .get(&item.table_id)
                .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
            else {
                continue;
            };
            if item.is_index {
                if !current.index_load_needed(item.id).1 {
                    continue;
                }
            } else {
                let (_, load_needed, analyzed) =
                    current.column_load_needed(item.id, requested.full_load);
                if !load_needed {
                    continue;
                }
                if !analyzed {
                    let empty = tidb_exec::cluster_stats_load::ClusterStatsItem {
                        id: item.id,
                        is_index: false,
                        stats_ver: 0,
                        flag: 0,
                        load_status: tidb_stats::StatsLoadedStatus::default(),
                        histogram: tidb_stats::Histogram {
                            id: item.id,
                            ..tidb_stats::Histogram::default()
                        },
                        topn: None,
                        cms: None,
                        fm_sketch: None,
                    };
                    self.stats.update_item(item.table_id, empty, table);
                    continue;
                }
            }
            let snapshot = self.transactions.open_snapshot(resource_group)?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            let started = std::time::Instant::now();
            let loaded = loader
                .load_item(
                    &mut snapshot,
                    item.table_id,
                    item.is_index,
                    item.id,
                    column_type.as_ref(),
                    requested.full_load,
                )
                .map_err(|error| error.to_string())?;
            if let Some(loaded) = loaded {
                // Go observes only a successful read with histogram metadata,
                // before cache publication. Skips and failed attempts do not
                // contribute to read latency.
                tidb_stats_handle_metrics::READ_STATS_HISTOGRAM
                    .observe(started.elapsed().as_millis() as f64);
                self.stats.update_item(item.table_id, loaded, table);
            }
        }
        // The domain publication above is authoritative for every session.
        Ok(Vec::new())
    }
}

impl crate::DataLockWaitsProvider for ClusterDataLockWaits {
    fn lock_waits(&self) -> Result<Vec<crate::DataLockWait>, String> {
        self.transactions.lock_waits().map(|entries| {
            entries
                .into_iter()
                .map(|entry| crate::DataLockWait {
                    txn: entry.txn,
                    wait_for_txn: entry.wait_for_txn,
                    key: entry.key,
                    resource_group_tag: entry.resource_group_tag,
                })
                .collect()
        })
    }
}

/// Go `domain.Domain`: the node's schema image and its follower, the
/// statistics handle, the DDL and transaction clients, the privilege and
/// process registries and the background workers -- shared by every session
/// it opens, one cluster-backed [`Session`] per authenticated connection.
pub struct Domain {
    global_config_syncer: Option<Arc<tidb_domain::globalconfigsync::GlobalConfigSyncer>>,
    bindings: Option<Arc<dyn crate::domain::cluster_binding_seam::ClusterBindings>>,
    binding_session_factory: Weak<Domain>,
    /// The `SHOW STATUS` providers this domain registered as it started its
    /// DDL and binding handle, unregistered when it closes.
    statistics: Vec<Arc<dyn crate::statusvar::Statistics>>,
    /// The write/read capability every connection's statements open their
    /// snapshots and publish their commits through.
    transactions: Arc<dyn ClusterTransactions>,
    /// Storage-backed wait-for graph exposed by `DATA_LOCK_WAITS`.
    data_lock_waits: Arc<ClusterDataLockWaits>,
    /// The route a stored-schema change this node can express takes.
    ddl: Arc<dyn ClusterDdl>,
    /// The route a stored-account change takes; see
    /// [`crate::domain::cluster_account_seam`].
    accounts: Arc<dyn ClusterAccountWriter>,
    /// The route a `SET GLOBAL` change takes; see
    /// [`crate::domain::cluster_sysvar_seam`].
    sysvars: Arc<dyn ClusterSysvarWriter>,
    /// The route an `ANALYZE TABLE` takes; see
    /// [`crate::domain::cluster_analyze_seam`].
    analyze: std::sync::RwLock<Arc<dyn ClusterAnalyze>>,
    /// The route a persisted statistics-lock operation takes.
    stats_lock: Arc<dyn ClusterStatsLock>,
    /// The cluster catalog, republished whole by the reload thread and by a
    /// DDL's own inline reload. A connection takes one `Arc` per statement, so
    /// no session ever sees a half-updated catalog.
    catalog: Arc<SharedClusterCatalog>,
    /// Go's one `privilege.Manager` per `Domain`, here loaded from the
    /// cluster's own `mysql.*`.
    privileges: PrivilegeRegistry,
    /// Go's one `sessmgr.Manager` per TiDB instance: what `SHOW PROCESSLIST`
    /// reads and `KILL` reaches into.
    processes: ProcessRegistry,
    /// Domain lifetime: independent of each connection's rebuilt catalog.
    plan_cache_invalidation: Arc<tidb_executor::PlanCacheInvalidation>,
    /// Go `kv.Storage.GetMPPClient()`: the TiFlash MPP client this node's
    /// sessions serve columnar reads with, when the node has one.
    mpp_client: Option<Arc<dyn tidb_txnkv::MppClient>>,
    /// Go `kv.Storage.GetClient()`: the coprocessor client this node's
    /// sessions send built requests through, when it was given one. `None`
    /// keeps every scan on the raw key/value path.
    kv_client: Option<Arc<dyn tidb_txnkv::Client>>,
    /// This node's server-info syncer, which
    /// `information_schema.TIDB_SERVERS_INFO` reads. `None` leaves that
    /// table empty, which is the honest answer for a node that never
    /// established an identity.
    server_info: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
    server_identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
    /// Go `Domain.connIDAllocator`: the node's one connection-ID allocator,
    /// shared with the SQL listener and drawn from by internal processes
    /// (auto-analyze's `NextConnID`).
    connection_ids: Arc<dyn tidb_util::globalconn::Allocator + Send + Sync>,
    cluster_topology: Option<Arc<tidb_domain::cluster_topology::ClusterTopology>>,
    cluster_config: Option<Arc<tidb_exec::cluster_config::ClusterConfigClient>>,
    cluster_diagnostics: Option<Arc<tidb_exec::cluster_diagnostics::ClusterDiagnosticsClient>>,
    cluster_peer: Option<Arc<tidb_exec::cluster_peer::ClusterPeerClient>>,
    /// Go's one process-wide `GlobalVarsAccessor`.
    global_vars: GlobalSysvars,
    /// The tables of the boot catalog no session can include, kept so the
    /// node reports them once at startup instead of per connection.
    boot_skipped: Vec<SkippedTable>,
    /// This node's loaded tables' `mysql.stats_*`, republished whole by the
    /// stats reload thread [`run_cluster_session_node`] owns. Plumbing only:
    /// the estimator that will read this is a parallel unit.
    stats: Arc<SharedStats>,
    /// Go's per-`tidb-server` auto-increment allocators. Held HERE, above the
    /// per-session catalogs, because a reserved id range must outlive the
    /// `KvTable` that was handing it out; see [`crate::domain::cluster_auto_id_seam`].
    auto_ids: Arc<dyn TableAutoIds>,
    /// Process-owned spill authority inherited by every connection.
    spill_storage: Option<Arc<tidb_util::spill_storage::SpillStorage>>,
    /// Process-wide memory admission authority installed before listener bind.
    mem_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    /// The catalog versions live statements and transactions still read at
    /// -- the MDL gate the schema-sync acknowledger consults before telling
    /// a Go DDL owner this node has the new schema. See
    /// [`schema_sync::SchemaPinRegistry`].
    schema_pins: Arc<schema_sync::SchemaPinRegistry>,
    /// The node's schema validator (Go `Domain.SchemaValidator`), which
    /// every commit asks whether its schema version is still in force. `None`
    /// is a tier with no cluster schema to follow, which commits unchecked.
    schema_validator: Option<Arc<SchemaValidator>>,
    /// One live planner view over the domain's canonical statistics.
    statistics_view: Arc<tidb_executor::driver::StatisticsView>,
    /// Go Domain starts one statistics-load pool, shared by user and internal sessions.
    stats_load_workers: std::sync::OnceLock<Arc<tidb_executor::driver::StatisticsLoadWorkers>>,
    /// Fully built tables of one schema version, shared by every session
    /// opened against it -- Go's one `table.Table` per `TableInfo` inside the
    /// shared `infoschema`. A session clones its table and swaps in its own
    /// storage seam, so building ~700 restored tables happens once per DDL
    /// instead of once per CONNECTION (~30MB retained each before this).
    session_kv_cache: Arc<Mutex<KvTableTemplates>>,
    /// The store's regions every session of this node reads (Go
    /// `kv.SplittableStore` and the region cache).
    region_store: Option<Arc<dyn tidb_kv::region_map::RegionStore>>,
    /// Go Domain's single workload-repository worker.
    workload_repository: std::sync::OnceLock<Arc<tidb_workloadrepo::Worker>>,
    /// Go Domain/StatsHandle's one node-global usage implementation.
    stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    /// Go Domain's index-GC and positive-lease usage-dump workers.
    stats_usage_workers: std::sync::OnceLock<StatsUsageWorkers>,
    /// Go Domain's statistics owner, used by analyze-job history GC and
    /// dead-instance cleanup.
    stats_owner: Option<Arc<dyn tidb_owner::Manager>>,
    /// Go Domain's capacity-200 advanced system-session pool. It is installed
    /// after stable `Arc` ownership because its factory retains only a weak
    /// reference back to the domain-shaped session factory.
    advanced_sys_session_pool:
        std::sync::OnceLock<Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>>>,
    /// Pinned Go Domain's durable DDL notifier, registered before the stats
    /// owner campaigns and driven by that owner's lifecycle.
    ddl_notifier: std::sync::OnceLock<Arc<tidb_ddl_notifier::DdlNotifier>>,
    /// Go `analyzeJobsCleanupWorker`, installed after stable `Arc` ownership.
    analyze_jobs_cleanup_worker: std::sync::OnceLock<AnalyzeJobsCleanupWorker>,
    /// Go `Domain.requestUnitsWriterLoop`.
    ru_stats_writer: std::sync::OnceLock<ru_stats_writer::RuStatsWriterWorker>,
    /// The cluster's BDR role in TiKV meta, which every session's ADMIN
    /// BDR ROLE statements read and write.
    bdr_role_store: std::sync::OnceLock<Arc<dyn crate::BdrRoleStore>>,
    ddl_job_history_store: std::sync::OnceLock<Arc<dyn crate::DdlJobHistoryStore>>,
    /// Go `autoAnalyzeWorker`, installed after stable `Arc` ownership.
    auto_analyze_worker: std::sync::OnceLock<AutoAnalyzeWorker>,
    /// Go Domain's capacity-16 historical-statistics mailbox.
    historical_stats_worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
    /// Go `StartHistoricalStatsWorker`, installed after stable `Arc` ownership.
    historical_stats_runtime: std::sync::OnceLock<HistoricalStatsRuntime>,
    /// Go `Domain.LoadSigningCertLoop`'s reload thread.
    signing_cert_loop: std::sync::OnceLock<SigningCertLoop>,
    /// Go's process-global approximate table-count cache.
    approximate_table_counts: Arc<ClusterApproximateTableCountProvider>,
    /// Go StatsHandle's one auto-analyze priority queue. The refresher creates
    /// it lazily because the source needs a weak reference to this factory.
    auto_analyze_priority_queue: Arc<
        std::sync::OnceLock<
            Arc<tidb_stats_handle_autoanalyze_priorityqueue::AnalysisPriorityQueue>,
        >,
    >,
    /// Go StatsHandle's one refresher over the priority queue.
    auto_analyze_refresher:
        std::sync::OnceLock<Arc<Mutex<tidb_stats_handle_autoanalyze_refresher::Refresher>>>,
}

impl Domain {
    /// The domain's ANALYZE client.
    pub(crate) fn analyze(&self) -> Arc<dyn ClusterAnalyze> {
        Arc::clone(&self.analyze.read().unwrap_or_else(std::sync::PoisonError::into_inner))
    }

    /// Replaces the domain's ANALYZE client.
    pub(crate) fn set_analyze(&self, analyze: Arc<dyn ClusterAnalyze>) {
        *self.analyze.write().unwrap_or_else(std::sync::PoisonError::into_inner) = analyze;
    }
}

impl Domain {
    /// Install the one domain notification queue shared by every session.
    pub fn with_global_config_syncer(
        mut self,
        syncer: Arc<tidb_domain::globalconfigsync::GlobalConfigSyncer>,
    ) -> Self {
        self.global_config_syncer = Some(syncer);
        self
    }

    /// The factory behind the `Arc` its internal sessions (restricted SQL,
    /// the binding and statistics maintenance) are opened through: Go's
    /// domain owns its session pool for its whole life.
    #[must_use]
    pub fn shared(mut self) -> Arc<Self> {
        Arc::new_cyclic(|factory| {
            self.binding_session_factory = factory.clone();
            self
        })
    }

    /// Install the node-owned global-binding authority before opening sessions.
    pub fn with_bindings(
        mut self,
        bindings: Arc<dyn crate::domain::cluster_binding_seam::ClusterBindings>,
    ) -> Arc<Self> {
        self.bindings = Some(Arc::clone(&bindings));
        // Go `NewBindingHandle`: `variable.RegisterStatistics(h)`.
        self.register_statistics(Arc::new(stat::BindingStatistics(bindings.cache())));
        let factory = self.shared();
        bindings.attach_session_pool(Arc::new(binding_maintenance::BindingSessions::new(
            &factory,
        )));
        factory
    }

    /// Binds the factory to an authority that has already read the cluster's
    /// catalog and accounts.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transactions: Arc<dyn ClusterTransactions>,
        ddl: Arc<dyn ClusterDdl>,
        accounts: Arc<dyn ClusterAccountWriter>,
        sysvars: Arc<dyn ClusterSysvarWriter>,
        analyze: Arc<dyn ClusterAnalyze>,
        stats_lock: Arc<dyn ClusterStatsLock>,
        catalog: Arc<SharedClusterCatalog>,
        privileges: PrivilegeRegistry,
        global_vars: GlobalSysvars,
        stats: Arc<SharedStats>,
        auto_ids: Arc<dyn TableAutoIds>,
    ) -> Self {
        global_vars.set_external_timestamp_provider(Arc::new(ClusterExternalTimestamp(
            Arc::clone(&transactions),
        )));
        let boot_skipped = cluster_session_catalog(
            &catalog.load(),
            &detached_storage(),
            &stats.load(),
            auto_ids.as_ref(),
        )
        .skipped;
        let stats_usage = Arc::new(tidb_stats_handle_usage::StatsUsageHandle::new());
        stats_usage.start_worker();
        let data_lock_waits = Arc::new(ClusterDataLockWaits {
            transactions: Arc::clone(&transactions),
        });
        let historical_stats_worker =
            Arc::new(HistoricalStatsWorker::new(ClusterHistoricalInfoSchema {
                catalog: Arc::clone(&catalog),
            }));
        let approximate_table_counts = Arc::new(ClusterApproximateTableCountProvider::new(
            Arc::clone(&transactions),
        ));
        Self {
            transactions,
            bindings: None,
            statistics: Vec::new(),
            global_config_syncer: None,
            binding_session_factory: Weak::new(),
            data_lock_waits,
            ddl,
            accounts,
            sysvars,
            analyze: std::sync::RwLock::new(analyze),
            stats_lock,
            catalog,
            privileges,
            processes: ProcessRegistry::default(),
            plan_cache_invalidation: Arc::default(),
            auto_ids,
            mpp_client: None,
            kv_client: None,
            server_info: None,
            server_identity: None,
            connection_ids: crate::domain::session_api::new_connection_id_allocator(
                &tidb_config::config_tree::Config::default(),
                None,
            ),
            cluster_topology: None,
            cluster_config: None,
            cluster_diagnostics: None,
            cluster_peer: None,
            global_vars,
            boot_skipped,
            statistics_view: Arc::new(tidb_executor::driver::StatisticsView::new(Arc::new(
                ClusterStatisticsSource(Arc::clone(&stats)),
            ))),
            stats,
            spill_storage: None,
            mem_arbitrator: None,
            schema_pins: Arc::new(schema_sync::SchemaPinRegistry::default()),
            schema_validator: None,
            stats_load_workers: std::sync::OnceLock::new(),
            session_kv_cache: Arc::new(Mutex::new(KvTableTemplates::default())),
            region_store: None,
            workload_repository: std::sync::OnceLock::new(),
            stats_usage,
            stats_usage_workers: std::sync::OnceLock::new(),
            stats_owner: None,
            advanced_sys_session_pool: std::sync::OnceLock::new(),
            ddl_notifier: std::sync::OnceLock::new(),
            analyze_jobs_cleanup_worker: std::sync::OnceLock::new(),
            ru_stats_writer: std::sync::OnceLock::new(),
            bdr_role_store: std::sync::OnceLock::new(),
            ddl_job_history_store: std::sync::OnceLock::new(),
            auto_analyze_worker: std::sync::OnceLock::new(),
            historical_stats_worker,
            historical_stats_runtime: std::sync::OnceLock::new(),
            signing_cert_loop: std::sync::OnceLock::new(),
            approximate_table_counts,
            auto_analyze_priority_queue: Arc::new(std::sync::OnceLock::new()),
            auto_analyze_refresher: std::sync::OnceLock::new(),
        }
    }

    /// Returns Go StatsHandle's single auto-analyze priority queue.
    pub fn auto_analyze_priority_queue(
        self: &Arc<Self>,
        stats_lease: Duration,
    ) -> Arc<tidb_stats_handle_autoanalyze_priorityqueue::AnalysisPriorityQueue> {
        Arc::clone(self.auto_analyze_priority_queue.get_or_init(|| {
            let source: Arc<dyn tidb_stats_handle_autoanalyze_priorityqueue::PriorityQueueSource> =
                Arc::new(ClusterPriorityQueueSource {
                    factory: Arc::downgrade(self),
                    stats_lease,
                    session_pool: self.advanced_sys_session_pool(),
                });
            tidb_stats_handle_autoanalyze_priorityqueue::AnalysisPriorityQueue::new(source)
        }))
    }

    /// What every connection's session gets once it is opened, whoever opens
    /// it (Go `createSessionWithOpt`): its connection id, its process-list
    /// entry, the privilege cache, the domain's services and the
    /// global-binding writer.
    fn attach_connection(
        &self,
        session: &mut Session,
        connection_id: u64,
        process: crate::process::ProcessGuard,
    ) {
        session.set_connection_id(connection_id);
        process.transaction_observer().use_external_owner();
        session.attach_process(connection_id, process);
        session.attach_privileges(self.privileges.clone());
        session.install_domain_services(crate::domain_services::DomainServices {
            ddl: Arc::new(ClusterDdlService {
                ddl: Arc::clone(&self.ddl),
                catalog: Arc::clone(&self.catalog),
            }),
            accounts: Arc::new(ClusterAccountService {
                accounts: Arc::clone(&self.accounts),
                factory: self.binding_session_factory.clone(),
            }),
            sysvars: Arc::new(ClusterSysvarService(Arc::clone(&self.sysvars))),
            analyze: self.analyze_service(),
            stats: self.stats_statement_service(),
        });
        if let Some(bindings) = &self.bindings {
            session.set_global_binding_cache(bindings.cache());
            session.set_global_binding_writer(Arc::new(InternalBindingWriter {
                factory: self.binding_session_factory.clone(),
                bindings: Arc::clone(bindings),
                connection_id,
            }));
        }
    }

    /// Go `Domain.SysSessionPool().Get()` for one internal statement: a
    /// session over this node with the internal identity, whose SQL an
    /// account executor runs in its own transaction.
    pub(crate) fn open_system_session(&self) -> Result<Session, SqlQueryError> {
        use std::sync::atomic::{AtomicU64, Ordering};
        // Apart from client connections and the advanced pool's range.
        static NEXT_SYSTEM_CONNECTION_ID: AtomicU64 = AtomicU64::new(1_u64 << 62);
        let connection_id = NEXT_SYSTEM_CONNECTION_ID.fetch_add(1, Ordering::Relaxed);
        let mut session = self.open_session(SessionContext {
            connection_id,
            peer_addr: crate::domain::client_socket::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
            identity: crate::domain::configured_user_store::AuthenticatedIdentity::internal(),
            client_found_rows: false,
            tls_state: None,
            compression_algorithm: tidb_mysql::consts::CompressionNone,
            compression_level: 0,
            cancellation: crate::domain::session_api::ConnectionCancellation::default(),
            close: crate::domain::session_api::ConnectionClose::default(),
        })?;
        session.set_restricted_sql(true);
        Ok(session)
    }

    /// Returns Go Domain's single advanced internal-session pool.
    fn advanced_sys_session_pool(
        self: &Arc<Self>,
    ) -> Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>> {
        Arc::clone(self.advanced_sys_session_pool.get_or_init(|| {
            let session_factory = Arc::downgrade(self);
            let next_connection_id = Arc::new(std::sync::atomic::AtomicU64::new(1_u64 << 61));
            Arc::new(tidb_syssession::AdvancedSessionPool::new(200, move || {
                use std::sync::atomic::Ordering;

                let factory = session_factory.upgrade().ok_or_else(|| {
                    tidb_syssession::SysSessionError::new("cluster session factory is stopped")
                })?;
                let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                let global_vars = factory.global_vars.clone();
                let session = factory
                    .open_session(SessionContext {
                        connection_id,
                        peer_addr: crate::domain::client_socket::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
                        identity: crate::domain::configured_user_store::AuthenticatedIdentity::internal(),
                        client_found_rows: false,
                        tls_state: None,
                        compression_algorithm: tidb_mysql::consts::CompressionNone,
                        compression_level: 0,
                        cancellation: crate::domain::session_api::ConnectionCancellation::default(),
                        close: crate::domain::session_api::ConnectionClose::default(),
                    })
                    .map_err(|error| tidb_syssession::SysSessionError::new(error.message))?;
                Ok(Arc::new(ClusterStatsSessionContext::new(
                    session,
                    global_vars,
                )))
            }))
        }))
    }

    /// Returns Go StatsHandle's single auto-analyze refresher.
    pub fn auto_analyze_refresher(
        self: &Arc<Self>,
        stats_lease: Duration,
    ) -> Arc<Mutex<tidb_stats_handle_autoanalyze_refresher::Refresher>> {
        Arc::clone(self.auto_analyze_refresher.get_or_init(|| {
            let concurrency = self
                .global_vars
                .get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_AUTO_ANALYZE_CONCURRENCY as usize);
            Arc::new(Mutex::new(
                tidb_stats_handle_autoanalyze_refresher::Refresher::new(
                    self.auto_analyze_priority_queue(stats_lease),
                    concurrency,
                ),
            ))
        }))
    }

    fn auto_analyze_refresh_parameters(
        &self,
    ) -> Result<tidb_stats_handle_autoanalyze_refresher::RefreshParameters, String> {
        let get = |name| {
            self.global_vars
                .get(name)
                .map_err(|error| format!("{error:?}"))
        };
        let prune_mode = get(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)?;
        Ok(tidb_stats_handle_autoanalyze_refresher::RefreshParameters {
            auto_analyze_ratio: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO)?,
            prune_mode: if prune_mode.eq_ignore_ascii_case("static") {
                tidb_stats_handle_autoanalyze_priorityqueue::PartitionPruneMode::Static
            } else {
                tidb_stats_handle_autoanalyze_priorityqueue::PartitionPruneMode::Dynamic
            },
            start_time: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_START_TIME)?,
            end_time: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_END_TIME)?,
            max_concurrency: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY)?
                .parse()
                .map_err(|error| format!("invalid auto analyze concurrency: {error}"))?,
        })
    }

    /// Starts Go `Domain.StartHistoricalStatsWorker`.
    pub fn start_historical_stats_worker(self: &Arc<Self>) {
        if !tidb_domain::historical_stats::enable_dump_historical_stats()
            || self.historical_stats_runtime.get().is_some()
        {
            return;
        }
        let handle = Arc::new(ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
        });
        let _ = self
            .historical_stats_runtime
            .set(HistoricalStatsRuntime::start(
                Arc::clone(&self.historical_stats_worker),
                handle,
            ));
    }

    /// Go `Domain.LoadSigningCertLoop`: binds the session-token signing
    /// certificate and reloads it periodically so a rotated one is picked up.
    pub fn load_signing_cert_loop(&self, signing_cert: &str, signing_key: &str) {
        tidb_sessionstates::set_cert_path(signing_cert);
        tidb_sessionstates::set_key_path(signing_key);
        let _ = self.signing_cert_loop.get_or_init(SigningCertLoop::start);
    }

    /// Pinned Go `StatsHandle.DumpHistoricalStatsBySnapshot` over the live
    /// shared catalog and restricted cluster transactions.
    pub fn dump_historical_stats_by_snapshot(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats::JsonTable>, Vec<String>), String> {
        ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
        }
        .dump_historical_stats_by_snapshot(database_name, table, snapshot_ts)
    }

    /// Pinned Go `StatsGC.ClearOutdatedHistoryStats` over restricted
    /// autocommit transactions.
    pub fn clear_outdated_history_stats(&self) -> Result<(), String> {
        ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
        }
        .clear_outdated_history_stats()
    }

    /// Pinned Go `StatsHandle.DeleteAnalyzeJobs` over one restricted
    /// autocommit transaction.
    pub fn delete_analyze_jobs_before(&self, cutoff: tidb_datatype::Time) -> Result<(), String> {
        ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
        }
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_delete_analyze_jobs(
                snapshot,
                &self.catalog.load(),
                &cutoff,
            )
            .map_err(|error| error.to_string())
        })
    }

    /// Pinned Go `CleanupCorruptedAnalyzeJobsOnCurrentInstance` over one
    /// restricted transaction.
    pub fn cleanup_corrupted_analyze_jobs_on_current_instance(
        &self,
        cutoff: tidb_datatype::Time,
    ) -> Result<(), String> {
        let syncer = self
            .server_info
            .as_ref()
            .ok_or_else(|| "server info is not initialized".to_owned())?;
        let local = syncer.local_server_info();
        let instance = tidb_domain::serverinfo_syncer::join_host_port(
            &local.static_info.ip,
            local.static_info.port,
        );
        let analyze_process_ids = self
            .processes
            .snapshot()
            .into_iter()
            .filter_map(|process| {
                process
                    .info
                    .as_deref()
                    .is_some_and(is_analyze_table_sql)
                    .then_some(process.id)
            })
            .collect::<HashSet<_>>();
        ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
        }
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_cleanup_corrupted_analyze_jobs_on_current_instance(
                snapshot,
                &self.catalog.load(),
                &instance,
                &analyze_process_ids,
                &cutoff,
                Some(self.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
    }

    /// Pinned Go `CleanupCorruptedAnalyzeJobsOnDeadInstances` over one
    /// restricted transaction.
    pub fn cleanup_corrupted_analyze_jobs_on_dead_instances(
        &self,
        cutoff: tidb_datatype::Time,
    ) -> Result<(), String> {
        let syncer = self
            .server_info
            .as_ref()
            .ok_or_else(|| "server info is not initialized".to_owned())?;
        let alive_instances = syncer
            .all_server_info()?
            .into_values()
            .map(|info| {
                tidb_domain::serverinfo_syncer::join_host_port(
                    &info.static_info.ip,
                    info.static_info.port,
                )
            })
            .collect::<HashSet<_>>();
        ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
        }
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_cleanup_corrupted_analyze_jobs_on_dead_instances(
                snapshot,
                &self.catalog.load(),
                &alive_instances,
                &cutoff,
                Some(self.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
    }

    /// Pinned Go `StatsGC.GCStats` over the current shared schema and
    /// independent restricted cluster transactions.
    pub fn gc_stats(&self, stats_lease: Duration, ddl_lease: Duration) -> Result<(), String> {
        let result = ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
        }
        .gc_stats(stats_lease, ddl_lease);
        self.check_auto_analyze_windows_at(Utc::now());
        result
    }

    fn check_auto_analyze_windows_at(&self, now: chrono::DateTime<Utc>) {
        let Ok(parameters) = self.auto_analyze_refresh_parameters() else {
            return;
        };
        let Ok((start, end)) = tidb_stats_handle_autoanalyze_exec::parse_auto_analysis_window(
            &parameters.start_time,
            &parameters.end_time,
        ) else {
            return;
        };
        let window =
            tidb_stats_handle_autoanalyze_priorityqueue::AutoAnalysisTimeWindow::new(start, end);
        if window.is_within_time_window(now) {
            return;
        }
        for process_id in tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.all() {
            // Go `Domain.CheckAutoAnalyzeWindows`.
            tidb_stats_handle_logutil::stats_logger().warn(
                "Kill auto analyze process because it exceeded the window",
                &[
                    tidb_log::Field::new("processID", tidb_log::Value::U64(process_id)),
                    tidb_log::Field::new(
                        "now",
                        tidb_log::Value::Str(tidb_log::format_time(
                            &now.with_timezone(&chrono::Local).fixed_offset(),
                        )),
                    ),
                    tidb_log::Field::new(
                        "start",
                        tidb_log::Value::Str(parameters.start_time.clone()),
                    ),
                    tidb_log::Field::new("end", tidb_log::Value::Str(parameters.end_time.clone())),
                ],
            );
            self.processes.kill(process_id, true);
        }
    }

    /// Starts Go Domain's usage workers after the factory has stable `Arc`
    /// ownership. Index GC runs for every lease; delta and column dumps run
    /// only for a positive lease.
    pub fn start_stats_usage_workers(
        self: &Arc<Self>,
        lease: crate::domain::node_config::StatsLease,
    ) {
        if self.stats_usage_workers.get().is_some() {
            return;
        }
        let _ = self
            .stats_usage_workers
            .set(StatsUsageWorkers::start(self, lease));
    }

    /// Starts pinned Go `analyzeJobsCleanupWorker`. Go starts it only when
    /// `statsLease > 0`; zero and negative leases return before this worker.
    pub fn start_analyze_jobs_cleanup_worker(
        self: &Arc<Self>,
        lease: crate::domain::node_config::StatsLease,
    ) {
        let crate::domain::node_config::StatsLease::Positive(lease) = lease else {
            return;
        };
        if self.analyze_jobs_cleanup_worker.get().is_some() {
            return;
        }
        let _ = self
            .analyze_jobs_cleanup_worker
            .set(AnalyzeJobsCleanupWorker::start(self, lease));
    }

    /// Starts Go `Domain.requestUnitsWriterLoop` (`pkg/domain/domain.go:830`)
    /// over this node's PD ResourceManager, meta store and internal-session
    /// pool. Go starts it unconditionally beside the other domain loops,
    /// over the store's PD client.
    pub fn start_ru_stats_writer<S>(
        self: &Arc<Self>,
        pd: tidb_txnkv::driver::tikv_store::TikvStore,
        opener: Arc<tidb_txnkv::transaction::RealOptimisticTransactionOpener<S>>,
        timeout: Duration,
    ) where
        S: tidb_txnkv::transaction::StoreClient + 'static,
    {
        if self.ru_stats_writer.get().is_some() {
            return;
        }
        let deps = ru_stats_writer::ClusterRuStatsDeps::new(
            pd,
            opener,
            self.advanced_sys_session_pool(),
            timeout,
        );
        let _ = self
            .ru_stats_writer
            .set(ru_stats_writer::RuStatsWriterWorker::start(self, deps));
    }

    /// Installs the TiKV meta store that holds the cluster's BDR role, so
    /// sessions opened from now on answer ADMIN SET / UNSET / SHOW BDR ROLE
    /// from it.
    pub fn install_bdr_role_store<S>(
        &self,
        opener: Arc<tidb_txnkv::transaction::RealOptimisticTransactionOpener<S>>,
        timeout: Duration,
    ) where
        S: tidb_txnkv::transaction::StoreClient + 'static,
    {
        let _ = self
            .bdr_role_store
            .set(Arc::new(bdr_role_store::ClusterBdrRoleStore::new(
                Arc::clone(&opener),
                timeout,
            )));
        // Go reads the DDL jobs through the same meta store.
        let _ = self.ddl_job_history_store.set(Arc::new(
            ddl_job_history_store::ClusterDdlJobHistoryStore::new(opener, timeout),
        ));
    }

    /// Starts pinned Go `autoAnalyzeWorker`. Like Go, only a positive stats
    /// lease starts it; every tick checks the process switch and ownership.
    pub fn start_auto_analyze_worker(
        self: &Arc<Self>,
        lease: crate::domain::node_config::StatsLease,
    ) {
        let crate::domain::node_config::StatsLease::Positive(lease) = lease else {
            return;
        };
        if self.auto_analyze_worker.get().is_some() {
            return;
        }
        let _ = self
            .auto_analyze_worker
            .set(AutoAnalyzeWorker::start(self, lease));
    }

    fn handle_auto_analyze_tick(self: &Arc<Self>, run_auto_analyze: bool, stats_lease: Duration) {
        let is_owner = self
            .stats_owner
            .as_ref()
            .is_some_and(|owner| owner.is_owner());
        if !run_auto_analyze || !is_owner {
            if let Some(refresher) = self.auto_analyze_refresher.get() {
                refresher
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .close_priority_queue();
            }
            return;
        }
        let Ok(parameters) = self.auto_analyze_refresh_parameters() else {
            return;
        };
        let refresher = self.auto_analyze_refresher(stats_lease);
        let _ = refresher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .analyze_highest_priority_tables(&parameters);
    }

    /// Go `statsUsageImpl.DumpColStatsUsageToKV`.
    pub fn dump_col_stats_usage_to_kv(&self, resource_group: &str) -> Result<(), String> {
        let row_ids = Arc::clone(&self.auto_ids);
        let mut pending = self
            .stats_usage
            .session_stats_list()
            .begin_column_stats_usage_dump();
        let entries = pending.entries();
        persist_column_stats_usage_batches(&mut pending, &entries, |batch| {
            let snapshot = self.transactions.open_snapshot(resource_group)?;
            let read_ts = snapshot.start_ts();
            let converted = batch
                .iter()
                .map(|(item, used_at)| {
                    system_time_timestamp(*used_at).map(|used_at| (*item, used_at))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let plan = {
                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                tidb_exec::cluster_stats_write::plan_column_stats_usage_dump(
                    &mut snapshot,
                    &self.catalog.load(),
                    &converted,
                    tidb_exec::mysql_bootstrap::utc_now_timestamp(),
                    Some(row_ids.as_ref()),
                )
                .map_err(|error| error.to_string())?
            };
            self.transactions
                .commit_optimistic_mutations(
                    plan.mutations,
                    read_ts,
                    resource_group,
                    crate::domain::session_transaction::restricted_transaction_options(&self.global_vars),
                )
                .map_err(|error| error.message)?;
            Ok(())
        })
    }

    /// Go `statsUsageImpl.DumpStatsDeltaToKV`.
    pub fn dump_stats_delta_to_kv(
        &self,
        force_dump: bool,
        target_table_ids: &[i64],
        resource_group: &str,
    ) -> Result<(), String> {
        Self::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.transactions.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            force_dump,
            target_table_ids,
            resource_group,
            &self.auto_ids,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn dump_stats_delta_to_kv_parts(
        stats_usage: &tidb_stats_handle_usage::StatsUsageHandle,
        transactions: &dyn ClusterTransactions,
        catalog_store: &SharedClusterCatalog,
        stats_store: &SharedStats,
        global_vars: &GlobalSysvars,
        force_dump: bool,
        target_table_ids: &[i64],
        resource_group: &str,
        row_ids: &Arc<dyn TableAutoIds>,
    ) -> Result<(), String> {
        let mut pending = stats_usage.session_stats_list().begin_table_delta_dump();
        let table_ids = pending.pending_table_ids(target_table_ids);
        for batch in table_ids.chunks(tidb_stats_handle_usage::DUMP_DELTA_BATCH_SIZE) {
            let batch_time = SystemTime::now();
            let catalog = catalog_store.load();
            let stats = stats_store.load();
            let mut original_updates = Vec::new();
            let mut parents = std::collections::HashMap::new();
            for &table_id in batch {
                pending.initialize_time(table_id, batch_time);
                let Some((database, parent_id)) = stats_delta_table(&catalog, table_id) else {
                    continue;
                };
                if matches!(
                    database,
                    "information_schema"
                        | "performance_schema"
                        | "metrics_schema"
                        | "mysql"
                        | "sys"
                        | "workload_schema"
                ) {
                    continue;
                }
                let Some(delta) = pending.get(table_id) else {
                    continue;
                };
                let realtime_count = stats
                    .get(&table_id)
                    .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                    .map(|table| table.hist_coll.realtime_count);
                if !tidb_stats_handle_usage::need_dump_stats_delta(
                    force_dump,
                    delta,
                    batch_time,
                    realtime_count,
                ) {
                    continue;
                }
                if let Some(parent_id) = parent_id {
                    parents.insert(table_id, parent_id);
                }
                original_updates.push(tidb_stats_handle_usage::DeltaUpdate {
                    table_id,
                    delta,
                    is_locked: false,
                });
            }
            if original_updates.is_empty() {
                continue;
            }
            // An internal statement: Go's `InRestrictedSQL` sessions never
            // start fair locking (`isolation/base.go:711-714`).
            let transaction = transactions.begin(
                true,
                false,
                resource_group,
                crate::domain::session_transaction::restricted_transaction_options(global_vars),
            )?;
            let read_ts = transaction.start_ts();
            let staged = MutationBuffer::new();
            transaction.bind_mutation_buffer(&staged);
            let prepared = (|| {
                let locked = {
                    let snapshot = transaction.snapshot_for(false)?;
                    let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                    tidb_exec::cluster_stats_write::load_stats_locked_table_ids(
                        &mut snapshot,
                        &catalog,
                    )
                    .map_err(|error| error.to_string())?
                };
                let updates = tidb_stats_handle_usage::prepare_delta_updates(
                    original_updates.clone(),
                    |table_id| parents.get(&table_id).copied(),
                    &locked,
                );
                for statement in tidb_exec::cluster_stats_write::stats_delta_statements(&updates) {
                    let ((), mutations) =
                        tidb_cluster::cluster_table_storage::lock_pessimistic_statement_with(
                            transaction.start_ts(),
                            &staged,
                            |retry_ts| {
                                let snapshot = match retry_ts {
                                    Some(retry_ts) => transaction.snapshot_at_for(retry_ts, true),
                                    None => transaction.snapshot_for(true),
                                }?;
                                Ok(tidb_cluster::cluster_table_storage::overlay_staged_mutations(
                                    snapshot, &staged,
                                ))
                            },
                            |keys, presume_not_exists, duplicate_hints| {
                                transaction.lock_staged_keys_with_assertions(
                                    keys,
                                    presume_not_exists,
                                    duplicate_hints,
                                    tidb_txnkv::transaction::LockWaitTime::Timeout(
                                        global_lock_wait_timeout(global_vars),
                                    ),
                                )
                            },
                            |snapshot, _start_ts| {
                                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                                let plan =
                                    tidb_exec::cluster_stats_write::plan_stats_delta_statement(
                                        &mut snapshot,
                                        &catalog,
                                        &statement,
                                        read_ts,
                                        tidb_exec::mysql_bootstrap::utc_now_timestamp(),
                                        Some(row_ids.as_ref()),
                                    )
                                    .map_err(|error| error.to_string())?;
                                Ok(((), plan.mutations))
                            },
                        )
                        .map_err(|error| error.to_string())?;
                    tidb_cluster::cluster_table_storage::stage_mutations(&staged, mutations)
                        .map_err(|error| error.to_string())?;
                }
                Ok::<_, String>(updates)
            })();
            let updates = match prepared {
                Ok(updates) => updates,
                Err(error) => {
                    let _ = transaction.rollback();
                    return Err(error);
                }
            };
            if let Err(error) = transaction.commit(&staged) {
                return Err(error.message);
            }
            for update in original_updates {
                pending.mark_persisted(update.table_id);
            }
            let historical_stats_enabled = global_vars
                .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value));
            if historical_stats_enabled {
                for update in updates {
                    if update.is_locked
                        || !stats
                            .get(&update.table_id)
                            .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                            .is_some_and(|table| table.is_initialized())
                    {
                        continue;
                    }
                    let result = (|| {
                        // Internal statement: no fair locking (Go `InRestrictedSQL`).
                        let transaction = transactions.begin(
                            true,
                            false,
                            resource_group,
                            crate::domain::session_transaction::restricted_transaction_options(global_vars),
                        )?;
                        let staged = MutationBuffer::new();
                        transaction.bind_mutation_buffer(&staged);
                        let ((modify_count, count), lock_mutations) =
                            tidb_cluster::cluster_table_storage::lock_pessimistic_statement_with(
                                transaction.start_ts(),
                                &staged,
                                |read_ts| match read_ts {
                                    Some(read_ts) => transaction.snapshot_at_for(read_ts, true),
                                    None => transaction.snapshot_for(true),
                                },
                                |keys, presume_not_exists, duplicate_hints| {
                                    transaction.lock_staged_keys_with_assertions(
                                        keys,
                                        presume_not_exists,
                                        duplicate_hints,
                                        tidb_txnkv::transaction::LockWaitTime::Timeout(global_lock_wait_timeout(global_vars)),
                                    )
                                },
                                |snapshot, _start_ts| {
                                    let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                                    let (counts, plan) = tidb_exec::cluster_stats_write::plan_historical_stats_meta_lock(
                                        &mut snapshot,
                                        &catalog,
                                        update.table_id,
                                        read_ts,
                                    )
                                    .map_err(|error| error.to_string())?;
                                    Ok((counts, plan.mutations))
                                },
                            )
                            .map_err(|error| error.to_string())?;
                        tidb_cluster::cluster_table_storage::stage_mutations(&staged, lock_mutations)
                            .map_err(|error| error.to_string())?;
                        let ((), replace_mutations) =
                            tidb_cluster::cluster_table_storage::lock_pessimistic_statement_with(
                                transaction.start_ts(),
                                &staged,
                                |retry_ts| match retry_ts {
                                    Some(retry_ts) => transaction.snapshot_at_for(retry_ts, true),
                                    None => transaction.snapshot_for(true),
                                },
                                |keys, presume_not_exists, duplicate_hints| {
                                    transaction.lock_staged_keys_with_assertions(
                                        keys,
                                        presume_not_exists,
                                        duplicate_hints,
                                        tidb_txnkv::transaction::LockWaitTime::Timeout(global_lock_wait_timeout(global_vars)),
                                    )
                                },
                                |snapshot, _start_ts| {
                                    let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                                    let plan = tidb_exec::cluster_stats_write::plan_historical_stats_meta_replace(
                                &mut snapshot,
                                &catalog,
                                update.table_id,
                                modify_count,
                                count,
                                read_ts,
                                "flush stats",
                                tidb_exec::mysql_bootstrap::local_now_datetime6(),
                                Some(row_ids.as_ref()),
                            )
                                    .map_err(|error| error.to_string())?;
                                    Ok(((), plan.mutations))
                                },
                            )
                            .map_err(|error| error.to_string())?;
                        tidb_cluster::cluster_table_storage::stage_mutations(
                            &staged,
                            replace_mutations,
                        )
                        .map_err(|error| error.to_string())?;
                        transaction.commit(&staged).map_err(|error| error.message)
                    })();
                    if let Err(error) = result {
                        // Go `statsHistoryImpl.RecordHistoricalStatsMeta`.
                        tidb_stats_handle_logutil::stats_logger().warn(
                            "record historical stats meta failed",
                            &[
                                tidb_log::Field::new("version", tidb_log::Value::U64(read_ts)),
                                tidb_log::Field::new(
                                    "source",
                                    tidb_log::Value::Str("flush stats".to_owned()),
                                ),
                                tidb_log::Field::new(
                                    "tableID",
                                    tidb_log::Value::I64(update.table_id),
                                ),
                                crate::domain::zap_error(error),
                            ],
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn gc_index_usage(&self) -> Result<(), String> {
        let catalog = self.catalog.load();
        self.stats_usage.gc_index_usage(|table_id| {
            catalog
                .databases
                .iter()
                .flat_map(|database| &database.tables)
                .find(|table| table.id == table_id)
                .cloned()
                .map(Arc::new)
        });
        Ok(())
    }

    /// Installs Go Domain's single workload-repository worker after the
    /// factory is placed in an `Arc` (the worker's internal-session pool keeps
    /// only a weak reference back to it).
    pub fn set_workload_repository(
        &self,
        worker: Arc<tidb_workloadrepo::Worker>,
    ) -> Result<(), Arc<tidb_workloadrepo::Worker>> {
        self.workload_repository.set(worker)
    }

    /// Replaces the factory's pin registry with the node-owned one the
    /// schema-sync acknowledger reads.
    #[must_use]
    pub fn with_schema_pins(mut self, pins: Arc<schema_sync::SchemaPinRegistry>) -> Self {
        self.schema_pins = pins;
        self
    }

    /// Binds the node's schema validator, so every session's commit carries
    /// Go's schema lease check.
    pub fn with_schema_validator(mut self, validator: Arc<SchemaValidator>) -> Self {
        self.schema_validator = Some(validator);
        self
    }

    pub fn with_spill_storage(
        mut self,
        spill_storage: Arc<tidb_util::spill_storage::SpillStorage>,
    ) -> Self {
        self.spill_storage = Some(spill_storage);
        self
    }

    /// Installs the process memory authority inherited by every session.
    #[must_use]
    pub fn with_mem_arbitrator(
        mut self,
        arbitrator: Arc<tidb_util::memory::MemArbitrator>,
    ) -> Self {
        self.mem_arbitrator = Some(arbitrator);
        self
    }

    /// This node's loaded tables' statistics. The consuming estimator is a
    /// parallel unit; this is the supply line it will read from.
    #[must_use]
    pub fn stats(&self) -> &Arc<SharedStats> {
        &self.stats
    }

    /// Gives this node's sessions the store's TiFlash MPP client (Go
    /// `kv.Storage.GetMPPClient()`), which serves their TiFlash reads.
    #[must_use]
    pub fn with_mpp_client(mut self, mpp: Arc<dyn tidb_txnkv::MppClient>) -> Self {
        self.mpp_client = Some(mpp);
        self
    }

    /// Gives this node's sessions the store's coprocessor client (Go
    /// `kv.Storage.GetClient()`), which serves their base-table scans, so a
    /// `WHERE` is evaluated at the region instead of after the range's bytes
    /// have crossed the network.
    ///
    /// The staged-write half is untouched: a session's uncommitted rows are
    /// merged client-side and re-tested by the same predicate (see
    /// [`tidb_executor::remote_scan`]).
    #[must_use]
    pub fn with_kv_client(mut self, client: Arc<dyn tidb_txnkv::Client>) -> Self {
        self.kv_client = Some(client);
        self
    }

    /// Hands every session the store's regions (Go `kv.SplittableStore`
    /// and the region cache).
    #[must_use]
    pub fn with_region_store(
        mut self,
        regions: Arc<dyn tidb_kv::region_map::RegionStore>,
    ) -> Self {
        self.region_store = Some(regions);
        self
    }

    /// Shares the leased numeric identity with connection allocation and admission.
    #[must_use]
    pub fn with_server_identity(
        mut self,
        identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
    ) -> Self {
        self.server_identity = identity;
        self
    }

    /// Installs the node's connection-ID allocator, built from the node's
    /// configuration and server identity (see
    /// [`crate::domain::session_api::new_connection_id_allocator`]).
    #[must_use]
    pub fn with_connection_ids(
        mut self,
        connection_ids: Arc<dyn tidb_util::globalconn::Allocator + Send + Sync>,
    ) -> Self {
        self.connection_ids = connection_ids;
        self
    }

    /// Go `Domain.NextConnID` / `ReleaseConnID`, over the node's one
    /// connection-ID allocator.
    pub fn connection_id_allocator(
        &self,
    ) -> Arc<dyn tidb_util::globalconn::Allocator + Send + Sync> {
        Arc::clone(&self.connection_ids)
    }

    /// Binds server information for publication and cluster discovery.
    #[must_use]
    pub fn with_server_info(mut self, syncer: Arc<tidb_domain::serverinfo_syncer::Syncer>) -> Self {
        self.cluster_topology = Some(Arc::new(
            tidb_domain::cluster_topology::ClusterTopology::new(Arc::clone(&syncer), None),
        ));
        // Go `ddl.Start`: `variable.RegisterStatistics(d)`, whose id is this
        // server's.
        self.register_statistics(Arc::new(stat::DdlStatistics {
            id: syncer.local_server_info().static_info.id.clone(),
            catalog: Arc::clone(&self.catalog),
        }));
        self.server_info = Some(syncer);
        self
    }

    /// Go `variable.RegisterStatistics`, kept for the domain's close.
    fn register_statistics(&mut self, statistics: Arc<dyn crate::statusvar::Statistics>) {
        crate::statusvar::register_statistics(Arc::clone(&statistics));
        self.statistics.push(statistics);
    }

    /// Binds outgoing cluster administration to the process RPC owner.
    pub fn with_cluster_peer_client(
        mut self,
        client: Arc<tidb_exec::cluster_peer::ClusterPeerClient>,
    ) -> Self {
        self.cluster_peer = Some(client);
        self
    }

    /// Shares the internal HTTP configuration retriever with every SQL session.
    #[must_use]
    pub fn with_cluster_config_client(
        mut self,
        client: Arc<tidb_exec::cluster_config::ClusterConfigClient>,
    ) -> Self {
        self.cluster_config = Some(client);
        self
    }

    /// Shares the diagnostics gRPC client the server-info and log cluster
    /// tables read through with every SQL session.
    #[must_use]
    pub fn with_cluster_diagnostics_client(
        mut self,
        client: Arc<tidb_exec::cluster_diagnostics::ClusterDiagnosticsClient>,
    ) -> Self {
        self.cluster_diagnostics = Some(client);
        self
    }

    /// Shares topology with every session.
    #[must_use]
    pub fn with_cluster_topology(
        mut self,
        topology: Arc<tidb_domain::cluster_topology::ClusterTopology>,
    ) -> Self {
        self.cluster_topology = Some(topology);
        self
    }

    /// Binds Go Domain's `/tidb/stats/owner` manager.
    #[must_use]
    pub fn with_stats_owner(mut self, owner: Arc<dyn tidb_owner::Manager>) -> Self {
        self.stats_owner = Some(owner);
        self
    }

    /// Starts Go Domain's statistics-owner campaign after the factory has
    /// reached its stable process lifetime.
    pub fn campaign_stats_owner(
        self: &Arc<Self>,
        stats_lease: crate::domain::node_config::StatsLease,
    ) -> Result<(), String> {
        let owner = self
            .stats_owner
            .as_ref()
            .ok_or_else(|| "statistics owner is not initialized".to_owned())?;
        let notifier = Arc::clone(self.ddl_notifier.get_or_init(|| {
            ddl_notifier::build_notifier(
                self,
                stats_lease.reload_interval().unwrap_or(Duration::ZERO),
            )
        }));
        owner.set_listener(Arc::new(tidb_owner::ListenersWrapper::new(vec![
            notifier as Arc<dyn tidb_owner::Listener>,
        ])));
        owner.campaign_owner(&[])
    }

    /// The boot catalog's tables this node cannot serve, with their reasons.
    #[must_use]
    pub fn boot_skipped_tables(&self) -> &[SkippedTable] {
        &self.boot_skipped
    }

    /// The schema version this node has followed the cluster to.
    #[must_use]
    pub fn followed_schema_version(&self) -> i64 {
        self.catalog.load().schema_version
    }

    /// The catalog this node currently serves, for the status server's
    /// `/schema` routes. Go answers those from `GetLatest()` per request, so
    /// this reads the live pointer rather than a captured snapshot.
    #[must_use]
    pub fn catalog_snapshot(&self) -> tidb_cluster::cluster_catalog::ClusterCatalog {
        (*self.catalog.load()).clone()
    }

    /// The process list of every connection this factory has open.
    #[must_use]
    pub fn processes(&self) -> ProcessRegistry {
        self.processes.clone()
    }
}

struct StatsUsageWorkers {
    stop: Arc<UsageWorkerStop>,
    threads: Vec<std::thread::JoinHandle<()>>,
    flush_on_drop: bool,
}

struct AnalyzeJobsCleanupWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct AutoAnalyzeWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AutoAnalyzeWorker {
    fn start(factory: &Arc<Domain>, stats_lease: Duration) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        let thread = crate::domain::session_api::sql_worker_thread("auto-analyze")
            .spawn(move || loop {
                if running.wait(stats_lease) {
                    return;
                }
                let Some(factory) = weak.upgrade() else {
                    return;
                };
                factory.handle_auto_analyze_tick(
                    tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst),
                    stats_lease,
                );
            })
            .expect("auto-analyze worker spawns");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for AutoAnalyzeWorker {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl AnalyzeJobsCleanupWorker {
    fn start(factory: &Arc<Domain>, stats_lease: Duration) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        let thread = crate::domain::session_api::sql_worker_thread("analyze-jobs-cleanup")
            .spawn(move || {
                let gc_interval = Duration::from_secs(60 * 60);
                let cleanup_interval = stats_lease.saturating_mul(100);
                let mut next_gc = std::time::Instant::now() + gc_interval;
                let mut next_cleanup = std::time::Instant::now() + cleanup_interval;
                loop {
                    let now = std::time::Instant::now();
                    let next = next_gc.min(next_cleanup);
                    if running.wait(next.saturating_duration_since(now)) {
                        return;
                    }
                    let Some(factory) = weak.upgrade() else {
                        return;
                    };
                    let now = std::time::Instant::now();
                    if now >= next_gc {
                        next_gc = now + gc_interval;
                        if factory
                            .stats_owner
                            .as_ref()
                            .is_some_and(|owner| owner.is_owner())
                        {
                            match timestamp_before(Duration::from_secs(7 * 24 * 60 * 60)) {
                                Ok(cutoff) => {
                                    if let Err(error) = factory.delete_analyze_jobs_before(cutoff) {
                                        log_gc_analyze_history_failed(error);
                                    }
                                }
                                Err(error) => log_gc_analyze_history_failed(error),
                            }
                        }
                    }
                    if now >= next_cleanup {
                        next_cleanup = now + cleanup_interval;
                        match timestamp_before(Duration::from_secs(10 * 60)) {
                            Ok(cutoff) => {
                                if let Err(error) = factory
                                    .cleanup_corrupted_analyze_jobs_on_current_instance(cutoff)
                                {
                                    log_cleanup_analyze_jobs_current_instance_failed(error);
                                }
                                if factory
                                    .stats_owner
                                    .as_ref()
                                    .is_some_and(|owner| owner.is_owner())
                                {
                                    if let Err(error) = factory
                                        .cleanup_corrupted_analyze_jobs_on_dead_instances(cutoff)
                                    {
                                        tidb_util::logutil::bg_logger().warn(
                                            "cleanup analyze jobs on dead instances failed",
                                            &[crate::domain::zap_error(error)],
                                        );
                                    }
                                }
                            }
                            Err(error) => log_cleanup_analyze_jobs_current_instance_failed(error),
                        }
                    }
                }
            })
            .expect("analyze-jobs cleanup worker spawns");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

// Go `Domain.analyzeJobsCleanupWorker`'s lines.
fn log_gc_analyze_history_failed(error: impl std::fmt::Display) {
    tidb_util::logutil::bg_logger().warn("gc analyze history failed", &[crate::domain::zap_error(error)]);
}

fn log_cleanup_analyze_jobs_current_instance_failed(error: impl std::fmt::Display) {
    tidb_util::logutil::bg_logger().warn(
        "cleanup analyze jobs on current instance failed",
        &[crate::domain::zap_error(error)],
    );
}

impl Drop for AnalyzeJobsCleanupWorker {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn timestamp_before(duration: Duration) -> Result<tidb_datatype::Time, String> {
    let nanoseconds = i64::try_from(duration.as_nanos())
        .map_err(|_| "analyze-job cleanup duration is too large".to_owned())?;
    let delta = tidb_datatype::MySqlDuration::from_nanoseconds(-nanoseconds, 0)
        .map_err(|error| error.to_string())?;
    tidb_exec::mysql_bootstrap::utc_now_timestamp()
        .add_duration(delta)
        .map_err(|error| error.to_string())
}

struct UsageWorkerStop {
    stopped: Mutex<bool>,
    wake: Condvar,
}

impl UsageWorkerStop {
    fn wait(&self, interval: Duration) -> bool {
        let stopped = self
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *stopped {
            return true;
        }
        *self
            .wake
            .wait_timeout(stopped, interval)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
    }
}

impl StatsUsageWorkers {
    fn start(factory: &Arc<Domain>, lease: crate::domain::node_config::StatsLease) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let mut threads = vec![spawn_usage_worker(
            "index-usage-gc",
            Weak::clone(&weak),
            Arc::clone(&stop),
            Duration::from_secs(30 * 60),
            Domain::gc_index_usage,
        )];
        let mut flush_on_drop = false;
        if let crate::domain::node_config::StatsLease::Positive(lease) = lease {
            flush_on_drop = true;
            let jitter = || {
                Duration::from_nanos(tidb_util::fastrand::uint64_n(
                    Duration::from_secs(60).as_nanos() as u64,
                ))
            };
            threads.push(spawn_usage_worker(
                "column-stats-usage-dump",
                Weak::clone(&weak),
                Arc::clone(&stop),
                lease.saturating_mul(100).saturating_add(jitter()),
                |factory| factory.dump_col_stats_usage_to_kv("default"),
            ));
            threads.push(spawn_usage_worker(
                "stats-delta-dump",
                Weak::clone(&weak),
                Arc::clone(&stop),
                lease.saturating_mul(20).saturating_add(jitter()),
                |factory| factory.dump_stats_delta_to_kv(false, &[], "default"),
            ));
        }
        Self {
            stop,
            threads,
            flush_on_drop,
        }
    }

    /// Stops and joins every usage worker before Go's shutdown delta flush.
    fn stop_before_shutdown_flush(self) -> bool {
        let flush = self.flush_on_drop;
        drop(self);
        flush
    }
}

impl Drop for StatsUsageWorkers {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        // Go `ddl.close` and `Domain.Close`: `variable.UnregisterStatistics`.
        for statistics in &self.statistics {
            crate::statusvar::unregister_statistics(statistics);
        }
        self.plan_cache_invalidation.stop_maintenance();
        // Positive-lease maintenance has already joined. For zero/negative
        // leases this is Go's quitStatsOwner lifetime instead.
        if let Some(owner) = &self.stats_owner {
            owner.close();
        }
        if let Some(refresher) = self.auto_analyze_refresher.get() {
            refresher
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .close();
        } else if let Some(queue) = self.auto_analyze_priority_queue.get() {
            queue.close();
        }
        let flush_stats = self
            .stats_usage_workers
            .take()
            .is_some_and(StatsUsageWorkers::stop_before_shutdown_flush);
        if flush_stats {
            if let Err(error) = self.dump_stats_delta_to_kv(true, &[], "default") {
                // Go `Handle.FlushStats`.
                tidb_stats_handle_logutil::stats_logger()
                    .warn("dump stats delta fail", &[crate::domain::zap_error(error)]);
            }
        }
    }
}

#[derive(Clone)]
struct ClusterHistoricalInfoSchema {
    catalog: Arc<SharedClusterCatalog>,
}

struct ClusterHistoricalSchemaView {
    catalog: ClusterCatalog,
}

impl InfoSchemaView for ClusterHistoricalSchemaView {
    fn table_by_id(&self, table_id: i64) -> Option<TableMeta> {
        self.catalog
            .databases
            .iter()
            .flat_map(|database| &database.tables)
            .find(|table| table.id == table_id)
            .map(|table| TableMeta {
                id: table.id,
                name: table.name.original().to_owned(),
            })
    }

    fn find_table_by_partition_id(&self, partition_id: i64) -> Option<TableMeta> {
        self.catalog
            .databases
            .iter()
            .flat_map(|database| &database.tables)
            .find(|table| {
                table.get_partition_info().is_some_and(|partition| {
                    partition
                        .read()
                        .definitions
                        .snapshot()
                        .iter()
                        .any(|definition| definition.id == partition_id)
                })
            })
            .map(|table| TableMeta {
                id: table.id,
                name: table.name.original().to_owned(),
            })
    }

    fn schema_by_table(&self, table: &TableMeta) -> Option<String> {
        self.catalog.databases.iter().find_map(|database| {
            database
                .tables
                .iter()
                .any(|candidate| candidate.id == table.id)
                .then(|| database.info.name.original().to_owned())
        })
    }
}

impl SessionInfoSchema for ClusterHistoricalInfoSchema {
    type View = ClusterHistoricalSchemaView;

    fn info_schema(&self) -> Self::View {
        ClusterHistoricalSchemaView {
            catalog: (*self.catalog.load()).clone(),
        }
    }
}

struct ClusterHistoricalStatsHandle {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
    global_vars: GlobalSysvars,
    /// The node's live row-ID allocators for new rows.
    auto_ids: Arc<dyn TableAutoIds>,
}

#[derive(Clone, Copy)]
enum OutdatedHistoryTable {
    Meta,
    Data,
}

impl ClusterHistoricalStatsHandle {
    fn commit_stats_plan(
        &self,
        build: impl FnOnce(
            &mut SnapshotMetaSnapshot,
            u64,
        ) -> Result<tidb_exec::cluster_stats_write::StatsWritePlan, String>,
    ) -> Result<(), String> {
        let snapshot = self.transactions.open_snapshot("default")?;
        let read_ts = snapshot.start_ts();
        let mut meta = SnapshotMetaSnapshot::new(snapshot);
        let plan = build(&mut meta, read_ts)?;
        self.transactions
            .commit_optimistic_mutations(
                plan.mutations,
                read_ts,
                "default",
                crate::domain::session_transaction::restricted_transaction_options(&self.global_vars),
            )
            .map_err(|error| error.message)
    }

    fn commit_outdated_history_delete(
        &self,
        catalog: &ClusterCatalog,
        table: OutdatedHistoryTable,
        cutoff: tidb_datatype::Time,
        limit: usize,
    ) -> Result<(), String> {
        let snapshot = self.transactions.open_snapshot("default")?;
        let read_ts = snapshot.start_ts();
        let plan = {
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            match table {
                OutdatedHistoryTable::Meta => {
                    tidb_exec::cluster_stats_write::plan_outdated_historical_meta_delete(
                        &mut snapshot,
                        catalog,
                        cutoff,
                        limit,
                    )
                }
                OutdatedHistoryTable::Data => {
                    tidb_exec::cluster_stats_write::plan_outdated_historical_data_delete(
                        &mut snapshot,
                        catalog,
                        cutoff,
                        limit,
                    )
                }
            }
            .map_err(|error| error.to_string())?
        };
        self.transactions
            .commit_optimistic_mutations(
                plan.mutations,
                read_ts,
                "default",
                crate::domain::session_transaction::restricted_transaction_options(&self.global_vars),
            )
            .map_err(|error| error.message)
    }

    /// Pinned Go `storage.ClearOutdatedHistoryStats` including its opening
    /// metadata count, 1,000-row metadata statements, and the single 50-row
    /// payload statement reached by Go's immediate return inside that loop.
    fn clear_outdated_history_stats(&self) -> Result<(), String> {
        let configured = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_HISTORICAL_STATS_DURATION)
            .map_err(|error| format!("read tidb_historical_stats_duration failed: {error:?}"))?;
        let retention_nanos = serde_json::from_value::<tidb_config::configtypes::Duration>(
            serde_json::Value::String(configured),
        )
        .map(|duration| duration.0)
        .map_err(|error| error.to_string())?;
        let retention_nanos = retention_nanos
            .checked_neg()
            .ok_or_else(|| "tidb_historical_stats_duration is out of range".to_owned())?;
        let retention = tidb_datatype::MySqlDuration::from_nanoseconds(retention_nanos, 0)
            .map_err(|error| error.to_string())?;
        // Historical create_time is DATETIME in the internal SYSTEM session,
        // so Go's NOW() cutoff must use that same wall clock, not UTC.
        let cutoff = tidb_exec::mysql_bootstrap::local_now_datetime()
            .add_duration(retention)
            .map_err(|error| error.to_string())?;
        let catalog = self.catalog.load();
        let count = {
            let snapshot = self.transactions.open_snapshot("default")?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            tidb_exec::cluster_stats_write::count_outdated_historical_stats(
                &mut snapshot,
                &catalog,
                cutoff,
            )
            .map_err(|error| error.to_string())?
        };
        if count == 0 {
            return Ok(());
        }
        for _ in 0..count.div_ceil(1_000) {
            self.commit_outdated_history_delete(
                &catalog,
                OutdatedHistoryTable::Meta,
                cutoff,
                1_000,
            )?;
        }
        self.commit_outdated_history_delete(&catalog, OutdatedHistoryTable::Data, cutoff, 50)
    }

    fn physical_table<'catalog>(
        catalog: &'catalog ClusterCatalog,
        physical_id: i64,
    ) -> Option<&'catalog tidb_model::table_info::TableInfo> {
        catalog
            .databases
            .iter()
            .flat_map(|database| &database.tables)
            .find(|table| {
                table.id == physical_id
                    || table.get_partition_info().is_some_and(|partition| {
                        partition
                            .read()
                            .definitions
                            .snapshot()
                            .iter()
                            .any(|definition| definition.id == physical_id)
                    })
            })
    }

    fn gc_table_stats(&self, catalog: &ClusterCatalog, physical_id: i64) -> Result<bool, String> {
        let histograms = {
            let snapshot = self.transactions.open_snapshot("default")?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            tidb_exec::cluster_stats_write::load_stats_gc_histograms(
                &mut snapshot,
                catalog,
                physical_id,
            )
            .map_err(|error| error.to_string())?
        };
        let Some(table) = Self::physical_table(catalog, physical_id) else {
            if histograms.is_empty() {
                self.commit_stats_plan(|snapshot, _| {
                    tidb_exec::cluster_stats_write::plan_stats_meta_delete_for_table(
                        snapshot,
                        catalog,
                        physical_id,
                    )
                    .map_err(|error| error.to_string())
                })?;
            } else {
                self.commit_stats_plan(|snapshot, version| {
                    tidb_exec::cluster_stats_write::plan_delete_table_stats(
                        snapshot,
                        catalog,
                        &[physical_id],
                        false,
                        version,
                        Some(self.auto_ids.as_ref()),
                    )
                    .map_err(|error| error.to_string())
                })?;
            }
            return Ok(false);
        };

        let logical_table_exists = table.id == physical_id;
        for (is_index, hist_id) in histograms {
            let exists = if is_index {
                table
                    .indices
                    .iter_deref()
                    .any(|index| index.read().id == hist_id)
            } else {
                table
                    .cols()
                    .iter_deref()
                    .any(|column| column.read().id == hist_id)
            };
            if exists {
                continue;
            }
            self.commit_stats_plan(|snapshot, version| {
                tidb_exec::cluster_stats_write::plan_stats_item_delete(
                    snapshot,
                    catalog,
                    physical_id,
                    hist_id,
                    is_index,
                    version,
                    Some(self.auto_ids.as_ref()),
                )
                .map_err(|error| error.to_string())
            })?;
        }
        Ok(logical_table_exists)
    }

    fn delete_table_history(
        &self,
        catalog: &ClusterCatalog,
        physical_id: i64,
    ) -> Result<(), String> {
        self.commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_historical_stats_data_delete_for_table(
                snapshot,
                catalog,
                physical_id,
            )
            .map_err(|error| error.to_string())
        })?;
        self.commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_historical_stats_meta_delete_for_table(
                snapshot,
                catalog,
                physical_id,
            )
            .map_err(|error| error.to_string())
        })
    }

    fn wall_clock_tso() -> Result<u64, String> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis();
        let millis = u64::try_from(millis).map_err(|_| "wall clock overflows u64".to_owned())?;
        Ok(millis << 18)
    }

    /// Pinned Go `storage.GCStats`, including its version window, per-item
    /// transaction boundaries, dropped-table two phases, history cleanup,
    /// warning-only expiry cleanup, and final persisted timestamp.
    fn gc_stats(&self, stats_lease: Duration, ddl_lease: Duration) -> Result<(), String> {
        let lease = stats_lease.max(ddl_lease);
        let nanos = i64::try_from(lease.as_nanos())
            .map_err(|_| "statistics GC lease exceeds Go time.Duration".to_owned())?;
        let offset = ((nanos.wrapping_mul(10) / 1_000_000) as u64) << 18;
        let now = Self::wall_clock_tso()?;
        if now < offset {
            return Ok(());
        }
        let gc_version = now - offset;
        let catalog = self.catalog.load();
        let last_gc = {
            let snapshot = self.transactions.open_snapshot("default")?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            tidb_exec::cluster_stats_write::load_stats_gc_timestamp(&mut snapshot, &catalog)
                .map_err(|error| error.to_string())?
        };
        let candidates = {
            let snapshot = self.transactions.open_snapshot("default")?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            tidb_exec::cluster_stats_write::load_stats_gc_candidates(
                &mut snapshot,
                &catalog,
                last_gc,
                gc_version,
            )
            .map_err(|error| error.to_string())?
        };

        for physical_id in candidates {
            let exists = self.gc_table_stats(&catalog, physical_id)?;
            if !exists {
                self.delete_table_history(&catalog, physical_id)?;
            }
        }

        if let Err(error) = self.clear_outdated_history_stats() {
            // Go `StatsGC.GCStats`, with `vardef.HistoricalStatsDuration`.
            let duration = self
                .global_vars
                .get(tidb_vardef::tidb_vars::TIDB_HISTORICAL_STATS_DURATION)
                .ok()
                .and_then(|configured| {
                    serde_json::from_value::<tidb_config::configtypes::Duration>(
                        serde_json::Value::String(configured),
                    )
                    .ok()
                })
                .map_or(0, |duration| duration.0);
            tidb_util::logutil::bg_logger().warn(
                "failed to gc outdated historical stats",
                &[
                    tidb_log::Field::new("duration", tidb_log::Value::Duration(duration)),
                    crate::domain::zap_error(error),
                ],
            );
        }
        self.commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_stats_gc_timestamp_write(
                snapshot,
                &catalog,
                gc_version,
                tidb_exec::mysql_bootstrap::utc_now_timestamp(),
                Some(self.auto_ids.as_ref()),
            )
            .map_err(|error| error.to_string())
        })
    }

    fn table_historical_json(
        &self,
        catalog: &ClusterCatalog,
        physical_id: i64,
        snapshot_ts: u64,
    ) -> Result<Option<tidb_stats::JsonTable>, String> {
        let snapshot = self.transactions.open_snapshot("default")?;
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        tidb_exec::cluster_stats_dump::table_historical_stats_to_json(
            &mut snapshot,
            catalog,
            physical_id,
            snapshot_ts,
        )
        .map_err(|error| error.to_string())
    }

    fn table_json(
        &self,
        catalog: &ClusterCatalog,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        physical_id: i64,
    ) -> Result<Option<tidb_stats::JsonTable>, String> {
        let table_stats = {
            let snapshot = self.transactions.open_snapshot("default")?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            tidb_exec::cluster_stats_dump::load_table_stats_payload(
                &mut snapshot,
                catalog,
                table,
                physical_id,
            )
            .map_err(|error| error.to_string())?
        };
        let Some(table_stats) = table_stats else {
            return Ok(None);
        };
        let snapshot = self.transactions.open_snapshot("default")?;
        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
        tidb_exec::cluster_stats_dump::table_stats_to_json_from_loaded(
            &mut snapshot,
            catalog,
            database_name,
            table,
            physical_id,
            table_stats,
        )
        .map_err(|error| error.to_string())
    }

    fn historical_json(
        &self,
        catalog: &ClusterCatalog,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        physical_id: i64,
        is_partition: bool,
    ) -> Result<Option<tidb_stats::JsonTable>, String> {
        if is_partition || table.get_partition_info().is_none() {
            return self.table_json(catalog, database_name, table, physical_id);
        }
        let partition = table.get_partition_info().expect("partition checked above");
        let mut partitions = std::collections::BTreeMap::new();
        for definition in partition.read().definitions.snapshot() {
            if let Some(json) = self.table_json(catalog, database_name, table, definition.id)? {
                partitions.insert(definition.name.lowercase().to_owned(), Some(json));
            }
        }
        if let Some(global) = self.table_json(catalog, database_name, table, table.id)? {
            partitions.insert(tidb_stats::TIDB_GLOBAL_STATS.to_owned(), Some(global));
        }
        Ok(Some(tidb_stats::JsonTable {
            database_name: database_name.to_owned(),
            table_name: table.name.lowercase().to_owned(),
            partitions: Some(partitions),
            ..tidb_stats::JsonTable::default()
        }))
    }

    fn historical_or_latest_json(
        &self,
        catalog: &ClusterCatalog,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        physical_id: i64,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats::JsonTable>, bool), String> {
        if let Some(historical) = self.table_historical_json(catalog, physical_id, snapshot_ts)? {
            return Ok((Some(historical), false));
        }
        Ok((
            self.table_json(catalog, database_name, table, physical_id)?,
            snapshot_ts != 0,
        ))
    }

    /// Pinned Go `statsReadWriter.DumpHistoricalStatsBySnapshot`, including
    /// its feature gate, per-physical-table fallback names, and dump metrics.
    fn dump_historical_stats_by_snapshot(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats::JsonTable>, Vec<String>), String> {
        let enabled = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .map_err(|error| format!("check tidb_enable_historical_stats failed: {error:?}"))?;
        if !tidb_vardef::option_values::tidb_opt_on(&enabled) {
            return Err("tidb_enable_historical_stats should be enabled".to_owned());
        }
        let result =
            self.dump_historical_stats_by_snapshot_inner(database_name, table, snapshot_ts);
        if result.is_ok() {
            tidb_stats_handle_metrics::dump_historical_stats_success_counter().inc();
        } else {
            tidb_stats_handle_metrics::dump_historical_stats_failed_counter().inc();
        }
        result
    }

    fn dump_historical_stats_by_snapshot_inner(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats::JsonTable>, Vec<String>), String> {
        let catalog = self.catalog.load();
        let Some(partition) = table.get_partition_info() else {
            let (json, fallback) = self.historical_or_latest_json(
                &catalog,
                database_name,
                table,
                table.id,
                snapshot_ts,
            )?;
            return Ok((
                json,
                fallback
                    .then(|| format!("{database_name}.{}", table.name.original()))
                    .into_iter()
                    .collect(),
            ));
        };

        let mut partitions = std::collections::BTreeMap::new();
        let mut fallbacks = Vec::new();
        for definition in partition.read().definitions.snapshot() {
            let (json, fallback) = self.historical_or_latest_json(
                &catalog,
                database_name,
                table,
                definition.id,
                snapshot_ts,
            )?;
            if fallback {
                fallbacks.push(format!(
                    "{database_name}.{} {}",
                    table.name.original(),
                    definition.name.original()
                ));
            }
            partitions.insert(definition.name.lowercase().to_owned(), json);
        }
        let (global, fallback) =
            self.historical_or_latest_json(&catalog, database_name, table, table.id, snapshot_ts)?;
        if fallback {
            fallbacks.push(format!("{database_name}.{} global", table.name.original()));
        }
        if global.is_some() {
            partitions.insert(tidb_stats::TIDB_GLOBAL_STATS.to_owned(), global);
        }
        Ok((
            Some(tidb_stats::JsonTable {
                database_name: database_name.to_owned(),
                table_name: table.name.lowercase().to_owned(),
                partitions: Some(partitions),
                ..tidb_stats::JsonTable::default()
            }),
            fallbacks,
        ))
    }
}

impl HistoricalStatsHandle for ClusterHistoricalStatsHandle {
    fn check_historical_stats_enable(&self) -> Result<bool, String> {
        self.global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .map(|value| tidb_vardef::option_values::tidb_opt_on(&value))
            .map_err(|error| format!("{error:?}"))
    }

    fn record_historical_stats_to_storage(
        &self,
        database_name: &str,
        table: &TableMeta,
        physical_id: i64,
        is_partition: bool,
    ) -> Result<u64, String> {
        let row_ids = self.auto_ids.clone();
        let catalog = self.catalog.load();
        let table_info = catalog
            .databases
            .iter()
            .flat_map(|database| &database.tables)
            .find(|candidate| candidate.id == table.id)
            .ok_or_else(|| format!("cannot get table by id {}", table.id))?;
        let Some(json) = self.historical_json(
            &catalog,
            database_name,
            table_info,
            physical_id,
            is_partition,
        )?
        else {
            // Go `RecordHistoricalStatsToStorage`.
            tidb_stats_handle_logutil::stats_logger().warn(
                "no stats data to record",
                &[
                    tidb_log::Field::new("dbName", tidb_log::Value::Str(database_name.to_string())),
                    tidb_log::Field::new("tableName", tidb_log::Value::Str(table.name.clone())),
                ],
            );
            return Ok(0);
        };
        // Internal statement: no fair locking (Go `InRestrictedSQL`).
        let transaction = self.transactions.begin(
            true,
            false,
            "default",
            crate::domain::session_transaction::restricted_transaction_options(&self.global_vars),
        )?;
        let staged = MutationBuffer::new();
        transaction.bind_mutation_buffer(&staged);
        let (version, blocks) = tidb_exec::cluster_stats_write::historical_stats_data_blocks(&json)
            .map_err(|error| error.to_string())?;
        let create_time = tidb_exec::mysql_bootstrap::local_now_datetime6();
        for (sequence, block) in blocks.iter().enumerate() {
            let ((), mutations) =
                tidb_cluster::cluster_table_storage::lock_pessimistic_statement_with(
                    transaction.start_ts(),
                    &staged,
                    |read_ts| match read_ts {
                        Some(read_ts) => transaction.snapshot_at_for(read_ts, true),
                        None => transaction.snapshot_for(true),
                    },
                    |keys, presume_not_exists, duplicate_hints| {
                        transaction.lock_staged_keys_with_assertions(
                            keys,
                            presume_not_exists,
                            duplicate_hints,
                            tidb_txnkv::transaction::LockWaitTime::Timeout(
                                global_lock_wait_timeout(&self.global_vars),
                            ),
                        )
                    },
                    |snapshot, _start_ts| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let plan =
                            tidb_exec::cluster_stats_write::plan_historical_stats_data_block(
                                &mut snapshot,
                                &catalog,
                                physical_id,
                                version,
                                sequence,
                                block,
                                create_time,
                                Some(row_ids.as_ref()),
                            )
                            .map_err(|error| error.to_string())?;
                        Ok(((), plan.mutations))
                    },
                )
                .map_err(|error| error.to_string())?;
            tidb_cluster::cluster_table_storage::stage_mutations(&staged, mutations)
                .map_err(|error| error.to_string())?;
        }
        transaction.commit(&staged).map_err(|error| error.message)?;
        Ok(version)
    }
}

/// The thread of Go `Domain.LoadSigningCertLoop`, stopped when the node is
/// (Go's `do.exit`).
struct SigningCertLoop {
    exit: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SigningCertLoop {
    fn start() -> Self {
        let (exit, exited) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("load-signing-cert-loop".to_owned())
            .spawn(move || {
                while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                    exited.recv_timeout(tidb_sessionstates::get_load_cert_interval())
                {
                    tidb_sessionstates::reload_signing_cert();
                }
            })
            .ok();
        Self {
            exit: Some(exit),
            thread,
        }
    }
}

impl Drop for SigningCertLoop {
    fn drop(&mut self) {
        drop(self.exit.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct HistoricalStatsRuntime {
    worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

struct ClusterHistoricalStatsMetrics;

impl HistoricalStatsMetrics for ClusterHistoricalStatsMetrics {
    fn inc_generate_failed(&self) {
        tidb_stats_handle_metrics::domain_metrics::generate_historical_stats_failed_counter().inc();
    }

    fn inc_generate_success(&self) {
        tidb_stats_handle_metrics::domain_metrics::generate_historical_stats_success_counter()
            .inc();
    }
}

impl HistoricalStatsRuntime {
    fn start(
        worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
        handle: Arc<ClusterHistoricalStatsHandle>,
    ) -> Self {
        let running = Arc::clone(&worker);
        let thread = crate::domain::session_api::sql_worker_thread("historical-stats-worker")
            .spawn(move || {
                while let Some(table_id) = running.recv_historical_stats_table() {
                    if let Err(error) = running.dump_historical_stats(
                        table_id,
                        handle.as_ref(),
                        &ClusterHistoricalStatsMetrics,
                    ) {
                        // Go `Domain.StartHistoricalStatsWorker`.
                        tidb_util::logutil::bg_logger().warn(
                            "dump historical stats failed",
                            &[
                                crate::domain::zap_error(error),
                                tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id)),
                            ],
                        );
                    }
                }
            })
            .expect("spawning historical statistics worker");
        Self {
            worker,
            thread: Some(thread),
        }
    }
}

impl Drop for HistoricalStatsRuntime {
    fn drop(&mut self) {
        self.worker.close_table_channel();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn spawn_usage_worker(
    name: &str,
    factory: Weak<Domain>,
    stop: Arc<UsageWorkerStop>,
    interval: Duration,
    action: fn(&Domain) -> Result<(), String>,
) -> std::thread::JoinHandle<()> {
    let worker_name = name.to_owned();
    let log_name = worker_name.clone();
    crate::domain::session_api::sql_worker_thread(worker_name)
        .spawn(move || {
            while !stop.wait(interval) {
                let Some(factory) = factory.upgrade() else {
                    return;
                };
                if let Err(error) = action(&factory) {
                    log_usage_worker_failure(&log_name, error);
                }
            }
        })
        .expect("spawning statistics usage worker")
}

// Go `subscriber.handle`'s best-effort DROP SCHEMA line: one entry whichever
// half of `delayedDeleteStats4PhysicalID` failed.
fn log_stats_gc_version_failed(physical_id: i64, error: String) {
    tidb_stats_handle_logutil::stats_logger().error(
        "Failed to update stats meta version for gc",
        &[
            tidb_log::Field::new("tableID", tidb_log::Value::I64(physical_id)),
            crate::domain::zap_error(error),
        ],
    );
}

// The failure line of the Go worker each usage worker ports:
// `indexUsageWorker`, `dumpColStatsUsageWorker`, `deltaUpdateTickerWorker`.
fn log_usage_worker_failure(name: &str, error: String) {
    let error = [crate::domain::zap_error(error)];
    match name {
        "index-usage-gc" => {
            tidb_stats_handle_logutil::stats_logger().error("gc index usage failed", &error);
        }
        "column-stats-usage-dump" => {
            tidb_util::logutil::bg_logger().warn("dump column stats usage failed", &error);
        }
        _ => tidb_util::logutil::bg_logger().warn("dump stats delta failed", &error),
    }
}

fn persist_column_stats_usage_batches(
    pending: &mut tidb_stats_handle_usage::ColumnStatsUsageDump<'_>,
    entries: &[(tidb_model::TableItemID, SystemTime)],
    mut persist: impl FnMut(&[(tidb_model::TableItemID, SystemTime)]) -> Result<(), String>,
) -> Result<(), String> {
    for batch in entries.chunks(tidb_stats_handle_usage::BATCH_INSERT_SIZE) {
        persist(batch)?;
    }
    pending.mark_persisted(entries.iter().map(|(item, _)| *item));
    Ok(())
}

fn system_time_timestamp(value: SystemTime) -> Result<tidb_datatype::Time, String> {
    let value: chrono::DateTime<chrono::Utc> = value.into();
    tidb_datatype::Time::from_date_checked(
        value.year(),
        i32::try_from(value.month()).expect("month fits in i32"),
        i32::try_from(value.day()).expect("day fits in i32"),
        i32::try_from(value.hour()).expect("hour fits in i32"),
        i32::try_from(value.minute()).expect("minute fits in i32"),
        i32::try_from(value.second()).expect("second fits in i32"),
        0,
        tidb_datatype::TimeType::Timestamp,
        6,
    )
    .map_err(|error| error.to_string())
}

fn stats_delta_table(catalog: &ClusterCatalog, physical_id: i64) -> Option<(&str, Option<i64>)> {
    for database in &catalog.databases {
        for table in &database.tables {
            if table.id == physical_id {
                return Some((database.info.name.lowercase(), None));
            }
            if table.partition.as_ref().is_some_and(|partition| {
                partition
                    .read()
                    .definitions
                    .snapshot()
                    .iter()
                    .any(|definition| definition.id == physical_id)
            }) {
                return Some((database.info.name.lowercase(), Some(table.id)));
            }
        }
    }
    None
}

#[cfg(test)]
fn partition_id_map(
    catalog: &ClusterCatalog,
    schema: &str,
    table: &str,
) -> Option<(i64, Vec<(String, i64)>)> {
    let (_, table) = catalog.find_table(schema, table)?;
    let partition = table.partition.as_ref()?.read();
    Some((
        table.id,
        partition
            .definitions
            .snapshot()
            .into_iter()
            .map(|definition| (definition.name.lowercase().to_owned(), definition.id))
            .collect(),
    ))
}

fn table_and_partition_ids(table: &tidb_model::table_info::TableInfo) -> Vec<i64> {
    let mut ids = vec![table.id];
    if let Some(partition) = &table.partition {
        ids.extend(
            partition
                .read()
                .definitions
                .snapshot()
                .into_iter()
                .map(|definition| definition.id),
        );
    }
    ids
}

fn stats_physical_ids(table: &tidb_model::table_info::TableInfo, dynamic: bool) -> Vec<i64> {
    let Some(partition) = &table.partition else {
        return vec![table.id];
    };
    let mut ids = partition
        .read()
        .definitions
        .snapshot()
        .into_iter()
        .map(|definition| definition.id)
        .collect::<Vec<_>>();
    if dynamic {
        ids.push(table.id);
    }
    ids
}

impl QuerySessionFactory for Domain {
    type Session = Session;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        let mut opened = self.open_storage_session(context.connection_id)?;
        let session = &mut opened;
        let identity = &context.identity;
        session.set_restricted_sql(identity.is_internal());
        session.set_user(
            format!("{}@{}", identity.username(), identity.host()),
            format!("{}@{}", identity.username(), context.peer_addr.host()),
        );
        session.set_client_found_rows(context.client_found_rows);
        session.set_tls_state(context.tls_state);
        session.set_compression_level(context.compression_level);
        session.set_compression_algorithm(context.compression_algorithm);
        if identity.in_sandbox_mode() {
            session.enable_sandbox_mode();
        }
        if identity.privilege_bypassed() {
            session.enable_privilege_bypass();
        }
        // Go's Domain-internal sessions never enter the server's client
        // list (`Server.clients` holds connections, the sys-session pool is
        // separate), so they never appear in SHOW PROCESSLIST. Registering
        // them here leaked every pooled internal session as a permanent
        // `Sleep` row.
        let guard = if identity.is_internal() {
            self.processes
                .register_internal(context.connection_id, session.current_database().to_owned())
        } else {
            self.processes.register(
                context.connection_id,
                identity.username().to_owned(),
                context.peer_addr.process_host(),
                session.current_database().to_owned(),
                Some(Arc::new(ConnectionKillTarget::new(
                    context.cancellation.clone(),
                    context.close.clone(),
                ))),
            )
        };
        self.attach_connection(session, context.connection_id, guard);
        Ok(opened)
    }

    fn install_tls_manager(
        &self,
        manager: Arc<dyn crate::process::TlsManager>,
        variables: Vec<(&'static str, String)>,
    ) {
        self.processes.set_tls_manager(manager);
        for (name, value) in variables {
            self.global_vars.set_startup(name, value);
        }
    }

    fn server_identity(&self) -> Option<Arc<tidb_domain::server_id::ServerIdAuthority>> {
        self.server_identity.clone()
    }

    fn connection_ids(&self) -> Option<Arc<dyn tidb_util::globalconn::Allocator + Send + Sync>> {
        Some(self.connection_id_allocator())
    }

    fn session_manager(&self) -> Option<Arc<dyn tidb_util::memoryusagealarm::SessionManager>> {
        Some(Arc::new(self.processes.clone()))
    }
}

struct ClusterGlobalIds {
    transactions: Arc<dyn ClusterTransactions>,
    global_vars: GlobalSysvars,
}

impl std::fmt::Debug for ClusterGlobalIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterGlobalIds")
    }
}

impl crate::GlobalIdGenerator for ClusterGlobalIds {
    fn gen_global_id(&self) -> Result<i64, tidb_executor::DriverError> {
        let allocate_once = || -> Result<i64, SqlQueryError> {
            let snapshot = self
                .transactions
                .open_snapshot("default")
                .map_err(SqlQueryError::unknown)?;
            let read_ts = snapshot.start_ts();
            let (id, mutation) = tidb_ddl::ddl_job_submit::reserve_global_id(
                &mut SnapshotMetaSnapshot::new(snapshot),
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            self.transactions.commit_optimistic_mutations(
                vec![mutation],
                read_ts,
                "default",
                crate::domain::session_transaction::restricted_transaction_options(&self.global_vars),
            )?;
            Ok(id)
        };
        let mut attempt = 0;
        loop {
            match allocate_once() {
                Err(error) if error.code == 9007 && attempt + 1 < tidb_txnkv::MAX_RETRY_COUNT => {
                    std::thread::sleep(tidb_txnkv::retry_backoff_delay(attempt));
                    attempt += 1;
                }
                result => {
                    return result.map_err(|error| {
                        tidb_executor::DriverError::Mysql(
                            tidb_executor::MysqlError::from_parts(
                                error.code,
                                error.state,
                                error.message,
                            )
                            .with_identity_opt(error.identity),
                        )
                    })
                }
            }
        }
    }
}

impl Domain {
    // No authentication identity, process-list entry, or user transaction is
    // copied into internal storage work. Its MDL holds nest in the caller's
    // connection scope, while its storage and transaction are independent.
    fn open_storage_session(&self, connection_id: u64) -> Result<Session, SqlQueryError> {
        // The connection's own snapshot slot and staged writes: one session,
        // one transaction, exactly as Go's session owns one `kv.Transaction`.
        let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&slot) as _;
        let mut storage = ClusterTableStorage::new(buffer.clone(), handle);
        if let Some(mpp) = self.mpp_client.as_ref() {
            storage = storage.with_mpp_client(Arc::clone(mpp));
        }
        if let Some(client) = self.kv_client.as_ref() {
            storage = storage.with_client(Arc::clone(client));
        }
        let loaded = self.catalog.load();
        // One fully built table set per schema version, shared the same way:
        // each session clones its tables (columns/indexes are Arc-shared) and
        // swaps in only its own storage seam below.
        let template_storage = detached_storage();
        let mut kv_templates = self
            .session_kv_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        kv_templates.reuse(&loaded);
        let mut built = cluster_session_catalog_with_templates(
            &loaded,
            &storage,
            None,
            self.auto_ids.as_ref(),
            &template_storage,
            Some(&mut kv_templates),
        );
        let statistics_loading = ClusterStatisticsLoading {
            view: Arc::clone(&self.statistics_view),
            loader: Arc::new(ClusterStatisticsItemLoader {
                transactions: Arc::clone(&self.transactions),
                catalog: Arc::clone(&self.catalog),
                stats: Arc::clone(&self.stats),
                global_vars: self.global_vars.clone(),
            }),
            workers: Arc::clone(
                self.stats_load_workers
                    .get_or_init(tidb_executor::driver::StatisticsLoadWorkers::new),
            ),
        };
        let mut session = Session::over_catalog(crate::SharedCatalog::new(built.catalog));
        session.set_stats_handle(statistics_loading.handle());
        session.set_global_id_generator(Arc::new(ClusterGlobalIds {
            transactions: Arc::clone(&self.transactions),
            global_vars: self.global_vars.clone(),
        }));
        if let Some(regions) = &self.region_store {
            session.set_region_store(Arc::clone(regions));
        }
        session.set_restricted_sql(true);
        {
            // Go `Txn(true)`: asking the bound statement snapshot for its
            // timestamp is its activation, which publishes the statement's
            // start ts (`StatementReadTs`).
            let activation_slot = Arc::clone(&slot);
            session.set_statement_txn_activator(Arc::new(move || {
                activation_slot
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .start_ts();
            }));
        }
        {
            let transactions = Arc::clone(&self.transactions);
            let historical_catalog = Arc::clone(&self.catalog);
            let historical_storage = storage.snapshot_only();
            let historical_slot = Arc::clone(&slot);
            let auto_ids = Arc::clone(&self.auto_ids);
            let globals = self.global_vars.clone();
            // Go plans a stale read from the domain's current statistics.
            let statistics_view = Arc::clone(&self.statistics_view);
            session.set_historical_read_provider(Arc::new(move |ts, resource_group, schema| {
                let hold = Arc::new(tidb_txnkv::ACTIVE_START_TS.hold(ts));
                let snapshot = transactions
                    .open_snapshot_at(ts, resource_group)
                    .map_err(tidb_executor::DriverError::unsupported)?;
                let (catalog, snapshot) = if let Some(schema) = schema {
                    (schema.clone(), snapshot)
                } else {
                    let mut metadata = SnapshotMetaSnapshot::new(snapshot);
                    let capacity = globals
                        .get("tidb_schema_version_cache_limit")
                        .ok()
                        .and_then(|value| value.parse::<usize>().ok())
                        .unwrap_or(16);
                    let loaded = historical_catalog
                        .historical_at(&mut metadata, capacity)
                        .map_err(historical_catalog_error)?;
                    let built = cluster_session_catalog_with_templates(
                        &loaded,
                        &historical_storage,
                        Some(Arc::clone(&statistics_view)),
                        auto_ids.as_ref(),
                        &historical_storage,
                        None,
                    );
                    (built.catalog, metadata.into_snapshot())
                };
                // SET retains schema only; each read binds its own native snapshot.
                drop(
                    historical_slot
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .bind(snapshot),
                );
                Ok(crate::HistoricalRead {
                    catalog,
                    timestamp_hold: hold,
                })
            }));
        }
        {
            let transactions = Arc::clone(&self.transactions);
            let catalogs = Arc::clone(&self.catalog);
            let connection_storage = storage.snapshot_only();
            let auto_ids = Arc::clone(&self.auto_ids);
            let globals = self.global_vars.clone();
            let statistics_view = Arc::clone(&self.statistics_view);
            let restricted_sql = Arc::new(NodeRestrictedSql {
                factory: self.binding_session_factory.clone(),
            });
            session.set_restricted_sql_executor(Arc::clone(&restricted_sql) as _);
            session.set_snapshot_schema_provider(Arc::new(move |ts, resource_group, validate| {
                let hold = tidb_txnkv::ACTIVE_START_TS.hold(ts);
                let snapshot = transactions
                    .open_snapshot_at(ts, resource_group)
                    .map_err(tidb_executor::DriverError::unsupported)?;
                if validate {
                    crate::gcutil::validate_snapshot(restricted_sql.as_ref(), ts)?;
                }
                let mut metadata = SnapshotMetaSnapshot::new(snapshot);
                let capacity = globals
                    .get("tidb_schema_version_cache_limit")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(16);
                let loaded = catalogs
                    .historical_at(&mut metadata, capacity)
                    .map_err(historical_catalog_error)?;
                let built = cluster_session_catalog_with_templates(
                    &loaded,
                    &connection_storage,
                    Some(Arc::clone(&statistics_view)),
                    auto_ids.as_ref(),
                    &connection_storage,
                    None,
                );
                drop(metadata);
                drop(hold);
                Ok(built.catalog)
            }));
        }
        session.set_plan_cache_invalidation(Arc::clone(&self.plan_cache_invalidation));
        session.set_plan_cache_schema_version(loaded.schema_version as u64);
        session.set_info_schema(Arc::clone(&loaded));
        let accounts = Arc::clone(&self.accounts);
        session.set_privilege_change_notifier(Arc::new(move || {
            accounts.notify_all_privileges_changed();
        }));
        if let Some(syncer) = &self.global_config_syncer {
            session.set_global_config_syncer(Arc::clone(syncer));
        }
        session.set_index_usage_collector(self.stats_usage.index_usage_collector());
        if self
            .stats_usage_workers
            .get()
            .is_some_and(|workers| workers.flush_on_drop)
            && tidb_config::config_tree::config::get_global_config().enable_collect_execution_info
        {
            session.set_session_index_usage_collector(
                self.stats_usage.new_session_index_usage_collector(),
            );
        }
        session.set_stats_collector(self.stats_usage.new_session_stats_item());
        session.set_data_lock_waits_provider(self.data_lock_waits.clone());
        session.set_column_stats_usage_provider(Arc::new(ClusterColumnStatsUsageProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
        }));
        session.set_approximate_table_count_provider(
            Arc::clone(&self.approximate_table_counts) as Arc<_>,
        );
        session.set_analyze_status_provider(Arc::new(ClusterAnalyzeStatusProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            approximate_counts: Arc::clone(&self.approximate_table_counts),
        }));
        session.set_table_storage_stats_provider(Arc::new(ClusterTableStorageStatsProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            row_stats_cache: StatsTableRowCache::new(),
        }));
        if let Some(store) = self.bdr_role_store.get() {
            session.set_bdr_role_store(Arc::clone(store));
        }
        if let Some(store) = self.ddl_job_history_store.get() {
            session.set_ddl_job_history_store(Arc::clone(store));
        }
        session.set_advisory_lock_service(Arc::new(transactions::ClusterAdvisoryLockService::new(
            Arc::clone(&self.transactions),
        )));
        if let Some(syncer) = self.server_info.as_ref() {
            session.set_server_info_syncer(Arc::clone(syncer));
        }
        if let Some(client) = &self.cluster_peer {
            session.set_cluster_peer_client(Arc::clone(client));
        }
        if let Some(client) = &self.cluster_config {
            session.set_cluster_config_client(Arc::clone(client));
        }
        if let Some(client) = &self.cluster_diagnostics {
            session.set_cluster_diagnostics_client(Arc::clone(client));
        }
        if let Some(topology) = &self.cluster_topology {
            session.set_cluster_topology(Arc::clone(topology));
        }
        // `ADMIN SHOW DDL` reports the version this node currently follows,
        // which moves as the reloader picks up peers' changes -- so it is
        // read at statement time rather than captured when the session opens.
        let catalog_versions = Arc::clone(&self.catalog);
        session.set_cluster_schema_version_source(Arc::new(move || {
            catalog_versions.load().schema_version
        }));
        // Go `preprocess.go:2270` writes each bound table into the
        // transaction's `GetRelatedTableForMDL` map; here the driver session
        // reports it into the node's pin registry, which is what the
        // schema-sync acknowledger's per-table gate reads
        // (`RemoveLockDDLJobs`'s check, in `schema_sync.rs`).
        session.set_mdl_related_table_sink(Arc::new(schema_sync::ConnectionMdlSink::new(
            Arc::clone(&self.schema_pins),
            connection_id,
        )));
        if let Some(worker) = self.workload_repository.get() {
            session.set_workload_repository(Arc::clone(worker));
        }
        if let Some(spill_storage) = self.spill_storage.as_ref() {
            session.set_spill_storage(Arc::clone(spill_storage));
        }
        if let Some(arbitrator) = self.mem_arbitrator.as_ref() {
            session.set_mem_arbitrator(Arc::clone(arbitrator));
        }

        session
            .attach_globals(Arc::new(self.global_vars.clone()))
            .map_err(map_error)?;

        session.node = Some(Box::new(NodeConn {
            domain: self
                .binding_session_factory
                .upgrade()
                .expect("a domain opens sessions from behind its shared Arc"),
            txn: TxnManager::new(
                    Arc::clone(&self.transactions),
                    Arc::new(NodeSchemaPins(Arc::clone(&self.schema_pins))),
                    connection_id,
                    buffer.clone(),
                ),
            slot: slot,
            follower: catalog_follower::CatalogFollower::new(
                    Arc::clone(&self.catalog),
                    storage,
                    Arc::clone(&self.auto_ids),
                    statistics_loading,
                    loaded.schema_version,
                    built.skipped,
                ),
            commit_hooks: Box::new({
                    let bindings = self.bindings.clone();
                    let schema_validator = self.schema_validator.clone();
                    move |session, schema_version| {
                        Box::new(NodeCommitHooks {
                            bindings: bindings.clone(),
                            schema_validator: schema_validator.clone(),
                            schema_version,
                            catalog: session.shared_catalog(),
                        })
                    }
                }),
            activate_statement_txn: false,
            buffer,
            connection_id: connection_id,
            ok_packet: WriteOutcome::default(),
            parked_local_infile: None,
            store: None,
        }));
        Ok(session)
    }
}

fn stats_session_error(message: impl Into<String>) -> tidb_sqlexec::SqlExecError {
    Box::new(std::io::Error::other(message.into()))
}

/// Go `GetSnapshotInfoSchema`'s failure as the statement sees it: a meta
/// read's storage error with a SQL identity (`ErrTxnAbortedByGC` for a
/// timestamp behind the txn safe point) is returned unwrapped.
fn historical_catalog_error(
    error: tidb_cluster::cluster_catalog::ClusterCatalogError,
) -> tidb_executor::DriverError {
    match error {
        tidb_cluster::cluster_catalog::ClusterCatalogError::Sql(error) => {
            tidb_executor::DriverError::Mysql(error)
        }
        other => tidb_executor::DriverError::unsupported(other.to_string()),
    }
}

struct ClusterStatsSessionState {
    session: Mutex<Option<Session>>,
    global_vars: GlobalSysvars,
    registered: std::sync::atomic::AtomicBool,
}

fn system_process_trackers(
    processes: ProcessRegistry,
) -> (tidb_sqlexec::TrackSysProc, tidb_sqlexec::UntrackSysProc) {
    let tracking = processes.clone();
    let track: tidb_sqlexec::TrackSysProc = Arc::new(move |id, process| {
        let process = (&*process as &dyn std::any::Any)
            .downcast_ref::<crate::process::SystemProcess>()
            .ok_or_else(|| stats_session_error("invalid system process owner"))?;
        tracking
            .track_system_process(id, process.clone())
            .map_err(stats_session_error)
    });
    let untrack: tidb_sqlexec::UntrackSysProc =
        Arc::new(move |id| processes.untrack_system_process(id));
    (track, untrack)
}

struct SystemSessionExecution<'a, 's> {
    session: &'a mut ClusterServerSession<'s>,
    connection_id: u64,
}

impl Drop for SystemSessionExecution<'_, '_> {
    fn drop(&mut self) {
        self.session.node.connection_id = self.connection_id;
        self.session.session.set_connection_id(self.connection_id);
    }
}

struct SysProcessTrackGuard {
    process_id: u64,
    untrack: tidb_sqlexec::UntrackSysProc,
}

impl Drop for SysProcessTrackGuard {
    fn drop(&mut self) {
        (self.untrack)(self.process_id);
    }
}

impl ClusterStatsSessionState {
    fn with_session<T>(
        &self,
        callback: impl FnOnce(&mut ClusterServerSession<'_>) -> Result<T, tidb_sqlexec::SqlExecError>,
    ) -> Result<T, tidb_sqlexec::SqlExecError> {
        let mut slot = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = slot
            .as_mut()
            .ok_or_else(|| stats_session_error("system session is closed"))?;
        let mut conn = session.node_conn();
        callback(&mut conn)
    }

    fn materialize(
        &self,
        sql: &str,
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        self.with_session(|session| {
            let mut result = session
                .reborrow()
                .execute(sql)
                .map_err(|error| stats_session_error(error.message))?;
            let source = result.source();
            let columns = source.columns()?;
            let mut rows = Vec::new();
            loop {
                let batch = source.next_batch(1024)?;
                if batch.is_empty() {
                    break;
                }
                rows.extend(batch);
            }
            source.finish()?;
            source.close()?;
            Ok((rows, stats_result_fields(columns)))
        })
    }

    fn execute_auto_analyze(
        &self,
        sql: &str,
        options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        let option = tidb_sqlexec::exec_option(options);
        if option.analyze_ver != 2 || !option.use_cur_session {
            return Err(stats_session_error(
                "auto analyze requires statistics version 2 in the current session",
            ));
        }
        let track_guard = if let Some(track) = option.track_sys_proc.as_ref() {
            let process = self.with_session(|session| {
                session
                    .session
                    .system_process()
                    .ok_or_else(|| stats_session_error("system session has no process owner"))
            })?;
            track(option.track_sys_proc_id, Arc::new(process))?;
            option
                .untrack_sys_proc
                .clone()
                .map(|untrack| SysProcessTrackGuard {
                    process_id: option.track_sys_proc_id,
                    untrack,
                })
        } else {
            None
        };
        let result = self.with_session(|session| {
            let execution = SystemSessionExecution {
                connection_id: session.node.connection_id,
                session,
            };
            if option.track_sys_proc.is_some() {
                execution.session.node.connection_id = option.track_sys_proc_id;
                execution
                    .session
                    .session
                    .set_connection_id(option.track_sys_proc_id);
            }
            execution
                .session
                .run_auto_analyze_sql(sql, option.analyze_snapshot, &option.partition_prune_mode)
                .map_err(|error| stats_session_error(error.message))?;
            Ok((Vec::new(), Vec::new()))
        });
        drop(track_guard);
        result
    }
}

fn stats_result_fields(
    columns: Vec<tidb_protocol::ColumnInfo>,
) -> Vec<tidb_resolve::ResultFieldRef> {
    columns
        .into_iter()
        .map(|column| {
            let mut field_type = tidb_datatype::FieldType::new(
                tidb_datatype::FieldTypeCode::from_mysql_type(column.type_code),
            );
            field_type.set_flags(u32::from(column.flag));
            field_type.set_flen(i64::from(column.column_length));
            field_type.set_decimal(i64::from(column.decimal));
            field_type.set_collation_name(tidb_datatype::collation_id_to_name(i32::from(
                column.charset,
            )));
            let model_column = tidb_model::GoShared::new(tidb_model::ColumnInfo {
                name: tidb_ast::CiString::new(column.org_name.clone()),
                field_type,
                ..tidb_model::ColumnInfo::default()
            });
            tidb_model::GoShared::new(tidb_resolve::ResultField {
                column: Some(model_column),
                column_as_name: tidb_ast::CiString::new(column.name),
                empty_org_name: column.org_name.is_empty(),
                table_as_name: tidb_ast::CiString::new(column.table),
                db_name: tidb_ast::CiString::new(column.schema),
                ..tidb_resolve::ResultField::default()
            })
        })
        .collect()
}

struct ClusterStatsSqlExecutor {
    state: Arc<ClusterStatsSessionState>,
}

impl ClusterStatsSqlExecutor {
    fn materialized_record_set(
        &self,
        sql: &str,
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        let (rows, fields) = self.state.materialize(sql)?;
        Ok((!fields.is_empty()).then(|| {
            Box::new(tidb_sqlexec::SimpleRecordSet::new(fields, rows, 1024))
                as Box<dyn tidb_sqlexec::RecordSet>
        }))
    }
}

impl tidb_sqlexec::SqlExecutor for ClusterStatsSqlExecutor {
    fn execute(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        Ok(self.materialized_record_set(sql)?.into_iter().collect())
    }

    fn execute_internal(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_util::sqlescape::SqlArg<'_>],
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)?;
        let escaped = String::from_utf8(escaped)?;
        self.materialized_record_set(&escaped)
    }

    fn execute_stmt(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
    ) -> Result<Option<Box<dyn tidb_sqlexec::RecordSet>>, tidb_sqlexec::SqlExecError> {
        let sql = String::from_utf8(statement.original_text().to_vec())?;
        self.materialized_record_set(&sql)
    }
}

impl tidb_sqlexec::RestrictedSqlExecutor for ClusterStatsSqlExecutor {
    fn parse_with_params(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_util::sqlescape::SqlArg<'_>],
    ) -> Result<tidb_ast::Stmt, tidb_sqlexec::SqlExecError> {
        let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)?;
        let escaped = String::from_utf8(escaped)?;
        tidb_parser::parse(&escaped).map_err(|error| stats_session_error(format!("{error:?}")))
    }

    fn exec_restricted_stmt(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
        _options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        let sql = String::from_utf8(statement.original_text().to_vec())?;
        self.state.materialize(&sql)
    }

    fn exec_restricted_sql(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        options: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        arguments: &[tidb_util::sqlescape::SqlArg<'_>],
    ) -> Result<
        (
            Vec<Vec<tidb_datatype::Datum>>,
            Vec<tidb_resolve::ResultFieldRef>,
        ),
        tidb_sqlexec::SqlExecError,
    > {
        let escaped = tidb_util::sqlescape::escape_sql(sql, arguments)?;
        let escaped = String::from_utf8(escaped)?;
        if is_analyze_table_sql(&escaped) && !options.is_empty() {
            self.state.execute_auto_analyze(&escaped, options)
        } else {
            self.state.materialize(&escaped)
        }
    }
}

struct ClusterStatsSessionContext {
    state: Arc<ClusterStatsSessionState>,
}

impl ClusterStatsSessionContext {
    fn new(session: Session, global_vars: GlobalSysvars) -> Self {
        Self {
            state: Arc::new(ClusterStatsSessionState {
                session: Mutex::new(Some(session)),
                global_vars,
                registered: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    fn with_session<T>(
        &self,
        callback: impl FnOnce(&mut ClusterServerSession<'_>) -> Result<T, tidb_sqlexec::SqlExecError>,
    ) -> Result<T, tidb_sqlexec::SqlExecError> {
        self.state.with_session(callback)
    }

    fn set_var(&self, name: &str, value: impl Into<String>) {
        let value = value.into();
        self.with_session(
            |session| match session.session.set_internal_system_var(name, value) {
                Ok(()) => Ok(()),
                Err(error) if error.equal(&tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE) => {
                    Ok(())
                }
                Err(error) => Err(stats_session_error(format!("{error:?}"))),
            },
        )
        .expect("validated global variable must be valid in an internal session");
    }

    fn scalar(&self, sql: &str) -> Result<Option<tidb_datatype::Datum>, String> {
        let (rows, _) = self
            .state
            .materialize(sql)
            .map_err(|error| error.to_string())?;
        Ok(rows
            .into_iter()
            .next()
            .and_then(|mut row| (!row.is_empty()).then(|| row.remove(0))))
    }

}

impl tidb_stats_handle_autoanalyze_exec::AutoAnalyzeSessionContext for ClusterStatsSessionContext {
    fn partition_prune_mode(&self) -> String {
        <Self as tidb_stats_handle_util::StatsSessionContext>::partition_prune_mode(self)
    }

    fn enable_analyze_snapshot(&self) -> bool {
        self.with_session(|session| {
            session
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT)
                .map(|value| tidb_vardef::option_values::tidb_opt_on(&value))
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
        .unwrap_or(false)
    }
}

impl tidb_syssession::SessionContext for ClusterStatsSessionContext {
    fn close(&self) {
        self.state
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }

    fn rollback_txn(&self, _context: &dyn tidb_sqlexec::ExecutionContext) {
        let _ = self.with_session(|session| {
            session
                .execute_internal("ROLLBACK")
                .map_err(|error| stats_session_error(error.message))?;
            Ok(())
        });
    }

    fn has_prepared_txn_future(&self) -> bool {
        false
    }

    fn txn_valid(&self) -> Result<bool, tidb_sqlexec::SqlExecError> {
        self.with_session(|session| Ok(session.node.txn.explicit.is_some()))
    }

    fn sql_executor(&self) -> Arc<dyn tidb_sqlexec::SqlExecutor> {
        Arc::new(ClusterStatsSqlExecutor {
            state: Arc::clone(&self.state),
        })
    }

    fn restricted_sql_executor(&self) -> Arc<dyn tidb_sqlexec::RestrictedSqlExecutor> {
        Arc::new(ClusterStatsSqlExecutor {
            state: Arc::clone(&self.state),
        })
    }

    fn register_internal_session(&self) {
        self.state
            .registered
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn unregister_internal_session(&self) {
        self.state
            .registered
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    fn contains_internal_session(&self) -> bool {
        self.state
            .registered
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn store_internal_session(&self) -> bool {
        self.register_internal_session();
        true
    }
}

impl tidb_stats_handle_util::StatsSessionContext for ClusterStatsSessionContext {
    fn global_system_var(&self, name: &str) -> Result<String, tidb_sqlexec::SqlExecError> {
        self.state
            .global_vars
            .get(name)
            .map_err(|error| stats_session_error(format!("{error:?}")))
    }

    fn set_enable_async_merge_global_stats(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_MERGE_GLOBAL_STATS,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_analyze_partition_concurrency(&self, concurrency: i64) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ANALYZE_PARTITION_CONCURRENCY,
            concurrency.to_string(),
        );
    }

    fn set_analyze_version(&self, version: i64) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION,
            version.to_string(),
        );
    }

    fn set_analyze_store_batch_size(&self, value: &str) -> Result<(), tidb_sqlexec::SqlExecError> {
        self.with_session(|session| {
            match session.session.set_internal_system_var(
                tidb_vardef::tidb_vars::TIDB_ANALYZE_STORE_BATCH_SIZE,
                value,
            ) {
                Ok(()) => Ok(()),
                Err(error) if error.equal(&tidb_error::catalog::variable::ERR_GLOBAL_VARIABLE) => {
                    Ok(())
                }
                Err(error) => Err(stats_session_error(format!("{error:?}"))),
            }
        })
    }

    fn set_enable_historical_stats(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_partition_prune_mode(&self, mode: &str) {
        self.set_var(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE, mode);
    }

    fn partition_prune_mode(&self) -> String {
        self.with_session(|session| {
            session
                .session
                .vars()
                .get_system(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
        .unwrap_or_default()
    }

    fn set_enable_analyze_snapshot(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ENABLE_ANALYZE_SNAPSHOT,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_analyze_skip_column_types(&self, value: std::collections::BTreeSet<String>) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES,
            value.into_iter().collect::<Vec<_>>().join(","),
        );
    }

    fn set_skip_missing_partition_stats(&self, enabled: bool) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_SKIP_MISSING_PARTITION_STATS,
            if enabled { "ON" } else { "OFF" },
        );
    }

    fn set_lock_wait_timeout(&self, milliseconds: i64) {
        self.set_var(
            "innodb_lock_wait_timeout",
            (milliseconds / 1_000).to_string(),
        );
    }

    fn set_time_zone(&self, value: &str) -> Result<(), tidb_sqlexec::SqlExecError> {
        self.with_session(|session| {
            session
                .session
                .set_internal_system_var("time_zone", value)
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
    }

    fn location(&self) -> String {
        self.with_session(|session| {
            session
                .session
                .vars()
                .get_system("time_zone")
                .map_err(|error| stats_session_error(format!("{error:?}")))
        })
        .unwrap_or_else(|_| "SYSTEM".to_owned())
    }

    fn set_statement_time_zone(&self, _value: &str) {
        // Rust constructs its statement context from SessionVars at every
        // statement boundary, so set_time_zone above updates both authorities.
    }

    fn transaction_start_ts(&self, active: bool) -> Result<u64, tidb_sqlexec::SqlExecError> {
        self.with_session(|session| {
            if let Some(transaction) = session.node.txn.explicit.as_ref() {
                return Ok(transaction.start_ts());
            }
            if active {
                return Err(stats_session_error(
                    "system-session transaction has not been activated",
                ));
            }
            Ok(0)
        })
    }
}

struct ClusterPriorityQueueSource {
    factory: Weak<Domain>,
    stats_lease: Duration,
    session_pool: Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>>,
}

impl ClusterPriorityQueueSource {
    fn factory(&self) -> Result<Arc<Domain>, String> {
        self.factory
            .upgrade()
            .ok_or_else(|| "cluster session factory is stopped".to_owned())
    }

    fn scalar(&self, sql: &str) -> Result<Option<tidb_datatype::Datum>, String> {
        let mut value = None;
        tidb_stats_handle_util::call_with_sctx(
            self.session_pool.as_ref(),
            |context| {
                value = context
                    .scalar(sql)
                    .map_err(|error| stats_session_error(error))?;
                Ok(())
            },
            &[],
        )
        .map_err(|error| error.to_string())?;
        Ok(value)
    }
}

impl tidb_stats_handle_autoanalyze_priorityqueue::PriorityQueueSource
    for ClusterPriorityQueueSource
{
    fn next_check_version_with_offset(&self) -> u64 {
        self.factory()
            .map(|factory| {
                factory
                    .stats
                    .next_check_version_with_offset(self.stats_lease)
            })
            .unwrap_or(0)
    }

    fn queue_inventory(
        &self,
    ) -> Result<tidb_stats_handle_autoanalyze_priorityqueue::QueueInventory, String> {
        use tidb_stats_handle_autoanalyze_priorityqueue::{
            InventoryTable, PartitionIdAndName, PartitionPruneMode, QueueInventory,
        };

        let factory = self.factory()?;
        let catalog = factory.catalog.load();
        let statistics = factory.stats.load();
        let snapshot = factory.transactions.open_snapshot("default")?;
        let current_ts = snapshot.start_ts();
        let locked_table_ids = {
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
            tidb_exec::cluster_stats_write::load_stats_locked_table_ids(&mut snapshot, &catalog)
                .map_err(|error| error.to_string())?
        };
        let ratio = factory
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO)
            .map_err(|error| format!("{error:?}"))?;
        let prune_mode = factory
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .map_err(|error| format!("{error:?}"))?;
        let requested_version = factory
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION)
            .map_err(|error| format!("{error:?}"))?
            .parse::<i32>()
            .map_err(|error| error.to_string())?;
        let mut tables = Vec::new();
        for database in &catalog.databases {
            for table in &database.tables {
                let global_stats = statistics
                    .get(&table.id)
                    .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                    .map(|stats| stats.as_ref().clone());
                let mut partition_stats = std::collections::HashMap::new();
                if let Some(partition) = table.get_partition_info() {
                    for definition in partition.read().definitions.snapshot() {
                        if let Some(stats) = statistics
                            .get(&definition.id)
                            .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                        {
                            partition_stats.insert(
                                PartitionIdAndName::new(definition.name.original(), definition.id),
                                stats.as_ref().clone(),
                            );
                        }
                    }
                }
                tables.push(InventoryTable {
                    schema_name: database.info.name.original().to_owned(),
                    table: Arc::new(table.clone()),
                    global_stats,
                    partition_stats,
                });
            }
        }
        Ok(QueueInventory {
            tables,
            locked_table_ids,
            prune_mode: if prune_mode.eq_ignore_ascii_case("static") {
                PartitionPruneMode::Static
            } else {
                PartitionPruneMode::Dynamic
            },
            auto_analyze_ratio: tidb_stats_handle_autoanalyze_exec::parse_auto_analyze_ratio(
                &ratio,
            ),
            requested_version,
            current_ts,
            auto_analyze_min_count: tidb_stats::DEFAULT_AUTO_ANALYZE_MIN_COUNT,
        })
    }
}

impl tidb_stats_handle_autoanalyze_priorityqueue::AnalysisJobContext
    for ClusterPriorityQueueSource
{
    fn lookup_table(
        &self,
        table_id: i64,
    ) -> tidb_stats_handle_autoanalyze_priorityqueue::TableLookup {
        let Ok(factory) = self.factory() else {
            return tidb_stats_handle_autoanalyze_priorityqueue::TableLookup::TableMissing;
        };
        let catalog = factory.catalog.load();
        for database in &catalog.databases {
            if let Some(table) = database.tables.iter().find(|table| table.id == table_id) {
                return tidb_stats_handle_autoanalyze_priorityqueue::TableLookup::Found {
                    schema_name: database.info.name.original().to_owned(),
                    table: Arc::new(table.clone()),
                };
            }
        }
        tidb_stats_handle_autoanalyze_priorityqueue::TableLookup::TableMissing
    }

    fn last_failed_analysis_duration(
        &self,
        schema: &str,
        table: &str,
        partitions: &[String],
    ) -> Result<i64, String> {
        let schema = sql_string_literal(schema);
        let table = sql_string_literal(table);
        let sql = if partitions.is_empty() {
            format!(
                "SELECT TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP) FROM mysql.analyze_jobs WHERE table_schema = {schema} AND table_name = {table} AND state = 'failed' AND partition_name = '' ORDER BY id DESC LIMIT 1"
            )
        } else {
            let partitions = partitions
                .iter()
                .map(|partition| sql_string_literal(partition))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "SELECT MIN(TIMESTAMPDIFF(SECOND, aj.start_time, CURRENT_TIMESTAMP)) AS min_duration FROM (SELECT MAX(id) AS max_id FROM mysql.analyze_jobs WHERE table_schema = {schema} AND table_name = {table} AND state = 'failed' AND partition_name IN ({partitions}) GROUP BY partition_name) AS latest_failures JOIN mysql.analyze_jobs aj ON aj.id = latest_failures.max_id"
            )
        };
        match self.scalar(&sql)? {
            None | Some(tidb_datatype::Datum::Null) => Ok(
                tidb_stats_handle_autoanalyze_priorityqueue::last_failed_analysis_duration(None),
            ),
            Some(tidb_datatype::Datum::Int(seconds)) => Ok(
                tidb_stats_handle_autoanalyze_priorityqueue::last_failed_analysis_duration(Some(
                    seconds,
                )),
            ),
            Some(value) => Err(format!("invalid last failed analysis duration {value:?}")),
        }
    }

    fn average_analysis_duration(
        &self,
        schema: &str,
        table: &str,
        partitions: &[String],
    ) -> Result<i64, String> {
        let schema = sql_string_literal(schema);
        let table = sql_string_literal(table);
        let predicate = if partitions.is_empty() {
            "partition_name = ''".to_owned()
        } else {
            let partitions = partitions
                .iter()
                .map(|partition| sql_string_literal(partition))
                .collect::<Vec<_>>()
                .join(",");
            format!("partition_name IN ({partitions})")
        };
        let sql = format!(
            "SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration FROM (SELECT start_time, end_time FROM mysql.analyze_jobs WHERE table_schema = {schema} AND table_name = {table} AND state = 'finished' AND fail_reason IS NULL AND {predicate} ORDER BY id DESC LIMIT 5) AS recent_analyses"
        );
        match self.scalar(&sql)? {
            None | Some(tidb_datatype::Datum::Null) => {
                Ok(tidb_stats_handle_autoanalyze_priorityqueue::average_analysis_duration(None))
            }
            Some(value) => {
                let seconds = value.to_f64().map_err(|error| error.to_string())?.value;
                Ok(
                    tidb_stats_handle_autoanalyze_priorityqueue::average_analysis_duration(Some(
                        seconds,
                    )),
                )
            }
        }
    }

    fn auto_analyze(
        &self,
        stats_version: i32,
        need_version_rewrite_warning: bool,
        sql: &str,
        arguments: &[String],
    ) -> bool {
        let mut analyzed = false;
        tidb_stats_handle_util::call_with_sctx(
            self.session_pool.as_ref(),
            |context| {
                // Go hands the stats handle `Domain.NextConnID` and
                // `ReleaseConnID` as its auto-analyze process-ID generator, so
                // the ID names this node's server ID and `KILL` routes it here.
                let factory = self.factory().map_err(stats_session_error)?;
                let ids = factory.connection_id_allocator();
                let released = Arc::clone(&ids);
                let generator = tidb_stats_handle_util::Generator::new(
                    move || ids.next_id(),
                    move |id| released.release(id),
                );
                let (track, untrack) = system_process_trackers(factory.processes());
                let arguments = arguments
                    .iter()
                    .map(|argument| tidb_util::sqlescape::SqlArg::from(argument.as_str()))
                    .collect::<Vec<_>>();
                analyzed = tidb_stats_handle_autoanalyze_exec::auto_analyze(
                    context,
                    &generator,
                    track,
                    untrack,
                    stats_version,
                    need_version_rewrite_warning,
                    sql,
                    &arguments,
                );
                Ok(())
            },
            &[],
        )
        .is_ok()
            && analyzed
    }

    fn auto_analyze_partition_batch_size(&self) -> usize {
        self.factory()
            .ok()
            .and_then(|factory| {
                factory
                    .global_vars
                    .get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_PARTITION_BATCH_SIZE)
                    .ok()
            })
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_AUTO_ANALYZE_PARTITION_BATCH_SIZE as usize)
    }
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

pub struct WorkloadRepositorySessionPool {
    factory: std::sync::Weak<Domain>,
    next_connection_id: std::sync::atomic::AtomicU64,
}

impl WorkloadRepositorySessionPool {
    pub fn new(factory: &Arc<Domain>) -> Self {
        Self {
            factory: Arc::downgrade(factory),
            next_connection_id: std::sync::atomic::AtomicU64::new(1_u64 << 62),
        }
    }
}

impl tidb_workloadrepo::SessionPool for WorkloadRepositorySessionPool {
    fn get(&self) -> Result<Box<dyn tidb_workloadrepo::RepositorySession>, String> {
        use std::sync::atomic::Ordering;

        let factory = self
            .factory
            .upgrade()
            .ok_or_else(|| "cluster session factory is stopped".to_owned())?;
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let session = factory
            .open_session(SessionContext {
                connection_id,
                peer_addr: crate::domain::client_socket::PeerAddress::Tcp(
                "127.0.0.1:0".parse().expect("loopback socket address"),
            ),
                identity: crate::domain::configured_user_store::AuthenticatedIdentity::internal(),
                client_found_rows: false,
                tls_state: None,
                compression_algorithm: tidb_mysql::consts::CompressionNone,
                compression_level: 0,
                cancellation: crate::domain::session_api::ConnectionCancellation::default(),
                close: crate::domain::session_api::ConnectionClose::default(),
            })
            .map_err(|error| error.message)?;
        Ok(Box::new(WorkloadRepositorySession { session }))
    }
}

struct WorkloadRepositorySession {
    session: Session,
}

fn workload_sql_literal(value: &tidb_workloadrepo::SqlArg) -> String {
    match value {
        tidb_workloadrepo::SqlArg::Null => "NULL".to_owned(),
        tidb_workloadrepo::SqlArg::UInt(value) => value.to_string(),
        tidb_workloadrepo::SqlArg::String(value) => {
            format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
        }
    }
}

fn bind_workload_sql(sql: &str, args: &[tidb_workloadrepo::SqlArg]) -> Result<String, String> {
    let mut rendered = sql.to_owned();
    for argument in args {
        let Some(index) = rendered.find("%?") else {
            return Err("too many workload repository SQL arguments".to_owned());
        };
        rendered.replace_range(index..index + 2, &workload_sql_literal(argument));
    }
    if rendered.contains("%?") {
        return Err("not enough workload repository SQL arguments".to_owned());
    }
    Ok(rendered)
}

fn workload_arg_from_datum(
    value: tidb_datatype::Datum,
) -> Result<tidb_workloadrepo::SqlArg, String> {
    match value {
        tidb_datatype::Datum::Null => Ok(tidb_workloadrepo::SqlArg::Null),
        tidb_datatype::Datum::UInt(value) => Ok(tidb_workloadrepo::SqlArg::UInt(value)),
        tidb_datatype::Datum::Int(value) => u64::try_from(value)
            .map(tidb_workloadrepo::SqlArg::UInt)
            .map_err(|_| "negative integer in workload repository metadata".to_owned()),
        tidb_datatype::Datum::Bytes(value) | tidb_datatype::Datum::Raw(value) => {
            String::from_utf8(value)
                .map(tidb_workloadrepo::SqlArg::String)
                .map_err(|error| error.to_string())
        }
        tidb_datatype::Datum::String(value) => value
            .as_utf8()
            .map(|value| tidb_workloadrepo::SqlArg::String(value.to_owned()))
            .map_err(|error| error.to_string()),
        other => Err(format!(
            "unsupported workload repository result value {other:?}"
        )),
    }
}

impl tidb_workloadrepo::RepositorySession for WorkloadRepositorySession {
    fn execute(
        &mut self,
        sql: &str,
        args: &[tidb_workloadrepo::SqlArg],
    ) -> Result<Vec<Vec<tidb_workloadrepo::SqlArg>>, String> {
        let sql = bind_workload_sql(sql, args)?;
        self.session
            .node_conn()
            .execute_internal(&sql)
            .map_err(|error| error.message)?
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(workload_arg_from_datum)
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect()
    }

    fn schema_exists(&self, schema: &str) -> bool {
        self.session
            .shared_catalog()
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .has_database(schema)
    }

    fn table_info(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<tidb_workloadrepo::TableInfo, String> {
        let catalog = self.session.shared_catalog();
        let catalog = catalog.lock().unwrap_or_else(|error| error.into_inner());
        let entry = catalog
            .table_in(schema, table)
            .ok_or_else(|| format!("table `{schema}`.`{table}` does not exist"))?;
        let columns = match entry {
            tidb_executor::TableEntry::Kv(table) => table
                .visible_columns()
                .iter()
                .map(|column| tidb_workloadrepo::Column {
                    name: column.name.clone(),
                    type_desc: column
                        .field_type
                        .type_desc(tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH),
                    comment: column.comment.clone(),
                })
                .collect(),
            _ => entry
                .column_types()
                .into_iter()
                .map(|(name, field_type)| tidb_workloadrepo::Column {
                    name,
                    type_desc: field_type.type_desc(tidb_datatype::STRICT_INTEGER_DISPLAY_WIDTH),
                    comment: String::new(),
                })
                .collect(),
        };
        let partitions = match entry {
            tidb_executor::TableEntry::Kv(table) => table
                .partition()
                .map(|partition| {
                    partition
                        .definitions
                        .iter()
                        .map(|definition| definition.name.clone())
                        .collect()
                })
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        Ok(tidb_workloadrepo::TableInfo {
            columns,
            partitions,
        })
    }
}

struct InternalBindingWriter {
    factory: Weak<Domain>,
    bindings: Arc<dyn crate::domain::cluster_binding_seam::ClusterBindings>,
    connection_id: u64,
}

impl crate::binding_arm::GlobalBindingWriter for InternalBindingWriter {
    fn execute(
        &self,
        operation: &mut dyn FnMut(&mut Session) -> Result<u64, tidb_executor::DriverError>,
    ) -> Result<u64, tidb_executor::DriverError> {
        let to_driver = |error: SqlQueryError| {
            tidb_executor::DriverError::Mysql(
                tidb_executor::MysqlError::from_parts(error.code, error.state, error.message)
                    .with_identity_opt(error.identity),
            )
        };
        let factory = self.factory.upgrade().ok_or_else(|| {
            tidb_executor::DriverError::unsupported("global binding session factory is stopped")
        })?;
        let mut internal_session = factory
            .open_storage_session(self.connection_id)
            .map_err(to_driver)?;
        let mut internal = internal_session.node_conn();
        internal
            .session
            .set_global_binding_cache(self.bindings.cache());
        internal.session.enable_privilege_bypass();
        internal
            .execute_internal("BEGIN PESSIMISTIC")
            .map_err(to_driver)?;
        let result = (|| {
            // Use Go's shared record-lock statement so binding writers on
            // different nodes serialize through the same storage transaction.
            internal.execute_internal(crate::binding_utils::LOCK_BIND_INFO_SQL)?;
            let resource_group = internal.session.current_resource_group().to_owned();
            let changed = internal.with_prelocked_statement(
                StatementReadShape::LockingRead,
                &resource_group,
                true,
                |session| operation(session).map_err(map_error),
            )?;
            internal.execute_internal("COMMIT")?;
            Ok(changed)
        })();
        match result {
            Ok(changed) => {
                // Do not replay a successful commit if cache refresh fails.
                if let Err(error) = self.bindings.reload() {
                    eprintln!("committed binding cache reload failed: {error}");
                }
                Ok(changed)
            }
            Err(error) => {
                let _ = internal.execute_internal("ROLLBACK");
                Err(to_driver(error))
            }
        }
    }
}

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
    /// [`QuerySession::execute_stmt`] answered without a result set.
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
    pub(crate) fn node_conn(&mut self) -> ClusterServerSession<'_> {
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

    fn execute_timed(self, sql: &str) -> Result<QueryResult<'s>, SqlQueryError> {
        let stmt = (*self.session).parse_statement(sql).map_err(|error| {
            self.session.record_parse_failure(&error);
            map_error(error)
        })?;
        self.execute_parsed(sql, &stmt)
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
        // A transaction statement runs as the connection runs it.
        let outcome = if classify_transaction_control_stmt(&inner).is_some() {
            self.control_transaction_parsed(&inner_sql, &inner)
                .map(|_| {
                    Some(WriteOutcome {
                        affected_rows: 0,
                        last_insert_id: 0,
                    })
                })
        } else {
            self.execute_write_parsed(&inner_sql, &inner)
        };
        match outcome {
            Ok(Some(outcome)) => affected = outcome.affected_rows,
            Ok(None) => match self.reborrow().execute_parsed(&inner_sql, &inner) {
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
        let columns = crate::domain::pipeline_session::select_columns(&columns);
        Ok(QueryResult::new(Box::new(
            crate::domain::pipeline_session::MaterializedResultSetSource::new(columns, result_rows),
        )))
    }

    /// The unguarded rebuild: `BEGIN` calls this directly because the session
    /// already reports in-transaction by the time the transaction opens, and
    /// Go pins the LATEST schema at transaction start
    /// (`domain.GetSnapshotInfoSchema(startTS)`) — a table committed before
    /// `BEGIN` is visible to every statement of the new transaction.
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

    /// Go `DDLExec.Next` after its `NewTxnInStmt` (`executor/ddl.go:151`):
    /// the DDL executor's checks against this node's catalog and the
    /// statement lowered to the job(s) the cluster runs. Every refusal here
    /// comes after the implicit commit, as Go's do.
    fn lower_ddl_exec(&mut self, stmt: &Stmt) -> Result<LoweredDdl, SqlQueryError> {
        // Go `DDLExec.executeCleanupTableLock` after the planner's
        // SUPER check: the `IsCleanup` unlock job, none when no named
        // table is locked.
        if let Stmt::Ddl(ddl) = stmt {
            if let tidb_ast::DdlStmt::CleanupTableLock(cleanup) = ddl.as_ref() {
                let statement = self
                    .session
                    .cluster_cleanup_table_lock_statement(&cleanup.tables)
                    .map_err(map_error)?;
                return Ok(LoweredDdl::one(statement.unwrap_or(
                    DdlStatement::AcceptedNoOp {
                        schema: String::new(),
                        table: String::new(),
                        warning: None,
                    },
                )));
            }
        }
        // Go `DDLExec.Next`: the DDL the session answers runs there;
        // a DROP that also names LOCAL temporary tables runs the job
        // for the rest and then drops them.
        if let Stmt::Ddl(ddl) = stmt {
            match self.session.ddl_exec_next(ddl) {
                crate::ddl_exec::DdlExecNext::Job => {}
                crate::ddl_exec::DdlExecNext::DropTableJob { persistent, local } => {
                    let persistent = Stmt::Ddl(tidb_ast::NodeBox::new(
                        tidb_ast::DdlStmt::DropTable(Box::new(persistent)),
                    ));
                    let context = self.session.ddl_statement_context();
                    let ddl_context: tidb_ddl::cluster_ddl::DdlContext =
                        std::sync::Arc::new(context.clone());
                    let statement = prepare_cluster_ddl_parsed(
                        &persistent,
                        self.session.current_database(),
                        &ddl_context,
                    )
                    .map_err(|error| SqlQueryError::from(error.to_mysql_error()))?
                    .ok_or_else(|| {
                        SqlQueryError::unknown(
                            "DROP TABLE has no cluster metadata operation",
                        )
                    })?;
                    return Ok(LoweredDdl::DropTableJob { statement, local });
                }
                _ => unreachable!("the router sends session DDL to `run_session_ddl`"),
            }
        }
        // The DDL executor's target checks; the ALTER spec walk's own
        // refusals are the lowering's (`cluster_ddl`).
        self.session.ddl_target_checks(stmt).map_err(map_error)?;
        // Go `DDLExec.executeLockTables`/`executeUnlockTables` with
        // `enable-table-lock` on: the planner's privilege check, then
        // `executor.LockTables`' checks against this node's catalog
        // build the lock job; UNLOCK with nothing held submits none.
        if tidb_config::config_tree::config::table_lock_enabled() {
            if let tidb_ast::Stmt::Ddl(ddl) = stmt {
                match &**ddl {
                    tidb_ast::DdlStmt::LockTables(locks) => {
                        return self
                            .session
                            .cluster_lock_tables_statement(locks)
                            .map(LoweredDdl::one)
                            .map_err(map_error);
                    }
                    tidb_ast::DdlStmt::UnlockTables => {
                        return Ok(LoweredDdl::one(
                            self.session.cluster_unlock_tables_statement().unwrap_or(
                                DdlStatement::AcceptedNoOp {
                                    schema: String::new(),
                                    table: String::new(),
                                    warning: None,
                                },
                            ),
                        ));
                    }
                    _ => {}
                }
            }
        }
        // go `SimpleExec.executeLockTables`/`executeUnlockTables`:
        // with the experimental table lock disabled
        // (`enable-table-lock=false`), both statements answer OK
        // with Warning 1235 pointing at the configuration
        // (`pkg/ddl/tests/serial/serial_test.go:1057`).
        if let tidb_ast::Stmt::Ddl(ddl) = stmt {
            let notice = match &**ddl {
                // Go's preprocessor resolves every named table, each
                // under its own schema, before the warning: the first
                // missing one is 1146 and no current database 1046.
                tidb_ast::DdlStmt::LockTables(locks) => {
                    for lock in locks.iter() {
                        self.session
                            .require_table_exists(&lock.table)
                            .map_err(map_error)?;
                    }
                    Some((
                        String::new(),
                        String::new(),
                        "LOCK TABLES is not supported. To enable this experimental \
                         feature, set 'enable-table-lock' in the configuration file."
                            .to_owned(),
                    ))
                }
                tidb_ast::DdlStmt::UnlockTables => Some((
                    String::new(),
                    String::new(),
                    "UNLOCK TABLES is not supported. To enable this experimental \
                     feature, set 'enable-table-lock' in the configuration file."
                        .to_owned(),
                )),
                _ => None,
            };
            if let Some((schema, table, message)) = notice {
                return Ok(LoweredDdl::one(DdlStatement::AcceptedNoOp {
                    schema,
                    table,
                    warning: Some((1235, message)),
                }));
            }
        }
        // Go `executor.AddResourceGroup` / `AlterResourceGroup` /
        // `DropResourceGroup` check against the session's schema
        // before the job; an `IF [NOT] EXISTS` that suppressed its
        // error submits none.
        if let Stmt::Ddl(ddl) = stmt {
            if matches!(
                &**ddl,
                tidb_ast::DdlStmt::CreateResourceGroup(_)
                    | tidb_ast::DdlStmt::AlterResourceGroup(_)
                    | tidb_ast::DdlStmt::DropResourceGroup(_)
            ) {
                // DROP reads `mysql.user` for an account bound to the
                // group (Go `MatchUserResourceGroupName`), so the
                // admission reads through a statement snapshot.
                let resource_group = self.session.current_resource_group().to_owned();
                let statement = self.probe_statement(
                    StatementReadShape::Unknown,
                    &resource_group,
                    |session| {
                        session
                            .cluster_resource_group_statement(ddl)
                            .map_err(map_error)
                    },
                )?;
                return Ok(LoweredDdl::one(statement.unwrap_or(
                    DdlStatement::AcceptedNoOp {
                        schema: String::new(),
                        table: String::new(),
                        warning: None,
                    },
                )));
            }
        }
        // A CREATE VIEW resolves its body against this node's own
        // catalog FIRST — a bad body fails here, at CREATE time,
        // exactly where Go's `executeCreateView` preprocess fails —
        // and the finished definition becomes the published
        // `TableInfo`.
        // The body's meta run reads through cluster storage, so it
        // needs the same statement-snapshot lifecycle the PREPARE
        // probe uses — work the client did not ask to run, reading
        // at a fresh timestamp and unbinding afterwards.
        let resource_group = self.session.current_resource_group().to_owned();
        let resolved = self.probe_statement(
            StatementReadShape::Unknown,
            &resource_group,
            |session| session.resolve_cluster_view_parsed(stmt).map_err(map_error),
        )?;
        if let Some((database, name, or_replace, view)) = resolved {
            let info = tidb_executor::view::build_view_table_info(&name, &view);
            return Ok(LoweredDdl::one(DdlStatement::CreateView {
                schema: database,
                name,
                or_replace,
                info: Box::new(info),
            }));
        }
        let context = self.session.ddl_statement_context();
        let ddl_context: tidb_ddl::cluster_ddl::DdlContext = std::sync::Arc::new(context.clone());
        match prepare_cluster_ddl_parsed(
            stmt,
            self.session.current_database(),
            &ddl_context,
        ) {
            Ok(Some(statement)) => {
                // Warnings the LOWERING produced (an ignored CHECK
                // constraint, Go ddl/create_table.go:1470) belong to
                // this statement; Go appends them to the session's
                // own context, so drain the lowering context's here.
                self.session.begin_routed_statement_warnings();
                self.session.drain_context_warnings(&context);
                Ok(LoweredDdl::one(statement))
            }
            Ok(None) => {
                if let Some(parts) = job_sequence_parts(stmt) {
                    self.session.begin_routed_statement_warnings();
                    self.session.drain_context_warnings(&context);
                    return Ok(LoweredDdl::Sequence { parts, context });
                }
                // Go's executor refuses a cached table before any
                // shape this node does not plan.
                tidb_ddl::cluster_ddl::cached_table::check_cached_table_ddl(
                    &self.node.domain.catalog.load(),
                    self.session.current_database(),
                    stmt,
                )
                .map_err(|error| SqlQueryError::from(error))?;
                Err(unplanned_ddl())
            }
            // The refusal carries Go's own errno where it has one
            // (`Unsupported ...` is 8200), so a client can tell a
            // shape this server will not do from an internal failure.
            // Warnings the lowering raised BEFORE the refusal (a
            // 1681 display-width warning beside an 8216 auto-random
            // refusal) survive it: go's failed DDL leaves both in
            // the warning buffer.
            Err(refusal) => {
                self.session.begin_routed_statement_warnings();
                self.session.drain_context_warnings(&context);
                tidb_ddl::cluster_ddl::cached_table::check_cached_table_ddl(
                    &self.node.domain.catalog.load(),
                    self.session.current_database(),
                    stmt,
                )
                .map_err(|error| SqlQueryError::from(error))?;
                Err(SqlQueryError::from(refusal.to_mysql_error()))
            }
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

    /// One part of a [`LoweredDdl::Sequence`], lowered by the ordinary path
    /// when its turn comes; its warnings join the statement's.
    fn lower_sequence_part(
        &mut self,
        part: &Stmt,
        context: &tidb_executor::StmtContext,
    ) -> Result<DdlStatement, SqlQueryError> {
        let ddl_context: tidb_ddl::cluster_ddl::DdlContext = Arc::new(context.clone());
        let lowered =
            prepare_cluster_ddl_parsed(part, self.session.current_database(), &ddl_context);
        self.session.drain_context_warnings(context);
        match lowered {
            Ok(Some(statement)) => Ok(statement),
            Ok(None) => Err(unplanned_ddl()),
            Err(refusal) => Err(SqlQueryError::from(refusal.to_mysql_error())),
        }
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

    /// Go `DDLExec.Next` for a DDL that runs as a job: `Compile` checked the
    /// plan and its privileges and `ExecStmt` the `tidb_snapshot` write while
    /// this connection's transaction was intact; `NewTxnInStmt` then commits
    /// it, and only then does the DDL executor check the statement against
    /// the catalog, lower it and run its job(s) -- a refusal from here on
    /// leaves the earlier writes committed, as Go's does.
    fn run_ddl_exec(&mut self, sql: &str, parsed: &Stmt) -> Result<WriteOutcome, SqlQueryError> {
        self.session
            .begin_ddl_statement(parsed)
            .map_err(map_error)?;
        // Go `DDLExec.Next`'s `NewTxnInStmt` (`executor/ddl.go:151`).
        self.new_txn_in_stmt()?;
        self.record_new_txn_in_stmt();
        match self.lower_ddl_exec(parsed)? {
            LoweredDdl::Jobs(statements) => {
                for statement in &statements {
                    self.run_ddl_job(sql, parsed, statement)?;
                }
            }
            LoweredDdl::Sequence { parts, context } => {
                for part in &parts {
                    let statement = self.lower_sequence_part(part, &context)?;
                    self.run_ddl_job(sql, parsed, &statement)?;
                }
            }
            LoweredDdl::DropTableJob { statement, local } => {
                self.run_ddl_job(sql, parsed, &statement)?;
                self
                    .session
                    .drop_local_temporary_tables(&local)
                    .map_err(map_error)?;
            }
        }
        // Go answers a DDL with an OK packet carrying no rows and no insert
        // id, whether it changed anything or was an IF [NOT] EXISTS no-op.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// One job through the DDL executor (`ddlExecutor.DoDDLJob`).
    fn run_ddl_job(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        statement: &DdlStatement,
    ) -> Result<(), SqlQueryError> {
        if let DdlStatement::ModifySchemaSetTiFlashReplica {
            name,
            count,
            labels,
        } = statement
        {
            return self.modify_schema_set_tiflash_replica(sql, parsed, name, *count, labels);
        }
        // The session owns the rest of the DDL path; this node installed
        // the service that publishes to the cluster.
        let service = Arc::clone(&self.domain_services().ddl);
        let created = self
            .session
            .run_ddl_statement(sql, parsed, statement, service.as_ref())
            .map_err(map_error)?;
        // Go's executor pre-splits the table a CREATE TABLE published and
        // the new identity `onTruncateTable` gave.
        if matches!(
            statement,
            DdlStatement::CreateTable { .. }
                | DdlStatement::CreateTableLike { .. }
                | DdlStatement::TruncateTable { .. }
        ) {
            self.pre_split_created_tables(created.as_slice());
        }
        Ok(())
    }

    /// Go `ModifySchemaSetTiFlashReplica`: an `ActionSetTiFlashReplica` job
    /// for each table of the database whose replica changes. A table TiFlash
    /// cannot carry is a note and is skipped; a failed job is counted, not
    /// returned; every 100 jobs (and after a timed-out wait) the loop waits
    /// for the unavailable replicas to drop below
    /// `tidb_batch_pending_tiflash_count`; a killed session stops it. The
    /// totals are the statement's message.
    fn modify_schema_set_tiflash_replica(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        name: &str,
        count: u64,
        labels: &[String],
    ) -> Result<(), SqlQueryError> {
        const CHECK_PENDING_TABLES_LIMIT: usize = 100;
        let catalog = self.node.domain.catalog.load();
        let (schema, tables) =
            tidb_ddl::cluster_ddl::alter_schema::schema_tiflash_replica_tables(&catalog, name)
                .map_err(|error| map_error(crate::ddl_exec::ddl_error(&error)))?;
        let ddl = Arc::clone(&self.domain_services().ddl);
        tidb_ddl::cluster_ddl::alter_table::check_tiflash_replica_count(count, &|| {
            ddl.tiflash_store_count()
        })
        .map_err(|error| map_error(crate::ddl_exec::ddl_error(&error)))?;
        let total = tables.len();
        let (mut succeeded, mut failed, mut skipped, mut one_fail) = (0_usize, 0_usize, 0_usize, 0_i64);
        let threshold = self
            .session
            .vars()
            .get_system(tidb_vardef::tidb_vars::TIDB_BATCH_PENDING_TIFLASH_COUNT)
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(4000);
        let killer = self.session.sql_killer();
        let (mut origin_version, mut pending_count, mut force_check) = (0_i64, 0_u32, false);
        for table in tables {
            if tidb_ddl_build::ddl_executor::is_session_done(&killer).0 {
                return Ok(());
            }
            let current = table
                .tiflash_replica
                .as_ref()
                .map(|replica| replica.read().clone());
            if !tidb_ddl_build::ddl_executor::should_modify_tiflash_replica(
                current.as_ref(),
                count,
                labels,
            ) {
                skipped += 1;
                continue;
            }
            if let Err(error) = tidb_ddl_build::ddl_executor::is_table_tiflash_supported(&schema, table)
            {
                self.session.append_warning(
                    tidb_util::context::WarningLevel::Note,
                    error.code,
                    error.message,
                );
                skipped += 1;
                continue;
            }
            if (succeeded + failed) % CHECK_PENDING_TABLES_LIMIT == 0 || force_check {
                let finished;
                (finished, origin_version, pending_count, force_check) = self
                    .wait_pending_tiflash_table_threshold(
                        &killer,
                        origin_version,
                        pending_count,
                        threshold,
                    );
                if finished {
                    return Ok(());
                }
            }
            let job = DdlStatement::SetTiFlashReplica {
                schema: schema.clone(),
                table: table.name.original().to_owned(),
                count,
                labels: labels.to_vec(),
                reset_available: false,
                hypo: false,
            };
            match self.run_ddl_job(sql, parsed, &job) {
                Ok(()) => succeeded += 1,
                Err(_) => {
                    one_fail = table.id;
                    failed += 1;
                }
            }
        }
        let fail_stmt = if failed > 0 {
            format!("(including table {one_fail})")
        } else {
            String::new()
        };
        self.session.set_statement_message(format!(
            "In total {total} tables: {succeeded} succeed, {failed} failed{fail_stmt}, {skipped} skipped"
        ));
        Ok(())
    }

    /// Go `waitPendingTableThreshold`: up to seven checks, three seconds
    /// apart, of the unavailable TiFlash replicas in the latest schema
    /// (`getPendingTiFlashTableCount`, recounted only on a new schema
    /// version). Answers whether the session is done, the version and count
    /// seen, and whether the wait timed out (a forced check next time).
    fn wait_pending_tiflash_table_threshold(
        &self,
        killer: &tidb_util::sqlkiller::SqlKiller,
        mut origin_version: i64,
        mut pending_count: u32,
        threshold: u32,
    ) -> (bool, i64, u32, bool) {
        const RETRY: usize = 7;
        const WAIT: std::time::Duration = std::time::Duration::from_millis(3000);
        for _ in 0..RETRY {
            if tidb_ddl_build::ddl_executor::is_session_done(killer).0 {
                return (true, origin_version, pending_count, false);
            }
            let latest = self.node.domain.catalog.load();
            if latest.schema_version != origin_version {
                origin_version = latest.schema_version;
                pending_count =
                    tidb_ddl::cluster_ddl::alter_schema::pending_tiflash_table_count(&latest);
            }
            if pending_count < threshold {
                return (false, origin_version, pending_count, false);
            }
            std::thread::sleep(WAIT);
        }
        (false, origin_version, pending_count, true)
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

    /// Executes DDL whose metadata belongs to this connection rather than to
    /// the cluster catalog.
    fn run_session_ddl(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
    ) -> Result<WriteOutcome, SqlQueryError> {
        // Go returns before NewTxnInStmt for local CREATE and local-only DROP.
        // Check before committing the cluster transaction (the staleness
        // check preceded `Compile`, which checked its privileges);
        // Session::run then owns the session-local table operation and
        // observation, its compile taking the node's plan.
        self.session
            .validate_snapshot_write(parsed)
            .map_err(map_error)?;
        if !self.session.ddl_preserves_transaction(parsed) {
            self.new_txn_in_stmt()?;
        }
        match self
            .session
            .exec_statement_parsed(parsed.clone(), sql)
            .map_err(map_error)?
        {
            StmtOutput::Affected(affected_rows) => Ok(WriteOutcome {
                affected_rows,
                last_insert_id: self.session.statement_insert_id(),
            }),
            StmtOutput::Done(_) => Ok(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }),
            StmtOutput::Rows { .. } => Err(SqlQueryError::unknown(
                "LOCAL temporary-table DDL unexpectedly returned rows",
            )),
        }
    }
}

impl QuerySession for Session {
    fn migration_session(&mut self) -> Option<&mut crate::Session> {
        Some(self)
    }

    fn record_parse_failure(&mut self, error: &SqlQueryError) {
        self.record_parse_failure_coded(error.code, error.message.clone(), error.identity);
    }

    fn metrics_resource_group(&self) -> &str {
        self.current_resource_group()
    }
    fn metrics_statement_resource_group(&self) -> &str {
        self.active_resource_group()
    }

    /// Go `handleQuery`'s one `ParseSQL`: the connection parses each
    /// statement of the command here and every door below reads that node.
    /// The parse sits on the statement boundary (go `ResetContextOfStmt`):
    /// the previous statement's warnings go, unless this statement is the
    /// one that reports them. Parsing through the bare `&self` door left
    /// the buffer uncleared, so a failed DDL's error rows leaked into every
    /// later `SHOW WARNINGS`.
    fn parse_statement(&mut self, sql: &str) -> Result<Stmt, SqlQueryError> {
        self.parse_at_statement_boundary(sql).map_err(map_error)
    }

    fn affected_rows(&self) -> u64 {
        self.node().ok_packet.affected_rows
    }

    fn last_insert_id(&self) -> u64 {
        self.node().ok_packet.last_insert_id
    }

    fn take_local_infile_request(&mut self) -> Option<String> {
        self.node()
            .parked_local_infile
            .as_ref()
            .map(|parked| parked.path.clone())
    }

    fn finish_execute_stmt(&mut self, cost: std::time::Duration) {
        self.finish_txn_write_throughput(cost);
    }

    /// The session's own `tidb_slow_log_threshold`, in milliseconds.
    fn slow_log_threshold(&self) -> Option<std::time::Duration> {
        let raw = self.vars().get_system("tidb_slow_log_threshold").ok()?;
        let millis: u64 = raw.parse().ok()?;
        Some(std::time::Duration::from_millis(millis))
    }

    fn set_connection_attributes(
        &mut self,
        attributes: std::collections::BTreeMap<String, String>,
    ) {
        Session::set_connection_attributes(self, attributes);
    }

    fn query_cancellation(
        &self,
    ) -> Option<Arc<dyn crate::domain::session_api::ActiveQueryCancellation>> {
        Some(Arc::new(self.begin_query_cancellation()))
    }

    fn sql_killer(&self) -> Option<Arc<tidb_util::sqlkiller::SqlKiller>> {
        Some(Session::sql_killer(self))
    }

    fn should_install_connection_alive(&self, stmt: &Stmt) -> bool {
        let session = self;
        crate::domain::session_api::should_install_connection_alive(stmt, |name| {
            session.prepared_statement_node(name)
        })
    }

    fn wait_timeout(&self) -> std::time::Duration {
        Session::wait_timeout(self)
    }

    /// This session's own `@@max_allowed_packet`; see the trait's own doc for
    /// why Go rebinds the packet reader from it on every packet.
    fn max_allowed_packet(&self) -> Option<usize> {
        Some(Session::max_allowed_packet(self) as usize)
    }

    fn try_consume_long_data(&mut self, bytes: i64) -> bool {
        Session::try_consume_long_data(self, bytes)
    }

    fn release_long_data(&mut self, bytes: i64) {
        Session::release_long_data(self, bytes);
    }

    fn connection_id(&self) -> u64 {
        Session::connection_id(self)
    }

    fn split_statements(
        &mut self,
        sql: &str,
        client_multi_statements: bool,
    ) -> Result<Vec<String>, SqlQueryError> {
        Session::split_statements(self, sql, client_multi_statements).map_err(map_error)
    }

    fn flush_multi_statement_warning(&mut self) {
        Session::flush_multi_statement_warning(self);
    }

    /// The live status word Go reads with `cc.ctx.Status()` before every
    /// OK/EOF packet. The driver session owns the transaction state this tier
    /// acts on, so the wire word and the tier's behaviour cannot disagree.
    fn wire_status(&self) -> WireStatus {
        WireStatus::of_session(self)
    }

    /// The OK/EOF packet's warning count, read off the same buffer
    /// `SHOW WARNINGS` reports (Go `ctx.WarningCount()`).
    fn warning_count(&self) -> u16 {
        self.wire_warning_count()
    }

    fn statement_info(&self) -> Vec<u8> {
        self.statement_message().as_bytes().to_vec()
    }

    fn warning_codes(&self) -> Vec<u16> {
        self.warnings()
            .iter()
            .map(|warning| warning.code())
            .collect()
    }

    /// Go `clientConn.initResultEncoder`'s read: this session's
    /// `@@character_set_results`.
    fn result_charset(&self) -> Cow<'_, str> {
        Session::result_charset(self)
    }

    fn input_charset(&self) -> Cow<'_, str> {
        Session::input_charset(self)
    }

    /// Go leaves `CurrentDB` empty when the handshake carried no schema; see
    /// the trait's own doc.
    fn deselect_database(&mut self) {
        Session::deselect_database(self);
    }

    fn set_collation(&mut self, collation_id: u8) -> Result<(), SqlQueryError> {
        Session::set_collation(self, i32::from(collation_id)).map_err(SqlQueryError::unknown)
    }

    fn close_prepared(&mut self, statement: &PreparedGeneral) {
        if let Some(prepared) = statement.prepared_ast() {
            Session::close_prepared(self, prepared);
        }
    }
    fn execute_stmt<'a>(
        &'a mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<QueryResult<'a>>, SqlQueryError> {
        self.node_conn().execute_stmt(sql, stmt)
    }
    fn finish_local_infile(&mut self, data: &[u8]) -> Result<(), SqlQueryError> {
        self.node_conn().finish_local_infile(data)
    }
    /// The catalog is refreshed first so a schema another node created since
    /// this connection opened is selectable, exactly as it is for a statement.
    fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        self.node_conn().select_database(name)
    }
    fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        self.node_conn().prepare_general(sql)
    }
    fn execute_general<'a>(
        &'a mut self,
        statement: &PreparedGeneral,
        values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        self.node_conn().execute_general(statement, values)
    }
}

/// Go `QueryCtx`'s steps that run on the store, behind [`QuerySession`]
/// for [`Session`].
impl<'s> ClusterServerSession<'s> {
    pub(crate) fn execute_stmt(
        mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<QueryResult<'s>>, SqlQueryError> {
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
            return Ok(None);
        }
        // A text `EXECUTE` of a prepared DDL runs that DDL (Go `ExecuteExec`
        // builds the prepared plan as its body).
        if let Some((body_sql, body)) = self.session.prepared_ddl_body(stmt) {
            return self.execute_stmt(&body_sql, &body);
        }
        if self.control_transaction_parsed(sql, stmt)?.is_some() {
            return Ok(None);
        }
        self.begin_statement();
        let compiled = match self.compile(sql, stmt) {
            Ok(CompiledStatement::Query) => {
                let _execute_stmt =
                    tidb_util::tracing::begin_thread_region_scope("session.ExecuteStmt");
                return self.open_query(sql, stmt).map(Some);
            }
            // Go `TraceExec` runs the statement it wraps and reports its spans.
            Ok(CompiledStatement::Trace) => {
                let Stmt::Admin(admin) = stmt else {
                    unreachable!("a TRACE plan compiles from ADMIN TRACE")
                };
                let tidb_ast::AdminStmt::Trace(trace) = admin.as_ref() else {
                    unreachable!("a TRACE plan compiles from ADMIN TRACE")
                };
                return self.execute_trace(trace).map(Some);
            }
            compiled => compiled,
        };
        match compiled.and_then(|compiled| self.run_without_result(compiled, sql, stmt)) {
            Ok(outcome) => {
                self.node.ok_packet = outcome;
                Ok(None)
            }
            Err(error) => {
                // Go `TiDBContext.ExecuteStmt` appends the error to the
                // statement context; the data-conversion codes a DML filed as
                // its own warning are not appended twice.
                if matches!(stmt, Stmt::Ddl(_))
                    || !matches!(error.code, 1148 | 1210 | 1264 | 1265 | 1366 | 1406)
                {
                    self
                        .session
                        .record_ddl_failure(error.code, error.message.clone(), error.identity);
                }
                Err(error)
            }
        }
    }


    pub(crate) fn finish_local_infile(&mut self, data: &[u8]) -> Result<(), SqlQueryError> {
        let parked = self.node.parked_local_infile.take().ok_or_else(|| {
            SqlQueryError::unknown("statement did not request a client-local file")
        })?;
        self.node.ok_packet = self.execute_local_infile_parsed(&parked.sql, &parked.stmt, data)?;
        Ok(())
    }

    pub(crate) fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.session.select_database(name).map_err(map_error)
    }


    pub(crate) fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
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
            Ok(columns) => crate::domain::pipeline_session::select_columns(&columns),
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


    pub(crate) fn execute_general(
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
                    crate::domain::pipeline_session::select_columns(&columns),
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
/// [`QuerySession::execute_stmt`] and by internal callers that know the
/// statement's kind.
impl<'s> ClusterServerSession<'s> {
    pub(crate) fn local_infile_path(&mut self, sql: &str) -> Result<Option<String>, SqlQueryError> {
        let stmt = (*self.session).parse_statement(sql).map_err(map_error)?;
        self.local_infile_path_parsed(sql, &stmt)
    }

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

    pub(crate) fn execute_local_infile(
        &mut self,
        sql: &str,
        data: &[u8],
    ) -> Result<WriteOutcome, SqlQueryError> {
        let stmt = (*self.session).parse_statement(sql).map_err(map_error)?;
        self.execute_local_infile_parsed(sql, &stmt, data)
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
    /// through [`QuerySession::parse_statement`]); routing, the SET door, the
    /// kind, the resource group, the prelock keys and the run all read that
    /// node.
    /// Go `ExecuteInternal`: one statement of internal SQL through
    /// [`QuerySession::execute_stmt`]. Answers the rows of a result set, none
    /// otherwise; [`QuerySession::affected_rows`] then holds the count.
    pub(crate) fn execute_internal(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, SqlQueryError> {
        let stmt = QuerySession::parse_statement(&mut *self.session, sql)?;
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

    pub(crate) fn execute_write_parsed(
        &mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<WriteOutcome>, SqlQueryError> {
        // A text `EXECUTE` of a prepared DDL runs that DDL (Go `ExecuteExec`
        // builds the prepared plan as its body). The DDL must take this
        // node's cluster route; the session would apply it to its own
        // in-process catalog, where no other statement can see it.
        if let Some((body_sql, body)) = self.session.prepared_ddl_body(stmt) {
            return self.execute_write_parsed(&body_sql, &body);
        }
        // A transaction statement an internal session runs as SQL (Go's
        // `ExecuteInternal("BEGIN")`) takes the same door the wire's does.
        if classify_transaction_control_stmt(stmt).is_some() {
            self.control_transaction_parsed(sql, stmt)?;
            return Ok(Some(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        self.begin_statement();
        match self.compile(sql, stmt)? {
            CompiledStatement::Trace | CompiledStatement::Query => Ok(None),
            compiled => self.run_without_result(compiled, sql, stmt).map(Some),
        }
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
            CompiledStatement::Write => {
                let _execute_stmt =
                    tidb_util::tracing::begin_thread_region_scope("session.ExecuteStmt");
                self.observe_routed_write(sql, stmt, |node| {
                    node.execute_ordinary_write_parsed(sql, stmt)
                })
            }
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
    /// through [`QuerySession::parse_statement`]).
    pub(crate) fn execute_parsed(
        mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<QueryResult<'s>, SqlQueryError> {
        // Go `TraceExec` runs the statement it wraps and reports its spans.
        if let Stmt::Admin(admin) = stmt {
            if let tidb_ast::AdminStmt::Trace(trace) = admin.as_ref() {
                return self.execute_trace(trace);
            }
        }
        // See `execute_write_parsed`: a prepared DDL body takes the DDL route.
        if let Some((body_sql, body)) = self.session.prepared_ddl_body(stmt) {
            return self.execute_parsed(&body_sql, &body);
        }
        // A transaction statement takes the connection's transaction door,
        // whichever entry it came through; it answers no rows.
        if classify_transaction_control_stmt(stmt).is_some() {
            self.control_transaction_parsed(sql, stmt)?;
            return Ok(QueryResult::new(Box::new(
                crate::domain::pipeline_session::affected_rows_source(0),
            )));
        }
        // Go `session.ExecuteStmt`: it returns the opened record set, which
        // the caller drains outside it.
        let _execute_stmt = tidb_util::tracing::begin_thread_region_scope("session.ExecuteStmt");
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
        // Refresh before access-shape classification. Waiting until the
        // statement lifecycle would allow a stale point shape to select MaxTS
        // before the executor sees the current schema.
        self.rebuild_catalog_if_stale();
        // The text protocol reaches DDL through `execute_write`; this covers a
        // front end that goes straight to the result-set path, so a routed
        // statement runs exactly once either way.
        self.session.clear_precompiled_statement();
        self
            .session
            .validate_statement_read_only_in_staleness(stmt)
            .map_err(map_error)?;
        self.session.restore_statement_variables();
        let executor = self
            .session
            .compile_statement(stmt)
            .map_err(map_error)?;
        if executor == CompiledExecutor::Simple && Session::is_account_statement(stmt) {
            self.observe_routed_write(sql, stmt, |node| node.run_account_executor(sql, stmt))?;
            return Ok(QueryResult::new(Box::new(
                crate::domain::pipeline_session::affected_rows_source(0),
            )));
        }
        let route = self.route_for(stmt, executor)?;
        if self.run_routed_statement(route, sql, stmt)?.is_some() {
            return Ok(QueryResult::new(Box::new(
                crate::domain::pipeline_session::affected_rows_source(0),
            )));
        }
        self.open_query(sql, stmt)
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

fn global_lock_wait_timeout(vars: &GlobalSysvars) -> Duration {
    let seconds = vars
        .get("innodb_lock_wait_timeout")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(tidb_vardef::defaults::DEF_INNODB_LOCK_WAIT_TIMEOUT as u64);
    Duration::from_secs(seconds)
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

fn map_error(error: tidb_executor::DriverError) -> SqlQueryError {
    let mapped = error.clone().to_mysql_error();
    SqlQueryError::from(mapped).with_cause(error)
}

/// Storage bound to no snapshot, used only to decide which tables a catalog
/// *could* be built over. Deciding that reads a `TableInfo`, never a row.
fn detached_storage() -> ClusterTableStorage {
    let slot: Arc<Mutex<dyn ClusterSnapshot>> = Arc::new(Mutex::new(SwappableSnapshot::new()));
    ClusterTableStorage::new(MutationBuffer::new(), slot)
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
