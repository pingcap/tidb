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
//! `SHOW` and `EXPLAIN` ([`crate::pipeline_session`] serves it over TCP against
//! an in-process catalog). [`crate::cluster_session`] binds that same driver's
//! catalog to real transactional storage (the `cluster-session-smoke` binary
//! proves it against a live cluster). [`crate::cluster_privileges`] turns the
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
//! * `SET autocommit = 0`, which carries no keyword at all -- see
//!   [`ClusterServerSession::begin_if_autocommit_off`] for why the variable
//!   itself is the routing question, and
//!   [`ClusterServerSession::commit_if_session_left_transaction`] for the
//!   statement that ends the transaction from the inside.
//! * `SAVEPOINT`, which under `autocommit = 0` is Go's `Txn(true)` and so is
//!   itself a transaction opening ([`ClusterServerSession::apply_savepoint`]).
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
//! same optimistic 2PC the DML path uses ([`tidb_exec::real_tikv_ddl`]), and a
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
//! [`crate::cluster_account_seam`] states why, and what a failure leaves
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
//! [`StatementSnapshot`]: tidb_exec::cluster_table_storage::StatementSnapshot
//! [`SessionTransaction`]: tidb_exec::cluster_table_storage::SessionTransaction

use std::borrow::Cow;
use std::cell::Cell;
use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::resultset_source::ResultSetSource;
use chrono::{Datelike, Timelike, Utc};
use tidb_domain::historical_stats::{
    HistoricalStatsMetrics, HistoricalStatsWorker, InfoSchemaView, SessionInfoSchema,
    StatsHandle as HistoricalStatsHandle, TableMeta,
};
use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::cluster_analyze::AnalyzeStatement;
use tidb_exec::cluster_catalog::ClusterCatalog;
use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::cluster_load_stats::prepare_cluster_load_stats;
use tidb_exec::cluster_stats_load::ClusterStatsLoader;
use tidb_exec::cluster_stats_lock::prepare_cluster_stats_lock;
use tidb_exec::cluster_stats_lock::ClusterStatsLockStatement;
use tidb_exec::real_tikv_analyze::prepare_cluster_analyze;
use tidb_exec::real_tikv_catalog::SnapshotMetaSnapshot;
use tidb_exec::real_tikv_ddl::prepare_cluster_ddl_with_context;
use tidb_exec::real_tikv_stats_lock::ClusterStatsLockCommitError;
use tidb_exec::stats_watch::SharedStats;
use tidb_executor::access_path::StatementReadShape;
use tidb_executor::cluster_storage::{
    BufferCheckpoint, ClusterSnapshot, ClusterTableStorage, MutationBuffer, SwappableSnapshot,
};
use tidb_executor::remote_scan::PushdownScanner;
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_session::privilege::PrivilegeRegistry;
use tidb_session::process::ProcessRegistry;
use tidb_session::{
    GlobalSysvars, Session, StmtKind, StmtOutput, StmtResult, StoredStateChange,
    TableStorageStatistics, TableStorageStatsProvider,
};
use tidb_stats_handle_cache::{StatsTableRowSource, TABLE_ROW_STATS_CACHE};

use tidb_exec::cluster_table_storage::LockKeysOutcome;

use crate::cluster_account_seam::ClusterAccountWriter;
use crate::cluster_analyze_seam::ClusterAnalyze;
use crate::cluster_session::{
    cluster_session_catalog, cluster_session_catalog_with_templates, planner_statistics,
    KvTableTemplates, SkippedTable, StatsTemplates, TableAutoIds,
};
use crate::cluster_stats_lock_seam::ClusterStatsLock;
use crate::cluster_sysvar_seam::ClusterSysvarWriter;
use crate::pipeline_session::MaterializedResultSetSource;
use crate::sql_node::{
    ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};
use crate::wire_status::WireStatus;

fn is_analyze_table_sql(sql: &str) -> bool {
    tidb_parser::normalize_digest(sql)
        .0
        .starts_with("analyze table")
}

/// The PD/TiKV control-plane deadline this node's boot and statements use, the
/// same one the bounded node applies.
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Pinned Go `statistics/handle.StatsOwnerKey`.
pub(crate) const STATS_OWNER_KEY: &str = "/tidb/stats/owner";
/// Pinned Go `statistics/handle.StatsPrompt`.
pub(crate) const STATS_OWNER_PROMPT: &str = "stats";

/// Go `errno.ErrTableaccessDenied`.
const ER_TABLEACCESS_DENIED_ERROR: u16 = 1142;

/// Go `kv.ErrWriteConflict`, the one cause an autocommit statement is replayed
/// for.
const ERR_WRITE_CONFLICT: u16 = tidb_exec::pessimistic_lock_error::ERR_WRITE_CONFLICT;

/// How many times an autocommit statement that lost the race is run again
/// before the conflict reaches the client.
///
/// This is `@@tidb_retry_limit`'s default, `DefTiDBRetryLimit = 10`
/// (`pkg/sessionctx/vardef/tidb_vars.go:1527`). Go scales it DOWN by
/// transaction size --
/// `maxRetryCount = limit - (limit-1) * txnSize/TxnTotalSizeLimit`
/// (`pkg/session/session.go:881-882`), with `TxnTotalSizeLimit` 100 MiB
/// (`pkg/config/config.go:65`) -- so a statement anywhere near this seam's
/// size gets the full 10. The bound is the contract: Go does NOT retry
/// forever, and after the last attempt it returns the last commit error
/// (`pkg/session/session.go:1272-1278`), so the client still sees 9007. A
/// conflict that outlives the budget is still reported, exactly as before.
const AUTOCOMMIT_RETRY_LIMIT: u32 = 10;

/// Go `kv.retryBackOffBase`, in milliseconds (`pkg/kv/txn.go:182-183`; the
/// comment there says microsecond and the code multiplies by
/// `time.Millisecond` -- the code is the contract).
const RETRY_BACK_OFF_BASE_MS: u32 = 1;

/// Go `kv.retryBackOffCap`, in milliseconds (`pkg/kv/txn.go:184-185`).
const RETRY_BACK_OFF_CAP_MS: u32 = 100;

/// Go `kv.BackOff` (`pkg/kv/txn.go:191-197`): exponential backoff with full
/// jitter, sleeping a uniform draw from `[0, min(cap, base * 2^attempts))`
/// milliseconds. `attempts` counts from 1, as Go's does -- it increments
/// `retryCnt` before the sleep -- so the first wait is drawn from `[0, 2)ms`.
///
/// This is also what bounds a spinning retry in time as well as in count: even
/// a statement that loses all ten races sleeps a capped amount rather than
/// hammering the conflicting key.
fn back_off(attempts: u32) {
    std::thread::sleep(Duration::from_millis(u64::from(jitter_below(
        back_off_upper_ms(attempts),
    ))));
}

/// The exclusive upper bound, in milliseconds, of attempt `attempts`'s sleep:
/// Go's `min(retryBackOffCap, retryBackOffBase * 2^attempts)`.
fn back_off_upper_ms(attempts: u32) -> u32 {
    RETRY_BACK_OFF_BASE_MS
        .checked_shl(attempts)
        .unwrap_or(u32::MAX)
        .min(RETRY_BACK_OFF_CAP_MS)
}

/// A uniform draw from `[0, upper)`, the jitter half of [`back_off`].
///
/// This is a real generator rather than a clock reading, and the difference is
/// measured rather than stylistic: this machine's `SystemTime` advances in
/// 1000ns steps, so `subsec_nanos() % upper` is IDENTICALLY ZERO for every
/// `upper` that divides 1000 -- which is the first three backoffs, `2`, `4`
/// and `8` ms, exactly the ones that matter when two sessions are colliding on
/// one key. Two conflicting connections would then both spin with no sleep and
/// stay in lockstep. Seeding once per connection thread is what makes two
/// racing sessions draw different sleeps at all.
///
/// The generator is `xorshift64*`; a backoff length needs decorrelation, not
/// cryptographic quality, and this keeps the node free of a random-number
/// dependency.
fn jitter_below(upper: u32) -> u32 {
    thread_local! {
        static STATE: Cell<u64> = const { Cell::new(0) };
    }
    STATE.with(|state| {
        let mut x = state.get();
        if x == 0 {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |since| since.as_nanos() as u64);
            // The thread-local's own address separates two connections that
            // seeded within the same clock tick.
            x = nanos ^ (state as *const Cell<u64> as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            if x == 0 {
                x = 0x9E37_79B9_7F4A_7C15;
            }
        }
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        state.set(x);
        u32::try_from((x.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 33) % u64::from(upper)).unwrap_or(0)
    })
}

mod boot;
mod ddl;
mod ddl_notifier;
pub(crate) mod schema_sync;
mod statistics;
mod transactions;

pub use boot::run_cluster_session_node;
pub(crate) use boot::run_cluster_session_node_with_spill;
pub use ddl::{ClusterDdl, RealClusterDdl};
pub use tidb_exec::real_tikv_ddl::ClusterDdlReport;
#[cfg(test)]
use transactions::sql_error;
pub use transactions::{ClusterTransactions, OpenClusterTransaction, RealClusterTransactions};

/// Which of this node's three paths one statement takes.
///
/// The two stored-state changes are named apart rather than lumped into a
/// boolean because they publish through different tiers -- the catalog through
/// [`ClusterDdl`], the accounts through
/// [`crate::cluster_account_seam::ClusterAccountWriter`] -- and each has its
/// own refusals.
enum StatementRoute {
    /// Changes nothing stored outside this process.
    Ordinary,
    /// One catalog change this node can express.
    Ddl(DdlStatement),
    /// DDL owned by this connection's LOCAL temporary-table namespace.
    LocalTemporaryDdl,
    /// One `mysql.*` account change.
    Accounts,
    /// One `SET GLOBAL` change to `mysql.global_variables`.
    GlobalVars,
    /// One `ANALYZE TABLE`, per table it named.
    Analyze(Vec<AnalyzeStatement>),
    /// One LOAD STATS statement awaiting its client-local file bytes.
    LoadStats,
    /// One foreground dump of this node's pending statistics deltas.
    FlushStatsDelta(FlushStatsDeltaTargets),
    /// One persisted `LOCK STATS` or `UNLOCK STATS` operation.
    StatsLock(ClusterStatsLockStatement),
}

enum FlushStatsDeltaTargets {
    All,
    Tables(Vec<i64>),
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

struct ClusterColumnStatsUsageProvider {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
}

struct ClusterTableStorageStatsProvider {
    transactions: Arc<dyn ClusterTransactions>,
    catalog: Arc<SharedClusterCatalog>,
}

struct ClusterTableRowSource<'a> {
    transactions: &'a Arc<dyn ClusterTransactions>,
    catalog: &'a Arc<SharedClusterCatalog>,
    resource_group: &'a str,
}

impl StatsTableRowSource for ClusterTableRowSource<'_> {
    type Error = String;

    fn table_row_counts(
        &self,
        table_ids: &[i64],
    ) -> Result<Vec<tidb_stats_handle_cache::TableRowCount>, Self::Error> {
        let snapshot = self.transactions.open_snapshot(self.resource_group)?;
        let catalog = self.catalog.load();
        let loader = ClusterStatsLoader::locate(&catalog).map_err(|error| error.to_string())?;
        loader
            .load_table_row_counts(&mut SnapshotMetaSnapshot::new(snapshot), table_ids)
            .map_err(|error| error.to_string())
    }

    fn column_lengths(
        &self,
        table_ids: &[i64],
    ) -> Result<Vec<tidb_stats_handle_cache::ColumnLength>, Self::Error> {
        let snapshot = self.transactions.open_snapshot(self.resource_group)?;
        let catalog = self.catalog.load();
        let loader = ClusterStatsLoader::locate(&catalog).map_err(|error| error.to_string())?;
        loader
            .load_column_lengths(&mut SnapshotMetaSnapshot::new(snapshot), table_ids)
            .map_err(|error| error.to_string())
    }
}

impl TableStorageStatsProvider for ClusterTableStorageStatsProvider {
    fn load_table_storage_statistics(
        &self,
        resource_group: &str,
    ) -> Result<Vec<TableStorageStatistics>, String> {
        let catalog = self.catalog.load();
        let physical_ids = catalog
            .databases
            .iter()
            .flat_map(|database| &database.tables)
            .flat_map(|table| {
                std::iter::once(table.id).chain(
                    table
                        .get_partition_info()
                        .into_iter()
                        .flat_map(|partition| partition.read().definitions.snapshot())
                        .map(|definition| definition.id),
                )
            })
            .collect::<Vec<_>>();
        TABLE_ROW_STATS_CACHE.update_by_id(
            &ClusterTableRowSource {
                transactions: &self.transactions,
                catalog: &self.catalog,
                resource_group,
            },
            &physical_ids,
        )?;

        Ok(catalog
            .databases
            .iter()
            .flat_map(|database| &database.tables)
            .map(|table| {
                let partitions = table
                    .get_partition_info()
                    .into_iter()
                    .flat_map(|partition| partition.read().definitions.snapshot())
                    .map(|definition| {
                        let row_count = TABLE_ROW_STATS_CACHE.get_table_rows(definition.id);
                        let (data_length, index_length) = TABLE_ROW_STATS_CACHE
                            .get_data_and_index_length(table, definition.id, row_count);
                        let average_row_length = if row_count == 0 {
                            0
                        } else {
                            data_length / row_count
                        };
                        (
                            definition.id,
                            (row_count, average_row_length, data_length, index_length),
                        )
                    })
                    .collect();
                TableStorageStatistics {
                    table_id: table.id,
                    table: TABLE_ROW_STATS_CACHE.estimate_data_length(table),
                    partitions,
                }
            })
            .collect())
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

impl tidb_session::AnalyzeStatusProvider for ClusterAnalyzeStatusProvider {
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

impl tidb_session::ColumnStatsUsageProvider for ClusterColumnStatsUsageProvider {
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
            .map(|value| tidb_session::varsutil::parse_analyze_skip_column_types(&value))
            .unwrap_or_default();
        let mut updated = std::collections::BTreeSet::new();
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
                    if self.stats.update_item(item.table_id, empty, table) {
                        updated.insert(item.table_id);
                    }
                    continue;
                }
            }
            let snapshot = self.transactions.open_snapshot(resource_group)?;
            let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
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
            if loaded.is_some_and(|loaded| self.stats.update_item(item.table_id, loaded, table)) {
                updated.insert(item.table_id);
            }
        }
        let snapshot = self.stats.load();
        Ok(updated
            .into_iter()
            .filter_map(|table_id| {
                let table = catalog
                    .databases
                    .iter()
                    .flat_map(|database| &database.tables)
                    .find(|table| table.id == table_id)?;
                let stats = snapshot.get(&table_id)?.loaded()?;
                Some((table_id, Arc::new(planner_statistics(stats, table))))
            })
            .collect())
    }
}

impl tidb_session::DataLockWaitsProvider for ClusterDataLockWaits {
    fn lock_waits(&self) -> Result<Vec<tidb_session::DataLockWait>, String> {
        self.transactions.lock_waits().map(|entries| {
            entries
                .into_iter()
                .map(|entry| tidb_session::DataLockWait {
                    txn: entry.txn,
                    wait_for_txn: entry.wait_for_txn,
                    key: entry.key,
                    resource_group_tag: entry.resource_group_tag,
                })
                .collect()
        })
    }
}

/// Opens one cluster-backed wide-SQL [`Session`] per authenticated connection.
pub struct ClusterSessionFactory {
    /// The write/read capability every connection's statements open their
    /// snapshots and publish their commits through.
    transactions: Arc<dyn ClusterTransactions>,
    /// Storage-backed wait-for graph exposed by `DATA_LOCK_WAITS`.
    data_lock_waits: Arc<ClusterDataLockWaits>,
    /// The route a stored-schema change this node can express takes.
    ddl: Arc<dyn ClusterDdl>,
    /// The route a stored-account change takes; see
    /// [`crate::cluster_account_seam`].
    accounts: Arc<dyn ClusterAccountWriter>,
    /// The route a `SET GLOBAL` change takes; see
    /// [`crate::cluster_sysvar_seam`].
    sysvars: Arc<dyn ClusterSysvarWriter>,
    /// The route an `ANALYZE TABLE` takes; see
    /// [`crate::cluster_analyze_seam`].
    analyze: Arc<dyn ClusterAnalyze>,
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
    /// The coprocessor this node's sessions serve base-table scans with, when
    /// it was given one. `None` keeps every scan on the raw key/value path.
    cop_scans: Option<Arc<dyn PushdownScanner>>,
    /// This node's server-info syncer, which
    /// `information_schema.TIDB_SERVERS_INFO` reads. `None` leaves that
    /// table empty, which is the honest answer for a node that never
    /// established an identity.
    server_info: Option<Arc<tidb_domain::serverinfo_syncer::Syncer>>,
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
    /// `KvTable` that was handing it out; see [`crate::cluster_auto_id_seam`].
    auto_ids: Arc<dyn TableAutoIds>,
    /// Process-owned spill authority inherited by every connection.
    spill_storage: Option<Arc<tidb_util::disk::SpillStorage>>,
    /// Process-wide memory admission authority installed before listener bind.
    mem_arbitrator: Option<Arc<tidb_util::memory::MemArbitrator>>,
    /// The catalog versions live statements and transactions still read at
    /// -- the MDL gate the schema-sync acknowledger consults before telling
    /// a Go DDL owner this node has the new schema. See
    /// [`schema_sync::SchemaPinRegistry`].
    schema_pins: Arc<schema_sync::SchemaPinRegistry>,
    /// Planner statistics built once per stats snapshot and handed to every
    /// session opened against it, the way Go's domain-level `StatsHandle`
    /// serves one `statistics.Table` per table to all sessions.
    session_stats_cache: Arc<Mutex<StatsTemplates>>,
    /// Fully built tables of one schema version, shared by every session
    /// opened against it -- Go's one `table.Table` per `TableInfo` inside the
    /// shared `infoschema`. A session clones its table and swaps in its own
    /// storage seam, so building ~700 restored tables happens once per DDL
    /// instead of once per CONNECTION (~30MB retained each before this).
    session_kv_cache: Arc<Mutex<KvTableTemplates>>,
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
    /// Go `autoAnalyzeWorker`, installed after stable `Arc` ownership.
    auto_analyze_worker: std::sync::OnceLock<AutoAnalyzeWorker>,
    /// Go Domain's capacity-16 historical-statistics mailbox.
    historical_stats_worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
    /// Go `StartHistoricalStatsWorker`, installed after stable `Arc` ownership.
    historical_stats_runtime: std::sync::OnceLock<HistoricalStatsRuntime>,
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

impl ClusterSessionFactory {
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
            data_lock_waits,
            ddl,
            accounts,
            sysvars,
            analyze,
            stats_lock,
            catalog,
            privileges,
            processes: ProcessRegistry::default(),
            auto_ids,
            cop_scans: None,
            server_info: None,
            global_vars,
            boot_skipped,
            stats,
            spill_storage: None,
            mem_arbitrator: None,
            schema_pins: Arc::new(schema_sync::SchemaPinRegistry::default()),
            session_stats_cache: Arc::new(Mutex::new(StatsTemplates::default())),
            session_kv_cache: Arc::new(Mutex::new(KvTableTemplates::default())),
            workload_repository: std::sync::OnceLock::new(),
            stats_usage,
            stats_usage_workers: std::sync::OnceLock::new(),
            stats_owner: None,
            advanced_sys_session_pool: std::sync::OnceLock::new(),
            ddl_notifier: std::sync::OnceLock::new(),
            analyze_jobs_cleanup_worker: std::sync::OnceLock::new(),
            auto_analyze_worker: std::sync::OnceLock::new(),
            historical_stats_worker,
            historical_stats_runtime: std::sync::OnceLock::new(),
            approximate_table_counts,
            auto_analyze_priority_queue: Arc::new(std::sync::OnceLock::new()),
            auto_analyze_refresher: std::sync::OnceLock::new(),
        }
    }

    /// Returns Go StatsHandle's single auto-analyze priority queue.
    pub(crate) fn auto_analyze_priority_queue(
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
                        peer_addr: "127.0.0.1:0".parse().expect("loopback socket address"),
                        identity: crate::configured_user_store::AuthenticatedIdentity::internal(),
                        secure_transport: false,
                        tls_status: None,
                        cancellation: crate::sql_node::ConnectionCancellation::default(),
                        close: crate::sql_node::ConnectionClose::default(),
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
    pub(crate) fn auto_analyze_refresher(
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
    pub(crate) fn start_historical_stats_worker(self: &Arc<Self>) {
        if !tidb_domain::historical_stats::enable_dump_historical_stats()
            || self.historical_stats_runtime.get().is_some()
        {
            return;
        }
        let handle = Arc::new(ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
        });
        let _ = self
            .historical_stats_runtime
            .set(HistoricalStatsRuntime::start(
                Arc::clone(&self.historical_stats_worker),
                handle,
            ));
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
        }
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_cleanup_corrupted_analyze_jobs_on_current_instance(
                snapshot,
                &self.catalog.load(),
                &instance,
                &analyze_process_ids,
                &cutoff,
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
        }
        .commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_cleanup_corrupted_analyze_jobs_on_dead_instances(
                snapshot,
                &self.catalog.load(),
                &alive_instances,
                &cutoff,
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
            eprintln!(
                "{{\"event\":\"auto_analyze_exceeded_window\",\"process_id\":{process_id},\"now\":{},\"start\":{},\"end\":{}}}",
                serde_json::to_string(&now.to_rfc3339()).unwrap_or_else(|_| "\"\"".to_owned()),
                serde_json::to_string(&parameters.start_time)
                    .unwrap_or_else(|_| "\"\"".to_owned()),
                serde_json::to_string(&parameters.end_time)
                    .unwrap_or_else(|_| "\"\"".to_owned()),
            );
            self.processes.kill(process_id, true);
        }
    }

    /// Starts Go Domain's usage workers after the factory has stable `Arc`
    /// ownership. Index GC runs for every lease; delta and column dumps run
    /// only for a positive lease.
    pub(crate) fn start_stats_usage_workers(
        self: &Arc<Self>,
        lease: crate::node_config::StatsLease,
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
    pub(crate) fn start_analyze_jobs_cleanup_worker(
        self: &Arc<Self>,
        lease: crate::node_config::StatsLease,
    ) {
        let crate::node_config::StatsLease::Positive(lease) = lease else {
            return;
        };
        if self.analyze_jobs_cleanup_worker.get().is_some() {
            return;
        }
        let _ = self
            .analyze_jobs_cleanup_worker
            .set(AnalyzeJobsCleanupWorker::start(self, lease));
    }

    /// Starts pinned Go `autoAnalyzeWorker`. Like Go, only a positive stats
    /// lease starts it; every tick checks the process switch and ownership.
    pub(crate) fn start_auto_analyze_worker(
        self: &Arc<Self>,
        lease: crate::node_config::StatsLease,
        run_auto_analyze: bool,
    ) {
        let crate::node_config::StatsLease::Positive(lease) = lease else {
            return;
        };
        if self.auto_analyze_worker.get().is_some() {
            return;
        }
        let _ =
            self.auto_analyze_worker
                .set(AutoAnalyzeWorker::start(self, lease, run_auto_analyze));
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
                )
                .map_err(|error| error.to_string())?
            };
            self.transactions
                .commit_optimistic_mutations(plan.mutations, read_ts, resource_group)
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
            let transaction = transactions.begin(true, resource_group)?;
            let read_ts = transaction.start_ts();
            let staged = MutationBuffer::new();
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
                        tidb_exec::cluster_table_storage::lock_pessimistic_statement_with(
                            transaction.start_ts(),
                            |retry_ts| {
                                let snapshot = match retry_ts {
                                    Some(retry_ts) => transaction.snapshot_at_for(retry_ts, true),
                                    None => transaction.snapshot_for(true),
                                }?;
                                Ok(tidb_exec::cluster_table_storage::overlay_staged_mutations(
                                    snapshot, &staged,
                                ))
                            },
                            |keys, presume_not_exists, duplicate_hints| {
                                transaction.lock_staged_keys_with_assertions(
                                    keys,
                                    presume_not_exists,
                                    duplicate_hints,
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
                                    )
                                    .map_err(|error| error.to_string())?;
                                Ok(((), plan.mutations))
                            },
                        )
                        .map_err(|error| error.to_string())?;
                    tidb_exec::cluster_table_storage::stage_mutations(&staged, mutations);
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
                .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value));
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
                        let transaction = transactions.begin(true, resource_group)?;
                        let staged = MutationBuffer::new();
                        let ((modify_count, count), lock_mutations) =
                            tidb_exec::cluster_table_storage::lock_pessimistic_statement_with(
                                transaction.start_ts(),
                                |read_ts| match read_ts {
                                    Some(read_ts) => transaction.snapshot_at_for(read_ts, true),
                                    None => transaction.snapshot_for(true),
                                },
                                |keys, presume_not_exists, duplicate_hints| {
                                    transaction.lock_staged_keys_with_assertions(
                                        keys,
                                        presume_not_exists,
                                        duplicate_hints,
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
                        tidb_exec::cluster_table_storage::stage_mutations(&staged, lock_mutations);
                        let ((), replace_mutations) =
                            tidb_exec::cluster_table_storage::lock_pessimistic_statement_with(
                                transaction.start_ts(),
                                |retry_ts| match retry_ts {
                                    Some(retry_ts) => transaction.snapshot_at_for(retry_ts, true),
                                    None => transaction.snapshot_for(true),
                                },
                                |keys, presume_not_exists, duplicate_hints| {
                                    transaction.lock_staged_keys_with_assertions(
                                        keys,
                                        presume_not_exists,
                                        duplicate_hints,
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
                            )
                                    .map_err(|error| error.to_string())?;
                                    Ok(((), plan.mutations))
                                },
                            )
                            .map_err(|error| error.to_string())?;
                        tidb_exec::cluster_table_storage::stage_mutations(
                            &staged,
                            replace_mutations,
                        );
                        transaction.commit(&staged).map_err(|error| error.message)
                    })();
                    if let Err(error) = result {
                        eprintln!(
                            "{{\"event\":\"record_historical_stats_meta_failed\",\"version\":{read_ts},\"source\":\"flush stats\",\"table_id\":{},\"error\":{error:?}}}",
                            update.table_id
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
    pub(crate) fn set_workload_repository(
        &self,
        worker: Arc<tidb_workloadrepo::Worker>,
    ) -> Result<(), Arc<tidb_workloadrepo::Worker>> {
        self.workload_repository.set(worker)
    }

    /// Replaces the factory's pin registry with the node-owned one the
    /// schema-sync acknowledger reads.
    #[must_use]
    pub(crate) fn with_schema_pins(mut self, pins: Arc<schema_sync::SchemaPinRegistry>) -> Self {
        self.schema_pins = pins;
        self
    }

    pub fn with_spill_storage(mut self, spill_storage: Arc<tidb_util::disk::SpillStorage>) -> Self {
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

    /// Serves this node's base-table scans through `scanner`, so a `WHERE`
    /// is evaluated at the region instead of after the range's bytes have
    /// crossed the network.
    ///
    /// The staged-write half is untouched: a session's uncommitted rows are
    /// merged client-side and re-tested by the same predicate (see
    /// [`tidb_executor::remote_scan`]).
    #[must_use]
    pub fn with_cop_scans(mut self, scanner: Arc<dyn PushdownScanner>) -> Self {
        self.cop_scans = Some(scanner);
        self
    }

    /// Binds the node's server-info syncer for
    /// `information_schema.TIDB_SERVERS_INFO`.
    #[must_use]
    pub fn with_server_info(mut self, syncer: Arc<tidb_domain::serverinfo_syncer::Syncer>) -> Self {
        self.server_info = Some(syncer);
        self
    }

    /// Binds Go Domain's `/tidb/stats/owner` manager.
    #[must_use]
    pub(crate) fn with_stats_owner(mut self, owner: Arc<dyn tidb_owner::Manager>) -> Self {
        self.stats_owner = Some(owner);
        self
    }

    /// Starts Go Domain's statistics-owner campaign after the factory has
    /// reached its stable process lifetime.
    pub(crate) fn campaign_stats_owner(
        self: &Arc<Self>,
        stats_lease: crate::node_config::StatsLease,
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
    pub fn catalog_snapshot(&self) -> tidb_exec::cluster_catalog::ClusterCatalog {
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
    fn start(
        factory: &Arc<ClusterSessionFactory>,
        stats_lease: Duration,
        run_auto_analyze: bool,
    ) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("auto-analyze".to_owned())
            .spawn(move || loop {
                if running.wait(stats_lease) {
                    return;
                }
                let Some(factory) = weak.upgrade() else {
                    return;
                };
                factory.handle_auto_analyze_tick(run_auto_analyze, stats_lease);
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
    fn start(factory: &Arc<ClusterSessionFactory>, stats_lease: Duration) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        let thread = std::thread::Builder::new()
            .name("analyze-jobs-cleanup".to_owned())
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
                                        eprintln!(
                                            "{{\"event\":\"gc_analyze_history_failed\",\"error\":{error:?}}}"
                                        );
                                    }
                                }
                                Err(error) => eprintln!(
                                    "{{\"event\":\"gc_analyze_history_failed\",\"error\":{error:?}}}"
                                ),
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
                                    eprintln!(
                                        "{{\"event\":\"cleanup_analyze_jobs_current_instance_failed\",\"error\":{error:?}}}"
                                    );
                                }
                                if factory
                                    .stats_owner
                                    .as_ref()
                                    .is_some_and(|owner| owner.is_owner())
                                {
                                    if let Err(error) = factory
                                        .cleanup_corrupted_analyze_jobs_on_dead_instances(cutoff)
                                    {
                                        eprintln!(
                                            "{{\"event\":\"cleanup_analyze_jobs_dead_instances_failed\",\"error\":{error:?}}}"
                                        );
                                    }
                                }
                            }
                            Err(error) => eprintln!(
                                "{{\"event\":\"cleanup_analyze_jobs_current_instance_failed\",\"error\":{error:?}}}"
                            ),
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
    fn start(factory: &Arc<ClusterSessionFactory>, lease: crate::node_config::StatsLease) -> Self {
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
            ClusterSessionFactory::gc_index_usage,
        )];
        let mut flush_on_drop = false;
        if let crate::node_config::StatsLease::Positive(lease) = lease {
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

impl Drop for ClusterSessionFactory {
    fn drop(&mut self) {
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
                eprintln!("{{\"event\":\"dump_stats_delta_failed\",\"error\":{error:?}}}");
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
        let plan = build(&mut SnapshotMetaSnapshot::new(snapshot), read_ts)?;
        self.transactions
            .commit_optimistic_mutations(plan.mutations, read_ts, "default")
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
            .commit_optimistic_mutations(plan.mutations, read_ts, "default")
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
        let cutoff = tidb_exec::mysql_bootstrap::utc_now_timestamp()
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
            eprintln!(
                "{{\"event\":\"clear_outdated_historical_stats_failed\",\"error\":{error:?}}}"
            );
        }
        self.commit_stats_plan(|snapshot, _| {
            tidb_exec::cluster_stats_write::plan_stats_gc_timestamp_write(
                snapshot,
                &catalog,
                gc_version,
                tidb_exec::mysql_bootstrap::utc_now_timestamp(),
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
        if !tidb_exec::option_values::tidb_opt_on(&enabled) {
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
            .map(|value| tidb_exec::option_values::tidb_opt_on(&value))
            .map_err(|error| format!("{error:?}"))
    }

    fn record_historical_stats_to_storage(
        &self,
        database_name: &str,
        table: &TableMeta,
        physical_id: i64,
        is_partition: bool,
    ) -> Result<u64, String> {
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
            eprintln!(
                "{{\"event\":\"no stats data to record\",\"dbName\":{database_name:?},\"tableName\":{:?}}}",
                table.name
            );
            return Ok(0);
        };
        let transaction = self.transactions.begin(true, "default")?;
        let staged = MutationBuffer::new();
        let (version, blocks) = tidb_exec::cluster_stats_write::historical_stats_data_blocks(&json)
            .map_err(|error| error.to_string())?;
        let create_time = tidb_exec::mysql_bootstrap::local_now_datetime6();
        for (sequence, block) in blocks.iter().enumerate() {
            let ((), mutations) =
                tidb_exec::cluster_table_storage::lock_pessimistic_statement_with(
                    transaction.start_ts(),
                    |read_ts| match read_ts {
                        Some(read_ts) => transaction.snapshot_at_for(read_ts, true),
                        None => transaction.snapshot_for(true),
                    },
                    |keys, presume_not_exists, duplicate_hints| {
                        transaction.lock_staged_keys_with_assertions(
                            keys,
                            presume_not_exists,
                            duplicate_hints,
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
                            )
                            .map_err(|error| error.to_string())?;
                        Ok(((), plan.mutations))
                    },
                )
                .map_err(|error| error.to_string())?;
            tidb_exec::cluster_table_storage::stage_mutations(&staged, mutations);
        }
        transaction.commit(&staged).map_err(|error| error.message)?;
        Ok(version)
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
        let thread = std::thread::Builder::new()
            .name("historical-stats-worker".to_owned())
            .spawn(move || {
                while let Some(table_id) = running.recv_historical_stats_table() {
                    if let Err(error) = running.dump_historical_stats(
                        table_id,
                        handle.as_ref(),
                        &ClusterHistoricalStatsMetrics,
                    ) {
                        eprintln!(
                            "{{\"event\":\"dump_historical_stats_failed\",\"table_id\":{table_id},\"error\":{error:?}}}"
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
    factory: Weak<ClusterSessionFactory>,
    stop: Arc<UsageWorkerStop>,
    interval: Duration,
    action: fn(&ClusterSessionFactory) -> Result<(), String>,
) -> std::thread::JoinHandle<()> {
    let worker_name = name.to_owned();
    let log_name = worker_name.clone();
    std::thread::Builder::new()
        .name(worker_name)
        .spawn(move || {
            while !stop.wait(interval) {
                let Some(factory) = factory.upgrade() else {
                    return;
                };
                if let Err(error) = action(&factory) {
                    eprintln!("{{\"event\":{log_name:?},\"error\":{error:?}}}");
                }
            }
        })
        .expect("spawning statistics usage worker")
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

impl QuerySessionFactory for ClusterSessionFactory {
    type Session = ClusterServerSession;

    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError> {
        // The connection's own snapshot slot and staged writes: one session,
        // one transaction, exactly as Go's session owns one `kv.Transaction`.
        let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
        let buffer = MutationBuffer::new();
        let handle: Arc<Mutex<dyn ClusterSnapshot>> = Arc::clone(&slot) as _;
        let mut storage = ClusterTableStorage::new(buffer.clone(), handle);
        if let Some(scanner) = self.cop_scans.as_ref() {
            storage = storage.with_remote_scanner(Arc::clone(scanner));
        }
        let loaded = self.catalog.load();
        let statistics = self.stats.load();
        // One planner-statistics set per stats snapshot, shared by every
        // session on it. Building histograms per connection cost ~50MB each.
        let mut templates = self
            .session_stats_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        templates.reuse(&statistics);
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
            &statistics,
            self.auto_ids.as_ref(),
            &mut templates,
            &template_storage,
            Some(&mut kv_templates),
        );
        built
            .catalog
            .set_statistics_item_loader(Arc::new(ClusterStatisticsItemLoader {
                transactions: Arc::clone(&self.transactions),
                catalog: Arc::clone(&self.catalog),
                stats: Arc::clone(&self.stats),
                global_vars: self.global_vars.clone(),
            }));
        let mut session = Session::with_catalog(Arc::new(Mutex::new(built.catalog)));
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
        session.set_analyze_status_provider(Arc::new(ClusterAnalyzeStatusProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            approximate_counts: Arc::clone(&self.approximate_table_counts),
        }));
        session.set_table_storage_stats_provider(Arc::new(ClusterTableStorageStatsProvider {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
        }));
        session.set_advisory_lock_service(Arc::new(transactions::ClusterAdvisoryLockService::new(
            Arc::clone(&self.transactions),
        )));
        if let Some(syncer) = self.server_info.as_ref() {
            session.set_server_info_syncer(Arc::clone(syncer));
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
            context.connection_id,
        )));
        session.set_server_start_timestamp(crate::real_tikv_node::server_start_unix_timestamp());
        if let Some(worker) = self.workload_repository.get() {
            session.set_workload_repository(Arc::clone(worker));
        }
        if let Some(spill_storage) = self.spill_storage.as_ref() {
            session.set_spill_storage(Arc::clone(spill_storage));
        }
        if let Some(arbitrator) = self.mem_arbitrator.as_ref() {
            session.set_mem_arbitrator(Arc::clone(arbitrator));
        }

        let identity = &context.identity;
        session.set_user(
            format!("{}@{}", identity.username(), identity.host()),
            format!("{}@{}", identity.username(), context.peer_addr.ip()),
        );
        session.set_connection_id(context.connection_id);
        session.set_secure_transport(context.secure_transport);
        session.set_tls_status(context.tls_status.clone());
        if identity.in_sandbox_mode() {
            session.enable_sandbox_mode();
        }
        if identity.privilege_bypassed() {
            session.enable_privilege_bypass();
        }
        let guard = self.processes.register(
            context.connection_id,
            identity.username().to_owned(),
            context.peer_addr.to_string(),
            session.current_database().to_owned(),
            Some(Arc::new(ConnectionKillTarget::new(
                context.cancellation.clone(),
                context.close.clone(),
            ))),
        );
        session.attach_process(context.connection_id, guard);
        session.attach_privileges(self.privileges.clone());
        session
            .attach_globals(self.global_vars.clone())
            .map_err(map_error)?;

        Ok(ClusterServerSession {
            session,
            stats_usage: Arc::clone(&self.stats_usage),
            global_vars: self.global_vars.clone(),
            buffer,
            slot,
            storage,
            transactions: Arc::clone(&self.transactions),
            ddl: Arc::clone(&self.ddl),
            accounts: Arc::clone(&self.accounts),
            sysvars: Arc::clone(&self.sysvars),
            analyze: Arc::clone(&self.analyze),
            approximate_table_counts: Arc::clone(&self.approximate_table_counts),
            stats_lock: Arc::clone(&self.stats_lock),
            catalog: Arc::clone(&self.catalog),
            schema_version: loaded.schema_version,
            stats: Arc::clone(&self.stats),
            statistics,
            explicit: None,
            savepoints: Vec::new(),
            skipped: built.skipped,
            auto_ids: Arc::clone(&self.auto_ids),
            schema_pins: Arc::clone(&self.schema_pins),
            connection_id: context.connection_id,
            transaction_pin: None,
            historical_stats_worker: Arc::clone(&self.historical_stats_worker),
        })
    }

    fn session_manager(&self) -> Option<Arc<dyn tidb_util::memoryusagealarm::SessionManager>> {
        Some(Arc::new(self.processes.clone()))
    }
}

fn stats_session_error(message: impl Into<String>) -> tidb_sqlexec::SqlExecError {
    Box::new(std::io::Error::other(message.into()))
}

struct ClusterStatsSessionState {
    session: Mutex<Option<ClusterServerSession>>,
    global_vars: GlobalSysvars,
    registered: std::sync::atomic::AtomicBool,
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
        callback: impl FnOnce(&mut ClusterServerSession) -> Result<T, tidb_sqlexec::SqlExecError>,
    ) -> Result<T, tidb_sqlexec::SqlExecError> {
        let mut slot = self
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let session = slot
            .as_mut()
            .ok_or_else(|| stats_session_error("system session is closed"))?;
        callback(session)
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
                .execute(sql)
                .map_err(|error| stats_session_error(error.message))?;
            let source = result.source();
            let columns = source.columns().map_err(stats_session_error)?;
            let mut rows = Vec::new();
            loop {
                let batch = source.next_batch(1024).map_err(stats_session_error)?;
                if batch.is_empty() {
                    break;
                }
                rows.extend(batch);
            }
            source.finish().map_err(stats_session_error)?;
            source.close().map_err(stats_session_error)?;
            Ok((rows, stats_result_fields(columns)))
        })
    }

    fn connection_id(&self) -> Result<u64, tidb_sqlexec::SqlExecError> {
        self.with_session(|session| Ok(session.connection_id))
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
            track(option.track_sys_proc_id, Arc::new(()))?;
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
            session
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
        let sql = String::from_utf8(statement.text().to_vec())?;
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
        let sql = String::from_utf8(statement.text().to_vec())?;
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
    fn new(session: ClusterServerSession, global_vars: GlobalSysvars) -> Self {
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
        callback: impl FnOnce(&mut ClusterServerSession) -> Result<T, tidb_sqlexec::SqlExecError>,
    ) -> Result<T, tidb_sqlexec::SqlExecError> {
        self.state.with_session(callback)
    }

    fn set_var(&self, name: &str, value: impl Into<String>) {
        let value = value.into();
        self.with_session(
            |session| match session.session.set_internal_system_var(name, value) {
                Ok(()) | Err(tidb_session::VarError::GlobalOnlyVariable(_)) => Ok(()),
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

    fn connection_id(&self) -> Result<u64, tidb_sqlexec::SqlExecError> {
        self.state.connection_id()
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
                .map(|value| tidb_exec::option_values::tidb_opt_on(&value))
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
                .execute_write("ROLLBACK")
                .map_err(|error| stats_session_error(error.message))?;
            Ok(())
        });
    }

    fn has_prepared_txn_future(&self) -> bool {
        false
    }

    fn txn_valid(&self) -> Result<bool, tidb_sqlexec::SqlExecError> {
        self.with_session(|session| Ok(session.explicit.is_some()))
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

    fn set_analyze_partition_merge_concurrency(&self, concurrency: i64) {
        self.set_var(
            tidb_vardef::tidb_vars::TIDB_MERGE_PARTITION_STATS_CONCURRENCY,
            concurrency.to_string(),
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
            if let Some(transaction) = session.explicit.as_ref() {
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
    factory: Weak<ClusterSessionFactory>,
    stats_lease: Duration,
    session_pool: Arc<tidb_syssession::AdvancedSessionPool<ClusterStatsSessionContext>>,
}

impl ClusterPriorityQueueSource {
    fn factory(&self) -> Result<Arc<ClusterSessionFactory>, String> {
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
                let process_id = context.connection_id()?;
                let generator = tidb_stats_handle_util::Generator::new(move || process_id, |_| {});
                let track: tidb_sqlexec::TrackSysProc = Arc::new(|_, _| Ok(()));
                let untrack: tidb_sqlexec::UntrackSysProc = Arc::new(|_| {});
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

struct WorkloadRepositorySessionPool {
    factory: std::sync::Weak<ClusterSessionFactory>,
    next_connection_id: std::sync::atomic::AtomicU64,
}

impl WorkloadRepositorySessionPool {
    fn new(factory: &Arc<ClusterSessionFactory>) -> Self {
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
                peer_addr: "127.0.0.1:0".parse().expect("loopback socket address"),
                identity: crate::configured_user_store::AuthenticatedIdentity::internal(),
                secure_transport: false,
                tls_status: None,
                cancellation: crate::sql_node::ConnectionCancellation::default(),
                close: crate::sql_node::ConnectionClose::default(),
            })
            .map_err(|error| error.message)?;
        Ok(Box::new(WorkloadRepositorySession { session }))
    }
}

struct WorkloadRepositorySession {
    session: ClusterServerSession,
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
        if self
            .session
            .execute_write(&sql)
            .map_err(|error| error.message)?
            .is_some()
        {
            return Ok(Vec::new());
        }
        let mut result = self.session.execute(&sql).map_err(|error| error.message)?;
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(256)?;
            if batch.is_empty() {
                break;
            }
            for row in batch {
                rows.push(
                    row.into_iter()
                        .map(workload_arg_from_datum)
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
        }
        source.finish()?;
        source.close()?;
        Ok(rows)
    }

    fn schema_exists(&self, schema: &str) -> bool {
        self.session
            .session
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
        let catalog = self.session.session.shared_catalog();
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

/// One connection's wide-SQL session over cluster storage.
pub struct ClusterServerSession {
    session: Session,
    /// Go Domain's node-global pending statistics deltas.
    stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    /// Process-global variables consulted while persisting statistics deltas.
    global_vars: GlobalSysvars,
    /// This connection's staged writes, published by `COMMIT` (or by the end
    /// of an autocommit statement).
    buffer: MutationBuffer,
    /// The slot every table of `session` reads through; rebound per statement.
    slot: Arc<Mutex<SwappableSnapshot>>,
    /// The handles every table of `session` was built over, kept so the
    /// connection's catalog can be rebuilt after a DDL without disturbing the
    /// snapshot slot or the staged writes.
    storage: ClusterTableStorage,
    transactions: Arc<dyn ClusterTransactions>,
    /// The route a stored-schema change takes; see [`ClusterDdl`].
    ddl: Arc<dyn ClusterDdl>,
    /// The route a stored-account change takes; see
    /// [`crate::cluster_account_seam`].
    accounts: Arc<dyn ClusterAccountWriter>,
    /// The route a `SET GLOBAL` change takes; see
    /// [`crate::cluster_sysvar_seam`].
    sysvars: Arc<dyn ClusterSysvarWriter>,
    /// The route an `ANALYZE TABLE` takes; see
    /// [`crate::cluster_analyze_seam`].
    analyze: Arc<dyn ClusterAnalyze>,
    /// Go's node-global `pdhelper.GlobalPDHelper`, shared by SHOW and ANALYZE.
    approximate_table_counts: Arc<ClusterApproximateTableCountProvider>,
    /// The route a persisted statistics-lock operation takes.
    stats_lock: Arc<dyn ClusterStatsLock>,
    /// The node's catalog, which this connection follows.
    catalog: Arc<SharedClusterCatalog>,
    /// The schema version `session`'s tables were built from. A move in
    /// `catalog` past this is what makes the connection rebuild them.
    schema_version: i64,
    /// The node's statistics, republished on its own cadence by the stats
    /// reload thread -- an `ANALYZE` changes these without changing the
    /// schema version, so they are followed separately.
    stats: Arc<SharedStats>,
    /// The exact snapshot this connection's catalog carries. A `store` on
    /// `stats` always publishes a NEW `Arc`, so pointer identity is what
    /// tells the connection its statistics moved.
    statistics: Arc<tidb_exec::stats_watch::StatsSnapshot>,
    /// The transaction an explicit `BEGIN` holds open. `None` is autocommit,
    /// where a statement prepares a timestamp of its own after planning and
    /// waits for it at its first read.
    explicit: Option<Box<dyn OpenClusterTransaction>>,
    /// The transaction's savepoints, oldest first: for each, the name
    /// lowercased and the buffer image taken when it was declared.
    ///
    /// The driver session owns the savepoint RULES -- which names exist, which
    /// ones a `ROLLBACK TO` or `RELEASE` drops, and the 1305 an unknown name
    /// reports -- exactly as it owns `in_transaction`. This owns what those
    /// rules mean for cluster storage, where the session's catalog image
    /// restores nothing (every table shares one `Arc` buffer). It is the same
    /// `MutationBuffer::staged()`/`restore()` pair
    /// [`ClusterServerSession::with_bound_statement`] already uses for
    /// statement-level rollback, held under a name.
    ///
    /// The two stacks stay in step because both apply the same rules to the
    /// same statement sequence, and the session's error arm runs FIRST -- a
    /// name this stack could not find is one the session already refused.
    savepoints: Vec<(
        String,
        BufferCheckpoint,
        std::collections::HashMap<i64, tidb_stats_handle_usage::TableDelta>,
    )>,
    /// Tables of the cluster this connection's catalog could not include,
    /// answered by name when a statement names one. Rebuilt with the catalog.
    skipped: Vec<SkippedTable>,
    /// The node's auto-increment allocators, needed on every catalog rebuild
    /// so the rebuilt tables keep the ranges the old ones had reserved.
    auto_ids: Arc<dyn TableAutoIds>,
    /// The node's MDL gate; statements and the explicit transaction register
    /// the catalog version they run on. See [`schema_sync::SchemaPinRegistry`].
    schema_pins: Arc<schema_sync::SchemaPinRegistry>,
    /// This connection's id, the registry's key.
    connection_id: u64,
    /// Held from `BEGIN` to `COMMIT`/`ROLLBACK` (dropped with the session on
    /// a disconnect): while it lives, the schema-sync acknowledger reports
    /// nothing newer than this transaction's catalog to a Go DDL owner --
    /// Go's metadata lock, at transaction scope.
    transaction_pin: Option<schema_sync::SchemaPinGuard>,
    /// The domain-global mailbox successful ANALYZE results enqueue into.
    historical_stats_worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
}

/// The session layer's next move after a pessimistic statement's lock step.
enum PessimisticStep {
    /// The statement stands; its keys are locked.
    Done,
    /// Re-execute the statement reading at this advanced `for_update_ts`.
    Retry {
        /// The statement timestamp the replay reads at.
        for_update_ts: u64,
    },
}

impl ClusterServerSession {
    /// The tables this connection's catalog left out, with their reasons.
    #[must_use]
    pub fn skipped_tables(&self) -> &[SkippedTable] {
        &self.skipped
    }

    fn run_flush_stats_delta(
        &mut self,
        targets: &FlushStatsDeltaTargets,
    ) -> Result<WriteOutcome, SqlQueryError> {
        if matches!(targets, FlushStatsDeltaTargets::Tables(ids) if ids.is_empty()) {
            return Ok(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            });
        }
        self.session.publish_table_delta();
        let resource_group = self.session.current_resource_group().to_owned();
        let target_ids = match targets {
            FlushStatsDeltaTargets::All => &[][..],
            FlushStatsDeltaTargets::Tables(ids) => ids.as_slice(),
        };
        ClusterSessionFactory::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.transactions.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            true,
            target_ids,
            &resource_group,
        )
        .map_err(SqlQueryError::unknown)?;
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Runs one statement inside this mode's snapshot/buffer lifecycle. When
    /// point-write keys are already known, they are locked WITH their rows
    /// before the snapshot is bound, so the statement's own read answers from
    /// the lock response instead of storage (Go's `InitReturnValues`/
    /// `SetPessimisticLockCache` fold, `pkg/executor/point_get.go:612-624`).
    /// An empty key set is the ordinary lifecycle.
    fn with_prelocked_statement<T>(
        &mut self,
        shape: StatementReadShape,
        prelock_keys: Vec<Vec<u8>>,
        resource_group: &str,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.begin_if_autocommit_off(resource_group)?;
        self.with_bound_statement(shape, &prelock_keys, resource_group, run)
    }

    /// Runs work the CLIENT did not ask to execute through the statement
    /// lifecycle: the PREPARE probe, which executes a query only to learn its
    /// result columns.
    ///
    /// The one thing it must not do is open the transaction `autocommit = 0`
    /// implies. Go's `PrepareStmt` calls `PrepareTxnCtx` too
    /// (`pkg/session/session.go:3171`), but with a nil statement and through
    /// `EnterNewTxnBeforeStmt`, which leaves the transaction *pending*: no
    /// timestamp is spent and the first statement that really reads is what
    /// takes one. Captured: `@@tidb_current_ts` is `0` after a PREPARE under
    /// `autocommit = 0`, and a read issued after another session's commit sees
    /// the NEW value. Opening it here would instead pin this connection's
    /// `start_ts` at PREPARE time, so every later statement of the
    /// transaction, and the conflict check of its commit, would live at a
    /// timestamp the client never asked for.
    ///
    /// An already-open transaction is read through as usual -- a PREPARE
    /// inside `BEGIN` costs no timestamp either way -- so this differs from
    /// [`Self::with_prelocked_statement`] in exactly one thing: it never OPENS
    /// one.
    fn probe_statement<T>(
        &mut self,
        shape: StatementReadShape,
        resource_group: &str,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.with_bound_statement(shape, &[], resource_group, run)
    }

    /// The statement lifecycle proper: savepoint, attempt, replay budget.
    fn with_bound_statement<T>(
        &mut self,
        shape: StatementReadShape,
        prelock_keys: &[Vec<u8>],
        resource_group: &str,
        mut run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        // Go's MDL considers a RUNNING statement a user of its schema
        // version; this hold is what keeps a Go DDL owner from publishing a
        // drop under a statement mid-flight. Dropped with the scope.
        let _statement_pin = self
            .schema_pins
            .hold(self.connection_id, self.schema_version);
        let savepoint = self.buffer.checkpoint();
        let delta_savepoint = self.session.table_delta_savepoint();
        let mut retried: u32 = 0;
        let outcome = loop {
            match self.attempt_statement(
                shape,
                savepoint.clone(),
                &prelock_keys,
                resource_group,
                &mut run,
            ) {
                Ok(value) => break Ok(value),
                Err(error) => {
                    self.session
                        .restore_table_delta_savepoint(delta_savepoint.clone());
                    if !self.may_retry_autocommit_statement(&error, retried) {
                        break Err(error);
                    }
                    retried += 1;
                    back_off(retried);
                    // The ids the losing attempt assigned carry into the
                    // replay -- Go's `RetryInfo.ResetOffset`
                    // (`pkg/session/session.go:1197`). The IDS cross between
                    // attempts; the timestamp must not, which is why this
                    // rewinds a list and touches nothing about the snapshot.
                    self.session
                        .retry_auto_ids()
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .begin_attempt();
                    // Go's replay calls `RebuildPlan` per attempt
                    // (`pkg/session/session.go:1207`), so a schema that moved
                    // under the conflict is picked up before the next try.
                    self.rebuild_catalog_if_stale();
                }
            }
        };
        // Go's `cleanRetryInfo` (`pkg/session/session.go:329-336`, deferred
        // from `doCommitWithRetry`): the ids belong to the statement that is
        // now over, however it ended. The next statement's rows are not these
        // rows, and reusing an id across statements would write the same id
        // twice.
        self.session
            .retry_auto_ids()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clean();
        self.session
            .publish_transaction_buffer_metrics(self.buffer.len(), self.buffer.memory_footprint());
        outcome
    }

    /// Runs one attempt of [`Self::with_bound_statement`]'s lifecycle.
    fn attempt_statement<T>(
        &mut self,
        shape: StatementReadShape,
        savepoint: BufferCheckpoint,
        prelock_keys: &[Vec<u8>],
        resource_group: &str,
        run: &mut impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        let autocommit = self.explicit.is_none();
        if autocommit {
            self.session.current_tso().clear();
        }
        // The statement's own timestamp, filled in by its first read and read
        // back by its publication after the read handle is gone. Autocommit
        // publishes THERE, not at a fresh one: see `StatementReadTs`.
        let read_ts = transactions::StatementReadTs::new(self.session.current_tso());
        let result = self.attempt_statement_inner(
            shape,
            &savepoint,
            prelock_keys,
            resource_group,
            run,
            &read_ts,
        );
        // The TSO clear is statement TEARDOWN, not a success step: every exit
        // of the attempt -- including the `?`s inside the loop -- must leave
        // no failed statement's timestamp published, or `SET @x =
        // @@tidb_current_ts` after a failed autocommit statement reads a
        // timestamp of a transaction that is not open.
        if autocommit {
            self.session.current_tso().clear();
        }
        result
    }

    /// The pessimistic statement loop of [`Self::attempt_statement`]; split
    /// out so the caller can run teardown on EVERY exit, `?`s included.
    fn attempt_statement_inner<T>(
        &mut self,
        shape: StatementReadShape,
        savepoint: &BufferCheckpoint,
        prelock_keys: &[Vec<u8>],
        resource_group: &str,
        run: &mut impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
        read_ts: &transactions::StatementReadTs,
    ) -> Result<T, SqlQueryError> {
        if let Some(transaction) = self.explicit.as_ref() {
            transaction
                .set_resource_group_name(resource_group)
                .map_err(SqlQueryError::unknown)?;
        }
        let write_transaction = (self.explicit.is_none()
            && shape == StatementReadShape::AutocommitWrite)
            .then(|| Arc::new(Mutex::new(None)));
        // Go `handlePessimisticDML`'s statement loop: run, lock the staged
        // keys, and on a lock conflict roll the STATEMENT back and re-execute
        // it reading at the advanced `for_update_ts`. `None` is the first
        // attempt, reading at the transaction's own snapshot.
        let mut retry_read_ts: Option<u64> = None;
        // The keys THIS statement's rounds fair-locked and retained; released
        // if the statement ultimately fails (Go `OnPessimisticStmtEnd`).
        let mut statement_locked: Vec<Vec<u8>> = Vec::new();
        // Go `PessimisticTxn.MaxRetryCount` (`pkg/config/config.go`, default
        // 256): the safety valve on the statement retry, with Go's own error.
        let mut retries: u32 = 0;
        const MAX_PESSIMISTIC_STATEMENT_RETRIES: u32 = 256;
        let result = loop {
            // Go's pessimistic point write takes its row lock DURING execution
            // (`PointGetExecutor.getAndLock`, `pkg/executor/point_get.go:549`),
            // asking TiKV to answer the row WITH the lock (`InitReturnValues`,
            // line 614) so the executor's one read costs no separate get. This
            // is that fold's session half: lock the classified keys BEFORE any
            // read exists, and the statement's row read is served from the
            // lock response through the transaction's value cache.
            // Every round re-attempts until the lock stands: a fair-locking
            // conflict keeps its locks (the next attempt here is answered
            // without an RPC by the worker's held-key filter), while a rolled-
            // back conflict genuinely needs the fresh acquisition at the new
            // `for_update_ts`.
            if !prelock_keys.is_empty() {
                let outcome = match self.explicit.as_ref() {
                    Some(transaction) => {
                        self.session.publish_transaction_lock_waiting(true);
                        let outcome =
                            transaction.lock_staged_keys_with_values(prelock_keys.to_vec());
                        self.session.publish_transaction_lock_waiting(false);
                        Some(outcome)
                    }
                    None => None,
                };
                match outcome {
                    None => {}
                    Some(Ok(LockKeysOutcome::Locked { newly_locked, .. })) => {
                        // A FAILED statement releases exactly what its rounds
                        // added (Go `OnPessimisticStmtEnd(isSuccessful=false)`);
                        // a pre-locked key joins the same list as a post-run
                        // delta key, so a duplicate-key failure AFTER the lock
                        // cannot leak the row lock it took first.
                        statement_locked.extend(newly_locked);
                    }
                    Some(Ok(LockKeysOutcome::RetryStatement {
                        for_update_ts,
                        newly_locked,
                        ..
                    })) => {
                        // Fair locking's retained locks stay owned by this
                        // statement until it ends, whichever way it ends.
                        statement_locked.extend(newly_locked);
                        if retries >= MAX_PESSIMISTIC_STATEMENT_RETRIES {
                            break Err(SqlQueryError::unknown(
                                "pessimistic lock retry limit reached",
                            ));
                        }
                        retries += 1;
                        // Nothing is staged yet -- this round never ran -- so
                        // only the read timestamp moves; the next round's lock
                        // attempt re-runs at it.
                        retry_read_ts = Some(for_update_ts);
                        continue;
                    }
                    // Statement-scoped 1205/1213 family and transaction-fatal
                    // errors surface exactly as they do from the post-run lock
                    // step; nothing needs rolling back, nothing was staged.
                    Some(Ok(LockKeysOutcome::StatementError(error))) => {
                        break Err(transactions::sql_error(error))
                    }
                    Some(Ok(LockKeysOutcome::TransactionError(error))) => {
                        break Err(transactions::sql_error(error))
                    }
                    Some(Err(error)) => break Err(SqlQueryError::unknown(error)),
                }
            }
            let snapshot = match self.explicit.as_ref() {
                // The transaction's timestamp is already spent; its
                // per-statement read handle costs nothing, so there is
                // nothing to defer.
                Some(transaction) => {
                    // Go's `e.lock`: the pessimistic lock cache may answer a
                    // read only when the statement itself takes locks
                    // (`pkg/executor/point_get.go:677`). A statement takes
                    // locks exactly when it produced prelock keys --
                    // `pessimistic_statement_prelock_keys` yields them for a
                    // point write and for `SELECT ... FOR UPDATE`, and
                    // nothing else (`access_path.rs`, `select.lock.is_none()`
                    // refuses the plain read). Without this gate the cached
                    // row -- captured at the LOCK's `for_update_ts` -- is
                    // served to a later plain `SELECT` that must read at
                    // `start_ts`, which silently breaks repeatable read.
                    let locking = !prelock_keys.is_empty();
                    match retry_read_ts {
                        Some(for_update_ts) => transaction
                            .snapshot_at_for(for_update_ts, locking)
                            .map_err(SqlQueryError::unknown)?,
                        None => transaction
                            .snapshot_for(locking)
                            .map_err(SqlQueryError::unknown)?,
                    }
                }
                None if matches!(
                    shape,
                    StatementReadShape::AutocommitPointGet
                        | StatementReadShape::AutocommitSingleRowRead
                ) =>
                {
                    // Go's clustered-handle point-get optimisation reads
                    // directly at MaxTS. Keep this on the connection worker:
                    // opening a reusable transaction would add unnecessary
                    // transaction state to every point read.
                    self.transactions
                        .open_max_ts_snapshot(resource_group)
                        .map_err(SqlQueryError::unknown)?
                }
                // Start the ordinary timestamped snapshot while the session
                // binds and builds the DML read path. The first read consumes
                // the prefetched result; publication uses the same owner.
                None if shape == StatementReadShape::AutocommitWrite => {
                    transactions::prefetched_write_snapshot(
                        Arc::clone(&self.transactions),
                        read_ts.clone(),
                        Arc::clone(
                            write_transaction
                                .as_ref()
                                .expect("autocommit write created its transaction handoff"),
                        ),
                        prelock_keys.to_vec(),
                        Arc::<str>::from(resource_group),
                    )
                }
                // Binding is still timestamp-free. After the statement's
                // shape is declared below, preparation starts the ordinary
                // future; the first read is what waits for and exposes its
                // snapshot.
                None => transactions::deferred_snapshot(
                    Arc::clone(&self.transactions),
                    read_ts.clone(),
                    Arc::<str>::from(resource_group),
                ),
            };
            if let Some(stale) = self.bind(snapshot) {
                // A previous statement that did not unbind would otherwise
                // leave its read transaction open for the rest of the
                // connection.
                drop(stale);
            }
            self.declare_read_shape(shape);
            if let Err(error) = self.prepare_snapshot() {
                Self::rollback_prefetched_write(write_transaction.clone());
                return Err(error);
            }
            let outcome = run(&mut self.session);
            let finished = self.finish_snapshot();
            match outcome {
                Ok(value) => {
                    if let Err(error) = finished {
                        Self::rollback_prefetched_write(write_transaction.clone());
                        break Err(error);
                    }
                    match self.lock_pessimistic_statement_keys(savepoint, &mut statement_locked) {
                        Ok(PessimisticStep::Done) => {}
                        Ok(PessimisticStep::Retry { for_update_ts }) => {
                            if retries >= MAX_PESSIMISTIC_STATEMENT_RETRIES {
                                // Go `handlePessimisticLockError`
                                // (`pkg/executor/adapter.go`): the retry
                                // budget is the transaction config's, and the
                                // message is Go's own.
                                self.buffer.restore(savepoint.clone());
                                break Err(SqlQueryError::unknown(
                                    "pessimistic lock retry limit reached",
                                ));
                            }
                            retries += 1;
                            // The statement's writes go back; its locks STAY
                            // (fair locking's whole point), and the replay
                            // reads at the timestamp that sees the version
                            // that beat it.
                            self.buffer.restore(savepoint.clone());
                            retry_read_ts = Some(for_update_ts);
                            continue;
                        }
                        Err(error) => break Err(error),
                    }
                    match self.commit_if_session_left_transaction().and_then(|()| {
                        self.flush_if_autocommit(
                            read_ts.get(),
                            write_transaction.clone(),
                            resource_group,
                        )
                    }) {
                        Ok(()) => break Ok(value),
                        Err(error) => break Err(error),
                    }
                }
                Err(error) => {
                    Self::rollback_prefetched_write(write_transaction.clone());
                    // The statement's own writes go; every earlier
                    // statement's writes in this transaction stay.
                    self.buffer.restore(savepoint.clone());
                    break Err(error);
                }
            }
        };
        if result.is_err() {
            Self::rollback_prefetched_write(write_transaction);
            // Go `OnPessimisticStmtEnd(isSuccessful=false)`: the locks a
            // FAILED statement's rounds accumulated go back, or a contender
            // blocks on them for the transaction's remaining lifetime. Best
            // effort -- a dead transaction thread has already released
            // everything by rolling the whole transaction back.
            if let Some(transaction) = self.explicit.as_ref() {
                let _ = transaction.release_statement_locks(std::mem::take(&mut statement_locked));
            }
        }
        result
    }

    /// Go `handlePessimisticDML`'s lock step for the statement that just ran:
    /// what the transaction says about the keys it staged.
    fn lock_pessimistic_statement_keys(
        &mut self,
        savepoint: &BufferCheckpoint,
        statement_locked: &mut Vec<Vec<u8>>,
    ) -> Result<PessimisticStep, SqlQueryError> {
        let Some(transaction) = self.explicit.as_ref() else {
            return Ok(PessimisticStep::Done);
        };
        if !transaction.is_pessimistic() {
            return Ok(PessimisticStep::Done);
        }
        let (before, after) = self.buffer.delta_since(*savepoint);
        let keys = tidb_exec::cluster_table_storage::pessimistic_lock_delta(&before, &after);
        if keys.is_empty() {
            return Ok(PessimisticStep::Done);
        }
        // Go `getPessimisticLazyCheckMode` (`pkg/executor/insert.go:346-350`):
        // the default ON checks lazy INSERT assertions in LockKeys, while OFF
        // inside an explicit client transaction defers them to prewrite.
        let check_in_lock = self
            .session
            .vars()
            .get_system("tidb_constraint_check_in_place_pessimistic")
            .is_ok_and(|value| value.eq_ignore_ascii_case("on") || value == "1");
        let key_set: std::collections::BTreeSet<Vec<u8>> = keys.iter().cloned().collect();
        let presume_not_exists = if check_in_lock {
            self.buffer
                .presume_not_exists_keys()
                .into_iter()
                .filter(|key| key_set.contains(key))
                .collect()
        } else {
            std::collections::BTreeSet::new()
        };
        let duplicate_hints = presume_not_exists
            .iter()
            .filter_map(|key| {
                self.buffer
                    .duplicate_key_hint_for(key)
                    .map(|hint| (key.clone(), hint))
            })
            .collect();
        // Every error exit rolls the STATEMENT back -- Go's `StmtRollback`
        // runs on any statement error, transport failures included.
        self.session.publish_transaction_lock_waiting(true);
        let lock_result =
            transaction.lock_staged_keys_with_assertions(keys, presume_not_exists, duplicate_hints);
        self.session.publish_transaction_lock_waiting(false);
        let outcome = match lock_result {
            Ok(outcome) => outcome,
            Err(error) => {
                self.buffer.restore(savepoint.clone());
                return Err(SqlQueryError::unknown(error));
            }
        };
        match outcome {
            LockKeysOutcome::Locked { newly_locked, .. } => {
                statement_locked.extend(newly_locked);
                Ok(PessimisticStep::Done)
            }
            LockKeysOutcome::RetryStatement {
                for_update_ts,
                newly_locked,
            } => {
                statement_locked.extend(newly_locked);
                Ok(PessimisticStep::Retry { for_update_ts })
            }
            LockKeysOutcome::StatementError(error) => {
                // Statement-scoped, Go's 1205/1213 family: this statement's
                // writes go, the transaction stays open.
                self.buffer.restore(savepoint.clone());
                Err(transactions::sql_error(error))
            }
            LockKeysOutcome::TransactionError(error) => {
                // The transaction is no longer usable; later statements and
                // `ROLLBACK` report that state on their own.
                self.buffer.restore(savepoint.clone());
                Err(transactions::sql_error(error))
            }
        }
    }

    /// Whether the statement that just failed is one Go would have retried
    /// internally instead of reporting.
    ///
    /// This is `isOptimisticTxnRetryable`
    /// (`pkg/sessiontxn/isolation/optimistic.go:66-104`) reduced to the only
    /// arm this node can reach. Go's `!sessVars.InTxn()` early return is the
    /// whole reason `tidb_disable_txn_auto_retry` -- which defaults to `true`
    /// (`pkg/sessionctx/vardef/tidb_vars.go:1528`) -- does not stop this:
    /// that variable is consulted eleven lines LATER, and only a
    /// multi-statement transaction ever gets there. A single autocommit
    /// statement has no earlier statements whose reads a replay could
    /// falsify, so it is retried unconditionally. Inside `BEGIN` we refuse,
    /// which is Go with the default variable.
    ///
    /// The error test is Go's `kv.IsTxnRetryableError` (`pkg/kv/error.go:85`)
    /// narrowed to the code that reaches this seam: only a write conflict.
    ///
    /// # The one thing this replay is NOT, and what it costs
    ///
    /// Go's replay runs in PESSIMISTIC mode whatever `@@tidb_txn_mode` says:
    /// `retry` calls `PrepareTxnCtx(ctx, nil)` (`pkg/session/session.go:1194`)
    /// while `RetryInfo.Retrying` is set, and `decideTxnMode`
    /// (`session.go:4921-4923`) returns `ast.Pessimistic` unconditionally for
    /// that. This node's replay stays optimistic, because it has no
    /// pessimistic statement path at all -- the seam is
    /// `RealOptimisticTransactionOpener` from end to end.
    ///
    /// What that does NOT cost is a lost update. The replay re-runs the whole
    /// statement against its own fresh timestamp and publishes at that same
    /// one, so a value derived from a stale read cannot reach the store: see
    /// `an_autocommit_update_that_loses_the_race_is_retried_at_a_new_read`,
    /// and the exhausted-budget case still reports 9007 rather than
    /// succeeding.
    ///
    /// What it does cost is the failure mode under a lock that is HELD rather
    /// than a commit that already landed. Measured on TiDB over `mockstore`
    /// (2026-08-03), with a second session holding a pessimistic lock on the
    /// row and `innodb_lock_wait_timeout = 1`:
    ///
    /// ```text
    /// s1 @@tidb_txn_mode -> pessimistic
    /// s1 @@tidb_disable_txn_auto_retry -> 1
    /// s2 holds a pessimistic lock on id=1
    /// s1 autocommit update -> [tikv:1205]Lock wait timeout exceeded; try
    ///                         restarting transaction
    /// ```
    ///
    /// Go WAITS on the lock -- 1205 is a pessimistic-lock error, so the replay
    /// really did take locks -- and against a lock that clears in time it
    /// succeeds. This node cannot wait: it spends its budget of re-reads and
    /// reports 9007. Closing that needs the pessimistic path
    /// (`tidb_txnkv::transaction::pessimistic::RealPessimisticTransaction`
    /// exists; nothing at this tier drives it), NOT a mode flag: locks have to
    /// be taken while the statement reads, and taking them after it has
    /// computed from a stale read would introduce exactly the lost update this
    /// seam exists to prevent.
    fn may_retry_autocommit_statement(&self, error: &SqlQueryError, retried: u32) -> bool {
        error.code == ERR_WRITE_CONFLICT
            && retried < AUTOCOMMIT_RETRY_LIMIT
            && self.explicit.is_none()
            && !self.session.in_transaction()
    }

    /// Tells the just-bound snapshot what shape the statement's whole read is,
    /// before the statement runs and therefore before its first read.
    ///
    /// This is Go's `AdviseOptimizeWithPlan`, and the position is the point of
    /// it: after the plan-shape question has been answered and before any
    /// timestamp has been spent. The declaration reaches only a snapshot that
    /// can take it -- an explicit transaction's read handle refuses by
    /// inheriting the trait default, which is `IsAutoCommitTxn`'s `!InTxn`
    /// half made structural rather than re-asked here.
    fn declare_read_shape(&self, shape: StatementReadShape) {
        if !matches!(
            shape,
            StatementReadShape::AutocommitPointGet | StatementReadShape::AutocommitSingleRowRead
        ) {
            return;
        }
        self.slot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .declare_autocommit_point_get();
    }

    fn prepare_snapshot(&self) -> Result<(), SqlQueryError> {
        self.slot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .prepare()
            .map_err(|error| SqlQueryError::unknown(error.to_string()))
    }

    fn bind(&self, snapshot: Box<dyn ClusterSnapshot>) -> Option<Box<dyn ClusterSnapshot>> {
        self.slot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .bind(snapshot)
    }

    /// Unbinds the statement's snapshot, ending the statement.
    ///
    /// Dropping an autocommit statement's handle finishes its read transaction
    /// on its own thread; dropping an explicit transaction's handle ends only
    /// the statement, because the transaction outlives it. Either way the drop
    /// is what makes the ordering unconditional.
    fn finish_snapshot(&self) -> Result<(), SqlQueryError> {
        let bound = self
            .slot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .unbind();
        drop(bound);
        Ok(())
    }

    /// Opens the transaction `autocommit = 0` implies, for the statement about
    /// to run.
    ///
    /// `SET autocommit = 0` is the third door onto this connection's
    /// transaction state, and the only one that carries no keyword: there is no
    /// `BEGIN` for [`classify_transaction_control`] to route on, just a
    /// variable the driver session turns OFF. So the variable IS the routing
    /// question, and asking the session for it -- rather than tracking a copy
    /// here -- is what keeps the two halves from disagreeing the way they did
    /// when a prepared `BEGIN` flipped one and not the other.
    ///
    /// The timing is Go's, not MySQL's `START TRANSACTION`: the `SET` itself
    /// takes no timestamp, and the transaction begins at the first statement
    /// that reads or writes data (captured). DDL, account and `SET GLOBAL`
    /// statements never reach here -- each commits the open transaction first,
    /// as Go does -- so none of them opens one either.
    fn begin_if_autocommit_off(&mut self, resource_group: &str) -> Result<(), SqlQueryError> {
        if self.explicit.is_some() || self.session.is_autocommit() {
            return Ok(());
        }
        self.open_explicit(resource_group)
    }

    fn open_explicit(&mut self, resource_group: &str) -> Result<(), SqlQueryError> {
        // Go `newProviderWithRequest`: `BEGIN <mode>` wins, a bare `BEGIN`
        // falls back to `@@tidb_txn_mode` -- whose default is PESSIMISTIC
        // (`vardef.DefTiDBTxnMode`). The session resolved the keyword half at
        // `BEGIN`; the variable half covers `SET autocommit = 0`, which
        // carries no keyword.
        let pessimistic = match self.session.txn_mode() {
            Some(mode) => mode.is_pessimistic(),
            None => tidb_planner::txn_mode::txn_mode_variable(
                &self
                    .session
                    .vars()
                    .get_system("tidb_txn_mode")
                    .unwrap_or_default(),
            )
            .is_pessimistic(),
        };
        let transaction = self
            .transactions
            .begin(pessimistic, resource_group)
            .map_err(SqlQueryError::unknown)?;
        self.session.current_tso().publish(transaction.start_ts());
        self.explicit = Some(transaction);
        // The transaction reads this catalog version until it ends; the pin
        // is what a Go owner's `WaitVersionSynced` waits out (Go
        // `CheckOldRunningTxn`, at this port's whole-transaction scope).
        self.transaction_pin = Some(
            self.schema_pins
                .hold(self.connection_id, self.schema_version),
        );
        Ok(())
    }

    /// Publishes the transaction a statement ended from the INSIDE.
    ///
    /// One statement does that: `SET autocommit = 1`, whose Go `SetSession`
    /// closure ends the ongoing transaction on the OFF->ON transition, so the
    /// statement's own finish commits it (captured: the row is durable
    /// immediately, and a `ROLLBACK` after it has nothing left to discard).
    /// The driver session applies that rule and clears its own transaction; the
    /// node hears about it only by asking, because no keyword went past
    /// [`Self::control_transaction`].
    ///
    /// The check is on the STATE rather than on the transition, so any future
    /// statement the session ends a transaction from is covered by the same
    /// line: an `explicit` the session no longer believes in is always one to
    /// publish.
    fn commit_if_session_left_transaction(&mut self) -> Result<(), SqlQueryError> {
        if self.explicit.is_none() || self.session.in_transaction() {
            return Ok(());
        }
        self.commit_explicit()
    }

    /// Publishes the buffer when the session is not inside `BEGIN`.
    ///
    /// An empty buffer -- every read statement -- publishes nothing and spends
    /// no timestamp, as a Go COMMIT of a transaction that wrote nothing does.
    fn flush_if_autocommit(
        &mut self,
        read_ts: Option<u64>,
        write_transaction: Option<transactions::WriteTransactionSlot>,
        resource_group: &str,
    ) -> Result<(), SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            return Ok(());
        }
        let write_details = self.buffer_write_details();
        if let Some(write_transaction) = write_transaction {
            let transaction = write_transaction
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
            if let Some(transaction) = transaction {
                return match transaction.commit(&self.buffer) {
                    Ok(()) => {
                        self.record_write_details(write_details);
                        self.session.publish_table_delta();
                        Ok(())
                    }
                    Err(error) => {
                        self.buffer.reset();
                        self.session.clear_table_delta();
                        Err(error)
                    }
                };
            }
        }
        let outcome = self.commit_autocommit_buffer(read_ts, resource_group);
        if outcome.is_ok() {
            self.record_write_details(write_details);
            self.session.publish_table_delta();
        } else {
            self.session.clear_table_delta();
        }
        outcome
    }

    fn buffer_write_details(&self) -> (isize, isize) {
        self.buffer.snapshot().iter().fold(
            (0_isize, 0_isize),
            |(write_size, write_keys), (key, value)| {
                let entry_size = key
                    .as_bytes()
                    .len()
                    .wrapping_add(value.as_ref().map_or(0, Vec::len));
                (
                    write_size.wrapping_add(entry_size as isize),
                    write_keys.wrapping_add(1),
                )
            },
        )
    }

    fn record_write_details(&mut self, (write_size, write_keys): (isize, isize)) {
        if write_size > 0 {
            self.session
                .txn_write_throughput_sli()
                .add_txn_write_size(write_size, write_keys);
        }
    }

    fn rollback_prefetched_write(write_transaction: Option<transactions::WriteTransactionSlot>) {
        let Some(write_transaction) = write_transaction else {
            return;
        };
        let transaction = write_transaction
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .take();
        if let Some(transaction) = transaction {
            let _ = transaction.rollback();
        }
    }

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction, at the timestamp the statement read at. A failed
    /// publication discards them, which is what a failed COMMIT does.
    ///
    /// A publication that lost the race against a commit made after the read is
    /// now a 9007 the client is told about, where publishing at a fresh
    /// timestamp made it a silent overwrite.
    fn commit_autocommit_buffer(
        &mut self,
        read_ts: Option<u64>,
        resource_group: &str,
    ) -> Result<(), SqlQueryError> {
        match self
            .transactions
            .commit(&self.buffer, read_ts, resource_group)
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.buffer.reset();
                Err(error)
            }
        }
    }

    /// Ends the explicit transaction by publishing its buffer at its own start
    /// timestamp.
    ///
    /// A `COMMIT` with no transaction open is the autocommit path: it can only
    /// find writes the previous statement already published, so the buffer is
    /// empty and nothing is spent.
    fn commit_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.savepoints.clear();
        self.session.current_tso().clear();
        self.transaction_pin = None;
        let Some(transaction) = self.explicit.take() else {
            // No transaction and no statement read: the buffer can only hold
            // what a previous statement already published, so there is nothing
            // to publish and no timestamp to publish it at.
            let resource_group = self.session.current_resource_group().to_owned();
            return self.commit_autocommit_buffer(None, &resource_group);
        };
        let write_details = self.buffer_write_details();
        match transaction.commit(&self.buffer) {
            Ok(()) => {
                self.record_write_details(write_details);
                self.session.publish_table_delta();
                Ok(())
            }
            Err(error) => {
                self.buffer.reset();
                self.session.clear_table_delta();
                Err(error)
            }
        }
    }

    /// Applies one savepoint statement to the connection's buffer, after the
    /// driver session has accepted it.
    ///
    /// Each arm is the byte half of the rule the session just applied:
    /// declaring a name replaces any earlier entry and pushes the image on
    /// top; `ROLLBACK TO` puts the named image back and drops the savepoints
    /// above it, keeping the named one so it can be rolled back to again;
    /// `RELEASE` drops the named one and those above it, touching no bytes.
    fn apply_savepoint(&mut self, control: &TransactionControl) -> Result<(), SqlQueryError> {
        match control {
            TransactionControl::Savepoint(name) => {
                // Go's `executeSavepoint` calls `Txn(true)`, so with autocommit
                // OFF the SAVEPOINT is what ACTIVATES the pending transaction:
                // the session just opened its own here, and this must open the
                // node's, or the image below would be taken over a buffer whose
                // transaction has not started (captured: under autocommit = 0,
                // `SAVEPOINT sp; INSERT; ROLLBACK TO sp` leaves the row gone and
                // the COMMIT publishes nothing).
                let resource_group = self.session.current_resource_group().to_owned();
                self.begin_if_autocommit_off(&resource_group)?;
                // In autocommit the statement succeeds while recording nothing,
                // exactly as the session's does -- which is what leaves a later
                // `ROLLBACK TO` to report 1305 (captured).
                if self.explicit.is_none() {
                    return Ok(());
                }
                let name = name.to_lowercase();
                let image = self.buffer.checkpoint();
                let delta = self.session.table_delta_savepoint();
                self.savepoints.retain(|(existing, _, _)| *existing != name);
                self.savepoints.push((name, image, delta));
            }
            TransactionControl::RollbackToSavepoint(name) => {
                let name = name.to_lowercase();
                if let Some(index) = self.savepoints.iter().position(|(sp, _, _)| *sp == name) {
                    self.buffer.restore(self.savepoints[index].1.clone());
                    self.session
                        .restore_table_delta_savepoint(self.savepoints[index].2.clone());
                    self.savepoints.truncate(index + 1);
                }
            }
            TransactionControl::ReleaseSavepoint(name) => {
                let name = name.to_lowercase();
                if let Some(index) = self.savepoints.iter().position(|(sp, _, _)| *sp == name) {
                    self.savepoints.truncate(index);
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Drops the explicit transaction without publishing anything, along with
    /// every write it staged.
    fn discard_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.buffer.reset();
        self.session.clear_table_delta();
        self.savepoints.clear();
        self.session.current_tso().clear();
        self.transaction_pin = None;
        match self.explicit.take() {
            Some(transaction) => transaction.rollback().map_err(SqlQueryError::unknown),
            None => Ok(()),
        }
    }

    /// Rebinds this connection's tables to the node's current catalog.
    ///
    /// A connection's tables are built once, so a table created after it
    /// opened has no entry at all and one dropped still answers. Rebuilding
    /// against the same [`ClusterTableStorage`] handles leaves the snapshot
    /// slot and the staged writes exactly where they are -- the tables are new
    /// objects over the same two shared halves.
    ///
    /// Not inside an explicit transaction: its statements read at one
    /// timestamp, and a schema that moved under them would describe rows that
    /// timestamp cannot see. Go holds the transaction's `InfoSchema` for the
    /// same reason.
    fn rebuild_catalog_if_stale(&mut self) {
        if self.explicit.is_some() || self.session.in_transaction() {
            return;
        }
        self.rebuild_catalog_now();
    }

    /// The unguarded rebuild: `BEGIN` calls this directly because the session
    /// already reports in-transaction by the time the transaction opens, and
    /// Go pins the LATEST schema at transaction start
    /// (`domain.GetSnapshotInfoSchema(startTS)`) — a table committed before
    /// `BEGIN` is visible to every statement of the new transaction.
    fn rebuild_catalog_now(&mut self) {
        let loaded = self.catalog.load();
        let statistics = self.stats.load();
        // Either half can move on its own: a DDL bumps the schema version, an
        // `ANALYZE` republishes the statistics. Both are rebuilt through the
        // same path, so a connection never plans against one half of a pair.
        if loaded.schema_version == self.schema_version
            && Arc::ptr_eq(&statistics, &self.statistics)
        {
            return;
        }
        let built =
            cluster_session_catalog(&loaded, &self.storage, &statistics, self.auto_ids.as_ref());
        let shared = self.session.shared_catalog();
        let mut catalog = shared.lock().unwrap_or_else(|poison| poison.into_inner());
        *catalog = built.catalog;
        drop(catalog);
        self.session
            .reinstall_local_temporary_statistics(|table_id| {
                statistics
                    .get(&table_id)
                    .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                    .cloned()
            });
        self.schema_version = loaded.schema_version;
        self.statistics = statistics;
        self.skipped = built.skipped;
    }

    /// Decides what this mode does with a statement that changes stored state:
    /// run it as a cluster catalog change, or refuse it with its own reason.
    ///
    /// [`StatementRoute::Ordinary`] means the statement changes nothing stored
    /// and takes its ordinary path. Every refusal is specific, because the
    /// reasons are: an `ALTER` is a DDL shape the cluster path cannot express,
    /// a `CREATE TABLE` with a foreign key is a clause it refuses by name, and
    /// a table-scoped `GRANT` is a `mysql.*` row shape the account writer does
    /// not encode (which it reports at persist time, where it knows).
    fn schema_route(&mut self, sql: &str) -> Result<StatementRoute, SqlQueryError> {
        let change = self
            .session
            .statement_stored_state_change(sql)
            .map_err(map_error)?;
        self.schema_route_for_change(sql, change)
    }

    fn flush_stats_delta_targets(
        &mut self,
        sql: &str,
    ) -> Result<Option<FlushStatsDeltaTargets>, SqlQueryError> {
        let prepared = self.session.prepare_ast(sql).map_err(map_error)?;
        let tidb_ast::Stmt::Admin(admin) = prepared.statement() else {
            return Ok(None);
        };
        let tidb_ast::AdminStmt::Flush(flush) = admin.as_ref() else {
            return Ok(None);
        };
        let tidb_ast::FlushTarget::StatsDelta { objects, .. } = &flush.target else {
            return Ok(None);
        };
        if objects
            .iter()
            .any(|object| matches!(object, tidb_ast::StatsObject::Global))
        {
            return Ok(Some(FlushStatsDeltaTargets::All));
        }

        let current_database = self.session.current_database().to_owned();
        let shared = self.session.shared_catalog();
        let catalog = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut ids = Vec::new();
        let mut warnings = Vec::new();
        let append_table =
            |database: &str, table: &str, ids: &mut Vec<i64>, warnings: &mut Vec<(u16, String)>| {
                let Some(tidb_executor::TableEntry::Kv(table)) = catalog.table_in(database, table)
                else {
                    warnings.push((1146, format!("Table '{database}.{table}' doesn't exist")));
                    return;
                };
                ids.push(table.table_id);
                if let Some(partition) = table.partition() {
                    ids.extend(partition.definitions.iter().map(|definition| definition.id));
                }
            };
        for object in objects {
            match object {
                tidb_ast::StatsObject::Global => unreachable!("handled above"),
                tidb_ast::StatsObject::Database(database) => {
                    if let Some(tables) = catalog.table_names(database) {
                        for table in tables {
                            append_table(database, &table, &mut ids, &mut warnings);
                        }
                    } else {
                        warnings.push((1049, format!("Unknown database '{database}'")));
                    }
                }
                tidb_ast::StatsObject::Table { database, table } => {
                    let database = database.as_deref().unwrap_or(&current_database);
                    append_table(database, table, &mut ids, &mut warnings);
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        drop(catalog);
        for (code, message) in warnings {
            self.session.append_routed_warning(code, message);
        }
        Ok(Some(FlushStatsDeltaTargets::Tables(ids)))
    }

    fn schema_route_for_change(
        &mut self,
        sql: &str,
        change: StoredStateChange,
    ) -> Result<StatementRoute, SqlQueryError> {
        match change {
            StoredStateChange::None => Ok(StatementRoute::Ordinary),
            StoredStateChange::Accounts => Ok(StatementRoute::Accounts),
            StoredStateChange::GlobalVars => Ok(StatementRoute::GlobalVars),
            StoredStateChange::Statistics => {
                if let Some(targets) = self.flush_stats_delta_targets(sql)? {
                    return Ok(StatementRoute::FlushStatsDelta(targets));
                }
                if prepare_cluster_load_stats(sql).is_some() {
                    return Ok(StatementRoute::LoadStats);
                }
                match prepare_cluster_analyze(sql, self.session.current_database()) {
                    Ok(Some(tables)) if !tables.is_empty() => Ok(StatementRoute::Analyze(tables)),
                    Ok(_) => Err(SqlQueryError::unknown(
                        "this node runs ANALYZE TABLE for a named table only",
                    )),
                    Err(refusal) => Err(SqlQueryError::unknown(refusal.to_string())),
                }
            }
            StoredStateChange::StatsLock => {
                match prepare_cluster_stats_lock(sql, self.session.current_database()) {
                    Ok(Some(statement)) => Ok(StatementRoute::StatsLock(statement)),
                    Ok(None) => Err(SqlQueryError::unknown(
                        "this node could not lower the statistics lock statement",
                    )),
                    Err(refusal) => Err(crate::sql_node::cluster_stats_lock_error(
                        ClusterStatsLockCommitError::Plan(refusal),
                    )),
                }
            }
            StoredStateChange::Schema => {
                if self
                    .session
                    .is_local_temporary_create(sql)
                    .map_err(map_error)?
                {
                    return Ok(StatementRoute::LocalTemporaryDdl);
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
                    |session| session.resolve_cluster_view(sql).map_err(map_error),
                )?;
                if let Some((database, name, or_replace, view)) = resolved {
                    let info = tidb_exec::cluster_ddl::build_view_table_info(&name, &view);
                    return Ok(StatementRoute::Ddl(DdlStatement::CreateView {
                        schema: database,
                        name,
                        or_replace,
                        info: Box::new(info),
                    }));
                }
                let context = self.session.ddl_statement_context();
                match prepare_cluster_ddl_with_context(
                    sql,
                    self.session.current_database(),
                    &context,
                ) {
                    Ok(Some(statement)) => {
                        // Warnings the LOWERING produced (an ignored CHECK
                        // constraint, Go ddl/create_table.go:1470) belong to
                        // this statement; Go appends them to the session's
                        // own context, so drain the lowering context's here.
                        self.session.begin_routed_statement_warnings();
                        self.session.drain_context_warnings(&context);
                        Ok(StatementRoute::Ddl(statement))
                    }
                    Ok(None) => Err(SqlQueryError::unknown(
                        "this node changes the cluster's catalog for CREATE TABLE, DROP TABLE, \
                         CREATE DATABASE, DROP DATABASE, CREATE/DROP VIEW, placement policy \
                         changes, index changes, and single-action ALTER INDEX changes only; \
                         run this statement on a TiDB server",
                    )),
                    // The refusal carries Go's own errno where it has one
                    // (`Unsupported ...` is 8200), so a client can tell a
                    // shape this server will not do from an internal failure.
                    Err(refusal) => Err(SqlQueryError::new(
                        refusal.code,
                        refusal.sql_state(),
                        refusal.to_string(),
                    )),
                }
            }
        }
    }

    /// Performs one cluster account change.
    ///
    /// The statement itself is run by the session driver, against a *scratch*
    /// account table read from the cluster inside this change's own
    /// transaction -- so the driver's own validation and error messages are
    /// what the client sees, and a statement that fails never reaches storage
    /// nor the node's live table. See [`crate::cluster_account_seam`] for why
    /// that ordering is the whole failure story.
    ///
    /// An open transaction is committed first, for the same reason a DDL
    /// commits one: MySQL and Go both commit implicitly before a statement
    /// that changes stored state outside it.
    fn run_account_statement(&mut self, sql: &str) -> Result<WriteOutcome, SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
        let pending = self.accounts.begin().map_err(SqlQueryError::unknown)?;
        let scratch = pending.registry();
        let live = self.session.swap_privileges(scratch);
        let applied = self.session.run(sql).map_err(map_error);
        // Restoring the live table is unconditional: a statement that failed
        // must not leave the connection reading the scratch copy.
        if let Some(live) = live {
            self.session.swap_privileges(live);
        }
        applied?;
        let changed = pending.commit()?;
        if !changed.is_empty() {
            eprintln!(
                "{{\"event\":\"cluster_accounts_changed\",\"users\":{}}}",
                serde_json::to_string(&changed).unwrap_or_else(|_| "[]".to_owned())
            );
        }
        // Go answers an account statement with an OK packet carrying no rows,
        // whether it changed anything or was an `IF [NOT] EXISTS` no-op.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Performs one `SET GLOBAL` change.
    ///
    /// Mirrors [`Self::run_account_statement`] exactly, against the sysvar
    /// seam instead of the account one: the assignments run through
    /// [`tidb_session::Session::apply_set`] against a *scratch*
    /// [`GlobalSysvars`] read from the cluster inside this change's own
    /// transaction, so an unknown variable, a wrong scope or a wrong value
    /// is refused before anything is persisted, and a statement that fails
    /// never reaches storage nor the node's live table.
    ///
    /// An open transaction is committed first, for the same reason a DDL or
    /// account statement commits one.
    fn run_global_var_statement(&mut self, sql: &str) -> Result<WriteOutcome, SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
        let pending = self.sysvars.begin().map_err(SqlQueryError::unknown)?;
        let scratch = pending.table();
        let live = self.session.swap_globals(scratch);
        let applied = self.session.apply_set(sql).map_err(map_error);
        // Restoring the live table is unconditional: a statement that failed
        // must not leave the connection reading the scratch copy, and a
        // session-scoped assignment mixed into the same `SET` must still
        // land on the connection's own live-seeded copies.
        self.session.swap_globals(live);
        applied?;
        let changed = pending.commit()?;
        if !changed.is_empty() {
            eprintln!(
                "{{\"event\":\"cluster_sysvars_changed\",\"variables\":{}}}",
                serde_json::to_string(&changed).unwrap_or_else(|_| "[]".to_owned())
            );
        }
        // Go answers a `SET` with an OK packet carrying no rows.
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
            other => {
                eprintln!(
                    "{{\"event\":\"unhandled_stats_schema_change\",\"action\":{}}}",
                    serde_json::to_string(&format!("{other:?}"))
                        .unwrap_or_else(|_| "\"unknown\"".to_owned())
                );
                Ok(())
            }
        }
    }

    fn notifier_transaction(&self) -> Result<&dyn OpenClusterTransaction, SqlQueryError> {
        self.explicit
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
        let transaction = self.notifier_transaction()?;
        let catalog = self.catalog.load();
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
                    &self.buffer,
                    |snapshot, _| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let plan =
                            tidb_exec::cluster_stats_write::plan_insert_table_stats_statement(
                                &mut snapshot,
                                &catalog,
                                &statement,
                                version,
                                now,
                            )
                            .map_err(|error| error.to_string())?;
                        Ok(((), plan.mutations))
                    },
                )
                .map_err(SqlQueryError::unknown)?;
            }
            self.record_schema_change_history(transaction, &self.buffer, &catalog, new_id, version)
                .map_err(SqlQueryError::unknown)?;
        }

        let mut count = 0i64;
        if adjust_global {
            for &old_id in retired_ids {
                let snapshot = transaction
                    .snapshot_for(false)
                    .map_err(SqlQueryError::unknown)?;
                let snapshot = tidb_exec::cluster_table_storage::overlay_staged_mutations(
                    snapshot,
                    &self.buffer,
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
            let snapshot =
                tidb_exec::cluster_table_storage::overlay_staged_mutations(snapshot, &self.buffer);
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
                    &self.buffer,
                    |snapshot, _| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let plan = tidb_exec::cluster_stats_write::plan_stats_delta_statement(
                            &mut snapshot,
                            &catalog,
                            &statement,
                            version,
                            now,
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
        let transaction = self.notifier_transaction()?;
        let catalog = self.catalog.load();
        let version = transaction.start_ts();
        for &physical_id in ids {
            let refreshed = transactions::stage_pessimistic_statement(
                transaction,
                &self.buffer,
                |snapshot, _| {
                    let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                    let plan = tidb_exec::cluster_stats_write::plan_stats_meta_version_refresh(
                        &mut snapshot,
                        &catalog,
                        physical_id,
                        version,
                    )
                    .map_err(|error| error.to_string())?;
                    Ok(((), plan.mutations))
                },
            );
            if let Err(error) = refreshed {
                if !best_effort {
                    return Err(SqlQueryError::unknown(error));
                }
                eprintln!(
                    "{{\"event\":\"stats_schema_gc_version_failed\",\"physical_id\":{physical_id},\"error\":{}}}",
                    serde_json::to_string(&error).unwrap_or_else(|_| "\"unknown\"".to_owned())
                );
                continue;
            }
            if let Err(error) = self.record_schema_change_history(
                transaction,
                &self.buffer,
                &catalog,
                physical_id,
                version,
            ) {
                if !best_effort {
                    return Err(SqlQueryError::unknown(error));
                }
                eprintln!(
                    "{{\"event\":\"stats_schema_history_failed\",\"physical_id\":{physical_id},\"error\":{}}}",
                    serde_json::to_string(&error).unwrap_or_else(|_| "\"unknown\"".to_owned())
                );
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
        let transaction = self.notifier_transaction()?;
        let catalog = self.catalog.load();
        let snapshot = transaction
            .snapshot_for(false)
            .map_err(SqlQueryError::unknown)?;
        let snapshot =
            tidb_exec::cluster_table_storage::overlay_staged_mutations(snapshot, &self.buffer);
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
        transactions::stage_pessimistic_statement(transaction, &self.buffer, |snapshot, _| {
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
            )
            .map_err(|error| error.to_string())?;
            Ok(((), plan.mutations))
        })
        .map_err(SqlQueryError::unknown)
    }

    fn stage_stats_columns(
        &self,
        table: &tidb_model::table_info::TableInfo,
        columns: &[tidb_model::ColumnInfo],
    ) -> Result<(), SqlQueryError> {
        let transaction = self.notifier_transaction()?;
        let catalog = self.catalog.load();
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
                        tidb_exec::system_row_write::origin_default(column, table.name.original())
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
            let snapshot =
                tidb_exec::cluster_table_storage::overlay_staged_mutations(snapshot, &self.buffer);
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
                    &self.buffer,
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
                            &self.buffer,
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
                    &self.buffer,
                    |snapshot, _| {
                        let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                        let plan = tidb_exec::cluster_stats_write::plan_stats_meta_version_refresh(
                            &mut snapshot,
                            &catalog,
                            physical_id,
                            version,
                        )
                        .map_err(|error| error.to_string())?;
                        Ok(((), plan.mutations))
                    },
                )
                .map_err(SqlQueryError::unknown)?;
            }
            self.record_schema_change_history(
                transaction,
                &self.buffer,
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
        let catalog = self.catalog.load();
        for target in tidb_exec::cluster_stats_write::GLOBAL_STATS_ID_TABLES {
            transactions::stage_pessimistic_statement(transaction, &self.buffer, |snapshot, _| {
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
            })
            .map_err(SqlQueryError::unknown)?;
        }
        Ok(())
    }

    fn stage_stats_version_refresh(&self) -> Result<(), SqlQueryError> {
        let transaction = self.notifier_transaction()?;
        let catalog = self.catalog.load();
        let version = transaction.start_ts();
        for target in tidb_exec::cluster_stats_write::STATS_VERSION_TABLES {
            transactions::stage_pessimistic_statement(transaction, &self.buffer, |snapshot, _| {
                let mut snapshot = SnapshotMetaSnapshot::new(snapshot);
                let plan = tidb_exec::cluster_stats_write::plan_update_stats_table_version(
                    &mut snapshot,
                    &catalog,
                    target,
                    version,
                )
                .map_err(|error| error.to_string())?;
                Ok(((), plan.mutations))
            })
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
        let enabled = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .is_ok_and(|value| tidb_exec::option_values::tidb_opt_on(&value));
        if !enabled
            || !self
                .stats
                .load()
                .get(&physical_id)
                .and_then(tidb_exec::stats_watch::TableStatsState::loaded)
                .is_some_and(|stats| stats.is_initialized())
        {
            return Ok(());
        }
        let (modify_count, count) =
            transactions::stage_pessimistic_statement(transaction, staged, |snapshot, _| {
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
            })?;
        let now = tidb_exec::mysql_bootstrap::local_now_datetime6();
        transactions::stage_pessimistic_statement(transaction, staged, |snapshot, _| {
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
            )
            .map_err(|error| error.to_string())?;
            Ok(((), plan.mutations))
        })
    }

    fn dynamic_partition_pruning(&self) -> Result<bool, SqlQueryError> {
        self.global_vars
            .get(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .map(|mode| mode == "dynamic")
            .map_err(|error| SqlQueryError::unknown(format!("{error:?}")))
    }

    fn run_ddl(
        &mut self,
        sql: &str,
        statement: &DdlStatement,
    ) -> Result<WriteOutcome, SqlQueryError> {
        // Go plans and checks DDL privileges before its implicit-commit
        // executor boundary. Do the same while this connection's transaction
        // is still intact: a denial must neither publish staged writes nor
        // reach the cluster catalog authority.
        let parsed = self.session.parse_statement(sql).map_err(map_error)?;
        self.session
            .require_statement_table_privileges(&parsed)
            .map_err(map_error)?;
        if self.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
        let report = self.ddl.execute(statement)?;
        // Go raises `job.Warning` on the session's own statement context, so
        // `SHOW WARNINGS` reports what the change did differently from what
        // was written. `toTError` gives a plain `fmt.Errorf` the generic
        // 1105 code.
        if let ClusterDdlReport::Applied {
            warning: Some(warning),
            ..
        }
        | ClusterDdlReport::AlreadySatisfied {
            warning: Some(warning),
            ..
        } = report
        {
            self.session.append_routed_warning(1105, warning);
        }
        // Go answers a DDL with an OK packet carrying no rows and no insert
        // id, whether it changed anything or was an IF [NOT] EXISTS no-op.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Executes DDL whose metadata belongs to this connection rather than to
    /// the cluster catalog.
    fn run_local_temporary_ddl(&mut self, sql: &str) -> Result<WriteOutcome, SqlQueryError> {
        let parsed = self.session.parse_statement(sql).map_err(map_error)?;
        self.session
            .require_statement_table_privileges(&parsed)
            .map_err(map_error)?;
        match self.session.run(sql).map_err(map_error)? {
            tidb_session::StmtResult::Affected(affected_rows) => Ok(WriteOutcome {
                affected_rows,
                last_insert_id: self.session.statement_insert_id(),
            }),
            tidb_session::StmtResult::Done(_) => Ok(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }),
            tidb_session::StmtResult::Rows(_) => Err(SqlQueryError::unknown(
                "LOCAL temporary-table DDL unexpectedly returned rows",
            )),
        }
    }
}

impl QuerySession for ClusterServerSession {
    fn local_infile_path(&mut self, sql: &str) -> Result<Option<String>, SqlQueryError> {
        let Some(statement) = prepare_cluster_load_stats(sql) else {
            return Ok(None);
        };
        if statement.path.is_empty() {
            return Err(SqlQueryError::unknown("Load Stats: file path is empty"));
        }
        Ok(Some(statement.path))
    }

    fn execute_local_infile(
        &mut self,
        sql: &str,
        data: &[u8],
    ) -> Result<WriteOutcome, SqlQueryError> {
        if prepare_cluster_load_stats(sql).is_none() {
            return Err(SqlQueryError::unknown(
                "statement did not request a client-local file",
            ));
        }
        self.run_load_stats(data)
    }

    fn finish_execute_stmt(&mut self, cost: std::time::Duration) {
        self.session.finish_txn_write_throughput(cost);
    }

    fn query_cancellation(&self) -> Option<Arc<dyn crate::sql_node::ActiveQueryCancellation>> {
        Some(Arc::new(self.session.begin_query_cancellation()))
    }

    fn wait_timeout(&self) -> std::time::Duration {
        self.session.wait_timeout()
    }

    /// This session's own `@@max_allowed_packet`; see the trait's own doc for
    /// why Go rebinds the packet reader from it on every packet.
    fn max_allowed_packet(&self) -> Option<usize> {
        Some(self.session.max_allowed_packet() as usize)
    }

    fn split_statements(
        &mut self,
        sql: &str,
        client_multi_statements: bool,
    ) -> Result<Vec<String>, SqlQueryError> {
        self.session
            .split_statements(sql, client_multi_statements)
            .map_err(map_error)
    }

    fn flush_multi_statement_warning(&mut self) {
        self.session.flush_multi_statement_warning();
    }

    /// The live status word Go reads with `cc.ctx.Status()` before every
    /// OK/EOF packet. The driver session owns the transaction state this tier
    /// acts on, so the wire word and the tier's behaviour cannot disagree.
    fn wire_status(&self) -> WireStatus {
        WireStatus::of_session(&self.session)
    }

    /// The OK/EOF packet's warning count, read off the same buffer
    /// `SHOW WARNINGS` reports (Go `ctx.WarningCount()`).
    fn warning_count(&self) -> u16 {
        self.session.wire_warning_count()
    }

    fn warning_codes(&self) -> Vec<u16> {
        self.session
            .warnings()
            .iter()
            .map(|warning| warning.code)
            .collect()
    }

    /// Go `clientConn.initResultEncoder`'s read: this session's
    /// `@@character_set_results`.
    fn result_charset(&self) -> Cow<'_, str> {
        self.session.result_charset()
    }

    fn input_charset(&self) -> Cow<'_, str> {
        self.session.input_charset()
    }

    /// Maps `BEGIN`/`COMMIT`/`ROLLBACK` onto the connection's buffer.
    ///
    /// The driver session owns the *state* (so `in_transaction` and the
    /// statement's OK-packet status flag agree with the in-process tier); this
    /// adds what the state means for cluster storage.
    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        let control = classify_transaction_control(sql);
        // Refused BEFORE the driver session is touched, which is the whole
        // point: `Session::control_transaction` sets `in_transaction` for any
        // BEGIN spelling, so honoring the refusal afterwards would leave the
        // session inside a transaction that this node never opened -- no
        // `self.explicit`, so every following statement reads at a fresh
        // timestamp, its writes stay in the buffer, and its COMMIT publishes
        // without a conflict check. The read-only node already refuses here
        // (`real_tikv_node`); this one used to fall through an empty arm.
        if let Some(TransactionControl::Unsupported(feature)) = control {
            return Err(SqlQueryError::unknown(format!(
                "{feature} is not supported yet"
            )));
        }
        // Go's `BEGIN` inside an open transaction implicitly COMMITS it --
        // and the commit must run BEFORE the schema refresh below, in Go's
        // own order. The refresh replaces the session's shared catalog, and
        // the driver session's commit checks that the catalog it opened on
        // is the one it is committing into; refreshing first turned every
        // implicit commit after a mid-run statistics republish into a
        // phantom "Write conflict" at `BEGIN` (sysbench abandons a
        // transaction on an ignorable 1213 and just issues the next BEGIN,
        // which is exactly this shape).
        if matches!(control, Some(TransactionControl::Begin { .. }))
            && self.session.in_transaction()
        {
            self.session
                .control_transaction("COMMIT")
                .map_err(map_error)?;
            self.commit_explicit()?;
        }
        // The refresh happens BEFORE the driver session pins its own schema
        // view for the transaction: Go activates a transaction with the
        // LATEST schema at start (`domain.GetSnapshotInfoSchema(startTS)`),
        // so a table committed before BEGIN is visible to every statement of
        // the new transaction, on a connection of any age.
        if matches!(control, Some(TransactionControl::Begin { .. })) {
            self.rebuild_catalog_now();
        }
        let state = self.session.control_transaction(sql).map_err(map_error)?;
        let Some(in_transaction) = state else {
            return Ok(None);
        };
        match control {
            Some(TransactionControl::Commit) => self.commit_explicit()?,
            Some(TransactionControl::Rollback) => self.discard_explicit()?,
            // A BEGIN drops the staged writes too: a leftover buffer at that
            // point could only come from a statement outside any transaction
            // whose autocommit already published it. Then it takes the one
            // timestamp every statement of the new transaction reads at, and
            // that its COMMIT will prewrite at.
            Some(TransactionControl::Begin { .. }) => {
                self.discard_explicit()?;
                // NO refresh here, deliberately. The refresh for this BEGIN
                // already ran above, BEFORE `session.control_transaction`
                // pinned the driver transaction's `base_version` -- one
                // refresh, then both pins, which is Go's shape (one
                // `GetSnapshotInfoSchema(startTS)` per activation). A second
                // rebuild HERE ran after that pin, so a reload or statistics
                // republish landing in the microseconds between them swapped
                // the connection's catalog under the just-opened transaction
                // -- whose COMMIT then failed the base-version guard with a
                // phantom 9007. Receipted live by the guard probe under rung
                // 8: `shared_version:86, base_version:1910` -- a freshly
                // rebuilt catalog (counter restarted) against a pin taken on
                // the long-lived one, once per run, exactly one thread.
                let resource_group = self.session.current_resource_group().to_owned();
                self.open_explicit(&resource_group)?;
            }
            Some(
                control @ (TransactionControl::Savepoint(_)
                | TransactionControl::RollbackToSavepoint(_)
                | TransactionControl::ReleaseSavepoint(_)),
            ) => self.apply_savepoint(&control)?,
            // Refused above, before the session was touched.
            Some(TransactionControl::Unsupported(_)) | None => {}
        }
        Ok(Some(in_transaction))
    }

    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        // Go resolves and plans against the current infoschema before it
        // chooses a statement snapshot. Keep schema refresh ahead of routing,
        // access-shape classification, and the statement lifecycle.
        self.rebuild_catalog_if_stale();
        // Routed before anything else: what happens to a stored-state change
        // must not depend on which answer shape it would otherwise have taken.
        match self.schema_route(sql)? {
            StatementRoute::Ddl(statement) => return self.run_ddl(sql, &statement).map(Some),
            StatementRoute::LocalTemporaryDdl => {
                return self.run_local_temporary_ddl(sql).map(Some);
            }
            StatementRoute::Accounts => return self.run_account_statement(sql).map(Some),
            StatementRoute::GlobalVars => return self.run_global_var_statement(sql).map(Some),
            StatementRoute::Analyze(tables) => return self.run_analyze(&tables).map(Some),
            StatementRoute::LoadStats => {
                return Err(SqlQueryError::unknown(
                    "LOAD STATS requires client-local file transfer",
                ))
            }
            StatementRoute::FlushStatsDelta(targets) => {
                return self.run_flush_stats_delta(&targets).map(Some);
            }
            StatementRoute::StatsLock(statement) => {
                return self.run_stats_lock(&statement).map(Some)
            }
            StatementRoute::Ordinary => {}
        }
        if self.session.apply_set(sql).map_err(map_error)?.is_some() {
            // A `SET` takes no snapshot, so it does not go through
            // `with_statement` -- but `SET autocommit = 1` ends the open
            // transaction from the inside, and that has to be published here
            // too or the write waits for a statement that may never come.
            self.commit_if_session_left_transaction()?;
            return Ok(Some(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        if self.session.statement_kind(sql).map_err(map_error)? != StmtKind::Write {
            return Ok(None);
        }
        let owned = sql.to_owned();
        let resource_group = self
            .session
            .statement_resource_group_sql(sql)
            .map_err(map_error)?;
        // A write declares nothing. Its read-before-write reaches the snapshot
        // as the same `get` a point-get SELECT issues, which is exactly why the
        // declaration is made from the statement rather than from the read.
        //
        // Go's pessimistic point write also folds its row read INTO its lock
        // (`PointGetExecutor.getAndLock`, `pkg/executor/point_get.go:612-624`)
        // -- and the text protocol carries that fold too, because a client-side
        // prepared driver (Connector/J without `useServerPrepStmts`) sends the
        // very statements the prepared path folds as plain COM_QUERY. The
        // classified keys are locked WITH their rows before any read exists;
        // the statement's read then answers from the lock response exactly as
        // in [`Self::execute_general`]'s prepared arm.
        let prelock_keys = match self.explicit.as_ref() {
            Some(transaction) if transaction.is_pessimistic() => {
                self.session.text_statement_prelock_keys(sql)
            }
            _ => Vec::new(),
        };
        let read_keys = self.storage.read_keys();
        let attempt_read_keys = read_keys.clone();
        let affected_rows = match self.with_prelocked_statement(
            StatementReadShape::Unknown,
            prelock_keys,
            &resource_group,
            move |session| {
                attempt_read_keys.begin();
                match session.run(&owned).map_err(map_error)? {
                    StmtResult::Affected(count) => Ok(count),
                    StmtResult::Done(_) => Ok(0),
                    StmtResult::Rows(_) => Err(SqlQueryError::unknown(
                        "a write statement unexpectedly produced rows",
                    )),
                }
            },
        ) {
            Ok(affected_rows) => affected_rows,
            Err(error) => {
                read_keys.cancel();
                return Err(error);
            }
        };
        let processed_keys = read_keys.finish().len() as i64;
        if affected_rows > 0 && processed_keys > 0 {
            self.session
                .txn_write_throughput_sli()
                .add_read_keys(processed_keys);
        }
        Ok(Some(WriteOutcome {
            affected_rows,
            last_insert_id: self.session.statement_insert_id(),
        }))
    }

    /// The catalog is refreshed first so a schema another node created since
    /// this connection opened is selectable, exactly as it is for a statement.

    /// Go leaves `CurrentDB` empty when the handshake carried no schema; see
    /// the trait's own doc.
    fn deselect_database(&mut self) {
        self.session.deselect_database();
    }

    fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.session.select_database(name).map_err(map_error)
    }

    fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        // Transaction control carries no markers and answers with no columns,
        // so there is nothing to probe -- and probing it would RUN it, which
        // for `ROLLBACK` means publishing the buffer this connection is about
        // to discard. It is applied at EXECUTE, through
        // [`Self::control_transaction`].
        if classify_transaction_control(sql).is_some() {
            return Ok(PreparedGeneral::new(sql.to_owned(), 0, Vec::new()));
        }
        let prepared_ast = self.session.prepare_ast(sql).map_err(map_error)?;
        let parameter_count = prepared_ast.parameter_count();
        let point_get_plan = prepared_ast.point_get_plan();
        let dml_plan = prepared_ast.dml_plan();
        let select_plan = prepared_ast.select_plan();
        let template = prepared_ast.statement().clone();
        let kind = self.session.statement_kind_parsed(&template);
        if kind == StmtKind::Write {
            // A prepared DDL is admitted here and executed at EXECUTE, so a
            // refusal -- an unsupported shape, an unsupported column type --
            // is reported at PREPARE, where Go reports it too.
            self.schema_route(sql)?;
            // Keep ordinary DML's parsed tree as well.  The MySQL binary
            // protocol prepares YCSB's INSERT/UPDATE/DELETE once and then
            // executes that handle thousands of times; throwing this tree
            // away here would send every execution back through parse and
            // SQL-text binding, defeating the prepared path's fixed-plan
            // work.  Routed schema/account/ANALYZE statements still use their
            // dedicated SQL route at EXECUTE and must not be run through the
            // ordinary statement driver.
            if !matches!(template, tidb_ast::Stmt::Dml(_)) {
                return Ok(PreparedGeneral::new(
                    sql.to_owned(),
                    parameter_count,
                    Vec::new(),
                ));
            }
            return Ok(PreparedGeneral::with_template_and_dml_plan(
                sql.to_owned(),
                parameter_count,
                Vec::new(),
                template,
                dml_plan,
            ));
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
            Ok(columns) => crate::pipeline_session::select_columns(&columns),
            Err(error @ tidb_executor::DriverError::Var(_)) => return Err(map_error(error)),
            // A query whose metadata cannot be resolved without real values
            // reports none at prepare time, matching the existing wire
            // fallback. Such shapes remain on the ordinary execute path.
            Err(_) => Vec::new(),
        };
        Ok(PreparedGeneral::with_template_and_point_get_plan(
            sql.to_owned(),
            parameter_count,
            result_columns,
            template,
            point_get_plan,
            select_plan,
        ))
    }

    fn execute_general<'a>(
        &'a mut self,
        statement: &PreparedGeneral,
        values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        // A `BEGIN` is a `BEGIN` whichever protocol carried it. Run as an
        // ordinary statement it would flip only the driver session's own flag
        // and leave `self.explicit` unopened -- the two pieces of transaction
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
            self.control_transaction(statement.sql())?;
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
        let route = if statement.template().is_some() {
            StatementRoute::Ordinary
        } else {
            self.schema_route(statement.sql())?
        };
        match route {
            StatementRoute::Ddl(ddl) => {
                return self
                    .run_ddl(statement.sql(), &ddl)
                    .map(GeneralExecuteOutcome::Write)
            }
            StatementRoute::LocalTemporaryDdl => {
                return self
                    .run_local_temporary_ddl(statement.sql())
                    .map(GeneralExecuteOutcome::Write)
            }
            StatementRoute::Accounts => {
                return self
                    .run_account_statement(statement.sql())
                    .map(GeneralExecuteOutcome::Write)
            }
            StatementRoute::GlobalVars => {
                return self
                    .run_global_var_statement(statement.sql())
                    .map(GeneralExecuteOutcome::Write)
            }
            StatementRoute::Analyze(tables) => {
                return self.run_analyze(&tables).map(GeneralExecuteOutcome::Write)
            }
            StatementRoute::LoadStats => {
                return Err(SqlQueryError::unknown(
                    "LOAD STATS requires client-local file transfer",
                ))
            }
            StatementRoute::FlushStatsDelta(targets) => {
                return self
                    .run_flush_stats_delta(&targets)
                    .map(GeneralExecuteOutcome::Write)
            }
            StatementRoute::StatsLock(statement) => {
                return self
                    .run_stats_lock(&statement)
                    .map(GeneralExecuteOutcome::Write)
            }
            StatementRoute::Ordinary => {}
        }
        let params = crate::pipeline_session::prepared_parameters(values);
        let sql = statement.sql().to_owned();
        let retained = statement.template();
        let (effective_template, binding_sql) = retained.map_or((None, None), |template| {
            let (effective, binding_sql) = self.session.prepared_statement_with_binding(template);
            (Some(effective), binding_sql)
        });
        let effective = effective_template.as_ref();
        // Plan-cache validation must see the current schema before the read
        // policy is declared. Rebuilding only inside the statement lifecycle
        // would let a stale row-handle plan choose MaxTS and then fall back to
        // a newly rebuilt, potentially multi-read plan.
        self.rebuild_catalog_if_stale();
        let resource_group = match effective {
            Some(template) => self.session.statement_resource_group(template).to_owned(),
            None => self
                .session
                .statement_resource_group_sql(statement.sql())
                .map_err(map_error)?,
        };
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
                    plan,
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
                    plan,
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
                    plan,
                    &params,
                    effective.expect("cacheable prepared DML retains a statement"),
                    binding_sql.as_deref(),
                )
            });
        let direct_dml = cached_dml.is_some();
        let direct = fast || fast_select || direct_dml;
        let bound_template = if direct {
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
            StatementReadShape::Unknown
        } else if direct_dml {
            StatementReadShape::AutocommitWrite
        } else {
            bound_template.as_ref().map_or_else(
                || self.session.statement_read_shape(&sql, &params),
                |bound| self.session.statement_read_shape_bound(bound),
            )
        };
        // Go's pessimistic point write folds its row read INTO its lock
        // (`PointGetExecutor.getAndLock` asks TiKV to answer the row with the
        // lock, `pkg/executor/point_get.go:612-624`, and caches it in
        // `TxnCtx.SetPessimisticLockCache`). Classify this statement once,
        // from whichever tree the fast paths left: a bound one carries
        // constants, an unbound template resolves its `?` markers against the
        // execute parameters -- the same walker reads both. Empty output (a
        // scan-shaped WHERE, a non-pessimistic or autocommit statement) keeps
        // today's read-then-lock order untouched.
        // Both pre-lock arms, write and locking read: `SELECT ... FOR UPDATE`
        // on a fully pinned clustered handle folds exactly like a point write
        // (go's `SelectLockExec` locks the rows as they are read), while a
        // plain point SELECT must NOT lock -- the read arm's own guards
        // (LockKind::Update, default wait, no ORDER BY/LIMIT) draw that line.
        let prelock_keys = match self.explicit.as_ref() {
            Some(transaction) if transaction.is_pessimistic() => {
                if let Some(bound) = bound_template.as_ref() {
                    self.session.statement_prelock_keys(bound, &[])
                } else if let Some(template) = effective {
                    self.session.statement_prelock_keys(template, &params)
                } else {
                    Vec::new()
                }
            }
            None if shape == StatementReadShape::AutocommitWrite => {
                if let Some(bound) = bound_template.as_ref() {
                    self.session.statement_prelock_keys(bound, &[])
                } else if let Some(template) = effective {
                    self.session.statement_prelock_keys(template, &params)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };
        let is_write = match effective {
            Some(template) => self.session.statement_kind_parsed(template) == StmtKind::Write,
            None => self.session.statement_kind(&sql).map_err(map_error)? == StmtKind::Write,
        };
        let write_read_keys = is_write.then(|| self.storage.read_keys());
        let attempt_read_keys = write_read_keys.clone();
        let output = match self.with_prelocked_statement(
            shape,
            prelock_keys,
            &resource_group,
            move |session| {
                if let Some(read_keys) = attempt_read_keys.as_ref() {
                    read_keys.begin();
                }
                if fast_select {
                    let cached = cached_select
                        .as_ref()
                        .expect("cached prepared SELECT carries its execution");
                    return session
                        .execute_prepared_select(cached, &sql)
                        .map_err(map_error);
                }
                if fast {
                    if let Some(cached) = cached_point_get.clone() {
                        match session.execute_prepared_point_get(cached) {
                            Ok(Some(output)) => return Ok(output),
                            // The cached plan's identity moved under it (a DDL
                            // between PREPARE and this EXECUTE). That is a cache
                            // MISS, not a statement failure: fall through and
                            // re-plan, exactly as Go's `GetPlanFromPlanCache`
                            // does.
                            Ok(None) => {}
                            Err(error) => return Err(map_error(error)),
                        }
                    }
                    // A missing/invalidated candidate falls through to the
                    // ordinary path. This is a cache miss, never permission to
                    // run a second, executor-local point planner.
                    let bound = tidb_executor::bind_statement(
                        effective_template
                            .as_ref()
                            .expect("fast prepared point read has a retained template")
                            .clone(),
                        &params,
                    )
                    .map_err(map_error)?;
                    return session
                        .run_parsed_bound_owned_with_sql(bound, &sql)
                        .map_err(map_error);
                }
                if direct_dml {
                    let cached = cached_dml
                        .as_ref()
                        .expect("direct prepared DML carries its bound statement");
                    let output = session
                        .execute_cached_prepared_dml(cached, &sql)
                        .map_err(map_error)?;
                    return Ok(output);
                }
                if let Some(bound) = bound_template.as_ref().cloned() {
                    session
                        .run_parsed_bound_owned_with_sql(bound, &sql)
                        .map_err(map_error)
                } else {
                    session.run_with_params(&sql, &params).map_err(map_error)
                }
            },
        ) {
            Ok(output) => output,
            Err(error) => {
                if let Some(read_keys) = write_read_keys {
                    read_keys.cancel();
                }
                return Err(error);
            }
        };
        if let Some(read_keys) = write_read_keys {
            let affected_rows = match &output {
                StmtOutput::Affected(count) => *count,
                StmtOutput::Rows { .. } | StmtOutput::Done(_) => 0,
            };
            let processed_keys = read_keys.finish().len() as i64;
            if affected_rows > 0 && processed_keys > 0 {
                self.session
                    .txn_write_throughput_sli()
                    .add_read_keys(processed_keys);
            }
        }
        let result_authority = matches!(&output, StmtOutput::Rows { .. })
            .then(|| self.session.result_materialization_authority());
        Ok(match output {
            StmtOutput::Rows { columns, rows } => {
                let field_types = columns.iter().map(|(_, field)| field.clone()).collect();
                GeneralExecuteOutcome::Rows(
                    QueryResult::new(Box::new(MaterializedResultSetSource::new(
                        crate::pipeline_session::select_columns(&columns),
                        rows,
                    )))
                    .with_cursor_materialization(
                        field_types,
                        result_authority.expect("a row result carries materialization authority"),
                    )
                    .with_statement_status(
                        self.session.wire_warning_count(),
                        WireStatus::of_session(&self.session),
                    ),
                )
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

    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
        // Refresh before access-shape classification. Waiting until the
        // statement lifecycle would allow a stale point shape to select MaxTS
        // before the executor sees the current schema.
        self.rebuild_catalog_if_stale();
        // The text protocol reaches DDL through `execute_write`; this covers a
        // front end that goes straight to the result-set path, so a routed
        // statement runs exactly once either way.
        match self.schema_route(sql)? {
            StatementRoute::Ddl(statement) => {
                self.run_ddl(sql, &statement)?;
                return Ok(QueryResult::new(Box::new(
                    crate::pipeline_session::affected_rows_source(0),
                )));
            }
            StatementRoute::LocalTemporaryDdl => {
                self.run_local_temporary_ddl(sql)?;
                return Ok(QueryResult::new(Box::new(
                    crate::pipeline_session::affected_rows_source(0),
                )));
            }
            StatementRoute::Accounts => {
                self.run_account_statement(sql)?;
                return Ok(QueryResult::new(Box::new(
                    crate::pipeline_session::affected_rows_source(0),
                )));
            }
            StatementRoute::GlobalVars => {
                self.run_global_var_statement(sql)?;
                return Ok(QueryResult::new(Box::new(
                    crate::pipeline_session::affected_rows_source(0),
                )));
            }
            StatementRoute::Analyze(tables) => {
                self.run_analyze(&tables)?;
                return Ok(QueryResult::new(Box::new(
                    crate::pipeline_session::affected_rows_source(0),
                )));
            }
            StatementRoute::LoadStats => {
                return Err(SqlQueryError::unknown(
                    "LOAD STATS requires client-local file transfer",
                ))
            }
            StatementRoute::FlushStatsDelta(targets) => {
                self.run_flush_stats_delta(&targets)?;
                return Ok(QueryResult::new(Box::new(
                    crate::pipeline_session::affected_rows_source(0),
                )));
            }
            StatementRoute::StatsLock(statement) => {
                self.run_stats_lock(&statement)?;
                return Ok(QueryResult::new(Box::new(
                    crate::pipeline_session::affected_rows_source(0),
                )));
            }
            StatementRoute::Ordinary => {}
        }
        let owned = sql.to_owned();
        let resource_group = self
            .session
            .statement_resource_group_sql(sql)
            .map_err(map_error)?;
        let shape = self.session.statement_read_shape(sql, &[]);
        // Go's `SELECT ... FOR UPDATE` on a clustered handle-pinned row folds
        // its lock INTO its one row read (`TryFastPlan` -> `PointGetPlan` with
        // `Lock=true`, executed by `getAndLock`). The text protocol reaches
        // this path for such reads whenever the driver prepares client-side,
        // so classify the same shape the prepared path classifies and let the
        // statement's read answer from the lock response. Empty output (a
        // scan-shaped WHERE, FOR SHARE, NOWAIT, or no pessimistic transaction)
        // keeps today's read-then-lock order untouched.
        let prelock_keys = match self.explicit.as_ref() {
            Some(transaction) if transaction.is_pessimistic() => {
                self.session.text_statement_prelock_keys(sql)
            }
            _ => Vec::new(),
        };
        // The rows are materialized inside the statement's snapshot, because
        // the snapshot's read transaction ends when the statement does; a lazy
        // source would be reading through a finished transaction.
        let source =
            self.with_prelocked_statement(shape, prelock_keys, &resource_group, move |session| {
                let output = session.run_with_columns(&owned).map_err(map_error)?;
                Ok(match output {
                    StmtOutput::Rows { columns, rows } => MaterializedResultSetSource::new(
                        crate::pipeline_session::select_columns(&columns),
                        rows,
                    ),
                    StmtOutput::Affected(count) => {
                        crate::pipeline_session::affected_rows_source(count)
                    }
                    StmtOutput::Done(_) => crate::pipeline_session::affected_rows_source(0),
                })
            })?;
        Ok(QueryResult::new(Box::new(source)).with_statement_status(
            self.session.wire_warning_count(),
            WireStatus::of_session(&self.session),
        ))
    }
}

fn map_error(error: tidb_executor::DriverError) -> SqlQueryError {
    let mapped = error.to_mysql_error();
    SqlQueryError::new(mapped.code, mapped.state, mapped.message)
}

/// Storage bound to no snapshot, used only to decide which tables a catalog
/// *could* be built over. Deciding that reads a `TableInfo`, never a row.
fn detached_storage() -> ClusterTableStorage {
    let slot: Arc<Mutex<dyn ClusterSnapshot>> = Arc::new(Mutex::new(SwappableSnapshot::new()));
    ClusterTableStorage::new(MutationBuffer::new(), slot)
}

#[cfg(test)]
mod tests;
