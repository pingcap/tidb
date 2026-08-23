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

use std::cell::Cell;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::cluster_analyze::AnalyzeStatement;
use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::real_tikv_analyze::prepare_cluster_analyze;
use tidb_exec::real_tikv_ddl::prepare_cluster_ddl_with_context;
use tidb_exec::stats_watch::SharedStats;
use tidb_executor::access_path::StatementReadShape;
use tidb_executor::cluster_storage::{
    BufferImage, ClusterSnapshot, ClusterTableStorage, MutationBuffer, SwappableSnapshot,
};
use tidb_executor::remote_scan::PushdownScanner;
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_session::privilege::PrivilegeRegistry;
use tidb_session::process::ProcessRegistry;
use tidb_session::{
    GlobalSysvars, PreparedAst, Session, StmtKind, StmtOutput, StmtResult, StoredStateChange,
};

use tidb_exec::cluster_table_storage::LockKeysOutcome;

use crate::cluster_account_seam::ClusterAccountWriter;
use crate::cluster_analyze_seam::ClusterAnalyze;
use crate::cluster_session::{
    cluster_session_catalog, cluster_session_catalog_with_templates, KvTableTemplates,
    SkippedTable, TableAutoIds, StatsTemplates,
};
use crate::cluster_sysvar_seam::ClusterSysvarWriter;
use crate::pipeline_session::MaterializedResultSetSource;
use crate::sql_node::{
    ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};
use crate::wire_status::WireStatus;

/// The PD/TiKV control-plane deadline this node's boot and statements use, the
/// same one the bounded node applies.
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

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
    /// One `mysql.*` account change.
    Accounts,
    /// One `SET GLOBAL` change to `mysql.global_variables`.
    GlobalVars,
    /// One `ANALYZE TABLE`, per table it named.
    Analyze(Vec<AnalyzeStatement>),
}

/// Opens one cluster-backed wide-SQL [`Session`] per authenticated connection.
pub struct ClusterSessionFactory {
    /// The write/read capability every connection's statements open their
    /// snapshots and publish their commits through.
    transactions: Arc<dyn ClusterTransactions>,
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
        Self {
            transactions,
            ddl,
            accounts,
            sysvars,
            analyze,
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
        }
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
        let built = cluster_session_catalog_with_templates(
            &loaded,
            &storage,
            &statistics,
            self.auto_ids.as_ref(),
            &mut templates,
            &template_storage,
            Some(&mut kv_templates),
        );
        let mut session = Session::with_catalog(Arc::new(Mutex::new(built.catalog)));
        session.set_advisory_lock_service(Arc::new(transactions::ClusterAdvisoryLockService::new(
            Arc::clone(&self.transactions),
        )));
        session.set_version_info(context.version_info.clone());
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
            buffer,
            slot,
            storage,
            transactions: Arc::clone(&self.transactions),
            ddl: Arc::clone(&self.ddl),
            accounts: Arc::clone(&self.accounts),
            sysvars: Arc::clone(&self.sysvars),
            analyze: Arc::clone(&self.analyze),
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
        })
    }
}

/// One connection's wide-SQL session over cluster storage.
pub struct ClusterServerSession {
    session: Session,
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
    /// [`ClusterServerSession::with_statement`] already uses for
    /// statement-level rollback, held under a name.
    ///
    /// The two stacks stay in step because both apply the same rules to the
    /// same statement sequence, and the session's error arm runs FIRST -- a
    /// name this stack could not find is one the session already refused.
    savepoints: Vec<(String, BufferImage)>,
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

    /// Runs one statement inside the snapshot/buffer lifecycle this mode is
    /// built around.
    ///
    /// The ordering is the correctness core: bind a snapshot, take a buffer
    /// savepoint, run, always unbind the snapshot, and only then decide what
    /// happens to the staged writes.
    ///
    /// Which snapshot is the whole `start_ts` question. Inside an explicit
    /// transaction the statement reads through the one transaction `BEGIN`
    /// opened, so it sees exactly what `BEGIN` saw -- repeatable read, and the
    /// timestamp the eventual prewrite will carry. Outside one, autocommit
    /// opens a fresh read transaction per statement, which is Go's implicit
    /// per-statement transaction. Like Go's oracle future, it starts after the
    /// plan-shape decision and is waited only by the first cluster read.
    ///
    /// An autocommit statement that loses the race is RUN AGAIN rather than
    /// refused, up to [`AUTOCOMMIT_RETRY_LIMIT`] times: see
    /// [`Self::may_retry_autocommit_statement`]. The loop is around the whole
    /// attempt and not around the publication, which is what makes the retry
    /// re-read -- each attempt builds its own [`transactions::StatementReadTs`]
    /// and its own deferred snapshot, so the replay reads at a new timestamp
    /// and publishes at that same new one. Retrying only the publication would
    /// resubmit the old buffer at the old `start_ts` and be the lost update
    /// again.
    fn with_statement<T>(
        &mut self,
        shape: StatementReadShape,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.with_prelocked_statement(shape, Vec::new(), run)
    }

    /// [`Self::with_statement`] for a statement whose point-write keys are
    /// already known: they are locked WITH their rows before the snapshot is
    /// bound, so the statement's own read answers from the lock response
    /// instead of storage (Go's `InitReturnValues`/
    /// `SetPessimisticLockCache` fold, `pkg/executor/point_get.go:612-624`).
    /// An empty key set is the ordinary lifecycle.
    fn with_prelocked_statement<T>(
        &mut self,
        shape: StatementReadShape,
        prelock_keys: Vec<Vec<u8>>,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.begin_if_autocommit_off()?;
        self.with_bound_statement(shape, &prelock_keys, run)
    }

    /// [`Self::with_statement`] for work the CLIENT did not ask to run: the
    /// PREPARE probe, which executes a query only to learn its result columns.
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
    /// [`Self::with_statement`] in exactly one thing: it never OPENS one.
    fn probe_statement<T>(
        &mut self,
        shape: StatementReadShape,
        run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.with_bound_statement(shape, &[], run)
    }

    /// The statement lifecycle proper: savepoint, attempt, replay budget.
    fn with_bound_statement<T>(
        &mut self,
        shape: StatementReadShape,
        prelock_keys: &[Vec<u8>],
        mut run: impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        // Go's MDL considers a RUNNING statement a user of its schema
        // version; this hold is what keeps a Go DDL owner from publishing a
        // drop under a statement mid-flight. Dropped with the scope.
        let _statement_pin = self
            .schema_pins
            .hold(self.connection_id, self.schema_version);
        let savepoint = self.buffer.staged();
        let mut retried: u32 = 0;
        let outcome = loop {
            match self.attempt_statement(shape, savepoint.clone(), &prelock_keys, &mut run) {
                Ok(value) => break Ok(value),
                Err(error) => {
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
                    self
                            .session
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
        self
                        .session
                        .retry_auto_ids()
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clean();
        outcome
    }

    /// Runs one attempt of [`Self::with_statement`]'s lifecycle.
    fn attempt_statement<T>(
        &mut self,
        shape: StatementReadShape,
        savepoint: BufferImage,
        prelock_keys: &[Vec<u8>],
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
        let result = self.attempt_statement_inner(shape, &savepoint, prelock_keys, run, &read_ts);
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
        savepoint: &BufferImage,
        prelock_keys: &[Vec<u8>],
        run: &mut impl FnMut(&mut Session) -> Result<T, SqlQueryError>,
        read_ts: &transactions::StatementReadTs,
    ) -> Result<T, SqlQueryError> {
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
            // lock response through the transaction worker's value cache.
            // Every round re-attempts until the lock stands: a fair-locking
            // conflict keeps its locks (the next attempt here is answered
            // without an RPC by the worker's held-key filter), while a rolled-
            // back conflict genuinely needs the fresh acquisition at the new
            // `for_update_ts`.
            if !prelock_keys.is_empty() {
                let outcome = match self.explicit.as_ref() {
                    Some(transaction) => Some(
                        transaction
                            .lock_staged_keys_with_values(prelock_keys.to_vec()),
                    ),
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
                Some(transaction) => match retry_read_ts {
                    Some(for_update_ts) => transaction
                        .snapshot_at(for_update_ts)
                        .map_err(SqlQueryError::unknown)?,
                    None => transaction.snapshot().map_err(SqlQueryError::unknown)?,
                },
                None if matches!(
                    shape,
                    StatementReadShape::AutocommitPointGet
                        | StatementReadShape::AutocommitSingleRowRead
                ) =>
                {
                    // Go's clustered-handle point-get optimisation reads
                    // directly at MaxTS. Keep this on the connection worker:
                    // opening a reusable transaction would add a channel hop
                    // and a pinned worker to every point read.
                    self.transactions
                        .open_max_ts_snapshot()
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
                    )
                }
                // Binding is still timestamp-free. After the statement's
                // shape is declared below, preparation starts the ordinary
                // future; the first read is what waits for and exposes its
                // snapshot.
                None => {
                    transactions::deferred_snapshot(Arc::clone(&self.transactions), read_ts.clone())
                }
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
                        self.flush_if_autocommit(read_ts.get(), write_transaction.clone())
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
        savepoint: &BufferImage,
        statement_locked: &mut Vec<Vec<u8>>,
    ) -> Result<PessimisticStep, SqlQueryError> {
        let Some(transaction) = self.explicit.as_ref() else {
            return Ok(PessimisticStep::Done);
        };
        if !transaction.is_pessimistic() {
            return Ok(PessimisticStep::Done);
        }
        let keys = tidb_exec::cluster_table_storage::pessimistic_lock_delta(
            savepoint,
            &self.buffer.staged(),
        );
        if keys.is_empty() {
            return Ok(PessimisticStep::Done);
        }
        // Every error exit rolls the STATEMENT back -- Go's `StmtRollback`
        // runs on any statement error, transport failures included.
        let outcome = match transaction.lock_staged_keys(keys) {
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
                // The transaction worker has ended itself; later statements
                // and `ROLLBACK` report the dead thread on their own.
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
    fn begin_if_autocommit_off(&mut self) -> Result<(), SqlQueryError> {
        if self.explicit.is_some() || self.session.is_autocommit() {
            return Ok(());
        }
        self.open_explicit()
    }

    fn open_explicit(&mut self) -> Result<(), SqlQueryError> {
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
            .begin(pessimistic)
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
    ) -> Result<(), SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            return Ok(());
        }
        if let Some(write_transaction) = write_transaction {
            let transaction = write_transaction
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .take();
            if let Some(transaction) = transaction {
                return match transaction.commit(&self.buffer) {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        self.buffer.reset();
                        Err(error)
                    }
                };
            }
        }
        self.commit_autocommit_buffer(read_ts)
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
    fn commit_autocommit_buffer(&mut self, read_ts: Option<u64>) -> Result<(), SqlQueryError> {
        match self.transactions.commit(&self.buffer, read_ts) {
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
            return self.commit_autocommit_buffer(None);
        };
        match transaction.commit(&self.buffer) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.buffer.reset();
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
                self.begin_if_autocommit_off()?;
                // In autocommit the statement succeeds while recording nothing,
                // exactly as the session's does -- which is what leaves a later
                // `ROLLBACK TO` to report 1305 (captured).
                if self.explicit.is_none() {
                    return Ok(());
                }
                let name = name.to_lowercase();
                let image = self.buffer.staged();
                self.savepoints.retain(|(existing, _)| *existing != name);
                self.savepoints.push((name, image));
            }
            TransactionControl::RollbackToSavepoint(name) => {
                let name = name.to_lowercase();
                if let Some(index) = self.savepoints.iter().position(|(sp, _)| *sp == name) {
                    self.buffer.restore(self.savepoints[index].1.clone());
                    self.savepoints.truncate(index + 1);
                }
            }
            TransactionControl::ReleaseSavepoint(name) => {
                let name = name.to_lowercase();
                if let Some(index) = self.savepoints.iter().position(|(sp, _)| *sp == name) {
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

    fn schema_route_prepared(
        &mut self,
        prepared: &PreparedAst,
    ) -> Result<StatementRoute, SqlQueryError> {
        self.schema_route_for_change(prepared.sql(), prepared.stored_state_change())
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
                match prepare_cluster_analyze(sql, self.session.current_database()) {
                    Ok(Some(tables)) if !tables.is_empty() => Ok(StatementRoute::Analyze(tables)),
                    Ok(_) => Err(SqlQueryError::unknown(
                        "this node runs ANALYZE TABLE for a named table only",
                    )),
                    Err(refusal) => Err(SqlQueryError::unknown(refusal.to_string())),
                }
            }
            StoredStateChange::Schema => {
                // A CREATE VIEW resolves its body against this node's own
                // catalog FIRST — a bad body fails here, at CREATE time,
                // exactly where Go's `executeCreateView` preprocess fails —
                // and the finished definition becomes the published
                // `TableInfo`.
                // The body's meta run reads through cluster storage, so it
                // needs the same statement-snapshot lifecycle the PREPARE
                // probe uses — work the client did not ask to run, reading
                // at a fresh timestamp and unbinding afterwards.
                let resolved = self.probe_statement(StatementReadShape::Unknown, |session| {
                    session.resolve_cluster_view(sql).map_err(map_error)
                })?;
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
                        self.session.begin_ddl_statement_warnings();
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

    /// Performs one cluster catalog change.
    ///
    /// An open transaction is committed first: MySQL and Go both commit
    /// implicitly before DDL, and leaving one open here would be worse than
    /// untidy -- its later statements read at a timestamp older than the
    /// change, so they would plan against a schema the cluster no longer has.
    ///
    /// The connection's own tables are not rebuilt here; its next statement
    /// finds the node's catalog moved and rebuilds them, which is the one
    /// place that decision lives.
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
            self.session.append_ddl_warning(1105, warning);
        }
        // Go answers a DDL with an OK packet carrying no rows and no insert
        // id, whether it changed anything or was an IF [NOT] EXISTS no-op.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }
}

impl QuerySession for ClusterServerSession {
    fn query_cancellation(&self) -> Option<Arc<dyn crate::sql_node::ActiveQueryCancellation>> {
        Some(Arc::new(self.session.begin_query_cancellation()))
    }

    fn wait_timeout(&self) -> std::time::Duration {
        self.session.wait_timeout()
    }

    /// This session's own `@@max_allowed_packet`; see the trait's own doc for
    /// why Go rebinds the packet reader from it on every packet.
    fn max_allowed_packet(&self) -> Option<usize> {
        self.session
            .vars()
            .get_system("max_allowed_packet")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
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

    /// Go `clientConn.initResultEncoder`'s read: this session's
    /// `@@character_set_results`.
    fn result_charset(&self) -> String {
        self.session.result_charset()
    }

    fn input_charset(&self) -> String {
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
                self.open_explicit()?;
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
        // Routed before anything else: what happens to a stored-state change
        // must not depend on which answer shape it would otherwise have taken.
        match self.schema_route(sql)? {
            StatementRoute::Ddl(statement) => return self.run_ddl(sql, &statement).map(Some),
            StatementRoute::Accounts => return self.run_account_statement(sql).map(Some),
            StatementRoute::GlobalVars => return self.run_global_var_statement(sql).map(Some),
            StatementRoute::Analyze(tables) => return self.run_analyze(&tables).map(Some),
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
        // A write declares nothing. Its read-before-write reaches the snapshot
        // as the same `get` a point-get SELECT issues, which is exactly why the
        // declaration is made from the statement rather than from the read.
        let affected_rows = self.with_statement(StatementReadShape::Unknown, move |session| {
            match session.run(&owned).map_err(map_error)? {
                StmtResult::Affected(count) => Ok(count),
                StmtResult::Done(_) => Ok(0),
                StmtResult::Rows(_) => Err(SqlQueryError::unknown(
                    "a write statement unexpectedly produced rows",
                )),
            }
        })?;
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
        let kind = prepared_ast.statement_kind(&self.session);
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
            let template = self.session.parse_statement(sql).map_err(map_error)?;
            if !matches!(template, tidb_ast::Stmt::Dml(_)) {
                return Ok(PreparedGeneral::new(
                    sql.to_owned(),
                    parameter_count,
                    Vec::new(),
                ));
            }
            return Ok(PreparedGeneral::with_template(
                sql.to_owned(),
                parameter_count,
                Vec::new(),
                template,
            ));
        }
        // Go reports a query's result columns at PREPARE time, which it gets
        // by planning the statement with every marker bound to NULL. Planning
        // reads the catalog and may read rows, so it takes a snapshot like any
        // other statement.
        let template = self.session.parse_statement(sql).map_err(map_error)?;
        // The PREPARE probe runs the statement with every marker NULL, which
        // is not the statement the client will execute; it declares nothing,
        // and -- see `probe_statement` -- it opens no transaction either.
        let probe: Vec<tidb_datatype::Datum> =
            std::iter::repeat_n(tidb_datatype::Datum::Null, parameter_count).collect();
        let zone = self.session.session_time_zone();
        let mut bound_probe = Some(prepared_ast.bind(&probe, &zone).map_err(map_error)?);
        let result_columns = self.probe_statement(StatementReadShape::Unknown, |session| {
            let bound = match bound_probe.take() {
                Some(bound) => bound,
                None => prepared_ast.bind(&probe, &zone).map_err(map_error)?,
            };
            match session.run_bound_prepared(bound) {
                Ok(StmtOutput::Rows { columns, .. }) => {
                    Ok(crate::pipeline_session::select_columns(&columns))
                }
                Err(error @ tidb_executor::DriverError::Var(_)) => Err(map_error(error)),
                // A query whose metadata cannot be resolved without real
                // values reports none at prepare time -- which a client
                // frames its EXECUTE against, so it is the shape that
                // answers `2014 Commands out of sync` rather than a harmless
                // omission. See `crate::pipeline_session::prepare_general`.
                _ => Ok(Vec::new()),
            }
        })?;
        Ok(PreparedGeneral::with_template_and_point_get_plan(
            sql.to_owned(),
            parameter_count,
            result_columns,
            template,
            point_get_plan,
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
            StatementRoute::Ordinary => {}
        }
        let params = crate::pipeline_session::prepared_parameters(values);
        let sql = statement.sql().to_owned();
        let retained = statement.template();
        let cached_point_get_plan = statement.point_get_plan().cloned();
        // YCSB's prepared point reads are a retained SELECT template whose
        // only changing value is the clustered key.  Resolve that key directly
        // from the template and execute values; cloning/binding the complete
        // AST is deferred to the refusal path.
        let fast_shape = retained
            .filter(|_| !self.session.has_session_bindings())
            .map(|template| {
                self.session
                    .fast_prepared_statement_read_shape(template, &params)
            });
        let fast = fast_shape == Some(StatementReadShape::AutocommitPointGet);
        let fast_dml = retained
            .filter(|_| !self.session.has_session_bindings())
            .map(|template| match template {
                tidb_ast::Stmt::Dml(dml) => matches!(
                    dml.as_ref(),
                    tidb_ast::DmlStmt::Insert(_) | tidb_ast::DmlStmt::Update(_)
                ),
                _ => false,
            })
            .unwrap_or(false);
        let direct = fast || fast_dml;
        let bound_template = if direct {
            None
        } else {
            retained
                .map(|template| tidb_executor::bind_statement(template.clone(), &params))
                .transpose()
                .map_err(map_error)?
        };
        let shape = if fast {
            StatementReadShape::AutocommitPointGet
        } else if fast_dml {
            match retained {
                Some(tidb_ast::Stmt::Dml(dml))
                    if matches!(dml.as_ref(), tidb_ast::DmlStmt::Update(_)) =>
                {
                    StatementReadShape::AutocommitWrite
                }
                _ => StatementReadShape::Unknown,
            }
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
        let prelock_keys = match self.explicit.as_ref() {
            Some(transaction) if transaction.is_pessimistic() => {
                if let Some(bound) = bound_template.as_ref() {
                    self.session.pessimistic_write_point_keys(bound, &[])
                } else if let Some(template) = retained {
                    self.session.pessimistic_write_point_keys(template, &params)
                } else {
                    Vec::new()
                }
            }
            _ => Vec::new(),
        };
        let output = self.with_prelocked_statement(shape, prelock_keys, move |session| {
            if fast {
                if let Some(cached) = cached_point_get_plan
                    .as_ref()
                    .and_then(|plan| session.bind_cached_prepared_point_get(plan, &params))
                {
                    return session
                        .execute_cached_prepared_point_get(cached)
                        .map_err(map_error);
                }
                if let Some(output) = session
                    .execute_fast_prepared_point_get(
                        retained.expect("fast prepared point read has a retained template"),
                        &params,
                    )
                    .map_err(map_error)?
                {
                    return Ok(output);
                }
                // A defensive refusal falls through to the ordinary path. It
                // is not expected after the pure shape check, but preserves
                // correctness if the catalog changes between classification
                // and execution.
                let bound = tidb_executor::bind_statement(
                    retained
                        .expect("fast prepared point read has a retained template")
                        .clone(),
                    &params,
                )
                .map_err(map_error)?;
                return session
                    .run_parsed_bound_owned_with_sql(bound, &sql)
                    .map_err(map_error);
            }
            if fast_dml {
                let template = retained.expect("fast prepared DML has a retained template");
                let output = match template {
                    tidb_ast::Stmt::Dml(dml)
                        if matches!(dml.as_ref(), tidb_ast::DmlStmt::Insert(_)) =>
                    {
                        session
                            .execute_fast_prepared_insert(template, &params)
                            .map_err(map_error)?
                    }
                    tidb_ast::Stmt::Dml(dml)
                        if matches!(dml.as_ref(), tidb_ast::DmlStmt::Update(_)) =>
                    {
                        session
                            .execute_fast_prepared_update(template, &params)
                            .map_err(map_error)?
                    }
                    _ => None,
                };
                if let Some(output) = output {
                    return Ok(output);
                }
                let bound =
                    tidb_executor::bind_statement(template.clone(), &params).map_err(map_error)?;
                return session
                    .run_parsed_bound_owned_with_sql(bound, &sql)
                    .map_err(map_error);
            }
            if let Some(bound) = bound_template.as_ref().cloned() {
                session
                    .run_parsed_bound_owned_with_sql(bound, &sql)
                    .map_err(map_error)
            } else {
                session.run_with_params(&sql, &params).map_err(map_error)
            }
        })?;
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
            StatementRoute::Ordinary => {}
        }
        let owned = sql.to_owned();
        let shape = self.session.statement_read_shape(sql, &[]);
        // The rows are materialized inside the statement's snapshot, because
        // the snapshot's read transaction ends when the statement does; a lazy
        // source would be reading through a finished transaction.
        let source = self.with_statement(shape, move |session| {
            let output = session.run_with_columns(&owned).map_err(map_error)?;
            Ok(match output {
                StmtOutput::Rows { columns, rows } => MaterializedResultSetSource::new(
                    crate::pipeline_session::select_columns(&columns),
                    rows,
                ),
                StmtOutput::Affected(count) => crate::pipeline_session::affected_rows_source(count),
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
