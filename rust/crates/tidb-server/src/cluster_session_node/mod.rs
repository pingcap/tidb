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
//! per-statement transaction. Inside `BEGIN` ... `COMMIT` the connection holds
//! one [`SessionTransaction`], every statement reads through it at the single
//! timestamp `BEGIN` took, and `COMMIT` prewrites the accumulated buffer on
//! that same transaction. That is Go's one `kv.Transaction` per session: later
//! statements do not see commits made after `BEGIN` (repeatable read), and a
//! writer that raced the transaction is rejected at prewrite as a write
//! conflict instead of being silently overwritten.
//!
//! Writes never touch the slot: they stage into the connection's
//! [`MutationBuffer`], which outlives the statement. A failed statement is
//! rolled back to the buffer snapshot taken before it ran, so an explicit
//! transaction keeps exactly the writes of its statements that succeeded.
//!
//! # DDL: the one stored-schema change this node performs
//!
//! `CREATE TABLE`, `DROP TABLE`, `CREATE DATABASE` and `DROP DATABASE` are not
//! run by the session driver against its own in-memory catalog -- that copy is
//! a *read* of the cluster's schema, so changing it alone would be a silently
//! wrong answer. They are routed to the [`ClusterDdl`] seam, which publishes
//! the meta-key mutations through the same optimistic 2PC the DML path uses
//! ([`tidb_exec::real_tikv_ddl`]), and a Go TiDB then sees the object.
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
//!   `TRUNCATE`, `RENAME`, `CREATE VIEW`/`INDEX`/`SEQUENCE`, and the
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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::cluster_analyze::{AnalyzeStatement, SampleMemoryQuota, MEM_QUOTA_ANALYZE_VARIABLE};
use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::real_tikv_analyze::prepare_cluster_analyze;
use tidb_exec::real_tikv_ddl::prepare_cluster_ddl;
use tidb_exec::stats_watch::SharedStats;
use tidb_executor::cluster_storage::{
    BufferImage, ClusterSnapshot, ClusterTableStorage, MutationBuffer, SwappableSnapshot,
};
use tidb_executor::pushdown_scan::PushdownScanner;
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_session::privilege::{GlobalPriv, PrivilegeRegistry};
use tidb_session::process::ProcessRegistry;
use tidb_session::{GlobalSysvars, Session, StmtKind, StmtOutput, StmtResult, StoredStateChange};

use crate::cluster_account_seam::ClusterAccountWriter;
use crate::cluster_analyze_seam::ClusterAnalyze;
use crate::cluster_session::{cluster_session_catalog, SkippedTable};
use crate::cluster_sysvar_seam::ClusterSysvarWriter;
use crate::pipeline_session::MaterializedResultSetSource;
use crate::sql_node::{
    ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult, QuerySession,
    QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};

/// The PD/TiKV control-plane deadline this node's boot and statements use, the
/// same one the bounded node applies.
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Go `errno.ErrTableaccessDenied`.
const ER_TABLEACCESS_DENIED_ERROR: u16 = 1142;

mod boot;
mod ddl;
mod transactions;

pub use boot::run_cluster_session_node;
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
    /// Go's one process-wide `GlobalVarsAccessor`.
    global_vars: GlobalSysvars,
    /// The tables of the boot catalog no session can include, kept so the
    /// node reports them once at startup instead of per connection.
    boot_skipped: Vec<SkippedTable>,
    /// This node's loaded tables' `mysql.stats_*`, republished whole by the
    /// stats reload thread [`run_cluster_session_node`] owns. Plumbing only:
    /// the estimator that will read this is a parallel unit.
    stats: Arc<SharedStats>,
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
    ) -> Self {
        let boot_skipped =
            cluster_session_catalog(&catalog.load(), &detached_storage(), &stats.load()).skipped;
        Self {
            transactions,
            ddl,
            accounts,
            sysvars,
            analyze,
            catalog,
            privileges,
            processes: ProcessRegistry::default(),
            cop_scans: None,
            global_vars,
            boot_skipped,
            stats,
        }
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
    /// [`tidb_executor::pushdown_scan`]).
    #[must_use]
    pub fn with_cop_scans(mut self, scanner: Arc<dyn PushdownScanner>) -> Self {
        self.cop_scans = Some(scanner);
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
            storage = storage.with_pushdown_scanner(Arc::clone(scanner));
        }
        let loaded = self.catalog.load();
        let statistics = self.stats.load();
        let built = cluster_session_catalog(&loaded, &storage, &statistics);
        let mut session = Session::with_catalog(Arc::new(Mutex::new(built.catalog)));

        let identity = &context.identity;
        session.set_user(
            format!("{}@{}", identity.username(), identity.host()),
            format!("{}@{}", identity.username(), context.peer_addr.ip()),
        );
        session.set_connection_id(context.connection_id);
        if identity.in_sandbox_mode() {
            session.enable_sandbox_mode();
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
        session.attach_globals(self.global_vars.clone());

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
    /// where each statement gets its own timestamp.
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
    /// per-statement transaction.
    fn with_statement<T>(
        &mut self,
        run: impl FnOnce(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        self.rebuild_catalog_if_stale();
        let savepoint = self.buffer.staged();
        let snapshot = match self.explicit.as_ref() {
            Some(transaction) => transaction.snapshot(),
            None => self.transactions.open_snapshot(),
        }
        .map_err(SqlQueryError::unknown)?;
        if let Some(stale) = self.bind(snapshot) {
            // A previous statement that did not unbind would otherwise leave
            // its read transaction open for the rest of the connection.
            drop(stale);
        }
        let outcome = run(&mut self.session);
        let finished = self.finish_snapshot();
        match outcome {
            Ok(value) => {
                finished?;
                self.flush_if_autocommit()?;
                Ok(value)
            }
            Err(error) => {
                // The statement's own writes go; every earlier statement's
                // writes in this transaction stay.
                self.buffer.restore(savepoint);
                Err(error)
            }
        }
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

    /// Publishes the buffer when the session is not inside `BEGIN`.
    ///
    /// An empty buffer -- every read statement -- publishes nothing and spends
    /// no timestamp, as a Go COMMIT of a transaction that wrote nothing does.
    fn flush_if_autocommit(&mut self) -> Result<(), SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            return Ok(());
        }
        self.commit_autocommit_buffer()
    }

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction. A failed publication discards them, which is what a failed
    /// COMMIT does.
    fn commit_autocommit_buffer(&mut self) -> Result<(), SqlQueryError> {
        match self.transactions.commit(&self.buffer) {
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
        let Some(transaction) = self.explicit.take() else {
            return self.commit_autocommit_buffer();
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
    fn apply_savepoint(&mut self, control: &TransactionControl) {
        match control {
            TransactionControl::Savepoint(name) => {
                // Outside an explicit transaction the session recorded
                // nothing, so neither does this.
                if self.explicit.is_none() {
                    return;
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
    }

    /// Drops the explicit transaction without publishing anything, along with
    /// every write it staged.
    fn discard_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.buffer.reset();
        self.savepoints.clear();
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
        let built = cluster_session_catalog(&loaded, &self.storage, &statistics);
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
    fn schema_route(&self, sql: &str) -> Result<StatementRoute, SqlQueryError> {
        match self
            .session
            .statement_stored_state_change(sql)
            .map_err(map_error)?
        {
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
                match prepare_cluster_ddl(sql, self.session.current_database()) {
                    Ok(Some(statement)) => Ok(StatementRoute::Ddl(statement)),
                    Ok(None) => Err(SqlQueryError::unknown(
                        "this node changes the cluster's catalog for CREATE TABLE, DROP TABLE, \
                         CREATE DATABASE and DROP DATABASE only; run this statement on a TiDB \
                         server",
                    )),
                    Err(refusal) => Err(SqlQueryError::unknown(refusal.to_string())),
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
        let changed = pending.commit().map_err(SqlQueryError::unknown)?;
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

    /// Performs one `ANALYZE TABLE`, one table at a time.
    ///
    /// Each table is its own transaction, which is what Go does too: an
    /// `ANALYZE TABLE t1, t2` is two analyses, and holding one transaction
    /// open across both would make the second table's row count describe the
    /// moment the first was read.
    ///
    /// An open transaction is committed first, for the same reason a DDL or
    /// an account statement commits one: MySQL and Go both commit implicitly
    /// before a statement that changes stored state outside it.
    fn run_analyze(&mut self, tables: &[AnalyzeStatement]) -> Result<WriteOutcome, SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
        // Go checks the privileges of EVERY named table before running any
        // of them: `buildAnalyze` appends the visitInfo for each and
        // `CheckPrivilege` runs over the whole plan, so `ANALYZE TABLE ok, no`
        // stores nothing at all.
        for statement in tables {
            self.require_analyze_privileges(statement)?;
        }
        // Go's analyze memory quota is process-wide and read at execution:
        // `variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`
        // (`pkg/executor/select.go:141`), so the value in force is whatever
        // `SET GLOBAL tidb_mem_quota_analyze` last stored. Its default, `-1`,
        // is no bound.
        let memory_quota = self.analyze_memory_quota();
        for statement in tables {
            let mut statement = statement.clone();
            statement.options.memory_quota = memory_quota;
            let statement = &statement;
            let report = self
                .analyze
                .execute(statement)
                .map_err(SqlQueryError::unknown)?;
            eprintln!(
                "{{\"event\":\"cluster_table_analyzed\",\"schema\":{},\"table\":{},\
                 \"table_id\":{},\"version\":{},\"scanned_rows\":{},\"sampled_rows\":{},\
                 \"sample_rate\":{},\"histograms\":{},\"buckets\":{},\"topn\":{}}}",
                serde_json::to_string(&statement.schema).unwrap_or_else(|_| "\"\"".to_owned()),
                serde_json::to_string(&statement.table).unwrap_or_else(|_| "\"\"".to_owned()),
                report.table_id,
                report.version,
                report.scanned_rows,
                report.sampled_rows,
                report.sample_rate,
                report.histogram_count,
                report.bucket_count,
                report.topn_count,
            );
        }
        // Go answers `ANALYZE TABLE` with an OK packet carrying no rows.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Go's privilege gate on `ANALYZE TABLE`: INSERT *and* SELECT on the
    /// table.
    ///
    /// `pkg/planner/core/planbuilder.go:3205` calls
    /// `requireInsertAndSelectPriv(as.TableNames)`, which appends
    /// `mysql.InsertPriv` and then `mysql.SelectPriv` for each table, each
    /// carrying its own `ErrTableaccessDenied`. INSERT is appended first, so
    /// an account holding neither is told about INSERT -- captured from a
    /// real TiDB, for a user with no privileges and for a SELECT-only user
    /// alike:
    ///
    /// ```text
    /// ERROR 1142 (42000): INSERT command denied to user 'zzlow'@'%' for table 'zzt'
    /// ```
    ///
    /// This is not a formality on a read: the TopN entries an `ANALYZE`
    /// writes into `mysql.stats_top_n` are ACTUAL COLUMN VALUES, readable by
    /// anyone who can read the statistics.
    fn require_analyze_privileges(
        &self,
        statement: &AnalyzeStatement,
    ) -> Result<(), SqlQueryError> {
        for required in [GlobalPriv::Insert, GlobalPriv::Select] {
            if self
                .session
                .has_table_privilege(&statement.schema, &statement.table, required)
            {
                continue;
            }
            let (user, host) = self.session.authenticated_identity().unwrap_or(("", ""));
            return Err(SqlQueryError::new(
                ER_TABLEACCESS_DENIED_ERROR,
                *b"42000",
                format!(
                    "{} command denied to user '{user}'@'{host}' for table '{}'",
                    required.print_name(),
                    statement.table
                ),
            ));
        }
        Ok(())
    }

    /// `tidb_mem_quota_analyze` as this node currently holds it.
    ///
    /// A variable that is missing or unreadable is Go's default: no bound. It
    /// is not a reason to refuse an `ANALYZE`, since Go runs every one of them
    /// unbounded by default anyway.
    fn analyze_memory_quota(&self) -> SampleMemoryQuota {
        self.session
            .vars()
            .get_global(MEM_QUOTA_ANALYZE_VARIABLE)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(
                SampleMemoryQuota::unlimited,
                SampleMemoryQuota::from_setting,
            )
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
        let changed = pending.commit().map_err(SqlQueryError::unknown)?;
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
    fn run_ddl(&mut self, statement: &DdlStatement) -> Result<WriteOutcome, SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
        self.ddl
            .execute(statement)
            .map_err(SqlQueryError::unknown)?;
        // Go answers a DDL with an OK packet carrying no rows and no insert
        // id, whether it changed anything or was an IF [NOT] EXISTS no-op.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }
}

impl QuerySession for ClusterServerSession {
    /// Maps `BEGIN`/`COMMIT`/`ROLLBACK` onto the connection's buffer.
    ///
    /// The driver session owns the *state* (so `in_transaction` and the
    /// statement's OK-packet status flag agree with the in-process tier); this
    /// adds what the state means for cluster storage.
    fn control_transaction(&mut self, sql: &str) -> Result<Option<bool>, SqlQueryError> {
        let control = classify_transaction_control(sql);
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
                self.explicit = Some(self.transactions.begin().map_err(SqlQueryError::unknown)?);
            }
            Some(
                control @ (TransactionControl::Savepoint(_)
                | TransactionControl::RollbackToSavepoint(_)
                | TransactionControl::ReleaseSavepoint(_)),
            ) => self.apply_savepoint(&control),
            Some(TransactionControl::Unsupported(_)) | None => {}
        }
        Ok(Some(in_transaction))
    }

    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        // Routed before anything else: what happens to a stored-state change
        // must not depend on which answer shape it would otherwise have taken.
        match self.schema_route(sql)? {
            StatementRoute::Ddl(statement) => return self.run_ddl(&statement).map(Some),
            StatementRoute::Accounts => return self.run_account_statement(sql).map(Some),
            StatementRoute::GlobalVars => return self.run_global_var_statement(sql).map(Some),
            StatementRoute::Analyze(tables) => return self.run_analyze(&tables).map(Some),
            StatementRoute::Ordinary => {}
        }
        if self.session.apply_set(sql).map_err(map_error)?.is_some() {
            return Ok(Some(WriteOutcome {
                affected_rows: 0,
                last_insert_id: 0,
            }));
        }
        if self.session.statement_kind(sql).map_err(map_error)? != StmtKind::Write {
            return Ok(None);
        }
        let owned = sql.to_owned();
        let affected_rows =
            self.with_statement(
                move |session| match session.run(&owned).map_err(map_error)? {
                    StmtResult::Affected(count) => Ok(count),
                    StmtResult::Done(_) => Ok(0),
                    StmtResult::Rows(_) => Err(SqlQueryError::unknown(
                        "a write statement unexpectedly produced rows",
                    )),
                },
            )?;
        Ok(Some(WriteOutcome {
            affected_rows,
            last_insert_id: self.session.statement_insert_id(),
        }))
    }

    /// The catalog is refreshed first so a schema another node created since
    /// this connection opened is selectable, exactly as it is for a statement.
    fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        self.rebuild_catalog_if_stale();
        self.session.select_database(name).map_err(map_error)
    }

    fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        let parameter_count = self.session.parameter_count(sql).map_err(map_error)?;
        let kind = self.session.statement_kind(sql).map_err(map_error)?;
        if kind == StmtKind::Write {
            // A prepared DDL is admitted here and executed at EXECUTE, so a
            // refusal -- an unsupported shape, an unsupported column type --
            // is reported at PREPARE, where Go reports it too.
            self.schema_route(sql)?;
            return Ok(PreparedGeneral::new(
                sql.to_owned(),
                parameter_count,
                Vec::new(),
            ));
        }
        // Go reports a query's result columns at PREPARE time, which it gets
        // by planning the statement with every marker bound to NULL. Planning
        // reads the catalog and may read rows, so it takes a snapshot like any
        // other statement.
        let owned = sql.to_owned();
        let result_columns = self.with_statement(move |session| {
            let probe: Vec<tidb_datatype::Datum> =
                std::iter::repeat_n(tidb_datatype::Datum::Null, parameter_count).collect();
            Ok(match session.run_with_params(&owned, &probe) {
                Ok(StmtOutput::Rows { columns, .. }) => {
                    crate::pipeline_session::select_columns(&columns)
                }
                // A query whose metadata cannot be resolved without real
                // values reports none at prepare time; the execute answer
                // still carries its own.
                _ => Vec::new(),
            })
        })?;
        Ok(PreparedGeneral::new(
            sql.to_owned(),
            parameter_count,
            result_columns,
        ))
    }

    fn execute_general<'a>(
        &'a mut self,
        statement: &PreparedGeneral,
        values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        match self.schema_route(statement.sql())? {
            StatementRoute::Ddl(ddl) => {
                return self.run_ddl(&ddl).map(GeneralExecuteOutcome::Write)
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
        let output = self.with_statement(move |session| {
            session.run_with_params(&sql, &params).map_err(map_error)
        })?;
        Ok(match output {
            StmtOutput::Rows { columns, rows } => GeneralExecuteOutcome::Rows(QueryResult::new(
                Box::new(MaterializedResultSetSource::new(
                    crate::pipeline_session::select_columns(&columns),
                    rows,
                )),
            )),
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
                self.run_ddl(&statement)?;
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
        // The rows are materialized inside the statement's snapshot, because
        // the snapshot's read transaction ends when the statement does; a lazy
        // source would be reading through a finished transaction.
        let source = self.with_statement(move |session| {
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
        Ok(QueryResult::new(Box::new(source)))
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
mod tests {
    use super::*;
    use crate::cluster_account_seam::PendingAccountChange;
    use crate::configured_user_store::ConfiguredUserStore;
    use crate::resultset_source::ResultSetSource;
    use crate::sql_node::{ConnectionCancellation, ConnectionClose};
    use sha1::{Digest, Sha1};
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, AtomicUsize, Ordering};
    use tidb_ast::CiString;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_exec::cluster_catalog::{ClusterCatalog, LoadedDatabase};
    use tidb_exec::pessimistic_lock_error::{commit_outcome_to_sql_error, ERR_WRITE_CONFLICT};
    use tidb_executor::cluster_storage::SnapshotPairs;
    use tidb_executor::storage::StorageError;
    use tidb_model::column::ColumnInfo as ModelColumnInfo;
    use tidb_model::db::DBInfo;
    use tidb_model::index::{IndexColumn, IndexInfo};
    use tidb_model::{SchemaState, TableInfo};
    use tidb_txnkv::transaction::{
        CommittedTransaction, OptimisticCommitOutcome, OptimisticTransactionReceipt,
        RolledBackTransaction, TransactionCause,
    };
    use tidb_txnkv::Key;

    const ABC_HASH: &str = "*0D3CED9BEC10A777AEC23CCC353A8C08A633045E";
    const SALT: [u8; 20] = [7; 20];
    /// Go `mysql.PriKeyFlag`.
    const PRI_KEY_FLAG: u32 = 1 << 1;

    /// The committed cluster: what a statement's snapshot reads and what a
    /// COMMIT publishes into. Nothing a statement stages may appear here
    /// before its transaction commits, which is what most of these tests
    /// assert.
    #[derive(Debug, Default)]
    struct MockCluster {
        committed: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        /// The timestamp of the last commit that touched each key, which is
        /// what a prewrite at `start_ts` is checked against -- TiKV's own
        /// write-conflict rule in miniature.
        versions: Mutex<BTreeMap<Vec<u8>, u64>>,
        /// Stands in for PD: every transaction and every commit takes one.
        clock: AtomicU64,
        /// Autocommit read transactions opened, so "one statement, one
        /// snapshot" stays countable.
        opened: AtomicUsize,
        /// Explicit transactions opened by `BEGIN`.
        begun: AtomicUsize,
        /// Read handles still bound. A statement that leaks one leaves this
        /// above zero, which is the lock-left-behind failure in miniature.
        live: AtomicUsize,
        /// Publications that actually carried mutations.
        publications: AtomicUsize,
        fail_commit: AtomicBool,
    }

    impl MockCluster {
        fn rows(&self) -> usize {
            self.committed.lock().expect("committed").len()
        }

        fn timestamp(&self) -> u64 {
            self.clock.fetch_add(1, Ordering::AcqRel) + 1
        }

        fn snapshot(&self) -> BTreeMap<Vec<u8>, Vec<u8>> {
            self.committed.lock().expect("committed").clone()
        }

        /// Publishes `staged` at `commit_ts`, refusing any key another
        /// transaction committed after `start_ts`.
        ///
        /// A refusal is returned the way the real coordinator returns one: as
        /// an `Ok` outcome carrying its cause, not as an `Err`. That is the
        /// shape a caller can mistake for success, so the mock reproduces it
        /// and lets the production classifier decide what the client is told.
        fn publish(
            self: &Arc<Self>,
            staged: Vec<(Key, Option<Vec<u8>>)>,
            start_ts: u64,
        ) -> OptimisticCommitOutcome {
            let receipt = OptimisticTransactionReceipt::new(1, start_ts, b"primary".to_vec(), 1);
            let rolled_back = |cause| {
                OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
                    receipt: OptimisticTransactionReceipt::new(1, start_ts, b"primary".to_vec(), 1),
                    cause,
                })
            };
            if self.fail_commit.load(Ordering::Acquire) {
                return rolled_back(TransactionCause::Transport {
                    detail: "the mock cluster refused this publication".to_owned(),
                });
            }
            let mut versions = self.versions.lock().expect("versions");
            for (key, _) in &staged {
                if versions
                    .get(key.as_bytes())
                    .is_some_and(|last| *last > start_ts)
                {
                    return rolled_back(TransactionCause::WriteConflict {
                        detail: format!("txnStartTS={start_ts}"),
                    });
                }
            }
            let commit_ts = self.timestamp();
            let mut committed = self.committed.lock().expect("committed");
            for (key, value) in staged {
                versions.insert(key.as_bytes().to_vec(), commit_ts);
                match value {
                    Some(value) => committed.insert(key.into_bytes(), value),
                    None => committed.remove(key.as_bytes()),
                };
            }
            drop(committed);
            drop(versions);
            self.publications.fetch_add(1, Ordering::AcqRel);
            OptimisticCommitOutcome::Committed(CommittedTransaction {
                receipt,
                secondary_failures: Vec::new(),
            })
        }
    }

    #[derive(Debug)]
    struct MockSnapshot {
        data: BTreeMap<Vec<u8>, Vec<u8>>,
        cluster: Arc<MockCluster>,
    }

    impl Drop for MockSnapshot {
        fn drop(&mut self) {
            self.cluster.live.fetch_sub(1, Ordering::AcqRel);
        }
    }

    impl ClusterSnapshot for MockSnapshot {
        fn get(&mut self, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
            Ok(self.data.get(key.as_bytes()).cloned())
        }

        fn scan(
            &mut self,
            start: &Key,
            end: &Key,
            limit: Option<usize>,
        ) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
                .take(limit.unwrap_or(usize::MAX))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    /// The transaction tier the session holds: an `Arc` so the test keeps its
    /// own view of the committed store while the session writes through it.
    #[derive(Debug)]
    struct MockTransactions(Arc<MockCluster>);

    impl ClusterTransactions for MockTransactions {
        fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
            self.0.opened.fetch_add(1, Ordering::AcqRel);
            self.0.live.fetch_add(1, Ordering::AcqRel);
            let _ = self.0.timestamp();
            Ok(Box::new(MockSnapshot {
                data: self.0.snapshot(),
                cluster: Arc::clone(&self.0),
            }))
        }

        fn commit(&self, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
            let staged = buffer.staged();
            if staged.is_empty() {
                return Ok(());
            }
            // Autocommit publishes at a fresh timestamp, so nothing committed
            // before it can conflict -- exactly what an implicit
            // single-statement transaction does.
            let start_ts = self.0.timestamp();
            let outcome = self.0.publish(staged, start_ts);
            commit_outcome_to_sql_error(&outcome).map_err(sql_error)?;
            buffer.reset();
            Ok(())
        }

        fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String> {
            self.0.begun.fetch_add(1, Ordering::AcqRel);
            Ok(Box::new(MockSessionTransaction {
                start_ts: self.0.timestamp(),
                data: self.0.snapshot(),
                cluster: Arc::clone(&self.0),
            }))
        }
    }

    /// One `BEGIN` ... `COMMIT` over the mock cluster: the rows it saw at
    /// `start_ts`, served to every statement, and a publication checked against
    /// that same `start_ts`.
    #[derive(Debug)]
    struct MockSessionTransaction {
        start_ts: u64,
        data: BTreeMap<Vec<u8>, Vec<u8>>,
        cluster: Arc<MockCluster>,
    }

    impl OpenClusterTransaction for MockSessionTransaction {
        fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
            self.cluster.live.fetch_add(1, Ordering::AcqRel);
            Ok(Box::new(MockSnapshot {
                data: self.data.clone(),
                cluster: Arc::clone(&self.cluster),
            }))
        }

        fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), SqlQueryError> {
            let staged = buffer.staged();
            if staged.is_empty() {
                return Ok(());
            }
            let outcome = self.cluster.publish(staged, self.start_ts);
            commit_outcome_to_sql_error(&outcome).map_err(sql_error)?;
            buffer.reset();
            Ok(())
        }

        fn rollback(self: Box<Self>) -> Result<(), String> {
            Ok(())
        }
    }

    /// The catalog writer, offline: the meta-key encoding and the 2PC are
    /// proven by `tidb-exec`'s own `cluster_ddl_source` tests, so what is
    /// modelled here is the part this node owns -- the published catalog
    /// moving, at a new schema version, from the statement's own thread.
    ///
    /// The `TableInfo` it publishes is not invented: it is the template
    /// `lower_ddl`/`build_table_info` produced from the statement text, which
    /// is what the real path writes too.
    struct MockDdl {
        catalog: Arc<SharedClusterCatalog>,
        /// Stands in for `NextGlobalID`.
        next_id: AtomicI64,
        /// Catalog changes actually published.
        applied: AtomicUsize,
    }

    impl MockDdl {
        fn new(catalog: Arc<SharedClusterCatalog>) -> Self {
            Self {
                catalog,
                next_id: AtomicI64::new(200),
                applied: AtomicUsize::new(0),
            }
        }

        fn allocate(&self) -> i64 {
            self.next_id.fetch_add(1, Ordering::AcqRel) + 1
        }
    }

    impl ClusterDdl for MockDdl {
        fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, String> {
            let current = self.catalog.load();
            let mut next = ClusterCatalog {
                schema_version: current.schema_version + 1,
                databases: current.databases.clone(),
            };
            let find = |databases: &mut Vec<LoadedDatabase>, name: &str| -> Option<usize> {
                let name = name.to_lowercase();
                databases
                    .iter()
                    .position(|database| database.info.name.lowercase() == name)
            };
            let mut created_id = None;
            match statement {
                DdlStatement::CreateDatabase {
                    name,
                    if_not_exists,
                } => {
                    if find(&mut next.databases, name).is_some() {
                        if *if_not_exists {
                            return Ok(ClusterDdlReport::AlreadySatisfied {
                                detail: format!("database `{name}` already exists"),
                            });
                        }
                        return Err(format!("Can't create database '{name}'; database exists"));
                    }
                    let id = self.allocate();
                    created_id = Some(id);
                    next.databases.push(LoadedDatabase {
                        info: DBInfo {
                            id,
                            name: CiString::new(name.clone()),
                            ..DBInfo::default()
                        },
                        tables: Vec::new(),
                    });
                }
                DdlStatement::DropDatabase { name, if_exists } => {
                    match find(&mut next.databases, name) {
                        Some(at) => {
                            next.databases.remove(at);
                        }
                        None if *if_exists => {
                            return Ok(ClusterDdlReport::AlreadySatisfied {
                                detail: format!("database `{name}` does not exist"),
                            })
                        }
                        None => return Err(format!("Unknown database '{name}'")),
                    }
                }
                DdlStatement::CreateTable {
                    schema,
                    table,
                    if_not_exists,
                    template,
                } => {
                    let at = find(&mut next.databases, schema)
                        .ok_or_else(|| format!("Unknown database '{schema}'"))?;
                    let lowered = table.to_lowercase();
                    if next.databases[at]
                        .tables
                        .iter()
                        .any(|stored| stored.name.lowercase() == lowered)
                    {
                        if *if_not_exists {
                            return Ok(ClusterDdlReport::AlreadySatisfied {
                                detail: format!("table `{schema}`.`{table}` already exists"),
                            });
                        }
                        return Err(format!("Table '{schema}.{table}' already exists"));
                    }
                    let id = self.allocate();
                    created_id = Some(id);
                    let mut info = TableInfo::clone(template);
                    info.id = id;
                    next.databases[at].tables.push(info);
                }
                DdlStatement::DropTable {
                    schema,
                    table,
                    if_exists,
                } => {
                    let at = find(&mut next.databases, schema)
                        .ok_or_else(|| format!("Unknown database '{schema}'"))?;
                    let lowered = table.to_lowercase();
                    let found = next.databases[at]
                        .tables
                        .iter()
                        .position(|stored| stored.name.lowercase() == lowered);
                    match found {
                        Some(index) => {
                            next.databases[at].tables.remove(index);
                        }
                        None if *if_exists => {
                            return Ok(ClusterDdlReport::AlreadySatisfied {
                                detail: format!("table `{schema}`.`{table}` does not exist"),
                            })
                        }
                        None => return Err(format!("Unknown table '{schema}.{table}'")),
                    }
                }
            }
            let schema_version = next.schema_version;
            // The real writer refreshes the node's catalog inline, before it
            // answers; so does this one.
            self.catalog.store(next);
            self.applied.fetch_add(1, Ordering::AcqRel);
            Ok(ClusterDdlReport::Applied {
                schema_version,
                created_id,
            })
        }
    }

    fn column(id: i64, offset: i32, name: &str, primary: bool) -> ModelColumnInfo {
        let mut field_type = FieldType::new(FieldTypeCode::LongLong);
        if primary {
            field_type.add_flags(PRI_KEY_FLAG);
        }
        let mut column = ModelColumnInfo::new(id, name, field_type);
        column.offset = offset;
        column
    }

    /// One column shaped the way `mysql.user`/`mysql.tables_priv` shape theirs:
    /// an `ENUM`/`SET` with its declared element list.
    fn named_value_column(
        id: i64,
        offset: i32,
        name: &str,
        code: FieldTypeCode,
        elems: &[&str],
    ) -> ModelColumnInfo {
        let mut field_type = FieldType::new(code);
        field_type.set_elems(elems.iter().map(|elem| (*elem).to_owned()).collect());
        let mut column = ModelColumnInfo::new(id, name, field_type);
        column.offset = offset;
        column
    }

    /// `app.t(id BIGINT PRIMARY KEY, v BIGINT)` and
    /// `app.g(id BIGINT PRIMARY KEY, grp BIGINT)`, plus one table mid-DDL the
    /// session must refuse by name, plus `app.acct` -- `mysql.user`'s own
    /// shape, an `ENUM('N','Y')` privilege column beside a `SET` one, which is
    /// what a `SELECT ... FROM mysql.user` has to serve.
    fn loaded_catalog() -> ClusterCatalog {
        let acct = TableInfo {
            id: 104,
            name: CiString::new("acct"),
            columns: vec![
                column(1, 0, "id", true),
                named_value_column(2, 1, "select_priv", FieldTypeCode::Enum, &["N", "Y"]),
                named_value_column(
                    3,
                    2,
                    "table_priv",
                    FieldTypeCode::Set,
                    &["Select", "Insert", "Update", "Grant"],
                ),
            ],
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let t = TableInfo {
            id: 101,
            name: CiString::new("t"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "v", false)],
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let g = TableInfo {
            id: 102,
            name: CiString::new("g"),
            columns: vec![column(1, 0, "id", true), column(2, 1, "grp", false)],
            pk_is_handle: true,
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        // `app.hnd(v BIGINT UNIQUE)`: no primary key, so its row handles come
        // from `KvTable`'s own `next_handle` counter (Go's `_tidb_rowid`)
        // rather than from a column, and the unique index gives a failure that
        // lands AFTER earlier rows of the same statement are already staged.
        // Both are what the rollback tests below need.
        let hnd = TableInfo {
            id: 105,
            name: CiString::new("hnd"),
            columns: vec![column(1, 0, "v", false)],
            indices: vec![IndexInfo {
                id: 1,
                name: CiString::new("uv"),
                table: CiString::new("hnd"),
                columns: vec![IndexColumn {
                    name: CiString::new("v"),
                    offset: 0,
                    length: -1,
                    ..IndexColumn::default()
                }],
                unique: true,
                state: SchemaState::PUBLIC,
                ..IndexInfo::default()
            }],
            state: SchemaState::PUBLIC,
            ..TableInfo::default()
        };
        let pending = TableInfo {
            id: 103,
            name: CiString::new("t_pending"),
            columns: vec![column(1, 0, "id", false)],
            state: SchemaState::NONE,
            ..TableInfo::default()
        };
        ClusterCatalog {
            schema_version: 11,
            databases: vec![LoadedDatabase {
                info: DBInfo {
                    id: 5,
                    name: CiString::new("app"),
                    ..DBInfo::default()
                },
                tables: vec![t, g, pending, acct, hnd],
            }],
        }
    }

    fn scramble(password: &[u8], salt: &[u8]) -> [u8; 20] {
        let stage_one = Sha1::digest(password);
        let stage_two = Sha1::digest(stage_one);
        let mut hasher = Sha1::new();
        hasher.update(salt);
        hasher.update(stage_two);
        let challenge = hasher.finalize();
        let mut response = [0; 20];
        for ((destination, stage_one), challenge) in response
            .iter_mut()
            .zip(stage_one.iter())
            .zip(challenge.iter())
        {
            *destination = stage_one ^ challenge;
        }
        response
    }

    /// One mock node: the committed rows, the published catalog, and the
    /// catalog writer, all shared by every connection opened on it -- which is
    /// what lets a test watch one connection's DDL reach another's.
    struct MockNode {
        cluster: Arc<MockCluster>,
        catalog: Arc<SharedClusterCatalog>,
        ddl: Arc<MockDdl>,
        accounts: Arc<MockAccountWriter>,
        sysvars: Arc<MockSysvarWriter>,
    }

    /// The account seam without a cluster: the "stored" accounts are one
    /// registry, and a change is persisted by publishing the scratch copy into
    /// it. That is the whole routing contract -- read the stored table, run
    /// the statement against a scratch copy, publish only on a successful
    /// persist -- with the 2PC replaced by a switch a test can flip.
    struct MockAccountWriter {
        /// What the "cluster" stores.
        stored: PrivilegeRegistry,
        /// The node's live table, which only a committed change reaches.
        live: PrivilegeRegistry,
        /// Whether the persist step succeeds, so a test can prove that a
        /// failed persist changes neither side.
        persists: Arc<AtomicBool>,
    }

    impl MockAccountWriter {
        fn new() -> Self {
            let stored = PrivilegeRegistry::default();
            let live = PrivilegeRegistry::default();
            Self {
                stored,
                live,
                persists: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    impl ClusterAccountWriter for MockAccountWriter {
        fn begin(&self) -> Result<Box<dyn PendingAccountChange>, String> {
            // The scratch table starts as a copy of what the cluster stores,
            // which is what makes the statement validate against the cluster's
            // truth rather than this node's.
            let scratch = PrivilegeRegistry::default();
            scratch.replace_from(&clone_registry(&self.stored));
            Ok(Box::new(MockPendingChange {
                scratch,
                stored: self.stored.clone(),
                live: self.live.clone(),
                persists: Arc::clone(&self.persists),
            }))
        }
    }

    struct MockPendingChange {
        scratch: PrivilegeRegistry,
        stored: PrivilegeRegistry,
        live: PrivilegeRegistry,
        persists: Arc<AtomicBool>,
    }

    impl PendingAccountChange for MockPendingChange {
        fn registry(&self) -> PrivilegeRegistry {
            self.scratch.clone()
        }

        fn commit(self: Box<Self>) -> Result<Vec<String>, String> {
            if !self.persists.load(Ordering::Acquire) {
                return Err("the persist was rejected".to_owned());
            }
            let changed: Vec<String> = self
                .scratch
                .accounts()
                .into_iter()
                .map(|(user, host)| format!("'{user}'@'{host}'"))
                .collect();
            self.stored.replace_from(&clone_registry(&self.scratch));
            self.live.replace_from(&clone_registry(&self.scratch));
            Ok(changed)
        }
    }

    /// The sysvar seam without a cluster, mirroring [`MockAccountWriter`]
    /// exactly: the "stored" overrides are one [`GlobalSysvars`] table, and a
    /// change is persisted by publishing the scratch copy into it.
    struct MockSysvarWriter {
        stored: GlobalSysvars,
        live: GlobalSysvars,
        persists: Arc<AtomicBool>,
    }

    impl MockSysvarWriter {
        fn new() -> Self {
            Self {
                stored: GlobalSysvars::default(),
                live: GlobalSysvars::default(),
                persists: Arc::new(AtomicBool::new(true)),
            }
        }
    }

    impl crate::cluster_sysvar_seam::ClusterSysvarWriter for MockSysvarWriter {
        fn begin(
            &self,
        ) -> Result<Box<dyn crate::cluster_sysvar_seam::PendingSysvarChange>, String> {
            let scratch = GlobalSysvars::from_cluster_rows(self.stored.overrides());
            Ok(Box::new(MockPendingSysvarChange {
                scratch,
                stored: self.stored.clone(),
                live: self.live.clone(),
                persists: Arc::clone(&self.persists),
            }))
        }
    }

    struct MockPendingSysvarChange {
        scratch: GlobalSysvars,
        stored: GlobalSysvars,
        live: GlobalSysvars,
        persists: Arc<AtomicBool>,
    }

    impl crate::cluster_sysvar_seam::PendingSysvarChange for MockPendingSysvarChange {
        fn table(&self) -> GlobalSysvars {
            self.scratch.clone()
        }

        fn commit(self: Box<Self>) -> Result<Vec<String>, String> {
            if !self.persists.load(Ordering::Acquire) {
                return Err("the persist was rejected".to_owned());
            }
            let before = self.stored.overrides();
            let after = self.scratch.overrides();
            let changed: Vec<String> = after
                .iter()
                .filter(|(name, value)| before.get(*name) != Some(*value))
                .map(|(name, _)| name.clone())
                .chain(
                    before
                        .keys()
                        .filter(|name| !after.contains_key(*name))
                        .cloned(),
                )
                .collect();
            self.stored
                .replace_from(&GlobalSysvars::from_cluster_rows(after));
            self.live
                .replace_from(&GlobalSysvars::from_cluster_rows(self.stored.overrides()));
            Ok(changed)
        }
    }

    /// A detached copy of one registry's rows, since
    /// [`PrivilegeRegistry::replace_from`] empties its source.
    fn clone_registry(source: &PrivilegeRegistry) -> PrivilegeRegistry {
        let copy = PrivilegeRegistry::bootstrapped_from(Vec::new());
        for (user, host) in source.accounts() {
            if source.is_role(&user, &host) {
                copy.create_role(&user, &host);
            } else {
                copy.create_user_with_plugin(
                    &user,
                    &host,
                    &source.auth_string(&user, &host).unwrap_or_default(),
                    &source.plugin(&user, &host).unwrap_or_default(),
                );
            }
        }
        for ((user, host), mask) in source.global_priv_masks() {
            copy.grant(&user, &host, mask);
        }
        copy
    }

    /// The node IS its committed store as far as an assertion is concerned, so
    /// a test that only cares about rows and timestamps reads them directly.
    impl std::ops::Deref for MockNode {
        type Target = MockCluster;

        fn deref(&self) -> &Self::Target {
            &self.cluster
        }
    }

    impl MockNode {
        fn start() -> Self {
            let catalog = Arc::new(SharedClusterCatalog::new(loaded_catalog()));
            Self {
                cluster: Arc::new(MockCluster::default()),
                ddl: Arc::new(MockDdl::new(Arc::clone(&catalog))),
                accounts: Arc::new(MockAccountWriter::new()),
                sysvars: Arc::new(MockSysvarWriter::new()),
                catalog,
            }
        }
    }

    /// One authenticated connection over a fresh mock node, plus the node the
    /// test inspects.
    fn open_session() -> (ClusterServerSession, MockNode) {
        let node = MockNode::start();
        let session = open_session_on(&node);
        (session, node)
    }

    /// The mock node has no rows in a TiKV to sample, so its analyzer refuses
    /// by name: what these tests exercise is the ROUTE -- that `ANALYZE TABLE`
    /// reaches the statistics seam at all, and that its refusal reaches the
    /// client -- not the histogram arithmetic, which
    /// [`tidb_stats::builder`] owns and tests directly.
    struct MockAnalyze;

    impl ClusterAnalyze for MockAnalyze {
        fn execute(
            &self,
            statement: &AnalyzeStatement,
        ) -> Result<tidb_exec::real_tikv_analyze::ClusterAnalyzeReport, String> {
            Err(format!(
                "the mock node stores no statistics for `{}`.`{}`",
                statement.schema, statement.table
            ))
        }
    }

    /// A second connection to the same mock node, which is what makes a racing
    /// writer -- or a peer that must notice a DDL -- expressible in SQL rather
    /// than in raw keys.
    fn open_session_on(node: &MockNode) -> ClusterServerSession {
        let cluster = Arc::clone(&node.cluster);
        let factory = ClusterSessionFactory::new(
            Arc::new(MockTransactions(cluster)),
            Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
            Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
            Arc::clone(&node.sysvars) as Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
            Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
            Arc::clone(&node.catalog),
            node.accounts.live.clone(),
            node.sysvars.live.clone(),
            Arc::new(SharedStats::new(
                tidb_exec::stats_watch::StatsSnapshot::new(),
            )),
        );
        let users =
            ConfiguredUserStore::parse(&format!("root\t%\tmysql_native_password\t{ABC_HASH}\n"))
                .expect("configured user store");
        let identity = users
            .authenticate_native("root", "127.0.0.1", &SALT, &scramble(b"abc", &SALT))
            .expect("authenticated identity");
        let peer_addr: SocketAddr = "127.0.0.1:4000".parse().expect("peer address");
        let mut session = factory
            .open_session(SessionContext {
                connection_id: 1,
                peer_addr,
                identity,
                cancellation: ConnectionCancellation::default(),
                close: ConnectionClose::default(),
            })
            .expect("the cluster session opens");
        // The catalog is loaded, not created here: `USE` is how a connection
        // reaches it, exactly as it does over the wire.
        session.execute_write("USE app").expect("USE app");
        session
    }

    fn rows(session: &mut ClusterServerSession, sql: &str) -> Vec<Vec<Datum>> {
        let mut result = session.execute(sql).expect("the query runs");
        let source = result.source();
        let mut rows = Vec::new();
        loop {
            let batch = source.next_batch(8).expect("batch");
            if batch.is_empty() {
                break;
            }
            rows.extend(batch);
        }
        source.finish().expect("finish");
        source.close().expect("close");
        rows
    }

    /// A `SELECT` over a stored `ENUM`/`SET` column answers with the element
    /// NAME, the way MySQL prints it.
    ///
    /// This is the shape that used to abort the SQL worker: the scan decoded
    /// the row into an `Enum` datum and then panicked appending it to the
    /// output chunk, so `SELECT ... FROM mysql.user` crashed the node rather
    /// than answering. The row here is seeded the way a Go bootstrap seeds
    /// `mysql.user`'s -- written into the committed store, never through this
    /// node's own INSERT -- because that is the case that crashed.
    #[test]
    fn a_select_over_stored_enum_and_set_columns_answers_with_their_names() {
        let (mut session, cluster) = open_session();
        let row = tidb_tablecodec::encode_table_row(
            None,
            &[
                Datum::new_enum(
                    tidb_datatype::MysqlEnum::new("Y", 2),
                    tidb_datatype::Collation::Binary,
                ),
                Datum::new_set(
                    tidb_datatype::MysqlSet::new("Select,Grant", 1 | 8),
                    tidb_datatype::Collation::Binary,
                ),
            ],
            &[2, 3],
            true,
            None,
        )
        .expect("the seeded account row encodes");
        cluster.committed.lock().expect("committed").insert(
            tidb_tablecodec::table_key::encode_row_key_with_handle(
                104,
                &tidb_tablecodec::table_key::RecordHandle::Int(1),
            ),
            row,
        );

        let selected = rows(&mut session, "SELECT id, select_priv, table_priv FROM acct");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0][0], Datum::Int(1));
        match (&selected[0][1], &selected[0][2]) {
            (Datum::Enum(member, _), Datum::Set(members, _)) => {
                assert_eq!((member.name(), member.value()), ("Y", 2));
                assert_eq!((members.name(), members.value()), ("Select,Grant", 9));
            }
            other => panic!("the ENUM/SET columns came back as {other:?}"),
        }
    }

    /// The catalog a session gets is the cluster's, minus exactly the tables
    /// this tier cannot lay out -- and those are named, not hidden.
    #[test]
    fn an_unservable_table_is_refused_by_name() {
        let (session, _) = open_session();
        let skipped = session.skipped_tables();
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0].name, "app.t_pending");
        assert_eq!(
            skipped[0].reason,
            "its schema state is 0 rather than public"
        );
    }

    /// Autocommit: each statement publishes its own writes, and each statement
    /// reads at its own snapshot. The snapshot count is the proof that the
    /// per-statement lifecycle actually runs.
    #[test]
    fn autocommit_publishes_each_statement_and_takes_a_fresh_snapshot() {
        let (mut session, cluster) = open_session();
        let outcome = session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
            .expect("insert")
            .expect("a write answers with an OK packet");
        assert_eq!(outcome.affected_rows, 3);
        assert_eq!(cluster.rows(), 3);
        assert_eq!(cluster.publications.load(Ordering::Acquire), 1);

        // The next statement reads the published rows through a NEW snapshot.
        let opened_before = cluster.opened.load(Ordering::Acquire);
        let selected = rows(&mut session, "SELECT id, v FROM t ORDER BY id DESC");
        assert_eq!(selected.len(), 3);
        assert_eq!(selected[0], vec![Datum::Int(3), Datum::Int(30)]);
        assert_eq!(cluster.opened.load(Ordering::Acquire), opened_before + 1);
        // A read publishes nothing.
        assert_eq!(cluster.publications.load(Ordering::Acquire), 1);
        // Every statement's snapshot was finished; none is still bound.
        assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    }

    /// The wide-SQL surface this node exists for: a join, a subquery, an
    /// aggregate with GROUP BY, and a window function, all over cluster
    /// storage.
    #[test]
    fn wide_sql_runs_over_cluster_storage() {
        let (mut session, _) = open_session();
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
            .expect("seed t");
        session
            .execute_write("INSERT INTO g (id, grp) VALUES (1, 100), (2, 100), (3, 200)")
            .expect("seed g");

        let joined = rows(
            &mut session,
            "SELECT t.id, g.grp FROM t JOIN g ON t.id = g.id ORDER BY t.id",
        );
        assert_eq!(joined.len(), 3);
        assert_eq!(joined[2], vec![Datum::Int(3), Datum::Int(200)]);

        let grouped = rows(
            &mut session,
            "SELECT g.grp, SUM(t.v) FROM t JOIN g ON t.id = g.id GROUP BY g.grp ORDER BY g.grp",
        );
        assert_eq!(grouped.len(), 2);

        let subquery = rows(
            &mut session,
            "SELECT id FROM t WHERE id IN (SELECT id FROM g WHERE grp = 200)",
        );
        assert_eq!(subquery, vec![vec![Datum::Int(3)]]);

        let windowed = rows(
            &mut session,
            "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t ORDER BY id",
        );
        assert_eq!(windowed.len(), 3);
        assert_eq!(windowed[0][1], Datum::Int(1));
    }

    /// An explicit transaction stages every statement's writes and publishes
    /// them exactly once, at COMMIT.
    #[test]
    fn an_explicit_transaction_publishes_once_at_commit() {
        let (mut session, cluster) = open_session();
        assert_eq!(session.control_transaction("BEGIN").unwrap(), Some(true));
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("first insert");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
            .expect("second insert");
        // Staged, not published: the cluster still holds nothing.
        assert_eq!(cluster.rows(), 0);
        assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
        // The transaction's own statements still see their staged rows,
        // through the buffer in front of the snapshot.
        assert_eq!(rows(&mut session, "SELECT id FROM t").len(), 2);

        assert_eq!(session.control_transaction("COMMIT").unwrap(), Some(false));
        assert_eq!(cluster.rows(), 2);
        assert_eq!(cluster.publications.load(Ordering::Acquire), 1);
        assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    }

    /// One `BEGIN` takes one timestamp, and every statement until `COMMIT`
    /// reads through that same transaction rather than opening its own.
    #[test]
    fn an_explicit_transaction_holds_one_transaction_for_every_statement() {
        let (mut session, cluster) = open_session();
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");
        let autocommit_snapshots = cluster.opened.load(Ordering::Acquire);

        session.control_transaction("BEGIN").expect("begin");
        assert_eq!(cluster.begun.load(Ordering::Acquire), 1);
        assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
        assert_eq!(rows(&mut session, "SELECT v FROM t").len(), 1);
        session
            .execute_write("UPDATE t SET v = 11 WHERE id = 1")
            .expect("update");
        // Not one of those statements opened a transaction of its own.
        assert_eq!(cluster.begun.load(Ordering::Acquire), 1);
        assert_eq!(
            cluster.opened.load(Ordering::Acquire),
            autocommit_snapshots,
            "a statement inside BEGIN must not take a timestamp of its own"
        );
        session.control_transaction("COMMIT").expect("commit");
        assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    }

    /// Repeatable read, which holding one transaction is what buys: a statement
    /// inside `BEGIN` cannot see a commit made after `BEGIN`, because there is
    /// no newer timestamp for it to see it at. Go's default isolation level.
    #[test]
    fn a_statement_inside_begin_does_not_see_a_commit_made_after_it() {
        let (mut reader, cluster) = open_session();
        reader
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");

        reader.control_transaction("BEGIN").expect("begin");
        assert_eq!(
            rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(10)]]
        );

        let mut writer = open_session_on(&cluster);
        writer
            .execute_write("UPDATE t SET v = 99 WHERE id = 1")
            .expect("the outside writer commits");
        assert_eq!(
            rows(&mut writer, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(99)]],
            "the outside writer's own commit is durable"
        );

        assert_eq!(
            rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(10)]],
            "a repeatable read must not see a commit made after BEGIN"
        );
        reader.control_transaction("ROLLBACK").expect("rollback");
        // And once the transaction is over, the session is back at the newest
        // committed state.
        assert_eq!(
            rows(&mut reader, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(99)]]
        );
    }

    /// The conflict detection the single `start_ts` exists for: an optimistic
    /// transaction that read a row another transaction then committed cannot
    /// publish over it. Go reports 9007 at COMMIT, and the writes are gone.
    #[test]
    fn an_explicit_transaction_that_lost_the_race_fails_at_commit() {
        let (mut loser, cluster) = open_session();
        loser
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");

        loser.control_transaction("BEGIN").expect("begin");
        assert_eq!(
            rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(10)]]
        );

        let mut winner = open_session_on(&cluster);
        winner
            .execute_write("UPDATE t SET v = 99 WHERE id = 1")
            .expect("the racing writer commits first");

        loser
            .execute_write("UPDATE t SET v = 50 WHERE id = 1")
            .expect("the statement itself succeeds; nothing is published yet");
        let error = loser
            .control_transaction("COMMIT")
            .expect_err("a prewrite at the BEGIN timestamp must lose to a newer commit");
        // The code, not just the text: the client is told 9007, which is the
        // one thing a caller that only looked for `Err` from the coordinator
        // could never report.
        assert_eq!(error.code, ERR_WRITE_CONFLICT, "{}", error.message);
        assert!(
            error.message.contains("9007"),
            "a lost race is a write conflict, got: {}",
            error.message
        );

        // The winner's row stands, and the loser staged nothing for the next
        // statement to publish by accident.
        assert_eq!(
            rows(&mut loser, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(99)]]
        );
        assert_eq!(cluster.publications.load(Ordering::Acquire), 2);
    }

    /// The same race under autocommit publishes normally: each statement is its
    /// own transaction at its own timestamp, so there is no older `start_ts` to
    /// conflict with. Nothing about autocommit changed.
    #[test]
    fn autocommit_takes_a_fresh_timestamp_and_does_not_conflict() {
        let (mut first, cluster) = open_session();
        first
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");
        let mut second = open_session_on(&cluster);
        second
            .execute_write("UPDATE t SET v = 99 WHERE id = 1")
            .expect("second writer");
        first
            .execute_write("UPDATE t SET v = 50 WHERE id = 1")
            .expect("an autocommit write reads and publishes at fresh timestamps");
        assert_eq!(
            rows(&mut first, "SELECT v FROM t WHERE id = 1"),
            vec![vec![Datum::Int(50)]]
        );
        assert_eq!(cluster.begun.load(Ordering::Acquire), 0);
    }

    #[test]
    fn rollback_discards_the_transactions_writes() {
        let (mut session, cluster) = open_session();
        session.control_transaction("BEGIN").expect("begin");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("insert");
        assert_eq!(
            session.control_transaction("ROLLBACK").unwrap(),
            Some(false)
        );
        assert_eq!(cluster.rows(), 0);
        assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
        // And the discarded row is gone from the session's own view too.
        assert!(rows(&mut session, "SELECT id FROM t").is_empty());
    }

    /// A statement that fails inside a transaction takes back only its own
    /// writes; the transaction's earlier statements survive to COMMIT. This is
    /// the buffer savepoint doing its job.
    #[test]
    fn a_failed_statement_keeps_the_transactions_earlier_writes() {
        let (mut session, cluster) = open_session();
        session.control_transaction("BEGIN").expect("begin");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("first insert");
        // A write naming a column the table does not have: the statement fails
        // after the session has already opened its snapshot.
        assert!(session
            .execute_write("INSERT INTO t (id, nosuch) VALUES (2, 20)")
            .is_err());
        session
            .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
            .expect("third insert");
        session.control_transaction("COMMIT").expect("commit");

        let published = rows(&mut session, "SELECT id FROM t ORDER BY id");
        assert_eq!(published, vec![vec![Datum::Int(1)], vec![Datum::Int(3)]]);
        assert_eq!(cluster.rows(), 2);
        // The failed statement finished its read transaction like any other.
        assert_eq!(cluster.live.load(Ordering::Acquire), 0);
    }

    /// A failure outside any transaction publishes nothing at all: autocommit
    /// only flushes a statement that succeeded.
    #[test]
    fn a_failed_autocommit_statement_publishes_nothing() {
        let (mut session, cluster) = open_session();
        assert!(session
            .execute_write("INSERT INTO t (id, nosuch) VALUES (2, 20)")
            .is_err());
        assert_eq!(cluster.rows(), 0);
        assert_eq!(cluster.publications.load(Ordering::Acquire), 0);
        assert_eq!(cluster.live.load(Ordering::Acquire), 0);
        // The connection is still usable, with an empty buffer.
        session
            .execute_write("INSERT INTO t (id, v) VALUES (7, 70)")
            .expect("the next statement still runs");
        assert_eq!(cluster.rows(), 1);
    }

    /// The staged record handles of one table, in key order: the row handles
    /// this session would publish if it committed right now.
    fn staged_handles(session: &ClusterServerSession, table_id: i64) -> Vec<i64> {
        session
            .buffer
            .staged()
            .into_iter()
            .filter_map(|(key, _)| {
                match tidb_tablecodec::table_key::decode_record_key(key.as_bytes()) {
                    Ok((id, tidb_tablecodec::table_key::RecordHandle::Int(handle)))
                        if id == table_id =>
                    {
                        Some(handle)
                    }
                    _ => None,
                }
            })
            .collect()
    }

    /// A statement that fails AFTER staging some of its own rows leaves the
    /// mutation buffer byte-for-byte where it found it.
    ///
    /// This asserts the property on the cluster seam itself, which no other
    /// test here does. It matters because the guard is not the one a reader of
    /// [`tidb_session::Session`] would assume: a cluster-backed
    /// `TableStorage::clone_box` clones `Arc` HANDLES, so the catalog image
    /// the session restores on a failed statement cannot take back a staged
    /// row -- the image and the original write into the SAME buffer. What
    /// takes the row back is [`ClusterServerSession::with_statement`]'s
    /// savepoint, and this is the test that fails when it is removed.
    ///
    /// The failure shape is the load-bearing part: `VALUES (1,10),(2,20),
    /// (3,99)` stages two rows and only then hits the duplicate handle, so a
    /// missing savepoint leaves REAL bytes behind. The sibling tests all fail
    /// their statement during planning, which stages nothing and therefore
    /// passes either way.
    #[test]
    fn a_failed_statement_leaves_no_bytes_of_its_own_in_the_mutation_buffer() {
        let (mut session, cluster) = open_session();
        session.control_transaction("BEGIN").expect("begin");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
            .expect("first insert");
        let staged_before = session.buffer.staged();
        assert_eq!(staged_handles(&session, 101), vec![3]);

        assert!(session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 99)")
            .is_err());
        // Not merely "no new rows are visible": the staged bytes ARE the ones
        // the failing statement started from.
        assert_eq!(session.buffer.staged(), staged_before);

        session.control_transaction("COMMIT").expect("commit");
        assert_eq!(cluster.rows(), 1);
        assert_eq!(
            rows(&mut session, "SELECT id FROM t"),
            vec![vec![Datum::Int(3)]]
        );
    }

    /// The two counters a failed statement moves are rolled back differently,
    /// and the difference is deliberate.
    ///
    /// `KvTable::next_handle` -- the `_tidb_rowid` counter of a table with no
    /// primary key -- is a plain field of the table, so the catalog image the
    /// session restores DOES take it back: the row after a failed statement
    /// reuses the handle that statement consumed. `AutoIdAllocator` is a
    /// SHARED cell the image keeps pointing at, so an AUTO_INCREMENT burn
    /// survives instead, which is Go's rule and which
    /// `tidb_session`'s `tests_statement_rollback` pins in process. On the
    /// cluster path only the first counter is reachable at all:
    /// [`crate::cluster_session`]'s `cluster_table` sets no auto-increment
    /// offset, so no statement here can consume the allocator.
    #[test]
    fn a_failed_statement_gives_back_the_row_handle_it_consumed() {
        let (mut session, cluster) = open_session();
        session.control_transaction("BEGIN").expect("begin");
        session
            .execute_write("INSERT INTO hnd (v) VALUES (10)")
            .expect("first insert");
        assert_eq!(staged_handles(&session, 105), vec![1]);

        // Row `20` stages at handle 2; row `10` then duplicates the unique
        // index and ends the statement.
        assert!(session
            .execute_write("INSERT INTO hnd (v) VALUES (20), (10)")
            .is_err());
        assert_eq!(staged_handles(&session, 105), vec![1]);

        session
            .execute_write("INSERT INTO hnd (v) VALUES (30)")
            .expect("third insert");
        // Handle 2, not 3: the counter came back with the catalog image.
        assert_eq!(staged_handles(&session, 105), vec![1, 2]);

        session.control_transaction("COMMIT").expect("commit");
        assert_eq!(
            rows(&mut session, "SELECT v FROM hnd ORDER BY v"),
            vec![vec![Datum::Int(10)], vec![Datum::Int(30)]]
        );
        assert!(cluster.rows() > 0);
    }

    /// `ROLLBACK TO` takes the mutation buffer back to the savepoint's own
    /// bytes, leaves the transaction OPEN, and lets `COMMIT` publish exactly
    /// the writes that survived.
    ///
    /// This is the cluster-path counterpart of `tidb_session`'s savepoint
    /// tests, and it is a separate test for the same reason the statement
    /// savepoint above is: the session's catalog image cannot roll a cluster
    /// write back, so only a buffer image proves anything here.
    #[test]
    fn rollback_to_a_savepoint_restores_the_buffer_and_keeps_the_transaction_open() {
        let (mut session, cluster) = open_session();
        session.control_transaction("BEGIN").expect("begin");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("first insert");
        assert_eq!(
            session.control_transaction("SAVEPOINT s1").unwrap(),
            Some(true)
        );
        let staged_at_savepoint = session.buffer.staged();
        session
            .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
            .expect("second insert");

        // ROLLBACK TO reports the transaction still open, and the buffer holds
        // the savepoint's bytes -- not merely "row 2 is invisible".
        assert_eq!(
            session.control_transaction("ROLLBACK TO s1").unwrap(),
            Some(true)
        );
        assert_eq!(session.buffer.staged(), staged_at_savepoint);
        assert_eq!(staged_handles(&session, 101), vec![1]);
        assert_eq!(cluster.publications.load(Ordering::Acquire), 0);

        // The transaction is still running, so this statement joins it.
        session
            .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
            .expect("third insert");
        session.control_transaction("COMMIT").expect("commit");
        assert_eq!(
            rows(&mut session, "SELECT id FROM t ORDER BY id"),
            vec![vec![Datum::Int(1)], vec![Datum::Int(3)]]
        );
        assert_eq!(cluster.rows(), 2);
    }

    /// The stack rules on the cluster path: a savepoint survives its own
    /// rollback, `RELEASE` drops the named one and those above it without
    /// touching bytes, and an unknown name is Go's 1305.
    #[test]
    fn the_savepoint_stack_follows_the_same_rules_on_the_cluster_path() {
        let (mut session, _cluster) = open_session();
        session.control_transaction("BEGIN").expect("begin");
        session.control_transaction("SAVEPOINT s1").expect("s1");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("insert");
        session.control_transaction("SAVEPOINT s2").expect("s2");

        // Releasing s1 drops s2 with it and keeps row 1 staged.
        session
            .control_transaction("RELEASE SAVEPOINT s1")
            .expect("release");
        assert_eq!(staged_handles(&session, 101), vec![1]);
        for sql in ["ROLLBACK TO s1", "ROLLBACK TO s2"] {
            let reported = session.control_transaction(sql).unwrap_err();
            assert!(
                format!("{reported:?}").contains("1305"),
                "{sql} did not report 1305: {reported:?}"
            );
        }

        // A fresh savepoint, rolled back to twice: it survives its own
        // rollback, matched case-insensitively.
        session.control_transaction("SAVEPOINT S3").expect("s3");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
            .expect("insert");
        session
            .control_transaction("ROLLBACK TO s3")
            .expect("first");
        assert_eq!(staged_handles(&session, 101), vec![1]);
        session
            .execute_write("INSERT INTO t (id, v) VALUES (3, 30)")
            .expect("insert");
        session
            .control_transaction("ROLLBACK TO s3")
            .expect("second");
        assert_eq!(staged_handles(&session, 101), vec![1]);
    }

    /// Ending the transaction takes the savepoint stack with it, so a name
    /// cannot outlive the transaction that declared it and reach into the
    /// next one's buffer.
    #[test]
    fn ending_the_transaction_clears_the_cluster_savepoint_stack() {
        for ending in ["ROLLBACK", "COMMIT"] {
            let (mut session, _cluster) = open_session();
            session.control_transaction("BEGIN").expect("begin");
            session.control_transaction("SAVEPOINT s1").expect("s1");
            session.control_transaction(ending).expect("ending");
            assert!(session.savepoints.is_empty(), "{ending} kept a savepoint");
            session.control_transaction("BEGIN").expect("begin");
            assert!(
                session.control_transaction("ROLLBACK TO s1").is_err(),
                "{ending} left a savepoint reachable"
            );
        }
    }

    /// A refused publication does not leave the writes staged for the next
    /// statement to publish by accident.
    #[test]
    fn a_refused_publication_drops_the_staged_writes() {
        let (mut session, cluster) = open_session();
        cluster.fail_commit.store(true, Ordering::Release);
        assert!(session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .is_err());
        assert_eq!(cluster.rows(), 0);

        cluster.fail_commit.store(false, Ordering::Release);
        session
            .execute_write("INSERT INTO t (id, v) VALUES (2, 20)")
            .expect("the next statement publishes only its own row");
        assert_eq!(cluster.rows(), 1);
        assert_eq!(
            rows(&mut session, "SELECT id FROM t"),
            vec![vec![Datum::Int(2)]]
        );
    }

    /// An account statement reaches the account seam -- not the catalog
    /// writer, and not the session's own in-memory table alone -- and what it
    /// did becomes the cluster's stored accounts.
    #[test]
    fn an_account_statement_is_persisted_and_then_published() {
        let (mut session, node) = open_session();
        session
            .execute_write("CREATE USER 'bob'@'%' IDENTIFIED BY 'pw'")
            .expect("the account statement is routed rather than refused");
        // The cluster stores it, which is the whole point: a node that only
        // changed its own copy would answer OK about an account nowhere else
        // has.
        assert!(
            node.accounts.stored.user_exists("bob", "%"),
            "the cluster did not gain the account"
        );
        // And the node's live table has it too, so the next connection can log
        // in as it without waiting for a reload.
        assert!(node.accounts.live.user_exists("bob", "%"));
        // `CREATE USER` is a DDL node in the parser, so it would otherwise
        // reach the catalog writer; it must not.
        assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
    }

    /// The failure invariant: a persist that fails leaves neither the cluster
    /// nor the node's live table changed, and the client is told.
    #[test]
    fn a_failed_persist_changes_neither_the_cluster_nor_the_live_table() {
        let (mut session, node) = open_session();
        node.accounts.persists.store(false, Ordering::Release);
        let error = session
            .execute_write("CREATE USER 'bob'@'%'")
            .expect_err("a failed persist must fail the statement");
        assert!(error.message.contains("rejected"), "{}", error.message);
        assert!(!node.accounts.stored.user_exists("bob", "%"));
        assert!(!node.accounts.live.user_exists("bob", "%"));
        // The connection is left reading the live table, not the scratch copy
        // the failed statement mutated -- otherwise this session would keep
        // answering as if the account existed.
        assert!(session.execute("SHOW GRANTS FOR 'bob'@'%'").is_err());
    }

    /// A statement the driver itself rejects never reaches storage, and leaves
    /// the connection reading the live table.
    #[test]
    fn a_statement_the_driver_rejects_never_reaches_the_cluster() {
        let (mut session, node) = open_session();
        session
            .execute_write("CREATE USER 'bob'@'%'")
            .expect("the first CREATE USER succeeds");
        session
            .execute_write("CREATE USER 'bob'@'%'")
            .expect_err("a duplicate account must be refused by the driver");
        assert!(node.accounts.stored.user_exists("bob", "%"));
    }

    /// `ANALYZE TABLE` reaches the statistics seam rather than the ordinary
    /// statement path.
    ///
    /// The mock analyzer refuses by naming the table, so the assertion is
    /// that ITS refusal is what the client is told: had the statement stayed
    /// on the ordinary path it would have come back as an unsupported
    /// administrative statement instead, which is a different sentence and
    /// would have left the cluster's statistics silently untouched.
    #[test]
    fn analyze_table_routes_to_the_statistics_seam() {
        let (mut session, _node) = open_session();
        let refusal = session
            .execute_write("ANALYZE TABLE t")
            .expect_err("the mock node has no statistics to store")
            .message;
        assert!(
            refusal.contains("the mock node stores no statistics for"),
            "the statistics seam's own refusal must reach the client: {refusal}"
        );
        assert!(
            refusal.contains("`t`"),
            "the refusal must name the table: {refusal}"
        );
    }

    /// Opens a connection authenticated as `user`, which is how a test says
    /// "somebody other than root".
    fn open_session_as(node: &MockNode, user: &str) -> ClusterServerSession {
        let cluster = Arc::clone(&node.cluster);
        let factory = ClusterSessionFactory::new(
            Arc::new(MockTransactions(cluster)),
            Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
            Arc::clone(&node.accounts) as Arc<dyn ClusterAccountWriter>,
            Arc::clone(&node.sysvars) as Arc<dyn crate::cluster_sysvar_seam::ClusterSysvarWriter>,
            Arc::new(MockAnalyze) as Arc<dyn ClusterAnalyze>,
            Arc::clone(&node.catalog),
            node.accounts.live.clone(),
            node.sysvars.live.clone(),
            Arc::new(SharedStats::new(
                tidb_exec::stats_watch::StatsSnapshot::new(),
            )),
        );
        let users =
            ConfiguredUserStore::parse(&format!("{user}\t%\tmysql_native_password\t{ABC_HASH}\n"))
                .expect("configured user store");
        let identity = users
            .authenticate_native(user, "127.0.0.1", &SALT, &scramble(b"abc", &SALT))
            .expect("authenticated identity");
        let peer_addr: SocketAddr = "127.0.0.1:4001".parse().expect("peer address");
        let mut session = factory
            .open_session(SessionContext {
                connection_id: 2,
                peer_addr,
                identity,
                cancellation: ConnectionCancellation::default(),
                close: ConnectionClose::default(),
            })
            .expect("the cluster session opens");
        session.execute_write("USE app").expect("USE app");
        session
    }

    /// An account with no privilege on the table cannot `ANALYZE` it.
    ///
    /// The statistics an `ANALYZE` writes are not metadata: a TopN entry in
    /// `mysql.stats_top_n` is an ACTUAL COLUMN VALUE. Letting any
    /// authenticated connection analyze any table therefore hands out the
    /// table's contents, which is why Go requires INSERT and SELECT on it
    /// (`planbuilder.go:3205` `requireInsertAndSelectPriv`).
    ///
    /// The assertion is on the seam as much as on the error: the mock
    /// analyzer refuses every table by name, so reaching it at all would show
    /// up here as ITS message rather than the access-denied one.
    #[test]
    fn analyze_without_privileges_on_the_table_is_refused_before_the_seam() {
        let node = MockNode::start();
        node.accounts.live.create_user("low", "%", "");
        let mut session = open_session_as(&node, "low");
        let refusal = session
            .execute_write("ANALYZE TABLE t")
            .expect_err("an account with no privilege on `t` may not analyze it");
        // Captured from a real TiDB, for a user with no privileges at all and
        // for a SELECT-only user alike -- INSERT is the visitInfo Go appends
        // first.
        assert_eq!(refusal.code, 1142);
        assert_eq!(refusal.state, *b"42000");
        assert_eq!(
            refusal.message,
            "INSERT command denied to user 'low'@'%' for table 't'"
        );
    }

    /// SELECT alone is not enough, which is Go's answer too: the INSERT the
    /// statement needs is the one that writes `mysql.stats_*`.
    #[test]
    fn analyze_with_only_select_on_the_table_is_still_refused() {
        let node = MockNode::start();
        node.accounts.live.create_user("ro", "%", "");
        node.accounts
            .live
            .grant_table("ro", "%", "app", "t", GlobalPriv::Select.mask());
        let mut session = open_session_as(&node, "ro");
        let refusal = session
            .execute_write("ANALYZE TABLE t")
            .expect_err("SELECT alone does not carry an ANALYZE");
        assert_eq!(refusal.code, 1142);
        assert_eq!(
            refusal.message,
            "INSERT command denied to user 'ro'@'%' for table 't'"
        );
    }

    /// INSERT and SELECT on the table carry it, and the statement then
    /// reaches the statistics seam -- the grant does not have to be global.
    #[test]
    fn analyze_with_insert_and_select_on_the_table_reaches_the_seam() {
        let node = MockNode::start();
        node.accounts.live.create_user("rw", "%", "");
        node.accounts.live.grant_table(
            "rw",
            "%",
            "app",
            "t",
            GlobalPriv::Select.mask() | GlobalPriv::Insert.mask(),
        );
        let mut session = open_session_as(&node, "rw");
        let refusal = session
            .execute_write("ANALYZE TABLE t")
            .expect_err("the mock node has no statistics to store")
            .message;
        assert!(
            refusal.contains("the mock node stores no statistics for"),
            "a privileged account must reach the statistics seam: {refusal}"
        );
    }

    /// The clauses of `ANALYZE TABLE` this node does not run are refused at
    /// admission -- before a transaction is opened -- and each names itself.
    #[test]
    fn analyze_clauses_this_node_does_not_run_are_refused_by_name() {
        let (mut session, _node) = open_session();
        for (sql, expected) in [
            ("ANALYZE TABLE t INDEX i", "INDEX"),
            ("ANALYZE TABLE t PREDICATE COLUMNS", "every column"),
            ("ANALYZE TABLE t WITH 3 CMSKETCH DEPTH", "CMSketch"),
        ] {
            let refusal = session
                .execute_write(sql)
                .expect_err("this clause is not one the node runs")
                .message;
            assert!(
                refusal.contains(expected),
                "`{sql}` must be refused by naming `{expected}`: {refusal}"
            );
        }
    }

    /// A stored-schema change the cluster DDL path cannot express keeps a
    /// precise refusal -- and it names its own reason rather than a generic
    /// unsupported error.
    #[test]
    fn a_ddl_shape_the_cluster_path_cannot_express_is_refused_precisely() {
        let (mut session, node) = open_session();
        for (sql, expected) in [
            (
                "ALTER TABLE t ADD COLUMN w BIGINT",
                "CREATE TABLE, DROP TABLE",
            ),
            ("TRUNCATE TABLE t", "CREATE TABLE, DROP TABLE"),
            ("CREATE INDEX i ON t (v)", "CREATE TABLE, DROP TABLE"),
            (
                "CREATE TABLE fk (id BIGINT PRIMARY KEY, other BIGINT, \
                 FOREIGN KEY (other) REFERENCES t (id))",
                "not supported by this node",
            ),
            (
                "CREATE TABLE parts (id BIGINT PRIMARY KEY) PARTITION BY HASH (id) PARTITIONS 2",
                "not supported by this node",
            ),
        ] {
            let error = session
                .execute_write(sql)
                .expect_err("an inexpressible schema change must be refused");
            let message = error.message.clone();
            assert!(
                message.contains(expected),
                "unexpected refusal for {sql}: {message}"
            );
        }
        // Nothing was published: a refusal happens before the writer is
        // reached at all.
        assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
        assert_eq!(node.catalog.load().schema_version, 11);
    }

    /// The unit this mode gained: a `CREATE TABLE` issued through the wide-SQL
    /// session executes as a cluster catalog change, and the SAME connection
    /// can then write and read the new table -- which it can only do if its
    /// own tables were rebuilt.
    #[test]
    fn create_table_runs_and_the_same_connection_uses_the_new_table() {
        let (mut session, node) = open_session();
        let outcome = session
            .execute_write("CREATE TABLE fresh (id BIGINT PRIMARY KEY, v BIGINT)")
            .expect("the catalog change runs")
            .expect("a DDL answers with an OK packet");
        assert_eq!(outcome.affected_rows, 0);
        assert_eq!(node.ddl.applied.load(Ordering::Acquire), 1);
        assert_eq!(node.catalog.load().schema_version, 12);

        session
            .execute_write("INSERT INTO fresh (id, v) VALUES (1, 10), (2, 20)")
            .expect("the new table takes writes on the same connection");
        assert_eq!(
            rows(&mut session, "SELECT id, v FROM fresh ORDER BY id"),
            vec![
                vec![Datum::Int(1), Datum::Int(10)],
                vec![Datum::Int(2), Datum::Int(20)]
            ]
        );
        // The rows went through the ordinary write path, into the mock
        // cluster, and every statement's snapshot was finished.
        assert_eq!(node.rows(), 2);
        assert_eq!(node.live.load(Ordering::Acquire), 0);
        // The connection's older tables survived the rebuild.
        assert!(rows(&mut session, "SELECT id FROM t").is_empty());
    }

    /// `DROP TABLE` removes the table from the connection's own catalog too,
    /// so the next statement naming it fails as an unknown table rather than
    /// reading a table the cluster no longer has.
    #[test]
    fn drop_table_removes_it_from_the_connections_own_catalog() {
        let (mut session, node) = open_session();
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("seed");
        session
            .execute_write("DROP TABLE t")
            .expect("the catalog change runs");
        assert_eq!(node.catalog.load().schema_version, 12);

        let Err(error) = session.execute("SELECT id FROM t") else {
            panic!("a dropped table must not answer");
        };
        assert!(
            error.message.to_lowercase().contains("t"),
            "unexpected error: {}",
            error.message
        );
        // The sibling table is untouched.
        assert!(rows(&mut session, "SELECT id FROM g").is_empty());
    }

    /// A second connection, opened before the DDL, notices it at its next
    /// statement: the node's catalog moved, so the connection rebuilds its
    /// tables rather than serving the schema it opened with.
    #[test]
    fn a_second_connection_sees_the_new_table_after_the_ddl() {
        let node = MockNode::start();
        let mut author = open_session_on(&node);
        let mut peer = open_session_on(&node);
        // The peer is live and bound to the pre-DDL catalog.
        assert!(rows(&mut peer, "SELECT id FROM t").is_empty());

        author
            .execute_write("CREATE TABLE shared (id BIGINT PRIMARY KEY, v BIGINT)")
            .expect("the catalog change runs");
        author
            .execute_write("INSERT INTO shared (id, v) VALUES (5, 50)")
            .expect("the author writes the new table");

        assert_eq!(
            rows(&mut peer, "SELECT id, v FROM shared"),
            vec![vec![Datum::Int(5), Datum::Int(50)]],
            "a connection that outlived the DDL must serve the new table"
        );
    }

    /// `CREATE DATABASE` and `DROP DATABASE` route the same way, and `USE`
    /// reaches a database this node created.
    #[test]
    fn create_and_drop_database_route_to_the_catalog_writer() {
        let (mut session, node) = open_session();
        session
            .execute_write("CREATE DATABASE extra")
            .expect("the catalog change runs");
        session.execute_write("USE extra").expect("USE extra");
        session
            .execute_write("CREATE TABLE here (id BIGINT PRIMARY KEY)")
            .expect("a table in the new database");
        assert!(rows(&mut session, "SELECT id FROM here").is_empty());

        session
            .execute_write("DROP DATABASE extra")
            .expect("the catalog change runs");
        assert!(session.execute("SELECT id FROM here").is_err());
        assert_eq!(node.ddl.applied.load(Ordering::Acquire), 3);
    }

    /// `IF NOT EXISTS` on an object that already exists writes nothing and
    /// still answers with an OK packet, as Go does.
    #[test]
    fn an_already_satisfied_ddl_publishes_nothing() {
        let (mut session, node) = open_session();
        let outcome = session
            .execute_write("CREATE TABLE IF NOT EXISTS t (id BIGINT PRIMARY KEY)")
            .expect("an IF NOT EXISTS no-op succeeds")
            .expect("it answers with an OK packet");
        assert_eq!(outcome.affected_rows, 0);
        assert_eq!(node.ddl.applied.load(Ordering::Acquire), 0);
        assert_eq!(node.catalog.load().schema_version, 11);
    }

    /// DDL commits an open transaction first, as MySQL and Go both do. The
    /// staged writes are published rather than lost, and the transaction is
    /// over when the DDL runs.
    #[test]
    fn a_ddl_implicitly_commits_the_open_transaction() {
        let (mut session, node) = open_session();
        session.control_transaction("BEGIN").expect("begin");
        session
            .execute_write("INSERT INTO t (id, v) VALUES (1, 10)")
            .expect("staged insert");
        assert_eq!(node.rows(), 0);

        session
            .execute_write("CREATE TABLE after (id BIGINT PRIMARY KEY)")
            .expect("the catalog change runs");
        assert_eq!(node.rows(), 1, "the DDL committed the open transaction");
        assert_eq!(node.publications.load(Ordering::Acquire), 1);
        assert!(
            !session.session.in_transaction(),
            "the implicit commit ends the transaction"
        );
        // And the new table is usable straight away, which it could not be if
        // the connection still believed it was inside the old transaction.
        assert!(rows(&mut session, "SELECT id FROM after").is_empty());
    }

    /// Inside an explicit transaction the connection keeps the schema its
    /// `BEGIN` saw, exactly as it keeps its snapshot: a peer's DDL must not
    /// change the tables a running transaction reads.
    #[test]
    fn a_transaction_keeps_the_schema_its_begin_saw() {
        let node = MockNode::start();
        let mut reader = open_session_on(&node);
        let mut author = open_session_on(&node);
        reader.control_transaction("BEGIN").expect("begin");
        assert!(rows(&mut reader, "SELECT id FROM t").is_empty());

        author
            .execute_write("DROP TABLE t")
            .expect("the peer drops the table");

        assert!(
            rows(&mut reader, "SELECT id FROM t").is_empty(),
            "a statement inside BEGIN must keep the schema BEGIN saw"
        );
        reader.control_transaction("COMMIT").expect("commit");
        // Once the transaction is over the connection follows the node again.
        assert!(reader.execute("SELECT id FROM t").is_err());
    }
}
