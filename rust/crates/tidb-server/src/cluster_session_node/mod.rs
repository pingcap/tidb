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
use tidb_exec::cluster_analyze::AnalyzeStatement;
use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::real_tikv_analyze::prepare_cluster_analyze;
use tidb_exec::real_tikv_ddl::prepare_cluster_ddl;
use tidb_exec::stats_watch::SharedStats;
use tidb_executor::cluster_storage::{
    BufferImage, ClusterSnapshot, ClusterTableStorage, MutationBuffer, SwappableSnapshot,
};
use tidb_executor::pushdown_scan::PushdownScanner;
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_session::privilege::PrivilegeRegistry;
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
mod statistics;
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
mod tests;
