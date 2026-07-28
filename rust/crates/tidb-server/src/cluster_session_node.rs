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
//! # What this mode refuses, and why
//!
//! * Every statement that changes stored accounts (`CREATE USER`, `GRANT`,
//!   `REVOKE`, `SET PASSWORD`, ...). The privilege registry here is a read of
//!   the cluster's `mysql.*` rows, and writing those rows through the session
//!   is a separate unit of work from the catalog DDL above; until it lands,
//!   such a statement is refused by name.
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

use tidb_exec::catalog_reload::ReloadedCatalog;
use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::cluster_table_storage::{
    commit_staged_buffer, SessionTransaction, StatementSnapshot,
};
use tidb_exec::real_tikv_catalog::{load_catalog_from_cluster, reload_catalog_from_cluster};
use tidb_exec::real_tikv_ddl::{commit_cluster_ddl, prepare_cluster_ddl, ClusterDdlReport};
use tidb_exec::real_tikv_read::{ProductionReadProcessAuthority, RealOptimisticTransactionOpener};
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SwappableSnapshot,
};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_session::privilege::PrivilegeRegistry;
use tidb_session::process::ProcessRegistry;
use tidb_session::{GlobalSysvars, Session, StmtKind, StmtOutput, StmtResult, StoredStateChange};

use crate::cluster_session::{cluster_session_catalog, SkippedTable};
use crate::node_config::NodeConfig;
use crate::pipeline_session::MaterializedResultSetSource;
use crate::real_tikv_node::{
    node_accounts, run_with_process_shutdown, spawn_catalog_reloader, spawn_schema_version_watch,
    RunConfiguredNodeError,
};
use crate::sql_node::{
    ConcurrentSqlNode, ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult,
    QuerySession, QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};

/// The PD/TiKV control-plane deadline this node's boot and statements use, the
/// same one the bounded node applies.
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything a connection needs from the cluster's transaction tier: one
/// fresh read snapshot per autocommit statement, one publication of its staged
/// writes, and the single transaction an explicit `BEGIN` holds open.
///
/// The seam exists so the statement lifecycle -- which is the correctness core
/// of this mode -- is exercised without a cluster. The production
/// implementation is [`RealClusterTransactions`]; the tests drive the same
/// lifecycle against an in-memory committed store.
pub trait ClusterTransactions: Send + Sync {
    /// Opens one autocommit statement's read snapshot at its own timestamp.
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes one autocommit statement's staged writes as its own
    /// transaction, then empties the buffer. An empty buffer publishes nothing.
    fn commit(&self, buffer: &MutationBuffer) -> Result<(), String>;

    /// Opens the one transaction an explicit `BEGIN` holds until `COMMIT` or
    /// `ROLLBACK`.
    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String>;
}

/// The transaction an explicit `BEGIN` holds open across its statements.
///
/// Every statement of the transaction reads through [`Self::snapshot`], so they
/// all share the timestamp `BEGIN` took, and [`Self::commit`] prewrites at that
/// same timestamp -- which is what makes a racing writer a write conflict
/// instead of a silent overwrite.
pub trait OpenClusterTransaction: Send {
    /// One statement's read handle. Dropping it ends the statement, never the
    /// transaction.
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes the staged writes at the transaction's own start timestamp and
    /// empties the buffer.
    fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), String>;

    /// Ends the transaction without publishing anything.
    fn rollback(self: Box<Self>) -> Result<(), String>;
}

/// The production transaction tier: real read-only transactions and the
/// optimistic 2PC, both over the node's one process authority.
pub struct RealClusterTransactions {
    opener: Arc<RealOptimisticTransactionOpener>,
    timeout: Duration,
}

impl RealClusterTransactions {
    /// Binds the tier to an already-connected authority's write capability.
    #[must_use]
    pub fn new(opener: RealOptimisticTransactionOpener, timeout: Duration) -> Self {
        Self {
            opener: Arc::new(opener),
            timeout,
        }
    }
}

impl ClusterTransactions for RealClusterTransactions {
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        StatementSnapshot::open(Arc::clone(&self.opener), self.timeout)
            .map(|snapshot| Box::new(snapshot) as Box<dyn ClusterSnapshot>)
            .map_err(|error| error.to_string())
    }

    fn commit(&self, buffer: &MutationBuffer) -> Result<(), String> {
        commit_staged_buffer(&self.opener, buffer, self.timeout)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn begin(&self) -> Result<Box<dyn OpenClusterTransaction>, String> {
        SessionTransaction::begin(Arc::clone(&self.opener), self.timeout)
            .map(|transaction| Box::new(transaction) as Box<dyn OpenClusterTransaction>)
            .map_err(|error| error.to_string())
    }
}

impl OpenClusterTransaction for SessionTransaction {
    fn snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String> {
        SessionTransaction::snapshot(self).map_err(|error| error.to_string())
    }

    fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), String> {
        SessionTransaction::commit(*self, buffer).map(|_| ())
    }

    fn rollback(self: Box<Self>) -> Result<(), String> {
        SessionTransaction::rollback(*self)
    }
}

/// This node's one route to the cluster's stored schema.
///
/// The seam exists for the same reason [`ClusterTransactions`] does: the
/// routing decision -- which statements become catalog changes, what happens
/// to an open transaction, when the connection's tables are rebuilt -- is
/// exercised without a cluster. The production implementation is
/// [`RealClusterDdl`].
pub trait ClusterDdl: Send + Sync {
    /// Publishes one admitted catalog change, then brings this node's own
    /// catalog up to it before answering.
    ///
    /// The two halves are one method because a caller that published without
    /// refreshing would answer the next statement from a catalog it knows to
    /// be stale.
    fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, String>;
}

/// The production catalog writer: the optimistic 2PC over the node's one
/// process authority, followed by an inline reload of the node's own catalog.
pub struct RealClusterDdl {
    opener: Arc<RealOptimisticTransactionOpener>,
    catalog: Arc<SharedClusterCatalog>,
    timeout: Duration,
}

impl RealClusterDdl {
    /// Binds the writer to an already-connected authority and the catalog slot
    /// the reload thread publishes into.
    #[must_use]
    pub fn new(
        opener: RealOptimisticTransactionOpener,
        catalog: Arc<SharedClusterCatalog>,
        timeout: Duration,
    ) -> Self {
        Self {
            opener: Arc::new(opener),
            catalog,
            timeout,
        }
    }

    /// Runs one reload pass inline, on the statement's own thread.
    ///
    /// Go's DDL owner PUTs the new version to etcd so every *other* node's
    /// watch fires; this node is the one that just wrote the change, so it
    /// needs no notification -- it reloads at once instead of waiting up to
    /// `lease/2` for the reload thread's tick. Both publishers replace the
    /// catalog whole in the same slot, so neither can observe the other
    /// half-applied.
    ///
    /// A failed reload is not a failed DDL: the change is committed in the
    /// cluster, and the lease tick will pick it up. Reporting the statement as
    /// failed would be a lie about what the cluster now holds, so the failure
    /// is emitted and the statement stands.
    fn refresh_catalog(&self) {
        let current = self.catalog.load();
        match reload_catalog_from_cluster(&self.opener, self.timeout, &current) {
            Ok(ReloadedCatalog::Unchanged { .. }) => {}
            Ok(ReloadedCatalog::Diffs { catalog, .. } | ReloadedCatalog::Full { catalog, .. }) => {
                self.catalog.store(catalog);
            }
            Err(error) => eprintln!(
                "{{\"event\":\"catalog_reload_after_ddl_failed\",\"schema_version\":{},\"error\":{:?}}}",
                current.schema_version,
                error.to_string()
            ),
        }
    }
}

impl ClusterDdl for RealClusterDdl {
    fn execute(&self, statement: &DdlStatement) -> Result<ClusterDdlReport, String> {
        let report = commit_cluster_ddl(&self.opener, statement, self.timeout)
            .map_err(|error| error.to_string())?;
        self.refresh_catalog();
        Ok(report)
    }
}

/// Opens one cluster-backed wide-SQL [`Session`] per authenticated connection.
pub struct ClusterSessionFactory {
    /// The write/read capability every connection's statements open their
    /// snapshots and publish their commits through.
    transactions: Arc<dyn ClusterTransactions>,
    /// The route a stored-schema change this node can express takes.
    ddl: Arc<dyn ClusterDdl>,
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
    /// Go's one process-wide `GlobalVarsAccessor`.
    global_vars: GlobalSysvars,
    /// The tables of the boot catalog no session can include, kept so the
    /// node reports them once at startup instead of per connection.
    boot_skipped: Vec<SkippedTable>,
}

impl ClusterSessionFactory {
    /// Binds the factory to an authority that has already read the cluster's
    /// catalog and accounts.
    #[must_use]
    pub fn new(
        transactions: Arc<dyn ClusterTransactions>,
        ddl: Arc<dyn ClusterDdl>,
        catalog: Arc<SharedClusterCatalog>,
        privileges: PrivilegeRegistry,
    ) -> Self {
        let boot_skipped = cluster_session_catalog(&catalog.load(), &detached_storage()).skipped;
        Self {
            transactions,
            ddl,
            catalog,
            privileges,
            processes: ProcessRegistry::default(),
            global_vars: GlobalSysvars::default(),
            boot_skipped,
        }
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
        let storage = ClusterTableStorage::new(buffer.clone(), handle);
        let loaded = self.catalog.load();
        let built = cluster_session_catalog(&loaded, &storage);
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
            catalog: Arc::clone(&self.catalog),
            schema_version: loaded.schema_version,
            explicit: None,
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
    /// The node's catalog, which this connection follows.
    catalog: Arc<SharedClusterCatalog>,
    /// The schema version `session`'s tables were built from. A move in
    /// `catalog` past this is what makes the connection rebuild them.
    schema_version: i64,
    /// The transaction an explicit `BEGIN` holds open. `None` is autocommit,
    /// where each statement gets its own timestamp.
    explicit: Option<Box<dyn OpenClusterTransaction>>,
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
                Err(SqlQueryError::unknown(error))
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
        let Some(transaction) = self.explicit.take() else {
            return self.commit_autocommit_buffer();
        };
        match transaction.commit(&self.buffer) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.buffer.reset();
                Err(SqlQueryError::unknown(error))
            }
        }
    }

    /// Drops the explicit transaction without publishing anything, along with
    /// every write it staged.
    fn discard_explicit(&mut self) -> Result<(), SqlQueryError> {
        self.buffer.reset();
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
        if loaded.schema_version == self.schema_version {
            return;
        }
        let built = cluster_session_catalog(&loaded, &self.storage);
        let shared = self.session.shared_catalog();
        let mut catalog = shared.lock().unwrap_or_else(|poison| poison.into_inner());
        *catalog = built.catalog;
        drop(catalog);
        self.schema_version = loaded.schema_version;
        self.skipped = built.skipped;
    }

    /// Decides what this mode does with a statement that changes stored state:
    /// run it as a cluster catalog change, or refuse it with its own reason.
    ///
    /// `None` means the statement changes nothing stored and takes its
    /// ordinary path. Every refusal is specific, because the reasons are: an
    /// account statement has no write path here at all, an `ALTER` is a DDL
    /// shape the cluster path cannot express, and a `CREATE TABLE` with a
    /// foreign key is a clause it refuses by name.
    fn schema_route(&self, sql: &str) -> Result<Option<DdlStatement>, SqlQueryError> {
        match self
            .session
            .statement_stored_state_change(sql)
            .map_err(map_error)?
        {
            StoredStateChange::None => Ok(None),
            StoredStateChange::Accounts => Err(SqlQueryError::unknown(
                "this node reads the cluster's accounts and cannot write them; run CREATE USER, \
                 GRANT, REVOKE or SET PASSWORD on a TiDB server",
            )),
            StoredStateChange::Schema => {
                match prepare_cluster_ddl(sql, self.session.current_database()) {
                    Ok(Some(statement)) => Ok(Some(statement)),
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
            Some(TransactionControl::Unsupported(_)) | None => {}
        }
        Ok(Some(in_transaction))
    }

    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        // Routed before anything else: what happens to a stored-state change
        // must not depend on which answer shape it would otherwise have taken.
        if let Some(statement) = self.schema_route(sql)? {
            return self.run_ddl(&statement).map(Some);
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
        if let Some(ddl) = self.schema_route(statement.sql())? {
            return self.run_ddl(&ddl).map(GeneralExecuteOutcome::Write);
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
        if let Some(statement) = self.schema_route(sql)? {
            self.run_ddl(&statement)?;
            return Ok(QueryResult::new(Box::new(
                crate::pipeline_session::affected_rows_source(0),
            )));
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

/// Starts the convergence node: wide SQL over cluster storage and cluster
/// accounts, served on the MySQL port.
pub fn run_cluster_session_node(config: NodeConfig) -> Result<(), RunConfiguredNodeError> {
    let mut loaded = None;
    let authority = ProductionReadProcessAuthority::connect_with_catalog(
        config.pd_endpoints.clone(),
        CONTROL_PLANE_TIMEOUT,
        |opener| {
            loaded = Some(
                load_catalog_from_cluster(opener, CONTROL_PLANE_TIMEOUT)
                    .map_err(|error| error.to_string())?,
            );
            // The authority insists on naming one bounded-read table because
            // the single-relation coprocessor path is built around one
            // relation. This node never opens a bounded read session -- every
            // statement goes through the session driver -- so the table is
            // inert, and naming a real one would only make startup depend on
            // the cluster happening to hold a table of that shape.
            Ok(ConfiguredTable::new(
                "",
                "",
                1,
                Vec::<ConfiguredColumn>::new(),
            ))
        },
    )
    .map_err(|error| RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string())))?;
    let startup = loaded.expect("the catalog closure ran exactly once");
    let schema_version = startup.schema_version;

    // `node_accounts` also hands back the privilege reloader (landed in
    // parallel); it must stay alive for the node's run and drop before the
    // authority's shutdown drain, like the catalog reloader below.
    let (users, privilege_reloader) = node_accounts(&config, &authority)?;
    let (catalog, reloader) =
        spawn_catalog_reloader(startup, authority.transaction_opener(), config.schema_lease)
            .map_err(|error| {
                RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
            })?;
    // The watch only makes the reload *prompt*; the tick above is what makes
    // it correct. It is listed before the reloader in the tuple below so it is
    // dropped first: a watch may not outlive the thread it nudges.
    let watcher = spawn_schema_version_watch(&config, &reloader);
    let factory = Arc::new(ClusterSessionFactory::new(
        Arc::new(RealClusterTransactions::new(
            authority.transaction_opener(),
            CONTROL_PLANE_TIMEOUT,
        )),
        Arc::new(RealClusterDdl::new(
            authority.transaction_opener(),
            Arc::clone(&catalog),
            CONTROL_PLANE_TIMEOUT,
        )),
        catalog,
        users.accounts(),
    ));
    let skipped = render_skipped(factory.boot_skipped_tables());

    run_with_process_shutdown(
        (factory, watcher, reloader, privilege_reloader),
        authority,
        move |(factory, watcher, reloader, privilege_reloader)| {
            let node =
                ConcurrentSqlNode::bind(&config, factory, Arc::clone(&users)).map_err(|error| {
                    crate::real_tikv_node::emit_connections_startup_failure(&error);
                    RunConfiguredNodeError::Node(error)
                })?;
            let address = node.local_addr().map_err(|error| {
                crate::real_tikv_node::emit_connections_startup_failure(&error);
                RunConfiguredNodeError::Node(error)
            })?;
            let shutdown = node.shutdown_handle();
            ctrlc::set_handler(move || shutdown.shutdown()).map_err(|error| {
                crate::real_tikv_node::emit_connections_startup_failure(&error);
                RunConfiguredNodeError::Signal(error)
            })?;
            eprintln!(
            "{{\"event\":\"cluster_session_node_ready\",\"address\":\"{address}\",\"schema_version\":{schema_version},\"max_connections\":{},\"account_count\":{},\"skipped_tables\":[{skipped}]}}",
            config.max_connections,
            users.len(),
        );
            let outcome = node.run().map_err(RunConfiguredNodeError::Node);
            // The reload threads hold their own transaction openers; joining
            // them here releases those PD handles before the authority's
            // shutdown drain. The watch goes first: it nudges the reloader,
            // so it must not outlive it.
            drop(watcher);
            drop(reloader);
            drop(privilege_reloader);
            outcome
        },
    )
}

/// Storage bound to no snapshot, used only to decide which tables a catalog
/// *could* be built over. Deciding that reads a `TableInfo`, never a row.
fn detached_storage() -> ClusterTableStorage {
    let slot: Arc<Mutex<dyn ClusterSnapshot>> = Arc::new(Mutex::new(SwappableSnapshot::new()));
    ClusterTableStorage::new(MutationBuffer::new(), slot)
}

/// Renders the boot-time refusals for the node's ready event.
fn render_skipped(skipped: &[SkippedTable]) -> String {
    skipped
        .iter()
        .map(|table| {
            format!(
                "{{\"table\":{:?},\"reason\":{:?}}}",
                table.name, table.reason
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
mod tests {
    use super::*;
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
    use tidb_executor::cluster_storage::SnapshotPairs;
    use tidb_executor::storage::StorageError;
    use tidb_model::column::ColumnInfo as ModelColumnInfo;
    use tidb_model::db::DBInfo;
    use tidb_model::{SchemaState, TableInfo};
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
        fn publish(
            self: &Arc<Self>,
            staged: Vec<(Key, Option<Vec<u8>>)>,
            start_ts: u64,
        ) -> Result<(), String> {
            if self.fail_commit.load(Ordering::Acquire) {
                return Err("the mock cluster refused this publication".to_owned());
            }
            let mut versions = self.versions.lock().expect("versions");
            for (key, _) in &staged {
                if versions
                    .get(key.as_bytes())
                    .is_some_and(|last| *last > start_ts)
                {
                    return Err(format!(
                        "[kv:9007]Write conflict, startTS={start_ts} [try again later]"
                    ));
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
            Ok(())
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

        fn scan(&mut self, start: &Key, end: &Key) -> Result<SnapshotPairs, StorageError> {
            Ok(self
                .data
                .range(start.as_bytes().to_vec()..end.as_bytes().to_vec())
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

        fn commit(&self, buffer: &MutationBuffer) -> Result<(), String> {
            let staged = buffer.staged();
            if staged.is_empty() {
                return Ok(());
            }
            // Autocommit publishes at a fresh timestamp, so nothing committed
            // before it can conflict -- exactly what an implicit
            // single-statement transaction does.
            let start_ts = self.0.timestamp();
            self.0.publish(staged, start_ts)?;
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

        fn commit(self: Box<Self>, buffer: &MutationBuffer) -> Result<(), String> {
            let staged = buffer.staged();
            if staged.is_empty() {
                return Ok(());
            }
            self.cluster.publish(staged, self.start_ts)?;
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

    /// `app.t(id BIGINT PRIMARY KEY, v BIGINT)` and
    /// `app.g(id BIGINT PRIMARY KEY, grp BIGINT)`, plus one table mid-DDL the
    /// session must refuse by name.
    fn loaded_catalog() -> ClusterCatalog {
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
                tables: vec![t, g, pending],
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

    /// A second connection to the same mock node, which is what makes a racing
    /// writer -- or a peer that must notice a DDL -- expressible in SQL rather
    /// than in raw keys.
    fn open_session_on(node: &MockNode) -> ClusterServerSession {
        let cluster = Arc::clone(&node.cluster);
        let factory = ClusterSessionFactory::new(
            Arc::new(MockTransactions(cluster)),
            Arc::clone(&node.ddl) as Arc<dyn ClusterDdl>,
            Arc::clone(&node.catalog),
            PrivilegeRegistry::default(),
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

    /// The account statements stay refused: the privilege registry here is a
    /// read of the cluster's `mysql.*` rows, and writing those rows through
    /// the session is a separate piece of work from the catalog DDL below.
    #[test]
    fn account_changes_are_refused_by_name() {
        let (mut session, _) = open_session();
        for sql in [
            "CREATE USER 'bob'@'%'",
            "GRANT SELECT ON app.t TO 'bob'@'%'",
            "SET PASSWORD FOR 'bob'@'%' = 'x'",
        ] {
            let error = session
                .execute_write(sql)
                .expect_err("an account change must be refused");
            let message = error.message.clone();
            assert!(
                message.contains("cannot write them"),
                "unexpected refusal for {sql}: {message}"
            );
        }
        // A GRANT parses as an administrative statement, so it would otherwise
        // take the result-set path; the refusal covers that one too.
        assert!(session
            .execute("GRANT SELECT ON app.t TO 'bob'@'%'")
            .is_err());
        // `CREATE USER` is a DDL node in the parser, so it would otherwise
        // reach the catalog writer; it must not.
        let error = session
            .execute_write("CREATE USER 'bob'@'%'")
            .expect_err("CREATE USER is an account change, not a catalog change");
        assert!(error.message.contains("CREATE USER"), "{}", error.message);
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
