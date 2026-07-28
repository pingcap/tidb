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
//! One statement is one read snapshot. The connection's tables are built once,
//! against a [`SwappableSnapshot`] slot every table shares; before a statement
//! the connection opens a [`StatementSnapshot`] into that slot, and afterwards
//! it takes it back and finishes the read transaction -- whether the statement
//! succeeded or failed, so a failure never leaves a lock behind.
//!
//! Writes never touch the slot: they stage into the connection's
//! [`MutationBuffer`], which outlives the statement. Autocommit publishes that
//! buffer at the end of each successful statement; inside `BEGIN` ... `COMMIT`
//! it accumulates and is published once. A failed statement is rolled back to
//! the buffer snapshot taken before it ran, so an explicit transaction keeps
//! exactly the writes of its statements that succeeded.
//!
//! # What this mode refuses, and why
//!
//! * Every statement that changes stored schema or accounts (`CREATE TABLE`,
//!   `ALTER`, `DROP`, `CREATE USER`, `GRANT`, ...). The catalog and the
//!   registry here are *reads* of the cluster's state; executing such a
//!   statement would change this process's copy alone, which is a silently
//!   wrong answer. It is refused by name until the cluster DDL path is wired
//!   in.
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

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_exec::catalog_watch::SharedCatalog as SharedClusterCatalog;
use tidb_exec::cluster_table_storage::{commit_staged_buffer, StatementSnapshot};
use tidb_exec::real_tikv_catalog::load_catalog_from_cluster;
use tidb_exec::real_tikv_read::{ProductionReadProcessAuthority, RealOptimisticTransactionOpener};
use tidb_executor::cluster_storage::{
    ClusterSnapshot, ClusterTableStorage, MutationBuffer, SwappableSnapshot,
};
use tidb_planner::read_only_scan::{ConfiguredColumn, ConfiguredTable};
use tidb_planner::transaction_control::{classify_transaction_control, TransactionControl};
use tidb_session::privilege::PrivilegeRegistry;
use tidb_session::process::ProcessRegistry;
use tidb_session::{GlobalSysvars, Session, StmtKind, StmtOutput, StmtResult};

use crate::cluster_session::{cluster_session_catalog, SkippedTable};
use crate::node_config::NodeConfig;
use crate::pipeline_session::MaterializedResultSetSource;
use crate::real_tikv_node::{
    node_accounts, run_with_process_shutdown, spawn_catalog_reloader, RunConfiguredNodeError,
};
use crate::sql_node::{
    ConcurrentSqlNode, ConnectionKillTarget, GeneralExecuteOutcome, PreparedGeneral, QueryResult,
    QuerySession, QuerySessionFactory, SessionContext, SqlQueryError, WriteOutcome,
};

/// The PD/TiKV control-plane deadline this node's boot and statements use, the
/// same one the bounded node applies.
const CONTROL_PLANE_TIMEOUT: Duration = Duration::from_secs(5);

/// Everything a connection needs from the cluster's transaction tier: one
/// fresh read snapshot per statement, and one publication of its staged
/// writes.
///
/// The seam exists so the statement lifecycle -- which is the correctness core
/// of this mode -- is exercised without a cluster. The production
/// implementation is [`RealClusterTransactions`]; the tests drive the same
/// lifecycle against an in-memory committed store.
pub trait ClusterTransactions: Send + Sync {
    /// Opens one statement's read snapshot at a single timestamp.
    fn open_snapshot(&self) -> Result<Box<dyn ClusterSnapshot>, String>;

    /// Publishes every staged write as one transaction, then empties the
    /// buffer. An empty buffer publishes nothing.
    fn commit(&self, buffer: &MutationBuffer) -> Result<(), String>;
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
}

/// Opens one cluster-backed wide-SQL [`Session`] per authenticated connection.
pub struct ClusterSessionFactory {
    /// The write/read capability every connection's statements open their
    /// snapshots and publish their commits through.
    transactions: Arc<dyn ClusterTransactions>,
    /// The cluster catalog, republished whole by the reload thread. A
    /// connection takes one `Arc` at open and keeps it, so no session ever
    /// sees a half-updated catalog.
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
        catalog: Arc<SharedClusterCatalog>,
        privileges: PrivilegeRegistry,
    ) -> Self {
        let boot_skipped = cluster_session_catalog(&catalog.load(), &detached_storage()).skipped;
        Self {
            transactions,
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
            transactions: Arc::clone(&self.transactions),
            skipped: Arc::new(built.skipped),
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
    transactions: Arc<dyn ClusterTransactions>,
    /// Tables of the cluster this connection's catalog could not include,
    /// answered by name when a statement names one.
    skipped: Arc<Vec<SkippedTable>>,
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
    /// The ordering is the correctness core: bind a fresh snapshot, take a
    /// buffer savepoint, run, always finish the read transaction, and only
    /// then decide what happens to the staged writes.
    fn with_statement<T>(
        &mut self,
        run: impl FnOnce(&mut Session) -> Result<T, SqlQueryError>,
    ) -> Result<T, SqlQueryError> {
        let savepoint = self.buffer.staged();
        let snapshot = self
            .transactions
            .open_snapshot()
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

    /// Unbinds the statement's snapshot and ends its read transaction.
    fn finish_snapshot(&self) -> Result<(), SqlQueryError> {
        let bound = self
            .slot
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .unbind();
        // Dropping the handle finishes the transaction on its own thread; the
        // drop is what makes that ordering unconditional.
        drop(bound);
        Ok(())
    }

    /// Publishes the buffer when the session is not inside `BEGIN`.
    ///
    /// An empty buffer -- every read statement -- publishes nothing and spends
    /// no timestamp, as a Go COMMIT of a transaction that wrote nothing does.
    fn flush_if_autocommit(&mut self) -> Result<(), SqlQueryError> {
        if self.session.in_transaction() {
            return Ok(());
        }
        self.commit_buffer()
    }

    /// Publishes the staged writes as one optimistic transaction. A failed
    /// publication discards them, which is what a failed COMMIT does.
    fn commit_buffer(&mut self) -> Result<(), SqlQueryError> {
        match self.transactions.commit(&self.buffer) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.buffer.reset();
                Err(SqlQueryError::unknown(error))
            }
        }
    }

    /// Refuses a statement that would change stored schema or accounts.
    ///
    /// The refusal names the statement's effect rather than reporting a
    /// generic parse or unsupported error, because the reason is specific to
    /// this mode: the catalog here is a read of the cluster's schema.
    fn refuse_stored_schema_change(&self, sql: &str) -> Result<(), SqlQueryError> {
        let changes = self
            .session
            .statement_changes_stored_schema(sql)
            .map_err(map_error)?;
        if changes {
            return Err(SqlQueryError::unknown(
                "this node serves a catalog loaded from the cluster and cannot change stored \
                 schema or accounts; run DDL, CREATE USER or GRANT on a TiDB server",
            ));
        }
        Ok(())
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
            Some(TransactionControl::Commit) => self.commit_buffer()?,
            // A ROLLBACK drops the staged writes. So does a BEGIN: a leftover
            // buffer at that point could only come from a statement outside
            // any transaction whose autocommit already published it.
            Some(TransactionControl::Rollback | TransactionControl::Begin { .. }) => {
                self.buffer.reset();
            }
            Some(TransactionControl::Unsupported(_)) | None => {}
        }
        Ok(Some(in_transaction))
    }

    fn execute_write(&mut self, sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        // Refused before anything else: a stored-schema change must not depend
        // on which answer shape it would otherwise have taken.
        self.refuse_stored_schema_change(sql)?;
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
            self.refuse_stored_schema_change(sql)?;
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
        self.refuse_stored_schema_change(statement.sql())?;
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
        self.refuse_stored_schema_change(sql)?;
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

    let users = node_accounts(&config, &authority)?;
    let (catalog, reloader) =
        spawn_catalog_reloader(startup, authority.transaction_opener(), config.schema_lease)
            .map_err(|error| {
                RunConfiguredNodeError::Engine(SqlQueryError::unknown(error.to_string()))
            })?;
    let factory = Arc::new(ClusterSessionFactory::new(
        Arc::new(RealClusterTransactions::new(
            authority.transaction_opener(),
            CONTROL_PLANE_TIMEOUT,
        )),
        catalog,
        users.accounts(),
    ));
    let skipped = render_skipped(factory.boot_skipped_tables());

    run_with_process_shutdown(
        (factory, reloader),
        authority,
        move |(factory, reloader)| {
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
            // The reload thread holds its own transaction opener; joining it here
            // releases those PD handles before the authority's shutdown drain.
            drop(reloader);
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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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
        /// Snapshots opened, so "one statement, one snapshot" is countable.
        opened: AtomicUsize,
        /// Snapshots still bound. A statement that leaks one leaves this above
        /// zero, which is the lock-left-behind failure in miniature.
        live: AtomicUsize,
        /// Publications that actually carried mutations.
        publications: AtomicUsize,
        fail_commit: AtomicBool,
    }

    impl MockCluster {
        fn rows(&self) -> usize {
            self.committed.lock().expect("committed").len()
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
            Ok(Box::new(MockSnapshot {
                data: self.0.committed.lock().expect("committed").clone(),
                cluster: Arc::clone(&self.0),
            }))
        }

        fn commit(&self, buffer: &MutationBuffer) -> Result<(), String> {
            let staged = buffer.staged();
            if staged.is_empty() {
                return Ok(());
            }
            if self.0.fail_commit.load(Ordering::Acquire) {
                return Err("the mock cluster refused this publication".to_owned());
            }
            let mut committed = self.0.committed.lock().expect("committed");
            for (key, value) in staged {
                match value {
                    Some(value) => committed.insert(key.into_bytes(), value),
                    None => committed.remove(key.as_bytes()),
                };
            }
            drop(committed);
            self.0.publications.fetch_add(1, Ordering::AcqRel);
            buffer.reset();
            Ok(())
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

    /// One authenticated connection over the mock cluster, plus the cluster
    /// handle the test inspects.
    fn open_session() -> (ClusterServerSession, Arc<MockCluster>) {
        let cluster = Arc::new(MockCluster::default());
        let factory = ClusterSessionFactory::new(
            Arc::new(MockTransactions(Arc::clone(&cluster))),
            Arc::new(SharedClusterCatalog::new(loaded_catalog())),
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
        (session, cluster)
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

    /// This node's catalog is a READ of the cluster's schema, so a statement
    /// that would change stored schema or accounts is refused by name rather
    /// than changing this process's copy alone.
    #[test]
    fn stored_schema_and_account_changes_are_refused() {
        let (mut session, _) = open_session();
        for sql in [
            "CREATE TABLE made_up (a BIGINT)",
            "DROP TABLE t",
            "ALTER TABLE t ADD COLUMN w BIGINT",
            "CREATE USER 'bob'@'%'",
            "GRANT SELECT ON app.t TO 'bob'@'%'",
        ] {
            let error = session
                .execute_write(sql)
                .expect_err("a stored-schema change must be refused");
            let message = error.message.clone();
            assert!(
                message.contains("cannot change stored"),
                "unexpected refusal for {sql}: {message}"
            );
        }
        // A GRANT parses as an administrative statement, so it would otherwise
        // take the result-set path; the refusal covers that one too.
        assert!(session
            .execute("GRANT SELECT ON app.t TO 'bob'@'%'")
            .is_err());
        // And the loaded catalog is untouched: `t` still answers.
        assert!(rows(&mut session, "SELECT id FROM t").is_empty());
    }
}
