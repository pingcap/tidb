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

//! Bounded concurrent listener and worker-local query-session ownership.

use std::fmt;
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tidb_exec::pessimistic_lock_error::{commit_outcome_to_sql_error, LockSqlError};
use tidb_exec::real_tikv_analyze::ClusterAnalyzeError;
use tidb_exec::real_tikv_ddl::ClusterDdlError;
use tidb_exec::real_tikv_dml::ConfiguredWriteError;
use tidb_ast::Stmt;
use tidb_planner::prepared_dml::{ConfiguredPreparedWriteTemplate, PreparedBindValue};
use tidb_planner::read_only_scan::ConfiguredPreparedPointReadTemplate;
use tidb_protocol::ColumnInfo;
use tidb_txnkv::transaction::OptimisticCommitOutcome;
use tidb_util::globalconn::{Allocator, GlobalAllocator};
use tidb_util::versioninfo::VersionInfo;

use crate::configured_user_store::{AuthenticatedIdentity, ConfiguredUserStore};
use crate::mysql_connection::{
    serve_mysql_connection_with_tls_and_version_info, MysqlConnectionError, MysqlConnectionRuntime,
};
use crate::mysql_tls::{resolve_server_tls, MysqlServerTls};
use crate::node_config::{NodeConfig, MAX_CONNECTION_WORKERS};
use crate::resultset_source::ResultSetSource;
use crate::wire_status::WireStatus;
use tidb_session::process::ProcessKillTarget;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
const STANDALONE_SERVER_ID: u64 = 1;
// A connection worker runs the planner, whose recursion is guarded by
// `stacker::maybe_grow(red_zone = 2 MB, segment = 16 MB)`. On the default
// 2 MB thread stack the red-zone check fails on EVERY select, so each
// statement mmap'ed and munmap'ed a fresh 16 MB segment -- measured at ~11%
// of the serving thread under sysbench point selects. Go never pays this
// because a goroutine's grown stack PERSISTS for the goroutine's life; a
// large reserved stack is that semantics for a dedicated thread (reserved
// address space, committed only as touched), leaving `maybe_grow` as the
// safety net for genuinely deep plans.
const SQL_WORKER_STACK_BYTES: usize = 32 * 1024 * 1024;

/// Cloneable process shutdown signal for the sole production accept loop.
#[derive(Clone, Debug, Default)]
pub struct ShutdownHandle {
    requested: Arc<AtomicBool>,
}

/// A backend query that can be interrupted during forced connection drain.
pub trait ActiveQueryCancellation: Send + Sync {
    /// Fires the query's canonical cancellation carrier.
    fn cancel(&self);
}

impl ActiveQueryCancellation for tidb_executor::StatementCancellation {
    fn cancel(&self) {
        tidb_executor::StatementCancellation::cancel(self);
    }
}

#[derive(Default)]
struct ConnectionCancellationState {
    requested: bool,
    generation: u64,
    active: Option<Arc<dyn ActiveQueryCancellation>>,
}

/// Cloneable connection-local registry for the currently active query.
#[derive(Clone, Default)]
pub struct ConnectionCancellation {
    state: Arc<Mutex<ConnectionCancellationState>>,
}

impl std::fmt::Debug for ConnectionCancellation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionCancellation")
            .finish_non_exhaustive()
    }
}

impl ConnectionCancellation {
    /// Installs one query cancellation and returns its generation lease.
    pub fn install(
        &self,
        cancellation: Arc<dyn ActiveQueryCancellation>,
    ) -> QueryCancellationLease {
        let (generation, requested) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.generation = state.generation.wrapping_add(1);
            let generation = state.generation;
            let requested = state.requested;
            state.active = Some(Arc::clone(&cancellation));
            (generation, requested)
        };
        if requested {
            cancellation.cancel();
        }
        QueryCancellationLease {
            connection: self.clone(),
            generation,
        }
    }

    fn cancel(&self) {
        let cancellation = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.requested = true;
            state.active.clone()
        };
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
    }

    /// Cancels only the statement running right now, leaving the connection
    /// able to run the next one -- Go's `KILL QUERY`, which differs from
    /// forced drain exactly in that it does not latch the request.
    fn cancel_current_query(&self) {
        let active = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .active
            .clone();
        if let Some(active) = active {
            active.cancel();
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .requested
    }

    fn clear(&self, generation: u64) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.generation == generation {
            state.active = None;
        }
    }
}

/// Cloneable handle that ends one connection, as Go's `KILL` /
/// `KILL CONNECTION` does.
///
/// Two things have to happen for a connection to actually go away: its
/// command loop must stop serving further commands, and a connection blocked
/// reading its NEXT command must wake up. The flag does the first and the
/// socket shutdown the second, which is how Go's `killConn` ends a session it
/// is not currently executing anything for.
#[derive(Clone, Debug, Default)]
pub struct ConnectionClose {
    closed: Arc<AtomicBool>,
    socket: Option<Arc<TcpStream>>,
}

impl ConnectionClose {
    /// Binds the handle to the connection's own socket.
    pub(crate) fn with_socket(socket: TcpStream) -> Self {
        Self {
            closed: Arc::new(AtomicBool::new(false)),
            socket: Some(Arc::new(socket)),
        }
    }

    /// Marks the connection closed and wakes it if it is waiting for its next
    /// command. The current command still finishes; the loop exits after it.
    pub fn request(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(socket) = &self.socket {
            let _ = socket.shutdown(Shutdown::Both);
        }
    }

    /// Whether a `KILL` has ended this connection.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// The [`ProcessKillTarget`] a server connection registers in the process
/// list: `KILL QUERY` cancels the running statement through the same carrier
/// forced drain uses, and `KILL` also ends the connection.
pub struct ConnectionKillTarget {
    cancellation: ConnectionCancellation,
    close: ConnectionClose,
}

impl ConnectionKillTarget {
    /// The kill target of one authenticated connection.
    #[must_use]
    pub const fn new(cancellation: ConnectionCancellation, close: ConnectionClose) -> Self {
        Self {
            cancellation,
            close,
        }
    }
}

impl ProcessKillTarget for ConnectionKillTarget {
    fn cancel_query(&self) {
        self.cancellation.cancel_current_query();
    }

    fn kill_connection(&self) {
        self.cancellation.cancel();
        self.close.request();
    }
}

/// RAII registration for one active query cancellation generation.
pub struct QueryCancellationLease {
    connection: ConnectionCancellation,
    generation: u64,
}

impl Drop for QueryCancellationLease {
    fn drop(&mut self) {
        self.connection.clear(self.generation);
    }
}

impl ShutdownHandle {
    /// Requests shutdown. Repeated requests are idempotent.
    pub fn shutdown(&self) {
        self.requested.store(true, Ordering::Release);
    }

    /// Whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.requested.load(Ordering::Acquire)
    }
}

/// A source-rendered error returned before a query result escapes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqlQueryError {
    /// MySQL error number.
    pub code: u16,
    /// Five-byte SQLSTATE.
    pub state: [u8; 5],
    /// Caller-rendered error text.
    pub message: String,
}

impl SqlQueryError {
    /// Creates one explicit client-visible query failure.
    #[must_use]
    pub fn new(code: u16, state: [u8; 5], message: impl Into<String>) -> Self {
        Self {
            code,
            state,
            message: message.into(),
        }
    }

    /// Creates the fail-closed generic boundary for an injected engine error.
    #[must_use]
    pub fn unknown(message: impl Into<String>) -> Self {
        Self::new(1105, *b"HY000", message)
    }

    /// The one failure whose answer is "nobody knows", not "it failed".
    ///
    /// Go `pkg/parser/terror/terror.go:265-269` defines
    /// `ErrResultUndetermined = ClassGlobal.NewStdErr(CodeResultUndetermined,
    /// mysql.Message("execution result undetermined", nil))`, which carries
    /// the default MySQL code `mysql.ErrUnknown` (1105). It exists because no
    /// SQL error code can express "unknown", and a client that receives an
    /// ordinary error is entitled to retry — which double-applies if the
    /// commit did land. `pkg/server/conn.go:1288-1291` therefore closes the
    /// connection rather than reporting either outcome.
    #[must_use]
    pub fn result_undetermined() -> Self {
        Self::new(1105, *b"HY000", RESULT_UNDETERMINED_MESSAGE)
    }

    /// Whether this failure is the undetermined verdict, so the caller must
    /// close the connection instead of answering.
    ///
    /// Go matches on the `terror` identity (`terror.ErrResultUndetermined
    /// .Equal(err)`); we have no error identities on the wire boundary, so the
    /// exact message is the identity. It is safe as an identity precisely
    /// because the client never sees it: Go closes the connection without
    /// writing an ERR packet at all, and so do we.
    #[must_use]
    pub fn is_result_undetermined(&self) -> bool {
        self.code == 1105 && self.message == RESULT_UNDETERMINED_MESSAGE
    }
}

/// Go `pkg/parser/terror/terror.go:268`: `mysql.Message("execution result
/// undetermined", nil)`.
pub const RESULT_UNDETERMINED_MESSAGE: &str = "execution result undetermined";

/// Preserves a transaction driver's client-visible error triple at the SQL
/// boundary. Domain-specific commit errors delegate here instead of
/// reclassifying an already typed failure from its display text.
pub(crate) fn lock_sql_error(error: &LockSqlError) -> SqlQueryError {
    SqlQueryError::new(error.code, error.state, error.message.clone())
}

/// Preserves the one DDL outcome that is not a failure verdict while mapping
/// every determinate catalog error to its ordinary client diagnostic.
pub(crate) fn cluster_ddl_error(error: ClusterDdlError) -> SqlQueryError {
    use tidb_exec::cluster_ddl::DdlPlanError;
    match error {
        ClusterDdlError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterDdlError::Commit(error) => lock_sql_error(&error),
        ClusterDdlError::Plan(tidb_exec::cluster_ddl::DdlPlanError::InvalidAutoRandom(reason)) => {
            SqlQueryError::new(8216, *b"HY000", format!("Invalid auto random: {reason}"))
        }
        ClusterDdlError::Plan(tidb_exec::cluster_ddl::DdlPlanError::AutoIdReadFailed) => {
            SqlQueryError::new(
                1467,
                *b"HY000",
                "Failed to read auto-increment value from storage engine",
            )
        }
        // Each of these already renders Go's own message; only the CODE was
        // missing, so every one of them reached a client as 1105 instead of
        // the code MySQL clients switch on. `pkg/errno` names them.
        ClusterDdlError::Plan(error @ DdlPlanError::UnknownDatabase(_)) => {
            SqlQueryError::new(1049, *b"42000", error.to_string())
        }
        ClusterDdlError::Plan(error @ DdlPlanError::DatabaseExists(_)) => {
            SqlQueryError::new(1007, *b"HY000", error.to_string())
        }
        // Go `ErrBadTable` (1051): DROP TABLE's own missing-table answer,
        // which Go's TestDropTableWithoutIfExists pins.
        ClusterDdlError::Plan(error @ DdlPlanError::UnknownTable { .. }) => {
            SqlQueryError::new(1051, *b"42S02", error.to_string())
        }
        // Go `infoschema.ErrTableNotExists` (1146): every other statement
        // resolves its table through `getSchemaAndTableByIdent`.
        ClusterDdlError::Plan(error @ DdlPlanError::TableNotExists { .. }) => {
            SqlQueryError::new(1146, *b"42S02", error.to_string())
        }
        ClusterDdlError::Plan(error @ DdlPlanError::TableExists { .. }) => {
            SqlQueryError::new(1050, *b"42S01", error.to_string())
        }
        ClusterDdlError::Plan(error @ DdlPlanError::DuplicateKeyName(_)) => {
            SqlQueryError::new(1061, *b"42000", error.to_string())
        }
        ClusterDdlError::Plan(error @ DdlPlanError::DuplicateColumnName(_)) => {
            SqlQueryError::new(1060, *b"42S21", error.to_string())
        }
        ClusterDdlError::Plan(error @ DdlPlanError::UnknownIndexColumn { .. }) => {
            SqlQueryError::new(1072, *b"42000", error.to_string())
        }
        // Go `ErrKeyNotExists` (1176): the ALTER INDEX visibility path's
        // own code, distinct from DROP INDEX's 1091.
        ClusterDdlError::Plan(error @ DdlPlanError::KeyNotExists { .. }) => {
            SqlQueryError::new(1176, *b"42000", error.to_string())
        }
        // Go `ErrCantDropFieldOrKey` (1091, 42000): DROP INDEX and
        // DROP PRIMARY KEY naming something the table does not have.
        ClusterDdlError::Plan(error @ DdlPlanError::UnknownIndex(_)) => {
            SqlQueryError::new(1091, *b"42000", error.to_string())
        }
        // Go `ErrBadField` (1054, 42S22): the statement named a column the
        // table does not have.
        ClusterDdlError::Plan(error @ DdlPlanError::UnknownColumn { .. }) => {
            SqlQueryError::new(1054, *b"42S22", error.to_string())
        }
        // The shared admission code already knows Go's error number for what
        // it refused; keep it rather than flattening to the generic 1105.
        ClusterDdlError::Plan(DdlPlanError::Admission(error)) => {
            SqlQueryError::new(error.code, error.sql_state(), error.reason)
        }
        other => SqlQueryError::unknown(other.to_string()),
    }
}

pub(crate) fn cluster_analyze_error(error: ClusterAnalyzeError) -> SqlQueryError {
    match error {
        ClusterAnalyzeError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterAnalyzeError::Commit(error) => lock_sql_error(&error),
        ClusterAnalyzeError::Other(detail) => SqlQueryError::unknown(detail),
    }
}

pub(crate) fn configured_write_error(error: &ConfiguredWriteError) -> SqlQueryError {
    match error {
        ConfiguredWriteError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ConfiguredWriteError::Commit(error) => lock_sql_error(error),
        other => SqlQueryError::unknown(other.to_string()),
    }
}

/// Returns the client error for a cluster state change that did not commit,
/// preserving the ambiguous outcome instead of reporting a false failure.
pub(crate) fn cluster_commit_error(
    outcome: &OptimisticCommitOutcome,
    _subject: &str,
) -> Option<SqlQueryError> {
    commit_outcome_to_sql_error(outcome)
        .err()
        .map(|error| lock_sql_error(&error))
}

/// A lazy query result owned by one worker-local session.
pub struct QueryResult<'a> {
    source: BoxedResultSetSource<'a>,
    cursor_materialization: Option<CursorMaterializationAuthority>,
    /// The count the result set's EOF packets carry (Go `writeEOF` reading
    /// `ctx.WarningCount()`).
    ///
    /// Go re-reads the session at each `writeEOF`; here the result holds the
    /// session's mutable borrow for as long as it is being written, so the
    /// session hands the count over with the result. A session that produces
    /// its rows eagerly -- which is every session that has a warning buffer
    /// today -- has already finished warning by then, so the two agree.
    warnings: u16,
    /// The status word the result set's EOF packets carry (Go
    /// `status := cc.ctx.Status()` in `pkg/server/conn.go`, threaded into
    /// `writeResultSet` -> `writeEOF`).
    ///
    /// Go snapshots the status once, right after the statement finished and
    /// before the first byte of the result set goes out; this holds that same
    /// snapshot, taken by the session at the same moment, because the result
    /// holds the session's mutable borrow while it is being written and
    /// nothing can change the transaction state under it in the meantime.
    status: WireStatus,
}

/// Typed statement policy retained only when a prepared cursor materializes
/// this result after execution.
pub(crate) struct CursorMaterializationAuthority {
    pub(crate) field_types: Vec<tidb_datatype::FieldType>,
    pub(crate) init_chunk_size: usize,
    pub(crate) max_chunk_size: usize,
    pub(crate) memory: tidb_executor::StatementMemory,
}

/// One connection-owned, typed prepared point-read definition.
///
/// It contains no storage response, snapshot, or interpolated SQL. The
/// immutable planner template is bound afresh for each execute, while result
/// metadata is derived from that same plan during prepare without opening PD
/// or TiKV.
#[derive(Clone, Debug)]
pub struct PreparedPointRead {
    template: ConfiguredPreparedPointReadTemplate,
    result_columns: Vec<ColumnInfo>,
    result_field_types: Vec<tidb_datatype::FieldType>,
}

impl PreparedPointRead {
    /// Creates a concrete prepared definition after parser/catalog admission.
    pub fn new(
        template: ConfiguredPreparedPointReadTemplate,
        result_columns: Vec<ColumnInfo>,
        result_field_types: Vec<tidb_datatype::FieldType>,
    ) -> Result<Self, SqlQueryError> {
        if result_columns.len() != result_field_types.len() {
            return Err(SqlQueryError::unknown(format!(
                "prepared point-read schema has {} wire columns but {} chunk field types",
                result_columns.len(),
                result_field_types.len()
            )));
        }
        Ok(Self {
            template,
            result_columns,
            result_field_types,
        })
    }

    /// Returns the immutable typed template retained by this connection.
    #[must_use]
    pub const fn template(&self) -> &ConfiguredPreparedPointReadTemplate {
        &self.template
    }

    /// Returns the exact signed-BIGINT result metadata sent at prepare time.
    #[must_use]
    pub fn result_columns(&self) -> &[ColumnInfo] {
        &self.result_columns
    }

    /// Returns the exact storage/result types retained with the prepared
    /// schema. Cursor materialization must not reconstruct these from MySQL
    /// column packets, which omit collation, signedness, and temporal FSP
    /// details needed by a [`tidb_chunk::chunk::Chunk`].
    #[must_use]
    pub fn result_field_types(&self) -> &[tidb_datatype::FieldType] {
        &self.result_field_types
    }

    /// Positional markers the typed template binds at execute time.
    #[must_use]
    pub const fn parameter_count(&self) -> usize {
        self.template.parameter_count()
    }
}

/// One connection-owned, typed prepared write definition.
///
/// Like a prepared read it holds no storage state and no interpolated SQL: the
/// immutable planner template is bound afresh for each execute.
#[derive(Clone, Debug)]
pub struct PreparedWrite {
    template: ConfiguredPreparedWriteTemplate,
}

impl PreparedWrite {
    /// Creates a concrete prepared write after parser/catalog admission.
    #[must_use]
    pub const fn new(template: ConfiguredPreparedWriteTemplate) -> Self {
        Self { template }
    }

    /// Returns the immutable typed template retained by this connection.
    #[must_use]
    pub const fn template(&self) -> &ConfiguredPreparedWriteTemplate {
        &self.template
    }

    /// Positional markers this statement's execute packet must supply.
    #[must_use]
    pub fn parameter_count(&self) -> usize {
        self.template.parameter_count()
    }
}

/// One connection-owned prepared statement of either admitted kind.
///
/// A prepared statement of any shape, held as the SQL its markers will be
/// bound into.
///
/// Go keeps the parsed statement and installs execute-time values on its
/// markers; this tier binds them into the text before running, which is what
/// `tidb_session::Session::run_with_params` does.
#[derive(Clone, Debug)]
pub struct PreparedGeneral {
    sql: String,
    parameter_count: usize,
    result_columns: Vec<ColumnInfo>,
    /// The parser-owned statement retained at COM_STMT_PREPARE.  General
    /// executes clone and bind this tree instead of reparsing `sql`.
    template: Option<Stmt>,
    /// A schema-versioned point-get plan compiled at PREPARE time.
    point_get_plan: Option<std::sync::Arc<tidb_executor::PreparedPointGetPlan>>,
}

impl PreparedGeneral {
    /// Creates one from its statement text and the metadata a PREPARE reports.
    #[must_use]
    pub fn new(sql: String, parameter_count: usize, result_columns: Vec<ColumnInfo>) -> Self {
        Self {
            sql,
            parameter_count,
            result_columns,
            template: None,
            point_get_plan: None,
        }
    }

    /// Creates a general statement with the parsed tree retained for binary
    /// prepared executes.
    #[must_use]
    pub fn with_template(
        sql: String,
        parameter_count: usize,
        result_columns: Vec<ColumnInfo>,
        template: Stmt,
    ) -> Self {
        Self {
            sql,
            parameter_count,
            result_columns,
            template: Some(template),
            point_get_plan: None,
        }
    }

    /// Creates a retained template together with its immutable point-get
    /// cache candidate.
    #[must_use]
    pub fn with_template_and_point_get_plan(
        sql: String,
        parameter_count: usize,
        result_columns: Vec<ColumnInfo>,
        template: Stmt,
        point_get_plan: Option<std::sync::Arc<tidb_executor::PreparedPointGetPlan>>,
    ) -> Self {
        Self {
            sql,
            parameter_count,
            result_columns,
            template: Some(template),
            point_get_plan,
        }
    }

    /// The statement text, whose markers an execute binds.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Positional markers the execute packet must supply.
    #[must_use]
    pub fn parameter_count(&self) -> usize {
        self.parameter_count
    }

    /// Result metadata sent at prepare time; empty for a statement that
    /// answers with an OK packet.
    #[must_use]
    pub fn result_columns(&self) -> &[ColumnInfo] {
        &self.result_columns
    }

    /// The parse retained at prepare time, when this statement came through
    /// the binary prepared protocol.
    #[must_use]
    pub fn template(&self) -> Option<&Stmt> {
        self.template.as_ref()
    }

    /// The immutable point-get cache candidate compiled at PREPARE time.
    #[must_use]
    pub fn point_get_plan(
        &self,
    ) -> Option<&std::sync::Arc<tidb_executor::PreparedPointGetPlan>> {
        self.point_get_plan.as_ref()
    }
}

/// What executing a general prepared statement produced.
pub enum GeneralExecuteOutcome<'a> {
    /// A result set, streamed the same way a COM_QUERY result is.
    Rows(QueryResult<'a>),
    /// An OK packet's affected-row count and last insert id.
    Write(WriteOutcome),
}

/// A write returns an OK packet and never a result set, so the two kinds carry
/// different response shapes and must stay distinguishable at execute time.
#[derive(Clone, Debug)]
pub enum PreparedStatement {
    /// A bounded configured point read returning binary rows.
    PointRead(PreparedPointRead),
    /// A bounded configured INSERT or point UPDATE returning affected rows.
    Write(PreparedWrite),
    /// Any other statement, bound and run through the session.
    General(PreparedGeneral),
    /// `BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT` and friends, carried as written.
    ///
    /// Transaction control is recognized at PREPARE and applied through
    /// [`QuerySession::control_transaction`] at EXECUTE, which is the same
    /// route the text protocol takes. Running it as an ordinary statement
    /// instead would flip only the driver session's own flag and leave the
    /// connection's transaction unopened, so every following statement would
    /// read at a fresh timestamp -- the wrong snapshot, not merely a slow one.
    TransactionControl(String),
}

impl PreparedStatement {
    /// Positional markers the execute packet must supply.
    #[must_use]
    pub fn parameter_count(&self) -> usize {
        match self {
            Self::PointRead(read) => read.parameter_count(),
            Self::Write(write) => write.parameter_count(),
            Self::General(general) => general.parameter_count(),
            // Transaction control has no markers to bind.
            Self::TransactionControl(_) => 0,
        }
    }

    /// Result metadata sent at prepare time; a write has no result columns.
    #[must_use]
    pub fn result_columns(&self) -> &[ColumnInfo] {
        match self {
            Self::PointRead(read) => read.result_columns(),
            Self::Write(_) => &[],
            Self::General(general) => general.result_columns(),
            Self::TransactionControl(_) => &[],
        }
    }
}

/// What one prepared write reported after a determinate commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteOutcome {
    /// Rows to report in the MySQL OK packet.
    pub affected_rows: u64,
    /// The auto-increment id this statement generated, which the OK packet
    /// carries as `last_insert_id`. Zero when the statement generated none.
    pub last_insert_id: u64,
}

impl<'a> QueryResult<'a> {
    /// Transfers one session-owned result source to the connection writer.
    #[must_use]
    pub fn new(source: Box<dyn ResultSetSource + 'a>) -> Self {
        Self {
            source: BoxedResultSetSource { inner: source },
            cursor_materialization: None,
            warnings: 0,
            status: WireStatus::AUTOCOMMIT,
        }
    }

    /// Attaches the exact typed policy captured inside the producing
    /// statement, before any `SET_VAR` overlay was restored.
    #[must_use]
    pub fn with_cursor_materialization(
        mut self,
        field_types: Vec<tidb_datatype::FieldType>,
        authority: tidb_session::ResultMaterializationAuthority,
    ) -> Self {
        let (memory, init_chunk_size, max_chunk_size) = authority.into_parts();
        self.cursor_materialization = Some(CursorMaterializationAuthority {
            field_types,
            init_chunk_size,
            max_chunk_size,
            memory,
        });
        self
    }

    /// Takes the cursor authority exactly once. Ordinary result-set writers
    /// never call this and retain their existing source-only path.
    pub(crate) fn take_cursor_materialization(&mut self) -> Option<CursorMaterializationAuthority> {
        self.cursor_materialization.take()
    }

    /// Attaches the warning count AND the status word this statement's EOF
    /// packets carry.
    ///
    /// The two travel together deliberately: Go reads both off the session in
    /// the same breath (`ctx.WarningCount()` and `ctx.Status()`), and a session
    /// that remembers one but forgets the other is exactly how a stale
    /// `SERVER_STATUS_AUTOCOMMIT` reached the wire while a transaction was
    /// open. A session that never calls this has no transaction concept at all,
    /// which is what [`WireStatus::AUTOCOMMIT`] states.
    #[must_use]
    pub fn with_statement_status(mut self, warnings: u16, status: WireStatus) -> Self {
        self.warnings = warnings;
        self.status = status;
        self
    }

    /// The status word for the EOF packets that frame this result set.
    #[must_use]
    pub fn wire_status(&self) -> WireStatus {
        self.status
    }

    /// The warning count for the EOF packets that frame this result set.
    #[must_use]
    pub fn warning_count(&self) -> u16 {
        self.warnings
    }

    /// Returns the sole mutable result-set owner.
    pub fn source(&mut self) -> &mut BoxedResultSetSource<'a> {
        &mut self.source
    }

    /// Consumes this result, returning its boxed source for re-wrapping.
    ///
    /// A prepared read with an `ORDER BY` buffers and sorts the observed scan
    /// stream by wrapping this source in a `SortingResultSetSource`; taking the
    /// box back out keeps that transform outside the storage-facing observer.
    #[must_use]
    pub fn into_source(self) -> Box<dyn ResultSetSource + 'a> {
        self.source.inner
    }
}

/// Sized adapter for a boxed worker-local result source.
pub struct BoxedResultSetSource<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
}

impl ResultSetSource for BoxedResultSetSource<'_> {
    fn next_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.inner.next_batch(max_rows)
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, String> {
        self.inner.columns()
    }

    fn finish(&mut self) -> Result<(), String> {
        self.inner.finish()
    }

    fn close(&mut self) -> Result<(), String> {
        self.inner.close()
    }
}

/// One authenticated connection's immutable session context.
#[derive(Clone, Debug)]
pub struct SessionContext {
    /// Server connection identity.
    pub connection_id: u64,
    /// Accepted peer address.
    pub peer_addr: SocketAddr,
    /// Identity established by ordinary password verification or validated
    /// process-wide skip-grant admission.
    pub identity: AuthenticatedIdentity,
    /// Whether the MySQL front end completed a TLS handshake for this
    /// connection.
    pub secure_transport: bool,
    /// The negotiated TLS `(cipher, version)` in their MySQL/OpenSSL
    /// spellings (`tidb_util::tlsutil`), `None` on a plaintext connection.
    /// Go reports the same pair through `Ssl_cipher`/`Ssl_version`
    /// (`server.go:1329`).
    pub tls_status: Option<(String, String)>,
    /// Forced-drain carrier on which the session registers each active query.
    pub cancellation: ConnectionCancellation,
    /// Handle a `KILL` uses to end this connection.
    pub close: ConnectionClose,
    /// Coherent build identity captured when the SQL listener started.
    pub version_info: VersionInfo,
}

/// Query capability retained entirely inside one fixed worker thread.
pub trait QuerySession {
    /// Starts one sequential query and returns its lazy result owner.
    fn execute<'a>(&'a mut self, sql: &str) -> Result<QueryResult<'a>, SqlQueryError>;

    /// Splits one COM_QUERY text into the statements the connection runs in
    /// order — Go `handleQuery` parses the whole text and loops the result
    /// (`conn.go:1861`). More than one statement is admitted by the client's
    /// `CLIENT_MULTI_STATEMENTS` bit, else by the session's
    /// `@@tidb_multi_statement_mode`. Sessions without a parser keep the
    /// default: the text is one statement, exactly as before.
    fn split_statements(
        &mut self,
        sql: &str,
        _client_multi_statements: bool,
    ) -> Result<Vec<String>, SqlQueryError> {
        Ok(vec![sql.to_owned()])
    }

    /// Called once after a multi-statement chain's LAST statement completes;
    /// Go appends the admission's parser warnings there (`conn.go:2262`).
    /// An aborted chain is never flushed, as Go's error return drops them.
    fn flush_multi_statement_warning(&mut self) {}

    /// The command-scoped cancellation authority installed before parsing.
    fn query_cancellation(&self) -> Option<Arc<dyn ActiveQueryCancellation>> {
        None
    }

    /// Maximum idle time while waiting for the next command packet.
    ///
    /// Go refreshes `PacketIO` from this session's `@@wait_timeout` before
    /// every command. Sessions without a variable store retain TiDB's 28,800
    /// second default rather than inheriting a listener-wide timeout.
    fn wait_timeout(&self) -> Duration {
        Duration::from_secs(28_800)
    }

    /// This session's `@@max_allowed_packet`, or `None` for a session that
    /// keeps no variable store.
    ///
    /// Go rebinds the reader from the SESSION VARIABLE on every packet:
    ///
    /// ```text
    /// func (cc *clientConn) readPacket() ([]byte, error) {
    ///     if cc.getCtx() != nil {
    ///         cc.pkt.SetMaxAllowedPacket(
    ///             cc.ctx.GetSessionVars().MaxAllowedPacket)
    ///     }
    /// ```
    ///
    /// so the limit a client READS and the limit the server ENFORCES are one
    /// value, and `SET max_allowed_packet` takes effect on the next packet.
    /// `None` is Go's `cc.getCtx() == nil`: the config seed
    /// (`PacketIO.SetMaxAllowedPacket(config.GetMaxAllowedPacket())`) stands
    /// until a session exists to ask.
    fn max_allowed_packet(&self) -> Option<usize> {
        None
    }

    /// Prepares a statement of any shape, reporting the marker count and the
    /// result columns a PREPARE sends.
    ///
    /// Sessions with their own specialized prepared paths keep them; this is
    /// the general one every other statement takes.
    fn prepare_general(&mut self, _sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "this session does not support general prepared statements",
        ))
    }

    /// Binds and runs one general prepared statement.
    fn execute_general<'a>(
        &'a mut self,
        _statement: &PreparedGeneral,
        _values: &[tidb_protocol::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "this session does not support general prepared statements",
        ))
    }

    /// Parses and types one bounded prepared point read without storage I/O.
    /// Sessions that have no real configured catalog fail closed.
    fn prepare_point_read(&mut self, _sql: &str) -> Result<PreparedPointRead, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "prepared point reads require a configured real-TiKV session",
        ))
    }

    /// Binds and starts one prepared point read through this session's
    /// ordinary concrete executor. Sessions without that authority fail
    /// closed and never synthesize rows.
    fn execute_prepared_point_read<'a>(
        &'a mut self,
        _statement: &PreparedPointRead,
        _parameters: &[i64],
    ) -> Result<QueryResult<'a>, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "prepared point reads require a configured real-TiKV session",
        ))
    }

    /// Parses and types one bounded prepared write without storage I/O.
    /// Sessions that have no real configured catalog fail closed.
    fn prepare_write(&mut self, _sql: &str) -> Result<PreparedWrite, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "prepared writes require a configured real-TiKV session",
        ))
    }

    /// Binds and commits one prepared write through this session's shared
    /// real transaction authority. Sessions without that authority fail closed
    /// and never report affected rows they did not persist.
    fn execute_prepared_write(
        &mut self,
        _statement: &PreparedWrite,
        _parameters: &[PreparedBindValue],
    ) -> Result<WriteOutcome, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "prepared writes require a configured real-TiKV session",
        ))
    }

    /// Applies a `BEGIN`/`START TRANSACTION`, `COMMIT`, or `ROLLBACK` statement.
    ///
    /// Returns `Some(in_transaction)` when the SQL is one of those
    /// transaction-control statements — the session updates its own transaction
    /// state and the caller answers with an OK packet whose status advertises
    /// `SERVER_STATUS_IN_TRANS` per the returned flag. Returns `None` when the
    /// SQL is not transaction control, so the caller runs it as an ordinary
    /// query. Sessions without transaction support keep the default and treat
    /// every statement as an ordinary query.
    fn control_transaction(&mut self, _sql: &str) -> Result<Option<bool>, SqlQueryError> {
        Ok(None)
    }

    /// Runs one statement that answers with an OK packet rather than a result
    /// set — a DML write or DDL, as MySQL answers them on the text protocol.
    ///
    /// Returns `Some(outcome)` when the SQL is such a statement (the caller
    /// writes an OK packet carrying `affected_rows`), and `None` when it is an
    /// ordinary query the caller should run through [`QuerySession::execute`].
    /// This mirrors [`QuerySession::control_transaction`]'s shape: sessions
    /// that serve only queries keep the default and answer everything with a
    /// result set.
    fn execute_write(&mut self, _sql: &str) -> Result<Option<WriteOutcome>, SqlQueryError> {
        Ok(None)
    }

    /// The warning count the OK/EOF packet carries for the statement that just
    /// finished (Go `TiDBContext.WarningCount`, read by `writeOkWith` and
    /// `writeEOF` in `pkg/server/conn.go`).
    ///
    /// This is a second, independent channel from `SHOW WARNINGS`: a driver
    /// learns that a statement warned only from this field. It is the same
    /// buffer `SHOW WARNINGS` reports, so it follows the same per-statement
    /// lifetime -- fresh per statement, inherited only by the statements that
    /// report the buffer. Sessions with no warning buffer report none.
    fn warning_count(&self) -> u16 {
        0
    }

    /// The live session status word every OK packet this session's statements
    /// produce must carry (Go `status := cc.ctx.Status()`, `pkg/server/conn.go`,
    /// read afresh per statement and passed to every `writeOkWith`/`writeEOF`).
    ///
    /// Result sets carry their own snapshot on [`QueryResult::wire_status`],
    /// because the result holds this session's mutable borrow while it is
    /// written. Everything answered with a bare OK packet -- a write's affected
    /// rows, transaction control, `COM_PING`, `COM_INIT_DB` -- reads this here,
    /// after the statement has already updated the session.
    ///
    /// A session with no transaction concept is permanently in autocommit and
    /// never in a transaction, which is what the default states.
    fn wire_status(&self) -> WireStatus {
        WireStatus::AUTOCOMMIT
    }

    /// Go `clientConn.initResultEncoder`: this session's
    /// `@@character_set_results`, which decides the charset every column
    /// definition's identifiers and every string cell of a result set go out
    /// in.
    ///
    /// Go reads it once per COMMAND, not once per connection, because the
    /// variable can be `SET` between two statements. The empty string is
    /// Go's `NewResultEncoder("")` state -- the variable unset -- which
    /// leaves metadata and data in their column charset; a session with no
    /// variables reports it and is unchanged.
    fn result_charset(&self) -> String {
        String::new()
    }

    /// Go `clientConn.initInputEncoder`: this session's
    /// `@@character_set_client`, applied to string-family binary parameters
    /// before they reach expression or storage semantics.
    fn input_charset(&self) -> String {
        "utf8mb4".to_owned()
    }

    /// Selects this session's current schema (Go `clientConn.useDB`).
    ///
    /// The handshake's initial database and `COM_INIT_DB` are the same
    /// operation in Go, and they are the same operation here. Sessions that
    /// have no schema concept fail closed rather than accept a name they will
    /// then ignore: a connection that reports success and silently resolves
    /// nothing is worse than one that refuses.
    fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        Err(SqlQueryError::new(
            ER_BAD_DB_ERROR,
            *b"42000",
            format!("Unknown database '{name}'"),
        ))
    }

    /// Selects NO schema, the state Go leaves a connection in whose handshake
    /// carried no initial database: `SessionVars.CurrentDB` stays empty, so
    /// `DATABASE()` is NULL (`builtinDatabaseSig` returns
    /// `currentDB, currentDB == "", nil`) and every unqualified name is
    /// `ErrNoDB` (1046) until a `USE` runs.
    ///
    /// Sessions that keep no schema of their own do nothing, which leaves
    /// them exactly as they were.
    fn deselect_database(&mut self) {}
}

/// Go `mysql.ErrBadDB` (1049): the errno a schema that does not exist gets.
/// A missing schema is emphatically not an access-denied failure.
pub const ER_BAD_DB_ERROR: u16 = 1049;

/// Process-owned factory invoked only after authentication, inside a worker.
pub trait QuerySessionFactory: Send + Sync + 'static {
    /// Worker-local session type. It deliberately has no `Send` bound.
    type Session: QuerySession;

    /// Opens a session from already-running process authorities.
    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError>;
}

/// Process-wide connection accounting with exactly-once owned-lease cleanup.
pub struct ConnectionTracker {
    connection_ids: GlobalAllocator,
    active: AtomicUsize,
    max_active: AtomicUsize,
    accepted: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
}

impl Default for ConnectionTracker {
    fn default() -> Self {
        Self {
            connection_ids: GlobalAllocator::new(|| STANDALONE_SERVER_ID, true),
            active: AtomicUsize::default(),
            max_active: AtomicUsize::default(),
            accepted: AtomicU64::default(),
            completed: AtomicU64::default(),
            failed: AtomicU64::default(),
        }
    }
}

impl fmt::Debug for ConnectionTracker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionTracker")
            .field("active", &self.active())
            .field("max_active", &self.max_active())
            .field("accepted", &self.accepted())
            .field("completed", &self.completed())
            .field("failed", &self.failed())
            .finish()
    }
}

impl ConnectionTracker {
    pub(crate) fn begin(self: &Arc<Self>) -> ConnectionLease {
        let id = self.connection_ids.next_id();
        self.accepted.fetch_add(1, Ordering::AcqRel);
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::AcqRel);
        ConnectionLease {
            tracker: Arc::clone(self),
            id,
            failed: false,
        }
    }

    /// Number of accepted connections currently inside their lifecycle.
    #[must_use]
    pub fn active(&self) -> usize {
        self.active.load(Ordering::Acquire)
    }

    /// Maximum simultaneously active connection count observed.
    #[must_use]
    pub fn max_active(&self) -> usize {
        self.max_active.load(Ordering::Acquire)
    }

    /// Total accepted connections.
    #[must_use]
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::Acquire)
    }

    /// Total lifecycles released exactly once.
    #[must_use]
    pub fn completed(&self) -> u64 {
        self.completed.load(Ordering::Acquire)
    }

    /// Total lifecycles that ended on a connection error.
    #[must_use]
    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Acquire)
    }
}

pub(crate) struct ConnectionLease {
    tracker: Arc<ConnectionTracker>,
    id: u64,
    failed: bool,
}

impl ConnectionLease {
    pub(crate) const fn id(&self) -> u64 {
        self.id
    }

    pub(crate) const fn mark_failed(&mut self) {
        self.failed = true;
    }
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        let previous = self.tracker.active.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0, "connection count underflow");
        self.tracker.completed.fetch_add(1, Ordering::AcqRel);
        if self.failed {
            self.tracker.failed.fetch_add(1, Ordering::AcqRel);
        }
        self.tracker.connection_ids.release(self.id);
    }
}

struct ConnectionWork {
    stream: TcpStream,
    peer_addr: SocketAddr,
    cancellation: ConnectionCancellation,
    registration: ActiveSocketRegistration,
}

struct WorkerPool {
    workers: Vec<WorkerHandle>,
    work_senders: Vec<mpsc::Sender<ConnectionWork>>,
    available_workers: mpsc::Receiver<usize>,
    available_sender: mpsc::SyncSender<usize>,
    /// A live clone so the accept loop can hand dedicated (per-connection)
    /// threads the same terminal guard the warm pool uses to report panics.
    terminal_sender: mpsc::Sender<WorkerTerminal>,
    terminal_workers: mpsc::Receiver<WorkerTerminal>,
}

#[derive(Clone)]
struct WorkerConnectionConfig {
    max_allowed_packet: usize,
    version_info: VersionInfo,
    tls: Option<MysqlServerTls>,
}

struct WorkerHandle {
    index: usize,
    join: JoinHandle<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerTerminalKind {
    Returned,
    Panicked,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WorkerTerminal {
    index: usize,
    kind: WorkerTerminalKind,
}

struct WorkerTerminalGuard {
    index: usize,
    terminal: mpsc::Sender<WorkerTerminal>,
}

impl Drop for WorkerTerminalGuard {
    fn drop(&mut self) {
        let kind = if std::thread::panicking() {
            WorkerTerminalKind::Panicked
        } else {
            WorkerTerminalKind::Returned
        };
        let _ = self.terminal.send(WorkerTerminal {
            index: self.index,
            kind,
        });
    }
}

/// Surfaces a PANICKED serving thread to the accept loop so admission stops,
/// draining everything else. A dedicated thread reporting `Returned` is just
/// its one client disconnecting -- exactly how Go's per-connection goroutines
/// end, thousands of times over a server's life -- so only an actual panic
/// stops admission here.
fn poll_terminal(terminal: &mpsc::Receiver<WorkerTerminal>) -> Result<(), SqlNodeError> {
    while let Ok(worker) = terminal.try_recv() {
        if worker.kind == WorkerTerminalKind::Panicked {
            return Err(SqlNodeError::WorkerTerminated {
                index: worker.index,
                panicked: true,
            });
        }
    }
    Ok(())
}

/// Go's `ErrConCount` reply: a client arriving when every worker is busy is
/// told `1040 Too many connections` and closed, rather than left waiting in
/// the listen backlog.
///
/// `server.go` returns `servererr.ErrConCount` from its capacity check and
/// `onConn` answers it with `conn.writeError(ctx, err)`, so the packet goes
/// out before any handshake -- which is why the sequence number is 0 and the
/// packet carries NO SQLSTATE marker. Go writes it through the same
/// `writeError`, whose 4.1 form is conditioned on
/// `cc.capability&mysql.ClientProtocol41`, and at this point the client has
/// sent no capability flags at all, so that bit is clear. Sending the marker
/// anyway is not cosmetic: the client parses the packet in the old form and
/// the marker lands INSIDE the message, which reads
/// `ERROR 1040 (HY000): #08004Too many connections`.
///
/// Failures are ignored on purpose: the connection is being refused, and a
/// peer that has already gone away is the ordinary case.
fn refuse_over_capacity(stream: &TcpStream) {
    let Ok(socket) = stream.try_clone() else {
        return;
    };
    let Ok(mut output) =
        tidb_protocol::PacketIoWriter::new(socket, tidb_protocol::CompressionAlgorithm::None)
    else {
        return;
    };
    let _ = crate::connection_writers::write_error(
        &mut output,
        0,
        tidb_error::mysql::errcode::ErrConCount,
        *b"08004",
        "Too many connections",
        false,
    );
    let _ = stream.shutdown(Shutdown::Both);
}

#[derive(Default)]
struct ActiveSockets {
    streams: Mutex<Vec<ActiveSocket>>,
    poisoned: AtomicBool,
}

struct ActiveSocket {
    key: u64,
    stream: TcpStream,
    cancellation: ConnectionCancellation,
}

struct ActiveSocketRegistration {
    key: u64,
    sockets: Arc<ActiveSockets>,
}

impl ActiveSocketRegistration {
    fn new(key: u64, sockets: Arc<ActiveSockets>) -> Self {
        Self { key, sockets }
    }
}

impl Drop for ActiveSocketRegistration {
    fn drop(&mut self) {
        self.sockets.remove(self.key);
    }
}

impl ActiveSockets {
    fn lock_streams(&self) -> (std::sync::MutexGuard<'_, Vec<ActiveSocket>>, bool) {
        match self.streams.lock() {
            Ok(streams) => (streams, false),
            Err(poisoned) => {
                self.poisoned.store(true, Ordering::Release);
                self.streams.clear_poison();
                (poisoned.into_inner(), true)
            }
        }
    }

    fn take_poisoned(&self, observed: bool) -> bool {
        let recorded = self.poisoned.swap(false, Ordering::AcqRel);
        observed || recorded
    }

    fn register(
        &self,
        key: u64,
        stream: TcpStream,
        cancellation: ConnectionCancellation,
    ) -> Result<(), SqlNodeError> {
        self.streams
            .lock()
            .map_err(|_| SqlNodeError::WorkerStatePoisoned)?
            .push(ActiveSocket {
                key,
                stream,
                cancellation,
            });
        Ok(())
    }

    fn remove(&self, key: u64) {
        let (mut streams, _) = self.lock_streams();
        streams.retain(|socket| socket.key != key);
    }

    fn len(&self) -> Result<usize, SqlNodeError> {
        let (streams, observed) = self.lock_streams();
        let len = streams.len();
        drop(streams);
        if self.take_poisoned(observed) {
            Err(SqlNodeError::WorkerStatePoisoned)
        } else {
            Ok(len)
        }
    }

    fn shutdown_all(&self) -> (usize, Option<SqlNodeError>) {
        let (streams, observed) = self.lock_streams();
        let forced = streams.len();
        for socket in streams.iter() {
            socket.cancellation.cancel();
            let _ = socket.stream.shutdown(Shutdown::Both);
        }
        drop(streams);
        let error = if self.take_poisoned(observed) {
            Some(SqlNodeError::WorkerStatePoisoned)
        } else {
            None
        };
        (forced, error)
    }

    fn cancel_queries(&self) -> Result<(), SqlNodeError> {
        let (streams, observed) = self.lock_streams();
        for socket in streams.iter() {
            socket.cancellation.cancel();
        }
        drop(streams);
        if self.take_poisoned(observed) {
            Err(SqlNodeError::WorkerStatePoisoned)
        } else {
            Ok(())
        }
    }
}

/// A loopback SQL node serving accepted connections on a warm worker pool
/// that grows with one dedicated thread per connection past its size --
/// Go's `go s.onConn(clientConn)` per accept -- under a bounded
/// accepted-socket queue.
pub struct ConcurrentSqlNode<F: QuerySessionFactory> {
    listener: TcpListener,
    factory: Arc<F>,
    users: Arc<ConfiguredUserStore>,
    tracker: Arc<ConnectionTracker>,
    max_allowed_packet: usize,
    version_info: VersionInfo,
    /// Server TLS material, or `None` for a plaintext-only MySQL port. This is
    /// the only thing that lets a connection advertise `CLIENT_SSL`.
    tls: Option<MysqlServerTls>,
    /// Go's `Instance.MaxConnections`: the simultaneous-connection limit,
    /// where zero means unlimited (`server.go`'s `checkConnectionCount`).
    /// It also bounds the warm pool; demand past it spawns one dedicated
    /// thread per connection.
    worker_count: usize,
    shutdown: ShutdownHandle,
    shutdown_grace: Duration,
    connection_timeout: Duration,
}

impl<F: QuerySessionFactory> ConcurrentSqlNode<F> {
    /// Binds the configured loopback endpoint and retains process authorities.
    pub fn bind(
        config: &NodeConfig,
        factory: Arc<F>,
        users: Arc<ConfiguredUserStore>,
    ) -> Result<Self, SqlNodeError> {
        let tls = resolve_server_tls(
            config.ssl_cert.as_deref(),
            config.ssl_key.as_deref(),
            config.auto_tls,
        )
        .map_err(|error| SqlNodeError::Tls(error.to_string()))?;
        let listener = TcpListener::bind((config.host, config.port)).map_err(SqlNodeError::Bind)?;
        listener
            .set_nonblocking(true)
            .map_err(SqlNodeError::Listener)?;
        eprintln!(
            "{{\"event\":\"mysql_tls\",\"enabled\":{},\"origin\":{:?}}}",
            tls.is_some(),
            tls.as_ref().map_or("none", MysqlServerTls::origin)
        );
        Ok(Self {
            listener,
            factory,
            users,
            tracker: Arc::new(ConnectionTracker::default()),
            max_allowed_packet: config.max_allowed_packet,
            version_info: config.version_info.clone(),
            tls,
            worker_count: config.max_connections,
            shutdown: ShutdownHandle::default(),
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            connection_timeout: config.connection_timeout,
        })
    }

    /// Returns the operating-system-selected listener address.
    pub fn local_addr(&self) -> Result<SocketAddr, SqlNodeError> {
        self.listener.local_addr().map_err(SqlNodeError::Listener)
    }

    /// Returns shared exact connection lifecycle counters.
    #[must_use]
    pub fn tracker(&self) -> Arc<ConnectionTracker> {
        Arc::clone(&self.tracker)
    }

    /// Returns the signal that stops acceptance and starts worker drain.
    #[must_use]
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        self.shutdown.clone()
    }

    /// Returns the configured bound on graceful connection drain.
    #[must_use]
    pub fn shutdown_grace_ms(&self) -> u128 {
        self.shutdown_grace.as_millis()
    }

    /// Overrides the graceful drain interval for deterministic lifecycle tests.
    #[must_use]
    pub fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Runs the production accept loop indefinitely.
    pub fn run(self) -> Result<(), SqlNodeError> {
        self.run_accept_loop(None)
    }

    /// Accepts exactly `connections` sockets, then drains all fixed workers.
    ///
    /// This bounded entry point exists for lifecycle and concurrency proofs;
    /// production uses [`Self::run`].
    pub fn serve_connections(self, connections: usize) -> Result<(), SqlNodeError> {
        self.run_accept_loop(Some(connections))
    }

    fn run_accept_loop(self, limit: Option<usize>) -> Result<(), SqlNodeError> {
        self.run_accept_loop_with(limit, |stream, timeout| {
            stream
                .set_nonblocking(false)
                .map_err(SqlNodeError::Listener)?;
            stream
                .set_read_timeout(Some(timeout))
                .map_err(SqlNodeError::Listener)?;
            stream
                .set_write_timeout(Some(timeout))
                .map_err(SqlNodeError::Listener)
        })
    }

    fn run_accept_loop_with<P>(
        self,
        limit: Option<usize>,
        mut prepare_stream: P,
    ) -> Result<(), SqlNodeError>
    where
        P: FnMut(&TcpStream, Duration) -> Result<(), SqlNodeError>,
    {
        let active_sockets = Arc::new(ActiveSockets::default());
        // Go pre-spawns nothing: `go s.onConn(clientConn)` runs one goroutine
        // per accepted connection. The warm pool keeps a bounded set of
        // threads for the common small fan-out (and for the deterministic
        // lifecycle proofs); demand past it is served by spawning exactly
        // what Go would -- a thread per connection.
        let warm_workers = self.worker_count.min(MAX_CONNECTION_WORKERS);
        let WorkerPool {
            mut workers,
            work_senders: worker_senders,
            available_workers,
            available_sender,
            terminal_sender,
            terminal_workers,
        } = spawn_workers(
            warm_workers,
            &self.factory,
            &self.users,
            &self.tracker,
            WorkerConnectionConfig {
                max_allowed_packet: self.max_allowed_packet,
                version_info: self.version_info.clone(),
                tls: self.tls.clone(),
            },
        )?;

        let mut accepted = 0_usize;
        let mut dedicated_workers: Vec<WorkerHandle> = Vec::new();
        let mut next_worker_index = warm_workers;
        let connection_config = WorkerConnectionConfig {
            max_allowed_packet: self.max_allowed_packet,
            version_info: self.version_info.clone(),
            tls: self.tls.clone(),
        };
        let accept_result = (|| loop {
            if limit == Some(accepted) || self.shutdown.is_shutdown_requested() {
                break Ok(());
            }
            // A panicked or returned worker must stop admission promptly, so
            // the terminal channel is polled on every iteration -- not only
            // while the loop idles inside accept.
            poll_terminal(&terminal_workers)?;
            // Reap dedicated threads whose client has gone: a joinable
            // thread's stack stays MAPPED until `join`, so deferring every
            // join to the drain would hold one dead stack per finished
            // connection -- tens of thousands of VMAs under connection
            // churn, exactly what Go never retains when a goroutine ends.
            dedicated_workers = dedicated_workers
                .drain(..)
                .filter_map(|mut handle| {
                    if handle.join.is_finished() {
                        // A panic already surfaced through the terminal guard,
                        // so the join outcome itself needs no handling: the
                        // point is releasing the dead thread's stack.
                        let _ = handle.join.join();
                        None
                    } else {
                        Some(handle)
                    }
                })
                .collect();
            // Go ACCEPTS first and checks capacity after
            // (`server.go`'s `onConn` -> `checkConnectionCount`):
            //
            //     if conns >= int(s.cfg.Instance.MaxConnections) {
            //         return servererr.ErrConCount
            //     }
            //     ...
            //     case servererr.ErrConCount:
            //         if err := conn.writeError(ctx, err); ...
            //
            // so a client over the limit is TOLD `1040 Too many connections`
            // and closed. Acquiring a worker BEFORE accepting left that client
            // in the listen backlog instead, where it waited out its own
            // connect timeout and reported "Lost connection ... waiting for
            // initial communication packet" -- a pooler cannot tell that from
            // a dead server, and 1040 is what tells it to back off.
            let (stream, peer_addr) = match self.listener.accept() {
                Ok(connection) => connection,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(ACCEPT_POLL_INTERVAL);
                    continue;
                }
                Err(error) => break Err(SqlNodeError::Listener(error)),
            };
            // Go's capacity test is a COUNT against the configured limit, and
            // a zero limit is UNLIMITED (`server.go`'s `checkConnectionCount`):
            //
            //     // When the value of Instance.MaxConnections is 0, the number
            //     // of connections is unlimited.
            //     if int(s.cfg.Instance.MaxConnections) == 0 { return nil }
            //     conns := s.ConnectionCount()
            //     if conns >= int(s.cfg.Instance.MaxConnections) { ... }
            //
            // The count says exactly what Go's says, and every admitted client
            // under the limit is guaranteed a serving thread because the
            // dispatch below grows past the warm pool on demand.
            if self.worker_count != 0 && self.tracker.active() >= self.worker_count {
                refuse_over_capacity(&stream);
                continue;
            }
            prepare_stream(&stream, self.connection_timeout)?;
            let connection_key = u64::try_from(accepted)
                .ok()
                .and_then(|value| value.checked_add(1))
                .expect("accepted connection count fits u64");
            let cancellation = ConnectionCancellation::default();
            active_sockets.register(
                connection_key,
                stream.try_clone().map_err(SqlNodeError::Listener)?,
                cancellation.clone(),
            )?;
            let registration =
                ActiveSocketRegistration::new(connection_key, Arc::clone(&active_sockets));
            // Fast path: an idle warm-pool thread. An empty channel also means
            // "momentarily between hands", but that no longer refuses (or
            // waits for) anything: Go serves each accepted connection on its
            // own goroutine (`go s.onConn(clientConn)`), so a pool-less moment
            // simply grows by one dedicated thread for this socket.
            match available_workers.try_recv() {
                Ok(worker_index) => {
                    eprintln!(
                        "{{\"event\":\"connection_dispatch\",\"connection_key\":{connection_key},\"worker_index\":{worker_index}}}"
                    );
                    if worker_senders[worker_index]
                        .send(ConnectionWork {
                            stream,
                            peer_addr,
                            cancellation,
                            registration,
                        })
                        .is_err()
                    {
                        break Err(SqlNodeError::WorkerQueueClosed);
                    }
                }
                Err(mpsc::TryRecvError::Empty) => {
                    eprintln!(
                        "{{\"event\":\"connection_dispatch_dedicated\",\"connection_key\":{connection_key},\"worker_index\":{next_worker_index}}}"
                    );
                    let index = next_worker_index;
                    next_worker_index += 1;
                    let job = dedicated_connection_job(
                        index,
                        stream,
                        peer_addr,
                        cancellation,
                        registration,
                        Arc::clone(&self.factory),
                        Arc::clone(&self.users),
                        Arc::clone(&self.tracker),
                        connection_config.clone(),
                        terminal_sender.clone(),
                    );
                    match std::thread::Builder::new()
                        .name(format!("tidb-sql-connection-{index}"))
                        .stack_size(SQL_WORKER_STACK_BYTES)
                        .spawn(job)
                    {
                        Ok(join) => dedicated_workers.push(WorkerHandle { index, join }),
                        Err(error) => break Err(SqlNodeError::WorkerSpawn(error)),
                    }
                }
                Err(mpsc::TryRecvError::Disconnected) => {
                    break Err(SqlNodeError::WorkerQueueClosed)
                }
            }
            accepted += 1;
        })();

        drop(worker_senders);
        drop(available_sender);
        drop(terminal_sender);
        workers.append(&mut dedicated_workers);
        let drain_result =
            drain_workers(workers, &active_sockets, self.shutdown_grace, &self.tracker);
        combine_node_results(accept_result, drain_result)
    }
}

type WorkerJob = Box<dyn FnOnce() + Send + 'static>;

fn run_worker<S>(
    index: usize,
    work_receiver: mpsc::Receiver<ConnectionWork>,
    available: mpsc::SyncSender<usize>,
    terminal: mpsc::Sender<WorkerTerminal>,
    mut serve: S,
) where
    S: FnMut(ConnectionWork),
{
    let _terminal = WorkerTerminalGuard { index, terminal };
    if available.send(index).is_err() {
        return;
    }
    loop {
        let Ok(work) = work_receiver.recv() else {
            return;
        };
        serve(work);
        if available.send(index).is_err() {
            return;
        }
    }
}

/// Serves one accepted connection to completion. Shared verbatim by the warm
/// pool and the dedicated per-connection threads so both paths run the same
/// handshake/query loop Go runs inside every `onConn` goroutine.
fn serve_connection_work<F: QuerySessionFactory>(
    work: ConnectionWork,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    connection: &WorkerConnectionConfig,
) {
    let ConnectionWork {
        stream,
        peer_addr,
        cancellation,
        registration: _registration,
    } = work;
    if let Err(error) = serve_mysql_connection_with_tls_and_version_info(
        stream,
        peer_addr,
        cancellation,
        factory.as_ref(),
        users.as_ref(),
        tracker,
        MysqlConnectionRuntime {
            max_allowed_packet: connection.max_allowed_packet,
            tls: connection.tls.as_ref(),
            version_info: &connection.version_info,
        },
    ) {
        let message = error.to_string();
        eprintln!("{{\"event\":\"connection_error\",\"error\":{message:?}}}");
    }
}

/// Builds the job for a dedicated thread that serves exactly ONE accepted
/// socket, the direct analogue of Go's per-connection goroutine
/// (`server.go`: `go s.onConn(clientConn)`). The terminal guard reports a
/// panic through the same channel the warm pool uses, so admission still
/// stops when a serving thread dies unexpectedly.
#[allow(clippy::too_many_arguments)]
fn dedicated_connection_job<F: QuerySessionFactory>(
    index: usize,
    stream: TcpStream,
    peer_addr: SocketAddr,
    cancellation: ConnectionCancellation,
    registration: ActiveSocketRegistration,
    factory: Arc<F>,
    users: Arc<ConfiguredUserStore>,
    tracker: Arc<ConnectionTracker>,
    connection: WorkerConnectionConfig,
    terminal: mpsc::Sender<WorkerTerminal>,
) -> WorkerJob {
    Box::new(move || {
        let _terminal = WorkerTerminalGuard { index, terminal };
        serve_connection_work(
            ConnectionWork {
                stream,
                peer_addr,
                cancellation,
                registration,
            },
            &factory,
            &users,
            &tracker,
            &connection,
        );
    })
}

fn spawn_workers<F: QuerySessionFactory>(
    count: usize,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    connection: WorkerConnectionConfig,
) -> Result<WorkerPool, SqlNodeError> {
    spawn_workers_with(count, factory, users, tracker, connection, |index, job| {
        std::thread::Builder::new()
            .name(format!("tidb-sql-connection-{index}"))
            .stack_size(SQL_WORKER_STACK_BYTES)
            .spawn(job)
    })
}

fn spawn_workers_with<F, S>(
    count: usize,
    factory: &Arc<F>,
    users: &Arc<ConfiguredUserStore>,
    tracker: &Arc<ConnectionTracker>,
    connection: WorkerConnectionConfig,
    mut spawn: S,
) -> Result<WorkerPool, SqlNodeError>
where
    F: QuerySessionFactory,
    S: FnMut(usize, WorkerJob) -> std::io::Result<JoinHandle<()>>,
{
    let mut workers = Vec::with_capacity(count);
    let mut work_senders = Vec::with_capacity(count);
    let (available_sender, available_receiver) = mpsc::sync_channel(count);
    let (terminal_sender, terminal_receiver) = mpsc::channel();
    for index in 0..count {
        let (work_sender, work_receiver) = mpsc::channel::<ConnectionWork>();
        let factory = Arc::clone(factory);
        let users = Arc::clone(users);
        let worker_tracker = Arc::clone(tracker);
        let worker_available = available_sender.clone();
        let worker_terminal = terminal_sender.clone();
        let worker_connection = connection.clone();
        let job: WorkerJob = Box::new(move || {
            run_worker(
                index,
                work_receiver,
                worker_available,
                worker_terminal,
                move |work| {
                    serve_connection_work(
                        work,
                        &factory,
                        &users,
                        &worker_tracker,
                        &worker_connection,
                    );
                },
            );
        });
        let worker = match spawn(index, job) {
            Ok(worker) => worker,
            Err(error) => {
                drop(work_sender);
                drop(work_senders);
                drop(available_receiver);
                drop(available_sender);
                drop(terminal_sender);
                let mut failures = vec![SqlNodeError::WorkerSpawn(error)];
                failures.extend(join_worker_failures(workers));
                eprintln!(
                    "{{\"event\":\"process_shutdown_stage\",\"stage\":\"connections\",\"outcome\":\"error\",\"active\":{},\"accepted\":{},\"completed\":{},\"failed\":{},\"forced_connections\":0}}",
                    tracker.active(), tracker.accepted(), tracker.completed(), tracker.failed(),
                );
                return Err(collapse_node_failures(failures));
            }
        };
        work_senders.push(work_sender);
        workers.push(WorkerHandle {
            index,
            join: worker,
        });
    }
    // The original terminal sender moves into the returned pool: the accept
    // loop clones it for every dedicated (per-connection) thread so those
    // report panics through the same channel as the warm pool.
    Ok(WorkerPool {
        workers,
        work_senders,
        available_workers: available_receiver,
        available_sender,
        terminal_sender,
        terminal_workers: terminal_receiver,
    })
}

fn join_worker_failures(workers: Vec<WorkerHandle>) -> Vec<SqlNodeError> {
    let mut panicked = Vec::new();
    for worker in workers {
        if worker.join.join().is_err() {
            panicked.push(worker.index);
        }
    }
    if panicked.is_empty() {
        Vec::new()
    } else {
        vec![SqlNodeError::WorkersPanicked { indexes: panicked }]
    }
}

fn drain_workers(
    workers: Vec<WorkerHandle>,
    active_sockets: &ActiveSockets,
    grace: Duration,
    tracker: &ConnectionTracker,
) -> Result<(), SqlNodeError> {
    let mut failures = Vec::new();
    if let Err(error) = active_sockets.cancel_queries() {
        failures.push(error);
    }
    let deadline = Instant::now() + grace;
    loop {
        match active_sockets.len() {
            Ok(0) => break,
            Ok(_) if Instant::now() < deadline => std::thread::sleep(ACCEPT_POLL_INTERVAL),
            Ok(_) => break,
            Err(error) => {
                failures.push(error);
                break;
            }
        }
    }
    let forced = match active_sockets.len() {
        Ok(0) => 0,
        Ok(_) => {
            let (forced, error) = active_sockets.shutdown_all();
            failures.extend(error);
            forced
        }
        Err(error) => {
            failures.push(error);
            let (forced, error) = active_sockets.shutdown_all();
            failures.extend(error);
            forced
        }
    };
    failures.extend(join_worker_failures(workers));
    let outcome = if failures.is_empty() {
        "success"
    } else {
        "error"
    };
    eprintln!(
        "{{\"event\":\"process_shutdown_stage\",\"stage\":\"connections\",\"outcome\":\"{outcome}\",\"active\":{},\"accepted\":{},\"completed\":{},\"failed\":{},\"forced_connections\":{forced}}}",
        tracker.active(), tracker.accepted(), tracker.completed(), tracker.failed(),
    );
    if failures.is_empty() {
        Ok(())
    } else {
        Err(collapse_node_failures(failures))
    }
}

fn combine_node_results(
    first: Result<(), SqlNodeError>,
    second: Result<(), SqlNodeError>,
) -> Result<(), SqlNodeError> {
    match (first, second) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(first), Err(second)) => Err(collapse_node_failures(vec![first, second])),
    }
}

fn collapse_node_failures(mut failures: Vec<SqlNodeError>) -> SqlNodeError {
    if failures.len() == 1 {
        failures.pop().expect("one SQL node failure")
    } else {
        SqlNodeError::Multiple(failures)
    }
}

/// Startup or runtime failure from [`ConcurrentSqlNode`].
#[derive(Debug)]
pub enum SqlNodeError {
    /// The configured address could not be bound.
    Bind(std::io::Error),
    /// The active listener failed.
    Listener(std::io::Error),
    /// A fixed connection worker could not be created.
    WorkerSpawn(std::io::Error),
    /// Every fixed worker exited before the accepted socket was handed off.
    WorkerQueueClosed,
    /// A worker exited while the accept loop still expected it to serve.
    WorkerTerminated {
        /// Fixed worker index.
        index: usize,
        /// Whether unwind caused the terminal observation.
        panicked: bool,
    },
    /// Every worker was joined; these indexes panicked.
    WorkersPanicked {
        /// Worker indexes whose join handles carried panics.
        indexes: Vec<usize>,
    },
    /// Shared admission or active-socket state was poisoned.
    WorkerStatePoisoned,
    /// One accepted connection failed in a direct lifecycle proof.
    Connection(MysqlConnectionError),
    /// Server TLS material for the MySQL port could not be obtained.
    Tls(String),
    /// Independent node or drain failures retained after all cleanup attempts.
    Multiple(Vec<SqlNodeError>),
}

impl fmt::Display for SqlNodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Bind(error) => write!(formatter, "failed to bind SQL listener: {error}"),
            Self::Listener(error) => write!(formatter, "SQL listener failed: {error}"),
            Self::WorkerSpawn(error) => write!(formatter, "failed to spawn SQL worker: {error}"),
            Self::WorkerQueueClosed => formatter.write_str("SQL worker queue closed"),
            Self::WorkerTerminated { index, panicked } => write!(
                formatter,
                "SQL worker {index} terminated while admission was active (panicked={panicked})"
            ),
            Self::WorkersPanicked { indexes } => {
                write!(formatter, "SQL workers panicked during join: {indexes:?}")
            }
            Self::WorkerStatePoisoned => formatter.write_str("SQL worker state is poisoned"),
            Self::Connection(error) => write!(formatter, "MySQL connection failed: {error}"),
            Self::Tls(detail) => write!(formatter, "MySQL port TLS is unusable: {detail}"),
            Self::Multiple(failures) => {
                formatter.write_str("multiple SQL node failures")?;
                for failure in failures {
                    write!(formatter, "; {failure}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for SqlNodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Bind(error) | Self::Listener(error) | Self::WorkerSpawn(error) => Some(error),
            Self::Connection(error) => Some(error),
            Self::WorkerQueueClosed
            | Self::WorkerTerminated { .. }
            | Self::WorkersPanicked { .. }
            | Self::WorkerStatePoisoned
            | Self::Tls(_)
            | Self::Multiple(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_config::{
        ConfiguredReadColumn, ConfiguredReadColumnKind, ConfiguredReadTable, MemoryArbitratorConfig,
    };
    use std::io::Read;
    use std::net::{IpAddr, Ipv4Addr};
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    struct UnusedSession;

    impl QuerySession for UnusedSession {
        fn execute<'a>(&'a mut self, _sql: &str) -> Result<QueryResult<'a>, SqlQueryError> {
            panic!("the drain regression never completes authentication")
        }
    }

    struct UnusedFactory;

    impl QuerySessionFactory for UnusedFactory {
        type Session = UnusedSession;

        fn open_session(&self, _context: SessionContext) -> Result<Self::Session, SqlQueryError> {
            panic!("the drain regression never completes authentication")
        }
    }

    fn test_config() -> NodeConfig {
        NodeConfig {
            report_status: false,
            advertise_address: String::new(),
            status_host: "0.0.0.0".to_owned(),
            status_port: 0,
            socket: String::new(),
            isolation_read_engines: Vec::new(),
            store_kind: crate::node_config::StoreKind::TiKv,
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: 0,
            affinity_cpus: Vec::new(),
            pd_endpoints: vec!["127.0.0.1:2379".to_owned()],
            read_tables: vec![ConfiguredReadTable {
                database: "test".to_owned(),
                table: "rows".to_owned(),
                table_id: 42,
                columns: vec![ConfiguredReadColumn {
                    name: "id".to_owned(),
                    id: 1,
                    kind: ConfiguredReadColumnKind::ClusteredPrimaryKey,
                }],
                indexes: Vec::new(),
            }],
            load_tables: Vec::new(),
            max_allowed_packet: tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET,
            auth_file: PathBuf::from("unused"),
            load_privileges: false,
            cluster_session: false,
            ssl_cert: None,
            ssl_key: None,
            // The unit tests here exercise worker lifecycle, not the wire, so
            // they take the plaintext port rather than pay for key generation.
            auto_tls: false,
            disconnect_on_expired_password: true,
            sem_enabled: false,
            skip_grant_table: false,
            max_connections: 2,
            connection_timeout: Duration::from_secs(5),
            max_topn_rows: 1_024,
            deadlock_history_capacity: 10,
            deadlock_history_collect_retryable: false,
            schema_lease: Duration::from_millis(45_000),
            cluster_security: tidb_pd_client::ClusterSecurity::plaintext(),
            spill_storage: tidb_util::disk::SpillStorageSpec {
                path: std::env::temp_dir().join("tidb-sql-node-unit-spill"),
                quota_bytes: -1,
                encryption: tidb_util::disk::SpillEncryptionMethod::Plaintext,
            },
            memory_arbitrator: MemoryArbitratorConfig {
                server_memory_limit: "80%".to_owned(),
                mode: "disable".to_owned(),
                soft_limit: "0".to_owned(),
            },
            version_info: VersionInfo::build_default(),
        }
    }

    #[test]
    fn accepted_socket_setup_error_still_drains_and_joins_workers() {
        let users = ConfiguredUserStore::parse(
            "root\t127.0.0.1\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
        )
        .unwrap();
        let node =
            ConcurrentSqlNode::bind(&test_config(), Arc::new(UnusedFactory), Arc::new(users))
                .unwrap()
                .with_shutdown_grace(Duration::from_millis(20));
        assert_eq!(node.shutdown_grace_ms(), 20);
        let address = node.local_addr().unwrap();
        let tracker = node.tracker();
        let server = std::thread::spawn(move || {
            let mut prepared = 0;
            node.run_accept_loop_with(Some(2), move |stream, timeout| {
                prepared += 1;
                if prepared == 2 {
                    return Err(SqlNodeError::Listener(std::io::Error::other(
                        "injected accepted-socket setup failure",
                    )));
                }
                stream
                    .set_nonblocking(false)
                    .map_err(SqlNodeError::Listener)?;
                stream
                    .set_read_timeout(Some(timeout))
                    .map_err(SqlNodeError::Listener)?;
                stream
                    .set_write_timeout(Some(timeout))
                    .map_err(SqlNodeError::Listener)
            })
        });

        let stalled_client = TcpStream::connect(address).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while tracker.active() != 1 {
            assert!(Instant::now() < deadline, "first worker did not start");
            std::thread::sleep(Duration::from_millis(1));
        }
        let failed_setup_client = TcpStream::connect(address).unwrap();
        let error = server.join().unwrap().unwrap_err();

        assert!(matches!(error, SqlNodeError::Listener(_)));
        assert_eq!(tracker.accepted(), 1);
        assert_eq!(tracker.completed(), 1);
        assert_eq!(
            tracker.active(),
            0,
            "all workers must be joined before return"
        );
        drop(failed_setup_client);
        drop(stalled_client);
    }

    #[test]
    fn partial_worker_spawn_failure_joins_every_spawned_prefix() {
        let users = Arc::new(
            ConfiguredUserStore::parse(
                "root\t127.0.0.1\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
            )
            .unwrap(),
        );
        let factory = Arc::new(UnusedFactory);
        let tracker = Arc::new(ConnectionTracker::default());
        let joined = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&joined);

        let result = spawn_workers_with(
            3,
            &factory,
            &users,
            &tracker,
            WorkerConnectionConfig {
                max_allowed_packet: tidb_protocol::DEFAULT_MAX_ALLOWED_PACKET,
                version_info: VersionInfo::build_default(),
                tls: None,
            },
            move |index, job: WorkerJob| {
                if index == 1 {
                    return Err(std::io::Error::other("injected second spawn failure"));
                }
                let observed = Arc::clone(&observed);
                std::thread::Builder::new().spawn(move || {
                    job();
                    observed.fetch_add(1, Ordering::AcqRel);
                })
            },
        );
        let error = match result {
            Ok(_) => panic!("injected worker spawn failure unexpectedly succeeded"),
            Err(error) => error,
        };

        assert!(matches!(error, SqlNodeError::WorkerSpawn(_)));
        assert_eq!(joined.load(Ordering::Acquire), 1);
    }

    #[test]
    fn worker_join_collects_every_panic_index() {
        let workers = vec![
            WorkerHandle {
                index: 0,
                join: std::thread::spawn(|| panic!("first worker panic")),
            },
            WorkerHandle {
                index: 1,
                join: std::thread::spawn(|| {}),
            },
            WorkerHandle {
                index: 2,
                join: std::thread::spawn(|| panic!("last worker panic")),
            },
        ];

        let failures = join_worker_failures(workers);
        let [SqlNodeError::WorkersPanicked { indexes }] = failures.as_slice() else {
            panic!("one aggregate worker panic failure");
        };
        assert_eq!(indexes, &[0, 2]);
    }

    #[test]
    fn worker_panic_surfaces_through_terminal_poll_and_stops_admission() {
        let (_available_tx, _available_rx) = mpsc::sync_channel::<usize>(1);
        let (terminal_tx, terminal_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let _terminal = WorkerTerminalGuard {
                index: 3,
                terminal: terminal_tx,
            };
            panic!("injected pre-availability worker panic");
        });
        // The panicked guard's terminal surfaces through the same poll the
        // accept loop runs every iteration; poll with a deadline because the
        // guard only sends while the panic unwinds.
        let deadline = Instant::now() + Duration::from_secs(2);
        let surfaced = loop {
            match poll_terminal(&terminal_rx) {
                Err(error @ SqlNodeError::WorkerTerminated { .. }) => break error,
                _ if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(5)),
                _ => panic!("the panicked worker never surfaced through the terminal poll"),
            }
        };
        assert!(matches!(
            surfaced,
            SqlNodeError::WorkerTerminated {
                index: 3,
                panicked: true,
            }
        ));
        // A dedicated thread whose client simply disconnected reports
        // `Returned`; that is normal goroutine completion and must not stop
        // admission.
        let (returned_tx, returned_rx) = mpsc::channel();
        returned_tx
            .send(WorkerTerminal {
                index: 4,
                kind: WorkerTerminalKind::Returned,
            })
            .unwrap();
        drop(returned_tx);
        assert!(poll_terminal(&returned_rx).is_ok());
        // An empty channel keeps the accept loop admitting.
        assert!(poll_terminal(&terminal_rx).is_ok());
        assert!(worker.join().is_err());
    }

    #[test]
    fn worker_panic_after_admission_releases_socket_and_stops_admission() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let _client = TcpStream::connect(address).unwrap();
        let (server, peer_addr) = listener.accept().unwrap();
        let sockets = Arc::new(ActiveSockets::default());
        let cancellation = ConnectionCancellation::default();
        let connection_key = 11;
        sockets
            .register(
                connection_key,
                server.try_clone().unwrap(),
                cancellation.clone(),
            )
            .unwrap();
        let registration = ActiveSocketRegistration::new(connection_key, Arc::clone(&sockets));
        let (work_tx, work_rx) = mpsc::channel();
        let (available_tx, available_rx) = mpsc::sync_channel(1);
        let (terminal_tx, terminal_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            run_worker(2, work_rx, available_tx, terminal_tx, |_work| {
                panic!("injected post-admission worker panic");
            });
        });
        assert_eq!(available_rx.recv().unwrap(), 2);
        work_tx
            .send(ConnectionWork {
                stream: server,
                peer_addr,
                cancellation,
                registration,
            })
            .unwrap();

        let terminal = terminal_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        assert_eq!(
            terminal,
            WorkerTerminal {
                index: 2,
                kind: WorkerTerminalKind::Panicked,
            }
        );
        assert_eq!(sockets.len().unwrap(), 0);
        assert!(worker.join().is_err());
    }

    #[test]
    fn simultaneous_accept_and_drain_failures_are_both_retained() {
        let error = combine_node_results(
            Err(SqlNodeError::Listener(std::io::Error::other(
                "injected accept failure",
            ))),
            Err(SqlNodeError::WorkersPanicked {
                indexes: vec![1, 4],
            }),
        )
        .unwrap_err();

        let SqlNodeError::Multiple(failures) = error else {
            panic!("independent accept and drain failures must be aggregated");
        };
        assert!(matches!(failures[0], SqlNodeError::Listener(_)));
        assert!(matches!(
            &failures[1],
            SqlNodeError::WorkersPanicked { indexes } if indexes == &[1, 4]
        ));
    }

    #[test]
    fn repeated_shutdown_signals_are_idempotent_and_stop_admission() {
        let users = ConfiguredUserStore::parse(
            "root\t127.0.0.1\tmysql_native_password\t*0000000000000000000000000000000000000000\n",
        )
        .unwrap();
        let node =
            ConcurrentSqlNode::bind(&test_config(), Arc::new(UnusedFactory), Arc::new(users))
                .unwrap();
        let shutdown = node.shutdown_handle();
        shutdown.shutdown();
        shutdown.shutdown();
        shutdown.shutdown();
        assert!(shutdown.is_shutdown_requested());

        // Admission stops before any accept: the bounded loop drains and
        // returns without ever handing a socket to a worker.
        let tracker = node.tracker();
        let server = std::thread::spawn(move || node.serve_connections(1).unwrap());
        let deadline = Instant::now() + Duration::from_secs(2);
        while !server.is_finished() {
            assert!(Instant::now() < deadline, "shutdown did not stop admission");
            std::thread::sleep(Duration::from_millis(5));
        }
        server.join().unwrap();
        assert_eq!(tracker.accepted(), 0);
    }

    #[test]
    fn queued_connection_receiver_death_releases_active_socket() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let _client = TcpStream::connect(address).unwrap();
        let (server, peer_addr) = listener.accept().unwrap();
        let sockets = Arc::new(ActiveSockets::default());
        let cancellation = ConnectionCancellation::default();
        let connection_key = 17;
        sockets
            .register(
                connection_key,
                server.try_clone().unwrap(),
                cancellation.clone(),
            )
            .unwrap();
        let registration = ActiveSocketRegistration::new(connection_key, Arc::clone(&sockets));
        let (sender, receiver) = mpsc::channel();
        sender
            .send(ConnectionWork {
                stream: server,
                peer_addr,
                cancellation,
                registration,
            })
            .unwrap();

        drop(receiver);

        assert_eq!(sockets.len().unwrap(), 0);
    }

    #[test]
    fn poisoned_active_socket_state_still_cancels_and_forces_shutdown() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(address).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let (server, _) = listener.accept().unwrap();
        let sockets = Arc::new(ActiveSockets::default());
        let cancellation = ConnectionCancellation::default();
        sockets.register(23, server, cancellation.clone()).unwrap();
        let poisoned_sockets = Arc::clone(&sockets);
        assert!(std::thread::spawn(move || {
            let _guard = poisoned_sockets.streams.lock().unwrap();
            panic!("injected active-socket state poison");
        })
        .join()
        .is_err());
        let workers = vec![WorkerHandle {
            index: 0,
            join: std::thread::spawn(|| {}),
        }];

        let error = drain_workers(
            workers,
            sockets.as_ref(),
            Duration::ZERO,
            &ConnectionTracker::default(),
        )
        .unwrap_err();

        assert!(matches!(error, SqlNodeError::WorkerStatePoisoned));
        assert!(cancellation.is_cancelled());
        let mut byte = [0_u8; 1];
        assert_eq!(client.read(&mut byte).unwrap(), 0);
    }
}
