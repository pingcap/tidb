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

//! The session API the MySQL front end drives: Go `session.Session` (the
//! query interface a connection holds) with its result sets, prepared
//! statements and cancellation carriers.



use std::borrow::Cow;
use std::net::Shutdown;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_ast::Stmt;
use tidb_exec::cluster_stats_lock::ClusterStatsLockError;
use tidb_cluster::pessimistic_lock_error::commit_outcome_to_sql_error;
use tidb_exec::real_tikv_analyze::ClusterAnalyzeError;
use tidb_exec::real_tikv_stats_lock::ClusterStatsLockCommitError;
use tidb_protocol::ColumnInfo;
use tidb_txnkv::transaction::OptimisticCommitOutcome;
use tidb_util::globalconn::{Allocator, GlobalAllocator, SimpleAllocator};

use crate::domain::configured_user_store::AuthenticatedIdentity;
use crate::domain::resultset_source::ResultSetSource;
use crate::domain::wire_status::WireStatus;
use crate::process::ProcessKillTarget;

// A connection worker runs the planner, whose recursion is guarded by
// `stacker::maybe_grow(red_zone = 2 MB, segment = 16 MB)`. On the default
// 2 MB thread stack the red-zone check fails on EVERY select, so each
// statement mmap'ed and munmap'ed a fresh 16 MB segment -- measured at ~11%
// of the serving thread under sysbench point selects. Go never pays this
// because a goroutine's grown stack PERSISTS for the goroutine's life; a
// large reserved stack is that semantics for a dedicated thread (reserved
// address space, committed only as touched), leaving `maybe_grow` as the
// safety net for genuinely deep plans.
pub const SQL_WORKER_STACK_BYTES: usize = tidb_util::worker_stack::WORKER_STACK_BYTES;

/// A named thread with the SQL worker's stack, for every background worker
/// that runs a session, a transaction or a catalog reload. Go runs these on
/// goroutines whose stacks grow; a debug build's commit path alone (the
/// client's prewrite futures) outgrows the default 2 MB thread stack, which
/// aborted the whole process from `tidb-binding-maintenance`'s GC commit.
pub fn sql_worker_thread(name: impl Into<String>) -> std::thread::Builder {
    std::thread::Builder::new()
        .name(name.into())
        .stack_size(SQL_WORKER_STACK_BYTES)
}


/// A backend query that can be interrupted during forced connection drain.
pub trait ActiveQueryCancellation: Send + Sync {
    /// Fires the query's canonical cancellation carrier.
    fn cancel(&self);

    /// Fires it with a watchdog's own kill signal (Go server `killQuery`).
    fn cancel_with(&self, signal: tidb_util::sqlkiller::KillSignal) {
        let _ = signal;
        self.cancel();
    }
}

impl ActiveQueryCancellation for tidb_executor::StatementCancellation {
    fn cancel(&self) {
        tidb_executor::StatementCancellation::cancel(self);
    }

    fn cancel_with(&self, signal: tidb_util::sqlkiller::KillSignal) {
        tidb_executor::StatementCancellation::cancel_with(self, signal);
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

    pub fn cancel(&self) {
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
        self.cancel_current_query_with(tidb_util::sqlkiller::KillSignal::QueryInterrupted);
    }

    /// [`Self::cancel_current_query`] with the signal the statement observes.
    fn cancel_current_query_with(&self, signal: tidb_util::sqlkiller::KillSignal) {
        let active = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .active
            .clone();
        if let Some(active) = active {
            active.cancel_with(signal);
        }
    }

    pub fn is_cancelled(&self) -> bool {
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
    socket: Option<Arc<crate::domain::client_socket::ClientSocket>>,
}

impl ConnectionClose {
    /// Binds the handle to the connection's own socket.
    pub fn with_socket(socket: crate::domain::client_socket::ClientSocket) -> Self {
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

    fn cancel_query_with(&self, signal: tidb_util::sqlkiller::KillSignal) {
        self.cancellation.cancel_current_query_with(signal);
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

pub use tidb_cluster::sql_query_error::{lock_sql_error, SqlQueryError, RESULT_UNDETERMINED_MESSAGE};
pub use tidb_ddl::real_tikv_ddl::cluster_ddl_error;

pub fn cluster_analyze_error(error: ClusterAnalyzeError) -> SqlQueryError {
    match error {
        ClusterAnalyzeError::Analyze(error) => {
            let error = tidb_executor::DriverError::from(error).to_mysql_error();
            SqlQueryError::new(error.code, error.state, error.message).with_identity(error.identity)
        }
        ClusterAnalyzeError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterAnalyzeError::Commit(error) => lock_sql_error(&error),
        ClusterAnalyzeError::Killed(error) => SqlQueryError::new(
            error.code,
            error.state.as_bytes().try_into().unwrap_or(*b"HY000"),
            error.message,
        ),
        ClusterAnalyzeError::MissingPartitionStats(detail) => {
            SqlQueryError::new(8243, *b"HY000", detail)
        }
        ClusterAnalyzeError::MissingPartitionItemStats(detail) => {
            SqlQueryError::new(8244, *b"HY000", detail)
        }
        ClusterAnalyzeError::Other(detail) => SqlQueryError::unknown(detail),
        ClusterAnalyzeError::MissingTable(name) => {
            SqlQueryError::new(1146, *b"42S02", format!("Table '{name}' doesn't exist"))
        }
        ClusterAnalyzeError::MissingIndex(detail) => SqlQueryError::new(8109, *b"HY000", detail),
        ClusterAnalyzeError::PartitionManagementOnNonpartitioned => {
            let error =
                tidb_executor::DriverError::PartitionManagementOnNonpartitioned.to_mysql_error();
            SqlQueryError::new(error.code, error.state, error.message).with_identity(error.identity)
        }
    }
}

pub fn cluster_stats_lock_error(error: ClusterStatsLockCommitError) -> SqlQueryError {
    match error {
        ClusterStatsLockCommitError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterStatsLockCommitError::Commit(error) => lock_sql_error(&error),
        ClusterStatsLockCommitError::Plan(ClusterStatsLockError::NoDatabaseSelected) => {
            SqlQueryError::new(1046, *b"3D000", "No database selected")
        }
        ClusterStatsLockCommitError::Plan(error @ ClusterStatsLockError::MissingTable { .. }) => {
            SqlQueryError::new(1146, *b"42S02", error.to_string())
        }
        ClusterStatsLockCommitError::Plan(
            error @ ClusterStatsLockError::UnknownPartition { .. },
        ) => SqlQueryError::new(1735, *b"HY000", error.to_string()),
        ClusterStatsLockCommitError::Plan(error) => SqlQueryError::unknown(error.to_string()),
        ClusterStatsLockCommitError::Other(detail) => SqlQueryError::unknown(detail),
    }
}

/// Returns the client error for a cluster state change that did not commit,
/// preserving the ambiguous outcome instead of reporting a false failure.
pub fn cluster_commit_error(
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
    /// Fallback values for eager sources. Lazy sources supply live statement
    /// state at each metadata/terminal boundary through ResultSetSource.
    warnings: u16,
    status: WireStatus,
    /// Statement affected-row value carried by a deprecated-EOF OK packet.
    affected_rows: u64,
    /// Statement last-insert-id value carried by a deprecated-EOF OK packet.
    last_insert_id: u64,
    /// Statement informational text carried by a deprecated-EOF OK packet.
    info: Vec<u8>,
}

/// Typed statement policy retained only when a prepared cursor materializes
/// this result after execution.
pub struct CursorMaterializationAuthority {
    pub current_tso: Option<tidb_executor::CurrentTso>,
    pub start_ts_guard: Option<Arc<tidb_txnkv::StartTsGuard>>,
    pub field_types: Vec<tidb_datatype::FieldType>,
    pub init_chunk_size: usize,
    pub max_chunk_size: usize,
    pub memory: tidb_executor::StatementMemory,
}

/// One connection-owned prepared statement of either admitted kind.
///
/// Ordinary statements retain the session-owned parsed definition and bind
/// current values per EXECUTE. Routed commands retain only their SQL text.
#[derive(Clone, Debug)]
pub struct PreparedGeneral {
    sql: String,
    /// The statement's SQL digest, computed once at PREPARE. Go keeps
    /// `NormalizedSQL`/`SQLDigest` on the `PlanCacheStmt` and every EXECUTE
    /// installs them with `InitSQLDigest` (`pkg/executor/select.go:1058`)
    /// instead of normalizing the text again.
    digest: String,
    parameter_count: usize,
    result_columns: Vec<ColumnInfo>,
    /// The session owns the parse, cache candidate and execution readiness.
    prepared_ast: Option<crate::PreparedAst>,
}

impl PreparedGeneral {
    /// Creates one from its statement text and the metadata a PREPARE reports.
    #[must_use]
    pub fn new(sql: String, parameter_count: usize, result_columns: Vec<ColumnInfo>) -> Self {
        let digest = tidb_parser::normalize_digest(&sql).1.to_string();
        Self {
            sql,
            digest,
            parameter_count,
            result_columns,
            prepared_ast: None,
        }
    }

    /// Retains the session's prepared definition and its cache lifecycle.
    #[must_use]
    pub fn with_prepared_ast(
        prepared_ast: crate::PreparedAst,
        result_columns: Vec<ColumnInfo>,
    ) -> Self {
        Self {
            sql: prepared_ast.sql().to_owned(),
            digest: tidb_parser::normalize_digest(prepared_ast.sql())
                .1
                .to_string(),
            parameter_count: prepared_ast.parameter_count(),
            result_columns,
            prepared_ast: Some(prepared_ast),
        }
    }

    /// The statement text, whose markers an execute binds.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// The digest of [`Self::sql`], fixed at PREPARE.
    #[must_use]
    pub fn digest(&self) -> &str {
        &self.digest
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
        self.prepared_ast
            .as_ref()
            .map(crate::PreparedAst::statement)
    }

    /// The session-owned prepared definition, when this is not a routed command.
    #[must_use]
    pub fn prepared_ast(&self) -> Option<&crate::PreparedAst> {
        self.prepared_ast.as_ref()
    }

    /// The immutable point-get cache candidate owned by the prepared definition.
    pub fn point_get_plan(&self) -> Option<std::sync::Arc<tidb_executor::PreparedPointGetPlan>> {
        self.prepared_ast
            .as_ref()
            .and_then(crate::PreparedAst::point_get_plan)
    }

    /// The immutable prepared SELECT cache descriptor.
    #[must_use]
    pub fn select_plan(&self) -> Option<std::sync::Arc<tidb_executor::PreparedSelectPlan>> {
        self.prepared_ast
            .as_ref()
            .and_then(crate::PreparedAst::select_plan)
    }

    /// The immutable prepared DML plan.
    #[must_use]
    pub fn dml_plan(&self) -> Option<std::sync::Arc<tidb_executor::PreparedDmlPlan>> {
        self.prepared_ast
            .as_ref()
            .and_then(crate::PreparedAst::dml_plan)
    }
}

/// What executing a general prepared statement produced.
pub enum GeneralExecuteOutcome<'a> {
    /// A result set, streamed the same way a COM_QUERY result is.
    Rows(QueryResult<'a>),
    /// An OK packet's affected-row count and last insert id.
    Write(WriteOutcome),
}

/// What one prepared write reported after a determinate commit.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
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
            affected_rows: 0,
            last_insert_id: 0,
            info: Vec::new(),
        }
    }

    /// Attaches the exact typed policy captured inside the producing
    /// statement, before any `SET_VAR` overlay was restored.
    #[must_use]
    pub fn with_cursor_materialization(
        mut self,
        field_types: Vec<tidb_datatype::FieldType>,
        mut authority: crate::ResultMaterializationAuthority,
    ) -> Self {
        let current_tso = authority.take_current_tso();
        let start_ts_guard = authority.take_start_ts_guard();
        let (memory, init_chunk_size, max_chunk_size) = authority.into_parts();
        self.cursor_materialization = Some(CursorMaterializationAuthority {
            current_tso,
            start_ts_guard,
            field_types,
            init_chunk_size,
            max_chunk_size,
            memory,
        });
        self
    }

    /// Takes the cursor authority exactly once. Ordinary result-set writers
    /// never call this and retain their existing source-only path.
    pub fn take_cursor_materialization(&mut self) -> Option<CursorMaterializationAuthority> {
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

    /// Attaches the statement values Go's `writeEOF` forwards to its
    /// deprecated-EOF OK packet. Legacy EOF clients ignore these fields.
    #[must_use]
    pub fn with_statement_output(
        mut self,
        affected_rows: u64,
        last_insert_id: u64,
        info: Vec<u8>,
    ) -> Self {
        self.affected_rows = affected_rows;
        self.last_insert_id = last_insert_id;
        self.info = info;
        self
    }

    /// The status word for the EOF packets that frame this result set.
    #[must_use]
    pub fn wire_status(&self) -> WireStatus {
        self.source
            .statement_status()
            .map_or(self.status, |state| state.status)
    }

    /// The warning count for the EOF packets that frame this result set.
    #[must_use]
    pub fn warning_count(&self) -> u16 {
        self.source
            .statement_status()
            .map_or(self.warnings, |state| state.warnings)
    }

    /// Statement affected rows preserved for deprecated-EOF framing.
    #[must_use]
    pub fn affected_rows(&self) -> u64 {
        self.source
            .statement_status()
            .map_or(self.affected_rows, |state| state.affected_rows)
    }

    /// Statement last insert id preserved for deprecated-EOF framing.
    #[must_use]
    pub fn last_insert_id(&self) -> u64 {
        self.source
            .statement_status()
            .map_or(self.last_insert_id, |state| state.last_insert_id)
    }

    /// Statement info preserved for deprecated-EOF framing.
    #[must_use]
    pub fn info(&self) -> &[u8] {
        self.source
            .statement_status()
            .map_or(&self.info, |state| state.info)
    }

    /// Returns the sole mutable result-set owner.
    pub fn source(&mut self) -> &mut BoxedResultSetSource<'a> {
        &mut self.source
    }

    /// Retains the process-list statement until this result set is finished.
    #[must_use]
    pub fn with_process_statement(
        self,
        statement: crate::process::ProcessStatementGuard,
    ) -> Self {
        let Self {
            source,
            cursor_materialization,
            warnings,
            status,
            affected_rows,
            last_insert_id,
            info,
        } = self;
        Self {
            source: BoxedResultSetSource {
                inner: Box::new(ProcessTrackedResultSet {
                    inner: source.inner,
                    statement: Some(statement),
                }),
            },
            cursor_materialization,
            warnings,
            status,
            affected_rows,
            last_insert_id,
            info,
        }
    }
}

struct ProcessTrackedResultSet<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
    statement: Option<crate::process::ProcessStatementGuard>,
}

impl ResultSetSource for ProcessTrackedResultSet<'_> {
    fn statement_status(&self) -> Option<crate::domain::resultset_source::StatementStatus<'_>> {
        self.inner.statement_status()
    }
    fn new_chunk(&self) -> Option<tidb_chunk::chunk::Chunk> {
        self.inner.new_chunk()
    }
    fn next_chunk(
        &mut self,
        chunk: &mut tidb_chunk::chunk::Chunk,
    ) -> Result<(), tidb_executor::MysqlError> {
        self.inner.next_chunk(chunk)
    }
    fn field_types(&self) -> &[tidb_datatype::FieldType] {
        self.inner.field_types()
    }

    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_executor::MysqlError> {
        self.inner.next_batch(max_rows)
    }

    fn supports_text_batch(&self) -> bool {
        self.inner.supports_text_batch()
    }

    fn next_text_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<
        Option<Box<dyn tidb_exec::distsql_recordset::TextResultBatch>>,
        tidb_executor::MysqlError,
    > {
        self.inner.next_text_batch(max_rows)
    }

    fn columns(&mut self) -> Result<Vec<ColumnInfo>, tidb_executor::MysqlError> {
        self.inner.columns()
    }

    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        let result = self.inner.finish();
        self.statement.take();
        result
    }

    fn close(&mut self) -> Result<(), tidb_executor::MysqlError> {
        let result = self.inner.close();
        self.statement.take();
        result
    }

    fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration) {
        self.inner.add_write_sql_resp_duration(duration);
    }

    fn add_result_chunk_cells(&mut self, cells: i64) {
        self.inner.add_result_chunk_cells(cells);
    }
}

/// Sized adapter for a boxed worker-local result source.
pub struct BoxedResultSetSource<'a> {
    inner: Box<dyn ResultSetSource + 'a>,
}

impl ResultSetSource for BoxedResultSetSource<'_> {
    fn statement_status(&self) -> Option<crate::domain::resultset_source::StatementStatus<'_>> {
        self.inner.statement_status()
    }
    fn new_chunk(&self) -> Option<tidb_chunk::chunk::Chunk> {
        self.inner.new_chunk()
    }
    fn next_chunk(
        &mut self,
        chunk: &mut tidb_chunk::chunk::Chunk,
    ) -> Result<(), tidb_executor::MysqlError> {
        self.inner.next_chunk(chunk)
    }
    fn field_types(&self) -> &[tidb_datatype::FieldType] {
        self.inner.field_types()
    }

    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_executor::MysqlError> {
        self.inner.next_batch(max_rows)
    }

    fn supports_text_batch(&self) -> bool {
        self.inner.supports_text_batch()
    }

    fn next_text_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<
        Option<Box<dyn tidb_exec::distsql_recordset::TextResultBatch>>,
        tidb_executor::MysqlError,
    > {
        self.inner.next_text_batch(max_rows)
    }

    fn columns(&mut self) -> Result<Vec<tidb_protocol::ColumnInfo>, tidb_executor::MysqlError> {
        self.inner.columns()
    }

    fn finish(&mut self) -> Result<(), tidb_executor::MysqlError> {
        self.inner.finish()
    }

    fn close(&mut self) -> Result<(), tidb_executor::MysqlError> {
        self.inner.close()
    }

    fn add_write_sql_resp_duration(&mut self, duration: std::time::Duration) {
        self.inner.add_write_sql_resp_duration(duration);
    }

    fn add_result_chunk_cells(&mut self, cells: i64) {
        self.inner.add_result_chunk_cells(cells);
    }
}

/// One authenticated connection's immutable session context.
#[derive(Clone, Debug)]
pub struct SessionContext {
    /// Server connection identity.
    pub connection_id: u64,
    /// Accepted peer: Go's `RemoteAddr` and `isUnixSocket`.
    pub peer_addr: crate::domain::client_socket::PeerAddress,
    /// Identity established by ordinary password verification or validated
    /// process-wide skip-grant admission.
    pub identity: AuthenticatedIdentity,
    /// Whether this client negotiated `CLIENT_FOUND_ROWS`, making UPDATE and
    /// duplicate-key UPDATE report successfully matched unchanged rows.
    pub client_found_rows: bool,
    /// Whether the MySQL front end completed a TLS handshake for this
    /// connection.
    pub secure_transport: bool,
    /// The negotiated TLS `(cipher, version)` in their MySQL/OpenSSL
    /// spellings (`tidb_util::tls`), `None` on a plaintext connection.
    /// Go reports the same pair through `Ssl_cipher`/`Ssl_version`
    /// (`server.go:1329`).
    pub tls_status: Option<(String, String)>,
    /// Forced-drain carrier on which the session registers each active query.
    pub cancellation: ConnectionCancellation,
    /// Handle a `KILL` uses to end this connection.
    pub close: ConnectionClose,
}

/// Query capability retained entirely inside one fixed worker thread.
pub trait QuerySession {
    /// Shared SQL state used by connection-owned migration handlers.
    fn migration_session(&mut self) -> Option<&mut crate::Session> { None }

    /// Go SessionVars.ResourceGroupName for command metrics.
    fn metrics_resource_group(&self) -> &str {
        "default"
    }
    /// Go StmtCtx.ResourceGroupName, including an admitted statement hint.
    fn metrics_statement_resource_group(&self) -> &str {
        self.metrics_resource_group()
    }

    /// Parses one COM_QUERY statement once: Go `handleQuery` runs
    /// `session.ParseSQL` and hands each node to `handleStmt`
    /// (`pkg/server/conn.go:1861`); no later step parses the text again.
    fn parse_statement(&mut self, sql: &str) -> Result<Stmt, SqlQueryError>;

    /// Go `QueryCtx.ExecuteStmt` (`pkg/server/driver_tidb.go:364`): runs one
    /// parsed statement, whatever its kind. `Some` is a result set the
    /// connection writes as rows. `None` is answered with an OK packet built
    /// from [`Self::affected_rows`], [`Self::last_insert_id`],
    /// [`Self::statement_info`], the status and the warnings -- unless the
    /// statement parked a client-local file request
    /// ([`Self::take_local_infile_request`]), which the connection serves
    /// first. A failed statement's error is already in the warning buffer
    /// (Go `StmtCtx.AppendError`).
    fn execute_stmt<'a>(
        &'a mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<QueryResult<'a>>, SqlQueryError>;

    /// Go `QueryCtx.AffectedRows` for the OK packet of the last statement
    /// that answered no result set.
    fn affected_rows(&self) -> u64 {
        0
    }

    /// Go `QueryCtx.LastInsertID` for the same OK packet.
    fn last_insert_id(&self) -> u64 {
        0
    }

    /// Go `ctx.Value(LoadDataVarKey)` / `ctx.Value(LoadStatsVarKey)` after
    /// `ExecuteStmt`: the client-local path the statement just executed
    /// parked, if it parked one.
    fn take_local_infile_request(&mut self) -> Option<String> {
        None
    }

    /// Go `handleLoadData` / `handleLoadStats`: finishes the parked statement
    /// over the file the client sent.
    fn finish_local_infile(&mut self, _data: &[u8]) -> Result<(), SqlQueryError> {
        Err(SqlQueryError::unknown(
            "this session does not accept client-local files",
        ))
    }

    /// Records a COM_QUERY parse failure on the session's warning buffer:
    /// go `session.go:1955-1968` opens a FRESH warning context on a parse
    /// failure and appends the syntax error into it, so `SHOW WARNINGS`
    /// after a failed parse reports the error row and NOT the previous
    /// statement's leftovers. Default: nothing, for sessions without a
    /// warning buffer.
    fn record_parse_failure(&mut self, _error: &SqlQueryError) {}

    /// Go `clientConn.addQueryMetrics`' transaction-write SLI finalizer,
    /// called after the SQL command has written its response.
    fn finish_execute_stmt(&mut self, _cost: Duration) {}

    /// Go `ConnectionInfo.Attributes`: the connection attributes of the
    /// handshake response, which the slow log prints. The default keeps
    /// sessions without a slow log silent.
    fn set_connection_attributes(
        &mut self,
        _attributes: std::collections::BTreeMap<String, String>,
    ) {
    }

    /// The session's `tidb_slow_log_threshold`, which gates the dashboard's
    /// slow-query histograms (`adapter.go:1958` reads it from the instance
    /// config; the session variable mirrors it per session). `None` marks a
    /// session that does not expose the variable: the slow-query observation
    /// is skipped rather than guessed at the default.
    fn slow_log_threshold(&self) -> Option<Duration> {
        None
    }

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

    /// The session's SQL killer, where the connection installs its liveness
    /// probe for a statement (Go `setSQLKillerConnectionAlive`). Sessions
    /// without one take no probe.
    fn sql_killer(&self) -> Option<Arc<tidb_util::sqlkiller::SqlKiller>> {
        None
    }

    /// Go `shouldInstallConnectionAlive`: whether `stmt` runs with the
    /// connection-liveness probe. Sessions without prepared statements answer
    /// from the statement alone.
    fn should_install_connection_alive(&self, stmt: &Stmt) -> bool {
        should_install_connection_alive(stmt, |_| None)
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
    /// value. Go refuses SQL `SET SESSION max_allowed_packet`; its internal
    /// session seeding and inherited GLOBAL copy take effect on the next
    /// packet.
    /// `None` is Go's `cc.getCtx() == nil`: the config seed
    /// (`PacketIO.SetMaxAllowedPacket(config.GetMaxAllowedPacket())`) stands
    /// until a session exists to ask.
    fn max_allowed_packet(&self) -> Option<usize> {
        None
    }

    /// Attempts to charge bytes retained by `COM_STMT_SEND_LONG_DATA` to this
    /// connection's persistent session tracker. Sessions without a memory
    /// tracker keep the historical unbounded behavior; real TiDB sessions
    /// override this with the same quota check as Go's `AppendParam`.
    fn try_consume_long_data(&mut self, _bytes: i64) -> bool {
        true
    }

    /// Releases bytes previously charged for `COM_STMT_SEND_LONG_DATA` when
    /// the statement is reset, executed, or closed.
    fn release_long_data(&mut self, _bytes: i64) {}

    /// Connection ID used in the client-visible memory-quota diagnostic.
    fn connection_id(&self) -> u64 {
        0
    }

    /// Release physical entries for COM_STMT_CLOSE using session cache policy.
    fn close_prepared(&mut self, _statement: &PreparedGeneral) {}

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

    /// MySQL warning numbers in the statement context that produced the
    /// response. Go records each of these in `clientConn.flush`.
    fn warning_codes(&self) -> Vec<u16> {
        Vec::new()
    }

    /// The length-encoded info string Go's `StatementContext.LastMessage`
    /// publishes in the INSERT/REPLACE/UPDATE OK packet. INSERT and REPLACE
    /// compose `Records: <records>  Duplicates: <dups>  Warnings: <warnings>`
    /// once the statement attempted more than one row or ran as INSERT ...
    /// SELECT (`InsertExec.setMessage` / `ReplaceExec.setMessage`); UPDATE
    /// composes its own matched/changed text. Sessions without a
    /// statement-message producer keep the protocol field empty.
    fn statement_info(&self) -> Vec<u8> {
        Vec::new()
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
    fn result_charset(&self) -> Cow<'_, str> {
        Cow::Borrowed("")
    }

    /// Go `clientConn.initInputEncoder`: this session's
    /// `@@character_set_client`, applied to string-family binary parameters
    /// before they reach expression or storage semantics.
    fn input_charset(&self) -> Cow<'_, str> {
        Cow::Borrowed("utf8mb4")
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

    /// Seeds the connection charset and collation from the handshake's
    /// collation id (Go `session.SetCollation`, run by
    /// `TiDBDriver.OpenCtx`). Go writes the session variables directly and
    /// issues no statement; an id outside Go's collation table fails the
    /// connection with Go's `GetCharsetInfoByID` error.
    ///
    /// Sessions that keep no charset variables of their own do nothing.
    fn set_collation(&mut self, _collation_id: u8) -> Result<(), SqlQueryError> {
        Ok(())
    }
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

    /// Connect SQL and new connections to the same process TLS owner.
    fn install_tls_manager(
        &self,
        _manager: Arc<dyn crate::process::TlsManager>,
        _variables: Vec<(&'static str, String)>,
    ) {
    }

    /// Domain's numeric identity; None is the standalone deployment.
    fn server_identity(&self) -> Option<Arc<tidb_domain::server_id::ServerIdAuthority>> {
        None
    }

    /// Domain's connection-ID allocator (Go `Domain.connIDAllocator`), when
    /// the factory owns one: client connections then draw their IDs from the
    /// same allocator as the factory's internal processes, as Go's
    /// `newClientConn` and auto-analyze both call `NextConnID`. None lets the
    /// connection tracker build its own.
    fn connection_ids(&self) -> Option<Arc<dyn Allocator + Send + Sync>> {
        None
    }

    /// Returns the server's session manager for process memory control.
    fn session_manager(&self) -> Option<Arc<dyn tidb_util::memoryusagealarm::SessionManager>> {
        None
    }
}


/// Go `Domain`'s connection-ID allocator (`domain.go`, "initialize the global
/// kill"): with global kill on, a global allocator whose IDs name this node's
/// server ID -- the leased one, or `serverIDForStandalone` without etcd --
/// so `KILL` can route by ID; otherwise the simple allocator. One instance
/// serves a node: client connections and internal process IDs such as
/// auto-analyze's (Go `NextConnID`) draw from it.
pub fn new_connection_id_allocator(
    config: &tidb_config::config_tree::Config,
    identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
) -> Arc<dyn Allocator + Send + Sync> {
    if config.enable_global_kill {
        let owner = identity;
        Arc::new(GlobalAllocator::new(
            move || {
                owner
                    .as_ref()
                    .map_or(tidb_util::globalconn::SERVER_ID_FOR_STANDALONE, |owner| owner.id())
            },
            config.enable_32bits_connection_id,
        ))
    } else {
        Arc::new(SimpleAllocator::new())
    }
}

/// Go `shouldInstallConnectionAlive` (`pkg/server/conn.go`): EXECUTE, TRACE
/// and EXPLAIN ANALYZE are unwrapped to the statement they run (`prepared`
/// resolves an EXECUTE's name), then the long-running kinds a client without
/// keepalive must not lose -- DDL, ANALYZE, LOAD DATA, IMPORT INTO, BACKUP /
/// RESTORE -- and the non-interruptible COMMIT / ROLLBACK run without the
/// probe.
pub fn should_install_connection_alive<'a>(
    mut stmt: &'a Stmt,
    prepared: impl Fn(&str) -> Option<&'a Stmt>,
) -> bool {
    use tidb_ast::{AdminStmt, BrieKind, DmlStmt, ExplainTarget, SessionStmt};
    loop {
        match stmt {
            Stmt::Session(session) => match &**session {
                SessionStmt::Execute { name, .. } => match prepared(name) {
                    Some(inner) => stmt = inner,
                    None => return false,
                },
                _ => break,
            },
            Stmt::Admin(admin) => match &**admin {
                AdminStmt::Trace(trace) => stmt = &trace.statement,
                AdminStmt::Explain(explain) => {
                    if !explain.analyze {
                        return true;
                    }
                    match &explain.target {
                        ExplainTarget::Statement(inner) => stmt = inner,
                        _ => break,
                    }
                }
                _ => break,
            },
            _ => break,
        }
    }
    match stmt {
        Stmt::Ddl(_) => false,
        Stmt::Admin(admin) => match &**admin {
            AdminStmt::Brie(brie) => !matches!(brie.kind, BrieKind::Backup | BrieKind::Restore),
            AdminStmt::AnalyzeTable(_) | AdminStmt::AnalyzeIncremental(_) => false,
            _ => true,
        },
        Stmt::Dml(dml) => !matches!(&**dml, DmlStmt::LoadData(_) | DmlStmt::ImportInto(_)),
        Stmt::Session(session) => !matches!(
            &**session,
            SessionStmt::Commit(_) | SessionStmt::Rollback { .. }
        ),
        Stmt::Query(_) => true,
    }
}

#[cfg(test)]
mod connection_alive_tests {
    use super::should_install_connection_alive;

    fn installs(sql: &str) -> bool {
        let stmt = tidb_parser::parse(sql).expect("parses");
        should_install_connection_alive(&stmt, |_| None)
    }

    /// Go `shouldInstallConnectionAlive`'s cases (`pkg/server/conn.go`).
    #[test]
    fn the_probe_follows_gos_statement_list() {
        for sql in [
            "SELECT 1",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "DELETE FROM t",
            "EXPLAIN SELECT 1",
            "EXPLAIN ANALYZE SELECT 1",
            "TRACE SELECT 1",
            "BEGIN",
            "SET @a = 1",
        ] {
            assert!(installs(sql), "{sql} runs with the probe");
        }
        for sql in [
            "CREATE TABLE t (a INT)",
            "ALTER TABLE t ADD COLUMN b INT",
            "ANALYZE TABLE t",
            "LOAD DATA LOCAL INFILE 'x' INTO TABLE t",
            "COMMIT",
            "ROLLBACK",
            "EXPLAIN ANALYZE ALTER TABLE t ADD COLUMN b INT",
            "TRACE CREATE TABLE t (a INT)",
            // An EXECUTE whose prepared statement is unknown is not probed.
            "EXECUTE s",
        ] {
            assert!(!installs(sql), "{sql} runs without the probe");
        }
    }
}
