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



use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tidb_ast::Stmt;
use tidb_executor::adapter::ResultFieldMetadata;

use crate::domain::configured_user_store::AuthenticatedIdentity;
use crate::domain::resultset_source::ResultSetSource;
use tidb_variable::wire_status::WireStatus;
use tidb_sessmgr::ProcessKillTarget;



/// A backend query that can be interrupted during forced connection drain.
pub trait ActiveQueryCancellation: Send + Sync {
    /// Fires the query's canonical cancellation carrier.
    fn cancel(&self);

    /// Fires it with a watchdog's own kill signal (Go server `killQuery`).
    fn cancel_with(&self, signal: tidb_sqlkiller::KillSignal) {
        let _ = signal;
        self.cancel();
    }
}

impl ActiveQueryCancellation for tidb_sqlkiller::StatementCancellation {
    fn cancel(&self) {
        tidb_sqlkiller::StatementCancellation::cancel(self);
    }

    fn cancel_with(&self, signal: tidb_sqlkiller::KillSignal) {
        tidb_sqlkiller::StatementCancellation::cancel_with(self, signal);
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
        self.cancel_current_query_with(tidb_sqlkiller::KillSignal::QueryInterrupted);
    }

    /// [`Self::cancel_current_query`] with the signal the statement observes.
    fn cancel_current_query_with(&self, signal: tidb_sqlkiller::KillSignal) {
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
/// server's wake -- its socket's shutdown -- the second, which is how Go's
/// `killConn` ends a session it is not currently executing anything for.
#[derive(Clone, Default)]
pub struct ConnectionClose {
    closed: Arc<AtomicBool>,
    wake: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for ConnectionClose {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConnectionClose")
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl ConnectionClose {
    /// Binds the handle to the server's wake for the connection: Go's
    /// `killConn` closing `cc.bufReadConn`.
    pub fn with_wake(wake: impl Fn() + Send + Sync + 'static) -> Self {
        Self {
            closed: Arc::new(AtomicBool::new(false)),
            wake: Some(Arc::new(wake)),
        }
    }

    /// Marks the connection closed and wakes it if it is waiting for its next
    /// command. The current command still finishes; the loop exits after it.
    pub fn request(&self) {
        self.closed.store(true, Ordering::Release);
        if let Some(wake) = &self.wake {
            wake();
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

    fn cancel_query_with(&self, signal: tidb_sqlkiller::KillSignal) {
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
    pub current_tso: Option<tidb_exprctx::CurrentTso>,
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
    result_fields: Vec<ResultFieldMetadata>,
    /// The session owns the parse, cache candidate and execution readiness.
    prepared_ast: Option<crate::PreparedAst>,
}

impl PreparedGeneral {
    /// Creates one from its statement text and the metadata a PREPARE reports.
    #[must_use]
    pub fn new(sql: String, parameter_count: usize, result_fields: Vec<ResultFieldMetadata>) -> Self {
        let digest = tidb_parser::normalize_digest(&sql).1.to_string();
        Self {
            sql,
            digest,
            parameter_count,
            result_fields,
            prepared_ast: None,
        }
    }

    /// Retains the session's prepared definition and its cache lifecycle.
    #[must_use]
    pub fn with_prepared_ast(
        prepared_ast: crate::PreparedAst,
        result_fields: Vec<ResultFieldMetadata>,
    ) -> Self {
        Self {
            sql: prepared_ast.sql().to_owned(),
            digest: tidb_parser::normalize_digest(prepared_ast.sql())
                .1
                .to_string(),
            parameter_count: prepared_ast.parameter_count(),
            result_fields,
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
    pub fn result_fields(&self) -> &[ResultFieldMetadata] {
        &self.result_fields
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

use tidb_executor::ddl_exec::WriteOutcome;

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
        statement: tidb_sessmgr::ProcessStatementGuard,
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
    statement: Option<tidb_sessmgr::ProcessStatementGuard>,
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
    ) -> Result<(), tidb_terror::MysqlError> {
        self.inner.next_chunk(chunk)
    }
    fn field_types(&self) -> &[tidb_datatype::FieldType] {
        self.inner.field_types()
    }

    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
        self.inner.next_batch(max_rows)
    }

    fn fields(&mut self) -> Result<Vec<ResultFieldMetadata>, tidb_terror::MysqlError> {
        self.inner.fields()
    }

    fn finish(&mut self) -> Result<(), tidb_terror::MysqlError> {
        let result = self.inner.finish();
        self.statement.take();
        result
    }

    fn close(&mut self) -> Result<(), tidb_terror::MysqlError> {
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
    ) -> Result<(), tidb_terror::MysqlError> {
        self.inner.next_chunk(chunk)
    }
    fn field_types(&self) -> &[tidb_datatype::FieldType] {
        self.inner.field_types()
    }

    fn next_batch(
        &mut self,
        max_rows: usize,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, tidb_terror::MysqlError> {
        self.inner.next_batch(max_rows)
    }

    fn fields(&mut self) -> Result<Vec<ResultFieldMetadata>, tidb_terror::MysqlError> {
        self.inner.fields()
    }

    fn finish(&mut self) -> Result<(), tidb_terror::MysqlError> {
        self.inner.finish()
    }

    fn close(&mut self) -> Result<(), tidb_terror::MysqlError> {
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
    pub peer_addr: crate::domain::peer_address::PeerAddress,
    /// Identity established by ordinary password verification or validated
    /// process-wide skip-grant admission.
    pub identity: AuthenticatedIdentity,
    /// Whether this client negotiated `CLIENT_FOUND_ROWS`, making UPDATE and
    /// duplicate-key UPDATE report successfully matched unchanged rows.
    pub client_found_rows: bool,
    /// Go `cc.tlsConn.ConnectionState()`, which `OpenCtx` hands to the
    /// session's `SetTLSState`: `None` for a connection without TLS.
    pub tls_state: Option<tidb_variable::vars::TlsConnectionState>,
    /// Go `mysql.CompressionNone` / `CompressionZlib` / `CompressionZstd`:
    /// the codec the connection switches to once the handshake OK is out,
    /// which the session records with `SetCompressionAlgorithm`.
    pub compression_algorithm: u8,
    /// Go `resp.ZstdLevel`, which `openSessionAndDoAuth` records with
    /// `SetCompressionLevel`.
    pub compression_level: i32,
    /// Forced-drain carrier on which the session registers each active query.
    pub cancellation: ConnectionCancellation,
    /// Handle a `KILL` uses to end this connection.
    pub close: ConnectionClose,
}

