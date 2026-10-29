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

//! Go `pkg/server/driver.go`: the interfaces the MySQL protocol layer drives
//! -- `IDriver` ([`QuerySessionFactory`]) opens one `QueryCtx`
//! ([`QuerySession`]) per authenticated connection. TiDB's own
//! implementations over the session are in `driver_tidb`.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use tidb_ast::Stmt;
use tidb_session::domain::session_api::{ActiveQueryCancellation, GeneralExecuteOutcome, PreparedGeneral, QueryResult, SessionContext};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_session::domain::wire_status::WireStatus;
use tidb_globalconn::Allocator;

/// Query capability retained entirely inside one fixed worker thread.
pub trait QuerySession {
    /// Shared SQL state used by connection-owned migration handlers.
    fn migration_session(&mut self) -> Option<&mut tidb_session::Session> { None }

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
    fn sql_killer(&self) -> Option<Arc<tidb_sqlkiller::SqlKiller>> {
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
        Err(SqlQueryError::from(
            tidb_error::MysqlError::from_terror(
                &tidb_error::catalog::infoschema::ERR_DATABASE_NOT_EXISTS
                    .generate_with_stack_by_args(&[name.into()]),
            ),
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

/// Process-owned factory invoked only after authentication, inside a worker.
pub trait QuerySessionFactory: Send + Sync + 'static {
    /// Worker-local session type. It deliberately has no `Send` bound.
    type Session: QuerySession;

    /// Opens a session from already-running process authorities.
    fn open_session(&self, context: SessionContext) -> Result<Self::Session, SqlQueryError>;

    /// Connect SQL and new connections to the same process TLS owner.
    fn install_tls_manager(
        &self,
        _manager: Arc<dyn tidb_sessmgr::TlsManager>,
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
    fn session_manager(&self) -> Option<Arc<dyn tidb_memoryusagealarm::SessionManager>> {
        None
    }

    /// Go `Domain.ExpensiveQueryHandle`, which the server's main runs over
    /// its session manager; `None` without a domain.
    fn expensive_query_handle(&self) -> Option<Arc<tidb_expensivequery::Handle>> {
        None
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
