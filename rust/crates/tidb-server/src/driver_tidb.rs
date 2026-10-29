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

//! Go `pkg/server/driver_tidb.go`: `TiDBDriver` over the domain and
//! `TiDBContext` over the session, each step answered by the session.

use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use tidb_ast::Stmt;
use tidb_executor::compiler::prepared::PreparedExec as _;
use tidb_executor::ddl_exec::map_error;
use tidb_session::domain::cluster_session_node::DomainSessions;
use tidb_domain_core::domain::Domain;
use tidb_session::domain::session_api::{ActiveQueryCancellation, GeneralExecuteOutcome, PreparedGeneral, QueryResult, SessionContext};
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_session::domain::wire_status::WireStatus;
use tidb_session::Session;
use tidb_globalconn::Allocator;

use crate::driver::{should_install_connection_alive, QuerySession, QuerySessionFactory};

impl QuerySessionFactory for Domain {
    type Session = Session;

    fn open_session(&self, context: SessionContext) -> Result<Session, SqlQueryError> {
        DomainSessions::open_session(self, context)
    }

    fn install_tls_manager(
        &self,
        manager: Arc<dyn tidb_sessmgr::TlsManager>,
        variables: Vec<(&'static str, String)>,
    ) {
        Domain::install_tls_manager(self, manager, variables);
    }

    fn server_identity(&self) -> Option<Arc<tidb_domain::server_id::ServerIdAuthority>> {
        Domain::server_identity(self)
    }

    fn connection_ids(&self) -> Option<Arc<dyn Allocator + Send + Sync>> {
        Some(self.connection_id_allocator())
    }

    fn session_manager(&self) -> Option<Arc<dyn tidb_sessmgr::processinfo::SessionManager>> {
        Some(Domain::session_manager(self))
    }

    fn expensive_query_handle(&self) -> Option<Arc<tidb_expensivequery::Handle>> {
        Some(Domain::expensive_query_handle(self))
    }
}

/// Go `TiDBContext`: the connection's session, as the protocol layer drives it.
impl QuerySession for Session {
    fn migration_session(&mut self) -> Option<&mut Session> {
        Some(self)
    }

    fn record_parse_failure(&mut self, error: &SqlQueryError) {
        self.record_parse_failure_coded(error.code, error.message.clone(), error.identity);
    }

    fn metrics_resource_group(&self) -> &str {
        self.current_resource_group()
    }
    fn metrics_statement_resource_group(&self) -> &str {
        self.active_resource_group()
    }

    /// Go `TiDBContext.Parse`.
    fn parse_statement(&mut self, sql: &str) -> Result<Stmt, SqlQueryError> {
        self.parse_at_statement_boundary(sql).map_err(map_error)
    }

    fn affected_rows(&self) -> u64 {
        Session::affected_rows(self)
    }

    fn last_insert_id(&self) -> u64 {
        self.statement_last_insert_id()
    }

    fn take_local_infile_request(&mut self) -> Option<String> {
        self.local_infile_request()
    }

    fn finish_execute_stmt(&mut self, cost: Duration) {
        self.finish_txn_write_throughput(cost);
    }

    /// The session's own `tidb_slow_log_threshold`, in milliseconds.
    fn slow_log_threshold(&self) -> Option<Duration> {
        let raw = self.vars().get_system("tidb_slow_log_threshold").ok()?;
        let millis: u64 = raw.parse().ok()?;
        Some(Duration::from_millis(millis))
    }

    fn set_connection_attributes(
        &mut self,
        attributes: std::collections::BTreeMap<String, String>,
    ) {
        Session::set_connection_attributes(self, attributes);
    }

    fn query_cancellation(&self) -> Option<Arc<dyn ActiveQueryCancellation>> {
        Some(Arc::new(self.begin_query_cancellation()))
    }

    fn sql_killer(&self) -> Option<Arc<tidb_sqlkiller::SqlKiller>> {
        Some(Session::sql_killer(self))
    }

    fn should_install_connection_alive(&self, stmt: &Stmt) -> bool {
        let session = self;
        should_install_connection_alive(stmt, |name| {
            session.prepared_statement_node(name)
        })
    }

    fn wait_timeout(&self) -> Duration {
        Session::wait_timeout(self)
    }

    /// This session's own `@@max_allowed_packet`; see the trait's own doc for
    /// why Go rebinds the packet reader from it on every packet.
    fn max_allowed_packet(&self) -> Option<usize> {
        Some(Session::max_allowed_packet(self) as usize)
    }

    fn try_consume_long_data(&mut self, bytes: i64) -> bool {
        Session::try_consume_long_data(self, bytes)
    }

    fn release_long_data(&mut self, bytes: i64) {
        Session::release_long_data(self, bytes);
    }

    fn connection_id(&self) -> u64 {
        Session::connection_id(self)
    }

    fn split_statements(
        &mut self,
        sql: &str,
        client_multi_statements: bool,
    ) -> Result<Vec<String>, SqlQueryError> {
        Session::split_statements(self, sql, client_multi_statements).map_err(map_error)
    }

    fn flush_multi_statement_warning(&mut self) {
        Session::flush_multi_statement_warning(self);
    }

    /// The live status word Go reads with `cc.ctx.Status()` before every
    /// OK/EOF packet. The driver session owns the transaction state this tier
    /// acts on, so the wire word and the tier's behaviour cannot disagree.
    fn wire_status(&self) -> WireStatus {
        WireStatus::of_session(self)
    }

    /// The OK/EOF packet's warning count, read off the same buffer
    /// `SHOW WARNINGS` reports (Go `ctx.WarningCount()`).
    fn warning_count(&self) -> u16 {
        self.wire_warning_count()
    }

    fn statement_info(&self) -> Vec<u8> {
        self.statement_message().as_bytes().to_vec()
    }

    fn warning_codes(&self) -> Vec<u16> {
        self.warnings()
            .iter()
            .map(|warning| warning.code())
            .collect()
    }

    /// Go `clientConn.initResultEncoder`'s read: this session's
    /// `@@character_set_results`.
    fn result_charset(&self) -> Cow<'_, str> {
        Session::result_charset(self)
    }

    fn input_charset(&self) -> Cow<'_, str> {
        Session::input_charset(self)
    }

    /// Go leaves `CurrentDB` empty when the handshake carried no schema; see
    /// the trait's own doc.
    fn deselect_database(&mut self) {
        Session::deselect_database(self);
    }

    fn set_collation(&mut self, collation_id: u8) -> Result<(), SqlQueryError> {
        Session::set_collation(self, i32::from(collation_id)).map_err(SqlQueryError::unknown)
    }

    fn close_prepared(&mut self, statement: &PreparedGeneral) {
        if let Some(prepared) = statement.prepared_ast() {
            Session::close_prepared(self, prepared);
        }
    }
    fn execute_stmt<'a>(
        &'a mut self,
        sql: &str,
        stmt: &Stmt,
    ) -> Result<Option<QueryResult<'a>>, SqlQueryError> {
        self.node_conn().execute_stmt(sql, stmt)
    }
    fn finish_local_infile(&mut self, data: &[u8]) -> Result<(), SqlQueryError> {
        self.node_conn().finish_local_infile(data)
    }
    /// The catalog is refreshed first so a schema another node created since
    /// this connection opened is selectable, exactly as it is for a statement.
    fn select_database(&mut self, name: &str) -> Result<(), SqlQueryError> {
        self.node_conn().select_database(name)
    }
    fn prepare_general(&mut self, sql: &str) -> Result<PreparedGeneral, SqlQueryError> {
        self.node_conn().prepare_general(sql)
    }
    fn execute_general<'a>(
        &'a mut self,
        statement: &PreparedGeneral,
        values: &[tidb_param::PreparedValue],
    ) -> Result<GeneralExecuteOutcome<'a>, SqlQueryError> {
        self.node_conn().execute_general(statement, values)
    }
}
