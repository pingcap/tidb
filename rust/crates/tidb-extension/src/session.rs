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

//! Go `pkg/extension/session.go`.

use crate::{AuthPlugin, Extensions};
use std::{collections::HashMap, sync::Arc};
use tidb_ast::Stmt;
use tidb_datatype::Datum;
use tidb_parser::auth::{RoleIdentity, UserIdentity};
use tidb_variable::connection_info::ConnectionInfo;

/// Go `ConnEventTp` and its constants, in Go's order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnEventTp {
    Connected,
    HandshakeAccepted,
    HandshakeRejected,
    Reset,
    Disconnected,
}
/// Go `ConnEventInfo`.
#[derive(Clone, Debug)]
pub struct ConnEventInfo {
    /// Go `ConnectionInfo`.
    pub connection_info: ConnectionInfo,
    /// Go `SessionAlias`.
    pub session_alias: String,
    /// Go `ActiveRoles`.
    pub active_roles: Vec<RoleIdentity>,
    /// Go `Error`.
    pub error: Option<String>,
}
/// Go `StmtEventTp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum StmtEventTp {
    Error,
    Success,
}

/// Go `StmtEventInfo`.
pub trait StmtEventInfo {
    /// Go `StmtEventInfo.User`.
    fn user(&self) -> Option<&UserIdentity>;
    /// Go `StmtEventInfo.ActiveRoles`.
    fn active_roles(&self) -> &[RoleIdentity];
    /// Go `StmtEventInfo.CurrentDB`.
    fn current_db(&self) -> &str;
    /// Go `StmtEventInfo.ConnectionInfo`.
    fn connection_info(&self) -> Option<&ConnectionInfo>;
    /// Go `StmtEventInfo.SessionAlias`.
    fn session_alias(&self) -> &str;
    /// Go `StmtEventInfo.StmtNode`.
    fn stmt_node(&self) -> Option<&Stmt>;
    /// Go `StmtEventInfo.ExecuteStmtNode`.
    fn execute_stmt_node(&self) -> Option<&Stmt>;
    /// Go `StmtEventInfo.ExecutePreparedStmt`.
    fn execute_prepared_stmt(&self) -> Option<&Stmt>;
    /// Go `StmtEventInfo.PreparedParams`.
    fn prepared_params(&self) -> &[Datum];
    /// Go `StmtEventInfo.OriginalText`.
    fn original_text(&self) -> &str;
    /// Go `StmtEventInfo.SQLDigest`.
    fn sql_digest(&self) -> (&str, Option<&tidb_parser::Digest>);
    /// Go `StmtEventInfo.AffectedRows`.
    fn affected_rows(&self) -> u64;
    /// Go `StmtEventInfo.RelatedTables`.
    fn related_tables(&self) -> &[tidb_stmtsummary::statement_summary::TableEntry];
    /// Go `StmtEventInfo.GetError`.
    fn get_error(&self) -> Option<&str>;
}

pub type ConnectionEventFunc = Arc<dyn Fn(ConnEventTp, &ConnEventInfo) + Send + Sync>;
pub type StmtEventFunc = Arc<dyn Fn(StmtEventTp, &dyn StmtEventInfo) + Send + Sync>;
pub type SessionHandlerFactory = Arc<dyn Fn() -> Option<SessionHandler> + Send + Sync>;
/// Go `SessionHandler`: a factory constructs these callbacks for each connection.
#[derive(Clone, Default)]
pub struct SessionHandler {
    /// Go `OnConnectionEvent`.
    pub on_connection_event: Option<ConnectionEventFunc>,
    /// Go `OnStmtEvent`.
    pub on_stmt_event: Option<StmtEventFunc>,
}
/// Go `SessionExtensions`.
#[derive(Clone, Default)]
pub struct SessionExtensions {
    connection_event_funcs: Vec<ConnectionEventFunc>,
    stmt_event_funcs: Vec<StmtEventFunc>,
    auth_plugins: HashMap<String, AuthPlugin>,
}
impl SessionExtensions {
    /// Go `newSessionExtensions`.
    pub(crate) fn new(es: &Extensions) -> Self {
        let mut result = Self::default();
        for m in &es.manifests {
            if let Some(handler) = m
                .session_handler_factory
                .as_ref()
                .and_then(|factory| factory())
            {
                if let Some(f) = handler.on_connection_event {
                    result.connection_event_funcs.push(f);
                }
                if let Some(f) = handler.on_stmt_event {
                    result.stmt_event_funcs.push(f);
                }
            }
            if let Some(plugins) = &m.auth_plugins {
                result.auth_plugins = plugins
                    .iter()
                    .map(|p| (p.name.clone(), p.clone()))
                    .collect();
            }
        }
        result
    }
    /// Go `SessionExtensions.OnConnectionEvent`.
    pub fn on_connection_event(&self, tp: ConnEventTp, event: &ConnEventInfo) {
        for f in &self.connection_event_funcs {
            f(tp, event);
        }
    }
    /// Go `SessionExtensions.HasStmtEventListeners`.
    pub fn has_stmt_event_listeners(&self) -> bool {
        !self.stmt_event_funcs.is_empty()
    }
    /// Go `SessionExtensions.OnStmtEvent`.
    pub fn on_stmt_event(&self, tp: StmtEventTp, event: &dyn StmtEventInfo) {
        for f in &self.stmt_event_funcs {
            f(tp, event);
        }
    }
    /// Go `SessionExtensions.GetAuthPlugin`.
    pub fn get_auth_plugin(&self, name: &str) -> Option<&AuthPlugin> {
        self.auth_plugins.get(name)
    }
}
