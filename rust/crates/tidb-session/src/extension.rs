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

//! Go `server/extension.go`: the statement event's reads of session variables and StmtCtx.
use crate::Session;
use tidb_ast::{SessionStmt, Stmt};
use tidb_datatype::Datum;
use tidb_executor::exec_stmt_observation::ExecStmtObservation as _;
use tidb_extension::{ConnEventInfo, SessionExtensions, StmtEventInfo, StmtEventTp};
use tidb_parser::auth::{RoleIdentity, UserIdentity};
use tidb_stmtsummary::statement_summary::TableEntry;
use tidb_variable::connection_info::ConnectionInfo;

/// Go `StmtCtx` fields retained until `onExtensionStmtEnd` (after record-set Close).
#[derive(Clone)]
pub(crate) struct StatementFacts {
    original_text: String,
    normalized: String,
    digest: tidb_parser::Digest,
    tables: Vec<TableEntry>,
    params: Vec<Datum>,
    affected_rows: u64,
}
impl StatementFacts {
    /// Go `stmtEventInfo`'s `StmtCtx` reads.
    pub(crate) fn from_observation(
        observation: &tidb_executor::exec_stmt_observation::StatementObservation,
        params: &[Datum],
        affected_rows: u64,
    ) -> Self {
        Self {
            original_text: observation.text.clone(),
            normalized: observation.normalized.clone(),
            digest: observation.digest.clone(),
            tables: observation
                .phases
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .tables
                .clone(),
            params: params.to_vec(),
            affected_rows,
        }
    }
}
/// Go `stmtEventInfo`: owned copies of fields Go keeps in the live statement context.
struct StatementEventInfo {
    user: Option<UserIdentity>,
    roles: Vec<RoleIdentity>,
    database: String,
    connection_info: Option<std::sync::Arc<ConnectionInfo>>,
    alias: String,
    node: Option<Stmt>,
    execute: Option<Stmt>,
    prepared: Option<Stmt>,
    original_text: String,
    normalized: String,
    digest: Option<tidb_parser::Digest>,
    params: Vec<Datum>,
    tables: Vec<TableEntry>,
    affected_rows: u64,
    error: Option<String>,
}
impl StmtEventInfo for StatementEventInfo {
    fn user(&self) -> Option<&UserIdentity> {
        self.user.as_ref()
    }
    fn active_roles(&self) -> &[RoleIdentity] {
        &self.roles
    }
    fn current_db(&self) -> &str {
        &self.database
    }
    fn connection_info(&self) -> Option<&ConnectionInfo> {
        self.connection_info.as_deref()
    }
    fn session_alias(&self) -> &str {
        &self.alias
    }
    fn stmt_node(&self) -> Option<&Stmt> {
        self.node.as_ref()
    }
    fn execute_stmt_node(&self) -> Option<&Stmt> {
        self.execute.as_ref()
    }
    fn execute_prepared_stmt(&self) -> Option<&Stmt> {
        self.prepared.as_ref()
    }
    fn prepared_params(&self) -> &[Datum] {
        &self.params
    }
    fn original_text(&self) -> &str {
        &self.original_text
    }
    fn sql_digest(&self) -> (&str, Option<&tidb_parser::Digest>) {
        (&self.normalized, self.digest.as_ref())
    }
    fn affected_rows(&self) -> u64 {
        self.affected_rows
    }
    fn related_tables(&self) -> &[TableEntry] {
        &self.tables
    }
    fn get_error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}
impl Session {
    /// Go `session.Close` before replacing a connection's session.
    pub fn close_for_reset(&mut self) {
        self.node_conn().close();
        self.process.take();
    }
    /// Go `UserPrivileges.GetAuthWithoutVerification`.
    pub fn auth_without_verification(&mut self) {
        self.auth_plugin_verification = None;
    }

    /// Go `session.SetExtensions` and `SessionVars.ConnectionInfo`.
    pub fn set_extensions(&mut self, extensions: SessionExtensions, info: ConnectionInfo) {
        self.session_extensions = extensions;
        self.vars.connection_info = Some(std::sync::Arc::new(info));
    }
    /// Go `stmtEventInfo.User`.
    pub fn extension_user(&self) -> Option<UserIdentity> {
        let login = self.vars.login_user.as_deref()?;
        let (username, hostname) = login.rsplit_once('@').unwrap_or((login, ""));
        let (auth_username, auth_hostname) = self.current_identity().unwrap_or_default();
        Some(UserIdentity {
            username: username.into(),
            hostname: hostname.into(),
            auth_username: auth_username.into(),
            auth_hostname: auth_hostname.into(),
            auth_plugin: self.vars.auth_plugin.clone(),
            current_user: false,
        })
    }
    /// Go `clientConn.onExtensionConnEvent`'s session fields.
    pub fn extension_connection_event_info(&self) -> Option<ConnEventInfo> {
        Some(ConnEventInfo {
            connection_info: self.vars.connection_info.as_deref()?.clone(),
            session_alias: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SESSION_ALIAS)
                .unwrap_or_default(),
            active_roles: self
                .active_roles()
                .iter()
                .map(|(user, host)| RoleIdentity {
                    username: user.clone(),
                    hostname: host.clone(),
                })
                .collect(),
            error: None,
        })
    }
    /// Go `onExtensionStmtEnd`, `onExtensionSQLParseFailed`, `onExtensionBinaryExecuteEnd`.
    pub fn on_extension_stmt_end(
        &mut self,
        stmt_ctx_valid: bool,
        sql: &str,
        node: Option<&Stmt>,
        binary: Option<(
            &crate::domain::session_api::PreparedGeneral,
            &[tidb_param::PreparedValue],
        )>,
        error: Option<&str>,
    ) {
        if !self.session_extensions.has_stmt_event_listeners() {
            return;
        }
        let facts = self.extension_statement.take().or_else(|| {
            self.statement_observation.as_ref().map(|obs| {
                StatementFacts::from_observation(
                    obs,
                    self.prepared_params.as_deref().unwrap_or_default(),
                    self.affected_rows(),
                )
            })
        });
        let facts = facts.filter(|_| stmt_ctx_valid);
        let execute = if binary.is_some() {
            Some(Stmt::Session(tidb_ast::NodeBox::new(
                SessionStmt::Execute {
                    name: String::new(),
                    using: Vec::new(),
                },
            )))
        } else {
            node.filter(|stmt| matches!(stmt, Stmt::Session(s) if matches!(s.as_ref(), SessionStmt::Execute { .. }))).cloned()
        };
        let text_prepared = execute.as_ref().and_then(|stmt| match stmt {
            Stmt::Session(s) => match s.as_ref() {
                SessionStmt::Execute { name, .. } => self.prepared_statements.get(name),
                _ => None,
            },
            _ => None,
        });
        let prepared = binary
            .and_then(|(p, _)| p.template().cloned())
            .or_else(|| text_prepared.map(|p| p.statement.clone()));
        let parse_failed = node.is_none() && binary.is_none();
        let original_text = if parse_failed {
            sql.to_owned()
        } else {
            facts
                .as_ref()
                .map(|f| f.original_text.clone())
                .or_else(|| binary.map(|(p, _)| p.sql().to_owned()))
                .or_else(|| text_prepared.map(|p| p.original_sql.clone()))
                .unwrap_or_else(|| sql.to_owned())
        };
        let (normalized, digest) = if parse_failed {
            (sql.to_owned(), None)
        } else if let Some(facts) = &facts {
            (facts.normalized.clone(), Some(facts.digest.clone()))
        } else {
            let (normalized, digest) = tidb_parser::normalize_digest(&original_text);
            (normalized, Some(digest))
        };
        let params = if let Some((_, values)) = binary {
            crate::domain::pipeline_session::prepared_parameters(values)
        } else {
            facts
                .as_ref()
                .map(|f| f.params.clone())
                .unwrap_or_else(|| self.prepared_params.as_deref().unwrap_or_default().to_vec())
        };
        let node = execute.clone().or_else(|| node.cloned());
        let mut tables = if !parse_failed && error.is_none() {
            facts.as_ref().map(|f| f.tables.clone()).unwrap_or_default()
        } else {
            Vec::new()
        };
        if let Some(Stmt::Session(s)) = &node {
            if let SessionStmt::Use(db) = s.as_ref() {
                tables = vec![TableEntry {
                    db: db.clone(),
                    table: String::new(),
                }];
            }
        }
        if !parse_failed && error.is_some() {
            if let Some(stmt) = prepared.as_ref().or(node.as_ref()) {
                tables = tidb_bindinfo::binding::collect_table_names(stmt)
                    .into_iter()
                    .map(|(db, table)| TableEntry {
                        db: if db.is_empty() {
                            self.vars.current_db.clone()
                        } else {
                            db
                        },
                        table,
                    })
                    .collect();
                if let Stmt::Session(s) = stmt {
                    if let SessionStmt::Use(db) = s.as_ref() {
                        tables = vec![TableEntry {
                            db: db.clone(),
                            table: String::new(),
                        }];
                    }
                }
            }
        }
        // Go `clientConn.preparedStmt2String` and `errors.Annotate` at binary execution.
        let error = error.map(|error| {
            if binary.is_none() {
                return error.to_owned();
            }
            let mut text = tidb_parser::normalize(sql, self.redact_mode());
            if self.redact_log_mode() != "ON" && !params.is_empty() {
                text.push_str(&tidb_redact::string(
                    &self.redact_log_mode(),
                    &format!(
                        " [arguments: {}]",
                        tidb_datatype::datums_to_str_no_err_smart(&params)
                    ),
                ));
            }
            format!("{text}: {error}")
        });
        let info = StatementEventInfo {
            user: self.extension_user(),
            roles: self
                .active_roles()
                .iter()
                .map(|(u, h)| RoleIdentity {
                    username: u.clone(),
                    hostname: h.clone(),
                })
                .collect(),
            database: self.vars.current_db.clone(),
            connection_info: self.vars.connection_info.clone(),
            alias: self
                .vars
                .get_system(tidb_vardef::tidb_vars::TIDB_SESSION_ALIAS)
                .unwrap_or_default(),
            node,
            execute,
            prepared,
            original_text,
            normalized,
            digest,
            params,
            tables,
            affected_rows: if error.is_some() || parse_failed || !stmt_ctx_valid {
                0
            } else {
                facts
                    .as_ref()
                    .map_or(self.affected_rows(), |f| f.affected_rows)
            },
            error: error.clone(),
        };
        self.session_extensions.on_stmt_event(
            if error.is_some() {
                StmtEventTp::Error
            } else {
                StmtEventTp::Success
            },
            &info,
        );
    }
}
