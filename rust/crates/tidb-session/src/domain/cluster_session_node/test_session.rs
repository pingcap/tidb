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

//! Go `session.CreateSession4Test` over this node, and the one-statement
//! command a [`Session`] runs through it.
//!
//! Go's testkit opens its sessions on a booted domain and runs every
//! statement through `session.ExecuteStmt`, so DDL takes the job path and a
//! write commits through the store. [`crate::domain::mock_store`] holds that
//! domain; this file is the node's half: the session it opens and the
//! connection-order statement it runs.

use std::sync::Arc;

use tidb_ast::Stmt;

use super::{ClusterServerSession, ClusterSessionFactory, InternalBindingWriter};
use crate::domain::resultset_source::ResultSetSource;
use crate::domain::session_api::{QuerySession, SqlQueryError};
use crate::{Session, StmtOutput};

impl ClusterSessionFactory {
    /// Go `session.CreateSession4Test`: a session over this node that no
    /// client logged in. Go's testkit never calls `Auth`, so
    /// `SessionVars.User` stays nil and every privilege request passes; the
    /// session still enters the process list, as testkit registers it with
    /// its mock session manager.
    pub fn create_session_4_test(
        &self,
        connection_id: u64,
    ) -> Result<ClusterServerSession, SqlQueryError> {
        let mut opened = self.open_storage_session(connection_id)?;
        let session = &mut opened.store.session;
        session.set_restricted_sql(false);
        session.set_connection_id(connection_id);
        let guard = self.processes.register(
            connection_id,
            String::new(),
            String::new(),
            session.current_database().to_owned(),
            None,
        );
        guard.transaction_observer().use_external_owner();
        session.attach_process(connection_id, guard);
        session.attach_privileges(self.privileges.clone());
        if let Some(bindings) = &self.bindings {
            session.set_global_binding_cache(bindings.cache());
            session.set_global_binding_writer(Arc::new(InternalBindingWriter {
                factory: self.binding_session_factory.clone(),
                bindings: Arc::clone(bindings),
                connection_id,
            }));
        }
        Ok(opened)
    }
}

impl ClusterServerSession {
    /// The SQL session this connection drives.
    pub(crate) fn driver_session_mut(&mut self) -> &mut Session {
        &mut self.store.session
    }

    /// Runs `body` over the driver session with a fresh committed snapshot
    /// bound to its tables, over the current schema: what a test that looks
    /// at the stored rows directly reads (Go's `tk.Session().GetStore()`
    /// snapshot at the current timestamp).
    pub(crate) fn with_read_snapshot<R>(
        &mut self,
        body: impl FnOnce(&mut Session) -> R,
    ) -> Result<R, tidb_executor::DriverError> {
        self.rebuild_catalog_if_stale();
        let snapshot = self
            .transactions
            .open_snapshot("default")
            .map_err(tidb_executor::DriverError::unsupported)?;
        let previous = crate::statement_lifecycle::bind_snapshot(
            &self.store.slot,
            &self.store.session,
            snapshot,
        );
        let result = body(&mut self.store.session);
        let _ = crate::statement_lifecycle::finish_snapshot(&self.store.slot);
        if let Some(previous) = previous {
            crate::statement_lifecycle::bind_snapshot(
                &self.store.slot,
                &self.store.session,
                previous,
            );
        }
        Ok(result)
    }

    /// Go `clientConn.handleQuery` for one statement, read back as the rows a
    /// [`Session::run`] caller expects: parse once, then transaction
    /// control, then a write answered by an OK packet, and only then a
    /// result-set query, drained here.
    pub(crate) fn run_statement_output(
        &mut self,
        sql: &str,
    ) -> Result<StmtOutput, tidb_executor::DriverError> {
        let parsed = match self.parse_statement(sql) {
            Ok(parsed) => parsed,
            Err(error) => {
                self.record_parse_failure(error.code, error.message.clone());
                return Err(driver_error(error));
            }
        };
        let control = match &parsed {
            Some(stmt) => self.control_transaction_parsed(sql, stmt),
            None => self.control_transaction(sql),
        };
        if control.map_err(driver_error)?.is_some() {
            return Ok(StmtOutput::Affected(0));
        }
        let written = match &parsed {
            Some(stmt) => self.execute_write_parsed(sql, stmt),
            None => self.execute_write(sql),
        };
        if let Err(error) = &written {
            crate::domain::session_api::record_text_write_failure(self, parsed.as_ref(), error);
        }
        if let Some(outcome) = written.map_err(driver_error)? {
            return Ok(match &parsed {
                Some(Stmt::Ddl(_)) => StmtOutput::Done(true),
                _ => StmtOutput::Affected(outcome.affected_rows),
            });
        }
        let mut result = match &parsed {
            Some(stmt) => self.execute_parsed(sql, stmt),
            None => self.execute(sql),
        }
        .map_err(driver_error)?;
        drain_result(result.source()).map_err(tidb_executor::DriverError::Mysql)
    }
}

/// The rows and column types of an opened result, closed afterwards as the
/// connection's writer closes it.
fn drain_result(source: &mut dyn ResultSetSource) -> Result<StmtOutput, tidb_executor::MysqlError> {
    let infos = source.columns()?;
    let native = source.field_types().to_vec();
    let columns = infos
        .iter()
        .enumerate()
        .map(|(index, info)| {
            let field_type = native
                .get(index)
                .filter(|_| native.len() == infos.len())
                .cloned()
                .unwrap_or_else(|| column_field_type(info));
            (info.name.clone(), field_type)
        })
        .collect();
    let mut rows = Vec::new();
    loop {
        let batch = source.next_batch(1024)?;
        if batch.is_empty() {
            break;
        }
        rows.extend(batch);
    }
    source.finish()?;
    source.close()?;
    Ok(StmtOutput::Rows { columns, rows })
}

/// The field type a column definition packet describes.
fn column_field_type(info: &tidb_protocol::ColumnInfo) -> tidb_datatype::FieldType {
    tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::from_mysql_type(
        info.type_code,
    ))
    .with_flags(u32::from(info.flag))
    .with_flen(i64::from(info.column_length))
    .with_decimal(i64::from(info.decimal))
}

fn driver_error(error: SqlQueryError) -> tidb_executor::DriverError {
    if let Some(cause) = error.driver_cause() {
        return cause.clone();
    }
    let mut mapped = tidb_executor::MysqlError::from_parts(error.code, error.state, error.message);
    if let Some(identity) = error.identity {
        mapped = mapped.with_identity(identity);
    }
    tidb_executor::DriverError::Mysql(mapped)
}
