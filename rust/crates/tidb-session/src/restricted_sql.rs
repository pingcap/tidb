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

//! Go `sqlexec.RestrictedSQLExecutor` as a session reaches it
//! (`sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)`).
//!
//! Go's `withRestrictedSQLExecutor` runs the statement on an internal session
//! borrowed from the domain's pool (`getInternalSession`), in that session's
//! own autocommit transaction: it reads committed data, never the issuing
//! session's open transaction or staged writes. A node installs an executor
//! with that shape; a session with no node (the in-process tier, whose own
//! catalog is the store) reads its own catalog.
//! With `ExecOptionUseCurSession`, `Session::execute_restricted_sql` borrows
//! the issuing session itself, as Go's `useCurrentSession` does, including
//! its transaction, variables and temporary tables.

use std::sync::{Arc, Mutex, PoisonError};

use tidb_context::SqlWarn;
use tidb_datatype::Datum;
use tidb_driver_error::DriverError;

use crate::Session;

use tidb_executor::memtable_reader::{exec_restricted_sql_over, RestrictedSql};
use tidb_sqlexec::RestrictedSqlExecutor;

/// The session a restricted statement is issued from, as Go's
/// `getInternalSession` reads and writes it (`s` beside the internal `se`):
/// the two optimizer variables the internal session copies from it, and the
/// internal session's warnings its `clean` appends to the issuer's statement.
/// An executor with no issuing session (the domain's own) keeps the default,
/// which copies nothing.
#[derive(Default)]
pub(crate) struct RestrictedSqlIssuer(Mutex<RestrictedSqlIssuerState>);

#[derive(Default)]
struct RestrictedSqlIssuerState {
    /// Go `SessionVars.OptimizerUseInvisibleIndexes` and
    /// `SkipMissingPartitionStats` of the issuer.
    inherited: Option<[String; 2]>,
    /// Go `SessionVars.InspectionTableCache` of the issuer.
    inspection_table_cache: Option<Arc<tidb_executor::memtable_reader::InspectionTableCache>>,
    warnings: Vec<SqlWarn>,
}

impl RestrictedSqlIssuer {
    fn state(&self) -> std::sync::MutexGuard<'_, RestrictedSqlIssuerState> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `se.sessionVars.OptimizerUseInvisibleIndexes =
    /// s.sessionVars.OptimizerUseInvisibleIndexes` and the same for
    /// `SkipMissingPartitionStats`. Go assigns the fields; the internal
    /// session here takes them as its session variables, which is where its
    /// planner reads them.
    pub(crate) fn inherit(&self, internal: &mut Session) -> Result<(), DriverError> {
        use tidb_vardef::tidb_vars::{TIDB_OPT_USE_INVISIBLE_INDEXES, TIDB_SKIP_MISSING_PARTITION_STATS};
        // `se.sessionVars.InspectionTableCache = s.sessionVars.InspectionTableCache`:
        // the internal session reads the issuer's inspection snapshots.
        if let Some(cache) = self.state().inspection_table_cache.clone() {
            internal.inspection_table_cache = cache;
        }
        let Some(values) = self.state().inherited.clone() else {
            return Ok(());
        };
        for (name, value) in [TIDB_OPT_USE_INVISIBLE_INDEXES, TIDB_SKIP_MISSING_PARTITION_STATS]
            .into_iter()
            .zip(values)
        {
            internal.set_session_sys_var(name, value)?;
        }
        Ok(())
    }

    /// `s.GetSessionVars().StmtCtx.AppendWarnings(warnings)` of
    /// `getInternalSession`'s `clean`.
    pub(crate) fn append_warnings(&self, warnings: &[SqlWarn]) {
        self.state().warnings.extend_from_slice(warnings);
    }
}

impl Session {
    /// This session as the issuer of a restricted statement, carrying the
    /// variables an internal session copies from it now.
    pub(crate) fn restricted_sql_issuer(&self) -> Arc<RestrictedSqlIssuer> {
        use tidb_vardef::tidb_vars::{TIDB_OPT_USE_INVISIBLE_INDEXES, TIDB_SKIP_MISSING_PARTITION_STATS};
        let mut state = self.restricted_sql_issuer.state();
        state.inherited =
            Some([TIDB_OPT_USE_INVISIBLE_INDEXES, TIDB_SKIP_MISSING_PARTITION_STATS]
                .map(|name| self.vars.get_system(name).unwrap_or_default()));
        state.inspection_table_cache = Some(Arc::clone(&self.inspection_table_cache));
        drop(state);
        Arc::clone(&self.restricted_sql_issuer)
    }

    /// The warnings internal sessions handed back since the last call join
    /// the statement's.
    pub(crate) fn append_restricted_sql_warnings(&mut self) {
        let warnings = std::mem::take(&mut self.restricted_sql_issuer.state().warnings);
        for warning in warnings {
            self.append_sql_warning(warning);
        }
    }

    /// Go `GetRestrictedSQLExecutor`'s backing: what the node installs.
    pub fn set_restricted_sql_executor(&mut self, executor: Arc<dyn RestrictedSqlExecutor>) {
        self.restricted_sql_executor = Some(executor);
    }

    /// The installed executor, which retriever snapshots carry.
    pub(crate) fn restricted_sql_executor(&self) -> RestrictedSql {
        self.restricted_sql_issuer();
        self.restricted_sql_executor.clone()
    }

    /// Go `sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, nil, sql)`
    /// for a caller outside this crate (the status port's metric profile).
    pub fn exec_restricted_sql_rows(&mut self, source: &str, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
        self.exec_restricted_sql(source, sql)
    }

    /// Go `ExecRestrictedSQL(ctx, nil, sql)` under `ctx`'s request source.
    pub(crate) fn exec_restricted_sql(
        &mut self,
        source: &str,
        sql: &str,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let result = exec_restricted_sql_over(self.restricted_sql_executor().as_deref(), source, sql, &[]);
        self.append_restricted_sql_warnings();
        result
    }
}

impl Session {
    /// Go `BaseExecutor.GetSysSession` for `ADMIN CHECK`'s
    /// `FastCheckTableExec`, whose SQL runs under `kv.InternalTxnAdmin`.
    pub(crate) fn get_sys_session(
        &self,
    ) -> Result<Box<dyn tidb_sqlexec::SqlExecutor>, DriverError> {
        use crate::domain::cluster_session_node::DomainSessions as _;
        let domain = self
            .domain_services()
            .and_then(|services| services.domain.upgrade())
            .ok_or_else(|| DriverError::unsupported("the session has no domain to take a system session from"))?;
        let session = domain
            .open_system_session()
            .map_err(crate::domain::cluster_session_node::sql_query_error_to_driver)?;
        Ok(Box::new(crate::domain::cluster_session_node::SystemSqlExecutor::new(session)))
    }
}

/// Go `session.GetRestrictedSQLExecutor`: a borrow of the issuing session.
/// Keeping this loan scoped lets nested executors use the session itself
/// without a self pointer or a second session/transaction.
pub struct SessionRestrictedSql<'s>(Mutex<&'s mut Session>);

impl Session {
    /// Go `session.GetRestrictedSQLExecutor`.
    pub fn get_restricted_sql_executor(&mut self) -> SessionRestrictedSql<'_> {
        SessionRestrictedSql(Mutex::new(self))
    }

    /// Go `session.withRestrictedSQLExecutor` and `session.ExecRestrictedSQL`.
    pub fn execute_restricted_sql(
        &mut self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        options: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        let option = tidb_sqlexec::exec_option(options);
        if !option.use_cur_session {
            if let Some(executor) = self.restricted_sql_executor() {
                let result = executor.exec_restricted_sql(context, options, sql, arguments);
                self.append_restricted_sql_warnings();
                return result;
            }
        }
        crate::domain::cluster_session_node::with_exec_option(self, &option, |session| {
            let sql = tidb_sqlexec::sql_with_params(sql, arguments)?;
            session.execute_internal_sql(context, &sql)
        })
    }

    /// Go `session.ExecuteInternalStmt`: set InRestrictedSQL only for the
    /// nested statement; a user session remains a user session afterwards.
    pub(crate) fn execute_internal_sql(
        &mut self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        let restricted = self.vars.restricted_sql;
        self.vars.restricted_sql = true;
        let result = if self.node.is_some() {
            crate::domain::cluster_session_node::execute_internal_under(self, context, sql)
        } else {
            // Go ExecuteStmt inside an executor: the outer statement already
            // owns the store loan and bound transaction. Compile and drain on
            // that same session instead of trying to borrow the store again.
            (|| {
                crate::domain::cluster_session_node::prepare_restricted_caller(self, context)?;
                let output =
                    self.execute_statement(sql)
                        .map_err(|error| -> tidb_sqlexec::SqlExecError {
                            Box::new(
                                tidb_store_driver_error::sql_query_error::SqlQueryError::from(
                                    error.to_mysql_error(),
                                ),
                            )
                        })?;
                let crate::StmtOutput::Rows { columns, rows } = output else {
                    return Ok((Vec::new(), Vec::new()));
                };
                let fields = columns
                    .into_iter()
                    .map(|(name, field_type)| {
                        tidb_model::GoShared::new(tidb_resolve::ResultField {
                            column: Some(tidb_model::GoShared::new(tidb_model::ColumnInfo {
                                name: tidb_ast::CiString::new(name.clone()),
                                field_type,
                                ..Default::default()
                            })),
                            column_as_name: tidb_ast::CiString::new(name),
                            ..Default::default()
                        })
                    })
                    .collect();
                Ok((rows, fields))
            })()
        };
        self.vars.restricted_sql = restricted;
        result
    }
}

impl RestrictedSqlExecutor for SessionRestrictedSql<'_> {
    /// Go `session.ParseWithParams`.
    fn parse_with_params(
        &self,
        _context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<tidb_ast::Stmt> {
        use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
        let sql = tidb_sqlexec::sql_with_params(sql, arguments)?;
        let session = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let statement = if session.vars.restricted_sql {
            session.parse_internal(&sql)
        } else {
            session.parse_statement(&sql)
        };
        statement.map_err(|error| -> tidb_sqlexec::SqlExecError {
            Box::new(tidb_store_driver_error::sql_query_error::SqlQueryError::from(error.to_mysql_error()))
        })
    }

    /// Go `session.ExecRestrictedStmt`.
    fn exec_restricted_stmt(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
        options: &[tidb_sqlexec::OptionFuncAlias],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        self.exec_restricted_sql(
            context,
            options,
            std::str::from_utf8(statement.original_text())?,
            &[],
        )
    }

    /// Go `session.ExecRestrictedSQL`.
    fn exec_restricted_sql(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        options: &[tidb_sqlexec::OptionFuncAlias],
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<(Vec<Vec<Datum>>, Vec<tidb_resolve::ResultFieldRef>)> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .execute_restricted_sql(context, options, sql, arguments)
    }
}
