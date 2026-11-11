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
        self.restricted_sql_issuer.state().inherited =
            Some([TIDB_OPT_USE_INVISIBLE_INDEXES, TIDB_SKIP_MISSING_PARTITION_STATS]
                .map(|name| self.vars.get_system(name).unwrap_or_default()));
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
