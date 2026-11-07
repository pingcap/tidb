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

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_driver_error::DriverError;

use crate::Session;

use tidb_executor::memtable_reader::{exec_restricted_sql_over, RestrictedSql};
use tidb_sqlexec::RestrictedSqlExecutor;

impl Session {
    /// Go `GetRestrictedSQLExecutor`'s backing: what the node installs.
    pub fn set_restricted_sql_executor(&mut self, executor: Arc<dyn RestrictedSqlExecutor>) {
        self.restricted_sql_executor = Some(executor);
    }

    /// The installed executor, which retriever snapshots carry.
    pub(crate) fn restricted_sql_executor(&self) -> RestrictedSql {
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
        exec_restricted_sql_over(self.restricted_sql_executor.as_deref(), source, sql, &[])
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
