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
use tidb_executor::DriverError;

use crate::Session;

/// Go `RestrictedSQLExecutor.ExecRestrictedSQL` with no options: one
/// schema-qualified statement on an internal session, answering its rows.
pub trait RestrictedSqlExecutor: Send + Sync {
    /// Runs `sql` in an internal session's own transaction.
    fn exec_restricted_sql(&self, sql: &str) -> Result<Vec<Vec<Datum>>, DriverError>;
}

/// The shared handle a session and its retriever snapshots carry.
pub(crate) type RestrictedSql = Option<Arc<dyn RestrictedSqlExecutor>>;

/// Go `ExecRestrictedSQL` through the session's executor, which the node
/// that opened the session installs.
pub(crate) fn exec_restricted_sql_over(
    executor: Option<&dyn RestrictedSqlExecutor>,
    sql: &str,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    executor
        .ok_or_else(|| {
            DriverError::unsupported("restricted SQL runs on the session's node".to_owned())
        })?
        .exec_restricted_sql(sql)
}

impl Session {
    /// Go `GetRestrictedSQLExecutor`'s backing: what the node installs.
    pub fn set_restricted_sql_executor(&mut self, executor: Arc<dyn RestrictedSqlExecutor>) {
        self.restricted_sql_executor = Some(executor);
    }

    /// The installed executor, which retriever snapshots carry.
    pub(crate) fn restricted_sql_executor(&self) -> RestrictedSql {
        self.restricted_sql_executor.clone()
    }

    /// Go `ExecRestrictedSQL(ctx, nil, sql)`.
    pub(crate) fn exec_restricted_sql(
        &mut self,
        sql: &str,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        exec_restricted_sql_over(self.restricted_sql_executor.as_deref(), sql)
    }
}
