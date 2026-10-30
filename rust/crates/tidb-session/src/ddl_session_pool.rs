// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The node's DDL session pool.
//!
//! Go's `pkg/ddl` takes its sessions from the pool the domain hands it: a
//! system session for a job the domain submits itself, and the sessions its
//! data checks (`checkModifyColumnData`, `checkForNullValue`,
//! `checkForeignKeyConstrain`, `verifyRemainRecordsForCheckConstraint`,
//! `checkExchangePartitionRecordValidation`) run their restricted SQL in; the
//! DDL package never imports the executor. The worker here holds a
//! [`tidb_ddl_build::session_pool::DdlSessionPool`] the same way, and this is
//! the implementation the node hands it: built before the node's domain (the
//! DDL is one of the domain's inputs) and bound to it once the domain exists,
//! as Go's domain hands DDL its session pool from the session factory.

use std::sync::{Arc, OnceLock, Weak};

use tidb_domain_core::domain::Domain;

use crate::domain::cluster_session_node::DomainSessions;

use tidb_ddl_build::session_pool::DdlSessionPool;
use tidb_terror::MysqlError;
use tidb_sessionctx::StmtContext;

/// The node's DDL session pool: system sessions of the node's domain.
#[derive(Debug, Default)]
pub struct NodeDdlSessionPool {
    domain: OnceLock<Weak<Domain>>,
}

impl NodeDdlSessionPool {
    /// Binds the pool to the domain whose sessions it hands out.
    pub fn bind(&self, domain: &Arc<Domain>) {
        if self.domain.set(Arc::downgrade(domain)).is_err() {
            panic!("a node's DDL session pool is bound once");
        }
    }
}

impl DdlSessionPool for NodeDdlSessionPool {
    /// A fresh statement of a default session.
    fn system_session(&self) -> Box<dyn tidb_ddl_build::session_context::DdlSessionContext> {
        Box::new(StmtContext::for_query())
    }

    /// One statement on a fresh system session of the node, in its own
    /// autocommit transaction (Go `withRestrictedSQLExecutor` without
    /// `ExecOptionUseCurSession`).
    fn exec_restricted_sql(
        &self,
        sql: &str,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, MysqlError> {
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| MysqlError::unknown("the DDL session pool has no running node"))?;
        let to_mysql = |error: tidb_store_driver_error::sql_query_error::SqlQueryError| {
            MysqlError::from_parts(error.code, error.state, error.message)
                .with_identity_opt(error.identity)
        };
        let mut session = domain.open_system_session().map_err(to_mysql)?;
        let rows = session.node_conn().execute_internal(sql).map_err(to_mysql);
        rows
    }
}
