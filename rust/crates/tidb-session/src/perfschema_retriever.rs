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

//! The session as Go's `perfSchemaTable.getRows` reads it: a memory-table
//! scan of a `performance_schema` table hands this session's state to
//! [`tidb_infoschema_perfschema::tables::get_rows`].

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_domain::cluster_topology::{ClusterServer, ClusterTopology};
use tidb_driver_error::DriverError;
use tidb_executor::mem_table_retrieval::{MemTableRead, MemTableRetriever};
use tidb_hack::GoCaseCompare as _;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_infoschema_perfschema::tables::PerfSchemaContext;

/// The retriever for the `performance_schema` tables Go fills when read.
pub(crate) struct PerfSchemaRetriever {
    variables: tidb_executor::account_state::VariablesState,
    registry: Option<tidb_sessmgr::ProcessRegistry>,
    connection_id: u64,
    login_user: Option<String>,
    topology: Option<Arc<ClusterTopology>>,
}

impl PerfSchemaRetriever {
    pub(crate) fn new(session: &crate::Session) -> Self {
        Self {
            variables: session.variables_state(),
            registry: session.process.as_ref().map(|guard| guard.registry().clone()),
            connection_id: session.vars.connection_id.unwrap_or(0),
            login_user: session.vars.login_user.clone(),
            topology: session.cluster_topology.clone(),
        }
    }
}

/// [`PerfSchemaRetriever`] over one statement's context.
struct StatementPerfSchema<'a> {
    retriever: &'a PerfSchemaRetriever,
    ctx: &'a tidb_sessionctx::StmtContext,
}

impl PerfSchemaContext for StatementPerfSchema<'_> {
    fn connection_id(&self) -> u64 {
        self.retriever.connection_id
    }

    fn user(&self) -> Option<(String, String)> {
        let login = self.retriever.login_user.as_deref()?;
        let (user, host) = login.rsplit_once('@')?;
        Some((user.to_owned(), host.to_owned()))
    }

    fn login_string(&self) -> Option<String> {
        self.retriever.login_user.clone()
    }

    fn client_ip(&self) -> Option<String> {
        self.user().map(|(_, host)| host)
    }

    fn session_manager(&self) -> Option<Arc<dyn tidb_sessmgr::processinfo::SessionManager>> {
        self.retriever
            .registry
            .clone()
            .map(|registry| Arc::new(registry) as Arc<dyn tidb_sessmgr::processinfo::SessionManager>)
    }

    fn get_data_from_session_variables(&self) -> Result<Vec<Vec<Datum>>, String> {
        self.retriever.variables.get_data_from_session_variables().map_err(|error| error.to_string())
    }

    fn get_store_server_info(&self) -> Result<Vec<ClusterServer>, String> {
        match &self.retriever.topology {
            Some(topology) => topology.get_store_server_info(),
            None => Err("pd unavailable".to_owned()),
        }
    }

    fn get_pd_server_info(&self) -> Result<Vec<ClusterServer>, String> {
        let mut warnings = Vec::new();
        let servers = match &self.retriever.topology {
            Some(topology) => topology.pd_servers(&mut warnings),
            None => Ok(Vec::new()),
        };
        for warning in warnings {
            self.append_warning(warning);
        }
        servers
    }

    fn append_warning(&self, message: String) {
        self.ctx.append_warning_parts(1105, &message);
    }
}

impl MemTableRetriever for PerfSchemaRetriever {
    fn serves(&self, read: &MemTableRead) -> bool {
        !read.is_cluster_scan
            && read.db_name.go_lower_eq(tidb_infoschema_perfschema::PERFORMANCE_SCHEMA)
            && tidb_infoschema_perfschema::tables::fills_table(&read.table_name.to_ascii_lowercase())
    }

    fn retrieve(
        &self,
        read: &MemTableRead,
        _catalog: &SessionExtendedInfoSchema,
        ctx: &tidb_sessionctx::StmtContext,
    ) -> Result<Vec<Vec<Datum>>, DriverError> {
        let sctx = StatementPerfSchema { retriever: self, ctx };
        tidb_infoschema_perfschema::tables::get_rows(&read.table_name.to_ascii_lowercase(), &sctx)
            .map_err(|error| DriverError::Mysql(tidb_terror::MysqlError::unknown(error)))
    }
}
