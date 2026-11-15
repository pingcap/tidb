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

//! Go `Domain`'s `sqlsvrapi.Server` and `sqlsvrapi.Runtime`, as DXF reaches
//! them: the domain is the server and the runtime of its own keyspace.

use std::sync::Arc;

use tidb_domain_sqlsvrapi::{KsRuntimeHandle, Runtime, Server};
use tidb_dxf_storage::SessionContext;

use super::Domain;

/// The domain as DXF's SQL server and current-keyspace runtime.
pub struct DomainSqlServer(pub Arc<Domain>);

impl Server<dyn SessionContext> for DomainSqlServer {
    /// Go `Domain.GetDDLOwnerMgr`.
    fn get_ddl_owner_mgr(&self) -> Option<Arc<dyn tidb_owner::Manager>> {
        self.0.ddl.owner_manager()
    }

    /// Go `GetRuntime`: the domain itself.
    fn get_runtime(&self) -> Arc<dyn Runtime<dyn SessionContext>> {
        Arc::new(Self(Arc::clone(&self.0)))
    }

    /// Go `AcquireKSRuntime`, through `crossKSSessMgr.Acquire`'s checks; a
    /// cross-keyspace runtime is next-gen only (row O15).
    fn acquire_ks_runtime(
        &self,
        target_ks: &str,
        holder_id: &str,
    ) -> Result<Arc<dyn KsRuntimeHandle<dyn SessionContext>>, String> {
        self.0.acquire_ks_runtime(target_ks,holder_id)
    }
}

impl Runtime<dyn SessionContext> for DomainSqlServer {
    fn store(&self) -> Arc<dyn tidb_txnkv::kv_storage::Storage> {self.0.transactions.kv_storage()}
    /// Go `Domain.AlterTableMode`: its ordinary DDL executor and pooled session.
    fn alter_table_mode(&self,_ctx:&tidb_gocontext::Context,target:&tidb_model::AlterTableModeTarget)->Result<(),String> {
        let pool=self.0.advanced_sys_session_pool();
        let session=pool.get().map_err(|error|error.to_string())?;
        let result=session.with_session_context(|context| -> tidb_syssession::Result<()> {
            let vars=context.ddl_statement_context().map_err(tidb_syssession::SysSessionError::new)?;
            // Go `Domain.AlterTableMode` calls `executor.AlterTableMode`.
            self.0.ddl.alter_table_mode(target.target_mode,target.schema_id,target.table_id,&vars)
                .map(|_|()).map_err(|error|tidb_syssession::SysSessionError::new(error.message))
        });
        pool.put(&session);
        result.map_err(|error|error.to_string())
    }

    /// Go `Store().GetKeyspace()`: the immutable identity of the opened store.
    fn store_keyspace(&self) -> String {
        self.0.store_keyspace().to_owned()
    }

    /// Go `SysSessionPool`: the domain's internal sessions, through the DXF
    /// pool over the same session factory.
    fn sys_session_pool(&self) -> Arc<dyn tidb_syssession::Pool<dyn SessionContext>> {
        self.0.dxf_session_pool()
    }
}
