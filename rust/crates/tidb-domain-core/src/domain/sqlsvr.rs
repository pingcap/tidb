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
        if holder_id.is_empty() {
            return Err("cross keyspace runtime holderID must not be empty".to_owned());
        }
        // misusing cross keyspace sessions might cause data written to the wrong
        // keyspace, or corrupt user data, and it's harder to diagnose those issues.
        if tidb_config_kerneltype::is_classic() || self.store_keyspace() == target_ks {
            return Err("cross keyspace is not available in classic kernel or current keyspace".to_owned());
        }
        Err(format!("cross keyspace runtime of keyspace {target_ks} is not supported"))
    }
}

impl Runtime<dyn SessionContext> for DomainSqlServer {
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
