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

//! The domain's pooled internal session as Go's `pkg/dxf/framework/storage`
//! reaches it through `sessionctx.Context`.

use tidb_dxf_storage::{Result, StorageError};
use tidb_vardef::tidb_vars::{TIDB_MEM_QUOTA_QUERY, TIDB_TXN_ENTRY_SIZE_LIMIT};

use super::sessions::ClusterStatsSessionContext;
use super::ttl_sessions::{read_global, read_system};

impl tidb_dxf_storage::SessionContext for ClusterStatsSessionContext {
    fn commit_txn(&self, _context: &dyn tidb_sqlexec::ExecutionContext) -> Result<()> {
        tidb_domain_core::sys_session::DomainSysSession::commit_txn(self).map_err(StorageError::Other)
    }

    fn txn_entry_size_limit(&self) -> u64 {
        read_system(&self.state, TIDB_TXN_ENTRY_SIZE_LIMIT).ok().and_then(|value| value.parse().ok()).unwrap_or(0)
    }

    fn set_txn_entry_size_limit(&self, limit: u64) {
        self.set_var(TIDB_TXN_ENTRY_SIZE_LIMIT, limit.to_string());
    }

    /// Go `vardef.TxnEntrySizeLimit`, which the global variable's `SetGlobal`
    /// stores.
    fn global_txn_entry_size_limit(&self) -> u64 {
        read_global(&self.state, TIDB_TXN_ENTRY_SIZE_LIMIT).ok().and_then(|value| value.parse().ok()).unwrap_or(0)
    }

    fn affected_rows(&self) -> u64 {
        tidb_stats_handle_util::StatsSessionContext::affected_rows(self)
    }

    fn mem_quota_query(&self) -> i64 {
        read_system(&self.state, TIDB_MEM_QUOTA_QUERY)
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_QUERY)
    }

    fn store_keyspace(&self) -> String {
        tidb_config::config_tree::config::get_global_keyspace_name()
    }

    fn sql_server(&self) -> std::sync::Arc<dyn tidb_domain_sqlsvrapi::Server<dyn tidb_dxf_storage::SessionContext>> {
        let domain = self
            .with_session(|session| Ok(std::sync::Arc::clone(&session.node.domain)))
            .expect("a pooled system session is open");
        std::sync::Arc::new(tidb_domain_core::domain::sqlsvr::DomainSqlServer(domain))
    }

    fn set_system_var(&self, name: &str, value: &str) -> Result<()> {
        self.with_session(|session| {
            session
                .session
                .set_internal_system_var(name, value.to_owned())
                .map_err(|error| tidb_domain_core::sys_session::stats_session_error(format!("{error:?}")))
        })
        .map_err(|error| StorageError::Other(error.to_string()))
    }
}
