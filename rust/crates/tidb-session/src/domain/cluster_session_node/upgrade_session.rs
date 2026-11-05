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

//! Go `runInBootstrapSession`'s session: one internal session of the domain,
//! as `pkg/session/upgrade_def.go` reaches it through `sessionapi.Session`.

use std::sync::Arc;

use tidb_datatype::Datum;
use tidb_domain_core::sys_session::{stats_session_error, DomainSysSession};
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use super::sessions::cluster_stats_session;
use super::Domain;
use crate::bootstrap::{UpgradeSession, UpgradeStore};

/// Go `createSession(store)` in `runInBootstrapSession`.
struct BootstrapSession<'a> {
    context: Arc<dyn DomainSysSession>,
    store: &'a dyn UpgradeStore,
    state_syncer: &'a dyn tidb_ddl_serverstate::Syncer,
}

impl UpgradeSession for BootstrapSession<'_> {
    fn execute_internal(&mut self, sql: &str) -> Result<Vec<Vec<Datum>>, SqlQueryError> {
        let mut result = None;
        cluster_stats_session(&*self.context)
            .with_session(|session| {
                result = Some(session.execute_internal(sql));
                Ok(())
            })
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
        result.expect("the session ran the statement")
    }

    fn set_global_sys_var(&mut self, name: &str, value: &str) -> Result<(), String> {
        cluster_stats_session(&*self.context)
            .with_session(|session| {
                tidb_executor::sessionctx::SessionContext::set_global_sys_var(
                    &mut *session.session,
                    name,
                    value.to_owned(),
                )
                .map_err(|error| stats_session_error(format!("{error:?}")))
            })
            .map_err(|error| error.to_string())
    }

    fn get_store(&self) -> &dyn UpgradeStore {
        self.store
    }

    fn state_syncer(&self) -> &dyn tidb_ddl_serverstate::Syncer {
        self.state_syncer
    }
}

/// Go `runInBootstrapSession`'s `upgrade(s)` over a new internal session of
/// the domain.
pub fn run_upgrade(
    domain: &Arc<Domain>,
    store: &dyn UpgradeStore,
    state_syncer: &dyn tidb_ddl_serverstate::Syncer,
) -> Result<(), String> {
    let context = super::open_pool_session(domain, 0)?;
    // Go's bootstrap and upgrade run under `kv.InternalTxnBootstrap`.
    cluster_stats_session(&*context).state.adopt_request_source_type(tidb_txnkv::INTERNAL_TXN_BOOTSTRAP);
    // For the bootstrap SQLs, the following variables should be compatible with old TiDB versions.
    cluster_stats_session(&*context).set_var(
        tidb_vardef::tidb_vars::TIDB_ENABLE_CLUSTERED_INDEX,
        "INT_ONLY",
    );
    let mut session = BootstrapSession {
        context,
        store,
        state_syncer,
    };
    crate::upgrade_run::upgrade(&mut session)
}
