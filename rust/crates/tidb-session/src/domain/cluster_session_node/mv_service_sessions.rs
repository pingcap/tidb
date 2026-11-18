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

//! The domain's pooled internal session as Go master's `pkg/mvservice`
//! reaches it through `sessionctx.Context`.

use tidb_datatype::Datum;
use tidb_mvservice::{MVServiceSessionContext, MvsError};
use tidb_sqlescape::SqlArg;
use tidb_syssession::SessionContext as _;

use super::sessions::ClusterStatsSessionContext;
use super::ttl_sessions::read_system;

/// A statement error as the service compares it: its MySQL code kept.
fn mvs_error(error: tidb_sqlexec::SqlExecError) -> MvsError {
    match error.downcast::<tidb_store_driver_error::sql_query_error::SqlQueryError>() {
        Ok(error) => MvsError::Sql { code: error.code, message: error.message.clone() },
        Err(error) => MvsError::Other(error.to_string()),
    }
}

fn maintenance_context() -> tikv_client::trace::TraceContext {
    tikv_client::util::with_internal_source_type(
        &tikv_client::trace::TraceContext::new(),
        tidb_txnkv::INTERNAL_TXN_MVIEW_MAINTENANCE,
    )
}

impl MVServiceSessionContext for ClusterStatsSessionContext {
    fn exec_restricted_sql(&self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, MvsError> {
        let options: [tidb_sqlexec::OptionFuncAlias; 1] = [std::sync::Arc::new(tidb_sqlexec::exec_option_use_cur_session)];
        self.restricted_sql_executor()
            .exec_restricted_sql(&maintenance_context(), &options, sql, args)
            .map(|(rows, _)| rows)
            .map_err(mvs_error)
    }

    fn execute_internal(&self, sql: &str) -> Result<(), MvsError> {
        self.sql_executor().execute_internal(&maintenance_context(), sql, &[]).map(|_| ()).map_err(mvs_error)
    }

    fn affected_rows(&self) -> u64 {
        tidb_stats_handle_util::StatsSessionContext::affected_rows(self)
    }

    fn table_info_by_id(&self, id: i64) -> Option<tidb_model::TableInfo> {
        self.with_session(|session| {
            let catalog = session.node.domain().catalog.get_latest();
            Ok(catalog.table_by_id(id).map(|(_, table)| table.meta().read().clone_like_go()))
        })
        .ok()
        .flatten()
    }

    fn schema_name_by_id(&self, id: i64) -> Option<String> {
        self.with_session(|session| {
            let catalog = session.node.domain().catalog.get_latest();
            Ok(catalog.schema_by_id(id).map(|db| db.read().name.lowercase().to_owned()))
        })
        .ok()
        .flatten()
    }

    fn global_system_var(&self, name: &str) -> Result<String, MvsError> {
        tidb_stats_handle_util::StatsSessionContext::global_system_var(self, name).map_err(mvs_error)
    }

    fn system_var(&self, name: &str) -> Option<String> {
        read_system(&self.state, name).ok()
    }

    fn set_system_var(&self, name: &str, value: &str) -> Result<(), MvsError> {
        self.with_session(|session| {
            session
                .session
                .set_internal_system_var(name, value.to_owned())
                .map_err(|error| tidb_domain_core::sys_session::stats_session_error(format!("{error:?}")))
        })
        .map_err(mvs_error)
    }

    fn store_current_version(&self) -> Result<u64, MvsError> {
        self.with_session(|session| session.store_current_version().map_err(Into::into)).map_err(mvs_error)
    }

    fn statement_now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}
