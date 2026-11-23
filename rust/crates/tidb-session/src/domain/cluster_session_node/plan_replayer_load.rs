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

//! Go `PlanReplayerLoadInfo.Update`'s `sctx` (`tidb_executor::plan_replayer`):
//! this connection's session, its nested statements and its statistics.

use tidb_context::WarningLevel;
use tidb_executor::sessionctx::SessionContext as _;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use super::{map_error, ClusterServerSession};

/// Go `errors.New` / `errors.NewNoStackErrorf` through `AppendWarning`.
const ER_UNKNOWN_ERROR: u16 = 1105;

impl tidb_executor::plan_replayer::PlanReplayerLoadSession for ClusterServerSession<'_> {
    fn load_session_vars(&self) -> &tidb_variable::SessionVars {
        &self.session.vars
    }

    fn load_session_vars_mut(&mut self) -> &mut tidb_variable::SessionVars {
        &mut self.session.vars
    }

    fn append_load_warning(&mut self, message: String) {
        self.session.append_warning(WarningLevel::Warning, ER_UNKNOWN_ERROR, message);
    }

    fn set_load_global_sys_var(&mut self, name: &str, value: String) -> Result<(), SqlQueryError> {
        self.session.set_global_sys_var(name, value).map_err(map_error)
    }

    fn execute_nested(&mut self, sql: &str) -> Result<(), SqlQueryError> {
        let stmt = self.session.parse_at_statement_boundary(sql).map_err(map_error)?;
        self.reborrow().execute_stmt(sql, &stmt).map(drop)
    }

    fn load_stats_json(&mut self, json: &tidb_stats_util::JsonTable) -> Result<(), SqlQueryError> {
        self.stats_statement_service().load_stats(json).map_err(map_error)
    }
}
