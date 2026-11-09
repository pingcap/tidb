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

//! Go `scheduler.go`.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use tidb_domain_serverinfo::serverinfo::ServerInfo;

/// Go `CheckTaskFinishedInterval`: the scheduler's tick. Exported for
/// testing.
pub static CHECK_TASK_FINISHED_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_millis(500));
/// Go `RetrySQLTimes`: the max retries of a SQL statement.
pub const RETRY_SQL_TIMES: usize = 30;
/// Go `RetrySQLInterval`: the initial interval between two SQL retries.
pub const RETRY_SQL_INTERVAL: Duration = Duration::from_secs(3);
/// Go `RetrySQLMaxInterval`: the max interval between two SQL retries.
pub const RETRY_SQL_MAX_INTERVAL: Duration = Duration::from_secs(30);

/// Go `CheckTaskFinishedInterval`'s current value.
pub(crate) fn check_task_finished_interval() -> Duration {
    *CHECK_TASK_FINISHED_INTERVAL.read().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Go's `infosync.GetAllServerInfo`: the cluster's server infos, which the
/// domain's server-info syncer answers (Go reads its global syncer).
pub trait ServerInfoSource: Send + Sync {
    /// Go `GetAllServerInfo`.
    ///
    /// # Errors
    /// The infos cannot be read.
    fn get_all_server_info(&self) -> Result<HashMap<String, ServerInfo>, String>;
}

impl ServerInfoSource for tidb_domain_serverinfo::serverinfo_syncer::Syncer {
    fn get_all_server_info(&self) -> Result<HashMap<String, ServerInfo>, String> {
        tidb_domain_serverinfo::serverinfo_syncer::Syncer::get_all_server_info(self)
    }
}

/// Go `GetLiveExecIDs`: the IDs of all live executor nodes.
///
/// # Errors
/// The server infos cannot be read, or there are none.
pub fn get_live_exec_ids(server_info: &dyn ServerInfoSource) -> Result<Vec<String>, String> {
    let server_infos = generate_task_executor_nodes(server_info)?;
    Ok(server_infos.iter().map(tidb_util_disttask::generate_exec_id).collect())
}

/// Go `generateTaskExecutorNodes`.
fn generate_task_executor_nodes(server_info: &dyn ServerInfoSource) -> Result<Vec<ServerInfo>, String> {
    let server_infos = server_info.get_all_server_info()?;
    if server_infos.is_empty() {
        return Err("not found instance".to_owned());
    }
    Ok(server_infos.into_values().collect())
}
