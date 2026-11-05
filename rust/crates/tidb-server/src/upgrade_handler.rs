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

//! Go `pkg/server/handler/upgrade_handler.go`: the status port's
//! `/upgrade/{start,finish,show}`, which pause user DDL for a smooth
//! upgrade and resume it after.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tidb_domain_serverinfo::serverinfo::VersionInfo;

/// Go `ClusterUpgradeHandler`'s store as the handler reaches it: the DDL's
/// state syncer, the etcd client the owner op value is read from, and the
/// sessions the paused jobs resume on.
#[derive(Clone)]
pub struct ClusterUpgrade {
    /// Go `dom.DDL().StateSyncer()`.
    pub state_syncer: Arc<dyn tidb_ddl_serverstate::Syncer>,
    /// Go `dom.GetEtcdClient()`; `None` without etcd.
    pub etcd_cli: Option<Arc<dyn tidb_owner::OwnerStore>>,
    /// Go `session.CreateSession(h.store)` for the job commands.
    pub sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    /// Go `infosync.GetAllServerInfo`.
    pub server_info: Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>,
    /// Go `do.DDL().OwnerManager()`.
    pub ddl_owner: Arc<dyn tidb_owner::Manager>,
}

/// Go `SimpleServerInfo` is some simple information such as version and address.
#[derive(Serialize)]
struct SimpleServerInfo {
    #[serde(flatten)]
    version_info: VersionInfo,
    #[serde(rename = "ddl_id")]
    id: String,
    ip: String,
    #[serde(rename = "listening_port")]
    port: usize,
    #[serde(rename = "server_id")]
    json_server_id: u64,
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Go `ClusterUpgradeInfo` is used to report cluster upgrade info when do http request.
#[derive(Serialize)]
struct ClusterUpgradeInfo {
    #[serde(rename = "servers_num", skip_serializing_if = "is_zero_usize")]
    servers_num: usize,
    #[serde(rename = "owner_id")]
    owner_id: String,
    #[serde(rename = "upgraded_percent")]
    upgraded_percent: usize,
    #[serde(rename = "is_all_server_version_consistent", skip_serializing_if = "is_false")]
    is_all_upgraded: bool,
    #[serde(rename = "all_servers_diff_info", skip_serializing_if = "Vec::is_empty")]
    all_servers_diff_infos: Vec<SimpleServerInfo>,
}

impl ClusterUpgrade {
    /// Go `StartUpgrade` is used to start the upgrade.
    fn start_upgrade(&self) -> Result<bool, String> {
        let is_upgrading = tidb_session::sync_upgrade::is_upgrading_cluster_state(self.state_syncer.as_ref())?;
        if is_upgrading {
            return Ok(true);
        }

        tidb_session::sync_upgrade::sync_upgrade_state(
            self.state_syncer.as_ref(),
            self.etcd_cli.as_deref(),
            Duration::from_secs(10),
        )?;
        Ok(false)
    }

    /// Go `FinishUpgrade` is used to finish the upgrade.
    fn finish_upgrade(&self) -> Result<bool, String> {
        let is_upgrading = tidb_session::sync_upgrade::is_upgrading_cluster_state(self.state_syncer.as_ref())?;
        if !is_upgrading {
            return Ok(true);
        }

        tidb_session::sync_upgrade::sync_normal_running(self.state_syncer.as_ref(), self.sess_pool.as_ref())?;
        Ok(false)
    }

    /// Go `showUpgrade`: the compact JSON `WriteData` marshals.
    fn show_upgrade(&self) -> Result<String, String> {
        // Check if we are upgrading by pausing user DDL(in other words by "/upgrade/start").
        let is_upgrading = tidb_session::sync_upgrade::is_upgrading_cluster_state(self.state_syncer.as_ref())?;
        if !is_upgrading {
            return Ok("\"The cluster state is normal.\"".to_owned());
        }

        let all_servers_info = self.server_info.get_all_server_info()?;
        let owner_id = self
            .ddl_owner
            .get_owner_id(&tidb_owner::Context::with_timeout(Duration::from_secs(3)))?;

        let mut all_versions_map: HashMap<String, (VersionInfo, usize)> = HashMap::new();
        let mut all_versions: Vec<VersionInfo> = Vec::with_capacity(all_servers_info.len());
        for v in all_servers_info.values() {
            let version_info = v.static_info.version_info.clone();
            let key = format!("{}\u{0}{}", version_info.version, version_info.git_hash);
            if let Some((_, count)) = all_versions_map.get_mut(&key) {
                *count += 1;
                continue;
            }
            all_versions_map.insert(key, (version_info.clone(), 1));
            all_versions.push(version_info);
        }
        let Some(mut max_ver_info) = all_versions.first().cloned() else {
            return Err("runtime error: index out of range [0] with length 0".to_owned());
        };
        for (k, _) in all_versions_map.values() {
            if max_ver_info.version < k.version {
                max_ver_info = k.clone();
            }
        }
        let max_key = format!("{}\u{0}{}", max_ver_info.version, max_ver_info.git_hash);
        let upgraded_percent = (all_versions_map[&max_key].1 * 100) / all_servers_info.len();
        let mut upgrade_info = ClusterUpgradeInfo {
            servers_num: all_servers_info.len(),
            owner_id,
            upgraded_percent,
            is_all_upgraded: upgraded_percent == 100,
            all_servers_diff_infos: Vec::new(),
        };
        // If IsAllUpgraded is false, return the all tidb servers version.
        if !upgrade_info.is_all_upgraded {
            let mut infos: Vec<_> = all_servers_info.values().collect();
            infos.sort_by(|left, right| left.static_info.id.cmp(&right.static_info.id));
            upgrade_info.all_servers_diff_infos = infos
                .into_iter()
                .map(|info| SimpleServerInfo {
                    version_info: info.static_info.version_info.clone(),
                    id: info.static_info.id.clone(),
                    ip: info.static_info.ip.clone(),
                    port: info.static_info.port,
                    json_server_id: info.static_info.json_server_id,
                })
                .collect();
        }
        serde_json::to_string(&upgrade_info).map_err(|error| error.to_string())
    }
}

/// Go `ClusterUpgradeHandler.ServeHTTP` (`/upgrade/{op}`): the compact JSON
/// of each `WriteData` call in order -- `show` writes its report and then
/// Go's `"success!"` -- or `WriteError`'s text.
pub fn cluster_upgrade_response(
    path: &str,
    request: &str,
    upgrade: Option<&ClusterUpgrade>,
) -> Option<Result<Vec<String>, String>> {
    let route = path.split_once('?').map_or(path, |(route, _)| route);
    let op = route.strip_prefix("/upgrade/")?;
    if op.is_empty() || op.contains('/') {
        return None;
    }
    if request.split_whitespace().next() != Some("POST") {
        return Some(Err("This API only support POST method".to_owned()));
    }
    let Some(h) = upgrade else {
        return Some(Err("the cluster upgrade handler is not initialized".to_owned()));
    };

    let (result, has_done) = match op {
        "start" => match h.start_upgrade() {
            Ok(has_done) => (Ok(None), has_done),
            Err(error) => (Err(error), false),
        },
        "finish" => match h.finish_upgrade() {
            Ok(has_done) => (Ok(None), has_done),
            Err(error) => (Err(error), false),
        },
        "show" => (h.show_upgrade().map(Some), false),
        _ => return Some(Err(format!("wrong operation:{op}"))),
    };

    let logger = tidb_logutil::bg_logger();
    let fields = || {
        [
            tidb_log::Field::new("category", tidb_log::Value::Str("upgrading".to_owned())),
            tidb_log::Field::new("operation", tidb_log::Value::Str(op.to_owned())),
            tidb_log::Field::new("hasDone", tidb_log::Value::Bool(has_done)),
        ]
    };
    let shown = match result {
        Err(error) => {
            logger.info("upgrade operation failed", &fields());
            return Some(Err(error));
        }
        Ok(shown) => shown,
    };
    let mut data: Vec<String> = shown.into_iter().collect();
    if has_done {
        match op {
            "start" => data.push(
                "\"It's a duplicated operation and the cluster is already in upgrading state.\"".to_owned(),
            ),
            "finish" => data.push(
                "\"It's a duplicated operation and the cluster is already in normal state.\"".to_owned(),
            ),
            _ => {}
        }
    } else {
        data.push("\"success!\"".to_owned());
    }
    logger.info("upgrade operation success", &fields());
    Some(Ok(data))
}
