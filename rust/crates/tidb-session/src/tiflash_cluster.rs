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
// See the License for the specific language governing permissions and
// limitations under the License.

//! The TiFlash cluster as Go's fine-grained shuffle pass asks it:
//! `infoschema.GetTiFlashServerInfo` and `getTiFlashServerMinLogicalCores`
//! (`pkg/planner/core/optimizer.go`).

use std::sync::Arc;

use tidb_domain::cluster_topology::{ClusterServer, ClusterTopology};
use tidb_executor::cluster_diagnostics::ClusterDiagnosticsClient;
use tidb_sessionctx::{TiFlashClusterInfo, TiFlashServer};

/// The node's cluster topology and diagnostics client.
pub(crate) struct SessionTiFlashCluster {
    pub(crate) topology: Arc<ClusterTopology>,
    pub(crate) diagnostics: Option<Arc<ClusterDiagnosticsClient>>,
}

impl TiFlashClusterInfo for SessionTiFlashCluster {
    fn tiflash_server_info(&self) -> Result<Vec<TiFlashServer>, String> {
        if tidb_config::config_tree::config::get_global_config().disaggregated_tiflash {
            return Err("operation not supported".to_owned());
        }
        Ok(self
            .topology
            .get_store_server_info()?
            .into_iter()
            .filter(|server| server.server_type == "tiflash")
            .map(|server| TiFlashServer {
                address: server.address,
                status_address: server.status_address,
                start_timestamp: server.start_timestamp,
            })
            .collect())
    }

    fn min_logical_cores(&self, servers: &[TiFlashServer]) -> Option<u64> {
        let manager = tidb_copr::mpp_probe::global_mpp_server_info_manager();
        let mut min_logical_cores: Option<u64> = None;
        let mut keep_min = |cores: u64| {
            min_logical_cores = Some(min_logical_cores.map_or(cores, |min| min.min(cores)));
        };
        // Go `splitTiFlashLogicalCoreCache`: a TiFlash that restarted (a new
        // start timestamp at the same address) may have new cores.
        let mut needing_refresh = Vec::new();
        for server in servers {
            match manager.get(&server.address) {
                Some(info) if info.start_timestamp == server.start_timestamp => {
                    keep_min(info.logical_cpu_count);
                }
                _ => needing_refresh.push(server),
            }
        }
        if let (false, Some(diagnostics)) = (needing_refresh.is_empty(), &self.diagnostics) {
            let asked: Vec<ClusterServer> = needing_refresh
                .iter()
                .map(|server| ClusterServer {
                    server_type: "tiflash".to_owned(),
                    address: server.address.clone(),
                    status_address: server.status_address.clone(),
                    start_timestamp: server.start_timestamp,
                    ..ClusterServer::default()
                })
                .collect();
            let mut warnings = Vec::new();
            let rows = diagnostics.fetch_cluster_server_info_without_privilege_check(
                &asked,
                tidb_proto::diagnosticspb::ServerInfoType::HardwareInfo,
                &mut warnings,
            );
            for row in rows {
                let text = |index: usize| match row.get(index) {
                    Some(tidb_datatype::Datum::Bytes(bytes)) => Some(String::from_utf8_lossy(bytes).into_owned()),
                    _ => None,
                };
                if text(4).as_deref() != Some("cpu-logical-cores") {
                    continue;
                }
                let Some(cores) = text(5).and_then(|value| value.parse::<u64>().ok()).filter(|cores| *cores > 0) else {
                    continue;
                };
                let Some(server) = needing_refresh
                    .iter()
                    .find(|server| text(1).as_deref() == Some(server.address.as_str()))
                else {
                    continue;
                };
                manager.add(tidb_copr::mpp_probe::MppServerInfo {
                    address: server.address.clone(),
                    logical_cpu_count: cores,
                    start_timestamp: server.start_timestamp,
                });
                keep_min(cores);
            }
        }
        min_logical_cores
    }
}
