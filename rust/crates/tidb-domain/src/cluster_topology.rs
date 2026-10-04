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

//! Go infoschema cluster discovery and Domain.checkReplicaRead's shared topology.

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::serverinfo_syncer::{join_host_port, Syncer};
use serde::Deserialize;

/// Go infoschema.ServerInfo, before SQL datum conversion.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ClusterServer {
    /// Component type, including tiflash_write for the SQL projection.
    pub server_type: String,
    /// Client address.
    pub address: String,
    /// Status endpoint.
    pub status_address: String,
    /// Component version.
    pub version: String,
    /// Build revision.
    pub git_hash: String,
    /// Unix startup time, or zero when unavailable.
    pub start_timestamp: i64,
    /// TiDB's live server ID.
    pub server_id: u64,
}

/// Store topology shared by metadata retrieval and adaptive read policy.
#[derive(Clone, Debug)]
pub struct ClusterStore {
    /// SQL-visible component metadata.
    pub server: ClusterServer,
    /// PD labels, in source order.
    pub labels: Vec<(String, String)>,
    /// Removing/removed stores cannot contribute an adaptive-read zone.
    pub removing: bool,
}

/// PD-backed discovery; implementations borrow process-owned capabilities.
pub trait ClusterDiscovery: Send + Sync {
    /// PD status reads fail the statement only if every member fails.
    fn pd_servers(&self, warnings: &mut Vec<String>) -> Result<Vec<ClusterServer>, String>;
    /// Live store metadata from the region cache's PD client.
    fn stores(&self) -> Result<Vec<ClusterStore>, String>;
    /// First successful TSO/scheduling discovery; absent services are empty.
    fn microservice_servers(
        &self,
        service: &str,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<ClusterServer>, String>;
}

/// Domain lifetime owner shared by SQL metadata and the adaptive policy loop.
pub struct ClusterTopology {
    syncer: Arc<Syncer>,
    discovery: Option<Arc<dyn ClusterDiscovery>>,
    adaptive_enabled: AtomicBool,
}

impl ClusterTopology {
    /// Go initializes enableAdaptiveReplicaRead to true.
    pub fn new(syncer: Arc<Syncer>, discovery: Option<Arc<dyn ClusterDiscovery>>) -> Self {
        Self {
            syncer,
            discovery,
            adaptive_enabled: AtomicBool::new(true),
        }
    }

    /// Current domain policy, consulted each statement rather than cached in a session.
    pub fn adaptive_enabled(&self) -> bool {
        self.adaptive_enabled.load(Ordering::Acquire)
    }

    /// Go Domain.checkReplicaRead: equal numbers of eligible TiDBs per TiKV zone.
    /// Errors leave the previous decision intact; a different global setting does too.
    pub fn check_replica_read(&self, global_replica_read: &str) -> Result<(), String> {
        if !global_replica_read.eq_ignore_ascii_case("closest-adaptive") {
            return Ok(());
        }
        let local = self.syncer.local_server_info();
        let zone = local
            .dynamic_info
            .labels
            .get("zone")
            .filter(|zone| !zone.is_empty());
        let Some(zone) = zone else {
            self.adaptive_enabled.store(false, Ordering::Release);
            return Ok(());
        };
        let stores = self.discovery.as_ref().ok_or("pd unavailable")?.stores()?;
        let mut zones = HashMap::<String, usize>::new();
        for store in stores {
            if store.removing
                || store
                    .labels
                    .iter()
                    .any(|(key, value)| key == "engine" && value == "tiflash")
            {
                continue;
            }
            if let Some((_, zone)) = store
                .labels
                .iter()
                .find(|(key, value)| key == "zone" && !value.is_empty())
            {
                zones.entry(zone.clone()).or_default();
            }
        }
        if !zones.contains_key(zone) {
            self.adaptive_enabled.store(false, Ordering::Release);
            return Ok(());
        }
        let mut local_ids = Vec::new();
        for server in self.syncer.all_server_info()?.values() {
            if let Some(server_zone) = server.dynamic_info.labels.get("zone") {
                if let Some(count) = zones.get_mut(server_zone) {
                    *count += 1;
                }
                if server_zone == zone {
                    local_ids.push(server.static_info.id.clone());
                }
            }
        }
        let enabled_count = zones.values().copied().min().unwrap_or(0);
        local_ids.sort();
        let enabled = !local_ids
            .iter()
            .skip(enabled_count)
            .any(|id| id == &local.static_info.id);
        self.adaptive_enabled.store(enabled, Ordering::Release);
        Ok(())
    }

    /// Go GetClusterServerInfo retriever ordering and fail-on-retriever-error contract.
    /// Warnings remain available to the statement even when a later retriever fails.
    pub fn servers(&self, warnings: &mut Vec<String>) -> Result<Vec<ClusterServer>, String> {
        let mut servers = Vec::new();
        let custom_version = !tidb_config::config_tree::config::get_global_config()
            .server_version
            .is_empty();
        for info in self.syncer.all_server_info()?.into_values() {
            let info = info.static_info;
            let version = if custom_version {
                info.version_info.version.clone()
            } else {
                let version = info
                    .version_info
                    .version
                    .split_once("TiDB-")
                    .map_or(info.version_info.version.as_str(), |(_, version)| version);
                version.strip_prefix('v').unwrap_or(version).to_owned()
            };
            servers.push(ClusterServer {
                server_type: "tidb".into(),
                address: join_host_port(&info.ip, info.port),
                status_address: join_host_port(&info.ip, info.status_port),
                version,
                git_hash: info.version_info.git_hash,
                start_timestamp: info.start_timestamp,
                server_id: info
                    .server_id_getter
                    .as_ref()
                    .map_or(info.json_server_id, |get| get()),
            });
        }
        if let Some(discovery) = &self.discovery {
            servers.extend(discovery.pd_servers(warnings)?);
            servers.extend(discovery.stores()?.into_iter().map(|store| store.server));
        }
        for (key, value) in self.syncer.topology_entries("/topology/tiproxy")? {
            if !key.ends_with("/info") {
                continue;
            }
            let proxy = serde_json::from_slice::<Option<Proxy>>(&value)
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            servers.push(ClusterServer {
                server_type: "tiproxy".into(),
                address: join_port(&proxy.ip, &proxy.port),
                status_address: join_port(&proxy.ip, &proxy.status_port),
                version: proxy.version,
                git_hash: proxy.git_hash,
                start_timestamp: proxy.start_timestamp,
                ..ClusterServer::default()
            });
        }
        for (key, value) in self.syncer.topology_entries("/topology/ticdc")? {
            if key.split('/').count() < 3 {
                continue;
            }
            let cdc = serde_json::from_slice::<Option<Cdc>>(&value)
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
            servers.push(ClusterServer {
                server_type: "ticdc".into(),
                address: cdc.address.clone(),
                status_address: cdc.address,
                version: cdc
                    .version
                    .strip_prefix('v')
                    .unwrap_or(&cdc.version)
                    .to_owned(),
                git_hash: cdc.git_hash,
                start_timestamp: cdc.start_timestamp,
                ..ClusterServer::default()
            });
        }
        if let Some(discovery) = &self.discovery {
            servers.extend(discovery.microservice_servers("tso", warnings)?);
            servers.extend(discovery.microservice_servers("scheduling", warnings)?);
        }
        for server in &mut servers {
            resolve_loopback(server);
        }
        Ok(servers)
    }
}

#[derive(Default, Deserialize)]
#[serde(default)]
struct Proxy {
    ip: String,
    port: String,
    status_port: String,
    version: String,
    git_hash: String,
    start_timestamp: i64,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
struct Cdc {
    address: String,
    version: String,
    git_hash: String,
    start_timestamp: i64,
}
fn join_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Go ResolveLoopBackAddr: substitute the public peer IP, preserving each port.
fn resolve_loopback(server: &mut ClusterServer) {
    let resolve = |address: &str| address.to_socket_addrs().ok()?.next();
    let (Some(address), Some(status)) = (resolve(&server.address), resolve(&server.status_address))
    else {
        return;
    };
    let local = |ip: std::net::IpAddr| ip.is_loopback() || ip.is_unspecified();
    if local(address.ip()) && !local(status.ip()) {
        server.address = std::net::SocketAddr::new(status.ip(), address.port()).to_string();
    } else if !local(address.ip()) && local(status.ip()) {
        server.status_address = std::net::SocketAddr::new(address.ip(), status.port()).to_string();
    }
}
