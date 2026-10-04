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

//! Live Go infoschema PD/store/microservice retrievers over the process PD client.

use serde::Deserialize;
use tidb_domain::cluster_topology::{ClusterDiscovery, ClusterServer, ClusterStore};
use tidb_pd_client::{ClusterSecurity, PdClient};
use tidb_proto::metapb;

/// Shares the node's PD membership and internal HTTP security policy.
pub struct PdClusterDiscovery {
    pd: PdClient,
    http: reqwest::blocking::Client,
    scheme: &'static str,
}

impl PdClusterDiscovery {
    /// No new PD worker, cache or TSO lifecycle is created.
    pub fn new(pd: PdClient, security: &ClusterSecurity) -> Result<Self, String> {
        Ok(Self {
            http: crate::cluster_http::cluster_http_client(security)?,
            pd,
            scheme: if security.is_tls_enabled() {
                "https"
            } else {
                "http"
            },
        })
    }

    fn members(&self) -> Vec<String> {
        self.pd
            .member_set()
            .member_urls
            .into_iter()
            .map(|url| strip_scheme(&url).to_owned())
            .collect()
    }

    fn get(&self, address: &str, path: &str) -> Result<reqwest::blocking::Response, String> {
        self.http
            .get(format!("{}://{address}{path}", self.scheme))
            .header("PD-Allow-follower-handle", "true")
            .timeout(self.pd.timeout())
            .send()
            .map_err(|error| error.to_string())
    }
}

impl ClusterDiscovery for PdClusterDiscovery {
    fn pd_servers(&self, warnings: &mut Vec<String>) -> Result<Vec<ClusterServer>, String> {
        let mut servers = Vec::new();
        let first_warning = warnings.len();
        let members = self.members();
        for address in &members {
            let status = self.get(address, "/pd/api/v1/status").and_then(|response| {
                response
                    .json::<Option<PdStatus>>()
                    .map(Option::unwrap_or_default)
                    .map_err(|error| error.to_string())
            });
            match status {
                Ok(status) => servers.push(ClusterServer {
                    server_type: "pd".into(),
                    address: address.clone(),
                    status_address: address.clone(),
                    version: strip_version(&status.version),
                    git_hash: status.git_hash,
                    start_timestamp: status.start_timestamp,
                    ..ClusterServer::default()
                }),
                Err(error) => warnings.push(error),
            }
        }
        if !members.is_empty() && servers.is_empty() {
            return Err(warnings[first_warning..].join("; "));
        }
        Ok(servers)
    }

    fn stores(&self) -> Result<Vec<ClusterStore>, String> {
        Ok(self
            .pd
            .all_store_metadata()
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter_map(project_store)
            .collect())
    }

    fn microservice_servers(
        &self,
        service: &str,
        warnings: &mut Vec<String>,
    ) -> Result<Vec<ClusterServer>, String> {
        for member in self.members() {
            let response = match self.get(&member, &format!("/pd/api/v2/ms/members/{service}")) {
                Ok(response) => response,
                Err(error) => {
                    warnings.push(error);
                    continue;
                }
            };
            if response.status() != reqwest::StatusCode::OK {
                continue;
            }
            match response.json::<Option<Vec<Microservice>>>() {
                Ok(services) => {
                    return Ok(services
                        .unwrap_or_default()
                        .into_iter()
                        .map(|node| ClusterServer {
                            server_type: service.to_owned(),
                            address: strip_scheme(&node.service_addr).to_owned(),
                            status_address: strip_scheme(&node.service_addr).to_owned(),
                            version: strip_version(&node.version),
                            git_hash: node.git_hash,
                            start_timestamp: node.start_timestamp,
                            ..ClusterServer::default()
                        })
                        .collect())
                }
                Err(error) => warnings.push(error.to_string()),
            }
        }
        Ok(Vec::new())
    }
}

fn project_store(store: metapb::Store) -> Option<ClusterStore> {
    // CLUSTER_INFO excludes tombstones, while adaptive reads also exclude
    // removing/removed nodes. Do not reuse routing's lossy store projection.
    if store.state == metapb::StoreState::Tombstone as i32 {
        return None;
    }
    let has_label = |key: &str, value: &str| {
        store
            .labels
            .iter()
            .any(|label| label.key == key && label.value == value)
    };
    let server_type = if has_label("engine", "tiflash") {
        if has_label("engine_role", "write") {
            "tiflash_write"
        } else {
            "tiflash"
        }
    } else if has_label("engine", "tiflash_compute") {
        "tiflash_compute"
    } else {
        "tikv"
    };
    Some(ClusterStore {
        server: ClusterServer {
            server_type: server_type.into(),
            address: store.address,
            status_address: store.status_address,
            version: strip_version(&store.version),
            git_hash: store.git_hash,
            start_timestamp: store.start_timestamp,
            ..ClusterServer::default()
        },
        removing: store.node_state == metapb::NodeState::Removing as i32
            || store.node_state == metapb::NodeState::Removed as i32,
        labels: store
            .labels
            .into_iter()
            .map(|label| (label.key, label.value))
            .collect(),
    })
}

fn strip_scheme(address: &str) -> &str {
    address
        .strip_prefix("http://")
        .or_else(|| address.strip_prefix("https://"))
        .unwrap_or(address)
}
fn strip_version(version: &str) -> String {
    version.strip_prefix('v').unwrap_or(version).to_owned()
}
#[derive(Default, Deserialize)]
#[serde(default)]
struct PdStatus {
    version: String,
    git_hash: String,
    start_timestamp: i64,
}
#[derive(Default, Deserialize)]
#[serde(default, rename_all = "kebab-case")]
struct Microservice {
    service_addr: String,
    version: String,
    git_hash: String,
    start_timestamp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_metadata_preserves_go_component_and_lifecycle_rules() {
        for (engine, role, expected) in [
            ("tikv", "", "tikv"),
            ("tiflash", "", "tiflash"),
            ("tiflash", "write", "tiflash_write"),
            ("tiflash_compute", "write", "tiflash_compute"),
        ] {
            let mut store = metapb::Store {
                address: "[2001:db8::1]:20160".into(),
                status_address: "192.0.2.1:20180".into(),
                version: "v9.0.0-alpha".into(),
                git_hash: "revision".into(),
                start_timestamp: 1234,
                labels: vec![
                    metapb::StoreLabel {
                        key: "engine".into(),
                        value: engine.into(),
                    },
                    metapb::StoreLabel {
                        key: "engine_role".into(),
                        value: role.into(),
                    },
                ],
                ..Default::default()
            };
            let projected = project_store(store.clone()).unwrap();
            assert_eq!(projected.server.server_type, expected);
            assert_eq!(projected.server.version, "9.0.0-alpha");
            assert_eq!(projected.server.git_hash, "revision");
            assert_eq!(projected.server.start_timestamp, 1234);
            for state in [metapb::NodeState::Removing, metapb::NodeState::Removed] {
                store.node_state = state as i32;
                assert!(project_store(store.clone()).unwrap().removing);
            }
            store.state = metapb::StoreState::Tombstone as i32;
            assert!(project_store(store).is_none());
        }
        let pd: PdStatus =
            serde_json::from_str(r#"{"version":"v9","git_hash":"pd","start_timestamp":5}"#)
                .unwrap();
        assert_eq!(
            (
                pd.version.as_str(),
                pd.git_hash.as_str(),
                pd.start_timestamp
            ),
            ("v9", "pd", 5)
        );
        let ms: Microservice = serde_json::from_str(r#"{"service-addr":"https://tso:3379","version":"v9","git-hash":"tso","start-timestamp":6}"#).unwrap();
        assert_eq!(
            (
                strip_scheme(&ms.service_addr),
                ms.git_hash.as_str(),
                ms.start_timestamp
            ),
            ("tso:3379", "tso", 6)
        );
    }
}
