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

//! Go executor.fetchClusterConfig over the process's internal HTTP policy.

use serde::Deserialize;
use tidb_datatype::Datum;
use tidb_domain::cluster_topology::ClusterServer;
use tidb_pd_client::ClusterSecurity;

/// Shared HTTP connection pool; request workers borrow it and join before return.
pub struct ClusterConfigClient {
    http: reqwest::blocking::Client,
    scheme: &'static str,
}

impl ClusterConfigClient {
    /// Uses the same CA, client identity and timeout as other internal HTTP consumers.
    pub fn new(security: &ClusterSecurity) -> Result<Self, String> {
        Ok(Self {
            http: crate::cluster_http::cluster_http_client(security)?,
            scheme: if security.is_tls_enabled() {
                "https"
            } else {
                "http"
            },
        })
    }

    /// Fetches selected nodes concurrently. Node errors become statement warnings;
    /// successful rows retain discovery order and lexically sorted setting keys.
    pub fn fetch(&self, servers: &[ClusterServer], warnings: &mut Vec<String>) -> Vec<Vec<Datum>> {
        std::thread::scope(|scope| {
            let mut workers = Vec::new();
            for server in servers {
                if server.status_address.is_empty() {
                    warnings.push(format!(
                        "{} node {} does not contain status address",
                        server.server_type, server.address
                    ));
                    continue;
                }
                workers.push(scope.spawn(move || self.fetch_node(server)));
            }
            let mut rows = Vec::new();
            for worker in workers {
                match worker.join() {
                    Ok(Ok(node_rows)) => rows.extend(node_rows),
                    Ok(Err(error)) => warnings.push(error),
                    Err(_) => warnings.push("cluster configuration worker panicked".into()),
                }
            }
            rows
        })
    }

    fn fetch_node(&self, server: &ClusterServer) -> Result<Vec<Vec<Datum>>, String> {
        let path = match server.server_type.as_str() {
            "pd" => "/pd/api/v1/config",
            "tidb" | "tikv" | "tiflash" | "ticdc" => "/config",
            "tiproxy" => "/api/admin/config?format=json",
            "tso" => "/tso/api/v1/config",
            "scheduling" => "/scheduling/api/v1/config",
            kind => {
                return Err(format!(
                    "currently we do not support get config from node type: {kind}({})",
                    server.address
                ))
            }
        };
        let url = format!("{}://{}{path}", self.scheme, server.status_address);
        let response = self
            .http
            .get(&url)
            .header("PD-Allow-follower-handle", "true")
            .send()
            .map_err(|error| error.to_string())?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(format!("request {url} failed: {}", response.status()));
        }
        let body = response.bytes().map_err(|error| error.to_string())?;
        // Go Decoder.Decode reads one value, and interface{} numbers are float64.
        let mut decoder = serde_json::Deserializer::from_slice(&body);
        let mut nested =
            Option::<serde_json::Map<String, serde_json::Value>>::deserialize(&mut decoder)
                .map_err(|error| error.to_string())?
                .unwrap_or_default();
        for value in nested.values_mut() {
            decode_go_numbers(value)?;
        }
        let mut items: Vec<_> = tidb_config::config_tree::flatten_config_items(&nested)
            .into_iter()
            .filter(|(key, _)| !tidb_config::config_tree::load::contain_hidden_config(key))
            .collect();
        items.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        items
            .into_iter()
            .map(|(key, value)| {
                let text = match value {
                    serde_json::Value::String(value) => value,
                    other => String::from_utf8(
                        tidb_model::serde_helpers::to_go_json(&other)
                            .map_err(|error| error.to_string())?,
                    )
                    .map_err(|error| error.to_string())?,
                };
                Ok([
                    server.server_type.clone(),
                    server.address.clone(),
                    key,
                    text,
                ]
                .into_iter()
                .map(|s| Datum::Bytes(s.into_bytes()))
                .collect())
            })
            .collect()
    }
}

fn decode_go_numbers(value: &mut serde_json::Value) -> Result<(), String> {
    match value {
        serde_json::Value::Number(number) => {
            let number = number
                .as_f64()
                .and_then(serde_json::Number::from_f64)
                .ok_or("configuration number is outside float64 range")?;
            *value = serde_json::Value::Number(number);
        }
        serde_json::Value::Array(values) => {
            for value in values {
                decode_go_numbers(value)?;
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                decode_go_numbers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
