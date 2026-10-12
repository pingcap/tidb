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

//! Go `infosync.PDLabelManager` over PD's region-label HTTP API (pd
//! `client/http`'s `SetRegionLabelRule`, `PatchRegionLabelRules`,
//! `GetAllRegionLabelRules` and `GetRegionLabelRulesByIDs`), over the node's
//! cluster HTTP client (the CA and client identity PD discovery uses).

use std::collections::BTreeMap;
use std::time::Duration;

use tidb_executor::ddl_label::{LabelCodec, LabelRulePatch, Rule};
use tidb_executor::label_manager::{filter_rules_by_keyspace, LabelRuleManager};

/// pd `RegionLabelRule`.
const REGION_LABEL_RULE: &str = "/pd/api/v1/config/region-label/rule";
/// pd `RegionLabelRules`.
const REGION_LABEL_RULES: &str = "/pd/api/v1/config/region-label/rules";
/// pd `RegionLabelRulesByIDs`.
const REGION_LABEL_RULES_BY_IDS: &str = "/pd/api/v1/config/region-label/rules/ids";
/// The request timeout the node's other PD HTTP calls use.
const PD_HTTP_TIMEOUT: Duration = Duration::from_secs(30);

/// Go `PDLabelManager`: PD's region-label rules at one endpoint.
pub struct PdLabelManager {
    endpoint: String,
    http: reqwest::blocking::Client,
}

impl std::fmt::Debug for PdLabelManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PdLabelManager")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl PdLabelManager {
    /// The manager for `pd_http` (`http://host:port`), switched to HTTPS and
    /// given the cluster identity when `security` enables TLS.
    pub fn with_security(
        pd_http: &str,
        security: &tidb_pd_client::ClusterSecurity,
    ) -> Result<Self, String> {
        let endpoint = if security.is_tls_enabled() {
            pd_http.replacen("http://", "https://", 1)
        } else {
            pd_http.to_owned()
        };
        Ok(Self {
            endpoint: endpoint.trim_end_matches('/').to_owned(),
            http: crate::cluster_http::cluster_http_client(security)?,
        })
    }

    /// pd `client.request`: a non-200 answer is the error
    /// `request pd http api failed with status: '<status>', body: '<body>'`.
    fn request(
        &self,
        method: reqwest::Method,
        uri: &str,
        body: Option<Vec<u8>>,
    ) -> Result<Vec<u8>, String> {
        let mut request = self
            .http
            .request(method, format!("{}{uri}", self.endpoint))
            .timeout(PD_HTTP_TIMEOUT);
        if let Some(body) = body {
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let response = request.send().map_err(|error| error.to_string())?;
        let status = response.status();
        let bytes = response
            .bytes()
            .map_err(|error| error.to_string())?
            .to_vec();
        if status != reqwest::StatusCode::OK {
            return Err(format!(
                "request pd http api failed with status: '{status}', body: '{}'",
                String::from_utf8_lossy(&bytes)
            ));
        }
        Ok(bytes)
    }
}

impl LabelRuleManager for PdLabelManager {
    fn put_label_rule(&self, rule: &Rule) -> Result<(), String> {
        let body = serde_json::to_vec(rule).map_err(|error| error.to_string())?;
        self.request(reqwest::Method::POST, REGION_LABEL_RULE, Some(body))
            .map(|_| ())
    }

    fn update_label_rules(&self, patch: &LabelRulePatch) -> Result<(), String> {
        let body = serde_json::to_vec(patch).map_err(|error| error.to_string())?;
        self.request(reqwest::Method::PATCH, REGION_LABEL_RULES, Some(body))
            .map(|_| ())
    }

    fn get_all_label_rules(&self, codec: &dyn LabelCodec) -> Result<Vec<Rule>, String> {
        let bytes = self.request(reqwest::Method::GET, REGION_LABEL_RULES, None)?;
        let rules: Option<Vec<Rule>> =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        Ok(filter_rules_by_keyspace(rules.unwrap_or_default(), codec))
    }

    fn get_label_rules(&self, rule_ids: &[String]) -> Result<BTreeMap<String, Rule>, String> {
        let body = serde_json::to_vec(rule_ids).map_err(|error| error.to_string())?;
        let bytes = self.request(reqwest::Method::GET, REGION_LABEL_RULES_BY_IDS, Some(body))?;
        let rules: Option<Vec<Rule>> =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        Ok(rules
            .unwrap_or_default()
            .into_iter()
            .map(|rule| (rule.id.clone(), rule))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use tidb_executor::ddl_label::{new_rule_patch, RegionLabel};

    use super::*;

    fn read_request(stream: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            let read = stream.read(&mut buffer).expect("read request");
            request.extend_from_slice(&buffer[..read]);
            let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|length| length.trim().parse::<usize>().ok())
                })
                .unwrap_or(0);
            if request.len() >= header_end + 4 + content_length {
                return request;
            }
        }
    }

    #[test]
    fn get_patch_and_put_use_pds_exact_region_label_api() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test PD");
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = thread::spawn(move || {
            let (mut get, _) = listener.accept().expect("GET connection");
            let request = read_request(&mut get);
            let request = String::from_utf8(request).expect("GET is text");
            assert!(
                request.starts_with("GET /pd/api/v1/config/region-label/rules/ids HTTP/1.1\r\n")
            );
            assert!(request.ends_with("[\"schema/db/t\"]"));
            let body = r#"[{"id":"schema/db/t","index":2,"labels":[{"key":"zone","value":"z1"}],"rule_type":"key-range","data":[]}]"#;
            write!(
                get,
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("GET response");

            let (mut patch, _) = listener.accept().expect("PATCH connection");
            let request = read_request(&mut patch);
            let request = String::from_utf8(request).expect("PATCH is text");
            assert!(request.starts_with("PATCH /pd/api/v1/config/region-label/rules HTTP/1.1\r\n"));
            let body = request.split("\r\n\r\n").nth(1).expect("PATCH body");
            let body: serde_json::Value = serde_json::from_str(body).expect("PATCH JSON");
            assert_eq!(body["sets"][0]["id"], "schema/db/t");
            assert_eq!(body["deletes"][0], "schema/db/gone");
            write!(
                patch,
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .expect("PATCH response");

            let (mut empty_patch, _) = listener.accept().expect("empty PATCH connection");
            let request = read_request(&mut empty_patch);
            let request = String::from_utf8(request).expect("empty PATCH is text");
            assert!(request.starts_with("PATCH /pd/api/v1/config/region-label/rules HTTP/1.1\r\n"));
            let body: serde_json::Value =
                serde_json::from_str(request.split("\r\n\r\n").nth(1).expect("empty PATCH body"))
                    .expect("empty PATCH JSON");
            assert_eq!(body["sets"], serde_json::json!([]));
            assert_eq!(body["deletes"], serde_json::json!([]));
            write!(
                empty_patch,
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .expect("empty PATCH response");

            let (mut post, _) = listener.accept().expect("POST connection");
            let request = read_request(&mut post);
            let request = String::from_utf8(request).expect("POST is text");
            assert!(request.starts_with("POST /pd/api/v1/config/region-label/rule HTTP/1.1\r\n"));
            write!(
                post,
                "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .expect("POST response");
        });

        let manager =
            PdLabelManager::with_security(&endpoint, &tidb_pd_client::ClusterSecurity::default())
                .expect("manager");
        let ids = vec!["schema/db/t".to_owned()];
        let rules = manager.get_label_rules(&ids).expect("GET rules");
        assert_eq!(rules.len(), 1);
        let patch = new_rule_patch(
            vec![Rule {
                id: "schema/db/t".to_owned(),
                labels: vec![RegionLabel {
                    key: "zone".to_owned(),
                    value: "z1".to_owned(),
                    ..RegionLabel::default()
                }]
                .into(),
                ..Rule::default()
            }],
            vec!["schema/db/gone".to_owned()],
        );
        manager.update_label_rules(&patch).expect("PATCH rules");
        manager
            .update_label_rules(&new_rule_patch(Vec::new(), Vec::new()))
            .expect("empty PATCH rules");
        manager
            .put_label_rule(&Rule {
                id: "schema/db/t".to_owned(),
                ..Rule::default()
            })
            .expect("POST rule");
        server.join().expect("test PD exits");
    }
}
