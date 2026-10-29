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

//! Go `infosync.PDLabelManager` over PD's region-label HTTP API: pd
//! `client/http`'s `SetRegionLabelRule`, `PatchRegionLabelRules`,
//! `GetAllRegionLabelRules` and `GetRegionLabelRulesByIDs` on the info
//! syncer's derivation of the store's PD HTTP client.
//!
//! Go converts between `label.Rule` and `pd.LabelRule` with a type cast;
//! here a rule is copied field by field into PD's type, and PD's answers are
//! decoded into TiDB's through the JSON shape the two share.

use std::collections::BTreeMap;

use tidb_ddl_label::{LabelCodec, LabelRulePatch, Rule};
use crate::label_manager::{filter_rules_by_keyspace, LabelRuleManager};
use tikv_client::pd_http;

use tidb_txnkv::cluster_http::block_on_pd_http;

/// Go `PDLabelManager`: PD's region-label rules through `pdHTTPCli`.
pub struct PdLabelManager {
    pd_http_client: pd_http::Client,
}

impl std::fmt::Debug for PdLabelManager {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PdLabelManager")
            .finish_non_exhaustive()
    }
}

impl PdLabelManager {
    /// Go `initLabelRuleManager`'s `&PDLabelManager{is.pdHTTPCli}`: the
    /// manager over the info syncer's derivation of the store's PD HTTP
    /// client.
    #[must_use]
    pub fn new(pd_http_client: &pd_http::Client) -> Self {
        Self {
            pd_http_client: crate::pd_http::info_syncer_pd_http_client(pd_http_client),
        }
    }
}

// Go's `(*pd.LabelRule)(rule)` cast.
fn to_pd_label_rule(rule: &Rule) -> pd_http::LabelRule {
    pd_http::LabelRule {
        id: rule.id.clone(),
        index: rule.index,
        labels: rule
            .labels
            .snapshot()
            .into_iter()
            .map(|label| pd_http::RegionLabel {
                key: label.key,
                value: label.value,
                ttl: label.ttl,
                start_at: label.start_at,
            })
            .collect(),
        rule_type: rule.rule_type.clone(),
        data: rule
            .data
            .as_ref()
            .map_or(serde_json::Value::Null, |data| data.read().clone()),
    }
}

// Go's `(*label.Rule)(labelRule)` cast.
fn from_pd_label_rule(label_rule: &pd_http::LabelRule) -> Result<Rule, String> {
    serde_json::to_value(label_rule)
        .and_then(serde_json::from_value)
        .map_err(|error| error.to_string())
}

impl LabelRuleManager for PdLabelManager {
    fn put_label_rule(&self, rule: &Rule) -> Result<(), String> {
        block_on_pd_http(
            self.pd_http_client
                .set_region_label_rule(&to_pd_label_rule(rule)),
        )
    }

    fn update_label_rules(&self, patch: &LabelRulePatch) -> Result<(), String> {
        let patch = pd_http::LabelRulePatch {
            set_rules: patch
                .set_rules
                .snapshot()
                .iter()
                .map(to_pd_label_rule)
                .collect(),
            delete_rules: patch.delete_rules.snapshot(),
        };
        block_on_pd_http(self.pd_http_client.patch_region_label_rules(&patch))
    }

    fn get_all_label_rules(&self, codec: &dyn LabelCodec) -> Result<Vec<Rule>, String> {
        let label_rules = block_on_pd_http(self.pd_http_client.get_all_region_label_rules())?;
        let rules = label_rules
            .iter()
            .map(from_pd_label_rule)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(filter_rules_by_keyspace(rules, codec))
    }

    fn get_label_rules(&self, rule_ids: &[String]) -> Result<BTreeMap<String, Rule>, String> {
        let label_rules =
            block_on_pd_http(self.pd_http_client.get_region_label_rules_by_ids(rule_ids))?;
        label_rules
            .iter()
            .map(|label_rule| Ok((label_rule.id.clone(), from_pd_label_rule(label_rule)?)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread;

    use tidb_ddl_label::{new_rule_patch, RegionLabel};

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

        let pd_http_client =
            tidb_txnkv::driver::tikv_store::new_pd_http_client_with_mock_service_discovery(
                "test",
                &[endpoint],
                Vec::new(),
            )
            .expect("PD HTTP client");
        let manager = PdLabelManager::new(&pd_http_client);
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
