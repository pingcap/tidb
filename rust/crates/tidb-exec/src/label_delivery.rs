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

//! Go `infosync.PDLabelManager` over PD's region-label HTTP API.

use std::time::Duration;

use tidb_executor::ddl_label::{LabelRulePatch, Rule};

/// A PD label-rule request failure.
#[derive(Debug)]
pub enum LabelDeliveryError {
    /// The request or response JSON could not be encoded or decoded.
    Codec(String),
    /// The request could not be sent.
    Transport(String),
    /// PD rejected the request.
    Rejected {
        /// HTTP status returned by PD.
        status: u16,
        /// PD's diagnostic response body.
        detail: String,
    },
    /// The synchronous DDL path could not create its request runtime.
    Runtime(String),
}

impl std::fmt::Display for LabelDeliveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Codec(detail) => write!(formatter, "label-rule JSON: {detail}"),
            Self::Transport(detail) => write!(formatter, "sending label-rule request: {detail}"),
            Self::Rejected { status, detail } => {
                write!(
                    formatter,
                    "PD rejected the label-rule request ({status}): {detail}"
                )
            }
            Self::Runtime(detail) => write!(formatter, "label-rule request runtime: {detail}"),
        }
    }
}

fn runtime() -> Result<tokio::runtime::Runtime, LabelDeliveryError> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| LabelDeliveryError::Runtime(error.to_string()))
}

fn rules_url(endpoint: &str, suffix: &str) -> String {
    format!(
        "{}/pd/api/v1/config/region-label/rules{suffix}",
        endpoint.trim_end_matches('/')
    )
}

/// Go `PDLabelManager.GetLabelRules`.
pub fn get_label_rules(
    endpoint: &str,
    rule_ids: &[String],
    timeout: Duration,
) -> Result<Vec<Rule>, LabelDeliveryError> {
    let body = serde_json::to_vec(rule_ids)
        .map_err(|error| LabelDeliveryError::Codec(error.to_string()))?;
    let url = rules_url(endpoint, "/ids");
    runtime()?.block_on(async move {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| LabelDeliveryError::Transport(error.to_string()))?;
        let response = client
            .get(url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| LabelDeliveryError::Transport(error.to_string()))?;
        let status = response.status();
        let response_body = response
            .bytes()
            .await
            .map_err(|error| LabelDeliveryError::Transport(error.to_string()))?;
        if !status.is_success() {
            return Err(LabelDeliveryError::Rejected {
                status: status.as_u16(),
                detail: String::from_utf8_lossy(&response_body).into_owned(),
            });
        }
        serde_json::from_slice(&response_body)
            .map_err(|error| LabelDeliveryError::Codec(error.to_string()))
    })
}

/// Go `PDLabelManager.UpdateLabelRules`.
pub fn patch_label_rules(
    endpoint: &str,
    patch: &LabelRulePatch,
    timeout: Duration,
) -> Result<(), LabelDeliveryError> {
    let body =
        serde_json::to_vec(patch).map_err(|error| LabelDeliveryError::Codec(error.to_string()))?;
    let url = rules_url(endpoint, "");
    runtime()?.block_on(async move {
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|error| LabelDeliveryError::Transport(error.to_string()))?;
        let response = client
            .patch(url)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| LabelDeliveryError::Transport(error.to_string()))?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let detail = response.text().await.unwrap_or_default();
        Err(LabelDeliveryError::Rejected {
            status: status.as_u16(),
            detail,
        })
    })
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
    fn get_and_patch_use_pds_exact_region_label_api() {
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
        });

        let ids = vec!["schema/db/t".to_owned()];
        let rules = get_label_rules(&endpoint, &ids, Duration::from_secs(2)).expect("GET rules");
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
        patch_label_rules(&endpoint, &patch, Duration::from_secs(2)).expect("PATCH rules");
        patch_label_rules(
            &endpoint,
            &new_rule_patch(Vec::new(), Vec::new()),
            Duration::from_secs(2),
        )
        .expect("empty PATCH rules");
        server.join().expect("test PD exits");
    }
}
