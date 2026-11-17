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

//! Go `br/pkg/pdutil`: pausing PD's schedulers over a key range with a
//! region label rule (`schedule=deny`) whose TTL is refreshed until released.

use std::time::Duration;

use tidb_log::{Field, Value};
use tidb_store_driver_txn::cluster_http::block_on_pd_http;
use tikv_client::pd_http;

/// Go `pauseTimeout`.
const PAUSE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Go `KeyRangeRule`: one hex-encoded key range of a `key-range` label rule.
#[derive(serde::Serialize)]
struct KeyRangeRule {
    #[serde(rename = "start_key")]
    start_key_hex: String,
    #[serde(rename = "end_key")]
    end_key_hex: String,
}

/// The calls Go's `PauseSchedulersByKeyRange` makes on its `pdhttp.Client`.
pub trait LabelRuleClient: Send + Sync {
    /// Go `pdhttp.Client.SetRegionLabelRule`.
    ///
    /// # Errors
    ///
    /// PD's error.
    fn set_region_label_rule(&self, rule: &pd_http::LabelRule) -> Result<(), String>;
    /// Go `pdhttp.Client.PatchRegionLabelRules`.
    ///
    /// # Errors
    ///
    /// PD's error.
    fn patch_region_label_rules(&self, patch: &pd_http::LabelRulePatch) -> Result<(), String>;
}

impl LabelRuleClient for pd_http::Client {
    fn set_region_label_rule(&self, rule: &pd_http::LabelRule) -> Result<(), String> {
        block_on_pd_http(pd_http::Client::set_region_label_rule(self, rule))
    }

    fn patch_region_label_rules(&self, patch: &pd_http::LabelRulePatch) -> Result<(), String> {
        block_on_pd_http(pd_http::Client::patch_region_label_rules(self, patch))
    }
}

/// What [`pause_schedulers_by_key_range`] hands back: Go's `done` channel
/// together with the caller's `cancel`. Dropping it cancels the refresh loop
/// and waits for the rule's removal (Go `cancel(); <-done`).
pub struct PausedSchedulers {
    cancel: Option<crossbeam_channel::Sender<()>>,
    done: Option<std::thread::JoinHandle<()>>,
}

impl Drop for PausedSchedulers {
    fn drop(&mut self) {
        drop(self.cancel.take());
        if let Some(done) = self.done.take() {
            let _ = done.join();
        }
    }
}

/// Go `PauseSchedulersByKeyRange`: pauses schedulers for regions in the key
/// range until the returned handle drops.
///
/// # Errors
///
/// PD's error setting the label rule.
pub fn pause_schedulers_by_key_range(
    pd_http_cli: std::sync::Arc<dyn LabelRuleClient>,
    start_key: &[u8],
    end_key: &[u8],
) -> Result<PausedSchedulers, String> {
    let paused = pause_scheduler_by_key_range_with_ttl(
        pd_http_cli,
        &[(start_key, end_key)],
        PAUSE_TIMEOUT,
    );
    // Wait for the rule to take effect because the PD operator is processed
    // asynchronously (pingcap/tidb#49477): two times the default
    // `patrol-region-interval`.
    std::thread::sleep(Duration::from_millis(20));
    paused
}

/// Go `pauseSchedulerByKeyRangeWithTTL`.
fn pause_scheduler_by_key_range_with_ttl(
    pd_http_cli: std::sync::Arc<dyn LabelRuleClient>,
    key_range: &[(&[u8], &[u8])],
    ttl: Duration,
) -> Result<PausedSchedulers, String> {
    // no table to restore, return empty rule
    if key_range.is_empty() || key_range[0].0.is_empty() {
        return Ok(PausedSchedulers { cancel: None, done: None });
    }
    let encoded_key_range_rule: Vec<KeyRangeRule> = key_range
        .iter()
        .map(|(start, end)| KeyRangeRule {
            start_key_hex: hex::encode(start),
            end_key_hex: hex::encode(end),
        })
        .collect();
    let mut rule = pd_http::LabelRule {
        id: uuid::Uuid::new_v4().to_string(),
        index: 0,
        labels: vec![pd_http::RegionLabel {
            key: "schedule".to_owned(),
            value: "deny".to_owned(),
            ttl: go_duration_string(ttl),
            start_at: String::new(),
        }],
        rule_type: "key-range".to_owned(),
        // Data should be a list of KeyRangeRule when rule type is key-range.
        data: serde_json::to_value(encoded_key_range_rule).map_err(|error| error.to_string())?,
    };
    pd_http_cli.set_region_label_rule(&rule)?;

    let (cancel, cancelled) = crossbeam_channel::bounded::<()>(0);
    let done = std::thread::spawn(move || {
        loop {
            crossbeam_channel::select! {
                recv(crossbeam_channel::after(ttl / 3)) -> _ => {
                    if let Err(error) = pd_http_cli.set_region_label_rule(&rule) {
                        tidb_log::warn(
                            "pause scheduler by key range failed, ignore it and wait next time pause",
                            &[Field::new("error", Value::Str(error))],
                        );
                    }
                }
                recv(cancelled) -> _ => break,
            }
        }
        // Set ttl to 0 to remove the rule.
        rule.labels[0].ttl = go_duration_string(Duration::ZERO);
        let delete_rule = pd_http::LabelRulePatch {
            set_rules: Vec::new(),
            delete_rules: vec![rule.id.clone()],
        };
        if let Err(error) = pd_http_cli.patch_region_label_rules(&delete_rule) {
            tidb_log::warn(
                "failed to delete region label rule, the rule will be removed after ttl expires",
                &[
                    Field::new("rule-id", Value::Str(rule.id.clone())),
                    Field::new("ttl", Value::Str(go_duration_string(ttl))),
                    Field::new("error", Value::Str(error)),
                ],
            );
        }
    });
    Ok(PausedSchedulers { cancel: Some(cancel), done: Some(done) })
}

/// Go `time.Duration.String` for the whole-second TTLs used here.
fn go_duration_string(duration: Duration) -> String {
    let secs = duration.as_secs();
    match (secs / 3600, secs / 60 % 60, secs % 60) {
        (0, 0, s) => format!("{s}s"),
        (0, m, s) => format!("{m}m{s}s"),
        (h, m, s) => format!("{h}h{m}m{s}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::io::{BufRead, Read, Write};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    /// Go `pd_serial_test.go:TestPauseSchedulersByKeyRange`: the rule is set
    /// with the TTL, refreshed before it expires, and deleted on release.
    #[test]
    fn test_pause_schedulers_by_key_range() {
        let ttl = Duration::from_secs(1);
        let label_expires: Arc<Mutex<(HashMap<String, Instant>, bool)>> = Arc::default();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let state = label_expires.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                let mut reader = std::io::BufReader::new(&mut stream);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                let mut length = 0;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).unwrap();
                    if line == "\r\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
                {
                    let mut state = state.lock().unwrap();
                    let (expires, deleted) = &mut *state;
                    if !*deleted {
                        match request.split_whitespace().next().unwrap() {
                            "PATCH" => {
                                assert_eq!(body["sets"].as_array().map_or(0, Vec::len), 0);
                                let deletes = body["deletes"].as_array().unwrap();
                                assert_eq!(deletes.len(), 1);
                                expires.remove(deletes[0].as_str().unwrap());
                                *deleted = true;
                            }
                            "POST" => {
                                let labels = body["labels"].as_array().unwrap();
                                assert_eq!(labels.len(), 1);
                                assert_eq!(labels[0]["key"], "schedule");
                                assert_eq!(labels[0]["value"], "deny");
                                let id = body["id"].as_str().unwrap().to_owned();
                                if labels[0]["ttl"] == "0s" {
                                    expires.remove(&id);
                                } else {
                                    assert_eq!(labels[0]["ttl"], "1s");
                                    if let Some(expire) = expires.get(&id) {
                                        assert!(*expire > Instant::now(), "should not expire before now");
                                    }
                                    expires.insert(id, Instant::now() + Duration::from_secs(1));
                                }
                            }
                            method => panic!("unexpected {method}"),
                        }
                    }
                }
                write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            }
        });
        let pd_http_cli = tidb_txnkv::driver::tikv_store::new_pd_http_client_with_mock_service_discovery(
            "test",
            &[format!("http://{address}")],
            Vec::new(),
        )
        .unwrap();
        let start_key = [0u8, 0, 0, 0];
        let end_key = [0xffu8, 0xff, 0xff, 0xff];
        let paused =
            pause_scheduler_by_key_range_with_ttl(Arc::new(pd_http_cli), &[(&start_key, &end_key)], ttl).unwrap();
        std::thread::sleep(ttl * 3);
        drop(paused);
        let state = label_expires.lock().unwrap();
        assert!(state.1, "the rule was deleted");
        assert!(state.0.is_empty());
    }

    #[test]
    fn ttl_strings_are_gos() {
        assert_eq!(go_duration_string(PAUSE_TIMEOUT), "5m0s");
        assert_eq!(go_duration_string(Duration::ZERO), "0s");
        assert_eq!(go_duration_string(Duration::from_secs(3600)), "1h0m0s");
    }
}
