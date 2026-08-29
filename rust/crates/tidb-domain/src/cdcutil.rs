// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! TiCDC changefeed compatibility checks from `pkg/util/cdcutil`.

use std::collections::HashMap;
use std::fmt::Write as _;

use serde::Deserialize;

use crate::serverinfo_syncer::EtcdOps;

/// Prefix of TiCDC information in etcd.
pub const CDC_PREFIX: &str = "/tidb/cdc/";
/// Path component of changefeed information in etcd.
pub const CHANGEFEED_PATH: &str = "/changefeed/info/";
/// Legacy TiCDC changefeed-information prefix.
pub const CDC_PREFIX_V61: &str = "/tidb/cdc/changefeed/info/";

const INVALID_TS: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyVersion {
    Legacy,
    Namespaced,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Changefeed {
    id: String,
    cluster: String,
    namespace: String,
    key_version: KeyVersion,
}

impl std::fmt::Display for Changefeed {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let version = match self.key_version {
            KeyVersion::Legacy => 1,
            KeyVersion::Namespaced => 2,
        };
        write!(
            formatter,
            "{{{} {} {} {version}}}",
            self.id, self.cluster, self.namespace
        )
    }
}

impl Changefeed {
    fn info_key(&self) -> String {
        match self.key_version {
            KeyVersion::Legacy => format!("{CDC_PREFIX_V61}{}", self.id),
            KeyVersion::Namespaced => format!(
                "{CDC_PREFIX}{}/{}/changefeed/info/{}",
                self.cluster, self.namespace, self.id
            ),
        }
    }

    fn status_key(&self) -> String {
        match self.key_version {
            KeyVersion::Legacy => format!("{CDC_PREFIX}changefeed/status/{}", self.id),
            KeyVersion::Namespaced => format!(
                "{CDC_PREFIX}{}/{}/changefeed/status/{}",
                self.cluster, self.namespace, self.id
            ),
        }
    }
}

#[derive(Deserialize)]
struct ChangefeedInfoView {
    state: String,
    #[serde(rename = "start-ts", default)]
    start: u64,
}

#[derive(Deserialize)]
struct ChangefeedStatusView {
    #[serde(rename = "checkpoint-ts", default)]
    checkpoint: u64,
}

/// TiCDC changefeeds grouped by `cluster/namespace`.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct CDCNameSet {
    changefeeds: HashMap<String, Vec<String>>,
}

impl CDCNameSet {
    fn save(&mut self, changefeed: &Changefeed) {
        let key = match changefeed.key_version {
            KeyVersion::Legacy => "<nil>".to_owned(),
            KeyVersion::Namespaced => format!("{}/{}", changefeed.cluster, changefeed.namespace),
        };
        self.changefeeds
            .entry(key)
            .or_default()
            .push(changefeed.id.clone());
    }

    /// Returns true when no changefeed is stored.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changefeeds.is_empty()
    }

    /// Converts the set to TiDB's user-facing message.
    #[must_use]
    pub fn message_to_user(&self) -> String {
        let mut output = "found CDC changefeed(s): ".to_owned();
        for (cluster_namespace, changefeeds) in &self.changefeeds {
            write!(
                output,
                "cluster/namespace: {cluster_namespace} changefeed(s): [{}], ",
                changefeeds.join(" ")
            )
            .expect("writing to String cannot fail");
        }
        output
    }

    #[cfg(test)]
    fn changefeed_names(&self) -> Vec<String> {
        self.changefeeds
            .iter()
            .flat_map(|(namespace, changefeeds)| {
                changefeeds
                    .iter()
                    .map(move |changefeed| format!("{namespace}/{changefeed}"))
            })
            .collect()
    }
}

fn valid_cluster_name(cluster: &str) -> bool {
    !cluster.is_empty()
        && cluster
            .split('-')
            .all(|part| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_alphanumeric()))
}

fn load_changefeeds(etcd: &dyn EtcdOps) -> Result<Vec<Changefeed>, String> {
    let mut output = Vec::new();
    for (key, _) in etcd.get_prefix(CDC_PREFIX)? {
        let Some(relative) = key.strip_prefix(CDC_PREFIX.trim_end_matches('/')) else {
            continue;
        };
        let Some((cluster_and_namespace, id)) = relative.split_once(CHANGEFEED_PATH) else {
            continue;
        };
        if cluster_and_namespace.is_empty() {
            output.push(Changefeed {
                id: id.to_owned(),
                cluster: String::new(),
                namespace: String::new(),
                key_version: KeyVersion::Legacy,
            });
            continue;
        }
        let Some((cluster, namespace)) = cluster_and_namespace
            .strip_prefix('/')
            .and_then(|value| value.split_once('/'))
        else {
            continue;
        };
        if !valid_cluster_name(cluster) {
            continue;
        }
        output.push(Changefeed {
            id: id.to_owned(),
            cluster: cluster.to_owned(),
            namespace: namespace.to_owned(),
            key_version: KeyVersion::Namespaced,
        });
    }
    Ok(output)
}

fn exact_value(etcd: &dyn EtcdOps, key: &str) -> Result<Option<Vec<u8>>, String> {
    Ok(etcd
        .get_prefix(key)?
        .into_iter()
        .find_map(|(candidate, value)| (candidate == key).then_some(value)))
}

fn checkpoint_ts_for(etcd: &dyn EtcdOps, changefeed: &Changefeed) -> Result<u64, String> {
    let Some(info) = exact_value(etcd, &changefeed.info_key())? else {
        return Ok(INVALID_TS);
    };
    let info: ChangefeedInfoView =
        serde_json::from_slice(&info).map_err(|error| error.to_string())?;
    match info.state.as_str() {
        "finished" => Ok(INVALID_TS),
        "failed" | "running" | "warning" | "normal" | "stopped" | "error" => {
            let checkpoint = match exact_value(etcd, &changefeed.status_key())? {
                None => 0,
                Some(status) if status.is_empty() => 0,
                Some(status) => {
                    serde_json::from_slice::<ChangefeedStatusView>(&status)
                        .map_err(|error| error.to_string())?
                        .checkpoint
                }
            };
            Ok(checkpoint.max(info.start))
        }
        _ => {
            tracing::warn!(changefeed = %changefeed, state = info.state, "Ignoring invalid changefeed.");
            Ok(INVALID_TS)
        }
    }
}

fn incompatible_changefeeds(etcd: &dyn EtcdOps, safe_ts: u64) -> Result<CDCNameSet, String> {
    let mut names = CDCNameSet::default();
    for changefeed in load_changefeeds(etcd)? {
        let checkpoint = checkpoint_ts_for(etcd, &changefeed)
            .map_err(|error| format!("failed to check changefeed {changefeed}: {error}"))?;
        if checkpoint < safe_ts {
            tracing::info!(changefeed = %changefeed, checkpoint_ts = checkpoint, safe_ts, "Found incompatible changefeed.");
            names.save(&changefeed);
        }
    }
    Ok(names)
}

/// Gets all running TiCDC changefeeds.
pub fn get_running_changefeeds(etcd: &dyn EtcdOps) -> Result<CDCNameSet, String> {
    incompatible_changefeeds(etcd, INVALID_TS)
}

/// Gets TiCDC changefeeds whose checkpoint is older than `safe_ts`.
pub fn get_incompatible_changefeeds_with_safe_ts(
    etcd: &dyn EtcdOps,
    safe_ts: u64,
) -> Result<CDCNameSet, String> {
    incompatible_changefeeds(etcd, safe_ts)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::*;

    #[derive(Default)]
    struct FakeEtcd {
        values: Mutex<BTreeMap<String, Vec<u8>>>,
    }

    impl FakeEtcd {
        fn put_value(&self, key: &str, value: &str) {
            self.values
                .lock()
                .unwrap()
                .insert(key.to_owned(), value.as_bytes().to_vec());
        }

        fn clear(&self) {
            self.values.lock().unwrap().clear();
        }
    }

    impl EtcdOps for FakeEtcd {
        fn lease_grant(&self, _: i64) -> Result<i64, String> {
            unreachable!()
        }
        fn lease_keep_alive_once(&self, _: i64) -> Result<(), String> {
            unreachable!()
        }
        fn lease_revoke(&self, _: i64) -> Result<(), String> {
            unreachable!()
        }
        fn put_with_lease(&self, _: &str, _: &[u8], _: i64) -> Result<(), String> {
            unreachable!()
        }
        fn get_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>, String> {
            Ok(self
                .values
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
        fn delete(&self, _: &str) -> Result<(), String> {
            unreachable!()
        }
        fn put(&self, _: &str, _: &[u8]) -> Result<(), String> {
            unreachable!()
        }
        fn delete_prefix(&self, _: &str) -> Result<(), String> {
            unreachable!()
        }
    }

    fn sorted(mut values: Vec<String>) -> Vec<String> {
        values.sort();
        values
    }

    #[test]
    fn test_cdc_check_with_embed_etcd() {
        let etcd = FakeEtcd::default();
        assert!(get_running_changefeeds(&etcd).unwrap().is_empty());

        etcd.put_value(
            "/tidb/cdc/default/default/changefeed/info/test",
            r#"{"state":"normal"}"#,
        );
        etcd.put_value(
            "/tidb/cdc/default/default/changefeed/info/test-1",
            r#"{"state":"finished"}"#,
        );
        etcd.put_value("/tidb/cdc/default/default/changefeed/status/test-1", "");
        assert_eq!(
            get_running_changefeeds(&etcd).unwrap().changefeed_names(),
            ["default/default/test"]
        );

        etcd.clear();
        etcd.put_value("/tidb/cdc/changefeed/info/test", r#"{"state":"stopped"}"#);
        assert_eq!(
            get_running_changefeeds(&etcd).unwrap().changefeed_names(),
            ["<nil>/test"]
        );

        etcd.clear();
        etcd.put_value(
            "/tidb/cdc/__backup__/changefeed/info/test",
            r#"{"state":"normal"}"#,
        );
        etcd.put_value(
            "/tidb/cdc/5402613591834624000/changefeed/info/test",
            r#"{"state":"normal"}"#,
        );
        assert!(get_running_changefeeds(&etcd).unwrap().is_empty());

        etcd.clear();
        for (name, state, start, checkpoint) in [
            ("st-ok", "normal", 1, Some(43)),
            ("st-fail", "normal", 1, Some(41)),
            ("skipped", "finished", 1, None),
            ("not-skipped", "failed", 1, Some(41)),
            ("nost-ok", "normal", 43, None),
            ("nost-fail", "normal", 41, None),
        ] {
            etcd.put_value(
                &format!("/tidb/cdc/default/default/changefeed/info/{name}"),
                &format!(r#"{{"state":"{state}","start-ts":{start}}}"#),
            );
            if let Some(checkpoint) = checkpoint {
                etcd.put_value(
                    &format!("/tidb/cdc/default/default/changefeed/status/{name}"),
                    &format!(r#"{{"checkpoint-ts":{checkpoint}}}"#),
                );
            }
        }
        assert_eq!(
            sorted(
                get_incompatible_changefeeds_with_safe_ts(&etcd, 42)
                    .unwrap()
                    .changefeed_names()
            ),
            [
                "default/default/nost-fail",
                "default/default/not-skipped",
                "default/default/st-fail"
            ]
        );
        assert!(get_incompatible_changefeeds_with_safe_ts(&etcd, 40)
            .unwrap()
            .is_empty());
        assert_eq!(
            sorted(
                get_incompatible_changefeeds_with_safe_ts(&etcd, 48)
                    .unwrap()
                    .changefeed_names()
            ),
            [
                "default/default/nost-fail",
                "default/default/nost-ok",
                "default/default/not-skipped",
                "default/default/st-fail",
                "default/default/st-ok"
            ]
        );
    }
}
