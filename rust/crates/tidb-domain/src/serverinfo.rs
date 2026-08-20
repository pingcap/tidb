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

//! SEED of Go `pkg/domain/serverinfo`: `info.go` (the data model, whole);
//! `syncer.go` is the etcd half — the topology session, TTL refresh, and
//! stale-info cleanup — and rides with the unported etcd client, alongside
//! its integration-bound tests (`syncer_test.go` runs an embedded etcd
//! cluster).
//!
//! The model's wire behavior is exact, including its two quirks:
//! * `ServerInfo.Marshal` reads `ServerIDGetter()` into `JSONServerID`
//!   right before encoding, and `Unmarshal` REBINDS the getter to answer
//!   the decoded id — the function field itself never crosses the wire
//!   (`json:"-"`).
//! * `ToTopologyInfo` reports `mysql.TiDBReleaseVersion` — the BUILD's
//!   release version — not the info's own `Version` field, and its deploy
//!   path is the running executable's directory.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Go `ServerInformationPath`.
pub const SERVER_INFORMATION_PATH: &str = "/tidb/server/info";
/// Go `KeyOpDefaultRetryCnt`.
pub const KEY_OP_DEFAULT_RETRY_CNT: usize = 5;
/// Go `KeyOpDefaultTimeout`.
pub const KEY_OP_DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
/// Go `TopologyInformationPath`.
pub const TOPOLOGY_INFORMATION_PATH: &str = "/topology/tidb";
/// Go `TopologySessionTTL` (seconds).
pub const TOPOLOGY_SESSION_TTL: u64 = 45;
/// Go `TopologyTimeToRefresh`.
pub const TOPOLOGY_TIME_TO_REFRESH: Duration = Duration::from_secs(30);

/// Go `VersionInfo`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct VersionInfo {
    /// The server version.
    pub version: String,
    /// The build's git hash.
    pub git_hash: String,
}

/// Go `StaticInfo`: generated at startup, never modified while running.
/// `ServerIDGetter` is carried as an optional closure exactly as Go's
/// `json:"-"` function field: absent from the wire, rebound on decode.
#[derive(Clone, Default, Deserialize, Serialize)]
pub struct StaticInfo {
    /// Go's embedded `VersionInfo`.
    #[serde(flatten)]
    pub version_info: VersionInfo,
    /// Go `ID` (`ddl_id`).
    #[serde(rename = "ddl_id")]
    pub id: String,
    /// Go `IP`.
    pub ip: String,
    /// Go `Port` (`listening_port`).
    #[serde(rename = "listening_port")]
    pub port: u32,
    /// Go `StatusPort`.
    pub status_port: u32,
    /// Go `Lease`.
    pub lease: String,
    /// Go `StartTimestamp`.
    pub start_timestamp: i64,
    /// Go `Keyspace`, always empty in the classic kernel.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub keyspace: String,
    /// Go `AssumedKeyspace`, the cross-keyspace impersonation name.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub assumed_keyspace: String,
    /// Go `ServerIDGetter` (`json:"-"`).
    #[serde(skip)]
    pub server_id_getter: Option<std::sync::Arc<dyn Fn() -> u64 + Send + Sync>>,
    /// Go `JSONServerID`: the marshal/unmarshal carrier for the id.
    #[serde(rename = "server_id", default)]
    pub json_server_id: u64,
}

impl std::fmt::Debug for StaticInfo {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StaticInfo")
            .field("version_info", &self.version_info)
            .field("id", &self.id)
            .field("ip", &self.ip)
            .field("port", &self.port)
            .field("status_port", &self.status_port)
            .field("lease", &self.lease)
            .field("start_timestamp", &self.start_timestamp)
            .field("keyspace", &self.keyspace)
            .field("assumed_keyspace", &self.assumed_keyspace)
            // The getter prints by presence, as Go's `%v` of a func value
            // is an address, not content.
            .field("server_id_getter", &self.server_id_getter.is_some())
            .field("json_server_id", &self.json_server_id)
            .finish()
    }
}

impl StaticInfo {
    /// Go `IsAssumed`.
    #[must_use]
    pub fn is_assumed(&self) -> bool {
        !self.assumed_keyspace.is_empty()
    }
}

/// Go `DynamicInfo`: may change while running.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct DynamicInfo {
    /// Go `Labels`.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

impl DynamicInfo {
    /// Go `Clone`.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        self.clone()
    }
}

/// Go `TopologyInfo`.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct TopologyInfo {
    /// Go's embedded `VersionInfo`.
    #[serde(flatten)]
    pub version_info: VersionInfo,
    /// Go `IP`.
    pub ip: String,
    /// Go `StatusPort`.
    pub status_port: u32,
    /// Go `DeployPath`.
    pub deploy_path: String,
    /// Go `StartTimestamp`.
    pub start_timestamp: i64,
    /// Go `Labels`.
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

/// Go `ServerInfo`: the static and dynamic sections together.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ServerInfo {
    /// Go's embedded `StaticInfo`.
    #[serde(flatten)]
    pub static_info: StaticInfo,
    /// Go's embedded `DynamicInfo`.
    #[serde(flatten)]
    pub dynamic_info: DynamicInfo,
}

impl ServerInfo {
    /// Go `Clone`.
    #[must_use]
    pub fn clone_like_go(&self) -> Self {
        self.clone()
    }

    /// Go `Marshal`: read the getter into `JSONServerID`, then encode.
    pub fn marshal(&mut self) -> Result<Vec<u8>, serde_json::Error> {
        if let Some(getter) = &self.static_info.server_id_getter {
            self.static_info.json_server_id = getter();
        }
        serde_json::to_vec(self)
    }

    /// Go `Unmarshal`: decode, then REBIND the getter to the decoded id.
    pub fn unmarshal(&mut self, bytes: &[u8]) -> Result<(), serde_json::Error> {
        *self = serde_json::from_slice(bytes)?;
        let id = self.static_info.json_server_id;
        self.static_info.server_id_getter = Some(std::sync::Arc::new(move || id));
        Ok(())
    }

    /// Go `ToTopologyInfo`: the RELEASE version (not this info's own), the
    /// executable's directory as the deploy path.
    #[must_use]
    pub fn to_topology_info(&self) -> TopologyInfo {
        let deploy_path = std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|dir| dir.to_string_lossy().into_owned()))
            .unwrap_or_default();
        TopologyInfo {
            version_info: VersionInfo {
                version: tidb_mysql::runtime_versions().tidb_release_version,
                git_hash: self.static_info.version_info.git_hash.clone(),
            },
            ip: self.static_info.ip.clone(),
            status_port: self.static_info.status_port,
            deploy_path,
            start_timestamp: self.static_info.start_timestamp,
            labels: self.dynamic_info.labels.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> ServerInfo {
        ServerInfo {
            static_info: StaticInfo {
                version_info: VersionInfo {
                    version: "v0.test".to_owned(),
                    git_hash: "abcd".to_owned(),
                },
                id: "ddl-1".to_owned(),
                ip: "10.0.0.7".to_owned(),
                port: 4000,
                status_port: 10080,
                lease: "45s".to_owned(),
                start_timestamp: 1_282_967_700,
                keyspace: String::new(),
                assumed_keyspace: String::new(),
                server_id_getter: Some(std::sync::Arc::new(|| 42)),
                json_server_id: 0,
            },
            dynamic_info: DynamicInfo {
                labels: [("foo".to_owned(), "bar".to_owned())].into_iter().collect(),
            },
        }
    }

    /// Go `Marshal`/`Unmarshal`: the getter's value crosses as
    /// `server_id`, the function itself never does, and decode REBINDS a
    /// getter answering the stored id.
    #[test]
    fn the_server_id_getter_round_trips_through_json() {
        let mut info = sample();
        let bytes = info.marshal().expect("encodes");
        let text = String::from_utf8(bytes.clone()).expect("utf8");
        assert!(text.contains("\"server_id\":42"), "{text}");
        assert!(text.contains("\"ddl_id\":\"ddl-1\""), "{text}");
        assert!(text.contains("\"listening_port\":4000"), "{text}");
        // Go omits an empty keyspace entirely (`omitempty`).
        assert!(!text.contains("keyspace"), "{text}");

        let mut decoded = ServerInfo::default();
        decoded.unmarshal(&bytes).expect("decodes");
        assert_eq!(decoded.static_info.json_server_id, 42);
        let getter = decoded
            .static_info
            .server_id_getter
            .as_ref()
            .expect("rebound");
        assert_eq!(getter(), 42);
        assert_eq!(decoded.dynamic_info.labels["foo"], "bar");
    }

    /// Go `ToTopologyInfo`: the topology's version is the BUILD release
    /// version, not the info's own; the deploy path is the executable's
    /// directory; start timestamp and labels carry over (the values
    /// `TestTopology` pins against its mock).
    #[test]
    fn topology_info_reports_the_release_version_and_deploy_path() {
        let topology = sample().to_topology_info();
        assert_eq!(
            topology.version_info.version,
            tidb_mysql::runtime_versions().tidb_release_version
        );
        assert_ne!(topology.version_info.version, "v0.test");
        assert_eq!(topology.version_info.git_hash, "abcd");
        assert_eq!(topology.start_timestamp, 1_282_967_700);
        assert_eq!(topology.labels["foo"], "bar");
        assert!(!topology.deploy_path.is_empty());
    }

    /// Go `DynamicInfo.Clone` and `ServerInfo.Clone`: label maps are
    /// INDEPENDENT copies.
    #[test]
    fn clones_do_not_share_their_label_maps() {
        let info = sample();
        let mut cloned = info.clone_like_go();
        cloned
            .dynamic_info
            .labels
            .insert("only".to_owned(), "clone".to_owned());
        assert!(!info.dynamic_info.labels.contains_key("only"));
        assert!(info.static_info.is_assumed() == false);
    }
}
