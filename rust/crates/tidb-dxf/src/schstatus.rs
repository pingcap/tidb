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

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, FixedOffset};
use serde::{Deserialize, Serialize};

use crate::task::{format_rfc3339_nano, go_zero_time};

/// Go `Version`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Version(pub isize);

/// Go `Version1`.
pub const VERSION1: Version = Version(1);

/// Go `TaskQueue`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct TaskQueue {
    /// Number of scheduled tasks in running or modifying state.
    #[serde(default, skip_serializing_if = "is_zero_isize")]
    pub scheduled_count: isize,
}

/// Go `Node`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct Node {
    /// Node identifier, equal to the subtask table's `exec_id`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// Whether this node owns DXF.
    #[serde(default, skip_serializing_if = "is_false")]
    pub is_owner: bool,
}

/// Go `NodeGroup`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct NodeGroup {
    /// CPUs available on a node.
    #[serde(default, skip_serializing_if = "is_zero_isize")]
    pub cpu_count: isize,
    /// Nodes required to run scheduled tasks.
    #[serde(default, skip_serializing_if = "is_zero_isize")]
    pub required_count: isize,
    /// Currently available nodes.
    #[serde(default, skip_serializing_if = "is_zero_isize")]
    pub current_count: isize,
    /// Nodes currently running subtasks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub busy_nodes: Vec<Node>,
}

go_string_type! {
    /// Go `Flag`.
    Flag
}

/// Go `PauseScaleInFlag`.
pub const PAUSE_SCALE_IN_FLAG: Flag = Flag::from_static("pause_scale_in");

/// Go `TTLInfo`.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct TtlInfo {
    /// Go duration in nanoseconds.
    #[serde(rename = "ttl", default, skip_serializing_if = "is_zero_i64")]
    pub ttl_nanoseconds: i64,
    /// Flag or tuning-factor expiration time.
    #[serde(
        default = "go_zero_time",
        serialize_with = "serialize_go_time",
        deserialize_with = "deserialize_go_time"
    )]
    pub expire_time: DateTime<FixedOffset>,
}

impl Default for TtlInfo {
    fn default() -> Self {
        Self {
            ttl_nanoseconds: 0,
            expire_time: go_zero_time(),
        }
    }
}

/// Go `TTLFlag`.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct TtlFlag {
    /// Whether the flag is enabled.
    #[serde(default, skip_serializing_if = "is_false")]
    pub enabled: bool,
    /// Embedded Go `TTLInfo`.
    #[serde(flatten)]
    pub ttl_info: TtlInfo,
}

impl TtlFlag {
    /// Go `(*TTLFlag).String`.
    #[must_use]
    pub fn string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// Go `Status`.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct Status {
    /// Status format version.
    #[serde(default, skip_serializing_if = "is_zero_version")]
    pub version: Version,
    /// Task queue status.
    pub task_queue: TaskQueue,
    /// TiDB worker resources.
    pub tidb_worker: NodeGroup,
    /// TiKV worker resources.
    pub tikv_worker: NodeGroup,
    /// Scheduler flags keyed by flag name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub flags: BTreeMap<Flag, TtlFlag>,
}

impl Status {
    /// Go `(*Status).String`.
    #[must_use]
    pub fn string(&mut self) -> String {
        if self.tidb_worker.busy_nodes.len() > 5 {
            let total = self.tidb_worker.busy_nodes.len();
            self.tidb_worker.busy_nodes[5] = Node {
                id: format!("... too many nodes, total {total} busy nodes ..."),
                is_owner: false,
            };
            let mut status = self.clone();
            status.tidb_worker.busy_nodes.truncate(6);
            return serde_json::to_string(&status).unwrap_or_default();
        }
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// Go `MinAmplifyFactor`.
pub const MIN_AMPLIFY_FACTOR: f64 = 1.0;
/// Go `MaxAmplifyFactor`.
pub const MAX_AMPLIFY_FACTOR: f64 = 10.0;
const DEFAULT_AMPLIFY_FACTOR: f64 = MIN_AMPLIFY_FACTOR;

/// Go `TuneFactors`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TuneFactors {
    /// Input-size and node-count amplification factor.
    #[serde(
        default,
        skip_serializing_if = "is_zero_f64",
        serialize_with = "serialize_go_f64"
    )]
    pub amplify_factor: f64,
}

/// Go `TTLTuneFactors`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct TtlTuneFactors {
    /// Embedded Go `TTLInfo`.
    #[serde(flatten)]
    pub ttl_info: TtlInfo,
    /// Embedded Go `TuneFactors`.
    #[serde(flatten)]
    pub tune_factors: TuneFactors,
}

impl TtlTuneFactors {
    /// Go `(*TTLTuneFactors).String`.
    #[must_use]
    pub fn string(&self) -> String {
        serde_json::to_string(self).unwrap_or_default()
    }
}

/// Go `GetDefaultTuneFactors`.
#[must_use]
pub fn get_default_tune_factors() -> TuneFactors {
    TuneFactors {
        amplify_factor: DEFAULT_AMPLIFY_FACTOR,
    }
}

fn is_zero_isize(value: &isize) -> bool {
    *value == 0
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

fn is_zero_f64(value: &f64) -> bool {
    *value == 0.0
}

fn is_zero_version(value: &Version) -> bool {
    value.0 == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn serialize_go_time<S: serde::Serializer>(
    value: &DateTime<FixedOffset>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if !(0..=9999).contains(&value.year()) {
        return Err(serde::ser::Error::custom(
            "time: year outside of range [0,9999]",
        ));
    }
    serializer.serialize_str(&format_rfc3339_nano(value))
}

fn serialize_go_f64<S: serde::Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
    if !value.is_finite() {
        return Err(serde::ser::Error::custom(format!(
            "json: unsupported value: {value}"
        )));
    }
    serializer.serialize_f64(*value)
}

fn deserialize_go_time<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<DateTime<FixedOffset>, D::Error> {
    let value = String::deserialize(deserializer)?;
    DateTime::parse_from_rfc3339(&value).map_err(serde::de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::{Node, Status};

    /// Go `TestStatusPrint`.
    #[test]
    fn status_print() {
        let mut status = Status::default();
        for i in 0..10 {
            status.tidb_worker.busy_nodes.push(Node {
                id: format!("tidb-{i}"),
                is_owner: false,
            });
        }
        assert_eq!(status.tidb_worker.busy_nodes.len(), 10);
        let decoded: Status = serde_json::from_str(&status.string()).unwrap();
        assert_eq!(decoded.tidb_worker.busy_nodes.len(), 6);
        assert!(decoded.tidb_worker.busy_nodes[5]
            .id
            .contains("too many nodes, total 10 busy nodes"));
        assert_eq!(status.tidb_worker.busy_nodes.len(), 10);
    }
}
