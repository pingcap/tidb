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

//! Go `br/pkg/checkpoint/restore.go`: the snapshot restore checkpoint's
//! keys, values and metadata, and its runner. Log restore's types are not
//! ported.

use serde_json::Value as Json;
use tidb_gocontext::Context;

use super::manager::{SnapshotMetaManager, TickDurationConfig};
use super::{gojson, CheckpointMessage, CheckpointRunner, RangeGroup};
use crate::errors::{self, Result};
use crate::pdutil::ClusterConfig;

/// Go `RestoreKeyType`: the table ID.
pub type RestoreKeyType = i64;

/// Go `RestoreValueType`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RestoreValueType {
    /// Go `RangeKey`: the file key of a range (`range-key`, omitempty).
    pub range_key: String,
    /// Go `Name`: the file name, for compacted restore (`name`, omitempty).
    pub name: String,
}

/// The restore checkpoint runner.
pub type RestoreRunner = CheckpointRunner<RestoreKeyType, RestoreValueType>;

/// Go `CheckpointItem`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CheckpointItem {
    table_id: RestoreKeyType,
    range_key: String,
    name: String,
}

/// Go `NewCheckpointRangeKeyItem`.
#[must_use]
pub fn new_checkpoint_range_key_item(table_id: RestoreKeyType, range_key: &str) -> CheckpointItem {
    CheckpointItem {
        table_id,
        range_key: range_key.to_owned(),
        name: String::new(),
    }
}

/// Go `NewCheckpointFileItem`.
#[must_use]
pub fn new_checkpoint_file_item(table_id: RestoreKeyType, file_name: &str) -> CheckpointItem {
    CheckpointItem {
        table_id,
        range_key: String::new(),
        name: file_name.to_owned(),
    }
}

/// Go `valueMarshalerForRestore`: `json.Marshal(group)`.
///
/// # Errors
/// Never; Go's signature has one.
pub fn value_marshaler_for_restore(
    group: &RangeGroup<RestoreKeyType, RestoreValueType>,
) -> Result<Vec<u8>> {
    let values: Vec<String> = group
        .group
        .iter()
        .map(|v| {
            let mut fields = Vec::new();
            if !v.range_key.is_empty() {
                fields.push(format!("\"range-key\":{}", gojson::string(&v.range_key)));
            }
            if !v.name.is_empty() {
                fields.push(format!("\"name\":{}", gojson::string(&v.name)));
            }
            format!("{{{}}}", fields.join(","))
        })
        .collect();
    let mut out = String::from("{");
    if group.group_key != 0 {
        out.push_str(&format!("\"group-key\":{},", group.group_key));
    }
    let groups = if group.group.is_empty() {
        "null".to_owned()
    } else {
        format!("[{}]", values.join(","))
    };
    out.push_str(&format!("\"groups\":{groups}}}"));
    Ok(out.into_bytes())
}

/// Go `AppendRangesForRestore`.
///
/// # Errors
/// An item with neither a range key nor a name, or the runner's error.
pub fn append_ranges_for_restore(
    ctx: &Context,
    r: &RestoreRunner,
    c: &CheckpointItem,
) -> Result<()> {
    let mut group = RestoreValueType::default();
    if !c.range_key.is_empty() {
        group.range_key.clone_from(&c.range_key);
    } else if !c.name.is_empty() {
        group.name.clone_from(&c.name);
    } else {
        return Err(errors::errorf(
            "either rangekey or name should be used in checkpoint append",
        ));
    }
    r.append(
        ctx,
        CheckpointMessage {
            group_key: c.table_id,
            group: vec![group],
        },
    )
}

/// Parses one flushed group back (Go `json.Unmarshal` into
/// `RangeGroup[RestoreKeyType, RestoreValueType]`).
///
/// # Errors
/// Invalid JSON.
pub fn parse_restore_group(data: &[u8]) -> Result<RangeGroup<RestoreKeyType, RestoreValueType>> {
    let v: Json = serde_json::from_slice(data).map_err(|e| errors::errorf(e.to_string()))?;
    let group = v["groups"]
        .as_array()
        .map(|vs| {
            vs.iter()
                .map(|g| RestoreValueType {
                    range_key: g["range-key"].as_str().unwrap_or_default().to_owned(),
                    name: g["name"].as_str().unwrap_or_default().to_owned(),
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(RangeGroup {
        group_key: v["group-key"].as_i64().unwrap_or(0),
        group,
    })
}

/// Go `StartCheckpointRunnerForRestore`.
///
/// # Errors
/// The manager cannot start a runner.
pub fn start_checkpoint_runner_for_restore(
    ctx: &Context,
    manager: &dyn SnapshotMetaManager,
) -> Result<std::sync::Arc<RestoreRunner>> {
    manager.start_checkpoint_runner(ctx, TickDurationConfig::default())
}

/// Go `StartCheckpointRestoreRunnerForTest`: both ticks are `tick`.
///
/// # Errors
/// The manager cannot start a runner.
pub fn start_checkpoint_restore_runner_for_test(
    ctx: &Context,
    tick: std::time::Duration,
    retry_duration: std::time::Duration,
    manager: &dyn SnapshotMetaManager,
) -> Result<std::sync::Arc<RestoreRunner>> {
    let cfg = TickDurationConfig {
        tick_duration_for_flush: tick,
        tick_duration_for_checksum: tick,
        retry_duration,
    };
    manager.start_checkpoint_runner(ctx, cfg)
}

/// Go `checkpoint.PreallocIDs`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PreallocIDs {
    /// Go `Start`.
    pub start: i64,
    /// Go `ReusableBorder`.
    pub reusable_border: i64,
    /// Go `End`.
    pub end: i64,
    /// Go `Hash`.
    pub hash: [u8; 32],
}

/// Go `CheckpointMetadataForSnapshotRestore`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CheckpointMetadataForSnapshotRestore {
    /// Go `UpstreamClusterID`.
    pub upstream_cluster_id: u64,
    /// Go `RestoreStartTS`.
    pub restore_start_ts: u64,
    /// Go `RestoredTS`.
    pub restored_ts: u64,
    /// Go `LogRestoredTS`.
    pub log_restored_ts: u64,
    /// Go `SchedulersConfig`.
    pub schedulers_config: Option<ClusterConfig>,
    /// Go `Hash`; Go's nil is empty.
    pub hash: Vec<u8>,
    /// Go `PreallocIDs`.
    pub prealloc_ids: Option<PreallocIDs>,
    /// Go `RestoreUUID`.
    pub restore_uuid: uuid::Uuid,
}

impl CheckpointMetadataForSnapshotRestore {
    /// Go `json.Marshal(meta)`.
    #[must_use]
    pub fn to_go_json(&self) -> Vec<u8> {
        let schedulers = self.schedulers_config.as_ref().map_or_else(
            || "null".to_owned(),
            |c| serde_json::to_string(c).unwrap_or_else(|_| "null".to_owned()),
        );
        // Go encodes the `[32]byte` hash as an array of numbers.
        let prealloc = self.prealloc_ids.map_or_else(
            || "null".to_owned(),
            |p| {
                let hash: Vec<String> = p.hash.iter().map(u8::to_string).collect();
                format!(
                    "{{\"Start\":{},\"ReusableBorder\":{},\"End\":{},\"Hash\":[{}]}}",
                    p.start,
                    p.reusable_border,
                    p.end,
                    hash.join(",")
                )
            },
        );
        format!(
            "{{\"upstream-cluster-id\":{},\"restore-start-ts\":{},\"restored-ts\":{},\"log-restored-ts\":{},\"schedulers-config\":{schedulers},\"hash\":{},\"prealloc-ids\":{prealloc},\"restore-uuid\":\"{}\"}}",
            self.upstream_cluster_id,
            self.restore_start_ts,
            self.restored_ts,
            self.log_restored_ts,
            gojson::opt_bytes(&self.hash),
            self.restore_uuid.hyphenated(),
        )
        .into_bytes()
    }

    /// Go `json.Unmarshal(data, &meta)`.
    ///
    /// # Errors
    /// Invalid JSON.
    pub fn from_go_json(data: &[u8]) -> Result<Self> {
        let v: Json = serde_json::from_slice(data).map_err(|e| errors::errorf(e.to_string()))?;
        let u64_of = |k: &str| v[k].as_u64().unwrap_or(0);
        let schedulers_config = match &v["schedulers-config"] {
            Json::Null => None,
            c => {
                Some(serde_json::from_value(c.clone()).map_err(|e| errors::errorf(e.to_string()))?)
            }
        };
        let prealloc_ids = match &v["prealloc-ids"] {
            Json::Null => None,
            p => {
                let mut hash = [0_u8; 32];
                for (i, b) in p["Hash"]
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .take(32)
                    .enumerate()
                {
                    hash[i] = b.as_u64().unwrap_or(0) as u8;
                }
                Some(PreallocIDs {
                    start: p["Start"].as_i64().unwrap_or(0),
                    reusable_border: p["ReusableBorder"].as_i64().unwrap_or(0),
                    end: p["End"].as_i64().unwrap_or(0),
                    hash,
                })
            }
        };
        let restore_uuid = match v["restore-uuid"].as_str() {
            Some(s) => uuid::Uuid::parse_str(s)
                .map_err(|e| errors::errorf(format!("invalid UUID: {e}")))?,
            None => uuid::Uuid::nil(),
        };
        Ok(Self {
            upstream_cluster_id: u64_of("upstream-cluster-id"),
            restore_start_ts: u64_of("restore-start-ts"),
            restored_ts: u64_of("restored-ts"),
            log_restored_ts: u64_of("log-restored-ts"),
            schedulers_config,
            hash: gojson::parse_bytes(&v["hash"])?,
            prealloc_ids,
            restore_uuid,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `json.Marshal(RangeGroup[int64, RestoreValueType])`.
    #[test]
    fn restore_value_json_matches_go() {
        let g = RangeGroup {
            group_key: 120,
            group: vec![
                RestoreValueType {
                    range_key: "1/a_b".into(),
                    name: String::new(),
                },
                RestoreValueType {
                    range_key: String::new(),
                    name: "c<d>.sst".into(),
                },
            ],
        };
        assert_eq!(
            String::from_utf8(value_marshaler_for_restore(&g).unwrap()).unwrap(),
            r#"{"group-key":120,"groups":[{"range-key":"1/a_b"},{"name":"c\u003cd\u003e.sst"}]}"#
        );
        let empty = RangeGroup::<RestoreKeyType, RestoreValueType> {
            group_key: 0,
            group: vec![],
        };
        assert_eq!(
            String::from_utf8(value_marshaler_for_restore(&empty).unwrap()).unwrap(),
            r#"{"groups":null}"#
        );
        assert_eq!(
            parse_restore_group(&value_marshaler_for_restore(&g).unwrap()).unwrap(),
            g
        );
    }

    /// `json.Marshal(CheckpointMetadataForSnapshotRestore)` captured from Go
    /// master (`testdata/restore_checkpoint_meta_go.jsonl`): a full one, an
    /// empty one, and one decoded from partial JSON.
    #[test]
    fn snapshot_restore_metadata_json_matches_go() {
        let lines: Vec<&str> = include_str!("../../testdata/restore_checkpoint_meta_go.jsonl")
            .lines()
            .collect();
        let mut hash = [0_u8; 32];
        for (i, b) in hash.iter_mut().enumerate() {
            *b = (i * 7) as u8;
        }
        let full = CheckpointMetadataForSnapshotRestore {
            upstream_cluster_id: 7_354_000_000_000_000_001,
            restore_start_ts: 470_516_450_163_163_137,
            restored_ts: 470_516_450_163_163_100,
            log_restored_ts: 0,
            schedulers_config: Some(ClusterConfig {
                schedulers: Some(vec![
                    "balance-leader-scheduler".into(),
                    "balance-region-scheduler".into(),
                ]),
                schedule_cfg: Some(
                    [
                        ("max-merge-region-keys".to_owned(), Json::from(0)),
                        (
                            "enable-location-replacement".to_owned(),
                            Json::from("false"),
                        ),
                        ("leader-schedule-limit".to_owned(), Json::from(40.5)),
                    ]
                    .into(),
                ),
                rule_id: "restore-rule-1".into(),
            }),
            hash: vec![1, 2, 3, 250],
            prealloc_ids: Some(PreallocIDs {
                start: 100,
                reusable_border: 120,
                end: 130,
                hash,
            }),
            restore_uuid: uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8").unwrap(),
        };
        assert_eq!(String::from_utf8(full.to_go_json()).unwrap(), lines[0]);
        assert_eq!(
            CheckpointMetadataForSnapshotRestore::from_go_json(lines[0].as_bytes()).unwrap(),
            full
        );
        let empty = CheckpointMetadataForSnapshotRestore::default();
        assert_eq!(String::from_utf8(empty.to_go_json()).unwrap(), lines[1]);
        let partial =
            CheckpointMetadataForSnapshotRestore::from_go_json(lines[2].as_bytes()).unwrap();
        assert_eq!(String::from_utf8(partial.to_go_json()).unwrap(), lines[2]);
    }
}
