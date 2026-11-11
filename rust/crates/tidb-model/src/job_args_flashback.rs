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

//! Go `pkg/meta/model/job_args.go`'s FLASHBACK CLUSTER arguments: `KeyRange`
//! and `FlashbackClusterArgs`.

use std::collections::BTreeMap;

use super::*;

/// Go `KeyRange`: `kv.KeyRange` without its unused fields. Untagged, so its
/// JSON keys are the Go field names.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyRange {
    /// First key of the range.
    #[serde(rename = "StartKey", default, with = "crate::serde_helpers::go_bytes")]
    pub start_key: Option<Vec<u8>>,
    /// First key after the range.
    #[serde(rename = "EndKey", default, with = "crate::serde_helpers::go_bytes")]
    pub end_key: Option<Vec<u8>>,
}

/// Go `FlashbackClusterArgs`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FlashbackClusterArgs {
    /// The timestamp the cluster goes back to.
    #[serde(rename = "flashback_ts", default, skip_serializing_if = "is_zero_u64")]
    pub flashback_ts: u64,
    /// PD's schedule configuration before the job closed it. A nil map and
    /// an empty one differ in the v1 argument list only.
    #[serde(rename = "pd_schedule_value", default, skip_serializing_if = "map_is_empty")]
    pub pd_schedule_value: Option<BTreeMap<String, serde_json::Value>>,
    /// Whether GC ran before the job.
    #[serde(rename = "enable_gc", default, skip_serializing_if = "is_false")]
    pub enable_gc: bool,
    /// `tidb_enable_auto_analyze` before the job.
    #[serde(rename = "enable_auto_analyze", default, skip_serializing_if = "is_false")]
    pub enable_auto_analyze: bool,
    /// `tidb_ttl_job_enable` before the job.
    #[serde(rename = "enable_ttl_job", default, skip_serializing_if = "is_false")]
    pub enable_ttl_job: bool,
    /// `tidb_super_read_only` before the job.
    #[serde(rename = "super_read_only", default, skip_serializing_if = "is_false")]
    pub super_read_only: bool,
    /// Regions the prepare step locked.
    #[serde(rename = "locked_region_cnt", default, skip_serializing_if = "is_zero_u64")]
    pub locked_region_cnt: u64,
    /// The flashback's start timestamp.
    #[serde(rename = "start_ts", default, skip_serializing_if = "is_zero_u64")]
    pub start_ts: u64,
    /// The flashback's commit timestamp.
    #[serde(rename = "commit_ts", default, skip_serializing_if = "is_zero_u64")]
    pub commit_ts: u64,
    /// The key ranges the flashback covers.
    #[serde(rename = "key_ranges", default, skip_serializing_if = "ranges_are_empty")]
    pub flashback_key_ranges: Option<Vec<KeyRange>>,
}

const fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

const fn is_false(value: &bool) -> bool {
    !*value
}

fn map_is_empty(value: &Option<BTreeMap<String, serde_json::Value>>) -> bool {
    value.as_ref().map_or(true, BTreeMap::is_empty)
}

fn ranges_are_empty(value: &Option<Vec<KeyRange>>) -> bool {
    value.as_ref().map_or(true, Vec::is_empty)
}

fn on_off(value: bool) -> GoAny {
    ColumnDefaultValue::Str(if value { "ON" } else { "OFF" }.into()).into()
}

impl JobArgs for FlashbackClusterArgs {
    job_args_identity_methods!(FlashbackCluster);

    fn get_args_v1(value: Option<&GoShared<Self>>, _job: &Job) -> GoSharedSlice<GoAny> {
        let value = value.expect("nil *FlashbackClusterArgs receiver").read();
        GoSharedSlice::from_vec(vec![
            ColumnDefaultValue::Uint(value.flashback_ts).into(),
            typed_value_any(
                builtin_type("map[string]interface {}", GoTypeKind::Map),
                value.pd_schedule_value.clone(),
            ),
            ColumnDefaultValue::Bool(value.enable_gc).into(),
            on_off(value.enable_auto_analyze),
            on_off(value.super_read_only),
            ColumnDefaultValue::Uint(value.locked_region_cnt).into(),
            ColumnDefaultValue::Uint(value.start_ts).into(),
            ColumnDefaultValue::Uint(value.commit_ts).into(),
            on_off(value.enable_ttl_job),
            typed_value_any(
                builtin_type("[]model.KeyRange", GoTypeKind::Slice),
                value.flashback_key_ranges.clone(),
            ),
        ])
    }

    fn decode_v1(job: &mut Job) -> Result<Option<GoShared<Self>>, serde_json::Error> {
        let mut decoder = V1Decoder::new(job)?;
        let uint64 = || builtin_type("uint64", GoTypeKind::Uint64);
        let string = || builtin_type("string", GoTypeKind::String);
        let flashback_ts = GoField::<u64>::default();
        let pd_schedule_value = GoField::<Option<BTreeMap<String, serde_json::Value>>>::default();
        let enable_gc = GoField::<bool>::default();
        let auto_analyze_value = GoField::<String>::default();
        let read_only_value = GoField::<String>::default();
        let locked_region_cnt = GoField::<u64>::default();
        let start_ts = GoField::<u64>::default();
        let commit_ts = GoField::<u64>::default();
        let ttl_job_enable_value = GoField::<String>::default();
        let flashback_key_ranges = GoField::<Option<Vec<KeyRange>>>::default();
        decoder.decode(&flashback_ts, uint64())?;
        decoder.decode(
            &pd_schedule_value,
            builtin_type("map[string]interface {}", GoTypeKind::Map),
        )?;
        decoder.decode(&enable_gc, builtin_type("bool", GoTypeKind::Bool))?;
        decoder.decode(&auto_analyze_value, string())?;
        decoder.decode(&read_only_value, string())?;
        decoder.decode(&locked_region_cnt, uint64())?;
        decoder.decode(&start_ts, uint64())?;
        decoder.decode(&commit_ts, uint64())?;
        decoder.decode(&ttl_job_enable_value, string())?;
        decoder.decode(
            &flashback_key_ranges,
            builtin_type("[]model.KeyRange", GoTypeKind::Slice),
        )?;
        decoder.finish(job);
        Ok(Some(GoShared::new(Self {
            flashback_ts: flashback_ts.get(),
            pd_schedule_value: pd_schedule_value.get(),
            enable_gc: enable_gc.get(),
            enable_auto_analyze: auto_analyze_value.get() == "ON",
            enable_ttl_job: ttl_job_enable_value.get() == "ON",
            super_read_only: read_only_value.get() == "ON",
            locked_region_cnt: locked_region_cnt.get(),
            start_ts: start_ts.get(),
            commit_ts: commit_ts.get(),
            flashback_key_ranges: flashback_key_ranges.get(),
        })))
    }
}

/// Go `GetFlashbackClusterArgs`.
pub fn get_flashback_cluster_args(
    job: &mut Job,
) -> Result<Option<GoShared<FlashbackClusterArgs>>, serde_json::Error> {
    get_or_decode_args(job)
}
