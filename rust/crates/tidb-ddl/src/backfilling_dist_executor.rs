// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/backfilling_dist_executor.go`.
use serde::{Deserialize, Serialize};

/// Go `BackfillTaskMetaVersion0`.
pub const BACKFILL_TASK_META_VERSION_0: i32 = 0;
/// Go `BackfillTaskMetaVersion1`.
pub const BACKFILL_TASK_META_VERSION_1: i32 = 1;
fn is_zero(value: &i32) -> bool {
    *value == 0
}
/// Go `BackfillTaskMeta`. Do not derive Debug: the storage URI may contain credentials.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BackfillTaskMeta {
    /// Go `Job`.
    pub job: tidb_model::Job,
    /// Go `EleIDs`.
    pub ele_ids: Option<Vec<i64>>,
    /// Go `EleTypeKey` (Go JSON byte slices).
    #[serde(with = "go_bytes")]
    pub ele_type_key: Vec<u8>,
    /// Go `CloudStorageURI`.
    pub cloud_storage_uri: String,
    /// Go `EstimateRowSize`.
    pub estimate_row_size: usize,
    /// Go `MergeTempIndex`.
    pub merge_temp_index: bool,
    /// Go `Version`.
    #[serde(skip_serializing_if = "is_zero")]
    pub version: i32,
}
/// Go encoding/json's byte-slice representation.
pub(crate) mod go_bytes {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        if value.is_empty() {
            serializer.serialize_none()
        } else {
            serializer.serialize_str(&base64::engine::general_purpose::STANDARD.encode(value))
        }
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        Option::<String>::deserialize(deserializer)?.map_or(Ok(Vec::new()), |text| {
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .map_err(serde::de::Error::custom)
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `backfilling_dist_scheduler_test.go:TestBackfillTaskMetaVersion`.
    #[test]
    fn test_backfill_task_meta_version() {
        let meta = BackfillTaskMeta::default();
        assert_eq!(meta.version, BACKFILL_TASK_META_VERSION_0);
        let meta = BackfillTaskMeta {
            version: BACKFILL_TASK_META_VERSION_1,
            ..Default::default()
        };
        assert_eq!(meta.version, BACKFILL_TASK_META_VERSION_1);
    }
}

/// Go `engineapi.ConflictInfo` in backfill external metadata.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConflictInfo {
    #[serde(skip_serializing_if = "zero_u64")]
    pub count: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
}
fn zero_u64(v: &u64) -> bool {
    *v == 0
}
/// Go `simplesst.MultipleFilesStat` in backfill external metadata.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct MultipleFilesStat {
    #[serde(rename = "min-key", with = "go_bytes")]
    pub min_key: Vec<u8>,
    #[serde(rename = "max-key", with = "go_bytes")]
    pub max_key: Vec<u8>,
    pub filenames: Option<Vec<[String; 2]>>,
    #[serde(rename = "max-overlapping-num")]
    pub max_overlapping_num: i64,
}
/// Go `globalsort.SortedKVMeta` embedded by the backfill protocol.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SortedKVMeta {
    #[serde(rename = "start-key", with = "go_bytes")]
    pub start_key: Vec<u8>,
    #[serde(rename = "end-key", with = "go_bytes")]
    pub end_key: Vec<u8>,
    #[serde(rename = "total-kv-size")]
    pub total_kv_size: u64,
    #[serde(rename = "total-kv-cnt")]
    pub total_kv_cnt: u64,
    #[serde(rename = "multiple-files-stats")]
    pub multiple_files_stats: Option<Vec<MultipleFilesStat>>,
    #[serde(rename = "conflict-info")]
    pub conflict_info: ConflictInfo,
}
/// Go `BackfillSubTaskMeta` including the v7.5 inline representation.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BackfillSubTaskMeta {
    #[serde(rename = "ExternalPath")]
    pub external_path: String,
    pub physical_table_id: i64,
    #[serde(with = "go_bytes")]
    pub row_start: Vec<u8>,
    #[serde(with = "go_bytes")]
    pub row_end: Vec<u8>,
    #[serde(skip_serializing_if = "Vec::is_empty", with = "go_byte_slices")]
    pub range_job_keys: Vec<Vec<u8>>,
    #[serde(skip_serializing_if = "Vec::is_empty", with = "go_byte_slices")]
    pub range_split_keys: Vec<Vec<u8>>,
    #[serde(rename = "data-files", skip_serializing_if = "Vec::is_empty")]
    pub data_files: Vec<String>,
    #[serde(rename = "stat-files", skip_serializing_if = "Vec::is_empty")]
    pub stat_files: Vec<String>,
    #[serde(skip_serializing_if = "zero_u64")]
    pub ts: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub meta_groups: Vec<SortedKVMeta>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub ele_ids: Vec<i64>,
    #[serde(flatten)]
    pub sorted_kv_meta: SortedKVMeta,
}
impl BackfillSubTaskMeta {
    /// Go `BackfillSubTaskMeta.Marshal`, excluding external fields if stored outside.
    pub fn marshal(&self) -> serde_json::Result<Vec<u8>> {
        if self.external_path.is_empty() {
            return serde_json::to_vec(self);
        }
        serde_json::to_vec(&serde_json::json!({
            "ExternalPath":self.external_path,"physical_table_id":self.physical_table_id,
            "row_start":base64_value(&self.row_start),"row_end":base64_value(&self.row_end),"ts":self.ts
        }))
    }
}
fn base64_value(value: &[u8]) -> Option<String> {
    use base64::Engine;
    (!value.is_empty()).then(|| base64::engine::general_purpose::STANDARD.encode(value))
}
mod go_byte_slices {
    use super::base64_value;
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    pub fn serialize<S: Serializer>(value: &[Vec<u8>], serializer: S) -> Result<S::Ok, S::Error> {
        value
            .iter()
            .map(|v| base64_value(v))
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Vec<u8>>, D::Error> {
        Option::<Vec<Option<String>>>::deserialize(deserializer)?
            .unwrap_or_default()
            .into_iter()
            .map(|s| {
                s.map_or(Ok(Vec::new()), |s| {
                    base64::engine::general_purpose::STANDARD
                        .decode(s)
                        .map_err(serde::de::Error::custom)
                })
            })
            .collect()
    }
}
/// Go `decodeBackfillSubTaskMeta`; the external reader supplies bytes from
/// `storeapi.Storage.ReadFile`, when the task uses external metadata.
pub fn decode_backfill_sub_task_meta(
    raw: &[u8],
    read_external: Option<
        &dyn Fn(&str) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>,
    >,
) -> Result<BackfillSubTaskMeta, Box<dyn std::error::Error + Send + Sync>> {
    let mut meta: BackfillSubTaskMeta = serde_json::from_slice(raw)?;
    if let Some(read) = read_external.filter(|_| !meta.external_path.is_empty()) {
        let mut combined: serde_json::Value = serde_json::from_slice(raw)?;
        let external: serde_json::Value = serde_json::from_slice(&read(&meta.external_path)?)?;
        if let (Some(combined), Some(external)) = (combined.as_object_mut(), external.as_object()) {
            combined.extend(external.clone());
        }
        meta = serde_json::from_value(combined)?;
    }
    if meta.row_start.is_empty() {
        meta.row_start = meta.sorted_kv_meta.start_key.clone();
        meta.row_end = meta.sorted_kv_meta.end_key.clone();
    }
    if meta.meta_groups.is_empty() {
        meta.meta_groups.push(meta.sorted_kv_meta.clone());
    }
    Ok(meta)
}
