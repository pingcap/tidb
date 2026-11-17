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
/// Go `BackfillTaskSummary`: the execution summary of a backfill task.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct BackfillTaskSummary {
    /// Go `IndexKVSize`: collected only for global-sort backfills.
    #[serde(skip_serializing_if = "zero_u64")]
    pub index_kv_size: u64,
    /// Go `MergeTempIndexTxnKVSize`.
    #[serde(skip_serializing_if = "zero_u64")]
    pub merge_temp_index_txn_kv_size: u64,
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
    /// Go `Summary`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<BackfillTaskSummary>,
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

/// Go `globalsort.SortedKVMeta`, embedded by the backfill protocol.
pub use tidb_ingestor_globalsort::SortedKVMeta;
fn zero_u64(v: &u64) -> bool {
    *v == 0
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
/// The fields of `BackfillSubTaskMeta` Go tags `external:"true"`, which
/// `BaseExternalMeta` keeps in external storage: everything but
/// `ExternalPath`, `physical_table_id`, `row_start`, `row_end` and `ts`.
const EXTERNAL_FIELDS: [&str; 12] = [
    "range_job_keys",
    "range_split_keys",
    "data-files",
    "stat-files",
    "meta_groups",
    "ele_ids",
    "start-key",
    "end-key",
    "total-kv-size",
    "total-kv-cnt",
    "multiple-files-stats",
    "conflict-info",
];

impl BackfillSubTaskMeta {
    /// Go `BackfillSubTaskMeta.Marshal` (`BaseExternalMeta.Marshal`): every
    /// field, or, when the meta has an `ExternalPath`, only those not tagged
    /// `external:"true"`.
    pub fn marshal(&self) -> serde_json::Result<Vec<u8>> {
        if self.external_path.is_empty() {
            return serde_json::to_vec(self);
        }
        self.marshal_fields(|name| !EXTERNAL_FIELDS.contains(&name))
    }

    /// Go `marshalExternalFields`: only the `external:"true"` fields.
    fn marshal_external_fields(&self) -> serde_json::Result<Vec<u8>> {
        self.marshal_fields(|name| EXTERNAL_FIELDS.contains(&name))
    }

    fn marshal_fields(&self, keep: impl Fn(&str) -> bool) -> serde_json::Result<Vec<u8>> {
        let serde_json::Value::Object(fields) = serde_json::to_value(self)? else {
            unreachable!("a struct serializes to an object")
        };
        let mut out = serde_json::Map::new();
        for (name, value) in fields {
            if keep(&name) {
                out.insert(name, value);
            }
        }
        serde_json::to_vec(&out)
    }

    /// Go `BaseExternalMeta.WriteJSONToExternalStorage`: the external fields
    /// to `ExternalPath`, when the meta has one.
    ///
    /// # Errors
    /// The file could not be written.
    pub fn write_external(
        &self,
        ctx: &tidb_gocontext::Context,
        store: &dyn tidb_objstore::storeapi::Storage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.external_path.is_empty() {
            return Ok(());
        }
        store.write_file(ctx, &self.external_path, &self.marshal_external_fields()?)?;
        Ok(())
    }
}

/// Go `writeExternalBackfillSubTaskMeta`: the meta's external fields go to
/// `external_path`.
///
/// # Errors
/// The file could not be written.
pub fn write_external_backfill_sub_task_meta(
    ctx: &tidb_gocontext::Context,
    store: &dyn tidb_objstore::storeapi::Storage,
    subtask: &mut BackfillSubTaskMeta,
    external_path: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    subtask.external_path = external_path;
    subtask.write_external(ctx, store)
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
