// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/proto.go`: the task and subtask metas, in Go's
//! JSON.

use std::collections::BTreeMap;

use serde_json::{json, Value};
use tidb_executor::importer::chunk_process::Chunk;
use tidb_executor::importer::job::Summary;
use tidb_executor::importer::Plan;

use crate::{plain, Error};

/// Go `TaskMeta`.
#[derive(Clone, Debug, Default)]
pub struct TaskMeta {
    /// Go `JobID`.
    pub job_id: i64,
    /// Go `Plan`.
    pub plan: Plan,
    /// Go `Stmt`: the statement the workers re-parse for the column list
    /// and SET clause.
    pub stmt: String,
    /// Go `Summary`.
    pub summary: Summary,
    /// Go `EligibleInstances`: the server info JSON of each instance that
    /// may run the task (one for an import from server disk).
    pub eligible_instances: Option<Vec<Value>>,
    /// Go `ChunkMap`: the chunks of each engine, when the submitter split
    /// the files.
    pub chunk_map: Option<BTreeMap<i32, Vec<Chunk>>>,
    /// Go `PreparedMetaExternalPath`.
    pub prepared_meta_external_path: String,
}

fn chunk_map_json(m: &Option<BTreeMap<i32, Vec<Chunk>>>) -> Value {
    m.as_ref().map_or(Value::Null, |m| {
        Value::Object(m.iter().map(|(k, v)| (k.to_string(), serde_json::to_value(v).unwrap_or(Value::Null))).collect())
    })
}

fn parse_chunk_map(v: Option<&Value>) -> Result<Option<BTreeMap<i32, Vec<Chunk>>>, Error> {
    let Some(Value::Object(m)) = v else { return Ok(None) };
    let mut out = BTreeMap::new();
    for (k, chunks) in m {
        let id = k.parse::<i32>().map_err(|e| plain(e.to_string()))?;
        let chunks: Option<Vec<Chunk>> = serde_json::from_value(chunks.clone()).map_err(|e| plain(e.to_string()))?;
        out.insert(id, chunks.unwrap_or_default());
    }
    Ok(Some(out))
}

impl TaskMeta {
    /// Go `json.Marshal(taskMeta)`.
    #[must_use]
    pub fn to_json(&self) -> Vec<u8> {
        let mut v = json!({
            "JobID": self.job_id,
            "Plan": self.plan.to_go_json(),
            "Stmt": self.stmt,
            "Summary": self.summary,
            "EligibleInstances": self.eligible_instances,
            "ChunkMap": chunk_map_json(&self.chunk_map),
        });
        if !self.prepared_meta_external_path.is_empty() {
            v["prepared_meta_external_path"] = json!(self.prepared_meta_external_path);
        }
        tidb_stringutil_json(&v)
    }

    /// Go `json.Unmarshal(bs, &taskMeta)`.
    ///
    /// # Errors
    /// The bytes are not a task meta.
    pub fn from_json(bs: &[u8]) -> Result<Self, Error> {
        let v: Value = serde_json::from_slice(bs).map_err(|e| plain(e.to_string()))?;
        let plan = match v.get("Plan") {
            Some(p) => Plan::from_go_json(p).map_err(plain)?,
            None => Plan::default(),
        };
        Ok(Self {
            job_id: v.get("JobID").and_then(Value::as_i64).unwrap_or_default(),
            plan,
            stmt: v.get("Stmt").and_then(Value::as_str).unwrap_or_default().to_owned(),
            summary: v
                .get("Summary")
                .map(|s| serde_json::from_value(s.clone()))
                .transpose()
                .map_err(|e| plain(e.to_string()))?
                .unwrap_or_default(),
            eligible_instances: v.get("EligibleInstances").and_then(Value::as_array).cloned(),
            chunk_map: parse_chunk_map(v.get("ChunkMap"))?,
            prepared_meta_external_path: v
                .get("prepared_meta_external_path")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned(),
        })
    }
}

/// Go's `json.Marshal` bytes: compact, with Go's HTML escaping.
fn tidb_stringutil_json(v: &Value) -> Vec<u8> {
    tidb_stringutil::go_json_html_escape(&v.to_string()).into_bytes()
}
/// Go `Checksum`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Checksum {
    /// Go `Sum`.
    #[serde(rename = "Sum", default)]
    pub sum: u64,
    /// Go `KVs`.
    #[serde(rename = "KVs", default)]
    pub kvs: u64,
    /// Go `Size`.
    #[serde(rename = "Size", default)]
    pub size: u64,
}

impl Checksum {
    /// Go `newFromKVChecksum`.
    #[must_use]
    pub fn from_kv_checksum(sum: &tidb_lightning_verification::KvChecksum) -> Self {
        Self { sum: sum.sum(), kvs: sum.sum_kvs(), size: sum.sum_size() }
    }

    /// Go `ToKVChecksum`.
    #[must_use]
    pub fn to_kv_checksum(&self) -> tidb_lightning_verification::KvChecksum {
        tidb_lightning_verification::KvChecksum::make(self.size, self.kvs, self.sum)
    }
}

/// Go `autoid.AllocatorType` as `MaxIDs` keys it: row ID 0, auto-increment
/// 1, auto-random 2.
#[must_use]
pub fn allocator_type_code(tp: tidb_lightning_backend_kv::AllocatorType) -> i32 {
    use tidb_lightning_backend_kv::AllocatorType;
    match tp {
        AllocatorType::RowId => 0,
        AllocatorType::AutoIncrement => 1,
        AllocatorType::AutoRandom => 2,
    }
}

/// The inverse of [`allocator_type_code`].
#[must_use]
pub fn allocator_type_of(code: i32) -> Option<tidb_lightning_backend_kv::AllocatorType> {
    use tidb_lightning_backend_kv::AllocatorType;
    match code {
        0 => Some(AllocatorType::RowId),
        1 => Some(AllocatorType::AutoIncrement),
        2 => Some(AllocatorType::AutoRandom),
        _ => None,
    }
}

/// Go `ImportStepMeta`. The global sort fields (`SortedDataMeta`,
/// `SortedIndexMetas`) are kept as Go's JSON.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImportStepMeta {
    /// Go `BaseExternalMeta.ExternalPath`.
    #[serde(rename = "ExternalPath", default)]
    pub external_path: String,
    /// Go `ID`: the engine ID.
    #[serde(rename = "ID", default)]
    pub id: i32,
    /// Go `Chunks`.
    #[serde(rename = "Chunks", default)]
    pub chunks: Option<Vec<Chunk>>,
    /// Go `Checksum`, by KV group ID.
    #[serde(rename = "Checksum", default)]
    pub checksum: Option<BTreeMap<i64, Checksum>>,
    /// Go `MaxIDs`, by allocator type code.
    #[serde(rename = "MaxIDs", default)]
    pub max_ids: Option<BTreeMap<i32, i64>>,
    /// Go `SortedDataMeta`.
    #[serde(rename = "SortedDataMeta", default)]
    pub sorted_data_meta: Value,
    /// Go `SortedIndexMetas`.
    #[serde(rename = "SortedIndexMetas", default)]
    pub sorted_index_metas: Value,
    /// Go `RecordedConflictKVCount`.
    #[serde(rename = "RecordedConflictKVCount", default, skip_serializing_if = "is_zero")]
    pub recorded_conflict_kv_count: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// Go `PostProcessStepMeta`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PostProcessStepMeta {
    /// Go `Checksum`, by KV group ID.
    #[serde(rename = "Checksum", default)]
    pub checksum: Option<BTreeMap<i64, Checksum>>,
    /// Go `DeletedRowsChecksum`.
    #[serde(rename = "DeletedRowsChecksum", default)]
    pub deleted_rows_checksum: Checksum,
    /// Go `TooManyConflictsFromIndex`.
    #[serde(rename = "too-many-conflicts-from-index", default, skip_serializing_if = "std::ops::Not::not")]
    pub too_many_conflicts_from_index: bool,
    /// Go `MaxIDs`.
    #[serde(rename = "MaxIDs", default)]
    pub max_ids: Option<BTreeMap<i32, i64>>,
}

/// Go `CollectConflictsStepMeta`'s fields the post-process step reads.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize)]
pub struct CollectConflictsStepMeta {
    /// Go `Checksum`.
    #[serde(rename = "checksum", default)]
    pub checksum: Option<Checksum>,
    /// Go `ConflictedRowCount`.
    #[serde(rename = "conflicted-row-count", default)]
    pub conflicted_row_count: i64,
    /// Go `TooManyConflictsFromIndex`.
    #[serde(rename = "too-many-conflicts-from-index", default)]
    pub too_many_conflicts_from_index: bool,
}

/// Go's `json.Marshal` of a subtask meta.
///
/// # Errors
/// The value does not encode.
pub fn marshal<T: serde::Serialize>(v: &T) -> Result<Vec<u8>, Error> {
    let v = serde_json::to_value(v).map_err(|e| plain(e.to_string()))?;
    Ok(tidb_stringutil_json(&v))
}

/// Go's `json.Unmarshal` of a subtask meta.
///
/// # Errors
/// The bytes are not a `T`.
pub fn unmarshal<T: serde::de::DeserializeOwned>(bs: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bs).map_err(|e| plain(e.to_string()))
}
