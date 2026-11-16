// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/proto.go`: the task and subtask metas, in Go's
//! JSON.

use std::collections::BTreeMap;

use serde_json::{json, Value};
use tidb_executor::importer::chunk_process::Chunk;
use tidb_executor::importer::job::Summary;
use tidb_executor::importer::Plan;

use tidb_ingestor_engineapi::ConflictInfo;
use tidb_ingestor_globalsort::SortedKVMeta;

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

/// Go `ImportStepMeta`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ImportStepMeta {
    /// Go `BaseExternalMeta.ExternalPath`.
    #[serde(rename = "ExternalPath", default)]
    pub external_path: String,
    /// Go `ID`: the engine ID.
    #[serde(rename = "ID", default)]
    pub id: i32,
    /// Go `Chunks` (external).
    #[serde(rename = "Chunks", default)]
    pub chunks: Option<Vec<Chunk>>,
    /// Go `Checksum`, by KV group ID.
    #[serde(rename = "Checksum", default)]
    pub checksum: Option<BTreeMap<i64, Checksum>>,
    /// Go `MaxIDs`, by allocator type code.
    #[serde(rename = "MaxIDs", default)]
    pub max_ids: Option<BTreeMap<i32, i64>>,
    /// Go `SortedDataMeta` (external).
    #[serde(rename = "SortedDataMeta", default)]
    pub sorted_data_meta: Option<SortedKVMeta>,
    /// Go `SortedIndexMetas` (external), by index ID.
    #[serde(rename = "SortedIndexMetas", default)]
    pub sorted_index_metas: Option<BTreeMap<i64, SortedKVMeta>>,
    /// Go `RecordedConflictKVCount`.
    #[serde(rename = "RecordedConflictKVCount", default, skip_serializing_if = "is_zero")]
    pub recorded_conflict_kv_count: u64,
}

fn is_zero(v: &u64) -> bool {
    *v == 0
}

/// A struct's JSON fields in Go field order, each marked when Go tags it
/// `external:"true"`: Go `globalsort.BaseExternalMeta`'s two encodings.
struct GoFields(Vec<(&'static str, String, bool)>);

impl GoFields {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn add(mut self, name: &'static str, v: &impl serde::Serialize, external: bool) -> Self {
        self.0.push((name, serde_json::to_string(v).expect("JSON value"), external));
        self
    }

    /// A field Go tags `omitempty`, present when `present`.
    fn add_if(self, present: bool, name: &'static str, v: &impl serde::Serialize, external: bool) -> Self {
        if present {
            self.add(name, v, external)
        } else {
            self
        }
    }

    /// Go `json.Marshal` of the fields `keep` selects.
    fn encode(&self, keep: impl Fn(bool) -> bool) -> Vec<u8> {
        let body: Vec<String> = self
            .0
            .iter()
            .filter(|(_, _, external)| keep(*external))
            .map(|(name, json, _)| format!("{}:{json}", serde_json::to_string(name).expect("JSON key")))
            .collect();
        tidb_stringutil::go_json_html_escape(&format!("{{{}}}", body.join(","))).into_bytes()
    }

    /// Go `BaseExternalMeta.Marshal`: every field, or only the internal
    /// ones when the external fields are in external storage.
    fn marshal(&self, external_path: &str) -> Vec<u8> {
        if external_path.is_empty() {
            self.encode(|_| true)
        } else {
            self.encode(|external| !external)
        }
    }

    /// Go `marshalExternalFields`.
    fn external(&self) -> Vec<u8> {
        self.encode(|external| external)
    }
}

/// Go's JSON of a `map[int64]V`: keys as strings, sorted as strings.
struct GoIntMap<'a, V>(&'a Option<BTreeMap<i64, V>>);

impl<V: serde::Serialize> serde::Serialize for GoIntMap<'_, V> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let Some(m) = self.0 else { return s.serialize_none() };
        let mut entries: Vec<(String, &V)> = m.iter().map(|(k, v)| (k.to_string(), v)).collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let mut map = s.serialize_map(Some(entries.len()))?;
        for (k, v) in entries {
            map.serialize_entry(&k, v)?;
        }
        map.end()
    }
}

/// Go's JSON of a `[][]byte`: base64 strings, `null` for a nil slice.
struct GoBytesList<'a>(&'a Option<Vec<Vec<u8>>>);

impl serde::Serialize for GoBytesList<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let Some(list) = self.0 else { return s.serialize_none() };
        let mut seq = s.serialize_seq(Some(list.len()))?;
        for k in list {
            seq.serialize_element(&GoBytes(k))?;
        }
        seq.end()
    }
}

struct GoBytes<'a>(&'a [u8]);

impl serde::Serialize for GoBytes<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        tidb_ingestor_simplesst::go_bytes::serialize(self.0, s)
    }
}

fn de_bytes_list<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<Vec<Vec<u8>>>, D::Error> {
    use base64::Engine as _;
    let list: Option<Vec<Option<String>>> = serde::Deserialize::deserialize(d)?;
    list.map(|l| {
        l.into_iter()
            .map(|k| {
                k.map_or(Ok(Vec::new()), |k| {
                    base64::engine::general_purpose::STANDARD.decode(k).map_err(serde::de::Error::custom)
                })
            })
            .collect()
    })
    .transpose()
}

/// Reads a meta's external fields from `store`: Go
/// `BaseExternalMeta.ReadJSONFromExternalStorage`.
fn read_external<T: serde::de::DeserializeOwned>(
    ctx: &tidb_gocontext::Context,
    store: &dyn tidb_objstore::storeapi::Storage,
    path: &str,
) -> Result<T, Error> {
    let data = store.read_file(ctx, path).map_err(|e| plain(e.to_string()))?;
    unmarshal(&data)
}

/// Go `BaseExternalMeta.WriteJSONToExternalStorage`.
fn write_external(
    ctx: &tidb_gocontext::Context,
    store: &dyn tidb_objstore::storeapi::Storage,
    path: &str,
    data: &[u8],
) -> Result<(), Error> {
    if path.is_empty() {
        return Ok(());
    }
    store.write_file(ctx, path, data).map_err(|e| plain(e.to_string()))
}

impl ImportStepMeta {
    fn fields(&self) -> GoFields {
        GoFields::new()
            .add("ExternalPath", &self.external_path, false)
            .add("ID", &self.id, false)
            .add("Chunks", &self.chunks, true)
            .add("Checksum", &GoIntMap(&self.checksum), false)
            .add("MaxIDs", &self.max_ids, false)
            .add("SortedDataMeta", &self.sorted_data_meta, true)
            .add("SortedIndexMetas", &GoIntMap(&self.sorted_index_metas), true)
            .add_if(self.recorded_conflict_kv_count != 0, "RecordedConflictKVCount", &self.recorded_conflict_kv_count, false)
    }

    /// Go `Marshal`.
    #[must_use]
    pub fn marshal(&self) -> Vec<u8> {
        self.fields().marshal(&self.external_path)
    }

    /// Go `WriteJSONToExternalStorage`.
    ///
    /// # Errors
    /// The file could not be written.
    pub fn write_external(&self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        write_external(ctx, store, &self.external_path, &self.fields().external())
    }

    /// Go `ReadJSONFromExternalStorage`.
    ///
    /// # Errors
    /// The file could not be read.
    pub fn read_external(&mut self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        if self.external_path.is_empty() {
            return Ok(());
        }
        let ext: Self = read_external(ctx, store, &self.external_path)?;
        self.chunks = ext.chunks;
        self.sorted_data_meta = ext.sorted_data_meta;
        self.sorted_index_metas = ext.sorted_index_metas;
        Ok(())
    }
}

/// Go `MergeSortStepMeta`.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize)]
pub struct MergeSortStepMeta {
    /// Go `BaseExternalMeta.ExternalPath`.
    #[serde(rename = "ExternalPath", default)]
    pub external_path: String,
    /// Go `KVGroup`.
    #[serde(rename = "kv-group", default)]
    pub kv_group: String,
    /// Go `DataFiles` (external).
    #[serde(rename = "data-files", default)]
    pub data_files: Option<Vec<String>>,
    /// Go's embedded `SortedKVMeta` (external).
    #[serde(flatten)]
    pub sorted_kv_meta: SortedKVMeta,
    /// Go `RecordedConflictKVCount`.
    #[serde(rename = "recorded-conflict-kv-count", default)]
    pub recorded_conflict_kv_count: u64,
}

/// The fields of an embedded `SortedKVMeta`, in Go order.
fn add_sorted_kv_meta(f: GoFields, m: &SortedKVMeta, external: bool) -> GoFields {
    f.add("start-key", &GoBytes(&m.start_key), external)
        .add("end-key", &GoBytes(&m.end_key), external)
        .add("total-kv-size", &m.total_kv_size, external)
        .add("total-kv-cnt", &m.total_kv_cnt, external)
        .add("multiple-files-stats", &m.multiple_files_stats, external)
        .add("conflict-info", &m.conflict_info, external)
}

impl MergeSortStepMeta {
    fn fields(&self) -> GoFields {
        let f = GoFields::new()
            .add("ExternalPath", &self.external_path, false)
            .add("kv-group", &self.kv_group, false)
            .add("data-files", &self.data_files, true);
        add_sorted_kv_meta(f, &self.sorted_kv_meta, true).add_if(
            self.recorded_conflict_kv_count != 0,
            "recorded-conflict-kv-count",
            &self.recorded_conflict_kv_count,
            false,
        )
    }

    /// Go `Marshal`.
    #[must_use]
    pub fn marshal(&self) -> Vec<u8> {
        self.fields().marshal(&self.external_path)
    }

    /// Go `WriteJSONToExternalStorage`.
    ///
    /// # Errors
    /// The file could not be written.
    pub fn write_external(&self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        write_external(ctx, store, &self.external_path, &self.fields().external())
    }

    /// Go `ReadJSONFromExternalStorage`.
    ///
    /// # Errors
    /// The file could not be read.
    pub fn read_external(&mut self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        if self.external_path.is_empty() {
            return Ok(());
        }
        let ext: Self = read_external(ctx, store, &self.external_path)?;
        self.data_files = ext.data_files;
        self.sorted_kv_meta = ext.sorted_kv_meta;
        Ok(())
    }
}

/// Go `WriteIngestStepMeta`.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize)]
pub struct WriteIngestStepMeta {
    /// Go `BaseExternalMeta.ExternalPath`.
    #[serde(rename = "ExternalPath", default)]
    pub external_path: String,
    /// Go `KVGroup`.
    #[serde(rename = "kv-group", default)]
    pub kv_group: String,
    /// Go's embedded `SortedKVMeta` (external).
    #[serde(rename = "sorted-kv-meta", default)]
    pub sorted_kv_meta: SortedKVMeta,
    /// Go `RecordedConflictKVCount`.
    #[serde(rename = "recorded-conflict-kv-count", default)]
    pub recorded_conflict_kv_count: u64,
    /// Go `DataFiles` (external).
    #[serde(rename = "data-files", default)]
    pub data_files: Option<Vec<String>>,
    /// Go `StatFiles` (external).
    #[serde(rename = "stat-files", default)]
    pub stat_files: Option<Vec<String>>,
    /// Go `RangeJobKeys` (external).
    #[serde(rename = "range-job-keys", default, deserialize_with = "de_bytes_list")]
    pub range_job_keys: Option<Vec<Vec<u8>>>,
    /// Go `RangeSplitKeys` (external).
    #[serde(rename = "range-split-keys", default, deserialize_with = "de_bytes_list")]
    pub range_split_keys: Option<Vec<Vec<u8>>>,
    /// Go `TS`.
    #[serde(rename = "ts", default)]
    pub ts: u64,
}

impl WriteIngestStepMeta {
    fn fields(&self) -> GoFields {
        GoFields::new()
            .add("ExternalPath", &self.external_path, false)
            .add("kv-group", &self.kv_group, false)
            .add("sorted-kv-meta", &self.sorted_kv_meta, true)
            .add_if(self.recorded_conflict_kv_count != 0, "recorded-conflict-kv-count", &self.recorded_conflict_kv_count, false)
            .add("data-files", &self.data_files, true)
            .add("stat-files", &self.stat_files, true)
            .add("range-job-keys", &GoBytesList(&self.range_job_keys), true)
            .add("range-split-keys", &GoBytesList(&self.range_split_keys), true)
            .add("ts", &self.ts, false)
    }

    /// Go `Marshal`.
    #[must_use]
    pub fn marshal(&self) -> Vec<u8> {
        self.fields().marshal(&self.external_path)
    }

    /// Go `WriteJSONToExternalStorage`.
    ///
    /// # Errors
    /// The file could not be written.
    pub fn write_external(&self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        write_external(ctx, store, &self.external_path, &self.fields().external())
    }

    /// Go `ReadJSONFromExternalStorage`.
    ///
    /// # Errors
    /// The file could not be read.
    pub fn read_external(&mut self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        if self.external_path.is_empty() {
            return Ok(());
        }
        let ext: Self = read_external(ctx, store, &self.external_path)?;
        self.sorted_kv_meta = ext.sorted_kv_meta;
        self.data_files = ext.data_files;
        self.stat_files = ext.stat_files;
        self.range_job_keys = ext.range_job_keys;
        self.range_split_keys = ext.range_split_keys;
        Ok(())
    }
}

/// Go `KVGroupConflictInfos`.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KVGroupConflictInfos {
    /// Go `ConflictInfos`, by KV group.
    #[serde(rename = "conflict-infos", default, skip_serializing_if = "Option::is_none")]
    pub conflict_infos: Option<BTreeMap<String, ConflictInfo>>,
}

impl KVGroupConflictInfos {
    /// Go `addDataConflictInfo`.
    pub fn add_data_conflict_info(&mut self, other: &ConflictInfo) {
        self.add_conflict_info(tidb_ingestor_globalsort::DATA_KV_GROUP, other);
    }

    /// Go `addIndexConflictInfo`.
    pub fn add_index_conflict_info(&mut self, index_id: i64, other: &ConflictInfo) {
        self.add_conflict_info(&tidb_ingestor_globalsort::index_id_2_kv_group(index_id), other);
    }

    /// Go `addConflictInfo`.
    pub fn add_conflict_info(&mut self, kv_group: &str, other: &ConflictInfo) {
        if other.count == 0 {
            return;
        }
        self.conflict_infos.get_or_insert_with(BTreeMap::new).entry(kv_group.to_owned()).or_default().merge(other);
    }

    /// The infos, empty for Go's nil map.
    #[must_use]
    pub fn infos(&self) -> &BTreeMap<String, ConflictInfo> {
        static EMPTY: BTreeMap<String, ConflictInfo> = BTreeMap::new();
        self.conflict_infos.as_ref().unwrap_or(&EMPTY)
    }
}

/// Go `CollectConflictsStepMeta`.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize)]
pub struct CollectConflictsStepMeta {
    /// Go `BaseExternalMeta.ExternalPath`.
    #[serde(rename = "ExternalPath", default)]
    pub external_path: String,
    /// Go `Infos` (external).
    #[serde(rename = "infos", default)]
    pub infos: KVGroupConflictInfos,
    /// Go `RecordedDataKVConflicts`.
    #[serde(rename = "recorded-data-kv-conflicts", default)]
    pub recorded_data_kv_conflicts: i64,
    /// Go `Checksum`.
    #[serde(rename = "checksum", default)]
    pub checksum: Option<Checksum>,
    /// Go `ConflictedRowCount`.
    #[serde(rename = "conflicted-row-count", default)]
    pub conflicted_row_count: i64,
    /// Go `ConflictedRowFilenames`.
    #[serde(rename = "conflicted-row-filenames", default)]
    pub conflicted_row_filenames: Option<Vec<String>>,
    /// Go `ConflictedRowRecordingCapped`.
    #[serde(rename = "conflicted-row-recording-capped", default)]
    pub conflicted_row_recording_capped: bool,
    /// Go `TooManyConflictsFromIndex`.
    #[serde(rename = "too-many-conflicts-from-index", default)]
    pub too_many_conflicts_from_index: bool,
}

impl CollectConflictsStepMeta {
    fn fields(&self) -> GoFields {
        let filenames = self.conflicted_row_filenames.as_deref().unwrap_or_default();
        GoFields::new()
            .add("ExternalPath", &self.external_path, false)
            .add("infos", &self.infos, true)
            .add_if(self.recorded_data_kv_conflicts != 0, "recorded-data-kv-conflicts", &self.recorded_data_kv_conflicts, false)
            .add_if(self.checksum.is_some(), "checksum", &self.checksum, false)
            .add_if(self.conflicted_row_count != 0, "conflicted-row-count", &self.conflicted_row_count, false)
            .add_if(!filenames.is_empty(), "conflicted-row-filenames", &filenames, false)
            .add_if(self.conflicted_row_recording_capped, "conflicted-row-recording-capped", &true, false)
            .add_if(self.too_many_conflicts_from_index, "too-many-conflicts-from-index", &true, false)
    }

    /// Go `Marshal`.
    #[must_use]
    pub fn marshal(&self) -> Vec<u8> {
        self.fields().marshal(&self.external_path)
    }

    /// Go `WriteJSONToExternalStorage`.
    ///
    /// # Errors
    /// The file could not be written.
    pub fn write_external(&self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        write_external(ctx, store, &self.external_path, &self.fields().external())
    }

    /// Go `ReadJSONFromExternalStorage`.
    ///
    /// # Errors
    /// The file could not be read.
    pub fn read_external(&mut self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        if self.external_path.is_empty() {
            return Ok(());
        }
        let ext: Self = read_external(ctx, store, &self.external_path)?;
        self.infos = ext.infos;
        Ok(())
    }
}

/// Go `ConflictResolutionStepMeta`.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize)]
pub struct ConflictResolutionStepMeta {
    /// Go `BaseExternalMeta.ExternalPath`.
    #[serde(rename = "ExternalPath", default)]
    pub external_path: String,
    /// Go `Infos` (external).
    #[serde(rename = "infos", default)]
    pub infos: KVGroupConflictInfos,
}

impl ConflictResolutionStepMeta {
    fn fields(&self) -> GoFields {
        GoFields::new().add("ExternalPath", &self.external_path, false).add("infos", &self.infos, true)
    }

    /// Go `Marshal`.
    #[must_use]
    pub fn marshal(&self) -> Vec<u8> {
        self.fields().marshal(&self.external_path)
    }

    /// Go `WriteJSONToExternalStorage`.
    ///
    /// # Errors
    /// The file could not be written.
    pub fn write_external(&self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        write_external(ctx, store, &self.external_path, &self.fields().external())
    }

    /// Go `ReadJSONFromExternalStorage`.
    ///
    /// # Errors
    /// The file could not be read.
    pub fn read_external(&mut self, ctx: &tidb_gocontext::Context, store: &dyn tidb_objstore::storeapi::Storage) -> Result<(), Error> {
        if self.external_path.is_empty() {
            return Ok(());
        }
        let ext: Self = read_external(ctx, store, &self.external_path)?;
        self.infos = ext.infos;
        Ok(())
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_ingestor_simplesst::MultipleFilesStat;

    fn kv() -> SortedKVMeta {
        SortedKVMeta {
            start_key: b"a<b".to_vec(),
            end_key: b"z&\x00".to_vec(),
            total_kv_size: 10,
            total_kv_cnt: 2,
            multiple_files_stats: Some(vec![MultipleFilesStat {
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
                filenames: vec![["p1/1/2/data/u/0".into(), "p0/1/2/data/u_stat/0".into()]],
                max_overlapping_num: 1,
            }]),
            conflict_info: ConflictInfo { count: 1, files: vec!["1/2/dup".into()] },
        }
    }

    fn infos() -> KVGroupConflictInfos {
        let mut m = BTreeMap::new();
        m.insert("data".to_owned(), ConflictInfo { count: 2, files: vec!["x".into()] });
        m.insert("2".to_owned(), ConflictInfo { count: 1, files: vec![] });
        KVGroupConflictInfos { conflict_infos: Some(m) }
    }

    /// Each meta's Go `Marshal` with and without `ExternalPath`, and its
    /// external file, against a Go master capture (`zzprobe/e05dxf`).
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn global_sort_metas_match_go() {
        let ctx = tidb_gocontext::Context::background();
        let dir = tempfile::tempdir().unwrap();
        let store = tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap();
        let idx = SortedKVMeta { conflict_info: ConflictInfo::default(), ..kv() };
        type Encode<'a> = Box<dyn FnMut(&str) -> (Vec<u8>, Option<Vec<u8>>) + 'a>;
        let mut cases: Vec<(&str, Encode<'_>)> = Vec::new();
        macro_rules! case {
            ($name:expr, $make:expr) => {{
                let store = &store;
                let ctx = &ctx;
                cases.push((
                    $name,
                    Box::new(move |path: &str| {
                        let mut m = $make;
                        m.external_path = path.to_owned();
                        if path.is_empty() {
                            return (m.marshal(), None);
                        }
                        m.write_external(ctx, store).unwrap();
                        (m.marshal(), Some(tidb_objstore::storeapi::Storage::read_file(store, ctx, path).unwrap()))
                    }),
                ));
            }};
        }
        let idx2 = idx.clone();
        case!("import", ImportStepMeta {
            id: 3,
            chunks: Some(vec![Chunk {
                path: "s3://b/t.csv".into(),
                file_size: 100,
                offset: 0,
                end_offset: 100,
                row_id_max: 9,
                ..Chunk::default()
            }]),
            checksum: Some(BTreeMap::from([
                (-1, Checksum { sum: 1, kvs: 2, size: 3 }),
                (2, Checksum { sum: 4, kvs: 5, size: 6 }),
                (10, Checksum { sum: 7, kvs: 8, size: 9 }),
            ])),
            max_ids: Some(BTreeMap::from([(0, 9), (1, 0), (2, 0)])),
            sorted_data_meta: Some(kv()),
            sorted_index_metas: Some(BTreeMap::from([(2, idx2.clone()), (10, SortedKVMeta::default())])),
            recorded_conflict_kv_count: 1,
            ..ImportStepMeta::default()
        });
        case!("merge", MergeSortStepMeta {
            kv_group: "2".into(),
            data_files: Some(vec!["a".into(), "b".into()]),
            sorted_kv_meta: kv(),
            recorded_conflict_kv_count: 1,
            ..MergeSortStepMeta::default()
        });
        case!("ingest", WriteIngestStepMeta {
            kv_group: "data".into(),
            sorted_kv_meta: kv(),
            data_files: Some(vec!["d".into()]),
            stat_files: Some(vec!["s".into()]),
            range_job_keys: Some(vec![b"a".to_vec(), vec![], b"z".to_vec()]),
            range_split_keys: Some(vec![b"a".to_vec()]),
            ts: 42,
            ..WriteIngestStepMeta::default()
        });
        case!("collect", CollectConflictsStepMeta { infos: infos(), recorded_data_kv_conflicts: 2, ..CollectConflictsStepMeta::default() });
        case!("resolve", ConflictResolutionStepMeta { infos: infos(), ..ConflictResolutionStepMeta::default() });
        case!("empty_ingest", WriteIngestStepMeta::default());
        case!("empty_import", ImportStepMeta::default());
        let want: Vec<serde_json::Value> = include_str!("../testdata/global_sort_metas_go.jsonl")
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(want.len(), cases.len());
        for ((name, f), want) in cases.iter_mut().zip(&want) {
            assert_eq!(want["case"], *name);
            let (full, _) = f("");
            assert_eq!(String::from_utf8(full).unwrap(), want["full"].as_str().unwrap(), "{name} full");
            let (internal, external) = f("7/plan/x/1/meta.json");
            assert_eq!(String::from_utf8(internal).unwrap(), want["internal"].as_str().unwrap(), "{name} internal");
            assert_eq!(String::from_utf8(external.unwrap()).unwrap(), want["external"].as_str().unwrap(), "{name} external");
        }
    }

    /// Reading the external file back restores the external fields.
    #[test]
    fn external_fields_round_trip() {
        let ctx = tidb_gocontext::Context::background();
        let dir = tempfile::tempdir().unwrap();
        let store = tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap();
        let m = WriteIngestStepMeta {
            external_path: "1/2/meta.json".into(),
            kv_group: "data".into(),
            sorted_kv_meta: kv(),
            data_files: Some(vec!["d".into()]),
            stat_files: Some(vec!["s".into()]),
            range_job_keys: Some(vec![b"a".to_vec(), b"z".to_vec()]),
            range_split_keys: Some(vec![b"a".to_vec()]),
            ts: 42,
            ..WriteIngestStepMeta::default()
        };
        m.write_external(&ctx, &store).unwrap();
        let mut back: WriteIngestStepMeta = unmarshal(&m.marshal()).unwrap();
        assert_eq!(back.data_files, None);
        back.read_external(&ctx, &store).unwrap();
        assert_eq!(back, m);
    }
}
