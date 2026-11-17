//! Go `pkg/lightning/backend` (`backend.go`): the `Backend` interface an
//! importer writes engines through, and `EngineManager`, `OpenedEngine`
//! and `ClosedEngine`, which name engines by table and engine ID.

use std::time::Duration;

use tidb_gocontext::Context;
use tidb_lightning_verification::KvPair;
use uuid::Uuid;

/// Go `importMaxRetryTimes`.
const IMPORT_MAX_RETRY_TIMES: usize = 3;

/// Go `engineNamespace`.
const ENGINE_NAMESPACE: Uuid = Uuid::from_u128(0xd68d_6abe_c59e_45d6_ade8_e2b0_ceb7_bedf);

/// A backend error: Go's error text, whether `common.IsRetryableError`
/// holds for it, and the error it was raised from, which Go's `errors.Cause`
/// reaches (a `*errors.Error` keeps its identity through the backend).
#[derive(Clone, Debug)]
pub struct Error {
    pub message: String,
    pub retryable: bool,
    pub cause: Option<std::sync::Arc<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    /// A non-retryable error.
    pub fn other(message: impl Into<String>) -> Self {
        Self { message: message.into(), retryable: false, cause: None }
    }

    /// A non-retryable error raised from `cause`, with its text.
    pub fn from_cause(cause: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self { message: cause.to_string(), retryable: false, cause: Some(std::sync::Arc::new(cause)) }
    }
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        self.message == other.message && self.retryable == other.retryable
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_deref().map(|cause| cause as &(dyn std::error::Error + 'static))
    }
}

/// Backend results.
pub type Result<T> = std::result::Result<T, Error>;

/// Go `makeTag`.
#[must_use]
pub fn make_tag(table_name: &str, engine_id: i64) -> String {
    format!("{table_name}:{engine_id}")
}

/// Go `MakeUUID`: the engine's tag and its name-based (SHA-1) UUID.
#[must_use]
pub fn make_uuid(table_name: &str, engine_id: i64) -> (String, Uuid) {
    let tag = make_tag(table_name, engine_id);
    let id = Uuid::new_v5(&ENGINE_NAMESPACE, tag.as_bytes());
    (tag, id)
}

/// Go `importdef.TableInfo`: the target table an engine belongs to.
#[derive(Clone, Debug, Default)]
pub struct TableInfo {
    pub id: i64,
    pub name: String,
}

/// Go `LocalEngineConfig`.
#[derive(Clone, Debug, Default)]
pub struct LocalEngineConfig {
    pub compact: bool,
    pub compact_threshold: i64,
    pub compact_concurrency: usize,
    pub block_size: usize,
}

/// Go `ExternalEngineConfig`: a global-sort engine over sorted files in
/// external storage.
#[derive(Clone)]
pub struct ExternalEngineConfig {
    pub ext_store: std::sync::Arc<dyn tidb_objstore::storeapi::Storage>,
    pub data_files: Vec<String>,
    pub stat_files: Vec<String>,
    pub start_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub job_keys: Vec<Vec<u8>>,
    pub split_keys: Vec<Vec<u8>>,
    /// Go `TotalFileSize`: can be an estimated value.
    pub total_file_size: i64,
    /// Go `TotalKVCount`: can be an estimated value.
    pub total_kv_count: i64,
    pub check_hotspot: bool,
    /// Go `MemCapacity`: the memory capacity for the whole subtask.
    pub mem_capacity: i64,
    /// Go `OnDup`: the action when a duplicate key is found during global
    /// sort.
    pub on_dup: tidb_ingestor_engineapi::OnDuplicateKey,
    /// Go `FilePrefix`: the prefix of files recording conflicted KVs.
    pub file_prefix: String,
}

impl std::fmt::Debug for ExternalEngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalEngineConfig")
            .field("data_files", &self.data_files.len())
            .field("stat_files", &self.stat_files.len())
            .field("file_prefix", &self.file_prefix)
            .finish_non_exhaustive()
    }
}

/// Go `EngineConfig`.
#[derive(Clone, Debug, Default)]
pub struct EngineConfig {
    pub table_info: TableInfo,
    pub local: LocalEngineConfig,
    /// Go `External`: the engine is a global-sort engine.
    pub external: Option<ExternalEngineConfig>,
    pub keep_sort_dir: bool,
    /// Go `TS`: the commit TS of the ingested data, allocated at close when 0.
    pub ts: u64,
}

/// Go `LocalWriterConfig`.
#[derive(Clone, Debug, Default)]
pub struct LocalWriterConfig {
    /// Go `Local.IsKVSorted`.
    pub is_kv_sorted: bool,
    /// Go `Local.MemCacheSize`.
    pub mem_cache_size: i64,
    /// Go `TiDB.TableName`.
    pub table_name: String,
}

/// Go `common.ChunkFlushStatus`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ChunkFlushStatus {
    pub flushed: bool,
}

/// Go `EngineWriter`.
pub trait EngineWriter: Send {
    /// Go `AppendRows`.
    fn append_rows(&mut self, ctx: &Context, column_names: &[String], rows: Rows) -> Result<()>;
    /// Go `IsSynced`.
    fn is_synced(&self) -> bool;
    /// Go `Close`.
    fn close(&mut self, ctx: &Context) -> Result<ChunkFlushStatus>;
}

/// Go `encode.Rows` as the import writers receive it: `kv.Pairs`, or an
/// index writer's `kv.GroupedPairs` (each index ID's pairs).
#[derive(Clone, Debug)]
pub enum Rows {
    /// Go `*kv.Pairs`.
    Pairs(Vec<KvPair>),
    /// Go `kv.GroupedPairs`.
    Grouped(std::collections::BTreeMap<i64, Vec<KvPair>>),
}

impl Rows {
    /// Go `kv.Rows2KvPairs`: every pair, the groups' one after another.
    #[must_use]
    pub fn into_kv_pairs(self) -> Vec<KvPair> {
        match self {
            Rows::Pairs(pairs) => pairs,
            Rows::Grouped(groups) => groups.into_values().flatten().collect(),
        }
    }
}

/// Go `Backend`.
pub trait Backend: Send + Sync {
    /// Go `Close`.
    fn close(&self);
    /// Go `RetryImportDelay`.
    fn retry_import_delay(&self) -> Duration;
    /// Go `ShouldPostProcess`.
    fn should_post_process(&self) -> bool;
    /// Go `OpenEngine`.
    fn open_engine(&self, ctx: &Context, config: &EngineConfig, engine_uuid: Uuid) -> Result<()>;
    /// Go `CloseEngine`.
    fn close_engine(&self, ctx: &Context, config: &EngineConfig, engine_uuid: Uuid) -> Result<()>;
    /// Go `ImportEngine`.
    fn import_engine(&self, ctx: &Context, engine_uuid: Uuid, region_split_size: i64, region_split_keys: i64) -> Result<()>;
    /// Go `CleanupEngine`.
    fn cleanup_engine(&self, ctx: &Context, engine_uuid: Uuid) -> Result<()>;
    /// Go `FlushEngine`.
    fn flush_engine(&self, ctx: &Context, engine_uuid: Uuid) -> Result<()>;
    /// Go `FlushAllEngines`.
    fn flush_all_engines(&self, ctx: &Context) -> Result<()>;
    /// Go `LocalWriter`.
    fn local_writer(&self, ctx: &Context, cfg: &LocalWriterConfig, engine_uuid: Uuid) -> Result<Box<dyn EngineWriter>>;
}

/// Go `EngineManager`.
#[derive(Clone)]
pub struct EngineManager<'a> {
    backend: &'a dyn Backend,
}

/// Go `engine`.
#[derive(Clone)]
struct EngineBase<'a> {
    backend: &'a dyn Backend,
    uuid: Uuid,
    id: i32,
}

/// Go `OpenedEngine`.
pub struct OpenedEngine<'a> {
    engine: EngineBase<'a>,
    table_name: String,
    config: EngineConfig,
}

/// Go `ClosedEngine`.
pub struct ClosedEngine<'a> {
    engine: EngineBase<'a>,
}

impl<'a> EngineManager<'a> {
    /// Go `MakeEngineManager`.
    #[must_use]
    pub fn new(backend: &'a dyn Backend) -> Self {
        Self { backend }
    }

    /// Go `EngineManager.OpenEngine`.
    pub fn open_engine(&self, ctx: &Context, config: &EngineConfig, table_name: &str, engine_id: i32) -> Result<OpenedEngine<'a>> {
        let (_, engine_uuid) = make_uuid(table_name, i64::from(engine_id));
        self.backend.open_engine(ctx, config, engine_uuid)?;
        Ok(OpenedEngine {
            engine: EngineBase { backend: self.backend, uuid: engine_uuid, id: engine_id },
            table_name: table_name.to_owned(),
            config: config.clone(),
        })
    }

    /// Go `UnsafeCloseEngine`.
    pub fn unsafe_close_engine(&self, ctx: &Context, cfg: &EngineConfig, table_name: &str, engine_id: i32) -> Result<ClosedEngine<'a>> {
        let (_, engine_uuid) = make_uuid(table_name, i64::from(engine_id));
        self.unsafe_close_engine_with_uuid(ctx, cfg, engine_uuid, engine_id)
    }

    /// Go `UnsafeCloseEngineWithUUID`.
    pub fn unsafe_close_engine_with_uuid(&self, ctx: &Context, cfg: &EngineConfig, engine_uuid: Uuid, id: i32) -> Result<ClosedEngine<'a>> {
        self.backend.close_engine(ctx, cfg, engine_uuid)?;
        Ok(ClosedEngine { engine: EngineBase { backend: self.backend, uuid: engine_uuid, id } })
    }
}

impl<'a> OpenedEngine<'a> {
    /// Go `OpenedEngine.Close`.
    pub fn close(self, ctx: &Context) -> Result<ClosedEngine<'a>> {
        let config = self.config;
        let engine = self.engine;
        engine.backend.close_engine(ctx, &config, engine.uuid)?;
        Ok(ClosedEngine { engine })
    }

    /// Go `OpenedEngine.Flush`.
    pub fn flush(&self, ctx: &Context) -> Result<()> {
        self.engine.backend.flush_engine(ctx, self.engine.uuid)
    }

    /// Go `OpenedEngine.LocalWriter`.
    pub fn local_writer(&self, ctx: &Context, cfg: &LocalWriterConfig) -> Result<Box<dyn EngineWriter>> {
        self.engine.backend.local_writer(ctx, cfg, self.engine.uuid)
    }

    /// Go `GetEngineUUID`.
    #[must_use]
    pub fn uuid(&self) -> Uuid {
        self.engine.uuid
    }

    /// Go `GetID`.
    #[must_use]
    pub fn id(&self) -> i32 {
        self.engine.id
    }

    /// The table name the engine was opened for.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table_name
    }
}

impl ClosedEngine<'_> {
    /// Go `ClosedEngine.Import`: retries a retryable failure up to three
    /// times.
    pub fn import(&self, ctx: &Context, region_split_size: i64, region_split_keys: i64) -> Result<()> {
        let mut last = None;
        for _ in 0..IMPORT_MAX_RETRY_TIMES {
            match self.engine.backend.import_engine(ctx, self.engine.uuid, region_split_size, region_split_keys) {
                Ok(()) => return Ok(()),
                Err(e) if !e.retryable => return Err(e),
                Err(e) => {
                    last = Some(e);
                    std::thread::sleep(self.engine.backend.retry_import_delay());
                }
            }
        }
        let e = last.expect("a failed attempt");
        Err(Error::other(format!(
            "[{}] import reach max retry {IMPORT_MAX_RETRY_TIMES} and still failed: {e}",
            self.engine.uuid
        )))
    }

    /// Go `ClosedEngine.Cleanup`.
    pub fn cleanup(&self, ctx: &Context) -> Result<()> {
        self.engine.backend.cleanup_engine(ctx, self.engine.uuid)
    }

    /// Go `GetUUID`.
    #[must_use]
    pub fn uuid(&self) -> Uuid {
        self.engine.uuid
    }

    /// Go `GetID`.
    #[must_use]
    pub fn id(&self) -> i32 {
        self.engine.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `MakeUUID`: `uuid.NewSHA1(engineNamespace, tag)` (RFC 4122
    /// name-based SHA-1, the values Python's `uuid.uuid5` gives too).
    #[test]
    fn test_make_uuid() {
        let (tag, id) = make_uuid("`db`.`table`", -1);
        assert_eq!(tag, "`db`.`table`:-1");
        assert_eq!(id.to_string(), "7e3f3a3c-67ce-506d-af34-417ec138fbcb");
        assert_eq!(make_uuid("`test`.`t`", 0).1.to_string(), "c32475d3-88af-5442-b950-a0e82e40d337");
    }
}
