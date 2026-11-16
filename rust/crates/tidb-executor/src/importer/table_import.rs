//! Go `pkg/executor/importer/table_import.go`'s `TableImporter`: the
//! engines, encoders and parsers one import of one table runs on, over the
//! local backend.

use std::sync::Arc;

use tidb_gocontext::Context;
use tidb_lightning_backend::{ClosedEngine, EngineConfig, EngineManager, OpenedEngine, TableInfo};
use tidb_lightning_backend_encode::{EncodingConfig, SessionOptions};
use tidb_lightning_backend_kv::PanickingAllocators;
use tidb_lightning_mydump::{Compression, Parser, SourceFileMeta, SourceType};

use super::chunk_process::Chunk;
use super::kv_encode::TableKVEncoder;
use super::LoadDataController;

/// Go `common.IndexEngineID`.
pub const INDEX_ENGINE_ID: i32 = -1;

/// Go `TableImporter`.
pub struct TableImporter {
    /// Go's embedded `LoadDataController`.
    pub controller: LoadDataController,
    /// Go `id`.
    pub id: String,
    /// Go `backend`.
    pub backend: Arc<tidb_ingestor_ingestctrl::local::Backend>,
    /// Go `tableInfo`.
    pub table_info: TableInfo,
    /// Go `keyspace`.
    pub keyspace: Vec<u8>,
    /// Go `encTable`'s allocators.
    pub allocators: PanickingAllocators,
    /// Go `regionSplitSize`.
    pub region_split_size: i64,
    /// Go `regionSplitKeys`.
    pub region_split_keys: i64,
    /// Go `diskQuota`.
    pub disk_quota: i64,
}

/// What [`TableImporter::import_selected_rows`] leaves for `PostProcess`.
pub struct SelectedRowsImported {
    /// The data engine's imported KV count: the imported rows.
    pub data_kv_count: i64,
    /// The local checksum of every imported KV.
    pub checksum: tidb_lightning_verification::KvGroupChecksum,
    /// The allocators' bases, for `RebaseAllocatorBases`.
    pub max_ids: std::collections::BTreeMap<tidb_lightning_backend_kv::AllocatorType, i64>,
}

/// Go `closeAndCleanupEngine`: failures are only logged in Go.
fn close_and_cleanup_engine(ctx: &Context, engine: OpenedEngine<'_>) {
    if let Ok(closed) = engine.close(ctx) {
        let _ = closed.cleanup(ctx);
    }
}

/// Go `config.SplitRegionSize`.
pub const SPLIT_REGION_SIZE: i64 = 96 << 20;
/// Go `config.SplitRegionKeys`.
pub const SPLIT_REGION_KEYS: i64 = 960_000;
/// Go `DefaultDiskQuota`.
pub const DEFAULT_DISK_QUOTA: i64 = 50 << 30;

/// Go `GetImportRootDir`: `<temp-dir>/import-<port>`.
#[must_use]
pub fn get_import_root_dir(cfg: &tidb_config::config_tree::config::Config) -> std::path::PathBuf {
    std::path::Path::new(&cfg.temp_dir).join(format!("import-{}", cfg.port))
}

/// Go `prepareSortDir`: creates the import root, and removes a stale sort
/// directory of the same import.
pub fn prepare_sort_dir(id: &str, cfg: &tidb_config::config_tree::config::Config) -> Result<std::path::PathBuf, String> {
    let import_dir = get_import_root_dir(cfg);
    let sort_dir = import_dir.join(id);
    match std::fs::metadata(&import_dir) {
        Ok(info) if info.is_dir() => {}
        Ok(_) => {
            std::fs::remove_file(&import_dir).map_err(|e| e.to_string())?;
            create_dir_0700(&import_dir)?;
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => create_dir_0700(&import_dir)?,
        Err(e) => return Err(e.to_string()),
    }
    match std::fs::metadata(&sort_dir) {
        Ok(_) => std::fs::remove_dir_all(&sort_dir).map_err(|e| e.to_string())?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.to_string()),
    }
    Ok(sort_dir)
}

fn create_dir_0700(dir: &std::path::Path) -> Result<(), String> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir).map_err(|e| e.to_string())
}

/// Go `adjustDiskQuota`: at most 80% of the sort directory's disk; 0 means
/// that 80%.
#[must_use]
pub fn adjust_disk_quota(disk_quota: i64, sort_dir: &std::path::Path) -> i64 {
    let Ok(size) = tidb_lightning_common::get_storage_size(sort_dir) else {
        return if disk_quota != 0 { disk_quota } else { DEFAULT_DISK_QUOTA };
    };
    let max_disk_quota = (size.capacity as f64 * 0.8) as i64;
    if disk_quota == 0 || disk_quota > max_disk_quota {
        max_disk_quota
    } else {
        disk_quota
    }
}

impl LoadDataController {
    /// Go `getLocalBackendCfg`.
    #[must_use]
    pub fn get_local_backend_cfg(&self, data_dir: std::path::PathBuf) -> tidb_ingestor_ingestctrl::local::BackendConfig {
        tidb_ingestor_ingestctrl::local::BackendConfig {
            local_store_dir: data_dir,
            resource_group_name: String::new(),
            task_type: "ImportInto".to_owned(),
            worker_concurrency: self.plan.thread_cnt,
            mem_table_size: 512 << 20,
            local_writer_mem_cache_size: 128 << 20,
            dupe_detect_enabled: false,
            kv_write_batch_size: 16 << 10,
            should_check_write_stall: true,
            pause_pd_scheduler_scope: tidb_ingestor_ingestctrl::local::PAUSE_PD_SCHEDULER_SCOPE_TABLE.into(),
            // Go `config.DefaultSwitchTiKVModeInterval` under Raft-KV2.
            raft_kv2_switch_mode_duration: if self.plan.is_raft_kv2 {
                tidb_ingestor_ingestctrl::tikv_mode::DEFAULT_SWITCH_TIKV_MODE_INTERVAL
            } else {
                std::time::Duration::ZERO
            },
        }
    }
}

impl TableImporter {
    /// Go `NewTableImporter` over the store's import client. `keyspace` is
    /// the store codec's keyspace prefix.
    pub fn new(
        controller: LoadDataController,
        id: &str,
        import_client: Arc<dyn tidb_ingestor_ingestctrl::region_job::ImportClient>,
        keyspace: Vec<u8>,
    ) -> Result<Self, String> {
        let meta = controller.table.meta().read().clone();
        let allocators = PanickingAllocators::new(meta.sep_auto_inc());
        let cfg = tidb_config::config_tree::config::get_global_config();
        let dir = prepare_sort_dir(id, &cfg)?;
        let backend_config = controller.get_local_backend_cfg(dir.clone());
        let backend = tidb_ingestor_ingestctrl::local::Backend::new(backend_config, import_client).map_err(|e| e.to_string())?;
        let disk_quota = adjust_disk_quota(controller.plan.disk_quota, &dir);
        Ok(Self {
            table_info: TableInfo { id: meta.id, name: meta.name.original().to_owned() },
            controller,
            id: id.to_owned(),
            backend: Arc::new(backend),
            keyspace,
            allocators,
            region_split_size: 2 * SPLIT_REGION_SIZE,
            region_split_keys: 2 * SPLIT_REGION_KEYS,
            disk_quota,
        })
    }

    /// Go `Allocators().Get(tp).Base()` of the row ID, auto-increment and
    /// auto-random allocators, as `PostProcess` rebases them.
    #[must_use]
    pub fn max_ids(&self) -> std::collections::BTreeMap<tidb_lightning_backend_kv::AllocatorType, i64> {
        use tidb_lightning_backend_kv::AllocatorType;
        [AllocatorType::RowId, AllocatorType::AutoIncrement, AllocatorType::AutoRandom]
            .into_iter()
            .map(|tp| (tp, self.allocators.get(tp).base()))
            .collect()
    }
}

/// Go `GetBackoffWeight`: the plan's `tidb_backoff_weight` when larger than
/// the checksum's default.
#[must_use]
pub fn get_backoff_weight(plan: &super::Plan) -> i64 {
    let default = tidb_ingestor_ingestctrl::checksum::DEFAULT_BACKOFF_WEIGHT;
    plan.important_sys_vars
        .get("tidb_backoff_weight")
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|w| *w > default)
        .unwrap_or(default)
}

/// A failed Go `VerifyChecksum`.
#[derive(Debug)]
pub enum VerifyChecksumError {
    /// The remote checksum could not be read.
    Remote(crate::DriverError),
    /// Go `common.ErrChecksumMismatch`, kept as the normalized error so the
    /// DXF retry check sees its ID like Go's `*errors.Error`.
    Mismatch(tidb_terror::TerrorError),
}

impl From<VerifyChecksumError> for crate::DriverError {
    fn from(error: VerifyChecksumError) -> Self {
        match error {
            VerifyChecksumError::Remote(error) => error,
            // Go's lightning errors have no MySQL code: the client sees 1105
            // with the `[RFC code]` text.
            VerifyChecksumError::Mismatch(error) => super::import::plain(error.to_string()),
        }
    }
}

/// Go `VerifyChecksum`: compares the local checksum with the remote one
/// under the plan's `checksum_table` level.
pub fn verify_checksum(
    plan: &super::Plan,
    local: &tidb_lightning_verification::KvChecksum,
    get_remote: impl FnOnce() -> Result<tidb_ingestor_ingestctrl::checksum::RemoteChecksum, crate::DriverError>,
) -> Result<(), VerifyChecksumError> {
    use super::PostOpLevel;
    if plan.checksum == PostOpLevel::Off {
        return Ok(());
    }
    let remote = match get_remote() {
        Ok(remote) => remote,
        Err(e) if plan.checksum != PostOpLevel::Optional => return Err(VerifyChecksumError::Remote(e)),
        Err(_) => return Ok(()),
    };
    if remote.is_equal(local) {
        return Ok(());
    }
    if plan.checksum == PostOpLevel::Optional {
        return Ok(());
    }
    Err(VerifyChecksumError::Mismatch(tidb_lightning_common::errors::ERR_CHECKSUM_MISMATCH.gen_with_stack_by_args(&[
        remote.checksum.into(),
        local.sum().into(),
        remote.total_kvs.into(),
        local.sum_kvs().into(),
        remote.total_bytes.into(),
        local.sum_size().into(),
    ])))
}

/// The session `RemoteChecksumTableBySQL` runs `ADMIN CHECKSUM TABLE` in.
pub trait ChecksumSession {
    /// Go `sqlexec.ExecSQL(ctx, se.GetSQLExecutor(), sql)`.
    fn exec_sql(&self, sql: &str) -> Result<Vec<Vec<tidb_datatype::Datum>>, crate::DriverError>;
    /// Go `GetSessionVars().SetSystemVar(name, value)`.
    fn set_system_var(&self, name: &str, value: &str) -> Result<(), crate::DriverError>;
    /// Go `GetSessionVars().DistSQLScanConcurrency()`.
    fn distsql_scan_concurrency(&self) -> usize;
    /// Go `GetSessionVars().SetDistSQLScanConcurrency(n)`.
    fn set_distsql_scan_concurrency(&self, n: usize);
}

fn datum_u64(d: Option<&tidb_datatype::Datum>) -> u64 {
    match d {
        Some(tidb_datatype::Datum::UInt(v)) => *v,
        Some(tidb_datatype::Datum::Int(v)) => *v as u64,
        _ => 0,
    }
}

/// Go `RemoteChecksumTableBySQL`: `ADMIN CHECKSUM TABLE`, retried up to 3
/// times on a retryable error with the scan concurrency halved each time.
pub fn remote_checksum_table_by_sql(
    se: &dyn ChecksumSession,
    plan: &super::Plan,
) -> Result<tidb_ingestor_ingestctrl::checksum::RemoteChecksum, crate::DriverError> {
    use tidb_ingestor_ingestctrl::checksum::{RemoteChecksum, MIN_DISTSQL_SCAN_CONCURRENCY};
    let table_name = plan.table_info.as_ref().map(|t| t.name.lowercase().to_owned()).unwrap_or_default();
    let sql = format!("ADMIN CHECKSUM TABLE {}", tidb_lightning_common::unique_table(&plan.db_name, &table_name));
    let backup = se.distsql_scan_concurrency();
    let mut factor = 1;
    let mut result = Err(super::import::plain("checksum not run"));
    for _ in 0..3 {
        result = (|| {
            // Go logs and goes on when the variable cannot be set.
            let _ = se.set_system_var("tidb_backoff_weight", &get_backoff_weight(plan).to_string());
            se.set_distsql_scan_concurrency((plan.distsql_scan_concurrency / factor).max(MIN_DISTSQL_SCAN_CONCURRENCY));
            let rows = se.exec_sql(&sql)?;
            let row = rows.first().ok_or_else(|| super::import::plain("empty checksum result"))?;
            let text = |i: usize| row.get(i).and_then(|d| d.sql_string().ok()).unwrap_or_default();
            Ok(RemoteChecksum {
                schema: text(0),
                table: text(1),
                checksum: datum_u64(row.get(2)),
                total_kvs: datum_u64(row.get(3)),
                total_bytes: datum_u64(row.get(4)),
            })
        })();
        let retryable = match &result {
            Ok(_) => false,
            Err(e) => tidb_lightning_common::is_retryable_error(&e.clone().to_mysql_error()),
        };
        if !retryable {
            break;
        }
        factor *= 2;
    }
    se.set_distsql_scan_concurrency(backup);
    result
}

/// What Go's `RebaseTableAllocators` reaches through the store: rebasing
/// one global allocator of the table.
pub trait AllocatorRebaser {
    /// Go `autoid.NewAllocator(...).Rebase(ctx, base, false)` for the
    /// allocator `tp` of the table, unsigned as `unsigned`, with Go's
    /// `CustomAutoIncCacheOption(cache)`.
    fn rebase(
        &self,
        db_id: i64,
        table: &tidb_model::TableInfo,
        tp: tidb_lightning_backend_kv::AllocatorType,
        unsigned: bool,
        cache: i64,
        base: i64,
    ) -> Result<(), crate::DriverError>;
}

/// Go `GetGlobalAutoIDAlloc`'s allocator choice: `(type, unsigned, cache)`
/// of each global allocator of the table.
pub fn get_global_auto_id_alloc_types(
    db_id: i64,
    table: &tidb_model::TableInfo,
) -> Result<Vec<(tidb_lightning_backend_kv::AllocatorType, bool, i64)>, String> {
    use tidb_lightning_backend_kv::AllocatorType;
    if db_id == 0 {
        return Err("internal error: dbID should not be 0".to_owned());
    }
    let has_row_id = tidb_lightning_common::table_has_auto_row_id(table);
    let has_auto_inc = table.get_auto_increment_col_info().is_some();
    let unsigned_inc = table.is_auto_inc_col_unsigned();
    if has_row_id || has_auto_inc {
        let mut allocators = Vec::with_capacity(2);
        if table.sep_auto_inc() && has_auto_inc {
            allocators.push((AllocatorType::AutoIncrement, unsigned_inc, 1));
        }
        allocators.push((AllocatorType::RowId, unsigned_inc, 2));
        return Ok(allocators);
    }
    if table.contains_auto_random_bits() {
        return Ok(vec![(AllocatorType::AutoRandom, table.is_auto_random_bit_col_unsigned(), 2)]);
    }
    Err(format!("internal error: table {} has no auto ID", table.name.original()))
}

/// Go `RebaseAllocatorBases`: each global allocator rebased to the
/// imported rows' maximum IDs.
pub fn rebase_allocator_bases(
    rebaser: &dyn AllocatorRebaser,
    max_ids: &std::collections::BTreeMap<tidb_lightning_backend_kv::AllocatorType, i64>,
    plan: &super::Plan,
) -> Result<(), crate::DriverError> {
    let Some(table) = plan.desired_table_info.as_ref() else { return Ok(()) };
    if !tidb_lightning_common::table_has_auto_id(table) {
        return Ok(());
    }
    for (tp, unsigned, cache) in get_global_auto_id_alloc_types(plan.db_id, table).map_err(super::import::plain)? {
        if let Some(base) = max_ids.get(&tp) {
            rebaser.rebase(plan.db_id, table, tp, unsigned, cache, *base)?;
        }
    }
    Ok(())
}

/// Go `PostProcess`: rebases the allocators, then verifies the checksum.
pub fn post_process(
    se: &dyn ChecksumSession,
    rebaser: &dyn AllocatorRebaser,
    max_ids: &std::collections::BTreeMap<tidb_lightning_backend_kv::AllocatorType, i64>,
    plan: &super::Plan,
    local_checksum: &tidb_lightning_verification::KvGroupChecksum,
) -> Result<(), crate::DriverError> {
    rebase_allocator_bases(rebaser, max_ids, plan)?;
    Ok(verify_checksum(plan, &local_checksum.merged_checksum(), || remote_checksum_table_by_sql(se, plan))?)
}

fn source_type_of(t: i32) -> SourceType {
    match t {
        x if x == SourceType::SQL as i32 => SourceType::SQL,
        x if x == SourceType::CSV as i32 => SourceType::CSV,
        x if x == SourceType::Parquet as i32 => SourceType::Parquet,
        _ => SourceType::Ignore,
    }
}

fn compression_of(c: i32) -> Compression {
    match c {
        1 => Compression::GZ,
        2 => Compression::LZ4,
        3 => Compression::ZStd,
        4 => Compression::XZ,
        5 => Compression::LZO,
        6 => Compression::Snappy,
        _ => Compression::None,
    }
}

impl Chunk {
    /// Go `toSourceFileMeta`.
    #[must_use]
    pub fn to_source_file_meta(&self) -> SourceFileMeta {
        SourceFileMeta {
            path: self.path.clone(),
            r#type: source_type_of(self.r#type),
            compression: compression_of(self.compression),
            file_size: self.file_size,
            parquet_meta: self.parquet_meta.clone(),
            ..SourceFileMeta::default()
        }
    }
}

impl LoadDataController {
    /// Go `calculateSubtaskCnt`.
    #[must_use]
    pub fn calculate_subtask_cnt(&self) -> usize {
        let max_engine_size = self.plan.max_engine_size;
        let total_size = self.total_real_size;
        let mut subtask_count = if total_size <= max_engine_size {
            1.0
        } else {
            (total_size as f64 / max_engine_size as f64).round()
        };
        if self.plan.is_global_sort() && self.execute_nodes_cnt > 0 {
            let nodes = self.execute_nodes_cnt as f64;
            subtask_count = (subtask_count / nodes).ceil() * nodes;
        }
        subtask_count as usize
    }

    /// Go `getAdjustedMaxEngineSize`.
    #[must_use]
    pub fn get_adjusted_max_engine_size(&self) -> i64 {
        let subtask_count = self.calculate_subtask_cnt();
        (self.total_real_size as f64 / subtask_count as f64).ceil() as i64
    }

    /// Go `SetExecuteNodeCnt`.
    pub fn set_execute_node_cnt(&mut self, cnt: usize) {
        self.execute_nodes_cnt = cnt;
    }

    /// Go `toMyDumpFiles`.
    fn to_mydump_files(&self) -> Vec<tidb_lightning_mydump::FileInfo> {
        let table_name = tidb_lightning_mydump::FilterTable {
            schema: self.plan.db_name.clone(),
            name: self.table.meta().read().name.original().to_owned(),
        };
        self.data_files
            .iter()
            .map(|f| tidb_lightning_mydump::FileInfo { table_name: table_name.clone(), file_meta: f.clone() })
            .collect()
    }

    /// Go `PopulateChunks`: the data files split into regions, grouped by
    /// engine; the index engine has no chunk.
    pub fn populate_chunks(
        &self,
        ctx: &Context,
    ) -> Result<std::collections::BTreeMap<i32, Vec<Chunk>>, String> {
        let meta = self.table.meta().read().clone();
        let table_meta = tidb_lightning_mydump::MDTableMeta {
            db: self.plan.db_name.clone(),
            name: meta.name.original().to_owned(),
            data_files: self.to_mydump_files(),
            ..Default::default()
        };
        let store = self.data_store.clone().ok_or_else(|| "the import has no data store".to_owned())?;
        let cfg = tidb_lightning_mydump::DataDivideConfig {
            column_cnt: meta.columns.len(),
            engine_data_size: self.get_adjusted_max_engine_size(),
            max_chunk_size: tidb_lightning_mydump::region::MAX_REGION_SIZE,
            concurrency: self.plan.thread_cnt,
            engine_concurrency: 0,
            batch_import_ratio: 0.0,
            store,
            table_meta,
            skip_parquet_row_count: tidb_lightning_common::skip_read_row_count(&meta),
            strict_format: self.plan.split_file,
            data_character_set: self.plan.charset.clone().unwrap_or_default(),
            data_invalid_char_replace: "\u{FFFD}".to_owned(),
            read_block_size: tidb_lightning_mydump::READ_BLOCK_SIZE as i64,
            csv: self.generate_csv_config(),
        };
        let regions = tidb_lightning_mydump::make_table_regions(ctx, &cfg)?;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64);
        let mut engine_chunks: std::collections::BTreeMap<i32, Vec<Chunk>> = std::collections::BTreeMap::new();
        for region in regions {
            engine_chunks.entry(region.engine_id).or_default().push(Chunk {
                path: region.file_meta.path.clone(),
                file_size: region.file_meta.file_size,
                offset: region.chunk.offset,
                end_offset: region.chunk.end_offset,
                prev_row_id_max: region.chunk.prev_row_id_max,
                row_id_max: region.chunk.row_id_max,
                r#type: region.file_meta.r#type as i32,
                compression: region.file_meta.compression as i32,
                timestamp,
                parquet_meta: region.file_meta.parquet_meta.clone(),
            });
        }
        engine_chunks.insert(INDEX_ENGINE_ID, Vec::new());
        Ok(engine_chunks)
    }
}

impl LoadDataController {
    /// Go `FullTableName`: `` `db`.`table` ``.
    #[must_use]
    pub fn full_table_name(&self) -> String {
        format!(
            "`{}`.`{}`",
            self.plan.db_name.replace('`', "``"),
            self.table.meta().read().name.original().replace('`', "``")
        )
    }

    /// Go `getParser`: the chunk's parser, past the ignored lines of a
    /// chunk at the start of its file, else at the chunk's offset.
    pub fn get_parser(&self, ctx: &Context, chunk: &Chunk) -> Result<Box<dyn Parser + Send>, String> {
        let store = self.data_store.clone().ok_or_else(|| "the import has no data store".to_owned())?;
        let file_meta = chunk.to_source_file_meta();
        let mut parser = super::new_load_data_parser(ctx, &self.plan, &self.generate_csv_config(), &store, &file_meta)?;
        if chunk.offset == 0 {
            super::handle_skip_n_rows(parser.as_mut(), self.plan.ignore_lines).map_err(|e| e.to_string())?;
            parser.set_row_id(chunk.prev_row_id_max);
        } else {
            parser.set_pos(chunk.offset, chunk.prev_row_id_max).map_err(|e| e.to_string())?;
        }
        Ok(parser)
    }

    /// Go `getKVEncoder`.
    pub fn get_kv_encoder(&self, chunk: &Chunk, allocators: PanickingAllocators) -> Result<TableKVEncoder, String> {
        let cfg = EncodingConfig {
            session_options: SessionOptions {
                sql_mode: self.plan.sql_mode,
                timestamp: chunk.timestamp,
                sys_vars: self.plan.important_sys_vars.clone(),
                auto_random_seed: chunk.prev_row_id_max,
                ..SessionOptions::default()
            },
            path: chunk.path.clone(),
            table: self.table.meta().read().clone(),
            use_identity_auto_row_id: false,
        };
        TableKVEncoder::new(&cfg, self, allocators).map_err(|e| e.to_string())
    }
}

impl TableImporter {
    /// Go `getKVEncoder`.
    pub fn get_kv_encoder(&self, chunk: &Chunk) -> Result<TableKVEncoder, String> {
        self.controller.get_kv_encoder(chunk, self.allocators.clone())
    }

    /// Go `GetKVEncoderForDupResolve`.
    pub fn get_kv_encoder_for_dup_resolve(&self) -> Result<TableKVEncoder, String> {
        let cfg = EncodingConfig {
            session_options: SessionOptions {
                sql_mode: self.controller.plan.sql_mode,
                sys_vars: self.controller.plan.important_sys_vars.clone(),
                ..SessionOptions::default()
            },
            path: String::new(),
            table: self.controller.table.meta().read().clone(),
            use_identity_auto_row_id: true,
        };
        TableKVEncoder::new_for_dup_resolve(&cfg, &self.controller, self.allocators.clone()).map_err(|e| e.to_string())
    }

    /// Go `GetKeySpace`.
    #[must_use]
    pub fn key_space(&self) -> &[u8] {
        &self.keyspace
    }

    fn engine_manager(&self) -> EngineManager<'_> {
        EngineManager::new(self.backend.as_ref())
    }

    /// Go `OpenIndexEngine`. The Rust local engine has no compaction, so
    /// Go's compaction threshold is not computed.
    pub fn open_index_engine(&self, ctx: &Context, engine_id: i32) -> Result<OpenedEngine<'_>, String> {
        let cfg = EngineConfig { table_info: self.table_info.clone(), ..EngineConfig::default() };
        self.engine_manager()
            .open_engine(ctx, &cfg, &self.controller.full_table_name(), engine_id)
            .map_err(|e| e.to_string())
    }

    /// Go `OpenDataEngine`.
    pub fn open_data_engine(&self, ctx: &Context, engine_id: i32) -> Result<OpenedEngine<'_>, String> {
        let cfg = EngineConfig { table_info: self.table_info.clone(), ..EngineConfig::default() };
        self.engine_manager()
            .open_engine(ctx, &cfg, &self.controller.full_table_name(), engine_id)
            .map_err(|e| e.to_string())
    }

    /// Go `ImportAndCleanup`: imports and cleans the engine; returns the
    /// data engine's imported KV count.
    pub fn import_and_cleanup(&self, ctx: &Context, closed: &ClosedEngine<'_>) -> Result<i64, String> {
        let import_err = closed.import(ctx, self.region_split_size, self.region_split_keys).err();
        let kv_count = if closed.id() == INDEX_ENGINE_ID {
            0
        } else {
            self.backend.get_imported_kv_count(*closed.uuid().as_bytes())
        };
        let cleanup_err = closed.cleanup(ctx).err();
        match (import_err, cleanup_err) {
            (None, None) => Ok(kv_count),
            (Some(a), None) | (None, Some(a)) => Err(a.to_string()),
            (Some(a), Some(b)) => Err(format!("{a}; {b}")),
        }
    }

    /// Go `ImportSelectedRows` up to its `PostProcess`: `thread_cnt`
    /// workers encode the rows from `chunk_ch` into one data and one index
    /// engine, which are then imported. Returns the data engine's imported
    /// KV count, the local checksum and the allocators' bases for
    /// [`post_process`], which the caller runs with its session.
    ///
    /// # Errors
    /// A row failed to encode, or an engine failed to import.
    pub fn import_selected_rows(
        &self,
        ctx: &Context,
        chunk_ch: &crossbeam_channel::Receiver<super::chunk_process::QueryChunk>,
    ) -> Result<SelectedRowsImported, super::chunk_process::ChunkError> {
        let data_engine = self.open_data_engine(ctx, 1)?;
        let index_engine = match self.open_index_engine(ctx, INDEX_ENGINE_ID) {
            Ok(engine) => engine,
            Err(e) => {
                close_and_cleanup_engine(ctx, data_engine);
                return Err(e.into());
            }
        };
        let checksum = std::sync::Mutex::new(tidb_lightning_verification::KvGroupChecksum::with_keyspace(&self.keyspace));
        let (worker_ctx, cancel) = Context::with_cancel(ctx);
        let failure: std::sync::Mutex<Option<super::chunk_process::ChunkError>> = std::sync::Mutex::new(None);
        let source = super::engine_process::ChunkSource::Query(chunk_ch);
        std::thread::scope(|scope| {
            for _ in 0..self.controller.plan.thread_cnt.max(1) {
                scope.spawn(|| {
                    let chunk = Chunk::default();
                    let mut chunk_checksum = tidb_lightning_verification::KvGroupChecksum::with_keyspace(&self.keyspace);
                    let result = super::engine_process::process_chunk_from(
                        &worker_ctx,
                        &chunk,
                        &source,
                        self,
                        &data_engine,
                        &index_engine,
                        &mut chunk_checksum,
                        None,
                    );
                    checksum.lock().unwrap_or_else(std::sync::PoisonError::into_inner).add(&chunk_checksum);
                    if let Err(e) = result {
                        failure.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(e);
                        cancel.cancel();
                    }
                });
            }
        });
        if let Some(e) = failure.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
            close_and_cleanup_engine(ctx, data_engine);
            close_and_cleanup_engine(ctx, index_engine);
            return Err(e);
        }
        let closed_data = match data_engine.close(ctx) {
            Ok(closed) => closed,
            Err(e) => {
                close_and_cleanup_engine(ctx, index_engine);
                return Err(e.to_string().into());
            }
        };
        let data_kv_count = match self.import_and_cleanup(ctx, &closed_data) {
            Ok(count) => count,
            Err(e) => {
                close_and_cleanup_engine(ctx, index_engine);
                return Err(e.into());
            }
        };
        let closed_index = index_engine.close(ctx).map_err(|e| e.to_string())?;
        self.import_and_cleanup(ctx, &closed_index)?;
        Ok(SelectedRowsImported {
            data_kv_count,
            checksum: checksum.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner),
            max_ids: self.max_ids(),
        })
    }

    /// Go `Close`.
    pub fn close(&self) {
        let _ = self.backend.close();
    }

    /// Go `Allocators`.
    #[must_use]
    pub fn allocators(&self) -> &PanickingAllocators {
        &self.allocators
    }
}
