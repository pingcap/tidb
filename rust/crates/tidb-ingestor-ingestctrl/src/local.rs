// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/local.go` and `engine_mgr.go`.
use crate::region_job::ImportClient;
use tidb_proto::import_sstpb as sst;
use crate::{Error, Result, engine::Engine};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tidb_gocontext::Context;

fn str_field(key: &'static str, value: &str) -> tidb_log::Field {
    tidb_log::Field::new(key, tidb_log::Value::Str(value.to_owned()))
}

/// Go `BackendConfig`, local-sort settings used by DDL.
#[derive(Clone)]
pub struct BackendConfig {
    /// Go `LocalStoreDir`.
    pub local_store_dir: PathBuf,
    /// Go `ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `TaskType`.
    pub task_type: String,
    /// Go `WorkerConcurrency`.
    pub worker_concurrency: usize,
    /// Go `MemTableSize`.
    pub mem_table_size: usize,
    /// Go `LocalWriterMemCacheSize`.
    pub local_writer_mem_cache_size: usize,
    /// Go `DupeDetectEnabled`.
    pub dupe_detect_enabled: bool,
    /// Go `KVWriteBatchSize`.
    pub kv_write_batch_size: usize,
    /// Go `ShouldCheckWriteStall`.
    pub should_check_write_stall: bool,
    /// Go `CheckpointEnabled`.
    pub checkpoint_enabled: bool,
    /// Go `PausePDSchedulerScope` (`config.PausePDSchedulerScopeTable` or
    /// `PausePDSchedulerScopeGlobal`).
    pub pause_pd_scheduler_scope: String,
    /// Go `RaftKV2SwitchModeDuration`: when non-zero, the engine's key range
    /// is switched to import mode this often while it is ingested.
    pub raft_kv2_switch_mode_duration: std::time::Duration,
}
/// Go `MaxWriteAndIngestRetryTimes`: the max retry times for write and
/// ingest.
pub static MAX_WRITE_AND_INGEST_RETRY_TIMES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(30);


/// Go `config.PausePDSchedulerScopeTable`.
pub const PAUSE_PD_SCHEDULER_SCOPE_TABLE: &str = "table";
/// Go `config.PausePDSchedulerScopeGlobal`.
pub const PAUSE_PD_SCHEDULER_SCOPE_GLOBAL: &str = "global";
/// Go `ForcePartitionRegionThreshold`: force partition ranges only for tables
/// of at least this many regions.
pub const FORCE_PARTITION_REGION_THRESHOLD: usize = 100;
/// Go `Backend`, owning local engines and their import client.
pub struct Backend {
    /// Go backend configuration.
    pub cfg: BackendConfig,
    engine_mgr: crate::engine_mgr::EngineManager,
    importer: Arc<dyn ImportClient>,
    write_limiter: crate::localhelper::StoreWriteLimiter,
    collector: Mutex<Option<crate::region_job::Collector>>,
}
impl Backend {
    /// Go `NewBackend`.
    pub fn new(cfg: BackendConfig, importer: Arc<dyn ImportClient>) -> Result<Self> {
        importer.check_multi_ingest_support(&Context::background())?;
        let engine_mgr = crate::engine_mgr::EngineManager::new(cfg.clone())?;
        Ok(Self {
            cfg,
            engine_mgr,
            importer,
            write_limiter: crate::localhelper::StoreWriteLimiter::new(0),
            collector: Mutex::new(None),
        })
    }

    /// Go `SetCollector`: told the bytes and pairs of every written job of
    /// an external engine.
    pub fn set_collector(&self, collector: Option<crate::region_job::Collector>) {
        *self.collector.lock().unwrap() = collector;
    }
    /// Go `Backend.OpenEngine` and `engineManager.OpenEngine`.
    pub fn open_engine(&self, id: [u8; 16], ts: u64) -> Result<Arc<Engine>> {
        self.engine_mgr.open_engine(id, ts)
    }

    /// Go `Backend.ImportEngine`: the external engine of `id`, or its local
    /// engine, written and ingested into TiKV through SST write and ingest
    /// RPCs, never transactional key writes.
    pub fn import_engine(
        &self,
        ctx: &Context,
        id: [u8; 16],
        region_split_size: u64,
        region_split_keys: usize,
    ) -> Result<()> {
        let (region_split_size, region_split_keys) =
            self.adjust_region_split_size_keys(ctx, region_split_size, region_split_keys);
        let engine_uuid = uuid::Uuid::from_bytes(id).to_string();
        if let Some(external) = self.engine_mgr.get_external_engine(id) {
            let mut external = external.lock().unwrap();
            return self.import_engine_inner(ctx, &engine_uuid, external.as_mut(), region_split_size, region_split_keys);
        }
        // skip if engine not exist. See the comment of `CloseEngine` for more detail.
        let Ok(engine) = self.engine(id) else {
            return Ok(());
        };
        // Go `lockEngine(engineUUID, importMutexStateImport)`.
        engine.set_importing(true);
        let result = LocalEngine::new(Arc::clone(&engine), region_split_size, region_split_keys).and_then(|mut local| {
            self.import_engine_inner(ctx, &engine_uuid, &mut local, region_split_size, region_split_keys)
        });
        engine.set_importing(false);
        result
    }

    /// Go `IngestExternalEngine`: an external engine created by
    /// `globalsort.NewExternalEngine` written and ingested into TiKV.
    ///
    /// # Errors
    /// The engine's data failed to split, write or ingest.
    pub fn ingest_external_engine(
        &self,
        ctx: &Context,
        e: &mut tidb_ingestor_globalsort::Engine,
        region_split_size: u64,
        region_split_keys: usize,
    ) -> Result<()> {
        let (region_split_size, region_split_keys) =
            self.adjust_region_split_size_keys(ctx, region_split_size, region_split_keys);
        self.import_engine_inner(ctx, "external", e, region_split_size, region_split_keys)
    }

    /// Go `adjustRegionSplitSizeKeys`: the given region split size and keys,
    /// raised to TiKV's configured ones when those are larger.
    fn adjust_region_split_size_keys(
        &self,
        ctx: &Context,
        mut region_split_size: u64,
        mut region_split_keys: usize,
    ) -> (u64, usize) {
        let split_conf = self.importer.all_stores(ctx).and_then(|stores| {
            get_region_split_size_keys(&stores, |host| self.importer.split_conf_from_store(ctx, host))
        });
        match split_conf {
            Ok((kv_region_split_size, kv_region_split_keys)) => {
                region_split_size = region_split_size.max(kv_region_split_size.max(0) as u64);
                region_split_keys = region_split_keys.max(kv_region_split_keys.max(0) as usize);
            }
            Err(error) => tidb_log::warn(
                "fail to get region split keys and size",
                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
            ),
        }
        (region_split_size, region_split_keys)
    }

    /// Go `Backend.ImportEngine` once the engine is chosen.
    fn import_engine_inner(
        &self,
        ctx: &Context,
        engine_uuid: &str,
        e: &mut dyn tidb_ingestor_engineapi::Engine,
        region_split_size: u64,
        region_split_keys: usize,
    ) -> Result<()> {
        let (lf_total_size, lf_length) = e.kv_statistics();
        if lf_total_size == 0 {
            // engine is empty, this is likes because it's a index engine but the table contains no index
            tidb_log::info("engine contains no kv, skip import", &[str_field("engine", engine_uuid)]);
            return Ok(());
        }
        // split sorted file into range about regionSplitSize per file
        let split_keys = get_region_split_keys(e, region_split_size, region_split_keys)?;
        let (start_key, end_key) = (split_keys[0].clone(), split_keys[split_keys.len() - 1].clone());

        let mut force_split_threshold = FORCE_PARTITION_REGION_THRESHOLD;
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/ForcePartitionRegionThreshold",
            &mut force_split_threshold,
        );
        // We only force partition range when the table is large enough (>= 100 regions).
        // This is to avoid unnecessary RPC calls for small tables.
        let _remove_table_split_range = (tidb_config_kerneltype::is_classic()
            && !start_key.is_empty()
            && !end_key.is_empty()
            && split_keys.len() - 1 >= force_split_threshold)
            .then(|| -> Result<ForceTableSplitRange<'_>> {
                let stores = self.importer.all_stores(ctx)?;
                Ok(self.force_table_split_range(&start_key, &end_key, &stores))
            })
            .transpose()?;

        let encode = |key: &[u8]| {
            if key.is_empty() {
                Vec::new()
            } else {
                let mut encoded = Vec::new();
                tidb_codec::encode_bytes(&mut encoded, key);
                encoded
            }
        };
        let _paused = if self.cfg.pause_pd_scheduler_scope == PAUSE_PD_SCHEDULER_SCOPE_TABLE {
            tidb_log::info("pause pd scheduler of table scope", &[]);
            let pd_http = self
                .importer
                .pd_http_client()
                .ok_or_else(|| Error::Other("pd http client is not available".into()))?;
            Some(
                tidb_br::pdutil::pause_schedulers_by_key_range(pd_http, &encode(&start_key), &encode(&end_key))
                    .map_err(Error::Other)?,
            )
        } else {
            None
        };

        // Go `switchModeBySplitKeys`: the range stays in import mode until
        // the import ends.
        let (switch_ctx, stop_switch) = Context::with_cancel(ctx);
        let switching = (!self.cfg.raft_kv2_switch_mode_duration.is_zero()).then(|| {
            tidb_log::info("switch import mode of ranges", &[]);
            crate::tikv_mode::switch_mode_periodically(
                &switch_ctx,
                Arc::clone(&self.importer),
                sst::Range { start: encode(&start_key), end: encode(&end_key) },
                self.cfg.raft_kv2_switch_mode_duration,
            )
        });

        tidb_log::info(
            "start import engine",
            &[
                str_field("uuid", engine_uuid),
                tidb_log::Field::new("region ranges", tidb_log::Value::I64(split_keys.len() as i64 - 1)),
                tidb_log::Field::new("count", tidb_log::Value::I64(lf_length)),
                tidb_log::Field::new("size", tidb_log::Value::I64(lf_total_size)),
            ],
        );
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/ReadyForImportEngine",
            &mut (),
        );
        let result = self.do_import(ctx, e, &split_keys, region_split_size, region_split_keys);
        stop_switch.cancel();
        if let Some(switching) = switching {
            let _ = switching.join();
        }
        result?;
        let (imported_size, imported_length) = e.imported_statistics();
        verify_imported_statistics(e, imported_length)?;
        tidb_log::info(
            "import engine success",
            &[
                str_field("uuid", engine_uuid),
                tidb_log::Field::new("size", tidb_log::Value::I64(lf_total_size)),
                tidb_log::Field::new("kvs", tidb_log::Value::I64(lf_length)),
                tidb_log::Field::new("importedSize", tidb_log::Value::I64(imported_size)),
                tidb_log::Field::new("importedCount", tidb_log::Value::I64(imported_length)),
            ],
        );
        Ok(())
    }

    /// Go `Backend.doImport` and `prepareAndSendJob`: the regions split and
    /// scattered at `split_keys` when the engine needs more than one, then
    /// the region jobs of every loaded range.
    fn do_import(
        &self,
        ctx: &Context,
        engine: &mut dyn tidb_ingestor_engineapi::Engine,
        split_keys: &[Vec<u8>],
        region_split_size: u64,
        region_split_keys: usize,
    ) -> Result<()> {
        let (lf_total_size, lf_length) = engine.kv_statistics();
        // if all the kv can fit in one region, skip split regions. TiDB will split one region for
        // the table when table is created.
        let need_split = split_keys.len() > 2
            || lf_total_size > region_split_size as i64
            || lf_length > region_split_keys as i64
            || tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/forceSplitRegion")
                .is_some();
        if need_split {
            let mut last_error = None;
            for attempt in 0..MAX_RETRY_TIMES {
                if tidb_failpoint::eval(
                    "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/skipSplitAndScatter",
                )
                .is_some()
                {
                    last_error = None;
                    break;
                }
                match self.split_and_scatter_region_in_batches(
                    ctx,
                    split_keys,
                    crate::rate_limiter_param::get_max_batch_split_ranges() as usize,
                    crate::rate_limiter_param::get_max_split_range_per_sec(),
                ) {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
                if ctx.sleep(std::time::Duration::from_secs((10u64 << attempt.min(4)).min(120))) {
                    return Err(Error::Other(ctx.err().unwrap().to_string()));
                }
            }
            if let Some(error) = last_error {
                return Err(error);
            }
        }
        let collector = self.collector.lock().unwrap().clone();
        crate::region_job::do_import(
            ctx,
            engine,
            self.importer.as_ref(),
            &self.cfg,
            region_split_size,
            region_split_keys,
            &self.write_limiter,
            collector.as_ref(),
        )
    }

    /// Go `Backend.forceTableSplitRange`: asks every TiKV to force-partition
    /// compactions at the table's range, refreshed every TTL/5, removed when
    /// the returned handle drops.
    fn force_table_split_range(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        stores: &[tidb_proto::metapb::Store],
    ) -> ForceTableSplitRange<'_> {
        const TTL_SECOND: u64 = 3600; // default 1 hour
        let range = sst::Range { start: start_key.to_vec(), end: end_key.to_vec() };
        let store_ids: Vec<(u64, String)> = stores
            .iter()
            .filter(|store| {
                !store.status_address.is_empty()
                    && !store.labels.iter().any(|l| l.key == "engine" && l.value == "tiflash")
            })
            .map(|store| (store.id, store.status_address.clone()))
            .collect();
        let add = sst::AddPartitionRangeRequest { range: Some(range.clone()), ttl_seconds: TTL_SECOND };
        let add_table_split_range = {
            let (importer, store_ids, add) = (self.importer.clone(), store_ids.clone(), add.clone());
            move || {
                call_on_stores(&store_ids, "call AddForcePartitionRange", |id| {
                    tidb_failpoint::inject_call(
                        "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/AddPartitionRangeForTable",
                        &mut (),
                    );
                    importer.add_force_partition_range(id, add.clone())
                });
            }
        };
        add_table_split_range();
        let (cancel, cancelled) = crossbeam_channel::bounded::<()>(0);
        let refresher = std::thread::spawn(move || loop {
            crossbeam_channel::select! {
                recv(cancelled) -> _ => return,
                recv(crossbeam_channel::after(std::time::Duration::from_secs(TTL_SECOND) / 5)) -> _ => add_table_split_range(),
            }
        });
        ForceTableSplitRange {
            backend: self,
            store_ids,
            remove: sst::RemovePartitionRangeRequest { range: Some(range) },
            cancel: Some(cancel),
            refresher: Some(refresher),
        }
    }
    /// Go `Backend.UpdateWriteSpeedLimit`.
    pub fn update_write_speed_limit(&self, limit: i64) {
        self.write_limiter.update_limit(limit);
    }
    /// Go `Backend.GetWriteSpeedLimit`.
    pub fn get_write_speed_limit(&self) -> i64 {
        self.write_limiter.limit()
    }
    /// Go `Backend.splitAndScatterRegionInBatches`.
    pub fn split_and_scatter_region_in_batches(
        &self,
        ctx: &Context,
        keys: &[Vec<u8>],
        batch_count: usize,
        max_per_sec: f64,
    ) -> Result<()> {
        crate::localhelper::split_and_scatter_region_in_batches(
            ctx,
            self.importer.as_ref(),
            keys,
            batch_count,
            max_per_sec,
        )
    }

    /// Go `DupeController.CollectRemoteDuplicateRows` for an index key range.
    pub fn collect_remote_duplicate_rows(
        &self,
        ctx: &Context,
        start: &[u8],
        end: &[u8],
        min_commit_ts: u64,
    ) -> Result<()> {
        crate::duplicate::collect_remote_duplicate_rows(
            ctx,
            self.importer.as_ref(),
            &self.cfg,
            start,
            end,
            min_commit_ts,
        )
    }
    /// Go `Backend.SetTSBeforeImportEngine`: a zero `ts` allocates one from
    /// PD.
    pub fn set_ts_before_import_engine(&self, id: [u8; 16], mut ts: u64) -> Result<()> {
        let engine = self.engine(id)?;
        if ts == 0 {
            let allocated = self.importer.get_ts()?;
            if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/afterSetTSBeforeImportEngine").is_some() {
                return Err(Error::Other("mock err".into()));
            }
            ts = allocated;
        }
        engine.set_ts(ts)
    }
    /// Go `Backend.ResetEngineSkipAllocTS`.
    pub fn reset_engine_skip_alloc_ts(&self, id: [u8; 16], generation: Option<u64>) -> Result<()> {
        self.engine_mgr.reset_imported(id, generation)
    }
    /// Go `Backend.CleanupEngine`.
    pub fn cleanup_engine(&self, id: [u8; 16]) -> Result<()> {
        self.engine_mgr.cleanup_engine(id)
    }
    /// Go `Backend.Close`.
    pub fn close(&self) -> Result<()> {
        self.engine_mgr.close()
    }
    /// Go `Backend.TotalMemoryConsume`.
    pub fn total_memory_consume(&self) -> u64 {
        self.engine_mgr.total_memory_consume()
    }
    /// Go `Backend.FlushEngine`.
    pub fn flush_engine(&self, id: [u8; 16]) -> Result<()> {
        self.engine_mgr.flush_engine(id)
    }
    /// Go `Backend.FlushAllEngines`.
    pub fn flush_all_engines(&self) -> Result<()> {
        self.engine_mgr.flush_all_engines()
    }
    /// Go `engineManager.rLockEngine`.
    pub(crate) fn engine(&self, id: [u8; 16]) -> Result<Arc<Engine>> {
        self.engine_mgr.engine(id)
    }
    /// The import client the backend writes and allocates TSOs through.
    pub(crate) fn importer(&self) -> &dyn ImportClient {
        self.importer.as_ref()
    }
    /// Go `Backend.GetImportedKVCount`.
    pub fn get_imported_kv_count(&self, id: [u8; 16]) -> i64 {
        self.engine_mgr.get_imported_kv_count(id) as i64
    }
    /// Go `Backend.GetExternalEngineKVStatistics`.
    pub fn get_external_engine_kv_statistics(&self, id: [u8; 16]) -> (i64, i64) {
        self.engine_mgr.get_external_engine_kv_statistics(id)
    }
    /// Go `Backend.GetExternalEngineConflictInfo`.
    pub fn get_external_engine_conflict_info(&self, id: [u8; 16]) -> tidb_ingestor_engineapi::ConflictInfo {
        self.engine_mgr.get_external_engine_conflict_info(id)
    }
    /// Go `engineManager.closeEngine`'s external branch: the global-sort
    /// engine over `cfg`'s sorted files, at `ts` or a TS allocated now.
    pub fn close_external_engine(
        &self,
        id: [u8; 16],
        cfg: &tidb_lightning_backend::ExternalEngineConfig,
        ts: u64,
    ) -> Result<()> {
        let ts = if ts == 0 { self.importer.get_ts()? } else { ts };
        self.engine_mgr.close_external_engine(id, cfg, ts);
        Ok(())
    }
}
/// Go's local `Engine` as `engineapi.Engine`, with the region split size and
/// key count `ImportEngine` sets on it (`localEngine.regionSplitSize`,
/// `regionSplitKeyCnt`). Its ingest data is the engine itself.
pub(crate) struct LocalEngine {
    id: String,
    engine: Arc<Engine>,
    region_split_size: u64,
    region_split_keys: usize,
    /// Go `TotalSize` and `Length`, read from the closed engine's sorted
    /// runs once, so a read failure fails the import instead of reading as
    /// an empty engine.
    kv_statistics: (i64, i64),
}

impl LocalEngine {
    /// The engine under its UUID, with `ImportEngine`'s region split size
    /// and key count.
    pub(crate) fn new(engine: Arc<Engine>, region_split_size: u64, region_split_keys: usize) -> Result<Self> {
        let id = engine.path().file_name().map(|name| name.to_string_lossy().into_owned()).unwrap_or_default();
        let (size, count) = engine.kv_statistics()?;
        Ok(Self { id, engine, region_split_size, region_split_keys, kv_statistics: (size as i64, count as i64) })
    }
}

fn engine_api_error(error: Error) -> tidb_ingestor_engineapi::Error {
    Box::new(error)
}

impl tidb_ingestor_engineapi::Engine for LocalEngine {
    fn id(&self) -> String {
        self.id.clone()
    }

    /// Go `Engine.LoadIngestData`: one range of the engine per batch.
    fn load_ingest_data(
        &mut self,
        ctx: &Context,
        out: &mut dyn FnMut(tidb_ingestor_engineapi::DataAndRanges) -> std::result::Result<(), tidb_ingestor_engineapi::Error>,
    ) -> std::result::Result<(), tidb_ingestor_engineapi::Error> {
        let data: Arc<dyn tidb_ingestor_engineapi::IngestData> = self.engine.clone();
        self.engine
            .load_ingest_data(ctx, |(start, end)| {
                out(tidb_ingestor_engineapi::DataAndRanges {
                    data: Arc::clone(&data),
                    sorted_ranges: vec![tidb_ingestor_engineapi::Range { start, end }],
                })
                .map_err(crate::region_job::engine_error)
            })
            .map_err(engine_api_error)
    }

    fn kv_statistics(&self) -> (i64, i64) {
        self.kv_statistics
    }

    fn imported_statistics(&self) -> (i64, i64) {
        let (size, count) = self.engine.imported_statistics();
        (size as i64, count as i64)
    }

    fn conflict_info(&self) -> tidb_ingestor_engineapi::ConflictInfo {
        tidb_ingestor_engineapi::ConflictInfo::default()
    }

    fn get_key_range(&self) -> std::result::Result<(Vec<u8>, Vec<u8>), tidb_ingestor_engineapi::Error> {
        Ok(self.engine.get_key_range().map_err(engine_api_error)?.unwrap_or_default())
    }

    fn get_region_split_keys(&self) -> std::result::Result<Vec<Vec<u8>>, tidb_ingestor_engineapi::Error> {
        self.engine
            .get_region_split_keys(self.region_split_size, self.region_split_keys as u64)
            .map_err(engine_api_error)
    }

    fn close(&mut self) -> std::result::Result<(), tidb_ingestor_engineapi::Error> {
        Ok(())
    }
}

/// Go `getRegionSplitKeys`: the engine's start and end keys when it fits in
/// one region, its region split keys otherwise.
fn get_region_split_keys(
    engine: &dyn tidb_ingestor_engineapi::Engine,
    size_limit: u64,
    keys_limit: usize,
) -> Result<Vec<Vec<u8>>> {
    let (start_key, end_key) = engine.get_key_range().map_err(crate::region_job::engine_error)?;
    if start_key.is_empty() {
        return Err(Error::Other("could not find first pair".into()));
    }
    let (engine_file_total_size, engine_file_length) = engine.kv_statistics();
    if engine_file_total_size <= size_limit as i64 && engine_file_length <= keys_limit as i64 {
        return Ok(vec![start_key, end_key]);
    }
    let keys = engine.get_region_split_keys().map_err(crate::region_job::engine_error);
    tidb_log::info(
        "split engine key ranges",
        &[
            str_field("engine", &engine.id()),
            tidb_log::Field::new("totalSize", tidb_log::Value::I64(engine_file_total_size)),
            tidb_log::Field::new("totalCount", tidb_log::Value::I64(engine_file_length)),
            tidb_log::Field::new("len(keys)", tidb_log::Value::I64(keys.as_ref().map_or(0, |keys| keys.len() as i64))),
        ],
    );
    keys
}

/// Go `verifyImportedStatistics`: an external engine must have imported
/// every pair it loaded.
fn verify_imported_statistics(engine: &dyn tidb_ingestor_engineapi::Engine, imported_kv_count: i64) -> Result<()> {
    let any: &dyn std::any::Any = engine;
    if let Some(ext_engine) = any.downcast_ref::<tidb_ingestor_globalsort::Engine>() {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/skipOnDuplicateKeyCheck").is_some() {
            return Ok(());
        }
        // Verify the imported statistics after import.
        // For external engine, use the total number of KVs loaded in LoadIngestData
        // (i.e., len(e.memKVsAndBuffers.kvs) across all batches) as the expected count.
        let expected_kv_count = ext_engine.get_total_loaded_kvs_count();
        if imported_kv_count != expected_kv_count {
            return Err(Error::Other(format!(
                "imported length mismatch, expected {expected_kv_count}, got {imported_kv_count}"
            )));
        }
    }
    Ok(())
}

impl crate::disk_quota::DiskUsage for Backend {
    /// Go `Backend.EngineFileSizes`.
    fn engine_file_sizes(&self) -> Vec<crate::disk_quota::EngineFileSize> {
        self.engine_mgr.engine_file_sizes()
    }
}

/// Go `nextKey`: integer row handles advance to the next row; other keys
/// append zero. Empty is infinity and remains empty.
pub fn next_key(key: &[u8]) -> Vec<u8> {
    if key.is_empty() {
        return Vec::new();
    }
    if key.len() == 19 && key[0] == b't' && &key[9..11] == b"_r" {
        let handle = u64::from_be_bytes(key[11..19].try_into().unwrap());
        if handle == u64::MAX {
            let table = u64::from_be_bytes(key[1..9].try_into().unwrap());
            let mut next = vec![b't'];
            next.extend_from_slice(&table.wrapping_add(1).to_be_bytes());
            return next;
        }
        let mut next = key[..11].to_vec();
        next.extend_from_slice(&(handle + 1).to_be_bytes());
        return next;
    }
    let mut next = key.to_vec();
    next.push(0);
    next
}
/// Go `splitRangeBySizeProps`.
pub fn split_range_by_size_props(
    full_range: (Vec<u8>, Vec<u8>),
    props: &crate::engine::SizeProperties,
    size_limit: u64,
    keys_limit: u64,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let (mut key, end) = full_range;
    let (mut size, mut keys) = (0, 0);
    let mut ranges: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for prop in props.index_handles.values() {
        if prop.key <= key {
            continue;
        }
        if prop.key > end {
            break;
        }
        size += prop.size;
        keys += prop.keys;
        if size >= size_limit || keys >= keys_limit {
            ranges.push((key, prop.key.clone()));
            key = prop.key.clone();
            size = 0;
            keys = 0;
        }
    }
    if key < end {
        if !ranges.is_empty() && keys == 0 {
            ranges.last_mut().unwrap().1 = end;
        } else {
            ranges.push((key, end));
        }
    }
    ranges
}
#[cfg(test)]
mod key_tests {
    use super::*;
    /// Go `TestNextKey`.
    #[test]
    fn test_next_key() {
        assert!(next_key(&[]).is_empty());
        for key in [vec![0], vec![255], vec![1, 255], b"abc".to_vec()] {
            let mut expected = key.clone();
            expected.push(0);
            assert_eq!(next_key(&key), expected);
        }
        assert!(next_key(&[1, 255]) < vec![2]);
        assert!(next_key(&[1, 255]) < vec![1, 255, 0, 1, 2]);
        /// Go `TestNextKey row-key fixture`.
        fn row(table: i64, handle: i64) -> Vec<u8> {
            let mut key = vec![b't'];
            key.extend_from_slice(&((table as u64) ^ (1 << 63)).to_be_bytes());
            key.extend_from_slice(b"_r");
            key.extend_from_slice(&((handle as u64) ^ (1 << 63)).to_be_bytes());
            key
        }
        for handle in [i64::MIN, 1, 255, i32::MAX as i64 - 1] {
            assert_eq!(next_key(&row(1, handle)), row(1, handle + 1));
        }
        assert_eq!(next_key(&row(1, i64::MAX)), row(2, 0)[..9]);
        // Go's common-handle case must append zero even when a second call
        // produces the integer-handle length; the original prefix stays valid.
        let mut common = row(1, 0)[..11].to_vec();
        common.extend_from_slice(b"1234567");
        let once = next_key(&common);
        assert_eq!(&once[..common.len()], &common);
        let _ = next_key(&once);
    }
}

/// Go `checkDiskAvail`; unknown heartbeat sizes are best-effort skips.
pub fn check_disk_avail(store: &tikv_client::pd_http::StoreInfo) -> Result<()> {
    let Some(capacity) = ram_in_bytes(&store.status.capacity).filter(|n| *n > 0) else {
        return Ok(());
    };
    let Some(available) = ram_in_bytes(&store.status.available) else {
        return Ok(());
    };
    if available.saturating_mul(100) / capacity < 10 {
        let kind = if store
            .store
            .labels
            .iter()
            .any(|label| label.key == "engine" && label.value == "tiflash")
        {
            "TiFlash"
        } else {
            "TiKV"
        };
        return Err(Error::Ingest(tidb_ingestor_errdef::ERR_KV_DISK_FULL.gen_with_stack(format!("the remaining storage capacity of {kind}({}) is less than 10%; please increase the storage capacity of {kind} and try again",store.store.address))));
    }
    Ok(())
}
/// Go `units.RAMInBytes`, for PD heartbeat capacities.
fn ram_in_bytes(text: &str) -> Option<i64> {
    let end = text
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(text.len());
    if end == 0 {
        return None;
    }
    let value = text[..end].parse::<f64>().ok()?;
    let mut suffix = text[end..]
        .strip_prefix(' ')
        .unwrap_or(&text[end..])
        .to_ascii_lowercase();
    let power = match suffix.chars().next() {
        Some('k') => 1,
        Some('m') => 2,
        Some('g') => 3,
        Some('t') => 4,
        Some('p') => 5,
        _ => 0,
    };
    if power != 0 {
        suffix.remove(0);
    }
    if suffix.starts_with('i') {
        suffix.remove(0);
    }
    if suffix.starts_with('b') {
        suffix.remove(0);
    }
    if !suffix.is_empty() || !value.is_finite() {
        return None;
    }
    let bytes = value * 1024_f64.powi(power);
    if bytes > i64::MAX as f64 {
        return None;
    }
    Some(bytes as i64)
}
/// Go `GetRegionSplitSizeKeys`: exclude tombstones and TiFlash, resolve a
/// wildcard status host against the store address, and try subsequent stores.
pub fn get_region_split_size_keys(
    stores: &[tidb_proto::metapb::Store],
    mut get_config: impl FnMut(&str) -> Result<(i64, i64)>,
) -> Result<(i64, i64)> {
    for store in stores {
        if store.state == tidb_proto::metapb::StoreState::Tombstone as i32
            || store.status_address.is_empty()
            || store
                .labels
                .iter()
                .any(|label| label.key == "engine" && label.value == "tiflash")
        {
            continue;
        }
        let mut address = store.status_address.clone();
        if let Some((host, port)) = address.rsplit_once(':') {
            if ["0.0.0.0", "::", "[::]"].contains(&host) {
                if let Some((store_host, _)) = store.address.rsplit_once(':') {
                    address = format!("{store_host}:{port}");
                }
            }
        }
        if let Ok(result) = get_config(&address) {
            return Ok(result);
        }
    }
    Err(Error::Other("get region split size and keys failed".into()))
}
/// Go `checkTiFlashVersion`, checking only tables with data files. Metadata
/// and replica rows are supplied by targetInfoGetter's SQL boundary.
pub fn check_ti_flash_version(
    version: (u64, u64, u64),
    tables: &[(String, String, bool)],
    replicas: &[(String, String)],
) -> Result<()> {
    if version >= (4, 0, 5) {
        return Ok(());
    }
    let conflicts: Vec<_> = tables
        .iter()
        .filter(|(db, table, has_data)| {
            *has_data && replicas.contains(&(db.clone(), table.clone()))
        })
        .map(|(db, table, _)| format!("`{}`.`{}`", db.replace('`', "``"), table.replace('`', "``")))
        .collect();
    if !conflicts.is_empty() {
        return Err(Error::Other(format!(
            "lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: [{}]. Please either upgrade TiDB to version >= 4.0.5 or add TiFlash replica after load data.",
            conflicts.join(", ")
        )));
    }
    Ok(())
}
#[cfg(test)]
mod check_tests {
    use super::*;
    /// Go `TestCheckDiskAvail`, all four heartbeat/store cases.
    #[test]
    fn test_check_disk_avail() {
        use tikv_client::pd_http::{MetaStore, StoreInfo, StoreLabel, StoreStatus};
        let mut store = StoreInfo {
            status: StoreStatus {
                capacity: "100 GB".into(),
                available: "50 GB".into(),
                ..Default::default()
            },
            ..Default::default()
        };
        check_disk_avail(&store).unwrap();
        check_disk_avail(&StoreInfo::default()).unwrap();
        store.status.available = "5 GB".into();
        store.store.address = "127.0.0.1:20160".into();
        let err = check_disk_avail(&store).unwrap_err();
        assert!(tidb_ingestor_errdef::ERR_KV_DISK_FULL.equal(Some(&err)));
        assert!(err.to_string().contains("TiKV(127.0.0.1:20160)"));
        store.store = MetaStore {
            address: "127.0.0.1:3930".into(),
            labels: vec![StoreLabel {
                key: "engine".into(),
                value: "tiflash".into(),
            }],
            ..Default::default()
        };
        let err = check_disk_avail(&store).unwrap_err();
        assert!(tidb_ingestor_errdef::ERR_KV_DISK_FULL.equal(Some(&err)));
        assert!(err.to_string().contains("TiFlash(127.0.0.1:3930)"));
    }
    /// Go `TestGetRegionSplitSizeKeys`.
    #[test]
    fn test_get_region_split_size_keys() {
        let stores: Vec<_> = (1..=3)
            .map(|n| tidb_proto::metapb::Store {
                address: format!("172.16.102.{n}:20160"),
                status_address: "0.0.0.0:20180".into(),
                ..Default::default()
            })
            .collect();
        let mut called = Vec::new();
        assert_eq!(
            get_region_split_size_keys(&stores, |host| {
                called.push(host.to_owned());
                if host == "172.16.102.3:20180" {
                    Ok((1, 2))
                } else {
                    Err(Error::Other("invalid connection".into()))
                }
            })
            .unwrap(),
            (1, 2)
        );
        assert_eq!(called.len(), 3);
    }
    /// Go `TestCheckRequirementsTiFlash`.
    #[test]
    fn test_check_requirements_ti_flash() {
        let tables: Vec<_> = [
            ("test", "t1"),
            ("test", "tbl"),
            ("test1", "t"),
            ("test1", "tbl"),
        ]
        .map(|(db, t)| (db.into(), t.into(), true))
        .into();
        let replicas: Vec<_> = [("db", "tbl"), ("test", "t1"), ("test1", "tbl")]
            .map(|(db, t)| (db.into(), t.into()))
            .into();
        assert_eq!(
            check_ti_flash_version((4, 0, 2), &tables, &replicas)
                .unwrap_err()
                .to_string(),
            "lightning local backend doesn't support TiFlash in this TiDB version. conflict tables: [`test`.`t1`, `test1`.`tbl`]. Please either upgrade TiDB to version >= 4.0.5 or add TiFlash replica after load data."
        );
        check_ti_flash_version((4, 0, 5), &tables, &replicas).unwrap();
    }
}

/// Go `checkMultiIngestSupport`: only online TiKV stores are probed. A TiFlash
/// cluster cannot hide exhausted transport retries by falling back to Ingest.
pub fn check_multi_ingest_support(
    ctx: &Context,
    stores: &[tidb_proto::metapb::Store],
    mut probe: impl FnMut(u64) -> Result<()>,
) -> Result<bool> {
    let is_tiflash = |store: &tidb_proto::metapb::Store| {
        store
            .labels
            .iter()
            .any(|label| label.key == "engine" && label.value == "tiflash")
    };
    let has_tiflash = stores
        .iter()
        .any(|store| store.state == 0 && is_tiflash(store));
    for store in stores
        .iter()
        .filter(|store| store.state == 0 && !is_tiflash(store))
    {
        let mut last_error = None;
        for attempt in 0..20 {
            if attempt > 0 && ctx.sleep(std::time::Duration::from_millis(100)) {
                return Err(Error::Source(Arc::new(ctx.err().unwrap())));
            }
            match probe(store.id) {
                Ok(()) => {
                    last_error = None;
                    break;
                }
                Err(error) => {
                    let mut cause: &(dyn std::error::Error + 'static) = &error;
                    while let Some(source) = cause.source() {
                        cause = source;
                    }
                    if cause
                        .downcast_ref::<tonic::Status>()
                        .is_some_and(|status| status.code() == tonic::Code::Unimplemented)
                    {
                        return Ok(false);
                    }
                    last_error = Some(error);
                }
            }
        }
        if let Some(error) = last_error {
            return if has_tiflash { Err(error) } else { Ok(false) };
        }
    }
    Ok(true)
}

#[cfg(test)]
mod multi_ingest_tests {
    use super::*;
    /// Go `TestMultiIngest`: all store-state, TiFlash, unsupported-RPC and retry cases.
    #[test]
    fn test_multi_ingest() {
        use tidb_proto::metapb::{Store, StoreLabel};
        let stores: Vec<_> = (0..3)
            .flat_map(|state| {
                [false, true].map(move |flash| Store {
                    id: (state * 2 + 1 + i32::from(flash)) as u64,
                    state,
                    labels: if flash {
                        vec![StoreLabel {
                            key: "engine".into(),
                            value: "tiflash".into(),
                        }]
                    } else {
                        vec![]
                    },
                    ..Default::default()
                })
            })
            .chain(std::iter::once(Store {
                id: 8,
                ..Default::default()
            }))
            .collect();
        for case in 0..12 {
            let selected: Vec<_> = stores
                .iter()
                .filter(|s| match case {
                    0..=3 => s.state == 0,
                    5 | 8 => s.labels.is_empty(),
                    9 => s.labels.is_empty() || s.state != 0,
                    _ => true,
                })
                .cloned()
                .collect();
            let mut calls = Vec::new();
            let result = check_multi_ingest_support(&Context::background(), &selected, |id| {
                calls.push(id);
                let s = selected.iter().find(|s| s.id == id).unwrap();
                let supported = match case {
                    1 | 5 => s.labels.is_empty(),
                    2 => !s.labels.is_empty(),
                    3 => s.id != 8,
                    6 => s.state == 0,
                    7 => s.state != 0,
                    _ => true,
                };
                if !supported {
                    return Err(Error::Source(Arc::new(tonic::Status::unimplemented(
                        "multi ingest not supported",
                    ))));
                }
                if (8..=10).contains(&case) || (case == 11 && calls.len() == 1) {
                    return Err(Error::Other("mock error".into()));
                }
                Ok(())
            });
            if case == 10 {
                assert_eq!(result.unwrap_err().to_string(), "mock error");
            } else {
                assert_eq!(
                    result.unwrap(),
                    ![2, 3, 7, 8, 9].contains(&case),
                    "case {case}"
                );
            }
            assert!(calls.iter().all(|id| {
                let s = selected.iter().find(|s| s.id == *id).unwrap();
                s.state == 0 && s.labels.is_empty()
            }));
        }
    }
}

/// Go `maxRetryTimes`.
const MAX_RETRY_TIMES: u64 = 20;

/// Go `forceTableSplitRange`'s `resetter`: stops the refresh, then removes
/// the partition range from every store.
struct ForceTableSplitRange<'a> {
    backend: &'a Backend,
    store_ids: Vec<(u64, String)>,
    remove: sst::RemovePartitionRangeRequest,
    cancel: Option<crossbeam_channel::Sender<()>>,
    refresher: Option<std::thread::JoinHandle<()>>,
}

impl Drop for ForceTableSplitRange<'_> {
    fn drop(&mut self) {
        drop(self.cancel.take());
        if let Some(refresher) = self.refresher.take() {
            let _ = refresher.join();
        }
        call_on_stores(&self.store_ids, "call RemoveForcePartitionRange", |id| {
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl/RemovePartitionRangeRequest",
                &mut (),
            );
            self.backend.importer.remove_force_partition_range(id, self.remove.clone())
        });
    }
}

/// The add/remove loops of Go `forceTableSplitRange`: one call per store,
/// logging which stores succeeded and the first error.
fn call_on_stores(stores: &[(u64, String)], message: &str, call: impl Fn(u64) -> Result<()> + Sync) {
    let results: Vec<(String, Result<()>)> = std::thread::scope(|scope| {
        let handles: Vec<_> = stores
            .iter()
            .map(|(id, addr)| (addr.clone(), scope.spawn(|| call(*id))))
            .collect();
        handles
            .into_iter()
            .map(|(addr, handle)| (addr, handle.join().unwrap_or_else(|_| Err(Error::Other("panic".into())))))
            .collect()
    });
    let mut success_stores = Vec::new();
    let mut failed_stores = Vec::new();
    let mut first_err = None;
    for (addr, result) in results {
        match result {
            Ok(()) => success_stores.push(addr),
            Err(error) => {
                failed_stores.push(addr);
                first_err.get_or_insert(error.to_string());
            }
        }
    }
    let mut fields = vec![
        tidb_log::Field::new("success stores", tidb_log::Value::Str(format!("{success_stores:?}"))),
        tidb_log::Field::new("failed stores", tidb_log::Value::Str(format!("{failed_stores:?}"))),
    ];
    if let Some(error) = first_err {
        fields.push(tidb_log::Field::new("error", tidb_log::Value::Str(error)));
    }
    tidb_log::info(message, &fields);
}
#[cfg(test)]
mod external_tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use tidb_ingestor_engineapi::OnDuplicateKey;
    use tidb_proto::{import_sstpb as sst, metapb};

    /// A write's region and pairs.
    type Write = (u64, Vec<(String, String)>);

    /// Two regions split at `key-00060`; records each write's region and
    /// pairs.
    #[derive(Default)]
    struct TwoRegions {
        writes: StdMutex<Vec<Write>>,
    }

    impl ImportClient for TwoRegions {
        fn get_region(&self, key: &[u8]) -> Result<crate::region_job::RegionInfo> {
            let peer = metapb::Peer { id: 1, store_id: 1, ..Default::default() };
            let (id, start, end) =
                if key < b"key-00060".as_slice() { (1, vec![], b"key-00060".to_vec()) } else { (2, b"key-00060".to_vec(), vec![]) };
            Ok(crate::region_job::RegionInfo {
                leader: Some(peer),
                region: metapb::Region { id, start_key: start, end_key: end, peers: vec![peer], ..Default::default() },
            })
        }
        fn invalidate_region(&self, _: &crate::region_job::RegionInfo) {}
        fn write(&self, _: u64, requests: Vec<sst::WriteRequest>) -> Result<sst::WriteResponse> {
            let mut metas = Vec::new();
            let mut pairs = Vec::new();
            for r in requests {
                match r.chunk {
                    Some(sst::write_request::Chunk::Meta(m)) => metas.push(m),
                    Some(sst::write_request::Chunk::Batch(b)) => {
                        assert_eq!(b.commit_ts, 42);
                        pairs.extend(b.pairs.into_iter().map(|p| {
                            (String::from_utf8(p.key).unwrap(), String::from_utf8(p.value).unwrap())
                        }));
                    }
                    None => {}
                }
            }
            self.writes.lock().unwrap().push((metas[0].region_id, pairs));
            Ok(sst::WriteResponse { metas, ..Default::default() })
        }
        fn multi_ingest(&self, _: u64, _: sst::MultiIngestRequest) -> Result<sst::IngestResponse> {
            Ok(sst::IngestResponse::default())
        }
        fn ingest(&self, _: u64, _: sst::IngestRequest) -> Result<sst::IngestResponse> {
            unreachable!()
        }
        fn duplicate_detect(
            &self,
            _: u64,
            _: sst::DuplicateDetectRequest,
            _: &mut (dyn FnMut(sst::DuplicateDetectResponse) -> Result<()> + Send),
        ) -> Result<()> {
            Ok(())
        }
        fn api_version(&self) -> i32 {
            0
        }
        fn encode_key(&self, key: &[u8]) -> Vec<u8> {
            key.to_vec()
        }
    }

    impl crate::localhelper::SplitClient for TwoRegions {
        fn split_keys_and_scatter(&self, _: &Context, _: &[Vec<u8>]) -> Result<Vec<u64>> {
            Ok(Vec::new())
        }
        fn wait_regions_scattered(&self, _: &Context, _: &[u64]) -> Result<usize> {
            Ok(0)
        }
    }

    /// Go `ImportEngine` of an external engine (`CloseEngine` with an
    /// external config): sorted files from 3 overlapping writers, split into
    /// range jobs, are written region by region with duplicates removed, and
    /// the imported count is verified against the loaded one.
    #[test]
    fn import_engine_writes_an_external_engine_region_by_region() {
        let ctx = Context::background();
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn tidb_objstore::storeapi::Storage> =
            Arc::new(tidb_objstore::LocalStorage::new(dir.path().to_str().unwrap()).unwrap());
        let meta = Arc::new(StdMutex::new(tidb_ingestor_globalsort::SortedKVMeta::default()));
        let mut want = std::collections::BTreeMap::<String, Vec<String>>::new();
        for w in 0..3 {
            let sink = Arc::clone(&meta);
            let mut writer = tidb_ingestor_simplesst::WriterBuilder::new()
                .set_prop_size_distance(64)
                .set_prop_keys_distance(3)
                .set_memory_size_limit(512)
                .set_block_size(256)
                .set_on_close_func(Arc::new(move |s: &tidb_ingestor_simplesst::WriterSummary| {
                    sink.lock().unwrap().merge_summary(s)
                }))
                .build(Arc::clone(&store), "1/2", &format!("w{w}"));
            for i in 0..90 {
                let (k, v) = (format!("key-{:05}", (i * 11 + w * 5) % 120), format!("v-{w}-{i}"));
                writer.write_row(&ctx, k.as_bytes(), v.as_bytes()).unwrap();
                want.entry(k).or_default().push(v);
            }
            writer.close(&ctx).unwrap();
        }
        let meta = meta.lock().unwrap().clone();
        let mut splitter = tidb_ingestor_globalsort::RangeSplitter::new(
            &ctx,
            meta.stats().to_vec(),
            Arc::clone(&store),
            i64::MAX,
            i64::MAX,
            100,
            9,
            i64::MAX,
            i64::MAX,
        )
        .unwrap();
        let group = splitter.split_one_ranges_group().unwrap();
        splitter.close().unwrap();
        assert!(group.end_key_of_group.is_empty());
        let mut job_keys = vec![meta.start_key.clone()];
        job_keys.extend(group.interior_range_job_keys);
        job_keys.push(meta.end_key.clone());
        let client = Arc::new(TwoRegions::default());
        let backend = Backend::new(
            BackendConfig {
                local_store_dir: dir.path().join("sort"),
                resource_group_name: String::new(),
                task_type: String::new(),
                worker_concurrency: 3,
                mem_table_size: 1024,
                local_writer_mem_cache_size: 1024,
                dupe_detect_enabled: false,
                kv_write_batch_size: 16 * 1024,
                should_check_write_stall: false,
                checkpoint_enabled: false,
                pause_pd_scheduler_scope: String::new(),
                raft_kv2_switch_mode_duration: std::time::Duration::ZERO,
            },
            client.clone(),
        )
        .unwrap();
        let id = *uuid::Uuid::new_v4().as_bytes();
        backend
            .close_external_engine(
                id,
                &tidb_lightning_backend::ExternalEngineConfig {
                    ext_store: Arc::clone(&store),
                    data_files: group.data_files,
                    stat_files: group.stat_files,
                    start_key: meta.start_key.clone(),
                    end_key: meta.end_key.clone(),
                    job_keys: job_keys.clone(),
                    split_keys: job_keys,
                    total_file_size: 100,
                    total_kv_count: 0,
                    check_hotspot: false,
                    mem_capacity: 1 << 30,
                    on_dup: OnDuplicateKey::Remove,
                    file_prefix: "1/3".into(),
                },
                42,
            )
            .unwrap();
        backend.import_engine(&ctx, id, 96 << 20, 7).unwrap();
        let unique: Vec<(String, String)> =
            want.into_iter().filter(|(_, v)| v.len() == 1).map(|(k, mut v)| (k, v.remove(0))).collect();
        let writes = client.writes.lock().unwrap();
        let mut written = Vec::new();
        for (region, pairs) in writes.iter() {
            assert!(pairs.len() <= 7, "split keys limit");
            for (k, _) in pairs {
                assert_eq!(*region, if k.as_str() < "key-00060" { 1 } else { 2 }, "{k} in region {region}");
            }
            written.extend(pairs.iter().cloned());
        }
        written.sort();
        assert_eq!(written, unique);
        assert_eq!(backend.get_external_engine_kv_statistics(id).1, unique.len() as i64);
        backend.cleanup_engine(id).unwrap();
        assert_eq!(backend.get_external_engine_kv_statistics(id), (0, 0));
    }
}
