// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/local.go` and `engine_mgr.go`.
use crate::region_job::ImportClient;
use crate::{Error, Result, engine::Engine};
use std::collections::HashMap;
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
    /// Go `PausePDSchedulerScope`: `"table"` pauses PD's schedulers on the
    /// engine's key range while it is ingested.
    pub pause_pd_scheduler_scope: String,
    /// Go `RaftKV2SwitchModeDuration`: when non-zero, the engine's key range
    /// is switched to import mode this often while it is ingested.
    pub raft_kv2_switch_mode_duration: std::time::Duration,
}

/// Go `config.PausePDSchedulerScopeTable`.
pub const PAUSE_PD_SCHEDULER_SCOPE_TABLE: &str = "table";
/// Go `Backend`, owning local engines and their import client.
pub struct Backend {
    /// Go backend configuration.
    pub cfg: BackendConfig,
    engines: Mutex<HashMap<[u8; 16], Arc<Engine>>>,
    importer: Arc<dyn ImportClient>,
    collector: Mutex<Option<crate::region_job::Collector>>,
}
impl Backend {
    /// Go `NewBackend`.
    pub fn new(cfg: BackendConfig, importer: Arc<dyn ImportClient>) -> Result<Self> {
        std::fs::create_dir_all(&cfg.local_store_dir)?;
        Ok(Self {
            cfg,
            engines: Mutex::new(HashMap::new()),
            importer,
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
        let mut engines = self.engines.lock().unwrap();
        if let Some(engine) = engines.get(&id) {
            return Ok(engine.clone());
        }
        let engine = Arc::new(Engine::open(
            self.cfg
                .local_store_dir
                .join(uuid::Uuid::from_bytes(id).to_string()),
            ts,
            self.cfg.dupe_detect_enabled,
            self.cfg.local_writer_mem_cache_size,
        )?);
        engines.insert(id, engine.clone());
        Ok(engine)
    }
    /// Go `Backend.ImportEngine`: import each sorted range using TiKV's SST
    /// write and ingest RPCs, never transactional key writes.
    pub fn import_engine(
        &self,
        ctx: &Context,
        id: [u8; 16],
        region_split_size: u64,
        region_split_keys: usize,
    ) -> Result<()> {
        let engine = self.engine(id)?;
        engine.set_importing(true);
        let result = self.ingest_engine(ctx, &engine, region_split_size, region_split_keys);
        engine.set_importing(false);
        result
    }

    /// Go `ingestEngine`: pauses PD's schedulers on the engine's key range
    /// and, under Raft-KV2, keeps the range in import mode, while the engine
    /// is written and ingested. Region split and scatter are not ported.
    fn ingest_engine(
        &self,
        ctx: &Context,
        engine: &Engine,
        region_split_size: u64,
        region_split_keys: usize,
    ) -> Result<()> {
        // Go `GetKeyRange`: the first key and the key after the last one.
        let Some((start_key, last_key)) = engine.get_first_and_last_key(&[], &[])? else {
            // Go: the engine contains no kv, skip import.
            return Ok(());
        };
        let mut end_key = last_key;
        end_key.push(0);
        self.with_ingest_guards(ctx, &start_key, &end_key, || {
            crate::region_job::do_import(
                ctx,
                engine,
                self.importer.as_ref(),
                &self.cfg,
                region_split_size,
                region_split_keys,
            )
        })
    }

    /// Go `IngestExternalEngine`: `ingestEngine` over a global-sort engine,
    /// whose loaded batches are ingested range by range as they are loaded.
    /// Then Go `verifyImportedStatistics`: the ingested pairs must be the
    /// loaded ones.
    pub fn ingest_external_engine(
        &self,
        ctx: &Context,
        e: &mut tidb_ingestor_globalsort::Engine,
        region_split_size: u64,
        region_split_keys: usize,
    ) -> Result<()> {
        use tidb_ingestor_engineapi::Engine as _;
        let (total_size, total_len) = e.kv_statistics();
        if total_size == 0 {
            tidb_log::info("engine contains no kv, skip import", &[str_field("engine", &e.id())]);
            return Ok(());
        }
        // Go `getRegionSplitKeys`.
        let (start_key, end_key) = e.get_key_range().map_err(crate::region_job::engine_error)?;
        if start_key.is_empty() {
            return Err(Error::Other("could not find first pair".into()));
        }
        let split_keys = if total_size <= region_split_size as i64 && total_len <= region_split_keys as i64 {
            vec![start_key, end_key]
        } else {
            e.get_region_split_keys().map_err(crate::region_job::engine_error)?
        };
        let (first, last) = (split_keys[0].clone(), split_keys[split_keys.len() - 1].clone());
        tidb_log::info(
            "start import engine",
            &[
                str_field("engine", &e.id()),
                tidb_log::Field::new("region ranges", tidb_log::Value::I64(split_keys.len() as i64 - 1)),
                tidb_log::Field::new("count", tidb_log::Value::I64(total_len)),
                tidb_log::Field::new("size", tidb_log::Value::I64(total_size)),
            ],
        );
        self.with_ingest_guards(ctx, &first, &last, || {
            let limiter = crate::region_job::new_ingest_limiter(ctx);
            e.load_ingest_data(ctx, &mut |batch| {
                let ranges = &batch.sorted_ranges;
                let (start_of_all, end_of_all) = (&ranges[0].start, &ranges[ranges.len() - 1].end);
                // Go `generateJobForRange`: nothing to ingest in the batch.
                if batch.data.get_first_and_last_key(start_of_all, end_of_all)?.is_none() {
                    tidb_log::warn(
                        "There is no pairs in range",
                        &[
                            str_field("startOfAllRanges", &hex::encode(start_of_all)),
                            str_field("endOfAllRanges", &hex::encode(end_of_all)),
                        ],
                    );
                    return Ok(());
                }
                let data = crate::region_job::IngestDataJob(batch.data.as_ref(), self.collector.lock().unwrap().clone());
                for r in ranges {
                    crate::region_job::import_range(
                        ctx,
                        &data,
                        self.importer.as_ref(),
                        &self.cfg,
                        &limiter,
                        &r.start,
                        &r.end,
                        region_split_size,
                        region_split_keys,
                    )
                    .map_err(|e| Box::new(e) as tidb_ingestor_engineapi::Error)?;
                }
                Ok(())
            })
            .map_err(|err| match err.downcast::<Error>() {
                Ok(e) => *e,
                Err(e) => crate::region_job::engine_error(e),
            })
        })?;
        let (imported_size, imported_len) = e.imported_statistics();
        // Go `verifyImportedStatistics`.
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ingestor/ingestctrl/skipOnDuplicateKeyCheck").is_none() {
            let expected = e.get_total_loaded_kvs_count();
            if imported_len != expected {
                return Err(Error::Other(format!("imported length mismatch, expected {expected}, got {imported_len}")));
            }
        }
        tidb_log::info(
            "import engine success",
            &[
                str_field("engine", &e.id()),
                tidb_log::Field::new("size", tidb_log::Value::I64(total_size)),
                tidb_log::Field::new("kvs", tidb_log::Value::I64(total_len)),
                tidb_log::Field::new("importedSize", tidb_log::Value::I64(imported_size)),
                tidb_log::Field::new("importedCount", tidb_log::Value::I64(imported_len)),
            ],
        );
        Ok(())
    }

    /// The PD scheduler pause and the Raft-KV2 import mode around `f`, on
    /// `[start_key, end_key)` (Go `ingestEngine`'s defers).
    fn with_ingest_guards(
        &self,
        ctx: &Context,
        start_key: &[u8],
        end_key: &[u8],
        f: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        let encode = |key: &[u8]| {
            if key.is_empty() {
                Vec::new()
            } else {
                let mut out = Vec::new();
                tidb_codec::encode_bytes(&mut out, &self.importer.encode_key(key));
                out
            }
        };
        let (sub_ctx, cancel) = Context::with_cancel(ctx);
        let mut background = Vec::new();
        if self.cfg.pause_pd_scheduler_scope == PAUSE_PD_SCHEDULER_SCOPE_TABLE {
            let paused = crate::pdutil::pause_schedulers_by_key_range(
                &sub_ctx,
                Arc::clone(&self.importer),
                encode(start_key),
                encode(end_key),
            );
            match paused {
                Ok(done) => background.extend(done),
                Err(e) => {
                    cancel.cancel();
                    return Err(e);
                }
            }
        }
        if !self.cfg.raft_kv2_switch_mode_duration.is_zero() {
            let range = tidb_proto::import_sstpb::Range { start: encode(start_key), end: encode(end_key) };
            background.push(crate::tikv_mode::switch_mode_periodically(
                &sub_ctx,
                Arc::clone(&self.importer),
                range,
                self.cfg.raft_kv2_switch_mode_duration,
            ));
        }
        let result = f();
        cancel.cancel();
        for done in background {
            let _ = done.join();
        }
        result
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
    /// Go `Backend.SetTSBeforeImportEngine`.
    pub fn set_ts_before_import_engine(&self, id: [u8; 16], ts: u64) -> Result<()> {
        self.engine(id)?.set_ts(ts)
    }
    /// Go `Backend.ResetEngineSkipAllocTS`.
    pub fn reset_engine_skip_alloc_ts(&self, id: [u8; 16], generation: Option<u64>) -> Result<()> {
        match self.engines.lock().unwrap().get(&id) {
            Some(engine) => engine.reset_imported(generation),
            None => Ok(()),
        }
    }
    /// Go `Backend.CleanupEngine`.
    pub fn cleanup_engine(&self, id: [u8; 16]) -> Result<()> {
        let engine = self.engines.lock().unwrap().remove(&id);
        if let Some(engine) = engine {
            engine.cleanup()?;
        }
        Ok(())
    }
    /// Go `Backend.Close`, preserving local files when checkpoints are enabled.
    pub fn close(&self) -> Result<()> {
        for engine in self.engines.lock().unwrap().values() {
            engine.close()?;
        }
        Ok(())
    }
    /// The import client the backend writes and allocates TSOs through.
    pub(crate) fn importer(&self) -> &dyn ImportClient {
        self.importer.as_ref()
    }

    /// Go `Backend.GetImportedKVCount`.
    pub fn get_imported_kv_count(&self, id: [u8; 16]) -> i64 {
        self.engine(id).map_or(0, |e| e.imported_kvs())
    }

    /// Every open engine.
    pub(crate) fn all_engines(&self) -> Vec<Arc<Engine>> {
        self.engines.lock().unwrap().values().cloned().collect()
    }

    pub(crate) fn engine(&self, id: [u8; 16]) -> Result<Arc<Engine>> {
        self.engines
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::Other(format!("engine {} not found", uuid::Uuid::from_bytes(id))))
    }
}
impl crate::disk_quota::DiskUsage for Backend {
    fn engine_file_sizes(&self) -> Vec<crate::disk_quota::EngineFileSize> {
        self.engines
            .lock()
            .unwrap()
            .iter()
            .map(|(id, e)| e.file_size(*id))
            .collect()
    }
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

    /// Go `IngestExternalEngine`: sorted files from 3 overlapping writers,
    /// split into range jobs, are written region by region with duplicates
    /// removed, and the imported count is verified against the loaded one.
    #[test]
    fn ingest_external_engine_writes_each_region_once() {
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
        let mut engine = tidb_ingestor_globalsort::Engine::new(
            tidb_ingestor_globalsort::EngineConfig {
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
            3,
            42,
        );
        let client = Arc::new(TwoRegions::default());
        let backend = Backend::new(
            BackendConfig {
                local_store_dir: dir.path().join("sort"),
                resource_group_name: String::new(),
                task_type: String::new(),
                worker_concurrency: 1,
                mem_table_size: 1024,
                local_writer_mem_cache_size: 1024,
                dupe_detect_enabled: false,
                kv_write_batch_size: 16 * 1024,
                should_check_write_stall: false,
                pause_pd_scheduler_scope: String::new(),
                raft_kv2_switch_mode_duration: std::time::Duration::ZERO,
            },
            client.clone(),
        )
        .unwrap();
        backend.ingest_external_engine(&ctx, &mut engine, 96 << 20, 7).unwrap();
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
        assert_eq!(written, unique);
        use tidb_ingestor_engineapi::Engine as _;
        assert_eq!(engine.imported_statistics().1, unique.len() as i64);
        assert_eq!(engine.get_total_loaded_kvs_count(), unique.len() as i64);
    }
}
