// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/local.go` and `engine_mgr.go`.
use crate::region_job::ImportClient;
use crate::{Error, Result, engine::Engine};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tidb_gocontext::Context;

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
}
impl Backend {
    /// Go `NewBackend`.
    pub fn new(cfg: BackendConfig, importer: Arc<dyn ImportClient>) -> Result<Self> {
        std::fs::create_dir_all(&cfg.local_store_dir)?;
        Ok(Self {
            cfg,
            engines: Mutex::new(HashMap::new()),
            importer,
        })
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
                encode(&start_key),
                encode(&end_key),
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
            let range = tidb_proto::import_sstpb::Range { start: encode(&start_key), end: encode(&end_key) };
            background.push(crate::tikv_mode::switch_mode_periodically(
                &sub_ctx,
                Arc::clone(&self.importer),
                range,
                self.cfg.raft_kv2_switch_mode_duration,
            ));
        }
        let result = crate::region_job::do_import(
            ctx,
            engine,
            self.importer.as_ref(),
            &self.cfg,
            region_split_size,
            region_split_keys,
        );
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
