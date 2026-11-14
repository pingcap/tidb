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
}
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
        let result = crate::region_job::do_import(
            ctx,
            &engine,
            self.importer.as_ref(),
            &self.cfg,
            region_split_size,
            region_split_keys,
        );
        engine.set_importing(false);
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
    fn engine(&self, id: [u8; 16]) -> Result<Arc<Engine>> {
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
