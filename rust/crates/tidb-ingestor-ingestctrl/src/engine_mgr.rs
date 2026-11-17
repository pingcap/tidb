// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/engine_mgr.go`. Engine storage uses extsort
//! generations in place of Pebble; management preserves Go's lifecycle.
use crate::{Error, Result, engine::Engine, local::BackendConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Go `engineManager.externalEngine`'s values; an engine is locked while it
/// is imported.
pub type ExternalEngine = Arc<Mutex<Box<dyn tidb_ingestor_engineapi::Engine>>>;

/// Go `engineManager`.
pub struct EngineManager {
    cfg: BackendConfig,
    /// Go `StoreHelper.GetTiKVCodec`, which `openLocalWriter` hands every
    /// writer.
    tikv_codec: Option<tidb_ingestor_simplesst::KeyCodec>,
    engines: Mutex<HashMap<[u8; 16], Arc<Engine>>>,
    external_engines: Mutex<HashMap<[u8; 16], ExternalEngine>>,
}
impl EngineManager {
    /// Go `newEngineManager`.
    pub fn new(cfg: BackendConfig, tikv_codec: Option<tidb_ingestor_simplesst::KeyCodec>) -> Result<Self> {
        prepare_sort_dir(&cfg)?;
        Ok(Self {
            cfg,
            tikv_codec,
            engines: Mutex::new(HashMap::new()),
            external_engines: Mutex::new(HashMap::new()),
        })
    }
    /// Go `engineManager.openEngine`.
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
        )?
        .with_tikv_codec(self.tikv_codec.clone()));
        engines.insert(id, engine.clone());
        Ok(engine)
    }
    /// Go `engineManager.rLockEngine`, represented by an Arc plus Engine's mutex.
    pub fn engine(&self, id: [u8; 16]) -> Result<Arc<Engine>> {
        self.engines
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| {
                Error::Other(format!("engine '{}' not found", uuid::Uuid::from_bytes(id)))
            })
    }
    /// Go `engineManager.flushEngine`.
    pub fn flush_engine(&self, id: [u8; 16]) -> Result<()> {
        self.engine(id)?.flush()
    }
    /// Go `engineManager.flushAllEngines`.
    pub fn flush_all_engines(&self) -> Result<()> {
        for engine in self.engines.lock().unwrap().values() {
            engine.flush()?;
        }
        Ok(())
    }
    /// Go `engineManager.closeEngine`: readable recovery and stopped writers.
    pub fn close_engine(&self, id: [u8; 16]) -> Result<()> {
        if let Ok(engine) = self.engine(id) {
            return engine.finish_write();
        }
        let path = self
            .cfg
            .local_store_dir
            .join(uuid::Uuid::from_bytes(id).to_string());
        if !path.is_dir() {
            return Err(Error::Other(format!(
                "engine '{}' does not exist",
                uuid::Uuid::from_bytes(id)
            )));
        }
        self.open_engine(id, 0)?.finish_write()
    }
    /// Go `engineManager.closeEngine`'s external branch: a global-sort engine
    /// over the sorted files, committed at `ts`.
    pub fn close_external_engine(
        &self,
        id: [u8; 16],
        cfg: &tidb_lightning_backend::ExternalEngineConfig,
        ts: u64,
    ) {
        let engine = tidb_ingestor_globalsort::Engine::new(
            tidb_ingestor_globalsort::EngineConfig {
                ext_store: Arc::clone(&cfg.ext_store),
                data_files: cfg.data_files.clone(),
                stat_files: cfg.stat_files.clone(),
                start_key: cfg.start_key.clone(),
                end_key: cfg.end_key.clone(),
                job_keys: cfg.job_keys.clone(),
                split_keys: cfg.split_keys.clone(),
                total_file_size: cfg.total_file_size,
                total_kv_count: cfg.total_kv_count,
                check_hotspot: cfg.check_hotspot,
                mem_capacity: cfg.mem_capacity,
                on_dup: cfg.on_dup,
                file_prefix: cfg.file_prefix.clone(),
            },
            self.cfg.worker_concurrency,
            ts,
        );
        self.external_engines
            .lock()
            .unwrap()
            .insert(id, Arc::new(Mutex::new(Box::new(engine))));
    }
    /// Go `engineManager.getExternalEngine`.
    pub fn get_external_engine(&self, id: [u8; 16]) -> Option<ExternalEngine> {
        self.external_engines.lock().unwrap().get(&id).cloned()
    }
    /// Go `engineManager.getExternalEngineKVStatistics`: the imported size
    /// and count.
    pub fn get_external_engine_kv_statistics(&self, id: [u8; 16]) -> (i64, i64) {
        self.get_external_engine(id)
            .map_or((0, 0), |engine| engine.lock().unwrap().imported_statistics())
    }
    /// Go `engineManager.getExternalEngineConflictInfo`.
    pub fn get_external_engine_conflict_info(&self, id: [u8; 16]) -> tidb_ingestor_engineapi::ConflictInfo {
        self.get_external_engine(id)
            .map(|engine| engine.lock().unwrap().conflict_info())
            .unwrap_or_default()
    }
    /// Go `engineManager.getImportedKVCount`.
    pub fn get_imported_kv_count(&self, id: [u8; 16]) -> u64 {
        self.engine(id)
            .map_or(0, |engine| engine.imported_statistics().1)
    }
    /// Go `engineManager.resetEngine`; absent engines are a no-op.
    pub fn reset_engine(&self, id: [u8; 16], ts: u64) -> Result<()> {
        match self.engine(id) {
            Ok(engine) => engine.reset(ts),
            Err(_) => Ok(()),
        }
    }
    /// Go `Backend.ResetEngineSkipAllocTS`, preserving in-flight writer generations.
    pub fn reset_imported(&self, id: [u8; 16], generation: Option<u64>) -> Result<()> {
        match self.engine(id) {
            Ok(engine) => engine.reset_imported(generation),
            Err(_) => Ok(()),
        }
    }
    /// Go `engineManager.cleanupEngine`: remove the map entry even on close failure.
    pub fn cleanup_engine(&self, id: [u8; 16]) -> Result<()> {
        let engine = self.engines.lock().unwrap().remove(&id);
        let Some(engine) = engine else {
            if let Some(external) = self.external_engines.lock().unwrap().remove(&id) {
                return external.lock().unwrap().close().map_err(crate::region_job::engine_error);
            }
            tidb_log::warn(
                "could not find engine in cleanupEngine",
                &[tidb_log::Field::new("uuid", tidb_log::Value::Str(uuid::Uuid::from_bytes(id).to_string()))],
            );
            return Ok(());
        };
        engine.cleanup()
    }
    /// Go `engineManager.cleanupAllLocalEngines`, returning diagnostics to the
    /// backend logger while continuing cleanup after a finish-write failure.
    pub fn cleanup_all_local_engines(&self) -> Vec<String> {
        let engines: Vec<_> = self
            .engines
            .lock()
            .unwrap()
            .iter()
            .map(|(id, e)| (*id, e.clone()))
            .collect();
        let mut errors = Vec::new();
        for (id, engine) in engines {
            if let Err(error) = engine.finish_write() {
                errors.push(format!(
                    "finish write failed, engine id={}: {error}",
                    uuid::Uuid::from_bytes(id)
                ));
            }
            if let Err(error) = self.cleanup_engine(id) {
                errors.push(format!(
                    "cleanup failed, engine id={}: {error}",
                    uuid::Uuid::from_bytes(id)
                ));
            }
        }
        if !errors.is_empty() {
            tidb_log::warn(
                "cleanup all local engines failed",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(errors.join("; ")),
                )],
            );
        }
        errors
    }
    /// Go `engineManager.engineFileSizes`.
    pub fn engine_file_sizes(&self) -> Vec<crate::disk_quota::EngineFileSize> {
        self.engines
            .lock()
            .unwrap()
            .iter()
            .map(|(id, e)| e.file_size(*id))
            .collect()
    }
    /// Go `engineManager.totalMemoryConsume`.
    pub fn total_memory_consume(&self) -> u64 {
        self.engines
            .lock()
            .unwrap()
            .values()
            .map(|e| e.total_memory_size())
            .sum()
    }
    /// Go `engineManager.close`: retain checkpoint data, otherwise remove engines.
    pub fn close(&self) -> Result<()> {
        for (id, external) in std::mem::take(&mut *self.external_engines.lock().unwrap()) {
            if let Err(error) = external.lock().unwrap().close() {
                tidb_log::warn(
                    "close external engine failed",
                    &[
                        tidb_log::Field::new("id", tidb_log::Value::Str(uuid::Uuid::from_bytes(id).to_string())),
                        tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    ],
                );
            }
        }
        let engines = std::mem::take(&mut *self.engines.lock().unwrap());
        let mut first_error = None;
        for engine in engines.values() {
            if let Err(error) = engine.close() {
                first_error.get_or_insert(error);
            }
        }
        let empty = std::fs::read_dir(&self.cfg.local_store_dir)
            .is_ok_and(|mut entries| entries.next().is_none());
        if !self.cfg.checkpoint_enabled || empty {
            if let Err(error) = std::fs::remove_dir_all(&self.cfg.local_store_dir) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    first_error.get_or_insert(error.into());
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}
/// Go `prepareSortDir`: checkpoint recovery may reuse an existing directory.
pub fn prepare_sort_dir(cfg: &BackendConfig) -> Result<()> {
    if cfg.checkpoint_enabled && cfg.local_store_dir.is_dir() {
        return Ok(());
    }
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&cfg.local_store_dir)?;
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `TestEngineManager BackendConfig fixture`.
    fn config(path: std::path::PathBuf) -> BackendConfig {
        BackendConfig {
            local_store_dir: path,
            resource_group_name: String::new(),
            task_type: String::new(),
            worker_concurrency: 8,
            mem_table_size: 1024,
            local_writer_mem_cache_size: 1024,
            dupe_detect_enabled: false,
            kv_write_batch_size: 1024,
            should_check_write_stall: false,
            checkpoint_enabled: false,
            pause_pd_scheduler_scope: crate::local::PAUSE_PD_SCHEDULER_SCOPE_GLOBAL.into(),
            raft_kv2_switch_mode_duration: std::time::Duration::ZERO,
        }
    }
    /// Go `TestEngineManager`.
    #[test]
    fn test_engine_manager() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path().join("sorted-kv"));
        let manager = EngineManager::new(cfg.clone(), None).unwrap();
        let id = *uuid::Uuid::new_v4().as_bytes();
        let engine = manager.open_engine(id, 123).unwrap();
        assert_eq!(manager.engine_file_sizes().len(), 1);
        engine
            .append_rows(0, &[(b"key".to_vec(), b"value".to_vec(), vec![])])
            .unwrap();
        manager.close_engine(id).unwrap();
        assert!(
            engine
                .new_iter(&[], &[])
                .unwrap()
                .next_pair()
                .unwrap()
                .is_some()
        );
        assert!(engine.append_rows(0, &[]).is_err());
        assert_eq!(manager.get_imported_kv_count(id), 0);
        assert!(
            manager
                .close_engine([0; 16])
                .unwrap_err()
                .to_string()
                .contains("does not exist")
        );
        manager.reset_engine([0; 16], 0).unwrap();
        manager.reset_engine(id, 456).unwrap();
        assert_eq!(engine.get_ts(), 456);
        assert_eq!(manager.engine_file_sizes().len(), 1);
        manager.cleanup_engine([0; 16]).unwrap();
        manager.cleanup_engine(id).unwrap();
        assert!(manager.engine_file_sizes().is_empty());
        assert_eq!(std::fs::read_dir(&cfg.local_store_dir).unwrap().count(), 0);
        manager.close().unwrap();
        assert!(!cfg.local_store_dir.exists());
    }
    /// Go `TestCleanupAllLocalEnginesLogsErrorOnly`, pinning continuation and diagnostics.
    #[test]
    fn test_cleanup_all_local_engines_logs_error_only() {
        let dir = tempfile::tempdir().unwrap();
        let manager = EngineManager::new(config(dir.path().join("sorted-kv")), None).unwrap();
        let engine = manager.open_engine([1; 16], 1).unwrap();
        engine.set_error("injected finishWrite error".into());
        let errors = manager.cleanup_all_local_engines();
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("injected finishWrite error"));
        assert!(manager.engine_file_sizes().is_empty());
        manager.close().unwrap();
    }
}
