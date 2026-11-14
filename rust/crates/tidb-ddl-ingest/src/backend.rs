// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/backend.go`, `engine_mgr.go` and `backend_mgr.go`.
use crate::{
    Error, Result,
    checkpoint::CheckpointManager,
    disk_root::{DiskRootImpl, ResourceTracker},
    engine::EngineInfo,
    mem_root::MemRootImpl,
};
use std::collections::HashMap;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};
use tidb_gocontext::Context;
use tidb_ingestor_ingestctrl::{
    disk_quota::{DiskUsage, check_disk_quota},
    local::Backend,
};

/// Go `UnregisterOpt`.
#[derive(Clone, Copy)]
pub struct UnregisterOpt(pub u8);
impl UnregisterOpt {
    /// Go `OptCloseEngines`.
    pub const CLOSE_ENGINES: Self = Self(1);
    /// Go `OptCleanData`.
    pub const CLEAN_DATA: Self = Self(2);
    /// Go `OptCheckDup`.
    pub const CHECK_DUP: Self = Self(4);
}
/// Go `owner.AcquireDistributedLock`'s returned release function.
pub type DistributedLock = Arc<dyn Fn(i64) -> Result<Box<dyn FnOnce() + Send>> + Send + Sync>;
/// Go `litBackendCtx`.
pub struct LitBackendCtx {
    job_id: i64,
    backend: Arc<Backend>,
    engines: Mutex<HashMap<i64, Arc<EngineInfo>>>,
    mem_root: Arc<MemRootImpl>,
    disk_root: Arc<DiskRootImpl>,
    checkpoint: Option<Arc<CheckpointManager>>,
    import_ts: u64,
    init_ts: u64,
    table: Mutex<Option<tidb_model::TableInfo>>,
    distributed_lock: Option<DistributedLock>,
    flushing: AtomicBool,
    time_of_last_flush: Mutex<Instant>,
    unregister: Mutex<()>,
    import: Mutex<()>,
}
impl LitBackendCtx {
    /// Go `newBackendContext` and `BackendCtxBuilder.Build` registration.
    pub fn new(
        job_id: i64,
        backend: Arc<Backend>,
        mem_root: Arc<MemRootImpl>,
        disk_root: Arc<DiskRootImpl>,
        checkpoint: Option<Arc<CheckpointManager>>,
        import_ts: u64,
        init_ts: u64,
        distributed_lock: Option<DistributedLock>,
    ) -> Arc<Self> {
        let bc = Arc::new(Self {
            job_id,
            backend,
            engines: Mutex::new(HashMap::new()),
            mem_root,
            disk_root: disk_root.clone(),
            checkpoint,
            import_ts,
            init_ts,
            table: Mutex::new(None),
            distributed_lock,
            flushing: AtomicBool::new(false),
            time_of_last_flush: Mutex::new(Instant::now()),
            unregister: Mutex::new(()),
            import: Mutex::new(()),
        });
        // Weak tracker avoids a cycle between the global disk root and backend.
        disk_root.add(job_id, Arc::new(BackendTracker(Arc::downgrade(&bc))));
        bc
    }
    /// Go `litBackendCtx.Register`.
    pub fn register(
        &self,
        index_ids: &[i64],
        uniques: &[bool],
        table: &tidb_model::TableInfo,
    ) -> Result<Vec<Arc<EngineInfo>>> {
        let table_name = table.name.lowercase();
        let mut engines = self.engines.lock().unwrap();
        let existing = index_ids
            .iter()
            .filter_map(|id| engines.get(id).cloned())
            .collect::<Vec<_>>();
        if !existing.is_empty() {
            if existing.len() != index_ids.len() {
                return Err(Error::Other(format!(
                    "engines index ID number mismatch: job ID {}, required number of index IDs: {}, actual number of engines: {}",
                    self.job_id,
                    index_ids.len(),
                    existing.len()
                )));
            }
            return Ok(existing);
        }
        self.mem_root.refresh_consumption();
        if !self
            .mem_root
            .check_consume((index_ids.len() * std::mem::size_of::<EngineInfo>()) as i64)
        {
            return Err(Error::Database(
                tidb_dbterror::ERR_INGEST_FAILED.fast_gen_by_args(&["memory used up".into()]),
            ));
        }
        let mut opened: Vec<Arc<EngineInfo>> = Vec::new();
        for (i, id) in index_ids.iter().enumerate() {
            let namespace = uuid::Uuid::parse_str("d68d6abe-c59e-45d6-ade8-e2b0ceb7bedf").unwrap();
            let uuid = *uuid::Uuid::new_v5(
                &namespace,
                format!("{table_name}:{}", *id as i32).as_bytes(),
            )
            .as_bytes();
            let engine = match self.backend.open_engine(uuid, self.get_import_ts()) {
                Ok(e) => e,
                Err(e) => {
                    for e in opened {
                        let _ = e.close(true);
                    }
                    return Err(e.into());
                }
            };
            opened.push(Arc::new(EngineInfo::new(
                *id,
                uniques[i],
                uuid,
                engine,
                self.backend.clone(),
                self.mem_root.clone(),
            )));
        }
        for e in &opened {
            engines.insert(e.index_id, e.clone());
        }
        *self.table.lock().unwrap() = Some(table.clone_like_go());
        Ok(opened)
    }
    /// Go `litBackendCtx.IngestIfQuotaExceeded`.
    pub fn ingest_if_quota_exceeded(
        &self,
        ctx: &Context,
        task_id: usize,
        count: i64,
    ) -> Result<()> {
        self.finish_chunk(task_id, count);
        self.disk_root.update_usage();
        let should_import = self.disk_root.should_import();
        let interval =
            tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/ingest/mockSyncIntervalMs")
                .map(|v| Duration::from_millis(v.int() as u64))
                .unwrap_or(Duration::from_secs(600));
        let force =
            tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/ingest/forceSyncFlagForTest")
                .is_some();
        if !force && !should_import && self.time_of_last_flush.lock().unwrap().elapsed() < interval
        {
            return Ok(());
        }
        if self
            .flushing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(());
        }
        struct Reset<'a>(&'a AtomicBool);
        impl Drop for Reset<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _reset = Reset(&self.flushing);
        let _import = self.import.lock().unwrap();
        self.flush_engines()?;
        *self.time_of_last_flush.lock().unwrap() = Instant::now();
        if !force && !should_import {
            return self.advance_watermark(false);
        }
        self.import_and_checkpoint(ctx)
    }
    /// Go `litBackendCtx.Ingest`.
    pub fn ingest(&self, ctx: &Context) -> Result<()> {
        let _import = self.import.lock().unwrap();
        self.flush_engines()?;
        self.import_and_checkpoint(ctx)
    }
    fn flush_engines(&self) -> Result<()> {
        for e in self.engines.lock().unwrap().values() {
            e.flush()?;
        }
        Ok(())
    }
    fn import_and_checkpoint(&self, ctx: &Context) -> Result<()> {
        struct Release(Option<Box<dyn FnOnce() + Send>>);
        impl Drop for Release {
            fn drop(&mut self) {
                if let Some(release) = self.0.take() {
                    release();
                }
            }
        }
        let _release = Release(
            self.distributed_lock
                .as_ref()
                .map(|acquire| acquire(self.job_id))
                .transpose()?,
        );
        for e in self.engines.lock().unwrap().values() {
            let generation = e.flushed_generation();
            self.backend
                .set_ts_before_import_engine(e.uuid, self.get_import_ts())?;
            if let Err(error) =
                self.backend
                    .import_engine(ctx, e.uuid, 96 * 1024 * 1024 * 10, 960000)
            {
                let table = self.table.lock().unwrap();
                let error = Error::from(error);
                return Err(
                    match table.as_ref().and_then(|table| {
                        table
                            .indices
                            .iter_deref()
                            .find(|i| i.read().id == e.index_id)
                            .map(|index| (table, index))
                    }) {
                        Some((table, index)) => {
                            crate::util::try_convert_to_key_exists_err(error, &index.read(), table)
                        }
                        None => error,
                    },
                );
            }
            let reset = if tidb_failpoint::eval(
                "github.com/pingcap/tidb/pkg/ddl/ingest/mockResetEngineFailed",
            )
            .is_some()
            {
                Err(tidb_ingestor_ingestctrl::Error::Other(
                    "mock reset engine failed".into(),
                ))
            } else {
                self.backend.reset_engine_skip_alloc_ts(e.uuid, generation)
            };
            if let Err(err) = reset {
                let _ = e.close(true);
                return Err(err.into());
            }
        }
        self.advance_watermark(true)
    }
    /// Go `litBackendCtx.FinishAndUnregisterEngines`.
    pub fn finish_and_unregister_engines(&self, ctx: &Context, opt: UnregisterOpt) -> Result<()> {
        let _guard = self.unregister.lock().unwrap();
        let mut engines = self.engines.lock().unwrap();
        for e in engines.values() {
            e.close(opt.0 & UnregisterOpt::CLEAN_DATA.0 != 0)?;
        }
        if opt.0 & UnregisterOpt::CHECK_DUP.0 != 0 {
            for e in engines.values().filter(|e| e.unique) {
                if let Some(table) = self.table.lock().unwrap().as_ref() {
                    self.collect_remote_duplicate_rows(ctx, e.index_id, table)?;
                }
                if tidb_failpoint::eval(
                    "github.com/pingcap/tidb/pkg/ddl/ingest/mockCollectRemoteDuplicateRowsFailed",
                )
                .is_some()
                {
                    return Err(Error::Other("context deadline exceeded".into()));
                }
            }
        }
        engines.clear();
        Ok(())
    }
    /// Go `litBackendCtx.CollectRemoteDuplicateRows` with ErrorOnDup.
    pub fn collect_remote_duplicate_rows(
        &self,
        ctx: &Context,
        index_id: i64,
        table: &tidb_model::TableInfo,
    ) -> Result<()> {
        let Some(index) = table.indices.iter_deref().find(|i| i.read().id == index_id) else {
            return Ok(());
        };
        let mut ids = vec![table.id];
        if let Some(partition) = table.get_partition_info() {
            ids.extend(partition.read().definitions.snapshot().iter().map(|d| d.id));
        }
        for id in ids {
            let start = tidb_tablecodec::table_key::encode_index_seek_key(id, index_id, &[]);
            let end = tidb_txnkv::Key::from_bytes(start.clone())
                .prefix_next()
                .as_bytes()
                .to_vec();
            self.backend
                .collect_remote_duplicate_rows(ctx, &start, &end, self.init_ts)
                .map_err(|error| {
                    crate::util::try_convert_to_key_exists_err(error.into(), &index.read(), table)
                })?;
        }
        Ok(())
    }
    /// Go `litBackendCtx.GetLocalBackend`.
    pub fn get_local_backend(&self) -> &Arc<Backend> {
        &self.backend
    }
    /// Go `litBackendCtx.NextStartKey`.
    pub fn next_start_key(&self) -> Vec<u8> {
        self.checkpoint
            .as_ref()
            .map_or_else(Vec::new, |cp| cp.next_start_key())
    }
    /// Go `litBackendCtx.TotalKeyCount`.
    pub fn total_key_count(&self) -> i64 {
        self.checkpoint
            .as_ref()
            .map_or(0, |cp| cp.total_key_count())
    }
    /// Go `litBackendCtx.AddChunk`.
    pub fn add_chunk(&self, id: usize, end: Vec<u8>) {
        if let Some(cp) = &self.checkpoint {
            cp.add_chunk(id, end);
        }
    }
    /// Go `litBackendCtx.UpdateChunk`.
    pub fn update_chunk(&self, id: usize, count: i64, done: bool) {
        if let Some(cp) = &self.checkpoint {
            cp.update_chunk(id, count, done);
        }
    }
    /// Go `litBackendCtx.FinishChunk`.
    pub fn finish_chunk(&self, id: usize, count: i64) {
        if let Some(cp) = &self.checkpoint {
            cp.finish_chunk(id, count);
        }
    }
    /// Go `litBackendCtx.GetImportTS`.
    pub fn get_import_ts(&self) -> u64 {
        self.checkpoint
            .as_ref()
            .map_or(self.import_ts, |cp| cp.get_import_ts())
    }
    /// Go `litBackendCtx.AdvanceWatermark`.
    pub fn advance_watermark(&self, imported: bool) -> Result<()> {
        if imported
            && tidb_failpoint::eval(
                "github.com/pingcap/tidb/pkg/ddl/ingest/ddlIngestFailOnceBeforeCheckpointUpdated",
            )
            .is_some()
        {
            return Err(Error::Other(
                "failpoint: ddlIngestFailOnceBeforeCheckpointUpdated".into(),
            ));
        }
        self.checkpoint
            .as_ref()
            .map_or(Ok(()), |cp| cp.advance_watermark(imported))
    }
    /// Go `litBackendCtx.Close`.
    pub fn close(&self) {
        if let Some(checkpoint) = &self.checkpoint {
            checkpoint.close();
        }
        self.disk_root.remove(self.job_id);
    }
}
impl ResourceTracker for LitBackendCtx {
    fn get_disk_usage(&self) -> u64 {
        check_disk_quota(self.backend.as_ref(), i64::MAX).2 as u64
    }
}
struct BackendTracker(std::sync::Weak<LitBackendCtx>);
impl ResourceTracker for BackendTracker {
    fn get_disk_usage(&self) -> u64 {
        self.0
            .upgrade()
            .map_or(0, |bc| ResourceTracker::get_disk_usage(bc.as_ref()))
    }
}
impl Drop for LitBackendCtx {
    fn drop(&mut self) {
        self.disk_root.remove(self.job_id);
    }
}

/// Go `BackendCtx`, shared by the local backend and unistore test backend.
pub trait BackendCtx: Send + Sync {
    /// Go `BackendCtx.Register`.
    fn register(
        &self,
        index_ids: &[i64],
        uniques: &[bool],
        table: &tidb_model::TableInfo,
    ) -> Result<Vec<Arc<dyn crate::engine::IngestEngine>>>;
    /// Go `BackendCtx.FinishAndUnregisterEngines`.
    fn finish_and_unregister_engines(&self, ctx: &Context, opt: UnregisterOpt) -> Result<()>;
    /// Go `BackendCtx.IngestIfQuotaExceeded`.
    fn ingest_if_quota_exceeded(&self, ctx: &Context, task_id: usize, count: i64) -> Result<()>;
    /// Go `BackendCtx.Ingest`.
    fn ingest(&self, ctx: &Context) -> Result<()>;
    /// Go `BackendCtx.GetLocalBackend`; absent on the mock path.
    fn get_local_backend(&self) -> Option<Arc<Backend>>;
    /// Go `CheckpointOperator.NextStartKey`.
    fn next_start_key(&self) -> Vec<u8>;
    /// Go `CheckpointOperator.TotalKeyCount`.
    fn total_key_count(&self) -> i64;
    /// Go `CheckpointOperator.AddChunk`.
    fn add_chunk(&self, id: usize, end: Vec<u8>);
    /// Go `CheckpointOperator.UpdateChunk`.
    fn update_chunk(&self, id: usize, count: i64, done: bool);
    /// Go `CheckpointOperator.FinishChunk`.
    fn finish_chunk(&self, id: usize, count: i64);
    /// Go `CheckpointOperator.GetImportTS`.
    fn get_import_ts(&self) -> u64;
    /// Go `CheckpointOperator.AdvanceWatermark`.
    fn advance_watermark(&self, imported: bool) -> Result<()>;
    /// Go `BackendCtx.CollectRemoteDuplicateRows`.
    fn collect_remote_duplicate_rows(
        &self,
        ctx: &Context,
        index_id: i64,
        table: &tidb_model::TableInfo,
    ) -> Result<()>;
    /// Go `BackendCtx.GetDiskUsage`.
    fn get_disk_usage(&self) -> u64;
    /// Go `BackendCtx.Close`.
    fn close(&self);
}
impl BackendCtx for LitBackendCtx {
    fn register(
        &self,
        ids: &[i64],
        uniques: &[bool],
        table: &tidb_model::TableInfo,
    ) -> Result<Vec<Arc<dyn crate::engine::IngestEngine>>> {
        Ok(LitBackendCtx::register(self, ids, uniques, table)?
            .into_iter()
            .map(|e| e as Arc<dyn crate::engine::IngestEngine>)
            .collect())
    }
    fn finish_and_unregister_engines(&self, ctx: &Context, opt: UnregisterOpt) -> Result<()> {
        LitBackendCtx::finish_and_unregister_engines(self, ctx, opt)
    }
    fn ingest_if_quota_exceeded(&self, ctx: &Context, id: usize, count: i64) -> Result<()> {
        LitBackendCtx::ingest_if_quota_exceeded(self, ctx, id, count)
    }
    fn ingest(&self, ctx: &Context) -> Result<()> {
        LitBackendCtx::ingest(self, ctx)
    }
    fn get_local_backend(&self) -> Option<Arc<Backend>> {
        Some(self.backend.clone())
    }
    fn next_start_key(&self) -> Vec<u8> {
        LitBackendCtx::next_start_key(self)
    }
    fn total_key_count(&self) -> i64 {
        LitBackendCtx::total_key_count(self)
    }
    fn add_chunk(&self, id: usize, end: Vec<u8>) {
        LitBackendCtx::add_chunk(self, id, end)
    }
    fn update_chunk(&self, id: usize, count: i64, done: bool) {
        LitBackendCtx::update_chunk(self, id, count, done)
    }
    fn finish_chunk(&self, id: usize, count: i64) {
        LitBackendCtx::finish_chunk(self, id, count)
    }
    fn get_import_ts(&self) -> u64 {
        LitBackendCtx::get_import_ts(self)
    }
    fn advance_watermark(&self, imported: bool) -> Result<()> {
        LitBackendCtx::advance_watermark(self, imported)
    }
    fn collect_remote_duplicate_rows(
        &self,
        ctx: &Context,
        index_id: i64,
        table: &tidb_model::TableInfo,
    ) -> Result<()> {
        LitBackendCtx::collect_remote_duplicate_rows(self, ctx, index_id, table)
    }
    fn get_disk_usage(&self) -> u64 {
        ResourceTracker::get_disk_usage(self)
    }
    fn close(&self) {
        LitBackendCtx::close(self)
    }
}
