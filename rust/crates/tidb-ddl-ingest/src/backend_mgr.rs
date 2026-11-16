// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/backend_mgr.go`.
use crate::{
    Error, Result,
    backend::{DistributedLock, LitBackendCtx},
    checkpoint::{
        CheckpointManager, CheckpointStorage, DistTaskCheckpointStorage, NormalCheckpointStorage,
        TimestampOracle,
    },
    env,
};
use std::sync::Arc;
use tidb_gocontext::Context;
use tidb_ingestor_ingestctrl::{local::Backend, region_job::RemoteImportClient};
use tidb_model::Job;

/// Go `ResignOwnerForTest`: the DDL scheduler resigns its ownership once
/// (`ownerResignAfterDispatchLoopCheck`).
pub static RESIGN_OWNER_FOR_TEST: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Go `BackendCtxBuilder`.
pub struct BackendCtxBuilder<S: tikv_client::PdClient> {
    ctx: Context,
    store: tidb_txnkv::driver::tikv_store::TikvStore<S>,
    job: Job,
    import_ts: u64,
    physical_id: i64,
    checkpoint_storage: Option<Arc<dyn CheckpointStorage>>,
    distributed_lock: Option<DistributedLock>,
    check_dup: bool,
}
impl<S: tikv_client::PdClient> BackendCtxBuilder<S> {
    /// Go `NewBackendCtxBuilder`.
    pub fn new(
        ctx: Context,
        store: tidb_txnkv::driver::tikv_store::TikvStore<S>,
        job: Job,
    ) -> Self {
        Self {
            ctx,
            store,
            job,
            import_ts: 0,
            physical_id: 0,
            checkpoint_storage: None,
            distributed_lock: None,
            check_dup: false,
        }
    }
    /// Go `BackendCtxBuilder.WithImportDistributedLock`.
    pub fn with_import_distributed_lock(mut self, lock: DistributedLock, import_ts: u64) -> Self {
        self.distributed_lock = Some(lock);
        self.import_ts = import_ts;
        self
    }
    /// Go `BackendCtxBuilder.WithCheckpointManagerParam`.
    pub fn with_checkpoint_manager_param(
        mut self,
        sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
        physical_id: i64,
    ) -> Self {
        self.physical_id = physical_id;
        self.checkpoint_storage = Some(Arc::new(NormalCheckpointStorage {
            sess_pool,
            job_id: self.job.id,
            physical_id,
        }));
        self
    }
    /// Go `BackendCtxBuilder.WithDistTaskCheckpointManagerParam`.
    pub fn with_dist_task_checkpoint_manager_param(
        mut self,
        storage: DistTaskCheckpointStorage,
        physical_id: i64,
    ) -> Self {
        self.physical_id = physical_id;
        self.checkpoint_storage = Some(Arc::new(storage));
        self
    }
    /// Go `BackendCtxBuilder.ForDuplicateCheck`.
    pub fn for_duplicate_check(mut self) -> Self {
        self.check_dup = true;
        self
    }
    /// Go `BackendCtxBuilder.Build`.
    pub fn build(
        self,
        backend: Option<Arc<Backend>>,
    ) -> Result<Arc<dyn crate::backend::BackendCtx>> {
        if let Some(error) = self.ctx.err() {
            return Err(Error::Other(error.to_string()));
        }
        tidb_failpoint::inject("github.com/pingcap/tidb/pkg/ddl/ingest/beforeCreateLocalBackend", |_| {
            RESIGN_OWNER_FOR_TEST.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        let path = gen_job_sort_path(self.job.id, self.check_dup)?;
        let mock_store: Arc<dyn std::any::Any + Send + Sync> = Arc::new(self.store.clone());
        let store = self.store;
        let oracle: Arc<dyn TimestampOracle> = Arc::new(move || {
            store
                .get_timestamp_async()
                .wait()
                .map_err(|e| Error::Other(e.to_string()))
        });
        let checkpoint = self
            .checkpoint_storage
            .map(|storage| {
                CheckpointManager::new(
                    storage,
                    self.physical_id,
                    &path,
                    env::instance_addr(),
                    oracle,
                )
                .map(Arc::new)
            })
            .transpose()?;
        let mut injected = MockNewBackendContext {
            store: mock_store,
            job: self.job.clone(),
            checkpoint: checkpoint.clone(),
            backend: None,
        };
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/ddl/ingest/mockNewBackendContext",
            &mut injected,
        );
        if let Some(backend) = injected.backend {
            return Ok(backend);
        }
        let resources = env::lightning_env()
            .ok_or_else(|| Error::Other("ingest environment is not initialized".into()))?;
        let backend = backend.ok_or_else(|| Error::Other("missing local ingest backend".into()))?;
        Ok(LitBackendCtx::new(
            self.job.id,
            backend,
            resources.mem_root.clone(),
            resources.disk_root.clone(),
            checkpoint,
            self.import_ts,
            self.job.real_start_ts,
            self.distributed_lock,
        ))
    }
}
/// Go `genJobSortPath`.
pub fn gen_job_sort_path(job_id: i64, check_dup: bool) -> Result<std::path::PathBuf> {
    Ok(env::gen_ingest_temp_data_dir()?.join(env::encode_backend_tag(job_id, check_dup)))
}
/// Go `CreateLocalBackend`. The caller supplies `ReorgMeta.GetConcurrency`
/// resolved against the process defaults, as the owner already does.
pub fn create_local_backend<S: tikv_client::PdClient>(
    store: tidb_txnkv::driver::tikv_store::TikvStore<S>,
    job: &Job,
    has_unique: bool,
    check_dup: bool,
    concurrency: usize,
    adjusted_worker_concurrency: usize,
) -> Result<Arc<Backend>> {
    let resources = env::lightning_env()
        .ok_or_else(|| Error::Other("ingest environment is not initialized".into()))?;
    let meta = job
        .reorg_meta
        .as_ref()
        .ok_or_else(|| Error::Other("missing DDL reorganization metadata".into()))?
        .read();
    let mut cfg = crate::config::gen_config(
        gen_job_sort_path(job.id, check_dup)?,
        &resources.mem_root,
        has_unique,
        meta.resource_group_name.to_utf8_lossy_go(),
        concurrency,
        meta.use_cloud_storage,
    );
    if adjusted_worker_concurrency > 0 {
        cfg.worker_concurrency = adjusted_worker_concurrency;
    }
    let security = tidb_config::config_tree::config::get_global_config()
        .security
        .clone();
    let security = tidb_pd_client::ClusterSecurity::new(
        security.cluster_ssl_ca,
        security.cluster_ssl_cert,
        security.cluster_ssl_key,
        security.cluster_verify_cn,
    );
    let backend = Arc::new(Backend::new(
        cfg,
        Arc::new(RemoteImportClient::new(store, security)),
    )?);
    backend.update_write_speed_limit(meta.get_max_write_speed());
    Ok(backend)
}

/// Go `mockNewBackendContext` InjectCall arguments.
pub struct MockNewBackendContext {
    /// Go test injection's store, typed as the node's native client.
    pub store: Arc<dyn std::any::Any + Send + Sync>,
    /// Go `b.job`.
    pub job: Job,
    /// Go `cpOp`.
    pub checkpoint: Option<Arc<CheckpointManager>>,
    /// Go `mockBackend` output.
    pub backend: Option<Arc<dyn crate::backend::BackendCtx>>,
}
