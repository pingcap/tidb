// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/config.go`.
use crate::mem_root::MemRootImpl;
use std::path::PathBuf;
use tidb_ingestor_ingestctrl::local::BackendConfig;

/// Go `ImporterRangeConcurrencyForTest`: when set, the backend's worker
/// concurrency is twice it, whatever the job's concurrency.
pub static IMPORTER_RANGE_CONCURRENCY_FOR_TEST: std::sync::RwLock<Option<usize>> = std::sync::RwLock::new(None);

/// Go `genConfig`, DDL's local-sort backend settings.
pub fn gen_config(
    job_sort_path: PathBuf,
    mem_root: &MemRootImpl,
    check_dup: bool,
    resource_group: String,
    concurrency: usize,
    global_sort: bool,
) -> BackendConfig {
    let mut worker_concurrency = concurrency * 2;
    if let Some(range_concurrency) = *IMPORTER_RANGE_CONCURRENCY_FOR_TEST.read().unwrap_or_else(std::sync::PoisonError::into_inner) {
        worker_concurrency = range_concurrency * 2;
    }
    let mut cfg = BackendConfig {
        local_store_dir: job_sort_path,
        resource_group_name: resource_group,
        task_type: "ddl".into(),
        worker_concurrency,
        mem_table_size: 512 << 20,
        local_writer_mem_cache_size: 128 << 20,
        dupe_detect_enabled: check_dup && !global_sort,
        kv_write_batch_size: 16 << 10,
        should_check_write_stall: true,
        checkpoint_enabled: true,
        pause_pd_scheduler_scope: tidb_ingestor_ingestctrl::local::PAUSE_PD_SCHEDULER_SCOPE_TABLE.into(),
        raft_kv2_switch_mode_duration: std::time::Duration::ZERO,
    };
    adjust_import_memory(mem_root, &mut cfg);
    cfg
}
/// Go `adjustImportMemory`.
pub fn adjust_import_memory(mem_root: &MemRootImpl, cfg: &mut BackendConfig) {
    if try_aggressive_memory(mem_root, cfg) {
        return;
    }
    let default_mem = (cfg.local_writer_mem_cache_size * cfg.worker_concurrency / 2
        + 4 * cfg.mem_table_size) as i64;
    let scale = default_mem / mem_root.max_memory_quota();
    if scale <= 1 {
        return;
    }
    cfg.local_writer_mem_cache_size /= scale as usize;
    cfg.mem_table_size /= scale as usize;
}
/// Go `tryAggressiveMemory`.
pub fn try_aggressive_memory(mem_root: &MemRootImpl, cfg: &BackendConfig) -> bool {
    let default_mem =
        (cfg.local_writer_mem_cache_size * cfg.worker_concurrency / 2 + cfg.mem_table_size) as i64;
    default_mem + mem_root.current_usage() <= mem_root.max_memory_quota()
}
/// Go `CopReadBatchSize`.
pub fn cop_read_batch_size(hint_size: usize, ddl_reorg_batch_size: usize) -> usize {
    if hint_size > 0 {
        hint_size
    } else {
        10 * ddl_reorg_batch_size
    }
}
