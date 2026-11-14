// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/env.go`.
use crate::Result;
use std::path::{Path, PathBuf};

/// Go `GetIngestTempDataDir`.
pub fn get_ingest_temp_data_dir() -> PathBuf {
    let cfg = tidb_config::config_tree::config::get_global_config();
    Path::new(&cfg.temp_dir).join(format!("tmp_ddl-{}", cfg.port))
}
/// Go `GenIngestTempDataDir`.
pub fn gen_ingest_temp_data_dir() -> Result<PathBuf> {
    let path = get_ingest_temp_data_dir();
    create_dir(&path)?;
    Ok(path)
}
/// Go `os.MkdirAll(path, 0700)` used by ingest.
pub(crate) fn create_dir(path: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}
/// Go `InstanceAddr`.
pub fn instance_addr() -> String {
    let cfg = tidb_config::config_tree::config::get_global_config();
    let host = if cfg.advertise_address.contains(':') {
        format!("[{}]", cfg.advertise_address)
    } else {
        cfg.advertise_address.clone()
    };
    format!("{host}:{}:{}", cfg.port, cfg.temp_dir)
}
/// Go `encodeBackendTag`.
pub fn encode_backend_tag(job_id: i64, check_dup: bool) -> String {
    if check_dup {
        format!("{job_id}-dup")
    } else {
        job_id.to_string()
    }
}
/// Go `decodeBackendTag`. Go parses only the decimal directory name.
pub fn decode_backend_tag(name: &str) -> std::result::Result<i64, std::num::ParseIntError> {
    name.parse()
}

/// Go `LitErrCleanSortPath`.
pub const LIT_ERR_CLEAN_SORT_PATH: &str = "clean up temp dir failed";
/// Go `LitWarnEnvInitFail`.
pub const LIT_WARN_ENV_INIT_FAIL: &str = "initialize environment failed";

fn log_clean_error(error: impl std::fmt::Display) {
    tidb_ddl_logutil::ddl_ingest_logger().error(
        LIT_ERR_CLEAN_SORT_PATH,
        &[tidb_log::Field::new(
            "error",
            tidb_log::Value::Str(error.to_string()),
        )],
    );
}

/// Go `CleanUpTempDir`: removes the data directories of jobs no longer in
/// `mysql.tidb_ddl_job`. `filter_processing_job_ids` is Go's
/// `filterProcessingJobIDs` on the caller's session (see
/// [`filter_processing_job_ids_sql`]).
pub fn clean_up_temp_dir(
    path: &Path,
    filter_processing_job_ids: impl FnOnce(&[i64]) -> std::result::Result<Vec<i64>, String>,
) {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => {
            tidb_ddl_logutil::ddl_ingest_logger().warn(
                LIT_ERR_CLEAN_SORT_PATH,
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(error.to_string()),
                )],
            );
            return;
        }
    };
    let mut to_check_job_ids = std::collections::BTreeSet::new();
    for entry in entries.flatten() {
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        match decode_backend_tag(&entry.file_name().to_string_lossy()) {
            Ok(job_id) => {
                to_check_job_ids.insert(job_id);
            }
            Err(error) => log_clean_error(error),
        }
    }
    if to_check_job_ids.is_empty() {
        return;
    }
    let id_slice = to_check_job_ids.iter().copied().collect::<Vec<_>>();
    let processing = match filter_processing_job_ids(&id_slice) {
        Ok(processing) => processing,
        Err(error) => {
            log_clean_error(error);
            return;
        }
    };
    for id in processing {
        to_check_job_ids.remove(&id);
    }
    for id in to_check_job_ids {
        tidb_ddl_logutil::ddl_ingest_logger().info(
            "remove stale temp index data",
            &[tidb_log::Field::new("jobID", tidb_log::Value::I64(id))],
        );
        for check_dup in [false, true] {
            if let Err(error) =
                std::fs::remove_dir_all(path.join(encode_backend_tag(id, check_dup)))
            {
                // Go `os.RemoveAll` succeeds on a missing path.
                if error.kind() != std::io::ErrorKind::NotFound {
                    log_clean_error(error);
                }
            }
        }
    }
}

/// Go `filterProcessingJobIDs`'s statement over the sorted job IDs.
pub fn filter_processing_job_ids_sql(job_ids: &[i64]) -> String {
    let ids = job_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    format!("SELECT job_id FROM mysql.tidb_ddl_job WHERE job_id IN ({ids})")
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `env_test.go:TestGenLightningDataDir`.
    #[test]
    fn test_gen_lightning_data_dir() {
        let dir = tempfile::tempdir().unwrap();
        let previous = tidb_config::config_tree::config::get_global_config();
        struct Restore(std::sync::Arc<tidb_config::config_tree::config::Config>);
        impl Drop for Restore {
            fn drop(&mut self) {
                tidb_config::config_tree::config::store_global_config(self.0.clone());
            }
        }
        let _restore = Restore(previous);
        tidb_config::config_tree::config::update_global(|cfg| {
            cfg.temp_dir = dir.path().to_string_lossy().into_owned();
            cfg.port = 5678;
        });
        assert_eq!(
            gen_ingest_temp_data_dir().unwrap(),
            dir.path().join("tmp_ddl-5678")
        );
    }
    /// Go `env_test.go:TestLitBackendCtxMgr`, with the session query's exact
    /// active-job sets supplied at the storage boundary.
    #[test]
    fn test_lit_backend_ctx_mgr() {
        let dir = tempfile::tempdir().unwrap();
        for id in [100, 101] {
            create_dir(&dir.path().join(id.to_string())).unwrap();
        }
        clean_up_temp_dir(dir.path(), |_| Ok(vec![100, 101]));
        assert!(dir.path().join("100").exists());
        assert!(dir.path().join("101").exists());
        clean_up_temp_dir(dir.path(), |_| Ok(vec![100]));
        assert!(dir.path().join("100").exists());
        assert!(!dir.path().join("101").exists());
        clean_up_temp_dir(dir.path(), |_| Ok(vec![]));
        assert!(!dir.path().join("100").exists());
        clean_up_temp_dir(&dir.path().join("unknown_path"), |_| {
            panic!("no query for missing dir")
        });
    }
}

/// Go `LitMemRoot`, `LitDiskRoot`, and `litRLimit` after environment setup.
pub struct LightningEnv {
    /// Go `LitMemRoot`.
    pub mem_root: std::sync::Arc<crate::mem_root::MemRootImpl>,
    /// Go `LitDiskRoot`.
    pub disk_root: std::sync::Arc<crate::disk_root::DiskRootImpl>,
    /// Go `litRLimit`.
    pub rlimit: u64,
}
/// Go `LitInitialized` is true only after a TiKV environment is initialized.
pub static LIT_INITIALIZED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static LIGHTNING_ENV: std::sync::OnceLock<LightningEnv> = std::sync::OnceLock::new();
/// Go `InitGlobalLightningEnv`.
pub fn init_global_lightning_env(path: &Path) -> bool {
    use std::sync::{Arc, atomic::Ordering};
    let cfg = tidb_config::config_tree::config::get_global_config();
    if cfg.store.0 != tidb_config::store::STORE_TYPE_TIKV {
        return false;
    }
    LIGHTNING_ENV.get_or_init(|| {
        let mut memory = tidb_memory::mem_total().map(|n| n / 2).unwrap_or(2 << 30);
        if let Some(value) =
            tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/ingest/setMemTotalInMB")
        {
            memory = value.int() as u64 * (1 << 20);
        }
        let disk_root = Arc::new(crate::disk_root::DiskRootImpl::new(path.to_owned()));
        disk_root.update_usage();
        if let Err(error) = disk_root.startup_check() {
            tidb_log::warn(
                "ingest backfill may not be available",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(error.to_string()),
                )],
            );
        }
        LightningEnv {
            mem_root: Arc::new(crate::mem_root::MemRootImpl::new(memory as i64)),
            disk_root,
            rlimit: gen_rlimit(),
        }
    });
    LIT_INITIALIZED.store(true, Ordering::SeqCst);
    true
}
/// Go ingest's initialized process resources.
pub fn lightning_env() -> Option<&'static LightningEnv> {
    LIGHTNING_ENV.get()
}
/// Go `util.GenRLimit("ddl-ingest")`: the soft `RLIMIT_NOFILE`, 1024 when
/// it cannot be read.
fn gen_rlimit() -> u64 {
    match nix::sys::resource::getrlimit(nix::sys::resource::Resource::RLIMIT_NOFILE) {
        Ok((soft, _)) => soft,
        Err(error) => {
            tidb_logutil::bg_logger().warn(
                "[ddl-ingest] get system open file limit error",
                &[
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    tidb_log::Field::new("default", tidb_log::Value::Str("1024".to_owned())),
                ],
            );
            1024
        }
    }
}
