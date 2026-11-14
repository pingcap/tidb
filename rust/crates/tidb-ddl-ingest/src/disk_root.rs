// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/ingest/disk_root.go`.
use crate::{Error, Result, check_env_error};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};
use tidb_lightning_common::get_storage_size;

const CAPACITY_THRESHOLD: f64 = 0.9;
/// Go `localSortHeadroomBytesPerSlot`.
pub const LOCAL_SORT_HEADROOM_BYTES_PER_SLOT: u64 = 2 * 1024 * 1024 * 1024;
/// Go `ResourceTracker`.
pub trait ResourceTracker: Send + Sync {
    /// Go `ResourceTracker.GetDiskUsage`.
    fn get_disk_usage(&self) -> u64;
}
#[derive(Default)]
struct State {
    capacity: u64,
    used: u64,
    bc_used: u64,
    items: HashMap<i64, Arc<dyn ResourceTracker>>,
}
/// Go `diskRootImpl`.
pub struct DiskRootImpl {
    path: PathBuf,
    state: RwLock<State>,
    updating: AtomicBool,
}
impl DiskRootImpl {
    /// Go `NewDiskRootImpl`.
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            state: RwLock::new(State::default()),
            updating: AtomicBool::new(false),
        }
    }
    /// Go `diskRootImpl.Add`.
    pub fn add(&self, id: i64, tracker: Arc<dyn ResourceTracker>) {
        self.state.write().unwrap().items.insert(id, tracker);
    }
    /// Go `diskRootImpl.Remove`.
    pub fn remove(&self, id: i64) {
        self.state.write().unwrap().items.remove(&id);
    }
    /// Go `diskRootImpl.Count`.
    pub fn count(&self) -> usize {
        self.state.read().unwrap().items.len()
    }
    /// Go `diskRootImpl.UpdateUsage`.
    pub fn update_usage(&self) {
        if self
            .updating
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let size = get_storage_size(&self.path).unwrap_or_default();
        self.updating.store(false, Ordering::SeqCst);
        let mut s = self.state.write().unwrap();
        s.capacity = size.capacity;
        s.used = size.capacity - size.available;
        s.bc_used = s.items.values().map(|item| item.get_disk_usage()).sum();
    }
    /// Go `diskRootImpl.ShouldImport`.
    pub fn should_import(&self) -> bool {
        let s = self.state.read().unwrap();
        if s.bc_used > tidb_vardef::DDL_DISK_QUOTA.load(Ordering::SeqCst) {
            return true;
        }
        if s.used == 0 && s.capacity == 0 {
            return false;
        }
        s.used as f64 >= s.capacity as f64 * CAPACITY_THRESHOLD
    }
    /// Go `diskRootImpl.UsageInfo`.
    pub fn usage_info(&self) -> String {
        let s = self.state.read().unwrap();
        format!(
            "disk usage: {}/{}, backend usage: {}",
            s.used, s.capacity, s.bc_used
        )
    }
    /// Go `diskRootImpl.PreCheckUsage`.
    pub fn pre_check_usage(&self) -> Result<()> {
        if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/ingest/mockIngestCheckEnvFailed")
            .is_some()
        {
            return Err(check_env_error("mock error"));
        }
        crate::env::create_dir(&self.path).map_err(|e| check_env_error(e.to_string()))?;
        let size = get_storage_size(&self.path).map_err(|e| check_env_error(e.to_string()))?;
        if risk_of_disk_full(size.available, size.capacity) && !cfg!(target_os = "macos") {
            return Err(check_env_error(format!(
                "no enough space in {}",
                self.path.display()
            )));
        }
        Ok(())
    }
    /// Go `diskRootImpl.StartupCheck`.
    pub fn startup_check(&self) -> Result<()> {
        let size = get_storage_size(&self.path)?;
        let quota = tidb_vardef::DDL_DISK_QUOTA.load(Ordering::SeqCst);
        if size.available < quota {
            return Err(Error::Other(format!(
                "the available disk space({}) in {} should be greater than @@tidb_ddl_disk_quota({quota})",
                size.available,
                self.path.display()
            )));
        }
        Ok(())
    }
}
/// Go `minFreeDiskBytes`.
pub fn min_free_disk_bytes(capacity: u64) -> u64 {
    capacity - (capacity as f64 * CAPACITY_THRESHOLD) as u64
}
/// Go `riskOfDiskFull`.
pub fn risk_of_disk_full(available: u64, capacity: u64) -> bool {
    available < min_free_disk_bytes(capacity)
}
/// Go `localSortDiskSpaceCheck`.
pub struct LocalSortDiskSpaceCheck<'a> {
    /// Go `execID`.
    pub exec_id: &'a str,
    /// Go `sortPath`.
    pub sort_path: &'a str,
    /// Go `availableBytes`.
    pub available_bytes: u64,
    /// Go `totalCapacityBytes`.
    pub total_capacity_bytes: u64,
    /// Go `currentTaskRuntimeSlots`.
    pub current_task_runtime_slots: i64,
}
/// Go `checkLocalSortDiskSpace`.
pub fn check_local_sort_disk_space_measured(p: LocalSortDiskSpaceCheck<'_>) -> Result<()> {
    let headroom = (p.current_task_runtime_slots as u64)
        .wrapping_mul(LOCAL_SORT_HEADROOM_BYTES_PER_SLOT)
        .min(tidb_vardef::DDL_DISK_QUOTA.load(Ordering::SeqCst));
    let threshold = min_free_disk_bytes(p.total_capacity_bytes).wrapping_add(headroom);
    if p.available_bytes > threshold {
        return Ok(());
    }
    Err(check_env_error(format!(
        "insufficient free disk space on TiDB node {} at {}: {} bytes available; available free disk space must be greater than {} bytes; the add-index job cannot start because low disk space would degrade SST ingestion. Free disk space on this TiDB node by removing unnecessary logs or files",
        p.exec_id, p.sort_path, p.available_bytes, threshold
    )))
}
/// Go `CheckLocalSortDiskSpace`.
pub fn check_local_sort_disk_space(exec_id: &str, current_task_runtime_slots: i64) -> Result<()> {
    if tidb_failpoint::eval(
        "github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceProbeFailed",
    )
    .is_some()
    {
        return Err(Error::Other("mock local sort disk probe failed".into()));
    }
    if tidb_failpoint::eval(
        "github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceInsufficient",
    )
    .is_some()
    {
        return Err(check_env_error("mock insufficient local sort disk space"));
    }
    let path = crate::env::gen_ingest_temp_data_dir()?;
    let size = get_storage_size(&path)?;
    match check_local_sort_disk_space_measured(LocalSortDiskSpaceCheck {
        exec_id,
        sort_path: &path.to_string_lossy(),
        available_bytes: size.available,
        total_capacity_bytes: size.capacity,
        current_task_runtime_slots,
    }) {
        Err(Error::Database(_)) if cfg!(target_os = "macos") => Ok(()),
        result => result,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `disk_root_test.go:TestRiskOfDiskFull`.
    #[test]
    fn test_risk_of_disk_full() {
        assert_eq!(min_free_disk_bytes(100), 10);
        assert_eq!(min_free_disk_bytes(101), 11);
        for (a, c, expected) in [
            (11, 100, false),
            (10, 100, false),
            (9, 100, true),
            (11, 101, false),
            (10, 101, true),
        ] {
            assert_eq!(risk_of_disk_full(a, c), expected);
        }
    }
    /// Go `disk_root_test.go:TestCheckLocalSortDiskSpace`, all six cases.
    #[test]
    fn test_check_local_sort_disk_space() {
        const GB: u64 = 1024 * 1024 * 1024;
        for (available, capacity, slots, error) in [
            (7, 20, 2, false),
            (6, 20, 2, true),
            (2, 10, 1, true),
            (121, 200, 60, false),
            (120, 200, 60, true),
            (1, 1, 1, true),
        ] {
            let result = check_local_sort_disk_space_measured(LocalSortDiskSpaceCheck {
                exec_id: "10.0.1.8:4000",
                sort_path: "/tmp/local-sort",
                available_bytes: available * GB,
                total_capacity_bytes: capacity * GB,
                current_task_runtime_slots: slots,
            });
            assert_eq!(result.is_err(), error);
            if let Err(e) = result {
                let e = e.to_string();
                assert!(e.contains("the add-index job cannot start because low disk space would degrade SST ingestion"));
                assert!(!e.contains("runtime slots"));
                assert!(!e.contains("bytes per slot"));
                if available == 6 {
                    assert!(e.contains("6442450944 bytes available; available free disk space must be greater than 6442450944 bytes"));
                }
                if capacity == 1 {
                    assert!(e.contains("1073741824 bytes available; available free disk space must be greater than 2254857831 bytes"));
                }
            }
        }
        assert_eq!(LOCAL_SORT_HEADROOM_BYTES_PER_SLOT, 2 * GB);
    }
    /// Go `disk_root_test.go:TestCheckLocalSortDiskSpaceErrorClassification`.
    #[test]
    fn test_check_local_sort_disk_space_error_classification() {
        {
            let _fp = tidb_failpoint::enable_for_test(
                "github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceProbeFailed",
                "return",
            );
            let err = check_local_sort_disk_space("10.0.1.8:4000", 1).unwrap_err();
            assert!(matches!(err, Error::Other(_)));
            assert!(
                err.to_string()
                    .contains("mock local sort disk probe failed")
            );
        }
        {
            let _fp = tidb_failpoint::enable_for_test(
                "github.com/pingcap/tidb/pkg/ddl/ingest/mockLocalSortDiskSpaceInsufficient",
                "return",
            );
            let err = check_local_sort_disk_space("10.0.1.8:4000", 1).unwrap_err();
            assert!(matches!(err, Error::Database(_)));
            assert!(
                err.to_string()
                    .contains("mock insufficient local sort disk space")
            );
        }
    }
}
