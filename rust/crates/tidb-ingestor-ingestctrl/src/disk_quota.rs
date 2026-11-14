// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/disk_quota.go`.
/// Go `backend.EngineFileSize`.
#[derive(Clone, Debug)]
pub struct EngineFileSize {
    /// Go `UUID`.
    pub uuid: [u8; 16],
    /// Go `DiskSize`.
    pub disk_size: i64,
    /// Go `MemSize`.
    pub mem_size: i64,
    /// Go `IsImporting`.
    pub is_importing: bool,
}
/// Go `DiskUsage`.
pub trait DiskUsage {
    /// Go `DiskUsage.EngineFileSizes`.
    fn engine_file_sizes(&self) -> Vec<EngineFileSize>;
}
/// Go `CheckDiskQuota`; importing engines are counted first, then increasing
/// size, so the largest idle engines are selected for import.
pub fn check_disk_quota(mgr: &dyn DiskUsage, quota: i64) -> (Vec<[u8; 16]>, usize, i64, i64) {
    let mut sizes = mgr.engine_file_sizes();
    sizes.sort_by_key(|s| (!s.is_importing, s.disk_size + s.mem_size));
    let (mut large, mut importing, mut disk, mut mem) = (Vec::new(), 0, 0, 0);
    for s in sizes {
        disk += s.disk_size;
        mem += s.mem_size;
        if disk + mem > quota {
            if s.is_importing {
                importing += 1;
            } else {
                large.push(s.uuid);
            }
        }
    }
    (large, importing, disk, mem)
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `disk_quota_test.go:TestCheckDiskQuota`.
    #[test]
    fn test_check_disk_quota() {
        struct Usage;
        impl DiskUsage for Usage {
            fn engine_file_sizes(&self) -> Vec<EngineFileSize> {
                [
                    (0x11, 1000, 0, false),
                    (0x33, 2000, 1000, true),
                    (0x55, 1500, 3500, false),
                    (0x77, 0, 7000, true),
                    (0x99, 4500, 4500, false),
                ]
                .into_iter()
                .map(|(id, disk, mem, importing)| EngineFileSize {
                    uuid: [id; 16],
                    disk_size: disk,
                    mem_size: mem,
                    is_importing: importing,
                })
                .collect()
            }
        }
        for (quota, large, in_progress) in [
            (30000, vec![], 0),
            (20000, vec![[0x99; 16]], 0),
            (12000, vec![[0x55; 16], [0x99; 16]], 0),
            (5000, vec![[0x11; 16], [0x55; 16], [0x99; 16]], 1),
        ] {
            assert_eq!(
                check_disk_quota(&Usage, quota),
                (large, in_progress, 9000, 16000)
            );
        }
    }
}
