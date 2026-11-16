// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! The part of Go `pkg/ingestor/ingestctrl/checksum.go` IMPORT INTO reads:
//! the remote checksum and the knobs of its `ADMIN CHECKSUM TABLE`.

use tidb_lightning_verification::KvChecksum;

/// Go `MinDistSQLScanConcurrency`.
pub const MIN_DISTSQL_SCAN_CONCURRENCY: usize = 4;

/// Go `DefaultBackoffWeight`: `15 * tikvstore.DefBackOffWeight`.
pub const DEFAULT_BACKOFF_WEIGHT: i64 = 15 * 2;

/// Go `RemoteChecksum`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoteChecksum {
    /// Go `Schema`.
    pub schema: String,
    /// Go `Table`.
    pub table: String,
    /// Go `Checksum`.
    pub checksum: u64,
    /// Go `TotalKVs`.
    pub total_kvs: u64,
    /// Go `TotalBytes`.
    pub total_bytes: u64,
}

impl RemoteChecksum {
    /// Go `IsEqual`.
    #[must_use]
    pub fn is_equal(&self, other: &KvChecksum) -> bool {
        self.checksum == other.sum() && self.total_kvs == other.sum_kvs() && self.total_bytes == other.sum_size()
    }
}
