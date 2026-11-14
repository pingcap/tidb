// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `br/pkg/task/backup.go`: SQL builder settings, not the physical runner.
use super::common::Config;
use tidb_proto::backup::CompressionType;

/// Go `CompressionConfig`.
pub struct CompressionConfig {
    /// Go `CompressionType`.
    pub compression_type: CompressionType,
    /// Go `CompressionLevel`.
    pub compression_level: i32,
}
/// Go `BackupConfig`, fields reached by `buildBRIE`.
pub struct BackupConfig {
    /// Go embedded `Config`.
    pub config: Config,
    /// Go `LastBackupTS`.
    pub last_backup_ts: u64,
    /// Go `BackupTS`.
    pub backup_ts: u64,
    /// Go `TimeAgo`, nanoseconds (Go time.Duration).
    pub time_ago: i64,
    /// Go `IgnoreStats`.
    pub ignore_stats: bool,
    /// Go embedded `CompressionConfig`.
    pub compression_config: CompressionConfig,
}
/// Go `DefaultBackupConfig`'s SQL builder settings.
pub fn default_backup_config(config: Config) -> BackupConfig {
    BackupConfig {
        config,
        last_backup_ts: 0,
        backup_ts: 0,
        time_ago: 0,
        ignore_stats: true,
        compression_config: CompressionConfig {
            compression_type: CompressionType::Zstd,
            compression_level: 0,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `common_test.go:TestDefaultBackup`, SQL builder fields.
    #[test]
    fn test_default_backup() {
        let cfg = default_backup_config(super::super::common::default_config());
        assert_eq!(cfg.backup_ts, 0);
        assert_eq!(cfg.last_backup_ts, 0);
        assert_eq!(cfg.time_ago, 0);
        assert!(cfg.ignore_stats);
        assert_eq!(cfg.compression_config.compression_type, CompressionType::Zstd);
        assert_eq!(cfg.compression_config.compression_level, 0);
        // DefaultBackupConfig replaces the common config after parsing flags.
        assert_eq!(cfg.config.concurrency, 0);
    }
}
