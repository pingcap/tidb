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

//! Go `br/pkg/task/restore.go`: SQL builder settings, not the import runner.
use super::common::Config;
/// Go `RestoreConfig` and its embedded `RestoreCommonConfig` SQL fields.
pub struct RestoreConfig {
    /// Go embedded `Config`.
    pub config: Config,
    /// Go `Online`.
    pub online: bool,
    /// Go `WaitTiflashReady`.
    pub wait_tiflash_ready: bool,
    /// Go `WithSysTable`.
    pub with_sys_table: bool,
    /// Go `LoadStats`.
    pub load_stats: bool,
}
/// Go `DefaultRestoreConfig`'s SQL builder settings.
pub fn default_restore_config(config: Config) -> RestoreConfig {
    RestoreConfig {
        config,
        online: false,
        wait_tiflash_ready: false,
        with_sys_table: true,
        load_stats: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `common_test.go:TestDefaultRestore`, SQL builder fields.
    #[test]
    fn test_default_restore() {
        let cfg = default_restore_config(super::super::common::default_config());
        assert!(!cfg.online);
        assert!(!cfg.wait_tiflash_ready);
        assert!(cfg.with_sys_table);
        assert!(cfg.load_stats);
        assert_eq!(cfg.config.concurrency, 0);
    }
}
