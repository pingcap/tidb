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

//! Go `br/pkg/config/kv.go`: the TiKV settings restore adapts to. The EBS
//! snapshot settings (`ebs.go`) are not reachable from SQL and are not
//! ported.

use serde::Deserialize;

/// Go `ConfigTerm`: a value and whether the user set it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConfigTerm<T> {
    /// Go `Value`.
    pub value: T,
    /// Go `Modified`.
    pub modified: bool,
}

/// Go `KVConfig`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KvConfig {
    /// Go `ImportGoroutines`.
    pub import_goroutines: ConfigTerm<u64>,
    /// Go `MergeRegionSize`.
    pub merge_region_size: ConfigTerm<u64>,
    /// Go `MergeRegionKeyCount`.
    pub merge_region_key_count: ConfigTerm<u64>,
}

/// Go `ParseImportThreadsFromConfig`: TiKV's `import.num-threads`.
///
/// # Errors
/// The JSON error.
pub fn parse_import_threads_from_config(resp: &[u8]) -> Result<u64, String> {
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Importer {
        #[serde(rename = "num-threads")]
        threads: u64,
    }
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Config {
        import: Importer,
    }
    let c: Config = serde_json::from_slice(resp).map_err(|e| e.to_string())?;
    Ok(c.import.threads)
}

/// Go `ParseMergeRegionSizeFromConfig`: TiKV's region split size (a
/// `units.RAMInBytes` size) and keys.
///
/// # Errors
/// The JSON or size error.
pub fn parse_merge_region_size_from_config(resp: &[u8]) -> Result<(u64, u64), String> {
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Coprocessor {
        #[serde(rename = "region-split-size")]
        region_split_size: String,
        #[serde(rename = "region-split-keys")]
        region_split_keys: u64,
    }
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Config {
        coprocessor: Coprocessor,
    }
    let c: Config = serde_json::from_slice(resp).map_err(|e| e.to_string())?;
    let rs = tidb_config_configtypes::ram_in_bytes(&c.coprocessor.region_split_size)?;
    Ok((rs as u64, c.coprocessor.region_split_keys))
}

/// Go `ParseLogBackupEnableFromConfig`: TiKV's `log-backup.enable`.
///
/// # Errors
/// The JSON error.
pub fn parse_log_backup_enable_from_config(resp: &[u8]) -> Result<bool, String> {
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct LogBackup {
        enable: bool,
    }
    #[derive(Deserialize, Default)]
    #[serde(default)]
    struct Config {
        #[serde(rename = "log-backup")]
        log_backup: LogBackup,
    }
    let c: Config = serde_json::from_slice(resp).map_err(|e| e.to_string())?;
    Ok(c.log_backup.enable)
}
