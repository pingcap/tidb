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

//! Go `br/pkg/task/show/cmd.go`: inspect a backup without restoring it.

use tidb_gocontext::Context;
use tidb_proto::backup::CipherInfo;

/// Go `show.Config`, local backend (remote BackendCfg is not implemented).
pub struct Config {
    /// Go `Config.Storage`.
    pub storage: String,
    /// Go `Config.Cipher`.
    pub cipher: CipherInfo,
}

/// Go `show.TimeStamp`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TimeStamp(pub u64);

impl std::fmt::Display for TimeStamp {
    /// Go `TimeStamp.String`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let time = chrono::DateTime::from_timestamp_millis((self.0 >> 18) as i64)
            .unwrap()
            .with_timezone(&chrono::Local);
        write!(f, "{}({})", self.0, time.format("Y%yM%mD%d,%H:%I:%M"))
    }
}

/// Go `show.RawRange`.
#[derive(Debug, PartialEq, Eq)]
pub struct RawRange {
    /// Go `RawRange.ColumnFamily`.
    pub column_family: String,
    /// Go `RawRange.StartKey`.
    pub start_key: Vec<u8>,
    /// Go `RawRange.EndKey`.
    pub end_key: Vec<u8>,
}

/// Go `show.Table`.
#[derive(Debug, PartialEq, Eq)]
pub struct Table {
    /// Go `Table.DBName`.
    pub db_name: String,
    /// Go `Table.TableName`.
    pub table_name: String,
    /// Go `Table.KVCount`.
    pub kv_count: u64,
    /// Go `Table.KVSize`.
    pub kv_size: u64,
    /// Go `Table.TiFlashReplica`.
    pub tiflash_replica: u32,
}

/// Go `show.ShowResult`.
#[derive(Debug)]
pub struct ShowResult {
    /// Go `ClusterID`.
    pub cluster_id: u64,
    /// Go `ClusterVersion`.
    pub cluster_version: String,
    /// Go `BRVersion`.
    pub br_version: String,
    /// Go `Version`.
    pub version: i32,
    /// Go `StartVersion`.
    pub start_version: TimeStamp,
    /// Go `EndVersion`.
    pub end_version: TimeStamp,
    /// Go `IsRawKV`.
    pub is_raw_kv: bool,
    /// Go `RawRanges`.
    pub raw_ranges: Vec<RawRange>,
    /// Go `Tables`.
    pub tables: Vec<Table>,
}

/// Go `show.CmdExecutor`.
pub struct CmdExecutor {
    meta: crate::metautil::MetaReader,
}

/// Go `show.CreateExec` / `Config.lameTaskConfig`.
pub fn create_exec(ctx: &Context, cfg: Config) -> Result<CmdExecutor, String> {
    if let Some(err) = ctx.err() {
        return Err(err.to_string());
    }
    let meta = super::read_backup_meta(&cfg.storage, &cfg.cipher)
        .map_err(|e| format!("failed to create execution: {e}"))?;
    Ok(CmdExecutor { meta })
}

impl CmdExecutor {
    /// Go `CmdExecutor.Read`.
    pub fn read(&self, ctx: &Context) -> Result<ShowResult, String> {
        let basic = self.meta.get_basic();
        let mut result = convert_basic(basic);
        if result.end_version < result.start_version {
            return Err(format!("[BR:Common:ErrInvalidMetaFile]invalid metafile: the start version({}) is greater than the end version({}), perhaps reading a backup meta from log backup", result.start_version, result.end_version));
        }
        if !result.is_raw_kv {
            result.tables = self
                .meta
                .read_schemas_files(ctx)?
                .into_iter()
                .map(convert_table)
                .collect();
        } else {
            if basic.raw_range_index.is_some() {
                return Err("[BR:Common:ErrInvalidMetaFile]invalid metafile: show raw kv with backup meta v2 isn't supported for now".into());
            }
            result.raw_ranges = basic.raw_ranges.iter().map(convert_raw_range).collect();
        }
        Ok(result)
    }
}

/// Go `convertBasic`.
fn convert_basic(basic: &tidb_proto::backup::BackupMeta) -> ShowResult {
    ShowResult {
        cluster_id: basic.cluster_id,
        cluster_version: basic.cluster_version.clone(),
        br_version: basic.br_version.clone(),
        version: basic.version,
        start_version: TimeStamp(basic.start_version),
        end_version: TimeStamp(basic.end_version),
        is_raw_kv: basic.is_raw_kv,
        raw_ranges: Vec::new(),
        tables: Vec::new(),
    }
}

/// Go `convertTable`.
fn convert_table(table: crate::metautil::Table) -> Table {
    Table {
        db_name: table.db.name.original().to_owned(),
        table_name: table
            .info
            .as_ref()
            .map_or_else(String::new, |t| t.name.original().to_owned()),
        kv_count: table.total_kvs,
        kv_size: table.total_bytes,
        tiflash_replica: table.tiflash_replicas,
    }
}

/// Go `convertRawRange`.
fn convert_raw_range(range: &tidb_proto::backup::RawRange) -> RawRange {
    RawRange {
        column_family: range.cf.clone(),
        start_key: range.start_key.clone(),
        end_key: range.end_key.clone(),
    }
}

#[cfg(test)]
#[path = "show_tests.rs"]
mod tests;
