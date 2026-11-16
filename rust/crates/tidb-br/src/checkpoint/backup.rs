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

//! Go `br/pkg/checkpoint/backup.go`: a backup's checkpoint of finished
//! ranges under `checkpoints/backup` in the backup storage.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_proto::backup::{CipherInfo, File};

use super::{
    gojson, CheckpointMessage, CheckpointRunner, ChecksumItem, ExternalCheckpointStorage,
    FlushPath, GlobalTimer, RangeGroup,
};
use crate::errors::{self, Result};
use crate::metautil::Storage;

/// Go `CheckpointBackupDir`.
pub const CHECKPOINT_BACKUP_DIR: &str = "checkpoints/backup";
/// Go `CheckpointDataDirForBackup`.
pub const CHECKPOINT_DATA_DIR_FOR_BACKUP: &str = "checkpoints/backup/data";
/// Go `CheckpointChecksumDirForBackup`.
pub const CHECKPOINT_CHECKSUM_DIR_FOR_BACKUP: &str = "checkpoints/backup/checksum";
/// Go `CheckpointMetaPathForBackup`.
pub const CHECKPOINT_META_PATH_FOR_BACKUP: &str = "checkpoints/backup/checkpoint.meta";
/// Go `CheckpointLockPathForBackup`.
pub const CHECKPOINT_LOCK_PATH_FOR_BACKUP: &str = "checkpoints/backup/checkpoint.lock";

/// Go `BackupKeyType`.
pub type BackupKeyType = String;

/// Go `BackupValueType` (`RangeType`, an `*rtree.Range`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BackupValueType {
    /// Go `StartKey`.
    pub start_key: Vec<u8>,
    /// Go `EndKey`.
    pub end_key: Vec<u8>,
    /// Go `Files`.
    pub files: Vec<File>,
}

/// The backup checkpoint runner.
pub type BackupRunner = CheckpointRunner<BackupKeyType, BackupValueType>;

/// Go `flushPathForBackup`.
fn flush_path_for_backup() -> FlushPath {
    FlushPath {
        checkpoint_data_dir: CHECKPOINT_DATA_DIR_FOR_BACKUP,
        checkpoint_checksum_dir: CHECKPOINT_CHECKSUM_DIR_FOR_BACKUP,
        checkpoint_lock_path: CHECKPOINT_LOCK_PATH_FOR_BACKUP,
    }
}

/// Go `valueMarshalerForBackup`: `json.Marshal(group)`, the group key
/// omitted when empty (`omitempty`).
pub(crate) fn value_marshaler_for_backup(
    group: &RangeGroup<BackupKeyType, BackupValueType>,
) -> Result<Vec<u8>> {
    let groups: Vec<String> = group
        .group
        .iter()
        .map(|v| gojson::range(&v.start_key, &v.end_key, &v.files))
        .collect();
    let mut out = String::from("{");
    if !group.group_key.is_empty() {
        out.push_str(&format!(
            "\"group-key\":{},",
            gojson::string(&group.group_key)
        ));
    }
    out.push_str(&format!("\"groups\":[{}]}}", groups.join(",")));
    Ok(out.into_bytes())
}

/// The decode of [`value_marshaler_for_backup`]'s JSON.
fn parse_backup_group(data: &[u8]) -> Result<RangeGroup<BackupKeyType, BackupValueType>> {
    let v: serde_json::Value =
        serde_json::from_slice(data).map_err(|e| errors::errorf(e.to_string()))?;
    let mut group = Vec::new();
    for g in v["groups"].as_array().cloned().unwrap_or_default() {
        let (start_key, end_key, files) = gojson::parse_range(&g)?;
        group.push(BackupValueType {
            start_key,
            end_key,
            files,
        });
    }
    Ok(RangeGroup {
        group_key: v["group-key"].as_str().unwrap_or_default().to_owned(),
        group,
    })
}

/// Go `StartCheckpointRunnerForBackup`.
///
/// # Errors
/// The checkpoint lock cannot be taken.
pub fn start_checkpoint_runner_for_backup(
    ctx: &Context,
    storage: Storage,
    cipher: Option<CipherInfo>,
    timer: Arc<dyn GlobalTimer>,
) -> Result<BackupRunner> {
    start_with_ticks(ctx, storage, cipher, Some(timer), super::DEFAULT_TICKS)
}

/// Go `StartCheckpointBackupRunnerForTest`: every tick is `tick`.
///
/// # Errors
/// The checkpoint lock cannot be taken.
pub fn start_checkpoint_backup_runner_for_test(
    ctx: &Context,
    storage: Storage,
    cipher: Option<CipherInfo>,
    tick: Duration,
    timer: Option<Arc<dyn GlobalTimer>>,
) -> Result<BackupRunner> {
    start_with_ticks(ctx, storage, cipher, timer, [tick; 4])
}

fn start_with_ticks(
    ctx: &Context,
    storage: Storage,
    cipher: Option<CipherInfo>,
    timer: Option<Arc<dyn GlobalTimer>>,
    ticks: [Duration; 4],
) -> Result<BackupRunner> {
    let cs = ExternalCheckpointStorage::new(ctx, storage, timer, flush_path_for_backup())?;
    Ok(CheckpointRunner::start(
        ctx,
        Arc::new(cs),
        cipher,
        Arc::new(value_marshaler_for_backup),
        ticks,
    ))
}

/// Go `AppendForBackup`.
///
/// # Errors
/// The runner's error.
pub fn append_for_backup(
    ctx: &Context,
    r: &BackupRunner,
    start_key: &[u8],
    end_key: &[u8],
    files: &[File],
) -> Result<()> {
    r.append(
        ctx,
        CheckpointMessage {
            group_key: String::new(),
            group: vec![BackupValueType {
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                files: files.to_vec(),
            }],
        },
    )
}

/// Go `WalkCheckpointFileForBackup`.
///
/// # Errors
/// A storage, decrypt or `f` error.
pub fn walk_checkpoint_file_for_backup(
    ctx: &Context,
    s: &Storage,
    cipher: Option<&CipherInfo>,
    f: &mut dyn FnMut(&BackupKeyType, BackupValueType) -> Result<()>,
) -> Result<Duration> {
    super::walk_checkpoint_file(
        ctx,
        s,
        cipher,
        CHECKPOINT_DATA_DIR_FOR_BACKUP,
        &parse_backup_group,
        f,
    )
}

/// Go `CheckpointMetadataForBackup`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct CheckpointMetadataForBackup {
    /// Go `GCServiceId`.
    pub gc_service_id: String,
    /// Go `ConfigHash`.
    pub config_hash: Vec<u8>,
    /// Go `BackupTS`.
    pub backup_ts: u64,
    /// Go `BackupID`.
    pub backup_id: u64,
    /// Go `CheckpointChecksum` (not written).
    pub checkpoint_checksum: HashMap<i64, ChecksumItem>,
    /// Go `LoadCheckpointDataMap` (not written).
    pub load_checkpoint_data_map: bool,
}

/// Go `LoadCheckpointMetadata`.
///
/// # Errors
/// A storage or decode error.
pub fn load_checkpoint_metadata(ctx: &Context, s: &Storage) -> Result<CheckpointMetadataForBackup> {
    let data = s
        .read_file(ctx, CHECKPOINT_META_PATH_FOR_BACKUP)
        .map_err(|e| errors::errorf(e.to_string()))?;
    let v: serde_json::Value =
        serde_json::from_slice(&data).map_err(|e| errors::errorf(e.to_string()))?;
    let (checksum, _) =
        super::load_checkpoint_checksum(ctx, s, CHECKPOINT_CHECKSUM_DIR_FOR_BACKUP)?;
    Ok(CheckpointMetadataForBackup {
        gc_service_id: v["gc-service-id"].as_str().unwrap_or_default().to_owned(),
        config_hash: gojson::parse_bytes(&v["config-hash"])?,
        backup_ts: v["backup-ts"].as_u64().unwrap_or(0),
        backup_id: v["backup-id"].as_u64().unwrap_or(0),
        checkpoint_checksum: checksum,
        load_checkpoint_data_map: false,
    })
}

/// Go `SaveCheckpointMetadata`: `backup-id` is `omitempty`.
///
/// # Errors
/// The write error.
pub fn save_checkpoint_metadata(
    ctx: &Context,
    s: &Storage,
    meta: &CheckpointMetadataForBackup,
) -> Result<()> {
    let mut data = format!(
        r#"{{"gc-service-id":{},"config-hash":{},"backup-ts":{}"#,
        gojson::string(&meta.gc_service_id),
        if meta.config_hash.is_empty() {
            "null".to_owned()
        } else {
            gojson::bytes(&meta.config_hash)
        },
        meta.backup_ts
    );
    if meta.backup_id != 0 {
        data.push_str(&format!(r#","backup-id":{}"#, meta.backup_id));
    }
    data.push('}');
    s.write_file(ctx, CHECKPOINT_META_PATH_FOR_BACKUP, data.as_bytes())
        .map_err(|e| errors::errorf(e.to_string()))
}

/// Go `RemoveCheckpointDataForBackup`.
///
/// # Errors
/// A storage error.
pub fn remove_checkpoint_data_for_backup(ctx: &Context, s: &Storage) -> Result<()> {
    super::remove_checkpoint_data(ctx, s, CHECKPOINT_BACKUP_DIR)
}
