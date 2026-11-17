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


//! Go `streamhelper/models.go`: the etcd keys of log backup tasks.

/// Go `streamKeyPrefix`.
const STREAM_KEY_PREFIX: &str = "/tidb/br-stream";
/// Go `taskInfoPath`.
const TASK_INFO_PATH: &str = "/info";
/// Go `taskCheckpointPath`.
const TASK_CHECKPOINT_PATH: &str = "/checkpoint";
/// Go `storageCheckPoint`.
const STORAGE_CHECKPOINT: &str = "/storage-checkpoint";
/// Go `taskRangesPath`.
const TASK_RANGES_PATH: &str = "/ranges";
/// Go `taskPausePath`.
const TASK_PAUSE_PATH: &str = "/pause";
/// Go `taskLastErrorPath`.
const TASK_LAST_ERROR_PATH: &str = "/last-error";
/// Go `checkpointTypeGlobal`.
pub const CHECKPOINT_TYPE_GLOBAL: &str = "central_global";

/// Go `PrefixOfTask`.
#[must_use]
pub fn prefix_of_task() -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_INFO_PATH}/")
}

/// Go `TaskOf`.
#[must_use]
pub fn task_of(name: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_INFO_PATH}/{name}")
}

/// Go `RangesOf`.
#[must_use]
pub fn ranges_of(name: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_RANGES_PATH}/{name}/")
}

/// Go `RangeKeyOf`: the start key appended raw (Go avoids `path.Join`, which
/// would fold `//` in it).
#[must_use]
pub fn range_key_of(name: &str, start_key: &[u8]) -> Vec<u8> {
    [ranges_of(name).as_bytes(), start_key].concat()
}

/// Go `encodeUint64`: big-endian.
#[must_use]
pub fn encode_uint64(num: u64) -> [u8; 8] {
    num.to_be_bytes()
}

/// Go `CheckPointsOf`.
#[must_use]
pub fn check_points_of(task: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_CHECKPOINT_PATH}/{task}/")
}

/// Go `GlobalCheckpointOf`.
#[must_use]
pub fn global_checkpoint_of(task: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_CHECKPOINT_PATH}/{task}/{CHECKPOINT_TYPE_GLOBAL}")
}

/// Go `StorageCheckpointOf`.
#[must_use]
pub fn storage_checkpoint_of(task: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{STORAGE_CHECKPOINT}/{task}")
}

/// Go `Pause`.
#[must_use]
pub fn pause(task: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_PAUSE_PATH}/{task}")
}

/// Go `PrefixOfPause`.
#[must_use]
pub fn prefix_of_pause() -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_PAUSE_PATH}/")
}

/// Go `LastErrorPrefixOf`.
#[must_use]
pub fn last_error_prefix_of(task: &str) -> String {
    format!("{STREAM_KEY_PREFIX}{TASK_LAST_ERROR_PATH}/{task}/")
}

/// Go `Range = kv.KeyRange`.
pub type Range = crate::spans::sorted::Span;
