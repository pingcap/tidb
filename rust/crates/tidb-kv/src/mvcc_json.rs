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

//! Go's `encoding/json` of kvrpcpb's MVCC records, as the store helper's
//! `GetMvccByEncodedKey` answer reaches the status port's `/mvcc/*` routes
//! and `TIDB_MVCC_INFO`.

use base64::Engine;
use serde::Serialize;
use tikv_client::proto::kvrpcpb;

fn is_zero(value: &u64) -> bool {
    *value == 0
}

fn is_zero_i32(value: &i32) -> bool {
    *value == 0
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn base64_bytes(bytes: &[u8]) -> Option<String> {
    (!bytes.is_empty()).then(|| base64::engine::general_purpose::STANDARD.encode(bytes))
}

/// Go `kvrpcpb.MvccLock` through `encoding/json`.
#[derive(Serialize)]
struct MvccLockJson {
    #[serde(rename = "type", skip_serializing_if = "is_zero_i32")]
    kind: i32,
    #[serde(skip_serializing_if = "is_zero")]
    start_ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    primary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    short_value: Option<String>,
    #[serde(skip_serializing_if = "is_zero")]
    ttl: u64,
    #[serde(skip_serializing_if = "is_zero")]
    for_update_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    txn_size: u64,
    #[serde(skip_serializing_if = "is_false")]
    use_async_commit: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    secondaries: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    rollback_ts: Vec<u64>,
    #[serde(skip_serializing_if = "is_zero")]
    last_change_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    versions_to_last_change: u64,
}

/// Go `kvrpcpb.MvccWrite` through `encoding/json`.
#[derive(Serialize)]
struct MvccWriteJson {
    #[serde(rename = "type", skip_serializing_if = "is_zero_i32")]
    kind: i32,
    #[serde(skip_serializing_if = "is_zero")]
    start_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    commit_ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    short_value: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    has_overlapped_rollback: bool,
    #[serde(skip_serializing_if = "is_false")]
    has_gc_fence: bool,
    #[serde(skip_serializing_if = "is_zero")]
    gc_fence: u64,
    #[serde(skip_serializing_if = "is_zero")]
    last_change_ts: u64,
    #[serde(skip_serializing_if = "is_zero")]
    versions_to_last_change: u64,
}

/// Go `kvrpcpb.MvccValue` through `encoding/json`.
#[derive(Serialize)]
struct MvccValueJson {
    #[serde(skip_serializing_if = "is_zero")]
    start_ts: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
}

/// Go `kvrpcpb.MvccInfo` through `encoding/json`.
#[derive(Serialize)]
pub struct MvccInfoJson {
    #[serde(skip_serializing_if = "Option::is_none")]
    lock: Option<MvccLockJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    writes: Vec<MvccWriteJson>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    values: Vec<MvccValueJson>,
}

/// Go `kvrpcpb.MvccGetByKeyResponse` through `encoding/json`. The region
/// error is always nil here: the helper retries a region error.
#[derive(Serialize)]
pub struct MvccGetByKeyResponseJson {
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    /// Go `MvccGetByKeyResponse.Info`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<MvccInfoJson>,
}

/// Go `encoding/json` of a `kvrpcpb.MvccGetByKeyResponse`.
pub fn response_json(response: &kvrpcpb::MvccGetByKeyResponse) -> MvccGetByKeyResponseJson {
    MvccGetByKeyResponseJson {
        error: response.error.clone(),
        info: response.info.as_ref().map(|info| MvccInfoJson {
            lock: info.lock.as_ref().map(|lock| MvccLockJson {
                kind: lock.r#type,
                start_ts: lock.start_ts,
                primary: base64_bytes(&lock.primary),
                short_value: base64_bytes(&lock.short_value),
                ttl: lock.ttl,
                for_update_ts: lock.for_update_ts,
                txn_size: lock.txn_size,
                use_async_commit: lock.use_async_commit,
                secondaries: lock
                    .secondaries
                    .iter()
                    .map(|key| base64::engine::general_purpose::STANDARD.encode(key))
                    .collect(),
                rollback_ts: lock.rollback_ts.clone(),
                last_change_ts: lock.last_change_ts,
                versions_to_last_change: lock.versions_to_last_change,
            }),
            writes: info.writes.iter().map(write_json).collect(),
            values: info.values.iter().map(value_json).collect(),
        }),
    }
}

fn write_json(write: &kvrpcpb::MvccWrite) -> MvccWriteJson {
    MvccWriteJson {
        kind: write.r#type,
        start_ts: write.start_ts,
        commit_ts: write.commit_ts,
        short_value: base64_bytes(&write.short_value),
        has_overlapped_rollback: write.has_overlapped_rollback,
        has_gc_fence: write.has_gc_fence,
        gc_fence: write.gc_fence,
        last_change_ts: write.last_change_ts,
        versions_to_last_change: write.versions_to_last_change,
    }
}

fn value_json(value: &kvrpcpb::MvccValue) -> MvccValueJson {
    MvccValueJson { start_ts: value.start_ts, value: base64_bytes(&value.value) }
}

/// Go `json.Marshal` of a `kvrpcpb.MvccWrite`.
pub fn marshal_write(write: &kvrpcpb::MvccWrite) -> String {
    serde_json::to_string(&write_json(write)).expect("the MVCC write encodes")
}

/// Go `json.Marshal` of a `kvrpcpb.MvccValue`.
pub fn marshal_value(value: &kvrpcpb::MvccValue) -> String {
    serde_json::to_string(&value_json(value)).expect("the MVCC value encodes")
}

/// Go `json.Marshal` of a `kvrpcpb.MvccGetByKeyResponse`.
pub fn marshal(response: &kvrpcpb::MvccGetByKeyResponse) -> String {
    serde_json::to_string(&response_json(response)).expect("the MVCC record encodes")
}
