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

//! Go `br/pkg/utils/error_handling.go`: how BR treats an error a TiKV
//! backup response or a storage call reports.

use std::collections::HashMap;
use std::sync::Mutex;

use tidb_log::{Field, Value};
use tidb_proto::backup;

/// Go `retryableErrorMsg`.
const RETRYABLE_ERROR_MSG: &[&str] = &[
    "server closed",
    "connection refused",
    "connection reset by peer",
    "channel closed",
    "error trying to connect",
    "connection closed before message completed",
    "body write aborted",
    "error during dispatch",
    "put object timeout",
    "timeout after",
    "internalerror",
    "not read from or written to within the timeout period",
    "<code>requesttimeout</code>",
    "<code>invalidpart</code>",
    "end of file before message length reached",
];

const IO_MSG: &str = "io";
const NOT_FOUND_MSG: &str = "notfound";
const PERMISSION_DENIED_MSG: &str = "permissiondenied";
const CREDENTIAL_NOT_FOUND_MSG: &str = "credential info not found";

const UNREACHABLE_RETRY_MSG: &str = "unreachable retry";
const RETRY_ON_KV_ERROR_MSG: &str = "retry on kv error";
const RETRY_ON_REGION_ERROR_MSG: &str = "retry on region error";
const CLUSTER_ID_MISMATCH_MSG: &str = "cluster id mismatch";
const UNKNOWN_ERROR_MSG: &str = "unknown error";
/// Go `contextCancelledMsg`.
pub const CONTEXT_CANCELLED_MSG: &str = "context canceled";
const RETRY_ON_UNKNOWN_ERROR_MSG: &str = "unknown error, retry it for a few times";
const NO_RETRY_ON_UNKNOWN_ERROR_MSG: &str = "unknown error, retried too many times, give up";
const RETRYABLE_STORAGE_ERROR_MSG: &str = "retryable storage error";

/// Go `ErrorHandlingStrategy`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorHandlingStrategy {
    /// Go `StrategyRetry`.
    Retry,
    /// Go `StrategyGiveUp`.
    GiveUp,
    /// Go `StrategyUnknown`.
    Unknown,
}

/// Go `ErrorHandlingResult`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorHandlingResult {
    /// Go `Strategy`.
    pub strategy: ErrorHandlingStrategy,
    /// Go `Reason`.
    pub reason: String,
}

fn result(strategy: ErrorHandlingStrategy, reason: impl Into<String>) -> ErrorHandlingResult {
    ErrorHandlingResult { strategy, reason: reason.into() }
}

/// Go `ErrorContext`: how often each store hit an unknown error, against
/// how often one is retried.
#[derive(Debug)]
pub struct ErrorContext {
    encounter_times: Mutex<HashMap<u64, i32>>,
    encounter_times_limitation: i32,
    description: String,
}

impl ErrorContext {
    /// Go `NewErrorContext`.
    #[must_use]
    pub fn new(scenario: &str, limitation: i32) -> Self {
        Self { encounter_times: Mutex::new(HashMap::new()), encounter_times_limitation: limitation, description: scenario.into() }
    }

    /// Go `NewDefaultContext`.
    #[must_use]
    pub fn new_default() -> Self {
        Self::new("default", 1)
    }

    /// Go `NewZeroRetryContext`.
    #[must_use]
    pub fn new_zero_retry(scenario: &str) -> Self {
        Self::new(scenario, 0)
    }
}

/// Go `HandleBackupError`.
#[must_use]
pub fn handle_backup_error(err: Option<&backup::Error>, store_id: u64, ec: &ErrorContext) -> ErrorHandlingResult {
    let Some(err) = err else { return result(ErrorHandlingStrategy::Retry, UNREACHABLE_RETRY_MSG) };
    let res = handle_backup_proto_error(err);
    if res.strategy == ErrorHandlingStrategy::Unknown && !err.msg.is_empty() {
        return handle_unknown_backup_error(&err.msg, store_id, ec);
    }
    res
}

/// Go `handleBackupProtoError`.
fn handle_backup_proto_error(e: &backup::Error) -> ErrorHandlingResult {
    match &e.detail {
        Some(backup::error::Detail::KvError(_)) => result(ErrorHandlingStrategy::Retry, RETRY_ON_KV_ERROR_MSG),
        Some(backup::error::Detail::RegionError(_)) => result(ErrorHandlingStrategy::Retry, RETRY_ON_REGION_ERROR_MSG),
        Some(backup::error::Detail::ClusterIdError(_)) => result(ErrorHandlingStrategy::GiveUp, CLUSTER_ID_MISMATCH_MSG),
        None => result(ErrorHandlingStrategy::Unknown, UNKNOWN_ERROR_MSG),
    }
}

/// Go `HandleUnknownBackupError`.
#[must_use]
pub fn handle_unknown_backup_error(msg: &str, uuid: u64, ec: &ErrorContext) -> ErrorHandlingResult {
    if message_is_not_found_storage_error(msg) {
        return result(
            ErrorHandlingStrategy::GiveUp,
            format!(
                "File or directory not found on TiKV Node (store id: {uuid}). workaround: please ensure br and tikv nodes share a same storage and the user of br and tikv has same uid."
            ),
        );
    }
    if message_is_permission_denied_storage_error(msg) {
        return result(
            ErrorHandlingStrategy::GiveUp,
            format!(
                "I/O permission denied error occurs on TiKV Node(store id: {uuid}). workaround: please ensure tikv has permission to read from & write to the storage."
            ),
        );
    }
    if message_is_credential_not_found_error(msg) {
        return result(
            ErrorHandlingStrategy::GiveUp,
            format!(
                "Credential info not found on TiKV Node (store id: {uuid}). workaround: please ensure the credential/access key is correctly configured for the storage."
            ),
        );
    }
    if msg.to_lowercase().contains(CONTEXT_CANCELLED_MSG) {
        return result(ErrorHandlingStrategy::GiveUp, CONTEXT_CANCELLED_MSG);
    }
    if message_is_retryable_storage_error(msg) {
        tidb_log::warn(
            RETRYABLE_STORAGE_ERROR_MSG,
            &[Field::new("description", Value::Str(ec.description.clone())), Field::new("error", Value::Str(msg.into()))],
        );
        return result(ErrorHandlingStrategy::Retry, RETRYABLE_STORAGE_ERROR_MSG);
    }
    let mut times = ec.encounter_times.lock().unwrap();
    let count = times.entry(uuid).or_insert(0);
    *count += 1;
    if *count <= ec.encounter_times_limitation {
        return result(ErrorHandlingStrategy::Retry, RETRY_ON_UNKNOWN_ERROR_MSG);
    }
    result(ErrorHandlingStrategy::GiveUp, NO_RETRY_ON_UNKNOWN_ERROR_MSG)
}

fn message_is_not_found_storage_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    lower.contains(IO_MSG) && lower.contains(NOT_FOUND_MSG)
}

fn message_is_permission_denied_storage_error(msg: &str) -> bool {
    msg.to_lowercase().contains(PERMISSION_DENIED_MSG)
}

fn message_is_credential_not_found_error(msg: &str) -> bool {
    msg.to_lowercase().contains(CREDENTIAL_NOT_FOUND_MSG)
}

/// Go `MessageIsRetryableStorageError`.
#[must_use]
pub fn message_is_retryable_storage_error(msg: &str) -> bool {
    let lower = msg.to_lowercase();
    RETRYABLE_ERROR_MSG.iter().any(|e| lower.contains(e))
}
