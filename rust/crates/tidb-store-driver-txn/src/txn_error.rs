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

//! Go `store/driver/txn/error.go`: `extractKeyErr` turns client-go's write
//! conflicts and retryable errors into TiDB's, decoding the table keys in
//! them (`newWriteConflictError`, `prettyWriteKey`,
//! `prettyLockNotFoundKey`), and hands every other storage error to
//! `ToTiDBErr` (`tidb_store_driver_error::driver_error::to_tidb_driver_error`).

use std::fmt;

use tidb_tablecodec::table_key::{decode_index_key, decode_meta_key, decode_record_key, hex};
use tidb_txnkv::driver::transaction_error::{RedactArg, Redaction, WriteConflict, WriteConflictArgs};
use tidb_txnkv::{StorageDriverError, TXN_RETRYABLE_MARK};
use tidb_store_driver_error::driver_error::ConvertedDriverError;

/// Retryable transaction failure exposed by the TiDB KV driver boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionError {
    /// A write conflict, optionally carrying structured TiKV diagnostics.
    WriteConflict {
        /// Conflict fields when TiKV supplied the detailed error payload.
        conflict: Option<WriteConflict>,
        /// Redaction policy used when rendering key diagnostics.
        redaction: Redaction,
    },
    /// A retry-safe KV message, including lock-not-found diagnostics when present.
    Retryable(String),
}

impl fmt::Display for TransactionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WriteConflict { conflict: None, .. } => formatter.write_str("Write conflict"),
            Self::WriteConflict {
                conflict: Some(conflict),
                redaction,
            } => formatter.write_str(&format_write_conflict(conflict, *redaction)),
            Self::Retryable(message) => write!(
                formatter,
                "[kv:8022]Error: KV error safe to retry {message} {} {TXN_RETRYABLE_MARK}",
                pretty_lock_not_found_key(message)
            ),
        }
    }
}

impl std::error::Error for TransactionError {}

/// Formats an encoded write key as source-shaped table/index/record/meta data.
///
/// The first string is the table prefix and the second string completes the
/// diagnostic, matching the pieces consumed by write-conflict formatting.
#[must_use]
pub fn pretty_write_key(key: &[u8]) -> (String, String) {
    if let Ok((table_id, index_id, values)) = decode_index_key(key) {
        let mut rest = format!(", indexID={index_id}, indexValues={{");
        for value in values {
            rest.push_str(&value);
            rest.push_str(", ");
        }
        rest.push_str("}}");
        return (format!("{{tableID={table_id}"), rest);
    }
    if let Ok((table_id, handle)) = decode_record_key(key) {
        return (
            format!("{{tableID={table_id}"),
            format!(", handle={handle}}}"),
        );
    }
    if let Ok((meta_key, field)) = decode_meta_key(key) {
        return (
            String::new(),
            format!(
                "{{metaKey=true, key={}, field={}}}",
                diagnostic_bytes(&meta_key),
                diagnostic_bytes(&field)
            ),
        );
    }
    let values = key
        .iter()
        .map(|byte| format!("0x{byte:x}"))
        .collect::<Vec<_>>()
        .join(", ");
    (String::new(), format!("[]byte{{{values}}}"))
}

/// Go `prettyWriteKey` over a `kvrpcpb.WriteConflict` key field. An empty
/// field is Go's nil slice -- proto3 never encodes an empty `bytes` field, and
/// client-go's early prewrite conflict (`NewErrWriteConflictWithArgs`) leaves
/// `Primary` unset -- so its `%#v` fallback prints `[]byte(nil)`, not the
/// `[]byte{}` of a decoded empty slice.
fn pretty_write_conflict_key(key: &[u8]) -> (String, String) {
    if key.is_empty() {
        return (String::new(), "[]byte(nil)".to_owned());
    }
    pretty_write_key(key)
}

/// Extracts and formats the byte-array key embedded in `TxnLockNotFound` text.
///
/// Nonmatching or malformed retry messages return an empty diagnostic.
#[must_use]
pub fn pretty_lock_not_found_key(raw_retry: &str) -> String {
    if !raw_retry.contains("TxnLockNotFound") {
        return String::new();
    }
    let Some(start) = raw_retry.find('[') else {
        return String::new();
    };
    let Some(end) = raw_retry[start..].find(']') else {
        return String::new();
    };
    let mut key = Vec::new();
    let encoded = raw_retry[start + 1..start + end].trim();
    if !encoded.is_empty() {
        for value in encoded.split(',') {
            let Ok(value) = value.trim().parse::<u8>() else {
                return String::new();
            };
            key.push(value);
        }
    }
    let (table, rest) = pretty_write_key(&key);
    table + &rest
}

fn diagnostic_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(value) => value.to_owned(),
        Err(_) => format!("0x{}", hex(bytes)),
    }
}

fn format_write_conflict(conflict: &WriteConflict, redaction: Redaction) -> String {
    format!(
        "[kv:9007]Write conflict, {} {TXN_RETRYABLE_MARK}",
        write_conflict_detail(conflict, redaction)
    )
}

/// Go `newWriteConflictError`'s `ErrWriteConflict` arguments rendered into
/// its message, between `Write conflict, ` and the retryable mark: the
/// timestamps, `prettyWriteKey` of the conflicting and primary keys with
/// their hex forms (the redacted argument positions), and the reason.
#[must_use]
pub fn write_conflict_detail(conflict: &WriteConflict, redaction: Redaction) -> String {
    new_write_conflict_args(conflict, redaction).detail()
}

/// Go `newWriteConflictError(conflict)`'s arguments under `redaction`.
#[must_use]
pub fn new_write_conflict_args(conflict: &WriteConflict, redaction: Redaction) -> WriteConflictArgs {
    let (key_table_id, key_rest) = pretty_write_conflict_key(&conflict.key);
    let (primary_table_id, primary_rest) = pretty_write_conflict_key(&conflict.primary);
    let key_rest = format!("{key_rest}, originalKey={}, primary=", hex(&conflict.key));
    let primary_rest = format!(
        "{primary_rest}, originalPrimaryKey={}",
        hex(&conflict.primary)
    );
    WriteConflictArgs {
        start_ts: conflict.start_ts,
        conflict_ts: conflict.conflict_ts,
        conflict_commit_ts: conflict.conflict_commit_ts,
        key_table_id: RedactArg::Str(key_table_id).redact(redaction),
        key_rest: RedactArg::Str(key_rest).redact(redaction),
        primary_table_id: RedactArg::Str(primary_table_id).redact(redaction),
        primary_rest: RedactArg::Str(primary_rest).redact(redaction),
        reason: conflict.reason.clone(),
    }
}

/// Go `extractKeyErr`'s answer: a transaction error with its keys decoded,
/// or what `ToTiDBErr` makes of anything else.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtractedKeyError {
    /// A write conflict or retryable error, keys decoded.
    Transaction(TransactionError),
    /// Every other storage error, through `ToTiDBErr`.
    Driver(ConvertedDriverError),
}

impl fmt::Display for ExtractedKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transaction(error) => error.fmt(formatter),
            Self::Driver(error) => error.fmt(formatter),
        }
    }
}

/// Go `extractKeyErr`.
#[must_use]
pub fn extract_key_err(error: &StorageDriverError) -> ExtractedKeyError {
    match error {
        StorageDriverError::WriteConflict {
            conflict: Some(conflict),
            redaction,
        } => ExtractedKeyError::Transaction(TransactionError::WriteConflict {
            conflict: Some(conflict.clone()),
            redaction: *redaction,
        }),
        StorageDriverError::Retryable { message } => {
            ExtractedKeyError::Transaction(TransactionError::Retryable(message.clone()))
        }
        other => ExtractedKeyError::Driver(tidb_store_driver_error::driver_error::to_tidb_driver_error(other)),
    }
}
