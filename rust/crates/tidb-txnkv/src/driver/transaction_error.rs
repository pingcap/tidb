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

//! Transaction error extraction and table-key diagnostics.

use std::fmt;

use tidb_codec::table_key::{decode_index_key, decode_meta_key, decode_record_key, hex};

use crate::TXN_RETRYABLE_MARK;

/// Diagnostic redaction policy applied to transaction key material.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Redaction {
    /// Emits decoded and hexadecimal key material without redaction markers.
    Disabled,
    /// Replaces all key material with the fixed `????` placeholder.
    Enabled,
    /// Wraps sensitive key material in TiDB redaction marker delimiters.
    Marker,
}

/// Structured fields returned by TiKV for one write conflict.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteConflict {
    /// Start timestamp of the transaction that encountered the conflict.
    pub start_ts: u64,
    /// Start timestamp of the conflicting transaction.
    pub conflict_ts: u64,
    /// Commit timestamp of the conflicting transaction.
    pub conflict_commit_ts: u64,
    /// Encoded table, record, index, metadata, or raw conflicting key.
    pub key: Vec<u8>,
    /// Encoded primary-lock key associated with the conflict.
    pub primary: Vec<u8>,
    /// TiKV-provided explanation of the conflict.
    pub reason: String,
}

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
    let prefix = format!(
        "[kv:9007]Write conflict, txnStartTS={}, conflictStartTS={}, conflictCommitTS={}, key=",
        conflict.start_ts, conflict.conflict_ts, conflict.conflict_commit_ts
    );
    if redaction == Redaction::Enabled {
        return format!(
            "{prefix}????, reason={} {TXN_RETRYABLE_MARK}",
            conflict.reason
        );
    }
    let (key_table, key_rest) = pretty_write_key(&conflict.key);
    let (primary_table, primary_rest) = pretty_write_key(&conflict.primary);
    let key_parts = format!(
        "{key_table}{key_rest}, originalKey={}, primary={primary_table}{primary_rest}, originalPrimaryKey={}",
        hex(&conflict.key),
        hex(&conflict.primary)
    );
    let key_parts = if redaction == Redaction::Marker {
        format!(
            "‹›‹{key_table}{key_rest}, originalKey={}, primary=›‹›‹{primary_table}{primary_rest}, originalPrimaryKey={}›",
            hex(&conflict.key),
            hex(&conflict.primary)
        )
    } else {
        key_parts
    };
    format!(
        "{prefix}{key_parts}, reason={} {TXN_RETRYABLE_MARK}",
        conflict.reason
    )
}
