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
    WriteConflictArgs::new(conflict, redaction).detail()
}

/// One string argument of a generated error after Go `errors.RedactErrorArg`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RedactArg {
    /// A Go `string` argument; under `ON` redaction it is the `?` placeholder.
    Str(String),
    /// Go `*redactFormatter`, the `MARKER` wrapping: its argument rendered
    /// between `‹` and `›`, with every marker inside doubled.
    Marked(Box<RedactArg>),
}

impl RedactArg {
    /// Go `RedactErrorArg` applied to one argument at a redacted position.
    #[must_use]
    pub fn redact(self, redaction: Redaction) -> Self {
        match redaction {
            Redaction::Disabled => self,
            Redaction::Enabled => Self::Str("?".to_owned()),
            Redaction::Marker => Self::Marked(Box::new(self)),
        }
    }
}

impl fmt::Display for RedactArg {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Str(value) => formatter.write_str(value),
            Self::Marked(inner) => {
                let mut output = String::from("‹");
                for character in inner.to_string().chars() {
                    output.push(character);
                    if matches!(character, '‹' | '›') {
                        output.push(character);
                    }
                }
                output.push('›');
                formatter.write_str(&output)
            }
        }
    }
}

/// Go `newWriteConflictError`'s `kv.ErrWriteConflict.FastGenByArgs`
/// arguments, as `RedactErrorArg` left them.
///
/// Go's session rewrites the two table-ID fields (`args[3]`, `args[5]`) of a
/// commit's write conflict before the client sees it
/// (`tryReplaceWriteConflictError`), so the arguments travel with the error
/// rather than only its rendered text.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WriteConflictArgs {
    /// `args[0]`: the transaction's start timestamp.
    pub start_ts: u64,
    /// `args[1]`: the conflicting transaction's start timestamp.
    pub conflict_ts: u64,
    /// `args[2]`: the conflicting transaction's commit timestamp.
    pub conflict_commit_ts: u64,
    /// `args[3]`: the conflicting key's `{tableID=N` field, empty when the key
    /// is not a table key.
    pub key_table_id: RedactArg,
    /// `args[4]`: the rest of the conflicting key, its `originalKey`, and the
    /// `primary=` label.
    pub key_rest: RedactArg,
    /// `args[5]`: the primary key's `{tableID=N` field.
    pub primary_table_id: RedactArg,
    /// `args[6]`: the rest of the primary key and its `originalPrimaryKey`.
    pub primary_rest: RedactArg,
    /// `args[7]`: TiKV's conflict reason name.
    pub reason: String,
}

impl WriteConflictArgs {
    /// Go `newWriteConflictError(conflict)` under `redaction`.
    #[must_use]
    pub fn new(conflict: &WriteConflict, redaction: Redaction) -> Self {
        let (key_table_id, key_rest) = pretty_write_conflict_key(&conflict.key);
        let (primary_table_id, primary_rest) = pretty_write_conflict_key(&conflict.primary);
        let key_rest = format!("{key_rest}, originalKey={}, primary=", hex(&conflict.key));
        let primary_rest = format!(
            "{primary_rest}, originalPrimaryKey={}",
            hex(&conflict.primary)
        );
        Self {
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

    /// Go `kv.ErrWriteConflict.FastGenByArgs(args...)` over arguments copied
    /// from an earlier error: `RedactErrorArg` runs again on the key
    /// positions.
    #[must_use]
    pub fn fast_gen_by_args(self, redaction: Redaction) -> Self {
        Self {
            key_table_id: self.key_table_id.redact(redaction),
            key_rest: self.key_rest.redact(redaction),
            primary_table_id: self.primary_table_id.redact(redaction),
            primary_rest: self.primary_rest.redact(redaction),
            ..self
        }
    }

    /// The formatted arguments between `Write conflict, ` and the retryable
    /// mark.
    #[must_use]
    pub fn detail(&self) -> String {
        format!(
            "txnStartTS={}, conflictStartTS={}, conflictCommitTS={}, key={}{}{}{}, reason={}",
            self.start_ts,
            self.conflict_ts,
            self.conflict_commit_ts,
            self.key_table_id,
            self.key_rest,
            self.primary_table_id,
            self.primary_rest,
            self.reason
        )
    }

    /// Go `GetMsg()` of the generated `ErrWriteConflict`, which is what
    /// `terror.ToSQLError` sends a client: no `[kv:9007]` class prefix.
    #[must_use]
    pub fn message(&self) -> String {
        format!("Write conflict, {} {TXN_RETRYABLE_MARK}", self.detail())
    }
}

impl Redaction {
    /// The process redaction mode (Go `errors.RedactLogEnabled`).
    #[must_use]
    pub fn current() -> Self {
        use tidb_mysql::errors::{redaction_mode, RedactionMode};
        match redaction_mode() {
            RedactionMode::Disabled => Self::Disabled,
            RedactionMode::Enabled => Self::Enabled,
            RedactionMode::Marker => Self::Marker,
        }
    }
}
