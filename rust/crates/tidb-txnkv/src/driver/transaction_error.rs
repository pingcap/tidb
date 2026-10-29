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

//! The write-conflict arguments and redaction modes Go's storage errors
//! carry; decoding the keys in them is the transaction driver's
//! (`tidb_store_driver_txn::txn_error`, Go `store/driver/txn/error.go`).

use std::fmt;

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
