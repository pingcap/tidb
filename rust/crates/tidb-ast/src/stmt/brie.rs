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

//! Backup/restore AST transcreated from `pkg/parser/ast/misc.go`'s BRIE nodes.

use crate::util::{back_quote, escape_string_literal, push_name_path, redact_url};

/// Backup/restore operation kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrieKind {
    /// Back up databases or tables.
    Backup,
    /// Cancel a BR job.
    CancelJob,
    /// Start log backup.
    StreamStart,
    /// Show log-backup metadata.
    StreamMetadata,
    /// Show log-backup status.
    StreamStatus,
    /// Pause log backup.
    StreamPause,
    /// Resume log backup.
    StreamResume,
    /// Stop log backup.
    StreamStop,
    /// Purge log-backup data.
    StreamPurge,
    /// Restore databases or tables.
    Restore,
    /// Restore to a point in time.
    RestorePoint,
    /// Show a BR job.
    ShowJob,
    /// Show a BR job query.
    ShowQuery,
    /// Show backup metadata.
    ShowBackupMetadata,
}

impl BrieKind {
    fn sql(self) -> &'static str {
        match self {
            Self::Backup => "BACKUP",
            Self::CancelJob => "CANCEL BR JOB",
            Self::StreamStart => "BACKUP LOGS",
            Self::StreamMetadata => "SHOW BACKUP LOGS METADATA",
            Self::StreamStatus => "SHOW BACKUP LOGS STATUS",
            Self::StreamPause => "PAUSE BACKUP LOGS",
            Self::StreamResume => "RESUME BACKUP LOGS",
            Self::StreamStop => "STOP BACKUP LOGS",
            Self::StreamPurge => "PURGE BACKUP LOGS",
            Self::Restore => "RESTORE",
            Self::RestorePoint => "RESTORE POINT",
            Self::ShowJob => "SHOW BR JOB",
            Self::ShowQuery => "SHOW BR JOB QUERY",
            Self::ShowBackupMetadata => "SHOW BACKUP METADATA",
        }
    }
}

/// Canonical BRIE option payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrieOption {
    /// Canonical option name.
    pub name: String,
    /// Canonical option value.
    pub value: BrieOptionValue,
}

/// Restore-visible option value families.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrieOptionValue {
    /// Quoted string value.
    String(String),
    /// Unsigned integer or boolean value.
    Unsigned(u64),
    /// Checksum or analyze level.
    Level(BrieOptionLevel),
    /// Rate limit stored as bytes per second.
    RateLimitBytes(u64),
    /// Relative backup time stored as microseconds.
    MicrosecondsAgo(u64),
    /// `CSV_HEADER = COLUMNS`.
    CsvHeaderColumns,
}

/// OFF/REQUIRED/OPTIONAL BRIE option level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrieOptionLevel {
    /// Disabled.
    Off,
    /// Required.
    Required,
    /// Optional.
    Optional,
}

impl BrieOption {
    fn restore_into(&self, out: &mut String) {
        out.push_str(&self.name);
        out.push_str(" = ");
        match &self.value {
            BrieOptionValue::String(value) => {
                out.push('\'');
                out.push_str(&escape_string_literal(value));
                out.push('\'');
            }
            BrieOptionValue::Unsigned(value) => out.push_str(&value.to_string()),
            BrieOptionValue::Level(level) => out.push_str(match level {
                BrieOptionLevel::Off => "OFF",
                BrieOptionLevel::Required => "REQUIRED",
                BrieOptionLevel::Optional => "OPTIONAL",
            }),
            BrieOptionValue::RateLimitBytes(value) => {
                out.push_str(&(value / 1_048_576).to_string());
                out.push_str(" MB/SECOND");
            }
            BrieOptionValue::MicrosecondsAgo(value) => {
                out.push_str(&(value / 1_000).to_string());
                out.push_str(" MICROSECOND AGO");
            }
            BrieOptionValue::CsvHeaderColumns => out.push_str("COLUMNS"),
        }
    }
}

/// TiDB backup/restore statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrieStmt {
    /// Operation kind.
    pub kind: BrieKind,
    /// Database targets.
    pub schemas: Vec<String>,
    /// Qualified table targets.
    pub tables: Vec<Vec<String>>,
    /// External-storage URL.
    pub storage: String,
    /// BR job identifier.
    pub job_id: i64,
    /// Ordered statement options.
    pub options: Vec<BrieOption>,
}

impl BrieStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str(self.kind.sql());
        match self.kind {
            BrieKind::Backup | BrieKind::Restore => {
                if !self.tables.is_empty() {
                    out.push_str(" TABLE ");
                    for (index, table) in self.tables.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        push_name_path(out, table);
                    }
                } else if !self.schemas.is_empty() {
                    out.push_str(" DATABASE ");
                    for (index, schema) in self.schemas.iter().enumerate() {
                        if index > 0 {
                            out.push_str(", ");
                        }
                        out.push_str(&back_quote(schema));
                    }
                } else {
                    out.push_str(" DATABASE *");
                }
                out.push_str(if self.kind == BrieKind::Backup {
                    " TO '"
                } else {
                    " FROM '"
                });
                out.push_str(&escape_string_literal(&self.storage));
                out.push('\'');
            }
            BrieKind::CancelJob | BrieKind::ShowJob | BrieKind::ShowQuery => {
                out.push(' ');
                out.push_str(&self.job_id.to_string());
            }
            BrieKind::StreamStart => push_storage(out, " TO ", &self.storage),
            BrieKind::RestorePoint
            | BrieKind::StreamMetadata
            | BrieKind::ShowBackupMetadata
            | BrieKind::StreamPurge => push_storage(out, " FROM ", &self.storage),
            BrieKind::StreamStatus
            | BrieKind::StreamPause
            | BrieKind::StreamResume
            | BrieKind::StreamStop => {}
        }
        for option in &self.options {
            out.push(' ');
            option.restore_into(out);
        }
    }

    /// Restores after redacting credentials in the external-storage URL.
    pub fn secure_text(&self) -> String {
        let mut redacted = self.clone();
        redacted.storage = redact_url(&redacted.storage);
        let mut out = String::new();
        redacted.restore_into(&mut out);
        out
    }
}

fn push_storage(out: &mut String, separator: &str, storage: &str) {
    out.push_str(separator);
    out.push('\'');
    out.push_str(&escape_string_literal(storage));
    out.push('\'');
}

// BEGIN GENERATED AST VISITOR IMPLEMENTATIONS

impl crate::Visitable for BrieKind {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Backup => {}
            Self::CancelJob => {}
            Self::StreamStart => {}
            Self::StreamMetadata => {}
            Self::StreamStatus => {}
            Self::StreamPause => {}
            Self::StreamResume => {}
            Self::StreamStop => {}
            Self::StreamPurge => {}
            Self::Restore => {}
            Self::RestorePoint => {}
            Self::ShowJob => {}
            Self::ShowQuery => {}
            Self::ShowBackupMetadata => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BrieOption {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self { name, value } = self;
        if !crate::Visitable::accept(value, visitor) {
            return false;
        }
        let _ = name;
        let _ = value;
        visitor.leave(self)
    }
}

impl crate::Visitable for BrieOptionValue {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::String(field_0) => {
                let _ = field_0;
            }
            Self::Unsigned(field_0) => {
                let _ = field_0;
            }
            Self::Level(field_0) => {
                if !crate::Visitable::accept(field_0, visitor) {
                    return false;
                }
                let _ = field_0;
            }
            Self::RateLimitBytes(field_0) => {
                let _ = field_0;
            }
            Self::MicrosecondsAgo(field_0) => {
                let _ = field_0;
            }
            Self::CsvHeaderColumns => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BrieOptionLevel {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        match self {
            Self::Off => {}
            Self::Required => {}
            Self::Optional => {}
        }
        visitor.leave(self)
    }
}

impl crate::Visitable for BrieStmt {
    fn accept<V: crate::Visitor>(&mut self, visitor: &mut V) -> bool {
        if visitor.enter(self) {
            return visitor.leave(self);
        }
        let Self {
            kind,
            schemas,
            tables,
            storage,
            job_id,
            options,
        } = self;
        if !crate::Visitable::accept(kind, visitor) {
            return false;
        }
        for value in options.iter_mut() {
            if !crate::Visitable::accept(value, visitor) {
                return false;
            }
        }
        let _ = kind;
        let _ = schemas;
        let _ = tables;
        let _ = storage;
        let _ = job_id;
        let _ = options;
        visitor.leave(self)
    }
}
// END GENERATED AST VISITOR IMPLEMENTATIONS
