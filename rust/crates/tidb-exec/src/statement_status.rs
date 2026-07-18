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

//! Dependency-closed statement status for the Go `StatementContext` seam.
//!
//! This leaf owns only status produced while a statement executes: affected,
//! found, record, deleted, updated, copied, and touched row counters; the
//! current/previous last-insert ID; an informational message; and ordered
//! warning entries.  It deliberately does not model Go's type/eval context,
//! SQL mode, warning filtering, or general session-variable integration. The
//! existing shared-catalog [`crate::Session`] calls
//! [`StatementStatus::begin_statement`] and
//! [`StatementStatus::finish_statement_with_outcome`] at its bounded live
//! statement boundary.

use crate::warning_publication::{StaticWarningHandler, WarningHandler};

/// The statement classes that affect Go's published `PrevAffectedRows` value.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum StatementKind {
    /// A DDL statement publishes zero affected rows.
    Ddl,
    /// An INSERT/UPDATE/DELETE statement publishes its affected-row counter.
    Dml,
    /// A SELECT publishes `ROW_COUNT() = -1`.
    Select,
    /// An ordinary session command publishes zero affected rows.
    Session,
    /// An owner has not selected a statement class yet.
    #[default]
    Unknown,
}

/// SHOW WARNINGS-compatible warning levels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarningLevel {
    /// A hard error retained in the statement warning list.
    Error,
    /// An ordinary warning.
    Warning,
    /// An informational note.
    Note,
}

/// One ordered statement warning.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatementWarning {
    /// SHOW WARNINGS level.
    pub level: WarningLevel,
    /// Source error/note text. Error construction and SQL codes belong to the
    /// future session/error-context owner.
    pub message: String,
}

impl StatementWarning {
    /// Creates a warning entry without inventing source error codes.
    pub fn new(level: WarningLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            message: message.into(),
        }
    }
}

/// The status visible after one statement is published.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PublishedStatementStatus {
    /// Affected rows for DML, or zero for DDL.
    pub affected_rows: u64,
    /// MySQL `ROW_COUNT()` value: -1 for SELECT, otherwise the applicable
    /// non-negative affected-row count.
    pub row_count: i64,
    /// The last insert ID visible after the statement.
    pub last_insert_id: u64,
    /// Warning entries in source order.
    pub warnings: Vec<StatementWarning>,
    /// Informational message associated with the statement.
    pub message: String,
    /// Whether the statement completed successfully.
    ///
    /// Go keeps this bit on `StatementContext.ExecSuccess` and fills it at
    /// the executor adapter boundary (`pkg/sessionctx/stmtctx/stmtctx.go:361-365`,
    /// `pkg/executor/adapter.go:1961`). It is carried with the published
    /// warning/message snapshot so a connection writer can distinguish an
    /// error-context attachment from a successful OK status without guessing
    /// from warning levels or rendered text.
    pub exec_success: bool,
}

/// Statement-scoped counters and publish/reset state.
#[derive(Debug, Default)]
pub struct StatementStatus {
    kind: StatementKind,
    affected_rows: u64,
    found_rows: u64,
    records: u64,
    deleted: u64,
    updated: u64,
    copied: u64,
    touched: u64,
    previous: PublishedStatementStatus,
    current_last_insert_id: Option<u64>,
    warnings: StaticWarningHandler,
    message: String,
}

impl StatementStatus {
    /// Starts one statement, clearing execution counters and warnings while
    /// retaining the previous published status for session readback.
    pub fn begin_statement(&mut self, kind: StatementKind) {
        self.kind = kind;
        self.clear_execution_state();
    }

    /// Fully resets the status object, matching `StatementContext.Reset`'s
    /// zeroing of current and previous statement fields.
    pub fn reset(&mut self) {
        self.kind = StatementKind::default();
        self.affected_rows = 0;
        self.found_rows = 0;
        self.records = 0;
        self.deleted = 0;
        self.updated = 0;
        self.copied = 0;
        self.touched = 0;
        self.previous = PublishedStatementStatus::default();
        self.current_last_insert_id = None;
        self.warnings.reset();
        self.message.clear();
    }

    /// Clears mutable execution counters and warning entries for a retry.
    /// Published status and the current last-insert ID are intentionally
    /// retained: Go's `ResetForRetry` resets `mu`/affected rows and warnings,
    /// but does not overwrite `Prev*` or `LastInsertID` fields.
    pub fn reset_for_retry(&mut self) {
        self.affected_rows = 0;
        self.found_rows = 0;
        self.records = 0;
        self.deleted = 0;
        self.updated = 0;
        self.copied = 0;
        self.touched = 0;
        self.warnings.reset();
        self.message.clear();
    }

    /// Adds affected rows.
    pub fn add_affected_rows(&mut self, rows: u64) {
        self.affected_rows = self.affected_rows.wrapping_add(rows);
    }

    /// Replaces affected rows.
    pub fn set_affected_rows(&mut self, rows: u64) {
        self.affected_rows = rows;
    }

    /// Returns affected rows accumulated by the current statement.
    pub const fn affected_rows(&self) -> u64 {
        self.affected_rows
    }

    /// Adds found rows.
    pub fn add_found_rows(&mut self, rows: u64) {
        self.found_rows = self.found_rows.wrapping_add(rows);
    }

    /// Returns found rows.
    pub const fn found_rows(&self) -> u64 {
        self.found_rows
    }

    /// Adds rows examined/recorded for the statement message.
    pub fn add_record_rows(&mut self, rows: u64) {
        self.records = self.records.wrapping_add(rows);
    }

    /// Returns recorded rows.
    pub const fn record_rows(&self) -> u64 {
        self.records
    }

    /// Adds deleted rows.
    pub fn add_deleted_rows(&mut self, rows: u64) {
        self.deleted = self.deleted.wrapping_add(rows);
    }

    /// Returns deleted rows.
    pub const fn deleted_rows(&self) -> u64 {
        self.deleted
    }

    /// Adds updated rows.
    pub fn add_updated_rows(&mut self, rows: u64) {
        self.updated = self.updated.wrapping_add(rows);
    }

    /// Returns updated rows.
    pub const fn updated_rows(&self) -> u64 {
        self.updated
    }

    /// Adds copied rows.
    pub fn add_copied_rows(&mut self, rows: u64) {
        self.copied = self.copied.wrapping_add(rows);
    }

    /// Returns copied rows.
    pub const fn copied_rows(&self) -> u64 {
        self.copied
    }

    /// Adds touched rows.
    pub fn add_touched_rows(&mut self, rows: u64) {
        self.touched = self.touched.wrapping_add(rows);
    }

    /// Returns touched rows.
    pub const fn touched_rows(&self) -> u64 {
        self.touched
    }

    /// Sets the generated/current last-insert ID. Setting zero is still an
    /// explicit set, matching Go's separate `LastInsertIDSet` bit.
    pub fn set_last_insert_id(&mut self, value: u64) {
        self.current_last_insert_id = Some(value);
    }

    /// Clears the explicit current last-insert ID marker.
    pub fn clear_last_insert_id(&mut self) {
        self.current_last_insert_id = None;
    }

    /// Returns the current explicit last-insert ID, if one was set.
    pub const fn current_last_insert_id(&self) -> Option<u64> {
        self.current_last_insert_id
    }

    /// Sets the statement informational message.
    pub fn set_message(&mut self, message: impl Into<String>) {
        self.message = message.into();
    }

    /// Returns the current informational message.
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Appends one warning, retaining at most `u16::MAX` entries just like the
    /// source `StaticWarnHandler`.
    pub fn append_warning(&mut self, warning: StatementWarning) {
        self.warnings.append_warnings(std::iter::once(warning));
    }

    /// Appends a batch through the source handler's batch boundary. The Go
    /// implementation checks capacity only before the append, so a caller
    /// that supplies a batch larger than the remaining capacity is preserved
    /// as-is; ordinary single-entry paths should use [`Self::append_warning`].
    pub fn append_warnings<I>(&mut self, warnings: I)
    where
        I: IntoIterator<Item = StatementWarning>,
    {
        self.warnings.append_warnings(warnings);
    }

    /// Appends an ordinary warning.
    pub fn warn(&mut self, message: impl Into<String>) {
        self.append_warning(StatementWarning::new(WarningLevel::Warning, message));
    }

    /// Appends a note.
    pub fn note(&mut self, message: impl Into<String>) {
        self.append_warning(StatementWarning::new(WarningLevel::Note, message));
    }

    /// Appends an error entry.
    pub fn error(&mut self, message: impl Into<String>) {
        self.append_warning(StatementWarning::new(WarningLevel::Error, message));
    }

    /// Replaces the ordered warning list. This is the direct source
    /// `SetWarnings` path and therefore does not apply the single-entry cap.
    pub fn set_warnings(&mut self, warnings: Vec<StatementWarning>) {
        self.warnings.set_warnings(warnings);
    }

    /// Returns an independent ordered snapshot of the current warning list.
    pub fn warnings(&self) -> Vec<StatementWarning> {
        self.warnings.warnings_snapshot()
    }

    /// Returns the source-compatible wrapping warning count.
    pub fn warning_count(&self) -> u16 {
        self.warnings.warning_count() as u16
    }

    /// Returns the wrapping Error count and retained total warning count.
    pub fn num_error_warnings(&self) -> (u16, usize) {
        self.warnings.num_error_warnings()
    }

    /// Publishes current counters/status and records it as the previous
    /// statement. The current counters remain readable until the next
    /// [`Self::begin_statement`] or reset call.
    pub fn finish_statement(&mut self) -> PublishedStatementStatus {
        self.finish_statement_with_outcome(true)
    }

    /// Publishes current counters/status with the executor outcome.
    ///
    /// Warnings and messages remain ordered/source-rendered; `exec_success`
    /// is the only additional lifecycle bit and does not reinterpret their
    /// text or manufacture an error entry.
    pub fn finish_statement_with_outcome(
        &mut self,
        exec_success: bool,
    ) -> PublishedStatementStatus {
        let affected_rows = match self.kind {
            StatementKind::Ddl => 0,
            StatementKind::Dml => self.affected_rows,
            StatementKind::Select | StatementKind::Session | StatementKind::Unknown => 0,
        };
        let row_count = match self.kind {
            StatementKind::Select => -1,
            StatementKind::Dml => self.affected_rows as i64,
            StatementKind::Ddl => 0,
            StatementKind::Session | StatementKind::Unknown => 0,
        };
        let status = PublishedStatementStatus {
            affected_rows,
            row_count,
            last_insert_id: self
                .current_last_insert_id
                .unwrap_or(self.previous.last_insert_id),
            warnings: self.warnings.warnings_snapshot(),
            message: self.message.clone(),
            exec_success,
        };
        self.previous = status.clone();
        status
    }

    /// Returns the most recently published status.
    pub const fn previous(&self) -> &PublishedStatementStatus {
        &self.previous
    }

    fn clear_execution_state(&mut self) {
        self.affected_rows = 0;
        self.found_rows = 0;
        self.records = 0;
        self.deleted = 0;
        self.updated = 0;
        self.copied = 0;
        self.touched = 0;
        self.current_last_insert_id = None;
        self.warnings.reset();
        self.message.clear();
    }
}
