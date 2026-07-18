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

//! Read-only warning publication metadata from `pkg/util/context/warn.go`.
//!
//! [`StaticWarningHandler`] and [`IgnoreWarnings`] are the shared mutable and
//! no-op warning sinks. [`WarningPublication`] is the borrowed publication
//! view over their common [`StatementWarning`] entries and the entries already
//! owned by [`super::statement_status::StatementStatus`].

use std::sync::Mutex;

use tidb_datatype::ConversionWarningAppender;
use tidb_error::terror::TerrorError;

use super::statement_status::{StatementWarning, WarningLevel};

/// A protocol-sized warning count. `StatementContext.WarningCount` publishes
/// a `uint16`, while the underlying warning handler keeps the source order.
pub const MAX_WARNING_COUNT: usize = u16::MAX as usize;

/// Source `WarnAppender` over the shared warning entry type.
pub trait WarningAppender {
    /// Appends an ordinary warning.
    fn append_warning(&self, message: String);
    /// Appends a note.
    fn append_note(&self, message: String);
}

/// Source `WarnHandler` operations needed by conversion and statement seams.
pub trait WarningHandler: WarningAppender {
    /// Returns the uncapped number of retained warnings.
    fn warning_count(&self) -> usize;
    /// Removes and returns entries beginning at `start`.
    fn truncate_warnings(&self, start: usize) -> Vec<StatementWarning>;
    /// Copies entries into caller-owned storage without aliasing the handler.
    fn copy_warnings(&self, destination: &mut Vec<StatementWarning>);
}

/// Thread-safe direct translation of Go's `StaticWarnHandler`.
#[derive(Debug, Default)]
pub struct StaticWarningHandler {
    warnings: Mutex<Vec<StatementWarning>>,
}

impl StaticWarningHandler {
    /// Creates a handler with the requested initial capacity.
    pub fn new(capacity: usize) -> Self {
        let warnings = if capacity == 0 {
            Vec::new()
        } else {
            Vec::with_capacity(capacity)
        };
        Self {
            warnings: Mutex::new(warnings),
        }
    }

    /// Copies an optional source handler into independent storage.
    pub fn from_handler(handler: Option<&dyn WarningHandler>) -> Self {
        let Some(handler) = handler else {
            return Self::new(0);
        };
        let mut warnings = Vec::with_capacity(handler.warning_count());
        handler.copy_warnings(&mut warnings);
        Self {
            warnings: Mutex::new(warnings),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<StatementWarning>> {
        self.warnings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Clears retained entries while preserving allocation capacity.
    pub fn reset(&self) {
        self.lock().clear();
    }

    /// Appends a source-ordered batch. Go checks the cap only before the batch,
    /// so a batch may carry the final length beyond `MaxUint16`.
    pub fn append_warnings(&self, warnings: impl IntoIterator<Item = StatementWarning>) {
        let mut retained = self.lock();
        if retained.len() < MAX_WARNING_COUNT {
            retained.extend(warnings);
        }
    }

    /// Appends an Error-level warning.
    pub fn append_error(&self, message: impl Into<String>) {
        self.append_with_level(WarningLevel::Error, message.into());
    }

    fn append_with_level(&self, level: WarningLevel, message: String) {
        let mut warnings = self.lock();
        if warnings.len() < MAX_WARNING_COUNT {
            warnings.push(StatementWarning::new(level, message));
        }
    }

    /// Returns an independent ordered snapshot.
    pub fn warnings_snapshot(&self) -> Vec<StatementWarning> {
        self.lock().clone()
    }

    /// Replaces the internal list directly, matching `SetWarnings` without
    /// imposing the single-append cap.
    pub fn set_warnings(&self, warnings: Vec<StatementWarning>) {
        *self.lock() = warnings;
    }

    /// Returns `(Error count, total count)`. The Error count wraps as Go's
    /// `uint16` does if `SetWarnings` installed more than 65,535 errors.
    pub fn num_error_warnings(&self) -> (u16, usize) {
        let warnings = self.lock();
        let errors = warnings
            .iter()
            .filter(|warning| warning.level == WarningLevel::Error)
            .fold(0_u16, |count, _| count.wrapping_add(1));
        (errors, warnings.len())
    }
}

impl WarningAppender for StaticWarningHandler {
    fn append_warning(&self, message: String) {
        self.append_with_level(WarningLevel::Warning, message);
    }

    fn append_note(&self, message: String) {
        self.append_with_level(WarningLevel::Note, message);
    }
}

impl WarningHandler for StaticWarningHandler {
    fn warning_count(&self) -> usize {
        self.lock().len()
    }

    fn truncate_warnings(&self, start: usize) -> Vec<StatementWarning> {
        let mut warnings = self.lock();
        if start >= warnings.len() {
            return Vec::new();
        }
        warnings.split_off(start)
    }

    fn copy_warnings(&self, destination: &mut Vec<StatementWarning>) {
        let warnings = self.lock();
        if destination.capacity() < warnings.len() {
            let mut replacement = Vec::with_capacity(warnings.len());
            replacement.extend(warnings.iter().cloned());
            *destination = replacement;
        } else {
            destination.clear();
            destination.extend(warnings.iter().cloned());
        }
    }
}

/// Source `IgnoreWarn`: every operation is a no-op or empty result.
#[derive(Clone, Copy, Debug, Default)]
pub struct IgnoreWarnings;

impl WarningAppender for IgnoreWarnings {
    fn append_warning(&self, _message: String) {}

    fn append_note(&self, _message: String) {}
}

impl WarningHandler for IgnoreWarnings {
    fn warning_count(&self) -> usize {
        0
    }

    fn truncate_warnings(&self, _start: usize) -> Vec<StatementWarning> {
        Vec::new()
    }

    fn copy_warnings(&self, destination: &mut Vec<StatementWarning>) {
        destination.clear();
    }
}

impl ConversionWarningAppender for StaticWarningHandler {
    fn append_conversion_warning(&self, warning: TerrorError) {
        WarningAppender::append_warning(self, warning.to_string());
    }
}

impl ConversionWarningAppender for IgnoreWarnings {
    fn append_conversion_warning(&self, _warning: TerrorError) {}
}

/// Serializes warning levels and message errors with the source JSON keys.
///
/// Typed `terror.Error` payload identity remains outside this message-only
/// statement seam; its message round trip is exact.
pub fn warnings_to_json(warnings: &[StatementWarning]) -> Result<String, serde_json::Error> {
    let values = warnings
        .iter()
        .map(|warning| {
            serde_json::json!({
                "level": level_name(warning.level),
                "msg": warning.message,
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&values)
}

/// Deserializes the message-shaped warning JSON emitted by
/// [`warnings_to_json`].
pub fn warnings_from_json(data: &str) -> Result<Vec<StatementWarning>, serde_json::Error> {
    let values: Vec<serde_json::Value> = serde_json::from_str(data)?;
    Ok(values
        .into_iter()
        .map(|value| {
            let level = value
                .get("level")
                .and_then(serde_json::Value::as_str)
                .map(level_from_name)
                .unwrap_or(WarningLevel::Warning);
            let message = value
                .get("msg")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            StatementWarning::new(level, message)
        })
        .collect())
}

const fn level_name(level: WarningLevel) -> &'static str {
    match level {
        WarningLevel::Error => "Error",
        WarningLevel::Warning => "Warning",
        WarningLevel::Note => "Note",
    }
}

fn level_from_name(level: &str) -> WarningLevel {
    match level {
        "Error" => WarningLevel::Error,
        "Note" => WarningLevel::Note,
        _ => WarningLevel::Warning,
    }
}

/// Counts exposed by a read-only warning publication.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WarningSummary {
    warning_count: u16,
    error_count: u16,
}

impl WarningSummary {
    /// Returns the protocol-sized total warning count.
    pub const fn warning_count(self) -> u16 {
        self.warning_count
    }

    /// Returns the protocol-sized number of `Error` level entries.
    pub const fn error_count(self) -> u16 {
        self.error_count
    }
}

/// Ordered warning entries published from one statement status.
///
/// The view borrows the statement owner's entries, so its lifetime cannot
/// outlive the status that produced it and no independent session sink is
/// introduced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WarningPublication<'a> {
    warnings: &'a [StatementWarning],
}

impl<'a> WarningPublication<'a> {
    /// Creates a publication view over already ordered statement warnings.
    pub const fn new(warnings: &'a [StatementWarning]) -> Self {
        Self { warnings }
    }

    /// Returns the source-order warning entries without copying them.
    pub const fn warnings(self) -> &'a [StatementWarning] {
        self.warnings
    }

    /// Returns levels in the exact order used by `SHOW WARNINGS`.
    pub fn levels(self) -> impl Iterator<Item = WarningLevel> + 'a {
        self.warnings.iter().map(|warning| warning.level)
    }

    /// Returns the source-shaped total/error count summary.
    pub fn summary(self) -> WarningSummary {
        let warning_count = self.warnings.len() as u16;
        let error_count = self
            .warnings
            .iter()
            .filter(|warning| warning.level == WarningLevel::Error)
            .count() as u16;
        WarningSummary {
            warning_count,
            error_count,
        }
    }

    /// Returns the protocol-sized total count.
    pub fn warning_count(self) -> u16 {
        self.summary().warning_count()
    }

    /// Returns `(error_count, total_count)`, matching `NumErrorWarnings`'s
    /// split between a `uint16` error count and an unbounded slice length.
    pub fn num_error_warnings(self) -> (u16, usize) {
        (self.summary().error_count(), self.warnings.len())
    }
}
