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

//! Warning collection shared by an attached and detached request.

use std::sync::{Arc, Mutex, MutexGuard};

/// The three warning levels exposed by TiDB's `WarnAppender` contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarningLevel {
    /// A statement error reported as a warning.
    Error,
    /// A regular statement warning.
    Warning,
    /// An informational statement note.
    Note,
}

/// Subsystem that owns a warning code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WarningClass {
    /// A statement-local warning without a subsystem error code.
    Statement,
    /// A warning synthesized from a TiKV `SelectResponse` error value.
    TiKv,
}

impl WarningLevel {
    /// Returns the MySQL `SHOW WARNINGS` spelling used by TiDB.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Error => "Error",
            Self::Warning => "Warning",
            Self::Note => "Note",
        }
    }
}

/// One owned warning message and its MySQL-visible level.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Warning {
    /// MySQL-visible warning level.
    pub level: WarningLevel,
    /// Error namespace used to interpret `code`.
    pub class: WarningClass,
    /// Source error code, when the warning originated from a coded error.
    pub code: Option<i32>,
    /// The rendered source error message.
    pub message: String,
}

/// Thread-safe warning appender shared by attached and detached contexts.
///
/// Go's `DistSQLContext.Detach` preserves the `WarnHandler` pointer. Cloning
/// this value therefore clones the `Arc`, not the warning vector: warnings
/// appended by a detached request remain visible to its owning session.
#[derive(Clone, Debug, Default)]
pub struct WarningCollector {
    warnings: Arc<Mutex<Vec<Warning>>>,
}

impl WarningCollector {
    /// Creates an empty warning collector.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a regular warning message.
    pub fn append_warning(&self, message: impl Into<String>) {
        self.append(WarningLevel::Warning, message);
    }

    /// Appends an informational note.
    pub fn append_note(&self, message: impl Into<String>) {
        self.append(WarningLevel::Note, message);
    }

    /// Appends an error-level warning.
    pub fn append_error(&self, message: impl Into<String>) {
        self.append(WarningLevel::Error, message);
    }

    /// Returns a stable snapshot of all warnings currently collected.
    #[must_use]
    pub fn warnings(&self) -> Vec<Warning> {
        self.lock().clone()
    }

    /// Returns the number of currently collected warnings.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    /// Reports whether no warnings have been collected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Reports whether two collectors retain the exact same underlying handler.
    #[must_use]
    pub fn shares_handler_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.warnings, &other.warnings)
    }

    fn append(&self, level: WarningLevel, message: impl Into<String>) {
        self.lock().push(Warning {
            level,
            class: WarningClass::Statement,
            code: None,
            message: message.into(),
        });
    }

    /// Appends a warning synthesized from a TiKV response error.
    pub fn append_tikv_warning(&self, code: i32, message: impl Into<String>) {
        self.lock().push(Warning {
            level: WarningLevel::Warning,
            class: WarningClass::TiKv,
            code: Some(code),
            message: message.into(),
        });
    }

    /// Appends an already-classified warning without losing its code namespace.
    pub fn append_owned_warning(&self, warning: Warning) {
        self.lock().push(warning);
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Warning>> {
        self.warnings
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
