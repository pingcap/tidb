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

//! Execution task kinds from `pkg/planner/property/task_type.go`.
//!
//! The source stores task kinds as an integer and returns a stable diagnostic
//! label, including for values added by a future planner. `Unknown(i32)`
//! preserves those forward-compatible raw values instead of collapsing them
//! into a fake known task.

use std::fmt;

/// Execution location used by planner physical tasks.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TaskType {
    /// TiDB root-layer execution.
    Root,
    /// Single-read coprocessor task.
    CopSingleRead,
    /// Multi-read/index-lookup coprocessor task.
    CopMultiRead,
    /// TiFlash MPP task.
    Mpp,
    /// An unknown source integer, retained for forward compatibility.
    Unknown(i32),
}

impl TaskType {
    /// Converts the source integer representation into a typed task kind.
    #[must_use]
    pub const fn from_raw(raw: i32) -> Self {
        match raw {
            0 => Self::Root,
            1 => Self::CopSingleRead,
            2 => Self::CopMultiRead,
            3 => Self::Mpp,
            other => Self::Unknown(other),
        }
    }

    /// Returns the source integer representation.
    #[must_use]
    pub const fn raw(self) -> i32 {
        match self {
            Self::Root => 0,
            Self::CopSingleRead => 1,
            Self::CopMultiRead => 2,
            Self::Mpp => 3,
            Self::Unknown(raw) => raw,
        }
    }

    /// Returns the source diagnostic label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Root => "rootTask",
            Self::CopSingleRead => "copSingleReadTask",
            Self::CopMultiRead => "copMultiReadTask",
            Self::Mpp => "mppTask",
            Self::Unknown(_) => "UnknownTaskType",
        }
    }
}

impl fmt::Display for TaskType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
