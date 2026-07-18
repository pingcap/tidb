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

//! Typed payload for Go's shared `ADMIN {CANCEL|PAUSE|RESUME} DDL JOBS` path.

/// The three DDL-job queue controls which share Go's `parseAdminDDLJobs`
/// helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminDdlJobControlKind {
    /// `ADMIN CANCEL DDL JOBS id [, id ...]`.
    Cancel,
    /// `ADMIN PAUSE DDL JOBS id [, id ...]`.
    Pause,
    /// `ADMIN RESUME DDL JOBS id [, id ...]`.
    Resume,
}

impl AdminDdlJobControlKind {
    fn restore_name(self) -> &'static str {
        match self {
            Self::Cancel => "CANCEL",
            Self::Pause => "PAUSE",
            Self::Resume => "RESUME",
        }
    }
}

/// A queued DDL-job control request.
///
/// Go's hand parser deliberately discards the one token after `DDL` before
/// parsing `job_ids`; its AST restore always emits `JOBS`. The parser owns
/// that compatibility quirk while this typed payload retains only source
/// visible semantic state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminDdlJobControlStmt {
    /// Requested queue operation.
    pub kind: AdminDdlJobControlKind,
    /// DDL job IDs in source order.
    pub job_ids: Vec<i64>,
}

impl AdminDdlJobControlStmt {
    pub(crate) fn restore_into(&self, out: &mut String) {
        out.push_str("ADMIN ");
        out.push_str(self.kind.restore_name());
        out.push_str(" DDL JOBS ");
        for (index, job_id) in self.job_ids.iter().enumerate() {
            if index > 0 {
                out.push_str(", ");
            }
            out.push_str(&job_id.to_string());
        }
    }
}
