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

//! `ALTER TABLE ... RENAME COLUMN old TO new` payload.
//!
//! Go's `HandParser.parseAlterRename` stores the two column names directly on
//! `AlterTableSpec`. Keep this source-owned payload independent from
//! `CHANGE COLUMN`, whose second name is part of a full column definition.

/// An existing column rename.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenameColumn {
    /// Existing column name.
    pub from: String,
    /// Replacement column name.
    pub to: String,
}
