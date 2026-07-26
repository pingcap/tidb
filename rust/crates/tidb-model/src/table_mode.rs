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

//! `pkg/meta/model/table_mode.go`: the table-mode metadata.

use tidb_ast::CiString;

/// Go `TableMode` (a `byte`): a table-level flag that blocks read/write while
/// the table is being imported (`IMPORT INTO`) or restored (BR). When the
/// mode is not [`TableMode::NORMAL`], DMLs/DDLs that change the table error;
/// only the internal `AlterTableMode` DDL may change the mode.
///
/// Modelled as a newtype over `u8` so that, like Go's `byte`, any stored
/// value round-trips and [`Display`](std::fmt::Display) yields `""` for an
/// unknown mode rather than being an invalid enum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TableMode(pub u8);

impl TableMode {
    /// The table is in normal mode (Go `TableModeNormal`, the zero value).
    pub const NORMAL: TableMode = TableMode(0);
    /// The table is in import mode (Go `TableModeImport`).
    pub const IMPORT: TableMode = TableMode(1);
    /// The table is in restore mode (Go `TableModeRestore`).
    pub const RESTORE: TableMode = TableMode(2);

    /// Go `CanTransitionTo`: whether the mode may change from `self` to
    /// `target`. Only import<->restore conversions are blocked.
    #[must_use]
    pub fn can_transition_to(self, target: TableMode) -> bool {
        if self == TableMode::IMPORT && target == TableMode::RESTORE {
            return false;
        }
        if self == TableMode::RESTORE && target == TableMode::IMPORT {
            return false;
        }
        true
    }
}

impl std::fmt::Display for TableMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match *self {
            TableMode::NORMAL => "Normal",
            TableMode::IMPORT => "Import",
            TableMode::RESTORE => "Restore",
            _ => "",
        })
    }
}

/// Go `AlterTableModeTarget`: a table-mode change request and, once resolved,
/// the metadata needed to build an `AlterTableMode` DDL job.
#[derive(Clone, Debug, Default)]
pub struct AlterTableModeTarget {
    /// The schema containing the target table (required input).
    pub schema_id: i64,
    /// The schema name; required for cross-keyspace requests and
    /// runtime-populated for local ones, validated against metadata.
    pub schema_name: CiString,
    /// The target table's ID (required input).
    pub table_id: i64,
    /// The table name; required for cross-keyspace requests and
    /// runtime-populated for local ones, validated against metadata.
    pub table_name: CiString,
    /// Runtime-populated from table metadata during resolution.
    pub current_mode: TableMode,
    /// The mode requested by the caller (required input).
    pub target_mode: TableMode,
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go TestTableModeCanTransitionTo.
    #[test]
    fn can_transition_to() {
        let cases = [
            (TableMode::NORMAL, TableMode::NORMAL, true),
            (TableMode::NORMAL, TableMode::IMPORT, true),
            (TableMode::NORMAL, TableMode::RESTORE, true),
            (TableMode::IMPORT, TableMode::NORMAL, true),
            (TableMode::IMPORT, TableMode::IMPORT, true),
            (TableMode::IMPORT, TableMode::RESTORE, false),
            (TableMode::RESTORE, TableMode::NORMAL, true),
            (TableMode::RESTORE, TableMode::IMPORT, false),
            (TableMode::RESTORE, TableMode::RESTORE, true),
        ];
        for (from, to, expect) in cases {
            assert_eq!(from.can_transition_to(to), expect, "{from} -> {to}");
        }
    }

    #[test]
    fn display_and_default() {
        assert_eq!(TableMode::NORMAL.to_string(), "Normal");
        assert_eq!(TableMode::IMPORT.to_string(), "Import");
        assert_eq!(TableMode::RESTORE.to_string(), "Restore");
        assert_eq!(TableMode(99).to_string(), "");
        // The zero value is Normal, like Go's byte zero value.
        assert_eq!(TableMode::default(), TableMode::NORMAL);
    }
}
