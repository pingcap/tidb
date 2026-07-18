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

//! Physical table-catalog effects for `DROP`, `TRUNCATE`, and `RENAME`.
//!
//! The Go execution boundaries are `pkg/ddl/executor.go`'s `DropTable`,
//! `TruncateTable`, and `RenameTable`; their catalog transitions are owned by
//! `pkg/ddl/table.go`'s `onDropTableOrView`, `onTruncateTable`,
//! `onRenameTable`, and `onRenameTables`. The caller owns TiDB's DDL implicit
//! commit; this leaf owns only the catalog mutation after that boundary.

use tidb_ast::DropTableStmt;

use crate::catalog::table_key;
use crate::{Database, ExecError};

impl Database {
    /// Moves a table's catalog entry from `old` to `new` (rows, columns, and
    /// `key_groups` untouched) — the shared mechanics behind both
    /// `ALTER TABLE ... RENAME` and `RENAME TABLE ... TO ...`.
    pub(crate) fn rename_table(&mut self, old: &[String], new: &[String]) -> Result<(), ExecError> {
        let old_key = table_key(old);
        let table = self
            .tables
            .remove(&old_key)
            .ok_or_else(|| ExecError::UnknownTable(old_key.clone()))?;
        let new_key = table_key(new);
        self.tables.insert(new_key.clone(), table);
        let next = self.auto_increment_next.borrow_mut().remove(&old_key);
        if let Some(next) = next {
            self.auto_increment_next.borrow_mut().insert(new_key, next);
        }
        Ok(())
    }

    /// `DROP TABLE [IF EXISTS] name [, name2, ...]`. Two independently
    /// confirmed (via `gorun`, not assumed) rules combine here:
    ///
    /// - Referential-integrity validation covers the complete statement
    ///   before any table is removed.
    /// - Existence is then checked per name: existing tables are still
    ///   removed when another listed name is missing.
    pub(crate) fn drop_table(&mut self, stmt: &DropTableStmt) -> Result<(), ExecError> {
        // Temporary tables aren't modelled (nor is `CREATE TEMPORARY
        // TABLE`), so a temporary drop is parse+restore only.
        if stmt.temporary != tidb_ast::DropTemporary::None {
            return Err(ExecError::Unsupported("DROP TEMPORARY TABLE"));
        }
        let keys: Vec<String> = stmt.names.iter().map(|name| table_key(name)).collect();
        let dropping: std::collections::HashSet<&String> = keys.iter().collect();

        if self.foreign_key_checks.is_enabled() {
            for (other_key, other_table) in &self.tables {
                if dropping.contains(other_key) {
                    continue;
                }
                if other_table
                    .foreign_keys
                    .iter()
                    .any(|foreign_key| dropping.contains(&foreign_key.ref_table))
                {
                    return Err(ExecError::ForeignKeyViolation);
                }
            }
        }

        let mut missing: Option<String> = None;
        for key in &keys {
            if self.tables.remove(key).is_some() {
                self.auto_increment_next.borrow_mut().remove(key);
            } else if !stmt.if_exists && missing.is_none() {
                missing = Some(key.clone());
            }
        }
        match missing {
            Some(key) => Err(ExecError::UnknownTable(key)),
            None => Ok(()),
        }
    }

    /// Applies the catalog half of TiDB's truncate transition after the
    /// caller has crossed the DDL implicit-commit boundary.
    pub(super) fn truncate_table(&mut self, name: &[String]) -> Result<(), ExecError> {
        let key = table_key(name);
        match self.tables.get_mut(&key) {
            Some(table) => {
                table.rows.clear();
                // `pkg/ddl/table.go:onTruncateTable` publishes a fresh table
                // identity. Its auto-ID allocator therefore restarts at 1,
                // even if CREATE TABLE specified AUTO_INCREMENT = N.
                if table.auto_increment.is_some() {
                    self.auto_increment_next.borrow_mut().insert(key, Some(1));
                }
                Ok(())
            }
            None => Err(ExecError::UnknownTable(key)),
        }
    }
}
