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

use crate::{Catalog, DriverError, TableEntry};
use tidb_hack::GoToLower;

pub(super) fn guard_alter_actions(
    catalog: &Catalog,
    database: &str,
    name: &str,
    actions: &[tidb_ast::AlterTableAction],
) -> Result<(), DriverError> {
    let cached = matches!(
        catalog.table_in(database, name),
        Some(TableEntry::Kv(table)) if table.is_cache_table()
    );
    if cached && !matches!(actions, [tidb_ast::AlterTableAction::Cache(_)]) {
        return Err(DriverError::OperationOnCachedTable("Alter Table"));
    }
    Ok(())
}

pub(super) fn alter_cache_action(
    catalog: &mut Catalog,
    database: &str,
    name: &str,
    mode: tidb_ast::AlterTableCacheMode,
) -> Result<(), DriverError> {
    let Some(TableEntry::Kv(table)) = catalog.table_mut_in(database, name) else {
        return Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(
            format!("{database}.{name}"),
        )));
    };
    let table = std::sync::Arc::make_mut(table);
    match mode {
        tidb_ast::AlterTableCacheMode::Cache => {
            if table.is_cached() {
                return Ok(());
            }
            if tidb_metadef::is_mem_or_sys_db(&database.go_to_lower()) {
                return Err(DriverError::UnsupportedAlterCacheForSystemTable);
            }
            table.enable_cache().map_err(|error| match error {
                crate::kv_table::KvTableError::CacheTableUnsupported(operation) => {
                    DriverError::OperationOnCachedTable(operation)
                }
                crate::kv_table::KvTableError::Storage(error) => error.into(),
                other => {
                    DriverError::unsupported(format!("cache table size check failed: {other:?}"))
                }
            })?;
        }
        tidb_ast::AlterTableCacheMode::NoCache => table.disable_cache(),
    }
    Ok(())
}

/// Go's `ErrOptOnCacheTable` (8242) refusals that run before a DDL job is
/// built, by statement kind: `AlterTable` (anything but one CACHE / NOCACHE),
/// `createIndex`, `dropIndex`, `TruncateTable`, and `renameTable` /
/// `renameTables`. DROP TABLE answers its own (`check_drop_object`). A table
/// that does not exist is left to the statement, which reports it.
pub fn check_cached_table_ddl(
    catalog: &Catalog,
    current_db: &str,
    stmt: &tidb_ast::Stmt,
) -> Result<(), DriverError> {
    let tidb_ast::Stmt::Ddl(ddl) = stmt else {
        return Ok(());
    };
    let cached = |path: &[String]| {
        let (database, name) = match path {
            [database, name] => (database.as_str(), name.as_str()),
            [name] => (current_db, name.as_str()),
            _ => return false,
        };
        matches!(
            catalog.table_in(database, name),
            Some(TableEntry::Kv(table)) if table.is_cache_table()
        )
    };
    let refused = |operation| Err(DriverError::OperationOnCachedTable(operation));
    match ddl.as_ref() {
        tidb_ast::DdlStmt::AlterTable(alter) => {
            if cached(&alter.name)
                && !matches!(
                    alter.actions.as_slice(),
                    [tidb_ast::AlterTableAction::Cache(_)]
                )
            {
                return refused("Alter Table");
            }
        }
        tidb_ast::DdlStmt::CreateIndex(create) if cached(&create.table) => {
            return refused("Create Index");
        }
        tidb_ast::DdlStmt::DropIndex(drop) if cached(&drop.table) => {
            return refused("Drop Index");
        }
        tidb_ast::DdlStmt::TruncateTable(name) if cached(name) => {
            return refused("Truncate Table");
        }
        tidb_ast::DdlStmt::RenameTable(rename) => {
            let operation = if rename.pairs.len() == 1 {
                "Rename Table"
            } else {
                "Rename Tables"
            };
            if rename.pairs.iter().any(|(old, _)| cached(old)) {
                return refused(operation);
            }
        }
        _ => {}
    }
    Ok(())
}
