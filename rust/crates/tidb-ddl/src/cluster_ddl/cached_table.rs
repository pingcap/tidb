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

//! Go's `ErrOptOnCacheTable` (8242) refusals that the DDL executor's entry
//! points raise against the latest infoschema before any job is built.

use tidb_error::mysql::MysqlError;

use tidb_infoschema::cluster_catalog::ClusterCatalog;

/// Go `executor.AlterTable` (anything but one CACHE / NOCACHE spec),
/// `createIndex`, `dropIndex`, `TruncateTable`, and `renameTable` /
/// `renameTables` over `stmt`, against `catalog` (Go
/// `e.infoCache.GetLatest()`). DROP TABLE answers its own
/// (`check_cluster_drop_object`). A table that does not exist is left to the
/// statement, which reports it.
pub fn check_cached_table_ddl(
    catalog: &ClusterCatalog,
    current_db: &str,
    stmt: &tidb_ast::Stmt,
) -> Result<(), MysqlError> {
    let tidb_ast::Stmt::Ddl(ddl) = stmt else {
        return Ok(());
    };
    let cached = |path: &[String]| {
        let (database, name) = match path {
            [database, name] => (database.as_str(), name.as_str()),
            [name] => (current_db, name.as_str()),
            _ => return false,
        };
        catalog.find_table(database, name).is_some_and(|(_, table)| {
            table.table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE
        })
    };
    let refused = |operation: &str| {
        Err(tidb_error::mysql::MysqlError::from_terror(
            &tidb_util::dbterror::ERR_OPT_ON_CACHE_TABLE
                .generate_with_stack_by_args(&[(operation).into()]),
        ))
    };
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
