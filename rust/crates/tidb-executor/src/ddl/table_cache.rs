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
    match mode {
        tidb_ast::AlterTableCacheMode::Cache => {
            if table.is_cached() {
                return Ok(());
            }
            if tidb_metadef::is_mem_or_sys_db(&database.to_ascii_lowercase()) {
                return Err(DriverError::UnsupportedAlterCacheForSystemTable);
            }
            table.enable_cache().map_err(|error| match error {
                crate::kv_table::KvTableError::CacheTableUnsupported(operation) => {
                    DriverError::OperationOnCachedTable(operation)
                }
                crate::kv_table::KvTableError::Storage(message)
                    if message.starts_with("Retryable(") =>
                {
                    DriverError::Txn(crate::TxnErrorKind::RegionUnavailable)
                }
                other => {
                    DriverError::unsupported(format!("cache table size check failed: {other:?}"))
                }
            })?;
        }
        tidb_ast::AlterTableCacheMode::NoCache => table.disable_cache(),
    }
    Ok(())
}
