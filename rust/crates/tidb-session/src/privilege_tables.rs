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

//! This session's own `mysql.*` tables as `tidb_exec`'s privilege row reader
//! reads them.

use tidb_executor::KvTableReader;

/// This session's own `mysql.*` tables, read the way
/// `tidb_privilege::cluster_privilege_load::load_privileges` reads a cluster's.
pub(crate) struct SessionPrivilegeTables<'a> {
    pub(crate) catalog: &'a mut tidb_executor::Catalog,
    /// TIMESTAMP columns decode in UTC, as the cluster reader decodes them.
    pub(crate) decode: tidb_table::kv_table::RowDecodeContext,
}

impl tidb_privilege::cluster_privilege_load::PrivilegeTables for SessionPrivilegeTables<'_> {
    fn rows(
        &mut self,
        table: &str,
        columns: &[&str],
        _timezone: Option<&tidb_datatype::SessionTimeZone>,
    ) -> Result<
        Option<tidb_privilege::cluster_privilege_load::PrivilegeTableRows>,
        tidb_syssession::mysql_system_tables::SystemTableError,
    > {
        let name = format!("mysql.{table}");
        let Some(tidb_executor::TableEntry::Kv(stored)) = self.catalog.table_mut_in("mysql", table)
        else {
            return Ok(None);
        };
        let stored = std::sync::Arc::make_mut(stored);
        let view = tidb_syssession::mysql_system_tables::SystemTableView::from_columns(
            &name,
            stored.table_id(),
            stored
                .columns()
                .iter()
                .map(|column| (column.name.clone(), column.id, column.field_type.clone())),
            columns,
        );
        let ids: Vec<i64> = stored.columns().iter().map(|column| column.id).collect();
        let rows = stored
            .scan_rows_with_context(&self.decode)
            .map_err(
                |error| tidb_syssession::mysql_system_tables::SystemTableError::Decode {
                    name: name.clone(),
                    detail: format!("{error:?}"),
                },
            )?;
        Ok(Some((
            view,
            rows.into_iter()
                .map(|row| ids.iter().copied().zip(row).collect())
                .collect(),
        )))
    }
}
