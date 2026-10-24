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

//! `mysql.tidb_ddl_reorg`, a reorganizing job's checkpoint: Go
//! `initDDLReorgHandle`, `updateDDLReorgHandle`, `getDDLReorgHandle` and
//! `removeDDLReorgHandle` (`pkg/ddl/job_scheduler.go`) as row mutations.

use tidb_datatype::Datum;
use tidb_model::TableInfo;
use tidb_txnkv::transaction::BufferMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::cluster_ddl::add_index::ReorgHandle;
use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};
use crate::system_row_write::{delete_row, insert_row, update_row, RowValues};

/// The located table.
pub struct DdlReorgTable {
    table: Box<TableInfo>,
    view: SystemTableView,
}

/// One stored row: its key, its values and the handle they hold.
pub struct StoredReorgRow {
    key: Vec<u8>,
    values: RowValues,
    /// The handle.
    pub handle: ReorgHandle,
    /// `job_id`.
    pub job_id: i64,
}

fn error(message: impl std::fmt::Display) -> String {
    message.to_string()
}

impl DdlReorgTable {
    /// Finds `mysql.tidb_ddl_reorg` in the catalog.
    pub fn locate(catalog: &ClusterCatalog) -> Result<Self, String> {
        let (_, table) = catalog
            .find_table("mysql", "tidb_ddl_reorg")
            .ok_or_else(|| "mysql.tidb_ddl_reorg does not exist".to_owned())?;
        let table = table.clone_like_go();
        let view = SystemTableView::project(
            "mysql.tidb_ddl_reorg",
            &table,
            &[
                "job_id",
                "ele_id",
                "ele_type",
                "start_key",
                "end_key",
                "physical_id",
                "reorg_meta",
            ],
        );
        Ok(Self {
            table: Box::new(table),
            view,
        })
    }

    /// The table's metadata, for allocating a row id.
    #[must_use]
    pub fn table(&self) -> &TableInfo {
        &self.table
    }

    fn column_id(&self, column: &str) -> Result<i64, String> {
        self.table
            .columns
            .iter_deref()
            .find(|candidate| candidate.read().name.lowercase() == column)
            .map(|candidate| candidate.read().id)
            .ok_or_else(|| format!("mysql.tidb_ddl_reorg has no column `{column}`"))
    }

    /// Go `getDDLReorgHandle`: the job's rows.
    pub fn load<S: MetaSnapshot>(
        &self,
        snapshot: &mut S,
        job_id: i64,
    ) -> Result<Vec<StoredReorgRow>, String> {
        let mut rows = Vec::new();
        for (key, value) in scan_system_table(snapshot, &self.view).map_err(error)? {
            let row = SystemRow::parse(&self.view, &key, &value).map_err(error)?;
            let Some(row_job) = row.i64("job_id").map_err(error)? else {
                continue;
            };
            if row_job != job_id {
                continue;
            }
            let handle = ReorgHandle {
                element_id: row.i64("ele_id").map_err(error)?.unwrap_or(0),
                element_type: row.bytes("ele_type").map_err(error)?.unwrap_or_default(),
                start_key: row.bytes("start_key").map_err(error)?.unwrap_or_default(),
                end_key: row.bytes("end_key").map_err(error)?.unwrap_or_default(),
                physical_id: row.i64("physical_id").map_err(error)?.unwrap_or(0),
            };
            rows.push(StoredReorgRow {
                key: key.to_vec(),
                values: row.into_values(),
                handle,
                job_id,
            });
        }
        Ok(rows)
    }

    /// Go `initDDLReorgHandle`: the job's rows replaced by one at `row_id`.
    pub fn init_mutations(
        &self,
        existing: &[StoredReorgRow],
        job_id: i64,
        handle: &ReorgHandle,
        row_id: i64,
    ) -> Result<Vec<BufferMutation>, String> {
        let mut mutations = self.delete_mutations(existing)?;
        let mut values = RowValues::new();
        values.insert(self.column_id("job_id")?, Datum::Int(job_id));
        values.insert(self.column_id("ele_id")?, Datum::Int(handle.element_id));
        values.insert(
            self.column_id("ele_type")?,
            Datum::Bytes(handle.element_type.clone()),
        );
        values.insert(
            self.column_id("start_key")?,
            Datum::Bytes(handle.start_key.clone()),
        );
        values.insert(
            self.column_id("end_key")?,
            Datum::Bytes(handle.end_key.clone()),
        );
        values.insert(
            self.column_id("physical_id")?,
            Datum::Int(handle.physical_id),
        );
        // Go stores an ingest checkpoint naming the physical table.
        values.insert(
            self.column_id("reorg_meta")?,
            Datum::Bytes(
                format!(
                    "{{\"checkpoint\":{{\"physical_id\":{},\"version\":1}}}}",
                    handle.physical_id
                )
                .into_bytes(),
            ),
        );
        mutations.extend(insert_row(&self.table, row_id, &values).map_err(error)?);
        Ok(mutations)
    }

    /// Go `updateDDLReorgHandle`: the row's element, keys and physical table.
    pub fn update_mutation(
        &self,
        row: &StoredReorgRow,
        handle: &ReorgHandle,
    ) -> Result<Vec<BufferMutation>, String> {
        let mut values = row.values.clone();
        values.insert(self.column_id("ele_id")?, Datum::Int(handle.element_id));
        values.insert(
            self.column_id("ele_type")?,
            Datum::Bytes(handle.element_type.clone()),
        );
        values.insert(
            self.column_id("start_key")?,
            Datum::Bytes(handle.start_key.clone()),
        );
        values.insert(
            self.column_id("end_key")?,
            Datum::Bytes(handle.end_key.clone()),
        );
        values.insert(
            self.column_id("physical_id")?,
            Datum::Int(handle.physical_id),
        );
        // Go `UPDATE mysql.tidb_ddl_reorg`: the row keeps its row id, and
        // the unique key (job_id, ele_id, ele_type) entry moves only when a
        // new element changes it.
        update_row(&self.table, &row.key, &row.values, &values).map_err(error)
    }

    /// Go `removeDDLReorgHandle`.
    pub fn delete_mutations(
        &self,
        existing: &[StoredReorgRow],
    ) -> Result<Vec<BufferMutation>, String> {
        let mut mutations = Vec::new();
        for row in existing {
            mutations.extend(delete_row(&self.table, &row.key, &row.values).map_err(error)?);
        }
        Ok(mutations)
    }
}

impl From<SystemTableError> for crate::cluster_ddl::DdlPlanError {
    fn from(error: SystemTableError) -> Self {
        Self::Encode(error.to_string())
    }
}
