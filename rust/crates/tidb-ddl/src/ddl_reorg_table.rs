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
//! `initDDLReorgHandle`, `updateDDLReorgHandle` (in `UpdateReorgMeta`) and
//! `cleanDDLReorgHandles` (`pkg/ddl/job_scheduler.go`, `reorg.go`) as Go's
//! statements on a pooled session, and `getDDLReorgHandle`. A worker whose
//! pool has no sessions yet stages the same rows as mutations.

use tidb_datatype::Datum;
use tidb_model::TableInfo;
use tidb_txnkv::transaction::BufferMutation;

use tidb_infoschema::cluster_catalog::MetaSnapshot;
use tidb_infoschema::SessionExtendedInfoSchema;
use crate::cluster_ddl::add_index::ReorgHandle;
use tidb_syssession::mysql_system_tables::{scan_system_table, SystemRow, SystemTableView};
use tidb_syssession::system_row_write::{delete_row, insert_row, update_row, RowValues};

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
    pub fn locate(catalog: &SessionExtendedInfoSchema) -> Result<Self, String> {
        let (_, table) = catalog
            .table_info_by_name("mysql", "tidb_ddl_reorg")
            .ok_or_else(|| "mysql.tidb_ddl_reorg does not exist".to_owned())?;
        let table = table.read();
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
    pub fn init_ddl_reorg_handle(
        &self,
        existing: &[StoredReorgRow],
        job_id: i64,
        handle: &ReorgHandle,
        row_id: i64,
    ) -> Result<Vec<BufferMutation>, String> {
        let mut mutations = self.remove_ddl_reorg_handle(existing)?;
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
            Datum::Bytes(job_reorg_meta(handle.physical_id)),
        );
        mutations.extend(insert_row(&self.table, row_id, &values).map_err(error)?);
        Ok(mutations)
    }

    /// Go `updateDDLReorgHandle`: the row's element, keys and physical table.
    pub fn update_ddl_reorg_handle(
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
    pub fn remove_ddl_reorg_handle(
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

/// Go `ingest.JobReorgMeta{Checkpoint: &ReorgCheckpoint{PhysicalID,
/// Version: JobCheckpointVersionCurrent}}` as Go's `json.Marshal` writes it:
/// every field, nil keys as `null`.
#[must_use]
pub fn job_reorg_meta(physical_id: i64) -> Vec<u8> {
    format!(
        "{{\"reorg_checkpoint\":{{\"local_sync_key\":null,\"local_key_count\":0,\"global_sync_key\":null,\"global_key_count\":0,\"instance_addr\":\"\",\"physical_id\":{physical_id},\"ts\":0,\"version\":1}}}}"
    )
    .into_bytes()
}

/// Go `sess.Session.RunInTxn`: `f` in one transaction of `s`, rolled back
/// when it fails.
fn run_in_txn(
    s: &mut dyn tidb_ddl_build::session_pool::StepSqlSession,
    f: impl FnOnce(
        &mut dyn tidb_ddl_build::session_pool::StepSqlSession,
    ) -> Result<(), tidb_terror::MysqlError>,
) -> Result<(), tidb_terror::MysqlError> {
    // Go `Begin`: `sessiontxn.NewTxn` enters an optimistic transaction.
    s.begin()?;
    if let Err(error) = f(s) {
        s.rollback();
        return Err(error);
    }
    s.commit()
}

/// Go `initDDLReorgHandle`: in one transaction, the job's rows replaced by
/// the handle's.
///
/// # Errors
///
/// The insert's or the transaction's error.
pub fn init_ddl_reorg_handle(
    s: &mut dyn tidb_ddl_build::session_pool::StepSqlSession,
    job_id: i64,
    handle: &ReorgHandle,
) -> Result<(), tidb_terror::MysqlError> {
    use tidb_ddl_jobsubmit::wrap_key_2_string;
    let raw_reorg_meta = job_reorg_meta(handle.physical_id);
    let del = format!("delete from mysql.tidb_ddl_reorg where job_id = {job_id}");
    let ins = format!(
        "insert into mysql.tidb_ddl_reorg(job_id, ele_id, ele_type, start_key, end_key, physical_id, reorg_meta) values ({}, {}, {}, {}, {}, {}, {})",
        job_id,
        handle.element_id,
        wrap_key_2_string(&handle.element_type),
        wrap_key_2_string(&handle.start_key),
        wrap_key_2_string(&handle.end_key),
        handle.physical_id,
        wrap_key_2_string(&raw_reorg_meta),
    );
    run_in_txn(s, |se| {
        if let Err(error) = se.execute(&del, &[]) {
            tidb_ddl_logutil::ddl_logger().info(
                "initDDLReorgHandle failed to delete",
                &[
                    tidb_log::Field::new("jobID", tidb_log::Value::I64(job_id)),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                ],
            );
        }
        se.execute(&ins, &[]).map(|_| ())
    })
}

/// Go `reorgInfo.UpdateReorgMeta`: `updateDDLReorgHandle` in a transaction
/// of its own, committed even when the update failed.
///
/// # Errors
///
/// The update's error, else the commit's.
pub fn update_reorg_meta(
    se: &mut dyn tidb_ddl_build::session_pool::StepSqlSession,
    job_id: i64,
    handle: &ReorgHandle,
) -> Result<(), tidb_terror::MysqlError> {
    use tidb_ddl_jobsubmit::wrap_key_2_string;
    if handle.start_key.is_empty() && handle.end_key.is_empty() {
        return Ok(());
    }
    se.begin()?;
    // Go `updateDDLReorgHandle`.
    let sql = format!(
        "update mysql.tidb_ddl_reorg set ele_id = {}, ele_type = {}, start_key = {}, end_key = {}, physical_id = {} where job_id = {}",
        handle.element_id,
        wrap_key_2_string(&handle.element_type),
        wrap_key_2_string(&handle.start_key),
        wrap_key_2_string(&handle.end_key),
        handle.physical_id,
        job_id,
    );
    let updated = se.execute(&sql, &[]).map(|_| ());
    let committed = se.commit();
    updated.and(committed)
}

/// Go `cleanDDLReorgHandles`.
///
/// # Errors
///
/// The delete's or the transaction's error.
pub fn clean_ddl_reorg_handles(
    se: &mut dyn tidb_ddl_build::session_pool::StepSqlSession,
    job_id: i64,
) -> Result<(), tidb_terror::MysqlError> {
    let sql = format!("delete from mysql.tidb_ddl_reorg where job_id = {job_id}");
    run_in_txn(se, |se| se.execute(&sql, &[]).map(|_| ()))
}

#[cfg(test)]
mod tests {
    /// The bytes Go's `json.Marshal` writes for
    /// `ingest.JobReorgMeta{Checkpoint: &ReorgCheckpoint{PhysicalID: 112,
    /// Version: JobCheckpointVersionCurrent}}` (printed by a Go program over
    /// `pkg/ddl/ingest`). Go's `getImportedKeyFromCheckpoint` reads
    /// `reorg_checkpoint`; the old `{"checkpoint":...}` was invisible to it.
    #[test]
    fn job_reorg_meta_is_gos_json() {
        assert_eq!(
            String::from_utf8(super::job_reorg_meta(112)).unwrap(),
            r#"{"reorg_checkpoint":{"local_sync_key":null,"local_key_count":0,"global_sync_key":null,"global_key_count":0,"instance_addr":"","physical_id":112,"ts":0,"version":1}}"#
        );
    }
}
