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
//! statements on a pooled session.

use crate::cluster_ddl::add_index::ReorgHandle;

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
