// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/index.go`'s distributed add-index task controls.
use tidb_model::GoShared;
use tidb_model::job::{JOB_PAUSE_REASON_KV_DISK_FULL, JOB_RESUME_REASON_KV_DISK_FULL};
use tidb_model::{AdminCommandOperator, Job, JobState};

/// Go `autoPauseAddIndexJobOnKVDiskFull`.
pub fn auto_pause_add_index_job_on_kv_disk_full(
    job: &mut Job,
    task_id: i64,
    task_err: Option<&(dyn std::error::Error + 'static)>,
) -> tidb_terror::TerrorError {
    let store_type = kv_disk_full_store_type(task_err);
    let mut message = format!("DXF add-index task {task_id} hit {store_type} disk full");
    if let Some(error) = task_err {
        message.push_str(": ");
        message.push_str(&error.to_string());
    }
    job.state = JobState::PAUSING;
    job.admin_operator = AdminCommandOperator::BY_SYSTEM;
    job.set_pause_reason(JOB_PAUSE_REASON_KV_DISK_FULL, message.clone());
    job.clear_resume_reason();
    job.error = Some(GoShared::new(
        tidb_dbterror::ERR_DDL_AUTO_PAUSED_BY_KV_DISK_FULL
            .fast_gen_by_args(&[job.id.into(), message.clone().into()]),
    ));
    tidb_dbterror::ERR_DDL_AUTO_PAUSED_BY_KV_DISK_FULL
        .gen_with_stack_by_args(&[job.id.into(), message.into()])
}
/// Go `kvDiskFullStoreType`.
pub fn kv_disk_full_store_type(
    task_err: Option<&(dyn std::error::Error + 'static)>,
) -> &'static str {
    let message = task_err
        .map(|e| e.to_string().to_lowercase())
        .unwrap_or_default();
    if message.contains("tiflash") {
        "TiFlash"
    } else if message.contains("tikv") {
        "TiKV"
    } else {
        "storage node"
    }
}
/// Go `shouldAutoPauseExistingKVDiskFullTask`.
pub fn should_auto_pause_existing_kv_disk_full_task(job: &Job, task: &tidb_dxf::Task) -> bool {
    task.base.state == tidb_dxf::TASK_STATE_PAUSED
        && task
            .error
            .as_ref()
            .is_some_and(|e| tidb_ingestor_errdef::is_kv_disk_full_error(e.as_ref()))
        && !job.has_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL)
}
#[cfg(test)]
mod tests {
    use super::*;
    /// Go `pkg/ddl/index_nokit_test.go:TestShouldAutoPauseExistingKVDiskFullTask`.
    #[test]
    fn test_should_auto_pause_existing_kv_disk_full_task() {
        let mut task = tidb_dxf::Task {
            base: tidb_dxf::TaskBase {
                id: 123,
                state: tidb_dxf::TASK_STATE_PAUSED,
                ..Default::default()
            },
            error: Some(std::sync::Arc::new(
                tidb_ingestor_errdef::ERR_KV_DISK_FULL.gen_with_stack("store 1 disk full"),
            )),
            ..Default::default()
        };
        let mut job = Job::default();
        job.id = 456;
        assert!(should_auto_pause_existing_kv_disk_full_task(&job, &task));
        job.set_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL);
        assert!(!should_auto_pause_existing_kv_disk_full_task(&job, &task));
        job.clear_resume_reason();
        task.base.state = tidb_dxf::TASK_STATE_RUNNING;
        assert!(!should_auto_pause_existing_kv_disk_full_task(&job, &task));
        task.base.state = tidb_dxf::TASK_STATE_PAUSED;
        task.error = Some(std::sync::Arc::new(std::io::Error::other("not disk full")));
        assert!(!should_auto_pause_existing_kv_disk_full_task(&job, &task));
        job.set_resume_reason(JOB_RESUME_REASON_KV_DISK_FULL);
        let err=tidb_ingestor_errdef::ERR_KV_DISK_FULL.gen_with_stack("the remaining storage capacity of TiFlash(127.0.0.1:3930) is less than 10%; please increase the storage capacity of TiFlash and try again");
        let err = auto_pause_add_index_job_on_kv_disk_full(&mut job, task.base.id, Some(&err));
        assert!(tidb_dbterror::ERR_DDL_AUTO_PAUSED_BY_KV_DISK_FULL.equal(Some(&err)));
        assert!(err.to_string().contains("TiFlash disk full"));
        assert!(!err.to_string().contains("because TiKV disk is full"));
        assert!(!err.to_string().contains("hit TiKV disk full"));
        assert!(job.is_pausing_or_paused_by_system_for_kv_disk_full());
        assert!(
            job.pause_reason
                .as_ref()
                .unwrap()
                .read()
                .message
                .to_string()
                .contains("TiFlash disk full")
        );
        assert!(job.resume_reason.is_none());
    }
}

/// Go `isRetryableError`, with the original DDL message and SQL-code tables.
pub(crate) fn is_retryable_error(
    error: &(dyn std::error::Error + 'static),
    retry_unknown: bool,
) -> bool {
    if tidb_dbterror::REORG_RETRYABLE_ERR_MSGS
        .iter()
        .any(|text| error.to_string().contains(text))
    {
        return true;
    }
    let mut cause = error;
    while let Some(source) = cause.source() {
        cause = source;
    }
    if let Some(error) = cause.downcast_ref::<tidb_terror::TerrorError>() {
        return tidb_dbterror::REORG_RETRYABLE_ERR_CODES
            .contains(&tidb_terror::MysqlError::from_terror(error).code);
    }
    if let Some(error) = cause.downcast_ref::<tidb_terror::MysqlError>() {
        return tidb_dbterror::REORG_RETRYABLE_ERR_CODES.contains(&error.code);
    }
    retry_unknown
}

/// Go `TaskKeyBuilder`.
pub struct TaskKeyBuilder {
    multi_schema_seq: i32,
    merge_temp_idx: bool,
}
impl Default for TaskKeyBuilder {
    fn default() -> Self {
        Self::new()
    }
}
impl TaskKeyBuilder {
    /// Go `NewTaskKeyBuilder`.
    pub fn new() -> Self {
        Self {
            multi_schema_seq: -1,
            merge_temp_idx: false,
        }
    }
    /// Go `TaskKeyBuilder.SetMergeTempIndex`.
    pub fn set_merge_temp_index(mut self, flag: bool) -> Self {
        self.merge_temp_idx = flag;
        self
    }
    /// Go `TaskKeyBuilder.SetMultiSchema`.
    pub fn set_multi_schema(mut self, info: Option<&tidb_model::MultiSchemaInfo>) -> Self {
        if let Some(info) = info {
            self.multi_schema_seq = info.seq;
        }
        self
    }
    /// Go `TaskKeyBuilder.Build`.
    pub fn build(&self, job_id: i64) -> String {
        let mut labels = Vec::new();
        if tidb_config_kerneltype::is_next_gen() {
            labels.push(tidb_keyspace::get_keyspace_name_by_settings());
        }
        labels.extend([
            "ddl".to_owned(),
            tidb_dxf::BACKFILL.to_string(),
            job_id.to_string(),
        ]);
        if self.multi_schema_seq >= 0 {
            labels.push(self.multi_schema_seq.to_string());
        }
        if self.merge_temp_idx {
            labels.push("merge".to_owned());
        }
        labels.join("/")
    }
}
