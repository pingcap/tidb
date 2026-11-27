// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ddl/index.go`'s distributed add-index task controls.
use tidb_model::GoShared;

/// Go `MockDMLExecution`: the hook the `mockDMLExecution` failpoint calls
/// after a backfill or global-index cleanup batch, test-only.
pub static MOCK_DML_EXECUTION: std::sync::Mutex<Option<Box<dyn Fn() + Send>>> = std::sync::Mutex::new(None);

/// Go `failpoint.Inject("mockDMLExecution", ...)`: `MockDMLExecution` runs
/// when the failpoint answers true.
pub(crate) fn inject_mock_dml_execution() {
    tidb_failpoint::inject("github.com/pingcap/tidb/pkg/ddl/mockDMLExecution", |value| {
        if value.as_bool() == Some(true) {
            if let Some(hook) = MOCK_DML_EXECUTION.lock().unwrap_or_else(std::sync::PoisonError::into_inner).as_ref() {
                hook();
            }
        }
    });
}
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

use tidb_log::{Field, Value};

/// Go `UpdateDDLJobReorgCfgInterval` (a `var` tests shorten), in
/// milliseconds.
pub static UPDATE_DDL_JOB_REORG_CFG_INTERVAL_MS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(2000);

/// Go `ReorgMeta.GetConcurrency` / `GetBatchSize` over the job's reorg meta:
/// an unset value falls back to the global `vardef` counters.
pub fn reorg_defaults() -> tidb_model::DDLReorgProcessDefaults {
    tidb_model::DDLReorgProcessDefaults::new(
        || i64::from(tidb_vardef::get_ddl_reorg_worker_counter()),
        || i64::from(tidb_vardef::get_ddl_reorg_batch_size()),
    )
}

/// Go `modifyTaskParamLoop`: every `UpdateDDLJobReorgCfgInterval` until
/// `done`, the job's latest reorg config (thread, batch size, max write
/// speed) is carried to its DXF task through `ModifyTaskByID`.
///
/// Go notes that `ADMIN ALTER DDL JOB` could call `ModifyTaskByID` itself,
/// but a task not yet created must still start with the altered config, so
/// the DDL side polls.
#[allow(clippy::too_many_arguments)]
pub fn modify_task_param_loop(
    ctx: &tidb_gocontext::Context,
    sys_tbl_mgr: &dyn tidb_ddl_systable::Manager,
    task_manager: &dyn tidb_dxf_storage::Manager,
    done: &crossbeam_channel::Receiver<()>,
    job_id: i64,
    task_id: i64,
    mut last_required_slots: i64,
    mut last_batch_size: i64,
    mut last_max_write_speed: i64,
) {
    let logger = tidb_ddl_logutil::ddl_logger().with_fields(&[
        Field::new("jobID", Value::I64(job_id)),
        Field::new("taskID", Value::I64(task_id)),
    ]);
    loop {
        let interval = std::time::Duration::from_millis(
            UPDATE_DDL_JOB_REORG_CFG_INTERVAL_MS.load(std::sync::atomic::Ordering::Relaxed),
        );
        crossbeam_channel::select! {
            recv(done) -> _ => return,
            recv(crossbeam_channel::after(interval)) -> _ => {}
        }

        let latest_job = match sys_tbl_mgr.get_job_by_id(job_id) {
            Ok(job) => job,
            Err(tidb_ddl_systable::SystemTableManagerError::NotFound) => {
                logger.info("job not found, might already finished", &[]);
                return;
            }
            Err(error) => {
                logger.error("get job failed, will retry later", &[zap_error(&error)]);
                continue;
            }
        };
        let latest_job = latest_job.job.expect("GetJobByID returns a job");
        let latest_job = latest_job.read();
        let reorg_meta = latest_job.reorg_meta.as_ref().expect("a DXF job has reorg meta").read();

        let mut modifies = Vec::with_capacity(3);
        let worker_cnt_limit = reorg_meta.get_concurrency(reorg_defaults());
        let required_slots = match adjust_concurrency(ctx, task_manager, worker_cnt_limit) {
            Ok(slots) => slots,
            Err(error) => {
                logger.error("adjust required slots failed", &[zap_error(&error)]);
                continue;
            }
        };
        if required_slots != last_required_slots {
            modifies.push(tidb_dxf::Modification {
                tp: tidb_dxf::MODIFY_REQUIRED_SLOTS,
                to: required_slots,
            });
        }
        let batch_size = reorg_meta.get_batch_size(reorg_defaults());
        if batch_size != last_batch_size {
            modifies.push(tidb_dxf::Modification { tp: tidb_dxf::MODIFY_BATCH_SIZE, to: batch_size });
        }
        let max_write_speed = reorg_meta.get_max_write_speed();
        if max_write_speed != last_max_write_speed {
            modifies.push(tidb_dxf::Modification {
                tp: tidb_dxf::MODIFY_MAX_WRITE_SPEED,
                to: max_write_speed,
            });
        }
        drop(reorg_meta);
        drop(latest_job);
        if modifies.is_empty() {
            continue;
        }
        let curr_task = match task_manager.get_task_by_id(ctx, task_id) {
            Ok(task) => task,
            Err(error) if error.is(&tidb_dxf_storage::StorageError::TaskNotFound) => {
                logger.info("task not found, might already finished", &[]);
                return;
            }
            Err(error) => {
                logger.error("get task failed, will retry later", &[zap_error(&error)]);
                continue;
            }
        };
        if !curr_task.base.state.can_move_to_modifying() {
            // user might modify param again while another modify is ongoing.
            logger.info(
                "task state is not suitable for modifying, will retry later",
                &[Field::new("state", Value::Str(curr_task.base.state.to_string()))],
            );
            continue;
        }
        if let Err(error) = task_manager.modify_task_by_id(
            ctx,
            task_id,
            &tidb_dxf::ModifyParam { prev_state: curr_task.base.state, modifications: modifies },
        ) {
            logger.error("modify task failed", &[zap_error(&error)]);
            continue;
        }
        logger.info(
            "modify task success",
            &[
                Field::new("oldRequiredSlots", Value::I64(last_required_slots)),
                Field::new("newRequiredSlots", Value::I64(required_slots)),
                Field::new("oldBatchSize", Value::I64(last_batch_size)),
                Field::new("newBatchSize", Value::I64(batch_size)),
                Field::new("oldMaxWriteSpeed", Value::Str(human_size(last_max_write_speed as f64))),
                Field::new("newMaxWriteSpeed", Value::Str(human_size(max_write_speed as f64))),
            ],
        );
        last_required_slots = required_slots;
        last_batch_size = batch_size;
        last_max_write_speed = max_write_speed;
    }
}

/// Go `estimateTableRowSize`: the table's `AVG_ROW_LENGTH` from
/// `information_schema.tables`, else [`estimate_row_size_from_region`]; 0
/// (logged) when neither answers.
pub fn estimate_table_row_size<S: tikv_client::PdClient>(
    store: &tidb_txnkv::driver::tikv_store::TikvStore<S>,
    exec: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    table_id: i64,
) -> i64 {
    let estimate = || -> Result<i64, String> {
        let rows = exec
            .exec_restricted_sql_with(
                "select AVG_ROW_LENGTH from information_schema.tables where TIDB_TABLE_ID = %?",
                &[tidb_sqlescape::SqlArg::Signed(table_id)],
            )
            .map_err(|error| error.to_string())?;
        let Some(row) = rows.first() else {
            return Err("no average row data".to_owned());
        };
        let avg_row_size = row.first().map_or(0, tidb_datatype::Datum::get_int64);
        if avg_row_size != 0 {
            return Ok(avg_row_size);
        }
        estimate_row_size_from_region(store, table_id)
    };
    // Go `defer util.Recover(...)`: a panic estimates 0.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(estimate))
        .unwrap_or_else(|_| Err("panic in estimateTableRowSize".to_owned()));
    let (size, error) = match result {
        Ok(size) => (size, None),
        Err(error) => (0, Some(error)),
    };
    let mut fields = vec![
        Field::new("tableID", Value::I64(table_id)),
        Field::new("size", Value::I64(size)),
    ];
    if let Some(error) = error {
        fields.push(zap_error(&error));
    }
    tidb_ddl_logutil::ddl_logger().info("estimate row size", &fields);
    size
}

/// Go `estimateRowSizeFromRegion`: PD's approximate size per key of the
/// table's second record region (the first and last may hold neighbours).
fn estimate_row_size_from_region<S: tikv_client::PdClient>(
    store: &tidb_txnkv::driver::tikv_store::TikvStore<S>,
    table_id: i64,
) -> Result<i64, String> {
    let pd_cli = store
        .pd_http_client()
        .ok_or_else(|| "pd http client is not available".to_owned())?;
    let (sk, ek) = tidb_tablecodec::table_key::get_table_handle_key_range(table_id);
    let (start, end) = match store.store().keyspace() {
        tikv_client::request::Keyspace::Enable { keyspace_id } => {
            tikv_client::request::ApiV2Codec::new(tikv_client::request::KeyMode::Txn, keyspace_id)
                .map_err(|error| error.to_string())?
                .encode_region_range(&sk, &ek)
        }
        _ => tikv_client::request::ApiV1Codec::new(tikv_client::request::KeyMode::Txn)
            .encode_region_range(&sk, &ek),
    };
    // We use the second region to prevent the influence of the front and back tables.
    let region_limit = 3;
    let region_infos = tidb_store_driver_txn::cluster_http::block_on_pd_http(
        pd_cli.get_regions_by_key_range(&tikv_client::pd_http::KeyRange::new(start, end), region_limit),
    )?;
    if region_infos.regions.len() != region_limit as usize {
        return Err("less than 3 regions".to_owned());
    }
    let sample = &region_infos.regions[1];
    // ApproximateSize is SST/blob file size (can reflect compression), while
    // ApproximateKvSize is KV data size and usually closer to logical table size.
    let size_in_mib = sample.approximate_size.max(sample.approximate_kv_size);
    if sample.approximate_keys == 0 || size_in_mib == 0 {
        return Err("zero approximate size".to_owned());
    }
    Ok(((size_in_mib as u64) * (1 << 20)) as i64 / sample.approximate_keys)
}

/// zap `zap.Error(err)`.
fn zap_error(error: &dyn std::fmt::Display) -> Field {
    Field::new("error", Value::Str(error.to_string()))
}

/// go-units `HumanSize`: `%.4g` of the size in decimal (1000) units.
fn human_size(mut size: f64) -> String {
    const ABBRS: [&str; 9] = ["B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    let mut i = 0;
    while size >= 1000.0 && i < ABBRS.len() - 1 {
        size /= 1000.0;
        i += 1;
    }
    format!("{}{}", tidb_goruntime::go_strconv::format_float(size, b'g', 4, 64), ABBRS[i])
}

/// Go `adjustConcurrency`.
///
/// # Errors
///
/// `GetCPUCountOfNode`'s error.
pub fn adjust_concurrency(
    ctx: &tidb_gocontext::Context,
    task_mgr: &dyn tidb_dxf_storage::Manager,
    worker_cnt: i64,
) -> tidb_dxf_storage::Result<i64> {
    let cpu_count = task_mgr.get_cpu_count_of_node(ctx)?;
    Ok(worker_cnt.min(cpu_count as i64))
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

    /// One scripted gomock expectation of `TestModifyTaskParamLoop`, in call
    /// order.
    enum Call {
        GetJobByID(Result<i64, tidb_ddl_systable::SystemTableManagerError>),
        GetCpuCount(tidb_dxf_storage::Result<isize>),
        GetTaskByID(tidb_dxf_storage::Result<tidb_dxf::TaskState>),
        ModifyTaskByID(Vec<(tidb_dxf::ModificationType, i64)>, tidb_dxf_storage::Result<()>),
    }

    /// Go `ddlmock.MockManager` and `mock.MockManager` under one controller:
    /// each call must be the next expectation (`GetJobByID` answers a job
    /// whose reorg meta is `jobs[i]`).
    struct Script {
        calls: std::sync::Mutex<std::collections::VecDeque<Call>>,
        jobs: Vec<(i64, i64, i64)>,
    }
    impl Script {
        fn next(&self) -> Call {
            self.calls.lock().unwrap().pop_front().expect("unexpected call")
        }
        fn satisfied(&self) -> bool {
            self.calls.lock().unwrap().is_empty()
        }
    }
    impl tidb_ddl_systable::Manager for Script {
        fn get_job_by_id(&self, job_id: i64) -> Result<tidb_model::JobW, tidb_ddl_systable::SystemTableManagerError> {
            assert_eq!(job_id, 1);
            let Call::GetJobByID(result) = self.next() else { panic!("expected GetJobByID") };
            result.map(|i| {
                let (concurrency, batch_size, speed) = self.jobs[i as usize];
                let meta = tidb_model::DDLReorgMeta::default();
                meta.set_concurrency(concurrency);
                meta.set_batch_size(batch_size);
                meta.set_max_write_speed(speed);
                let mut job = Job::default();
                job.reorg_meta = Some(GoShared::new(meta));
                tidb_model::JobW { job: Some(GoShared::new(job)), bytes: Default::default() }
            })
        }
        fn get_job_bytes_by_id_with_se(&self, _: &tidb_ddl_systable::DdlSession, _: i64) -> Result<Vec<u8>, tidb_ddl_systable::SystemTableManagerError> {
            unreachable!()
        }
        fn get_mdl_ver(&self, _: i64) -> Result<i64, tidb_ddl_systable::SystemTableManagerError> {
            unreachable!()
        }
        fn get_min_job_id(&self, _: i64) -> Result<i64, tidb_ddl_systable::SystemTableManagerError> {
            unreachable!()
        }
        fn has_flashback_cluster_job(&self, _: i64) -> Result<bool, tidb_ddl_systable::SystemTableManagerError> {
            unreachable!()
        }
    }
    impl tidb_dxf_storage::Manager for Script {
        fn get_cpu_count_of_node(&self, _: &tidb_gocontext::Context) -> tidb_dxf_storage::Result<isize> {
            let Call::GetCpuCount(result) = self.next() else { panic!("expected GetCPUCountOfNode") };
            result
        }
        fn get_task_by_id(&self, _: &tidb_gocontext::Context, task_id: i64) -> tidb_dxf_storage::Result<tidb_dxf::Task> {
            assert_eq!(task_id, 1);
            let Call::GetTaskByID(result) = self.next() else { panic!("expected GetTaskByID") };
            result.map(|state| tidb_dxf::Task {
                base: tidb_dxf::TaskBase { state, ..Default::default() },
                ..Default::default()
            })
        }
        fn modify_task_by_id(&self, _: &tidb_gocontext::Context, task_id: i64, param: &tidb_dxf::ModifyParam) -> tidb_dxf_storage::Result<()> {
            assert_eq!(task_id, 1);
            let Call::ModifyTaskByID(expected, result) = self.next() else { panic!("expected ModifyTaskByID") };
            assert_eq!(param.prev_state, tidb_dxf::TASK_STATE_RUNNING);
            let got: Vec<_> = param.modifications.iter().map(|m| (m.tp.clone(), m.to)).collect();
            assert_eq!(got, expected);
            result
        }
    }

    /// Go `pkg/ddl/index_nokit_test.go:TestModifyTaskParamLoop`.
    #[test]
    fn test_modify_task_param_loop() {
        use tidb_ddl_systable::SystemTableManagerError::{NotFound, Sql};
        use tidb_dxf_storage::StorageError::{Other, TaskNotFound};
        use Call::*;
        UPDATE_DDL_JOB_REORG_CFG_INTERVAL_MS.store(10, std::sync::atomic::Ordering::Relaxed);
        // jobs[0] is Go's currentJob, jobs[1] its modifiedJob, jobs[2] modifiedJob2.
        let jobs = vec![(1, 2, 3), (4, 5, 6), (7, 8, 9)];
        let err = || Other("some error".to_owned());
        let modify = |slots, batch, speed| {
            vec![
                (tidb_dxf::MODIFY_REQUIRED_SLOTS, slots),
                (tidb_dxf::MODIFY_BATCH_SIZE, batch),
                (tidb_dxf::MODIFY_MAX_WRITE_SPEED, speed),
            ]
        };
        let run = |calls: Vec<Call>, close_done: bool| {
            let script = Script { calls: std::sync::Mutex::new(calls.into()), jobs: jobs.clone() };
            let (done_tx, done) = crossbeam_channel::bounded::<()>(0);
            if close_done {
                drop(done_tx);
            } else {
                std::mem::forget(done_tx);
            }
            modify_task_param_loop(&tidb_gocontext::Context::background(), &script, &script, &done, 1, 1, 1, 2, 3);
            assert!(script.satisfied());
        };
        // return on done
        run(vec![], true);
        // retry on get job error; return on job not found
        run(vec![GetJobByID(Err(Sql("some error".to_owned()))), GetJobByID(Err(NotFound))], false);
        // adjust concurrency failed, retry
        run(vec![GetJobByID(Ok(0)), GetCpuCount(Err(err())), GetJobByID(Err(NotFound))], false);
        // nothing modified, retry
        run(vec![GetJobByID(Ok(0)), GetCpuCount(Ok(123)), GetJobByID(Err(NotFound))], false);
        // detect modify, but the task has done
        run(vec![GetJobByID(Ok(1)), GetCpuCount(Ok(123)), GetTaskByID(Err(TaskNotFound))], false);
        // detect modify, fail to get task, after retry, found task state is un-modifiable
        run(
            vec![
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)), GetTaskByID(Err(err())),
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)), GetTaskByID(Ok(tidb_dxf::TASK_STATE_CANCELLING)),
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)), GetTaskByID(Err(TaskNotFound)),
            ],
            false,
        );
        // detect modify, success after retry, and we update internal variable to avoid modify twice
        run(
            vec![
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)), GetTaskByID(Ok(tidb_dxf::TASK_STATE_RUNNING)),
                ModifyTaskByID(modify(4, 5, 6), Err(err())),
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)), GetTaskByID(Ok(tidb_dxf::TASK_STATE_RUNNING)),
                ModifyTaskByID(modify(4, 5, 6), Ok(())),
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)),
                GetJobByID(Err(NotFound)),
            ],
            false,
        );
        // modify twice, both success
        run(
            vec![
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)), GetTaskByID(Ok(tidb_dxf::TASK_STATE_RUNNING)),
                ModifyTaskByID(modify(4, 5, 6), Ok(())),
                GetJobByID(Ok(1)), GetCpuCount(Ok(123)),
                GetJobByID(Ok(2)), GetCpuCount(Ok(123)), GetTaskByID(Ok(tidb_dxf::TASK_STATE_RUNNING)),
                ModifyTaskByID(modify(7, 8, 9), Ok(())),
                GetJobByID(Ok(2)), GetCpuCount(Ok(123)),
                GetJobByID(Err(NotFound)),
            ],
            false,
        );
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
    // Transient PD TSO stream errors.
    if let Some(error) = cause.downcast_ref::<tikv_client::pd_errs::PdError>() {
        if error.definition() == tikv_client::pd_errs::ERR_CLIENT_CREATE_TSO_STREAM
            || error.definition() == tikv_client::pd_errs::ERR_CLIENT_TSO_STREAM_CLOSED
        {
            return true;
        }
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

#[cfg(test)]
mod pd_retry_tests {
    /// An error that annotates another, as Go `errors.Annotate` does.
    #[derive(Debug)]
    struct Wrapped(tikv_client::pd_errs::PdError);

    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "wrapped: {}", self.0)
        }
    }

    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    /// Go `TestIsRetryableError`'s PD rows (08e20a9ea20): transient TSO
    /// stream errors are retryable during reorg, wrapped or not; an
    /// unrelated PD error is not made retryable.
    #[test]
    fn test_pd_tso_stream_errors_are_retryable() {
        use tikv_client::pd_errs;
        for error in [
            pd_errs::client_create_tso_stream(pd_errs::RETRY_TIMEOUT_ERR),
            pd_errs::ERR_CLIENT_TSO_STREAM_CLOSED.error(),
        ] {
            assert!(super::is_retryable_error(&error, false), "{error}");
            assert!(super::is_retryable_error(&Wrapped(error.clone()), false), "{error}");
        }
        let unrelated = pd_errs::ERR_CLIENT_PROTO_UNMARSHAL.error();
        assert!(!super::is_retryable_error(&unrelated, false), "{unrelated}");
    }
}
