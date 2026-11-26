// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/job.go`: submitting an import as a job and its
//! DXF task.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_dxf::TaskBase;
use tidb_executor_importer::chunk_process::Chunk;
use tidb_executor_importer::Plan;
use tidb_gocontext::Context;

use crate::planner::LogicalPlan;
use crate::{plain, Error};

/// Go `SubmitStandaloneTask`: a task only this instance runs (an import
/// from its disk, or one without the distributed framework).
///
/// # Errors
/// The job or the task could not be created.
pub fn submit_standalone_task(
    ctx: &Context,
    plan: &Plan,
    stmt: &str,
    chunk_map: Option<BTreeMap<i32, Vec<Chunk>>>,
) -> Result<(i64, TaskBase), Error> {
    let instance = crate::import_store()?
        .local_server_info()
        .ok_or_else(|| plain("server info of this instance is not available"))?;
    do_submit_task(ctx, plan, stmt, Some(instance), chunk_map)
}

/// Go `SubmitTask`: a task any instance may run.
///
/// # Errors
/// The job or the task could not be created.
pub fn submit_task(ctx: &Context, plan: &Plan, stmt: &str) -> Result<(i64, TaskBase), Error> {
    do_submit_task(ctx, plan, stmt, None, None)
}

/// Go `ShouldUseAsyncPrepare`: whether the task prepares in DXF's prepare
/// mode. Only a next-gen global-sort import from files does; a starter
/// deployment's imports are small, and preparing them before submission
/// gives faster validation feedback.
#[must_use]
pub fn should_use_async_prepare(plan: &Plan) -> bool {
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/importinto/mockDisableAsyncPrepare").is_some() {
        return false;
    }
    plan.query_source.is_none()
        && tidb_config_kerneltype::is_next_gen()
        && !tidb_config_deploymode::is_starter()
        && plan.is_global_sort()
}

/// Go `TaskKey`.
#[must_use]
pub fn task_key(job_id: i64) -> String {
    tidb_dxf_importinto_taskkey::for_job(job_id)
}

/// Go `doSubmitTask`: the job row, the table's import mode and the task,
/// in one transaction.
fn do_submit_task(
    ctx: &Context,
    plan: &Plan,
    stmt: &str,
    instance: Option<serde_json::Value>,
    chunk_map: Option<BTreeMap<i32, Vec<Chunk>>>,
) -> Result<(i64, TaskBase), Error> {
    let task_manager = tidb_dxf_storage::get_task_manager().map_err(|e| Arc::new(e) as Error)?;
    let ctx = tidb_dxf_storage::with_internal_source_type(ctx, tidb_txnkv::INTERNAL_DIST_TASK);
    let mut logical = LogicalPlan {
        plan: plan.clone(),
        stmt: stmt.to_owned(),
        eligible_instances: instance.map(|i| vec![i]),
        chunk_map,
        ..LogicalPlan::default()
    };
    let async_prepare = should_use_async_prepare(plan);
    // Go sets the plan's thread and node counts to 1 after the logical plan
    // copied it; the prepare stage computes them.
    let (mut thread_cnt, mut max_node_cnt) = (plan.thread_cnt, plan.max_node_cnt);
    if async_prepare {
        logical.prepare_mode = tidb_dxf::PREPARE_MODE_REQUIRED;
        (thread_cnt, max_node_cnt) = (1, 1);
    }
    let table = plan.table_info.as_ref().ok_or_else(|| plain("import plan has no table info"))?;
    let parameters = plan.parameters.clone().unwrap_or_default();
    let mut job_id = 0;
    let mut task_id = 0;
    let mut failure: Option<Error> = None;
    // Go `kv.IsUserKS(se.GetStore())`.
    let keyspace = tidb_keyspace::get_keyspace_name_by_settings();
    let running_on_user_ks = tidb_txnkv::is_user_keyspace(kernel_type(), &keyspace);
    let in_job_txn = !tidb_config_kerneltype::is_next_gen() || tidb_txnkv::is_system_keyspace(kernel_type(), &keyspace);
    let submit = |se: &dyn tidb_dxf_storage::SessionContext, job_id: i64, logical: &mut LogicalPlan, manager: &tidb_dxf_storage::TaskManager| {
        logical.job_id = job_id;
        let plan_ctx = tidb_dxf_planner::PlanCtx {
            ctx: ctx.clone(),
            session_ctx: Some(se),
            task_key: task_key(job_id),
            task_type: tidb_dxf::IMPORT_INTO,
            thread_cnt: thread_cnt as isize,
            max_node_cnt: max_node_cnt as isize,
            keyspace: plan.keyspace.clone(),
            ..Default::default()
        };
        tidb_dxf_planner::Planner::new().run(&plan_ctx, &*logical, manager).map_err(crate::planner::unbox)
    };
    let result = task_manager.with_new_txn(&ctx, &mut |se| {
        let attempt = (|| -> Result<(), Error> {
            let conn = se.sql_executor();
            job_id = tidb_executor_importer::job::create_job(
                &ctx,
                conn.as_ref(),
                &plan.db_name,
                table.name.lowercase(),
                table.id,
                &plan.user,
                &plan.group_key,
                &parameters,
                plan.total_file_size,
            )
            .map_err(crate::driver)?;
            if !tidb_config_kerneltype::is_next_gen() {
                let target = tidb_model::AlterTableModeTarget {
                    schema_id: plan.db_id,
                    table_id: table.id,
                    target_mode: tidb_model::TableMode::IMPORT,
                    ..Default::default()
                };
                se.sql_server().get_runtime().alter_table_mode(&ctx, &target).map_err(plain)?;
            }
            // In a classic kernel or the SYSTEM keyspace, the task is
            // submitted in the job's transaction.
            if in_job_txn {
                task_id = submit(se, job_id, &mut logical, &task_manager)?;
            }
            Ok(())
        })();
        attempt.map_err(|e| {
            let text = e.to_string();
            failure = Some(e);
            tidb_dxf_storage::StorageError::Other(text)
        })
    });
    if let Err(e) = result {
        return Err(failure.unwrap_or_else(|| Arc::new(e)));
    }
    // In a next-gen user keyspace, the task goes to the DXF service in the
    // SYSTEM keyspace after the job is created. Go does not clean up the
    // job if this fails either.
    let mut dxf_task_mgr = task_manager.clone();
    if running_on_user_ks {
        tidb_failpoint::inject_call(
            "github.com/pingcap/tidb/pkg/dxf/importinto/afterUserImportJobCreatedBeforeDXFTask",
            &mut job_id.clone(),
        );
        dxf_task_mgr = tidb_dxf_storage::get_dxf_svc_task_mgr().map_err(|e| Arc::new(e) as Error)?;
        let result = dxf_task_mgr.with_new_txn(&ctx, &mut |se| {
            submit(se, job_id, &mut logical, &dxf_task_mgr).map(|id| task_id = id).map_err(|e| {
                let text = e.to_string();
                failure = Some(e);
                tidb_dxf_storage::StorageError::Other(text)
            })
        });
        if let Err(e) = result {
            return Err(failure.unwrap_or_else(|| Arc::new(e)));
        }
    }
    tidb_dxf_handle::notify_task_change();
    let task = dxf_task_mgr.get_task_base_by_id(&ctx, task_id).map_err(|e| Arc::new(e) as Error)?;
    let mut fields = vec![
        tidb_log::Field::new("job-id", tidb_log::Value::I64(job_id)),
        tidb_log::Field::new("task-key", tidb_log::Value::Str(task.key.clone())),
        tidb_log::Field::new("task-id", tidb_log::Value::I64(task.id)),
        tidb_log::Field::new("global-sort", tidb_log::Value::Bool(plan.is_global_sort())),
        tidb_log::Field::new("async-prepare", tidb_log::Value::Bool(async_prepare)),
    ];
    if !async_prepare {
        fields.extend([
            tidb_log::Field::new(
                "data-size",
                tidb_log::Value::Str(tidb_config_configtypes::bytes_size(plan.total_file_size as f64)),
            ),
            tidb_log::Field::new("thread-cnt", tidb_log::Value::I64(plan.thread_cnt as i64)),
            tidb_log::Field::new("max-node-cnt", tidb_log::Value::I64(plan.max_node_cnt as i64)),
        ]);
    }
    tidb_logutil::bg_logger().info("job submitted to task queue", &fields);
    Ok((job_id, task))
}

pub(crate) fn kernel_type() -> tidb_txnkv::KernelType {
    if tidb_config_kerneltype::is_next_gen() {
        tidb_txnkv::KernelType::NextGen
    } else {
        tidb_txnkv::KernelType::Classic
    }
}

/// Go `executor.cancelAndWaitImportJob`: the job's task is cancelled and
/// waited for; a job whose task was never created is cancelled directly.
///
/// # Errors
/// The task could not be cancelled, or the job changed meanwhile.
pub fn cancel_and_wait_import_job(ctx: &Context, job_id: i64) -> Result<(), Error> {
    let ctx = tidb_dxf_storage::with_internal_source_type(ctx, tidb_txnkv::INTERNAL_DIST_TASK);
    let manager = tidb_dxf_storage::get_dxf_svc_task_mgr().map_err(|e| Arc::new(e) as Error)?;
    let key = task_key(job_id);
    match manager.get_task_base_by_key_with_history(&ctx, &key) {
        Ok(_) => {
            manager
                .with_new_txn(&ctx, &mut |se| tidb_dxf_storage::TaskManager::cancel_task_by_key_session(&ctx, se, &key))
                .map_err(|e| Arc::new(e) as Error)?;
            tidb_dxf_handle::wait_task_done_by_key(&ctx, &key).map_err(|e| Arc::new(e) as Error)
        }
        Err(e) if e.is(&tidb_dxf_storage::StorageError::TaskNotFound) => {
            let mut probe_miss_job_id = job_id;
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/executor/afterCancelImportTaskProbeMiss",
                &mut probe_miss_job_id,
            );
            // In next-gen, the import job and DXF task are created in
            // separate transactions. The job row can exist before the DXF
            // task row is committed, or the task submission can fail after
            // the job is created. If the task row is committed after the
            // lookup, do not wait for it: the dangling fallback either
            // cancels the still-pending import job or reports that the
            // scheduler changed the job state first.
            tidb_logutil::bg_logger().info(
                "cancel import job directly after initial dxf task lookup found no task",
                &[
                    tidb_log::Field::new("jobID", tidb_log::Value::I64(job_id)),
                    tidb_log::Field::new("taskKey", tidb_log::Value::Str(key.clone())),
                ],
            );
            let mut dangling_job_id = job_id;
            tidb_failpoint::inject_call(
                "github.com/pingcap/tidb/pkg/executor/beforeCancelDanglingImportJob",
                &mut dangling_job_id,
            );
            cancel_dangling_import_job(&ctx, job_id)
        }
        Err(e) => Err(Arc::new(e)),
    }
}

/// Go `cancelDanglingImportJob`.
fn cancel_dangling_import_job(ctx: &Context, job_id: i64) -> Result<(), Error> {
    let manager = tidb_dxf_storage::get_task_manager().map_err(|e| Arc::new(e) as Error)?;
    let mut failure: Option<Error> = None;
    let result = manager.with_new_session(&mut |se| {
        let attempt = tidb_executor_importer::job::cancel_pending_job(ctx, se.sql_executor().as_ref(), job_id)
            .map_err(crate::driver)
            .and_then(|()| {
                if se.affected_rows() == 0 {
                    Err(plain("job state changed during cancel, please try again later"))
                } else {
                    Ok(())
                }
            });
        attempt.map_err(|e| {
            let text = e.to_string();
            failure = Some(e);
            tidb_dxf_storage::StorageError::Other(text)
        })
    });
    result.map_err(|e| failure.take().unwrap_or_else(|| Arc::new(e)))
}

/// Go `util.IsRaftKv2`: whether any TiKV runs the `raft-kv2` engine.
///
/// # Errors
/// The cluster config cannot be read.
pub fn is_raft_kv2(ctx: &Context, se: &dyn tidb_dxf_storage::SessionContext) -> Result<bool, Error> {
    let rows = tidb_dxf_storage::exec_sql(
        ctx,
        se,
        "select `value` from information_schema.cluster_config where type = 'tikv' and `key` = 'storage.engine'",
        &[],
    )
    .map_err(|e| Arc::new(e) as Error)?;
    // All nodes have the same engine.
    let engine = rows.first().and_then(|r| r.first()).and_then(|d| d.sql_string().ok());
    Ok(engine.as_deref() == Some("raft-kv2"))
}

/// Go `handle.GetCloudStorageURI`.
#[must_use]
pub fn get_cloud_storage_uri(cluster_id: Option<u64>) -> String {
    tidb_dxf_handle::get_cloud_storage_uri(cluster_id)
}

#[cfg(test)]
mod tests {
    /// Go `ShouldUseAsyncPrepare`: only a next-gen, non-starter global-sort
    /// import from files.
    #[test]
    fn async_prepare_only_for_next_gen_global_sort_files() {
        let mut plan = tidb_executor_importer::Plan { cloud_storage_uri: "s3://b/p".into(), ..Default::default() };
        let next_gen = tidb_config_kerneltype::is_next_gen();
        assert_eq!(super::should_use_async_prepare(&plan), next_gen);
        plan.query_source = Some(tidb_executor_importer::query_source::QuerySource::default());
        assert!(!super::should_use_async_prepare(&plan));
        plan.query_source = None;
        plan.cloud_storage_uri.clear();
        assert!(!super::should_use_async_prepare(&plan));
        if next_gen {
            plan.cloud_storage_uri = "s3://b/p".into();
            tidb_config_deploymode::set(tidb_config_deploymode::Mode::Starter).unwrap();
            assert!(!super::should_use_async_prepare(&plan));
            tidb_config_deploymode::set(tidb_config_deploymode::Mode::Premium).unwrap();
        }
    }
}
