// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/job.go`: submitting an import as a job and its
//! DXF task.

use std::collections::BTreeMap;
use std::sync::Arc;

use tidb_dxf::TaskBase;
use tidb_executor::importer::chunk_process::Chunk;
use tidb_executor::importer::Plan;
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
    let table = plan.table_info.as_ref().ok_or_else(|| plain("import plan has no table info"))?;
    let parameters = plan.parameters.clone().unwrap_or_default();
    let mut job_id = 0;
    let mut task_id = 0;
    let mut failure: Option<Error> = None;
    let result = task_manager.with_new_txn(&ctx, &mut |se| {
        let attempt = (|| -> Result<(), Error> {
            let conn = se.sql_executor();
            job_id = tidb_executor::importer::job::create_job(
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
            logical.job_id = job_id;
            let plan_ctx = tidb_dxf_planner::PlanCtx {
                ctx: ctx.clone(),
                session_ctx: Some(se),
                task_key: task_key(job_id),
                task_type: tidb_dxf::IMPORT_INTO,
                thread_cnt: plan.thread_cnt as isize,
                max_node_cnt: plan.max_node_cnt as isize,
                keyspace: plan.keyspace.clone(),
                ..Default::default()
            };
            task_id = tidb_dxf_planner::Planner::new()
                .run(&plan_ctx, &logical, &task_manager)
                .map_err(crate::planner::unbox)?;
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
    tidb_dxf_handle::notify_task_change();
    let task = task_manager.get_task_base_by_id(&ctx, task_id).map_err(|e| Arc::new(e) as Error)?;
    tidb_logutil::bg_logger().info(
        "job submitted to task queue",
        &[
            tidb_log::Field::new("job-id", tidb_log::Value::I64(job_id)),
            tidb_log::Field::new("task-id", tidb_log::Value::I64(task.id)),
            tidb_log::Field::new("task-key", tidb_log::Value::Str(task.key.clone())),
        ],
    );
    Ok((job_id, task))
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
        Err(e) if e.is(&tidb_dxf_storage::StorageError::TaskNotFound) => cancel_dangling_import_job(&ctx, job_id),
        Err(e) => Err(Arc::new(e)),
    }
}

/// Go `cancelDanglingImportJob`.
fn cancel_dangling_import_job(ctx: &Context, job_id: i64) -> Result<(), Error> {
    let manager = tidb_dxf_storage::get_task_manager().map_err(|e| Arc::new(e) as Error)?;
    let mut failure: Option<Error> = None;
    let result = manager.with_new_session(&mut |se| {
        let attempt = tidb_executor::importer::job::cancel_pending_job(ctx, se.sql_executor().as_ref(), job_id)
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

/// Go `handle.GetCloudStorageURI`: `tidb_cloud_storage_uri` with the
/// `dxf/<cluster ID>` prefix appended.
#[must_use]
pub fn get_cloud_storage_uri(cluster_id: Option<u64>) -> String {
    let cloud_uri = tidb_vardef::CLOUD_STORAGE_URI.read().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
    if cloud_uri.is_empty() {
        return cloud_uri;
    }
    let Ok(mut u) = tidb_objstore::parse_raw_url(&cloud_uri) else { return cloud_uri };
    let prefix = tidb_objstore::storeapi::Prefix::new(&String::from_utf8_lossy(&u.path));
    let cluster = if prefix.as_str().is_empty() { String::new() } else { cluster_id.map(|id| id.to_string()).unwrap_or_default() };
    let joined = if cluster.is_empty() { "dxf".to_owned() } else { format!("dxf/{cluster}") };
    u.path = prefix.join_str(&joined).to_path().into_bytes();
    u.string()
}
