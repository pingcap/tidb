// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/clean_up.go`: the cleanup of a finished import
//! task before it moves to history. It redacts the task meta, switches the
//! table back to normal mode, and deletes the global sort files.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tidb_dxf::Task;
use tidb_dxf_scheduler::{BatchCleaner, Cleaner};
use tidb_gocontext::Context;

use crate::proto::{PostProcessStepMeta, TaskMeta};
use crate::{plain, Error};

/// Go `cleanMeteringConcurrency`: metering sends use little CPU and memory,
/// so four concurrent workers are a temporary conservative setting to
/// improve throughput without unbounded pressure.
const CLEAN_METERING_CONCURRENCY: usize = 4;

/// Go `ImportCleaner`.
struct ImportCleaner;

/// Go `init`'s `RegisterCleanerFactory`.
pub(crate) fn register() {
    tidb_dxf_scheduler::register_cleaner_factory(
        tidb_dxf::IMPORT_INTO,
        Arc::new(|| Arc::new(ImportCleaner) as Arc<dyn Cleaner>),
    );
}

impl Cleaner for ImportCleaner {
    /// Go `Clean`.
    fn clean(&self, ctx: &Context, task: &mut Task) -> Result<(), Error> {
        self.batch_clean(ctx, std::slice::from_mut(task))
    }

    fn as_batch_cleaner(&self) -> Option<&dyn BatchCleaner> {
        Some(self)
    }

    fn as_expired_file_cleaner(&self) -> Option<&dyn tidb_dxf_scheduler::ExpiredFileCleaner> {
        Some(self)
    }
}

impl tidb_dxf_scheduler::ExpiredFileCleaner for ImportCleaner {
    /// Go `CleanExpiredFiles`: the conflict row files past their retention.
    fn clean_expired_files(
        &self,
        ctx: &Context,
        task_info_getter: &dyn tidb_dxf_storage::TaskCleanupInfoGetter,
        cloud_storage_uri: &str,
    ) -> Result<(), Error> {
        tidb_dxf_importinto_conflictrows::clean_conflict_row_files(ctx, task_info_getter, cloud_storage_uri)
            .map_err(|e| plain(e.to_string()))
    }
}

/// Go `cleanFileGroup`: the tasks sharing one global sort store.
struct CleanFileGroup {
    non_partitioned_dirs: Vec<String>,
    task_ids: Vec<i64>,
}

impl BatchCleaner for ImportCleaner {
    /// Go `BatchClean`.
    fn batch_clean(&self, ctx: &Context, tasks: &mut [Task]) -> Result<(), Error> {
        // we can only clean up files after all write&ingest subtasks are
        // finished, since they might share the same file.
        let mut meter_tasks: Vec<usize> = Vec::new();
        let mut file_groups: BTreeMap<String, CleanFileGroup> = BTreeMap::new();
        for (index, task) in tasks.iter_mut().enumerate() {
            let mut meta = TaskMeta::from_json(&task.meta)?;
            let cloud_storage_uri = meta.plan.cloud_storage_uri.clone();
            let is_global_sort = meta.plan.is_global_sort();
            redact_sensitive_info(task, &mut meta);
            clean_table_mode(ctx, &meta)?;
            if let Some(e) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/dxf/importinto/mockCleanupError") {
                if e.bool() {
                    return Err(plain("mock cleanup error"));
                }
            }
            if is_global_sort {
                let group = file_groups
                    .entry(cloud_storage_uri)
                    .or_insert_with(|| CleanFileGroup { non_partitioned_dirs: Vec::new(), task_ids: Vec::new() });
                group.non_partitioned_dirs.push(task.base.id.to_string());
                group.task_ids.push(task.base.id);
                if tidb_config_kerneltype::is_next_gen() && task.base.state == tidb_dxf::TASK_STATE_SUCCEED {
                    meter_tasks.push(index);
                }
            }
        }
        for (uri, group) in &file_groups {
            clean_external_files(ctx, uri, group)?;
        }
        let meter_tasks: Vec<&Task> = meter_tasks.into_iter().map(|index| &tasks[index]).collect();
        send_meter_on_clean_in_parallel(ctx, &meter_tasks, &send_meter_on_clean)
    }
}

/// Go `sendMeterOnCleanFunc`.
type SendMeterOnCleanFunc<'a> = &'a (dyn Fn(&Context, &Task, &tidb_logutil::Logger) -> Result<(), Error> + Sync);

/// Go `sendMeterOnCleanInParallel`: sends the tasks' metering data on
/// `cleanMeteringConcurrency` workers. The first failure -- a panic
/// included, as Go's `ErrorGroupWithRecover` -- cancels the group's context
/// and is the result.
fn send_meter_on_clean_in_parallel(
    ctx: &Context,
    tasks: &[&Task],
    send_fn: SendMeterOnCleanFunc<'_>,
) -> Result<(), Error> {
    if tasks.is_empty() {
        return Ok(());
    }

    let (eg_ctx, cancel) = Context::with_cancel(ctx);
    let first_error: Mutex<Option<Error>> = Mutex::new(None);
    let fail = |error: Error| {
        first_error.lock().unwrap_or_else(std::sync::PoisonError::into_inner).get_or_insert(error);
        cancel.cancel();
    };
    let (task_tx, task_rx) = crossbeam_channel::bounded::<&Task>(0);
    let worker_count = CLEAN_METERING_CONCURRENCY.min(tasks.len());
    let cancel_err = std::thread::scope(|scope| {
        // Go `close(taskCh)`: the sender drops when this closure returns,
        // before the scope joins its workers.
        let task_tx = task_tx;
        for _ in 0..worker_count {
            let task_rx = task_rx.clone();
            let (eg_ctx, fail) = (&eg_ctx, &fail);
            scope.spawn(move || {
                let run = || -> Result<(), Error> {
                    for task in &task_rx {
                        let logger = tidb_logutil::bg_logger()
                            .with_fields(&[tidb_log::Field::new("task-id", tidb_log::Value::I64(task.base.id))]);
                        if let Err(error) = send_fn(eg_ctx, task, &logger) {
                            logger.warn(
                                "failed to send metering data on cleanup",
                                &[tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string()))],
                            );
                            return Err(error);
                        }
                    }
                    Ok(())
                };
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(run)) {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => fail(error),
                    // Go `util.GetRecoverError`: `errors.Errorf("%v", r)`.
                    Err(payload) => fail(plain(
                        payload
                            .downcast_ref::<&str>()
                            .map(|message| (*message).to_owned())
                            .or_else(|| payload.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "non-string panic payload".to_owned()),
                    )),
                }
            });
        }
        drop(task_rx);
        let done = eg_ctx.done();
        for task in tasks {
            crossbeam_channel::select! {
                recv(done) -> _ => {
                    // Go: `cancelErr := egCtx.Err(); close(taskCh)`.
                    return eg_ctx.err();
                }
                send(task_tx, *task) -> _ => {}
            }
        }
        None
    });
    cancel.cancel();
    if let Some(error) = first_error.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner) {
        return Err(error);
    }
    cancel_err.map_or(Ok(()), |error| Err(Arc::new(error) as Error))
}

/// Go `sendMeterOnClean`: the row count and KV sizes of a succeeded
/// global-sort import, from its post-process subtask's checksums.
fn send_meter_on_clean(ctx: &Context, task: &Task, _logger: &tidb_logutil::Logger) -> Result<(), Error> {
    let task_manager = tidb_dxf_storage::get_task_manager().map_err(|e| Arc::new(e) as Error)?;
    let subtasks = task_manager
        .get_all_subtasks_by_step_and_state(
            ctx,
            task.base.id,
            tidb_dxf::IMPORT_STEP_POST_PROCESS,
            &tidb_dxf::SUBTASK_STATE_SUCCEED,
        )
        .map_err(|e| Arc::new(e) as Error)?;
    if subtasks.len() != 1 {
        // should not happen, checksum is required in nextgen
        return Ok(());
    }
    let st_meta: PostProcessStepMeta =
        serde_json::from_slice(&subtasks[0].meta).map_err(|e| Arc::new(e) as Error)?;
    let (mut row_count, mut data_kv_size, mut index_kv_size) = (0_u64, 0_u64, 0_u64);
    for (group, ck_sum) in st_meta.checksum.iter().flatten() {
        if *group == tidb_lightning_verification::DATA_KV_GROUP_ID {
            row_count = ck_sum.kvs;
            data_kv_size = ck_sum.size;
        } else {
            index_kv_size += ck_sum.size;
        }
    }
    tidb_dxf_handle::send_row_and_size_meter_data(
        ctx,
        task,
        row_count as i64,
        data_kv_size as i64,
        index_kv_size as i64,
    )
    .map_err(plain)
}

/// Go `redactSensitiveInfo`: drops the statement and redacts the URLs.
pub(crate) fn redact_sensitive_info(task: &mut Task, meta: &mut TaskMeta) {
    meta.stmt = String::new();
    meta.plan.path = tidb_ast::redact_url(&meta.plan.path);
    if !meta.plan.cloud_storage_uri.is_empty() {
        meta.plan.cloud_storage_uri = tidb_ast::redact_url(&meta.plan.cloud_storage_uri);
    }
    task.meta = meta.to_json();
}

/// Go `cleanTableMode`: on a classic kernel, the table back to normal
/// mode, unless it no longer exists.
fn clean_table_mode(ctx: &Context, meta: &TaskMeta) -> Result<(), Error> {
    if tidb_config_kerneltype::is_next_gen() {
        return Ok(());
    }
    let Some(table) = meta.plan.table_info.as_ref() else { return Ok(()) };
    let manager = tidb_dxf_storage::get_task_manager().map_err(|e| Arc::new(e) as Error)?;
    let target = tidb_model::AlterTableModeTarget {
        schema_id: meta.plan.db_id,
        table_id: table.id,
        target_mode: tidb_model::TableMode::NORMAL,
        ..Default::default()
    };
    let result = manager.with_new_txn(ctx, &mut |se| {
        se.sql_server().get_runtime().alter_table_mode(ctx, &target).map_err(tidb_dxf_storage::StorageError::Other)
    });
    match result {
        Ok(()) => Ok(()),
        // Go `infoschema.ErrTableNotExists`: "Table '%s.%s' doesn't exist".
        Err(e) if e.to_string().contains("doesn't exist") => {
            tidb_logutil::bg_logger().warn(
                "table not found during import cleanup, skip altering table mode",
                &[tidb_log::Field::new("tableID", tidb_log::Value::I64(table.id))],
            );
            Ok(())
        }
        Err(e) => Err(Arc::new(e)),
    }
}

/// Go `cleanExternalFiles`.
fn clean_external_files(ctx: &Context, uri: &str, group: &CleanFileGroup) -> Result<(), Error> {
    let task = tidb_lightning_log::begin_logutil_task(
        tidb_logutil::bg_logger().with_fields(&[tidb_log::Field::new("task-ids", tidb_log::Value::Str(format!("{:?}", group.task_ids)))]),
        "cleanup global sorted data",
    );
    let store = match tidb_executor_importer::get_sort_store(ctx, uri) {
        Ok(store) => store,
        Err(e) => {
            tidb_logutil::bg_logger()
                .warn("failed to create store", &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))]);
            return Err(crate::driver(e));
        }
    };
    let dirs: Vec<&str> = group.non_partitioned_dirs.iter().map(String::as_str).collect();
    let result = tidb_ingestor_globalsort::clean_up_files(ctx, store.as_ref(), &dirs);
    task.end(tidb_log::Level::Info, None, &[]);
    result.map_err(|e| {
        tidb_logutil::bg_logger().warn(
            "failed to clean up files of tasks",
            &[tidb_log::Field::new("error", tidb_log::Value::Str(e.to_string()))],
        );
        crate::planner::global_sort_error(e)
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::time::Duration;

    use super::*;

    fn succeeded_tasks(count: usize) -> Vec<Task> {
        (0..count)
            .map(|i| {
                let mut task = Task::default();
                task.base.id = i as i64 + 1;
                task.base.state = tidb_dxf::TASK_STATE_SUCCEED;
                task
            })
            .collect()
    }

    /// Go `TestSendMeterOnCleanInParallelSuccess`.
    #[test]
    fn test_send_meter_on_clean_in_parallel_success() {
        let tasks = succeeded_tasks(CLEAN_METERING_CONCURRENCY * 2);
        let tasks: Vec<&Task> = tasks.iter().collect();

        let processed_task_count = AtomicI32::new(0);
        let send_fn = |_: &Context, _: &Task, _: &tidb_logutil::Logger| -> Result<(), Error> {
            processed_task_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };

        send_meter_on_clean_in_parallel(&Context::background(), &tasks, &send_fn).unwrap();
        assert_eq!(processed_task_count.load(Ordering::SeqCst), tasks.len() as i32);
    }

    /// Go `TestSendMeterOnCleanInParallelCancellation`.
    #[test]
    fn test_send_meter_on_clean_in_parallel_cancellation() {
        // send error stops starting pending tasks
        {
            let (ctx, _cancel) = Context::with_timeout(&Context::background(), Duration::from_secs(3));
            let tasks = succeeded_tasks(CLEAN_METERING_CONCURRENCY * 2);
            let tasks: Vec<&Task> = tasks.iter().collect();
            let releases: Vec<_> =
                tasks.iter().map(|_| crossbeam_channel::bounded::<Error>(1)).collect();

            let (started_tx, started_rx) = crossbeam_channel::bounded::<i64>(tasks.len());
            let send_fn = |ctx: &Context, task: &Task, _: &tidb_logutil::Logger| -> Result<(), Error> {
                started_tx.send(task.base.id).unwrap();
                let done = ctx.done();
                crossbeam_channel::select! {
                    recv(releases[task.base.id as usize - 1].1) -> error => Err(error.unwrap()),
                    recv(done) -> _ => Err(Arc::new(ctx.err().unwrap()) as Error),
                }
            };

            let send_err: Error = plain("metering failed");
            let err = std::thread::scope(|scope| {
                let done = scope.spawn(|| send_meter_on_clean_in_parallel(&ctx, &tasks, &send_fn));
                let started_task_ids: Vec<i64> = (0..CLEAN_METERING_CONCURRENCY)
                    .map(|_| started_rx.recv_timeout(Duration::from_secs(3)).expect("a worker started"))
                    .collect();
                releases[started_task_ids[0] as usize - 1].0.send(Arc::clone(&send_err)).unwrap();
                done.join().unwrap().unwrap_err()
            });
            assert!(Arc::ptr_eq(&err, &send_err), "{err}");
            assert!(started_rx.is_empty());
        }

        // parent cancellation is propagated
        {
            let (ctx, cancel) = Context::with_cancel(&Context::background());
            cancel.cancel();
            let tasks = succeeded_tasks(1);
            let tasks: Vec<&Task> = tasks.iter().collect();

            let err = send_meter_on_clean_in_parallel(
                &ctx,
                &tasks,
                &|ctx: &Context, _: &Task, _: &tidb_logutil::Logger| match ctx.err() {
                    Some(error) => Err(Arc::new(error) as Error),
                    None => Ok(()),
                },
            )
            .unwrap_err();
            assert_eq!(err.to_string(), tidb_gocontext::CtxError::Canceled.to_string());
        }
    }

    /// Go `TestSendMeterOnCleanInParallelRecoversPanic`.
    #[test]
    fn test_send_meter_on_clean_in_parallel_recovers_panic() {
        let tasks = succeeded_tasks(1);
        let tasks: Vec<&Task> = tasks.iter().collect();
        let send_fn = |_: &Context, _: &Task, _: &tidb_logutil::Logger| -> Result<(), Error> {
            panic!("metering panic")
        };

        let err = send_meter_on_clean_in_parallel(&Context::background(), &tasks, &send_fn).unwrap_err();
        assert!(err.to_string().contains("metering panic"), "{err}");
    }
}
