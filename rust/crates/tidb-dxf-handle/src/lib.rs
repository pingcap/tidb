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

//! Go `pkg/dxf/framework/handle`: the API task types use to submit a task,
//! wait for it and control it, and the schedule status.
//!
//! Not ported yet, with the callers that need them: `GetCloudStorageURI`,
//! `NewObjStore` and `NewObjStoreWithRecording` (Go `pkg/objstore`; global
//! sort of DDL ingest and `IMPORT INTO`), `GetScheduleTuneFactors` (a meta
//! transaction on the session's store; DDL reorg and the importer),
//! `SendRowAndSizeMeterData` (the cleanup of DDL ingest and `IMPORT INTO`),
//! and `GetBusyNodes` / `GetScheduleStatus` (the next-gen status port, row
//! O15).

mod status;

pub use status::*;

use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use tidb_dxf::{ExtraParams, Task, TaskBase, TaskType, TASK_STATE_FAILED, TASK_STATE_PAUSED, TASK_STATE_REVERTED, TASK_STATE_SUCCEED};
use tidb_dxf_storage::{get_dxf_svc_task_mgr, get_task_manager, StorageError};
use tidb_gocontext::{Context, CtxError};
use tidb_log::{Field, Value};

/// Go `checkTaskFinishInterval`.
pub static CHECK_TASK_FINISH_INTERVAL: RwLock<Duration> = RwLock::new(Duration::from_millis(300));

/// Go `TaskChangedCh`: speeds up task scheduling, such as when a task is
/// submitted on the node of the scheduler manager.
pub static TASK_CHANGED_CH: LazyLock<(crossbeam_channel::Sender<()>, crossbeam_channel::Receiver<()>)> =
    LazyLock::new(|| crossbeam_channel::bounded(1));

/// Go `DXFLogCategory`: the log category of DXF's sampled logs.
pub const DXF_LOG_CATEGORY: &str = "dxf";
/// Go `SampleLogTick`: the sampling window of DXF's sampled logs.
pub const SAMPLE_LOG_TICK: Duration = Duration::from_secs(60);
/// Go `SampleLogFirst`: the logs with the same level and message kept per
/// [`SAMPLE_LOG_TICK`].
pub const SAMPLE_LOG_FIRST: u64 = 10;
/// Go `NextGenTargetScope`: on a next-gen kernel DXF runs as a service only
/// on nodes of this scope, so every task targets it.
pub const NEXT_GEN_TARGET_SCOPE: &str = "dxf_service";
/// Go `defRegionSplitSize` (96 MiB).
const DEF_REGION_SPLIT_SIZE: i64 = 96 << 20;
/// Go `defRegionSplitKeys`.
const DEF_REGION_SPLIT_KEYS: i64 = 960_000;

/// The package's errors.
#[derive(Debug, Clone)]
pub enum HandleError {
    /// The task manager's error.
    Storage(StorageError),
    /// Go `ctx.Err()`.
    Context(CtxError),
    /// The error the task stopped with.
    Task(Arc<dyn std::error::Error + Send + Sync>),
    /// Go's `errors.Errorf` text.
    Other(String),
}

impl std::fmt::Display for HandleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "{error}"),
            Self::Context(error) => write!(f, "{error}"),
            Self::Task(error) => write!(f, "{error}"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for HandleError {}

impl From<StorageError> for HandleError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

/// The package's result.
pub type Result<T> = std::result::Result<T, HandleError>;

/// Go `NotifyTaskChange`: tells the scheduler manager a task changed, either
/// submitted or finished.
pub fn notify_task_change() {
    let _ = TASK_CHANGED_CH.0.try_send(());
}

/// Go `NewSampleErrVerboseLogger`: a sampled logger with DXF's defaults.
#[must_use]
pub fn new_sample_err_verbose_logger(fields: &[Field]) -> tidb_logutil::SampledLogger {
    let mut all_fields = Vec::with_capacity(fields.len() + 1);
    all_fields.push(Field::new(tidb_logutil::LOG_FIELD_CATEGORY, Value::Str(DXF_LOG_CATEGORY.into())));
    all_fields.extend_from_slice(fields);
    tidb_logutil::sample_err_verbose_logger_factory(SAMPLE_LOG_TICK, SAMPLE_LOG_FIRST, all_fields)()
}

/// Go `GetCPUCountOfNode`: the CPU count of the managed nodes.
///
/// # Errors
/// No task manager is set, or the query failed.
pub fn get_cpu_count_of_node() -> Result<isize> {
    Ok(get_dxf_svc_task_mgr()?.get_cpu_count_of_node()?)
}

/// Go `SubmitTask`.
///
/// # Errors
/// See [`submit_task_with_extra_params`].
pub fn submit_task(
    task_key: &str,
    task_type: &TaskType,
    keyspace: &str,
    required_slots: isize,
    target_scope: &str,
    max_node_cnt: isize,
    task_meta: &[u8],
) -> Result<Task> {
    submit_task_with_extra_params(
        task_key,
        task_type,
        keyspace,
        required_slots,
        target_scope,
        max_node_cnt,
        &ExtraParams::default(),
        task_meta,
    )
}

/// Go `SubmitTaskWithExtraParams`: creates the task unless one with the key
/// exists, in the task table or its history.
///
/// # Errors
/// A task with the key exists (`TaskAlreadyExists`), or the task manager
/// failed.
#[allow(clippy::too_many_arguments)]
pub fn submit_task_with_extra_params(
    task_key: &str,
    task_type: &TaskType,
    keyspace: &str,
    required_slots: isize,
    target_scope: &str,
    max_node_cnt: isize,
    extra_params: &ExtraParams,
    task_meta: &[u8],
) -> Result<Task> {
    let task_manager = get_dxf_svc_task_mgr()?;
    match task_manager.get_task_by_key_with_history(task_key) {
        Ok(_) => return Err(StorageError::TaskAlreadyExists.into()),
        Err(error) if !error.is(&StorageError::TaskNotFound) => return Err(error.into()),
        Err(_) => {}
    }

    let task_id = task_manager.create_task(
        task_key,
        task_type,
        keyspace,
        required_slots,
        target_scope,
        max_node_cnt,
        extra_params,
        task_meta,
    )?;

    let task = task_manager.get_task_by_id(task_id)?;

    notify_task_change();
    Ok(task)
}

/// Go `WaitTaskDoneOrPaused`: waits for the task to finish or pause; an
/// error when it failed or was cancelled.
///
/// # Errors
/// See [`wait_task_done_or_paused_with_result`].
pub fn wait_task_done_or_paused(ctx: &Context, id: i64) -> Result<()> {
    wait_task_done_or_paused_with_result(ctx, id).map(drop)
}

/// Go `WaitTaskDoneOrPausedWithResult`: the task once it finished or
/// paused.
///
/// # Errors
/// The task was reverted or failed, `ctx` ended, or the task manager
/// failed.
pub fn wait_task_done_or_paused_with_result(ctx: &Context, id: i64) -> Result<Task> {
    let logger = tidb_logutil::bg_logger().with_fields(&[Field::new("task-id", Value::I64(id))]);
    wait_task(ctx, id, |t| t.is_done() || t.state == TASK_STATE_PAUSED)?;
    let task_manager = get_dxf_svc_task_mgr()?;
    let found = task_manager.get_task_by_id_with_history(id)?;

    if found.base.state == TASK_STATE_SUCCEED {
        Ok(found)
    } else if found.base.state == TASK_STATE_REVERTED {
        let error = found.error.as_ref().map_or_else(String::new, ToString::to_string);
        logger.error("task reverted", &[Field::new("error", Value::Str(error))]);
        match &found.error {
            Some(error) => Err(HandleError::Task(Arc::clone(error))),
            None => Err(HandleError::Other(format!("task stopped with state {}", found.base.state))),
        }
    } else if found.base.state == TASK_STATE_PAUSED {
        logger.info("task paused", &[]);
        Ok(found)
    } else if found.base.state == TASK_STATE_FAILED {
        // Go formats a nil error with %v as "<nil>".
        let error = found.error.as_ref().map_or_else(|| "<nil>".to_owned(), ToString::to_string);
        Err(HandleError::Other(format!("task stopped with state {}, err {error}", found.base.state)))
    } else {
        Ok(found)
    }
}

/// Go `WaitTaskDoneByKey`.
///
/// # Errors
/// No such task, `ctx` ended, or the task manager failed.
pub fn wait_task_done_by_key(ctx: &Context, task_key: &str) -> Result<()> {
    let task_manager = get_dxf_svc_task_mgr()?;
    let task = task_manager.get_task_by_key_with_history(task_key)?;
    wait_task(ctx, task.base.id, TaskBase::is_done).map(drop)
}

/// Go `WaitTask`: polls the task until `match_fn` accepts it.
///
/// # Errors
/// `ctx` ended, or no task manager is set.
pub fn wait_task(ctx: &Context, id: i64, match_fn: impl Fn(&TaskBase) -> bool) -> Result<TaskBase> {
    let task_manager = get_dxf_svc_task_mgr()?;
    let interval = *CHECK_TASK_FINISH_INTERVAL.read().unwrap_or_else(std::sync::PoisonError::into_inner);
    let ticker = crossbeam_channel::tick(interval);
    let sample_logger = tidb_logutil::sample_logger_factory(
        SAMPLE_LOG_TICK,
        SAMPLE_LOG_FIRST,
        vec![
            Field::new(tidb_logutil::LOG_FIELD_CATEGORY, Value::Str(DXF_LOG_CATEGORY.into())),
            Field::new("task-id", Value::I64(id)),
        ],
    )();
    let done = ctx.done();
    loop {
        crossbeam_channel::select! {
            recv(done) -> _ => return Err(HandleError::Context(ctx.err().unwrap_or(CtxError::Canceled))),
            recv(ticker) -> _ => {
                let task = match task_manager.get_task_base_by_id_with_history(id) {
                    Ok(task) => task,
                    Err(error) => {
                        sample_logger.error("cannot get task during waiting", &[Field::new("error", Value::Str(error.to_string()))]);
                        continue;
                    }
                };
                if match_fn(&task) {
                    return Ok(task);
                }
            }
        }
    }
}

/// Go `CancelTask`: nothing when there is no such task.
///
/// # Errors
/// The task manager failed.
pub fn cancel_task(task_key: &str) -> Result<()> {
    let task_manager = get_dxf_svc_task_mgr()?;
    let task = match task_manager.get_task_by_key(task_key) {
        Ok(task) => task,
        Err(error) if error.is(&StorageError::TaskNotFound) => {
            tidb_logutil::bg_logger().info("task not exist", &[Field::new("taskKey", Value::Str(task_key.into()))]);
            return Ok(());
        }
        Err(error) => return Err(error.into()),
    };
    Ok(task_manager.cancel_task(task.base.id)?)
}

/// Go `PauseTask`: nothing when the task is not pausable.
///
/// # Errors
/// No task manager is set.
pub fn pause_task(task_key: &str) -> Result<()> {
    let task_manager = get_dxf_svc_task_mgr()?;
    // Go's `found` is false on an error, which the not-pausable branch hides.
    if !task_manager.pause_task(task_key).unwrap_or(false) {
        tidb_logutil::bg_logger().info("task not pausable", &[Field::new("taskKey", Value::Str(task_key.into()))]);
    }
    Ok(())
}

/// Go `ResumeTask`: nothing when the task is not resumable.
///
/// # Errors
/// No task manager is set.
pub fn resume_task(task_key: &str) -> Result<()> {
    let task_manager = get_dxf_svc_task_mgr()?;
    // Go's `found` is false on an error, which the not-resumable branch hides.
    if !task_manager.resume_task(task_key).unwrap_or(false) {
        tidb_logutil::bg_logger().info("task not resumable", &[Field::new("taskKey", Value::Str(task_key.into()))]);
    }
    Ok(())
}

/// The error of [`run_with_retry`]: `f`'s, or Go's `ctx.Err()`.
#[derive(Debug)]
pub enum RetryError<E> {
    /// The function's error.
    Func(E),
    /// `ctx` ended while backing off.
    Context(CtxError),
}

impl<E: std::fmt::Display> std::fmt::Display for RetryError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Func(error) => write!(f, "{error}"),
            Self::Context(error) => write!(f, "{error}"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for RetryError<E> {}

/// Go `RunWithRetry`: runs `f` until it succeeds, fails with an error it
/// marks not retryable, or `max_retry` runs failed (the last error).
///
/// # Errors
/// `f`'s error, or `ctx` ended while backing off.
pub fn run_with_retry<E: std::fmt::Display>(
    ctx: &Context,
    max_retry: usize,
    backoffer: &mut dyn tidb_backoff::Backoffer,
    logger: &tidb_logutil::Logger,
    mut f: impl FnMut(&Context) -> (bool, std::result::Result<(), E>),
) -> std::result::Result<(), RetryError<E>> {
    let mut last_err = None;
    for i in 0..max_retry {
        let (retryable, result) = f(ctx);
        let err = match result {
            Ok(()) => return Ok(()),
            Err(err) if !retryable => return Err(RetryError::Func(err)),
            Err(err) => err,
        };
        tidb_metrics::ddl::RETRYABLE_ERROR_COUNT.with_label_values(&[&err.to_string()]).inc();
        logger.warn(
            "met retryable error",
            &[
                Field::new("retry-count", Value::I64(i as i64)),
                Field::new("max-retry", Value::I64(max_retry as i64)),
                Field::new("error", Value::Str(err.to_string())),
            ],
        );
        last_err = Some(err);
        // Go `time.After(backoffer.Backoff(i))`; the backoff is a Go duration.
        let backoff = Duration::from_nanos(u64::try_from(backoffer.backoff(i as isize)).unwrap_or(0));
        if ctx.sleep(backoff) {
            return Err(RetryError::Context(ctx.err().unwrap_or(CtxError::Canceled)));
        }
    }
    last_err.map_or(Ok(()), |err| Err(RetryError::Func(err)))
}

/// Go `GetDefaultRegionSplitConfig`: the default region split size and keys.
#[must_use]
pub fn get_default_region_split_config() -> (i64, i64) {
    if tidb_config_kerneltype::is_next_gen() {
        const NEXT_GEN_REGION_SPLIT_SIZE: i64 = 1 << 30;
        // the keys:size ratio is 10 times larger than Classic, the reason seems
        // that nextgen TiKV want to control region split through size only, so
        // they set a very large keys limit.
        const NEXT_GEN_REGION_SPLIT_KEYS: i64 = 102_400_000;
        return (NEXT_GEN_REGION_SPLIT_SIZE, NEXT_GEN_REGION_SPLIT_KEYS);
    }
    (DEF_REGION_SPLIT_SIZE, DEF_REGION_SPLIT_KEYS)
}

/// Go `GetTargetScope`: the target scope of new tasks. On a classic kernel
/// it is the service scope of the instance the user connects to; on a
/// next-gen kernel it is always [`NEXT_GEN_TARGET_SCOPE`].
#[must_use]
pub fn get_target_scope() -> String {
    if tidb_config_kerneltype::is_next_gen() {
        return NEXT_GEN_TARGET_SCOPE.to_owned();
    }
    tidb_vardef::service_scope()
}

/// Go `UpdatePauseScaleInFlag`: stores the pause-scale-in flag in
/// `mysql.tidb`.
///
/// # Errors
/// No task manager is set, or the statement failed.
pub fn update_pause_scale_in_flag(flag: &tidb_dxf::schstatus::TtlFlag) -> Result<()> {
    let manager = get_task_manager()?;
    let bytes = tidb_stringutil::go_json_marshal(flag);
    manager.with_new_txn(&mut |se| {
        tidb_dxf_storage::exec_sql(
            &tidb_sqlexec::BackgroundContext,
            se,
            "REPLACE INTO mysql.tidb(variable_name, variable_value) VALUES(%?, %?)",
            &[
                tidb_sqlescape::SqlArg::String(tidb_meta_tidbvar::DXF_SCHEDULE_PAUSE_SCALE_IN.as_bytes()),
                tidb_sqlescape::SqlArg::String(bytes.as_bytes()),
            ],
        )
        .map(drop)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tidb_gocontext::{Context, CtxError};

    use super::{get_default_region_split_config, get_target_scope, run_with_retry, RetryError};

    fn backoffer() -> tidb_backoff::Exponential {
        // Go `backoff.NewExponential(100*time.Millisecond, 1, time.Second)`.
        tidb_backoff::new_exponential(100_000_000, 1.0, 1_000_000_000)
    }

    /// Go `require.Eventually(t, end.Load, 5*time.Second, 100*time.Millisecond)`.
    fn eventually(end: &AtomicBool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !end.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "condition never satisfied");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Go `TestRunWithRetry`.
    #[test]
    fn test_run_with_retry() {
        let ctx = Context::background();
        let logger = tidb_logutil::bg_logger();

        // retry count exceed
        let err = run_with_retry(&ctx, 3, &mut backoffer(), &logger, |_| (true, Err("mock error"))).unwrap_err();
        assert!(err.to_string().contains("mock error"));

        // non-retryable error
        let end = Arc::new(AtomicBool::new(false));
        let handle = {
            let (end, ctx, logger) = (Arc::clone(&end), ctx.clone(), logger.clone());
            std::thread::spawn(move || {
                let err = run_with_retry(&ctx, usize::MAX, &mut backoffer(), &logger, |_| (false, Err("mock error")));
                end.store(true, Ordering::SeqCst);
                assert!(err.is_err());
            })
        };
        eventually(&end);
        handle.join().unwrap();

        // fail with retryable error once, then success
        let end = Arc::new(AtomicBool::new(false));
        let handle = {
            let (end, ctx, logger) = (Arc::clone(&end), ctx.clone(), logger.clone());
            std::thread::spawn(move || {
                let mut i = 0;
                let result = run_with_retry(&ctx, usize::MAX, &mut backoffer(), &logger, |_| {
                    if i == 0 {
                        i += 1;
                        return (true, Err("mock error"));
                    }
                    (false, Ok(()))
                });
                end.store(true, Ordering::SeqCst);
                assert!(result.is_ok());
            })
        };
        eventually(&end);
        handle.join().unwrap();

        // context done
        let (subctx, cancel) = Context::with_cancel(&ctx);
        cancel.cancel();
        let err =
            run_with_retry(&subctx, usize::MAX, &mut backoffer(), &logger, |_| (true, Err("mock error"))).unwrap_err();
        assert!(matches!(err, RetryError::Context(CtxError::Canceled)));
    }

    /// Go `TestGetTargetScope`.
    #[test]
    fn test_get_target_scope() {
        let bak = tidb_vardef::service_scope();
        tidb_vardef::set_service_scope("test-scope".to_owned());
        if tidb_config_kerneltype::is_next_gen() {
            assert_eq!(get_target_scope(), "dxf_service");
        } else {
            assert_eq!(get_target_scope(), "test-scope");
        }
        tidb_vardef::set_service_scope(bak);
    }

    /// Go `TestGetDefaultRegionSplitConfig`.
    #[test]
    fn test_get_default_region_split_config() {
        let (size, keys) = get_default_region_split_config();
        if tidb_config_kerneltype::is_next_gen() {
            assert_eq!(size, 1 << 30);
            assert_eq!(keys, 102_400_000);
            return;
        }
        assert_eq!(size, 96 << 20);
        assert_eq!(keys, 960_000);
    }
}
