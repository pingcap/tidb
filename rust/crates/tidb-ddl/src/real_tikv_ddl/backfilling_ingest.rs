// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `backfilling.go:addIndexWithLocalIngest` and the local ingest operators.
use super::*;
use super::operators::{new_add_index_ingest_pipeline, ScanContext};
use operator::{AsyncPipeline, Context, OperatorError};
use tidb_ddl_ingest::backend::{BackendCtx, UnregisterOpt};
use tidb_ddl_ingest::backend_mgr::{BackendCtxBuilder, create_local_backend};

/// How an ingest pipeline failed.
#[derive(Debug)]
pub enum PipelineError {
    /// A SQL failure of the reorganization (a duplicate entry, a bad value).
    Sql(LockSqlError),
    /// A storage or owner failure of the step.
    Storage(ClusterDdlError),
    /// A backend or engine failure.
    Ingest(tidb_ddl_ingest::Error),
    /// The step's context ended: the owner changed or the job was cancelled.
    Interrupted,
    /// Any other error an operator returned (Go's `error`), kept as it is.
    Operator(OperatorError),
}
impl From<tidb_ddl_ingest::Error> for PipelineError {
    fn from(e: tidb_ddl_ingest::Error) -> Self {
        Self::Ingest(e)
    }
}
impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(e) => f.write_str(&e.message),
            Self::Storage(e) => e.fmt(f),
            Self::Ingest(e) => e.fmt(f),
            Self::Interrupted => f.write_str("context canceled"),
            Self::Operator(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(e) => Some(e),
            Self::Ingest(e) => Some(e),
            Self::Operator(e) => Some(e),
            Self::Sql(_) | Self::Interrupted => None,
        }
    }
}
impl PipelineError {
    /// The error a pipeline's operator returned, as the caller matches it:
    /// a [`PipelineError`] an operator raised is answered as itself (an
    /// engine failure other than a SQL one stays behind the operator's
    /// error, which keeps its cause), anything else as `Operator`.
    pub(super) fn of(error: &OperatorError) -> Self {
        match error.cause().downcast_ref::<PipelineError>() {
            Some(Self::Sql(e)) => Self::Sql(e.clone()),
            Some(Self::Storage(e)) => Self::Storage(e.clone()),
            Some(Self::Interrupted) => Self::Interrupted,
            Some(Self::Ingest(tidb_ddl_ingest::Error::Database(e))) => {
                Self::Ingest(tidb_ddl_ingest::Error::Database(e.clone()))
            }
            Some(Self::Operator(e)) => Self::Operator(e.clone()),
            Some(Self::Ingest(_)) | None => Self::Operator(error.clone()),
        }
    }
}

/// Go `expectedIngestWorkerCnt`.
pub(super) fn expected_ingest_worker_cnt(
    concurrency: usize,
    avg_row_size: usize,
    use_global_sort: bool,
) -> (usize, usize) {
    if use_global_sort {
        return (concurrency, concurrency);
    }
    if avg_row_size == 0 {
        return (
            (concurrency / 2).clamp(1, 16),
            (concurrency / 2 + 2).min(16),
        );
    }
    let ratio = if avg_row_size <= 200 {
        0.5
    } else if avg_row_size <= 500 {
        1.0
    } else if avg_row_size <= 1000 {
        2.0
    } else if avg_row_size <= 3000 {
        4.0
    } else {
        8.0
    };
    (
        ((concurrency as f64 * ratio) as usize).max(1),
        concurrency.max(1),
    )
}
/// Go `ddlCtx.addIndexWithLocalIngest`: non-transactional SST import for one
/// physical table. The caller retains Go's temporary-index merge afterwards.
#[allow(clippy::too_many_arguments)]
pub(super) fn add_index_with_local_ingest<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    reorg: &AddIndexReorg,
    job: &tidb_model::Job,
    handle: &ReorgHandle,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
    warnings: &mut Vec<(u16, String, i64)>,
) -> Result<ReorgResult, ClusterDdlError> {
    if !reorg_runnable(step_cancelled, check_owner)? {
        return Ok(ReorgResult::Interrupted);
    }
    let uniques = reorg
        .index_ids
        .iter()
        .map(|id| {
            reorg
                .table
                .indices
                .iter_deref()
                .find(|i| i.read().id == *id)
                .map(|i| i.read().unique)
                .ok_or_else(|| ClusterDdlError::backfill_message(format!("index info not found: {id}")))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ctx = tidb_gocontext::Context::background();
    let backend = if tidb_config::config_tree::config::get_global_config()
        .store
        .0
        == tidb_config::store::STORE_TYPE_TIKV
    {
        Some(
            create_local_backend(
                opener.store().clone(),
                job,
                uniques.iter().any(|v| *v),
                false,
                reorg.concurrency,
                0,
            )
            .map_err(|e| ClusterDdlError::backfill(&e))?,
        )
    } else {
        None
    };
    // Go `estimateTableRowSize`.
    let avg_row_size = crate::index::estimate_table_row_size(opener.store(), sess_pool.as_ref(), reorg.table.id).max(0) as usize;
    // Go `reorgInfo.UpdateConfigFromSysTbl`: the job's reorg meta takes the
    // running job's alterable config.
    let update_pool = Arc::clone(&sess_pool);
    let update_config = move || match super::add_index_reorg::latest_job(update_pool.as_ref(), job.id) {
        Ok(Some(latest)) if latest.state == tidb_model::JobState::RUNNING && latest.is_alterable() => {
            if let (Some(meta), Some(latest_meta)) = (job.reorg_meta.as_ref(), latest.reorg_meta.as_ref()) {
                let defaults = crate::index::reorg_defaults();
                let latest_meta = latest_meta.read();
                let meta = meta.read();
                meta.set_concurrency(latest_meta.get_concurrency(defaults));
                meta.set_batch_size(latest_meta.get_batch_size(defaults));
                meta.set_max_write_speed(latest_meta.get_max_write_speed());
            }
        }
        Ok(_) => {}
        Err(error) => tidb_ddl_logutil::ddl_logger().warn(
            "failed to get latest job from system table",
            &[
                tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id)),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
            ],
        ),
    };
    let bc = BackendCtxBuilder::new(ctx.clone(), opener.store().clone(), job.clone())
        .with_checkpoint_manager_param(sess_pool, handle.physical_id)
        .build(backend.clone())
        .map_err(|e| ClusterDdlError::backfill(&e))?;
    // Go `NewLocalWorkerCtx`.
    let wctx = Arc::new(Context::new());
    let scan = Arc::new(ScanContext {
        opener: Arc::clone(opener),
        timeout,
        backfiller: backfiller.shared(),
        reorg: Arc::new(reorg.clone()),
        physical_id: handle.physical_id,
        warnings: Arc::default(),
    });
    let result = (|| {
        let engines = bc.register(&reorg.index_ids, &uniques, &reorg.table)?;
        let pipe = new_add_index_ingest_pipeline(
            &wctx,
            Arc::clone(&scan),
            Arc::clone(&bc),
            engines,
            job.id,
            handle.start_key.clone(),
            handle.end_key.clone(),
            job.reorg_meta.clone(),
            avg_row_size,
            reorg.concurrency,
            Arc::new(tidb_dxf_execute::NoopCollector),
        );
        let result = execute_and_close_pipeline(
            &wctx,
            &pipe,
            Some((job, &update_config)),
            Some(&bc),
            avg_row_size,
            check_owner,
            step_cancelled,
        );
        match result {
            Ok(()) => bc
                .finish_and_unregister_engines(
                    &ctx,
                    UnregisterOpt(UnregisterOpt::CLEAN_DATA.0 | UnregisterOpt::CHECK_DUP.0),
                )
                .map_err(PipelineError::from),
            Err(error) => {
                let _ = bc.finish_and_unregister_engines(&ctx, UnregisterOpt::CLOSE_ENGINES);
                Err(error)
            }
        }
    })();
    wctx.cancel();
    super::super::backfilling::merge_warnings(warnings, std::mem::take(&mut scan.warnings.lock().unwrap()));
    let rows = bc.total_key_count();
    bc.close();
    let close = backend.as_ref().map_or(Ok(()), |backend| backend.close());
    match result {
        Ok(()) => {
            close.map_err(|e| ClusterDdlError::backfill(&e))?;
            Ok(ReorgResult::Done(rows))
        }
        Err(PipelineError::Interrupted) => Ok(ReorgResult::Interrupted),
        Err(PipelineError::Sql(error)) => Ok(sql_failure(&error, rows)),
        Err(PipelineError::Storage(error)) => Err(error),
        Err(PipelineError::Ingest(tidb_ddl_ingest::Error::Database(error))) => Ok(sql_failure(
            &LockSqlError::from(tidb_terror::MysqlError::from_terror(&error)),
            rows,
        )),
        Err(PipelineError::Ingest(error)) => Err(ClusterDdlError::backfill(&error)),
        Err(PipelineError::Operator(error)) => Err(ClusterDdlError::backfill(&error)),
    }
}
/// Go `readIndexStepExecutor.ResourceModified` and the tuning half of
/// `adjustWorkerCntAndMaxWriteSpeed`: the pipeline's reader and writer pools
/// take the worker counts `concurrency` calls for.
pub(super) fn tune_pipeline(
    pipe: &AsyncPipeline,
    concurrency: usize,
    avg_row_size: usize,
    global_sort: bool,
    wait: bool,
) -> Result<(), String> {
    let (Some(reader), Some(writer)) = pipe.get_reader_and_writer() else {
        return Err("failed to get local ingest mode reader or writer".to_owned());
    };
    let (target_reader_cnt, target_writer_cnt) = expected_ingest_worker_cnt(concurrency, avg_row_size, global_sort);
    if target_reader_cnt as i32 != reader.worker_pool_size() {
        reader.tune_worker_pool_size(target_reader_cnt as i32, wait);
    }
    if target_writer_cnt as i32 != writer.worker_pool_size() {
        writer.tune_worker_pool_size(target_writer_cnt as i32, wait);
    }
    Ok(())
}

/// Go `executeAndClosePipeline`: runs the pipeline to its end, retuning it
/// from the job's config while it runs (`reorg_info`), and answers the first
/// operator error. Go's reorganization context is cancelled when the owner
/// changes or the job is cancelled; here a watcher cancels `ctx` once
/// `check_owner` or `step_cancelled` says the step may not continue, and that
/// is answered as `Interrupted`.
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_and_close_pipeline(
    ctx: &Arc<Context>,
    pipe: &Arc<AsyncPipeline>,
    reorg_info: Option<(&tidb_model::Job, &(dyn Fn() + Sync))>,
    bc: Option<&Arc<dyn BackendCtx>>,
    avg_row_size: usize,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
) -> Result<(), PipelineError> {
    pipe.execute().map_err(|error| PipelineError::of(&error))?;
    let close_result = std::thread::scope(|scope| {
        let (done_tx, done_rx) = crossbeam_channel::bounded::<()>(0);
        let watch_done = done_rx.clone();
        scope.spawn(move || loop {
            match watch_done.recv_timeout(Duration::from_millis(50)) {
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
                _ => return,
            }
            if !matches!(reorg_runnable(step_cancelled, check_owner), Ok(true)) {
                ctx.cancel();
                return;
            }
        });
        if let (Some((job, update_config)), Some(bc)) = (reorg_info, bc) {
            let adjust_pipe = Arc::clone(pipe);
            scope.spawn(move || {
                adjust_worker_cnt_and_max_write_speed(&done_rx, &adjust_pipe, bc, avg_row_size, job, update_config)
            });
        }
        let result = pipe.close();
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/afterPipeLineClose", &mut Arc::clone(pipe));
        drop(done_tx);
        result
    });
    let result = match ctx.operator_err() {
        Some(error) => Err(PipelineError::of(&error)),
        None => close_result.map_err(|error| PipelineError::of(&error)),
    };
    if result.is_err() && !reorg_runnable(step_cancelled, check_owner).map_err(PipelineError::Storage)? {
        return Err(PipelineError::Interrupted);
    }
    result
}

/// Go `adjustWorkerCntAndMaxWriteSpeed`: every `UpdateDDLJobReorgCfgInterval`
/// until the pipeline closes, the job's latest reorg config retunes the
/// write speed and the reader and writer pools.
fn adjust_worker_cnt_and_max_write_speed(
    done: &crossbeam_channel::Receiver<()>,
    pipe: &AsyncPipeline,
    bc: &Arc<dyn BackendCtx>,
    avg_row_size: usize,
    job: &tidb_model::Job,
    update_config: &(dyn Fn() + Sync),
) {
    loop {
        let interval = Duration::from_millis(
            crate::index::UPDATE_DDL_JOB_REORG_CFG_INTERVAL_MS.load(Ordering::SeqCst),
        );
        match done.recv_timeout(interval) {
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {}
            _ => return,
        }
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/onUpdateJobParam", &mut ());
        update_config();
        let Some(meta) = job.reorg_meta.as_ref() else {
            continue;
        };
        let (max_write_speed, use_cloud_storage) = {
            let meta = meta.read();
            (meta.get_max_write_speed(), meta.use_cloud_storage)
        };
        if let Some(backend) = bc.get_local_backend() {
            if max_write_speed != backend.get_write_speed_limit() {
                backend.update_write_speed_limit(max_write_speed);
                tidb_ddl_logutil::ddl_ingest_logger().info(
                    "adjust ddl job config success",
                    &[
                        tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id)),
                        tidb_log::Field::new("max write speed", tidb_log::Value::I64(backend.get_write_speed_limit())),
                    ],
                );
            }
        }
        let concurrency = crate::cluster_ddl::add_index::job_concurrency(job);
        let (Some(reader), Some(writer)) = pipe.get_reader_and_writer() else {
            tidb_ddl_logutil::ddl_ingest_logger().error(
                "failed to get local ingest mode reader or writer",
                &[tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id))],
            );
            return;
        };
        let (target_reader_cnt, target_writer_cnt) = expected_ingest_worker_cnt(concurrency, avg_row_size, use_cloud_storage);
        if reader.worker_pool_size() != target_reader_cnt as i32 || writer.worker_pool_size() != target_writer_cnt as i32 {
            reader.tune_worker_pool_size(target_reader_cnt as i32, false);
            writer.tune_worker_pool_size(target_writer_cnt as i32, false);
            tidb_ddl_logutil::ddl_ingest_logger().info(
                "adjust ddl job config success",
                &[
                    tidb_log::Field::new("jobID", tidb_log::Value::I64(job.id)),
                    tidb_log::Field::new("table scan operator count", tidb_log::Value::I64(i64::from(reader.worker_pool_size()))),
                    tidb_log::Field::new("index ingest operator count", tidb_log::Value::I64(i64::from(writer.worker_pool_size()))),
                ],
            );
        }
        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/checkReorgConcurrency", &mut job.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `addindextest2.TestAlterJobOnDXF`: thread=8 tunes the running
    /// pipeline's reader and writer pools to four and six workers, waiting
    /// for the retired ones.
    #[test]
    fn test_alter_job_on_dxf() {
        let ctx = Arc::new(Context::new());
        let op = |workers| {
            Arc::new(operator::AsyncOperator::<Task, Task>::new_async_operator_with_transform(
                Arc::clone(&ctx),
                "test",
                workers,
                |task| task,
            ))
        };
        let (source, reader, writer, sink) = (op(1), op(1), op(2), op(1));
        let pipe = AsyncPipeline::new(vec![source, reader, writer, sink]);
        tune_pipeline(&pipe, 8, 0, false, true).unwrap();
        let (reader, writer) = pipe.get_reader_and_writer();
        assert_eq!((reader.unwrap().worker_pool_size(), writer.unwrap().worker_pool_size()), (4, 6));
        tune_pipeline(&pipe, 1, 0, false, true).unwrap();
        let (reader, writer) = pipe.get_reader_and_writer();
        assert_eq!((reader.unwrap().worker_pool_size(), writer.unwrap().worker_pool_size()), (1, 2));
        assert!(tune_pipeline(&AsyncPipeline::new(Vec::new()), 8, 0, false, true).is_err());
    }
    struct Task;
    impl tidb_resourcemanager_pool_workerpool::TaskMayPanic<OperatorError> for Task {
        fn recover_args(&self) -> (String, String, Option<OperatorError>) {
            (String::new(), String::new(), None)
        }
    }
    /// Go `backfilling_txn_executor_test.go:TestExpectedIngestWorkerCnt`.
    #[test]
    fn test_expected_ingest_worker_cnt() {
        for (concurrency, size, global, reader, writer) in [
            (10, 100, true, 10, 10),
            (20, 500, true, 20, 20),
            (10, 0, false, 5, 7),
            (40, 0, false, 16, 16),
            (1, 0, false, 1, 2),
            (10, 100, false, 5, 10),
            (10, 300, false, 10, 10),
            (10, 600, false, 20, 10),
            (10, 2000, false, 40, 10),
            (10, 5000, false, 80, 10),
        ] {
            assert_eq!(
                expected_ingest_worker_cnt(concurrency, size, global),
                (reader, writer)
            );
        }
    }
}
