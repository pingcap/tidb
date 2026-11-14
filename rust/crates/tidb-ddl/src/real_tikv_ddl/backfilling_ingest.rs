// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `backfilling.go:addIndexWithLocalIngest` and the local ingest operators.
use super::*;
use tidb_ddl_build::table_backfill::IngestBatch;
use tidb_ddl_ingest::backend::{BackendCtx, UnregisterOpt};
use tidb_ddl_ingest::backend_mgr::{BackendCtxBuilder, create_local_backend};

/// Go `TableScanTask`.
struct TableScanTask {
    id: usize,
    start: Vec<u8>,
    end: Vec<u8>,
}
/// Go `IndexRecordChunk`: each task is fully scanned and its transaction
/// rolled back before local ingest is allowed to flush or import its rows.
struct IndexRecordChunk {
    id: usize,
    batches: Vec<IngestBatch>,
}
/// Go `IndexWriteResult`, consumed in order by the checkpoint/result sink.
struct IndexWriteResult {
    id: usize,
    count: i64,
    warnings: Vec<(u16, String, i64)>,
}
pub(super) enum PipelineError {
    Sql(LockSqlError),
    Storage(ClusterDdlError),
    Ingest(tidb_ddl_ingest::Error),
    Interrupted,
}
impl From<tidb_ddl_ingest::Error> for PipelineError {
    fn from(e: tidb_ddl_ingest::Error) -> Self {
        Self::Ingest(e)
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
                .ok_or_else(|| ClusterDdlError::Backfill(format!("index info not found: {id}")))
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
            .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?,
        )
    } else {
        None
    };
    let bc = BackendCtxBuilder::new(ctx.clone(), opener.store().clone(), job.clone())
        .with_checkpoint_manager_param(sess_pool, handle.physical_id)
        .build(backend.clone())
        .map_err(|e| ClusterDdlError::Backfill(e.to_string()))?;
    let result = (|| {
        let engines = bc.register(&reorg.index_ids, &uniques, &reorg.table)?;
        let start = bc.next_start_key().max(handle.start_key.clone());
        let result = execute_and_close_pipeline(
            opener,
            timeout,
            backfiller,
            reorg,
            handle.physical_id,
            &start,
            &handle.end_key,
            &bc,
            job.reorg_meta.as_ref(),
            &engines,
            check_owner,
            step_cancelled,
            warnings,
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
    let rows = bc.total_key_count();
    bc.close();
    let close = backend.as_ref().map_or(Ok(()), |backend| backend.close());
    match result {
        Ok(()) => {
            close.map_err(|e| ClusterDdlError::Backfill(e.to_string()))?;
            Ok(ReorgResult::Done(rows))
        }
        Err(PipelineError::Interrupted) => Ok(ReorgResult::Interrupted),
        Err(PipelineError::Sql(error)) => Ok(sql_failure(&error, rows)),
        Err(PipelineError::Storage(error)) => Err(error),
        Err(PipelineError::Ingest(tidb_ddl_ingest::Error::Database(error))) => Ok(sql_failure(
            &LockSqlError::from(tidb_terror::MysqlError::from_terror(&error)),
            rows,
        )),
        Err(PipelineError::Ingest(error)) => Err(ClusterDdlError::Backfill(error.to_string())),
    }
}
/// Go `executeAndClosePipeline`, composed scan / index-ingest / result-sink workers.
#[allow(clippy::too_many_arguments)]
pub(super) fn execute_and_close_pipeline<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    reorg: &AddIndexReorg,
    physical_id: i64,
    start: &[u8],
    end: &[u8],
    bc: &Arc<dyn BackendCtx>,
    reorg_meta: Option<&tidb_model::GoShared<tidb_model::DDLReorgMeta>>,
    engines: &[Arc<dyn tidb_ddl_ingest::engine::IngestEngine>],
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
    warnings: &mut Vec<(u16, String, i64)>,
) -> Result<(), PipelineError> {
    let (reader_cnt, writer_cnt) = expected_ingest_worker_cnt(reorg.concurrency, 0, false);
    let stop = AtomicBool::new(false);
    let error = Mutex::new(None);
    let collected_warnings = Mutex::new(Vec::new());
    let ctx = tidb_gocontext::Context::background();
    std::thread::scope(|scope| {
        let (task_tx, task_rx) = crossbeam_channel::bounded::<TableScanTask>(128);
        let (chunk_tx, chunk_rx) = crossbeam_channel::bounded::<IndexRecordChunk>(1);
        let (result_tx, result_rx) = crossbeam_channel::bounded::<IndexWriteResult>(1);
        let stop = &stop;
        let error = &error;
        let collected_warnings = &collected_warnings;
        for _ in 0..reader_cnt {
            let task_rx = task_rx.clone();
            let chunk_tx = chunk_tx.clone();
            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                for task in task_rx {
                    if stop.load(Ordering::SeqCst) {
                        continue;
                    }
                    crate::job_ctx::attach_top_profiling_info(
                        reorg.job_id,
                        &reorg.query.to_utf8_lossy_go(),
                    );
                    let result = catch_reorg_panic(|| {
                        scan_records(
                            opener,
                            timeout,
                            backfiller,
                            reorg,
                            physical_id,
                            &task,
                            bc.as_ref(),
                            reorg_meta,
                            check_owner,
                            step_cancelled,
                        )
                    });
                    match result {
                        Ok(batches) => {
                            for batch in &batches {
                                bc.update_chunk(
                                    task.id,
                                    batch.batch.added as i64,
                                    batch.batch.done,
                                );
                            }
                            if chunk_tx
                                .send(IndexRecordChunk {
                                    id: task.id,
                                    batches,
                                })
                                .is_err()
                            {
                                return;
                            }
                        }
                        Err(e) => {
                            let mut first = error.lock().unwrap();
                            if first.is_none() {
                                *first = Some(e);
                            }
                            stop.store(true, Ordering::SeqCst);
                        }
                    }
                }
            }));
        }
        drop(task_rx);
        drop(chunk_tx);
        for worker_id in 0..writer_cnt {
            let chunk_rx = chunk_rx.clone();
            let result_tx = result_tx.clone();
            scope.spawn(tidb_cpuprofile::labels::inherit(move || {
                let writers = engines
                    .iter()
                    .zip(&reorg.index_ids)
                    .map(|(engine, index_id)| {
                        engine
                            .clone()
                            .create_writer(
                                worker_id,
                                bc.get_local_backend()
                                    .map_or(0, |b| b.cfg.local_writer_mem_cache_size),
                            )
                            .map(|w| (*index_id, w))
                    })
                    .collect::<tidb_ddl_ingest::Result<std::collections::HashMap<_, _>>>();
                let mut writers = match writers {
                    Ok(w) => w,
                    Err(e) => {
                        let mut first = error.lock().unwrap();
                        if first.is_none() {
                            *first = Some(e.into());
                        }
                        stop.store(true, Ordering::SeqCst);
                        return;
                    }
                };
                for record in chunk_rx {
                    if stop.load(Ordering::SeqCst) {
                        continue;
                    }
                    crate::job_ctx::attach_top_profiling_info(
                        reorg.job_id,
                        &reorg.query.to_utf8_lossy_go(),
                    );
                    for batch in record.batches {
                        let result = catch_reorg_panic(|| {
                            if tidb_failpoint::eval(
                                "github.com/pingcap/tidb/pkg/ddl/mockWriteLocalError",
                            )
                            .is_some()
                            {
                                return Err(tidb_ddl_ingest::Error::Other(
                                    "mock write local error".into(),
                                )
                                .into());
                            }
                            for entry in batch.entries {
                                if tidb_failpoint::eval(
                                    "github.com/pingcap/tidb/pkg/ddl/mockLocalWriterPanic",
                                )
                                .is_some()
                                {
                                    panic!("mock panic");
                                }
                                writers
                                    .get_mut(&entry.index_id)
                                    .expect("registered index writer")
                                    .write_row(&entry.key, &entry.value, &entry.row_id)?;
                                if tidb_failpoint::eval(
                                    "github.com/pingcap/tidb/pkg/ddl/mockLocalWriterError",
                                )
                                .is_some()
                                {
                                    return Err(tidb_ddl_ingest::Error::Other(
                                        "mock engine error".into(),
                                    )
                                    .into());
                                }
                            }
                            Ok(())
                        });
                        if let Err(e) = result {
                            let mut first = error.lock().unwrap();
                            if first.is_none() {
                                *first = Some(e.into());
                            }
                            stop.store(true, Ordering::SeqCst);
                            break;
                        }
                        if result_tx
                            .send(IndexWriteResult {
                                id: record.id,
                                count: batch.batch.added as i64,
                                warnings: batch.batch.warnings,
                            })
                            .is_err()
                        {
                            return;
                        }
                    }
                }
            }));
        }
        drop(chunk_rx);
        drop(result_tx);
        let sink_ctx = &ctx;
        scope.spawn(tidb_cpuprofile::labels::inherit(move || {
            for result in result_rx {
                if stop.load(Ordering::SeqCst) {
                    continue;
                }
                let outcome = catch_reorg_panic(|| {
                    bc.ingest_if_quota_exceeded(sink_ctx, result.id, result.count)?;
                    Ok(())
                });
                if let Err(e) = outcome {
                    let mut first = error.lock().unwrap();
                    if first.is_none() {
                        *first = Some(e);
                    }
                    stop.store(true, Ordering::SeqCst);
                    continue;
                }
                super::super::backfilling::merge_warnings(
                    &mut collected_warnings.lock().unwrap(),
                    result.warnings,
                );
            }
        }));
        // Go `TableScanTaskSource.generateTasks`: region ranges, monotonic IDs.
        let region_store = backfiller.region_store();
        let mut start = start.to_vec();
        let mut id = 0;
        while start.as_slice() < end && !stop.load(Ordering::SeqCst) {
            let ranges = super::super::backfilling::load_table_ranges(
                region_store.as_deref(),
                physical_id,
                &start,
                end,
                128,
            );
            match ranges {
                Ok(ranges) if ranges.is_empty() => break,
                Ok(ranges) => {
                    for (range_start, range_end) in ranges {
                        start = range_end.clone();
                        if task_tx
                            .send(TableScanTask {
                                id,
                                start: range_start,
                                end: range_end,
                            })
                            .is_err()
                        {
                            break;
                        }
                        id += 1;
                    }
                }
                Err(e) => {
                    let mut first = error.lock().unwrap();
                    if first.is_none() {
                        *first = Some(PipelineError::Storage(e));
                    }
                    stop.store(true, Ordering::SeqCst);
                }
            }
        }
        drop(task_tx);
    });
    if let Some(error) = error.into_inner().unwrap() {
        return Err(error);
    }
    if !reorg_runnable(step_cancelled, check_owner).map_err(PipelineError::Storage)? {
        return Err(PipelineError::Interrupted);
    }
    if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockFlushError").is_some() {
        return Err(tidb_ddl_ingest::Error::Other("mock flush error".into()).into());
    }
    bc.ingest(&ctx)?;
    super::super::backfilling::merge_warnings(warnings, collected_warnings.into_inner().unwrap());
    Ok(())
}
/// Go `tableScanWorker.scanRecords` / `wrapInBeginRollback`: one timestamp for
/// the complete region task, released before any chunk enters the writer.
#[allow(clippy::too_many_arguments)]
fn scan_records<S: StoreClient>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    reorg: &AddIndexReorg,
    physical_id: i64,
    task: &TableScanTask,
    bc: &dyn BackendCtx,
    reorg_meta: Option<&tidb_model::GoShared<tidb_model::DDLReorgMeta>>,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
) -> Result<Vec<IngestBatch>, PipelineError> {
    reorg_transaction(
        opener,
        timeout,
        &crate::job_ctx::backfill_request_source(reorg.job_id),
        |_, snapshot, buffer, _| {
            let result = (|| {
                if tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/mockScanRecordError")
                    .is_some()
                {
                    return Err(PipelineError::Ingest(tidb_ddl_ingest::Error::Other(
                        "mock scan record error".into(),
                    )));
                }
                if let Some(meta) = reorg_meta {
                    let mut meta = meta.read().clone();
                    tidb_failpoint::inject_call(
                        "github.com/pingcap/tidb/pkg/ddl/scanRecordExec",
                        &mut meta,
                    );
                }
                bc.add_chunk(task.id, task.end.clone());
                let mut start = task.start.clone();
                let mut chunks = Vec::new();
                loop {
                    if !reorg_runnable(step_cancelled, check_owner)
                        .map_err(PipelineError::Storage)?
                    {
                        return Err(PipelineError::Interrupted);
                    }
                    tidb_failpoint::inject_call(
                        "github.com/pingcap/tidb/pkg/ddl/beforeGetChunk",
                        &mut (),
                    );
                    let work = AddIndexReorgWork::Ingest {
                        physical_id,
                        start: &start,
                        end: &task.end,
                        batch_size: reorg.batch_size,
                    };
                    let chunk =
                        match backfiller.reorg_add_index(reorg, work, snapshot.clone(), buffer) {
                            Ok(AddIndexReorgAnswer::Ingest(chunk)) => chunk,
                            Err(e) => return Err(PipelineError::Sql(e)),
                            _ => {
                                return Err(PipelineError::Storage(ClusterDdlError::Backfill(
                                    "ingest scan answered something else".into(),
                                )));
                            }
                        };
                    let done = chunk.batch.done;
                    start = chunk.batch.next_key.clone();
                    chunks.push(chunk);
                    if done {
                        break;
                    }
                }
                Ok(chunks)
            })();
            buffer.reset();
            Ok((result, Vec::new()))
        },
    )
    .map_err(PipelineError::Storage)?
}

/// Go `AsyncOperator` panic recovery, reporting `ErrReorgPanic` to the sink.
fn catch_reorg_panic<T>(
    work: impl FnOnce() -> Result<T, PipelineError>,
) -> Result<T, PipelineError> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)).unwrap_or_else(|_| {
        Err(PipelineError::Ingest(tidb_ddl_ingest::Error::Database(
            tidb_dbterror::ERR_REORG_PANIC.gen_with_stack_by_args(&[]),
        )))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
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
