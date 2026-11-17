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

//! The owner's side of ADD INDEX's transactional reorganization: Go
//! `getReorgInfo` (the handle's first persistence) and `runReorgJob` over the
//! txn / txn-merge backfill (`writePhysicalTableRecord`,
//! `addIndexTxnWorker`, `mergeIndexWorker`). Every batch runs in its own
//! optimistic transaction that locks the rows it indexed, and is retried on a
//! write conflict like Go's `kv.RunInNewTxn(..., retryable=true, ...)`.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use tidb_model::JobState;
use tidb_txnkv::transaction::BufferMutation;

use super::*;
use tidb_infoschema::cluster_catalog::PagedMetaSnapshot;
use crate::cluster_ddl::add_index::{
    AddIndexReorg, ReorgHandle, ReorgOutcome, ReorgResult, ReorgStageRequest, COLUMN_ELEMENT_KEY,
    INDEX_ELEMENT_KEY, TEMP_INDEX_PREFIX,
};
use super::backfilling::{write_physical_table_record, BackfillExecutor, BatchStep, TableRecordOutcome};

#[path = "backfilling_ingest.rs"]
pub(crate) mod ingest;
#[path = "backfilling_operators.rs"]
pub mod operators;
#[path = "backfilling_dist.rs"]
pub(crate) mod dist;

/// Runs what `reorg` asks for and answers its outcome. A storage or owner
/// failure is the caller's error; a SQL failure of the reorganization itself
/// (a duplicate entry, a bad value) is the outcome Go's
/// `runReorgJobAndHandleErr` turns into a rollback.
pub(super) fn run_add_index_reorg<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    reorg: &AddIndexReorg,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    reorg_slot: &ReorgSlot,
) -> Result<ReorgOutcome, ClusterDdlError>
where
    S: StoreClient,
{
    // Go `w.sessPool`: every reorg-handle statement runs on its sessions.
    let sess_pool = backfiller.session_pool().ok_or_else(||
        ClusterDdlError::backfill_message("the DDL worker has no session pool".to_owned()))?;
    let mut warnings = Vec::new();
    let result = match &reorg.stage {
        ReorgStageRequest::Init => init_reorg(
            opener,
            timeout,
            backfiller,
            sess_pool.as_ref(),
            reorg,
        )?,
        ReorgStageRequest::Run(handle) => {
            // Go `jobCtx.initStepCtx`: a step a reorg timeout ended leaves
            // its context to the next.
            let step_cancelled = reorg_slot.step_ctx();
            std::thread::scope(|scope| {
                let (stop_checking_job_cancelled, stopped) = std::sync::mpsc::channel::<()>();
                let step_cancelled = &step_cancelled;
                let checker_pool = sess_pool.as_ref();
                scope.spawn(move || {
                    check_job_cancelled(
                        reorg.job_id,
                        || latest_job(checker_pool, reorg.job_id),
                        CHECK_JOB_CANCELLED_INTERVAL,
                        stopped,
                        step_cancelled,
                    )
                });
                let result = run_reorg(
                    opener,
                    timeout,
                    backfiller,
                    &sess_pool,
                    reorg,
                    handle,
                    step_cancelled,
                    reorg_slot,
                    &mut warnings,
                );
                drop(stop_checking_job_cancelled);
                // Go `runReorgJobAndHandleErr`: an index reorganization's
                // error is the job's to handle, not a retry of this owner's.
                match result {
                    Err(error) if is_index_reorg(reorg) && check_owner().is_ok() => {
                        Ok(ReorgResult::Error(reorg_fn_error(error)))
                    }
                    result => result,
                }
            })?
        }
        ReorgStageRequest::Check(check) => {
            reorg_transaction(opener, timeout, &crate::job_ctx::internal_ddl_source(), |_, snapshot, buffer, _| {
                let answer = backfiller.reorg_add_index(
                    reorg,
                    AddIndexReorgWork::CheckColumn(check),
                    snapshot,
                    buffer,
                );
                buffer.reset();
                Ok((answer, Vec::new()))
            })
            .and_then(|answer| match answer {
                Ok(AddIndexReorgAnswer::Checked(found)) => Ok(ReorgResult::Checked(
                    found.map(|error| (error.code, error.message)),
                )),
                Ok(_) => Err(ClusterDdlError::backfill_message(
                    "a column check answered something else".to_owned(),
                )),
                // Go: a failure to run the check is retried, not the job's
                // error.
                Err(error) => Err(ClusterDdlError::Backfill(crate::cluster_ddl::reorg_failure(
                    error.code,
                    &error.message,
                    error.identity,
                ))),
            })?
        }
        ReorgStageRequest::CheckForeignKey(check) => {
            reorg_transaction(opener, timeout, &crate::job_ctx::internal_ddl_source(), |_, snapshot, buffer, _| {
                let answer = backfiller.stage_foreign_key_check(check, snapshot, buffer);
                buffer.reset();
                Ok((answer, Vec::new()))
            })
            // Go `onCreateForeignKey` rolls the job back on any error the
            // check returns.
            .map(|answer| {
                ReorgResult::Checked(answer.err().map(|error| (error.code, error.message)))
            })?
        }
        ReorgStageRequest::BuildMaterializedView(build) => {
            // Go `runReorgJob(..., buildCreateMaterializedViewData)`: an error
            // rolls the job back.
            match crate::cluster_ddl::materialized_view::build_create_materialized_view_data(sess_pool.as_ref(), build) {
                Ok(read_ts) => ReorgResult::Built(read_ts),
                Err(error) => ReorgResult::Failed {
                    code: error.code,
                    message: error.message.clone(),
                    rows: reorg.row_count,
                    identity: error.identity,
                },
            }
        }
        ReorgStageRequest::CheckExchangePartition(check) => {
            reorg_transaction(opener, timeout, &crate::job_ctx::internal_ddl_source(), |_, snapshot, buffer, _| {
                let answer = backfiller.stage_exchange_partition_check(check, snapshot, buffer);
                buffer.reset();
                Ok((answer, Vec::new()))
            })
            // Go `onExchangeTablePartition` rolls the job back on any error
            // the validation returns.
            .map(|answer| {
                ReorgResult::Checked(answer.err().map(|error| (error.code, error.message)))
            })?
        }
    };
    Ok(ReorgOutcome {
        job_id: reorg.job_id,
        merging: reorg.merging,
        column: reorg.column.clone(),
        sub_job: reorg.sub_job,
        partition_phase: reorg.partition_phase,
        row_count: reorg.row_count,
        warnings,
        result,
    })
}

/// The element a reorganization's handle names: the changing column of a
/// row rewrite, else the first index.
fn reorg_element(reorg: &AddIndexReorg) -> Option<(i64, &'static [u8])> {
    // Go `BuildElements(tbl.Meta().Columns[0], indices)`: a partition
    // reorganization's row copy is its first column's element.
    if reorg.partition_phase == Some(crate::cluster_ddl::add_index::PartitionReorgPhase::CopyRows) {
        return reorg
            .table
            .columns
            .get(0)
            .map(|column| (column.read().id, COLUMN_ELEMENT_KEY));
    }
    match &reorg.column {
        Some(column) => Some((column.changing_column_id, COLUMN_ELEMENT_KEY)),
        None => reorg.index_ids.first().map(|id| (*id, INDEX_ELEMENT_KEY)),
    }
}

/// Go `cleanupDDLReorgHandles`: best effort, once a job finished.
pub(super) fn clean_ddl_reorg_handles(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    job_id: i64,
) -> Result<(), ClusterDdlError> {
    let mut se = sess_pool.open_sql_session().map_err(DdlPlanError::from_mysql_error)?;
    crate::ddl_reorg_table::clean_ddl_reorg_handles(se.as_mut(), job_id)
        .map_err(DdlPlanError::from_mysql_error)?;
    Ok(())
}

/// One reorganization transaction: `work` reads through the snapshot,
/// stages into the buffer and names extra mutations; the whole is committed
/// at once and retried on a write conflict.
pub(super) fn reorg_transaction<S, T>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    source: &tidb_txnkv::RequestSource,
    work: impl FnMut(
        &mut SnapshotMetaSnapshot,
        Arc<Mutex<dyn ClusterSnapshot>>,
        &MutationBuffer,
        u64,
    ) -> Result<(T, Vec<BufferMutation>), ClusterDdlError>,
) -> Result<T, ClusterDdlError>
where
    S: StoreClient,
{
    reorg_transaction_at(opener, timeout, source, None, work)
}

/// [`reorg_transaction`] reading at `start_ts` when given: the start
/// timestamp of a transaction a pooled session opened (Go
/// `wrapInBeginRollback`'s `txn.StartTS()`).
pub(super) fn reorg_transaction_at<S, T>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    source: &tidb_txnkv::RequestSource,
    start_ts: Option<u64>,
    mut work: impl FnMut(
        &mut SnapshotMetaSnapshot,
        Arc<Mutex<dyn ClusterSnapshot>>,
        &MutationBuffer,
        u64,
    ) -> Result<(T, Vec<BufferMutation>), ClusterDdlError>,
) -> Result<T, ClusterDdlError>
where
    S: StoreClient,
{
    let mut attempt: u32 = 0;
    loop {
        let mut options = tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions::from(
            tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
        )
        .with_request_source(source.clone());
        // Go `updateTxnEntrySizeLimitIfNeeded`: the GLOBAL limit, not the
        // session's, bounds a backfill transaction's entries.
        let options_entry_limit = tidb_vardef::TXN_ENTRY_SIZE_LIMIT.load(std::sync::atomic::Ordering::SeqCst);
        options.entry_size_limit = options_entry_limit;
        let transaction = match start_ts {
            Some(start_ts) => SessionTransaction::begin_at(Arc::clone(opener), start_ts, timeout, options)?,
            None => SessionTransaction::begin(Arc::clone(opener), timeout, options)?,
        };
        let start_ts = transaction.start_ts();
        let buffer = MutationBuffer::new();
        // Go `updateTxnEntrySizeLimitIfNeeded`'s `kv.SizeLimits{Entry,
        // Total: kv.TxnTotalSizeLimit}` over the batch's writes.
        if options_entry_limit > 0 {
            buffer.set_pending_size_limits(options_entry_limit, tidb_txnkv::txn_total_size_limit());
        }
        let staged = (|| {
            let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
            slot.lock()
                .map_err(|_| ClusterDdlError::backfill_message("snapshot slot poisoned".to_owned()))?
                .bind(
                    transaction
                        .snapshot()
                        .map_err(|error| ClusterDdlError::backfill_storage(error))?,
                );
            let handle: Arc<Mutex<dyn ClusterSnapshot>> = slot;
            let mut meta = SnapshotMetaSnapshot::new(
                transaction
                    .snapshot()
                    .map_err(|error| ClusterDdlError::backfill_storage(error))?,
            );
            work(&mut meta, handle, &buffer, start_ts)
        })();
        let (value, mutations) = match staged {
            Ok(staged) => staged,
            Err(error) => {
                let _ = transaction.rollback();
                return Err(error);
            }
        };
        if buffer.is_empty() && mutations.is_empty() {
            transaction
                .rollback()
                .map_err(ClusterDdlError::NotCommitted)?;
            return Ok(value);
        }
        match transaction.commit_with(&buffer, mutations) {
            Ok(_) => return Ok(value),
            Err(error) => {
                let cause = classify_session_ddl_commit_error(0, error);
                if matches!(cause, ClusterDdlError::ConcurrentSchemaChange { .. })
                    && attempt + 1 < tidb_txnkv::MAX_RETRY_COUNT
                {
                    std::thread::sleep(tidb_txnkv::retry_backoff_delay(attempt));
                    attempt += 1;
                    continue;
                }
                return Err(cause);
            }
        }
    }
}

/// Go `tablecodec.TempIndexPrefix | id`'s seek key in `physical_id`.
fn temp_index_seek_key(physical_id: i64, index_id: i64) -> Vec<u8> {
    tidb_tablecodec::table_key::encode_index_seek_key(physical_id, TEMP_INDEX_PREFIX | index_id, &[])
}

/// Go `tablecodec.GetTableIndexKeyRange` of an index's temporary entries.
fn temp_index_range(physical_id: i64, index_id: i64) -> (Vec<u8>, Vec<u8>) {
    let start = temp_index_seek_key(physical_id, index_id);
    let end = tidb_txnkv::Key::from_bytes(start.clone())
        .prefix_next()
        .as_bytes()
        .to_vec();
    (start, end)
}

fn sql_failure(error: &LockSqlError, rows: i64) -> ReorgResult {
    ReorgResult::Failed {
        code: error.code,
        message: error.message.clone(),
        rows,
        identity: error.identity,
    }
}

/// Go `ReorgWaitTimeout`: how long the owner waits on a reorganization
/// before recording its progress in the job.
const REORG_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Go `runReorgJob`'s `updateProgressInverval`: how long a step waits on the
/// reorganization before it returns the reorg timeout.
fn update_progress_interval() -> Duration {
    tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs")
        .and_then(|value| value.as_int())
        .map_or(REORG_WAIT_TIMEOUT, |ms| Duration::from_millis(ms as u64))
}

fn table_range(
    backfiller: &dyn IndexBackfiller,
    reorg: &AddIndexReorg,
    physical_id: i64,
    snapshot: Arc<Mutex<dyn ClusterSnapshot>>,
    buffer: &MutationBuffer,
) -> Result<Result<Option<(Vec<u8>, Vec<u8>)>, LockSqlError>, ClusterDdlError> {
    Ok(
        match backfiller.reorg_add_index(
            reorg,
            AddIndexReorgWork::TableRange { physical_id },
            snapshot,
            buffer,
        ) {
            Ok(AddIndexReorgAnswer::Range(range)) => Ok(range),
            Ok(_) => {
                return Err(ClusterDdlError::backfill_message(
                    "a table-range request answered something else".to_owned(),
                ))
            }
            Err(error) => Err(error),
        },
    )
}

/// Go `getReorgInfo` with `SnapshotVer == 0`, after
/// `checkIfTableReorgWorkCanSkip` / `checkIfTempIndexReorgWorkCanSkip`.
fn init_reorg<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    reorg: &AddIndexReorg,
) -> Result<ReorgResult, ClusterDdlError>
where
    S: StoreClient,
{
    let (first_index, element_type) = reorg_element(reorg)
        .ok_or_else(|| ClusterDdlError::backfill_message("ADD INDEX reorg names no index".to_owned()))?;
    let last_index = *reorg.index_ids.last().unwrap_or(&first_index);
    let first_physical = *reorg
        .physical_ids
        .first()
        .ok_or_else(|| ClusterDdlError::backfill_message("ADD INDEX reorg names no table".to_owned()))?;
    crate::job_ctx::attach_reorg_skip_check(&reorg.query.to_utf8_lossy_go());
    let mut initialized = None;
    // Go `getReorgInfo` / `getReorgInfoFromPartitions`: `delayForAsyncCommit`
    // before the reorganization's version, which is this transaction's
    // start timestamp.
    crate::delay_for_async_commit();
    let result = reorg_transaction(opener, timeout, &crate::job_ctx::backfill_request_source(reorg.job_id), |meta, snapshot, buffer, start_ts| {
        let handle = if reorg.merging {
            // Go `checkIfTempIndexReorgWorkCanSkip`.
            let empty = match tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/skipReorgWorkForTempIndex") {
                Some(value) => value.bool(),
                None => {
                    let mut empty = true;
                    for physical_id in &reorg.physical_ids {
                        for index_id in &reorg.index_ids {
                            let (start, end) = temp_index_range(*physical_id, *index_id);
                            if !meta.scan_page(&start, &end, 1).map_err(DdlPlanError::from)?.is_empty() {
                                empty = false;
                            }
                        }
                    }
                    empty
                }
            };
            if empty {
                return Ok((ReorgResult::Skipped, Vec::new()));
            }
            let mut end = temp_index_seek_key(first_physical, last_index);
            end.push(255);
            ReorgHandle {
                element_id: first_index,
                element_type: INDEX_ELEMENT_KEY.to_vec(),
                start_key: temp_index_seek_key(first_physical, first_index),
                end_key: end,
                physical_id: first_physical,
            }
        } else {
            let mut first_range = None;
            let mut empty = true;
            for (offset, physical_id) in reorg.physical_ids.iter().enumerate() {
                let range = match table_range(
                    backfiller,
                    reorg,
                    *physical_id,
                    Arc::clone(&snapshot),
                    buffer,
                )? {
                    Ok(range) => range,
                    Err(error) => return Ok((sql_failure(&error, reorg.row_count), Vec::new())),
                };
                empty &= range.is_none();
                if offset == 0 {
                    first_range = range;
                }
            }
            if empty {
                return Ok((ReorgResult::Skipped, Vec::new()));
            }
            let (start_key, end_key) = first_range.unwrap_or_default();
            ReorgHandle {
                element_id: first_index,
                element_type: element_type.to_vec(),
                start_key,
                end_key,
                physical_id: first_physical,
            }
        };
        initialized = Some(handle);
        Ok((ReorgResult::Initialized(start_ts), Vec::new()))
    })?;
    if let Some(handle) = initialized {
        let mut se = sess_pool
            .open_sql_session()
            .map_err(DdlPlanError::from_mysql_error)?;
        crate::ddl_reorg_table::init_ddl_reorg_handle(se.as_mut(), reorg.job_id, &handle)
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(result)
}

/// An ADD INDEX reorganization (Go `addTableIndex`), not a column change,
/// a partition reorganization or a global index cleanup.
fn is_index_reorg(reorg: &AddIndexReorg) -> bool {
    reorg.column.is_none() && !reorg.clean_up_global_index && reorg.partition_phase.is_none()
}

/// The error `addTableIndex` returns, as the job records it.
fn reorg_fn_error(error: ClusterDdlError) -> DdlPlanError {
    match error {
        ClusterDdlError::Plan(error) => error,
        // The step's own error, as Go's `isRetryableError` reads its cause.
        ClusterDdlError::Backfill(error) => error,
        // A SQL failure keeps the Go identity it was raised with.
        ClusterDdlError::Commit(error) | ClusterDdlError::IndexBackfill(error) => {
            crate::cluster_ddl::reorg_failure(error.code, &error.message, error.identity)
        }
        error => DdlPlanError::Encode(error.to_string()),
    }
}

/// Go `isReorgRunnable`: the step's context must not be cancelled and this
/// node must be the owner.
fn reorg_runnable(
    step_cancelled: &AtomicBool,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
) -> Result<bool, ClusterDdlError> {
    if step_cancelled.load(Ordering::SeqCst) {
        return Ok(false);
    }
    check_owner().map_err(ClusterDdlError::SchemaSync)?;
    Ok(true)
}

/// Go `reorgCtx`: the reorganization's `reorgFn` (a local backfill or merge,
/// or `executeDistTask`) runs on its own thread, and each job step waits on
/// its `doneCh` until the progress tick (`ErrWaitReorgTimeout`), recording
/// the rows and warnings so far.
struct ReorgCtx {
    /// Go `jobContext.stepCtx`, which outlives the steps a reorg timeout
    /// ends.
    step_cancelled: Arc<AtomicBool>,
    /// Go `reorgCtx.doneCh`.
    done: crossbeam_channel::Receiver<Result<ReorgResult, ClusterDdlError>>,
    /// Go `reorgCtx.rowCount`.
    row_count: Arc<AtomicI64>,
    /// Go `reorgCtx.mu.warnings`.
    warnings: Arc<Mutex<Vec<(u16, String, i64)>>>,
}

/// The reorganization of one owner run of a job (Go `reorgContexts`, keyed
/// by the job). When the run ends, its step context is cancelled, as Go's
/// worker context is: the reorganization then pauses or cancels its task as
/// the job's state says, or just stops.
#[derive(Default)]
pub(crate) struct ReorgSlot(Mutex<Option<ReorgCtx>>);

impl ReorgSlot {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<ReorgCtx>> {
        self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `jobContext.initStepCtx`: the running reorganization's step
    /// context, or a new one.
    fn step_ctx(&self) -> Arc<AtomicBool> {
        self.lock().as_ref().map_or_else(|| Arc::new(AtomicBool::new(false)), |ctx| Arc::clone(&ctx.step_cancelled))
    }

    /// Go `jobContext.cleanStepCtx(dbterror.ErrCancelledDDLJob)` of a job
    /// that turned to its rollback while a reorganization ran: the
    /// reorganization's step context is cancelled, and the job's next steps
    /// start their own.
    pub(crate) fn cancel_step(&self) {
        if let Some(ctx) = self.lock().take() {
            ctx.step_cancelled.store(true, Ordering::SeqCst);
        }
    }

    /// Go `runReorgJob`: `reorg_fn` starts on its own thread unless the job's
    /// reorganization already runs, and this step waits for it at most one
    /// progress interval; a step the interval ends records the rows
    /// (`rc.getRowCount`) and the warnings so far (`mergeWarningsIntoJob`).
    fn run(
        &self,
        step_cancelled: &Arc<AtomicBool>,
        row_count: i64,
        warnings: &mut Vec<(u16, String, i64)>,
        reorg_fn: impl FnOnce(Arc<AtomicI64>, Arc<Mutex<Vec<(u16, String, i64)>>>) -> Result<ReorgResult, ClusterDdlError>
            + Send
            + 'static,
    ) -> Result<ReorgResult, ClusterDdlError> {
        let (done, rows, reorg_warnings) = {
            let mut slot = self.lock();
            let ctx = slot.get_or_insert_with(|| {
                let (done_tx, done) = crossbeam_channel::bounded(1);
                let rows = Arc::new(AtomicI64::new(row_count));
                let reorg_warnings = Arc::new(Mutex::new(Vec::new()));
                let (thread_rows, thread_warnings) = (Arc::clone(&rows), Arc::clone(&reorg_warnings));
                std::thread::spawn(tidb_cpuprofile::labels::inherit(move || {
                    let _ = done_tx.send(reorg_fn(thread_rows, thread_warnings));
                }));
                ReorgCtx { step_cancelled: Arc::clone(step_cancelled), done, row_count: rows, warnings: reorg_warnings }
            });
            (ctx.done.clone(), Arc::clone(&ctx.row_count), Arc::clone(&ctx.warnings))
        };
        let take_warnings = |warnings: &mut Vec<(u16, String, i64)>| {
            let part = std::mem::take(&mut *reorg_warnings.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
            super::backfilling::merge_warnings(warnings, part);
        };
        match done.recv_timeout(update_progress_interval()) {
            Ok(result) => {
                self.lock().take();
                take_warnings(warnings);
                result
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/onRunReorgJobTimeout", &mut ());
                take_warnings(warnings);
                Ok(ReorgResult::Progress(rows.load(Ordering::SeqCst)))
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                self.lock().take();
                Err(ClusterDdlError::backfill_message("the reorganization ended without a result".to_owned()))
            }
        }
    }
}

impl Drop for ReorgSlot {
    fn drop(&mut self) {
        self.cancel_step();
    }
}

/// The interval of Go `runOneJobStep`'s job-cancelled ticker.
const CHECK_JOB_CANCELLED_INTERVAL: Duration = Duration::from_secs(2);

/// Go `runOneJobStep`'s job-cancelled checker: every two seconds it asks
/// `sysTblMgr.GetJobByID` for the job and cancels the step's context once
/// the job is cancelling, cancelled, pausing or paused; it ends with the
/// step, or when the job is gone.
fn check_job_cancelled(
    job_id: i64,
    get_job_by_id: impl Fn() -> Result<Option<tidb_model::Job>, ClusterDdlError>,
    interval: Duration,
    stopped: std::sync::mpsc::Receiver<()>,
    step_cancelled: &AtomicBool,
) {
    while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(interval) {
        match get_job_by_id() {
            Ok(Some(job)) => {
                // Go injects the step's job before it reads the latest; this
                // checker holds only the job's id, so it passes the job it
                // read.
                tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/checkJobCancelled", &mut job.clone());
                if matches!(
                    job.state,
                    JobState::CANCELLING | JobState::CANCELLED | JobState::PAUSING | JobState::PAUSED
                ) {
                    step_cancelled.store(true, Ordering::SeqCst);
                    return;
                }
            }
            Ok(None) => return,
            Err(error) => tidb_ddl_logutil::ddl_logger().error(
                "get job failed, will retry later",
                &[
                    tidb_log::Field::new("job_id", tidb_log::Value::I64(job_id)),
                    zap_error(error),
                ],
            ),
        }
    }
}

/// Go `sysTblMgr.GetJobByID` on a session of the DDL pool.
pub(crate) fn latest_job(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    job_id: i64,
) -> Result<Option<tidb_model::Job>, ClusterDdlError> {
    let pool = sess_pool.sess_pool().ok_or_else(||
        ClusterDdlError::backfill_message("the DDL session pool has no sessions".to_owned()))?;
    match tidb_ddl_systable::Manager::get_job_by_id(
        &tidb_ddl_systable::SystemTableManager::new(pool), job_id,
    ) {
        Ok(job) => Ok(job.job.map(|job| job.read().clone())),
        Err(tidb_ddl_systable::SystemTableManagerError::NotFound) => Ok(None),
        Err(error) => Err(ClusterDdlError::backfill(&error)),
    }
}

/// Go `runReorgJob` from the persisted handle: every physical table from
/// the handle's on, each in batches of `batch_size` rows (or temporary
/// entries, for each index in turn while merging).
#[allow(clippy::too_many_arguments)]
fn run_reorg<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    sess_pool_arc: &Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    reorg: &AddIndexReorg,
    handle: &ReorgHandle,
    step_cancelled_arc: &Arc<AtomicBool>,
    reorg_slot: &ReorgSlot,
    warnings: &mut Vec<(u16, String, i64)>,
) -> Result<ReorgResult, ClusterDdlError>
where
    S: StoreClient,
{
    let sess_pool = sess_pool_arc.as_ref();
    // Go `addTableIndex`: an ingest job with a distributed reorg runs both
    // its read step and its temp-index merge as a DXF task; a local ingest
    // reads through the backend and merges as the transactional path does.
    let ingest_job = if reorg.column.is_none() && !reorg.clean_up_global_index && reorg.partition_phase.is_none() {
        latest_job(sess_pool, reorg.job_id)?
            .map(|job| {
                reorg
                    .sub_job
                    .and_then(|sequence| {
                        job.multi_schema_info.as_ref().and_then(|multi| {
                            multi.read().sub_jobs.get(sequence as usize).map(|sub| sub.read().to_proxy_job(&job, sequence as i64))
                        })
                    })
                    .unwrap_or(job)
            })
            .filter(|job| job.reorg_meta.as_ref().is_some_and(|meta| meta.read().reorg_type == tidb_model::ReorgType::INGEST))
    } else {
        None
    };
    let (opener, sess_pool, reorg, step_cancelled) =
        (Arc::clone(opener), Arc::clone(sess_pool_arc), reorg.clone(), Arc::clone(step_cancelled_arc));
    if let Some(job) = ingest_job.clone().filter(|job| job.reorg_meta.as_ref().is_some_and(|m| m.read().is_dist_reorg)) {
        let row_count = reorg.row_count;
        return reorg_slot.run(step_cancelled_arc, row_count, warnings, move |_, _| {
            // Go `isReorgRunnable(stepCtx, true)`: a distributed
            // reorganization does not check the ownership.
            dist::execute_dist_task(&opener, timeout, sess_pool.as_ref(), &reorg, &job, &|| Ok(()), &step_cancelled)
        });
    }
    // Go's local `reorgFn` (the txn backfill, the local ingest, the temp
    // index merge) on the job's `reorgCtx`; `isReorgRunnable(ctx, false)`
    // checks the worker's ownership.
    let (backfiller, handle, row_count) = (backfiller.shared(), handle.clone(), reorg.row_count);
    reorg_slot.run(step_cancelled_arc, row_count, warnings, move |total, warnings| {
        let check_owner = || backfiller.check_owner();
        run_local_reorg(
            &opener,
            timeout,
            backfiller.as_ref(),
            &sess_pool,
            &reorg,
            &handle,
            ingest_job,
            &check_owner,
            &step_cancelled,
            &total,
            &warnings,
        )
    })
}

/// Go's local `reorgFn` from `handle` on: every physical table's backfill or
/// temp-index merge, `total` and `warnings` counting as it goes.
#[allow(clippy::too_many_arguments)]
fn run_local_reorg<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    sess_pool_arc: &Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    reorg: &AddIndexReorg,
    handle: &ReorgHandle,
    ingest_job: Option<tidb_model::Job>,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
    total: &AtomicI64,
    shared_warnings: &Mutex<Vec<(u16, String, i64)>>,
) -> Result<ReorgResult, ClusterDdlError>
where
    S: StoreClient,
{
    let sess_pool = sess_pool_arc.as_ref();
    let mut step_warnings = Vec::new();
    let warnings = &mut step_warnings;
    let flush_warnings = |warnings: &mut Vec<(u16, String, i64)>| {
        super::backfilling::merge_warnings(
            &mut shared_warnings.lock().unwrap_or_else(std::sync::PoisonError::into_inner),
            std::mem::take(warnings),
        );
    };
    let ingest_job = ingest_job.filter(|_| !reorg.merging);
    let element_type = reorg_element(reorg).map_or(INDEX_ELEMENT_KEY, |(_, kind)| kind);
    let first_partition = reorg
        .physical_ids
        .iter()
        .position(|id| *id == handle.physical_id)
        .unwrap_or(0);
    let first_element = reorg
        .index_ids
        .iter()
        .position(|id| *id == handle.element_id)
        .unwrap_or(0);
    let mut current = handle.clone();
    for (partition_offset, physical_id) in
        reorg.physical_ids.iter().enumerate().skip(first_partition)
    {
        let resuming = partition_offset == first_partition;
        // The work items of this physical table: the whole row range for a
        // backfill, each index's temporary range for a merge.
        let mut items: Vec<(Option<i64>, Vec<u8>, Vec<u8>)> = Vec::new();
        if reorg.merging {
            let skip = if resuming { first_element } else { 0 };
            for (element_offset, index_id) in reorg.index_ids.iter().enumerate().skip(skip) {
                let (start, end) = temp_index_range(*physical_id, *index_id);
                let start = if resuming && element_offset == first_element {
                    current.start_key.clone().max(start)
                } else {
                    start
                };
                items.push((Some(*index_id), start, end));
            }
        } else if resuming {
            items.push((None, current.start_key.clone(), current.end_key.clone()));
        } else {
            // Go `updateReorgInfo`: the next partition's range, persisted.
            let range = reorg_transaction(opener, timeout, &crate::job_ctx::backfill_request_source(reorg.job_id), |_, snapshot, buffer, _| {
                Ok((
                    table_range(backfiller, reorg, *physical_id, snapshot, buffer)?,
                    Vec::new(),
                ))
            })?;
            match range {
                Ok(Some((start, end))) => items.push((None, start, end)),
                // Go `getTableRange` of an empty table: no start, no end.
                Ok(None) => items.push((None, Vec::new(), Vec::new())),
                Err(error) => return Ok(sql_failure(&error, total.load(Ordering::SeqCst))),
            }
        }
        if !resuming {
            // Go `updateReorgInfo` after the previous partition: the next
            // partition (an empty one too) and its range, persisted, then
            // `afterUpdatePartitionReorgInfo`.
            let (start, end) = match (items.first(), items.last()) {
                (Some(first), Some(last)) => (first.1.clone(), last.2.clone()),
                _ => (Vec::new(), Vec::new()),
            };
            let next = ReorgHandle {
                element_id: items.first().and_then(|item| item.0).unwrap_or(current.element_id),
                element_type: element_type.to_vec(),
                start_key: start,
                end_key: end,
                physical_id: *physical_id,
            };
            update_reorg_meta(sess_pool, reorg, &next);
            after_update_partition_reorg_info(sess_pool, reorg)?;
        }
        for (index_id, start, end) in items {
            current = ReorgHandle {
                element_id: index_id.unwrap_or(current.element_id),
                element_type: element_type.to_vec(),
                start_key: start,
                end_key: end,
                physical_id: *physical_id,
            };
            if current.start_key >= current.end_key {
                continue;
            }
            // Go `writePhysicalTableRecord`, before it picks local ingest or
            // the txn backfill.
            if let Some(value) = tidb_failpoint::eval("github.com/pingcap/tidb/pkg/ddl/MockCaseWhenParseFailure") {
                if value.bool() {
                    let error_count = latest_job(sess_pool, reorg.job_id)?.map_or(0, |job| job.error_count);
                    return Err(ClusterDdlError::backfill_message(format!(
                        "job.ErrCount:{error_count}, mock unknown type: ast.whenClause."
                    )));
                }
            }
            let handle = current.clone();
            if let Some(job)=&ingest_job {
                let pool=backfiller.session_pool().ok_or_else(||ClusterDdlError::backfill_message("ingest requires the DDL session pool"))?;
                update_reorg_meta(sess_pool,reorg,&handle);
                let result = ingest::add_index_with_local_ingest(opener,timeout,backfiller,pool,reorg,job,&handle,check_owner,step_cancelled,warnings);
                flush_warnings(warnings);
                match result? {
                    ReorgResult::Done(rows)=>{total.fetch_add(rows,Ordering::SeqCst);continue;},
                    ReorgResult::Failed{code,message,rows,identity}=>return Ok(ReorgResult::Failed{code,message,rows:total.load(Ordering::SeqCst)+rows,identity}),
                    other=>return Ok(other),
                }
            }
            let backfill_data = |start: &[u8], end: &[u8], batch_size: usize| {
                run_batch(
                    opener,
                    timeout,
                    backfiller,
                    reorg,
                    handle.physical_id,
                    start,
                    end,
                    batch_size,
                    index_id,
                    check_owner,
                    step_cancelled,
                )
            };
            // Go `reorgInfo.UpdateConfigFromSysTbl`.
            let reload_config = || {
                latest_job(sess_pool, reorg.job_id)
                .map(|job| {
                    // Go `writePhysicalTableRecord`'s config tick.
                    if let Some(job) = &job {
                        tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/checkReorgWorkerCnt", &mut job.clone());
                    }
                    job.filter(|job| job.state == JobState::RUNNING && job.is_alterable())
                        .map(|job| {
                            (
                                crate::cluster_ddl::add_index::job_concurrency(&job),
                                crate::cluster_ddl::add_index::job_batch_size(&job),
                            )
                        })
                })
                .inspect_err(|error| {
                    tidb_ddl_logutil::ddl_logger().warn(
                        "failed to get latest job from system table",
                        &[
                            tidb_log::Field::new("jobID", tidb_log::Value::I64(reorg.job_id)),
                            tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                        ],
                    );
                })
                .ok()
                .flatten()
            };
            let range_end_key = |start: &[u8], end: &[u8]| {
                reorg_transaction(opener, timeout, &crate::job_ctx::backfill_request_source(reorg.job_id), |_, snapshot, buffer, _| {
                    let answer = backfiller.reorg_add_index(
                        reorg,
                        AddIndexReorgWork::RangeEndKey { start, end },
                        snapshot,
                        buffer,
                    );
                    buffer.reset();
                    Ok((answer, Vec::new()))
                })
                .and_then(|answer| match answer {
                    Ok(AddIndexReorgAnswer::EndKey(key)) => Ok(key),
                    Ok(_) => Err(ClusterDdlError::backfill_message(
                        "a range-end request answered something else".to_owned(),
                    )),
                    Err(error) => Err(ClusterDdlError::Backfill(crate::cluster_ddl::reorg_failure(
                    error.code,
                    &error.message,
                    error.identity,
                ))),
                })
            };
            let update_reorg_meta = |next_key: &[u8]| {
                let next = ReorgHandle {
                    start_key: next_key.to_vec(),
                    ..handle.clone()
                };
                update_reorg_meta(sess_pool, reorg, &next);
            };
            let region_store = backfiller.region_store();
            let job_query = reorg.query.to_utf8_lossy_go();
            let exec = BackfillExecutor {
                job_id: reorg.job_id,
                job_query: &job_query,
                region_store: region_store.as_deref(),
                physical_id: current.physical_id,
                concurrency: reorg.concurrency,
                batch_size: reorg.batch_size,
                reload_config: &reload_config,
                // Go `getActualEndKey`: the temp-index merge keeps the region
                // end; the row workers stop at the rows the range has.
                shrink_end_key: index_id.is_none(),
                backfill_data: &backfill_data,
                range_end_key: &range_end_key,
                update_reorg_meta: &update_reorg_meta,
            };
            let written = write_physical_table_record(&exec, &current.start_key, &current.end_key, total, warnings);
            flush_warnings(warnings);
            let (outcome, next_key) = written?;
            current.start_key = next_key;
            match outcome {
                TableRecordOutcome::Done => {}
                TableRecordOutcome::Interrupted => return Ok(ReorgResult::Interrupted),
                TableRecordOutcome::Failed(error) => return Ok(sql_failure(&error, total.load(Ordering::SeqCst))),
            }
        }
    }
    if reorg.table.get_partition_info().is_some() {
        // Go's last `updateReorgInfo` finds no next partition and still
        // reaches `afterUpdatePartitionReorgInfo`.
        after_update_partition_reorg_info(sess_pool, reorg)?;
    }
    Ok(ReorgResult::Done(total.load(Ordering::SeqCst)))
}

/// Go `failpoint.InjectCall("afterUpdatePartitionReorgInfo", reorgInfo.Job)`;
/// the job is read only while the failpoint is enabled.
fn after_update_partition_reorg_info(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    reorg: &AddIndexReorg,
) -> Result<(), ClusterDdlError> {
    const NAME: &str = "github.com/pingcap/tidb/pkg/ddl/afterUpdatePartitionReorgInfo";
    if tidb_failpoint::eval(NAME).is_some() {
        if let Some(mut job) = latest_job(sess_pool, reorg.job_id)? {
            tidb_failpoint::inject_call(NAME, &mut job);
        }
    }
    Ok(())
}

/// Go `BackfillData` for one batch of `[start, end)`, in its own
/// transaction, after Go's `isReorgRunnable`.
#[allow(clippy::too_many_arguments)]
fn run_batch<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    reorg: &AddIndexReorg,
    physical_id: i64,
    start: &[u8],
    end: &[u8],
    batch_size: usize,
    merge_index: Option<i64>,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    step_cancelled: &AtomicBool,
) -> Result<BatchStep, ClusterDdlError>
where
    S: StoreClient,
{
    let step = reorg_transaction(opener, timeout, &crate::job_ctx::backfill_request_source(reorg.job_id), |_, snapshot, buffer, _| {
        if !reorg_runnable(step_cancelled, check_owner)? {
            return Ok((BatchStep::Interrupted, Vec::new()));
        }
        let work = match merge_index {
            Some(index_id) => AddIndexReorgWork::Merge {
                index_id,
                physical_id,
                start,
                end,
                batch_size,
            },
            None if reorg.partition_phase
                == Some(crate::cluster_ddl::add_index::PartitionReorgPhase::CopyRows) =>
            {
                AddIndexReorgWork::PartitionCopy { physical_id, start, end, batch_size }
            }
            None if reorg.clean_up_global_index => {
                AddIndexReorgWork::CleanUpGlobalIndex { physical_id, start, end, batch_size }
            }
            None if reorg.column.is_some() => {
                AddIndexReorgWork::UpdateColumn { physical_id, start, end, batch_size }
            }
            None => AddIndexReorgWork::Backfill { physical_id, start, end, batch_size },
        };
        let batch = match backfiller.reorg_add_index(reorg, work, snapshot, buffer) {
            Ok(AddIndexReorgAnswer::Batch(batch)) => batch,
            Ok(_) => {
                return Err(ClusterDdlError::backfill_message(
                    "a batch request answered something else".to_owned(),
                ))
            }
            Err(error) => {
                // Nothing of a failed batch is kept (Go's txn rolls back).
                buffer.reset();
                return Ok((BatchStep::Failed(error), Vec::new()));
            }
        };
        let mut mutations = Vec::with_capacity(batch.locks.len());
        for key in &batch.locks {
            mutations.push(
                BufferMutation::lock_only(key.clone())
                    .map_err(|error| ClusterDdlError::backfill(&error))?,
            );
        }
        Ok((
            BatchStep::Advanced {
                next_key: batch.next_key,
                done: batch.done,
                added: batch.added,
                batch_warnings: batch.warnings,
            },
            mutations,
        ))
    });
    // Go's workers' `BackfillData`, after the batch's transaction.
    if merge_index.is_none() && reorg.partition_phase.is_none() && !reorg.clean_up_global_index {
        if reorg.column.is_some() {
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/mockUpdateColumnWorkerStuck", &mut ());
        } else {
            tidb_failpoint::inject_call("github.com/pingcap/tidb/pkg/ddl/mockAddIndexTxnWorkerStuck", &mut ());
        }
    }
    step
}

/// Go `reorgInfo.UpdateReorgMeta`: the handle's own pooled transaction;
/// failure is only logged.
fn update_reorg_meta(
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    reorg: &AddIndexReorg,
    next: &ReorgHandle,
) {
    let updated = sess_pool.open_sql_session()
        .and_then(|mut se| crate::ddl_reorg_table::update_reorg_meta(se.as_mut(), reorg.job_id, next));
    if let Err(error) = updated {
        tidb_ddl_logutil::ddl_logger().warn(
            "update reorg meta failed",
            &[
                tidb_log::Field::new("job ID", tidb_log::Value::I64(reorg.job_id)),
                tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
            ],
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `runOneJobStep`'s checker cancels the step's context for a job
    /// that is cancelling, cancelled, pausing or paused, keeps polling a
    /// job in any other state or one it failed to read, and ends when the
    /// job is gone or the step is; `isReorgRunnable` refuses a cancelled
    /// step before it asks for the owner.
    #[test]
    fn the_job_cancelled_checker_cancels_the_step_as_go() {
        let job_in = |state| {
            let mut job = tidb_model::Job::default();
            job.state = state;
            job
        };
        let run = |answers: Vec<Result<Option<tidb_model::Job>, ClusterDdlError>>| {
            let polls = std::cell::Cell::new(0);
            let answers = std::cell::RefCell::new(answers.into_iter());
            let step_cancelled = AtomicBool::new(false);
            let (stop, stopped) = std::sync::mpsc::channel::<()>();
            check_job_cancelled(
                7,
                || {
                    polls.set(polls.get() + 1);
                    match answers.borrow_mut().next() {
                        Some(answer) => answer,
                        None => {
                            // The step ends.
                            let _ = stop.send(());
                            Ok(Some(job_in(JobState::RUNNING)))
                        }
                    }
                },
                Duration::from_millis(1),
                stopped,
                &step_cancelled,
            );
            (step_cancelled.load(Ordering::SeqCst), polls.get())
        };
        for state in [
            JobState::CANCELLING,
            JobState::CANCELLED,
            JobState::PAUSING,
            JobState::PAUSED,
        ] {
            let answers = vec![
                Ok(Some(job_in(JobState::RUNNING))),
                Err(ClusterDdlError::backfill_message("get job failed".to_owned())),
                Ok(Some(job_in(state))),
                Ok(Some(job_in(JobState::RUNNING))),
            ];
            assert_eq!(run(answers), (true, 3), "{state:?}");
        }
        for state in [
            JobState::QUEUEING,
            JobState::RUNNING,
            JobState::ROLLINGBACK,
            JobState::ROLLBACK_DONE,
            JobState::DONE,
            JobState::SYNCED,
        ] {
            assert_eq!(run(vec![Ok(Some(job_in(state)))]), (false, 2), "{state:?}");
        }
        assert_eq!(run(vec![Ok(None), Ok(Some(job_in(JobState::CANCELLING)))]), (false, 1));

        let owner_asked = std::sync::atomic::AtomicUsize::new(0);
        let check_owner = || {
            owner_asked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        };
        let step_cancelled = AtomicBool::new(false);
        assert!(reorg_runnable(&step_cancelled, &check_owner).unwrap());
        step_cancelled.store(true, Ordering::SeqCst);
        assert!(!reorg_runnable(&step_cancelled, &check_owner).unwrap());
        assert_eq!(owner_asked.load(Ordering::SeqCst), 1);
    }

    /// Go `runReorgJob`: the reorganization starts once on its own thread;
    /// each step waits one progress interval and, while it still runs,
    /// answers its rows so far (`ErrWaitReorgTimeout`) with its warnings
    /// moved to the job; a later step answers its result.
    #[test]
    fn a_reorganization_outlives_the_steps_that_wait_for_it() {
        let _interval =
            tidb_failpoint::enable_for_test("github.com/pingcap/tidb/pkg/ddl/updateProgressIntervalInMs", "return(1000)");
        let slot = ReorgSlot::default();
        let step_cancelled = slot.step_ctx();
        let started = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let (finish, finished) = crossbeam_channel::bounded::<()>(0);
        let reorg_fn = {
            let started = Arc::clone(&started);
            move |rows: Arc<AtomicI64>, warnings: Arc<Mutex<Vec<(u16, String, i64)>>>| {
                started.fetch_add(1, Ordering::SeqCst);
                rows.fetch_add(7, Ordering::SeqCst);
                warnings.lock().unwrap().push((1366, "bad value".to_owned(), 2));
                let _ = finished.recv();
                rows.fetch_add(3, Ordering::SeqCst);
                Ok(ReorgResult::Done(rows.load(Ordering::SeqCst)))
            }
        };
        let mut warnings = Vec::new();
        let first = slot.run(&step_cancelled, 10, &mut warnings, reorg_fn.clone()).unwrap();
        assert!(matches!(first, ReorgResult::Progress(17)), "{first:?}");
        assert_eq!(warnings, [(1366, "bad value".to_owned(), 2)]);
        // The next step joins the running reorganization: no second start.
        let finish_later = finish.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            let _ = finish_later.send(());
        });
        let second = slot.run(&step_cancelled, 17, &mut warnings, reorg_fn).unwrap();
        assert!(matches!(second, ReorgResult::Done(20)), "{second:?}");
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert!(slot.lock().is_none(), "a finished reorganization leaves the slot");
    }
}
