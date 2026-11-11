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

use tidb_model::JobState;
use tidb_txnkv::transaction::BufferMutation;

use super::*;
use tidb_infoschema::cluster_catalog::PagedMetaSnapshot;
use crate::cluster_ddl::add_index::{
    AddIndexReorg, ReorgHandle, ReorgOutcome, ReorgResult, ReorgStageRequest, COLUMN_ELEMENT_KEY,
    INDEX_ELEMENT_KEY, TEMP_INDEX_PREFIX,
};
use crate::ddl_reorg_table::DdlReorgTable;
use super::backfilling::{write_physical_table_record, BackfillExecutor, BatchStep, TableRecordOutcome};

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
) -> Result<ReorgOutcome, ClusterDdlError>
where
    S: StoreClient,
{
    let tables = locate_tables(opener, timeout)?;
    // Go `w.sessPool`: the reorg handle's statements run on its sessions; a
    // pool without sessions yet stages the rows in the reorg transactions.
    let sess_pool = backfiller
        .session_pool()
        .filter(|sess_pool| sess_pool.has_step_sessions());
    let mut warnings = Vec::new();
    let result = match &reorg.stage {
        ReorgStageRequest::Init => init_reorg(
            opener,
            timeout,
            backfiller,
            sess_pool.as_deref(),
            reorg,
            &tables,
        )?,
        ReorgStageRequest::Run(handle) => run_reorg(
            opener,
            timeout,
            backfiller,
            sess_pool.as_deref(),
            reorg,
            &tables,
            handle,
            check_owner,
            &mut warnings,
        )?,
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
                Ok(_) => Err(ClusterDdlError::Backfill(
                    "a column check answered something else".to_owned(),
                )),
                // Go: a failure to run the check is retried, not the job's
                // error.
                Err(error) => Err(ClusterDdlError::Backfill(error.message)),
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
pub(super) fn clean_ddl_reorg_handles<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    sess_pool: Option<&dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    job_id: i64,
) -> Result<(), ClusterDdlError>
where
    S: StoreClient,
{
    if let Some(sess_pool) = sess_pool.filter(|sess_pool| sess_pool.has_step_sessions()) {
        let mut se = sess_pool
            .open_sql_session()
            .map_err(DdlPlanError::from_mysql_error)?;
        crate::ddl_reorg_table::clean_ddl_reorg_handles(se.as_mut(), job_id)
            .map_err(DdlPlanError::from_mysql_error)?;
        return Ok(());
    }
    let tables = locate_tables(opener, timeout)?;
    reorg_transaction(opener, timeout, &crate::job_ctx::internal_ddl_source(), |meta, _, _, _| {
        let existing = tables
            .reorg
            .load(meta, job_id)
            .map_err(ClusterDdlError::Backfill)?;
        let mutations = tables
            .reorg
            .remove_ddl_reorg_handle(&existing)
            .map_err(ClusterDdlError::Backfill)?;
        Ok(((), mutations))
    })
}

struct ReorgTables {
    reorg: DdlReorgTable,
    jobs: DdlJobTable,
    system_db_id: i64,
}

fn locate_tables<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
) -> Result<ReorgTables, ClusterDdlError>
where
    S: StoreClient,
{
    reorg_transaction(opener, timeout, &crate::job_ctx::internal_ddl_source(), |meta, _, _, _| {
        let catalog = load_info_schema(meta).map_err(DdlPlanError::from)?;
        let reorg = DdlReorgTable::locate(&catalog).map_err(ClusterDdlError::Backfill)?;
        let jobs = DdlJobTable::locate(&catalog)
            .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?;
        let system_db_id = catalog
            .schema_by_name("mysql")
            .map(|database| database.read().id)
            .ok_or_else(|| ClusterDdlError::Backfill("database mysql does not exist".to_owned()))?;
        Ok((
            ReorgTables {
                reorg,
                jobs,
                system_db_id,
            },
            Vec::new(),
        ))
    })
}

/// One reorganization transaction: `work` reads through the snapshot,
/// stages into the buffer and names extra mutations; the whole is committed
/// at once and retried on a write conflict.
pub(super) fn reorg_transaction<S, T>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    source: &tidb_txnkv::RequestSource,
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
        let transaction = SessionTransaction::begin(
            Arc::clone(opener),
            timeout,
            tidb_store_driver_txn::session_commit_protocol::SessionTransactionOptions::from(
                tidb_store_driver_txn::session_commit_protocol::bootstrap_commit_protocol(),
            )
            .with_request_source(source.clone()),
        )?;
        let start_ts = transaction.start_ts();
        let buffer = MutationBuffer::new();
        let staged = (|| {
            let slot = Arc::new(Mutex::new(SwappableSnapshot::new()));
            slot.lock()
                .map_err(|_| ClusterDdlError::Backfill("snapshot slot poisoned".to_owned()))?
                .bind(
                    transaction
                        .snapshot()
                        .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
                );
            let handle: Arc<Mutex<dyn ClusterSnapshot>> = slot;
            let mut meta = SnapshotMetaSnapshot::new(
                transaction
                    .snapshot()
                    .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
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
    }
}

/// Go `ReorgWaitTimeout`: how long the owner waits on a reorganization
/// before recording its progress in the job.
const REORG_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

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
                return Err(ClusterDdlError::Backfill(
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
    sess_pool: Option<&dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    reorg: &AddIndexReorg,
    tables: &ReorgTables,
) -> Result<ReorgResult, ClusterDdlError>
where
    S: StoreClient,
{
    let (first_index, element_type) = reorg_element(reorg)
        .ok_or_else(|| ClusterDdlError::Backfill("ADD INDEX reorg names no index".to_owned()))?;
    let last_index = *reorg.index_ids.last().unwrap_or(&first_index);
    let first_physical = *reorg
        .physical_ids
        .first()
        .ok_or_else(|| ClusterDdlError::Backfill("ADD INDEX reorg names no table".to_owned()))?;
    let row_ids = backfiller.system_row_ids();
    crate::job_ctx::attach_reorg_skip_check(&reorg.query.to_utf8_lossy_go());
    let mut initialized = None;
    let result = reorg_transaction(opener, timeout, &crate::job_ctx::backfill_request_source(reorg.job_id), |meta, snapshot, buffer, start_ts| {
        let handle = if reorg.merging {
            let mut empty = true;
            for physical_id in &reorg.physical_ids {
                for index_id in &reorg.index_ids {
                    let (start, end) = temp_index_range(*physical_id, *index_id);
                    if !meta
                        .scan_page(&start, &end, 1)
                        .map_err(DdlPlanError::from)?
                        .is_empty()
                    {
                        empty = false;
                    }
                }
            }
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
        if sess_pool.is_some() {
            initialized = Some(handle);
            return Ok((ReorgResult::Initialized(start_ts), Vec::new()));
        }
        let existing = tables
            .reorg
            .load(meta, reorg.job_id)
            .map_err(ClusterDdlError::Backfill)?;
        let id = row_ids
            .row_allocator_for(tables.system_db_id, tables.reorg.table())
            .alloc_next()
            .map_err(|error| {
                ClusterDdlError::Backfill(format!(
                    "mysql.tidb_ddl_reorg's row-ID allocator: {error}"
                ))
            })?;
        let row_id = i64::try_from(id).map_err(|_| {
            ClusterDdlError::Backfill(
                "mysql.tidb_ddl_reorg's row-ID allocator overflowed".to_owned(),
            )
        })?;
        let mutations = tables
            .reorg
            .init_ddl_reorg_handle(&existing, reorg.job_id, &handle, row_id)
            .map_err(ClusterDdlError::Backfill)?;
        Ok((ReorgResult::Initialized(start_ts), mutations))
    })?;
    if let (Some(sess_pool), Some(handle)) = (sess_pool, initialized) {
        let mut se = sess_pool
            .open_sql_session()
            .map_err(DdlPlanError::from_mysql_error)?;
        crate::ddl_reorg_table::init_ddl_reorg_handle(se.as_mut(), reorg.job_id, &handle)
            .map_err(DdlPlanError::from_mysql_error)?;
    }
    Ok(result)
}

/// Go `isReorgRunnable`: the job must still be running (not cancelling,
/// pausing or paused) and this node its owner.
fn reorg_runnable(
    meta: &mut SnapshotMetaSnapshot,
    tables: &ReorgTables,
    job_id: i64,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
) -> Result<bool, ClusterDdlError> {
    check_owner().map_err(ClusterDdlError::SchemaSync)?;
    let job = tables
        .jobs
        .load_by_id(meta, job_id)
        .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?;
    Ok(match job {
        Some(active) => {
            let job = &active.job;
            !(job.is_cancelling()
                || job.is_pausing()
                || job.is_paused()
                || job.state == JobState::CANCELLED)
        }
        None => false,
    })
}

/// Go `sysTblMgr.GetJobByID`: the job as `mysql.tidb_ddl_job` has it now,
/// selected on a session of the DDL's pool; a pool no node has bound reads
/// the row off a snapshot.
fn latest_job<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    sess_pool: Option<&dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    tables: &ReorgTables,
    job_id: i64,
) -> Result<Option<tidb_model::Job>, ClusterDdlError>
where
    S: StoreClient,
{
    match sess_pool.and_then(|sess_pool| sess_pool.sess_pool()) {
        Some(pool) => match tidb_ddl_systable::Manager::get_job_by_id(
            &tidb_ddl_systable::SystemTableManager::new(pool),
            job_id,
        ) {
            Ok(job) => Ok(job.job.map(|job| job.read().clone())),
            Err(tidb_ddl_systable::SystemTableManagerError::NotFound) => Ok(None),
            Err(error) => Err(ClusterDdlError::Backfill(error.to_string())),
        },
        None => reorg_transaction(
            opener,
            timeout,
            &crate::job_ctx::internal_ddl_source(),
            |meta, _, _, _| {
                let job = tables
                    .jobs
                    .load_by_id(meta, job_id)
                    .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?;
                Ok((job.map(|active| active.job), Vec::new()))
            },
        ),
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
    sess_pool: Option<&dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    reorg: &AddIndexReorg,
    tables: &ReorgTables,
    handle: &ReorgHandle,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
    warnings: &mut Vec<(u16, String, i64)>,
) -> Result<ReorgResult, ClusterDdlError>
where
    S: StoreClient,
{
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
    let started = std::time::Instant::now();
    let mut total: i64 = reorg.row_count;
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
                Ok(None) => {}
                Err(error) => return Ok(sql_failure(&error, total)),
            }
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
            let handle = current.clone();
            let backfill_data = |start: &[u8], end: &[u8], batch_size: usize| {
                run_batch(
                    opener,
                    timeout,
                    backfiller,
                    reorg,
                    tables,
                    handle.physical_id,
                    start,
                    end,
                    batch_size,
                    index_id,
                    check_owner,
                )
            };
            // Go `reorgInfo.UpdateConfigFromSysTbl`.
            let reload_config = || {
                latest_job(opener, timeout, sess_pool, tables, reorg.job_id)
                .map(|job| {
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
                    Ok(_) => Err(ClusterDdlError::Backfill(
                        "a range-end request answered something else".to_owned(),
                    )),
                    Err(error) => Err(ClusterDdlError::Backfill(error.message)),
                })
            };
            let update_reorg_meta = |next_key: &[u8]| {
                let next = ReorgHandle {
                    start_key: next_key.to_vec(),
                    ..handle.clone()
                };
                update_reorg_meta(opener, timeout, sess_pool, reorg, tables, &next);
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
                deadline: started + REORG_WAIT_TIMEOUT,
            };
            let (outcome, next_key) = write_physical_table_record(
                &exec,
                &current.start_key,
                &current.end_key,
                &mut total,
                warnings,
            )?;
            current.start_key = next_key;
            match outcome {
                TableRecordOutcome::Done => {}
                TableRecordOutcome::Progress => return Ok(ReorgResult::Progress(total)),
                TableRecordOutcome::Interrupted => return Ok(ReorgResult::Interrupted),
                TableRecordOutcome::Failed(error) => return Ok(sql_failure(&error, total)),
            }
        }
    }
    Ok(ReorgResult::Done(total))
}

/// Go `BackfillData` for one batch of `[start, end)`, in its own
/// transaction, after Go's `isReorgRunnable`.
#[allow(clippy::too_many_arguments)]
fn run_batch<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    backfiller: &dyn IndexBackfiller,
    reorg: &AddIndexReorg,
    tables: &ReorgTables,
    physical_id: i64,
    start: &[u8],
    end: &[u8],
    batch_size: usize,
    merge_index: Option<i64>,
    check_owner: &(dyn Fn() -> Result<(), String> + Sync),
) -> Result<BatchStep, ClusterDdlError>
where
    S: StoreClient,
{
    reorg_transaction(opener, timeout, &crate::job_ctx::backfill_request_source(reorg.job_id), |meta, snapshot, buffer, _| {
        if !reorg_runnable(meta, tables, reorg.job_id, check_owner)? {
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
                return Err(ClusterDdlError::Backfill(
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
                    .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
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
    })
}

/// Go `reorgInfo.UpdateReorgMeta(nextKey, sessPool)`: the handle in a
/// transaction of its own (on the worker's sessions, or staged on the reorg
/// table by a worker whose pool has no sessions yet); a failure is only
/// logged.
fn update_reorg_meta<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    sess_pool: Option<&dyn tidb_ddl_build::session_pool::DdlSessionPool>,
    reorg: &AddIndexReorg,
    tables: &ReorgTables,
    next: &ReorgHandle,
) where
    S: StoreClient,
{
    let updated = match sess_pool {
        Some(sess_pool) => sess_pool
            .open_sql_session()
            .and_then(|mut se| crate::ddl_reorg_table::update_reorg_meta(se.as_mut(), reorg.job_id, next))
            .map_err(|error| error.to_string()),
        None => reorg_transaction(opener, timeout, &crate::job_ctx::internal_ddl_source(), |meta, _, _, _| {
            let existing = tables.reorg.load(meta, reorg.job_id).map_err(ClusterDdlError::Backfill)?;
            let mut mutations = Vec::new();
            if let Some(row) = existing.first() {
                if row.handle != *next {
                    mutations.extend(
                        tables
                            .reorg
                            .update_ddl_reorg_handle(row, next)
                            .map_err(ClusterDdlError::Backfill)?,
                    );
                }
            }
            Ok(((), mutations))
        })
        .map_err(|error| error.to_string()),
    };
    if let Err(error) = updated {
        tidb_ddl_logutil::ddl_logger().warn(
            "update reorg meta failed",
            &[
                tidb_log::Field::new("job ID", tidb_log::Value::I64(reorg.job_id)),
                tidb_log::Field::new("error", tidb_log::Value::Str(error)),
            ],
        );
    }
}
