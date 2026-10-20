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

//! Go `delRange.startEmulator` (`pkg/ddl/delete_range.go`): a store without
//! native delete-range support (the embedded store, Go's mockstore) has its
//! `mysql.gc_delete_range` rows worked off by the DDL layer itself, each
//! range's keys deleted in transactions of `delBatchSize` keys and the row
//! then moved to `mysql.gc_delete_range_done`. On TiKV the GC worker does
//! this after the safe point instead.

use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};

use tidb_txnkv::transaction::BufferMutation;

use super::add_index_reorg::reorg_transaction;
use super::*;
use crate::cluster_catalog::PagedMetaSnapshot;
use crate::ddl_delete_range::{reserve_row_id_in_transaction, DelRangeTable, StoredDelRange};

/// Go `delBatchSize`.
const DEL_BATCH_SIZE: usize = 65536;
/// Go `delBackLog`.
const DEL_BACK_LOG: usize = 128;

/// The emulator's wake-up handle (Go `delRange.emulatorCh`). Dropping it
/// stops the emulator.
pub struct DelRangeEmulator {
    wake: SyncSender<()>,
}

impl DelRangeEmulator {
    /// Go `addDelRangeJob`'s `dr.emulatorCh <- struct{}{}`. A full backlog
    /// already guarantees a pass after this job's rows were written.
    pub fn notify(&self) {
        match self.wake.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) | Err(TrySendError::Disconnected(())) => {}
        }
    }
}

/// Go `delRange.start` for a store without delete-range support.
pub fn start_del_range_emulator<S>(
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    row_ids: Option<Arc<dyn crate::table_from_meta::TableAutoIds>>,
) -> Result<DelRangeEmulator, String>
where
    S: StoreClient,
{
    let (wake, woken) = sync_channel(DEL_BACK_LOG);
    std::thread::Builder::new()
        .name("ddl-del-range-emulator".to_owned())
        .spawn(move || run_emulator(&opener, timeout, row_ids.as_ref(), &woken))
        .map_err(|error| error.to_string())?;
    Ok(DelRangeEmulator { wake })
}

fn run_emulator<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    row_ids: Option<&Arc<dyn crate::table_from_meta::TableAutoIds>>,
    woken: &Receiver<()>,
) where
    S: StoreClient,
{
    while woken.recv().is_ok() {
        // Go `terror.Log(errors.Trace(err))`.
        if let Err(error) = do_del_range_work(opener, timeout, row_ids) {
            tidb_log::error(
                "encountered error",
                &[
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    tidb_log::Field::new(
                        "stack",
                        tidb_log::Value::Str(
                            std::backtrace::Backtrace::force_capture().to_string(),
                        ),
                    ),
                ],
            );
        }
    }
}

struct DelRangeTables {
    ranges: DelRangeTable,
    done: DelRangeTable,
}

/// Go `delRange.doDelRangeWork`: every recorded range, worked off in turn.
pub fn do_del_range_work<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    row_ids: Option<&Arc<dyn crate::table_from_meta::TableAutoIds>>,
) -> Result<(), ClusterDdlError>
where
    S: StoreClient,
{
    let (tables, rows) = reorg_transaction(opener, timeout, |meta, _, _, _| {
        let catalog = load_cluster_catalog(meta).map_err(DdlPlanError::from)?;
        let tables = DelRangeTables {
            ranges: DelRangeTable::locate(&catalog, "gc_delete_range")
                .map_err(ClusterDdlError::Backfill)?,
            done: DelRangeTable::locate(&catalog, "gc_delete_range_done")
                .map_err(ClusterDdlError::Backfill)?,
        };
        // Go `LoadDeleteRanges(ctx, sctx, math.MaxInt64)`.
        let rows = tables
            .ranges
            .load(meta, i64::MAX as u64)
            .map_err(ClusterDdlError::Backfill)?;
        Ok(((tables, rows), Vec::new()))
    })?;
    for row in &rows {
        do_task(opener, timeout, row_ids, &tables, row)?;
    }
    Ok(())
}

/// Go `delRange.doTask`.
fn do_task<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    row_ids: Option<&Arc<dyn crate::table_from_meta::TableAutoIds>>,
    tables: &DelRangeTables,
    row: &StoredDelRange,
) -> Result<(), ClusterDdlError>
where
    S: StoreClient,
{
    let task = &row.task;
    let mut old_start_key = task.start_key.clone();
    loop {
        // One transaction deletes up to `delBatchSize` keys.
        let next_start_key = reorg_transaction(opener, timeout, |meta, _, _, _| {
            let pairs = meta
                .scan_page(&old_start_key, &task.end_key, DEL_BATCH_SIZE)
                .map_err(DdlPlanError::from)?;
            let Some((last, _)) = pairs.last() else {
                return Ok((None, Vec::new()));
            };
            let mut next = last.to_vec();
            next.push(0);
            let mut mutations = Vec::with_capacity(pairs.len());
            for (key, _) in &pairs {
                mutations.push(
                    BufferMutation::delete(key.to_vec())
                        .map_err(|error| ClusterDdlError::Backfill(error.to_string()))?,
                );
            }
            Ok((Some(next), mutations))
        })?;
        let Some(next_start_key) = next_start_key else {
            complete_delete_range(opener, timeout, row_ids, tables, task)?;
            return Ok(());
        };
        // Go `UpdateDeleteRange`: `UPDATE ... SET start_key = new WHERE
        // job_id, element_id AND start_key = old`; a failure is only logged.
        let update = reorg_transaction(opener, timeout, |meta, _, _, _| {
            let rows = tables
                .ranges
                .load(meta, u64::MAX)
                .map_err(ClusterDdlError::Backfill)?;
            let mut mutations = Vec::new();
            if let Some(stored) = rows.iter().find(|stored| {
                stored.task.job_id == task.job_id
                    && stored.task.element_id == task.element_id
                    && stored.task.start_key == old_start_key
            }) {
                mutations.push(
                    tables
                        .ranges
                        .update_start_key_mutation(stored, &next_start_key)
                        .map_err(ClusterDdlError::Backfill)?,
                );
            }
            Ok(((), mutations))
        });
        if let Err(error) = update {
            tidb_ddl_logutil::ddl_logger().error(
                "delRange emulator update task failed",
                &[tidb_log::Field::new(
                    "error",
                    tidb_log::Value::Str(error.to_string()),
                )],
            );
        }
        old_start_key = next_start_key;
    }
}

/// Go `util.CompleteDeleteRange(sctx, dr, true)`: in one transaction, the
/// row copied into `gc_delete_range_done` (`INSERT IGNORE ... SELECT`) and
/// removed from `gc_delete_range`.
fn complete_delete_range<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    row_ids: Option<&Arc<dyn crate::table_from_meta::TableAutoIds>>,
    tables: &DelRangeTables,
    task: &crate::ddl_delete_range::DelRangeTask,
) -> Result<(), ClusterDdlError>
where
    S: StoreClient,
{
    reorg_transaction(opener, timeout, |meta, _, _, _| {
        let rows = tables
            .ranges
            .load(meta, u64::MAX)
            .map_err(ClusterDdlError::Backfill)?;
        let mut mutations = Vec::new();
        for stored in rows.iter().filter(|stored| {
            stored.task.job_id == task.job_id && stored.task.element_id == task.element_id
        }) {
            let done = tables
                .done
                .load(meta, u64::MAX)
                .map_err(ClusterDdlError::Backfill)?;
            let row_id = match row_ids {
                Some(ids) => {
                    let id = ids
                        .row_allocator_for(tables.done.db_id(), tables.done.table())
                        .alloc_next()
                        .map_err(|error| {
                            ClusterDdlError::Backfill(format!(
                                "mysql.gc_delete_range_done's row-ID allocator: {error}"
                            ))
                        })?;
                    i64::try_from(id).map_err(|_| {
                        ClusterDdlError::Backfill(
                            "mysql.gc_delete_range_done's row-ID allocator overflowed".to_owned(),
                        )
                    })?
                }
                None => reserve_row_id_in_transaction(
                    meta,
                    tables.done.db_id(),
                    tables.done.table(),
                    1,
                    &mut mutations,
                )?,
            };
            mutations.extend(
                tables
                    .done
                    .insert_ignore_mutations(&done, &stored.task, stored.ts, row_id)
                    .map_err(ClusterDdlError::Backfill)?,
            );
            mutations.extend(
                tables
                    .ranges
                    .delete_mutations(stored)
                    .map_err(ClusterDdlError::Backfill)?,
            );
        }
        Ok(((), mutations))
    })
}
