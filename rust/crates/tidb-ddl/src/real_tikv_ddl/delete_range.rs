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
use tidb_infoschema::cluster_catalog::PagedMetaSnapshot;

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
    sess_pool: Arc<dyn tidb_ddl_build::session_pool::DdlSessionPool>,
) -> Result<DelRangeEmulator, String>
where
    S: StoreClient,
{
    let (wake, woken) = sync_channel(DEL_BACK_LOG);
    std::thread::Builder::new()
        .name("ddl-del-range-emulator".to_owned())
        .spawn(move || run_emulator(&opener, timeout, sess_pool.as_ref(), &woken))
        .map_err(|error| error.to_string())?;
    Ok(DelRangeEmulator { wake })
}

fn run_emulator<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
    woken: &Receiver<()>,
) where
    S: StoreClient,
{
    while woken.recv().is_ok() {
        // Go `terror.Log(errors.Trace(err))`.
        if let Err(error) = do_del_range_work(opener, timeout, sess_pool) {
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

fn log_error(message: &str, error: &dyn std::fmt::Display) {
    tidb_ddl_logutil::ddl_logger().error(
        message,
        &[tidb_log::Field::new(
            "error",
            tidb_log::Value::Str(error.to_string()),
        )],
    );
}

/// Go `delRange.doDelRangeWork`: every recorded range, worked off in turn,
/// on one session of the pool.
pub fn do_del_range_work<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    sess_pool: &dyn tidb_ddl_build::session_pool::DdlSessionPool,
) -> Result<(), ClusterDdlError>
where
    S: StoreClient,
{
    let mut sctx = sess_pool.open_sql_session().map_err(|error| {
        log_error("delRange emulator get session failed", &error);
        DdlPlanError::from_mysql_error(error)
    })?;
    let ranges = crate::ddl_delete_range::load_delete_ranges(sctx.as_mut(), i64::MAX as u64)
        .map_err(|error| {
            log_error("delRange emulator load tasks failed", &error);
            DdlPlanError::from_mysql_error(error)
        })?;
    for r in &ranges {
        if let Err(error) = do_task(opener, timeout, sctx.as_mut(), r) {
            log_error("delRange emulator do task failed", &error);
            return Err(error);
        }
    }
    Ok(())
}

/// Go `delRange.doTask`.
fn do_task<S>(
    opener: &Arc<RealOptimisticTransactionOpener<S>>,
    timeout: Duration,
    sctx: &mut dyn tidb_ddl_build::session_pool::StepSqlSession,
    task: &crate::ddl_delete_range::DelRangeTask,
) -> Result<(), ClusterDdlError>
where
    S: StoreClient,
{
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
            if let Err(error) = crate::ddl_delete_range::complete_delete_range(sctx, task, true) {
                log_error("delRange emulator complete task failed", &error);
                return Err(DdlPlanError::from_mysql_error(error).into());
            }
            return Ok(());
        };
        if let Err(error) = crate::ddl_delete_range::update_delete_range(
            sctx,
            task,
            &next_start_key,
            &old_start_key,
        ) {
            log_error("delRange emulator update task failed", &error);
        }
        old_start_key = next_start_key;
    }
}
