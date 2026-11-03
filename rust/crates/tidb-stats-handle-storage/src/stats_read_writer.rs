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

//! Go `pkg/statistics/handle/storage/stats_read_writer.go`: the statistics
//! handle's saves, each Go's `storage` statements on a session of its pool
//! inside one pessimistic transaction (`util.CallWithSCtx(SPool(), ...,
//! util.FlagWrapTxn)`).

use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{StatsSessionContext, FLAG_WRAP_TXN};
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use crate::cluster_stats_load::ClusterStatsItem;
use crate::save::{self, AnalyzeResults};
use crate::update;

/// Go `statsHandler.SPool()` with `util.CallWithSCtx`: `f` runs on one of
/// the pool's statistics sessions, and a statement's error keeps its MySQL
/// identity.
pub trait StatsSessionPool: Send + Sync {
    /// Go `util.CallWithSCtx(pool, f, flags...)`.
    fn call_with_sctx(
        &self,
        f: &mut dyn FnMut(&dyn StatsSessionContext) -> Result<(), SqlExecError>,
        flags: &[i32],
    ) -> Result<(), SqlQueryError>;
}

/// One `util.CallWithSCtx(..., util.FlagWrapTxn)` returning `f`'s version.
fn wrap_txn(
    pool: &dyn StatsSessionPool,
    mut f: impl FnMut(&dyn StatsSessionContext) -> Result<u64, SqlExecError>,
) -> Result<u64, SqlQueryError> {
    let mut stats_ver = 0;
    pool.call_with_sctx(
        &mut |sctx| {
            stats_ver = f(sctx)?;
            Ok(())
        },
        &[FLAG_WRAP_TXN],
    )?;
    Ok(stats_ver)
}

/// Go `statsReadWriter.SaveAnalyzeResultToStorage`'s save; returns
/// `statsVer`.
pub fn save_analyze_result_to_storage(
    pool: &dyn StatsSessionPool,
    results: &AnalyzeResults<'_>,
    analyze_snapshot: bool,
) -> Result<u64, SqlQueryError> {
    wrap_txn(pool, |sctx| {
        save::save_analyze_result_to_storage(sctx, results, analyze_snapshot)
    })
}

/// Go `statsReadWriter.SaveColOrIdxStatsToStorage`'s save; returns
/// `statsVer`.
pub fn save_col_or_idx_stats_to_storage(
    pool: &dyn StatsSessionPool,
    table_id: i64,
    count: i64,
    modify_count: i64,
    item: &ClusterStatsItem,
    stats_version: i64,
    update_analyze_time: bool,
) -> Result<u64, SqlQueryError> {
    wrap_txn(pool, |sctx| {
        save::save_col_or_idx_stats_to_storage(
            sctx,
            table_id,
            count,
            modify_count,
            item,
            stats_version,
            update_analyze_time,
        )
    })
}

/// Go `statsReadWriter.handleSlowStatsSaving`'s version bump
/// (`UpdateStatsMetaVerAndLastHistUpdateVer`).
pub fn update_stats_meta_ver_and_last_hist_update_ver(
    pool: &dyn StatsSessionPool,
    physical_id: i64,
) -> Result<u64, SqlQueryError> {
    wrap_txn(pool, |sctx| {
        update::update_stats_meta_ver_and_last_hist_update_ver(sctx, physical_id)
    })
}
