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

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tidb_datatype::{parse_time, Time, TimeType, MAX_FSP};
use tidb_log::{Field, Value};
use tidb_model::TableItemID;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_usage_predicatecolumn::cluster_predicate_column::ColStatsTimeInfo;
use tidb_stats_handle_usage_predicatecolumn::predicate_column::save_column_stats_usage_for_table;
use tidb_stats_handle_util::{StatsSessionContext, FLAG_WRAP_TXN};
use tidb_stats_util::JsonPredicateColumn;
use tidb_store_driver_error::sql_query_error::SqlQueryError;

use crate::cluster_stats_load::ClusterStatsItem;
use crate::gc;
use crate::save::{self, AnalyzeResults, MetaUpdate};
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

/// Go `statsReadWriter.SaveAnalyzeResultToStorage` without its history
/// record: the save and, when it versioned the table, its
/// `handleSlowStatsSaving`. Returns the final `statsVer` (0: not versioned).
pub fn save_analyze_result_to_storage(
    pool: &dyn StatsSessionPool,
    lease: Duration,
    results: &AnalyzeResults<'_>,
    analyze_snapshot: bool,
) -> Result<u64, SqlQueryError> {
    let start = Instant::now();
    let stats_ver = wrap_txn(pool, |sctx| {
        save::save_analyze_result_to_storage(sctx, results, analyze_snapshot)
    })?;
    after_save(pool, lease, results.stats.table_id, start, stats_ver)
}

/// The `statsVer != 0` tail of Go's save wrappers: `handleSlowStatsSaving`'s
/// version, when it bumped one, replaces the save's.
fn after_save(
    pool: &dyn StatsSessionPool,
    lease: Duration,
    table_id: i64,
    start: Instant,
    stats_ver: u64,
) -> Result<u64, SqlQueryError> {
    if stats_ver == 0 {
        return Ok(0);
    }
    let version = handle_slow_stats_saving(pool, lease, table_id, start)?;
    Ok(if version != 0 { version } else { stats_ver })
}

/// Go `statsReadWriter.SaveColOrIdxStatsToStorage` without its history
/// record; returns the final `statsVer`.
pub fn save_col_or_idx_stats_to_storage(
    pool: &dyn StatsSessionPool,
    lease: Duration,
    table_id: i64,
    count: i64,
    modify_count: i64,
    item: &ClusterStatsItem,
    stats_version: i64,
    update_analyze_time: bool,
) -> Result<u64, SqlQueryError> {
    let start = Instant::now();
    let stats_ver = wrap_txn(pool, |sctx| {
        save::save_col_or_idx_stats_to_storage(
            sctx,
            table_id,
            count,
            modify_count,
            item,
            stats_version,
            update_analyze_time,
        )
    })?;
    after_save(pool, lease, table_id, start, stats_ver)
}

/// Go `handleSlowStatsSaving`'s version bump
/// (`UpdateStatsMetaVerAndLastHistUpdateVer` in its own transaction).
fn update_stats_meta_ver_and_last_hist_update_ver(
    pool: &dyn StatsSessionPool,
    physical_id: i64,
) -> Result<u64, SqlQueryError> {
    wrap_txn(pool, |sctx| {
        update::update_stats_meta_ver_and_last_hist_update_ver(sctx, physical_id)
    })
}

/// Go `cache.LeaseOffset`.
const LEASE_OFFSET: u32 = 5;

/// Go `handleSlowStatsSaving`'s `isLoadIntervalExceeded`: a save that took
/// five statistics leases or more (a disabled lease never does).
#[must_use]
pub fn is_load_interval_exceeded(lease: Duration, elapsed: Duration) -> bool {
    lease > Duration::ZERO && elapsed >= lease.saturating_mul(LEASE_OFFSET)
}

/// Go `statsReadWriter.handleSlowStatsSaving`: a slow save re-versions the
/// table's `stats_meta` so other nodes do not miss it; returns that
/// version, or 0 when the save was not slow.
pub fn handle_slow_stats_saving(
    pool: &dyn StatsSessionPool,
    lease: Duration,
    table_id: i64,
    start: Instant,
) -> Result<u64, SqlQueryError> {
    let elapsed = start.elapsed();
    if !is_load_interval_exceeded(lease, elapsed) {
        return Ok(0);
    }
    let logger = tidb_stats_handle_logutil::stats_logger();
    logger.warn(
        "Update stats cache is too slow",
        &[
            Field::new(
                "duration",
                Value::Duration(i64::try_from(elapsed.as_nanos()).unwrap_or(i64::MAX)),
            ),
            Field::new("physicalID", Value::I64(table_id)),
        ],
    );
    match update_stats_meta_ver_and_last_hist_update_ver(pool, table_id) {
        Ok(stats_ver) => {
            logger.info(
                "Successfully updated stats meta version for slow saving",
                &[
                    Field::new("statsVer", Value::U64(stats_ver)),
                    Field::new("physicalID", Value::I64(table_id)),
                ],
            );
            Ok(stats_ver)
        }
        Err(error) => {
            logger.error(
                "Failed to update stats meta version for slow saving, the stats cache on other TiDB nodes may be inconsistent",
                &[
                    Field::new("physicalID", Value::I64(table_id)),
                    tidb_logutil::zap_error(error.message),
                ],
            );
            Err(SqlQueryError::unknown(
                "failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later",
            ))
        }
    }
}

/// Go `statsGCImpl.DeleteTableStatsFromKV`.
pub fn delete_table_stats_from_kv(
    pool: &dyn StatsSessionPool,
    stats_ids: &[i64],
    soft: bool,
) -> Result<(), SqlQueryError> {
    wrap_txn(pool, |sctx| {
        gc::delete_table_stats_from_kv(sctx, stats_ids, soft)?;
        Ok(0)
    })
    .map(|_| ())
}

/// Go `statsReadWriter.SaveMetaToStorage`'s save; returns `statsVer`.
pub fn save_meta_to_storage(
    pool: &dyn StatsSessionPool,
    refresh_last_hist_ver: bool,
    meta_updates: &[MetaUpdate],
) -> Result<u64, SqlQueryError> {
    wrap_txn(pool, |sctx| {
        save::save_meta_to_storage(sctx, refresh_last_hist_ver, meta_updates)
    })
}

/// Go `statsReadWriter.SaveColumnStatsUsageToStorage`.
pub fn save_column_stats_usage_to_storage(
    pool: &dyn StatsSessionPool,
    physical_id: i64,
    predicate_columns: &[Option<JsonPredicateColumn>],
) -> Result<(), SqlQueryError> {
    wrap_txn(pool, |sctx| {
        let mut col_stats_usage = HashMap::with_capacity(predicate_columns.len());
        for column in predicate_columns.iter().flatten() {
            col_stats_usage.insert(
                TableItemID {
                    table_id: physical_id,
                    id: column.id,
                    is_index: false,
                    is_sync_load_failed: false,
                },
                ColStatsTimeInfo {
                    last_used_at: parse_time_or_nil(column.last_used_at.as_deref())?,
                    last_analyzed_at: parse_time_or_nil(column.last_analyzed_at.as_deref())?,
                },
            );
        }
        save_column_stats_usage_for_table(sctx, &col_stats_usage)?;
        Ok(0)
    })
    .map(|_| ())
}

/// Go `parseTimeOrNil`: a UTC timestamp at the maximum precision.
fn parse_time_or_nil(value: Option<&str>) -> Result<Option<Time>, SqlExecError> {
    let Some(value) = value else {
        return Ok(None);
    };
    parse_time(value, TimeType::Timestamp, MAX_FSP, false, false, false, &chrono::Utc)
        .map(|parsed| Some(parsed.time))
        .map_err(|error| Box::new(error) as SqlExecError)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use super::{handle_slow_stats_saving, is_load_interval_exceeded, StatsSessionPool};
    use tidb_sqlexec::SqlExecError;
    use tidb_stats_handle_util::StatsSessionContext;
    use tidb_store_driver_error::sql_query_error::SqlQueryError;

    /// A pool whose every call fails, counting the calls.
    struct FailingPool(AtomicUsize);

    impl StatsSessionPool for FailingPool {
        fn call_with_sctx(
            &self,
            _: &mut dyn FnMut(&dyn StatsSessionContext) -> Result<(), SqlExecError>,
            _: &[i32],
        ) -> Result<(), SqlQueryError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(SqlQueryError::unknown("injected refresh failure"))
        }
    }

    #[test]
    fn a_fast_save_keeps_its_version_and_a_failed_refresh_is_go_error() {
        let pool = FailingPool(AtomicUsize::new(0));
        let lease = Duration::from_secs(3600);
        assert_eq!(handle_slow_stats_saving(&pool, lease, 7, Instant::now()).unwrap(), 0);
        assert_eq!(pool.0.load(Ordering::SeqCst), 0);
        let slow_start = Instant::now() - Duration::from_millis(10);
        let error = handle_slow_stats_saving(&pool, Duration::from_millis(1), 7, slow_start)
            .expect_err("a failed refresh fails the save");
        assert_eq!(pool.0.load(Ordering::SeqCst), 1);
        assert_eq!(
            error.message,
            "failed to update stats meta version during analyze result save. The system may be too busy. Please retry the operation later"
        );
    }

    #[test]
    fn a_save_is_slow_from_five_leases_and_never_with_the_lease_disabled() {
        let lease = Duration::from_secs(3);
        assert!(!is_load_interval_exceeded(lease, Duration::from_secs(14)));
        assert!(is_load_interval_exceeded(lease, Duration::from_secs(15)));
        assert!(!is_load_interval_exceeded(Duration::ZERO, Duration::from_secs(3600)));
    }
}
