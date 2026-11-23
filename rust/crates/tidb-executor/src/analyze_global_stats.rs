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

//! Go `pkg/executor/analyze_global_stats.go`: after the partitions are
//! saved, ANALYZE merges the global statistics of each dynamically pruned
//! partitioned table it analyzed.

use std::collections::BTreeMap;
use std::time::Duration;

use chrono::TimeZone;
use tidb_datatype::SessionTimeZone;
use tidb_model::table_info::TableInfo;
use tidb_sqlkiller::SqlKiller;
use tidb_stats_handle_globalstats::global_stats::{
    merge_partition_stats_2_global_stats_by_table_id, write_global_stats_to_storage, GlobalStats,
    GlobalStatsError, GlobalStatsInfo, MergeSession,
};
use tidb_stats_handle_globalstats::GlobalStatsMergeError;
use tidb_stats_handle_storage::stats_read_writer::StatsSessionPool;
use tidb_store_driver_txn::transaction::{RealOptimisticTransactionOpener, StoreClient};

use crate::analyze::AnalyzeStatement;
use crate::real_tikv_analyze::{
    find_statement_table, AnalyzeJobKind, AnalyzeJobLifecycle, AnalyzeJobSpec, ClusterAnalyzeError,
    ResolvedClusterAnalyze,
};
use tidb_infoschema::cluster_catalog::load_info_schema;
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;

/// Go `(*AnalyzeExec).handleGlobalStats`: one merge job per key of the
/// `globalStatsMap`, keyed `(tableID, indexID)` with `-1` for the columns. A
/// failed merge fails only its job; a missing partition's statistics are a
/// warning.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_global_stats<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    stats_pool: &dyn StatsSessionPool,
    jobs: &dyn AnalyzeJobLifecycle,
    resolved: &[ResolvedClusterAnalyze],
    global_stats_map: &BTreeMap<(i64, i64), GlobalStatsInfo>,
    warnings: &mut Vec<(u16, String)>,
    timeout: Duration,
    stats_lease: Duration,
    killer: &SqlKiller,
) {
    for (&(table_id, index_id), info) in global_stats_map {
        let Some(table) = resolved.iter().find(|resolved| resolved.table.id == table_id) else {
            tidb_logutil::bg_logger().warn(
                "cannot find the partitioned table, skip merging global stats",
                &[tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id))],
            );
            continue;
        };
        let job = new_analyze_handle_global_stats_job(table, index_id);
        let id = jobs.insert(&job);
        if let Some(id) = id {
            jobs.start(id);
        }
        let merged = merge_and_write(opener, stats_pool, table, info, timeout, stats_lease, killer);
        if let Err(error) = &merged {
            tidb_logutil::bg_logger().warn(
                "merge global stats failed",
                &[
                    tidb_log::Field::new("info", tidb_log::Value::Str(job.job_info.clone())),
                    tidb_log::Field::new("error", tidb_log::Value::Str(error.to_string())),
                    tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id)),
                ],
            );
            // Go `statsGlobalImpl.MergePartitionStats2GlobalStatsByTableID`:
            // missing partition-level stats are reported as a warning.
            if let ClusterAnalyzeError::Mysql(error) = error {
                if error.equal(&tidb_datatype::types_errors::ERR_PARTITION_STATS_MISSING)
                    || error.equal(&tidb_datatype::types_errors::ERR_PARTITION_COLUMN_STATS_MISSING)
                {
                    warnings.push((error.code, error.message.clone()));
                }
            }
        }
        if let Some(id) = id {
            let failure = merged.as_ref().err().map(ToString::to_string);
            jobs.finish(id, 0, failure.as_deref(), AnalyzeJobKind::GlobalStatsMerge);
        }
    }
}

/// Go `newAnalyzeHandleGlobalStatsJob`.
fn new_analyze_handle_global_stats_job(resolved: &ResolvedClusterAnalyze, index_id: i64) -> AnalyzeJobSpec {
    resolved.global_job_spec((index_id != -1).then_some(index_id))
}

/// Go `MergePartitionStats2GlobalStatsByTableID` then
/// `WriteGlobalStatsToStorage`, over the table read from the catalog.
fn merge_and_write<S: StoreClient>(
    opener: &RealOptimisticTransactionOpener<S>,
    stats_pool: &dyn StatsSessionPool,
    resolved: &ResolvedClusterAnalyze,
    info: &GlobalStatsInfo,
    timeout: Duration,
    stats_lease: Duration,
    killer: &SqlKiller,
) -> Result<(), ClusterAnalyzeError> {
    let mut transaction = opener
        .begin_read_only()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let table = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_info_schema(&mut snapshot).map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        let table = find_statement_table(&catalog, &resolved.statement)?.read().clone();
        table
    };
    transaction
        .finish_without_writes()
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let statement = &resolved.statement;
    let global_stats = match &statement.eval_context.session_zone() {
        SessionTimeZone::Local => merge(Some(&chrono::Local), stats_pool, &table, info, statement, killer),
        SessionTimeZone::Named(zone) => merge(Some(zone), stats_pool, &table, info, statement, killer),
        SessionTimeZone::Fixed { offset_secs, .. } => {
            let zone = chrono::FixedOffset::east_opt(*offset_secs)
                .ok_or_else(|| ClusterAnalyzeError::Other("invalid session timezone offset".to_owned()))?;
            merge(Some(&zone), stats_pool, &table, info, statement, killer)
        }
    }?;
    write_global_stats_to_storage(stats_pool, stats_lease, &global_stats, info, table.id).map_err(ClusterAnalyzeError::Sql)
}

/// The merge under the statement's time zone, with what Go reads from the
/// session and the table's options (`OptionsMap[tableID].FilledOpts`).
fn merge<TZ: TimeZone + Sync>(
    timezone: Option<&TZ>,
    stats_pool: &dyn StatsSessionPool,
    table: &TableInfo,
    info: &GlobalStatsInfo,
    statement: &AnalyzeStatement,
    killer: &SqlKiller,
) -> Result<GlobalStats, ClusterAnalyzeError> {
    let session = MergeSession {
        skip_missing_partition_stats: statement.skip_missing_partition_stats,
        timezone,
        killer,
        num_topn: statement.options.num_topn as u32,
        num_buckets: statement.options.num_buckets as usize,
        partition_merge_concurrency: statement.partition_merge_concurrency,
    };
    merge_partition_stats_2_global_stats_by_table_id(&session, stats_pool, table, info.is_index, &info.hist_ids).map_err(
        |error| match error {
            GlobalStatsError::Exec(error) => ClusterAnalyzeError::StatsRead(error),
            GlobalStatsError::Sql(error) => ClusterAnalyzeError::Sql(error),
            GlobalStatsError::Mysql(error) => ClusterAnalyzeError::Mysql(error),
            GlobalStatsError::Merge(GlobalStatsMergeError::Killed(error)) => ClusterAnalyzeError::Killed(error),
            GlobalStatsError::Merge(error) => ClusterAnalyzeError::Other(error.to_string()),
            GlobalStatsError::Other(detail) => ClusterAnalyzeError::Other(detail),
        },
    )
}
