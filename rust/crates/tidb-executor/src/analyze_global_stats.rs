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
use tidb_planner::plan_builder::analyze::{Analyze, AnalyzeOptionType, AnalyzeOpts};

use crate::analyze::AnalyzeStatement;
use crate::real_tikv_analyze::{AnalyzeJobKind, AnalyzeJobLifecycle, AnalyzeJobSpec, ClusterAnalyzeError};

/// Go `(*AnalyzeExec).handleGlobalStats`: one merge job per key of the
/// `globalStatsMap`, keyed `(tableID, indexID)` with `-1` for the columns. A
/// failed merge fails only its job; a missing partition's statistics are a
/// warning.
#[allow(clippy::too_many_arguments)]
pub(crate) fn handle_global_stats(
    stats_pool: &dyn StatsSessionPool,
    jobs: &dyn AnalyzeJobLifecycle,
    plan: &Analyze,
    vars: &AnalyzeStatement,
    global_stats_map: &BTreeMap<(i64, i64), GlobalStatsInfo>,
    warnings: &mut Vec<(u16, String)>,
    stats_lease: Duration,
    killer: &SqlKiller,
) {
    for (&(table_id, index_id), info) in global_stats_map {
        let Some((tbl_info, analyze_info)) = plan
            .col_tasks
            .iter()
            .map(|task| (&task.tbl_info, &task.analyze_info))
            .chain(plan.idx_tasks.iter().map(|task| (&task.tbl_info, &task.analyze_info)))
            .find(|(tbl_info, _)| tbl_info.read().id == table_id)
        else {
            tidb_logutil::bg_logger().warn(
                "cannot find the partitioned table, skip merging global stats",
                &[tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id))],
            );
            continue;
        };
        let table = tbl_info.read().clone();
        let job = new_analyze_handle_global_stats_job(&table, &analyze_info.db_name, index_id);
        let id = jobs.insert(&job);
        if let Some(id) = id {
            jobs.start(id);
        }
        let global_opts = plan
            .options_map
            .get(&table_id)
            .map_or(&plan.opts, |options| &options.filled_opts);
        let merged = merge_and_write(stats_pool, &table, info, global_opts, vars, stats_lease, killer);
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
fn new_analyze_handle_global_stats_job(table: &TableInfo, db_name: &str, index_id: i64) -> AnalyzeJobSpec {
    let table_name = table.name.original();
    let job_info = if index_id == -1 {
        format!("merge global stats for {db_name}.{table_name} columns")
    } else {
        let idx_name = table
            .indices
            .iter_deref()
            .find_map(|index| {
                let index = index.read();
                (index.id == index_id).then(|| index.name.lowercase().to_owned())
            })
            .unwrap_or_default();
        format!("merge global stats for {db_name}.{table_name}'s index {idx_name}")
    };
    AnalyzeJobSpec {
        schema: db_name.to_owned(),
        table: table_name.to_owned(),
        partition: String::new(),
        job_info,
    }
}

/// Go `MergePartitionStats2GlobalStatsByTableID` then
/// `WriteGlobalStatsToStorage`.
fn merge_and_write(
    stats_pool: &dyn StatsSessionPool,
    table: &TableInfo,
    info: &GlobalStatsInfo,
    opts: &AnalyzeOpts,
    vars: &AnalyzeStatement,
    stats_lease: Duration,
    killer: &SqlKiller,
) -> Result<(), ClusterAnalyzeError> {
    let global_stats = match &vars.eval_context.session_zone() {
        SessionTimeZone::Local => merge(Some(&chrono::Local), stats_pool, table, info, opts, vars, killer),
        SessionTimeZone::Named(zone) => merge(Some(zone), stats_pool, table, info, opts, vars, killer),
        SessionTimeZone::Fixed { offset_secs, .. } => {
            let zone = chrono::FixedOffset::east_opt(*offset_secs)
                .ok_or_else(|| ClusterAnalyzeError::Other("invalid session timezone offset".to_owned()))?;
            merge(Some(&zone), stats_pool, table, info, opts, vars, killer)
        }
    }?;
    write_global_stats_to_storage(stats_pool, stats_lease, &global_stats, info, table.id).map_err(ClusterAnalyzeError::Sql)
}

/// The merge under the statement's time zone, with what Go reads from the
/// session and the table's filled options.
fn merge<TZ: TimeZone + Sync>(
    timezone: Option<&TZ>,
    stats_pool: &dyn StatsSessionPool,
    table: &TableInfo,
    info: &GlobalStatsInfo,
    opts: &AnalyzeOpts,
    vars: &AnalyzeStatement,
    killer: &SqlKiller,
) -> Result<GlobalStats, ClusterAnalyzeError> {
    let session = MergeSession {
        skip_missing_partition_stats: vars.skip_missing_partition_stats,
        timezone,
        killer,
        num_topn: opts.get(&AnalyzeOptionType::NumTopN).copied().unwrap_or_default() as u32,
        num_buckets: opts.get(&AnalyzeOptionType::NumBuckets).copied().unwrap_or_default() as usize,
        partition_merge_concurrency: vars.partition_merge_concurrency,
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
