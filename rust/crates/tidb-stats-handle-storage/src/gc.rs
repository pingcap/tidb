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

//! Go `pkg/statistics/handle/storage/gc.go`'s statements.

use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, get_start_ts, StatsSessionContext};

/// Go `lockstats.DeleteLockSQL`.
const DELETE_LOCK_SQL: &str = "DELETE FROM mysql.stats_table_locked WHERE table_id = %?";

/// Go `DeleteTableStatsFromKV`: each table's statistics removed and its
/// `stats_meta` re-versioned so other nodes notice. A soft delete (`DROP
/// STATS`) resets the histogram rows instead of deleting them.
pub fn delete_table_stats_from_kv<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    stats_ids: &[i64],
    soft: bool,
) -> Result<(), SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    for &stats_id in stats_ids {
        let id = [SqlArg::Signed(stats_id)];
        exec_rows(
            sctx,
            "update mysql.stats_meta set version = %?, last_stats_histograms_version = %? where table_id = %? ",
            &[
                SqlArg::Unsigned(start_ts),
                SqlArg::Unsigned(start_ts),
                SqlArg::Signed(stats_id),
            ],
        )?;
        if soft {
            exec_rows(
                sctx,
                "update mysql.stats_histograms set distinct_count = 0, null_count = 0, tot_col_size = 0, modify_count = 0, version = %?,cm_sketch = null, stats_ver = 0, flag = 0, correlation = 0, last_analyze_pos = null where table_id = %?",
                &[SqlArg::Unsigned(start_ts), SqlArg::Signed(stats_id)],
            )?;
        } else {
            exec_rows(sctx, "delete from mysql.stats_histograms where table_id = %?", &id)?;
        }
        for sql in [
            "delete from mysql.stats_buckets where table_id = %?",
            "delete from mysql.stats_top_n where table_id = %?",
            "delete from mysql.stats_fm_sketch where table_id = %?",
            "delete from mysql.column_stats_usage where table_id = %?",
            "delete from mysql.analyze_options where table_id = %?",
            DELETE_LOCK_SQL,
        ] {
            exec_rows(sctx, sql, &id)?;
        }
    }
    Ok(())
}
