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

//! Go `pkg/statistics/handle/storage/update.go`.

use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, get_start_ts, StatsSessionContext};

/// Go `UpdateStatsMetaVerAndLastHistUpdateVer`.
pub fn update_stats_meta_ver_and_last_hist_update_ver<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    physical_id: i64,
) -> Result<u64, SqlExecError> {
    let start_ts = get_start_ts(sctx)?;
    exec_rows(
        sctx,
        "update mysql.stats_meta set version=%?, last_stats_histograms_version=%? where table_id =%?",
        &[
            SqlArg::Unsigned(start_ts),
            SqlArg::Unsigned(start_ts),
            SqlArg::Signed(physical_id),
        ],
    )?;
    Ok(start_ts)
}
