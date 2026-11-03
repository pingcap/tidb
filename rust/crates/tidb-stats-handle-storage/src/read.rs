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

//! Go `pkg/statistics/handle/storage/read.go`'s statements.

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, StatsSessionContext};

/// Go `StatsMetaCountAndModifyCount` (`for_update`: `...ForUpdate`): the
/// table's `count` and `modify_count`, and whether it has no row.
pub fn stats_meta_count_and_modify_count<C: StatsSessionContext + ?Sized>(
    sctx: &C,
    table_id: i64,
    for_update: bool,
) -> Result<(i64, i64, bool), SqlExecError> {
    let sql = if for_update {
        "select count, modify_count from mysql.stats_meta where table_id = %? for update"
    } else {
        "select count, modify_count from mysql.stats_meta where table_id = %?"
    };
    let (rows, _) = exec_rows(sctx, sql, &[SqlArg::Signed(table_id)])?;
    let Some(row) = rows.first() else {
        return Ok((0, 0, true));
    };
    Ok((datum_i64(row.first()), datum_i64(row.get(1)), false))
}

/// Go `row.GetInt64(i)`.
fn datum_i64(datum: Option<&Datum>) -> i64 {
    match datum {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}
