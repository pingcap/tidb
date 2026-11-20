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

//! Go master `pkg/util/mviewutil/refresh_source.go`: a materialized view's
//! registered input logs in `mysql.tidb_mview_fast_refresh_source_info`,
//! read and written through the caller's SQL executor (and so in the
//! caller's transaction). `ResolveRefreshSources`, which reads an info
//! schema, lives with its session (`tidb-session`'s `mview_refresh`).

use tidb_datatype::Datum;
use tidb_model::MViewRefreshSource;
use tidb_sqlescape::SqlArg;

/// Go `RefreshSourceInfoTableName`.
pub const REFRESH_SOURCE_INFO_TABLE_NAME: &str = tidb_metadef::system_tables_def::TIDB_MVIEW_FAST_REFRESH_SOURCE_INFO_TABLE_NAME;

/// Go `sqlexec.ExecSQL` on the caller's `sqlexec.SQLExecutor`: `sql` with
/// its `%?` arguments, answering its rows.
pub type ExecSql<'e, E> = dyn FnMut(&str, &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, E> + 'e;

/// Go `ReadRefreshSourceIDs`: the view's registered log IDs, ascending.
///
/// # Errors
///
/// The executor's.
pub fn read_refresh_source_ids<E>(exec: &mut ExecSql<'_, E>, mview_id: i64) -> Result<Vec<i64>, E> {
    let rows = exec(
        &format!("SELECT MLOG_ID FROM mysql.{REFRESH_SOURCE_INFO_TABLE_NAME} WHERE MVIEW_ID = %? ORDER BY MLOG_ID"),
        &[SqlArg::Signed(mview_id)],
    )?;
    Ok(rows
        .iter()
        .filter_map(|row| match row.first() {
            Some(Datum::Int(id)) => Some(*id),
            Some(Datum::UInt(id)) => i64::try_from(*id).ok(),
            _ => None,
        })
        .collect())
}

/// Go `InsertRefreshSourceIDs`: registers a whole log set in the caller's
/// transaction. "Duplicate records are errors; ignoring them could combine
/// different baselines."
///
/// # Errors
///
/// The executor's.
pub fn insert_refresh_source_ids<E>(exec: &mut ExecSql<'_, E>, mview_id: i64, mlog_ids: &[i64]) -> Result<(), E> {
    if mlog_ids.is_empty() {
        return Ok(());
    }
    let arguments: Vec<SqlArg<'_>> =
        mlog_ids.iter().flat_map(|&mlog_id| [SqlArg::Signed(mview_id), SqlArg::Signed(mlog_id)]).collect();
    // "Only placeholder counts are dynamic; values are escaped by
    // ExecuteInternal."
    exec(
        &format!(
            "INSERT INTO mysql.{REFRESH_SOURCE_INFO_TABLE_NAME} (MVIEW_ID, MLOG_ID) VALUES {}(%?, %?)",
            "(%?, %?),".repeat(mlog_ids.len() - 1)
        ),
        &arguments,
    )
    .map(|_| ())
}

/// Go `DeleteRefreshSourceInfo`: removes complete log sets in the caller's
/// transaction, a thousand views per statement.
///
/// # Errors
///
/// The executor's.
pub fn delete_refresh_source_info<E>(exec: &mut ExecSql<'_, E>, mview_ids: &[i64]) -> Result<(), E> {
    const BATCH_SIZE: usize = 1000;
    for batch in mview_ids.chunks(BATCH_SIZE) {
        let arguments: Vec<SqlArg<'_>> = batch.iter().map(|&id| SqlArg::Signed(id)).collect();
        exec(
            &format!(
                "DELETE FROM mysql.{REFRESH_SOURCE_INFO_TABLE_NAME} WHERE MVIEW_ID IN ({}%?)",
                "%?,".repeat(batch.len() - 1)
            ),
            &arguments,
        )?;
    }
    Ok(())
}

/// Go `RefreshSourceLogIDs`: the sources' log IDs, sorted and deduplicated.
#[must_use]
pub fn refresh_source_log_ids(sources: &[MViewRefreshSource]) -> Vec<i64> {
    let mut ids: Vec<i64> = sources.iter().map(|source| source.mlog_id).collect();
    ids.sort_unstable();
    ids.dedup();
    ids
}

/// Go `ReplaceRefreshSourceIDs`: atomically replaces a baseline in the
/// caller's transaction.
///
/// # Errors
///
/// The executor's.
pub fn replace_refresh_source_ids<E>(exec: &mut ExecSql<'_, E>, mview_id: i64, mlog_ids: &[i64]) -> Result<(), E> {
    delete_refresh_source_info(exec, &[mview_id])?;
    insert_refresh_source_ids(exec, mview_id, mlog_ids)
}
