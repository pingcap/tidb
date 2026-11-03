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

//! Go `statsLockImpl`: the lock-state policy ([`crate::lock_stats`]) over
//! Go's statements, run by a statistics session in the transaction
//! `util.CallWithSCtx(pool, f, util.FlagWrapTxn)` opens.

use std::collections::BTreeSet;

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;
use tidb_sqlexec::SqlExecError;
use tidb_stats_handle_util::{exec_rows, get_start_ts, StatsSessionContext};

use crate::cluster_stats_lock::{apply_stats_lock_request, StatsLockRequest};
use crate::lock_stats::StatsLockTransaction;

/// Go `selectSQL` (`query_lock.go`).
const SELECT_SQL: &str = "SELECT table_id FROM mysql.stats_table_locked";
/// Go `insertSQL` (`lock_stats.go`).
const INSERT_SQL: &str =
    "INSERT INTO mysql.stats_table_locked (table_id) VALUES (%?) ON DUPLICATE KEY UPDATE table_id = %?";
/// Go `updateMetaVersionSQL`.
const UPDATE_META_VERSION_SQL: &str =
    "UPDATE mysql.stats_meta SET version = %? WHERE table_id = %?";
/// Go `selectDeltaSQL` (`unlock_stats.go`).
const SELECT_DELTA_SQL: &str =
    "SELECT count, modify_count FROM mysql.stats_table_locked WHERE table_id = %?";
/// Go `updateDeltaSQL`.
const UPDATE_DELTA_SQL: &str = "UPDATE mysql.stats_meta SET version = %?, count = IF(count + %? > 0, count + %?, 0), modify_count = modify_count + %? WHERE table_id = %?";
/// Go `DeleteLockSQL`.
const DELETE_LOCK_SQL: &str = "DELETE FROM mysql.stats_table_locked WHERE table_id = %?";

/// The lock policy's statements on one statistics session.
pub struct SessionStatsLockTransaction<'a, C: StatsSessionContext + ?Sized> {
    sctx: &'a C,
}

/// Go `row.GetInt64(i)`.
fn get_int64(datum: Option<&Datum>) -> i64 {
    match datum {
        Some(Datum::Int(value)) => *value,
        Some(Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

impl<C: StatsSessionContext + ?Sized> StatsLockTransaction for SessionStatsLockTransaction<'_, C> {
    type Error = SqlExecError;

    /// Go `QueryLockedTables`.
    fn query_locked_tables(&mut self) -> Result<BTreeSet<i64>, Self::Error> {
        let (rows, _) = exec_rows(self.sctx, SELECT_SQL, &[])?;
        Ok(rows.iter().map(|row| get_int64(row.first())).collect())
    }

    /// Go `insertIntoStatsTableLockedAndUpdateStatsVersion`.
    fn insert_lock_and_update_meta_version(&mut self, table_id: i64) -> Result<(), Self::Error> {
        exec_rows(
            self.sctx,
            INSERT_SQL,
            &[SqlArg::Signed(table_id), SqlArg::Signed(table_id)],
        )?;
        let version = get_start_ts(self.sctx)?;
        exec_rows(
            self.sctx,
            UPDATE_META_VERSION_SQL,
            &[SqlArg::Unsigned(version), SqlArg::Signed(table_id)],
        )?;
        Ok(())
    }

    /// Go `getStatsDeltaFromTableLocked`.
    fn lock_delta(&mut self, table_id: i64) -> Result<(i64, i64), Self::Error> {
        let (rows, _) = exec_rows(self.sctx, SELECT_DELTA_SQL, &[SqlArg::Signed(table_id)])?;
        Ok(rows.first().map_or((0, 0), |row| {
            (get_int64(row.first()), get_int64(row.get(1)))
        }))
    }

    /// Go `updateDelta`.
    fn update_meta_delta(
        &mut self,
        table_id: i64,
        count_delta: i64,
        modify_count_delta: i64,
    ) -> Result<(), Self::Error> {
        let version = get_start_ts(self.sctx)?;
        exec_rows(
            self.sctx,
            UPDATE_DELTA_SQL,
            &[
                SqlArg::Unsigned(version),
                SqlArg::Signed(count_delta),
                SqlArg::Signed(count_delta),
                SqlArg::Signed(modify_count_delta),
                SqlArg::Signed(table_id),
            ],
        )?;
        Ok(())
    }

    fn delete_lock(&mut self, table_id: i64) -> Result<(), Self::Error> {
        exec_rows(self.sctx, DELETE_LOCK_SQL, &[SqlArg::Signed(table_id)])?;
        Ok(())
    }
}

/// Go `statsLockImpl.LockTables` / `LockPartitions` / `RemoveLockedTables`
/// / `RemoveLockedPartitions`: the request in one `FlagWrapTxn` transaction
/// of a session from `pool`. Answers Go's skipped-target message.
///
/// # Errors
///
/// A statement's or the transaction's error.
pub fn execute_stats_lock<C>(
    pool: &dyn tidb_syssession::Pool<C>,
    request: &StatsLockRequest,
) -> Result<String, SqlExecError>
where
    C: StatsSessionContext + ?Sized + 'static,
{
    let mut skipped = String::new();
    tidb_stats_handle_util::call_with_sctx(
        pool,
        |sctx| {
            skipped = apply_stats_lock_request(&mut SessionStatsLockTransaction { sctx }, request)?;
            Ok(())
        },
        &[tidb_stats_handle_util::FLAG_WRAP_TXN],
    )?;
    Ok(skipped)
}
