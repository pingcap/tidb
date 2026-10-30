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

//! Go `pkg/bindinfo/binding_operator.go`: the global-binding writes
//! (`CreateBinding`, `DropBinding`, `SetBindingStatus`, `GCBinding`) on the
//! operator's internal session. The caller's writer runs them in one
//! transaction that already holds `mysql.bind_info`'s lock row (Go
//! `callWithSCtx` with `lockBindInfoTable`), and reloads the cache after it.

use tidb_datatype::Datum;
use tidb_driver_error::DriverError;
use tidb_hack::GoToLower;

use crate::binding::{
    Binding, BIND_INFO_COLUMNS, STATUS_DELETED, STATUS_DISABLED, STATUS_ENABLED, STATUS_USING,
};

/// Go `pkg/bindinfo.Lease`.
pub const LEASE: std::time::Duration = std::time::Duration::from_secs(3);

/// What the operator does on its session: Go `exec(sctx, sql, args...)`
/// with the statement's affected rows and warnings (Go reads them through
/// `StmtCtx.AffectedRows()` and `execRows(sctx, "show warnings")`), and
/// Go `time.Now()`.
pub trait BindingOperatorSession {
    /// One statement with `?` parameters: its affected-row count and the
    /// messages of the warnings it raised.
    ///
    /// # Errors
    ///
    /// The statement's error.
    fn exec(&mut self, sql: &str, params: &[Datum]) -> Result<(u64, Vec<String>), DriverError>;

    /// Go `types.NewTime(types.FromGoTime(time.Now().Add(offset)),
    /// mysql.TypeTimestamp, 6).String()`, `offset` in microseconds.
    fn now_timestamp(&self, offset_micros: i64) -> String;
}

/// Go's plain `errors.New`, which a client sees as 1105.
fn plain_error(message: &str) -> DriverError {
    DriverError::Mysql(tidb_terror::MysqlError::unknown(message))
}

/// Go `bindingOperator.CreateBinding`'s storage half (`prepareHints` and the
/// cache reload stay with the caller): each binding's older rows for the
/// same statement become `deleted`, then the binding is inserted, and a
/// warning the insert raised fails the operation.
///
/// # Errors
///
/// A statement's error, or the insert's first warning.
pub fn create_binding<S: BindingOperatorSession + ?Sized>(
    session: &mut S,
    bindings: &[Binding],
) -> Result<(), DriverError> {
    for binding in bindings {
        let update_ts = session.now_timestamp(0);
        session.exec(
            "UPDATE mysql.bind_info SET status = ?, update_time = ? WHERE original_sql = ? AND update_time < ?",
            &[
                Datum::new_string(STATUS_DELETED.to_owned()),
                Datum::new_string(update_ts.clone()),
                Datum::new_string(binding.original_sql.clone()),
                Datum::new_string(update_ts.clone()),
            ],
        )?;
        // Go stores NULL for an empty digest.
        let digest = |digest: &str| {
            if digest.is_empty() {
                Datum::Null
            } else {
                Datum::new_string(digest.to_owned())
            }
        };
        let (_, warnings) = session.exec(
            &format!(
                "INSERT INTO mysql.bind_info({BIND_INFO_COLUMNS}, plan_digest) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            ),
            &[
                Datum::new_string(binding.original_sql.clone()),
                Datum::new_string(binding.bind_sql.clone()),
                Datum::new_string(binding.db.go_to_lower()),
                Datum::new_string(binding.status.to_owned()),
                Datum::new_string(update_ts.clone()),
                Datum::new_string(update_ts),
                Datum::new_string(binding.charset.clone()),
                Datum::new_string(binding.collation.clone()),
                Datum::new_string(binding.source.to_owned()),
                digest(&binding.sql_digest),
                digest(&binding.plan_digest),
            ],
        )?;
        if let Some(warning) = warnings.first() {
            return Err(plain_error(warning));
        }
    }
    Ok(())
}

/// Go `bindingOperator.DropBinding`: each digest's live rows become
/// `deleted`, stamped one microsecond apart after now so that every
/// `update_time < ts` comparison sees a strictly later time.
///
/// # Errors
///
/// `sql digest is empty`, or a statement's error.
pub fn drop_binding<S: BindingOperatorSession + ?Sized>(
    session: &mut S,
    sql_digests: &[String],
) -> Result<u64, DriverError> {
    if sql_digests.is_empty() {
        return Err(plain_error("sql digest is empty"));
    }
    let mut deleted_rows = 0;
    for (i, sql_digest) in sql_digests.iter().enumerate() {
        let update_ts = session.now_timestamp(1 + i as i64);
        let (affected, _) = session.exec(
            "UPDATE mysql.bind_info SET status = ?, update_time = ? WHERE sql_digest = ? AND update_time < ? AND status != ?",
            &[
                Datum::new_string(STATUS_DELETED.to_owned()),
                Datum::new_string(update_ts.clone()),
                Datum::new_string(sql_digest.clone()),
                Datum::new_string(update_ts),
                Datum::new_string(STATUS_DELETED.to_owned()),
            ],
        )?;
        deleted_rows += affected;
    }
    Ok(deleted_rows)
}

/// Go `bindingOperator.SetBindingStatus`: whether a binding of `sql_digest`
/// moved to `new_status` (disabling covers both `enabled` and `using`).
///
/// # Errors
///
/// The statement's error.
pub fn set_binding_status<S: BindingOperatorSession + ?Sized>(
    session: &mut S,
    new_status: &str,
    sql_digest: &str,
) -> Result<bool, DriverError> {
    let (old_status0, old_status1) = if new_status == STATUS_DISABLED {
        (STATUS_USING, STATUS_ENABLED)
    } else if new_status == STATUS_ENABLED {
        (STATUS_DISABLED, STATUS_DISABLED)
    } else {
        ("", "")
    };
    let update_ts = session.now_timestamp(0);
    let (affected, _) = session.exec(
        "UPDATE mysql.bind_info SET status = ?, update_time = ? WHERE sql_digest = ? AND update_time < ? AND status IN (?, ?)",
        &[
            Datum::new_string(new_status.to_owned()),
            Datum::new_string(update_ts.clone()),
            Datum::new_string(sql_digest.to_owned()),
            Datum::new_string(update_ts),
            Datum::new_string(old_status0.to_owned()),
            Datum::new_string(old_status1.to_owned()),
        ],
    )?;
    Ok(affected > 0)
}

/// Go `bindingOperator.GCBinding`: `deleted` rows older than ten leases,
/// which every node has observed, are physically removed.
///
/// # Errors
///
/// The statement's error.
pub fn gc_binding<S: BindingOperatorSession + ?Sized>(session: &mut S) -> Result<(), DriverError> {
    let ten_leases = i64::try_from((10 * LEASE).as_micros()).unwrap_or(i64::MAX);
    let update_time = session.now_timestamp(-ten_leases);
    session.exec(
        "DELETE FROM mysql.bind_info WHERE status = 'deleted' and update_time < ?",
        &[Datum::new_string(update_time)],
    )?;
    Ok(())
}
