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

use tidb_sqlescape::SqlArg;

use crate::binding_utils::{exec, exec_rows, InternalSqlRunner};
use tidb_driver_error::DriverError;
use tidb_hack::GoToLower;

use crate::binding::{
    Binding, BIND_INFO_COLUMNS, STATUS_DELETED, STATUS_DISABLED, STATUS_ENABLED, STATUS_USING,
};

/// Go `pkg/bindinfo.Lease`.
pub const LEASE: std::time::Duration = std::time::Duration::from_secs(3);

/// The operator's pooled session (Go `binding_operator.go`'s `sctx`): its
/// SQL executors and affected rows ([`InternalSqlRunner`]), which Go's
/// `exec` and `execRows` run on, and Go `time.Now()`.
pub trait BindingOperatorSession: InternalSqlRunner {
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
    session: &S,
    bindings: &[Binding],
) -> Result<(), DriverError> {
    for binding in bindings {
        let update_ts = session.now_timestamp(0);
        exec(
            session,
            "UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE original_sql = %? AND update_time < %?",
            &[
                SqlArg::from(STATUS_DELETED),
                SqlArg::from(update_ts.as_str()),
                SqlArg::from(binding.original_sql.as_str()),
                SqlArg::from(update_ts.as_str()),
            ],
        )?;
        // Go stores NULL for an empty digest.
        fn digest(digest: &str) -> SqlArg<'_> {
            if digest.is_empty() {
                SqlArg::Null
            } else {
                SqlArg::from(digest)
            }
        }
        let db = binding.db.go_to_lower();
        exec(
            session,
            &format!(
                "INSERT INTO mysql.bind_info({BIND_INFO_COLUMNS}, plan_digest) \
                 VALUES (%?, %?, %?, %?, %?, %?, %?, %?, %?, %?, %?)"
            ),
            &[
                SqlArg::from(binding.original_sql.as_str()),
                SqlArg::from(binding.bind_sql.as_str()),
                SqlArg::from(db.as_str()),
                SqlArg::from(binding.status),
                SqlArg::from(update_ts.as_str()),
                SqlArg::from(update_ts.as_str()),
                SqlArg::from(binding.charset.as_str()),
                SqlArg::from(binding.collation.as_str()),
                SqlArg::from(binding.source),
                digest(&binding.sql_digest),
                digest(&binding.plan_digest),
            ],
        )?;
        // Go `execRows(sctx, "show warnings")`: a warning the insert raised
        // fails the operation with its message (`GetString(2)`).
        let warnings = exec_rows(session, "show warnings", &[])?;
        if let Some(warning) = warnings.first() {
            let message = warning.get(2).and_then(|datum| datum.sql_string().ok()).unwrap_or_default();
            return Err(plain_error(&message));
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
    session: &S,
    sql_digests: &[String],
) -> Result<u64, DriverError> {
    if sql_digests.is_empty() {
        return Err(plain_error("sql digest is empty"));
    }
    let mut deleted_rows = 0;
    for (i, sql_digest) in sql_digests.iter().enumerate() {
        let update_ts = session.now_timestamp(1 + i as i64);
        let affected = exec(
            session,
            "UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status != %?",
            &[
                SqlArg::from(STATUS_DELETED),
                SqlArg::from(update_ts.as_str()),
                SqlArg::from(sql_digest.as_str()),
                SqlArg::from(update_ts.as_str()),
                SqlArg::from(STATUS_DELETED),
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
    session: &S,
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
    let affected = exec(
        session,
        "UPDATE mysql.bind_info SET status = %?, update_time = %? WHERE sql_digest = %? AND update_time < %? AND status IN (%?, %?)",
        &[
            SqlArg::from(new_status),
            SqlArg::from(update_ts.as_str()),
            SqlArg::from(sql_digest),
            SqlArg::from(update_ts.as_str()),
            SqlArg::from(old_status0),
            SqlArg::from(old_status1),
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
pub fn gc_binding<S: BindingOperatorSession + ?Sized>(session: &S) -> Result<(), DriverError> {
    let ten_leases = i64::try_from((10 * LEASE).as_micros()).unwrap_or(i64::MAX);
    let update_time = session.now_timestamp(-ten_leases);
    exec(
        session,
        "DELETE FROM mysql.bind_info WHERE status = 'deleted' and update_time < %?",
        &[SqlArg::from(update_time.as_str())],
    )?;
    Ok(())
}
