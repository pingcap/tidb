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

//! GC session utilities transcreated from Go `pkg/util/gcutil`.

use std::{convert::Infallible, fmt};

use chrono::TimeZone as _;

const SELECT_VARIABLE_VALUE_SQL: &str =
    "SELECT HIGH_PRIORITY variable_value FROM mysql.tidb WHERE variable_name=%?";
const GC_SAFE_POINT_VARIABLE: &str = "tikv_gc_safe_point";

/// The `sessionctx.Context` capabilities used by Go `pkg/util/gcutil`.
pub trait Context {
    /// Error returned by the session implementation.
    type Error;

    /// Go `GlobalVarsAccessor.GetGlobalSysVar`.
    fn get_global_sys_var(&mut self, name: &str) -> Result<String, Self::Error>;

    /// Go `GlobalVarsAccessor.SetGlobalSysVar`.
    fn set_global_sys_var(&mut self, name: &str, value: &str) -> Result<(), Self::Error>;

    /// Go `RestrictedSQLExecutor.ExecRestrictedSQL` with its positional arguments.
    fn exec_restricted_sql(
        &mut self,
        sql: &str,
        arguments: &[&str],
        internal_source_type: &str,
    ) -> Result<Vec<Vec<String>>, Self::Error>;
}

/// A failure returned by the GC utility package.
#[derive(Debug)]
pub enum Error<E = Infallible> {
    /// The session context returned an error unchanged.
    Context(E),
    /// The restricted query did not return exactly one row.
    MissingSafePoint,
    /// client-go `CompatibleParseGCTime` rejected the stored value.
    InvalidGcTime {
        /// Stored value that could not be parsed.
        value: String,
    },
    /// Go `variable.ErrSnapshotTooOld` (8055).
    SnapshotTooOld {
        /// `model.TSConvert2Time(safePointTS).String()`.
        safe_point_time: String,
    },
}

impl<E: fmt::Display> fmt::Display for Error<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Context(error) => error.fmt(formatter),
            Self::MissingSafePoint => formatter.write_str("can not get 'tikv_gc_safe_point'"),
            Self::InvalidGcTime { value } => write!(
                formatter,
                "string \"{value}\" doesn't has a prefix that matches format \"20060102-15:04:05.000 -0700\""
            ),
            Self::SnapshotTooOld { safe_point_time } => {
                write!(
                    formatter,
                    "Snapshot is older than GC safe point {safe_point_time}"
                )
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for Error<E> {}

/// Go `CheckGCEnable`.
pub fn check_gc_enable<C: Context>(ctx: &mut C) -> Result<bool, Error<C::Error>> {
    let value = ctx
        .get_global_sys_var(tidb_vardef::tidb_vars::TIDB_GC_ENABLE)
        .map_err(Error::Context)?;
    Ok(tidb_opt_on(&value))
}

/// Go `DisableGC`.
pub fn disable_gc<C: Context>(ctx: &mut C) -> Result<(), Error<C::Error>> {
    ctx.set_global_sys_var(tidb_vardef::tidb_vars::TIDB_GC_ENABLE, "OFF")
        .map_err(Error::Context)
}

/// Go `EnableGC`.
pub fn enable_gc<C: Context>(ctx: &mut C) -> Result<(), Error<C::Error>> {
    ctx.set_global_sys_var(tidb_vardef::tidb_vars::TIDB_GC_ENABLE, "ON")
        .map_err(Error::Context)
}

/// Go `ValidateSnapshot`.
pub fn validate_snapshot<C: Context>(ctx: &mut C, snapshot_ts: u64) -> Result<(), Error<C::Error>> {
    let safe_point_ts = get_gc_safe_point(ctx)?;
    validate_snapshot_with_gc_safe_point(snapshot_ts, safe_point_ts).map_err(|error| match error {
        Error::SnapshotTooOld { safe_point_time } => Error::SnapshotTooOld { safe_point_time },
        Error::Context(never) => match never {},
        Error::MissingSafePoint | Error::InvalidGcTime { .. } => unreachable!(),
    })
}

/// Go `ValidateSnapshotWithGCSafePoint`.
pub fn validate_snapshot_with_gc_safe_point(
    snapshot_ts: u64,
    safe_point_ts: u64,
) -> Result<(), Error> {
    if safe_point_ts > snapshot_ts {
        return Err(Error::SnapshotTooOld {
            safe_point_time: format_tso_in_process_location(safe_point_ts),
        });
    }
    Ok(())
}

/// Go `GetGCSafePoint`.
pub fn get_gc_safe_point<C: Context>(ctx: &mut C) -> Result<u64, Error<C::Error>> {
    let rows = ctx
        .exec_restricted_sql(SELECT_VARIABLE_VALUE_SQL, &[GC_SAFE_POINT_VARIABLE], "gc")
        .map_err(Error::Context)?;
    if rows.len() != 1 {
        return Err(Error::MissingSafePoint);
    }
    let value = rows[0].first().cloned().unwrap_or_default();
    compatible_parse_gc_time(&value)
        .map(go_time_to_ts)
        .map_err(|()| Error::InvalidGcTime { value })
}

fn tidb_opt_on(value: &str) -> bool {
    value == "1" || value.eq_ignore_ascii_case("ON")
}

fn compatible_parse_gc_time(value: &str) -> Result<chrono::DateTime<chrono::FixedOffset>, ()> {
    if let Ok(parsed) = chrono::DateTime::parse_from_str(value, "%Y%m%d-%H:%M:%S %z") {
        return Ok(parsed);
    }
    let mut fields = value.split(' ').collect::<Vec<_>>();
    fields.pop();
    chrono::DateTime::parse_from_str(&fields.join(" "), "%Y%m%d-%H:%M:%S %z").map_err(|_| ())
}

fn go_time_to_ts(value: chrono::DateTime<chrono::FixedOffset>) -> u64 {
    (value.timestamp_millis() as u64) << 18
}

fn format_tso_in_process_location(tso: u64) -> String {
    let millis = (tso >> 18) as i64;
    let value = chrono::Local
        .timestamp_millis_opt(millis)
        .single()
        .expect("TSO physical milliseconds fit Chrono");
    let mut output = value.format("%Y-%m-%d %H:%M:%S").to_string();
    let fractional = millis.rem_euclid(1_000);
    if fractional != 0 {
        let fraction = format!("{fractional:03}");
        output.push('.');
        output.push_str(fraction.trim_end_matches('0'));
    }
    output.push(' ');
    output.push_str(&value.format("%z %Z").to_string());
    output
}
