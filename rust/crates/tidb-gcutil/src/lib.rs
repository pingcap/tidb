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
                    "snapshot is older than GC safe point {safe_point_time}"
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

/// client-go `CompatibleParseGCTime` over `GCTimeFormat`
/// (`20060102-15:04:05.000 -0700`: the milliseconds are part of the layout),
/// retrying without the last space-separated field (an older zone name).
fn compatible_parse_gc_time(value: &str) -> Result<chrono::DateTime<chrono::FixedOffset>, ()> {
    const GC_TIME_FORMAT: &str = "%Y%m%d-%H:%M:%S.%3f %z";
    if let Ok(parsed) = chrono::DateTime::parse_from_str(value, GC_TIME_FORMAT) {
        return Ok(parsed);
    }
    let mut fields = value.split(' ').collect::<Vec<_>>();
    fields.pop();
    chrono::DateTime::parse_from_str(&fields.join(" "), GC_TIME_FORMAT).map_err(|_| ())
}

fn go_time_to_ts(value: chrono::DateTime<chrono::FixedOffset>) -> u64 {
    (value.timestamp_millis() as u64) << 18
}

/// Go `model.TSConvert2Time(tso).String()`: the TSO's physical milliseconds
/// as a `time.Local` value, printed by `time.Time.String()`.
fn format_tso_in_process_location(tso: u64) -> String {
    tidb_util::go_time_layout::go_unix_milli_string((tso >> 18) as i64)
}

#[cfg(test)]
mod snapshot_validation_tests {

    use crate::{
        check_gc_enable, validate_snapshot, validate_snapshot_with_gc_safe_point, Context, Error,
    };

    /// A mock session context answering the mysql.tidb rows the GC utility reads.
    struct MockContext {
        globals: std::collections::HashMap<String, String>,
        tidb_table: std::collections::HashMap<String, String>,
        fail_reads: bool,
    }

    impl Context for MockContext {
        type Error = String;

        fn get_global_sys_var(&mut self, name: &str) -> Result<String, Self::Error> {
            if self.fail_reads {
                return Err("read refused".to_owned());
            }
            self.globals
                .get(name)
                .cloned()
                .ok_or_else(|| format!("unknown sysvar {name}"))
        }

        fn set_global_sys_var(&mut self, name: &str, value: &str) -> Result<(), Self::Error> {
            self.globals.insert(name.to_owned(), value.to_owned());
            Ok(())
        }

        fn exec_restricted_sql(
            &mut self,
            _sql: &str,
            arguments: &[&str],
            _internal_source_type: &str,
        ) -> Result<Vec<Vec<String>>, Self::Error> {
            if self.fail_reads {
                return Err("read refused".to_owned());
            }
            let key = arguments[0];
            match self.tidb_table.get(key) {
                Some(value) => Ok(vec![vec![value.clone()]]),
                None => Ok(Vec::new()),
            }
        }
    }

    /// Go `ValidateSnapshot` refuses a snapshot older than the GC safe point
    /// with the safe point time rendered through `TSConvert2Time`, and accepts
    /// a newer one; `CheckGCEnable` reads the table value ON/OFF.
    #[test]
    fn snapshot_validation_matches_go() {
        let mut ctx = MockContext {
            globals: std::collections::HashMap::new(),
            tidb_table: std::collections::HashMap::new(),
            fail_reads: false,
        };
        // The GC worker's `GCTimeFormat`, milliseconds included.
        ctx.tidb_table.insert(
            "tikv_gc_safe_point".to_owned(),
            "20240101-00:00:00.000 +0000".to_owned(),
        );

        // A snapshot after the safe point passes.
        validate_snapshot(&mut ctx, 1_704_067_200_000_000_000).unwrap();
        // A snapshot before it refuses with the rendered safe point time.
        match validate_snapshot(&mut ctx, 1) {
            Err(Error::SnapshotTooOld { safe_point_time }) => {
                // `TSConvert2Time` renders in the process's local zone.
                use chrono::TimeZone as _;
                let local = chrono::Local
                    .timestamp_millis_opt(1_704_067_200_000)
                    .single()
                    .unwrap()
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();
                assert!(safe_point_time.starts_with(&local), "{safe_point_time}");
                // `time.Time.String()` names the zone (`PST`, `UTC`, `-03`);
                // it never prints chrono's `%Z` offset form `-08:00`.
                let zone_name = safe_point_time.rsplit(' ').next().unwrap();
                assert!(!zone_name.contains(':'), "{safe_point_time}");
                // Go `errno.ErrSnapshotTooOld`'s message text.
                assert_eq!(
                    Error::<std::convert::Infallible>::SnapshotTooOld {
                        safe_point_time: safe_point_time.clone(),
                    }
                    .to_string(),
                    format!("snapshot is older than GC safe point {safe_point_time}")
                );
                assert_eq!(
                    safe_point_time,
                    tidb_util::go_time_layout::go_time_string(
                        chrono::Local
                            .timestamp_millis_opt(1_704_067_200_000)
                            .single()
                            .unwrap()
                    )
                );
            }
            other => panic!("expected SnapshotTooOld, got {other:?}"),
        }
        // An older value carrying a zone name after the offset parses
        // through the retry without its last field.
        ctx.tidb_table.insert(
            "tikv_gc_safe_point".to_owned(),
            "20261020-05:17:17.103 -0700 PDT".to_owned(),
        );
        validate_snapshot(&mut ctx, 1_792_498_637_104_u64 << 18).unwrap();
        assert!(matches!(
            validate_snapshot(&mut ctx, 1_792_498_637_102_u64 << 18),
            Err(Error::SnapshotTooOld { .. })
        ));
        // Without the milliseconds the layout does not match, as in Go.
        ctx.tidb_table.insert(
            "tikv_gc_safe_point".to_owned(),
            "20240101-00:00:00 +0000".to_owned(),
        );
        assert!(matches!(
            crate::get_gc_safe_point(&mut ctx),
            Err(Error::InvalidGcTime { .. })
        ));
        // A read failure propagates unchanged.
        ctx.fail_reads = true;
        assert!(validate_snapshot(&mut ctx, 1).is_err());
    }

    /// Go `CheckGCEnable` reads the table ON/OFF value, not a sysvar.
    #[test]
    fn gc_enable_reads_the_table_value_like_go() {
        let mut ctx = MockContext {
            globals: std::collections::HashMap::new(),
            tidb_table: std::collections::HashMap::new(),
            fail_reads: false,
        };
        ctx.globals
            .insert("tidb_gc_enable".to_owned(), "ON".to_owned());
        assert!(check_gc_enable(&mut ctx).unwrap());
        ctx.globals
            .insert("tidb_gc_enable".to_owned(), "OFF".to_owned());
        assert!(!check_gc_enable(&mut ctx).unwrap());
    }
}
