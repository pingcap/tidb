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

use tidb_executor::DriverError;

use tidb_executor::memtable_reader::RestrictedSqlExecutor;

/// Go `validateSnapshot`: the GC safe point read through restricted SQL, on
/// an internal session at the current time, independent of the user's
/// transaction, staged writes and snapshot pin.
pub(crate) fn validate_snapshot(
    executor: &dyn RestrictedSqlExecutor,
    ts: u64,
) -> Result<(), DriverError> {
    tidb_gcutil::validate_snapshot(&mut RestrictedGcContext(executor), ts).map_err(driver_error)
}

/// `tidb_gcutil`'s session context over a restricted SQL executor.
struct RestrictedGcContext<'a>(&'a dyn RestrictedSqlExecutor);

impl tidb_gcutil::Context for RestrictedGcContext<'_> {
    type Error = DriverError;

    fn get_global_sys_var(&mut self, name: &str) -> Result<String, Self::Error> {
        Err(DriverError::unsupported(format!(
            "restricted GC context reads no global variable ({name})"
        )))
    }

    fn set_global_sys_var(&mut self, name: &str, _value: &str) -> Result<(), Self::Error> {
        Err(DriverError::unsupported(format!(
            "restricted GC context writes no global variable ({name})"
        )))
    }

    fn exec_restricted_sql(
        &mut self,
        sql: &str,
        arguments: &[&str],
        _internal_source_type: &str,
    ) -> Result<Vec<Vec<String>>, Self::Error> {
        let mut statement = sql.to_owned();
        for argument in arguments {
            let quoted = format!("'{}'", argument.replace('\\', "\\\\").replace('\'', "''"));
            statement = statement.replacen("%?", &quoted, 1);
        }
        Ok(self
            .0
            .exec_restricted_sql(&statement)?
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|value| crate::datum_text(value).unwrap_or_default())
                    .collect()
            })
            .collect())
    }
}

pub(crate) fn driver_error(error: tidb_gcutil::Error<DriverError>) -> DriverError {
    match error {
        tidb_gcutil::Error::Context(error) => error,
        tidb_gcutil::Error::SnapshotTooOld { safe_point_time } => DriverError::from(
            tidb_error::catalog::variable::ERR_SNAPSHOT_TOO_OLD
                .generate_with_stack_by_args(&[safe_point_time.into()]),
        ),
        other => DriverError::unsupported(other.to_string()),
    }
}
