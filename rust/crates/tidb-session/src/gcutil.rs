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

use crate::Session;

impl tidb_gcutil::Context for Session {
    type Error = DriverError;

    fn get_global_sys_var(&mut self, name: &str) -> Result<String, Self::Error> {
        self.vars
            .get_global(name)
            .map_err(crate::variables::var_error)
    }

    fn set_global_sys_var(&mut self, name: &str, value: &str) -> Result<(), Self::Error> {
        self.vars
            .set_global(name, value.to_owned())
            .map(|_| ())
            .map_err(crate::variables::var_error)
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
        let ctx = self.statement_context(false);
        let (_, rows) = self.with_catalog_mut(|catalog| {
            tidb_executor::run_select_meta_in(&statement, catalog, "mysql", &ctx)
        })?;
        Ok(rows
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
        tidb_gcutil::Error::SnapshotTooOld { safe_point_time } => {
            DriverError::SnapshotTooOld(safe_point_time)
        }
        other => DriverError::unsupported(other.to_string()),
    }
}
