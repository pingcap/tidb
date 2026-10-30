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

//! Go `pkg/statistics/handle/lockstats`' `QueryLockedTables` over an
//! in-process catalog: the statement Go's restricted SQL session runs against
//! `mysql.stats_table_locked`, for `SHOW STATS_LOCKED`.

use std::collections::BTreeSet;

use tidb_datatype::Datum;

use crate::{Catalog, DriverError, StmtContext};

/// Reads every persisted lock ID, the query Go's `QueryLockedTables` runs.
pub fn query_catalog_locked_tables(
    catalog: &Catalog,
    context: &StmtContext,
) -> Result<BTreeSet<i64>, DriverError> {
    let rows = crate::run_select_meta_in(
        "SELECT table_id FROM mysql.stats_table_locked",
        catalog,
        "mysql",
        context,
    )?
    .1;
    rows.into_iter()
        .map(|row| required_i64(row.first(), "stats_table_locked.table_id"))
        .collect()
}

fn required_i64(value: Option<&Datum>, column: &str) -> Result<i64, DriverError> {
    match value {
        Some(Datum::Int(value)) => Ok(*value),
        Some(Datum::UInt(value)) => i64::try_from(*value)
            .map_err(|_| DriverError::unsupported(format!("invalid {column}: {value}"))),
        value => Err(DriverError::unsupported(format!(
            "invalid {column}: {value:?}"
        ))),
    }
}
