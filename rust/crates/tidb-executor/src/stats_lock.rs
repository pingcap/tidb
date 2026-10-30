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

//! In-process persistence adapter for Go `pkg/statistics/handle/lockstats`.
//!
//! It deliberately executes the same statements Go's restricted SQL session
//! executes against the actual `mysql.stats_table_locked` and
//! `mysql.stats_meta` tables. The caller supplies a statement-staged catalog,
//! making the complete lock/unlock operation atomic.

use std::collections::BTreeSet;

use tidb_datatype::Datum;
use tidb_stats_handle_lockstats::lock_stats::StatsLockTransaction;

use crate::{Catalog, DriverError, StmtContext};

/// One lockstats transaction over an in-process catalog.
pub struct CatalogStatsLockTransaction<'a> {
    catalog: &'a mut Catalog,
    context: &'a StmtContext,
    start_ts: u64,
}

impl<'a> CatalogStatsLockTransaction<'a> {
    /// Binds the storage adapter to the statement's staged catalog.
    #[must_use]
    pub fn new(catalog: &'a mut Catalog, context: &'a StmtContext, start_ts: u64) -> Self {
        Self {
            catalog,
            context,
            start_ts,
        }
    }
}

impl StatsLockTransaction for CatalogStatsLockTransaction<'_> {
    type Error = DriverError;

    fn query_locked_tables(&mut self) -> Result<BTreeSet<i64>, Self::Error> {
        query_catalog_locked_tables(self.catalog, self.context)
    }

    fn insert_lock_and_update_meta_version(&mut self, table_id: i64) -> Result<(), Self::Error> {
        crate::run_insert_in(
            &format!(
                "INSERT INTO mysql.stats_table_locked (table_id) VALUES ({table_id}) \
                 ON DUPLICATE KEY UPDATE table_id = {table_id}"
            ),
            self.catalog,
            "mysql",
            self.context,
        )?;
        crate::run_update_in(
            &format!(
                "UPDATE mysql.stats_meta SET version = {} WHERE table_id = {table_id}",
                self.start_ts
            ),
            self.catalog,
            "mysql",
            self.context,
        )?;
        Ok(())
    }

    fn lock_delta(&mut self, table_id: i64) -> Result<(i64, i64), Self::Error> {
        let rows = crate::run_select_meta_in(
            &format!(
                "SELECT count, modify_count FROM mysql.stats_table_locked \
                 WHERE table_id = {table_id}"
            ),
            self.catalog,
            "mysql",
            self.context,
        )?
        .1;
        let Some(row) = rows.first() else {
            return Ok((0, 0));
        };
        Ok((
            required_i64(row.first(), "stats_table_locked.count")?,
            required_i64(row.get(1), "stats_table_locked.modify_count")?,
        ))
    }

    fn update_meta_delta(
        &mut self,
        table_id: i64,
        count_delta: i64,
        modify_count_delta: i64,
    ) -> Result<(), Self::Error> {
        crate::run_update_in(
            &format!(
                "UPDATE mysql.stats_meta SET version = {}, \
                 count = IF(count + {count_delta} > 0, count + {count_delta}, 0), \
                 modify_count = modify_count + {modify_count_delta} \
                 WHERE table_id = {table_id}",
                self.start_ts
            ),
            self.catalog,
            "mysql",
            self.context,
        )?;
        Ok(())
    }

    fn delete_lock(&mut self, table_id: i64) -> Result<(), Self::Error> {
        crate::run_delete_in(
            &format!("DELETE FROM mysql.stats_table_locked WHERE table_id = {table_id}"),
            self.catalog,
            "mysql",
            self.context,
        )?;
        Ok(())
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_stats_handle_lockstats::lock_stats::{add_locked_tables, remove_locked_tables};
    use tidb_stats_handle_types::StatsLockTable;

    #[test]
    fn catalog_adapter_persists_lock_rows_and_merges_deltas() {
        let mut catalog = tidb_infoschema_perfschema::mock_catalog();
        let context = StmtContext::for_query();
        tidb_infoschema_perfschema::run_create_system_table_in(
            tidb_metadef::system_tables_def::CREATE_STATS_META_TABLE,
            &mut catalog,
            "mysql",
            tidb_metadef::system::STATS_META_TABLE_ID,
        )
        .unwrap();
        tidb_infoschema_perfschema::run_create_system_table_in(
            tidb_metadef::system_tables_def::CREATE_STATS_TABLE_LOCKED_TABLE,
            &mut catalog,
            "mysql",
            tidb_metadef::system::STATS_TABLE_LOCKED_TABLE_ID,
        )
        .unwrap();
        crate::run_insert_in(
            "INSERT INTO mysql.stats_meta(version, table_id, modify_count, count) \
             VALUES (1, 10, 2, 5), (1, 11, 3, 7)",
            &mut catalog,
            "mysql",
            &context,
        )
        .unwrap();
        let tables = std::collections::BTreeMap::from([(
            10,
            StatsLockTable {
                full_name: "test.t".to_owned(),
                partition_info: Some(std::collections::BTreeMap::from([(11, "p0".to_owned())])),
            },
        )]);
        let mut transaction = CatalogStatsLockTransaction::new(&mut catalog, &context, 100);
        assert_eq!(add_locked_tables(&mut transaction, &tables).unwrap(), "");
        crate::run_update_in(
            "UPDATE mysql.stats_table_locked SET count = 4, modify_count = 6 \
             WHERE table_id = 11",
            &mut catalog,
            "mysql",
            &context,
        )
        .unwrap();
        let mut transaction = CatalogStatsLockTransaction::new(&mut catalog, &context, 200);
        assert_eq!(remove_locked_tables(&mut transaction, &tables).unwrap(), "");
        let rows = crate::run_select_meta_in(
            "SELECT table_id, version, modify_count, count FROM mysql.stats_meta ORDER BY table_id",
            &catalog,
            "mysql",
            &context,
        )
        .unwrap()
        .1;
        assert_eq!(
            rows,
            vec![
                vec![
                    Datum::Int(10),
                    Datum::UInt(200),
                    Datum::Int(8),
                    Datum::UInt(9)
                ],
                vec![
                    Datum::Int(11),
                    Datum::UInt(200),
                    Datum::Int(9),
                    Datum::UInt(11)
                ],
            ]
        );
    }
}
