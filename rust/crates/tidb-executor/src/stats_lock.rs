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

use std::collections::{BTreeMap, BTreeSet};

use tidb_datatype::Datum;
use tidb_stats::StatsLockTransaction;

use crate::{Catalog, DriverError, SchemaErrorKind, StmtContext, TableEntry};

/// Executes one parsed `LOCK STATS` or `UNLOCK STATS` through the ordinary
/// catalog-backed executor path and returns Go's optional warning text.
pub fn execute_catalog_stats_lock(
    statement: &tidb_ast::StatsLockStmt,
    lock: bool,
    catalog: &mut Catalog,
    current_database: &str,
    context: &StmtContext,
    start_ts: u64,
) -> Result<String, DriverError> {
    if statement.tables.is_empty() {
        return Err(DriverError::unsupported(if lock {
            "Lock Stats: table should not empty"
        } else {
            "Unlock Stats: table should not empty "
        }));
    }

    let only_partitions = statement.tables.len() == 1 && !statement.tables[0].partitions.is_empty();
    if only_partitions {
        let target = &statement.tables[0];
        let (database, table_name) =
            crate::driver::split_table_path_pub(&target.name, current_database)?;
        let table = kv_table(catalog, database, table_name)?;
        let Some(partition) = table.partition() else {
            return Err(DriverError::unsupported(format!(
                "table {}.{} is not a partition table",
                database.to_lowercase(),
                table_name.to_lowercase()
            )));
        };
        let mut partition_names = BTreeMap::new();
        for written_name in &target.partitions {
            let definition = partition
                .definitions
                .iter()
                .find(|definition| definition.name.eq_ignore_ascii_case(written_name))
                .ok_or_else(|| DriverError::UnknownPartition {
                    partition: written_name.to_lowercase(),
                    table: table.name.clone(),
                })?;
            partition_names.insert(definition.id, written_name.to_lowercase());
        }
        let table_id = table.table_id;
        let displayed_table = if lock {
            format!("{}.{}", database.to_lowercase(), table_name.to_lowercase())
        } else {
            format!("{database}.{table_name}")
        };
        let mut transaction = CatalogStatsLockTransaction::new(catalog, context, start_ts);
        return if lock {
            tidb_stats::add_locked_partitions(
                &mut transaction,
                table_id,
                &displayed_table,
                &partition_names,
            )
        } else {
            tidb_stats::remove_locked_partitions(
                &mut transaction,
                table_id,
                &displayed_table,
                &partition_names,
            )
        };
    }

    let mut tables = BTreeMap::new();
    for target in &statement.tables {
        let (database, table_name) =
            crate::driver::split_table_path_pub(&target.name, current_database)?;
        let table = kv_table(catalog, database, table_name)?;
        let partition_info = table.partition().map(|partition| {
            partition
                .definitions
                .iter()
                .map(|definition| {
                    (
                        definition.id,
                        format!(
                            "{}.{} partition ({})",
                            database.to_lowercase(),
                            table_name.to_lowercase(),
                            definition.name.to_lowercase()
                        ),
                    )
                })
                .collect()
        });
        tables.insert(
            table.table_id,
            tidb_stats::StatsLockTable {
                partition_info,
                full_name: format!("{}.{}", database.to_lowercase(), table_name.to_lowercase()),
            },
        );
    }
    let mut transaction = CatalogStatsLockTransaction::new(catalog, context, start_ts);
    if lock {
        tidb_stats::add_locked_tables(&mut transaction, &tables)
    } else {
        tidb_stats::remove_locked_tables(&mut transaction, &tables)
    }
}

fn kv_table<'a>(
    catalog: &'a Catalog,
    database: &str,
    table_name: &str,
) -> Result<&'a crate::KvTable, DriverError> {
    match catalog.table_in(database, table_name) {
        Some(TableEntry::Kv(table)) => Ok(table),
        _ => Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
            "{database}.{table_name}"
        )))),
    }
}

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
    use tidb_stats::{add_locked_tables, remove_locked_tables, StatsLockTable};

    #[test]
    fn catalog_adapter_persists_lock_rows_and_merges_deltas() {
        let mut catalog = Catalog::default();
        let context = StmtContext::for_query();
        let settings = crate::CreateTableSettings::default();
        crate::run_create_table_in(
            tidb_metadef::system_tables_def::CREATE_STATS_META_TABLE,
            &mut catalog,
            "mysql",
            settings,
            &context,
        )
        .unwrap();
        crate::run_create_table_in(
            tidb_metadef::system_tables_def::CREATE_STATS_TABLE_LOCKED_TABLE,
            &mut catalog,
            "mysql",
            settings,
            &context,
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
