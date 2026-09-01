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

//! Cluster storage planning for Go `pkg/statistics/handle/lockstats`.

use std::collections::{BTreeMap, BTreeSet};

use tidb_datatype::{Datum, Time};
use tidb_model::table_info::TableInfo;
use tidb_stats::{StatsLockTable, StatsLockTransaction};
use tidb_txnkv::transaction::OptimisticMutation;

use crate::cluster_catalog::{ClusterCatalog, MetaSnapshot};
use crate::mysql_system_tables::{scan_system_table, SystemRow, SystemTableError, SystemTableView};
use crate::system_row_write::{
    defaults_row, delete_clustered_row, store_clustered_row, RowEncodeError, RowValues,
};

const LOCK_TABLE: &str = "stats_table_locked";
const META_TABLE: &str = "stats_meta";

/// One table target retained from a parsed lock statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsLockTarget {
    /// Schema name after default-schema resolution.
    pub schema: String,
    /// Table name as written.
    pub table: String,
    /// Named partitions, if this is a partition-only operation.
    pub partitions: Vec<String>,
}

/// One parsed `LOCK STATS` or `UNLOCK STATS` statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClusterStatsLockStatement {
    /// `true` for LOCK and `false` for UNLOCK.
    pub lock: bool,
    /// Targets in source order.
    pub targets: Vec<StatsLockTarget>,
}

/// Parses a cluster-routed statistics lock statement.
pub fn prepare_cluster_stats_lock(
    sql: &str,
    default_schema: &str,
) -> Result<Option<ClusterStatsLockStatement>, ClusterStatsLockError> {
    let Ok(statement) = tidb_parser::parse(sql) else {
        return Ok(None);
    };
    let tidb_ast::Stmt::Admin(admin) = statement else {
        return Ok(None);
    };
    let (lock, statement) = match admin.as_ref() {
        tidb_ast::AdminStmt::LockStats(statement) => (true, statement.as_ref()),
        tidb_ast::AdminStmt::UnlockStats(statement) => (false, statement.as_ref()),
        _ => return Ok(None),
    };
    let targets = statement
        .tables
        .iter()
        .map(|target| {
            let (schema, table) = match target.name.as_slice() {
                [table] if !default_schema.is_empty() => (default_schema.to_owned(), table.clone()),
                [_table] => return Err(ClusterStatsLockError::NoDatabaseSelected),
                [schema, table] => (schema.clone(), table.clone()),
                _ => {
                    return Err(ClusterStatsLockError::Invalid(
                        "empty table name".to_owned(),
                    ))
                }
            };
            Ok(StatsLockTarget {
                schema,
                table,
                partitions: target.partitions.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(ClusterStatsLockStatement { lock, targets }))
}

/// Separates target-resolution failures from the storage transaction used by
/// the shared lockstats policy.
pub(crate) enum ClusterStatsLockApplyError<E> {
    Plan(ClusterStatsLockError),
    Transaction(E),
}

/// Why a cluster lock operation could not be planned.
#[derive(Debug)]
pub enum ClusterStatsLockError {
    /// A bare table name was used with no selected schema.
    NoDatabaseSelected,
    /// The named table is absent from the statement's infoschema.
    MissingTable {
        /// Schema named by or resolved for the statement.
        schema: String,
        /// Missing table name.
        table: String,
    },
    /// A partition name is absent from the named table.
    UnknownPartition {
        /// Missing partition name.
        partition: String,
        /// Table against which the partition was resolved.
        table: String,
    },
    /// Missing table or invalid target.
    Invalid(String),
    /// System-row read failure.
    Read(SystemTableError),
    /// System-row encoding failure.
    Encode(RowEncodeError),
}

impl std::fmt::Display for ClusterStatsLockError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoDatabaseSelected => formatter.write_str("No database selected"),
            Self::MissingTable { schema, table } => {
                write!(formatter, "Table '{schema}.{table}' doesn't exist")
            }
            Self::UnknownPartition { partition, table } => {
                write!(
                    formatter,
                    "Unknown partition '{partition}' in table '{table}'"
                )
            }
            Self::Invalid(detail) => formatter.write_str(detail),
            Self::Read(error) => error.fmt(formatter),
            Self::Encode(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ClusterStatsLockError {}

impl From<SystemTableError> for ClusterStatsLockError {
    fn from(error: SystemTableError) -> Self {
        Self::Read(error)
    }
}

impl From<RowEncodeError> for ClusterStatsLockError {
    fn from(error: RowEncodeError) -> Self {
        Self::Encode(error)
    }
}

/// Resolves one statement's targets once, then drives Go's lockstats policy
/// through the supplied transaction boundary.
pub(crate) fn apply_cluster_stats_lock<T: StatsLockTransaction>(
    transaction: &mut T,
    catalog: &ClusterCatalog,
    statement: &ClusterStatsLockStatement,
) -> Result<String, ClusterStatsLockApplyError<T::Error>> {
    if statement.targets.is_empty() {
        return Err(ClusterStatsLockApplyError::Plan(
            ClusterStatsLockError::Invalid(if statement.lock {
                "Lock Stats: table should not empty".to_owned()
            } else {
                "Unlock Stats: table should not empty ".to_owned()
            }),
        ));
    }
    let only_partitions =
        statement.targets.len() == 1 && !statement.targets[0].partitions.is_empty();
    if only_partitions {
        let target = &statement.targets[0];
        let table = user_table(catalog, &target.schema, &target.table)
            .map_err(ClusterStatsLockApplyError::Plan)?;
        let Some(partition) = table.get_partition_info() else {
            return Err(ClusterStatsLockApplyError::Plan(
                ClusterStatsLockError::Invalid(format!(
                    "table {}.{} is not a partition table",
                    target.schema.to_lowercase(),
                    target.table.to_lowercase()
                )),
            ));
        };
        let partition = partition.read();
        let mut partitions = BTreeMap::new();
        for written in &target.partitions {
            let definition = partition
                .definitions
                .map_visible(|definition| {
                    definition
                        .name
                        .lowercase()
                        .eq_ignore_ascii_case(written)
                        .then(|| (definition.id, definition.name.original().to_owned()))
                })
                .into_iter()
                .flatten()
                .next()
                .ok_or_else(|| {
                    ClusterStatsLockApplyError::Plan(ClusterStatsLockError::UnknownPartition {
                        partition: written.to_lowercase(),
                        table: table.name.original().to_owned(),
                    })
                })?;
            partitions.insert(definition.0, written.to_lowercase());
        }
        let displayed = if statement.lock {
            format!(
                "{}.{}",
                target.schema.to_lowercase(),
                target.table.to_lowercase()
            )
        } else {
            format!("{}.{}", target.schema, target.table)
        };
        if statement.lock {
            tidb_stats::add_locked_partitions(transaction, table.id, &displayed, &partitions)
                .map_err(ClusterStatsLockApplyError::Transaction)
        } else {
            tidb_stats::remove_locked_partitions(transaction, table.id, &displayed, &partitions)
                .map_err(ClusterStatsLockApplyError::Transaction)
        }
    } else {
        let mut tables = BTreeMap::new();
        for target in &statement.targets {
            let table = user_table(catalog, &target.schema, &target.table)
                .map_err(ClusterStatsLockApplyError::Plan)?;
            let partition_info = table.get_partition_info().map(|partition| {
                partition
                    .read()
                    .definitions
                    .map_visible(|definition| {
                        (
                            definition.id,
                            format!(
                                "{}.{} partition ({})",
                                target.schema.to_lowercase(),
                                target.table.to_lowercase(),
                                definition.name.lowercase()
                            ),
                        )
                    })
                    .into_iter()
                    .collect()
            });
            tables.insert(
                table.id,
                StatsLockTable {
                    partition_info,
                    full_name: format!(
                        "{}.{}",
                        target.schema.to_lowercase(),
                        target.table.to_lowercase()
                    ),
                },
            );
        }
        if statement.lock {
            tidb_stats::add_locked_tables(transaction, &tables)
                .map_err(ClusterStatsLockApplyError::Transaction)
        } else {
            tidb_stats::remove_locked_tables(transaction, &tables)
                .map_err(ClusterStatsLockApplyError::Transaction)
        }
    }
}

/// Plans Go's `SELECT table_id FROM mysql.stats_table_locked` statement.
pub(crate) fn query_cluster_locked_tables<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
) -> Result<BTreeSet<i64>, ClusterStatsLockError> {
    Ok(read_rows(snapshot, system_table(catalog, LOCK_TABLE)?)?
        .into_keys()
        .collect())
}

/// Plans Go's one `INSERT ... ON DUPLICATE KEY UPDATE` lock statement.
pub(crate) fn plan_cluster_insert_lock<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    now: Time,
) -> Result<Vec<OptimisticMutation>, ClusterStatsLockError> {
    let table = system_table(catalog, LOCK_TABLE)?;
    let rows = read_rows(snapshot, table)?;
    if let Some(row) = rows.get(&table_id) {
        // The duplicate arm still writes `table_id = table_id` and therefore
        // participates in Go's pessimistic statement locking.
        return Ok(store_clustered_row(table, Some(row), row)?);
    }
    let mut row = defaults_row(table, now)?;
    set(table, &mut row, "table_id", Datum::Int(table_id))?;
    Ok(store_clustered_row(table, None, &row)?)
}

/// Plans Go's one stats-meta version update statement.
pub(crate) fn plan_cluster_update_meta_version<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    start_ts: u64,
) -> Result<Vec<OptimisticMutation>, ClusterStatsLockError> {
    let table = system_table(catalog, META_TABLE)?;
    let rows = read_rows(snapshot, table)?;
    let Some(initial) = rows.get(&table_id) else {
        return Ok(Vec::new());
    };
    let mut updated = initial.clone();
    set(table, &mut updated, "version", Datum::UInt(start_ts))?;
    Ok(store_clustered_row(table, Some(initial), &updated)?)
}

/// Executes Go's one lock-delta select against the transaction snapshot.
pub(crate) fn query_cluster_lock_delta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
) -> Result<(i64, i64), ClusterStatsLockError> {
    let table = system_table(catalog, LOCK_TABLE)?;
    let rows = read_rows(snapshot, table)?;
    let Some(row) = rows.get(&table_id) else {
        return Ok((0, 0));
    };
    Ok((
        row_i64(table, row, "count")?,
        row_i64(table, row, "modify_count")?,
    ))
}

/// Plans Go's one delta merge into `mysql.stats_meta`.
pub(crate) fn plan_cluster_update_meta_delta<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
    count_delta: i64,
    modify_count_delta: i64,
    start_ts: u64,
) -> Result<Vec<OptimisticMutation>, ClusterStatsLockError> {
    let table = system_table(catalog, META_TABLE)?;
    let rows = read_rows(snapshot, table)?;
    let Some(initial) = rows.get(&table_id) else {
        return Ok(Vec::new());
    };
    let mut updated = initial.clone();
    let count = i128::from(row_u64(table, initial, "count")?) + i128::from(count_delta);
    let count = u64::try_from(count.max(0))
        .map_err(|_| ClusterStatsLockError::Invalid("stats_meta.count overflow".to_owned()))?;
    let modify_count = row_i64(table, initial, "modify_count")?
        .checked_add(modify_count_delta)
        .ok_or_else(|| {
            ClusterStatsLockError::Invalid("stats_meta.modify_count overflow".to_owned())
        })?;
    set(table, &mut updated, "version", Datum::UInt(start_ts))?;
    set(table, &mut updated, "count", Datum::UInt(count))?;
    set(
        table,
        &mut updated,
        "modify_count",
        Datum::Int(modify_count),
    )?;
    Ok(store_clustered_row(table, Some(initial), &updated)?)
}

/// Plans Go's one lock-row delete statement.
pub(crate) fn plan_cluster_delete_lock<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    table_id: i64,
) -> Result<Vec<OptimisticMutation>, ClusterStatsLockError> {
    let table = system_table(catalog, LOCK_TABLE)?;
    let rows = read_rows(snapshot, table)?;
    rows.get(&table_id)
        .map_or(Ok(Vec::new()), |row| Ok(delete_clustered_row(table, row)?))
}

fn read_rows<S: MetaSnapshot>(
    snapshot: &mut S,
    table: &TableInfo,
) -> Result<BTreeMap<i64, RowValues>, ClusterStatsLockError> {
    let view = full_view(table);
    let table_id_column = column_id(table, "table_id")?;
    let mut rows = BTreeMap::new();
    for (key, value) in scan_system_table(snapshot, &view)? {
        let values = SystemRow::parse(&view, &key, &value)?.into_values();
        let table_id = datum_i64(values.get(&table_id_column), "table_id")?;
        rows.insert(table_id, values);
    }
    Ok(rows)
}

fn system_table<'a>(
    catalog: &'a ClusterCatalog,
    name: &str,
) -> Result<&'a TableInfo, ClusterStatsLockError> {
    user_table(catalog, "mysql", name)
}

fn user_table<'a>(
    catalog: &'a ClusterCatalog,
    schema: &str,
    table: &str,
) -> Result<&'a TableInfo, ClusterStatsLockError> {
    catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase().eq_ignore_ascii_case(schema))
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|stored| stored.name.lowercase().eq_ignore_ascii_case(table))
        })
        .ok_or_else(|| ClusterStatsLockError::MissingTable {
            schema: schema.to_owned(),
            table: table.to_owned(),
        })
}

fn full_view(table: &TableInfo) -> SystemTableView {
    let names = table
        .cols()
        .iter_deref()
        .map(|column| column.read().name.lowercase().to_owned())
        .collect::<Vec<_>>();
    let borrowed = names.iter().map(String::as_str).collect::<Vec<_>>();
    SystemTableView::project(table.name.original(), table, &borrowed)
}

fn column_id(table: &TableInfo, name: &str) -> Result<i64, ClusterStatsLockError> {
    table
        .find_public_column_by_name(name)
        .map(|column| column.read().id)
        .ok_or_else(|| {
            ClusterStatsLockError::Invalid(format!(
                "{}.{} has no column `{name}`",
                table.name.original(),
                table.id
            ))
        })
}

fn set(
    table: &TableInfo,
    row: &mut RowValues,
    column: &str,
    value: Datum,
) -> Result<(), ClusterStatsLockError> {
    row.insert(column_id(table, column)?, value);
    Ok(())
}

fn row_i64(table: &TableInfo, row: &RowValues, column: &str) -> Result<i64, ClusterStatsLockError> {
    datum_i64(row.get(&column_id(table, column)?), column)
}

fn row_u64(table: &TableInfo, row: &RowValues, column: &str) -> Result<u64, ClusterStatsLockError> {
    match row.get(&column_id(table, column)?) {
        Some(Datum::UInt(value)) => Ok(*value),
        Some(Datum::Int(value)) => u64::try_from(*value)
            .map_err(|_| ClusterStatsLockError::Invalid(format!("invalid {column}: {value}"))),
        value => Err(ClusterStatsLockError::Invalid(format!(
            "invalid {column}: {value:?}"
        ))),
    }
}

fn datum_i64(value: Option<&Datum>, column: &str) -> Result<i64, ClusterStatsLockError> {
    match value {
        Some(Datum::Int(value)) => Ok(*value),
        Some(Datum::UInt(value)) => i64::try_from(*value)
            .map_err(|_| ClusterStatsLockError::Invalid(format!("invalid {column}: {value}"))),
        value => Err(ClusterStatsLockError::Invalid(format!(
            "invalid {column}: {value:?}"
        ))),
    }
}

#[cfg(test)]
mod statement_tests {
    use super::*;
    use crate::cluster_catalog::{
        load_cluster_catalog, ClusterCatalogError, MetaPairs, MetaSnapshot,
    };
    use crate::cluster_stats_load::ClusterStatsLoader;
    use crate::cluster_stats_write::plan_loaded_stats_meta_write;
    use crate::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapEnvironment};
    use std::collections::BTreeMap;
    use tidb_datatype::TimeType;
    use tidb_txnkv::transaction::OptimisticMutationKind;

    #[derive(Default)]
    struct MetaStore {
        pairs: BTreeMap<Vec<u8>, Vec<u8>>,
    }

    impl MetaSnapshot for MetaStore {
        fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
            Ok(self.pairs.get(raw_key).cloned())
        }

        fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
            Ok(self
                .pairs
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }
    }

    impl MetaStore {
        fn apply(&mut self, mutations: &[OptimisticMutation]) {
            for mutation in mutations {
                match mutation.kind() {
                    OptimisticMutationKind::Insert => {
                        assert!(self
                            .pairs
                            .insert(mutation.key().to_vec(), mutation.value().to_vec())
                            .is_none());
                    }
                    OptimisticMutationKind::PutExisting
                    | OptimisticMutationKind::IndexPut
                    | OptimisticMutationKind::UniqueIndexInsert
                    | OptimisticMutationKind::MetaPut
                    | OptimisticMutationKind::SystemRowPut => {
                        self.pairs
                            .insert(mutation.key().to_vec(), mutation.value().to_vec());
                    }
                    OptimisticMutationKind::Delete
                    | OptimisticMutationKind::IndexDelete
                    | OptimisticMutationKind::MetaDelete
                    | OptimisticMutationKind::SystemRowDelete => {
                        self.pairs.remove(mutation.key());
                    }
                    OptimisticMutationKind::LockOnly => {}
                }
            }
        }
    }

    fn timestamp() -> Time {
        Time::from_date_checked(2026, 8, 31, 12, 0, 0, 0, TimeType::Timestamp, 0).unwrap()
    }

    fn bootstrapped() -> MetaStore {
        let mut store = MetaStore::default();
        let write = plan_mysql_bootstrap(
            &mut store,
            468_772_000_000_000_000,
            &BootstrapEnvironment {
                system_tz: "UTC".to_owned(),
                new_collation_enabled: true,
                cluster_id: 7,
                current_timestamp: timestamp(),
                ddl_table_version: 0,
            },
        )
        .unwrap();
        store.apply(&write.mutations);
        store
    }

    #[test]
    fn lock_insert_and_meta_version_are_separate_go_statements() {
        const TABLE_ID: i64 = 42;
        let mut store = bootstrapped();
        let catalog = load_cluster_catalog(&mut store).unwrap();
        let meta =
            plan_loaded_stats_meta_write(&mut store, &catalog, TABLE_ID, 5, 2, 10, timestamp())
                .unwrap();
        store.apply(&meta.mutations);

        let insert = plan_cluster_insert_lock(&mut store, &catalog, TABLE_ID, timestamp()).unwrap();
        assert!(!insert.is_empty());
        store.apply(&insert);
        let loader = ClusterStatsLoader::locate(&catalog).unwrap();
        assert_eq!(
            loader.load_meta(&mut store, TABLE_ID).unwrap(),
            Some((10, 0, 2, 5, 10))
        );

        let version = plan_cluster_update_meta_version(&mut store, &catalog, TABLE_ID, 11).unwrap();
        assert!(!version.is_empty());
        store.apply(&version);
        assert_eq!(
            loader.load_meta(&mut store, TABLE_ID).unwrap(),
            Some((11, 0, 2, 5, 10))
        );

        let duplicate =
            plan_cluster_insert_lock(&mut store, &catalog, TABLE_ID, timestamp()).unwrap();
        assert!(
            !duplicate.is_empty(),
            "ON DUPLICATE KEY UPDATE must still lock the existing row"
        );
    }
}
