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
use crate::mysql_system_tables::{
    scan_system_table, HandleLayout, SystemRow, SystemTableError, SystemTableView,
};
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

/// Planned mutations and optional skipped-target warning.
#[derive(Debug, Default)]
pub struct ClusterStatsLockPlan {
    /// Mutations committed atomically.
    pub mutations: Vec<OptimisticMutation>,
    /// Go's warning text, empty when no target was skipped.
    pub warning: String,
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

/// Plans one statement against the catalog and rows from the same snapshot.
pub fn plan_cluster_stats_lock<S: MetaSnapshot>(
    snapshot: &mut S,
    catalog: &ClusterCatalog,
    statement: &ClusterStatsLockStatement,
    start_ts: u64,
    now: Time,
) -> Result<ClusterStatsLockPlan, ClusterStatsLockError> {
    if statement.targets.is_empty() {
        return Err(ClusterStatsLockError::Invalid(if statement.lock {
            "Lock Stats: table should not empty".to_owned()
        } else {
            "Unlock Stats: table should not empty ".to_owned()
        }));
    }
    let lock_table = system_table(catalog, LOCK_TABLE)?;
    let meta_table = system_table(catalog, META_TABLE)?;
    if matches!(HandleLayout::of(lock_table), HandleLayout::RowId)
        || matches!(HandleLayout::of(meta_table), HandleLayout::RowId)
    {
        return Err(ClusterStatsLockError::Invalid(
            "the pinned Go statistics lock tables require clustered primary keys".to_owned(),
        ));
    }
    let locks = read_rows(snapshot, lock_table)?;
    let meta = read_rows(snapshot, meta_table)?;
    let mut transaction = ClusterTransaction {
        lock_table,
        meta_table,
        initial_locks: locks.clone(),
        locks,
        initial_meta: meta.clone(),
        meta,
        start_ts,
        now,
    };

    let only_partitions =
        statement.targets.len() == 1 && !statement.targets[0].partitions.is_empty();
    let warning = if only_partitions {
        let target = &statement.targets[0];
        let table = user_table(catalog, &target.schema, &target.table)?;
        let Some(partition) = table.get_partition_info() else {
            return Err(ClusterStatsLockError::Invalid(format!(
                "table {}.{} is not a partition table",
                target.schema.to_lowercase(),
                target.table.to_lowercase()
            )));
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
                .ok_or_else(|| ClusterStatsLockError::UnknownPartition {
                    partition: written.to_lowercase(),
                    table: table.name.original().to_owned(),
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
            tidb_stats::add_locked_partitions(&mut transaction, table.id, &displayed, &partitions)?
        } else {
            tidb_stats::remove_locked_partitions(
                &mut transaction,
                table.id,
                &displayed,
                &partitions,
            )?
        }
    } else {
        let mut tables = BTreeMap::new();
        for target in &statement.targets {
            let table = user_table(catalog, &target.schema, &target.table)?;
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
            tidb_stats::add_locked_tables(&mut transaction, &tables)?
        } else {
            tidb_stats::remove_locked_tables(&mut transaction, &tables)?
        }
    };
    Ok(ClusterStatsLockPlan {
        mutations: transaction.finish()?,
        warning,
    })
}

struct ClusterTransaction<'a> {
    lock_table: &'a TableInfo,
    meta_table: &'a TableInfo,
    initial_locks: BTreeMap<i64, RowValues>,
    locks: BTreeMap<i64, RowValues>,
    initial_meta: BTreeMap<i64, RowValues>,
    meta: BTreeMap<i64, RowValues>,
    start_ts: u64,
    now: Time,
}

impl StatsLockTransaction for ClusterTransaction<'_> {
    type Error = ClusterStatsLockError;

    fn query_locked_tables(&mut self) -> Result<BTreeSet<i64>, Self::Error> {
        Ok(self.locks.keys().copied().collect())
    }

    fn insert_lock_and_update_meta_version(&mut self, table_id: i64) -> Result<(), Self::Error> {
        if !self.locks.contains_key(&table_id) {
            let mut row = defaults_row(self.lock_table, self.now)?;
            set(self.lock_table, &mut row, "table_id", Datum::Int(table_id))?;
            self.locks.insert(table_id, row);
        }
        if let Some(row) = self.meta.get_mut(&table_id) {
            set(self.meta_table, row, "version", Datum::UInt(self.start_ts))?;
        }
        Ok(())
    }

    fn lock_delta(&mut self, table_id: i64) -> Result<(i64, i64), Self::Error> {
        let Some(row) = self.locks.get(&table_id) else {
            return Ok((0, 0));
        };
        Ok((
            row_i64(self.lock_table, row, "count")?,
            row_i64(self.lock_table, row, "modify_count")?,
        ))
    }

    fn update_meta_delta(
        &mut self,
        table_id: i64,
        count_delta: i64,
        modify_count_delta: i64,
    ) -> Result<(), Self::Error> {
        let Some(row) = self.meta.get_mut(&table_id) else {
            return Ok(());
        };
        let count = i128::from(row_u64(self.meta_table, row, "count")?) + i128::from(count_delta);
        let count = u64::try_from(count.max(0))
            .map_err(|_| ClusterStatsLockError::Invalid("stats_meta.count overflow".to_owned()))?;
        let modify_count = row_i64(self.meta_table, row, "modify_count")?
            .checked_add(modify_count_delta)
            .ok_or_else(|| {
                ClusterStatsLockError::Invalid("stats_meta.modify_count overflow".to_owned())
            })?;
        set(self.meta_table, row, "version", Datum::UInt(self.start_ts))?;
        set(self.meta_table, row, "count", Datum::UInt(count))?;
        set(
            self.meta_table,
            row,
            "modify_count",
            Datum::Int(modify_count),
        )?;
        Ok(())
    }

    fn delete_lock(&mut self, table_id: i64) -> Result<(), Self::Error> {
        self.locks.remove(&table_id);
        Ok(())
    }
}

impl ClusterTransaction<'_> {
    fn finish(self) -> Result<Vec<OptimisticMutation>, ClusterStatsLockError> {
        let mut mutations = diff_rows(self.lock_table, &self.initial_locks, &self.locks)?;
        mutations.extend(diff_rows(self.meta_table, &self.initial_meta, &self.meta)?);
        Ok(mutations)
    }
}

fn diff_rows(
    table: &TableInfo,
    initial: &BTreeMap<i64, RowValues>,
    final_rows: &BTreeMap<i64, RowValues>,
) -> Result<Vec<OptimisticMutation>, ClusterStatsLockError> {
    let ids = initial
        .keys()
        .chain(final_rows.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut mutations = Vec::new();
    for id in ids {
        match (initial.get(&id), final_rows.get(&id)) {
            (None, Some(final_row)) => {
                mutations.extend(store_clustered_row(table, None, final_row)?);
            }
            (Some(initial_row), None) => {
                mutations.extend(delete_clustered_row(table, initial_row)?);
            }
            (Some(initial_row), Some(final_row)) if initial_row != final_row => {
                mutations.extend(store_clustered_row(table, Some(initial_row), final_row)?);
            }
            _ => {}
        }
    }
    Ok(mutations)
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
